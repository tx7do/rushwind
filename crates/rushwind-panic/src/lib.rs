//! Pluggable process-level panic reporting for RushWind.
//!
//! One panic-hook dispatcher, installed exactly once, over a reporter
//! registry that plugs and unplugs without races. The authoritative
//! design document — the audit this closes, the race model, and the
//! boundary semantics — lives at `docs/panic-reporting.md`.
//!
//! # Why one hook covers everything
//!
//! `set_hook`-installed hooks fire **before** `catch_unwind` catches
//! the unwind. A process-wide dispatcher therefore reports every
//! panic — App lifecycle panics that a supervisor will aggregate,
//! broker handler panics that die silently at a detached spawn
//! boundary, everything — without wrapping a single call site.
//!
//! # The dispatch pipeline
//!
//! The hook itself does the minimum: capture the payload message and
//! location into a [`PanicReport`], enqueue it, and **wait** for the
//! standing worker thread to deliver it. Reporters run on the worker,
//! outside the panic machinery entirely. Two laws follow from that
//! placement:
//!
//! - **Crash-flush.** `dispatch` returns only after the report has
//!   been delivered, so a fatal uncaught panic logs before its unwind
//!   proceeds toward process exit.
//! - **A panicking reporter cannot kill the process.** On current std
//!   a panic raised *inside* hook machinery aborts unconditionally —
//!   `catch_unwind` there is decoration. The worker's calls sit
//!   outside that machinery, so each is `catch_unwind`-wrapped and a
//!   panicking reporter costs its own report, not the process; the
//!   reporters after it in the chain still run. The worker's own
//!   re-entrant hook invocation (its panic raise fires the global
//!   hook) is answered by the captured pre-existing hook only — a
//!   thread-local flag breaks the report loop.
//!
//! The law remains where it must: anything raising inside the hook
//! itself — the captured pre-existing hook, our own pre-enqueue
//! machinery — aborts the process per std's double-panic rule. That
//! is std's floor, not this crate's choice.
//!
//! # The race model, in one breath
//!
//! Install happens once, under the registry mutex, and the hook is
//! **never uninstalled** — delivery to an empty registry is a no-op
//! handshake, so "restore the old hook" does not exist as an
//! operation, and last-writer-wins cannot happen. Unplug is strictly
//! revocable: the worker holds the registry lock across reporter
//! calls, so a guard dropped mid-report waits for the in-flight call
//! and its reporter receives nothing afterwards. Reports are
//! delivered FIFO on the single worker: raise order is delivery
//! order, and concurrent dispatches serialize at the queue.
//!
//! The honest boundaries (serialization consequences, not races):
//! reporter order is registration order, concurrent `plug` calls
//! order by lock acquisition, one slow reporter delays every report
//! behind it, and a `report` that calls [`plug`]/[`plug_once`] or
//! drops a [`PlugGuard`] deadlocks — the worker holds the registry
//! lock across the call. A panic is a rare path; all of these only
//! cost something when one is in flight. The flush gate also bounds
//! the queue naturally: a thread holds at most one queued report
//! while it waits, so queue depth never exceeds the number of
//! concurrently-panicking threads — a panic storm backpressures, it
//! does not pile up memory.
//!
//! # Scoped context
//!
//! [`scoped`] attaches key-value pairs (broker: topic and message id;
//! HTTP: request identity) to a future's polls; a panic raised inside
//! a poll carries them on [`PanicReport::context`]. The implementation
//! is an executor-agnostic std TLS around each poll — no runtime
//! dependency, no unsafe.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::PanicHookInfo;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::task::{Context, Poll};
/// One panic, captured at raise time: the payload rendered through the
/// shared `&str`/`String` downcast shape, the panic location, the
/// raising thread's name, the delivery sequence, and any context
/// attached through [`scoped`].
///
/// Reporters receive this value, not the live `PanicHookInfo` —
/// delivery happens on the worker thread, after the hook that raised
/// the panic has returned.
#[derive(Debug, Clone)]
pub struct PanicReport {
    /// The payload message: `&'static str` and `String` payloads pass
    /// through; anything else (any `panic_any` value) renders as the
    /// documented fallback.
    pub message: String,
    /// `file:line:col` of the panic site, when the runtime provided
    /// one.
    pub location: Option<String>,
    /// The raising thread's name, when it has one. Tokio names its
    /// worker threads; unnamed raw threads report `None`.
    pub thread: Option<String>,
    /// Monotonic delivery sequence: assigned at enqueue under the queue
    /// lock, so delivery observes these strictly increasing — the FIFO
    /// pin.
    pub sequence: u64,
    /// The key-value pairs attached by the innermost enclosing
    /// [`scoped`] at raise time (broker deliveries carry topic and
    /// message id here). Empty for panics outside any scope.
    pub context: Vec<(String, String)>,
}

impl PanicReport {
    /// Captures the raise-time view of `info`. Runs inside the hook —
    /// before any reporter code, so reporters see a stable snapshot
    /// even if the panic site's data is consumed by the unwind. The
    /// sequence field stays zero here; dispatch assigns it under the
    /// queue lock, where enqueue order is decided.
    fn capture(info: &PanicHookInfo<'_>) -> Self {
        Self {
            message: payload_message(info),
            location: info.location().map(|l| l.to_string()),
            thread: std::thread::current().name().map(str::to_owned),
            sequence: 0,
            context: current_context(),
        }
    }

    /// The context rendered as one compact `k=v` string — the shape the
    /// tracing backend logs and log greppers want. Empty context
    /// renders as `-`.
    pub fn context_line(&self) -> String {
        if self.context.is_empty() {
            return "-".to_owned();
        }
        self.context
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The string one payload renders as — the `&str`/`String` downcast
/// shape `rushwind-core` and `rushwind-http` each carry, unified here.
/// A non-string payload (any `panic_any` value) renders as the
/// documented fallback.
pub fn payload_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

// The raise-time context stack. `scoped` sets it around each poll of
// the wrapped future — a panic raised inside the poll fires the hook
// while the value is in place, so the report carries the delivery's
// identity. An executor-agnostic std TLS: no tokio dependency, works
// on any runtime that polls the future.
type ContextPairs = Arc<[(String, String)]>;

thread_local! {
    static CURRENT_CONTEXT: RefCell<Option<ContextPairs>> = const { RefCell::new(None) };
}

/// Reads the innermost enclosing scope's pairs. Runs inside the hook.
fn current_context() -> Vec<(String, String)> {
    CURRENT_CONTEXT.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|context| context.iter().cloned().collect())
            .unwrap_or_default()
    })
}

/// The poll-scoped restoration: the previous scope goes back on drop,
/// so nested scopes unwind correctly — including through a panic.
struct ContextGuard {
    previous: Option<ContextPairs>,
}

impl ContextGuard {
    fn enter(context: ContextPairs) -> Self {
        let previous = CURRENT_CONTEXT.with(|slot| slot.borrow_mut().replace(context));
        Self { previous }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CURRENT_CONTEXT.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

/// Attaches `context` to every panic raised while polling `fut`, on
/// any thread the runtime happens to poll it on.
///
/// The broker engines wrap every handler dispatch with this, carrying
/// the delivery's topic and message id; the HTTP edge carries the
/// request identity. The context rides a thread-local set around each
/// poll — the hook reads it at raise time, while the value is in
/// place. Cost: one boxed inner future and one pair list per wrap
/// (per delivery), both small.
///
/// Nesting composes: an inner [`scoped`] shadows the outer while
/// polled; the outer returns when the inner's poll ends, unwind
/// included.
pub fn scoped<K, V, F>(context: impl IntoIterator<Item = (K, V)>, fut: F) -> Scoped<F>
where
    K: Into<String>,
    V: Into<String>,
    F: Future,
{
    let pairs: Vec<(String, String)> = context
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
    Scoped {
        context: pairs.into(),
        inner: Box::pin(fut),
    }
}

/// The [`scoped`] future: context in place around every poll, restored
/// on every exit path.
pub struct Scoped<F> {
    context: Arc<[(String, String)]>,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Projection through Pin<Box<F>> is safe by construction — no
        // unsafe in this crate.
        let this = self.get_mut();
        let _guard = ContextGuard::enter(Arc::clone(&this.context));
        this.inner.as_mut().poll(cx)
    }
}

/// The panic-reporting plug point.
///
/// Implementations are [`Send`] + [`Sync`] and shareable behind an
/// [`Arc`]. `report` runs on the worker thread, one call per report,
/// in registration order. It must not panic (a panicking reporter
/// costs its own report — the chain continues without it) and must not
/// call [`plug`]/[`plug_once`] or drop a [`PlugGuard`] — delivery
/// holds the registry lock across the call, so re-entering it
/// deadlocks.
pub trait PanicReporter: Send + Sync {
    /// Receives one panic. Report delivery is FIFO from the hook —
    /// treat this as the hot path of a rare event: no panics, no
    /// registry re-entry, minimal work.
    fn report(&self, report: &PanicReport);
}

/// The captured pre-existing hook, stored outside the registry so the
/// worker's re-entrant dispatch path can reach it without touching the
/// registry lock (delivery holds that lock across reporter calls).
/// Written once at install time, read-only afterwards.
type PrevHook = Box<dyn Fn(&PanicHookInfo<'_>) + Send + Sync>;

static PREV_HOOK: OnceLock<PrevHook> = OnceLock::new();

struct Entry {
    /// Keys [`PlugGuard`] removal.
    id: u64,
    /// The reporter's concrete type when registered through
    /// [`plug_once`] — the dedup key. `None` for plain [`plug`]
    /// registrations, which always add.
    kind: Option<TypeId>,
    reporter: Arc<dyn PanicReporter>,
}

struct Registry {
    entries: Vec<Entry>,
    next_id: u64,
}

impl Registry {
    const fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
        }
    }
}

static REGISTRY: Mutex<Registry> = Mutex::new(Registry::new());

/// Locks the registry, tolerating poison: a panic unwinding through a
/// registry holder cannot lose the mechanism — the registry carries no
/// invariant worth poisoning.
fn registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner())
}

/// One queued panic: the captured report plus the flush handshake the
/// dispatching thread waits on.
struct Job {
    report: PanicReport,
    done: Sender<()>,
}

static QUEUE: Mutex<VecDeque<Job>> = Mutex::new(VecDeque::new());
static QUEUE_READY: Condvar = Condvar::new();

/// The enqueue-order counter. Assignments happen under the queue lock,
/// so the sequence numbers enter the queue strictly increasing and the
/// worker's FIFO pop delivers them in the same order.
static SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Locks the queue, poison-tolerant for the same reason as the
/// registry.
fn queue() -> MutexGuard<'static, VecDeque<Job>> {
    QUEUE.lock().unwrap_or_else(|e| e.into_inner())
}

// Marks the dispatching thread. The worker's own panic raises fire
// the global hook; on the worker those run the captured hook only —
// delivering from inside delivery would re-enqueue onto the very
// worker that is mid-call, and wait on it.
thread_local! {
    static WORKER_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// Set when the worker could not be spawned or died from machinery
/// outside the per-report guards. Dispatch then proceeds without
/// waiting — a lost report beats a hung process.
static WORKER_DOWN: AtomicBool = AtomicBool::new(false);

/// Installs the dispatcher once. The caller holds the registry lock.
/// Every `plug` calls this; only the first call does the work — the
/// `get_or_init` closure runs exactly once, and spawn + set_hook ride
/// inside it.
fn ensure_installed() {
    let mut first = false;
    let _ = PREV_HOOK.get_or_init(|| {
        first = true;
        // The take/spawn/set sequence runs under the registry mutex,
        // and this is the only place in the crate that touches the
        // hook slot — there is no interleaving to lose a chain to. The
        // captured hook lands and the worker starts *before* the
        // dispatcher goes live, so a live dispatcher never observes a
        // missing prev or worker.
        if std::thread::Builder::new()
            .name("rushwind-panic-worker".to_owned())
            .spawn(worker_loop)
            .is_err()
        {
            WORKER_DOWN.store(true, Ordering::Release);
        }
        std::panic::take_hook()
    });
    if first {
        std::panic::set_hook(Box::new(dispatch));
    }
}

/// The dispatcher: the one hook ever installed by this crate.
fn dispatch(info: &PanicHookInfo<'_>) {
    // Worker re-entry: this raise belongs to a reporter mid-delivery.
    // Surface it through the captured hook and return — enqueueing
    // would wait on the worker that is busy inside this very raise.
    if WORKER_THREAD.with(Cell::get) {
        if let Some(prev) = PREV_HOOK.get() {
            prev(info);
        }
        return;
    }

    // The captured hook runs synchronously in the hook, raise-time
    // semantics preserved. A panic raised *here* — by the captured
    // hook or by the capture below — aborts the process per std's
    // double-panic rule; no guard inside the hook can change that.
    if let Some(prev) = PREV_HOOK.get() {
        prev(info);
    }

    if WORKER_DOWN.load(Ordering::Acquire) {
        return;
    }

    let mut report = PanicReport::capture(info);
    let (done, wait) = channel();
    // Sequence assignment rides the queue lock: enqueue order is
    // decided here, and the worker's FIFO pop preserves it.
    {
        let mut q = queue();
        report.sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        q.push_back(Job { report, done });
    }
    QUEUE_READY.notify_one();
    // The flush gate: the unwind of this panic proceeds only after the
    // report has been delivered — a fatal crash still logs. A closed
    // channel means the worker vanished between the WORKER_DOWN check
    // and now; proceed rather than hang on a corpse.
    let _ = wait.recv();
}

/// The standing delivery thread: FIFO out of the queue, the registry
/// lock held across each report's calls (that is what makes unplug
/// strictly revocable), every reporter call caught individually.
fn worker_loop() {
    WORKER_THREAD.with(|w| w.set(true));
    loop {
        // The belt around the whole iteration: anything raising in our
        // own machinery (not a reporter — those are caught inner)
        // retires the worker and unblocks waiting dispatches instead
        // of hanging them on a corpse.
        let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let job = {
                let mut q = queue();
                loop {
                    match q.pop_front() {
                        Some(job) => break job,
                        None => q = QUEUE_READY.wait(q).unwrap_or_else(|e| e.into_inner()),
                    }
                }
            };
            let reg = registry();
            for entry in &reg.entries {
                let reporter = Arc::clone(&entry.reporter);
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reporter.report(&job.report)
                }));
            }
            drop(reg);
            let _ = job.done.send(());
        }));
        if delivered.is_err() {
            // Retire the worker: drop every queued job — their senders
            // close, so dispatches already waiting on the flush gate
            // proceed instead of hanging on a corpse.
            queue().clear();
            WORKER_DOWN.store(true, Ordering::Release);
            return;
        }
    }
}

/// Registers `reporter` at the end of the chain and returns the guard
/// that unplugs exactly it.
///
/// Reporters run in registration order; concurrent calls order by lock
/// acquisition. The dispatcher hook and the delivery worker install on
/// the first call in the process and never uninstall.
pub fn plug(reporter: Arc<dyn PanicReporter>) -> PlugGuard {
    let mut reg = registry();
    ensure_installed();
    let id = reg.next_id;
    reg.next_id += 1;
    reg.entries.push(Entry {
        id,
        kind: None,
        reporter,
    });
    PlugGuard { id }
}

/// Registers `reporter`, replacing any existing registration of the
/// same concrete reporter type.
///
/// The idempotence plug for assembly-style callers: re-running
/// bootstrap, or re-entering a test harness, must not stack duplicate
/// reporters. The previous registration's guard becomes stale — its
/// drop removes nothing.
pub fn plug_once<T: PanicReporter + 'static>(reporter: Arc<T>) -> PlugGuard {
    let mut reg = registry();
    ensure_installed();
    let kind = TypeId::of::<T>();
    reg.entries.retain(|entry| entry.kind != Some(kind));
    let id = reg.next_id;
    reg.next_id += 1;
    reg.entries.push(Entry {
        id,
        kind: Some(kind),
        reporter,
    });
    PlugGuard { id }
}

/// Unplugs one reporter.
///
/// Dropping the guard removes the reporter it was minted for — nothing
/// else. The dispatcher hook itself stays installed for the life of
/// the process (delivery to an empty registry is a no-op handshake, so
/// this is unobservable). Dropping a stale guard — one already
/// superseded by [`plug_once`] — removes nothing.
pub struct PlugGuard {
    id: u64,
}

impl fmt::Debug for PlugGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlugGuard").field("id", &self.id).finish()
    }
}

impl Drop for PlugGuard {
    fn drop(&mut self) {
        registry().entries.retain(|entry| entry.id != self.id);
    }
}

#[cfg(feature = "tracing")]
/// The default backend: one `tracing::error!` event per panic, under
/// the `rushwind.panic` target, carrying the payload message, the
/// delivery sequence, and — when present — the panic location, the
/// raising thread's name, and the [`scoped`] context as one `k=v`
/// line. Export is a subscriber decision — the same
/// bricks-versus-baseplate split as every other RushWind crate.
pub struct TracingReporter;

#[cfg(feature = "tracing")]
impl PanicReporter for TracingReporter {
    fn report(&self, report: &PanicReport) {
        match &report.location {
            Some(location) => tracing::error!(
                target: "rushwind.panic",
                message = %report.message,
                sequence = %report.sequence,
                location = %location,
                thread = ?report.thread,
                context = %report.context_line(),
                "panic captured"
            ),
            None => tracing::error!(
                target: "rushwind.panic",
                message = %report.message,
                sequence = %report.sequence,
                thread = ?report.thread,
                context = %report.context_line(),
                "panic captured"
            ),
        }
    }
}

#[cfg(feature = "metrics")]
/// The metrics bridge: one `panics_total` counter increment per
/// panic, over whatever [`rushwind_metrics::Metrics`] engine the
/// application assembled (Prometheus pull, OTLP push, DogStatsD).
/// Pair it with [`TracingReporter`] — the counter answers "how
/// often", the log answers "what and where".
pub struct MetricsReporter {
    metrics: Arc<dyn rushwind_metrics::Metrics>,
}

#[cfg(feature = "metrics")]
impl MetricsReporter {
    /// Wraps the assembled metrics engine.
    pub fn new(metrics: Arc<dyn rushwind_metrics::Metrics>) -> Arc<Self> {
        Arc::new(Self { metrics })
    }
}

#[cfg(feature = "metrics")]
impl PanicReporter for MetricsReporter {
    fn report(&self, _report: &PanicReport) {
        self.metrics.counter("panics_total", 1.0, &[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Mutex;

    /// A counting reporter that records every report message it sees.
    struct Recorder {
        messages: Mutex<Vec<String>>,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                messages: Mutex::new(Vec::new()),
            })
        }

        fn seen(&self) -> Vec<String> {
            self.messages.lock().expect("recorder poisoned").clone()
        }
    }

    impl PanicReporter for Recorder {
        fn report(&self, report: &PanicReport) {
            self.messages
                .lock()
                .expect("recorder poisoned")
                .push(report.message.clone());
        }
    }

    /// A reporter with its own type, for `plug_once` dedup pins.
    struct OtherRecorder(Arc<Recorder>);

    impl PanicReporter for OtherRecorder {
        fn report(&self, report: &PanicReport) {
            self.0.report(report);
        }
    }

    /// A reporter that panics on every report — the isolation pin.
    struct PanickingReporter;

    impl PanicReporter for PanickingReporter {
        fn report(&self, _report: &PanicReport) {
            panic!("reporter panicked");
        }
    }

    /// A reporter tagging a shared order log — the registration-order
    /// pin.
    struct Tagged(String, Arc<Mutex<Vec<String>>>);

    impl PanicReporter for Tagged {
        fn report(&self, report: &PanicReport) {
            self.1
                .lock()
                .expect("order log poisoned")
                .push(format!("{}:{}", self.0, report.message));
        }
    }

    fn controlled_panic(message: &'static str) {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| panic!("{}", message)));
    }

    #[test]
    fn plug_roundtrip_and_strict_revocation() {
        let recorder = Recorder::new();
        let guard = plug(recorder.clone());

        controlled_panic("plug-roundtrip-one");
        assert!(recorder.seen().contains(&"plug-roundtrip-one".to_owned()));

        drop(guard);
        let seen_before = recorder.seen().len();
        controlled_panic("plug-roundtrip-two");
        assert_eq!(recorder.seen().len(), seen_before);
    }

    #[test]
    fn hook_fires_before_catch_unwind() {
        // The pin that proves the mechanism covers the App lifecycle
        // layer: the panic below is caught, the reporter still receives
        // it — the hook runs at raise time, not at catch time.
        let recorder = Recorder::new();
        let _guard = plug(recorder.clone());

        controlled_panic("before-catch-unwind");
        assert!(recorder.seen().contains(&"before-catch-unwind".to_owned()));
    }

    #[test]
    fn payload_message_shapes() {
        let recorder = Recorder::new();
        let _guard = plug(recorder.clone());

        controlled_panic("static str shape");
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            panic!("{}", format!("owned {} shape", "string"))
        }));
        let _ = panic::catch_unwind(AssertUnwindSafe(|| panic::panic_any(42_i32)));

        let seen = recorder.seen();
        assert!(seen.contains(&"static str shape".to_owned()));
        assert!(seen.contains(&"owned string shape".to_owned()));
        assert!(seen.contains(&"unknown panic payload".to_owned()));
    }

    #[test]
    fn plug_once_replaces_same_type() {
        let first = Recorder::new();
        let stale = plug_once(first.clone());
        let second = Recorder::new();
        let live = plug_once(second.clone());

        controlled_panic("plug-once-dedup");
        assert!(second.seen().contains(&"plug-once-dedup".to_owned()));
        assert!(!first.seen().contains(&"plug-once-dedup".to_owned()));

        // The stale guard is stale: dropping it must not remove the
        // live registration.
        drop(stale);
        controlled_panic("plug-once-survives-stale-drop");
        assert!(second
            .seen()
            .contains(&"plug-once-survives-stale-drop".to_owned()));

        // Different types coexist.
        let other = Recorder::new();
        let _other_guard = plug_once(Arc::new(OtherRecorder(other.clone())));
        controlled_panic("plug-once-coexist");
        assert!(second.seen().contains(&"plug-once-coexist".to_owned()));
        assert!(other.seen().contains(&"plug-once-coexist".to_owned()));
        drop(live);
    }

    #[test]
    fn panicking_reporter_costs_its_report_not_the_process() {
        let panicking = plug(Arc::new(PanickingReporter));
        let successor = Recorder::new();
        let _successor_guard = plug(successor.clone());

        // The process must survive a reporter that always panics, the
        // reporters behind it must still receive the panic, and the
        // reporter panic itself must not loop back through the chain.
        controlled_panic("chain-continues");
        assert!(successor
            .seen()
            .iter()
            .any(|m| m.contains("chain-continues")));
        assert!(!successor
            .seen()
            .iter()
            .any(|m| m.contains("reporter panicked")));
        drop(panicking);
    }

    #[test]
    fn reporters_run_in_registration_order() {
        let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let first = plug(Arc::new(Tagged("first".to_owned(), Arc::clone(&order))));
        let _second = plug(Arc::new(Tagged("second".to_owned(), Arc::clone(&order))));

        controlled_panic("ordering");
        let seen = order.lock().expect("order log poisoned").clone();
        let mine: Vec<&String> = seen
            .iter()
            .filter(|entry| entry.contains("ordering"))
            .collect();
        assert_eq!(
            mine,
            vec![&"first:ordering".to_owned(), &"second:ordering".to_owned()]
        );
        drop(first);
    }

    #[test]
    fn concurrent_plug_unplug_smoke() {
        let rounds = 200u32;
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(std::thread::spawn(move || {
                for _ in 0..rounds {
                    let recorder = Recorder::new();
                    let guard = plug(recorder);
                    controlled_panic("stress");
                    drop(guard);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("stress thread panicked");
        }
    }

    /// A reporter capturing the full report — the field-level pins.
    #[derive(Default)]
    struct FullRecorder {
        reports: Mutex<Vec<PanicReport>>,
    }

    impl PanicReporter for FullRecorder {
        fn report(&self, report: &PanicReport) {
            self.reports
                .lock()
                .expect("recorder poisoned")
                .push(report.clone());
        }
    }

    #[test]
    fn thread_and_sequence_fields() {
        let recorder = Arc::new(FullRecorder::default());
        let _guard = plug(recorder.clone() as Arc<dyn PanicReporter>);

        controlled_panic("fields-one");
        controlled_panic("fields-two");

        let reports = recorder.reports.lock().expect("recorder poisoned");
        let mine: Vec<&PanicReport> = reports
            .iter()
            .filter(|r| r.message.starts_with("fields-"))
            .collect();
        assert_eq!(mine.len(), 2);
        // The raising thread's name rides the report.
        assert_eq!(
            mine[0].thread,
            std::thread::current().name().map(str::to_owned)
        );
        // Sequences are strictly increasing in delivery order — the
        // FIFO pin: assignment happens under the queue lock, delivery
        // pops FIFO.
        assert!(mine[0].sequence < mine[1].sequence);
    }

    #[test]
    fn context_rides_scoped_futures() {
        let recorder = Arc::new(FullRecorder::default());
        let _guard = plug(recorder.clone() as Arc<dyn PanicReporter>);

        // A panic raised inside a scoped future's poll carries the
        // scope's pairs — the broker delivery shape.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            futures::executor::block_on(scoped(
                [("topic", "orders"), ("message_id", "m-1")],
                async {
                    panic!("inside-context");
                },
            ));
        }));

        // A panic outside any scope carries none.
        controlled_panic("outside-context");

        let reports = recorder.reports.lock().expect("recorder poisoned");
        let inside = reports
            .iter()
            .find(|r| r.message.contains("inside-context"))
            .expect("scoped panic delivered");
        assert_eq!(
            inside.context,
            vec![
                ("topic".to_owned(), "orders".to_owned()),
                ("message_id".to_owned(), "m-1".to_owned())
            ]
        );
        let outside = reports
            .iter()
            .find(|r| r.message.contains("outside-context"))
            .expect("plain panic delivered");
        assert!(outside.context.is_empty());
    }

    #[cfg(feature = "tracing")]
    #[test]
    fn tracing_reporter_output_shape() {
        // The default backend's event shape, pinned against a capturing
        // subscriber. The reporter is called directly — the delivery
        // pipeline has its own pins, and `with_default` is
        // thread-scoped while delivery runs on the worker.
        struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

        impl std::io::Write for CaptureWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .expect("capture poisoned")
                    .extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer_buffer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || CaptureWriter(Arc::clone(&writer_buffer)))
            .with_ansi(false)
            .finish();

        let report = PanicReport {
            message: "output-shape".to_owned(),
            location: Some("src/lib.rs:1:1".to_owned()),
            thread: Some("the-raiser".to_owned()),
            sequence: 7,
            context: vec![("topic".to_owned(), "orders".to_owned())],
        };
        tracing::subscriber::with_default(subscriber, || TracingReporter.report(&report));

        let captured = String::from_utf8(buffer.lock().expect("capture poisoned").clone())
            .expect("utf8 capture");
        assert!(captured.contains("rushwind.panic"), "{captured}");
        assert!(captured.contains("output-shape"), "{captured}");
        assert!(captured.contains("src/lib.rs:1:1"), "{captured}");
        assert!(captured.contains("the-raiser"), "{captured}");
        assert!(captured.contains("topic=orders"), "{captured}");
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn metrics_reporter_counts() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct MockMetrics {
            counters: Mutex<Vec<(String, f64)>>,
        }

        impl rushwind_metrics::Metrics for MockMetrics {
            fn counter(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
                self.counters
                    .lock()
                    .expect("mock poisoned")
                    .push((name.to_owned(), value));
            }
            fn histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
            fn gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        }

        let mock = Arc::new(MockMetrics::default());
        let reporter = MetricsReporter::new(mock.clone() as Arc<dyn rushwind_metrics::Metrics>);
        reporter.report(&PanicReport {
            message: "metric".to_owned(),
            location: None,
            thread: None,
            sequence: 1,
            context: Vec::new(),
        });
        let counted = mock.counters.lock().expect("mock poisoned");
        assert_eq!(counted.as_slice(), [("panics_total".to_owned(), 1.0)]);
    }

    #[test]
    fn concurrent_panics_deliver_in_enqueue_order() {
        // One shared recorder across four panicking threads: the
        // delivered sequences must be exactly the assigned set, strictly
        // increasing in arrival order — FIFO under concurrency, not just
        // on one thread.
        let recorder = Arc::new(FullRecorder::default());
        let _guard = plug(recorder.clone() as Arc<dyn PanicReporter>);

        let rounds = 100u32;
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(std::thread::spawn(move || {
                for _ in 0..rounds {
                    controlled_panic("fifo-stress");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("fifo thread panicked");
        }

        let reports = recorder.reports.lock().expect("recorder poisoned");
        let sequences: Vec<u64> = reports
            .iter()
            .filter(|report| report.message.contains("fifo-stress"))
            .map(|report| report.sequence)
            .collect();
        assert_eq!(sequences.len(), (4 * rounds) as usize);
        assert!(
            sequences.windows(2).all(|window| window[0] < window[1]),
            "delivery must observe the enqueue order strictly"
        );
    }

    #[test]
    fn nested_scopes_shadow_and_restore() {
        // One executor: futures' block_on cannot nest (thread-local
        // Enter guard), so the inner panicking future is caught with
        // FutureExt::catch_unwind inside the single block_on.
        let recorder = Arc::new(FullRecorder::default());
        let _guard = plug(recorder.clone() as Arc<dyn PanicReporter>);

        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            futures::executor::block_on(async {
                scoped([("outer", "1")], async {
                    let _ = std::panic::AssertUnwindSafe(scoped([("inner", "2")], async {
                        panic!("inner-raise");
                    }))
                    .catch_unwind()
                    .await;
                    panic!("outer-raise");
                })
                .await;
            });
        }));

        let reports = recorder.reports.lock().expect("recorder poisoned");
        let inner = reports
            .iter()
            .find(|report| report.message.contains("inner-raise"))
            .expect("inner panic delivered");
        assert_eq!(
            inner.context,
            vec![("inner".to_owned(), "2".to_owned())],
            "the inner scope shadows the outer while polled"
        );
        let outer = reports
            .iter()
            .find(|report| report.message.contains("outer-raise"))
            .expect("outer panic delivered");
        assert_eq!(
            outer.context,
            vec![("outer".to_owned(), "1".to_owned())],
            "the outer scope returns after the inner poll ends — unwind included"
        );
    }

    #[cfg(feature = "tracing")]
    #[test]
    fn tracing_reporter_absent_fields_stay_absent() {
        // The None arm: no location recorded at all, empty context
        // renders as the dash placeholder.
        struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

        impl std::io::Write for CaptureWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .expect("capture poisoned")
                    .extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer_buffer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || CaptureWriter(Arc::clone(&writer_buffer)))
            .with_ansi(false)
            .finish();

        let report = PanicReport {
            message: "absent-shape".to_owned(),
            location: None,
            thread: None,
            sequence: 9,
            context: Vec::new(),
        };
        tracing::subscriber::with_default(subscriber, || TracingReporter.report(&report));

        let captured = String::from_utf8(buffer.lock().expect("capture poisoned").clone())
            .expect("utf8 capture");
        assert!(captured.contains("absent-shape"), "{captured}");
        assert!(captured.contains("context=-"), "{captured}");
        assert!(
            !captured.contains("location="),
            "the None arm must not record a location field: {captured}"
        );
    }
}

//! [`App`] and its [`Builder`].

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use rushwind_transport::{Server, ServerError, StopSignal};
use tokio::sync::watch;
use tokio::time::error::Elapsed;
use tokio::time::{sleep_until, timeout};

/// A before/after-stop hook. Receives the phase budget and returns a
/// bounded future.
type Hook = Box<dyn Fn(Duration) -> HookFuture + Send + Sync>;

/// Future type of a lifecycle hook.
type HookFuture = Pin<Box<dyn Future<Output = Result<(), ServerError>> + Send>>;

/// Frozen configuration of an [`App`], assembled by [`Builder`].
struct Options {
    name: Option<String>,
    version: Option<String>,
    servers: Vec<Arc<dyn Server>>,
    stop_timeout: Duration,
    hooks_before: Vec<Hook>,
    hooks_after: Vec<Hook>,
}

/// Builder for [`App`], following the composable-option pattern: chain
/// `server(...)`, `before_stop(...)` / `after_stop(...)` and friends to
/// assemble the application piece by piece.
pub struct Builder {
    name: Option<String>,
    version: Option<String>,
    servers: Vec<Arc<dyn Server>>,
    stop_timeout: Option<Duration>,
    hooks_before: Vec<Hook>,
    hooks_after: Vec<Hook>,
}

impl Default for Builder {
    fn default() -> Self {
        App::builder()
    }
}

impl Builder {
    /// Sets the application name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the application version.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Attaches a server to the application. All servers start concurrently
    /// when [`App::run`] is invoked and stop concurrently during shutdown.
    pub fn server<T>(mut self, server: Arc<T>) -> Self
    where
        T: Server + 'static,
    {
        self.servers.push(server);
        self
    }

    /// Attaches an already-erased server (`Arc<dyn Server>`) to the
    /// application. Equivalent to [`Builder::server`] for servers whose
    /// concrete type is not visible at the registration site.
    pub fn erased_server(mut self, server: Arc<dyn Server>) -> Self {
        self.servers.push(server);
        self
    }

    /// Overrides the default per-phase shutdown budget (10 s).
    pub fn stop_timeout(mut self, d: Duration) -> Self {
        self.stop_timeout = Some(d);
        self
    }

    /// Registers a hook invoked sequentially **before** any server's stop is
    /// called during shutdown. Typical uses: deregister from a service
    /// registry, drain inbound queues.
    pub fn before_stop<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ServerError>> + Send + 'static,
    {
        self.hooks_before.push(Box::new(move |budget: Duration| {
            Box::pin(hook(budget)) as HookFuture
        }));
        self
    }

    /// Registers a hook invoked sequentially **after** all servers have
    /// stopped. Typical uses: close database connections, flush buffers.
    pub fn after_stop<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ServerError>> + Send + 'static,
    {
        self.hooks_after.push(Box::new(move |budget: Duration| {
            Box::pin(hook(budget)) as HookFuture
        }));
        self
    }

    /// Freezes the builder into an [`App`].
    pub fn build(self) -> App {
        let (done_tx, done_rx) = watch::channel(());
        App {
            opts: Options {
                name: self.name,
                version: self.version,
                servers: self.servers,
                stop_timeout: self.stop_timeout.unwrap_or(Duration::from_secs(10)),
                hooks_before: self.hooks_before,
                hooks_after: self.hooks_after,
            },
            stop_signal: StopSignal::new(),
            done_tx: Mutex::new(Some(done_tx)),
            done_rx,
            outcome: OnceLock::new(),
        }
    }
}

/// The application orchestrator. Owns servers and drives the fixed
/// lifecycle documented at the crate root.
pub struct App {
    opts: Options,
    stop_signal: StopSignal,
    done_tx: Mutex<Option<watch::Sender<()>>>,
    done_rx: watch::Receiver<()>,
    outcome: OnceLock<Option<ServerError>>,
}

impl App {
    /// Creates an empty [`Builder`].
    pub fn builder() -> Builder {
        Builder {
            name: None,
            version: None,
            servers: Vec::new(),
            stop_timeout: None,
            hooks_before: Vec::new(),
            hooks_after: Vec::new(),
        }
    }

    /// The configured application name, if any.
    pub fn name(&self) -> Option<&str> {
        self.opts.name.as_deref()
    }

    /// The configured application version, if any.
    pub fn version(&self) -> Option<&str> {
        self.opts.version.as_deref()
    }

    /// Triggers a graceful shutdown: every server's start future is expected
    /// to observe the stop signal and return; shutdown phases then run to
    /// completion inside [`App::run`].
    pub fn stop(&self) {
        self.stop_signal.signal();
    }

    /// Returns a fresh receiver reporting completion of [`App::run`].
    /// Awaiting `closed()` on it resolves exactly when the lifecycle —
    /// including every shutdown phase — has finished.
    pub fn subscribe_done(&self) -> watch::Receiver<()> {
        self.done_rx.clone()
    }

    /// The terminal outcome of [`App::run`], available after completion (see
    /// [`App::subscribe_done`]). `None` means the lifecycle completed without
    /// a non-cooperative error.
    pub fn outcome(&self) -> Option<ServerError> {
        self.outcome.get().and_then(|o| o.clone())
    }

    /// Runs the full lifecycle and blocks until shutdown completes.
    ///
    /// `external` is a caller-owned stop signal; firing it triggers the same
    /// shutdown path as [`App::stop`] or an OS termination signal.
    ///
    /// The terminal error aggregates the first non-cooperative server-start
    /// error (including recorded panics and timeouts), else the first
    /// non-cooperative server-stop error.
    pub async fn run(&self, external: StopSignal) -> Result<(), ServerError> {
        // Phase 1 — all servers run concurrently until a shutdown trigger.
        let mut starts: FuturesUnordered<_> = FuturesUnordered::new();
        for server in &self.opts.servers {
            let fut = AssertUnwindSafe(server.start(self.stop_signal.clone())).catch_unwind();
            starts.push(fut);
        }

        // Aggregation slot for the first non-cooperative start error, fed by
        // both the trigger loop and the drain loop below.
        let mut first_start: Option<ServerError> = None;

        if !starts.is_empty() {
            let mut os_signal = std::pin::pin!(wait_os_shutdown());
            // A single select over the shutdown triggers; any one of them
            // moves the lifecycle into its shutdown phases.
            tokio::select! {
                biased;
                // Stream branch listed first under `biased`: a completed
                // item that loses the race to a signal branch would
                // otherwise be dropped by the select, losing its error
                // record.
                r = starts.next() => {
                    if let Some(r) = r {
                        if first_start.is_none() {
                            first_start = collapse_start(r);
                        }
                    }
                }
                _ = self.stop_signal.wait() => {}
                _ = external.wait() => {}
                _ = &mut os_signal => {}
            }
        } else {
            let mut os_signal = std::pin::pin!(wait_os_shutdown());
            tokio::select! {
                _ = self.stop_signal.wait() => {}
                _ = external.wait() => {}
                _ = &mut os_signal => {}
            }
        }

        // The shutdown clock starts NOW, never at run start — each phase
        // gets its budget created at the moment it begins.
        self.stop_signal.signal();
        let deadline = tokio::time::Instant::now() + self.opts.stop_timeout;

        // Bounded drain: remaining start futures get the full budget to
        // observe the signal cooperatively; ill-behaved ones are dropped
        // (cancellation-by-drop) once the deadline lapses.
        while !starts.is_empty() {
            tokio::select! {
                biased;
                // Stream branch first under `biased` for the same
                // losslessness reason as the trigger loop above.
                r = starts.next() => {
                    match r {
                        Some(r) => {
                            if first_start.is_none() {
                                first_start = collapse_start(r);
                            }
                        }
                        None => break,
                    }
                }
                _ = sleep_until(deadline) => {
                    starts.clear();
                }
            }
        }

        // Phase 2 — before hooks, sequential, fresh budget each. Hook
        // outcomes are deliberately discarded: a failing, hanging, or
        // panicking hook must never block the teardown of the servers
        // below it — the panic is caught and discarded like any other
        // outcome, so the sequence reaches phase 3.
        for hook in &self.opts.hooks_before {
            let budget = self.opts.stop_timeout;
            let bounded = timeout(budget, hook(budget));
            let _ = AssertUnwindSafe(bounded).catch_unwind().await;
        }

        // Phase 3 — concurrent server teardown, each bounded by a fresh
        // deadline; panics are isolated into records.
        let mut stops: FuturesUnordered<_> = FuturesUnordered::new();
        for server in &self.opts.servers {
            let bounded = timeout(self.opts.stop_timeout, server.stop());
            stops.push(AssertUnwindSafe(bounded).catch_unwind());
        }
        let mut first_stop: Option<ServerError> = None;
        while let Some(r) = stops.next().await {
            if first_stop.is_none() {
                first_stop = collapse_stop(r);
            }
        }

        // Phase 4 — after hooks, same shape as phase 2: panics caught
        // and discarded so the terminal aggregation below always runs.
        for hook in &self.opts.hooks_after {
            let budget = self.opts.stop_timeout;
            let bounded = timeout(budget, hook(budget));
            let _ = AssertUnwindSafe(bounded).catch_unwind().await;
        }

        // Aggregate the terminal outcome: the first non-cooperative start
        // error, else the first non-cooperative stop error.
        let mut outcome: Option<ServerError> = None;
        if let Some(e) = first_start {
            if !is_cooperative(&e) {
                outcome = Some(e);
            }
        }
        if outcome.is_none() {
            if let Some(e) = first_stop {
                if !is_cooperative(&e) {
                    outcome = Some(e);
                }
            }
        }

        // Publish the outcome, then close the done channel so every
        // subscriber's `closed()` resolves.
        let _ = self.outcome.set(outcome.clone());
        if let Some(tx) = self.done_tx.lock().expect("done_tx poisoned").take() {
            drop(tx);
        }

        outcome.map_or(Ok(()), Err)
    }
}

/// Raw result of a server start future: `catch_unwind` wraps the server's
/// own result. Start futures carry no deadline wrapper.
type StartRawResult = Result<Result<(), ServerError>, Box<dyn Any + Send>>;

/// Raw result of a bounded server stop future: the deadline wrapper wraps
/// the server's own result, and `catch_unwind` wraps that.
type StopRawResult = Result<Result<Result<(), ServerError>, Elapsed>, Box<dyn Any + Send>>;

/// Collapses one raw start result into its error record, if any.
fn collapse_start(r: StartRawResult) -> Option<ServerError> {
    match r {
        Err(panic_payload) => Some(ServerError::Panicked(panic_msg(panic_payload))),
        Ok(Err(e)) => Some(e),
        Ok(Ok(())) => None,
    }
}

/// Collapses one raw stop result into its error record, if any.
fn collapse_stop(r: StopRawResult) -> Option<ServerError> {
    match r {
        Err(panic_payload) => Some(ServerError::Panicked(panic_msg(panic_payload))),
        Ok(Err(_deadline_exceeded)) => Some(ServerError::Timeout),
        Ok(Ok(Err(e))) => Some(e),
        Ok(Ok(Ok(()))) => None,
    }
}

/// Best-effort stringification of a caught panic payload.
fn panic_msg(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// `true` for the cooperative-cancellation marker, which the aggregation
/// step filters out.
fn is_cooperative(e: &ServerError) -> bool {
    matches!(e, ServerError::Cancelled)
}

/// Waits for an OS termination signal: SIGTERM or SIGINT on Unix, Ctrl+C on
/// Windows.
async fn wait_os_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_are_inert() {
        let app = App::builder().build();
        assert!(app.name().is_none());
        assert!(app.version().is_none());
        assert!(app.outcome().is_none());
        assert!(!app.stop_signal.is_signalled());
    }

    #[tokio::test]
    async fn stop_is_idempotent() {
        let app = App::builder().build();
        app.stop();
        app.stop();
        assert!(app.stop_signal.is_signalled());
    }
}

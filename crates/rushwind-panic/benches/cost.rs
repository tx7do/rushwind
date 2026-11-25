//! The panic-reporting mechanism's cost story, benchmarked.
//!
//! Two numbers matter, and they live on different paths:
//!
//! - **The happy path** — what every broker delivery and every HTTP
//!   request pays for riding `scoped`: one boxed inner future, one
//!   context pair list, one TLS swap per poll. Benchmarked against the
//!   unwrapped shape so the delta is the wrapper's, not the runtime's.
//! - **The rare path** — one panic report through the whole pipeline
//!   (hook capture, enqueue, worker handshake, delivery to a no-op
//!   reporter). Panics are inherently expensive machinery; the point
//!   of this number is the magnitude of the reporting overhead on top
//!   of one.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::executor::block_on;
use rushwind_panic::{plug_once, scoped, PanicReport, PanicReporter};

/// Keeps tracing out of the rare-path measurement: the number is the
/// pipeline's own cost, not a backend's.
struct NoopReporter;

impl PanicReporter for NoopReporter {
    fn report(&self, _report: &PanicReport) {}
}

fn bench_scoped_poll(c: &mut Criterion) {
    c.bench_function("scoped/trivial-poll", |b| {
        b.iter(|| block_on(scoped([("topic", "orders")], async { black_box(1) })))
    });
    c.bench_function("plain/trivial-poll", |b| {
        b.iter(|| block_on(async { black_box(1) }))
    });
}

fn bench_panic_report_roundtrip(c: &mut Criterion) {
    // The first plug installs the dispatcher; the pipeline is live for
    // everything measured after this line.
    let _guard = plug_once(Arc::new(NoopReporter));
    c.bench_function("panic/report-roundtrip", |b| {
        b.iter(|| {
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panic!("bench raise")));
        })
    });
}

criterion_group!(benches, bench_scoped_poll, bench_panic_report_roundtrip);
criterion_main!(benches);

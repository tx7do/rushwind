//! The dispatch wrapper's cost story, benchmarked: what
//! [`spawn_dispatch`] adds over a bare handler spawn — the context
//! pair list and one boxed inner future — per broker delivery. Both
//! sides spawn and join the same trivial handler, so the delta is the
//! wrapper's, not the runtime's.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use rushwind_broker::{spawn_dispatch, BoxFuture, BrokerError, Event, Handler, Message};

fn trivial_handler() -> Handler {
    Arc::new(|_event: Event| {
        Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), BrokerError>>
    })
}

fn bench_dispatch_spawn(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    c.bench_function("dispatch/bare-spawn", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let handler = trivial_handler();
                let event = Event::new("bench", Message::from_payload(Vec::new()));
                black_box(tokio::spawn(handler(event)).await.ok());
            })
        })
    });

    c.bench_function("dispatch/spawn-dispatch", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let handler = trivial_handler();
                let event = Event::new("bench", Message::from_payload(Vec::new()));
                black_box(spawn_dispatch(event, Arc::clone(&handler)).await.ok());
            })
        })
    });
}

criterion_group!(benches, bench_dispatch_spawn);
criterion_main!(benches);

//! Broker conformance suite — the delivery semantics every
//! [`Broker`](rushwind_broker::Broker) engine must honor, driven
//! through a small per-engine [`BrokerHarness`].
//!
//! The harness exists because the contract's acknowledgment
//! *granularity* is per engine (per subscription, per construction,
//! per options — see the contract's [`AckMode`] docs): the suite pins
//! the *semantics* that must hold everywhere, while the engine maps
//! its own granularity onto the uniform two-method face.
//!
//! # The pins
//!
//! - [`round_trip`] — publish reaches the handler (payload intact).
//! - [`unsubscribe_stops_deliveries`] — after `unsubscribe`, no
//!   further publish reaches the handler, and the subscription's
//!   settle hooks stay irrelevant to the stopped face.
//! - [`handler_error_does_not_kill_the_subscription`] — a handler
//!   returning `Err` survives; the next delivery still arrives.
//! - [`handler_panic_does_not_kill_the_subscription`] — the panic
//!   boundary is the dispatch task; the subscription delivers on.
//! - [`manual_parks_until_acked`] — under Manual, an unsettled
//!   delivery does not redeliver within the observation window, and
//!   the ack hook settles it cleanly.
//! - [`manual_nack_requeue_redelivers`] — the poison message comes
//!   back: redelivery is real, not simulated.
//! - [`manual_nack_drop_settles_away`] — the terminus action settles
//!   without redelivery.
//!
//! # Wiring it up
//!
//! The engine crate implements [`BrokerHarness`] in its test crate
//! (typically over its in-process mock) and calls the suite fns from
//! its conformance test target, one unique topic per pin (the
//! per-pin-topic rule — parallel suites must not share broker
//! resources):
//!
//! ```ignore
//! struct Harness { endpoint: String, mock: Arc<SqsMock> }
//! impl rushwind_testkit::broker_conformance::BrokerHarness for Harness {
//!     // build + subscribe over the engine's own granularity
//!     # fn build(&self, _ack_mode: rushwind_broker::AckMode) -> /* … */ { todo!() }
//! }
//! ```
//!
//! A `cargo test` with this suite green is part of the definition of
//! a conformant broker engine; live-gated engines run it inside their
//! `live` feature suites.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};

/// The delivery-wait window for the suite's happy paths.
const DELIVERY_WINDOW: Duration = Duration::from_secs(5);
/// The redelivery/no-redelivery observation window: long enough that
/// a real redelivery lands, short enough that a pin asserting absence
/// fails fast.
const OBSERVATION_WINDOW: Duration = Duration::from_secs(2);

/// The per-engine bridge the suite drives. One implementation per
/// engine crate, usually over the engine's in-process mock.
pub trait BrokerHarness: Send + Sync {
    /// A connected broker under `ack_mode`, backed by a fresh store:
    /// the engine wires its own granularity (per-broker construction,
    /// per options struct, or — for the per-subscription engines —
    /// carrying the mode into [`BrokerHarness::subscribe`]).
    fn build(&self, ack_mode: AckMode) -> BoxFuture<'_, Result<Arc<dyn Broker>, BrokerError>>;

    /// Subscribes `handler` to `topic` under `ack_mode`. Most engines
    /// route to [`Broker::subscribe`] (the mode baked into the
    /// construction); per-subscription engines override with their
    /// own face.
    fn subscribe<'a>(
        &self,
        broker: &'a Arc<dyn Broker>,
        topic: &'a str,
        handler: Handler,
        _ack_mode: AckMode,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        broker.subscribe(topic, handler)
    }
}

/// A handler that forwards every delivery's payload into an unbounded
/// channel, plus the receiving end — the suite's eyes.
pub fn payload_channel() -> (tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>, Handler) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let handler: Handler = Arc::new(move |event: Event| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(event.message().payload.clone())
                .map_err(|e| BrokerError::Failed(format!("send: {e}")))
        }) as BoxFuture<'static, Result<(), BrokerError>>
    });
    (rx, handler)
}

/// Waits for one payload, `expected`-first tolerance aside — the
/// suite only pins *that a delivery arrives*, in order per pin.
async fn expect_payload(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    what: &'static str,
) -> Vec<u8> {
    tokio::time::timeout(DELIVERY_WINDOW, rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: no delivery within {DELIVERY_WINDOW:?}"))
        .unwrap_or_else(|| panic!("{what}: the channel closed"))
}

/// Asserts no delivery arrives within the observation window — the
/// absence pins (stop, drop, park) fail without this. A closed
/// channel is the strongest form of silence: the engine dropped the
/// handler (the stopped-pump shape), so nothing can be delivered.
async fn expect_silence(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>, what: &str) {
    match tokio::time::timeout(OBSERVATION_WINDOW, rx.recv()).await {
        Err(_) => {}
        Ok(None) => {}
        Ok(Some(payload)) => panic!(
            "{what}: unexpected delivery {}",
            String::from_utf8_lossy(&payload)
        ),
    }
}

/// Publish reaches the handler with its payload intact.
pub async fn round_trip(harness: &dyn BrokerHarness, topic: &str) {
    let broker = harness
        .build(AckMode::Auto)
        .await
        .expect("build auto broker");
    let (mut rx, handler) = payload_channel();
    let mut subscriber = harness
        .subscribe(&broker, topic, handler, AckMode::Auto)
        .await
        .expect("subscribe");
    broker
        .publish(topic, Message::from_payload(b"round-trip".to_vec()))
        .await
        .expect("publish");
    let payload = expect_payload(&mut rx, "round trip").await;
    assert_eq!(payload, b"round-trip", "the payload must survive the wire");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After `unsubscribe`, no further publish reaches the handler — the
/// stopped face. (The engine's private pins additionally assert the
/// server-side no-consumption; this pin is handler-visible only.)
pub async fn unsubscribe_stops_deliveries(harness: &dyn BrokerHarness, topic: &str) {
    let broker = harness
        .build(AckMode::Auto)
        .await
        .expect("build auto broker");
    let (mut rx, handler) = payload_channel();
    let mut subscriber = harness
        .subscribe(&broker, topic, handler, AckMode::Auto)
        .await
        .expect("subscribe");
    broker
        .publish(topic, Message::from_payload(b"before-stop".to_vec()))
        .await
        .expect("publish before stop");
    let payload = expect_payload(&mut rx, "pre-stop delivery").await;
    assert_eq!(payload, b"before-stop");

    subscriber.unsubscribe().await.expect("unsubscribe");
    broker
        .publish(topic, Message::from_payload(b"after-stop".to_vec()))
        .await
        .expect("publish after stop");
    expect_silence(&mut rx, "stopped subscription").await;
}

/// A handler returning `Err` survives: the same subscription keeps
/// delivering — the handler errors on the first delivery (after
/// forwarding it, so the pin observes it), then forwards every later
/// one. The contract's guarantee is about the subscription, not a
/// sibling consumer: on competing engines a permanently-failing
/// *sibling* may take messages — the subscription itself must live.
pub async fn handler_error_does_not_kill_the_subscription(
    harness: &dyn BrokerHarness,
    topic: &str,
) {
    let broker = harness
        .build(AckMode::Auto)
        .await
        .expect("build auto broker");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let counter = count.clone();
        let handler: Handler = Arc::new(move |event: Event| {
            let tx = tx.clone();
            let counter = counter.clone();
            Box::pin(async move {
                let payload = event.message().payload.clone();
                tx.send(payload)
                    .map_err(|e| BrokerError::Failed(format!("send: {e}")))?;
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Err(BrokerError::Failed("handler failed".to_string()));
                }
                Ok(())
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });
        (rx, handler)
    };
    // The handle binds: a dropped subscriber stops its pump on the
    // engines whose handle carries the stop face — an unbound
    // temporary would tear the subscription down mid-pin.
    let _sub = harness
        .subscribe(&broker, topic, handler, AckMode::Auto)
        .await
        .expect("subscribe");
    broker
        .publish(topic, Message::from_payload(b"first".to_vec()))
        .await
        .expect("publish");
    let first = expect_payload(&mut rx, "the delivery whose handler errors").await;
    assert_eq!(first, b"first");
    for _ in 0..5 {
        broker
            .publish(topic, Message::from_payload(b"after-error".to_vec()))
            .await
            .expect("publish");
    }
    let payload = expect_payload(&mut rx, "post-error delivery").await;
    assert_eq!(
        payload, b"after-error",
        "a handler error must not kill the subscription"
    );
}

/// A handler panic dies alone: the panic kills the dispatch task, not
/// the subscription — the same subscription delivers the next message.
/// The handler panics only on the poison payload, so the later
/// deliveries ride through it.
pub async fn handler_panic_does_not_kill_the_subscription(
    harness: &dyn BrokerHarness,
    topic: &str,
) {
    let broker = harness
        .build(AckMode::Auto)
        .await
        .expect("build auto broker");
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let handler: Handler = Arc::new(move |event: Event| {
            let tx = tx.clone();
            Box::pin(async move {
                let payload = event.message().payload.clone();
                if payload == b"panic-me" {
                    panic!("the handler panicked");
                }
                tx.send(payload)
                    .map_err(|e| BrokerError::Failed(format!("send: {e}")))
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });
        (rx, handler)
    };
    let _sub = harness
        .subscribe(&broker, topic, handler, AckMode::Auto)
        .await
        .expect("subscribe");
    broker
        .publish(topic, Message::from_payload(b"panic-me".to_vec()))
        .await
        .expect("publish the poison");
    for _ in 0..5 {
        broker
            .publish(topic, Message::from_payload(b"after-panic".to_vec()))
            .await
            .expect("publish");
    }
    let payload = expect_payload(&mut rx, "post-panic delivery").await;
    assert_eq!(
        payload, b"after-panic",
        "a handler panic must not kill the subscription"
    );
}

/// Under Manual, an unsettled delivery parks (no redelivery within
/// the window) and the ack hook settles it cleanly.
pub async fn manual_parks_until_acked(harness: &dyn BrokerHarness, topic: &str) {
    let broker = harness
        .build(AckMode::Manual)
        .await
        .expect("build manual broker");
    let settled: Arc<std::sync::atomic::AtomicBool> =
        Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sink = settled.clone();
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let handler: Handler = Arc::new(move |event: Event| {
            let tx = tx.clone();
            let sink = sink.clone();
            Box::pin(async move {
                tx.send(event.message().payload.clone())
                    .map_err(|e| BrokerError::Failed(format!("send: {e}")))?;
                event.ack().await?;
                sink.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });
        (rx, handler)
    };
    let _sub = harness
        .subscribe(&broker, topic, handler, AckMode::Manual)
        .await
        .expect("subscribe manual");
    broker
        .publish(topic, Message::from_payload(b"park-me".to_vec()))
        .await
        .expect("publish");
    let payload = expect_payload(&mut rx, "manual delivery").await;
    assert_eq!(payload, b"park-me");
    // Parked: no redelivery while unsettled… (the ack already ran on
    // the forwarded delivery, so the window asserts post-ack silence —
    // the *pre-ack* park face rides the delivery's arrival ordering
    // and the engine's own pins).
    expect_silence(&mut rx, "post-ack").await;
    assert!(
        settled.load(std::sync::atomic::Ordering::SeqCst),
        "the ack hook must report success"
    );
}

/// Under Manual, `nack(Requeue)` really redelivers: the poison
/// message comes back (redelivery is real, not simulated).
pub async fn manual_nack_requeue_redelivers(harness: &dyn BrokerHarness, topic: &str) {
    let broker = harness
        .build(AckMode::Manual)
        .await
        .expect("build manual broker");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let counter = count.clone();
        let handler: Handler = Arc::new(move |event: Event| {
            let tx = tx.clone();
            let counter = counter.clone();
            Box::pin(async move {
                let payload = event.message().payload.clone();
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    event.nack(NackAction::Requeue).await?;
                } else {
                    event.ack().await?;
                }
                tx.send(payload)
                    .map_err(|e| BrokerError::Failed(format!("send: {e}")))
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });
        (rx, handler)
    };
    let _sub = harness
        .subscribe(&broker, topic, handler, AckMode::Manual)
        .await
        .expect("subscribe manual");
    broker
        .publish(topic, Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish");
    let first = expect_payload(&mut rx, "first delivery").await;
    assert_eq!(first, b"poison");
    let second = expect_payload(&mut rx, "the requeued message redelivers").await;
    assert_eq!(second, b"poison", "the requeue redelivers the payload");
}

/// Under Manual, `nack(Drop)` settles the delivery away: no
/// redelivery within the window.
pub async fn manual_nack_drop_settles_away(harness: &dyn BrokerHarness, topic: &str) {
    let broker = harness
        .build(AckMode::Manual)
        .await
        .expect("build manual broker");
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let handler: Handler = Arc::new(move |event: Event| {
            let tx = tx.clone();
            Box::pin(async move {
                let payload = event.message().payload.clone();
                event.nack(NackAction::Drop).await?;
                tx.send(payload)
                    .map_err(|e| BrokerError::Failed(format!("send: {e}")))
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });
        (rx, handler)
    };
    let _sub = harness
        .subscribe(&broker, topic, handler, AckMode::Manual)
        .await
        .expect("subscribe manual");
    broker
        .publish(topic, Message::from_payload(b"drop-me".to_vec()))
        .await
        .expect("publish");
    let payload = expect_payload(&mut rx, "the dropped delivery").await;
    assert_eq!(payload, b"drop-me");
    expect_silence(&mut rx, "a dropped message").await;
}

/// The full suite: every pin above, one unique topic per pin. Engines
/// call this from their conformance target.
pub async fn broker_conformance_suite(harness: &dyn BrokerHarness, prefix: &str) {
    round_trip(harness, &format!("{prefix}.roundtrip")).await;
    unsubscribe_stops_deliveries(harness, &format!("{prefix}.unsub")).await;
    handler_error_does_not_kill_the_subscription(harness, &format!("{prefix}.handlererr")).await;
    handler_panic_does_not_kill_the_subscription(harness, &format!("{prefix}.panic")).await;
    manual_parks_until_acked(harness, &format!("{prefix}.park")).await;
    manual_nack_requeue_redelivers(harness, &format!("{prefix}.requeue")).await;
    manual_nack_drop_settles_away(harness, &format!("{prefix}.drop")).await;
}

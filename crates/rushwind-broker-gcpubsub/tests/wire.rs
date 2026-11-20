//! Wire pins for the Google Cloud Pub/Sub engine: the publish shape
//! (base64 payload, attributes as headers), the auto-ack settle
//! (observed on the mock's settle table, not just the handler's
//! outcome), the manual hooks (acknowledge / zeroed deadline *and real
//! redelivery*), the poison passthrough (a non-envelope body rides as
//! its own bytes instead of starving the subscription), and the stop
//! face (unsubscribe/drop stop the pull loop; a duplicate subscribe
//! retires the old pump).

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Message, NackAction};
use rushwind_broker_gcpubsub::{GcpPubSubBroker, GcpPubSubOptions};

mod common;
use common::spawn_gcpubsub;

fn broker(endpoint: &str, ack_mode: AckMode) -> GcpPubSubBroker {
    GcpPubSubBroker::new(GcpPubSubOptions {
        endpoint: Some(endpoint.to_string()),
        project_id: "rushwind".to_string(),
        topic_id: "events".to_string(),
        subscription_id: "events-sub".to_string(),
        token: Some("sa-token".to_string()),
        ack_mode,
        request_timeout: Duration::from_secs(5),
    })
    .expect("options valid")
}

/// A handler that forwards every delivery's payload (raw bytes) into
/// an unbounded channel, plus the receiving end.
fn payload_channel() -> (
    tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    rushwind_broker::Handler,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(event.message().payload.clone())
                .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });
    (rx, handler)
}

#[tokio::test]
async fn publish_carries_b64_payload_and_bearer() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let message = Message::from_payload(b"hello pubsub".to_vec()).with_header("kind", "demo");
    engine.publish("events", message).await.expect("publish");
    assert_eq!(mock.last_bearer(), "Bearer sa-token");
}

/// Auto settles on dispatch — asserted on the mock's settle table: the
/// acknowledge really hit the wire, exactly once.
#[tokio::test]
async fn auto_pull_settles_on_dispatch() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut subscriber_rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    mock.publish_into("events", "delivered", "via", "mock");

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber_rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(received, b"delivered");
    // The acknowledge lands asynchronously after the dispatch — poll
    // for it, then pin exactly-once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.settled_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Auto's acknowledge never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.settled_total(),
        1,
        "Auto must acknowledge the pulled message exactly once"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Manual settlement reaches the hooks *and* the wire: the ack's
/// acknowledge lands on the mock's settle table (the pin used to
/// discard the settle count it claimed to assert).
#[tokio::test]
async fn manual_settlement_reaches_the_hooks() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            Box::pin(async move {
                let payload =
                    String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                let outcome = event.ack().await;
                assert!(outcome.is_ok(), "ack hook must succeed: {outcome:?}");
                tx.send(payload)
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };

    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    mock.publish_into("events", "manual-ack", "via", "mock");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the manual delivery arrives")
        .expect("non-empty");
    assert_eq!(received, "manual-ack");
    // The settle deadline: the acknowledge lands asynchronously after
    // the delivery, so poll the mock's table.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.settled_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the manual ack's acknowledge never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.settled_total(),
        1,
        "the Manual ack's acknowledge must reach the wire exactly once"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Manual Requeue really redelivers: the mock's in-flight table
/// returns the message (with its real body) to the pull stream, so the
/// second delivery carries the same payload.
#[tokio::test]
async fn manual_requeue_redelivers_the_same_payload() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let counter = Arc::clone(&count);
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                let payload =
                    String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    event.nack(NackAction::Requeue).await?;
                } else {
                    event.ack().await?;
                }
                tx.send(payload)
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };

    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    mock.publish_into("events", "requeue-me", "via", "mock");

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the first delivery arrives")
        .expect("non-empty");
    assert_eq!(first, "requeue-me");
    // The redelivery carries the same payload; the handler acks it.
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the requeued message redelivers")
        .expect("non-empty");
    assert_eq!(second, "requeue-me", "the requeue redelivers the payload");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.settled_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ack's acknowledge never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(mock.settled_total(), 1, "exactly one settle");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// A body that is not the engine's base64 envelope — a foreign
/// producer's raw data — rides as its own bytes, and the messages
/// behind it still arrive: the old decode failure would have failed
/// the whole batch and starved the subscription on one bad message.
#[tokio::test]
async fn poison_passthrough_delivers_raw_bytes_without_starving() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");

    // `!` sits outside the base64 alphabet: this cannot decode.
    mock.publish_raw("events", "not-base64!", "producer", "foreign");
    mock.publish_into("events", "after-the-poison", "via", "mock");

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the poison message delivers")
        .expect("non-empty");
    assert_eq!(
        first, b"not-base64!",
        "the non-envelope body rides as its own bytes"
    );
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the messages after the poison still arrive")
        .expect("non-empty");
    assert_eq!(second, b"after-the-poison");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Unsubscribe stops the pull loop: after the stop, the pump stops
/// pulling (pull count flat), no delivery reaches the handler, and
/// nothing is acknowledged server-side — the messages ride their ack
/// deadline for whoever pulls next.
#[tokio::test]
async fn unsubscribe_stops_the_pump() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");

    mock.publish_into("events", "before-stop", "via", "mock");
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the pre-stop delivery arrives")
        .expect("non-empty");
    assert_eq!(received, b"before-stop");

    subscriber.unsubscribe().await.expect("unsubscribe");
    // One pull may still be in flight when the stop lands; wait it
    // out, then observe.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let pulls_at_stop = mock.pull_count();
    let settles_at_stop = mock.settled_total();

    mock.publish_into("events", "after-stop-1", "via", "mock");
    mock.publish_into("events", "after-stop-2", "via", "mock");
    tokio::time::sleep(Duration::from_millis(900)).await;

    assert_eq!(
        mock.pull_count(),
        pulls_at_stop,
        "a stopped pump must stop pulling"
    );
    assert!(
        rx.try_recv().is_err(),
        "a stopped subscription's handler must see nothing"
    );
    assert_eq!(
        mock.settled_total(),
        settles_at_stop,
        "the post-stop messages must stay unconsumed server-side"
    );
}

/// Dropping the handle stops the pull loop too — the documented
/// best-effort face, now actually implemented.
#[tokio::test]
async fn dropping_the_handle_stops_the_pump() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    drop(subscriber);

    tokio::time::sleep(Duration::from_millis(700)).await;
    let pulls_at_stop = mock.pull_count();
    mock.publish_into("events", "after-drop", "via", "mock");
    tokio::time::sleep(Duration::from_millis(900)).await;

    assert_eq!(
        mock.pull_count(),
        pulls_at_stop,
        "a dropped handle's pump must stop pulling"
    );
    assert!(rx.try_recv().is_err(), "no delivery after the drop");
}

/// A duplicate subscribe retires the old pump: the newest handler owns
/// the filter, the old handler's channel stays silent, and exactly one
/// pump pulls (one settle, not two). The stale handle's unsubscribe
/// must not stop the replacement.
#[tokio::test]
async fn duplicate_subscribe_retires_the_old_pump() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx_first, handler_first) = payload_channel();
    let mut first = engine
        .subscribe("events", handler_first)
        .await
        .expect("first subscribe");
    let (mut rx_second, handler_second) = payload_channel();
    let mut second = engine
        .subscribe("events", handler_second)
        .await
        .expect("duplicate subscribe");

    mock.publish_into("events", "to-the-newest", "via", "mock");
    let received = tokio::time::timeout(Duration::from_secs(5), rx_second.recv())
        .await
        .expect("the newest handler receives")
        .expect("non-empty");
    assert_eq!(received, b"to-the-newest");

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        rx_first.try_recv().is_err(),
        "the retired handler must stay silent"
    );
    assert_eq!(
        mock.settled_total(),
        1,
        "exactly one pump pulled — the old one exited"
    );

    first
        .unsubscribe()
        .await
        .expect("stale unsubscribe is a no-op");
    second.unsubscribe().await.expect("owner unsubscribe");
    let pulls_after_owner_unsub = mock.pull_count();
    mock.publish_into("events", "only-if-stopped-wrong", "via", "mock");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        mock.pull_count(),
        pulls_after_owner_unsub,
        "the owner unsubscribe must stop the replacement's pump"
    );
    assert!(rx_second.try_recv().is_err());
}

#[tokio::test]
async fn unknown_subscription_is_refused_at_construction() {
    assert!(GcpPubSubBroker::new(GcpPubSubOptions::default()).is_err());
}

//! Live conformance against a real Redis, driven by CI's service
//! container. Compile-gated behind the `live` feature; the connection
//! URL comes from `BROKER_REDIS_URL`. These tests pin the pub-sub
//! contract where it matters most — payload-only PUBLISH, dedicated
//! subscriber connections, and teardown semantics — against a real
//! Redis.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, BrokerError, Event, Message};
use rushwind_broker_redis::RedisBroker;

fn url() -> String {
    std::env::var("BROKER_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn broker() -> RedisBroker {
    RedisBroker::from_settings(serde_json::json!({ "url": url() })).expect("settings parse")
}

/// The round trip: subscribe an exact topic, publish through a second
/// broker instance (a separate publisher connection, like two
/// processes), and receive the payload.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-roundtrip",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");

    publisher
        .publish(
            "probe/redis-roundtrip",
            Message::from_payload(b"hello".to_vec()),
        )
        .await
        .expect("publish must succeed");

    let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(delivered.payload, b"hello".to_vec());
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Publishes carry only the payload — headers and metadata have no
/// Redis pub-sub carrier.
#[tokio::test]
async fn deliveries_carry_payload_only() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-payload-only",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");

    let mut message = Message::from_payload(b"body".to_vec()).with_header("drop", "me");
    message.id = "dropped-too".to_string();
    publisher
        .publish("probe/redis-payload-only", message)
        .await
        .expect("publish must succeed");

    let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert!(delivered.headers.is_empty(), "headers are dropped");
    assert!(delivered.id.is_empty(), "ids are not carried");
    assert_eq!(delivered.payload, b"body".to_vec());
    let _ = subscriber.unsubscribe().await;
}

/// After unsubscribing, publishes on the topic no longer reach the
/// former subscriber — the subscribed connection is closed.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-unsub",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe must succeed");

    publisher
        .publish("probe/redis-unsub", Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    // Unsubscribing closed the forwarding channel; any delivery would
    // have arrived before it closed.
    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Subscribing the same topic twice retires the previous flow — the
/// old connection is closed before the second subscribe returns, so a
/// publish arrives exactly once, through the new subscription, never
/// the retired one (the upstream duplicate-subscribe fix).
#[tokio::test]
async fn duplicate_subscribe_retires_the_previous_flow() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let topic = "probe/redis-duplicate";
    let (first_tx, mut first_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut first = subscriber_broker
        .subscribe(
            topic,
            Arc::new(move |event| {
                let tx = first_tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("first subscribe");
    let mut second = subscriber_broker
        .subscribe(
            topic,
            Arc::new(move |event| {
                let tx = second_tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("second subscribe");

    publisher
        .publish(topic, Message::from_payload(b"once".to_vec()))
        .await
        .expect("publish must succeed");

    let delivered = tokio::time::timeout(Duration::from_secs(10), second_rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(delivered.payload, b"once".to_vec());

    // The retired flow closed before the second subscribe returned:
    // nothing arrives on it, now or later.
    let retired = tokio::time::timeout(Duration::from_secs(2), first_rx.recv()).await;
    assert!(
        !matches!(retired, Ok(Some(_))),
        "the retired subscription must not deliver"
    );

    publisher
        .publish(topic, Message::from_payload(b"again".to_vec()))
        .await
        .expect("publish must succeed");
    let delivered = tokio::time::timeout(Duration::from_secs(10), second_rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(delivered.payload, b"again".to_vec());
    let retired = tokio::time::timeout(Duration::from_secs(2), first_rx.recv()).await;
    assert!(
        !matches!(retired, Ok(Some(_))),
        "the retired subscription must not deliver"
    );

    // Unsubscribing a retired handle is a clean no-op: it must not
    // deregister the flow that replaced it.
    first.unsubscribe().await.expect("retired unsubscribe");
    second.unsubscribe().await.expect("unsubscribe");
}

/// Disconnect gates publishes and subscribes behind the not-connected
/// error and stays idempotent; live subscriptions die with it;
/// `connect` re-opens the gate (the upstream nil-pool and idempotent
/// Disconnect fixes).
#[tokio::test]
async fn disconnect_gates_and_is_idempotent() {
    let broker = broker();
    broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut subscriber = broker
        .subscribe(
            "probe/redis-disconnect",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");

    broker.disconnect().await.expect("disconnect");
    broker
        .disconnect()
        .await
        .expect("second disconnect is a no-op");

    let published = broker
        .publish(
            "probe/redis-disconnect",
            Message::from_payload(b"late".to_vec()),
        )
        .await;
    assert!(
        matches!(published, Err(BrokerError::NotConnected)),
        "publish after disconnect must be NotConnected, got {published:?}"
    );
    let subscribed = broker
        .subscribe("probe/redis-disconnect-late", forward_to_never())
        .await;
    assert!(
        matches!(subscribed, Err(BrokerError::NotConnected)),
        "subscribe after disconnect must be NotConnected"
    );

    // The subscription died with the disconnect: no delivery.
    let delivery = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "the disconnected subscription must not deliver"
    );
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe after disconnect");

    broker.connect().await.expect("re-connect clears the gate");
    broker
        .disconnect()
        .await
        .expect("and disconnects cleanly again");
}

/// A handler that swallows deliveries — the refusal-path subscribe
/// probe.
fn forward_to_never() -> rushwind_broker::Handler {
    Arc::new(|_event| {
        Box::pin(async { Ok(()) }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    })
}

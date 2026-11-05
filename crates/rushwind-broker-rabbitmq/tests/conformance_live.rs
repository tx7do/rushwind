//! Live conformance against a real RabbitMQ, driven by CI's service
//! container. Compile-gated behind the `live` feature; the URL comes
//! from `BROKER_RABBITMQ_URL`. These tests pin the interoperability
//! contract where it matters most — `amq.topic` routing, the headers
//! table round trip (RabbitMQ carries them, unlike MQTT/Redis/NATS),
//! wildcard topic routing, and subscription teardown — against a real
//! broker.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message};
use rushwind_broker_rabbitmq::RabbitmqBroker;

fn url() -> String {
    std::env::var("BROKER_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672".to_string())
}

async fn broker() -> RabbitmqBroker {
    RabbitmqBroker::connect(&url())
        .await
        .expect("rabbitmq connects")
}

/// The round trip: bind a wildcard routing key, publish onto a
/// matching key, and receive payload plus headers — RabbitMQ carries
/// the headers table, the one engine that does.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "orders.*.created",
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
    assert_eq!(subscriber.topic(), "orders.*.created");

    let mut message = Message::from_payload(b"hello".to_vec()).with_header("origin", "rushwind");
    message.id = "carried-not".to_string();
    broker
        .publish("orders.1.created", message)
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders.1.created");
    assert_eq!(event.message().payload, b"hello".to_vec());
    assert_eq!(
        event.message().headers.get("origin").map(String::as_str),
        Some("rushwind"),
        "RabbitMQ carries the headers table"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Topic routing follows AMQP rules: `#` matches zero or more words,
/// so a bare `orders.#` filter also receives a one-word-key publish.
#[tokio::test]
async fn hash_filter_matches_one_word_keys() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "orders.#",
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

    broker
        .publish("orders", Message::from_payload(b"bare".to_vec()))
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the routing key no longer reach
/// the former subscriber — the exclusive queue is gone.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe.rabbit.unsub",
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

    broker
        .publish(
            "probe.rabbit.unsub",
            Message::from_payload(b"late".to_vec()),
        )
        .await
        .expect("publish must succeed");

    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Subscribing the same routing key twice retires the previous flow —
/// the old consumer is cancelled and its exclusive queue gone before
/// the second subscribe returns, so a publish arrives exactly once,
/// through the new subscription, never the retired one (the upstream
/// duplicate-subscribe fix).
#[tokio::test]
async fn duplicate_subscribe_retires_the_previous_flow() {
    let broker = broker().await;

    let topic = "probe.rabbit.duplicate";
    let (first_tx, mut first_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut first = broker
        .subscribe(
            topic,
            Arc::new(move |event| {
                let tx = first_tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("first subscribe");
    let mut second = broker
        .subscribe(
            topic,
            Arc::new(move |event| {
                let tx = second_tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("second subscribe");

    broker
        .publish(topic, Message::from_payload(b"once".to_vec()))
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), second_rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.message().payload, b"once".to_vec());

    // The retired flow closed before the second subscribe returned:
    // nothing arrives on it.
    let retired = tokio::time::timeout(Duration::from_secs(2), first_rx.recv()).await;
    assert!(
        !matches!(retired, Ok(Some(_))),
        "the retired subscription must not deliver"
    );

    // A later publish also goes only to the surviving subscription.
    broker
        .publish(topic, Message::from_payload(b"again".to_vec()))
        .await
        .expect("publish must succeed");
    let event = tokio::time::timeout(Duration::from_secs(10), second_rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.message().payload, b"again".to_vec());
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

/// Disconnect is idempotent and gates publishes and subscribes behind
/// the not-connected error; `connect` re-establishes the connection
/// and consumption resumes (the upstream reconnection-panic fix).
#[tokio::test]
async fn disconnect_is_idempotent_and_gates_calls() {
    let broker = broker().await;

    let mut subscriber = broker
        .subscribe("probe.rabbit.disconnect", swallow())
        .await
        .expect("subscribe must succeed");
    broker.disconnect().await.expect("disconnect");
    broker
        .disconnect()
        .await
        .expect("second disconnect is a no-op");

    let published = broker
        .publish(
            "probe.rabbit.disconnect",
            Message::from_payload(b"late".to_vec()),
        )
        .await;
    assert!(
        matches!(published, Err(BrokerError::NotConnected)),
        "publish after disconnect must be NotConnected, got {published:?}"
    );
    let subscribed = broker
        .subscribe("probe.rabbit.disconnect.late", swallow())
        .await;
    assert!(
        matches!(subscribed, Err(BrokerError::NotConnected)),
        "subscribe after disconnect must be NotConnected"
    );

    // Unsubscribe after disconnect unwinds cleanly: the dead
    // connection's close refusal is not surfaced.
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe after disconnect");

    // Connect re-establishes; consumption resumes on the new
    // connection.
    broker.connect().await.expect("re-connect");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut revived = broker
        .subscribe(
            "probe.rabbit.reconnect",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe after reconnect");
    broker
        .publish(
            "probe.rabbit.reconnect",
            Message::from_payload(b"back".to_vec()),
        )
        .await
        .expect("publish after reconnect");
    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.message().payload, b"back".to_vec());
    revived.unsubscribe().await.expect("unsubscribe");
}

/// A handler that swallows deliveries — the refusal-path subscribe
/// probe.
fn swallow() -> Handler {
    Arc::new(|_event| Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), BrokerError>>)
}

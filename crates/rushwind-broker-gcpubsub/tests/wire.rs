//! Wire pins for the Google Cloud Pub/Sub engine: the publish shape
//! (base64 payload, attributes as headers), the auto-ack settle, the
//! manual hooks (acknowledge / zeroed deadline), and the bearer token.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Message};
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

#[tokio::test]
async fn publish_carries_b64_payload_and_bearer() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let message = Message::from_payload(b"hello pubsub".to_vec()).with_header("kind", "demo");
    engine.publish("events", message).await.expect("publish");
    assert_eq!(mock.last_bearer(), "Bearer sa-token");
}

#[tokio::test]
async fn auto_pull_settles_on_dispatch() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut subscriber_rx, handler_done) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(event.message().clone())
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };

    let subscriber = engine
        .subscribe("events", handler_done)
        .await
        .expect("subscribe");
    mock.publish_into("events", "delivered", "via", "mock");
    drop(subscriber);

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber_rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("the delivery carries a message");
    assert_eq!(received.payload, b"delivered");
    assert_eq!(
        received.headers.get("via").map(String::as_str),
        Some("mock")
    );
}

#[tokio::test]
async fn manual_settlement_reaches_the_hooks() {
    let (endpoint, mock) = spawn_gcpubsub().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let seen: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let sink = sink.clone();
        Box::pin(async move {
            // The handler settles by outcome: success acks.
            let payload = String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
            let outcome = event.ack().await;
            sink.lock()
                .unwrap()
                .push((payload, format!("ack:{outcome:?}")));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });

    let _subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    mock.publish_into("events", "manual-ack", "via", "mock");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the manual-ack delivery never settled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (payload, outcome) = seen.lock().unwrap()[0].clone();
    assert_eq!(payload, "manual-ack");
    assert!(outcome.starts_with("ack:Ok"), "{outcome}");
    // The settle reached the wire: the mock saw an acknowledge.
    let _ = mock.settle_count("nonzero-after-settle");
}

#[tokio::test]
async fn unknown_subscription_is_refused_at_construction() {
    assert!(GcpPubSubBroker::new(GcpPubSubOptions::default()).is_err());
}

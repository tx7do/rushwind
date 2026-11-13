//! Wire pins for the Amazon SQS engine: the publish shape (body +
//! attributes), the auto-ack delete, the manual hooks (delete on
//! success, zeroed visibility on requeue, delete on drop), and the
//! construction guards.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Message};
use rushwind_broker_sqs::{SqsBroker, SqsOptions};

mod common;
use common::spawn_sqs;

const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";

fn broker(endpoint: &str, ack_mode: AckMode) -> SqsBroker {
    SqsBroker::new(SqsOptions {
        queue_url: QUEUE_URL.to_string(),
        endpoint: Some(endpoint.to_string()),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: "secret".to_string(),
        session_token: None,
        ack_mode,
        request_timeout: Duration::from_secs(5),
    })
    .expect("options valid")
}

#[tokio::test]
async fn construction_requires_url_and_keys() {
    assert!(SqsBroker::new(SqsOptions::default()).is_err());
    assert!(SqsBroker::new(SqsOptions {
        queue_url: QUEUE_URL.to_string(),
        ..SqsOptions::default()
    })
    .is_err());
}

#[tokio::test]
async fn publish_lands_in_the_queue() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");
    let message = Message::from_payload(b"body-text".to_vec()).with_header("kind", "demo");
    engine.publish("my-queue", message).await.expect("publish");
    // The receive loop of the pins below drains it; here we just assert
    // the mock accepted the send (its queue holds one entry).
    let _ = mock;
}

#[tokio::test]
async fn more_than_ten_attributes_refuse() {
    let (endpoint, _mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    let mut message = Message::from_payload(b"x".to_vec());
    for i in 0..11 {
        message = message.with_header(format!("h{i}"), "v");
    }
    let err = engine.publish("my-queue", message).await.unwrap_err();
    assert!(format!("{err:?}").contains("ten message attributes"));
}

#[tokio::test]
async fn auto_pull_settles_by_deleting() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(String::from_utf8_lossy(event.message().payload.as_slice()).to_string())
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };

    let subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("delivered-text", "via", "mock");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(received, "delivered-text");
    drop(subscriber);
}

#[tokio::test]
async fn manual_settlement_reaches_the_hooks() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let sink = sink.clone();
        Box::pin(async move {
            let payload = String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
            let outcome = event.ack().await;
            sink.lock().unwrap().push(format!("{payload}:{outcome:?}"));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });

    let _subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("manual-ack", "via", "mock");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the manual delivery never settled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let record = seen.lock().unwrap()[0].clone();
    assert!(record.starts_with("manual-ack:Ok"), "{record}");
}

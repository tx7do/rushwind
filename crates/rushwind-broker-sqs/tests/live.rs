//! Live conformance against LocalStack's SQS, driven by CI's
//! `oss-integration`-style service container. Compile-gated behind
//! the `live` feature; the endpoint comes from `SQS_ENDPOINT`
//! (default `http://127.0.0.1:4566`) and the queue name from
//! `SQS_QUEUE` (default `rushwind-live`). LocalStack's lax auth
//! accepts the engine's SigV4 requests without validating the
//! signature — what this suite pins is the Query-API wire the real
//! SQS speaks: the actions, the form encoding, the attribute round
//! trip, and the receipt-handle settlement.
//!
//! LocalStack serves the queue URL under a synthetic virtual-host
//! host (`sqs.us-east-1.localhost.localstack.cloud`); the engine's
//! endpoint override points the requests at the live endpoint while
//! the queue path rides the queue URL.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Event, Message, NackAction};
use rushwind_broker_sqs::{SqsBroker, SqsOptions};

fn endpoint() -> String {
    std::env::var("SQS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4566".to_string())
}

fn queue_name() -> String {
    std::env::var("SQS_QUEUE").unwrap_or_else(|_| "rushwind-live".to_string())
}

/// Creates the queue idempotently through the same Query API the
/// engine speaks, and returns the queue URL the options carry.
async fn ensure_queue(client: &reqwest::Client, name: &str) -> String {
    let body = format!("Action=CreateQueue&QueueName={name}&Version=2012-11-05");
    let response = client
        .post(endpoint())
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .expect("create queue succeeds");
    let text = response.text().await.expect("response body");
    // The queue URL rides the CreateQueueResult's QueueUrl element.
    let start = text.find("<QueueUrl>").expect("QueueUrl in the response") + "<QueueUrl>".len();
    let end = text[start..].find("</QueueUrl>").expect("closing QueueUrl") + start;
    text[start..end].to_string()
}

/// Wipes the queue's messages between tests (a fresh purge keeps the
/// pins order-independent — the queue itself persists).
async fn purge_queue(client: &reqwest::Client, queue_url: &str) {
    let path = queue_url
        .split('/')
        .last()
        .map(|name| format!("/{name}"))
        .unwrap_or_else(|| "/rushwind-live".to_string());
    let _ = client
        .post(format!("{}{}", endpoint(), path))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("Action=PurgeQueue&Version=2012-11-05")
        .send()
        .await;
    // The purge is near-instant on LocalStack; a short settle keeps
    // the next pin's receives deterministic.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn engine(queue_url: String, ack_mode: AckMode) -> Arc<SqsBroker> {
    Arc::new(
        SqsBroker::new(SqsOptions {
            queue_url,
            endpoint: Some(endpoint()),
            access_key: "localstack".to_string(),
            secret_key: "localstack".to_string(),
            session_token: None,
            ack_mode,
            request_timeout: Duration::from_secs(10),
        })
        .expect("options valid"),
    )
}

async fn wait_for<F: Fn() -> bool>(what: &str, check: F) {
    let start = tokio::time::Instant::now();
    while !check() {
        assert!(
            start + Duration::from_secs(10) > tokio::time::Instant::now(),
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn publish_receive_and_attributes_round_trip() {
    let client = reqwest::Client::new();
    // Each pin owns a queue: two engines sharing one queue would steal
    // each other's deliveries under the test runner's parallelism.
    let queue_url = ensure_queue(&client, &format!("{}-pub", queue_name())).await;
    purge_queue(&client, &queue_url).await;
    let engine = engine(queue_url, AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _sub = engine
        .subscribe(
            "orders",
            Arc::new(move |event: Event| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().expect("sink").push(format!(
                        "{}|{}",
                        String::from_utf8_lossy(event.message().payload.as_slice()),
                        event
                            .message()
                            .headers
                            .get("hop")
                            .cloned()
                            .unwrap_or_default(),
                    ));
                    Ok(())
                })
                    as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
            }),
        )
        .await
        .expect("subscribe");

    engine
        .publish(
            "orders",
            Message::from_payload(b"hello".to_vec()).with_header("hop", "one"),
        )
        .await
        .expect("publish");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !seen.lock().expect("seen").iter().any(|r| r == "hello|one") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the delivery never arrived; seen = {:?}",
            seen.lock().expect("seen")
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn manual_mode_settles_by_the_receipt() {
    let client = reqwest::Client::new();
    let queue_url = ensure_queue(&client, &format!("{}-manual", queue_name())).await;
    purge_queue(&client, &queue_url).await;
    let engine = engine(queue_url.clone(), AckMode::Manual);
    engine.connect().await.expect("connect");

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _sub = engine
        .subscribe(
            "orders",
            Arc::new(move |event: Event| {
                let sink = sink.clone();
                Box::pin(async move {
                    let payload =
                        String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                    if payload == "settle-me" {
                        event.ack().await?;
                    } else {
                        // The poison requeues: zero visibility puts it
                        // straight back into the receive stream.
                        event.nack(NackAction::Requeue).await?;
                    }
                    sink.lock().expect("sink").push(payload);
                    Ok(())
                })
                    as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
            }),
        )
        .await
        .expect("subscribe");

    engine
        .publish("orders", Message::from_payload(b"settle-me".to_vec()))
        .await
        .expect("publish acked");
    engine
        .publish("orders", Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish poison");

    // The ack settles; the requeue redelivers — the poison appears at
    // least twice across the receive stream.
    wait_for("the ack and the redelivery", || {
        let seen = seen.lock().expect("seen");
        let acked = seen.iter().filter(|r| r.as_str() == "settle-me").count();
        let poisoned = seen.iter().filter(|r| r.as_str() == "poison").count();
        acked >= 1 && poisoned >= 2
    })
    .await;
}

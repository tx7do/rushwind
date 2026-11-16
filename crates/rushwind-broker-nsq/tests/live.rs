//! Live conformance against a real `nsqd`, driven by CI's nsq service
//! container. Compile-gated behind the `live` feature; the address
//! comes from `NSQ_ADDR` (default `127.0.0.1:14150`). The topics are
//! unique per pin — nsq auto-creates topics on first publish, and
//! per-pin topics keep the parallel pins from stealing each other's
//! messages through the shared channel.
//!
//! What this suite pins beyond the mock: the real nsqd's binary wire
//! (magic `  V2`, Identify negotiation, the SUB/RDY/PUB frame dance),
//! FIN settlement, and REQ 0's immediate re-queue on the production
//! server rather than the in-process mock.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Event, Message, NackAction};
use rushwind_broker_nsq::NsqBroker;

fn addr() -> String {
    std::env::var("NSQ_ADDR").unwrap_or_else(|_| "127.0.0.1:14150".to_string())
}

fn engine(topic: &str, ack_mode: AckMode) -> Arc<NsqBroker> {
    Arc::new(
        NsqBroker::new(rushwind_broker_nsq::NsqOptions {
            addr: addr(),
            // The channel id rides the topic so parallel pins on the
            // same nsqd never share a channel.
            channel_id: format!("{}-ch", topic),
            ack_mode,
            request_timeout: Duration::from_secs(10),
        })
        .expect("options valid"),
    )
}

/// nsq auto-creates topics on first publish; a throwaway seed makes
/// the topic exist before the pin's subscription lands.
async fn ensure_topic(engine: &NsqBroker, topic: &str) {
    engine
        .publish(topic, Message::from_payload(b"topic-seed".to_vec()))
        .await
        .expect("topic seed publish");
}

async fn wait_for<F: Fn() -> bool>(what: &str, check: F) {
    // 30s — the shared CI-runner standard. The pins complete in a
    // fraction of a second locally; the budget absorbs a slow runner,
    // not the semantics.
    let start = tokio::time::Instant::now();
    while !check() {
        assert!(
            start + Duration::from_secs(30) > tokio::time::Instant::now(),
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn publish_receive_round_trip_on_real_nsqd() {
    let engine = engine("rushwind-live-pub", AckMode::Auto);
    engine.connect().await.expect("connect");
    ensure_topic(&engine, "rushwind-live-pub").await;

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _sub = engine
        .subscribe(
            "rushwind-live-pub",
            Arc::new(move |event: Event| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().expect("sink").push(
                        String::from_utf8_lossy(event.message().payload.as_slice()).to_string(),
                    );
                    Ok(())
                })
                    as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
            }),
        )
        .await
        .expect("subscribe");

    engine
        .publish(
            "rushwind-live-pub",
            Message::from_payload(b"hello-nsq".to_vec()),
        )
        .await
        .expect("publish");

    wait_for("the delivery", || {
        seen.lock().expect("seen").iter().any(|r| r == "hello-nsq")
    })
    .await;
}

#[tokio::test]
async fn manual_mode_fins_and_requeues() {
    let engine = engine("rushwind-live-manual", AckMode::Manual);
    engine.connect().await.expect("connect");
    ensure_topic(&engine, "rushwind-live-manual").await;

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _sub = engine
        .subscribe(
            "rushwind-live-manual",
            Arc::new(move |event: Event| {
                let sink = sink.clone();
                Box::pin(async move {
                    let payload =
                        String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                    if payload == "settle-me" {
                        // FIN: settled, never redelivered.
                        event.ack().await?;
                    } else {
                        // REQ 0: the real nsqd re-queues immediately.
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
        .publish(
            "rushwind-live-manual",
            Message::from_payload(b"settle-me".to_vec()),
        )
        .await
        .expect("publish acked");
    engine
        .publish(
            "rushwind-live-manual",
            Message::from_payload(b"poison".to_vec()),
        )
        .await
        .expect("publish poison");

    // The ack settles; the REQ re-queues — the poison redelivers.
    wait_for("the ack and the redelivery", || {
        let seen = seen.lock().expect("seen");
        let acked = seen.iter().filter(|r| r.as_str() == "settle-me").count();
        let poisoned = seen.iter().filter(|r| r.as_str() == "poison").count();
        acked >= 1 && poisoned >= 2
    })
    .await;
}

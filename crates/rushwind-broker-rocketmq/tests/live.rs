//! Live conformance against a real RocketMQ 5.x broker, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! broker address comes from `ROCKETMQ_ADDR` (default
//! `127.0.0.1:10911`). The topics pre-register through the engine's
//! own constructor-time admin face is out of scope here — the harness
//! creates them via `mqadmin` inside the container before the pins
//! run (the broker's auto-create branch NPEs server-side on 5.3).
//!
//! What this suite pins beyond the mock: the real broker's remoting
//! wire — the frame encoding `(serializeType << 24) | headerLength`,
//! the SEND_MESSAGE field set answering code 0 with the broker's own
//! msgId, the HEARTBEAT registration chain (the consumer group must
//! register through the HeartbeatData schema before any PULL
//! resolves), the PULL header's three fields (`requestSource`,
//! `sysFlag: 7`, `subVersion`), and the FOUND body's CommitLog record
//! stream decoding (the layout the live experiments established:
//! SIZE-inclusive records, BODYLEN at 84, the properties string
//! before the record's end).

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Event, Message, NackAction};
use rushwind_broker_rocketmq::{RocketmqBroker, RocketmqOptions};

fn addr() -> String {
    std::env::var("ROCKETMQ_ADDR").unwrap_or_else(|_| "127.0.0.1:10911".to_string())
}

/// Pre-registers the pin's topic through `mqadmin` inside the broker
/// container: broker 5.3's auto-create branch NPEs server-side, so the
/// harness registers before the pins run (the CI job wires the
/// container name). Idempotent — the updateTopic face is a create-or-
/// update.
async fn ensure_topic(topic: &str) {
    let container = std::env::var("ROCKETMQ_CONTAINER").unwrap_or_default();
    if container.is_empty() {
        return; // The deployment registered its topics already.
    }
    let _ = std::process::Command::new("docker")
        .args([
            "exec",
            &container,
            "sh",
            "mqadmin",
            "updateTopic",
            "-n",
            &format!("{}:9876", container.replace("broker", "namesrv").as_str()),
            "-c",
            "DefaultCluster",
            "-t",
            topic,
        ])
        .output();
}

/// The broker's real name, discovered once through `mqadmin` (the
/// sends carry it and the broker's mapping context keys on it — a
/// mismatched name reads as another broker's queue).
fn broker_name() -> String {
    std::sync::OnceLock::<String>::new()
        .get_or_init(|| {
            let container = std::env::var("ROCKETMQ_CONTAINER").unwrap_or_default();
            if container.is_empty() {
                return "broker-a".to_string();
            }
            let output = std::process::Command::new("docker")
                .args([
                    "exec",
                    &container,
                    "sh",
                    "-c",
                    "hostname || grep brokerName /home/rocketmq/broker.conf",
                ])
                .output();
            let name = output
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().split('=').last().unwrap_or_default().to_string())
                .unwrap_or_default();
            if name.is_empty() {
                "broker-a".to_string()
            } else {
                name
            }
        })
        .clone()
}

fn engine(topic: &str, ack_mode: AckMode) -> Arc<RocketmqBroker> {
    Arc::new(
        RocketmqBroker::new(RocketmqOptions {
            broker_addr: addr(),
            broker_name: broker_name(),
            producer_group: "rushwind".to_string(),
            consumer_group: format!("{}-cg", topic),
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
            start + Duration::from_secs(20) > tokio::time::Instant::now(),
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn publish_receive_round_trip_on_real_broker() {
    ensure_topic("rushwind-live-pub").await;
    let engine = engine("rushwind-live-pub", AckMode::Auto);
    engine.connect().await.expect("connect");

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
            Message::from_payload(b"hello-rocketmq".to_vec()),
        )
        .await
        .expect("publish");

    wait_for("the delivery", || {
        seen.lock()
            .expect("seen")
            .iter()
            .any(|r| r == "hello-rocketmq")
    })
    .await;
}

#[tokio::test]
async fn manual_mode_settles_and_redelivers() {
    ensure_topic("rushwind-live-manual").await;
    let engine = engine("rushwind-live-manual", AckMode::Manual);
    engine.connect().await.expect("connect");

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
                        event.ack().await?;
                    } else {
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

    // The Manual window: the ack closes past the batch, the requeue
    // re-opens the pull at the batch's start — the poison redelivers.
    wait_for("the ack and the redelivery", || {
        let seen = seen.lock().expect("seen");
        let acked = seen.iter().filter(|r| r.as_str() == "settle-me").count();
        let poisoned = seen.iter().filter(|r| r.as_str() == "poison").count();
        acked >= 1 && poisoned >= 2
    })
    .await;
}

#[tokio::test]
async fn headers_round_trip_through_the_wire() {
    ensure_topic("rushwind-live-headers").await;
    let engine = engine("rushwind-live-headers", AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _sub = engine
        .subscribe(
            "rushwind-live-headers",
            Arc::new(move |event: Event| {
                let sink = sink.clone();
                Box::pin(async move {
                    let payload =
                        String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                    let hop = event
                        .message()
                        .headers
                        .get("hop")
                        .cloned()
                        .unwrap_or_default();
                    sink.lock().expect("sink").push(format!("{payload}|{hop}"));
                    Ok(())
                })
                    as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
            }),
        )
        .await
        .expect("subscribe");

    engine
        .publish(
            "rushwind-live-headers",
            Message::from_payload(b"with-headers".to_vec()).with_header("hop", "three"),
        )
        .await
        .expect("publish");

    wait_for("the headered delivery", || {
        seen.lock()
            .expect("seen")
            .iter()
            .any(|r| r == "with-headers|three")
    })
    .await;
}

#[tokio::test]
async fn lifecycle_gates_the_wire() {
    ensure_topic("rushwind-live-gates").await;
    let engine = engine("rushwind-live-gates", AckMode::Auto);
    engine.disconnect().await.expect("disconnect");
    assert!(matches!(
        engine
            .publish("rushwind-live-gates", Message::from_payload(b"x".to_vec()))
            .await,
        Err(rushwind_broker::BrokerError::NotConnected)
    ));
    engine.connect().await.expect("reconnect");
    engine
        .publish(
            "rushwind-live-gates",
            Message::from_payload(b"back".to_vec()),
        )
        .await
        .expect("publish after reconnect");
}

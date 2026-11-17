//! The RocketMQ engine's semantic pins against the in-process mock
//! broker: the publish wire (properties string + payload), Auto
//! delivery with offset commits, the Manual one-batch window (park
//! until ack, nack-requeue redelivery, nack-drop settles away),
//! unsubscribing, the failed-send refusal, and the lifecycle/options
//! validation.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, BrokerError, Event, Handler, Message, NackAction};
use rushwind_broker_rocketmq::{RocketmqBroker, RocketmqOptions};

mod common;
use common::spawn_rocketmq;

fn engine(addr: &str, ack_mode: AckMode) -> RocketmqBroker {
    RocketmqBroker::new(RocketmqOptions {
        broker_addr: addr.to_string(),
        broker_name: "broker-a".to_string(),
        producer_group: "rushwind".to_string(),
        consumer_group: "rushwind".to_string(),
        ack_mode,
        request_timeout: Duration::from_secs(5),
    })
    .expect("options valid")
}

type Seen = Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>;

fn collector(out: Seen) -> Handler {
    Arc::new(move |event: Event| {
        let out = out.clone();
        Box::pin(async move {
            out.lock().expect("collector").push((
                String::from_utf8_lossy(event.message().payload.as_slice()).to_string(),
                event
                    .message()
                    .headers
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    })
}

/// Polls until the predicate holds or the deadline passes.
async fn wait_until(what: &str, deadline: Duration, predicate: impl Fn() -> bool) {
    let start = tokio::time::Instant::now();
    while !predicate() {
        assert!(
            start + deadline > tokio::time::Instant::now(),
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn publish_carries_the_properties_string_and_payload() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Auto);
    engine.connect().await.expect("connect");

    let message = Message::from_payload(b"hello".to_vec())
        .with_header("color", "red")
        .with_header("size", "big");
    engine.publish("events", message).await.expect("publish");

    let sent = mock.last_send().expect("the send reached the mock");
    assert_eq!(sent.topic, "events");
    assert_eq!(sent.body, b"hello");
    // The headers joined the KEY\u0001VALUE\u0002 properties string.
    assert!(
        sent.properties.contains("color\u{0001}red"),
        "{:?}",
        sent.properties
    );
    assert!(
        sent.properties.contains("size\u{0001}big"),
        "{:?}",
        sent.properties
    );
    assert_eq!(sent.properties.matches('\u{0002}').count(), 1);
}

#[tokio::test]
async fn auto_mode_delivers_and_commits() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let _sub = engine
        .subscribe("events", collector(seen.clone()))
        .await
        .expect("subscribe");

    engine
        .publish(
            "events",
            Message::from_payload(b"m0".to_vec()).with_header("hop", "one"),
        )
        .await
        .expect("publish");
    engine
        .publish("events", Message::from_payload(b"m1".to_vec()))
        .await
        .expect("publish");

    wait_until("both deliveries", Duration::from_secs(5), || {
        seen.lock().expect("seen").len() == 2
    })
    .await;
    // The properties string rides back through the pull's
    // msgFoundList — headers survive the round trip.
    {
        let seen = seen.lock().expect("seen");
        assert!(seen.iter().any(|(payload, headers)| payload == "m0"
            && headers.get("hop").map(String::as_str) == Some("one")));
        assert!(seen.iter().any(|(payload, _)| payload == "m1"));
    }
    // Auto settles at dispatch: the batch's nextBeginOffset commits.
    wait_until("the offset commit", Duration::from_secs(5), || {
        mock.committed("events") == 2
    })
    .await;
}

#[tokio::test]
async fn manual_mode_parks_until_acked() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Manual);
    engine.connect().await.expect("connect");

    let release = Arc::new(tokio::sync::Notify::new());
    let release_for_handler = release.clone();
    let touched = Arc::new(AtomicUsize::new(0));
    let touched_for_handler = touched.clone();
    let handler: Handler = Arc::new(move |event: Event| {
        let release = release_for_handler.clone();
        let touched = touched_for_handler.clone();
        Box::pin(async move {
            touched.fetch_add(1, Ordering::SeqCst);
            tokio::select! {
                _ = release.notified() => event.ack().await,
            }
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });

    let _sub = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    engine
        .publish("events", Message::from_payload(b"parked".to_vec()))
        .await
        .expect("publish");

    // The delivery arrives but settles nothing: no commit while the
    // handler parks (the one-batch window holds).
    wait_until("the parked delivery", Duration::from_secs(5), || {
        touched.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        mock.committed("events"),
        0,
        "Manual parks the window until the handler settles"
    );

    release.notify_waiters();
    wait_until("the ack commit", Duration::from_secs(5), || {
        mock.committed("events") == 1
    })
    .await;
}

#[tokio::test]
async fn manual_mode_nack_requeue_redelivers() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Manual);
    engine.connect().await.expect("connect");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let sink = seen.clone();
    let attempts_for_handler = attempts.clone();
    let handler: Handler = Arc::new(move |event: Event| {
        let sink = sink.clone();
        let attempts = attempts_for_handler.clone();
        Box::pin(async move {
            let payload = String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                event.nack(NackAction::Requeue).await?;
                sink.lock()
                    .expect("sink")
                    .push((format!("{payload}:requeued"), BTreeMap::new()));
                return Ok(());
            }
            event.ack().await?;
            sink.lock()
                .expect("sink")
                .push((format!("{payload}:acked"), BTreeMap::new()));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });

    let _sub = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    engine
        .publish("events", Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish");

    // The nack requeues: the next pull re-pulls from the batch's start
    // offset — the same message redelivers, then the ack settles.
    wait_until(
        "the requeue-and-redeliver cycle",
        Duration::from_secs(5),
        || {
            let seen = seen.lock().expect("seen");
            seen.iter().any(|(p, _)| p == "poison:requeued")
                && seen.iter().any(|(p, _)| p == "poison:acked")
        },
    )
    .await;
    wait_until("the final commit", Duration::from_secs(5), || {
        mock.committed("events") == 1
    })
    .await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the message delivered exactly twice"
    );
}

#[tokio::test]
async fn manual_mode_nack_drop_commits_past() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Manual);
    engine.connect().await.expect("connect");

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = attempts.clone();
    let handler: Handler = Arc::new(move |event: Event| {
        let attempts = attempts_for_handler.clone();
        Box::pin(async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            event.nack(NackAction::Drop).await?;
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });

    let _sub = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    engine
        .publish("events", Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish");

    // Drop settles the poison away like an ack: the window closes past
    // the batch, nothing redelivers.
    wait_until("the drop commit", Duration::from_secs(5), || {
        mock.committed("events") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the dropped poison never redelivers"
    );
}

#[tokio::test]
async fn unsubscribe_stops_the_pump() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let mut sub = engine
        .subscribe("events", collector(seen.clone()))
        .await
        .expect("subscribe");
    engine
        .publish("events", Message::from_payload(b"first".to_vec()))
        .await
        .expect("publish");
    wait_until("the first delivery", Duration::from_secs(5), || {
        seen.lock().expect("seen").len() == 1
    })
    .await;

    sub.unsubscribe().await.expect("unsubscribe");
    // The quiesce: any receive already in flight when the stop landed
    // completes against the pre-stop queue (empty here), and the
    // pump's next loop-top check observes the stop — the post-publish
    // message cannot reach a stopped subscription.
    tokio::time::sleep(Duration::from_millis(250)).await;
    engine
        .publish("events", Message::from_payload(b"second".to_vec()))
        .await
        .expect("publish after unsubscribe");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        seen.lock().expect("seen").len(),
        1,
        "no delivery reaches a stopped pump"
    );
    assert_eq!(
        mock.committed("events"),
        1,
        "only the first batch committed"
    );
}

#[tokio::test]
async fn a_failed_send_refuses_the_publish() {
    let (addr, mock) = spawn_rocketmq().await;
    let engine = engine(&addr, AckMode::Auto);
    engine.connect().await.expect("connect");
    mock.set_send_reject(Some("queue full".to_string()));

    let err = engine
        .publish("events", Message::from_payload(b"x".to_vec()))
        .await
        .expect_err("the send failed");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("queue full")),
        "{err:?}"
    );
}

#[tokio::test]
async fn lifecycle_and_options_validation() {
    // The unset broker address refuses both constructor faces.
    let err = RocketmqBroker::new(RocketmqOptions::default()).expect_err("addr unset");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("broker_addr")),
        "{err:?}"
    );
    let err = RocketmqBroker::from_settings(serde_json::json!({})).expect_err("addr unset");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("broker_addr")),
        "{err:?}"
    );

    // The settings wire takes the snake_case shape (ack_mode
    // lowercase), ignoring unknown fields.
    let engine = RocketmqBroker::from_settings(serde_json::json!({
        "broker_addr": "127.0.0.1:10911",
        "consumer_group": "g",
        "ack_mode": "manual",
        "unknown_field": 1,
    }))
    .expect("settings valid");
    assert_eq!(Broker::name(&engine), "rocketmq");

    // Disconnect gates the wire faces.
    engine.disconnect().await.expect("disconnect");
    assert!(matches!(
        engine
            .publish("events", Message::from_payload(b"x".to_vec()))
            .await,
        Err(BrokerError::NotConnected)
    ));
    assert!(matches!(
        engine
            .subscribe("events", collector(Arc::new(Mutex::new(Vec::new()))))
            .await,
        Err(BrokerError::NotConnected)
    ));
}

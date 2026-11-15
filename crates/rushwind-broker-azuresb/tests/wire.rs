//! Wire pins for the Azure Service Bus engine: the SAS reference
//! vector, the publish wire (user properties + BrokerProperties +
//! payload), Auto destructive receives, the Manual peek-lock window
//! (park until ack, abandon-requeue redelivery, drop settles away),
//! unsubscribing, the failed-send refusal, and the
//! lifecycle/options validation.

#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, BrokerError, Event, Handler, Message, NackAction};
use rushwind_broker_azuresb::{sas_token, AzureSbBroker, AzureSbOptions};

mod common;
use common::spawn_azuresb;

fn engine(endpoint: &str, ack_mode: AckMode) -> AzureSbBroker {
    AzureSbBroker::new(AzureSbOptions {
        entity_url: "https://rushwind.example.servicebus.windows.net/orders".to_string(),
        endpoint: Some(endpoint.to_string()),
        sas_key_name: "rushwind-send".to_string(),
        sas_key: "a-fixed-test-key==".to_string(),
        sas_token: None,
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

#[test]
fn the_sas_reference_vector_is_reproduced() {
    // The token was computed once with the SAS docs' algorithm
    // (sig = base64 HMAC-SHA256 over "{url-encoded resource}\n{expiry}"
    // keyed with the SAS key, everything outside RFC 3986's unreserved
    // set percent-encoded) and frozen — any drift in the signing
    // chain fails this.
    let token = sas_token(
        "https://rushwind.example.servicebus.windows.net/orders",
        "rushwind-send",
        "a-fixed-test-key==",
        1_800_000_000,
    );
    assert_eq!(
        token,
        "SharedAccessSignature sr=https%3A%2F%2Frushwind.example.servicebus.windows.net%2Forders&sig=x9DnP9bag%2BCe48goAPjoXUFh7kdKNLm1wIDjzniahMU%3D&se=1800000000&sk=rushwind-send"
    );
}

#[tokio::test]
async fn publish_carries_properties_broker_properties_and_payload() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let message = Message::from_payload(b"hello".to_vec()).with_header("color", "red");
    engine.publish("orders", message).await.expect("publish");

    let sent = mock.last_send().expect("the send reached the mock");
    assert_eq!(sent.body, b"hello");
    assert_eq!(
        sent.properties.get("color").map(String::as_str),
        Some("red"),
        "the header rode as a user-property header"
    );
    assert!(
        sent.broker_properties["MessageId"].is_string(),
        "the message id rode the BrokerProperties envelope: {:?}",
        sent.broker_properties
    );
    // The minted SAS token rode the Authorization header.
    let auth = mock.last_auth();
    assert!(
        auth.starts_with("SharedAccessSignature sr=https%3A%2F%2F"),
        "{auth}"
    );
    assert!(auth.contains("sk=rushwind-send"), "{auth}");
}

#[tokio::test]
async fn auto_mode_receives_and_deletes() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let _sub = engine
        .subscribe("orders", collector(seen.clone()))
        .await
        .expect("subscribe");

    engine
        .publish(
            "orders",
            Message::from_payload(b"m0".to_vec()).with_header("hop", "one"),
        )
        .await
        .expect("publish");
    engine
        .publish("orders", Message::from_payload(b"m1".to_vec()))
        .await
        .expect("publish");

    wait_until("both deliveries", Duration::from_secs(5), || {
        seen.lock().expect("seen").len() == 2
    })
    .await;
    {
        let seen = seen.lock().expect("seen");
        assert!(
            seen.iter().any(|(payload, headers)| payload == "m0"
                && headers.get("hop").map(String::as_str) == Some("one")),
            "the user property rode back: {seen:?}"
        );
        assert!(seen.iter().any(|(payload, _)| payload == "m1"));
    }
    // The destructive receive settled both: the queue is empty,
    // nothing is locked.
    wait_until("both destructive receives", Duration::from_secs(5), || {
        mock.destructive_receives() == 2
    })
    .await;
    assert_eq!(mock.locked_len(), 0);
    assert_eq!(mock.queue_len(), 0);
}

#[tokio::test]
async fn manual_mode_parks_until_acked() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Manual);
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
        .subscribe("orders", handler)
        .await
        .expect("subscribe");
    engine
        .publish("orders", Message::from_payload(b"parked".to_vec()))
        .await
        .expect("publish");

    // The delivery arrives peek-locked: the mock parks it in-flight.
    wait_until("the peek-locked delivery", Duration::from_secs(5), || {
        touched.load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until("the lock is held", Duration::from_secs(5), || {
        mock.locked_len() == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        mock.locked_len(),
        1,
        "Manual parks the lock until the handler settles"
    );

    release.notify_waiters();
    wait_until("the completion", Duration::from_secs(5), || {
        mock.locked_len() == 0
    })
    .await;
    assert_eq!(mock.queue_len(), 0, "the completed message is settled away");
}

#[tokio::test]
async fn manual_mode_nack_requeue_redelivers() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Manual);
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
        .subscribe("orders", handler)
        .await
        .expect("subscribe");
    engine
        .publish("orders", Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish");

    // The abandon releases the lock and re-enters the queue — the
    // message redelivers with DeliveryCount incremented, then the ack
    // completes it.
    wait_until(
        "the abandon-and-redeliver cycle",
        Duration::from_secs(5),
        || {
            let seen = seen.lock().expect("seen");
            seen.iter().any(|(p, _)| p == "poison:requeued")
                && seen.iter().any(|(p, _)| p == "poison:acked")
        },
    )
    .await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the message delivered exactly twice"
    );
    assert_eq!(
        mock.last_receive_broker().expect("received")["DeliveryCount"],
        serde_json::json!(1),
        "the redelivery's DeliveryCount incremented"
    );
    assert_eq!(mock.locked_len(), 0);
    assert_eq!(mock.queue_len(), 0);
}

#[tokio::test]
async fn manual_mode_nack_drop_settles_away() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Manual);
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
        .subscribe("orders", handler)
        .await
        .expect("subscribe");
    engine
        .publish("orders", Message::from_payload(b"poison".to_vec()))
        .await
        .expect("publish");

    // Drop completes the poison: settled away, never redelivered.
    wait_until("the drop completion", Duration::from_secs(5), || {
        attempts.load(Ordering::SeqCst) == 1 && mock.locked_len() == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the dropped poison never redelivers"
    );
    assert_eq!(mock.queue_len(), 0);
}

#[tokio::test]
async fn unsubscribe_stops_the_pump() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let mut sub = engine
        .subscribe("orders", collector(seen.clone()))
        .await
        .expect("subscribe");
    engine
        .publish("orders", Message::from_payload(b"first".to_vec()))
        .await
        .expect("publish");
    wait_until("the first delivery", Duration::from_secs(5), || {
        seen.lock().expect("seen").len() == 1
    })
    .await;

    sub.unsubscribe().await.expect("unsubscribe");
    engine
        .publish("orders", Message::from_payload(b"second".to_vec()))
        .await
        .expect("publish after unsubscribe");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        seen.lock().expect("seen").len(),
        1,
        "no delivery reaches a stopped pump"
    );
    assert_eq!(mock.queue_len(), 1, "the message stayed queued, unreceived");
}

#[tokio::test]
async fn a_failed_send_refuses_the_publish() {
    let (endpoint, mock) = spawn_azuresb().await;
    let engine = engine(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");
    mock.set_send_reject(true);

    let err = engine
        .publish("orders", Message::from_payload(b"x".to_vec()))
        .await
        .expect_err("the send failed");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("500")),
        "{err:?}"
    );
}

#[tokio::test]
async fn lifecycle_and_options_validation() {
    // The unset entity url refuses.
    let err = AzureSbBroker::new(AzureSbOptions::default()).expect_err("entity unset");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("entity_url")),
        "{err:?}"
    );

    // Neither keys nor token refuses.
    let err = AzureSbBroker::new(AzureSbOptions {
        entity_url: "https://rushwind.example.servicebus.windows.net/orders".to_string(),
        ..AzureSbOptions::default()
    })
    .expect_err("no credential");
    assert!(
        matches!(err, BrokerError::Failed(ref m) if m.contains("SAS")),
        "{err:?}"
    );

    // The settings wire takes the snake_case shape (ack_mode
    // lowercase), ignoring unknown fields.
    let engine = AzureSbBroker::from_settings(serde_json::json!({
        "entity_url": "https://rushwind.example.servicebus.windows.net/orders",
        "endpoint": "http://127.0.0.1:1",
        "sas_key_name": "rushwind-send",
        "sas_key": "a-fixed-test-key==",
        "ack_mode": "manual",
        "unknown_field": 1,
    }))
    .expect("settings valid");
    assert_eq!(Broker::name(&engine), "azuresb");

    // Disconnect gates the wire faces.
    engine.disconnect().await.expect("disconnect");
    assert!(matches!(
        engine
            .publish("orders", Message::from_payload(b"x".to_vec()))
            .await,
        Err(BrokerError::NotConnected)
    ));
    assert!(matches!(
        engine
            .subscribe("orders", collector(Arc::new(Mutex::new(Vec::new()))))
            .await,
        Err(BrokerError::NotConnected)
    ));
}

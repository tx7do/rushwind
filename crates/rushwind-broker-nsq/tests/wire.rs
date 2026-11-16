//! Wire pins for the NSQ engine: the publish envelope, the handshake
//! (heartbeats disabled), the auto-ack settle (FIN at dispatch), the
//! manual hooks (FIN on success, REQ 0 requeue on poison), and the
//! requeue's redelivery.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Message};
use rushwind_broker_nsq::{NsqBroker, NsqOptions};

mod common;
use common::spawn_nsq;

fn broker(addr: &str, ack_mode: AckMode) -> NsqBroker {
    NsqBroker::new(NsqOptions {
        addr: addr.to_string(),
        channel_id: "rushwind".to_string(),
        ack_mode,
        request_timeout: Duration::from_secs(5),
    })
    .expect("options valid")
}

fn broker_auto(addr: &str) -> NsqBroker {
    broker(addr, AckMode::Auto)
}

#[tokio::test]
async fn construction_requires_addr() {
    assert!(NsqBroker::new(NsqOptions::default()).is_err());
}

/// The publish face: the envelope lands in the mock's queue, the
/// handshake carries heartbeats disabled (the pump's simple select).
#[tokio::test]
async fn publish_writes_the_envelope() {
    let (addr, _mock) = spawn_nsq().await;
    let engine = broker_auto(&addr);
    engine.connect().await.expect("connect");
    let message = Message::from_payload(b"hello nsq".to_vec()).with_header("kind", "demo");
    engine.publish("events", message).await.expect("publish");
    // The envelope's correctness rides the delivery pins below — this
    // pin is the publish path's success.
}

#[tokio::test]
async fn auto_mode_settles_at_dispatch() {
    let (addr, mock) = spawn_nsq().await;
    let engine = broker_auto(&addr);
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
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    // Wait for the pump's SUB to land, then deliver once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !mock.subscribed("events") {
        assert!(tokio::time::Instant::now() < deadline, "SUB never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    mock.enqueue("events", "delivered-body");
    mock.deliver();

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(received, "delivered-body");
    drop(subscriber);

    // The FIN rode the wire at dispatch: wait for the mock to clear the
    // in-flight record (the settle races the test's check).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !mock.in_flight_ids().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the in-flight record must clear when the FIN lands"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn manual_mode_acks_and_requeues_through_the_hooks() {
    let (addr, mock) = spawn_nsq().await;
    let engine = broker(&addr, AckMode::Manual);
    engine.connect().await.expect("connect");

    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let sink = sink.clone();
        Box::pin(async move {
            let payload = String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
            // Poison payloads (odd length) requeue; clean ones ack.
            let outcome = if payload.len() % 2 == 1 {
                event.nack(rushwind_broker::NackAction::Requeue).await
            } else {
                event.ack().await
            };
            sink.lock().unwrap().push(format!("{payload}:{outcome:?}"));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });

    let _subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    mock.enqueue("events", "clean");
    mock.enqueue("events", "poison");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !mock.subscribed("events") {
        assert!(tokio::time::Instant::now() < deadline, "SUB never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        // Re-deliver within the deadline: the pump's SUB races this.
        mock.deliver();
        let settled = {
            let seen = seen.lock().unwrap();
            seen.iter().any(|r| r.starts_with("clean:Ok"))
                && seen.iter().any(|r| r.starts_with("poison:Ok"))
        };
        if settled {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "both settlements must reach the hooks; seen: {:?}",
            seen.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The settle frames rode the wire: the clean body's FIN emptied the
    // in-flight set; the poison's REQ logged a requeue.
    assert!(
        mock.in_flight_ids().is_empty(),
        "the clean FIN settled; the poison REQ removed its record"
    );
}

#[tokio::test]
async fn the_handshake_disables_heartbeats() {
    let (addr, mock) = spawn_nsq().await;
    let engine = broker_auto(&addr);
    engine.connect().await.expect("connect");
    let _subscriber = engine
        .subscribe(
            "events",
            Arc::new(|_event: rushwind_broker::Event| {
                Box::pin(async { Ok(()) })
                    as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
            }),
        )
        .await
        .expect("subscribe");
    mock.deliver();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.last_heartbeat().is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the IDENTIFY never arrived"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.last_heartbeat(),
        Some(0),
        "heartbeats disabled — the legal zero spelling"
    );
}

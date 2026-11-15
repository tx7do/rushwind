//! The memory reference broker's semantic pins: the publish/subscribe
//! round trip, round-robin dispatch across slots, AckMode Auto settle,
//! Manual in-flight parking with ack/requeue redelivery, unsubscribing,
//! and the connect/disconnect lifecycle.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, BrokerError, Event, Handler, Message, NackAction};
use rushwind_broker_memory::MemoryBroker;

fn broker() -> MemoryBroker {
    MemoryBroker::new()
}

fn collector(out: Arc<std::sync::Mutex<Vec<String>>>) -> Handler {
    Arc::new(move |event: Event| {
        let out = out.clone();
        Box::pin(async move {
            out.lock()
                .expect("collector")
                .push(String::from_utf8_lossy(event.message().payload.as_slice()).to_string());
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    })
}

#[tokio::test]
async fn publish_subscribe_round_trip() {
    let engine = broker();
    engine.connect().await.expect("connect");

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _sub = engine
        .subscribe("events", collector(seen.clone()))
        .await
        .expect("subscribe");

    engine
        .publish("events", Message::from_payload(b"hello".to_vec()))
        .await
        .expect("publish");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.lock().expect("seen").is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the delivery never arrived"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen.lock().expect("seen")[0], "hello");
}

/// Two subscriber slots on one topic split the deliveries round-robin —
/// the dispatch order the contract's baseline fixes.
#[tokio::test]
async fn two_slots_split_the_load() {
    let engine = broker();
    engine.connect().await.expect("connect");

    let seen_a = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_b = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _sub_a = engine
        .subscribe("events", collector(seen_a.clone()))
        .await
        .expect("subscribe a");
    let _sub_b = engine
        .subscribe("events", collector(seen_b.clone()))
        .await
        .expect("subscribe b");

    for i in 0..4 {
        engine
            .publish(
                "events",
                Message::from_payload(format!("m{i}").into_bytes()),
            )
            .await
            .expect("publish");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let total = seen_a.lock().expect("a").len() + seen_b.lock().expect("b").len();
        if total == 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "deliveries incomplete"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !seen_a.lock().expect("a").is_empty() && !seen_b.lock().expect("b").is_empty(),
        "round-robin reaches both slots"
    );
}

#[tokio::test]
async fn manual_mode_parks_until_acked() {
    let engine = broker();
    engine.connect().await.expect("connect");

    // The handler parks the message in-flight and settles only when the
    // test releases it.
    let release = Arc::new(tokio::sync::Notify::new());
    let release_for_handler = release.clone();
    let handler: Handler = Arc::new(move |event: Event| {
        let release = release_for_handler.clone();
        Box::pin(async move {
            tokio::select! {
                _ = release.notified() => event.ack().await,
            }
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });

    let sub_with_mode =
        MemoryBroker::subscribe_with_mode(&engine, "events", handler, AckMode::Manual)
            .await
            .expect("subscribe");

    engine
        .publish("events", Message::from_payload(b"parked".to_vec()))
        .await
        .expect("publish");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        engine.in_flight_len(),
        1,
        "Manual parks the delivery until the handler settles"
    );

    release.notify_waiters();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while engine.in_flight_len() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ack must clear the in-flight record"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(sub_with_mode);
}

#[tokio::test]
async fn nack_requeue_redelivers() {
    let engine = broker();
    engine.connect().await.expect("connect");

    // First attempt: nack with Requeue. Second attempt: ack.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
                sink.lock().unwrap().push(format!("{payload}:requeued"));
                return Ok(());
            }
            event.ack().await?;
            sink.lock().unwrap().push(format!("{payload}:acked"));
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), BrokerError>>
    });

    let sub_with_mode =
        MemoryBroker::subscribe_with_mode(&engine, "events", handler, AckMode::Manual)
            .await
            .expect("subscribe");

    engine
        .publish(
            "events",
            Message::from_payload(b"poison-then-clean".to_vec()),
        )
        .await
        .expect("publish");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let seen = seen.lock().expect("seen");
            let requeued = seen.iter().any(|r| r.ends_with(":requeued"));
            let acked = seen.iter().any(|r| r.ends_with(":acked"));
            if requeued && acked {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the requeue-and-redeliver cycle must complete; seen: {:?}",
                *seen
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(sub_with_mode);
}

#[tokio::test]
async fn unsubscribe_stops_dispatch() {
    let engine = broker();
    engine.connect().await.expect("connect");

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut sub = engine
        .subscribe("events", collector(seen.clone()))
        .await
        .expect("subscribe");
    sub.unsubscribe().await.expect("unsubscribe");

    engine
        .publish("events", Message::from_payload(b"unseen".to_vec()))
        .await
        .expect("publish queues");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The queue holds the message (nothing to dispatch it to); the
    // handler saw nothing.
    assert!(seen.lock().expect("seen").is_empty());
    assert_eq!(engine.queued_len("events"), 1);
}

#[tokio::test]
async fn lifecycle_gates_publish_and_subscribe() {
    let engine = broker();
    engine.disconnect().await.expect("disconnect");
    assert!(matches!(
        engine
            .publish("events", Message::from_payload(b"x".to_vec()))
            .await,
        Err(BrokerError::NotConnected)
    ));
    assert!(matches!(
        engine
            .subscribe(
                "events",
                collector(Arc::new(std::sync::Mutex::new(Vec::new())))
            )
            .await,
        Err(BrokerError::NotConnected)
    ));
    engine.connect().await.expect("reconnect");
    engine
        .publish("events", Message::from_payload(b"back".to_vec()))
        .await
        .expect("publish after reconnect");
}

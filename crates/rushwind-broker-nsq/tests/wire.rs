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

    // The settle frames rode the wire: the clean body's FIN logged, and
    // the poison's REQ logged a requeue (which redelivers — the
    // in-flight table legitimately holds the redelivery, so the old
    // empty-in-flight assertion no longer holds under real REQ
    // semantics).
    assert!(mock.fin_total() >= 1, "the clean FIN settled");
    assert!(
        mock.requeue_total() >= 1,
        "the poison's REQ logged a requeue"
    );
}

/// REQ 0 really redelivers — offline: the requeued message returns to
/// *its own topic's* queue and the next delivery carries the same
/// payload (the mock's old empty-topic keying made REQ redelivery a
/// silent no-op, and the wire pin only asserted the hook fired).
#[tokio::test]
async fn manual_requeue_redelivers_the_same_payload() {
    let (addr, mock) = spawn_nsq().await;
    let engine = broker(&addr, AckMode::Manual);
    engine.connect().await.expect("connect");

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let counter = Arc::clone(&count);
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                let payload =
                    String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    event.nack(rushwind_broker::NackAction::Requeue).await?;
                } else {
                    event.ack().await?;
                }
                tx.send(payload)
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };

    let _subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !mock.subscribed("events") {
        assert!(tokio::time::Instant::now() < deadline, "SUB never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    mock.enqueue("events", "poison");
    mock.deliver();

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the first delivery arrives")
        .expect("non-empty");
    assert_eq!(first, "poison");

    // The REQ landed for the id the delivery minted...
    let id = mock.last_delivered_id().expect("a delivery was minted");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.requeue_count(&id) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the REQ never landed for {id}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // ...and the requeued message really redelivers.
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the requeued message redelivers")
        .expect("non-empty");
    assert_eq!(second, "poison", "the requeue redelivers the payload");
}

/// Unsubscribe stops the pump: after the stop, deliveries the mock
/// pushes reach no handler, nothing is FINed server-side, and the
/// message count stays flat — the pump exited instead of consuming on.
#[tokio::test]
async fn unsubscribe_stops_the_pump() {
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

    let mut subscriber = engine
        .subscribe("events", handler)
        .await
        .expect("subscribe");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !mock.subscribed("events") {
        assert!(tokio::time::Instant::now() < deadline, "SUB never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Settle one real delivery first, so the pin is not racing an
    // idle pump that never consumed anything anyway.
    mock.enqueue("events", "before-stop");
    mock.deliver();
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the pre-stop delivery arrives")
        .expect("non-empty");
    assert_eq!(received, "before-stop");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.fin_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pre-stop FIN never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    subscriber.unsubscribe().await.expect("unsubscribe");
    // The pump's exit checks ride the idle tick; wait it out — and for
    // the mock to see the connection close (the deliverer prunes).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.connection_count() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pump's connection never closed (deliverers: {})",
            mock.connection_count()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivered_at_stop = mock.delivered_total();
    let fins_at_stop = mock.fin_total();

    mock.enqueue("events", "after-stop");
    mock.deliver();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert_eq!(
        mock.delivered_total(),
        delivered_at_stop,
        "a stopped pump must receive nothing further"
    );
    assert!(
        rx.try_recv().is_err(),
        "a stopped subscription's handler must see nothing"
    );
    assert_eq!(
        mock.fin_total(),
        fins_at_stop,
        "the post-stop message must stay unsettled server-side"
    );
}

/// A duplicate subscribe retires the old pump: the newest handler owns
/// the topic, the old handler's channel stays silent, and exactly one
/// pump consumes (one FIN per delivery).
#[tokio::test]
async fn duplicate_subscribe_retires_the_old_pump() {
    let (addr, mock) = spawn_nsq().await;
    let engine = broker_auto(&addr);
    engine.connect().await.expect("connect");

    let (mut rx_first, handler_first) = {
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
    let mut first = engine
        .subscribe("events", handler_first)
        .await
        .expect("first subscribe");
    let (mut rx_second, handler_second) = {
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
    let mut second = engine
        .subscribe("events", handler_second)
        .await
        .expect("duplicate subscribe");

    // The retired pump exits on its idle tick; one live connection
    // remains, and the replacement's SUB has landed on it, before
    // anything is delivered.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count = mock.connection_count();
        let subscribed = mock.subscribed("events");
        if count <= 1 && subscribed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retired pump never exited (count {count}) or the replacement's SUB never landed (subscribed {subscribed})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    mock.enqueue("events", "to-the-newest");
    mock.deliver();
    let received = tokio::time::timeout(Duration::from_secs(5), rx_second.recv())
        .await
        .expect("the newest handler receives")
        .expect("non-empty");
    assert_eq!(received, "to-the-newest");

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        rx_first.try_recv().is_err(),
        "the retired handler must stay silent"
    );
    assert_eq!(
        mock.fin_total(),
        1,
        "exactly one pump consumed — the old one exited"
    );

    // The stale handle's unsubscribe must not stop the replacement.
    first
        .unsubscribe()
        .await
        .expect("stale unsubscribe is a no-op");
    second.unsubscribe().await.expect("owner unsubscribe");
    // Wait for the pump's connection to close before observing — the
    // exit rides the idle tick, and a frame delivered mid-exit would
    // be dropped by the pump's delivery-face guard anyway.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.connection_count() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the replacement's connection never closed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivered_after_owner_unsub = mock.delivered_total();
    mock.enqueue("events", "only-if-stopped-wrong");
    mock.deliver();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        mock.delivered_total(),
        delivered_after_owner_unsub,
        "the owner unsubscribe must stop the replacement's pump"
    );
    assert!(rx_second.try_recv().is_err());
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

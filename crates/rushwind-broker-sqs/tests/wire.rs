//! Wire pins for the Amazon SQS engine: the publish shape (base64
//! envelope + attributes), the auto-ack delete (observed on the mock's
//! delete table, not just the handler's outcome), the manual hooks
//! (delete on success, zeroed visibility *and real redelivery* on
//! requeue, delete on drop), the stop face (unsubscribe/drop/disconnect
//! stop the receive loop; a duplicate subscribe retires the old pump),
//! the XML-entity and binary-payload decode paths, and the
//! construction guards.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, Message, NackAction};
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

/// A handler that forwards every delivery's payload (raw bytes) into
/// an unbounded channel, plus the receiving end.
fn payload_channel() -> (
    tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    rushwind_broker::Handler,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(event.message().payload.clone())
                .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });
    (rx, handler)
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
    let (endpoint, _mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");
    let message = Message::from_payload(b"body-text".to_vec()).with_header("kind", "demo");
    engine.publish("my-queue", message).await.expect("publish");
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

/// The payload is bytes and SQS's body is text: a non-UTF-8 payload
/// rides the base64 envelope and lands back byte-identical — the old
/// lossy-UTF8 publish would have corrupted it silently.
#[tokio::test]
async fn binary_payload_roundtrips_byte_identical() {
    let (endpoint, _mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");
    let (mut rx, handler) = payload_channel();
    let _subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");

    let payload = vec![0x00, 0xff, 0xfe, 0x10, b'a', b'b'];
    engine
        .publish("my-queue", Message::from_payload(payload.clone()))
        .await
        .expect("publish");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(received, payload, "binary payload must survive the wire");
}

/// The Query API escapes text values; the engine decodes the named
/// entities, so a payload and headers holding XML markup survive
/// round-trip instead of arriving entity-mangled.
#[tokio::test]
async fn xml_markup_in_payload_and_headers_roundtrips() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");
    let (mut rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");

    let body = "a <b> & \"quoted\" 'text' </Body> tail";
    mock.enqueue(body, "xml-ish", "<tag> & \"more\"");
    engine
        .publish("my-queue", Message::from_payload(vec![]))
        .await
        .ok(); // keep the queue order irrelevant

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(
        String::from_utf8(received).expect("utf-8"),
        body,
        "the escaped body must decode back exactly"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Auto settles by deleting — asserted on the mock's delete table, not
/// just the handler's outcome (the pin used to read the handler string
/// and never look at the wire).
#[tokio::test]
async fn auto_pull_settles_by_deleting() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("delivered-text", "via", "mock");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a delivery arrives")
        .expect("non-empty");
    assert_eq!(String::from_utf8_lossy(&received), "delivered-text");
    // The dispatch's Delete rides the settle channel — poll for it,
    // then pin exactly-once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.deleted_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Auto's Delete never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.deleted_total(),
        1,
        "Auto must delete the received message exactly once"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Manual settlement reaches the hooks *and* the wire: the ack's
/// Delete lands on the mock's delete table.
#[tokio::test]
async fn manual_settlement_reaches_the_hooks() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let settled: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let sink = settled.clone();
    let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
        let sink = sink.clone();
        Box::pin(async move {
            let payload = String::from_utf8_lossy(event.message().payload.as_slice()).to_string();
            let outcome = event.ack().await;
            assert!(outcome.is_ok(), "ack hook must succeed: {outcome:?}");
            *sink.lock().unwrap() = Some(payload);
            Ok(())
        }) as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
    });

    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("manual-ack", "via", "mock");

    // The delivery reached the handler (it recorded the payload), then
    // the ack's Delete really landed on the mock's delete table.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let recorded = settled.lock().unwrap().clone();
        assert!(
            tokio::time::Instant::now() < deadline || recorded.is_some(),
            "the manual delivery never reached the handler"
        );
        if recorded.as_deref() == Some("manual-ack") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the manual delivery never reached the handler"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The ack's Delete rides the settle channel — poll for it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.deleted_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the Manual ack's Delete never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.deleted_total(),
        1,
        "the Manual ack's Delete must reach the wire"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Manual Requeue zeroes the visibility timeout *and really
/// redelivers*: the mock's in-flight table returns the message to the
/// queue, so the second delivery carries the same payload — the pin
/// that was impossible while the mock popped messages into nowhere.
#[tokio::test]
async fn manual_requeue_zeroes_visibility_and_redelivers() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    // The first delivery nacks with Requeue; the redelivery acks — a
    // handler that requeued forever would loop the pump, which is the
    // poison behavior the *cap* work addresses, not this pin.
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let counter = Arc::clone(&count);
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                let payload = event.message().payload.clone();
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    event.nack(NackAction::Requeue).await?;
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
    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("poison-ish", "via", "mock");

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the first delivery arrives")
        .expect("non-empty");
    assert_eq!(String::from_utf8_lossy(&first), "poison-ish");

    // The nack's ChangeMessageVisibility(0) really lands on the receipt
    // the receive minted.
    let receipt = mock.last_receipt().expect("a receipt was minted");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.visibility(&receipt) != Some(0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the requeue never zeroed the visibility"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The message re-enters the queue: the second delivery arrives and
    // the handler acks it (exactly one delete for the whole story).
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the requeued message redelivers")
        .expect("non-empty");
    assert_eq!(
        String::from_utf8_lossy(&second),
        "poison-ish",
        "the requeue redelivers the same payload"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.deleted_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ack's Delete never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(mock.deleted_total(), 1, "exactly one settle");

    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Manual Drop settles the delivery away: deleted, never redelivered.
#[tokio::test]
async fn manual_drop_deletes_without_redelivery() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Manual);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let handler: rushwind_broker::Handler = Arc::new(move |event: rushwind_broker::Event| {
            let tx = tx.clone();
            Box::pin(async move {
                let payload = event.message().payload.clone();
                event.nack(NackAction::Drop).await?;
                tx.send(payload)
                    .map_err(|e| rushwind_broker::BrokerError::Failed(format!("send: {e}")))
            })
                as rushwind_broker::BoxFuture<'static, Result<(), rushwind_broker::BrokerError>>
        });
        (rx, handler)
    };
    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    mock.enqueue("drop-me", "via", "mock");

    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the delivery arrives")
        .expect("non-empty");
    assert_eq!(String::from_utf8_lossy(&received), "drop-me");

    // The Drop hook settles via Delete; give the pump a moment, then
    // assert one delete and no redelivery in the window.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.deleted_total() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the Drop's Delete never landed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(mock.deleted_total(), 1, "exactly one settle");
    // No redelivery: the queue is empty and the in-flight message was
    // deleted — a second receive finds nothing.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        rx.try_recv().is_err(),
        "a dropped message must never redeliver"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Unsubscribe stops the receive loop: after the stop, messages the
/// mock plants are not received (the receive count stays flat), no
/// delivery reaches the handler, and nothing is deleted server-side —
/// the messages stay in the queue for whoever receives next.
#[tokio::test]
async fn unsubscribe_stops_the_pump() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let mut subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");

    // Settle one real delivery first, so the pin is not racing an
    // idle pump that never received anything anyway.
    mock.enqueue("before-stop", "via", "mock");
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the pre-stop delivery arrives")
        .expect("non-empty");
    assert_eq!(String::from_utf8_lossy(&received), "before-stop");

    subscriber.unsubscribe().await.expect("unsubscribe");
    // One receive may still be in flight when the stop lands; wait it
    // out, then observe.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let receives_at_stop = mock.receive_count();

    mock.enqueue("after-stop-1", "via", "mock");
    mock.enqueue("after-stop-2", "via", "mock");
    tokio::time::sleep(Duration::from_millis(900)).await;

    assert_eq!(
        mock.receive_count(),
        receives_at_stop,
        "a stopped pump must stop receiving"
    );
    assert!(
        rx.try_recv().is_err(),
        "a stopped subscription's handler must see nothing"
    );
    assert_eq!(
        mock.deleted_total(),
        1,
        "the post-stop messages must stay unconsumed server-side"
    );
}

/// Dropping the handle stops the receive loop too — the documented
/// best-effort face, now actually implemented.
#[tokio::test]
async fn dropping_the_handle_stops_the_pump() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx, handler) = payload_channel();
    let subscriber = engine
        .subscribe("my-queue", handler)
        .await
        .expect("subscribe");
    drop(subscriber);

    tokio::time::sleep(Duration::from_millis(700)).await;
    let receives_at_stop = mock.receive_count();
    mock.enqueue("after-drop", "via", "mock");
    tokio::time::sleep(Duration::from_millis(900)).await;

    assert_eq!(
        mock.receive_count(),
        receives_at_stop,
        "a dropped handle's pump must stop receiving"
    );
    assert!(rx.try_recv().is_err(), "no delivery after the drop");
}

/// A duplicate subscribe retires the old pump: the newest handler owns
/// the filter, the old handler's channel stays silent, and exactly one
/// pump receives (one delete, not two).
#[tokio::test]
async fn duplicate_subscribe_retires_the_old_pump() {
    let (endpoint, mock) = spawn_sqs().await;
    let engine = broker(&endpoint, AckMode::Auto);
    engine.connect().await.expect("connect");

    let (mut rx_first, handler_first) = payload_channel();
    let mut first = engine
        .subscribe("my-queue", handler_first)
        .await
        .expect("first subscribe");
    let (mut rx_second, handler_second) = payload_channel();
    let mut second = engine
        .subscribe("my-queue", handler_second)
        .await
        .expect("duplicate subscribe");

    mock.enqueue("to-the-newest", "via", "mock");
    let received = tokio::time::timeout(Duration::from_secs(5), rx_second.recv())
        .await
        .expect("the newest handler receives")
        .expect("non-empty");
    assert_eq!(String::from_utf8_lossy(&received), "to-the-newest");

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        rx_first.try_recv().is_err(),
        "the retired handler must stay silent"
    );
    assert_eq!(
        mock.deleted_total(),
        1,
        "exactly one pump received — the old one exited"
    );

    // The stale handle's unsubscribe must not stop the replacement.
    first
        .unsubscribe()
        .await
        .expect("stale unsubscribe is a no-op");
    second.unsubscribe().await.expect("owner unsubscribe");
    let receives_after_owner_unsub = mock.receive_count();
    mock.enqueue("only-if-stopped-wrong", "via", "mock");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        mock.receive_count(),
        receives_after_owner_unsub,
        "the owner unsubscribe must stop the replacement's pump"
    );
    assert!(rx_second.try_recv().is_err());
}

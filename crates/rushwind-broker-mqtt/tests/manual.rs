//! The acknowledgment wire face, pinned against a minimal frame-level
//! MQTT mock. There is no embedded MQTT broker in this crate's unit
//! lanes, but *which PUBACKs the engine emits* is observable wire
//! behavior that only a socket can judge — the mock speaks exactly the
//! four packet pairs the face needs (CONNECT/CONNACK, SUBSCRIBE/SUBACK,
//! PUBLISH, PUBACK) and counts what arrives.
//!
//! Pins:
//! - Auto settles on dispatch: the PUBACK rides once per delivery,
//!   whatever the handler did.
//! - Manual ack sends the PUBACK.
//! - Manual `NackAction::Requeue` withholds the PUBACK — the packet
//!   stays unacknowledged so the broker re-sends it on session resume.
//! - Manual `NackAction::Drop` sends the PUBACK — the only wire action
//!   that stops the re-sends.
//! - A broadcast delivery (two matching filters) settles exactly once:
//!   the first settle sends the packet's single PUBACK.

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{AckMode, Broker, NackAction};
use rushwind_broker_mqtt::{event_channel, MqttBroker, MqttOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

const PACKET_CONNECT: u8 = 1;
const PACKET_PUBLISH: u8 = 3;
const PACKET_PUBACK: u8 = 4;
const PACKET_SUBSCRIBE: u8 = 8;
const PACKET_UNSUBSCRIBE: u8 = 10;
const PACKET_PINGREQ: u8 = 12;
const PACKET_DISCONNECT: u8 = 14;

/// MQTT's variable-length remaining-size encoding.
fn encode_varint(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return out;
        }
    }
}

fn packet(first_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first_byte];
    out.extend(encode_varint(body.len()));
    out.extend_from_slice(body);
    out
}

fn connack() -> Vec<u8> {
    packet(0x20, &[0x00, 0x00])
}

fn suback(packet_id: u16) -> Vec<u8> {
    packet(
        0x90,
        &[packet_id.to_be_bytes()[0], packet_id.to_be_bytes()[1], 0x01],
    )
}

fn unsuback(packet_id: u16) -> Vec<u8> {
    packet(
        0xb0,
        &[packet_id.to_be_bytes()[0], packet_id.to_be_bytes()[1]],
    )
}

/// A QoS 1 PUBLISH: topic length, topic, packet id, payload.
fn publish(packet_id: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(payload);
    packet(PACKET_PUBLISH << 4 | 0b0010, &body)
}

/// One MQTT packet read off the wire: `(fixed_header_first_byte, body)`.
async fn read_packet(stream: &mut tokio::net::tcp::OwnedReadHalf) -> Option<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await.ok()?;
    let mut remaining = 0usize;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.ok()?;
        remaining |= ((byte[0] & 0x7f) as usize) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; remaining];
    if remaining > 0 {
        stream.read_exact(&mut body).await.ok()?;
    }
    Some((first[0], body))
}

/// What the mock reports back to the test.
#[derive(Debug)]
enum MockEvent {
    /// A PUBACK naming its packet id.
    PubAck(u16),
    /// The socket closed (DISCONNECT or EOF).
    Closed,
}

type Deliver = (u16, String, Vec<u8>);

/// One command for the mock's writer task: a handshake reply, or a
/// PUBLISH the test commands.
#[derive(Debug)]
enum WriteCmd {
    Reply(Vec<u8>),
    Deliver(Deliver),
}

/// The mock's writer task: owns the socket's write half exclusively —
/// every frame is written to completion, never cancelled mid-write by
/// a read (a select over both halves tore frames and produced phantom
/// PUBACKs). Deliveries commanded before the first SUBSCRIBE buffer
/// and flush once the subscribed flag is set.
async fn run_mock_writer(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut cmd_rx: mpsc::UnboundedReceiver<WriteCmd>,
    mut subscribed: watch::Receiver<bool>,
) {
    let mut pending: Vec<Deliver> = Vec::new();
    loop {
        tokio::select! {
            command = cmd_rx.recv() => {
                let Some(command) = command else {
                    return;
                };
                match command {
                    WriteCmd::Reply(bytes) => {
                        writer.write_all(&bytes).await.expect("mock reply write");
                    }
                    WriteCmd::Deliver((id, topic, payload)) => {
                        if *subscribed.borrow() {
                            let frame = publish(id, &topic, &payload);
                            writer.write_all(&frame).await.expect("mock publish write");
                        } else {
                            pending.push((id, topic, payload));
                        }
                    }
                }
            }
            // The subscribed flip arrives here even when no command
            // follows it — a watch keeps the value pending, so the
            // buffered deliveries drain whatever the timing was.
            _ = subscribed.changed() => {}
        }
        if *subscribed.borrow() {
            for (id, topic, payload) in pending.drain(..) {
                let frame = publish(id, &topic, &payload);
                writer.write_all(&frame).await.expect("mock publish write");
            }
        }
    }
}

/// The mock broker: a reader task that parses packets and hands the
/// writer its replies (its read loop is never cancelled — a cancelled
/// read loses the bytes it consumed and desynchronizes the stream),
/// plus the writer task above.
async fn run_mock(
    stream: TcpStream,
    mut publish_rx: mpsc::UnboundedReceiver<Deliver>,
    event_tx: mpsc::UnboundedSender<MockEvent>,
) {
    let (mut reader, writer) = stream.into_split();
    let (sub_tx, sub_rx) = watch::channel(false);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let writer_task = tokio::spawn(run_mock_writer(writer, cmd_rx, sub_rx));
    // The test's deliver() commands bridge into the writer's command
    // channel — the writer stays the socket's single writer.
    let bridge_tx = cmd_tx.clone();
    tokio::spawn(async move {
        while let Some(deliver) = publish_rx.recv().await {
            if bridge_tx.send(WriteCmd::Deliver(deliver)).is_err() {
                return;
            }
        }
    });
    loop {
        let Some((first, body)) = read_packet(&mut reader).await else {
            let _ = event_tx.send(MockEvent::Closed);
            writer_task.abort();
            return;
        };
        match first >> 4 {
            PACKET_CONNECT => {
                cmd_tx
                    .send(WriteCmd::Reply(connack()))
                    .expect("mock command send");
            }
            PACKET_SUBSCRIBE => {
                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                cmd_tx
                    .send(WriteCmd::Reply(suback(packet_id)))
                    .expect("mock command send");
                sub_tx.send_replace(true);
            }
            PACKET_UNSUBSCRIBE => {
                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                cmd_tx
                    .send(WriteCmd::Reply(unsuback(packet_id)))
                    .expect("mock command send");
            }
            PACKET_PUBACK => {
                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                event_tx
                    .send(MockEvent::PubAck(packet_id))
                    .expect("mock event send");
            }
            PACKET_PINGREQ => {}
            PACKET_DISCONNECT => {
                let _ = event_tx.send(MockEvent::Closed);
                return;
            }
            _ => {}
        }
    }
}

/// A running mock plus the handles the tests drive.
struct Mock {
    addr: String,
    publish_tx: mpsc::UnboundedSender<Deliver>,
    events: mpsc::UnboundedReceiver<MockEvent>,
}

async fn start_mock() -> Mock {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("mock bind");
    let addr = listener.local_addr().expect("mock addr").to_string();
    let (publish_tx, publish_rx) = mpsc::unbounded_channel();
    let (event_tx, events) = mpsc::unbounded_channel();
    // The accept rides the spawned task: the engine dials only after
    // this returns, so an in-body accept would deadlock the test.
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        run_mock(stream, publish_rx, event_tx).await;
    });
    Mock {
        addr,
        publish_tx,
        events,
    }
}

/// An engine over the mock, with a keep-alive far beyond every test
/// window so no PINGREQ can interleave the mock's reads with the
/// frames the pins wait on.
fn broker_over(mock: &Mock, ack_mode: AckMode) -> MqttBroker {
    let mut options = MqttOptions::new(mock.addr.clone());
    options.keep_alive = Duration::from_secs(600);
    MqttBroker::with_ack_mode(options, ack_mode)
}

impl Mock {
    fn deliver(&self, packet_id: u16, topic: &str, payload: &[u8]) {
        self.publish_tx
            .send((packet_id, topic.to_string(), payload.to_vec()))
            .expect("mock command send");
    }

    /// Asserts the next PUBACK names `packet_id`, within the window.
    async fn expect_puback(&mut self, packet_id: u16) {
        let deadline = Duration::from_secs(5);
        match tokio::time::timeout(deadline, self.events.recv()).await {
            Ok(Some(MockEvent::PubAck(seen))) if seen == packet_id => {}
            Ok(Some(MockEvent::PubAck(seen))) => {
                panic!("expected PUBACK {packet_id}, got {seen}");
            }
            Ok(Some(other)) => panic!("expected PUBACK {packet_id}, got {other:?}"),
            Ok(None) => panic!("mock closed before PUBACK {packet_id}"),
            Err(_) => panic!("PUBACK {packet_id} never arrived within {deadline:?}"),
        }
    }

    /// Asserts *no* PUBACK arrives within the window — the withhold
    /// pin: without this, a Requeue that PUBACKs would silently pass.
    async fn expect_no_puback(&mut self) {
        let window = Duration::from_millis(700);
        match tokio::time::timeout(window, self.events.recv()).await {
            Err(_) => {}
            Ok(Some(MockEvent::PubAck(seen))) => {
                panic!("a PUBACK ({seen}) escaped — the withhold/exactly-once gate is broken");
            }
            Ok(Some(other)) => panic!("unexpected mock event {other:?}"),
            Ok(None) => panic!("mock closed during the withhold window"),
        }
    }
}

/// The Manual face: Requeue withholds the PUBACK, Drop sends it, a
/// handler ack sends it. One broker, one subscription, three
/// deliveries — the split is observable packet by packet.
#[tokio::test]
async fn manual_requeue_withholds_and_drop_and_ack_puback() {
    let mut mock = start_mock().await;
    let broker = broker_over(&mock, AckMode::Manual);
    broker.connect().await.expect("connect");

    let (mut rx, forward) = event_channel();
    let mut subscriber = broker
        .subscribe("probe/manual", Arc::new(forward))
        .await
        .expect("subscribe");

    // Delivery 1: Requeue withholds — the packet stays unacknowledged.
    mock.deliver(1, "probe/manual", b"requeue-me");
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivery 1 arrives")
        .expect("channel open");
    event.nack(NackAction::Requeue).await.expect("requeue hook");
    mock.expect_no_puback().await;

    // Delivery 2: Drop takes the PUBACK — the terminus action.
    mock.deliver(2, "probe/manual", b"drop-me");
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivery 2 arrives")
        .expect("channel open");
    event.nack(NackAction::Drop).await.expect("drop hook");
    mock.expect_puback(2).await;

    // Delivery 3: a plain ack sends the PUBACK.
    mock.deliver(3, "probe/manual", b"ack-me");
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivery 3 arrives")
        .expect("channel open");
    event.ack().await.expect("ack hook");
    mock.expect_puback(3).await;

    subscriber.unsubscribe().await.expect("unsubscribe");
    broker.disconnect().await.expect("disconnect");
}

/// Auto settles on dispatch: the PUBACK rides once, whatever the
/// handler did — here, a handler that never settles still yields
/// exactly one PUBACK for the delivery.
#[tokio::test]
async fn auto_settles_on_dispatch_exactly_once() {
    let mut mock = start_mock().await;
    let broker = broker_over(&mock, AckMode::Auto);
    broker.connect().await.expect("connect");

    let (mut rx, forward) = event_channel();
    let mut subscriber = broker
        .subscribe("probe/auto", Arc::new(forward))
        .await
        .expect("subscribe");

    mock.deliver(1, "probe/auto", b"auto-me");
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    // The handler does nothing: Auto's PUBACK must not depend on it.
    drop(event);
    mock.expect_puback(1).await;
    // Exactly once: nothing else escapes after the settle.
    mock.expect_no_puback().await;

    subscriber.unsubscribe().await.expect("unsubscribe");
    broker.disconnect().await.expect("disconnect");
}

/// A broadcast Manual delivery settles exactly once: two *matching*
/// filters (the duplicate-filter subscribe replaces its handler in
/// place, so broadcast here means two filters over one topic), the
/// first settle sends the packet's single PUBACK, the second is a
/// no-op — two PUBACKs for one packet id would be a protocol violation
/// the broker is free to answer with a disconnect.
#[tokio::test]
async fn manual_broadcast_settles_exactly_once() {
    let mut mock = start_mock().await;
    let broker = broker_over(&mock, AckMode::Manual);
    broker.connect().await.expect("connect");

    let (mut rx_all, forward_all) = event_channel();
    let mut subscriber_all = broker
        .subscribe("probe/#", Arc::new(forward_all))
        .await
        .expect("wildcard subscribe");
    let (mut rx_exact, forward_exact) = event_channel();
    let mut subscriber_exact = broker
        .subscribe("probe/once", Arc::new(forward_exact))
        .await
        .expect("exact subscribe");

    // Both filters match: two events, each carrying settle hooks for
    // the same packet id — one PUBACK must represent them both.
    mock.deliver(1, "probe/once", b"once");
    let first = tokio::time::timeout(Duration::from_secs(5), rx_all.recv())
        .await
        .expect("wildcard delivery arrives")
        .expect("channel open");
    let second = tokio::time::timeout(Duration::from_secs(5), rx_exact.recv())
        .await
        .expect("exact delivery arrives")
        .expect("channel open");
    first.ack().await.expect("first settle");
    mock.expect_puback(1).await;
    second
        .ack()
        .await
        .expect("second settle is an honest no-op");
    mock.expect_no_puback().await;

    subscriber_all
        .unsubscribe()
        .await
        .expect("wildcard unsubscribe");
    subscriber_exact
        .unsubscribe()
        .await
        .expect("exact unsubscribe");
    broker.disconnect().await.expect("disconnect");
}

//! STOMP engine for the RushWind broker contract — a minimal STOMP
//! 1.2 client hand-rolled over tokio TCP, covering the
//! CONNECT/SEND/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT surface.
//!
//! # The wire behavior
//!
//! Publishes are SEND frames to the `/topic/{topic}` destination —
//! against RabbitMQ's stomp plugin that maps to the `amq.topic`
//! exchange with the topic as the routing key, the same wire surface
//! the RabbitMQ engine uses. Payloads are binary-safe: every SEND
//! carries a `content-length` header, and MESSAGE frames are decoded
//! honoring it. Subscriptions use `ack:auto` — STOMP over RabbitMQ
//! acknowledges implicitly, so the event ack is a no-op.
//!
//! One TCP connection carries every subscription. A single
//! connection task owns the socket: it performs the CONNECT
//! handshake, re-sends every live SUBSCRIBE after each (re)connect,
//! and multiplexes outbound frames (publishes, unsubscribes) with
//! inbound MESSAGE routing. A broken connection re-dials with a
//! doubling backoff capped at thirty seconds and replays the
//! subscriptions.
//!
//! `disconnect` tears down orderly: UNSUBSCRIBE frames go out for
//! every live subscription while the connection can still carry
//! them, then the DISCONNECT frame, then the connection task exits.
//! Subscriptions stay registered for the next `connect`, which
//! replays them after the handshake. `disconnect` is idempotent, and
//! publishes after it fail with [`BrokerError::NotConnected`].
//!
//! # Divergences
//!
//! - STOMP frames carry no headers on the default path here; the
//!   payload travels alone, as in the MQTT and Redis engines.
//! - `Broker::disconnect` closes the socket; subscriptions resume on
//!   the next `connect` rather than surviving the disconnect.
//!
//! # Testing
//!
//! Live conformance tests run against RabbitMQ's stomp plugin via the
//! `live` feature; there is no embedded STOMP broker for CI's unit
//! lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Notify};

/// The per-connection state shared with the frame senders.
struct Shared {
    addr: String,
    /// Live subscriptions: id → (topic, handler). The SUBSCRIBE
    /// destination is rebuilt from the topic.
    subscriptions: Mutex<HashMap<String, (String, Handler)>>,
    /// The frame sender of the live connection; `None` while
    /// disconnected.
    frames: Mutex<Option<mpsc::UnboundedSender<Vec<u8>>>>,
    connected: Mutex<bool>,
    /// Monotonic subscription ids.
    next_subscription_id: AtomicU64,
    /// The live connection generation; older tasks exit when it moves
    /// on.
    generation: AtomicU64,
}

/// A STOMP-backed broker over one TCP connection to the broker's
/// STOMP listener.
pub struct StompBroker {
    shared: Arc<Shared>,
    /// Per-generation connection-task shutdown.
    shutdown: Mutex<Option<Arc<Notify>>>,
}

impl StompBroker {
    /// Connects to the STOMP listener at `addr` (`host:port`),
    /// anonymous (no login/passcode headers).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            shared: Arc::new(Shared {
                addr: addr.into(),
                subscriptions: Mutex::new(HashMap::new()),
                frames: Mutex::new(None),
                connected: Mutex::new(false),
                next_subscription_id: AtomicU64::new(0),
                generation: AtomicU64::new(0),
            }),
            shutdown: Mutex::new(None),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: StompSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Ok(Self::new(settings.addr))
    }
}

/// The bootstrap factory's settings wire shape for
/// [`StompBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct StompSettings {
    /// The STOMP listener address as `host:port`.
    pub addr: String,
}

/// Encodes one STOMP frame: command, headers, optional body and the
/// terminating NUL.
fn encode_frame(command: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(command.as_bytes());
    frame.push(b'\n');
    for (name, value) in headers {
        frame.extend_from_slice(name.as_bytes());
        frame.push(b':');
        frame.extend_from_slice(value.as_bytes());
        frame.push(b'\n');
    }
    frame.push(b'\n');
    frame.extend_from_slice(body);
    frame.push(0);
    frame
}

/// A parsed inbound frame: the command, its headers, and the body.
struct Frame {
    command: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// Reads one STOMP frame from the stream: headers to the blank line,
/// body either by `content-length` or to the terminating NUL.
async fn read_frame(reader: &mut OwnedReadHalf) -> Result<Frame, BrokerError> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();
    loop {
        reader
            .read_exact(&mut byte)
            .await
            .map_err(|e| BrokerError::Failed(format!("stomp read: {e}")))?;
        buffer.push(byte[0]);
        if buffer.ends_with(b"\n\n") {
            break;
        }
        if buffer.len() > 64 * 1024 {
            return Err(BrokerError::Failed(
                "stomp frame headers too large".to_string(),
            ));
        }
    }
    let header_block = String::from_utf8_lossy(&buffer[..buffer.len() - 2]).to_string();
    let mut lines = header_block.split('\n');
    let command = lines.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    let mut body = Vec::new();
    match headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.parse::<usize>().ok())
    {
        Some(length) => {
            body.resize(length, 0);
            reader
                .read_exact(&mut body)
                .await
                .map_err(|e| BrokerError::Failed(format!("stomp read: {e}")))?;
            reader
                .read_exact(&mut byte)
                .await
                .map_err(|e| BrokerError::Failed(format!("stomp read: {e}")))?;
        }
        None => loop {
            reader
                .read_exact(&mut byte)
                .await
                .map_err(|e| BrokerError::Failed(format!("stomp read: {e}")))?;
            if byte[0] == 0 {
                break;
            }
            body.push(byte[0]);
        },
    }
    Ok(Frame {
        command,
        headers,
        body,
    })
}

async fn write_frame(writer: &mut OwnedWriteHalf, frame: &[u8]) -> Result<(), BrokerError> {
    writer
        .write_all(frame)
        .await
        .map_err(|e| BrokerError::Failed(format!("stomp write: {e}")))
}

/// The permanent connection task: dials, handshakes, re-sends every
/// live subscription, then multiplexes outbound frames with inbound
/// MESSAGE routing. A broken connection re-dials with a doubling
/// backoff capped at thirty seconds; a superseded generation exits.
async fn connection_loop(
    shared: Arc<Shared>,
    shutdown: Arc<Notify>,
    generation: u64,
    mut frames_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut backoff_secs = 1u64;
    loop {
        if shared.generation.load(Ordering::SeqCst) != generation {
            return;
        }
        // Dial and perform the CONNECT handshake.
        let Ok(stream) = TcpStream::connect(&shared.addr).await else {
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
            continue;
        };
        let (mut reader, mut writer) = stream.into_split();
        let connect_frame = encode_frame(
            "CONNECT",
            &[
                ("accept-version", "1.2".to_string()),
                ("host", "/".to_string()),
            ],
            &[],
        );
        if write_frame(&mut writer, &connect_frame).await.is_err() {
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
            continue;
        }
        match read_frame(&mut reader).await {
            Ok(frame) if frame.command == "CONNECTED" => {}
            _ => {
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(30);
                continue;
            }
        }

        // Live: re-send every subscription, then multiplex.
        {
            let subscriptions = shared.subscriptions.lock().await.clone();
            for (id, (topic, _handler)) in &subscriptions {
                let frame = encode_frame(
                    "SUBSCRIBE",
                    &[
                        ("id", id.clone()),
                        ("destination", format!("/topic/{topic}")),
                        ("ack", "auto".to_string()),
                    ],
                    &[],
                );
                let _ = write_frame(&mut writer, &frame).await;
            }
            *shared.connected.lock().await = true;
        }

        loop {
            if shared.generation.load(Ordering::SeqCst) != generation {
                let _ = write_frame(&mut writer, &encode_frame("DISCONNECT", &[], &[])).await;
                return;
            }
            let broken = tokio::select! {
                _ = shutdown.notified() => {
                    let _ = write_frame(
                        &mut writer,
                        &encode_frame("DISCONNECT", &[], &[]),
                    )
                    .await;
                    return;
                }
                frame = frames_rx.recv() => match frame {
                    Some(frame) => write_frame(&mut writer, &frame).await.is_err(),
                    None => return,
                },
                read = read_frame(&mut reader) => match read {
                    Ok(frame) => {
                        route_message(&shared, frame).await;
                        false
                    }
                    Err(_) => true,
                },
            };
            if broken {
                break;
            }
            backoff_secs = 1;
        }
        *shared.connected.lock().await = false;
        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}

/// Routes an inbound MESSAGE frame to its subscription's handler —
/// the destination is the event topic, the body the payload.
async fn route_message(shared: &Shared, frame: Frame) {
    if frame.command != "MESSAGE" {
        return;
    }
    let Some(subscription_id) = frame
        .headers
        .iter()
        .find(|(name, _)| name == "subscription")
        .map(|(_, value)| value.clone())
    else {
        return;
    };
    // The event topic is the subscription's original topic, not the
    // full /topic/… destination the server echoes back.
    let found = async {
        let subscriptions = shared.subscriptions.lock().await;
        subscriptions.get(&subscription_id).cloned()
    };
    let Some((topic, handler)) = found.await else {
        return;
    };
    let event = Event::new(topic, Message::from_payload(frame.body));
    tokio::spawn(handler(event));
}

impl Broker for StompBroker {
    fn name(&self) -> &'static str {
        "stomp"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Retire the previous connection generation.
            if let Some(previous) = self.shutdown.lock().await.take() {
                previous.notify_waiters();
            }
            let generation = self
                .shared
                .generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let shutdown = Arc::new(Notify::new());
            *self.shutdown.lock().await = Some(Arc::clone(&shutdown));

            let (frames_tx, frames_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            *self.shared.frames.lock().await = Some(frames_tx.clone());

            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                connection_loop(shared, shutdown, generation, frames_rx).await;
            });

            // The task's first handshake drives the connect; wait for
            // it so callers can publish straight away.
            for _ in 0..100 {
                if *self.shared.connected.lock().await {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(BrokerError::Failed("stomp connect timed out".to_string()))
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            *self.shared.connected.lock().await = false;
            // Orderly teardown, upstream's ordering (UNSUBSCRIBE frames
            // need a live connection — they must go out before the
            // DISCONNECT, not after it): UNSUBSCRIBE for every live
            // subscription, the DISCONNECT frame, then drop the frame
            // sender, whose closure ends the connection task and the
            // socket. The subscriptions map survives so the next
            // `connect` replays them after its handshake. A double
            // disconnect finds no sender and is a no-op.
            let frames = self.shared.frames.lock().await.clone();
            if let Some(frames) = frames {
                for id in self.shared.subscriptions.lock().await.keys() {
                    let frame = encode_frame("UNSUBSCRIBE", &[("id", id.clone())], &[]);
                    let _ = frames.send(frame);
                }
                let _ = frames.send(encode_frame("DISCONNECT", &[], &[]));
                *self.shared.frames.lock().await = None;
            }
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            let Some(frames) = self.shared.frames.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            if !*self.shared.connected.lock().await {
                return Err(BrokerError::NotConnected);
            }
            let frame = encode_frame(
                "SEND",
                &[
                    ("destination", format!("/topic/{topic}")),
                    ("content-length", message.payload.len().to_string()),
                ],
                &message.payload,
            );
            frames.send(frame).map_err(|_| BrokerError::NotConnected)
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            let subscription_id = format!(
                "sub-{}",
                self.shared
                    .next_subscription_id
                    .fetch_add(1, Ordering::SeqCst)
            );
            let destination = format!("/topic/{topic}");
            self.shared.subscriptions.lock().await.insert(
                subscription_id.clone(),
                (topic.to_string(), Arc::clone(&handler)),
            );
            if let Some(frames) = self.shared.frames.lock().await.clone() {
                let frame = encode_frame(
                    "SUBSCRIBE",
                    &[
                        ("id", subscription_id.clone()),
                        ("destination", destination.clone()),
                        ("ack", "auto".to_string()),
                    ],
                    &[],
                );
                let _ = frames.send(frame);
            }
            Ok(Box::new(StompSubscriber {
                shared: Arc::clone(&self.shared),
                topic: topic.to_string(),
                subscription_id,
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The STOMP subscription handle: the id is the routing key from
/// MESSAGE frames back to this subscription's handler.
struct StompSubscriber {
    shared: Arc<Shared>,
    topic: String,
    subscription_id: String,
}

impl Subscriber for StompSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.shared
                .subscriptions
                .lock()
                .await
                .remove(&self.subscription_id);
            if let Some(frames) = self.shared.frames.lock().await.clone() {
                let frame =
                    encode_frame("UNSUBSCRIBE", &[("id", self.subscription_id.clone())], &[]);
                let _ = frames.send(frame);
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> StompBroker {
        StompBroker::new("127.0.0.1:61613")
    }

    fn sink() -> Handler {
        let (tx, _rx) = mpsc::unbounded_channel::<Event>();
        Arc::new(move |event: Event| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(event);
                Ok::<(), BrokerError>(())
            }) as BoxFuture<'static, Result<(), BrokerError>>
        })
    }

    /// Publishing without a connection fails with NotConnected — a
    /// clear error, never a panic on a stale handle.
    #[tokio::test]
    async fn publish_before_connect_is_not_connected() {
        let broker = broker();
        let result = broker
            .publish("probe/never", Message::from_payload(vec![]))
            .await;
        assert!(
            matches!(result, Err(BrokerError::NotConnected)),
            "unconnected publish must fail with NotConnected, got {result:?}"
        );
    }

    /// Disconnect is idempotent — including before any connect — and
    /// afterwards publishes still fail with NotConnected.
    #[tokio::test]
    async fn disconnect_is_idempotent() {
        let broker = broker();
        broker.disconnect().await.expect("first disconnect");
        broker.disconnect().await.expect("second disconnect");
        let result = broker
            .publish("probe/never", Message::from_payload(vec![]))
            .await;
        assert!(matches!(result, Err(BrokerError::NotConnected)));
    }

    /// Subscribing while disconnected defers the SUBSCRIBE to the next
    /// connect (the registration survives for the reconnect replay),
    /// and the handle's unsubscribe tears the deferred registration
    /// down without hanging.
    #[tokio::test]
    async fn subscribe_before_connect_defers_and_unsubscribes() {
        let broker = broker();
        let mut subscriber = broker
            .subscribe("probe/deferred", sink())
            .await
            .expect("offline subscribe defers the registration");
        assert_eq!(subscriber.topic(), "probe/deferred");
        assert_eq!(
            broker.shared.subscriptions.lock().await.len(),
            1,
            "the deferred registration is kept for the next connect"
        );
        subscriber
            .unsubscribe()
            .await
            .expect("offline unsubscribe removes the registration");
        assert!(
            broker.shared.subscriptions.lock().await.is_empty(),
            "unsubscribe tears the deferred registration down"
        );
    }
}

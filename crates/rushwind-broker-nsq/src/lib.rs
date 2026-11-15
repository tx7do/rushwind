//! NSQ engine for the RushWind broker contract — the nsqd binary TCP
//! protocol (version 2) hand-written over tokio, no SDK (the only Rust
//! client is unmaintained since 2017; the protocol is small enough to
//! speak directly, the house's STOMP precedent).
//!
//! # The mapping
//!
//! - A contract **topic** is an nsq topic; the engine subscribes with
//!   a fixed channel ([`NsqOptions::channel_id`], default `rushwind`)
//!   — every subscribe to the same contract topic shares that channel,
//!   nsq's own load-balanced fan-in.
//! - **Publish** rides `PUB <topic>` with the framed body; headers ride
//!   nsq's JSON body convention — the engine wraps the payload in
//!   `{"payload": base64, "headers": {...}}` so the wire shape carries
//!   the contract's metadata (nsq has no attribute surface of its own).
//! - **Deliveries** arrive as `SUB` frames once `RDY` grants flow; the
//!   message id (16 bytes) drives settlement. [`AckMode::Auto`] (the
//!   default) sends `FIN` the moment a delivery is dispatched — nsq
//!   redelivers only un-`FIN`ed messages past their requeue policy, so
//!   settling on dispatch means never redelivering. [`AckMode::Manual`]
//!   hands the native settlement to the handler: the ack hook sends
//!   `FIN` (settled), the nack hook sends `REQ <id> 0` — a zero-delay
//!   requeue, nsq's own immediate redelivery, for
//!   [`NackAction::Requeue`]; for [`NackAction::Drop`] it sends `FIN`
//!   (settled away).
//! - **Settlement commands** ride an mpsc channel back to the pump,
//!   which owns the socket's write half — the handler's hooks are
//!   plain channel sends, the pump serializes them onto the wire.
//! - Heartbeats are disabled at IDENTIFY (`heartbeat_interval: -1`) —
//!   the pump's select over frames and settlement commands stays
//!   simple, and nsqd's TCP keepalive covers liveness.
//! - Reconnects replay `SUB` + `RDY`; un-`FIN`ed messages redeliver
//!   server-side after the connection drops (nsq's in-flight policy).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Notify};

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct NsqOptions {
    /// The nsqd TCP address (`host:4150`).
    pub addr: String,
    /// The subscription channel every subscribe uses. Default
    /// `rushwind`.
    pub channel_id: String,
    /// The acknowledgment policy. Default [`AckMode::Auto`].
    pub ack_mode: AckMode,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for NsqOptions {
    fn default() -> Self {
        Self {
            addr: String::new(),
            channel_id: "rushwind".to_string(),
            ack_mode: AckMode::Auto,
            request_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
struct Inner {
    options: NsqOptions,
    closed: AtomicBool,
    /// topic → shared state (the pump's shutdown + the settle channel
    /// the subscribe handler hands out).
    subscriptions: Mutex<HashMap<String, Arc<SubscriptionShared>>>,
}

#[derive(Debug)]
struct SubscriptionShared {
    done: Arc<Notify>,
}

/// The NSQ broker.
#[derive(Debug)]
pub struct NsqBroker {
    inner: Arc<Inner>,
}

impl NsqBroker {
    /// Builds the broker from the options. Fails when the address is
    /// unset.
    pub fn new(options: NsqOptions) -> Result<Self, BrokerError> {
        if options.addr.is_empty() {
            return Err(BrokerError::Failed("nsq: addr unset".to_string()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                closed: AtomicBool::new(false),
                subscriptions: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`addr`, `channel_id`, `ack_mode`), unknown fields
    /// ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            addr: String,
            channel_id: Option<String>,
            ack_mode: Option<AckMode>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::new(NsqOptions {
            addr: wire.addr,
            channel_id: wire.channel_id.unwrap_or_else(|| "rushwind".to_string()),
            ack_mode: wire.ack_mode.unwrap_or_default(),
            request_timeout: Duration::from_secs(10),
        })
    }
}

impl Broker for NsqBroker {
    fn name(&self) -> &'static str {
        "nsq"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // Connectionless: the pump dials per subscription task. Connect
        // is the lifecycle's live mark.
        Box::pin(async { Ok(()) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .closed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let subscriptions = self.inner.subscriptions.lock().await;
            for shared in subscriptions.values() {
                shared.done.notify_waiters();
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
            if self.inner.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            // The envelope: payload base64, headers in the map.
            let envelope = json_envelope(&message);
            let body = envelope.into_bytes();
            let frame = pub_frame(topic, &body);
            self.send_frame(&frame, "publish").await
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if self.inner.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let done = Arc::new(Notify::new());
            let shared = Arc::new(SubscriptionShared { done: done.clone() });
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
            }

            let engine = NsqBroker {
                inner: Arc::clone(&self.inner),
            };
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let task_done = done.clone();
            let ack_mode = self.inner.options.ack_mode;
            let channel = self.inner.options.channel_id.clone();
            tokio::spawn(async move {
                engine
                    .run_pump(&topic_for_task, &channel, handler, ack_mode, task_done)
                    .await;
            });

            Ok(Box::new(NsqSubscriber {
                topic: topic.to_string(),
                done,
            }) as Box<dyn Subscriber>)
        })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        let _ = (topic, message);
        Box::pin(async {
            Err(BrokerError::Failed(
                "request not implemented by this engine".to_string(),
            ))
        })
    }
}

/// The subscriber handle: dropping it (or unsubscribing) stops the
/// pump.
struct NsqSubscriber {
    topic: String,
    done: Arc<Notify>,
}

impl Subscriber for NsqSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        self.done.notify_waiters();
        Box::pin(async { Ok(()) })
    }
}

impl NsqBroker {
    /// Sends one prebuilt frame to nsqd (a short-lived dial per frame —
    /// the publish face; the subscribe face owns a long-lived socket in
    /// the pump).
    async fn send_frame(&self, frame: &[u8], context: &str) -> Result<(), BrokerError> {
        let mut socket = tokio::time::timeout(
            self.inner.options.request_timeout,
            TcpStream::connect(&self.inner.options.addr),
        )
        .await
        .map_err(|_| BrokerError::Failed(format!("nsq {context}: connect timeout")))?
        .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        // Speak the protocol version magic first.
        socket
            .write_all(NSQ_MAGIC)
            .await
            .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        socket
            .write_all(frame)
            .await
            .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        // Read the response frame: [4-byte size][4-byte type][payload].
        let mut size_buf = [0u8; 4];
        tokio::time::timeout(
            self.inner.options.request_timeout,
            socket.read_exact(&mut size_buf),
        )
        .await
        .map_err(|_| BrokerError::Failed(format!("nsq {context}: response timeout")))?
        .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        let size = u32::from_be_bytes(size_buf) as usize;
        if size < 4 {
            return Err(BrokerError::Failed(format!(
                "nsq {context}: frame too short"
            )));
        }
        // Skip the 4-byte frame type, then the payload.
        let mut type_buf = [0u8; 4];
        socket
            .read_exact(&mut type_buf)
            .await
            .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        let mut payload = vec![0u8; size - 4];
        tokio::time::timeout(
            self.inner.options.request_timeout,
            socket.read_exact(&mut payload),
        )
        .await
        .map_err(|_| BrokerError::Failed(format!("nsq {context}: response timeout")))?
        .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        if payload.first() == Some(&b'+') {
            Ok(())
        } else {
            Err(BrokerError::Failed(format!(
                "nsq {context}: {}",
                String::from_utf8_lossy(&payload)
            )))
        }
    }

    /// The subscribe pump: dial, handshake (magic + IDENTIFY with
    /// heartbeats disabled + SUB + RDY), then select over inbound
    /// frames, settle commands, and the shutdown notification.
    async fn run_pump(
        &self,
        topic: &str,
        channel: &str,
        handler: Handler,
        ack_mode: AckMode,
        task_done: Arc<Notify>,
    ) {
        loop {
            if self.inner.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let backoff_secs = 1u64;
            match self
                .dial_pump(topic, channel, &handler, ack_mode, &task_done)
                .await
            {
                PumpOutcome::Closed => return,
                PumpOutcome::Broken => {
                    // Backoff, then retry the whole dial; un-acked
                    // messages redeliver server-side.
                    let _ = tokio::time::timeout(
                        Duration::from_secs(backoff_secs),
                        task_done.notified(),
                    )
                    .await;
                }
            }
        }
    }

    async fn dial_pump(
        &self,
        topic: &str,
        channel: &str,
        handler: &Handler,
        ack_mode: AckMode,
        task_done: &Arc<Notify>,
    ) -> PumpOutcome {
        let connect = async {
            let mut socket = TcpStream::connect(&self.inner.options.addr)
                .await
                .map_err(|e| BrokerError::Failed(format!("nsq connect: {e}")))?;
            socket
                .write_all(NSQ_MAGIC)
                .await
                .map_err(|e| BrokerError::Failed(format!("nsq magic: {e}")))?;
            // IDENTIFY with heartbeats disabled.
            let identify = identify_frame();
            socket
                .write_all(&identify)
                .await
                .map_err(|e| BrokerError::Failed(format!("nsq identify: {e}")))?;
            expect_ok(&mut socket).await?;
            // SUB topic channel.
            socket
                .write_all(format!("SUB {topic} {channel}\n").as_bytes())
                .await
                .map_err(|e| BrokerError::Failed(format!("nsq sub: {e}")))?;
            expect_ok(&mut socket).await?;
            socket
                .write_all(b"RDY 64\n")
                .await
                .map_err(|e| BrokerError::Failed(format!("nsq rdy: {e}")))?;
            Ok::<TcpStream, BrokerError>(socket)
        };
        let socket = match connect.await {
            Ok(socket) => socket,
            Err(_) => return PumpOutcome::Broken,
        };

        let (reader, mut writer) = socket.into_split();
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        // The writer task: serializes outbound command frames.
        let writer_task = tokio::spawn(async move {
            while let Some(frame) = outbound_rx.recv().await {
                if writer.write_all(&frame).await.is_err() {
                    return;
                }
            }
        });

        let mut reader = reader;
        loop {
            let frame = tokio::select! {
                _ = task_done.notified() => {
                    let _ = outbound_tx.send(closing_frame());
                    return PumpOutcome::Closed;
                }
                read = read_frame(&mut reader) => match read {
                    Ok(frame) => frame,
                    Err(_) => {
                        // Broken: drop the writer task and retry the dial.
                        let _ = outbound_tx.send(Vec::new());
                        drop(outbound_tx);
                        writer_task.abort();
                        return PumpOutcome::Broken;
                    }
                },
            };
            match frame {
                NsqFrame::Message { id, payload } => {
                    let message = json_de_envelope(&payload);
                    match ack_mode {
                        AckMode::Auto => {
                            let event = Event::new(topic.to_string(), message);
                            tokio::spawn(handler(event));
                            let _ = outbound_tx.send(fin_frame(&id));
                        }
                        AckMode::Manual => {
                            let fin_tx = outbound_tx.clone();
                            let fin_id = id.clone();
                            let req_tx = outbound_tx.clone();
                            let req_id = id.clone();
                            let event = Event::settle_with(
                                topic.to_string(),
                                message,
                                move || {
                                    let tx = fin_tx.clone();
                                    let id = fin_id.clone();
                                    Box::pin(async move {
                                        tx.send(fin_frame(&id)).map_err(|e| {
                                            BrokerError::Failed(format!("nsq settle: {e}"))
                                        })
                                    })
                                        as BoxFuture<'static, Result<(), BrokerError>>
                                },
                                move |action| {
                                    let tx = req_tx.clone();
                                    let id = req_id.clone();
                                    Box::pin(async move {
                                        let command = match action {
                                            // Zero-delay requeue: nsq's own
                                            // immediate redelivery.
                                            NackAction::Requeue => requeue_frame(&id),
                                            // Settled away — acknowledged.
                                            NackAction::Drop => fin_frame(&id),
                                        };
                                        tx.send(command).map_err(|e| {
                                            BrokerError::Failed(format!("nsq settle: {e}"))
                                        })
                                    })
                                        as BoxFuture<'static, Result<(), BrokerError>>
                                },
                            );
                            tokio::spawn(handler(event));
                        }
                    }
                }
                // Responses and errors: a non-+OK response breaks the
                // pump (the error carries the reason server-side).
                NsqFrame::Response(payload) if payload.starts_with(b"+OK") => {}
                NsqFrame::Response(_) => {}
                NsqFrame::Error(payload) => {
                    let _ = payload;
                    let _ = outbound_tx.send(Vec::new());
                    drop(outbound_tx);
                    writer_task.abort();
                    return PumpOutcome::Broken;
                }
            }
        }
    }
}

/// The pump's exit reasons.
enum PumpOutcome {
    /// The subscription (or engine) is done: end the pump for good.
    Closed,
    /// The connection broke: retry the dial after backoff.
    Broken,
}

/// The protocol magic a client sends on connect.
const NSQ_MAGIC: &[u8] = b"  V2";

/// The envelope's payload key.
const PAYLOAD_KEY: &str = "payload";
/// The envelope's headers key.
const HEADERS_KEY: &str = "headers";

/// The JSON envelope builder: payload base64, headers as a map.
fn json_envelope(message: &Message) -> String {
    let headers: serde_json::Map<String, serde_json::Value> = message
        .headers
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect();
    serde_json::json!({
        PAYLOAD_KEY: base64_of(&message.payload),
        HEADERS_KEY: headers,
    })
    .to_string()
}

/// Decodes the envelope back into a contract message.
fn json_de_envelope(payload: &[u8]) -> Message {
    let parsed: serde_json::Value =
        serde_json::from_slice(payload).unwrap_or(serde_json::Value::Null);
    let mut message = Message::from_payload(de64_of(
        parsed
            .get(PAYLOAD_KEY)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default(),
    ));
    if let Some(headers) = parsed
        .get(HEADERS_KEY)
        .and_then(serde_json::Value::as_object)
    {
        for (key, value) in headers {
            if let Some(value) = value.as_str() {
                message.headers.insert(key.clone(), value.to_string());
            }
        }
    }
    message
}

/// The three wire faces the engine uses.
fn base64_of(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn de64_of(text: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .unwrap_or_default()
}

/// The `IDENTIFY` frame with heartbeats disabled.
fn identify_frame() -> Vec<u8> {
    let body = serde_json::json!({"heartbeat_interval": -1, "client_id": "rushwind"})
        .to_string()
        .into_bytes();
    command_with_body("IDENTIFY", &[], &body)
}

/// `PUB <topic>` with a framed body.
fn pub_frame(topic: &str, body: &[u8]) -> Vec<u8> {
    command_with_body(&format!("PUB {topic}"), &[], body)
}

/// `FIN <message-id>`.
fn fin_frame(id: &str) -> Vec<u8> {
    format!("FIN {id}\n").into_bytes()
}

/// `REQ <message-id> 0` — a zero-delay requeue.
fn requeue_frame(id: &str) -> Vec<u8> {
    format!("REQ {id} 0\n").into_bytes()
}

/// `CLS` — the graceful close command.
fn closing_frame() -> Vec<u8> {
    b"CLS\n".to_vec()
}

/// A command with an optional inline body: `<command>\n` + (4-byte
/// length + body when a body exists).
fn command_with_body(command: &str, _query: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut frame = format!("{command}\n").into_bytes();
    if !body.is_empty() {
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(body);
    }
    frame
}

/// A decoded inbound frame.
enum NsqFrame {
    Response(Vec<u8>),
    Error(Vec<u8>),
    Message {
        /// The 16-byte message id, hex-encoded — the settlement key
        /// FIN/REQ speak.
        id: String,
        payload: Vec<u8>,
    },
}

/// Reads one inbound frame: 4-byte size, 4-byte frame type, payload.
async fn read_frame(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<NsqFrame, BrokerError> {
    let mut header = [0u8; 8];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|e| BrokerError::Failed(format!("nsq frame: {e}")))?;
    let size = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let frame_type = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    let mut payload = vec![0u8; size.saturating_sub(4)];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| BrokerError::Failed(format!("nsq frame: {e}")))?;
    match frame_type {
        0 => Ok(NsqFrame::Response(payload)),
        1 => Ok(NsqFrame::Error(payload)),
        2 => {
            // [8-byte timestamp][2-byte attempts][16-byte id][body]
            if payload.len() < 26 {
                return Err(BrokerError::Failed(
                    "nsq frame: message payload too short".to_string(),
                ));
            }
            let id = hex_encode(&payload[10..26]);
            Ok(NsqFrame::Message {
                id,
                payload: payload[26..].to_vec(),
            })
        }
        _ => Err(BrokerError::Failed(format!(
            "nsq frame: unknown frame type {frame_type}"
        ))),
    }
}

/// Reads the +OK response after a client command.
async fn expect_ok(socket: &mut TcpStream) -> Result<(), BrokerError> {
    let mut size_buf = [0u8; 4];
    socket
        .read_exact(&mut size_buf)
        .await
        .map_err(|e| BrokerError::Failed(format!("nsq handshake: {e}")))?;
    let size = u32::from_be_bytes(size_buf) as usize;
    if size < 4 {
        return Err(BrokerError::Failed(
            "nsq handshake: frame too short".to_string(),
        ));
    }
    // Skip the 4-byte frame type, then the payload.
    let mut type_buf = [0u8; 4];
    socket
        .read_exact(&mut type_buf)
        .await
        .map_err(|e| BrokerError::Failed(format!("nsq handshake: {e}")))?;
    let mut payload = vec![0u8; size - 4];
    socket
        .read_exact(&mut payload)
        .await
        .map_err(|e| BrokerError::Failed(format!("nsq handshake: {e}")))?;
    if payload.first() == Some(&b'+') {
        Ok(())
    } else {
        Err(BrokerError::Failed(format!(
            "nsq handshake: {}",
            String::from_utf8_lossy(&payload)
        )))
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|byte| format!("{byte:02x}")).collect()
}

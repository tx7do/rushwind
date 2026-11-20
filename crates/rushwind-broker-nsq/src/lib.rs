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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The idle-tick spacing: heartbeats are disabled at IDENTIFY, so a
/// quiet topic sends no frames and the stop flags would otherwise go
/// unread for as long as the broker stays silent.
const IDLE_CHECK: Duration = Duration::from_millis(500);

use rushwind_broker::{
    AckMode, BoxFuture, Broker, BrokerError, Event, Handler, Message, NackAction, Subscriber,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

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

/// One subscription's stop face. A checked flag — read at the pump's
/// loop top, after the dial, and before every dispatch — not a wakeup:
/// a notify-style signal landing between the pump's await points
/// (the whole dial/handshake window, the frame read, a settle flush)
/// would be lost, and the pump would keep consuming for a
/// subscription that was unsubscribed.
#[derive(Debug)]
struct SubscriptionShared {
    stopped: AtomicBool,
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
            self.inner.closed.store(true, Ordering::SeqCst);
            let subscriptions = self.inner.subscriptions.lock().await;
            for shared in subscriptions.values() {
                shared.stopped.store(true, Ordering::SeqCst);
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
            if self.inner.closed.load(Ordering::SeqCst) {
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
            let shared = Arc::new(SubscriptionShared {
                stopped: AtomicBool::new(false),
            });
            {
                let mut subscriptions = self.inner.subscriptions.lock().await;
                // A duplicate subscribe retires the old pump — the
                // rabbitmq/redis engines' same-topic shape: the newest
                // handler owns the filter, and the previous pump exits at
                // its next check instead of leaking past the overwritten
                // registry entry.
                if let Some(previous) = subscriptions.get(topic) {
                    previous.stopped.store(true, Ordering::SeqCst);
                }
                subscriptions.insert(topic.to_string(), Arc::clone(&shared));
            }

            let shared_for_handle = Arc::clone(&shared);
            let engine = NsqBroker {
                inner: Arc::clone(&self.inner),
            };
            let topic_for_task = topic.to_string();
            let handler = Arc::clone(&handler);
            let ack_mode = self.inner.options.ack_mode;
            let channel = self.inner.options.channel_id.clone();
            tokio::spawn(async move {
                engine
                    .run_pump(&topic_for_task, &channel, handler, ack_mode, shared)
                    .await;
            });

            Ok(Box::new(NsqSubscriber {
                topic: topic.to_string(),
                shared: shared_for_handle,
                inner: Arc::clone(&self.inner),
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

/// The subscription handle. `unsubscribe` stops the pump
/// authoritatively; dropping it stops the same pump best-effort — the
/// registry's lock may be contended at drop time, in which case only
/// the explicit call is guaranteed.
struct NsqSubscriber {
    topic: String,
    shared: Arc<SubscriptionShared>,
    inner: Arc<Inner>,
}

impl NsqSubscriber {
    /// Stops the pump this handle owns, if it still owns the registry
    /// entry: a duplicate subscribe replaces the entry, and a stale
    /// handle's teardown must then touch nothing — stopping the
    /// replacement's pump would orphan its deliveries.
    async fn stop_owned(&self) {
        let mut subscriptions = self.inner.subscriptions.lock().await;
        let owned = subscriptions
            .get(&self.topic)
            .map(|current| Arc::ptr_eq(current, &self.shared))
            .unwrap_or(false);
        if owned {
            self.shared.stopped.store(true, Ordering::SeqCst);
            subscriptions.remove(&self.topic);
        }
    }
}

impl Subscriber for NsqSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.stop_owned().await;
            Ok(())
        })
    }
}

impl Drop for NsqSubscriber {
    fn drop(&mut self) {
        // The best-effort half of the stop contract: try_lock fails
        // under an active holder, and only the explicit unsubscribe
        // guarantees the stop.
        if let Ok(mut subscriptions) = self.inner.subscriptions.try_lock() {
            let owned = subscriptions
                .get(&self.topic)
                .map(|current| Arc::ptr_eq(current, &self.shared))
                .unwrap_or(false);
            if owned {
                self.shared.stopped.store(true, Ordering::SeqCst);
                subscriptions.remove(&self.topic);
            }
        }
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
        // Skip the 4-byte frame type, then the payload. Both reads ride
        // the same timeout as their neighbors — a server stopping
        // mid-frame must not hang the publish forever.
        let mut type_buf = [0u8; 4];
        tokio::time::timeout(
            self.inner.options.request_timeout,
            socket.read_exact(&mut type_buf),
        )
        .await
        .map_err(|_| BrokerError::Failed(format!("nsq {context}: response timeout")))?
        .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        let mut payload = vec![0u8; size - 4];
        tokio::time::timeout(
            self.inner.options.request_timeout,
            socket.read_exact(&mut payload),
        )
        .await
        .map_err(|_| BrokerError::Failed(format!("nsq {context}: response timeout")))?
        .map_err(|e| BrokerError::Failed(format!("nsq {context}: {e}")))?;
        // Heartbeats skip (nsqd's timer fires on its own schedule and
        // one can land between the command and its OK); the real
        // nsqd's success body is plain "OK".
        if payload == b"_heartbeat_" {
            return Ok(());
        }
        if payload.starts_with(b"OK") {
            return Ok(());
        }
        Err(BrokerError::Failed(format!(
            "nsq {context}: {}",
            String::from_utf8_lossy(&payload)
        )))
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
        shared: Arc<SubscriptionShared>,
    ) {
        let stopped =
            || self.inner.closed.load(Ordering::SeqCst) || shared.stopped.load(Ordering::SeqCst);
        loop {
            if stopped() {
                return;
            }
            match self
                .dial_pump(topic, channel, &handler, ack_mode, &shared)
                .await
            {
                PumpOutcome::Closed => return,
                PumpOutcome::Broken => {
                    // Backoff, then retry the whole dial; un-acked
                    // messages redeliver server-side. The sleep stays
                    // checkable: ten short sleeps with a flag check
                    // between each, so a stop landing during the backoff
                    // takes effect within a tenth of a second instead of
                    // waiting out a lost notification.
                    for _ in 0..10 {
                        if stopped() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
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
        shared: &SubscriptionShared,
    ) -> PumpOutcome {
        let stopped =
            || self.inner.closed.load(Ordering::SeqCst) || shared.stopped.load(Ordering::SeqCst);
        let connect = async {
            let mut socket = tokio::time::timeout(
                self.inner.options.request_timeout,
                TcpStream::connect(&self.inner.options.addr),
            )
            .await
            .map_err(|_| BrokerError::Failed("nsq connect: timeout".to_string()))?
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
        // The stop may have landed during the dial — the whole
        // handshake is a notify-loss window unless the flag is read
        // right here, before RDY opens the flow.
        if stopped() {
            return PumpOutcome::Closed;
        }

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

        // The reader task: complete frames only, pushed to the pump's
        // channel. The pump's wait is a channel recv (cancellation-safe)
        // — no read is ever dropped mid-frame with its bytes.
        let (frames_tx, mut frames_rx) = mpsc::unbounded_channel();
        let mut reader = reader;
        let reader_task = tokio::spawn(async move {
            loop {
                match read_frame(&mut reader).await {
                    Ok(frame) => {
                        if frames_tx.send(Ok(frame)).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = frames_tx.send(Err(e));
                        return;
                    }
                }
            }
        });

        loop {
            if stopped() {
                // CLS rides the writer best-effort; un-acked messages
                // redeliver server-side either way (the socket closes
                // with this pump).
                let _ = outbound_tx.send(closing_frame());
                drop(outbound_tx);
                reader_task.abort();
                return PumpOutcome::Closed;
            }
            let frame = tokio::select! {
                frame = frames_rx.recv() => match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) | None => {
                        // Broken: the socket died — drop the writer and
                        // retry the dial.
                        drop(outbound_tx);
                        writer_task.abort();
                        return PumpOutcome::Broken;
                    }
                },
                // The idle tick: a quiet topic (heartbeats disabled)
                // sends no frames, so the stop flags would otherwise go
                // unread for as long as the broker stays silent.
                _ = tokio::time::sleep(IDLE_CHECK) => continue,
            };
            match frame {
                NsqFrame::Message { id, payload } => {
                    // The delivery-face stop: a message already read when
                    // the stop lands is dropped without dispatch — no FIN
                    // is sent, so nsqd redelivers it to whoever receives
                    // next after the in-flight timeout.
                    if stopped() {
                        continue;
                    }
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
                // Responses and errors: the real nsqd's success body
                // is plain "OK" (no plus prefix — that spelling was
                // the mock's invention, the live suite caught it).
                NsqFrame::Response(payload) if payload.starts_with(b"OK") => {}
                NsqFrame::Response(_) => {}
                NsqFrame::Error(_payload) => {
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
    // The heartbeat interval's honest zero: a negative value requests
    // "no heartbeats" in a way the real nsqd refuses at SUB time
    // (E_INVALID "cannot SUB with heartbeats disabled" — negative is
    // simply not a legal spelling), while zero legitimately disables
    // the server's heartbeat. The mock reproduces the refusal so the
    // offline pins carry the same handshake contract.
    let body = serde_json::json!({
        "heartbeat_interval": 0,
        "client_id": "rushwind",
    })
    .to_string()
    .into_bytes();
    command_with_body("IDENTIFY", &[], &body)
}

/// `PUB <topic>` with a framed body.
fn pub_frame(topic: &str, body: &[u8]) -> Vec<u8> {
    command_with_body(&format!("PUB {topic}"), &[], body)
}

/// `FIN <message-id>` — the id as nsqd minted it (16 ASCII text
/// characters carried raw in the message frame).
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
        /// The message id — nsqd's 16 ASCII text characters carried
        /// raw in the frame — the settlement key FIN/REQ speak
        /// verbatim.
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
            // The id's 16 bytes carry a 16-character ASCII-hex string
            // (nsqd mints them as text) — FIN/REQ echo that text
            // verbatim; a hex re-encode of the raw bytes spells 32
            // characters and the real nsqd answers E_INVALID
            // "invalid message ID" (a fatal error that kills the
            // connection — the live suite caught the mock's lenient
            // spelling).
            let id = String::from_utf8_lossy(&payload[10..26]).into_owned();
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

/// Reads the OK response after a client command (the real nsqd's
/// success body is plain "OK" — the live suite caught the mock's
/// invented "+OK" spelling). Heartbeat frames (`_heartbeat_`) skip:
/// nsqd's ioloop delivers them whenever its timer fires, and one can
/// land between a command and its OK.
async fn expect_ok(socket: &mut TcpStream) -> Result<(), BrokerError> {
    loop {
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
        if payload == b"_heartbeat_" {
            continue;
        }
        if payload.starts_with(b"OK") {
            return Ok(());
        }
        return Err(BrokerError::Failed(format!(
            "nsq handshake: {}",
            String::from_utf8_lossy(&payload)
        )));
    }
}

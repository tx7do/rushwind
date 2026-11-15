//! An in-process mock nsqd speaking the binary protocol subset the
//! engine uses — magic + IDENTIFY handshake (heartbeats recorded), SUB
//! per topic, RDY silent, PUB into an in-memory topic queue, message
//! frames pushed to live subscriber connections, FIN settles, REQ 0
//! re-delivers immediately.
//!
//! Connection structure: the socket is split — a reader task parses
//! commands and hands responses + registrations to a writer task; the
//! writer select-loops over client commands and the message frames the
//! test's `deliver()` broadcasts. One channel per connection; SUB
//! registers it.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Default)]
struct NsqMockInner {
    /// topic → queued contract bodies.
    queue: BTreeMap<String, VecDeque<Vec<u8>>>,
    /// topics with a live SUB.
    subscribed: BTreeSet<String>,
    /// hex id → in-flight body (REQ restores the body to the queue).
    in_flight: BTreeMap<String, Vec<u8>>,
    /// hex id → FIN count.
    fins: BTreeMap<String, i64>,
    /// hex id → REQ count.
    requeues: BTreeMap<String, i64>,
    last_identify_heartbeat: Option<i64>,
    next_id: u64,
    /// one frame channel per live subscriber connection.
    deliverers: Vec<UnboundedSender<Vec<u8>>>,
}

/// The shared mock handle: state access for the pins plus the
/// `deliver()` trigger.
pub struct NsqMock {
    inner: Mutex<NsqMockInner>,
}

impl Default for NsqMock {
    fn default() -> Self {
        Self {
            inner: Mutex::new(NsqMockInner::default()),
        }
    }
}

impl NsqMock {
    pub fn fin_count(&self, id: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .fins
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    pub fn requeue_count(&self, id: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .requeues
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    pub fn last_heartbeat(&self) -> Option<i64> {
        self.inner.lock().expect("mock").last_identify_heartbeat
    }

    /// Whether a SUB for `topic` has landed — the pins wait on this
    /// before delivering (the pump dials asynchronously after subscribe
    /// returns).
    pub fn subscribed(&self, topic: &str) -> bool {
        self.inner.lock().expect("mock").subscribed.contains(topic)
    }

    pub fn in_flight_ids(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("mock")
            .in_flight
            .keys()
            .cloned()
            .collect()
    }

    /// Plants a contract body into a topic's queue — wrapped in the
    /// JSON envelope the engine's publish face speaks (payload base64,
    /// headers map), so the pump's decode sees a real published frame.
    pub fn enqueue(&self, topic: &str, body: &str) {
        use base64::Engine as _;
        let mut inner = self.inner.lock().expect("mock");
        let envelope = serde_json::json!({
            "payload": base64::engine::general_purpose::STANDARD.encode(body.as_bytes()),
            "headers": {},
        })
        .to_string();
        inner
            .queue
            .entry(topic.to_string())
            .or_default()
            .push_back(envelope.into_bytes());
    }

    /// Drives the queue out to every live subscriber connection — the
    /// mock's push side of message delivery. Ids are minted here and
    /// recorded in-flight; REQ restores the body to the queue head,
    /// FIN removes the record.
    pub fn deliver(&self) {
        let mut inner = self.inner.lock().expect("mock");
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let subscribed_topics: Vec<String> = inner
            .queue
            .keys()
            .filter(|topic| inner.subscribed.contains(*topic))
            .cloned()
            .collect();
        for topic in &subscribed_topics {
            let mut drained: Vec<Vec<u8>> = Vec::new();
            if let Some(queue) = inner.queue.get_mut(topic) {
                while let Some(body) = queue.pop_front() {
                    drained.push(body);
                }
            }
            for body in drained {
                inner.next_id += 1;
                let id_bytes = inner.next_id.to_be_bytes();
                let mut id = [0u8; 16];
                id[8..].copy_from_slice(&id_bytes);
                let hex = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
                inner.in_flight.insert(hex.clone(), body.clone());
                // [8-byte timestamp][2-byte attempts][16-byte id][body]
                let mut payload = vec![0u8; 10];
                payload[9] = 0;
                payload.extend_from_slice(&id);
                payload.extend_from_slice(&body);
                let size = (4 + payload.len()) as u32;
                let mut frame = size.to_be_bytes().to_vec();
                frame.extend_from_slice(&2u32.to_be_bytes());
                frame.extend_from_slice(&payload);
                frames.push(frame);
            }
        }
        for frame in frames {
            for tx in &inner.deliverers {
                let _ = tx.send(frame.clone());
            }
        }
    }
}

/// The outcome of one client command on the mock.
enum CommandOutcome {
    /// A response frame to write back (or empty = silent).
    Response(Vec<u8>),
    /// A SUB: response + the connection's delivery channel.
    Subscribed {
        response: Vec<u8>,
        frames: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    },
    /// CLS — end the connection.
    Closed,
}

/// Executes one client command against the mock.
fn handle(mock: &NsqMock, command: &str, body: Option<Vec<u8>>) -> CommandOutcome {
    let mut inner = mock.inner.lock().expect("mock");
    if command.starts_with("IDENTIFY") {
        if let Some(body) = &body {
            let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            inner.last_identify_heartbeat =
                parsed.get("heartbeat_interval").and_then(Value::as_i64);
        }
        return CommandOutcome::Response(respond_ok());
    }
    if let Some(rest) = command.strip_prefix("SUB ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() != 2 {
            return CommandOutcome::Response(respond_error("SUB needs topic and channel"));
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        inner.deliverers.push(tx);
        inner.subscribed.insert(parts[0].to_string());
        return CommandOutcome::Subscribed {
            response: respond_ok(),
            frames: rx,
        };
    }
    if let Some(rest) = command.strip_prefix("PUB ") {
        let topic = rest.trim().to_string();
        let Some(body) = body else {
            return CommandOutcome::Response(respond_error("PUB needs a framed body"));
        };
        inner.queue.entry(topic).or_default().push_back(body);
        return CommandOutcome::Response(respond_ok());
    }
    if let Some(id) = command.strip_prefix("FIN ") {
        let id = id.trim().to_string();
        inner.in_flight.remove(&id);
        *inner.fins.entry(id).or_insert(0) += 1;
        return CommandOutcome::Response(Vec::new());
    }
    if let Some(rest) = command.strip_prefix("REQ ") {
        let id = rest
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        if let Some(body) = inner.in_flight.remove(&id) {
            // Requeue into the holding pen: deliver() pushes it back to
            // whichever topic is subscribed.
            inner
                .queue
                .entry(String::new())
                .or_default()
                .push_back(body);
        }
        *inner.requeues.entry(id).or_insert(0) += 1;
        return CommandOutcome::Response(Vec::new());
    }
    if command.starts_with("RDY") {
        return CommandOutcome::Response(Vec::new());
    }
    if command == "CLS" {
        return CommandOutcome::Closed;
    }
    CommandOutcome::Response(respond_error("unknown command"))
}

fn respond_ok() -> Vec<u8> {
    let payload = b"+OK";
    let size = (4 + payload.len()) as u32;
    let mut frame = size.to_be_bytes().to_vec();
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn respond_error(message: &str) -> Vec<u8> {
    let payload = format!("E_INVALID {message}").into_bytes();
    let size = (4 + payload.len()) as u32;
    let mut frame = size.to_be_bytes().to_vec();
    frame.extend_from_slice(&1u32.to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Boots the mock nsqd; returns the TCP address and the shared handle.
pub async fn spawn_nsq() -> (String, Arc<NsqMock>) {
    let mock = Arc::new(NsqMock::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let address = listener.local_addr().expect("local addr");
    let server = mock.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let server = server.clone();
            tokio::spawn(async move {
                serve(server, stream).await;
            });
        }
    });
    (address.to_string(), mock)
}

/// Serves one client connection: the magic, then two cooperating tasks
/// — a reader parsing client commands into the mock (responses and
/// settle frames ride the outbound channel), and the writer serializing
/// both client-bound frames and the test's delivered message frames.
async fn serve(mock: Arc<NsqMock>, stream: tokio::net::TcpStream) {
    let (mut reader, mut writer) = stream.into_split();

    // Handshake on the plain halves: magic + IDENTIFY + SUB are driven
    // inline before the select loop (the engine's dial does the same
    // sequence and waits on each +OK).
    let mut magic = [0u8; 4];
    if reader.read_exact(&mut magic).await.is_err() {
        eprintln!("[nsq mock] magic read failed");
        return;
    }
    eprintln!("[nsq mock] magic received");

    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let (deliver_tx, mut deliver_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let _ = deliver_tx;

    // The writer: client responses, settle frames, and delivered
    // message frames, all through one serialized half.
    tokio::spawn(async move {
        loop {
            tokio::select! {
                frame = outbound_rx.recv() => match frame {
                    Some(frame) => {
                        if writer.write_all(&frame).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
                frame = deliver_rx.recv() => match frame {
                    Some(frame) => {
                        if writer.write_all(&frame).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    });

    // The reader: parses client commands and runs them.
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let parsed = loop {
            if let Some(parsed) = parse_command(&buffer) {
                break parsed;
            }
            match reader.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        };
        let (command, body, consumed) = parsed;
        buffer.drain(..consumed);
        match handle(&mock, &command, body) {
            CommandOutcome::Response(frame) => {
                if !frame.is_empty() && outbound_tx.send(frame).is_err() {
                    return;
                }
            }
            CommandOutcome::Subscribed { response, frames } => {
                if outbound_tx.send(response).is_err() {
                    return;
                }
                // Frames the test delivers ride the mock's broadcast into
                // this forward task, then out the serialized wire.
                let mut frames = frames;
                let forward_tx = outbound_tx.clone();
                tokio::spawn(async move {
                    while let Some(frame) = frames.recv().await {
                        if forward_tx.send(frame).is_err() {
                            return;
                        }
                    }
                });
            }
            CommandOutcome::Closed => return,
        }
    }
}

/// Parses one command line from the buffer, returning the command, its
/// optional framed body, and the consumed byte count.
fn parse_command(buffer: &[u8]) -> Option<(String, Option<Vec<u8>>, usize)> {
    let newline = buffer.iter().position(|b| *b == b'\n')?;
    let line = String::from_utf8_lossy(&buffer[..newline]).to_string();
    let mut consumed = newline + 1;
    if line.starts_with("IDENTIFY") || line.starts_with("PUB") {
        if buffer.len() < consumed + 4 {
            return None; // body header not yet arrived
        }
        let size = u32::from_be_bytes([
            buffer[consumed],
            buffer[consumed + 1],
            buffer[consumed + 2],
            buffer[consumed + 3],
        ]) as usize;
        if buffer.len() < consumed + 4 + size {
            return None; // body not yet arrived
        }
        let body = buffer[consumed + 4..consumed + 4 + size].to_vec();
        consumed += 4 + size;
        return Some((line, Some(body), consumed));
    }
    Some((line, None, consumed))
}

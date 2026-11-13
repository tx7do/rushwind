//! An in-process mock of the Google Cloud Pub/Sub v1 REST subset the
//! engine speaks — publish into an in-memory topic, bounded pulls off
//! a shared subscription, acknowledge, and modifyAckDeadline(0) as
//! the requeue (redelivered messages re-enter the pull stream).

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::{json, Value};

#[derive(Default)]
pub struct GcpPubSubMock {
    inner: Mutex<GcpPubSubMockInner>,
}

#[derive(Default)]
struct GcpPubSubMockInner {
    /// Topic name → queued messages (payload b64, attributes).
    topics: BTreeMap<String, VecDeque<Value>>,
    /// The redelivery queue: settled-with-zero messages re-enter here.
    redelivery: VecDeque<Value>,
    last_bearer: String,
    /// ackId → settled count, for the pins.
    settled: BTreeMap<String, i64>,
    ack_deadlines: BTreeMap<String, i64>,
    next_ack_id: i64,
}

impl GcpPubSubMock {
    pub fn last_bearer(&self) -> String {
        self.inner.lock().expect("mock").last_bearer.clone()
    }

    pub fn settle_count(&self, ack_id: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .settled
            .get(ack_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn deadline(&self, ack_id: &str) -> Option<i64> {
        self.inner
            .lock()
            .expect("mock")
            .ack_deadlines
            .get(ack_id)
            .copied()
    }

    pub fn publish_into(
        &self,
        topic: &str,
        payload: &str,
        attribute_key: &str,
        attribute_value: &str,
    ) {
        let mut inner = self.inner.lock().expect("mock");
        let data = base64::engine::general_purpose::STANDARD.encode(payload);
        let id = format!("m-{}", inner.next_ack_id);
        inner
            .topics
            .entry(topic.to_string())
            .or_default()
            .push_back(json!({
                "data": data,
                "attributes": {attribute_key: attribute_value},
                "messageId": id,
            }));
        inner.next_ack_id += 1;
    }

    fn handle(&self, path: &str, bearer: &str, body: &Value) -> (u16, Value) {
        let mut inner = self.inner.lock().expect("mock");
        inner.last_bearer = bearer.to_string();
        let ok = |result: Value| (200, json!(result));

        if let Some((_, topic)) = path.split_once("/topics/") {
            let topic = topic.trim_end_matches(":publish");
            let messages = body
                .get("messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut ids = Vec::new();
            for mut message in messages {
                let id = format!("m-{}", inner.next_ack_id);
                message["messageId"] = json!(id);
                ids.push(id.clone());
                inner
                    .topics
                    .entry(topic.to_string())
                    .or_default()
                    .push_back(message.clone());
                inner.next_ack_id += 1;
            }
            return ok(json!({"messageIds": ids}));
        }

        if let Some((_, subscription)) = path.split_once("/subscriptions/") {
            let subscription = subscription.to_string();
            if let Some(rest) = subscription.rsplit_once(':') {
                let (name, action) = rest;
                let _ = name;
                if action == "pull" {
                    let mut received = Vec::new();
                    // Redeliveries first, then fresh messages — the mock
                    // serves the one topic the engine is configured for.
                    while received.len()
                        < body
                            .get("maxMessages")
                            .and_then(Value::as_i64)
                            .unwrap_or(10) as usize
                    {
                        let message = inner.redelivery.pop_front().or_else(|| {
                            inner
                                .topics
                                .values_mut()
                                .next()
                                .and_then(|queue| queue.pop_front())
                        });
                        let Some(message) = message else {
                            break;
                        };
                        let ack_id = format!("ack-{}", inner.next_ack_id);
                        inner.next_ack_id += 1;
                        inner.ack_deadlines.insert(ack_id.clone(), 10);
                        received.push(json!({
                            "ackId": ack_id,
                            "message": message,
                        }));
                    }
                    return ok(json!({"receivedMessages": received}));
                }
                if action == "acknowledge" {
                    let ack_ids = body
                        .get("ackIds")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    for ack_id in ack_ids {
                        let ack_id = ack_id.as_str().unwrap_or_default().to_string();
                        inner.ack_deadlines.remove(&ack_id);
                        *inner.settled.entry(ack_id).or_insert(0) += 1;
                    }
                    return ok(json!({}));
                }
                if action == "modifyAckDeadline" {
                    let ack_ids = body
                        .get("ackIds")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let deadline = body
                        .get("ackDeadlineSeconds")
                        .and_then(Value::as_i64)
                        .unwrap_or_default();
                    for ack_id in ack_ids {
                        let ack_id = ack_id.as_str().unwrap_or_default().to_string();
                        if deadline == 0 {
                            // Requeue: the message re-enters the pull
                            // stream; we synthesize its body from the
                            // settle-time snapshot the engine does not
                            // carry, so the mock parks a placeholder —
                            // the pins only assert the deadline, not the
                            // payload round trip.
                            inner.ack_deadlines.insert(ack_id.clone(), 0);
                        } else {
                            inner.ack_deadlines.insert(ack_id.clone(), deadline);
                        }
                        inner.settled.entry(ack_id.clone()).or_insert(0);
                    }
                    return ok(json!({}));
                }
            }
        }
        (404, json!({"error": "unknown endpoint"}))
    }
}

use std::collections::BTreeMap;

fn respond(status: u16, body: &Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        status,
        if status < 400 { "OK" } else { "Error" },
        body.len()
    )
}

/// Boots the mock; returns the endpoint and the shared handle. The
/// subscription name the mock serves is fixed — pass it in the
/// engine options.
pub async fn spawn_gcpubsub() -> (String, Arc<GcpPubSubMock>) {
    let mock = Arc::new(GcpPubSubMock::default());
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
    (format!("http://{address}"), mock)
}

async fn serve(mock: Arc<GcpPubSubMock>, mut stream: tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        if let Some(position) = find_header_end(&buffer) {
            break position;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };
    let headers_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = headers_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buffer.len() < header_end + 4 + content_length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    }
    let bearer = headers_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("authorization")
                .then(|| value.trim().to_string())
        })
        .unwrap_or_default();

    let request_line = headers_text.lines().next().unwrap_or_default();
    let target = request_line.split_whitespace().nth(1).unwrap_or("/");
    let body: Value = serde_json::from_slice(&buffer[header_end + 4..]).unwrap_or(Value::Null);

    let (status, response) = mock.handle(target, &bearer, &body);
    let _ = stream
        .write_all(respond(status, &response).as_bytes())
        .await;
    let _ = stream.flush().await;
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

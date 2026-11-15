//! An in-process mock RocketMQ broker speaking the JSON-mode remoting
//! subset the engine uses — SEND_MESSAGE stores into an in-memory
//! topic queue (the properties string parsed back into a map),
//! PULL_MESSAGE serves bounded windows with `pullStatus`/`nextBeginOffset`/
//! `msgFoundList` (base64 body, properties, queueOffset),
//! UPDATE_CONSUMER_OFFSET records the commit. One connection per
//! remoting call (the engine is connectionless), each handled
//! independently.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CODE_SEND_MESSAGE: i64 = 10;
const CODE_PULL_MESSAGE: i64 = 20;
const CODE_UPDATE_CONSUMER_OFFSET: i64 = 15;

struct StoredMessage {
    body: Vec<u8>,
    properties: BTreeMap<String, String>,
}

#[derive(Default)]
struct TopicState {
    messages: VecDeque<StoredMessage>,
    committed: i64,
    pulls: i64,
}

#[derive(Clone)]
pub struct LastSend {
    pub topic: String,
    pub properties: String,
    pub body: Vec<u8>,
}

#[derive(Default)]
struct MockInner {
    topics: BTreeMap<String, TopicState>,
    last_send: Option<LastSend>,
    send_reject: Option<String>,
}

/// The shared mock handle: state access for the pins.
pub struct RocketmqMock {
    inner: Mutex<MockInner>,
}

impl RocketmqMock {
    pub fn last_send(&self) -> Option<LastSend> {
        self.inner.lock().expect("mock").last_send.clone()
    }

    pub fn committed(&self, topic: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .topics
            .get(topic)
            .map(|state| state.committed)
            .unwrap_or(0)
    }

    pub fn pull_count(&self, topic: &str) -> i64 {
        self.inner
            .lock()
            .expect("mock")
            .topics
            .get(topic)
            .map(|state| state.pulls)
            .unwrap_or(0)
    }

    pub fn set_send_reject(&self, remark: Option<String>) {
        self.inner.lock().expect("mock").send_reject = remark;
    }
}

/// Spawns the mock on an ephemeral port; returns the broker address
/// (`host:port`, the options' `broker_addr` shape) and the handle.
pub async fn spawn_rocketmq() -> (String, Arc<RocketmqMock>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();
    let mock = Arc::new(RocketmqMock {
        inner: Mutex::new(MockInner::default()),
    });
    let mock_for_task = Arc::clone(&mock);
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let mock = Arc::clone(&mock_for_task);
            tokio::spawn(async move {
                serve(mock, socket).await;
            });
        }
    });
    (addr, mock)
}

/// One connection = one or more framed requests (the engine dials per
/// call, but the loop tolerates reuse).
async fn serve(mock: Arc<RocketmqMock>, mut socket: TcpStream) {
    loop {
        let mut total_buf = [0u8; 4];
        match socket.read_exact(&mut total_buf).await {
            Ok(_) => {}
            Err(_) => return, // connection closed
        }
        let total = u32::from_be_bytes(total_buf) as usize;
        if total < 8 {
            return;
        }
        let mut payload = vec![0u8; total];
        if socket.read_exact(&mut payload).await.is_err() {
            return;
        }
        let header_len =
            u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize;
        let header_start = 8;
        let header_end = header_start + header_len;
        if payload.len() < header_end {
            return;
        }
        let Ok(header) = serde_json::from_slice::<Value>(&payload[header_start..header_end]) else {
            return;
        };
        let body = payload[header_end..].to_vec();
        let code = header.get("code").and_then(Value::as_i64).unwrap_or(-1);
        let opaque = header.get("opaque").and_then(Value::as_i64).unwrap_or(0);
        let ext = header.get("extFields").cloned().unwrap_or(Value::Null);
        let (code, ext_fields, remark) = dispatch(&mock, code, &ext, &body);
        let response = response_frame(code, opaque, &ext_fields, remark.as_deref());
        if socket.write_all(&response).await.is_err() {
            return;
        }
    }
}

/// Dispatches one request; returns the response code, extFields, and
/// the error remark (None on success).
fn dispatch(
    mock: &RocketmqMock,
    code: i64,
    ext: &Value,
    body: &[u8],
) -> (i64, Value, Option<String>) {
    match code {
        CODE_SEND_MESSAGE => {
            let topic = ext.get("Topic").and_then(Value::as_str).unwrap_or_default();
            let properties = ext
                .get("Properties")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let reject = mock.inner.lock().expect("mock").send_reject.clone();
            if let Some(remark) = reject {
                return (1, json!({}), Some(remark));
            }
            let mut inner = mock.inner.lock().expect("mock");
            let state = inner.topics.entry(topic.to_string()).or_default();
            state.messages.push_back(StoredMessage {
                body: body.to_vec(),
                properties: parse_properties(&properties),
            });
            inner.last_send = Some(LastSend {
                topic: topic.to_string(),
                properties,
                body: body.to_vec(),
            });
            (
                0,
                json!({
                    "sendStatus": "SEND_OK",
                    "msgId": "0A00000000000000000000000000",
                }),
                None,
            )
        }
        CODE_PULL_MESSAGE => {
            let topic = ext.get("topic").and_then(Value::as_str).unwrap_or_default();
            let offset = ext.get("offset").and_then(Value::as_i64).unwrap_or(0);
            let max = ext.get("maxMsgNums").and_then(Value::as_i64).unwrap_or(32);
            let mut inner = mock.inner.lock().expect("mock");
            let state = inner.topics.entry(topic.to_string()).or_default();
            state.pulls += 1;
            let len = state.messages.len() as i64;
            let start = offset.clamp(0, len) as usize;
            let end = ((offset + max).clamp(0, len)) as usize;
            let served: Vec<Value> = state
                .messages
                .iter()
                .enumerate()
                .skip(start)
                .take(end.saturating_sub(start))
                .map(|(index, message)| {
                    json!({
                        "body": base64::engine::general_purpose::STANDARD.encode(&message.body),
                        "properties": message.properties,
                        "queueOffset": index,
                    })
                })
                .collect();
            if served.is_empty() {
                (
                    0,
                    json!({
                        "pullStatus": "NO_NEW_MSG",
                        "nextBeginOffset": len,
                        "minOffset": 0,
                        "maxOffset": len,
                    }),
                    None,
                )
            } else {
                let next = start as i64 + served.len() as i64;
                (
                    0,
                    json!({
                        "pullStatus": "FOUND",
                        "nextBeginOffset": next,
                        "minOffset": 0,
                        "maxOffset": len,
                        "msgFoundList": served,
                    }),
                    None,
                )
            }
        }
        CODE_UPDATE_CONSUMER_OFFSET => {
            let topic = ext.get("topic").and_then(Value::as_str).unwrap_or_default();
            let commit = ext
                .get("commitOffset")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let mut inner = mock.inner.lock().expect("mock");
            inner.topics.entry(topic.to_string()).or_default().committed = commit;
            (0, json!({}), None)
        }
        _ => (1, json!({}), Some("unknown code".to_string())),
    }
}

/// Parses the engine's properties string back into a map (the
/// KEY\u0001VALUE\u0002 convention).
fn parse_properties(properties: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for pair in properties.split('\u{0002}') {
        if pair.is_empty() {
            continue;
        }
        if let Some((key, value)) = pair.split_once('\u{0001}') {
            map.insert(key.to_string(), value.to_string());
        }
    }
    map
}

/// Builds the remoting response frame (JSON serialization, no body).
fn response_frame(code: i64, opaque: i64, ext_fields: &Value, remark: Option<&str>) -> Vec<u8> {
    let header = json!({
        "code": code,
        "language": "JAVA",
        "version": 0,
        "opaque": opaque,
        "flag": 1,
        "remark": remark,
        "extFields": ext_fields,
    })
    .to_string()
    .into_bytes();
    let header_len = header.len() as u32;
    // The total covers everything after the total field itself: the
    // serialize-type field, the header-length field, the header.
    let total = 8 + header_len;
    let mut frame = total.to_be_bytes().to_vec();
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(&header_len.to_be_bytes());
    frame.extend_from_slice(&header);
    frame
}

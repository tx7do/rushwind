//! An in-process mock of the Azure Service Bus entity's HTTP REST
//! subset the engine speaks — send `POST /messages` into an
//! in-memory queue (user properties as request headers, the message
//! id from the `BrokerProperties` header, the payload as the body),
//! destructive receive `DELETE /messages/head`, peek-lock receive
//! `POST /messages/head` (locks the message, `LockToken` in
//! `BrokerProperties`), complete `DELETE /messages/{id}/{token}`,
//! abandon `PUT /messages/{id}/{token}` (the message re-enters the
//! queue with `DeliveryCount` incremented).

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

struct StoredMessage {
    body: Vec<u8>,
    properties: BTreeMap<String, String>,
    message_id: String,
    delivery_count: i64,
}

#[derive(Clone)]
pub struct LastSend {
    pub properties: BTreeMap<String, String>,
    pub broker_properties: Value,
    pub body: Vec<u8>,
}

#[derive(Default)]
struct MockInner {
    queue: VecDeque<StoredMessage>,
    /// lockToken → parked peek-locked message.
    locked: BTreeMap<String, StoredMessage>,
    last_auth: String,
    last_send: Option<LastSend>,
    /// The BrokerProperties the mock last handed out on a receive.
    last_receive_broker: Option<Value>,
    send_reject: bool,
    destructive_receives: i64,
    peek_receives: i64,
    next_id: u64,
    next_lock: u64,
}

/// The shared mock handle: state access for the pins.
pub struct AzureSbMock {
    inner: Mutex<MockInner>,
}

impl Default for AzureSbMock {
    fn default() -> Self {
        Self {
            inner: Mutex::new(MockInner::default()),
        }
    }
}

impl AzureSbMock {
    pub fn last_send(&self) -> Option<LastSend> {
        self.inner.lock().expect("mock").last_send.clone()
    }

    pub fn last_receive_broker(&self) -> Option<Value> {
        self.inner.lock().expect("mock").last_receive_broker.clone()
    }

    pub fn last_auth(&self) -> String {
        self.inner.lock().expect("mock").last_auth.clone()
    }

    pub fn locked_len(&self) -> usize {
        self.inner.lock().expect("mock").locked.len()
    }

    pub fn queue_len(&self) -> usize {
        self.inner.lock().expect("mock").queue.len()
    }

    pub fn destructive_receives(&self) -> i64 {
        self.inner.lock().expect("mock").destructive_receives
    }

    pub fn set_send_reject(&self, rejected: bool) {
        self.inner.lock().expect("mock").send_reject = rejected;
    }
}

/// Boots the mock; returns the endpoint (`http://host:port`) and the
/// handle.
pub async fn spawn_azuresb() -> (String, Arc<AzureSbMock>) {
    let mock = Arc::new(AzureSbMock::default());
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

/// The headers the wire owns — user properties are everything else.
const RESERVED: [&str; 9] = [
    "authorization",
    "brokerproperties",
    "content-type",
    "content-length",
    "host",
    "user-agent",
    "accept",
    "accept-encoding",
    "connection",
];

async fn serve(mock: Arc<AzureSbMock>, mut stream: tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // HTTP/1.1 keep-alive: the engine's reqwest client pools
    // connections, so the mock serves request after request on one
    // connection until the client hangs up.
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
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
                Ok(0) | Err(_) => return,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }

        let mut request_line = headers_text
            .lines()
            .next()
            .unwrap_or_default()
            .split_whitespace();
        let method = request_line.next().unwrap_or("GET").to_string();
        let target = request_line.next().unwrap_or("/").to_string();
        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        for line in headers_text.lines().skip(1) {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }
        let body = buffer[header_end + 4..header_end + 4 + content_length].to_vec();

        let (status, response_headers, response_body) =
            dispatch(&mock, &method, &target, &headers, &body);
        let response = build_response(status, &response_headers, &response_body);
        if stream.write_all(&response).await.is_err() {
            return;
        }
        let _ = stream.flush().await;
        buffer.drain(..header_end + 4 + content_length);
    }
}

/// Dispatches one request; returns the status, the response headers
/// (BrokerProperties + user properties ride back in it), and the
/// body.
fn dispatch(
    mock: &AzureSbMock,
    method: &str,
    target: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let path = target.split('?').next().unwrap_or(target);
    let mut inner = mock.inner.lock().expect("mock");
    inner.last_auth = headers.get("authorization").cloned().unwrap_or_default();
    let broker = headers
        .get("brokerproperties")
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or(json!({}));

    // User properties: every header the wire does not own.
    let mut properties = BTreeMap::new();
    for (name, value) in headers {
        if !RESERVED.contains(&name.as_str()) {
            properties.insert(name.clone(), value.clone());
        }
    }

    match (method, path) {
        ("POST", "/messages") => {
            if inner.send_reject {
                return (500, Vec::new(), b"the mock refused the send".to_vec());
            }
            let message_id = broker
                .get("MessageId")
                .and_then(Value::as_str)
                .unwrap_or("mock-id")
                .to_string();
            let stored = StoredMessage {
                body: body.to_vec(),
                properties: properties.clone(),
                message_id,
                delivery_count: 0,
            };
            inner.last_send = Some(LastSend {
                properties,
                broker_properties: broker,
                body: body.to_vec(),
            });
            inner.queue.push_back(stored);
            (201, Vec::new(), Vec::new())
        }
        ("DELETE", "/messages/head") => {
            // The destructive receive: settled the moment it is
            // handed out.
            let Some(message) = inner.queue.pop_front() else {
                return (204, Vec::new(), Vec::new());
            };
            inner.destructive_receives += 1;
            let broker = json!({
                "MessageId": message.message_id,
                "SequenceNumber": inner.destructive_receives,
                "DeliveryCount": message.delivery_count,
            });
            inner.last_receive_broker = Some(broker.clone());
            respond_with_message(&message, &broker)
        }
        ("POST", "/messages/head") => {
            // The peek-lock receive: locked until settled.
            let Some(message) = inner.queue.pop_front() else {
                return (204, Vec::new(), Vec::new());
            };
            inner.peek_receives += 1;
            let token = format!("lk-{}", inner.next_lock);
            inner.next_lock += 1;
            let broker = json!({
                "MessageId": message.message_id,
                "LockToken": token,
                "DeliveryCount": message.delivery_count,
            });
            inner.last_receive_broker = Some(broker.clone());
            let (status, response_headers, response_body) = respond_with_message(&message, &broker);
            inner.locked.insert(token, message);
            (status, response_headers, response_body)
        }
        (_, rest) if rest.starts_with("/messages/") => {
            let token = rest.rsplit('/').next().unwrap_or_default().to_string();
            match (method, inner.locked.remove(&token)) {
                ("DELETE", Some(_)) => (200, Vec::new(), Vec::new()),
                ("PUT", Some(mut message)) => {
                    // Abandon: the lock releases, DeliveryCount
                    // increments, the message re-enters the queue.
                    message.delivery_count += 1;
                    inner.queue.push_back(message);
                    (200, Vec::new(), Vec::new())
                }
                (_, None) => (404, Vec::new(), b"no such lock".to_vec()),
                _ => (405, Vec::new(), b"unsupported settle".to_vec()),
            }
        }
        _ => (404, Vec::new(), b"unknown path".to_vec()),
    }
}

/// The receive response: the payload as the body, the message
/// metadata in `BrokerProperties`, the user properties as headers.
fn respond_with_message(
    message: &StoredMessage,
    broker: &Value,
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut response_headers = vec![("BrokerProperties".to_string(), broker.to_string())];
    for (name, value) in &message.properties {
        response_headers.push((name.clone(), value.clone()));
    }
    (200, response_headers, message.body.clone())
}

/// Builds one HTTP/1.1 response.
fn build_response(status: u16, headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str(&format!("Content-Length: {}\r\n", body.len()));
    response.push_str("\r\n");
    let mut bytes = response.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

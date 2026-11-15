//! An in-process mock of the Conductor server's REST subset the
//! engine speaks — start (`POST /workflow`: requires `name`, mints
//! the id when the body carries no `workflowId`, answers the id as
//! plain text), describe (`GET /workflow/{id}`: the status wire
//! name), terminate (`DELETE /workflow/{id}`).

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

#[derive(Default)]
pub struct ConductorMockInner {
    /// id → (workflow name, status wire name).
    workflows: BTreeMap<String, (String, String)>,
    last_start_body: Option<Value>,
    last_bearer: String,
    next_id: u64,
}

/// The shared mock handle: state access for the pins.
pub struct ConductorMock {
    inner: Mutex<ConductorMockInner>,
}

impl Default for ConductorMock {
    fn default() -> Self {
        Self {
            inner: Mutex::new(ConductorMockInner::default()),
        }
    }
}

impl ConductorMock {
    pub fn last_start_body(&self) -> Value {
        self.inner
            .lock()
            .expect("mock")
            .last_start_body
            .clone()
            .unwrap_or(Value::Null)
    }

    pub fn last_bearer(&self) -> String {
        self.inner.lock().expect("mock").last_bearer.clone()
    }

    /// Overrides a workflow's status wire name (the pins' window).
    pub fn set_status(&self, id: &str, status: &str) {
        let mut inner = self.inner.lock().expect("mock");
        if let Some(entry) = inner.workflows.get_mut(id) {
            entry.1 = status.to_string();
        }
    }
}

/// Boots the mock; returns the server base URL and the handle.
pub async fn spawn_conductor() -> (String, Arc<ConductorMock>) {
    let mock = Arc::new(ConductorMock::default());
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

async fn serve(mock: Arc<ConductorMock>, mut stream: tokio::net::TcpStream) {
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
        let bearer = headers_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            })
            .unwrap_or_default();
        let body: Value =
            serde_json::from_slice(&buffer[header_end + 4..header_end + 4 + content_length])
                .unwrap_or(Value::Null);

        let (status, content_type, response_body) =
            dispatch(&mock, &method, &target, &bearer, &body);
        let response = build_response(status, content_type, &response_body);
        if stream.write_all(&response).await.is_err() {
            return;
        }
        let _ = stream.flush().await;
        buffer.drain(..header_end + 4 + content_length);
    }
}

/// Dispatches one request; returns the status, the content type, and
/// the body. The start's answer is the minted id as plain text.
fn dispatch(
    mock: &ConductorMock,
    method: &str,
    target: &str,
    bearer: &str,
    body: &Value,
) -> (u16, &'static str, Vec<u8>) {
    let path = target.split('?').next().unwrap_or(target);
    let mut inner = mock.inner.lock().expect("mock");
    inner.last_bearer = bearer.to_string();

    match (method, path) {
        ("POST", "/workflow") => {
            let name = body.get("name").and_then(Value::as_str).unwrap_or_default();
            if name.is_empty() {
                return (
                    400,
                    "application/json",
                    br#"{"message":"Workflow name cannot be empty or null"}"#.to_vec(),
                );
            }
            let id = match body.get("workflowId").and_then(Value::as_str) {
                Some(minted) if !minted.is_empty() => minted.to_string(),
                _ => {
                    let id = format!("wf-{}", inner.next_id);
                    inner.next_id += 1;
                    id
                }
            };
            inner.last_start_body = Some(body.clone());
            inner
                .workflows
                .entry(id.clone())
                .or_insert_with(|| (name.to_string(), "RUNNING".to_string()));
            (200, "text/plain", id.into_bytes())
        }
        ("GET", rest) if rest.starts_with("/workflow/") => {
            let id = rest.trim_start_matches("/workflow/");
            match inner.workflows.get(id) {
                Some((name, status)) => (
                    200,
                    "application/json",
                    json!({
                        "workflowId": id,
                        "workflowName": name,
                        "status": status,
                    })
                    .to_string()
                    .into_bytes(),
                ),
                None => (
                    404,
                    "application/json",
                    json!({"message": format!("No such workflow found by id: {id}")})
                        .to_string()
                        .into_bytes(),
                ),
            }
        }
        ("DELETE", rest) if rest.starts_with("/workflow/") => {
            let id = rest.trim_start_matches("/workflow/");
            match inner.workflows.get_mut(id) {
                Some(entry) => {
                    entry.1 = "TERMINATED".to_string();
                    (200, "application/json", b"\"OK\"".to_vec())
                }
                None => (
                    404,
                    "application/json",
                    json!({"message": format!("No such workflow found by id: {id}")})
                        .to_string()
                        .into_bytes(),
                ),
            }
        }
        _ => (404, "application/json", b"unknown path".to_vec()),
    }
}

/// Builds one HTTP/1.1 response.
fn build_response(status: u16, content_type: &str, body: &[u8]) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\n");
    response.push_str(&format!("Content-Type: {content_type}\r\n"));
    response.push_str(&format!("Content-Length: {}\r\n", body.len()));
    response.push_str("\r\n");
    let mut bytes = response.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

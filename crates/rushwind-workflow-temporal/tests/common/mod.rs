//! An in-process mock of the Temporal server's gRPC-gateway HTTP
//! subset the engine speaks — workflow start/describe/terminate over
//! an in-memory store keyed by namespace/id, phases driven by the
//! test.

#![cfg(test)]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Canceled,
    Terminated,
    TimedOut,
    ContinuedAsNew,
}

/// The gRPC enum's wire name per status.
fn wire_name(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Running => "WORKFLOW_EXECUTION_STATUS_RUNNING",
        ExecutionStatus::Completed => "WORKFLOW_EXECUTION_STATUS_COMPLETED",
        ExecutionStatus::Failed => "WORKFLOW_EXECUTION_STATUS_FAILED",
        ExecutionStatus::Canceled => "WORKFLOW_EXECUTION_STATUS_CANCELED",
        ExecutionStatus::Terminated => "WORKFLOW_EXECUTION_STATUS_TERMINATED",
        ExecutionStatus::TimedOut => "WORKFLOW_EXECUTION_STATUS_TIMED_OUT",
        ExecutionStatus::ContinuedAsNew => "WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW",
    }
}

#[derive(Default)]
pub struct TemporalMock {
    /// namespace/id → (status, request echo).
    workflows: Mutex<BTreeMap<String, (ExecutionStatus, Value)>>,
    last_bearer: Mutex<String>,
}

impl TemporalMock {
    pub fn last_bearer(&self) -> String {
        self.last_bearer.lock().expect("mock bearer").clone()
    }

    pub fn last_start_body(&self) -> Value {
        self.workflows
            .lock()
            .expect("mock store")
            .values()
            .next()
            .map(|(_, request)| request.clone())
            .unwrap_or(Value::Null)
    }

    pub fn set_status(&self, namespace: &str, id: &str, status: ExecutionStatus) {
        let key = format!("{namespace}/{id}");
        let request = self
            .workflows
            .lock()
            .expect("mock store")
            .get(&key)
            .map(|(_, request)| request.clone())
            .unwrap_or(Value::Null);
        self.workflows
            .lock()
            .expect("mock store")
            .insert(key, (status, request));
    }

    fn handle(&self, method: &str, path: &str, bearer: &str, body: &Value) -> (u16, Value) {
        *self.last_bearer.lock().expect("mock bearer") = bearer.to_string();
        let prefix = "/api/v1/namespaces/";
        let Some(rest) = path.strip_prefix(prefix) else {
            return (404, json!({"message": "unknown endpoint"}));
        };
        // /{ns}/workflows/{id}[/terminate]
        let segments: Vec<&str> = rest.trim_matches('/').split('/').collect();
        if segments.len() < 3 || segments[1] != "workflows" {
            return (404, json!({"message": "unknown endpoint"}));
        }
        let namespace = segments[0].to_string();
        let id = segments[2].to_string();
        let key = format!("{namespace}/{id}");
        match (method, segments.get(3).copied()) {
            ("POST", None) => {
                let mut stored = body.clone();
                if let Some(meta) = stored.as_object_mut() {
                    meta.insert("workflowId".to_string(), json!(id));
                    meta.insert("namespace".to_string(), json!(namespace));
                }
                self.workflows
                    .lock()
                    .expect("mock store")
                    .insert(key, (ExecutionStatus::Running, stored));
                (
                    200,
                    json!({
                        "runExecutionInfo": {"runId": "run-1"},
                        "workflowExecutionInfo": {
                            "execution": {"workflowId": id, "runId": "run-1"},
                            "type": {"name": stored_type(body)},
                            "status": "WORKFLOW_EXECUTION_STATUS_RUNNING"
                        }
                    }),
                )
            }
            ("GET", None) => match self.workflows.lock().expect("mock store").get(&key) {
                Some((status, request)) => (
                    200,
                    json!({
                        "workflowExecutionInfo": {
                            "execution": {"workflowId": id, "runId": "run-1"},
                            "type": {"name": stored_type(request)},
                            "status": wire_name(*status)
                        }
                    }),
                ),
                None => (404, json!({"message": "not found"})),
            },
            ("POST", Some("terminate")) => {
                let mut workflows = self.workflows.lock().expect("mock store");
                match workflows.get(&key).cloned() {
                    Some((status, request)) => {
                        let next = if status == ExecutionStatus::Running {
                            ExecutionStatus::Terminated
                        } else {
                            status
                        };
                        workflows.insert(key, (next, request));
                        (200, json!({}))
                    }
                    None => (404, json!({"message": "not found"})),
                }
            }
            _ => (404, json!({"message": "unknown endpoint"})),
        }
    }
}

/// The workflow type name the request's workflowType field carries —
/// echoed in the mock's describe.
fn stored_type(body: &Value) -> String {
    body.pointer("/workflowType/name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn respond(status: u16, body: &Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        status,
        if status < 400 { "OK" } else { "Error" },
        body.len()
    )
}

pub async fn spawn_temporal() -> (String, Arc<TemporalMock>) {
    let mock = Arc::new(TemporalMock::default());
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

async fn serve(mock: Arc<TemporalMock>, mut stream: tokio::net::TcpStream) {
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
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts
        .next()
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
    let body: Value = serde_json::from_slice(&buffer[header_end + 4..]).unwrap_or(Value::Null);

    let (status, response) = mock.handle(&method, &path, &bearer, &body);
    let _ = stream
        .write_all(respond(status, &response).as_bytes())
        .await;
    let _ = stream.flush().await;
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

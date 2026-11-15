//! An in-process mock of the Argo Server REST subset the engine
//! speaks — workflow create/get/terminate over an in-memory store
//! keyed by namespace/name, phases driven by the test.

#![cfg(test)]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Phase {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Default)]
pub struct ArgoMock {
    workflows: Mutex<BTreeMap<String, Phase>>,
    last_bearer: Mutex<String>,
    last_path: Mutex<String>,
}

impl ArgoMock {
    pub fn last_bearer(&self) -> String {
        self.last_bearer.lock().expect("mock bearer").clone()
    }

    pub fn last_path(&self) -> String {
        self.last_path.lock().expect("mock path").clone()
    }

    pub fn set_phase(&self, namespace: &str, name: &str, phase: Phase) {
        self.workflows
            .lock()
            .expect("mock store")
            .insert(format!("{namespace}/{name}"), phase);
    }

    fn handle(&self, method: &str, path: &str, bearer: &str, body: &Value) -> (u16, Value) {
        *self.last_bearer.lock().expect("mock bearer") = bearer.to_string();
        *self.last_path.lock().expect("mock path") = path.to_string();
        let prefix = "/api/v1/workflows/";
        let Some(rest) = path.strip_prefix(prefix) else {
            return (404, json!({"message": "unknown endpoint"}));
        };
        let segments: Vec<&str> = rest.trim_matches('/').split('/').collect();
        match (method, segments.as_slice()) {
            ("POST", [namespace]) => {
                let name = body
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if name.is_empty() {
                    return (400, json!({"message": "workflow name required"}));
                }
                let key = format!("{namespace}/{name}");
                if self
                    .workflows
                    .lock()
                    .expect("mock store")
                    .contains_key(&key)
                {
                    return (409, json!({"message": "workflow already exists"}));
                }
                self.workflows
                    .lock()
                    .expect("mock store")
                    .insert(key.clone(), Phase::Running);
                (
                    200,
                    json!({"metadata": {"name": name, "namespace": namespace},
                           "status": {"phase": "Running"}}),
                )
            }
            ("GET", [namespace, name]) => {
                let key = format!("{namespace}/{name}");
                match self
                    .workflows
                    .lock()
                    .expect("mock store")
                    .get(&key)
                    .copied()
                {
                    Some(phase) => (
                        200,
                        json!({"metadata": {"name": name, "namespace": namespace},
                               "status": {"phase": phase_name(phase)}}),
                    ),
                    None => (404, json!({"message": "not found"})),
                }
            }
            ("POST", [namespace, name, "terminate"]) => {
                let key = format!("{namespace}/{name}");
                let mut workflows = self.workflows.lock().expect("mock store");
                match workflows.get(&key).copied() {
                    Some(_) => {
                        workflows.insert(key, Phase::Cancelled);
                        (200, json!({}))
                    }
                    None => (404, json!({"message": "not found"})),
                }
            }
            _ => (404, json!({"message": "unknown endpoint"})),
        }
    }
}

fn phase_name(phase: Phase) -> &'static str {
    match phase {
        Phase::Running => "Running",
        Phase::Succeeded => "Succeeded",
        Phase::Failed => "Failed",
        Phase::Cancelled => "Cancelled",
    }
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

pub async fn spawn_argo() -> (String, Arc<ArgoMock>) {
    let mock = Arc::new(ArgoMock::default());
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

async fn serve(mock: Arc<ArgoMock>, mut stream: tokio::net::TcpStream) {
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

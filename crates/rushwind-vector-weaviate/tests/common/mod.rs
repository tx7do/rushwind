//! An in-process mock of the Weaviate REST/GraphQL subset the engine
//! speaks — schema create, object batch (uuid-keyed), object delete,
//! and the nearVector GraphQL search with real distance math scored
//! Weaviate-style (`_additional.distance`: cosine = 1 − cos, l2-squared
//! = euclid², dot = 2 − 2·cos — all smallest-first).

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rushwind_vector::{cosine_distance, euclidean_distance};
use serde_json::{json, Value};

#[derive(Default)]
struct WeaviateClass {
    metric: String,
    dimension: Option<usize>,
    /// object uuid → (contract id, vector).
    objects: BTreeMap<String, (String, Vec<f32>)>,
}

#[derive(Default)]
pub struct WeaviateMock {
    classes: Mutex<BTreeMap<String, WeaviateClass>>,
    last_bearer: Mutex<String>,
}

impl WeaviateMock {
    pub fn last_bearer(&self) -> String {
        self.last_bearer.lock().expect("mock bearer").clone()
    }

    pub fn class_names(&self) -> Vec<String> {
        self.classes
            .lock()
            .expect("mock classes")
            .keys()
            .cloned()
            .collect()
    }

    pub fn object_count(&self, class: &str) -> usize {
        self.classes
            .lock()
            .expect("mock classes")
            .get(class)
            .map(|c| c.objects.len())
            .unwrap_or(0)
    }

    fn handle(&self, method: &str, path: &str, bearer: &str, body: &Value) -> (u16, Value) {
        *self.last_bearer.lock().expect("mock bearer") = bearer.to_string();
        let mut classes = self.classes.lock().expect("mock classes");
        match (method, path) {
            ("POST", "/v1/schema") => {
                let class = body
                    .get("class")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let metric = body
                    .pointer("/vectorIndexConfig/distance")
                    .and_then(Value::as_str)
                    .unwrap_or("cosine")
                    .to_string();
                if classes.contains_key(&class) {
                    return (
                        422,
                        json!({"error": [{"message": format!("class {class} already exists")}]}),
                    );
                }
                classes.insert(
                    class,
                    WeaviateClass {
                        metric,
                        dimension: None,
                        objects: BTreeMap::new(),
                    },
                );
                (200, json!({}))
            }
            ("POST", "/v1/batch/objects") => {
                let objects = body
                    .get("objects")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for object in &objects {
                    let class = object
                        .get("class")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let Some(entry) = classes.get_mut(&class) else {
                        return (
                            400,
                            json!({"error": [{"message": format!("class {class} not found")}]}),
                        );
                    };
                    let uuid = object
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let contract_id = object
                        .pointer("/properties/contractId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let vector: Vec<f32> = object
                        .get("vector")
                        .and_then(Value::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|v| v.as_f64().map(|f| f as f32))
                                .collect()
                        })
                        .unwrap_or_default();
                    if entry.dimension.is_none() {
                        entry.dimension = Some(vector.len());
                    }
                    entry.objects.insert(uuid, (contract_id, vector));
                }
                (200, json!([]))
            }
            ("DELETE", p) if p.starts_with("/v1/objects/") => {
                let uuid = p.trim_start_matches("/v1/objects/");
                let mut removed = false;
                for class in classes.values_mut() {
                    if class.objects.remove(uuid).is_some() {
                        removed = true;
                    }
                }
                if removed {
                    (204, Value::Null)
                } else {
                    (404, json!({"error": [{"message": "not found"}]}))
                }
            }
            ("POST", "/v1/graphql") => {
                let query = body
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                // Parse `Get { Class}(nearVector: {vector: [...]}, limit: N)`.
                let parsed = parse_get(query);
                let Some((class, vector, limit)) = parsed else {
                    return (400, json!({"errors": [{"message": "unparseable query"}]}));
                };
                let Some(entry) = classes.get(&class) else {
                    return (200, json!({"data": {"Get": {&class: []}}}));
                };
                let Some(dimension) = entry.dimension else {
                    return (200, json!({"data": {"Get": {&class: []}}}));
                };
                if vector.len() != dimension {
                    return (
                        400,
                        json!({"errors": [{"message": "vector dimension mismatch"}]}),
                    );
                }
                let mut scored: Vec<(f64, &String)> = entry
                    .objects
                    .values()
                    .map(|(contract_id, object_vector)| {
                        let distance = match entry.metric.as_str() {
                            "cosine" => cosine_distance(&vector, object_vector) as f64,
                            "l2-squared" => {
                                let d = euclidean_distance(&vector, object_vector);
                                (d * d) as f64
                            }
                            "dot" => {
                                let cos = 1.0 - cosine_distance(&vector, object_vector);
                                (2.0 - 2.0 * cos) as f64
                            }
                            _ => 0.0,
                        };
                        (distance, contract_id)
                    })
                    .collect();
                // All Weaviate distances rank smallest-first.
                scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let rows: Vec<Value> = scored
                    .into_iter()
                    .take(limit)
                    .map(|(distance, contract_id)| {
                        json!({"contractId": contract_id,
                               "_additional": {"distance": distance.to_string()}})
                    })
                    .collect();
                (200, json!({"data": {"Get": {&class: rows}}}))
            }
            _ => (404, json!({"error": [{"message": "unknown endpoint"}]})),
        }
    }
}

/// Parses the one GraphQL shape the engine emits.
fn parse_get(query: &str) -> Option<(String, Vec<f32>, usize)> {
    let inner = query
        .strip_prefix("{ Get {")
        .and_then(|rest| rest.strip_suffix("} }"))?;
    let class_part = inner.trim_start();
    let (class, rest) = class_part.split_once('(')?;
    let vector_part = rest
        .find("nearVector: {vector: ")
        .map(|at| &rest[at + "nearVector: {vector: ".len()..])?;
    // The vector is a bracketed array — locate its own closing bracket,
    // not the nearVector object's brace.
    let vector_open = vector_part.find('[')?;
    let vector_close = vector_part[vector_open..].find(']')? + vector_open + 1;
    let vector_text = &vector_part[..vector_close];
    let vector: Vec<f32> = vector_text
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .filter_map(|part| part.trim().parse::<f32>().ok())
        .collect();
    let limit_part = rest.find("limit: ")?;
    let limit_text = &rest[limit_part + "limit: ".len()..];
    let limit: usize = limit_text
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()?;
    Some((class.trim().to_string(), vector, limit))
}

fn respond(status: u16, body: &Value) -> String {
    let body = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        status,
        if status < 400 { "OK" } else { "Error" },
        body.len()
    )
}

/// Boots the mock; returns the base URL and the shared handle.
pub async fn spawn_weaviate() -> (String, Arc<WeaviateMock>) {
    let mock = Arc::new(WeaviateMock::default());
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

async fn serve(mock: Arc<WeaviateMock>, mut stream: tokio::net::TcpStream) {
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
    let path = parts.next().unwrap_or_default().to_string();
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

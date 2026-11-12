//! An in-process mock of the qdrant REST subset the engine speaks —
//! collections config, point upsert/delete, brute-force search with
//! real distance math — no external qdrant needed. The search scores
//! follow qdrant's own per-metric shape: cosine answers the
//! *similarity*, euclid the raw *distance*, dot the raw product.
//!
//! One mock serves one test: the listener rides a spawned task for
//! the process lifetime, the store stays reachable through the
//! returned handle for wire-level assertions.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rushwind_vector::{cosine_distance, dot_product, euclidean_distance};
use serde_json::{json, Value};

/// One named vector's config: dimension + qdrant distance name.
type VectorConfig = (usize, String);

#[derive(Default)]
struct Collection {
    vectors: BTreeMap<String, VectorConfig>,
    /// Point id (wire JSON echo) → stored payload + named vectors.
    points: Vec<(Value, Value, BTreeMap<String, Vec<f32>>)>,
}

#[derive(Default)]
struct Store {
    collections: BTreeMap<String, Collection>,
}

/// The mock's shared state: the store plus the headers of the last
/// request (the api-key pin reads them).
pub struct QdrantMock {
    store: Mutex<Store>,
    last_headers: Mutex<Vec<(String, String)>>,
}

impl QdrantMock {
    fn headers(&self) -> Vec<(String, String)> {
        self.last_headers.lock().expect("mock headers").clone()
    }

    pub fn last_header(&self, name: &str) -> Option<String> {
        self.headers()
            .into_iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }

    /// The vectors config a collection ended up with — the
    /// collection-birth pin reads it.
    pub fn collection_config(&self, collection: &str) -> Option<Value> {
        let store = self.store.lock().expect("mock store");
        let collection = store.collections.get(collection)?;
        let mut named = serde_json::Map::new();
        for (name, (size, distance)) in &collection.vectors {
            named.insert(name.clone(), json!({"size": size, "distance": distance}));
        }
        Some(Value::Object(named))
    }

    fn handle(&self, method: &str, path: &str, body: &Value) -> (u16, Value) {
        let mut store = self.store.lock().expect("mock store");
        let not_found = || {
            (
                404,
                json!({"status": {"error": "Not found"}, "result": null}),
            )
        };
        let ok = |result: Value| (200, json!({"result": result, "status": "ok"}));

        // /collections[/{name}][/points[/search|/delete]]
        let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
        match (method, segments.as_slice()) {
            ("GET", ["collections", name]) => match store.collections.get(*name) {
                Some(collection) => {
                    let mut named = serde_json::Map::new();
                    for (vector_name, (size, distance)) in &collection.vectors {
                        named.insert(
                            vector_name.clone(),
                            json!({"size": size, "distance": distance}),
                        );
                    }
                    ok(json!({
                        "config": {"params": {"vectors": Value::Object(named)}},
                    }))
                }
                None => not_found(),
            },
            ("PUT", ["collections", name]) => {
                let collection = store.collections.entry((*name).to_string()).or_default();
                if let Some(vectors) = body.get("vectors").and_then(Value::as_object) {
                    for (vector_name, params) in vectors {
                        collection.vectors.insert(
                            vector_name.clone(),
                            (
                                params.get("size").and_then(Value::as_u64).unwrap_or(0) as usize,
                                params
                                    .get("distance")
                                    .and_then(Value::as_str)
                                    .unwrap_or("Cosine")
                                    .to_string(),
                            ),
                        );
                    }
                }
                ok(json!(true))
            }
            ("PUT", ["collections", name, "points"]) => {
                let Some(collection) = store.collections.get_mut(*name) else {
                    return not_found();
                };
                for point in body
                    .get("points")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let id = point.get("id").cloned().unwrap_or(Value::Null);
                    let mut vectors = BTreeMap::new();
                    if let Some(map) = point.get("vector").and_then(Value::as_object) {
                        for (vector_name, values) in map {
                            let vector: Vec<f32> = values
                                .as_array()
                                .map(|items| {
                                    items
                                        .iter()
                                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                                        .collect()
                                })
                                .unwrap_or_default();
                            vectors.insert(vector_name.clone(), vector);
                        }
                    }
                    let payload = point.get("payload").cloned().unwrap_or(Value::Null);
                    if let Some(slot) = collection
                        .points
                        .iter_mut()
                        .find(|(existing, _, _)| *existing == id)
                    {
                        slot.1 = payload;
                        slot.2 = vectors;
                    } else {
                        collection.points.push((id, payload, vectors));
                    }
                }
                ok(json!({"operation_id": 0, "status": "completed"}))
            }
            ("POST", ["collections", name, "points", "search"]) => {
                let Some(collection) = store.collections.get(*name) else {
                    return not_found();
                };
                let vector_name = body
                    .pointer("/vector/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some((size, distance)) = collection.vectors.get(vector_name).cloned() else {
                    return (
                        400,
                        json!({"status": {"error": format!("vector {vector_name:?} not configured")}}),
                    );
                };
                let query: Vec<f32> = body
                    .pointer("/vector/vector")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect()
                    })
                    .unwrap_or_default();
                if query.len() != size {
                    return (
                        400,
                        json!({"status": {"error": format!("dimension mismatch: expected {size}")}}),
                    );
                }
                let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(10) as usize;
                let with_payload = body
                    .get("with_payload")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                type Scored<'a> = Vec<(f64, &'a Value, &'a Value, &'a BTreeMap<String, Vec<f32>>)>;
                let mut scored: Scored<'_> = collection
                    .points
                    .iter()
                    .filter_map(|(id, payload, vectors)| {
                        let vector = vectors.get(vector_name)?;
                        let score = match distance.as_str() {
                            // Qdrant answers cosine searches with the
                            // similarity, euclid with the distance.
                            "Cosine" => (1.0 - cosine_distance(&query, vector)) as f64,
                            "Euclid" => euclidean_distance(&query, vector) as f64,
                            "Dot" => dot_product(&query, vector) as f64,
                            _ => return None,
                        };
                        Some((score, id, payload, vectors))
                    })
                    .collect();
                // Qdrant answers best-first: similarity metrics sort
                // their score descending, the euclidean distance sorts
                // ascending (smaller = closer).
                if distance == "Euclid" {
                    scored
                        .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    scored
                        .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                }
                let result: Vec<Value> = scored
                    .into_iter()
                    .take(limit)
                    .map(|(score, id, payload, vectors)| {
                        let mut stored = serde_json::Map::new();
                        for (vector_name, vector) in vectors {
                            stored.insert(vector_name.clone(), json!(vector));
                        }
                        let mut hit = json!({
                            "id": id,
                            "version": 1,
                            "score": score,
                            "vector": Value::Object(stored),
                        });
                        if with_payload {
                            hit["payload"] = payload.clone();
                        }
                        hit
                    })
                    .collect();
                ok(Value::Array(result))
            }
            ("POST", ["collections", name, "points", "delete"]) => {
                let Some(collection) = store.collections.get_mut(*name) else {
                    return not_found();
                };
                let ids: Vec<Value> = body
                    .get("points")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                collection
                    .points
                    .retain(|(existing, _, _)| !ids.contains(existing));
                ok(json!({"operation_id": 0, "status": "completed"}))
            }
            _ => not_found(),
        }
    }
}

fn respond(status: u16, body: &Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        status,
        if status == 200 { "OK" } else { "Error" },
        body.len()
    )
}

/// Boots the mock on an ephemeral port; returns the base URL and the
/// shared state handle.
pub async fn spawn_qdrant() -> (String, Arc<QdrantMock>) {
    let mock = Arc::new(QdrantMock {
        store: Mutex::new(Store::default()),
        last_headers: Mutex::new(Vec::new()),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let address = listener.local_addr().expect("local addr");
    let server = mock.clone();
    // The listener moves into the task: it lives for the process
    // lifetime, so a dropped factory result never kills the server.
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

async fn serve(mock: Arc<QdrantMock>, mut stream: tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Read the header block, then the body per its Content-Length.
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
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

    // Record the request headers (the api-key pin reads them).
    let parsed_headers: Vec<(String, String)> = headers_text
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect();
    *mock.last_headers.lock().expect("mock headers") = parsed_headers;

    let request_line = headers_text.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let path = target.split('?').next().unwrap_or_default().to_string();
    let body: Value = serde_json::from_slice(&buffer[header_end + 4..]).unwrap_or(Value::Null);

    let (status, response) = mock.handle(&method, &path, &body);
    let _ = stream
        .write_all(respond(status, &response).as_bytes())
        .await;
    let _ = stream.flush().await;
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

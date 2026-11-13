//! An in-process mock of the Milvus v2 RESTful subset the engine
//! speaks — collections create/describe/list, entity insert/delete,
//! brute-force search with real distance math scoring Milvus-style
//! (COSINE answers `1 − cos`, L2 the raw distance, IP the raw
//! product). One mock per test crate; the listener rides a spawned
//! task for the process lifetime.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rushwind_vector::{cosine_distance, dot_product, euclidean_distance};
use serde_json::{json, Value};

#[derive(Default)]
struct Collection {
    metric: String,
    dimension: usize,
    /// id → vector.
    points: BTreeMap<String, Vec<f32>>,
}

#[derive(Default)]
struct Store {
    collections: BTreeMap<String, Collection>,
    last_bearer: Mutex<String>,
}

pub struct MilvusMock {
    store: Mutex<Store>,
}

impl MilvusMock {
    pub fn last_bearer(&self) -> String {
        self.store
            .lock()
            .expect("mock store")
            .last_bearer
            .lock()
            .expect("mock bearer")
            .clone()
    }

    pub fn collection_names(&self) -> Vec<String> {
        self.store
            .lock()
            .expect("mock store")
            .collections
            .keys()
            .cloned()
            .collect()
    }

    pub fn collection_dimension(&self, name: &str) -> Option<usize> {
        self.store
            .lock()
            .expect("mock store")
            .collections
            .get(name)
            .map(|c| c.dimension)
    }

    fn handle(&self, path: &str, bearer: &str, body: &Value) -> (u16, Value) {
        {
            let store = self.store.lock().expect("mock store");
            *store.last_bearer.lock().expect("mock bearer") = bearer.to_string();
        }
        let ok = |result: Value| (200, json!({"code": 0, "data": result}));
        let fail = |code: i64, message: &str| (200, json!({"code": code, "message": message}));
        let mut store = self.store.lock().expect("mock store");
        match path {
            "/v2/vectordb/collections/create" => {
                let name = body
                    .get("collectionName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let metric = body
                    .get("metricType")
                    .and_then(Value::as_str)
                    .unwrap_or("COSINE");
                let dimension = body
                    .get("dimension")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                if store.collections.contains_key(name) {
                    return fail(1100, "collection already exists");
                }
                store.collections.insert(
                    name.to_string(),
                    Collection {
                        metric: metric.to_string(),
                        dimension,
                        points: BTreeMap::new(),
                    },
                );
                ok(json!({}))
            }
            "/v2/vectordb/collections/describe" => {
                let name = body
                    .get("collectionName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match store.collections.get(name) {
                    Some(collection) => ok(json!({
                        "collectionName": name,
                        "fields": [{
                            "name": "id",
                            "type": "VarChar",
                        }, {
                            "name": "vector",
                            "type": "FloatVector",
                            "params": {"dimension": collection.dimension},
                        }],
                        "metricType": collection.metric,
                    })),
                    None => fail(100, "can't find collection"),
                }
            }
            "/v2/vectordb/collections/list" => {
                ok(json!(store.collections.keys().cloned().collect::<Vec<_>>()))
            }
            "/v2/vectordb/entities/insert" => {
                let name = body
                    .get("collectionName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(collection) = store.collections.get_mut(name) else {
                    return fail(100, "can't find collection");
                };
                for point in body
                    .get("data")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let id = point
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let vector: Vec<f32> = point
                        .get("vector")
                        .and_then(Value::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|v| v.as_f64().map(|f| f as f32))
                                .collect()
                        })
                        .unwrap_or_default();
                    collection.points.insert(id, vector);
                }
                ok(
                    json!({"insertCount": body.get("data").and_then(Value::as_array).map(|a| a.len()).unwrap_or_default()}),
                )
            }
            "/v2/vectordb/entities/delete" => {
                let name = body
                    .get("collectionName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(collection) = store.collections.get_mut(name) else {
                    return fail(100, "can't find collection");
                };
                let filter = body
                    .get("filter")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(id) = filter
                    .strip_prefix("id == \"")
                    .and_then(|rest| rest.strip_suffix('"'))
                else {
                    return fail(1800, "unsupported filter");
                };
                collection.points.remove(id);
                ok(json!({"deleteCount": 1}))
            }
            "/v2/vectordb/entities/search" => {
                let name = body
                    .get("collectionName")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(collection) = store.collections.get(name) else {
                    return fail(100, "can't find collection");
                };
                let query: Vec<f32> = body
                    .pointer("/data/0")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect()
                    })
                    .unwrap_or_default();
                if query.len() != collection.dimension {
                    return fail(
                        1100,
                        &format!("dimension mismatch: expected {}", collection.dimension),
                    );
                }
                let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(10) as usize;
                let mut scored: Vec<(f64, &String, &Vec<f32>)> = collection
                    .points
                    .iter()
                    .map(|(id, vector)| {
                        // Milvus's COSINE face answers the distance
                        // 1 − cos (smallest-first), L2 the raw distance,
                        // IP the raw product (largest-first).
                        let score = match collection.metric.as_str() {
                            "COSINE" => cosine_distance(&query, vector) as f64,
                            "L2" => euclidean_distance(&query, vector) as f64,
                            "IP" => dot_product(&query, vector) as f64,
                            _ => 0.0,
                        };
                        (score, id, vector)
                    })
                    .collect();
                // Milvus ranks smallest-distance-first for COSINE/L2 and
                // largest-IP-first for IP.
                if collection.metric == "IP" {
                    scored
                        .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    scored
                        .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                }
                let result: Vec<Value> = scored
                    .into_iter()
                    .take(limit)
                    .map(|(distance, id, vector)| {
                        json!({"id": id, "distance": distance, "vector": vector})
                    })
                    .collect();
                ok(Value::Array(result))
            }
            _ => fail(404, "unknown endpoint"),
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

/// Boots the mock; returns the base URL and the shared handle.
pub async fn spawn_milvus() -> (String, Arc<MilvusMock>) {
    let mock = Arc::new(MilvusMock {
        store: Mutex::new(Store::default()),
    });
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

async fn serve(mock: Arc<MilvusMock>, mut stream: tokio::net::TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

//! An in-process mock of the Neo4j HTTP transactional subset the
//! engine speaks — SHOW INDEXES, vector index creation, MERGE upsert,
//! kNN query, and DELETE — with brute-force search scoring
//! Neo4j-style: cosine similarity in [-1,1] (bigger-better), euclidean
//! distance, dot raw product.

#![cfg(test)]
// The shared mock compiles into every test crate; the pins a given
// crate does not use read as dead code there.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rushwind_vector::{cosine_distance, dot_product, euclidean_distance};
use serde_json::{json, Value};

#[derive(Default)]
pub struct Neo4jMock {
    indexes: Mutex<BTreeMap<String, (usize, String)>>,
    nodes: Mutex<BTreeMap<String, BTreeMap<String, Vec<f32>>>>,
    last_auth: Mutex<String>,
}

impl Neo4jMock {
    pub fn last_auth(&self) -> String {
        self.last_auth.lock().expect("mock auth").clone()
    }

    pub fn label_index_dimension(&self, label: &str) -> Option<usize> {
        self.indexes
            .lock()
            .expect("mock indexes")
            .get(label)
            .map(|(d, _)| *d)
    }

    fn run_statement(&self, statement: &str, params: &Value, auth: &str) -> Result<Value, String> {
        *self.last_auth.lock().expect("mock auth") = auth.to_string();
        let trimmed = statement.trim();
        if trimmed.starts_with("SHOW INDEXES") {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let indexes = self.indexes.lock().expect("mock indexes");
            if trimmed.contains("vector.dimensions") {
                let dimension = indexes.get(name).map(|(d, _)| json!(d));
                return Ok(json!(match dimension {
                    Some(d) => vec![json!({"row": [d]})],
                    None => vec![],
                }));
            }
            let count = indexes.contains_key(name) as i64;
            return Ok(json!([{"row": [count]}]));
        }
        if trimmed.starts_with("CALL db.index.vector.createNodeIndex") {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let dimension = params
                .get("dimension")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize;
            let similarity = params
                .get("similarity")
                .and_then(Value::as_str)
                .unwrap_or("cosine");
            self.indexes
                .lock()
                .expect("mock indexes")
                .insert(name.to_string(), (dimension, similarity.to_string()));
            return Ok(json!([]));
        }
        if trimmed.starts_with("MERGE") {
            let label = extract_label(trimmed).ok_or("no label")?;
            let id = params.get("id").and_then(Value::as_str).unwrap_or_default();
            let vector: Vec<f32> = params
                .get("vector")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                        .collect()
                })
                .unwrap_or_default();
            let index_name = format!("{label}_index");
            let Some((dimension, _)) = self
                .indexes
                .lock()
                .expect("mock indexes")
                .get(&index_name)
                .cloned()
            else {
                return Err(format!("there is no index named {index_name}"));
            };
            if vector.len() != dimension {
                return Err(format!(
                    "the provided vector has length {}, but the index expects dimension {}",
                    vector.len(),
                    dimension
                ));
            }
            self.nodes
                .lock()
                .expect("mock nodes")
                .entry(label)
                .or_default()
                .insert(id.to_string(), vector);
            return Ok(json!([]));
        }
        if trimmed.starts_with("CALL db.index.vector.queryNodes") {
            let (index_name, _) = extract_quoted_after(trimmed, "queryNodes('")
                .ok_or_else(|| "no index name".to_string())?;
            let topk = params.get("topk").and_then(Value::as_u64).unwrap_or(10) as usize;
            let query: Vec<f32> = params
                .get("vector")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                        .collect()
                })
                .unwrap_or_default();
            let indexes = self.indexes.lock().expect("mock indexes");
            let Some((dimension, similarity)) = indexes.get(&index_name).cloned() else {
                return Err(format!("there is no index named {index_name}"));
            };
            let label = index_name.trim_end_matches("_index");
            let nodes = self.nodes.lock().expect("mock nodes");
            let bucket = nodes.get(label);
            let mut scored: Vec<(f64, String)> = bucket
                .map(|bucket| {
                    bucket
                        .iter()
                        .filter(|(_, vector)| vector.len() == dimension)
                        .map(|(id, vector)| {
                            let score = match similarity.as_str() {
                                "cosine" => (1.0 - cosine_distance(&query, vector)) as f64,
                                "euclidean" => euclidean_distance(&query, vector) as f64,
                                "dot" => dot_product(&query, vector) as f64,
                                _ => 0.0,
                            };
                            (score, id.clone())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            // Cosine similarity and dot rank largest-first; the
            // euclidean distance ranks smallest-first.
            if similarity == "euclidean" {
                scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            } else {
                scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            }
            let rows: Vec<Value> = scored
                .into_iter()
                .take(topk)
                .map(|(score, id)| json!({"row": [id, score]}))
                .collect();
            return Ok(json!(rows));
        }
        if trimmed.starts_with("MATCH") {
            let label = extract_label(trimmed).ok_or("no label")?;
            let id = params.get("id").and_then(Value::as_str).unwrap_or_default();
            {
                let mut guard = self.nodes.lock().expect("mock nodes");
                let Some(bucket) = guard.get_mut(&label) else {
                    return Ok(json!([]));
                };
                bucket.remove(id);
            }
            return Ok(json!([]));
        }
        Err("unsupported statement".to_string())
    }
}

fn extract_label(statement: &str) -> Option<String> {
    let at = statement.find("(n:").map(|at| at + 3)?;
    let tail = &statement[at..];
    let end = tail
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

fn extract_quoted_after(statement: &str, marker: &str) -> Option<(String, usize)> {
    let at = statement.find(marker)? + marker.len();
    let tail = &statement[at..];
    let end = tail.find('\'')?;
    Some((tail[..end].to_string(), end))
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

/// Boots the mock; returns the base URL and the shared handle.
pub async fn spawn_neo4j() -> (String, Arc<Neo4jMock>) {
    let mock = Arc::new(Neo4jMock::default());
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

async fn serve(mock: Arc<Neo4jMock>, mut stream: tokio::net::TcpStream) {
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
    let authorization = headers_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("authorization")
                .then(|| value.trim().to_string())
        })
        .unwrap_or_default();

    let body: Value = serde_json::from_slice(&buffer[header_end + 4..]).unwrap_or(Value::Null);
    let statement = body
        .pointer("/statements/0/statement")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let params = body
        .pointer("/statements/0/parameters")
        .cloned()
        .unwrap_or(Value::Null);

    let response = match mock.run_statement(&statement, &params, &authorization) {
        Ok(rows) => json!({"results": [{"data": rows}], "errors": []}),
        Err(message) => json!({"results": [], "errors": [{"message": message}]}),
    };
    let _ = stream.write_all(respond(201, &response).as_bytes()).await;
    let _ = stream.flush().await;
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

//! Neo4j engine for the RushWind vector contract — the HTTP
//! transactional Cypher API over hand-written reqwest, no SDK.
//!
//! # The mapping
//!
//! Neo4j's vector indexes are parameterized per label+property at
//! creation (`db.index.vector.createNodeIndex` with `vector.dimension`
//! and `vector.similarity`), one similarity per index — so the same
//! resolution applies as qdrant/Milvus/Weaviate: each contract field
//! stores under **three metric-suffixed labels**
//! (`{Collection}_{field}_{metric}` — Neo4j labels are CamelCase and
//! alphanumeric), each backed by its own vector index. A search
//! addresses the suffix its metric names; 3× storage is the
//! documented price of per-query metric choice.
//!
//! - **Dimension discipline**: the index creation statement pins the
//!   dimension server-side (`vector.dimension`); a mismatched write is
//!   refused by the database itself and the engine maps that error to
//!   the typed [`VectorError::DimensionMismatch`] — the server's
//!   truth, no client shadow state.
//! - **Similarity faces**: Neo4j vector indexes run `cosine`
//!   (`db.index.vector.queryNodes` returns a similarity in [-1, 1],
//!   already bigger-better — the score passes through), `euclidean`
//!   (a distance in [0, ∞), converted), and `dot` (returned
//!   similarity is the raw product, bigger-better).
//! - **Ids**: the node's `contractId` property carries the contract's
//!   string id verbatim; nodes are `(:Node {contractId, embedding})`.
//!   A MERGE on `contractId` makes the upsert replace. Absent
//!   collections answer the memory engine's empty page (the kNN query
//!   against a nonexistent index is caught and mapped); deletes MATCH
//!   by `contractId` and DETACH DELETE across every suffix label.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::time::Duration;

use rushwind_vector::{
    distance_to_score, BoxFuture, DistanceMetric, Hit, SearchResult, VectorError, VectorIndex,
    VectorQuery,
};
use serde_json::json;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct Neo4jOptions {
    /// The Neo4j HTTP base URL (`http://127.0.0.1:7474`).
    pub url: String,
    /// The database name. Default `neo4j`.
    pub database: String,
    /// The basic-auth username.
    pub username: String,
    /// The basic-auth password.
    pub password: String,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for Neo4jOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            database: "neo4j".to_string(),
            username: String::new(),
            password: String::new(),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The label suffix per contract metric.
fn metric_suffix(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "Cosine",
        DistanceMetric::Euclidean => "Euclidean",
        DistanceMetric::Dot => "Dot",
    }
}

/// The Neo4j similarity name per contract metric.
fn neo4j_similarity(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::Dot => "dot",
    }
}

/// The label per contract collection/field/metric — CamelCase
/// alphanumeric, Neo4j's label alphabet.
fn label(collection: &str, field: &str, metric: DistanceMetric) -> String {
    let raw = format!("{}_{}_{}", collection, field, metric_suffix(metric));
    let mut out = String::new();
    let mut upper_next = true;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            if upper_next {
                out.extend(c.to_uppercase());
                upper_next = false;
            } else {
                out.push(c);
            }
        } else {
            upper_next = true;
        }
    }
    out
}

/// The engine.
#[derive(Debug)]
pub struct Neo4jVectorIndex {
    options: Neo4jOptions,
    http: reqwest::Client,
    /// The labels each contract collection has spawned — Cypher labels
    /// cannot be prefix-matched, so the delete sweep reads this
    /// registry (seeded at upsert) instead of guessing.
    labels:
        std::sync::Mutex<std::collections::BTreeMap<String, std::collections::BTreeSet<String>>>,
}

impl Neo4jVectorIndex {
    /// Builds the engine; fails when the URL is unset.
    pub fn new(options: Neo4jOptions) -> Result<Self, VectorError> {
        if options.url.trim().is_empty() {
            return Err(VectorError::InvalidQuery("neo4j: url unset".to_string()));
        }
        Ok(Self {
            options: Neo4jOptions {
                url: options.url.trim_end_matches('/').to_string(),
                ..options
            },
            http: reqwest::Client::new(),
            labels: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`url`, `database`, `username`, `password`,
    /// `timeout_ms`), unknown fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, VectorError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            url: String,
            database: Option<String>,
            username: Option<String>,
            password: Option<String>,
            timeout_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| VectorError::InvalidQuery(format!("neo4j settings: {e}")))?;
        Self::new(Neo4jOptions {
            url: wire.url,
            database: wire.database.unwrap_or_else(|| "neo4j".to_string()),
            username: wire.username.unwrap_or_default(),
            password: wire.password.unwrap_or_default(),
            request_timeout: wire
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(10)),
        })
    }

    /// The dimension the named index was created with, read off the
    /// server's index metadata (`vector.dimensions`) — `None` when the
    /// index does not exist.
    async fn index_dimension(&self, index_name: &str) -> Result<Option<usize>, VectorError> {
        let rows = self
            .run(
                "SHOW INDEXES YIELD name, type, vector.dimensions WHERE name = $name AND type = 'VECTOR' RETURN vector.dimensions AS d",
                json!({"name": index_name}),
                &format!("index dims {index_name}"),
            )
            .await
            .map_err(|e| VectorError::Failed(format!("neo4j index dims: {e}")))?;
        Ok(rows
            .first()
            .and_then(|row| row.get(0).cloned())
            .map(|cell| {
                cell.as_array()
                    .and_then(|values| values.first().cloned())
                    .unwrap_or_else(|| cell.clone())
            })
            .and_then(|value| value.as_i64())
            .map(|d| d as usize))
    }

    /// Runs one Cypher statement through the transactional endpoint
    /// (autocommit form). Returns the data rows.
    async fn run(
        &self,
        statement: &str,
        params: serde_json::Value,
        context: &str,
    ) -> Result<Vec<serde_json::Value>, VectorError> {
        let url = format!(
            "{}/db/{}/tx/commit",
            self.options.url, self.options.database
        );
        let body = json!({
            "statements": [{
                "statement": statement,
                "parameters": params,
            }]
        });
        let mut request = self
            .http
            .post(&url)
            .timeout(self.options.request_timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(
                serde_json::to_vec(&body)
                    .map_err(|e| VectorError::Failed(format!("neo4j {context} encode: {e}")))?,
            );
        if !self.options.username.is_empty() {
            request = request.basic_auth(&self.options.username, Some(&self.options.password));
        }
        let response = request
            .send()
            .await
            .map_err(|e| VectorError::Failed(format!("neo4j {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| VectorError::Failed(format!("neo4j {context}: {e}")))?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| VectorError::Failed(format!("neo4j {context}: {e}")))?;
        // Cypher errors ride a 200 with `errors: [...]`; a database-side
        // dimension mismatch names the index in the message.
        if let Some(errors) = body.get("errors").and_then(serde_json::Value::as_array) {
            if let Some(first) = errors.first() {
                let message = first
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if message.contains("dimension") {
                    // Extract both numbers from the message when the
                    // server names them; the typed error carries the
                    // query's length against the index's configured
                    // dimension read from the message when present.
                    return Err(VectorError::DimensionMismatch {
                        expected: extract_dimension(&message).unwrap_or(0),
                        got: extract_got(&message).unwrap_or(0),
                    });
                }
                return Err(VectorError::Failed(format!("neo4j {context}: {message}")));
            }
        }
        if !status.is_success() {
            return Err(VectorError::Failed(format!(
                "neo4j {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        let rows = body
            .pointer("/results/0/data")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(rows
            .into_iter()
            .filter_map(|row| row.get("row").cloned())
            .collect())
    }
}

/// The index dimension named in a server error message, when present.
fn extract_dimension(message: &str) -> Option<usize> {
    for (marker, take) in [("expected dimension", 1), ("dimension ", 1)] {
        if let Some(at) = message.find(marker) {
            let tail = &message[at + marker.len()..];
            let digits: String = tail
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<usize>() {
                let _ = take;
                return Some(n);
            }
        }
    }
    None
}

/// The offending vector length named in a server error message.
fn extract_got(message: &str) -> Option<usize> {
    for marker in ["got", "but got", "provided vector length", "has length "] {
        if let Some(at) = message.rfind(marker) {
            let tail = &message[at + marker.len()..];
            let digits: String = tail
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<usize>() {
                return Some(n);
            }
        }
    }
    None
}

impl VectorIndex for Neo4jVectorIndex {
    fn upsert<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        field: &'a str,
        vector: &'a [f32],
    ) -> BoxFuture<'a, Result<(), VectorError>> {
        Box::pin(async move {
            if vector.is_empty() {
                return Err(VectorError::InvalidQuery("vector is empty".to_string()));
            }
            if id.is_empty() {
                return Err(VectorError::InvalidQuery("id is empty".to_string()));
            }
            let db = self.options.database.clone();
            for metric in [
                DistanceMetric::Cosine,
                DistanceMetric::Euclidean,
                DistanceMetric::Dot,
            ] {
                let node_label = label(collection, field, metric);
                let index_name = format!("{node_label}_index");
                // The index exists-check rides the SHOW INDEXES
                // statement; births are once-per-lifetime statements.
                let known = self
                    .run(
                        "SHOW INDEXES YIELD name, type WHERE name = $name AND type = 'VECTOR' RETURN count(*) AS c",
                        json!({"name": index_name}),
                        &format!("index check {index_name}"),
                    )
                    .await;
                let exists = known
                    .ok()
                    .and_then(|rows| rows.first().cloned())
                    .and_then(|row| {
                        row.get(0).map(|cell| {
                            // The RETURN count(*) row is `[count]` — the
                            // cell is the array, the count its head.
                            cell.as_array()
                                .and_then(|values| values.first().cloned())
                                .unwrap_or_else(|| cell.clone())
                        })
                    })
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    > 0;
                let _ = db;
                if !exists {
                    let similarity = neo4j_similarity(metric);
                    self.run(
                        &format!(
                            "CALL db.index.vector.createNodeIndex($name, '{node_label}', 'embedding', $dimension, {{similarityFunction: '{similarity}'}})"
                        ),
                        json!({
                            "name": index_name,
                            "dimension": vector.len(),
                            "similarity": neo4j_similarity(metric),
                        }),
                        &format!("index create {index_name}"),
                    )
                    .await?;
                }
                self.run(
                    &format!(
                        "MERGE (n:{node_label} {{contractId: $id}}) SET n.embedding = $vector"
                    ),
                    json!({"id": id, "vector": vector}),
                    &format!("merge {node_label}"),
                )
                .await?;
                self.labels
                    .lock()
                    .expect("label registry poisoned")
                    .entry(collection.to_string())
                    .or_default()
                    .insert(node_label);
            }
            Ok(())
        })
    }

    fn search<'a>(
        &'a self,
        collection: &'a str,
        query: &'a VectorQuery,
    ) -> BoxFuture<'a, Result<SearchResult, VectorError>> {
        Box::pin(async move {
            query.validate()?;
            let node_label = label(collection, &query.field, query.metric);
            let index_name = format!("{node_label}_index");
            match self.index_dimension(&index_name).await? {
                None => return Ok(SearchResult::default()),
                Some(size) if size != query.vector.len() => {
                    return Err(VectorError::DimensionMismatch {
                        expected: size,
                        got: query.vector.len(),
                    });
                }
                Some(_) => {}
            }
            let rows = match self
                .run(
                    &format!(
                        "CALL db.index.vector.queryNodes('{index_name}', $topk, $vector) YIELD node, score RETURN node.contractId AS id, score"
                    ),
                    json!({"topk": query.top_k, "vector": query.vector}),
                    &format!("knn {node_label}"),
                )
                .await
            {
                Ok(rows) => rows,
                // A nonexistent index answers an empty page — the memory
                // engine's semantics (the query refused nothing that
                // exists).
                Err(VectorError::Failed(msg))
                    if msg.contains("does not exist")
                        || msg.contains("not found")
                        || msg.contains("no such index")
                        || msg.contains("no index named") =>
                {
                    return Ok(SearchResult::default());
                }
                Err(e) => return Err(e),
            };
            let mut hits = Vec::new();
            for row in rows {
                let values = match row.as_array() {
                    Some(values) if values.len() >= 2 => values,
                    _ => continue,
                };
                let Some(id) = values[0].as_str() else {
                    continue;
                };
                let Some(similarity) = values[1].as_f64() else {
                    continue;
                };
                let score = match query.metric {
                    // Cosine similarity passes through; dot is the raw
                    // product; euclidean arrives as a distance.
                    DistanceMetric::Cosine | DistanceMetric::Dot => similarity as f32,
                    DistanceMetric::Euclidean => {
                        distance_to_score(DistanceMetric::Euclidean, similarity as f32)
                    }
                };
                hits.push(Hit {
                    id: id.to_string(),
                    score,
                    vector: Vec::new(),
                });
            }
            hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.id.cmp(&b.id))
            });
            let total = hits.len();
            Ok(SearchResult { hits, total })
        })
    }

    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<(), VectorError>> {
        Box::pin(async move {
            // The delete contract removes the record **across fields**;
            // the sweep walks every label the collection has spawned
            // (seeded at upsert) and DETACH DELETEs by contractId.
            let labels: Vec<String> = {
                let registry = self.labels.lock().expect("label registry poisoned");
                registry
                    .get(collection)
                    .map(|set| set.iter().cloned().collect())
                    .unwrap_or_default()
            };
            for node_label in labels {
                self.run(
                    &format!("MATCH (n:{node_label} {{contractId: $id}}) DETACH DELETE n"),
                    json!({"id": id}),
                    &format!("delete {node_label}"),
                )
                .await?;
            }
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>> {
        Box::pin(async { Ok(()) })
    }
}

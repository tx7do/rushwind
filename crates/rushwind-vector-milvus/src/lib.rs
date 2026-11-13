//! Milvus engine for the RushWind vector contract — the v2 RESTful
//! API over hand-written reqwest, no SDK.
//!
//! # The mapping
//!
//! Milvus fixes a metric per collection exactly as qdrant fixes one
//! per named vector, so the engine applies the same resolution: each
//! contract field stores under **three metric-suffixed collections**
//! (`{collection}__cosine/__euclidean/__dot`, the same vectors three
//! times, each carrying its own metric), and a search addresses the
//! suffix its metric names — server-side top-k correct under every
//! contract metric at the documented 3×-storage cost. A pre-created
//! bare-named collection works in single-metric mode, refusing
//! cross-metric queries rather than silently re-ranking.
//!
//! - **Dimension discipline** reads the collection's describe response
//!   (`fields` → the vector field's `params.dimension`) — the server's
//!   truth, no client shadow state. The first upsert into a missing
//!   collection births all three suffixes at the upserted dimension
//!   (`idType: Varchar`, so contract string ids ride verbatim).
//! - **Scores** — Milvus answers COSINE searches with `1 − cos` (a
//!   distance, converted through [`rushwind_vector::distance_to_score`]),
//!   L2 with the raw distance (converted), IP with the inner product
//!   (bigger-better already). The engine re-sorts descending with the
//!   memory engine's exact id-ascending tiebreak.
//! - Absent collections and unseeded fields answer the memory engine's
//!   empty page; deletes of missing ids are Milvus's own no-op.

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
pub struct MilvusOptions {
    /// The Milvus REST base URL (`http://127.0.0.1:19530`).
    pub url: String,
    /// The bearer token (Milvus's token auth or a gateway's JWT),
    /// when the deployment requires one.
    pub token: Option<String>,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for MilvusOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: None,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The Milvus vector-index engine.
#[derive(Debug)]
pub struct MilvusVectorIndex {
    options: MilvusOptions,
    http: reqwest::Client,
}

/// The metric names the engine's collection suffixes speak.
fn metric_suffix(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::Dot => "dot",
    }
}

/// The Milvus metric name per contract metric.
fn milvus_metric(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "COSINE",
        DistanceMetric::Euclidean => "L2",
        DistanceMetric::Dot => "IP",
    }
}

impl MilvusVectorIndex {
    /// Builds the engine; fails when the URL is unset.
    pub fn new(options: MilvusOptions) -> Result<Self, VectorError> {
        if options.url.trim().is_empty() {
            return Err(VectorError::InvalidQuery("milvus: url unset".to_string()));
        }
        Ok(Self {
            options: MilvusOptions {
                url: options.url.trim_end_matches('/').to_string(),
                ..options
            },
            http: reqwest::Client::new(),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`url`, `token`, `timeout_ms`), unknown fields
    /// ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, VectorError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            url: String,
            token: Option<String>,
            timeout_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| VectorError::InvalidQuery(format!("milvus settings: {e}")))?;
        Self::new(MilvusOptions {
            url: wire.url,
            token: wire.token,
            request_timeout: wire
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(10)),
        })
    }

    /// Posts one v2 endpoint, requires `code: 0`, returns the body.
    async fn post_v2(
        &self,
        path: &str,
        body: serde_json::Value,
        context: &str,
    ) -> Result<serde_json::Value, VectorError> {
        let url = format!("{}{}", self.options.url, path);
        let mut request = self
            .http
            .post(&url)
            .timeout(self.options.request_timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(
                serde_json::to_vec(&body)
                    .map_err(|e| VectorError::Failed(format!("milvus {context} encode: {e}")))?,
            );
        if let Some(token) = &self.options.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| VectorError::Failed(format!("milvus {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| VectorError::Failed(format!("milvus {context}: {e}")))?;
        if !status.is_success() {
            return Err(VectorError::Failed(format!(
                "milvus {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| VectorError::Failed(format!("milvus {context}: {e}")))?;
        let code = body.get("code").and_then(serde_json::Value::as_i64);
        match code {
            Some(0) => Ok(body),
            // Milvus answers absent collections with a nonzero code and
            // a "collection not found" message — the absent answer.
            _ => {
                let message = body
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                if message.contains("not found") || message.contains("can't find collection") {
                    return Err(VectorError::Failed("milvus: not found".to_string()));
                }
                Err(VectorError::Failed(format!(
                    "milvus {context}: code {}: {message}",
                    code.unwrap_or_default()
                )))
            }
        }
    }

    /// Whether the suffixed collection exists — describe errors read
    /// as absent.
    async fn exists(&self, collection: &str) -> Result<bool, VectorError> {
        let outcome = self
            .post_v2(
                "/v2/vectordb/collections/describe",
                json!({"collectionName": collection}),
                &format!("describe {collection}"),
            )
            .await;
        match outcome {
            Ok(_) => Ok(true),
            Err(VectorError::Failed(msg)) if msg.contains("not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// The vector dimension of an existing collection, read from its
    /// describe response's fields.
    async fn dimension_of(&self, collection: &str) -> Result<Option<usize>, VectorError> {
        let info = self
            .post_v2(
                "/v2/vectordb/collections/describe",
                json!({"collectionName": collection}),
                &format!("describe {collection}"),
            )
            .await?;
        let fields = info
            .pointer("/data/fields")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        for field in fields {
            if field.get("type").and_then(serde_json::Value::as_str) == Some("FloatVector") {
                return Ok(field
                    .pointer("/params/dimension")
                    .and_then(serde_json::Value::as_u64)
                    .map(|d| d as usize));
            }
        }
        Ok(None)
    }

    /// Creates one suffixed collection at `dimension`.
    async fn create_collection(
        &self,
        collection: &str,
        metric: DistanceMetric,
        dimension: usize,
    ) -> Result<(), VectorError> {
        self.post_v2(
            "/v2/vectordb/collections/create",
            json!({
                "collectionName": collection,
                "dimension": dimension,
                "metricType": milvus_metric(metric),
                "idType": "Varchar",
                "maxIdLength": 512,
            }),
            &format!("create {collection}"),
        )
        .await
        .map(|_| ())
    }

    /// The three suffixed names for a contract collection/field pair.
    fn suffixed(collection: &str, field: &str, metric: DistanceMetric) -> String {
        format!("{collection}__{field}__{}", metric_suffix(metric),)
    }
}

impl VectorIndex for MilvusVectorIndex {
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
            for metric in [
                DistanceMetric::Cosine,
                DistanceMetric::Euclidean,
                DistanceMetric::Dot,
            ] {
                let name = Self::suffixed(collection, field, metric);
                match self.exists(&name).await? {
                    false => {
                        self.create_collection(&name, metric, vector.len()).await?;
                    }
                    true => {
                        if let Some(size) = self.dimension_of(&name).await? {
                            if size != vector.len() {
                                return Err(VectorError::DimensionMismatch {
                                    expected: size,
                                    got: vector.len(),
                                });
                            }
                        }
                    }
                }
                self.post_v2(
                    "/v2/vectordb/entities/insert",
                    json!({
                        "collectionName": name,
                        "data": [{"id": id, "vector": vector}],
                    }),
                    &format!("insert {name}"),
                )
                .await?;
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
            let name = Self::suffixed(collection, &query.field, query.metric);
            if !self.exists(&name).await? {
                // The memory engine's absent-collection semantics.
                return Ok(SearchResult::default());
            }
            if let Some(size) = self.dimension_of(&name).await? {
                if query.vector.len() != size {
                    return Err(VectorError::DimensionMismatch {
                        expected: size,
                        got: query.vector.len(),
                    });
                }
            }
            let body = self
                .post_v2(
                    "/v2/vectordb/entities/search",
                    json!({
                        "collectionName": name,
                        "data": [query.vector],
                        "limit": query.top_k,
                        "outputFields": ["id", "vector"],
                    }),
                    &format!("search {name}"),
                )
                .await?;
            let mut hits = Vec::new();
            for point in body
                .pointer("/data")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(id) = point.get("id").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let Some(distance) = point.get("distance").and_then(serde_json::Value::as_f64)
                else {
                    continue;
                };
                let stored = point
                    .get("vector")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let score = match query.metric {
                    // COSINE answers 1 − cos; L2 the raw distance; both
                    // convert. IP arrives as the raw product.
                    DistanceMetric::Cosine | DistanceMetric::Euclidean => {
                        distance_to_score(query.metric, distance as f32)
                    }
                    DistanceMetric::Dot => distance as f32,
                };
                hits.push(Hit {
                    id: id.to_string(),
                    score,
                    vector: stored,
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
            // Milvus's suffix layout puts each field's vectors in their
            // own collections, so the sweep asks the server for its
            // collection list and deletes every suffix child of the
            // contract collection. Absent children are no-ops.
            let listing = self
                .post_v2(
                    "/v2/vectordb/collections/list",
                    json!({}),
                    "list collections",
                )
                .await?;
            let names = listing
                .get("data")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let owned: Vec<String> = names
                .iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .filter(|name| name.starts_with(&format!("{collection}__")))
                .collect();
            for name in owned {
                let outcome = self
                    .post_v2(
                        "/v2/vectordb/entities/delete",
                        json!({
                            "collectionName": name,
                            "filter": format!("id == \"{id}\""),
                        }),
                        &format!("delete {name}"),
                    )
                    .await;
                match outcome {
                    Ok(_) => {}
                    // A vanished mid-delete collection is the no-op.
                    Err(VectorError::Failed(msg)) if msg.contains("not found") => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>> {
        Box::pin(async { Ok(()) })
    }
}

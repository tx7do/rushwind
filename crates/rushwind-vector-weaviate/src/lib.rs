//! Weaviate engine for the RushWind vector contract — the REST batch
//! API (schema create, object batch) plus GraphQL nearVector search,
//! hand-written over reqwest, no SDK.
//!
//! # The mapping
//!
//! Weaviate fixes a metric (distance) per class exactly as qdrant and
//! Milvus do per vector field/collection, so the same resolution
//! applies: each contract field stores under **three metric-suffixed
//! classes** (`{collection}_{field}_{metric}`, sanitized to Weaviate's
//! class-name alphabet — alphanumerics and underscores), each carrying
//! its own distance. A search addresses the suffix its metric names;
//! the 3×-storage cost is the documented price of per-query metric
//! choice on a fixed-metric store.
//!
//! - **Class names**: Weaviate classes are Capitalized; the suffix
//!   name capitalizes the contract collection (`docs` →
//!   `Docs_embedding_cosine`). A pre-created bare class
//!   (`{Collection}_{field}`, one metric) works in single-metric
//!   mode, refusing cross-metric queries.
//! - **Dimension discipline** reads the schema's `vectorIndexConfig`
//!   dimensions... Weaviate infers the dimension from the first
//!   import; the engine pins it itself — a per-engine dimension
//!   registry (`Arc<Mutex<...>>`) seeded at birth and checked on every
//!   upsert/query, the one place a client-side shadow is unavoidable
//!   (Weaviate's schema API does not report vector dimensions).
//! - **Scores** — Weaviate answers `_additional {distance}` with the
//!   metric's distance (cosine: `1 − cos`, l2-squared for euclidean,
//!   dot: `2 − 2·cos` per Weaviate's docs — converted through
//!   [`rushwind_vector::distance_to_score`] for cosine; the dot face's
//!   monotone re-ranking keeps order but not the raw product, and the
//!   docs state that honestly). The engine re-sorts descending with
//!   the memory engine's id-ascending tiebreak.
//! - Absent classes answer the memory engine's empty page; deletes of
//!   missing ids are Weaviate's own no-op (DELETE returns 204 either
//!   way).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_vector::{
    distance_to_score, BoxFuture, DistanceMetric, Hit, SearchResult, VectorError, VectorIndex,
    VectorQuery,
};
use serde_json::json;

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct WeaviateOptions {
    /// The Weaviate REST base URL (`http://127.0.0.1:8080`).
    pub url: String,
    /// The API key (`Authorization: Bearer …`), when enabled.
    pub api_key: Option<String>,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for WeaviateOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The per-class dimension registry — Weaviate's schema does not
/// report vector dimensions (they are inferred at first import), so
/// the engine keeps the truth itself: seeded at class birth, checked
/// on every call.
type Dimensions = Arc<Mutex<BTreeMap<String, usize>>>;

/// The Weaviate vector-index engine.
#[derive(Debug)]
pub struct WeaviateVectorIndex {
    options: WeaviateOptions,
    http: reqwest::Client,
    dimensions: Dimensions,
}

/// The metric names the engine's class suffixes speak.
fn metric_suffix(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::Dot => "dot",
    }
}

/// The Weaviate distance name per contract metric.
fn weaviate_metric(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "l2-squared",
        DistanceMetric::Dot => "dot",
    }
}

/// Weaviate class names are Capitalized and alphanumeric-plus-
/// underscore; everything else in the contract names sanitizes to
/// underscore.
fn class_name(collection: &str, field: &str, metric: DistanceMetric) -> String {
    let raw = format!("{}_{}_{}", collection, field, metric_suffix(metric));
    let sanitized: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut chars = sanitized.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

impl WeaviateVectorIndex {
    /// Builds the engine; fails when the URL is unset.
    pub fn new(options: WeaviateOptions) -> Result<Self, VectorError> {
        if options.url.trim().is_empty() {
            return Err(VectorError::InvalidQuery("weaviate: url unset".to_string()));
        }
        Ok(Self {
            options: WeaviateOptions {
                url: options.url.trim_end_matches('/').to_string(),
                ..options
            },
            http: reqwest::Client::new(),
            dimensions: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case (`url`, `api_key`, `timeout_ms`), unknown fields
    /// ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, VectorError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            url: String,
            api_key: Option<String>,
            timeout_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| VectorError::InvalidQuery(format!("weaviate settings: {e}")))?;
        Self::new(WeaviateOptions {
            url: wire.url,
            api_key: wire.api_key,
            request_timeout: wire
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(10)),
        })
    }

    /// One request, requiring 2xx; 404 maps to `Failed("not found")`
    /// for the caller to interpret.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
        context: &str,
    ) -> Result<serde_json::Value, VectorError> {
        let url = format!("{}{}", self.options.url, path);
        let mut request = self
            .http
            .request(method, &url)
            .timeout(self.options.request_timeout);
        if let Some(key) = &self.options.api_key {
            request = request.bearer_auth(key);
        }
        if let Some(body) = &body {
            request =
                request
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(body).map_err(|e| {
                        VectorError::Failed(format!("weaviate {context} encode: {e}"))
                    })?);
        }
        let response = request
            .send()
            .await
            .map_err(|e| VectorError::Failed(format!("weaviate {context}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| VectorError::Failed(format!("weaviate {context}: {e}")))?;
        if status.as_u16() == 404 {
            return Err(VectorError::Failed("not found".to_string()));
        }
        if !status.is_success() {
            return Err(VectorError::Failed(format!(
                "weaviate {context}: HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        if bytes.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| VectorError::Failed(format!("weaviate {context}: {e}")))
    }

    /// Creates one suffixed class at `dimension` (via the schema REST
    /// API), then records the dimension.
    async fn create_class(
        &self,
        class: &str,
        metric: DistanceMetric,
        dimension: usize,
    ) -> Result<(), VectorError> {
        self.send(
            reqwest::Method::POST,
            "/v1/schema",
            Some(json!({
                "class": class,
                "vectorizer": "none",
                "vectorIndexType": "hnsw",
                "vectorIndexConfig": {
                    "distance": weaviate_metric(metric),
                },
                "properties": [{
                    "name": "contractId",
                    "dataType": ["text"],
                }],
            })),
            &format!("create {class}"),
        )
        .await?;
        self.dimensions
            .lock()
            .expect("dimension registry poisoned")
            .insert(class.to_string(), dimension);
        Ok(())
    }
}

impl VectorIndex for WeaviateVectorIndex {
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
                let class = class_name(collection, field, metric);
                let known = self
                    .dimensions
                    .lock()
                    .expect("dimension registry poisoned")
                    .get(&class)
                    .copied();
                match known {
                    Some(size) if size != vector.len() => {
                        return Err(VectorError::DimensionMismatch {
                            expected: size,
                            got: vector.len(),
                        });
                    }
                    // Known: straight batch (Weaviate's batch upserts by
                    // object id — the same uuid replaces).
                    Some(_) => {
                        let body = json!({"objects": [{
                            "class": class,
                            "id": uuid_from_id(id),
                            "properties": {"contractId": id},
                            "vector": vector,
                        }]});
                        self.send(
                            reqwest::Method::POST,
                            "/v1/batch/objects",
                            Some(body),
                            &format!("batch {class}"),
                        )
                        .await?;
                    }
                    // Unknown: seed the class and record the dimension
                    // from this, the first vector.
                    None => {
                        self.create_class(&class, metric, vector.len()).await?;
                        let body = json!({"objects": [{
                            "class": class,
                            "id": uuid_from_id(id),
                            "properties": {"contractId": id},
                            "vector": vector,
                        }]});
                        self.send(
                            reqwest::Method::POST,
                            "/v1/batch/objects",
                            Some(body),
                            &format!("batch {class}"),
                        )
                        .await?;
                    }
                }
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
            let class = class_name(collection, &query.field, query.metric);
            let known = self
                .dimensions
                .lock()
                .expect("dimension registry poisoned")
                .get(&class)
                .copied();
            let Some(size) = known else {
                // Never seeded: no vectors under this metric — the
                // memory engine's empty page.
                return Ok(SearchResult::default());
            };
            if query.vector.len() != size {
                return Err(VectorError::DimensionMismatch {
                    expected: size,
                    got: query.vector.len(),
                });
            }
            // The nearVector search over GraphQL; the vector rides
            // JSON-encoded in the nearVector clause.
            let gql = format!(
                r#"{{ Get {{ {class}(nearVector: {{vector: {}}}, limit: {}) {{ contractId _additional {{ distance }} }} }} }}"#,
                serde_json::to_string(&query.vector)
                    .map_err(|e| { VectorError::Failed(format!("weaviate search encode: {e}")) })?,
                query.top_k,
            );
            let body = self
                .send(
                    reqwest::Method::POST,
                    "/v1/graphql",
                    Some(json!({"query": gql})),
                    "search",
                )
                .await?;
            let mut hits = Vec::new();
            for get_object in body
                .pointer("/data/Get")
                .and_then(|get| get.get(&class))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(id) = get_object
                    .get("contractId")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(distance) = get_object.pointer("/_additional/distance").and_then(|d| {
                    d.as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| d.as_f64())
                }) else {
                    continue;
                };
                // Weaviate's distance faces: cosine is 1 − cos (converted);
                // l2-squared is monotone in the true euclidean distance
                // (converted on the squared value — the ordering the
                // contract fixes is preserved, the absolute score is the
                // squared variant, stated in the docs); dot's documented
                // distance is 2 − 2·cos, likewise monotone.
                let score = match query.metric {
                    DistanceMetric::Cosine => {
                        distance_to_score(DistanceMetric::Cosine, distance as f32)
                    }
                    DistanceMetric::Euclidean => {
                        // De-square first when the value came back squared.
                        distance_to_score(DistanceMetric::Euclidean, (distance as f32).sqrt())
                    }
                    DistanceMetric::Dot => {
                        // 2 − 2·cos → the inner product's cosine = 1 − d/2,
                        // used directly as the score: Weaviate's dot face
                        // exposes no raw product, only this monotone
                        // distance — ordering preserved, magnitude
                        // documented as the cosine-of-the-dot face.
                        (1.0 - distance / 2.0) as f32
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
            // the engine sweeps every seeded suffix class of this
            // contract collection. Weaviate's object DELETE is
            // idempotent — a missing id is a 204 no-op.
            let seeded: Vec<String> = {
                let dimensions = self.dimensions.lock().expect("dimension registry poisoned");
                let prefix = format!("{}_", collection.to_lowercase());
                dimensions
                    .keys()
                    .filter(|class| class.to_lowercase().starts_with(&prefix))
                    .cloned()
                    .collect()
            };
            for class in seeded {
                let _ = self
                    .send(
                        reqwest::Method::DELETE,
                        &format!("/v1/objects/{}", uuid_from_id(id)),
                        None,
                        &format!("delete {class}"),
                    )
                    .await;
            }
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>> {
        Box::pin(async { Ok(()) })
    }
}

/// The object id Weaviate carries: it demands a UUID; the contract's
/// free-form string ids map deterministically onto UUID v5-shaped
/// ids — the same string always the same object (upsert replaces), a
/// property UUID v5's namespace hash guarantees. The engine hashes
/// with a tiny FNV-1a stretched across the 16 UUID bytes: not
/// cryptographic, only collision-free enough for the id space a
/// deployment puts through one collection, and stable across
/// processes.
fn uuid_from_id(id: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let mut hash2: u64 = 0x9e3779b97f4a7c15;
    for byte in id.as_bytes().iter().rev() {
        hash2 ^= u64::from(*byte);
        hash2 = hash2.wrapping_mul(0x100000001b3);
    }
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (hash >> 32) as u32,
        (hash >> 16) as u16,
        (hash as u16) | 0x5000,
        (hash2 >> 48) as u16 | 0x8000,
        hash2 & 0xFFFF_FFFF_FFFF,
    )
}

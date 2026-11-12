//! Qdrant engine for the RushWind vector contract — the contract's
//! first external adapter, hand-written over reqwest against the
//! qdrant REST API (no SDK dependency; the house's S3/STOMP/Apollo
//! precedent).
//!
//! # The mapping
//!
//! - A contract **collection** is a qdrant collection; a contract
//!   **field** is a qdrant **named vector** — with one twist the
//!   metric contract forces: qdrant fixes a distance per named vector
//!   while the contract's metric is a per-query choice, so the engine
//!   stores each field under **three named vectors** suffixed with
//!   the metric (`{field}__cosine`, `{field}__euclidean`, `{field}__dot`,
//!   the same data three times, each carrying its own distance). A
//!   search then addresses the suffix its metric names — correct
//!   server-side top-k under every contract metric, at the documented
//!   cost of tripled vector storage. Deployments that know their
//!   metric up front can halve or third that by pre-creating the
//!   collection with the bare field name (see below).
//! - **Dimension discipline** rides qdrant's own: the collection
//!   info's configured size is the expected dimension, so a
//!   mismatched upsert or query answers
//!   [`VectorError::DimensionMismatch`] with the server's truth — no
//!   client-side shadow state. The first upsert into a missing
//!   collection births it with all three suffixes at the upserted
//!   dimension.
//! - **Pre-existing collections** the deployment created with a bare
//!   field name (`{field}`, one metric) work in single-metric mode:
//!   upserts and searches go to the bare name, and a query naming a
//!   different metric refuses [`VectorError::InvalidQuery`] — qdrant
//!   cannot re-rank a fixed distance, and the engine does not
//!   silently rescore. A search on an *unseeded* field (the
//!   collection exists, the field never configured) answers the empty
//!   [`SearchResult`](rushwind_vector::SearchResult) — the memory
//!   engine's semantics; only an upsert refuses a missing field
//!   configuration, since qdrant's vectors config is fixed at
//!   creation.
//! - **Scores** — qdrant answers cosine searches with the cosine
//!   *similarity* (already bigger-is-better, identical vectors 1.0)
//!   and euclidean searches with the raw *distance* (converted
//!   through [`rushwind_vector::distance_to_score`]); dot arrives
//!   raw. The engine re-sorts descending regardless — the one
//!   convention the contract fixes.
//! - **Point ids**: qdrant carries u64 or UUID ids only, while the
//!   contract speaks free-form strings. The engine maps honestly:
//!   decimal strings ride as u64, UUID-shaped strings pass through,
//!   and anything else becomes a deterministic UUID-shaped point id
//!   (SHA-1 of the id, hand-rolled — no dependency) with the original
//!   id echoed in the point's payload (`__contract_id`), which search
//!   hits read back — the mapping is stable across upserts (replace
//!   hits the same point) and reversible on the way out.
//! - Delete of a missing id is qdrant's own no-op; `close` has
//!   nothing to release (the reqwest client is stateless).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::time::Duration;

use rushwind_vector::{
    distance_to_score, BoxFuture, DistanceMetric, Hit, SearchResult, VectorError, VectorIndex,
    VectorQuery,
};
use serde_json::json;

/// The payload key the original contract id echoes back under.
const CONTRACT_ID_PAYLOAD: &str = "__contract_id";

/// The engine's settings.
#[derive(Debug, Clone)]
pub struct QdrantOptions {
    /// The qdrant REST base URL (`http://127.0.0.1:6333`).
    pub url: String,
    /// The API key, when the deployment requires one (`api-key` header).
    pub api_key: Option<String>,
    /// The per-request timeout. Default 10 s.
    pub request_timeout: Duration,
}

impl Default for QdrantOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// The qdrant vector-index engine.
#[derive(Debug)]
pub struct QdrantVectorIndex {
    options: QdrantOptions,
    http: reqwest::Client,
}

/// The metric names the engine's named-vector suffixes speak.
fn metric_suffix(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Euclidean => "euclidean",
        DistanceMetric::Dot => "dot",
    }
}

/// The distance name qdrant's wire speaks, per metric.
fn distance_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "Cosine",
        DistanceMetric::Euclidean => "Euclid",
        DistanceMetric::Dot => "Dot",
    }
}

impl QdrantVectorIndex {
    /// Builds the engine; fails when the URL is unset.
    pub fn new(options: QdrantOptions) -> Result<Self, VectorError> {
        if options.url.trim().is_empty() {
            return Err(VectorError::InvalidQuery("qdrant: url unset".to_string()));
        }
        Ok(Self {
            options: QdrantOptions {
                url: options.url.trim_end_matches('/').to_string(),
                ..options
            },
            http: reqwest::Client::new(),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// snake_case keys (`url`, `api_key`, `timeout_ms`), unknown
    /// fields ignored.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, VectorError> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct SettingsWire {
            url: String,
            api_key: Option<String>,
            timeout_ms: Option<u64>,
        }
        let wire: SettingsWire = serde_json::from_value(settings)
            .map_err(|e| VectorError::InvalidQuery(format!("qdrant settings: {e}")))?;
        Self::new(QdrantOptions {
            url: wire.url,
            api_key: wire.api_key,
            request_timeout: wire
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(10)),
        })
    }

    /// One authenticated, timed request.
    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.options.url))
            .timeout(self.options.request_timeout);
        if let Some(key) = &self.options.api_key {
            request = request.header("api-key", key);
        }
        match body {
            Some(body) => request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&body).unwrap_or_default()),
            None => request,
        }
    }

    /// Sends a request and requires a success status; returns the
    /// body bytes.
    async fn send_ok(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Vec<u8>, VectorError> {
        let response = self
            .request(method, path, body)
            .send()
            .await
            .map_err(|e| VectorError::Failed(format!("qdrant {path}: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| VectorError::Failed(format!("qdrant {path}: {e}")))?;
        if !status.is_success() {
            let detail = String::from_utf8_lossy(&bytes);
            return Err(VectorError::Failed(format!(
                "qdrant {path}: HTTP {}: {detail}",
                status.as_u16()
            )));
        }
        Ok(bytes.to_vec())
    }

    /// Fetches the collection's info; `None` when the collection does
    /// not exist.
    async fn collection_info(
        &self,
        collection: &str,
    ) -> Result<Option<serde_json::Value>, VectorError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!("/collections/{collection}"),
                None,
            )
            .send()
            .await
            .map_err(|e| VectorError::Failed(format!("qdrant info {collection}: {e}")))?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(VectorError::Failed(format!(
                "qdrant info {collection}: HTTP {}",
                response.status().as_u16()
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|e| VectorError::Failed(format!("qdrant info {collection}: {e}")))?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| VectorError::Failed(format!("qdrant info {collection}: {e}")))?;
        Ok(body.get("result").cloned())
    }

    /// The vectors config of a collection info — qdrant's named map,
    /// or `None` for the single unnamed shape (whose shape has no
    /// field name to address).
    fn vectors_config(
        info: &serde_json::Value,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let vectors = info.pointer("/config/params/vectors")?;
        if vectors.get("size").is_some() {
            return None;
        }
        vectors.as_object().cloned()
    }

    /// Reads one named vector's `(size, distance)` out of the config.
    fn named_config(
        config: &serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Option<(usize, DistanceMetric)> {
        let params = config.get(name)?;
        let size = params.get("size")?.as_u64()? as usize;
        let distance = match params.get("distance").and_then(serde_json::Value::as_str) {
            Some("Euclid") => DistanceMetric::Euclidean,
            Some("Dot") => DistanceMetric::Dot,
            _ => DistanceMetric::Cosine,
        };
        Some((size, distance))
    }

    /// Maps a contract id to its qdrant point id: decimal strings
    /// ride as u64, UUID-shaped strings pass through, everything else
    /// hashes to a deterministic UUID-shaped id (the payload carries
    /// the original back out).
    fn point_id(id: &str) -> serde_json::Value {
        if let Ok(n) = id.parse::<u64>() {
            return json!(n);
        }
        if is_uuid_shape(id) {
            return json!(id);
        }
        json!(uuid_from_sha1(id))
    }
}

impl VectorIndex for QdrantVectorIndex {
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
            let point_id = Self::point_id(id);
            let info = self.collection_info(collection).await?;
            // The named vectors this upsert writes: all three metric
            // suffixes for an engine-born collection, the bare field
            // for a pre-existing single-metric one.
            let (named_vectors, write_names): (
                serde_json::Map<String, serde_json::Value>,
                Vec<String>,
            ) = match info {
                None => {
                    let mut named = serde_json::Map::new();
                    for metric in [
                        DistanceMetric::Cosine,
                        DistanceMetric::Euclidean,
                        DistanceMetric::Dot,
                    ] {
                        named.insert(
                            format!("{field}__{}", metric_suffix(metric)),
                            json!({
                                "size": vector.len(),
                                "distance": distance_name(metric),
                            }),
                        );
                    }
                    let writes = named.keys().cloned().collect();
                    (named, writes)
                }
                Some(info) => {
                    let Some(config) = Self::vectors_config(&info) else {
                        return Err(VectorError::Failed(format!(
                            "qdrant collection {collection:?} carries an unnamed vector; the contract addresses named fields"
                        )));
                    };
                    let suffixes: Vec<Option<(usize, DistanceMetric)>> = [
                        DistanceMetric::Cosine,
                        DistanceMetric::Euclidean,
                        DistanceMetric::Dot,
                    ]
                    .map(|metric| {
                        Self::named_config(&config, &format!("{field}__{}", metric_suffix(metric)))
                    })
                    .into();
                    if suffixes.iter().any(|s| s.is_some()) {
                        // Engine-born field: every suffix must be
                        // present and dimension-agreed.
                        if suffixes.iter().any(|s| s.is_none()) {
                            return Err(VectorError::Failed(format!(
                                "qdrant collection {collection:?} carries a partial {field:?} suffix set"
                            )));
                        }
                        let size = suffixes[0].expect("checked above").0;
                        if size != vector.len() {
                            return Err(VectorError::DimensionMismatch {
                                expected: size,
                                got: vector.len(),
                            });
                        }
                        let writes = [
                            DistanceMetric::Cosine,
                            DistanceMetric::Euclidean,
                            DistanceMetric::Dot,
                        ]
                        .map(|metric| format!("{field}__{}", metric_suffix(metric)))
                        .into();
                        (serde_json::Map::new(), writes)
                    } else if let Some((size, _)) = Self::named_config(&config, field) {
                        // Pre-existing single-metric collection.
                        if size != vector.len() {
                            return Err(VectorError::DimensionMismatch {
                                expected: size,
                                got: vector.len(),
                            });
                        }
                        (serde_json::Map::new(), vec![field.to_string()])
                    } else {
                        return Err(VectorError::Failed(format!(
                            "qdrant collection {collection:?} does not configure field {field:?}; qdrant's vectors config is fixed at collection creation"
                        )));
                    }
                }
            };
            if !named_vectors.is_empty() {
                self.send_ok(
                    reqwest::Method::PUT,
                    &format!("/collections/{collection}"),
                    Some(json!({ "vectors": named_vectors })),
                )
                .await?;
            }
            let mut vector_map = serde_json::Map::new();
            for name in &write_names {
                vector_map.insert(name.clone(), json!(vector));
            }
            self.send_ok(
                reqwest::Method::PUT,
                &format!("/collections/{collection}/points?wait=true"),
                Some(json!({ "points": [{
                    "id": point_id,
                    "vector": vector_map,
                    "payload": { CONTRACT_ID_PAYLOAD: id },
                }]})),
            )
            .await
            .map(|_| ())
        })
    }

    fn search<'a>(
        &'a self,
        collection: &'a str,
        query: &'a VectorQuery,
    ) -> BoxFuture<'a, Result<SearchResult, VectorError>> {
        Box::pin(async move {
            query.validate()?;
            let info = self.collection_info(collection).await?;
            let Some(info) = info else {
                // The memory engine's absent-collection semantics: no
                // collection, no vectors, an empty page — not an error.
                return Ok(SearchResult::default());
            };
            let Some(config) = Self::vectors_config(&info) else {
                return Err(VectorError::Failed(format!(
                    "qdrant collection {collection:?} carries an unnamed vector; the contract addresses named fields"
                )));
            };
            // The named vector this search addresses: the metric's
            // suffix when the field is engine-born, the bare field
            // when it is a single-metric deployment.
            let suffixed = format!(
                "{field}__{suffix}",
                field = query.field,
                suffix = metric_suffix(query.metric)
            );
            let (search_name, expected) = if let Some(config) =
                Self::named_config(&config, &suffixed)
            {
                (suffixed, config.0)
            } else if let Some((size, distance)) = Self::named_config(&config, &query.field) {
                if query.metric != distance {
                    return Err(VectorError::InvalidQuery(format!(
                        "field {:?} ranks by {distance:?}; the query names {:?} — qdrant fixes a pre-configured field's metric",
                        query.field, query.metric
                    )));
                }
                (query.field.clone(), size)
            } else {
                // An existing collection that never configured the
                // field: nothing stored under it — an empty page.
                return Ok(SearchResult::default());
            };
            if query.vector.len() != expected {
                return Err(VectorError::DimensionMismatch {
                    expected,
                    got: query.vector.len(),
                });
            }
            let body = json!({
                "vector": { "name": search_name, "vector": query.vector },
                "limit": query.top_k,
                "with_vector": true,
                "with_payload": true,
            });
            let bytes = self
                .send_ok(
                    reqwest::Method::POST,
                    &format!("/collections/{collection}/points/search"),
                    Some(body),
                )
                .await?;
            let body: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| VectorError::Failed(format!("qdrant search: {e}")))?;
            let mut hits = Vec::new();
            for point in body
                .get("result")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                // The contract id rides the payload; the point id is
                // the fallback for points written without one.
                let id = point
                    .pointer("/payload/__contract_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .or_else(|| point.get("id").map(point_id_string))
                    .unwrap_or_default();
                let Some(qdrant_score) = point.get("score").and_then(serde_json::Value::as_f64)
                else {
                    continue;
                };
                let stored = point
                    .get("vector")
                    .and_then(|v| v.get(&search_name))
                    .and_then(vector_of)
                    .unwrap_or_default();
                hits.push(Hit {
                    id,
                    score: contract_score(query.metric, qdrant_score),
                    vector: stored,
                });
            }
            // Score descending; id ascending breaks ties
            // deterministically — the memory engine's exact rule, so
            // the engines answer identically on ties.
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
            let point_id = Self::point_id(id);
            let body = json!({ "points": [point_id] });
            self.send_ok(
                reqwest::Method::POST,
                &format!("/collections/{collection}/points/delete?wait=true"),
                Some(body),
            )
            .await
            .map(|_| ())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>> {
        // No pooled resources: the reqwest client is stateless.
        Box::pin(async { Ok(()) })
    }
}

/// Converts qdrant's answer score to the contract's convention —
/// cosine arrives as similarity (bigger-better already), euclid as a
/// distance (converted), dot raw.
fn contract_score(metric: DistanceMetric, qdrant_score: f64) -> f32 {
    match metric {
        DistanceMetric::Cosine | DistanceMetric::Dot => qdrant_score as f32,
        DistanceMetric::Euclidean => {
            distance_to_score(DistanceMetric::Euclidean, qdrant_score as f32)
        }
    }
}

/// The point id back to a contract string — qdrant echoes u64 numbers
/// and UUID strings.
fn point_id_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Pulls a float vector out of a bare array.
fn vector_of(value: &serde_json::Value) -> Option<Vec<f32>> {
    value.as_array().map(|items| {
        items
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect()
    })
}

/// The deterministic point id for a free-form contract id: SHA-1 of
/// the id, rendered in UUID's 8-4-4-4-12 shape (the hash is one-way;
/// the payload carries the original back out — the shape is what
/// qdrant requires, not a version claim).
fn uuid_from_sha1(id: &str) -> String {
    let digest = sha1(id.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// SHA-1 (FIPS 180-4) — twenty lines of standard arithmetic so the id
/// mapping needs no dependency; pinned against the reference vectors.
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in message.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// The UUID shape qdrant accepts as a point id: 8-4-4-4-12 hex.
fn is_uuid_shape(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ => {
                if !byte.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_shape_validation() {
        assert!(is_uuid_shape("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!is_uuid_shape("550e8400e29b41d4a716446655440000"));
        assert!(!is_uuid_shape("550e8400-e29b-41d4-a716-44665544000"));
        assert!(!is_uuid_shape("550e8400ge29b41d4a71644665544-0000"));
        assert!(!is_uuid_shape(""));
    }

    #[test]
    fn point_ids_map_deterministically() {
        // Decimal ids ride as numbers, UUIDs pass through, free-form
        // ids hash to a stable shape.
        assert_eq!(QdrantVectorIndex::point_id("42"), serde_json::json!(42));
        assert_eq!(
            QdrantVectorIndex::point_id("550e8400-e29b-41d4-a716-446655440000"),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(
            QdrantVectorIndex::point_id("user-1"),
            QdrantVectorIndex::point_id("user-1"),
            "the mapping must be stable across calls — replace hits the same point"
        );
        assert!(is_uuid_shape(
            QdrantVectorIndex::point_id("user-1").as_str().unwrap()
        ));
    }

    #[test]
    fn sha1_matches_the_reference_vectors() {
        let hex = |data: &[u8]| {
            sha1(data)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        // A message that spans the padding boundary (56 <= len < 64):
        // padding needs an extra block.
        let long = "a".repeat(56);
        assert_eq!(
            hex(long.as_bytes()),
            "c2db330f6083854c99d4b5bfb6e8f29f201be699"
        );
    }

    #[test]
    fn settings_parse_snake_case() {
        let engine = QdrantVectorIndex::from_settings(serde_json::json!({
            "url": "http://127.0.0.1:6333",
            "api_key": "secret",
            "timeout_ms": 250,
            "unknown_field": 1,
        }))
        .expect("settings valid");
        assert_eq!(engine.options.api_key.as_deref(), Some("secret"));
        assert_eq!(engine.options.request_timeout, Duration::from_millis(250));

        assert!(
            QdrantVectorIndex::from_settings(serde_json::json!({})).is_err(),
            "an unset url refuses"
        );
    }

    #[test]
    fn unset_url_refuses_construction() {
        assert!(matches!(
            QdrantVectorIndex::new(QdrantOptions::default()),
            Err(VectorError::InvalidQuery(msg)) if msg.contains("url unset")
        ));
    }
}

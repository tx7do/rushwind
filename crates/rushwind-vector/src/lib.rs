//! Vector-retrieval contract for RushWind (RAG / semantic-search
//! scenarios): a unified [`VectorQuery`] over any engine, one score
//! convention, and the distance math engines without native ranking
//! need. Types and pure functions only — no drivers, mirroring
//! go-crud's `vector` module.
//!
//! # The core shapes
//!
//! A [`VectorQuery`] names the vector **field**, carries the
//! pre-computed query vector, a `top_k`, and the [`DistanceMetric`].
//! Embedding generation is **out of scope** — the contract transports
//! vectors somebody else computed.
//!
//! **Scores are always "bigger = more similar."** Engines whose native
//! metric is a distance convert through [`distance_to_score`] before
//! answering: cosine distance `d = 1 − cos` → score `1 − d`; L2
//! distance → `1 / (1 + d)`; inner product's score is the raw dot
//! product, sorted descending. Hits in a [`SearchResult`] are ordered
//! by score, descending.
//!
//! Search is **top-k nearest-neighbor, not pagination**:
//! [`SearchResult::total`] is the hit count, not a collection size.
//!
//! # Dimension discipline
//!
//! The first vector upserted into a `(collection, field)` fixes its
//! dimension; a later upsert with a different length refuses with
//! [`VectorError::DimensionMismatch`], and so does a query whose
//! vector disagrees with the stored dimension.
//!
//! # The pgvector text codec
//!
//! [`pgvector`] encodes/decodes vectors in pgvector's text format
//! (`[1,2,3]`) — the wire shape SQL engines store in a `vector(N)`
//! column.
//!
//! # Engines
//!
//! Engines live in `rushwind-vector-*` crates and implement
//! [`VectorIndex`] over their native storage (`memory` today; the
//! external engine family — qdrant/milvus/weaviate, plus the
//! vector-capable faces of existing storage engines — is scoped per
//! engine when needed). Every engine passes the shared conformance
//! suite (`rushwind_testkit::rushwind_vector_conformance_suite!`).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::future::Future;
use std::pin::Pin;

/// Future type used across the vector contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by vector engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum VectorError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The vector's length disagrees with the dimension the
    /// `(collection, field)` already fixed.
    DimensionMismatch {
        /// The dimension the field already fixed.
        expected: usize,
        /// The offending vector's length.
        got: usize,
    },
    /// The query itself is malformed (empty vector, zero `top_k`).
    InvalidQuery(String),
}

impl std::fmt::Display for VectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "vector operation failed: {msg}"),
            Self::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
            Self::InvalidQuery(msg) => write!(f, "invalid query: {msg}"),
        }
    }
}

impl std::error::Error for VectorError {}

/// The distance metric a query ranks by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistanceMetric {
    /// Cosine similarity through a cosine-distance face: `d = 1 − cos`,
    /// score `1 − d`. Magnitude-blind.
    Cosine,
    /// Straight-line (L2) distance, score `1 / (1 + d)`.
    Euclidean,
    /// Raw inner product as the score, descending.
    Dot,
}

/// A unified vector search request.
///
/// `field` names the vector column inside the collection (a schema may
/// carry several); `vector` is the already-computed query embedding —
/// the contract does not embed.
#[derive(Debug, Clone)]
pub struct VectorQuery {
    /// The vector field to search.
    pub field: String,
    /// The query vector; its length must match the field's dimension.
    pub vector: Vec<f32>,
    /// How many nearest neighbors to return; must be at least 1.
    pub top_k: usize,
    /// The metric to rank by.
    pub metric: DistanceMetric,
}

impl VectorQuery {
    /// Builds a query for `field` ranked by `metric`.
    pub fn new(field: impl Into<String>, vector: Vec<f32>, top_k: usize) -> Self {
        Self {
            field: field.into(),
            vector,
            top_k,
            metric: DistanceMetric::Cosine,
        }
    }

    /// Overrides the metric (cosine by default).
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Validates the self-contained invariants: a non-empty vector and
    /// a positive `top_k`. Dimension agreement with the stored field is
    /// an engine-side check — only the engine knows the dimension.
    ///
    /// # Errors
    ///
    /// [`VectorError::InvalidQuery`] when an invariant fails.
    pub fn validate(&self) -> Result<(), VectorError> {
        if self.vector.is_empty() {
            return Err(VectorError::InvalidQuery("vector is empty".to_string()));
        }
        if self.top_k == 0 {
            return Err(VectorError::InvalidQuery(
                "top_k must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// One search hit: the record id, its score, and the stored vector.
/// Hits arrive sorted by score, descending.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// The record id the vector belongs to.
    pub id: String,
    /// Bigger = more similar. See [`distance_to_score`] for the
    /// conversion convention.
    pub score: f32,
    /// The stored vector, echoed back for verification-side work.
    pub vector: Vec<f32>,
}

/// A search answer: the ordered hits and their count. `total` is the
/// hit count (top-k semantics), never the collection size.
#[derive(Debug, Clone, Default)]
pub struct SearchResult {
    /// Hits ordered by score, descending.
    pub hits: Vec<Hit>,
    /// The number of hits returned.
    pub total: usize,
}

/// The vector-index contract: upsert, search, delete over named
/// collections. Engines must be callable through shared references
/// (`&self`).
pub trait VectorIndex: Send + Sync {
    /// Inserts or replaces the vector stored for `id` under
    /// `(collection, field)`. An id re-upserted replaces its vector —
    /// search reflects the new position.
    fn upsert<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        field: &'a str,
        vector: &'a [f32],
    ) -> BoxFuture<'a, Result<(), VectorError>>;

    /// Searches `collection` by the query: the `top_k` nearest vectors
    /// under the query's metric, scores bigger-first.
    fn search<'a>(
        &'a self,
        collection: &'a str,
        query: &'a VectorQuery,
    ) -> BoxFuture<'a, Result<SearchResult, VectorError>>;

    /// Removes the record `id` from `collection` — across every vector
    /// field, matching native point-delete semantics. A no-op when
    /// missing.
    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<(), VectorError>>;

    /// Releases engine resources.
    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>>;
}

/// The dot product `a·b`.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// The L2 (straight-line) distance between `a` and `b`.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// The cosine distance `1 − cos(a, b)`.
///
/// A zero vector has no direction; either side zero reads as distance
/// `1.0` (score `0.0`) — the fail-neutral answer — instead of NaN.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let na = dot_product(a, a).sqrt();
    let nb = dot_product(b, b).sqrt();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    1.0 - dot_product(a, b) / (na * nb)
}

/// Converts a native distance to the contract's score — the one
/// conversion every distance-native engine applies before answering:
///
/// - cosine: `1 − d` (identical vectors score `1.0`),
/// - euclidean: `1 / (1 + d)` (identical vectors score `1.0`),
/// - dot: the raw value — the caller passes the dot product itself;
///   "distance" is a misnomer for that metric.
pub fn distance_to_score(metric: DistanceMetric, distance: f32) -> f32 {
    match metric {
        DistanceMetric::Cosine => 1.0 - distance,
        DistanceMetric::Euclidean => 1.0 / (1.0 + distance),
        DistanceMetric::Dot => distance,
    }
}

/// Scores a candidate against the query under `metric` — distance
/// first, then the [`distance_to_score`] conversion.
pub fn score(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> f32 {
    let distance = match metric {
        DistanceMetric::Cosine => cosine_distance(query, candidate),
        DistanceMetric::Euclidean => euclidean_distance(query, candidate),
        DistanceMetric::Dot => dot_product(query, candidate),
    };
    distance_to_score(metric, distance)
}

/// pgvector's text format for vectors: `[1,2,3]` — the wire shape a
/// SQL engine stores in (and reads back from) a `vector(N)` column.
pub mod pgvector {
    use super::VectorError;

    /// Encodes a vector as pgvector text: `[1,2,3]`.
    pub fn encode(vector: &[f32]) -> String {
        let body: Vec<String> = vector.iter().map(|v| format!("{v}")).collect();
        format!("[{}]", body.join(","))
    }

    /// Decodes pgvector text (`[1,2,3]`, whitespace tolerated, empty
    /// `[]` refused) back into a vector.
    ///
    /// # Errors
    ///
    /// [`VectorError::InvalidQuery`] when the text is not the
    /// bracketed comma-separated shape.
    pub fn decode(text: &str) -> Result<Vec<f32>, VectorError> {
        let trimmed = text.trim();
        let inner = trimmed
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| VectorError::InvalidQuery(format!("not pgvector text: {text:?}")))?;
        if inner.trim().is_empty() {
            return Err(VectorError::InvalidQuery(
                "pgvector text must hold at least one component".to_string(),
            ));
        }
        inner
            .split(',')
            .map(|part| {
                part.trim()
                    .parse::<f32>()
                    .map_err(|e| VectorError::InvalidQuery(format!("bad component {part:?}: {e}")))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Score conventions: identical vectors score 1.0 under cosine and
    /// euclidean; dot's score is the raw product.
    #[test]
    fn score_conventions() {
        let a = [1.0, 0.0];
        let b = [1.0, 0.0];
        assert!((score(DistanceMetric::Cosine, &a, &b) - 1.0).abs() < 1e-6);
        assert!((score(DistanceMetric::Euclidean, &a, &b) - 1.0).abs() < 1e-6);
        assert!((score(DistanceMetric::Dot, &a, &b) - 1.0).abs() < 1e-6);

        let c = [2.0, 0.0];
        // cosine is magnitude-blind: [2,0] still scores 1.0 against
        // [1,0]; dot doubles.
        assert!((score(DistanceMetric::Cosine, &a, &c) - 1.0).abs() < 1e-6);
        assert!((score(DistanceMetric::Dot, &a, &c) - 2.0).abs() < 1e-6);
    }

    /// Zero vectors read as cosine distance 1.0 — fail-neutral, not
    /// NaN.
    #[test]
    fn zero_vector_is_neutral_not_nan() {
        let zero = [0.0, 0.0];
        let a = [1.0, 0.0];
        assert!((cosine_distance(&zero, &a) - 1.0).abs() < 1e-6);
        assert!((cosine_distance(&a, &zero) - 1.0).abs() < 1e-6);
    }

    /// The pgvector codec round-trips, and refuses what it cannot
    /// parse.
    #[test]
    fn pgvector_round_trips() {
        let v = vec![1.0, -2.5, 3.25];
        let text = pgvector::encode(&v);
        assert_eq!(text, "[1,-2.5,3.25]");
        assert_eq!(pgvector::decode(&text).expect("decode"), v);
        assert_eq!(
            pgvector::decode(" [ 1 , 2 ] ").expect("decode"),
            vec![1.0, 2.0]
        );
        assert!(pgvector::decode("[]").is_err());
        assert!(pgvector::decode("1,2").is_err());
        assert!(pgvector::decode("[1,x]").is_err());
    }

    /// Query validation refuses empty vectors and zero top_k.
    #[test]
    fn query_validation() {
        let ok = VectorQuery::new("embedding", vec![1.0], 3);
        assert!(ok.validate().is_ok());
        assert!(VectorQuery::new("embedding", vec![], 3).validate().is_err());
        assert!(VectorQuery::new("embedding", vec![1.0], 0)
            .validate()
            .is_err());
    }
}

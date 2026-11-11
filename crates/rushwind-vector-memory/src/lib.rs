//! In-memory reference engine for the RushWind vector contract.
//!
//! Brute-force top-k over upserted vectors — the semantic baseline the
//! external engine adapters (qdrant/milvus/weaviate-class systems, the
//! vector-capable faces of the storage engines) are judged against.
//! Every operation completes synchronously behind the contract's
//! future face; nothing here ever yields.
//!
//! Dimension discipline matches the contract: the first vector
//! upserted into a `(collection, field)` fixes its dimension, and
//! later upserts or queries that disagree refuse with
//! [`VectorError::DimensionMismatch`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::ready;
use std::sync::Mutex;

use rushwind_vector::{score, BoxFuture, Hit, SearchResult, VectorError, VectorIndex, VectorQuery};

/// One collection's fields: field → id → vector.
type Fields = HashMap<String, HashMap<String, Vec<f32>>>;

/// The in-memory vector index.
///
/// Construct with [`MemoryVectorIndex::new`]; share it by `Arc`.
#[derive(Debug, Default)]
pub struct MemoryVectorIndex {
    /// collection → field → id → vector.
    collections: Mutex<HashMap<String, Fields>>,
}

/// The in-memory engine's settings — none today; the type exists so
/// the settings wire stays parseable.
#[derive(Debug, Default, serde::Deserialize)]
struct MemoryVectorSettings {}

impl MemoryVectorIndex {
    /// An empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// the in-memory engine has no knobs — `{}` (unknown fields are
    /// ignored, matching serde's default).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, VectorError> {
        let _settings: MemoryVectorSettings = serde_json::from_value(settings)
            .map_err(|e| VectorError::Failed(format!("settings parse: {e}")))?;
        Ok(Self::new())
    }

    /// Reads the fixed dimension of `field` — the length of any stored
    /// vector, `None` when the field holds nothing yet.
    fn dimension_of(fields: &Fields, field: &str) -> Option<usize> {
        fields
            .get(field)
            .and_then(|ids| ids.values().next())
            .map(Vec::len)
    }
}

impl VectorIndex for MemoryVectorIndex {
    fn upsert<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
        field: &'a str,
        vector: &'a [f32],
    ) -> BoxFuture<'a, Result<(), VectorError>> {
        Box::pin(ready(self.upsert_now(collection, id, field, vector)))
    }

    fn search<'a>(
        &'a self,
        collection: &'a str,
        query: &'a VectorQuery,
    ) -> BoxFuture<'a, Result<SearchResult, VectorError>> {
        Box::pin(ready(self.search_now(collection, query)))
    }

    fn delete<'a>(
        &'a self,
        collection: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<(), VectorError>> {
        Box::pin(ready(self.delete_now(collection, id)))
    }

    fn close(&self) -> BoxFuture<'_, Result<(), VectorError>> {
        self.collections
            .lock()
            .expect("index lock poisoned")
            .clear();
        Box::pin(ready(Ok(())))
    }
}

impl MemoryVectorIndex {
    /// The synchronous upsert the future face completes immediately.
    fn upsert_now(
        &self,
        collection: &str,
        id: &str,
        field: &str,
        vector: &[f32],
    ) -> Result<(), VectorError> {
        if vector.is_empty() {
            return Err(VectorError::InvalidQuery(
                "upserted vector is empty".to_string(),
            ));
        }
        let mut collections = self.collections.lock().expect("index lock poisoned");
        let fields = collections.entry(collection.to_string()).or_default();
        if let Some(expected) = Self::dimension_of(fields, field) {
            if expected != vector.len() {
                return Err(VectorError::DimensionMismatch {
                    expected,
                    got: vector.len(),
                });
            }
        }
        fields
            .entry(field.to_string())
            .or_default()
            .insert(id.to_string(), vector.to_vec());
        Ok(())
    }

    /// The synchronous search the future face completes immediately.
    fn search_now(
        &self,
        collection: &str,
        query: &VectorQuery,
    ) -> Result<SearchResult, VectorError> {
        query.validate()?;
        let collections = self.collections.lock().expect("index lock poisoned");
        let Some(fields) = collections.get(collection) else {
            return Ok(SearchResult::default());
        };
        let Some(ids) = fields.get(&query.field) else {
            return Ok(SearchResult::default());
        };
        if let Some(expected) = ids.values().next().map(Vec::len) {
            if expected != query.vector.len() {
                return Err(VectorError::DimensionMismatch {
                    expected,
                    got: query.vector.len(),
                });
            }
        }
        let mut scored: Vec<(f32, String, Vec<f32>)> = ids
            .iter()
            .map(|(id, vector)| {
                (
                    score(query.metric, &query.vector, vector),
                    id.clone(),
                    vector.clone(),
                )
            })
            .collect();
        // Score descending; id ascending breaks ties deterministically.
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        scored.truncate(query.top_k);
        let hits = scored
            .into_iter()
            .map(|(s, id, vector)| Hit {
                id,
                score: s,
                vector,
            })
            .collect::<Vec<_>>();
        Ok(SearchResult {
            total: hits.len(),
            hits,
        })
    }

    /// The synchronous delete the future face completes immediately.
    fn delete_now(&self, collection: &str, id: &str) -> Result<(), VectorError> {
        let mut collections = self.collections.lock().expect("index lock poisoned");
        if let Some(fields) = collections.get_mut(collection) {
            for ids in fields.values_mut() {
                ids.remove(id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_vector::DistanceMetric;

    /// A two-vector ranking under all three metrics: the aligned vector
    /// outranks the orthogonal one, scores descend.
    #[tokio::test]
    async fn aligned_outranks_orthogonal() {
        let index = MemoryVectorIndex::new();
        index
            .upsert("docs", "a", "embedding", &[1.0, 0.0])
            .await
            .expect("upsert");
        index
            .upsert("docs", "b", "embedding", &[0.0, 1.0])
            .await
            .expect("upsert");

        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::Dot,
        ] {
            let query = VectorQuery::new("embedding", vec![1.0, 0.0], 2).with_metric(metric);
            let result = index.search("docs", &query).await.expect("search");
            assert_eq!(result.hits.len(), 2, "{metric:?}");
            assert_eq!(result.hits[0].id, "a", "{metric:?}");
            assert_eq!(result.hits[1].id, "b", "{metric:?}");
            assert!(result.hits[0].score > result.hits[1].score, "{metric:?}");
            assert_eq!(result.total, 2);
        }
    }

    /// Upsert replaces: the same id re-upserted moves.
    #[tokio::test]
    async fn upsert_replaces() {
        let index = MemoryVectorIndex::new();
        index
            .upsert("docs", "a", "v", &[1.0, 0.0])
            .await
            .expect("upsert");
        index
            .upsert("docs", "a", "v", &[0.0, 1.0])
            .await
            .expect("replace");
        let query = VectorQuery::new("v", vec![0.0, 1.0], 1);
        let result = index.search("docs", &query).await.expect("search");
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].id, "a");
    }
}

//! Weaviate-specific wire pins beyond the shared suite: the
//! metric-suffix class birth (Capitalized names), the dimension
//! mismatch typed off the engine registry, the bearer token, the id
//! sweep delete, and unseeded empty pages.

#![cfg(test)]

use std::time::Duration;

use rushwind_vector::{VectorError, VectorIndex, VectorQuery};
use rushwind_vector_weaviate::{WeaviateOptions, WeaviateVectorIndex};

mod common;
use common::spawn_weaviate;

fn engine(url: String, api_key: Option<String>) -> WeaviateVectorIndex {
    WeaviateVectorIndex::new(WeaviateOptions {
        url,
        api_key,
        request_timeout: Duration::from_secs(5),
    })
    .expect("engine builds")
}

#[tokio::test]
async fn first_upsert_births_the_metric_suffix_classes() {
    let (url, mock) = spawn_weaviate().await;
    let index = engine(url, None);
    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect("upsert births");
    let names = mock.class_names();
    for suffix in ["cosine", "euclidean", "dot"] {
        assert!(
            names.contains(&format!("Docs_embedding_{suffix}")),
            "Docs_embedding_{suffix} must have been born; got {names:?}"
        );
    }
}

#[tokio::test]
async fn dimension_mismatch_is_typed() {
    let (url, _mock) = spawn_weaviate().await;
    let index = engine(url, None);
    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0])
        .await
        .expect("fixes dimension 2");
    let err = index
        .upsert("docs", "2", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect_err("mismatch refuses");
    assert!(matches!(
        err,
        VectorError::DimensionMismatch {
            expected: 2,
            got: 3
        }
    ));
    let query = VectorQuery::new("embedding", vec![1.0, 0.0, 0.0], 1);
    let err = index
        .search("docs", &query)
        .await
        .expect_err("query refuses");
    assert!(matches!(
        err,
        VectorError::DimensionMismatch {
            expected: 2,
            got: 3
        }
    ));
}

#[tokio::test]
async fn upsert_replaces_by_contract_id() {
    let (url, mock) = spawn_weaviate().await;
    let index = engine(url, None);
    index
        .upsert("docs", "user-1", "embedding", &[1.0, 0.0])
        .await
        .expect("first");
    index
        .upsert("docs", "user-1", "embedding", &[0.0, 1.0])
        .await
        .expect("replace");
    // The same uuid carries the replaced vector: one object per id.
    assert_eq!(
        mock.object_count("Docs_embedding_cosine"),
        1,
        "the same contract id maps to the same object — replace, not duplicate"
    );
}

#[tokio::test]
async fn the_api_key_rides_the_requests() {
    let (url, mock) = spawn_weaviate().await;
    let index = engine(url, Some("weaviate-secret".to_string()));
    index
        .upsert("docs", "1", "embedding", &[1.0])
        .await
        .expect("upsert");
    assert_eq!(mock.last_bearer(), "Bearer weaviate-secret");
}

#[tokio::test]
async fn delete_sweeps_every_suffix_class() {
    let (url, _mock) = spawn_weaviate().await;
    let index = engine(url, None);
    index
        .upsert("docs", "user-1", "embedding", &[1.0, 0.0])
        .await
        .expect("seed");
    index.delete("docs", "user-1").await.expect("delete sweeps");
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 5);
    let result = index.search("docs", &query).await.expect("search");
    assert_eq!(result.total, 0);
    // Unknown collections delete as a no-op.
    index
        .delete("no-such", "user-1")
        .await
        .expect("no-op delete");
}

#[tokio::test]
async fn unseeded_collection_searches_empty() {
    let (url, _mock) = spawn_weaviate().await;
    let index = engine(url, None);
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
    let result = index.search("nothing", &query).await.expect("search");
    assert_eq!(result.total, 0);
}

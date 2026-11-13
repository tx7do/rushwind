//! Milvus-specific wire pins beyond the shared suite: the
//! metric-suffix collection birth, the typed dimension mismatch off
//! the server's describe, the bearer token, and the id-sweep delete.

#![cfg(test)]

use std::time::Duration;

use rushwind_vector::{VectorError, VectorIndex, VectorQuery};
use rushwind_vector_milvus::{MilvusOptions, MilvusVectorIndex};

mod common;
use common::spawn_milvus;

fn engine(url: String, token: Option<String>) -> MilvusVectorIndex {
    MilvusVectorIndex::new(MilvusOptions {
        url,
        token,
        request_timeout: Duration::from_secs(5),
    })
    .expect("engine builds")
}

#[tokio::test]
async fn first_upsert_births_the_metric_suffixes() {
    let (url, mock) = spawn_milvus().await;
    let index = engine(url, None);
    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect("upsert births");
    for suffix in ["cosine", "euclidean", "dot"] {
        let name = format!("docs__embedding__{suffix}");
        assert!(
            mock.collection_names().contains(&name),
            "{name} must have been born"
        );
        assert_eq!(mock.collection_dimension(&name), Some(3));
    }
}

#[tokio::test]
async fn dimension_mismatch_is_typed_off_the_describe() {
    let (url, _mock) = spawn_milvus().await;
    let index = engine(url, None);
    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0])
        .await
        .expect("fixes dimension 2");
    let err = index
        .upsert("docs", "2", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect_err("mismatch refuses");
    assert!(
        matches!(
            err,
            VectorError::DimensionMismatch {
                expected: 2,
                got: 3
            }
        ),
        "got {err:?}"
    );
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
async fn the_token_rides_the_requests() {
    let (url, mock) = spawn_milvus().await;
    let index = engine(url, Some("milvus-secret".to_string()));
    index
        .upsert("docs", "1", "embedding", &[1.0])
        .await
        .expect("upsert");
    assert_eq!(mock.last_bearer(), "Bearer milvus-secret");
}

#[tokio::test]
async fn delete_sweeps_every_suffix_child() {
    let (url, mock) = spawn_milvus().await;
    let index = engine(url, None);
    index
        .upsert("docs", "user-1", "embedding", &[1.0, 0.0])
        .await
        .expect("seed");
    assert_eq!(mock.collection_names().len(), 3, "three suffix children");

    index.delete("docs", "user-1").await.expect("delete sweeps");
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 5);
    let result = index.search("docs", &query).await.expect("search");
    assert_eq!(result.total, 0, "every suffix child saw the delete");

    // Deleting from an unknown collection is the no-op.
    index
        .delete("no-such", "user-1")
        .await
        .expect("no-op delete");
}

#[tokio::test]
async fn unseeded_collection_searches_empty() {
    let (url, _mock) = spawn_milvus().await;
    let index = engine(url, None);
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
    let result = index.search("nothing", &query).await.expect("search");
    assert_eq!(result.total, 0);
}

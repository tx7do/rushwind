//! Neo4j-specific wire pins beyond the shared suite: the
//! metric-suffix index birth (server-pinned dimensions), the typed
//! dimension mismatch mapped from the server's error prose, the
//! basic-auth header, and the registry-swept delete.

#![cfg(test)]

use std::time::Duration;

use rushwind_vector::{VectorError, VectorIndex, VectorQuery};
use rushwind_vector_neo4j::{Neo4jOptions, Neo4jVectorIndex};

mod common;
use common::spawn_neo4j;

fn engine(url: String) -> Neo4jVectorIndex {
    Neo4jVectorIndex::new(Neo4jOptions {
        url,
        username: "neo4j".to_string(),
        password: "password".to_string(),
        request_timeout: Duration::from_secs(5),
        ..Neo4jOptions::default()
    })
    .expect("engine builds")
}

#[tokio::test]
async fn first_upsert_births_the_three_metric_indexes() {
    let (url, mock) = spawn_neo4j().await;
    let index = engine(url);
    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect("upsert births");
    for suffix in ["Cosine", "Euclidean", "Dot"] {
        let label = format!("DocsEmbedding{suffix}");
        assert_eq!(
            mock.label_index_dimension(&format!("{label}_index")),
            Some(3),
            "{label} index born at the upserted dimension"
        );
    }
}

#[tokio::test]
async fn dimension_mismatch_maps_to_the_typed_error() {
    let (url, _mock) = spawn_neo4j().await;
    let index = engine(url);
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
        "the server's prose maps onto the typed error, got {err:?}"
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
async fn the_basic_auth_header_rides_the_requests() {
    let (url, mock) = spawn_neo4j().await;
    let index = engine(url);
    index
        .upsert("docs", "1", "embedding", &[1.0])
        .await
        .expect("upsert");
    let encoded = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode("neo4j:password")
    };
    assert_eq!(mock.last_auth(), format!("Basic {encoded}"));
}

#[tokio::test]
async fn delete_sweeps_every_registered_label() {
    let (url, _mock) = spawn_neo4j().await;
    let index = engine(url);
    index
        .upsert("docs", "user-1", "embedding", &[1.0, 0.0])
        .await
        .expect("seed");
    index.delete("docs", "user-1").await.expect("delete sweeps");
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 5);
    let result = index.search("docs", &query).await.expect("search");
    assert_eq!(result.total, 0);
}

#[tokio::test]
async fn unseeded_collection_searches_empty() {
    let (url, _mock) = spawn_neo4j().await;
    let index = engine(url);
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
    let result = index.search("nothing", &query).await.expect("search");
    assert_eq!(result.total, 0);
}

//! Qdrant-specific wire pins beyond the shared conformance suite: the
//! metric-suffix collection birth, the typed dimension mismatch read
//! off the server's config, the api-key header, the id refusals, and
//! the pre-existing single-metric collection mode.

#![cfg(test)]

use std::time::Duration;

use rushwind_vector::{DistanceMetric, VectorError, VectorIndex, VectorQuery};
use rushwind_vector_qdrant::{QdrantOptions, QdrantVectorIndex};

mod common;
use common::spawn_qdrant;

fn engine(url: &str, api_key: Option<String>) -> QdrantVectorIndex {
    QdrantVectorIndex::new(QdrantOptions {
        url: url.to_string(),
        api_key,
        request_timeout: Duration::from_secs(5),
    })
    .expect("engine builds")
}

/// The first upsert births the collection with all three metric
/// suffixes at the upserted dimension — the shape every contract
/// metric then searches server-side.
#[tokio::test]
async fn first_upsert_births_the_metric_suffixes() {
    let (url, mock) = spawn_qdrant().await;
    let index = engine(&url, None);

    index
        .upsert("docs", "1", "embedding", &[1.0, 0.0, 0.0])
        .await
        .expect("upsert births");

    let config = mock.collection_config("docs").expect("collection exists");
    for (suffix, distance) in [
        ("cosine", "Cosine"),
        ("euclidean", "Euclid"),
        ("dot", "Dot"),
    ] {
        let name = format!("embedding__{suffix}");
        assert_eq!(
            config[&name]["size"],
            serde_json::json!(3),
            "{name} at the upserted dimension"
        );
        assert_eq!(config[&name]["distance"], serde_json::json!(distance));
    }
}

/// A dimension disagreement against a pre-configured field answers
/// the typed mismatch with the server's truth — no second upsert
/// reaches the wire.
#[tokio::test]
async fn dimension_mismatch_is_typed_off_the_server_config() {
    let (url, _mock) = spawn_qdrant().await;
    let index = engine(&url, None);

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
}

/// The api-key option rides every request as the `api-key` header.
#[tokio::test]
async fn api_key_rides_the_requests() {
    let (url, mock) = spawn_qdrant().await;
    let index = engine(&url, Some("qdrant-secret".to_string()));

    index
        .upsert("docs", "1", "embedding", &[1.0])
        .await
        .expect("upsert");
    assert_eq!(
        mock.last_header("api-key").as_deref(),
        Some("qdrant-secret"),
        "the key must ride the wire"
    );
}

/// Free-form contract ids map deterministically onto qdrant's u64/UUID
/// point ids: the same string replaces the same point, and search hits
/// carry the original id back from the payload.
#[tokio::test]
async fn free_form_ids_map_deterministically() {
    let (url, _mock) = spawn_qdrant().await;
    let index = engine(&url, None);

    index
        .upsert("docs", "user-1", "embedding", &[1.0, 0.0])
        .await
        .expect("free-form id maps");
    // The same string maps to the same point: this replaces, not
    // duplicates.
    index
        .upsert("docs", "user-1", "embedding", &[0.0, 1.0])
        .await
        .expect("replace");

    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 5);
    let result = index.search("docs", &query).await.expect("search");
    assert_eq!(result.total, 1, "the replace must not duplicate the point");
    assert_eq!(
        result.hits[0].id, "user-1",
        "the payload echoes the original id"
    );

    // Delete by the contract string hits the same mapped point.
    index.delete("docs", "user-1").await.expect("delete");
    let result = index
        .search("docs", &query)
        .await
        .expect("search after delete");
    assert_eq!(result.total, 0);
}

/// A pre-existing collection with a bare field name works in
/// single-metric mode: searches under the field's own metric go
/// through, a different metric refuses with the field's truth.
#[tokio::test]
async fn pre_existing_single_metric_field_is_respected() {
    let (url, mock) = spawn_qdrant().await;
    let index = engine(&url, None);

    // Seed by hand: a collection carrying only the bare field under
    // euclid — what a deployment that knows its metric creates.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let create = serde_json::json!({
            "vectors": {"embedding": {"size": 2, "distance": "Euclid"}}
        });
        let request = format!(
            "PUT /collections/docs HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            create.to_string().len(),
            create
        );
        let mut stream = tokio::net::TcpStream::connect(url.trim_start_matches("http://"))
            .await
            .expect("connect");
        stream.write_all(request.as_bytes()).await.expect("write");
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await.expect("read");
    }
    assert!(
        mock.collection_config("docs").is_some(),
        "collection created"
    );

    // Upsert rides the bare name (single-metric mode).
    index
        .upsert("docs", "1", "embedding", &[0.0, 0.0])
        .await
        .expect("single-metric upsert");

    // Search under the field's own metric works.
    let query =
        VectorQuery::new("embedding", vec![1.0, 0.0], 1).with_metric(DistanceMetric::Euclidean);
    let result = index.search("docs", &query).await.expect("euclid search");
    assert_eq!(result.total, 1);
    // 1/(1+1) — the euclidean distance-1 conversion.
    assert!((result.hits[0].score - 1.0 / 2.0).abs() < 1e-6);

    // A different metric refuses with the field's truth.
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 1);
    let err = index
        .search("docs", &query)
        .await
        .expect_err("cosine refuses");
    assert!(
        matches!(err, VectorError::InvalidQuery(ref m) if m.contains("Euclidean")),
        "got {err:?}"
    );
}

/// Search over an unseeded field of an existing collection is the
/// empty page, not an error — the memory engine's semantics.
#[tokio::test]
async fn unseeded_field_searches_empty() {
    let (url, _mock) = spawn_qdrant().await;
    let index = engine(&url, None);

    index
        .upsert("docs", "1", "title-vector", &[1.0])
        .await
        .expect("seed another field");
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
    let result = index.search("docs", &query).await.expect("search");
    assert_eq!(result.total, 0);
}

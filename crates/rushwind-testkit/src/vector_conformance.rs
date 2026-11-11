//! The vector conformance suite.
//!
//! [`rushwind_vector_conformance_suite!`] expands to a module of
//! `#[tokio::test]` functions pinning the [`VectorIndex`](rushwind_vector::VectorIndex)
//! contract on a concrete engine: top-k ordering under every metric,
//! the bigger-is-more-similar score convention, dimension discipline,
//! upsert replacement, delete semantics, and the empty faces. The
//! suite is deliberately engine-agnostic — the same tests gate the
//! in-memory reference engine and every external vector adapter.
//!
//! # Wiring it up
//!
//! The consuming crate needs the feature-gated testkit plus tokio:
//!
//! ```toml
//! [dev-dependencies]
//! rushwind-testkit = { version = "0.0.1", features = ["vector"] }
//! tokio = { version = "1", features = ["macros", "rt"] }
//! ```
//!
//! Then, in the engine crate's `tests/conformance.rs`, define the
//! factory at the test-crate root and reference it by crate-rooted
//! path:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rushwind_vector::VectorIndex;
//!
//! async fn index() -> Arc<dyn VectorIndex> {
//!     Arc::new(MyEngine::new().await.expect("connect"))
//! }
//!
//! rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);
//! ```
//!
//! `$factory` must produce a **fresh, empty** index per call; the
//! suite seeds and cleans up through the contract's own methods. The
//! vectors the suite uses are two-dimensional and tiny — the pins are
//! semantic, not scale tests.

/// Generates the vector conformance suite for a
/// [`VectorIndex`](rushwind_vector::VectorIndex) implementation.
///
/// `$factory` must be a **crate-rooted path** to an async function of
/// type `async fn() -> Arc<dyn VectorIndex>` producing a fresh, empty
/// index (e.g. `crate::index`).
#[macro_export]
macro_rules! rushwind_vector_conformance_suite {
    ($factory:expr) => {
        mod rushwind_vector_conformance {
            //! The shared vector conformance tests; each function pins
            //! one slice of the retrieval contract semantics.

            use std::sync::Arc;

            use $crate::rushwind_vector::{DistanceMetric, VectorError, VectorIndex, VectorQuery};

            async fn index() -> Arc<dyn VectorIndex> {
                $factory().await
            }

            /// Seeds three 2-d vectors on field "embedding" of
            /// collection "docs": one aligned with the query axis, one
            /// orthogonal, one opposed.
            async fn seeded() -> Arc<dyn VectorIndex> {
                let index = index().await;
                index
                    .upsert("docs", "aligned", "embedding", &[1.0, 0.0])
                    .await
                    .expect("upsert aligned");
                index
                    .upsert("docs", "orthogonal", "embedding", &[0.0, 1.0])
                    .await
                    .expect("upsert orthogonal");
                index
                    .upsert("docs", "opposed", "embedding", &[-1.0, 0.0])
                    .await
                    .expect("upsert opposed");
                index
            }

            /// Ranking under every metric: aligned first, orthogonal
            /// second, opposed last; scores strictly descend; total is
            /// the hit count.
            #[tokio::test]
            async fn ranking_under_every_metric() {
                for metric in [
                    DistanceMetric::Cosine,
                    DistanceMetric::Euclidean,
                    DistanceMetric::Dot,
                ] {
                    let index = seeded().await;
                    let query =
                        VectorQuery::new("embedding", vec![1.0, 0.0], 3).with_metric(metric);
                    let result = index.search("docs", &query).await.expect("search");
                    assert_eq!(result.total, 3, "{metric:?}");
                    let ids: Vec<&str> = result.hits.iter().map(|h| h.id.as_str()).collect();
                    assert_eq!(ids, ["aligned", "orthogonal", "opposed"], "{metric:?}");
                    assert!(
                        result.hits[0].score > result.hits[1].score
                            && result.hits[1].score > result.hits[2].score,
                        "{metric:?}: scores must strictly descend"
                    );
                }
            }

            /// Identical vectors score ~1.0 under every metric — cosine
            /// and euclidean by their 1−d / 1/(1+d) faces, dot because
            /// [1,0]·[1,0] is itself 1.0.
            #[tokio::test]
            async fn identical_vector_scores_one() {
                for metric in [
                    DistanceMetric::Cosine,
                    DistanceMetric::Euclidean,
                    DistanceMetric::Dot,
                ] {
                    let index = seeded().await;
                    let query =
                        VectorQuery::new("embedding", vec![1.0, 0.0], 1).with_metric(metric);
                    let result = index.search("docs", &query).await.expect("search");
                    assert_eq!(result.hits[0].id, "aligned", "{metric:?}");
                    assert!(
                        (result.hits[0].score - 1.0).abs() < 1e-5,
                        "{metric:?}: identical vectors score 1.0, got {}",
                        result.hits[0].score
                    );
                }
            }

            /// top_k truncates: only that many hits come back, the
            /// best ones.
            #[tokio::test]
            async fn top_k_truncates_to_the_best() {
                let index = seeded().await;
                let query = VectorQuery::new("embedding", vec![1.0, 0.0], 2);
                let result = index.search("docs", &query).await.expect("search");
                assert_eq!(result.total, 2);
                assert_eq!(result.hits[0].id, "aligned");
                assert_eq!(result.hits[1].id, "orthogonal");
            }

            /// Upsert replaces the stored vector: the same id moves to
            /// its new position.
            #[tokio::test]
            async fn upsert_replaces() {
                let index = seeded().await;
                index
                    .upsert("docs", "opposed", "embedding", &[0.0, 1.0])
                    .await
                    .expect("replace");
                let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
                let result = index.search("docs", &query).await.expect("search");
                let opposed = result
                    .hits
                    .iter()
                    .find(|h| h.id == "opposed")
                    .expect("replaced id still present");
                // [0,1] vs [1,0] is orthogonal — the middle rank, above
                // nothing here since aligned tops and opposed now ties
                // orthogonal; the pin is the rank move off the bottom.
                assert!(
                    opposed.score > result.hits[2].score || result.hits[2].id != "opposed",
                    "the replaced vector must not rank last-by-opposition anymore"
                );
            }

            /// Delete removes the record from search, across fields;
            /// deleting a missing id is a no-op.
            #[tokio::test]
            async fn delete_removes_from_search() {
                let index = seeded().await;
                index.delete("docs", "aligned").await.expect("delete");
                index.delete("docs", "missing").await.expect("no-op delete");

                let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
                let result = index.search("docs", &query).await.expect("search");
                assert!(!result.hits.iter().any(|h| h.id == "aligned"));
                assert_eq!(result.total, 2);
            }

            /// Dimension discipline: the first upsert fixes it; later
            /// upserts and queries that disagree refuse with
            /// DimensionMismatch.
            #[tokio::test]
            async fn dimension_is_fixed_and_enforced() {
                let index = index().await;
                index
                    .upsert("docs", "a", "embedding", &[1.0, 0.0])
                    .await
                    .expect("first upsert fixes the dimension");

                match index
                    .upsert("docs", "b", "embedding", &[1.0, 0.0, 0.0])
                    .await
                {
                    Err(VectorError::DimensionMismatch {
                        expected: 2,
                        got: 3,
                    }) => {}
                    other => panic!("upsert mismatch must be DimensionMismatch(2,3): {other:?}"),
                }
                let query = VectorQuery::new("embedding", vec![1.0, 0.0, 0.0, 0.0], 1);
                match index.search("docs", &query).await {
                    Err(VectorError::DimensionMismatch {
                        expected: 2,
                        got: 4,
                    }) => {}
                    other => panic!("query mismatch must be DimensionMismatch(2,4): {other:?}"),
                }
            }

            /// An empty (or absent) collection answers an empty result,
            /// not an error.
            #[tokio::test]
            async fn empty_collection_is_empty_not_error() {
                let index = index().await;
                let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
                let result = index.search("nothing", &query).await.expect("search");
                assert_eq!(result.total, 0);
                assert!(result.hits.is_empty());

                index
                    .upsert("docs", "a", "other-field", &[1.0, 0.0])
                    .await
                    .expect("seed a different field");
                let result = index.search("docs", &query).await.expect("search");
                assert_eq!(result.total, 0, "a query on an unseeded field sees nothing");
            }

            /// Malformed queries refuse before any scan: empty vector,
            /// zero top_k.
            #[tokio::test]
            async fn malformed_queries_refuse() {
                let index = seeded().await;
                for query in [
                    VectorQuery::new("embedding", vec![], 1),
                    VectorQuery::new("embedding", vec![1.0, 0.0], 0),
                ] {
                    match index.search("docs", &query).await {
                        Err(VectorError::InvalidQuery(_)) => {}
                        other => panic!("must refuse with InvalidQuery: {other:?}"),
                    }
                }
            }
        }
    };
}

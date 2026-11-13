//! One search code path, five engines.
//!
//! The demo seeds the same three documents into every engine it can
//! reach (in-memory always; the external engines when their
//! `VECTOR_*_URL` environment variable names a running server) and
//! runs the identical query — bigger-is-more-similar scores, top-k
//! ordering, and the dimension discipline come back the same shape
//! everywhere. Engine choice is a constructor detail.

use std::sync::Arc;

use rushwind_vector::{DistanceMetric, VectorIndex, VectorQuery};
use rushwind_vector_memory::MemoryVectorIndex;

/// One named engine built behind the shared trait object.
async fn engine(name: &str) -> Option<(String, Arc<dyn VectorIndex>)> {
    match name {
        "memory" => Some((
            "memory".to_string(),
            Arc::new(MemoryVectorIndex::new()) as Arc<dyn VectorIndex>,
        )),
        "qdrant" => env_url("VECTOR_QDRANT_URL").map(|url| {
            let engine = rushwind_vector_qdrant::QdrantVectorIndex::new(
                rushwind_vector_qdrant::QdrantOptions {
                    url,
                    ..Default::default()
                },
            )
            .expect("qdrant options valid");
            (
                "qdrant".to_string(),
                Arc::new(engine) as Arc<dyn VectorIndex>,
            )
        }),
        "milvus" => env_url("VECTOR_MILVUS_URL").map(|url| {
            let engine = rushwind_vector_milvus::MilvusVectorIndex::new(
                rushwind_vector_milvus::MilvusOptions {
                    url,
                    ..Default::default()
                },
            )
            .expect("milvus options valid");
            (
                "milvus".to_string(),
                Arc::new(engine) as Arc<dyn VectorIndex>,
            )
        }),
        "weaviate" => env_url("VECTOR_WEAVIATE_URL").map(|url| {
            let engine = rushwind_vector_weaviate::WeaviateVectorIndex::new(
                rushwind_vector_weaviate::WeaviateOptions {
                    url,
                    ..Default::default()
                },
            )
            .expect("weaviate options valid");
            (
                "weaviate".to_string(),
                Arc::new(engine) as Arc<dyn VectorIndex>,
            )
        }),
        "neo4j" => env_url("VECTOR_NEO4J_URL").map(|url| {
            let engine =
                rushwind_vector_neo4j::Neo4jVectorIndex::new(rushwind_vector_neo4j::Neo4jOptions {
                    url,
                    username: std::env::var("VECTOR_NEO4J_USER").unwrap_or_default(),
                    password: std::env::var("VECTOR_NEO4J_PASSWORD").unwrap_or_default(),
                    ..Default::default()
                })
                .expect("neo4j options valid");
            (
                "neo4j".to_string(),
                Arc::new(engine) as Arc<dyn VectorIndex>,
            )
        }),
        _ => None,
    }
}

fn env_url(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|url| !url.is_empty())
}

async fn demo(index: &dyn VectorIndex, engine: &str) {
    // Three documents on one axis: aligned, orthogonal, opposed.
    for (id, vector) in [
        ("aligned", vec![1.0, 0.0]),
        ("orthogonal", vec![0.0, 1.0]),
        ("opposed", vec![-1.0, 0.0]),
    ] {
        index
            .upsert("docs", id, "embedding", &vector)
            .await
            .expect("upsert");
    }
    let query = VectorQuery::new("embedding", vec![1.0, 0.0], 3);
    let result = index.search("docs", &query).await.expect("search");
    println!("--- {engine} ---");
    for hit in &result.hits {
        println!("  {:>10}  score {:.4}", hit.id, hit.score);
    }
    println!("  best: {} (total {})", result.hits[0].id, result.total);
}

#[tokio::main]
async fn main() {
    for name in ["memory", "qdrant", "milvus", "weaviate", "neo4j"] {
        match engine(name).await {
            Some((label, index)) => demo(index.as_ref(), &label).await,
            None => println!(
                "--- {name} --- (skipped: set VECTOR_{}_URL to run)",
                name.to_uppercase()
            ),
        }
    }
    // The score convention is the contract: identical vectors score
    // 1.0 under cosine, whatever engine answered.
    let _ = DistanceMetric::Cosine;
}

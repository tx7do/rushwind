//! The vector contract conformance suite, run against the Neo4j
//! engine over an in-process mock of the transactional Cypher subset
//! it speaks — the same semantic pins the memory reference engine
//! passes, proving the external adapter is contract-faithful.

use std::sync::Arc;

use rushwind_vector::VectorIndex;
use rushwind_vector_neo4j::{Neo4jOptions, Neo4jVectorIndex};

mod common;
use common::spawn_neo4j;

async fn index() -> Arc<dyn VectorIndex> {
    let (url, _mock) = spawn_neo4j().await;
    Arc::new(
        Neo4jVectorIndex::new(Neo4jOptions {
            url,
            ..Neo4jOptions::default()
        })
        .expect("engine builds"),
    )
}

rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);

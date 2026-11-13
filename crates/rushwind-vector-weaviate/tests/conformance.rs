//! The vector contract conformance suite, run against the Weaviate
//! engine over an in-process mock of the REST/GraphQL subset it
//! speaks — the same semantic pins the memory reference engine
//! passes, proving the external adapter is contract-faithful.

use std::sync::Arc;

use rushwind_vector::VectorIndex;
use rushwind_vector_weaviate::{WeaviateOptions, WeaviateVectorIndex};

mod common;
use common::spawn_weaviate;

async fn index() -> Arc<dyn VectorIndex> {
    let (url, _mock) = spawn_weaviate().await;
    Arc::new(
        WeaviateVectorIndex::new(WeaviateOptions {
            url,
            ..WeaviateOptions::default()
        })
        .expect("engine builds"),
    )
}

rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);

//! The vector contract conformance suite, run against the Milvus
//! engine over an in-process mock of the v2 RESTful subset it
//! speaks — the same semantic pins the memory reference engine
//! passes, proving the external adapter is contract-faithful.

use std::sync::Arc;

use rushwind_vector::VectorIndex;
use rushwind_vector_milvus::{MilvusOptions, MilvusVectorIndex};

mod common;
use common::spawn_milvus;

async fn index() -> Arc<dyn VectorIndex> {
    let (url, _mock) = spawn_milvus().await;
    Arc::new(
        MilvusVectorIndex::new(MilvusOptions {
            url,
            ..MilvusOptions::default()
        })
        .expect("engine builds"),
    )
}

rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);

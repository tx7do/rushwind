//! The vector contract conformance suite, run against the qdrant
//! engine over an in-process mock of the REST subset it speaks — the
//! same semantic pins the memory reference engine passes, proving the
//! external adapter is contract-faithful.

use std::sync::Arc;

use rushwind_vector::VectorIndex;
use rushwind_vector_qdrant::{QdrantOptions, QdrantVectorIndex};

mod common;
use common::spawn_qdrant;

async fn index() -> Arc<dyn VectorIndex> {
    let (url, _mock) = spawn_qdrant().await;
    Arc::new(
        QdrantVectorIndex::new(QdrantOptions {
            url,
            ..QdrantOptions::default()
        })
        .expect("engine builds"),
    )
}

rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);

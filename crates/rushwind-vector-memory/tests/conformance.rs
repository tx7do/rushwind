//! The vector contract conformance suite, run against the in-memory
//! reference engine — the semantic baseline every external vector
//! adapter is judged against.

use std::sync::Arc;

use rushwind_vector::VectorIndex;
use rushwind_vector_memory::MemoryVectorIndex;

async fn index() -> Arc<dyn VectorIndex> {
    Arc::new(MemoryVectorIndex::new())
}

rushwind_testkit::rushwind_vector_conformance_suite!(crate::index);

//! The in-memory brute-force reference engine's search cost — the
//! semantic baseline every external vector adapter is measured
//! against. `top_k = 10` over N 128-dim vectors, the RAG-suggestive
//! shape.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use rushwind_vector::{DistanceMetric, VectorIndex, VectorQuery};
use rushwind_vector_memory::MemoryVectorIndex;

fn seeded(n: usize) -> Arc<MemoryVectorIndex> {
    let index = Arc::new(MemoryVectorIndex::new());
    let base: Vec<f32> = (0..128).map(|i| (i % 17) as f32).collect();
    // Seeding runs once per benchmark setup, outside the timed path.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    rt.block_on(async {
        for i in 0..n {
            let mut vector = base.clone();
            vector[i % 128] += 0.5;
            index
                .upsert("bench", &i.to_string(), "embedding", &vector)
                .await
                .expect("seed");
        }
    });
    index
}

fn bench_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_vector_search");
    for n in [1_000, 10_000] {
        let index = seeded(n);
        let query = VectorQuery::new(
            "embedding",
            (0..128).map(|i| (i % 17) as f32 + 0.25).collect(),
            10,
        )
        .with_metric(DistanceMetric::Cosine);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("bench runtime");
        group.bench_function(format!("cosine_top10_n{n}"), |b| {
            b.to_async(&rt).iter(|| index.search("bench", &query))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_search);
criterion_main!(benches);

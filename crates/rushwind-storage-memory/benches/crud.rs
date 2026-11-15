//! The in-memory reference engine's CRUD cost — the semantic baseline
//! every storage adapter is measured against. Point reads, filtered
//! lists, and count over N seeded rows: the shapes the conformance
//! suite pins for correctness, here timed.

use criterion::{criterion_group, criterion_main, Criterion};
use rushwind_storage::{
    ColumnKind, FilterExpr, ListQuery, Op, QueryCtx, Record, Repository, Schema, Value,
};
use std::sync::Arc;

use rushwind_storage_memory::MemoryRepo;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("schema is valid")
}

fn seeded(n: usize) -> Arc<MemoryRepo> {
    let repo = Arc::new(MemoryRepo::new(schema()).expect("schema is valid"));
    let ctx = QueryCtx::all_access();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    let repo_for_seed = Arc::clone(&repo);
    rt.block_on(async move {
        // Contract pks are positive ints: pk 0 reads as "no pk" and
        // triggers auto-assignment — seed from 1.
        for i in 1..=n {
            let row = Record::new()
                .set("id", i as i64)
                .set("name", format!("widget-{i}"))
                .set("age", (i % 80) as i64)
                .set("owner_id", (i % 4) as i64)
                .set("unit_id", 1i64);
            repo_for_seed.create(ctx.clone(), row).await.expect("seed");
        }
    });
    repo
}

fn bench_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_storage_crud");
    for n in [1_000, 10_000] {
        let repo = seeded(n);
        let ctx = QueryCtx::all_access();

        let point_repo = Arc::clone(&repo);
        let point_ctx = ctx.clone();
        let point_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("bench runtime");
        group.bench_function(format!("point_get_n{n}"), |b| {
            b.iter(|| {
                let id = rand_id(n);
                let ctx = point_ctx.clone();
                point_rt
                    .block_on(async { point_repo.get(ctx, Value::Int(id)).await })
                    .expect("point read")
            })
        });

        let list_repo = Arc::clone(&repo);
        let list_ctx = ctx.clone();
        let list_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("bench runtime");
        group.bench_function(format!("filtered_list_n{n}"), |b| {
            b.iter(|| {
                let query = ListQuery {
                    filter: Some(FilterExpr::cond("age", Op::Gte, [Value::Int(40)])),
                    ..ListQuery::default()
                };
                let ctx = list_ctx.clone();
                list_rt
                    .block_on(async { list_repo.list(ctx, &query).await })
                    .expect("filtered list")
            })
        });

        let count_repo = Arc::clone(&repo);
        let count_ctx = ctx.clone();
        let count_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("bench runtime");
        group.bench_function(format!("count_n{n}"), |b| {
            b.iter(|| {
                let ctx = count_ctx.clone();
                count_rt
                    .block_on(async {
                        count_repo
                            .count(
                                ctx,
                                Some(FilterExpr::cond("owner_id", Op::Eq, [Value::Int(1)])),
                            )
                            .await
                    })
                    .expect("count")
            })
        });
    }
    group.finish();
}

/// A cheap LCG so the point reads scatter across the key space without
/// pulling a rand dependency.
fn rand_id(n: usize) -> i64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
    let next = STATE.fetch_add(1, Ordering::Relaxed);
    1 + (((next
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407))
        >> 33) as i64
        % n as i64)
}

criterion_group!(benches, bench_crud);
criterion_main!(benches);

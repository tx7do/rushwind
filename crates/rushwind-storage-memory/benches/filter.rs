//! The in-memory reference engine's JSON and regex filter cost — the
//! semantic baseline every fetch-and-evaluate adapter (Cassandra,
//! InfluxDB) is measured against. The contract evaluator's JSON faces
//! parse the row's document per row per query (the dependency-free
//! reader over `Value::Json` canonical text), so the bench times the
//! shapes the JSON suite pins — array membership, document equality,
//! the point-path extraction, and the regex family — over N seeded
//! rows.
//!
//! The regexp face's cost (an order of magnitude above the JSON
//! faces) is the reference semantics' honest shape: the evaluator is
//! stateless, so the pattern compiles per row — the push-down engines
//! render the pattern once per query into their native regex
//! machinery (RE2/ICU/PCRE), and the dialect faces are where the
//! cost goes to die.

use criterion::{criterion_group, criterion_main, Criterion};
use rushwind_storage::{
    ColumnKind, FilterExpr, ListQuery, Op, QueryCtx, Record, Repository, Schema, Value,
};
use std::sync::Arc;

use rushwind_storage_memory::MemoryRepo;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("meta", ColumnKind::Json)
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
        for i in 1..=n {
            // The shapes rotate: arrays, documents, NULL — the JSON
            // suite's seed shapes at bench scale.
            let meta = match i % 4 {
                0 => Value::Array(vec![Value::Text("red".into()), Value::Text("blue".into())]),
                1 => Value::Array(vec![]),
                2 => Value::Json(format!(r#"{{"k":{i},"kind":"row-{i}"}}"#)),
                _ => Value::Null,
            };
            let row = Record::new()
                .set("id", i as i64)
                .set("name", format!("widget-{i}"))
                .set("meta", meta);
            repo_for_seed
                .create(ctx.clone(), row)
                .await
                .expect("seed row");
        }
    });
    repo
}

fn bench_array_contains(c: &mut Criterion) {
    let repo = seeded(10_000);
    let filter = FilterExpr::cond("meta", Op::ArrayContains, [Value::Text("red".into())]);
    let query = ListQuery::default().filtered(filter);
    c.bench_function("json/array_contains/10k", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("bench runtime"),
        )
        .iter(|| async {
            let page = repo
                .list(QueryCtx::all_access(), &query)
                .await
                .expect("list");
            std::hint::black_box(page.total)
        })
    });
}

fn bench_json_contains(c: &mut Criterion) {
    let repo = seeded(10_000);
    let filter = FilterExpr::cond(
        "meta",
        Op::JsonContains,
        [Value::Json(r#"{"k":5000,"kind":"row-5000"}"#.to_string())],
    );
    let query = ListQuery::default().filtered(filter);
    c.bench_function("json/json_contains/10k", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("bench runtime"),
        )
        .iter(|| async {
            let page = repo
                .list(QueryCtx::all_access(), &query)
                .await
                .expect("list");
            std::hint::black_box(page.total)
        })
    });
}

fn bench_json_path(c: &mut Criterion) {
    let repo = seeded(10_000);
    let filter = FilterExpr::from(rushwind_storage::FilterNode::Cond(
        rushwind_storage::Condition::new("meta", Op::Eq, [Value::Int(5000)]).with_json_path("k"),
    ));
    let query = ListQuery::default().filtered(filter);
    c.bench_function("json/json_path/10k", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("bench runtime"),
        )
        .iter(|| async {
            let page = repo
                .list(QueryCtx::all_access(), &query)
                .await
                .expect("list");
            std::hint::black_box(page.total)
        })
    });
}

fn bench_regexp(c: &mut Criterion) {
    let repo = seeded(10_000);
    let filter = FilterExpr::cond("name", Op::Regexp, [Value::Text("^widget-[0-9]*9$".into())]);
    let query = ListQuery::default().filtered(filter);
    c.bench_function("json/regexp/10k", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("bench runtime"),
        )
        .iter(|| async {
            let page = repo
                .list(QueryCtx::all_access(), &query)
                .await
                .expect("list");
            std::hint::black_box(page.total)
        })
    });
}

criterion_group!(
    benches,
    bench_array_contains,
    bench_json_contains,
    bench_json_path,
    bench_regexp
);
criterion_main!(benches);

//! The JSON and regex filter faces, one query surface, two engines.
//!
//! The same demo queries run against the in-memory reference engine and
//! a SQLite database through the SeaORM adapter — JSON_CONTAINS,
//! ARRAY_CONTAINS, and json_path render per dialect (SQLite decomposes
//! the needle over its JSON1 functions), REGEXP rides the contract's
//! own engine on memory and refuses on SQLite (which ships no regexp
//! function — the honest face prints the refusal rather than
//! silently narrowing). Engine choice is a constructor detail;
//! everything downstream is the contract.

use std::sync::Arc;

use rushwind_storage::{
    ColumnKind, FilterExpr, ListQuery, Op, QueryCtx, Record, Repository, Schema, Value,
};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_seaorm::SeaRepo;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("meta", ColumnKind::Json)
        .build()
        .expect("demo schema is valid")
}

async fn in_memory() -> Arc<dyn Repository> {
    Arc::new(MemoryRepo::new(schema()).expect("schema is valid"))
}

async fn sqlite() -> Arc<dyn Repository> {
    let repo = SeaRepo::sqlite_memory(schema())
        .await
        .expect("sqlite opens");
    repo.migrate_create().await.expect("table creates");
    Arc::new(repo)
}

/// The five demo rows: a two-element array, an empty array, an object
/// with nested scalars, NULL, and an object holding an array — the
/// shapes the JSON faces legally meet.
fn rows() -> Vec<Record> {
    vec![
        Record::new().set("name", "alpha").set(
            "meta",
            Value::Array(vec![Value::Text("red".into()), Value::Text("blue".into())]),
        ),
        Record::new()
            .set("name", "bravo")
            .set("meta", Value::Array(vec![])),
        Record::new()
            .set("name", "charlie")
            .set("meta", Value::Json(r#"{"k":1,"kind":"two"}"#.to_string())),
        Record::new().set("name", "delta").set("meta", Value::Null),
        Record::new().set("name", "echo").set(
            "meta",
            Value::Json(r#"{"tags":["red","green"]}"#.to_string()),
        ),
    ]
}

async fn seed(repo: &Arc<dyn Repository>) -> Vec<i64> {
    let mut ids = Vec::new();
    for row in rows() {
        let created = repo
            .create(QueryCtx::all_access(), row)
            .await
            .expect("seed row");
        ids.push(created.get("id").and_then(Value::as_i64).expect("int id"));
    }
    ids
}

async fn ids_matching(repo: &Arc<dyn Repository>, filter: FilterExpr) -> Result<Vec<i64>, String> {
    let query = ListQuery::default().filtered(filter);
    let page = repo
        .list(QueryCtx::all_access(), &query)
        .await
        .map_err(|error| error.to_string())?;
    Ok(page
        .items
        .iter()
        .map(|r| r.get("id").and_then(Value::as_i64).expect("int id"))
        .collect())
}

fn show(engine: &str, what: &str, outcome: &Result<Vec<i64>, String>) {
    match outcome {
        Ok(ids) => println!("  [{engine}] {what} -> ids {ids:?}"),
        Err(error) => println!("  [{engine}] {what} -> refused: {error}"),
    }
}

#[tokio::main]
async fn main() {
    let memory = in_memory().await;
    let sqlite = sqlite().await;
    let memory_ids = seed(&memory).await;
    let sqlite_ids = seed(&sqlite).await;
    println!("seeded ids: memory {memory_ids:?}, sqlite {sqlite_ids:?}");
    assert_eq!(memory_ids, sqlite_ids, "the engines assign ids identically");

    // ARRAY_CONTAINS: the array membership face (an object column
    // never matches whatever it holds).
    let by_tag = FilterExpr::cond("meta", Op::ArrayContains, [Value::Text("red".into())]);
    let memory_out = ids_matching(&memory, by_tag.clone()).await;
    let sqlite_out = ids_matching(&sqlite, by_tag).await;
    show("memory", "meta ARRAY_CONTAINS 'red'", &memory_out);
    show("sqlite", "meta ARRAY_CONTAINS 'red'", &sqlite_out);
    assert_eq!(memory_out, sqlite_out, "the faces agree");

    // JSON_CONTAINS: whole-document equality (the needle rides its
    // canonical text; SQLite decomposes it over json_each).
    let by_document = FilterExpr::cond(
        "meta",
        Op::JsonContains,
        [Value::Json(r#"["red","blue"]"#.to_string())],
    );
    let memory_out = ids_matching(&memory, by_document.clone()).await;
    let sqlite_out = ids_matching(&sqlite, by_document).await;
    show(
        "memory",
        r#"meta JSON_CONTAINS '["red","blue"]'"#,
        &memory_out,
    );
    show(
        "sqlite",
        r#"meta JSON_CONTAINS '["red","blue"]'"#,
        &sqlite_out,
    );
    assert_eq!(memory_out, sqlite_out, "the faces agree");

    // json_path: the dotted path extracts, the operator rides the
    // extracted scalar (missing paths read NULL).
    let by_path = FilterExpr::from(rushwind_storage::FilterNode::Cond(
        rushwind_storage::Condition::new("meta", Op::Eq, [Value::Int(1)]).with_json_path("k"),
    ));
    let memory_out = ids_matching(&memory, by_path.clone()).await;
    let sqlite_out = ids_matching(&sqlite, by_path).await;
    show("memory", "meta->k EQ 1", &memory_out);
    show("sqlite", "meta->k EQ 1", &sqlite_out);
    assert_eq!(memory_out, sqlite_out, "the faces agree");

    // REGEXP: the contract's own engine on memory; SQLite ships no
    // regexp function and refuses — the honest face, printed rather
    // than hidden.
    let by_regex = FilterExpr::cond("name", Op::Regexp, [Value::Text("^a[a-z]+$".into())]);
    let memory_out = ids_matching(&memory, by_regex.clone()).await;
    let sqlite_out = ids_matching(&sqlite, by_regex).await;
    show("memory", "name REGEXP '^a[a-z]+$'", &memory_out);
    show(
        "sqlite",
        "name REGEXP '^a[a-z]+$' (no regexp function)",
        &sqlite_out,
    );
    assert!(memory_out.is_ok(), "memory matches");
    assert!(
        sqlite_out.is_err(),
        "sqlite refuses — never silently narrows"
    );
}

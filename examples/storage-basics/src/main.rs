//! One repository code path, two engines.
//!
//! The same demo function runs against the in-memory reference engine and a
//! SQLite database through the SeaORM adapter — engine choice is a
//! constructor detail, everything downstream is the contract.

use std::sync::Arc;

use rushwind_storage::{
    ColumnKind, FieldMask, FilterExpr, ListQuery, Op, Paging, QueryCtx, Record, Repository, Schema,
    Sort, SortDir, Value, Viewer,
};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_seaorm::SeaRepo;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
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

fn field(row: &Record, name: &str) -> String {
    match row.get(name) {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::Real(f)) => f.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Array(items)) => rushwind_storage::json::encode(&Value::Array(items.clone())),
        Some(Value::Json(text)) => text.clone(),
        Some(Value::Null) | None => "NULL".into(),
    }
}

/// The engine-agnostic scenario the demo runs against both repositories.
async fn scenario(repo: &Arc<dyn Repository>) {
    let all = QueryCtx::all_access();

    // -- create: primary keys come back filled in ---------------------------
    let seed = [
        ("alpha", 5, Some(1.5), 1i64, 10i64),
        ("Bravo", 10, None, 1, 20),
        ("charlie", 15, Some(2.5), 2, 10),
        ("delta", 20, None, 2, 20),
        ("Echo", 25, Some(3.5), 3, 30),
    ];
    for (name, age, score, owner, unit) in seed {
        repo.create(
            all.clone(),
            Record::new()
                .set("name", name)
                .set("age", age)
                .set("score", score)
                .set("owner_id", owner)
                .set("unit_id", unit),
        )
        .await
        .expect("create lands");
    }

    // -- filter + sort -------------------------------------------------------
    let hot = ListQuery {
        filter: Some(FilterExpr::any([
            FilterExpr::cond("name", Op::Ilike, [Value::Text("a%".into())]),
            FilterExpr::cond("score", Op::Gt, [Value::Real(2.0)]),
        ])),
        sort: Sort::by("age", SortDir::Desc),
        ..ListQuery::page(1, 10)
    };
    let page = repo.list(all.clone(), &hot).await.expect("list succeeds");
    println!(
        "  filtered+sorted: {:?}  (total {})",
        page.items
            .iter()
            .map(|r| field(r, "name"))
            .collect::<Vec<_>>(),
        page.total
    );

    // -- cursor paging: stream everything in key order -----------------------
    let mut token = String::new();
    let mut via_cursor = Vec::new();
    loop {
        let stream = ListQuery {
            paging: Paging::Token {
                token: token.clone(),
                limit: 2,
            },
            ..ListQuery::default()
        };
        let page = repo.list(all.clone(), &stream).await.expect("cursor page");
        via_cursor.extend(page.items.iter().map(|r| field(r, "name")));
        match page.next_token {
            Some(next) => token = next,
            None => break,
        }
    }
    println!("  cursor stream:   {via_cursor:?}");

    // -- tenancy: a viewer only sees and touches its own rows ----------------
    let outsider = QueryCtx::new(Viewer::own(2));
    let mine = repo
        .list(outsider.clone(), &ListQuery::page(1, 10))
        .await
        .expect("scoped list");
    println!(
        "  viewer 2 owns:   {:?}",
        mine.items
            .iter()
            .map(|r| field(r, "name"))
            .collect::<Vec<_>>()
    );
    let hidden = repo.get(outsider, Value::Int(1)).await.expect("scoped get");
    println!("  viewer 2 sees widget 1: {hidden:?}");

    // -- projection ----------------------------------------------------------
    let masked = ListQuery {
        mask: Some(FieldMask::of(["id", "name"])),
        ..ListQuery::page(1, 1)
    };
    let head = repo.list(all.clone(), &masked).await.expect("masked list");
    println!(
        "  masked row:      {:?}",
        head.items[0].columns().collect::<Vec<_>>()
    );

    // -- upsert: insert, then update through the same door -------------------
    let zebra = Record::new()
        .set("id", 9)
        .set("name", "zebra")
        .set("age", 1)
        .set("score", Value::Null)
        .set("owner_id", 1)
        .set("unit_id", 1);
    repo.upsert(all.clone(), zebra.clone())
        .await
        .expect("upsert insert");
    repo.upsert(all.clone(), zebra.set("age", 2))
        .await
        .expect("upsert update");
    let stored = repo
        .get(all.clone(), Value::Int(9))
        .await
        .expect("get")
        .expect("zebra exists");
    println!(
        "  upserted twice:  {} age {}",
        field(&stored, "name"),
        field(&stored, "age")
    );

    // -- errors are values ----------------------------------------------------
    let missing = repo
        .get(all.clone(), Value::Int(404))
        .await
        .expect("get succeeds");
    println!("  get(404):        {missing:?}");
    let bad = repo
        .list(
            all.clone(),
            &ListQuery::default().filtered(FilterExpr::cond("nope", Op::Eq, [Value::Int(1)])),
        )
        .await
        .expect_err("unknown column is an error");
    println!("  bad filter:      {bad:?}");
}

#[tokio::main]
async fn main() {
    println!("== in-memory engine ==");
    scenario(&in_memory().await).await;
    println!();
    println!("== SQLite via SeaORM ==");
    scenario(&sqlite().await).await;
}

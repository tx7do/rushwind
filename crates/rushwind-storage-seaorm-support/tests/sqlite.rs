//! The pipeline against a live in-memory SQLite — the SQL semantics the
//! deployments' copies only asserted indirectly: slicing, totals,
//! filter binding by column kind, the unknown-order guard, no_paging.

use sea_orm::entity::prelude::*;
use sea_orm::{Database, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect, Select};

use rushwind_storage_seaorm_support::paging::{fetch_paged, Params, Sorting};

mod todo {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "todos")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        pub title: String,
        pub priority: i32,
        pub done: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
use todo::{Column, Entity};

async fn db() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("memory db");
    db.execute_unprepared(
        "CREATE TABLE todos (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            priority INTEGER NOT NULL,
            done BOOLEAN NOT NULL
        )",
    )
    .await
    .expect("schema");
    for (id, title, priority, done) in [
        (1, "alpha report", 1, true),
        (2, "beta task", 3, false),
        (3, "gamma report", 2, false),
        (4, "delta memo", 5, true),
        (5, "epsilon task", 4, false),
    ] {
        db.execute_unprepared(&format!(
            "INSERT INTO todos (id, title, priority, done) VALUES ({id}, '{title}', {priority}, {done})"
        ))
        .await
        .expect("seed");
    }
    db
}

fn base() -> Select<Entity> {
    Entity::find()
}

#[tokio::test]
async fn page_slicing_and_total_share_the_base() {
    let db = db().await;
    let params = Params {
        page: Some(2),
        page_size: Some(2),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(total, 5, "the total matches the unfiltered base");
    assert_eq!(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![3, 4],
        "second page of two, id order"
    );
}

#[tokio::test]
async fn no_paging_returns_every_row_with_row_count_total() {
    let db = db().await;
    let params = Params {
        no_paging: true,
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(total, 5, "the total is the row count itself");
}

#[tokio::test]
async fn filters_bind_by_column_kind() {
    let db = db().await;

    // Text icontains → lower(x) LIKE lower(y).
    let params = Params {
        query: Some(r#"{"title__icontains":"REPORT"}"#.into()),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!((rows.len(), total), (2, 2));

    // Numeric column with a numeric literal binds numeric.
    let params = Params {
        query: Some(r#"{"priority__gte":"3"}"#.into()),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!((rows.len(), total), (3, 3));

    // Numeric column with the front-end's "undefined" echo: the
    // condition drops instead of failing the query.
    let params = Params {
        query: Some(r#"{"priority__gt":"undefined"}"#.into()),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!((rows.len(), total), (5, 5), "unfiltered");

    // Bool column binds bool.
    let params = Params {
        query: Some(r#"{"done":"true"}"#.into()),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!((rows.len(), total), (2, 2));
    assert!(rows.iter().all(|r| r.done));
}

#[tokio::test]
async fn ordering_lands_only_on_real_columns() {
    let db = db().await;

    // Known column, descending via the minus spelling.
    let params = Params {
        order_by: Some(r#"["-priority"]"#.into()),
        ..Default::default()
    };
    let (rows, _) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.priority).collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1]
    );

    // An unknown name is skipped — the fallback id order survives.
    let params = Params {
        order_by: Some(r#"["not_a_column"]"#.into()),
        ..Default::default()
    };
    let (rows, _) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );

    // The structured sorting list rides beside order_by.
    let params = Params {
        sorting: vec![Sorting {
            field: "priority".into(),
            desc: false,
        }],
        ..Default::default()
    };
    let (rows, _) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.priority).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

#[tokio::test]
async fn offset_limit_wins_over_page_slicing() {
    let db = db().await;
    let params = Params {
        page: Some(9),
        page_size: Some(9),
        offset: Some(1),
        limit: Some(2),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, base(), &params).await.unwrap();
    assert_eq!(total, 5);
    assert_eq!(rows.iter().map(|r| r.id).collect::<Vec<_>>(), vec![2, 3]);
}

#[tokio::test]
async fn base_predicates_survive_the_request_pass() {
    let db = db().await;

    // A base carrying its own WHERE: the request pipeline only adds.
    let done_only = Entity::find().filter(Column::Done.eq(true));
    let params = Params {
        page_size: Some(10),
        ..Default::default()
    };
    let (rows, total) = fetch_paged(&db, done_only, &params).await.unwrap();
    assert_eq!((rows.len(), total), (2, 2));
    assert!(rows.iter().all(|r| r.done));
}

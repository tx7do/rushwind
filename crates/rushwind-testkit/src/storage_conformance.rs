//! The repository conformance suite.
//!
//! [`rushwind_storage_conformance_suite!`] expands to a module of
//! `#[tokio::test]` functions pinning the [`Repository`] contract on a
//! concrete engine: CRUD semantics, all three paging strategies, the filter
//! tree translation, sorting, field masks, viewer tenancy scoping, and the
//! audit hook. The suite's expectations are deliberately engine-agnostic —
//! the same suite gates the in-memory reference engine and every external
//! storage adapter.
//!
//! # Wiring it up
//!
//! The consuming crate needs `tokio` and the feature-gated testkit:
//!
//! ```toml
//! [dev-dependencies]
//! rushwind-testkit = { version = "0.0.1", features = ["storage"] }
//! tokio = { version = "1", features = ["macros", "rt"] }
//! ```
//!
//! Then, in the engine crate's `tests/conformance.rs`, define the two
//! factories at the test-crate root and reference them by crate-rooted
//! path — the suite expands into nested modules where a bare identifier
//! does not resolve:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rushwind_storage::Repository;
//!
//! async fn fresh_repo() -> Arc<dyn Repository> {
//!     Arc::new(MyEngine::new(rushwind_testkit::storage_conformance::suite_schema()))
//! }
//!
//! async fn fresh_tenant_repo() -> Arc<dyn Repository> {
//!     Arc::new(MyEngine::new(rushwind_testkit::storage_conformance::tenant_suite_schema()))
//! }
//!
//! rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
//! ```
//!
//! `$factory` must produce a **fresh, empty** repository bound to
//! [`suite_schema`], `$tenant_factory` the same bound to
//! [`tenant_suite_schema`]; the suite seeds and cleans up through the
//! contract's own methods. It is awaited, so engines that connect
//! asynchronously (every real driver) are first-class. A `cargo test` with
//! this suite green is the definition of a conformant storage engine; CI
//! enforces it for every adapter crate. The tenancy module is the
//! isolation gate: an engine that lets a cross-tenant upsert, update, or
//! delete through fails here, not in production.

use std::sync::Mutex;

use crate::rushwind_storage::{AuditEntry, Auditor, ColumnKind, Record, Schema};

/// The standard schema every conformant engine is tested against.
///
/// Table `widgets`: an integer primary key `id`, a nullable real `score`
/// (pins `NULL` semantics), text `name` (pins pattern operators), and the
/// tenancy columns `owner_id`/`unit_id` (pin viewer scoping). Deliberately
/// **not** tenant-scoped — the tenant suite runs against
/// [`tenant_suite_schema`].
pub fn suite_schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid")
}

/// The tenant-scoped schema the tenancy suite runs against: the standard
/// table plus the conventional tenant column, declared as the schema's
/// tenant column.
pub fn tenant_suite_schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("owner_id", ColumnKind::Int)
        .column(
            crate::rushwind_storage::Viewer::TENANT_COLUMN,
            ColumnKind::Int,
        )
        .tenant_column(crate::rushwind_storage::Viewer::TENANT_COLUMN)
        .build()
        .expect("tenant suite schema is valid")
}

/// Builds a suite row without an id (the engine assigns the primary key).
pub fn widget(name: &str, age: i64, score: Option<f64>, owner_id: i64, unit_id: i64) -> Record {
    Record::new()
        .set("name", name)
        .set("age", age)
        .set("score", score)
        .set("owner_id", owner_id)
        .set("unit_id", unit_id)
}

/// Builds a tenant-suite row without an id, owned by `tenant_id`.
pub fn tenant_widget(name: &str, age: i64, owner_id: i64, tenant_id: i64) -> Record {
    Record::new()
        .set("name", name)
        .set("age", age)
        .set("owner_id", owner_id)
        .set(crate::rushwind_storage::Viewer::TENANT_COLUMN, tenant_id)
}

/// Builds a suite row with an explicit primary key.
pub fn widget_with_id(
    id: i64,
    name: &str,
    age: i64,
    score: Option<f64>,
    owner_id: i64,
    unit_id: i64,
) -> Record {
    widget(name, age, score, owner_id, unit_id).set("id", id)
}

/// Builds a tenant-suite row with an explicit primary key.
pub fn tenant_widget_with_id(
    id: i64,
    name: &str,
    age: i64,
    owner_id: i64,
    tenant_id: i64,
) -> Record {
    tenant_widget(name, age, owner_id, tenant_id).set("id", id)
}

/// The five standard tenant rows: three belong to tenant 5, two to
/// tenant 6. All owner 1 to keep the ownership axis out of the way.
pub fn tenant_standard_rows() -> Vec<Record> {
    vec![
        tenant_widget_with_id(1, "t5-alpha", 5, 1, 5),
        tenant_widget_with_id(2, "t5-Bravo", 10, 1, 5),
        tenant_widget_with_id(3, "t5-charlie", 15, 1, 5),
        tenant_widget_with_id(4, "t6-delta", 20, 1, 6),
        tenant_widget_with_id(5, "t6-Echo", 25, 1, 6),
    ]
}

/// The five standard rows the filter and scope tests seed.
pub fn standard_rows() -> Vec<Record> {
    vec![
        widget("alpha", 5, Some(1.5), 1, 10),
        widget("Bravo", 10, None, 1, 20),
        widget("charlie", 15, Some(2.5), 2, 10),
        widget("delta", 20, None, 2, 20),
        widget("Echo", 25, Some(3.5), 3, 30),
    ]
}

/// Thread-safe audit sink the suite injects into a [`QueryCtx`](crate::rushwind_storage::QueryCtx).
#[derive(Default)]
pub struct CountingAuditor {
    entries: Mutex<Vec<AuditEntry>>,
}

impl CountingAuditor {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// All recorded entries so far, in recording order.
    pub fn snapshot(&self) -> Vec<AuditEntry> {
        self.entries.lock().expect("audit log poisoned").clone()
    }
}

impl Auditor for CountingAuditor {
    fn record(&self, entry: AuditEntry) {
        self.entries.lock().expect("audit log poisoned").push(entry);
    }
}

/// The action names of the recorded entries — what the audit tests assert.
pub fn action_names(entries: &[AuditEntry]) -> Vec<&'static str> {
    entries.iter().map(|e| e.action.as_str()).collect()
}

/// Generates the full repository conformance suite for a storage engine.
///
/// `$factory` must be a **crate-rooted path** to an async function of type
/// `async fn() -> Arc<dyn Repository>` producing a **fresh, empty**
/// repository bound to [`suite_schema`]; `$tenant_factory` the same bound
/// to [`tenant_suite_schema`]. The suite expands into two nested modules —
/// the base contract suite and the tenancy suite — where bare identifiers
/// from the invoking module do not resolve.
///
/// Invoke this from the engine crate's integration tests (see the module
/// documentation for the full wiring, including the required `tokio`
/// dev-dependency and the `storage` feature).
#[macro_export]
macro_rules! rushwind_storage_conformance_suite {
    ($factory:expr, tenant: $tenant_factory:expr) => {
        mod rushwind_storage_conformance {
            use std::sync::Arc;

            use $crate::rushwind_storage::{
                DataRange, FieldMask, FilterExpr, ListQuery, Op, Paging, QueryCtx, Record,
                Repository, Sort, SortDir, StorageError, Value, Viewer,
            };
            use $crate::storage_conformance::{
                action_names, standard_rows, widget, widget_with_id, CountingAuditor,
            };

            type Repo = Arc<dyn Repository>;

            // ---- shared helpers -------------------------------------------------

            /// The all-access context every non-tenancy test uses.
            fn ctx() -> QueryCtx {
                QueryCtx::all_access()
            }

            /// A list query with defaults except what the test sets.
            fn query() -> ListQuery {
                ListQuery::default()
            }

            /// Seeds rows through the contract itself (batch, atomic).
            async fn seed(repo: &Repo, rows: Vec<Record>) {
                repo.batch_create(ctx(), rows)
                    .await
                    .expect("seed rows land");
            }

            /// Seeds the five standard rows.
            async fn seed_standard(repo: &Repo) {
                seed(repo, standard_rows()).await;
            }

            /// `n` plainly-ordered rows ("w1".."wn", ages 1..=n) for paging tests.
            async fn seed_indexed(repo: &Repo, n: i64) {
                let rows: Vec<Record> = (1..=n)
                    .map(|i| widget_with_id(i, &format!("w{i}"), i, None, 1, 1))
                    .collect();
                seed(repo, rows).await;
            }

            /// Lists and returns the primary keys of one page.
            async fn ids_of(repo: &Repo, q: &ListQuery) -> Vec<i64> {
                let page = repo.list(ctx(), q).await.expect("list succeeds");
                page.items
                    .iter()
                    .map(|r| r.get("id").and_then(Value::as_i64).expect("int id"))
                    .collect()
            }

            /// Lists through an explicit context and returns primary keys.
            async fn ids_with(repo: &Repo, c: &QueryCtx, q: &ListQuery) -> Vec<i64> {
                let page = repo.list(c.clone(), q).await.expect("list succeeds");
                page.items
                    .iter()
                    .map(|r| r.get("id").and_then(Value::as_i64).expect("int id"))
                    .collect()
            }

            /// Lists a filtered query and returns primary keys.
            async fn ids_filtered(repo: &Repo, f: FilterExpr) -> Vec<i64> {
                ids_of(repo, &query().filtered(f)).await
            }

            /// Asserts `err` is an [`StorageError::InvalidQuery`].
            fn assert_invalid(err: StorageError) {
                assert!(
                    matches!(err, StorageError::InvalidQuery(_)),
                    "expected InvalidQuery, got: {err}"
                );
            }

            /// Reads the integer id out of a stored row.
            fn id_of(row: &Record) -> i64 {
                row.get("id").and_then(Value::as_i64).expect("int id")
            }

            // ---- CRUD semantics -------------------------------------------------

            #[tokio::test]
            async fn create_backfills_primary_key() {
                let repo = $factory().await;
                let stored = repo
                    .create(ctx(), widget("anvil", 1, None, 1, 1))
                    .await
                    .expect("create succeeds");
                let id = id_of(&stored);
                assert!(id > 0, "create must backfill a usable primary key");
                let fetched = repo
                    .get(ctx(), Value::Int(id))
                    .await
                    .expect("get succeeds")
                    .expect("created row is readable");
                assert_eq!(fetched.get("name").and_then(Value::as_str), Some("anvil"));
            }

            #[tokio::test]
            async fn create_roundtrips_every_column() {
                let repo = $factory().await;
                let stored = repo
                    .create(ctx(), widget("bolt", 7, Some(2.5), 4, 9))
                    .await
                    .expect("create succeeds");
                let fetched = repo
                    .get(ctx(), Value::Int(id_of(&stored)))
                    .await
                    .expect("get succeeds")
                    .expect("row exists");
                assert_eq!(fetched.get("name").and_then(Value::as_str), Some("bolt"));
                assert_eq!(fetched.get("age").and_then(Value::as_i64), Some(7));
                assert_eq!(fetched.get("score"), Some(&Value::Real(2.5)));
                assert_eq!(fetched.get("owner_id").and_then(Value::as_i64), Some(4));
                assert_eq!(fetched.get("unit_id").and_then(Value::as_i64), Some(9));
            }

            #[tokio::test]
            async fn null_survives_the_roundtrip() {
                let repo = $factory().await;
                let stored = repo
                    .create(ctx(), widget("nullish", 1, None, 1, 1))
                    .await
                    .expect("create succeeds");
                let fetched = repo
                    .get(ctx(), Value::Int(id_of(&stored)))
                    .await
                    .expect("get succeeds")
                    .expect("row exists");
                // A missing field and an explicit NULL are different things;
                // the contract pins the explicit NULL.
                assert_eq!(fetched.get("score"), Some(&Value::Null));
            }

            #[tokio::test]
            async fn get_missing_returns_none() {
                let repo = $factory().await;
                let row = repo
                    .get(ctx(), Value::Int(404))
                    .await
                    .expect("get succeeds");
                assert!(row.is_none());
            }

            #[tokio::test]
            async fn update_patches_only_given_fields() {
                let repo = $factory().await;
                let stored = repo
                    .create(ctx(), widget("rivet", 3, Some(1.0), 1, 1))
                    .await
                    .expect("create succeeds");
                let id = id_of(&stored);
                let updated = repo
                    .update(ctx(), Value::Int(id), Record::new().set("age", 99))
                    .await
                    .expect("update succeeds");
                assert_eq!(updated.get("age").and_then(Value::as_i64), Some(99));
                assert_eq!(updated.get("name").and_then(Value::as_str), Some("rivet"));
                let fetched = repo
                    .get(ctx(), Value::Int(id))
                    .await
                    .expect("get succeeds")
                    .expect("row exists");
                assert_eq!(fetched.get("age").and_then(Value::as_i64), Some(99));
                assert_eq!(fetched.get("name").and_then(Value::as_str), Some("rivet"));
                assert_eq!(fetched.get("score"), Some(&Value::Real(1.0)));
            }

            #[tokio::test]
            async fn update_missing_is_not_found() {
                let repo = $factory().await;
                let err = repo
                    .update(ctx(), Value::Int(404), Record::new().set("age", 1))
                    .await
                    .expect_err("missing update must fail");
                assert_eq!(err, StorageError::NotFound);
            }

            #[tokio::test]
            async fn delete_removes_the_row() {
                let repo = $factory().await;
                let stored = repo
                    .create(ctx(), widget("scrapped", 1, None, 1, 1))
                    .await
                    .expect("create succeeds");
                let id = Value::Int(id_of(&stored));
                repo.delete(ctx(), id.clone())
                    .await
                    .expect("delete succeeds");
                let gone = repo.get(ctx(), id).await.expect("get succeeds");
                assert!(gone.is_none());
            }

            #[tokio::test]
            async fn delete_missing_is_not_found() {
                let repo = $factory().await;
                let err = repo
                    .delete(ctx(), Value::Int(404))
                    .await
                    .expect_err("missing delete must fail");
                assert_eq!(err, StorageError::NotFound);
            }

            #[tokio::test]
            async fn batch_create_is_atomic() {
                let repo = $factory().await;
                let first = repo
                    .create(ctx(), widget("first", 1, None, 1, 1))
                    .await
                    .expect("create succeeds");
                let first_id = id_of(&first);
                let clash = widget_with_id(first_id, "clash", 2, None, 1, 1);
                let fresh = widget("fresh", 3, None, 1, 1);
                let err = repo
                    .batch_create(ctx(), vec![fresh, clash])
                    .await
                    .expect_err("duplicate pk must fail the whole batch");
                assert!(
                    matches!(err, StorageError::Conflict(_)),
                    "expected Conflict, got: {err}"
                );
                let total = repo.count(ctx(), None).await.expect("count succeeds");
                assert_eq!(total, 1, "a failed batch must land nothing");
            }

            #[tokio::test]
            async fn upsert_inserts_then_updates() {
                let repo = $factory().await;
                let inserted = repo
                    .upsert(ctx(), widget_with_id(9, "first", 1, None, 1, 1))
                    .await
                    .expect("upsert insert succeeds");
                assert_eq!(id_of(&inserted), 9);
                let again = repo
                    .upsert(ctx(), widget_with_id(9, "second", 2, None, 1, 1))
                    .await
                    .expect("upsert update succeeds");
                assert_eq!(again.get("name").and_then(Value::as_str), Some("second"));
                let total = repo.count(ctx(), None).await.expect("count succeeds");
                assert_eq!(total, 1, "upsert must not duplicate rows");
            }

            // ---- filtering ------------------------------------------------------

            #[tokio::test]
            async fn comparison_operators() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                // Standard rows land with ids 1..=5; their ages are 5,10,15,20,25.
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::Gt, [Value::Int(10)])).await,
                    vec![3, 4, 5]
                );
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::Gte, [Value::Int(10)])).await,
                    vec![2, 3, 4, 5]
                );
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::Lt, [Value::Int(15)])).await,
                    vec![1, 2]
                );
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::Lte, [Value::Int(15)])).await,
                    vec![1, 2, 3]
                );
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::Eq, [Value::Int(15)])).await,
                    vec![3]
                );
                assert_eq!(
                    ids_filtered(&repo, FilterExpr::cond("age", Op::NotEq, [Value::Int(10)])).await,
                    vec![1, 3, 4, 5]
                );
            }

            #[tokio::test]
            async fn pattern_operators() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                // SQL wildcards, case-sensitive:
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::Like, [Value::Text("%a%".into())])
                    )
                    .await,
                    vec![1, 2, 3, 4],
                    "%a% matches alpha, Bravo, charlie, delta"
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::Like, [Value::Text("a%".into())])
                    )
                    .await,
                    vec![1],
                    "a% is a case-sensitive prefix"
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::NotLike, [Value::Text("%a%".into())])
                    )
                    .await,
                    vec![5]
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::Ilike, [Value::Text("%E%".into())])
                    )
                    .await,
                    vec![3, 4, 5],
                    "ilike folds case"
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::Contains, [Value::Text("lt".into())])
                    )
                    .await,
                    vec![4]
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::StartsWith, [Value::Text("ch".into())])
                    )
                    .await,
                    vec![3]
                );
                assert_eq!(
                    ids_filtered(
                        &repo,
                        FilterExpr::cond("name", Op::EndsWith, [Value::Text("o".into())])
                    )
                    .await,
                    vec![2, 5]
                );
            }

            #[tokio::test]
            async fn null_operators() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let null_rows = query().filtered(FilterExpr::cond("score", Op::IsNull, []));
                assert_eq!(ids_of(&repo, &null_rows).await, vec![2, 4]);
                let set_rows = query().filtered(FilterExpr::cond("score", Op::IsNotNull, []));
                assert_eq!(ids_of(&repo, &set_rows).await, vec![1, 3, 5]);
            }

            #[tokio::test]
            async fn set_and_range_operators() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let in_set = query().filtered(FilterExpr::cond(
                    "age",
                    Op::In,
                    [Value::Int(5), Value::Int(25)],
                ));
                assert_eq!(ids_of(&repo, &in_set).await, vec![1, 5]);
                let not_in = query().filtered(FilterExpr::cond(
                    "age",
                    Op::NotIn,
                    [Value::Int(5), Value::Int(25)],
                ));
                assert_eq!(ids_of(&repo, &not_in).await, vec![2, 3, 4]);
                let between =
                    query().filtered(FilterExpr::cond("age", Op::Between, [10.into(), 20.into()]));
                assert_eq!(ids_of(&repo, &between).await, vec![2, 3, 4]);
                let outside = query().filtered(FilterExpr::cond(
                    "age",
                    Op::NotBetween,
                    [Value::Int(10), Value::Int(20)],
                ));
                assert_eq!(ids_of(&repo, &outside).await, vec![1, 5]);
            }

            #[tokio::test]
            async fn real_range_operators_compare_numerically() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let q = query().filtered(FilterExpr::cond("score", Op::Gt, [Value::Real(2.0)]));
                assert_eq!(ids_of(&repo, &q).await, vec![3, 5]);
            }

            #[tokio::test]
            async fn and_or_groups_nest() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let mixed = FilterExpr::any([
                    FilterExpr::all([
                        FilterExpr::cond("age", Op::Gte, [Value::Int(10)]),
                        FilterExpr::cond("age", Op::Lte, [Value::Int(20)]),
                    ]),
                    FilterExpr::cond("name", Op::Eq, [Value::Text("alpha".into())]),
                ]);
                assert_eq!(
                    ids_of(&repo, &query().filtered(mixed)).await,
                    vec![1, 2, 3, 4]
                );

                let narrow = FilterExpr::all([
                    FilterExpr::any([
                        FilterExpr::cond("age", Op::Eq, [Value::Int(5)]),
                        FilterExpr::cond("age", Op::Eq, [Value::Int(25)]),
                    ]),
                    FilterExpr::cond("unit_id", Op::Eq, [Value::Int(10)]),
                ]);
                assert_eq!(ids_of(&repo, &query().filtered(narrow)).await, vec![1]);
            }

            #[tokio::test]
            async fn invalid_filters_are_rejected_before_the_engine() {
                let repo = $factory().await;
                let unknown = query().filtered(FilterExpr::cond("nope", Op::Eq, [Value::Int(1)]));
                let err = repo
                    .list(ctx(), &unknown)
                    .await
                    .expect_err("unknown column must fail");
                assert_invalid(err);

                let bad_arity = query().filtered(FilterExpr::cond("age", Op::Between, [1.into()]));
                let err = repo
                    .list(ctx(), &bad_arity)
                    .await
                    .expect_err("between needs two operands");
                assert_invalid(err);
            }

            // ---- sorting --------------------------------------------------------

            #[tokio::test]
            async fn sorting_asc_desc_and_secondary() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let asc = query().ordered(Sort::by("name", SortDir::Asc));
                assert_eq!(ids_of(&repo, &asc).await, vec![2, 5, 1, 3, 4]);
                let desc = query().ordered(Sort::by("name", SortDir::Desc));
                assert_eq!(ids_of(&repo, &desc).await, vec![4, 3, 1, 5, 2]);

                let secondary =
                    query().ordered(Sort::by("unit_id", SortDir::Asc).then("age", SortDir::Desc));
                assert_eq!(ids_of(&repo, &secondary).await, vec![3, 1, 4, 2, 5]);
            }

            // ---- paging ---------------------------------------------------------

            #[tokio::test]
            async fn paging_page_mode() {
                let repo = $factory().await;
                seed_indexed(&repo, 7).await;
                let q = ListQuery::page(1, 3);
                let page = repo.list(ctx(), &q).await.expect("list succeeds");
                assert_eq!(page.total, 7);
                assert_eq!(page.items.len(), 3);
                let q = ListQuery::page(3, 3);
                let page = repo.list(ctx(), &q).await.expect("list succeeds");
                assert_eq!(page.items.len(), 1);
                let q = ListQuery::page(4, 3);
                let page = repo.list(ctx(), &q).await.expect("list succeeds");
                assert!(page.items.is_empty());
                assert_eq!(page.total, 7);
            }

            #[tokio::test]
            async fn paging_offset_mode() {
                let repo = $factory().await;
                seed_indexed(&repo, 7).await;
                let q = ListQuery {
                    paging: Paging::Offset {
                        offset: 3,
                        limit: 3,
                    },
                    ..query()
                };
                assert_eq!(ids_of(&repo, &q).await, vec![4, 5, 6]);
                let tail = ListQuery {
                    paging: Paging::Offset {
                        offset: 6,
                        limit: 3,
                    },
                    ..query()
                };
                assert_eq!(ids_of(&repo, &tail).await, vec![7]);
            }

            #[tokio::test]
            async fn paging_token_stream_covers_everything_once() {
                let repo = $factory().await;
                seed_indexed(&repo, 7).await;
                let mut token = String::new();
                let mut seen: Vec<i64> = Vec::new();
                let mut pages = 0;
                loop {
                    let q = ListQuery {
                        paging: Paging::Token {
                            token: token.clone(),
                            limit: 3,
                        },
                        ..query()
                    };
                    let page = repo.list(ctx(), &q).await.expect("list succeeds");
                    seen.extend(page.items.iter().map(id_of));
                    pages += 1;
                    match page.next_token {
                        Some(next) => token = next,
                        None => break,
                    }
                }
                assert_eq!(
                    seen,
                    vec![1, 2, 3, 4, 5, 6, 7],
                    "cursor streams in pk order, once"
                );
                assert!(
                    pages >= 3,
                    "seven rows at limit 3 need at least three pages"
                );
            }

            #[tokio::test]
            async fn paging_and_sorting_violations_are_rejected() {
                let repo = $factory().await;
                let bad_page = ListQuery {
                    paging: Paging::Page { page: 0, size: 3 },
                    ..query()
                };
                let err = repo
                    .list(ctx(), &bad_page)
                    .await
                    .expect_err("page 0 must fail");
                assert_invalid(err);

                let token_with_sort = ListQuery {
                    paging: Paging::Token {
                        token: String::new(),
                        limit: 3,
                    },
                    sort: Sort::by("name", SortDir::Asc),
                    ..query()
                };
                let err = repo
                    .list(ctx(), &token_with_sort)
                    .await
                    .expect_err("token paging pins pk order");
                assert_invalid(err);

                let stale_cursor = ListQuery {
                    paging: Paging::Token {
                        token: "!!!not-a-cursor".into(),
                        limit: 3,
                    },
                    ..query()
                };
                let err = repo
                    .list(ctx(), &stale_cursor)
                    .await
                    .expect_err("garbage cursors must fail");
                assert_invalid(err);
            }

            // ---- projection -----------------------------------------------------

            #[tokio::test]
            async fn field_mask_projects_returned_rows() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let masked = ListQuery {
                    mask: Some(FieldMask::of(["id", "name"])),
                    ..query()
                };
                let page = repo.list(ctx(), &masked).await.expect("list succeeds");
                assert_eq!(page.items.len(), 5);
                for row in &page.items {
                    for column in row.columns() {
                        assert!(
                            column == "id" || column == "name",
                            "masked rows may only carry masked columns, got {column:?}"
                        );
                    }
                }
                let plain = repo.list(ctx(), &query()).await.expect("list succeeds");
                assert_eq!(
                    plain.items[0].len(),
                    repo.schema().columns.len(),
                    "unmasked rows carry every declared column"
                );
            }

            // ---- viewer tenancy -------------------------------------------------

            #[tokio::test]
            async fn viewer_all_and_none_bound_the_table() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let total = repo
                    .count(QueryCtx::all_access(), None)
                    .await
                    .expect("count succeeds");
                assert_eq!(total, 5);

                let denied = QueryCtx::default(); // DataRange::None closes by default
                let page = repo
                    .list(denied.clone(), &query())
                    .await
                    .expect("a denied scope is not an error, just empty");
                assert!(page.items.is_empty());
                assert_eq!(page.total, 0);
                let count = repo
                    .count(denied.clone(), None)
                    .await
                    .expect("count under denial succeeds");
                assert_eq!(count, 0);
                let hidden = repo
                    .get(denied.clone(), Value::Int(1))
                    .await
                    .expect("get under denial succeeds");
                assert!(
                    hidden.is_none(),
                    "denied rows look exactly like missing rows"
                );
            }

            #[tokio::test]
            async fn viewer_own_scope_isolates_rows() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let own = QueryCtx::new(Viewer::own(1));
                let q = query();
                let page = repo.list(own.clone(), &q).await.expect("list succeeds");
                assert_eq!(page.items.len(), 2, "viewer 1 owns rows 1 and 2");
                // Out-of-scope rows look exactly like missing rows:
                let foreign = repo
                    .get(own.clone(), Value::Int(3))
                    .await
                    .expect("get succeeds");
                assert!(foreign.is_none());
                // The boundary holds for writes too:
                let err = repo
                    .update(own.clone(), Value::Int(3), Record::new().set("age", 0))
                    .await
                    .expect_err("out-of-scope update must fail");
                assert_eq!(err, StorageError::NotFound);
                let err = repo
                    .delete(own.clone(), Value::Int(3))
                    .await
                    .expect_err("out-of-scope delete must fail");
                assert_eq!(err, StorageError::NotFound);
                // And the row really is untouched:
                let intact = repo
                    .get(ctx(), Value::Int(3))
                    .await
                    .expect("get succeeds")
                    .expect("row survived");
                assert_eq!(intact.get("age").and_then(Value::as_i64), Some(15));
            }

            #[tokio::test]
            async fn viewer_unit_and_user_scopes() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let unit = QueryCtx::new(Viewer::unit(10));
                assert_eq!(ids_with(&repo, &unit, &query()).await, vec![1, 3]);

                let user = QueryCtx::new(Viewer {
                    range: DataRange::User,
                    subjects: vec![2, 3],
                    ..Viewer::default()
                });
                assert_eq!(ids_with(&repo, &user, &query()).await, vec![3, 4, 5]);
            }

            #[tokio::test]
            async fn count_and_exists_honor_the_scope() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let own = QueryCtx::new(Viewer::own(1));
                let n = repo.count(own.clone(), None).await.expect("count succeeds");
                assert_eq!(n, 2, "viewer 1 owns rows 1 and 2 only");
                let foreign = repo
                    .exists(
                        own.clone(),
                        Some(FilterExpr::cond(
                            "name",
                            Op::Eq,
                            [Value::Text("charlie".into())],
                        )),
                    )
                    .await
                    .expect("exists succeeds");
                assert!(!foreign, "an out-of-scope row satisfies no predicate");
                let own_row = repo
                    .exists(
                        own.clone(),
                        Some(FilterExpr::cond(
                            "name",
                            Op::Eq,
                            [Value::Text("alpha".into())],
                        )),
                    )
                    .await
                    .expect("exists succeeds");
                assert!(own_row);
            }

            #[tokio::test]
            async fn create_on_an_occupied_pk_conflicts() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let err = repo
                    .create(ctx(), widget_with_id(1, "impostor", 0, None, 1, 1))
                    .await
                    .expect_err("creating onto an occupied pk must conflict");
                assert!(
                    matches!(err, StorageError::Conflict(_)),
                    "expected Conflict, got: {err}"
                );
                let intact = repo
                    .get(ctx(), Value::Int(1))
                    .await
                    .expect("get succeeds")
                    .expect("row survived the collision");
                assert_eq!(intact.get("name").and_then(Value::as_str), Some("alpha"));
            }

            #[tokio::test]
            async fn upsert_never_touches_an_out_of_scope_row() {
                let repo = $factory().await;
                seed_standard(&repo).await;
                let own = QueryCtx::new(Viewer::own(1));
                // Row 3 belongs to owner 2: invisible to this viewer. The
                // upsert must take the insert face — and an insert onto an
                // occupied pk is a conflict, never a silent overwrite of
                // the invisible occupant.
                let err = repo
                    .upsert(own.clone(), widget_with_id(3, "hijack", 0, None, 1, 1))
                    .await
                    .expect_err("upsert onto an out-of-scope occupied pk must fail");
                assert!(
                    matches!(err, StorageError::Conflict(_)),
                    "expected Conflict, got: {err}"
                );
                let intact = repo
                    .get(ctx(), Value::Int(3))
                    .await
                    .expect("get succeeds")
                    .expect("the foreign row survived untouched");
                assert_eq!(intact.get("name").and_then(Value::as_str), Some("charlie"));
                assert_eq!(intact.get("age").and_then(Value::as_i64), Some(15));
                // And the in-scope face still updates:
                let updated = repo
                    .upsert(own.clone(), widget_with_id(1, "alpha", 99, None, 1, 1))
                    .await
                    .expect("in-scope upsert succeeds");
                assert_eq!(updated.get("age").and_then(Value::as_i64), Some(99));
            }

            // ---- auditing -------------------------------------------------------

            #[tokio::test]
            async fn audit_entries_flow_to_the_sink() {
                let repo = $factory().await;
                let auditor = Arc::new(CountingAuditor::new());
                let audited = QueryCtx::all_access().audited(auditor.clone());

                let stored = repo
                    .create(audited.clone(), widget("logged", 1, None, 1, 1))
                    .await
                    .expect("create succeeds");
                let id = id_of(&stored);
                repo.update(audited.clone(), Value::Int(id), Record::new().set("age", 2))
                    .await
                    .expect("update succeeds");
                repo.delete(audited.clone(), Value::Int(id))
                    .await
                    .expect("delete succeeds");

                let names = action_names(&auditor.snapshot());
                assert_eq!(names, vec!["create", "update", "delete"]);
            }
        }

        mod rushwind_storage_tenancy {
            use std::sync::Arc;

            use $crate::rushwind_storage::{
                ListQuery, Paging, QueryCtx, Record, Repository, StorageError, Value, Viewer,
            };
            use $crate::storage_conformance::{
                tenant_standard_rows, tenant_widget, tenant_widget_with_id,
            };

            type Repo = Arc<dyn Repository>;

            /// The platform context: sees and writes across every tenant.
            fn platform() -> QueryCtx {
                QueryCtx::all_access()
            }

            /// A tenant business context bound to one tenant id.
            fn tenant(id: i64) -> QueryCtx {
                QueryCtx::new(Viewer::tenant(id))
            }

            /// A viewer whose tenancy nobody attached — the fail-closed
            /// shape. Ownership `Own(1)` so the *only* axis under test is
            /// the tenant one.
            fn bare() -> QueryCtx {
                QueryCtx::new(Viewer::own(1))
            }

            async fn seed(repo: &Repo) {
                repo.batch_create(platform(), tenant_standard_rows())
                    .await
                    .expect("platform seeds the tenant rows");
            }

            fn row(id: i64, name: &str, age: i64, owner: i64, tid: i64) -> Record {
                tenant_widget_with_id(id, name, age, owner, tid)
            }

            fn tenant_of(stored: &Record) -> Option<i64> {
                stored.get(Viewer::TENANT_COLUMN).and_then(Value::as_i64)
            }

            // ---- writes ----------------------------------------------------------

            #[tokio::test]
            async fn create_force_stamps_the_tenant() {
                let repo = $tenant_factory().await;
                // A tenant actor forging another tenant's column loses to
                // the stamp:
                let forged = row(0, "impostor", 1, 1, 6).set(Viewer::TENANT_COLUMN, 6);
                let stored = repo
                    .create(tenant(5), forged)
                    .await
                    .expect("tenant create succeeds");
                assert_eq!(
                    tenant_of(&stored),
                    Some(5),
                    "the stamp wins over the payload"
                );
                let seen = repo
                    .get(tenant(6), Value::Int(id_of_value(stored.get("id"))))
                    .await
                    .expect("get succeeds");
                assert!(seen.is_none(), "the stamped row belongs to tenant 5 only");
            }

            #[tokio::test]
            async fn platform_create_respects_explicit_tenant() {
                let repo = $tenant_factory().await;
                let stored = repo
                    .create(platform(), row(0, "for-tenant-6", 1, 1, 6))
                    .await
                    .expect("platform create succeeds");
                assert_eq!(tenant_of(&stored), Some(6));
                let seen = repo
                    .get(tenant(6), Value::Int(id_of_value(stored.get("id"))))
                    .await
                    .expect("get succeeds");
                assert!(seen.is_some(), "tenant 6 sees the row addressed to it");
            }

            #[tokio::test]
            async fn a_tenantless_row_is_nobodys() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                // A tenant-scoped row whose tenant column carries no value
                // is nobody's: NULL matches no tenant predicate, so only
                // the platform view reaches it. A tenant actor cannot see,
                // count, or adopt it — the stamp's other half.
                let stored = repo
                    .create(
                        platform(),
                        Record::new()
                            .set("name", "tenantless")
                            .set("age", 1)
                            .set("owner_id", 1),
                    )
                    .await
                    .expect("platform create succeeds");
                assert_eq!(
                    tenant_of(&stored),
                    None,
                    "no payload value means the tenant column stays NULL"
                );
                let id = id_of_value(stored.get("id"));
                let seen = repo
                    .get(tenant(5), Value::Int(id))
                    .await
                    .expect("get succeeds");
                assert!(
                    seen.is_none(),
                    "a NULL-tenant row is invisible to every tenant"
                );
                let n = repo.count(tenant(5), None).await.expect("count succeeds");
                assert_eq!(
                    n, 3,
                    "the tenantless row adds nothing to any tenant's count"
                );
                let all = repo
                    .count(platform(), None)
                    .await
                    .expect("platform count succeeds");
                assert_eq!(all, 6, "only the platform view reaches it");
            }

            #[tokio::test]
            async fn batch_create_stamps_every_row() {
                let repo = $tenant_factory().await;
                let forged = vec![row(0, "a", 1, 1, 9), row(0, "b", 2, 1, 9)];
                let stored = repo
                    .batch_create(tenant(5), forged)
                    .await
                    .expect("batch create succeeds");
                assert_eq!(stored.len(), 2, "every input row lands");
                for one in &stored {
                    assert_eq!(tenant_of(one), Some(5), "the stamp applies to every row");
                }
            }

            #[tokio::test]
            async fn tenant_cannot_migrate_a_row_by_patching_the_tenant_column() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                let updated = repo
                    .update(
                        tenant(5),
                        Value::Int(1),
                        Record::new().set(Viewer::TENANT_COLUMN, 6),
                    )
                    .await
                    .expect("in-tenant update succeeds");
                assert_eq!(
                    tenant_of(&updated),
                    Some(5),
                    "the stamp overrides a tenant-migrating patch"
                );
            }

            // ---- reads -----------------------------------------------------------

            #[tokio::test]
            async fn list_and_count_are_tenant_scoped() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                let page = repo
                    .list(tenant(5), &ListQuery::default())
                    .await
                    .expect("list succeeds");
                let t5: Vec<i64> = page
                    .items
                    .iter()
                    .map(|r| id_of_value(r.get("id")))
                    .collect();
                assert_eq!(t5, vec![1, 2, 3]);
                assert_eq!(page.total, 3);
                let n = repo.count(tenant(6), None).await.expect("count succeeds");
                assert_eq!(n, 2);
                let all = repo
                    .count(platform(), None)
                    .await
                    .expect("platform count succeeds");
                assert_eq!(all, 5);
            }

            #[tokio::test]
            async fn cross_tenant_get_is_not_found() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                let seen = repo
                    .get(tenant(5), Value::Int(4))
                    .await
                    .expect("get succeeds");
                assert!(
                    seen.is_none(),
                    "a foreign row is indistinguishable from missing"
                );
            }

            #[tokio::test]
            async fn cross_tenant_update_and_delete_are_not_found() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                let err = repo
                    .update(tenant(5), Value::Int(4), Record::new().set("age", 0))
                    .await
                    .expect_err("cross-tenant update must fail");
                assert_eq!(err, StorageError::NotFound);
                let err = repo
                    .delete(tenant(5), Value::Int(4))
                    .await
                    .expect_err("cross-tenant delete must fail");
                assert_eq!(err, StorageError::NotFound);
                let intact = repo
                    .get(platform(), Value::Int(4))
                    .await
                    .expect("get succeeds")
                    .expect("the foreign row survived untouched");
                assert_eq!(intact.get("age").and_then(Value::as_i64), Some(20));
            }

            #[tokio::test]
            async fn cross_tenant_upsert_conflicts_and_leaves_the_foreign_row_intact() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                // Row 4 belongs to tenant 6. Tenant 5 upserts onto it —
                // the update face requires visibility, the insert face
                // hits an occupied pk: a conflict that mutates nothing.
                let err = repo
                    .upsert(tenant(5), row(4, "hijack", 0, 5, 5))
                    .await
                    .expect_err("cross-tenant upsert must fail");
                assert!(
                    matches!(err, StorageError::Conflict(_)),
                    "expected Conflict, got: {err}"
                );
                let intact = repo
                    .get(platform(), Value::Int(4))
                    .await
                    .expect("get succeeds")
                    .expect("the foreign row survived");
                assert_eq!(intact.get("name").and_then(Value::as_str), Some("t6-delta"));
                assert_eq!(tenant_of(&intact), Some(6));
                assert_eq!(intact.get("age").and_then(Value::as_i64), Some(20));
                // The in-tenant face still updates:
                let updated = repo
                    .upsert(tenant(5), row(1, "t5-alpha", 99, 1, 5))
                    .await
                    .expect("in-tenant upsert succeeds");
                assert_eq!(updated.get("age").and_then(Value::as_i64), Some(99));
                assert_eq!(tenant_of(&updated), Some(5));
            }

            #[tokio::test]
            async fn token_paging_is_tenant_scoped() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                let mut seen = Vec::new();
                let mut token = String::new();
                loop {
                    let q = ListQuery {
                        paging: Paging::Token {
                            token: std::mem::take(&mut token),
                            limit: 2,
                        },
                        ..ListQuery::default()
                    };
                    let page = repo.list(tenant(5), &q).await.expect("token page succeeds");
                    let ids: Vec<i64> = page
                        .items
                        .iter()
                        .map(|r| id_of_value(r.get("id")))
                        .collect();
                    seen.extend(ids);
                    match page.next_token {
                        Some(next) => token = next,
                        None => break,
                    }
                }
                assert_eq!(seen, vec![1, 2, 3], "token streaming sees one tenant only");
            }

            // ---- fail-closed -----------------------------------------------------

            #[tokio::test]
            async fn unidentified_viewer_fails_closed_on_every_path() {
                let repo = $tenant_factory().await;
                seed(&repo).await;
                // A viewer whose tenancy nobody attached must *error* on a
                // tenant-scoped table — never silently pass with the tenant
                // predicate dropped, never just see an empty table.
                macro_rules! fail_closed {
                    ($what:literal, $call:expr) => {
                        match $call {
                            Err(StorageError::MissingViewer) => {}
                            other => panic!(
                                "{} must fail closed with MissingViewer, got: {:?}",
                                $what, other
                            ),
                        }
                    };
                }
                fail_closed!("get", repo.get(bare(), Value::Int(1)).await);
                fail_closed!("list", repo.list(bare(), &ListQuery::default()).await);
                fail_closed!("count", repo.count(bare(), None).await);
                fail_closed!(
                    "create",
                    repo.create(bare(), tenant_widget("x", 1, 1, 5)).await
                );
                fail_closed!(
                    "batch_create",
                    repo.batch_create(bare(), vec![tenant_widget("x", 1, 1, 5)])
                        .await
                );
                fail_closed!(
                    "update",
                    repo.update(bare(), Value::Int(1), Record::new().set("age", 2))
                        .await
                );
                fail_closed!("upsert", repo.upsert(bare(), row(1, "x", 1, 1, 5)).await);
                fail_closed!("delete", repo.delete(bare(), Value::Int(1)).await);
                // And the row is still intact afterwards:
                let intact = repo
                    .get(platform(), Value::Int(1))
                    .await
                    .expect("get succeeds")
                    .expect("row survived");
                assert_eq!(tenant_of(&intact), Some(5));
            }

            /// Reads the integer id out of a stored row.
            fn id_of_value(v: Option<&Value>) -> i64 {
                v.and_then(Value::as_i64).expect("int id")
            }
        }
    };
}

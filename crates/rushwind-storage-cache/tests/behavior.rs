//! Behavioral tests for the cache decorator beyond contract transparency:
//! hit ratios, singleflight coalescing, TTL expiry, invalidation, and
//! scope isolation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushwind_storage::{QueryCtx, Record, RepoFuture, Repository, Schema, Value};
use rushwind_storage_cache::CacheRepo;
use rushwind_storage_memory::MemoryRepo;

fn schema() -> Schema {
    rushwind_testkit::storage_conformance::suite_schema()
}

/// Counts `get` calls and optionally stalls them, so tests can observe
/// exactly how many times the backend was actually touched.
struct CountingInner {
    repo: MemoryRepo,
    gets: AtomicUsize,
    stall: Option<Duration>,
}

impl CountingInner {
    fn new(stall: Option<Duration>) -> Self {
        Self {
            repo: MemoryRepo::new(schema()).expect("schema is valid"),
            gets: AtomicUsize::new(0),
            stall,
        }
    }

    fn get_count(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }

    async fn seed(&self, name: &str) -> i64 {
        let stored = self
            .repo
            .create(QueryCtx::all_access(), Record::new().set("name", name))
            .await
            .expect("seed row");
        stored.get("id").and_then(Value::as_i64).expect("int id")
    }
}

impl Repository for CountingInner {
    fn schema(&self) -> &Schema {
        self.repo.schema()
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let repo = &self.repo;
        let stall = self.stall;
        Box::pin(async move {
            if let Some(stall) = stall {
                tokio::time::sleep(stall).await;
            }
            repo.get(ctx, id).await
        })
    }

    fn list<'a>(
        &'a self,
        ctx: QueryCtx,
        query: &'a rushwind_storage::ListQuery,
    ) -> RepoFuture<'a, rushwind_storage::Page<Record>> {
        self.repo.list(ctx, query)
    }

    fn count(
        &self,
        ctx: QueryCtx,
        filter: Option<rushwind_storage::FilterExpr>,
    ) -> RepoFuture<'_, u64> {
        self.repo.count(ctx, filter)
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        self.repo.create(ctx, row)
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        self.repo.batch_create(ctx, rows)
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        self.repo.update(ctx, id, patch)
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        self.repo.upsert(ctx, row)
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        self.repo.delete(ctx, id)
    }
}

fn cache_over(inner: Arc<CountingInner>) -> Arc<CacheRepo> {
    CacheRepo::new(inner)
}

fn cache_with_ttl(inner: Arc<CountingInner>, ttl: Duration) -> Arc<CacheRepo> {
    CacheRepo::with_ttl(inner, ttl)
}

#[tokio::test]
async fn repeated_gets_hit_the_backend_once() {
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("bolt").await;
    let cache = cache_over(Arc::clone(&inner));

    for _ in 0..5 {
        let row = cache
            .get(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect("get succeeds");
        assert!(row.is_some());
    }
    assert_eq!(inner.get_count(), 1, "repeats must be served from cache");
}

#[tokio::test]
async fn missing_rows_are_cached_too() {
    let inner = Arc::new(CountingInner::new(None));
    let cache = cache_over(Arc::clone(&inner));

    for _ in 0..3 {
        let row = cache
            .get(QueryCtx::all_access(), Value::Int(404))
            .await
            .expect("get succeeds");
        assert!(row.is_none());
    }
    assert_eq!(
        inner.get_count(),
        1,
        "negative results must be cached to stop penetration"
    );
}

#[tokio::test]
async fn concurrent_misses_coalesce_into_one_load() {
    let inner = Arc::new(CountingInner::new(Some(Duration::from_millis(50))));
    let id = inner.seed("stampede").await;
    let cache = cache_over(Arc::clone(&inner));

    let mut joiners = Vec::new();
    for _ in 0..8 {
        let cache = Arc::clone(&cache);
        joiners.push(tokio::spawn(async move {
            cache
                .get(QueryCtx::all_access(), Value::Int(id))
                .await
                .expect("get succeeds")
        }));
    }
    let mut rows = Vec::new();
    for joiner in joiners {
        rows.push(joiner.await.expect("task joins"));
    }
    assert!(rows.iter().all(|row| row.is_some()));
    assert_eq!(
        inner.get_count(),
        1,
        "eight concurrent misses must ride one backend load"
    );
}

#[tokio::test]
async fn ttl_expiry_sends_reads_back_to_the_backend() {
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("milk").await;
    let cache = cache_with_ttl(Arc::clone(&inner), Duration::from_millis(30));

    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("first get");
    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("cached get");
    assert_eq!(inner.get_count(), 1);

    tokio::time::sleep(Duration::from_millis(60)).await;
    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("expired get");
    assert_eq!(inner.get_count(), 2, "expired entries must be reloaded");
}

#[tokio::test]
async fn writes_invalidate_every_cached_view() {
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("stale").await;
    let cache = cache_over(Arc::clone(&inner));

    let before = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    assert_eq!(before.get("name").and_then(Value::as_str), Some("stale"));

    cache
        .update(
            QueryCtx::all_access(),
            Value::Int(id),
            Record::new().set("name", "fresh"),
        )
        .await
        .expect("update lands");

    let after = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    assert_eq!(
        after.get("name").and_then(Value::as_str),
        Some("fresh"),
        "a cached stale row must not survive its own update"
    );
}

#[tokio::test]
async fn loads_in_flight_during_a_write_are_dropped_as_stale() {
    // The loader stalls; the write lands mid-load; the load's result must
    // not repopulate the cache with the pre-write row.
    let inner = Arc::new(CountingInner::new(Some(Duration::from_millis(80))));
    let id = inner.seed("race").await;
    let cache = cache_over(Arc::clone(&inner));

    let loader_cache = Arc::clone(&cache);
    let loader = tokio::spawn(async move {
        loader_cache
            .get(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect("get succeeds")
    });
    // Let the loader register itself, then write while it is stalled.
    tokio::time::sleep(Duration::from_millis(20)).await;
    cache
        .update(
            QueryCtx::all_access(),
            Value::Int(id),
            Record::new().set("name", "after-write"),
        )
        .await
        .expect("update lands");
    let _ = loader.await.expect("task joins");

    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    let name = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    assert_eq!(
        name.get("name").and_then(Value::as_str),
        Some("after-write"),
        "a load that started before a write must not cache the pre-write row"
    );
}

#[tokio::test]
async fn different_viewers_never_share_entries() {
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("scoped").await;
    let cache = cache_over(Arc::clone(&inner));

    let first = QueryCtx::new(rushwind_storage::Viewer::own(1));
    let second = QueryCtx::new(rushwind_storage::Viewer::own(2));

    cache
        .get(first.clone(), Value::Int(id))
        .await
        .expect("get succeeds");
    cache
        .get(second.clone(), Value::Int(id))
        .await
        .expect("get succeeds");
    assert_eq!(
        inner.get_count(),
        2,
        "each viewer's scope is its own cache namespace"
    );

    // And the same viewer still hits:
    cache
        .get(first, Value::Int(id))
        .await
        .expect("get succeeds");
    assert_eq!(inner.get_count(), 2);
}

#[tokio::test]
async fn different_tenants_never_share_entries() {
    // Same data range, same actor — different tenant ids. The tenant axis
    // is part of the scope key, so the two viewers keep separate cache
    // namespaces; folding them together would hand one tenant's cached
    // rows to the other.
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("tenanted").await;
    let cache = cache_over(Arc::clone(&inner));

    let tenant_five = QueryCtx::new(rushwind_storage::Viewer::tenant(5));
    let tenant_six = QueryCtx::new(rushwind_storage::Viewer::tenant(6));

    let five = cache
        .get(tenant_five.clone(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("an unscoped table shows the row to every tenant viewer");
    assert_eq!(five.get("name").and_then(Value::as_str), Some("tenanted"));
    let six = cache
        .get(tenant_six.clone(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("tenant 6 must not inherit tenant 5's cache entry");
    assert_eq!(six.get("name").and_then(Value::as_str), Some("tenanted"));
    assert_eq!(
        inner.get_count(),
        2,
        "each tenant id is its own cache namespace"
    );

    // And the same tenant still hits:
    cache
        .get(tenant_five, Value::Int(id))
        .await
        .expect("get succeeds");
    cache
        .get(tenant_six, Value::Int(id))
        .await
        .expect("get succeeds");
    assert_eq!(inner.get_count(), 2);
}

#[tokio::test]
async fn deletes_invalidate_too() {
    let inner = Arc::new(CountingInner::new(None));
    let id = inner.seed("gone").await;
    let cache = cache_over(Arc::clone(&inner));

    cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds");
    cache
        .delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");
    let row = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds");
    assert!(row.is_none(), "a deleted row must not resurrect from cache");
}

#[tokio::test]
async fn create_invalidates_a_stale_negative_entry() {
    let inner = Arc::new(CountingInner::new(None));
    let cache = cache_over(Arc::clone(&inner));

    // Cache the absence of id 7.
    let missing = cache
        .get(QueryCtx::all_access(), Value::Int(7))
        .await
        .expect("get succeeds");
    assert!(missing.is_none());

    // Land a row on exactly that id.
    cache
        .create(
            QueryCtx::all_access(),
            rushwind_testkit::storage_conformance::widget_with_id(7, "arrived", 1, None, 1, 1),
        )
        .await
        .expect("create lands");

    let row = cache
        .get(QueryCtx::all_access(), Value::Int(7))
        .await
        .expect("get succeeds")
        .expect("the negative entry must have been invalidated by the create");
    assert_eq!(row.get("name").and_then(Value::as_str), Some("arrived"));
    assert_eq!(
        inner.get_count(),
        2,
        "the post-create read must go back to the backend"
    );
}

#[tokio::test]
async fn batch_create_invalidates_every_landed_id() {
    let inner = Arc::new(CountingInner::new(None));
    let cache = cache_over(Arc::clone(&inner));

    // Pre-cache both ids as absent.
    for id in [8, 9] {
        let row = cache
            .get(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect("get succeeds");
        assert!(row.is_none());
    }

    cache
        .batch_create(
            QueryCtx::all_access(),
            vec![
                rushwind_testkit::storage_conformance::widget_with_id(8, "eight", 1, None, 1, 1),
                rushwind_testkit::storage_conformance::widget_with_id(9, "nine", 2, None, 1, 1),
            ],
        )
        .await
        .expect("batch lands");

    let eight = cache
        .get(QueryCtx::all_access(), Value::Int(8))
        .await
        .expect("get succeeds")
        .expect("id 8 must be visible");
    let nine = cache
        .get(QueryCtx::all_access(), Value::Int(9))
        .await
        .expect("get succeeds")
        .expect("id 9 must be visible");
    assert_eq!(eight.get("name").and_then(Value::as_str), Some("eight"));
    assert_eq!(nine.get("name").and_then(Value::as_str), Some("nine"));
}

#[tokio::test]
async fn upsert_invalidates_the_cached_row() {
    let inner = Arc::new(CountingInner::new(None));
    let cache = cache_over(Arc::clone(&inner));
    let id = 5;
    cache
        .create(
            QueryCtx::all_access(),
            rushwind_testkit::storage_conformance::widget_with_id(id, "before", 1, None, 1, 1),
        )
        .await
        .expect("create lands");

    let cached = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    assert_eq!(cached.get("age").and_then(Value::as_i64), Some(1));

    cache
        .upsert(
            QueryCtx::all_access(),
            rushwind_testkit::storage_conformance::widget_with_id(id, "before", 42, None, 1, 1),
        )
        .await
        .expect("upsert lands");

    let after = cache
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds")
        .expect("row exists");
    assert_eq!(
        after.get("age").and_then(Value::as_i64),
        Some(42),
        "the cached pre-upsert row must not survive its own upsert"
    );
    assert_eq!(inner.get_count(), 2, "the post-upsert read reloads");
}

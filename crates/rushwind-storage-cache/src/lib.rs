//! Cache-aside decorator for the [`Repository`] contract.
//!
//! Point reads (`get`) are cached with a TTL; concurrent misses on the same
//! key are **coalesced into one backend load** (the singleflight pattern
//! that prevents a cache stampede), and missing rows are cached too, so a
//! nonexistent id cannot punch through on every request. Every write path
//! invalidates all cached views of the touched primary key.
//!
//! The two tenancy-critical decisions:
//!
//! - **Cache keys include the viewer's scope, on both axes.** `Viewer::own(1)`
//!   and `Viewer::own(2)` never share an entry, and neither do two viewers
//!   with the same data range but different [`Tenancy`] ids — the tenant
//!   axis is part of the key, so the cache can never leak a row across
//!   tenants (the decorator stays transparent to the conformance suite's
//!   viewer and tenancy semantics).
//! - **Invalidation bumps a per-key generation.** A load that started
//!   before a write committed must not repopulate the cache with a stale
//!   row after the write invalidated — the generation check drops it.
//!
//! The decorator never audits on its own behalf: it forwards the ctx — and
//! therefore the audit sink — to the inner engine, whose entries flow
//! unchanged. List, count and exists pass straight through: their
//! invalidation surface is the whole table, which no per-key cache earns
//! back.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use rushwind_storage::{
    DataRange, FilterExpr, ListQuery, Page, QueryCtx, Record, RepoFuture, Repository, StorageError,
    Tenancy, Value,
};

/// The default time-to-live for a cached row.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

/// A [`Repository`] decorator adding cache-aside reads to any inner engine.
pub struct CacheRepo {
    inner: Arc<dyn Repository>,
    ttl: Duration,
    state: Mutex<State>,
}

struct State {
    entries: HashMap<CacheKey, Entry>,
    /// One load in progress per key; joiners ride the senders.
    inflight: HashMap<CacheKey, Vec<oneshot::Sender<Outcome>>>,
    /// Bumped on every write touching the id; guards against stale
    /// repopulation by loads that started before the write.
    generations: HashMap<i64, u64>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    scope: ScopeKey,
    id: i64,
}

/// The viewer shape a cache entry is bound to — every field the read
/// scope depends on. The tenant axis rides along: two viewers can carry
/// identical ranges, actors, and subjects yet address different tenants,
/// and folding them into one key would serve tenant 5's rows to tenant 6.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ScopeKey {
    tenancy: Tenancy,
    range: DataRange,
    actor: Option<i64>,
    unit: Option<i64>,
    subjects: Vec<i64>,
}

impl ScopeKey {
    fn of(viewer: &rushwind_storage::Viewer) -> Self {
        Self {
            tenancy: viewer.tenancy,
            range: viewer.range,
            actor: viewer.actor_id,
            unit: viewer.unit_id,
            subjects: viewer.subjects.clone(),
        }
    }
}

struct Entry {
    row: Option<Record>,
    expires_at: Instant,
}

type Outcome = Result<Option<Record>, StorageError>;

impl CacheRepo {
    /// Wraps `inner` with a cache using [`DEFAULT_TTL`].
    pub fn new(inner: Arc<dyn Repository>) -> Arc<Self> {
        Self::with_ttl(inner, DEFAULT_TTL)
    }

    /// Wraps `inner` with a cache using an explicit time-to-live.
    pub fn with_ttl(inner: Arc<dyn Repository>, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            ttl,
            state: Mutex::new(State {
                entries: HashMap::new(),
                inflight: HashMap::new(),
                generations: HashMap::new(),
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("cache state poisoned")
    }

    /// Drops every cached view of `id` and invalidates loads in flight for
    /// it. Called after each successful write.
    fn invalidate(&self, id: i64) {
        let mut state = self.lock();
        state.entries.retain(|key, _| key.id != id);
        *state.generations.entry(id).or_insert(0) += 1;
    }

    fn expect_id(&self, id: &Value) -> Result<i64, StorageError> {
        id.as_i64()
            .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
    }

    /// The read path: hit → serve; miss → load once, coalescing joiners.
    async fn get_async(&self, ctx: QueryCtx, id: Value) -> Outcome {
        let id = self.expect_id(&id)?;
        let key = CacheKey {
            scope: ScopeKey::of(&ctx.viewer),
            id,
        };

        // Reserve: hit → serve from cache; joiner → ride the in-flight
        // load; loader → registered as the one backend reader, carrying the
        // generation snapshot it must still match when it lands.
        let (joiner, generation) = {
            let mut state = self.lock();
            if let Some(entry) = state.entries.get(&key) {
                if entry.expires_at > Instant::now() {
                    return Ok(entry.row.clone());
                }
                state.entries.remove(&key);
            }
            match state.inflight.get_mut(&key) {
                Some(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    (Some(rx), 0)
                }
                None => {
                    let generation = state.generations.get(&id).copied().unwrap_or(0);
                    state.inflight.insert(key.clone(), Vec::new());
                    (None, generation)
                }
            }
        };
        if let Some(rx) = joiner {
            return rx.await.unwrap_or_else(|_| {
                Err(StorageError::Backend(
                    "cache loader dropped its load".into(),
                ))
            });
        }

        let outcome = self.inner.get(ctx, Value::Int(id)).await;

        let mut state = self.lock();
        let waiters = state.inflight.remove(&key).unwrap_or_default();
        if let Ok(row) = &outcome {
            // A write that committed while we loaded bumped the generation;
            // its invalidation outranks this stale load.
            if state.generations.get(&id).copied().unwrap_or(0) == generation {
                state.entries.insert(
                    key,
                    Entry {
                        row: row.clone(),
                        expires_at: Instant::now() + self.ttl,
                    },
                );
            }
        }
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
        outcome
    }

    async fn create_async(&self, ctx: QueryCtx, row: Record) -> Result<Record, StorageError> {
        let stored = self.inner.create(ctx, row).await?;
        if let Some(id) = self.primary_of(&stored) {
            self.invalidate(id);
        }
        Ok(stored)
    }

    async fn batch_create_async(
        &self,
        ctx: QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        let stored = self.inner.batch_create(ctx, rows).await?;
        for row in &stored {
            if let Some(id) = self.primary_of(row) {
                self.invalidate(id);
            }
        }
        Ok(stored)
    }

    async fn update_async(
        &self,
        ctx: QueryCtx,
        id: Value,
        patch: Record,
    ) -> Result<Record, StorageError> {
        let id = self.expect_id(&id)?;
        let updated = self.inner.update(ctx, Value::Int(id), patch).await?;
        self.invalidate(id);
        Ok(updated)
    }

    async fn upsert_async(&self, ctx: QueryCtx, row: Record) -> Result<Record, StorageError> {
        let stored = self.inner.upsert(ctx, row).await?;
        if let Some(id) = self.primary_of(&stored) {
            self.invalidate(id);
        }
        Ok(stored)
    }

    async fn delete_async(&self, ctx: QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        self.inner.delete(ctx, Value::Int(id)).await?;
        self.invalidate(id);
        Ok(())
    }

    fn primary_of(&self, row: &Record) -> Option<i64> {
        row.get(&self.inner.schema().primary_key)
            .and_then(Value::as_i64)
    }
}

impl Repository for CacheRepo {
    fn schema(&self) -> &rushwind_storage::Schema {
        self.inner.schema()
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(self.get_async(ctx, id))
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        Box::pin(async move { self.inner.list(ctx, query).await })
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        Box::pin(async move { self.inner.count(ctx, filter).await })
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(self.create_async(ctx, row))
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        Box::pin(self.batch_create_async(ctx, rows))
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        Box::pin(self.update_async(ctx, id, patch))
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(self.upsert_async(ctx, row))
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        Box::pin(self.delete_async(ctx, id))
    }
}

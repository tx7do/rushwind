//! Soft-delete decorator for the [`Repository`] contract, engine-agnostic because
//! it is a decorator: it wraps *any* repository (in-memory, SeaORM,
//! MongoDB, …) and needs nothing from the engine but a nullable timestamp
//! column.
//!
//! The convention: the table declares a `deleted_at` integer column (unix
//! epoch millis); `NULL` means alive. The decorator then
//!
//! - turns [`Repository::delete`] into a tombstone write,
//! - filters tombstoned rows out of every read (get / list / count /
//!   exists) and out of [`Repository::update`] targets — a deleted row is
//!   indistinguishable from a missing one, exactly like viewer scoping,
//! - offers [`SoftDeleteRepo::restore_async`] (clear the tombstone) and
//!   [`SoftDeleteRepo::purge_async`] (the real delete) as opt-outs, and
//! - lets [`Repository::upsert`] resurrect a tombstoned id with fresh data
//!   — upsert writes into the visible world.
//!
//! Audit entries flow through untouched: the inner engine still records
//! every mutation; a delete is audited as `Update` (it *is* one), the
//! purge as `Delete`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rushwind_storage::{
    AuditAction, ColumnKind, FilterExpr, FilterNode, ListQuery, Op, Page, QueryCtx, Record,
    RepoFuture, Repository, Schema, StorageError, Value,
};

/// The conventional tombstone column name.
pub const DELETED_AT_COLUMN: &str = "deleted_at";

/// A [`Repository`] decorator implementing soft deletes over any engine.
pub struct SoftDeleteRepo {
    inner: Arc<dyn Repository>,
    column: String,
}

impl std::fmt::Debug for SoftDeleteRepo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SoftDeleteRepo")
            .field("table", &self.inner.schema().table)
            .field("column", &self.column)
            .finish()
    }
}

impl SoftDeleteRepo {
    /// Wraps `inner`, using the conventional [`DELETED_AT_COLUMN`] column.
    ///
    /// The schema must declare it as an integer column; without the column
    /// there is nothing to tombstone with.
    pub fn new(inner: Arc<dyn Repository>) -> Result<Arc<Self>, StorageError> {
        Self::with_column(inner, DELETED_AT_COLUMN)
    }

    /// Wraps `inner` with an explicit tombstone column name.
    pub fn with_column(
        inner: Arc<dyn Repository>,
        column: impl Into<String>,
    ) -> Result<Arc<Self>, StorageError> {
        let column = column.into();
        let schema = inner.schema();
        let declared = schema.column(&column).ok_or_else(|| {
            StorageError::InvalidQuery(format!(
                "soft delete needs a {column:?} column on table {:?}",
                schema.table
            ))
        })?;
        if declared.kind != ColumnKind::Int {
            return Err(StorageError::InvalidQuery(format!(
                "the {column:?} column must be an integer (unix epoch millis)"
            )));
        }
        Ok(Arc::new(Self { inner, column }))
    }

    /// The raw inner repository — the escape hatch `purge` and tests use to
    /// see tombstoned rows.
    pub fn inner(&self) -> &Arc<dyn Repository> {
        &self.inner
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or_default()
    }

    /// The alive predicate: `deleted_at IS NULL`.
    fn alive() -> FilterExpr {
        FilterExpr::cond(DELETED_AT_COLUMN, Op::IsNull, [])
    }

    /// Conjoins the alive predicate onto a query's own filter.
    fn alive_query(query: &ListQuery) -> ListQuery {
        let mut filter = query.filter.clone().unwrap_or_default();
        filter = match filter.node() {
            FilterNode::All(children) if children.is_empty() => Self::alive(),
            _ => FilterExpr::all([filter, Self::alive()]),
        };
        ListQuery {
            filter: Some(filter),
            ..query.clone()
        }
    }

    fn is_tombstoned(row: &Record, column: &str) -> bool {
        !matches!(row.get(column), Some(Value::Null) | None)
    }

    fn expect_id(&self, id: &Value) -> Result<i64, StorageError> {
        id.as_i64()
            .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
    }

    /// Clears the tombstone on a soft-deleted row and returns it.
    pub async fn restore_async(&self, ctx: &QueryCtx, id: Value) -> Result<Record, StorageError> {
        let id = self.expect_id(&id)?;
        let raw = self
            .inner
            .get(ctx.clone(), Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        if !Self::is_tombstoned(&raw, &self.column) {
            // An alive row looks missing to restore — same doctrine as
            // scoping: only tombstones are restorable.
            return Err(StorageError::NotFound);
        }
        let restored = self
            .inner
            .update(
                self.silent(ctx),
                Value::Int(id),
                Record::new().set(&self.column, Value::Null),
            )
            .await?;
        self.audit(
            ctx,
            rushwind_storage::AuditAction::Update,
            Some(Value::Int(id)),
        );
        Ok(restored)
    }

    /// A copy of the caller's context without the audit sink: tombstone
    /// writes reach the inner engine silently, and *this* decorator speaks
    /// for them — a delete is audited as `Delete`, not as the `Update` it
    /// happens to be underneath.
    fn silent(&self, ctx: &QueryCtx) -> QueryCtx {
        QueryCtx {
            viewer: ctx.viewer.clone(),
            auditor: None,
        }
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(rushwind_storage::AuditEntry::now(
            action,
            self.inner.schema().table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }

    /// The real delete — bypasses the tombstone entirely.
    pub async fn purge_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        self.inner.delete(ctx.clone(), Value::Int(id)).await
    }

    /// Lists tombstoned rows (the inverse view; masks and filters still
    /// apply, the alive predicate is flipped).
    pub async fn list_deleted_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(self.inner.schema())?;
        let mut filter = query.filter.clone().unwrap_or_default();
        filter = match filter.node() {
            FilterNode::All(children) if children.is_empty() => {
                FilterExpr::cond(DELETED_AT_COLUMN, Op::IsNotNull, [])
            }
            _ => FilterExpr::all([
                filter,
                FilterExpr::cond(DELETED_AT_COLUMN, Op::IsNotNull, []),
            ]),
        };
        self.inner
            .list(
                ctx.clone(),
                &ListQuery {
                    filter: Some(filter),
                    ..query.clone()
                },
            )
            .await
    }

    // ---- synchronous-shaped helpers over the async trait -------------------

    async fn get_async(&self, ctx: &QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let row = self.inner.get(ctx.clone(), id).await?;
        Ok(row.filter(|row| !Self::is_tombstoned(row, &self.column)))
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        self.inner
            .list(ctx.clone(), &Self::alive_query(query))
            .await
    }

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        let alive = Self::alive();
        let combined = match filter {
            Some(existing) => FilterExpr::all([existing, alive]),
            None => alive,
        };
        self.inner.count(ctx.clone(), Some(combined)).await
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        let mut alive = row;
        alive.insert(&self.column, Value::Null);
        self.inner.create(ctx.clone(), alive).await
    }

    async fn batch_create_async(
        &self,
        ctx: &QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        let rows = rows
            .into_iter()
            .map(|mut row| {
                row.insert(&self.column, Value::Null);
                row
            })
            .collect();
        self.inner.batch_create(ctx.clone(), rows).await
    }

    async fn update_async(
        &self,
        ctx: &QueryCtx,
        id: Value,
        patch: Record,
    ) -> Result<Record, StorageError> {
        let raw = self
            .inner
            .get(ctx.clone(), id.clone())
            .await?
            .ok_or(StorageError::NotFound)?;
        if Self::is_tombstoned(&raw, &self.column) {
            return Err(StorageError::NotFound);
        }
        self.inner.update(ctx.clone(), id, patch).await
    }

    async fn upsert_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        let mut alive = row;
        alive.insert(&self.column, Value::Null);
        self.inner.upsert(ctx.clone(), alive).await
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        // A tombstone write on a row that is not visible (missing or
        // already tombstoned) must be NotFound, exactly like a real delete.
        let raw = self
            .inner
            .get(ctx.clone(), Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        if Self::is_tombstoned(&raw, &self.column) {
            return Err(StorageError::NotFound);
        }
        self.inner
            .update(
                self.silent(ctx),
                Value::Int(id),
                Record::new().set(&self.column, Self::now_millis()),
            )
            .await?;
        self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

impl Repository for SoftDeleteRepo {
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(async move { self.get_async(&ctx, id).await })
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        Box::pin(async move { self.list_async(&ctx, query).await })
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        Box::pin(async move { self.count_async(&ctx, filter).await })
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.create_async(&ctx, row).await })
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        Box::pin(async move { self.batch_create_async(&ctx, rows).await })
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.update_async(&ctx, id, patch).await })
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.upsert_async(&ctx, row).await })
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        Box::pin(async move { self.delete_async(&ctx, id).await })
    }
}

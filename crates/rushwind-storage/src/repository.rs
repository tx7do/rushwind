//! The one contract every storage engine implements.

use std::future::Future;
use std::pin::Pin;

use crate::context::QueryCtx;
use crate::error::StorageError;
use crate::field_mask::FieldMask;
use crate::filter::FilterExpr;
use crate::paging::{Page, Paging};
use crate::record::Record;
use crate::schema::Schema;
use crate::sorting::Sort;
use crate::tree_path::RecursiveCte;
use crate::value::Value;

/// The future type returned by [`Repository`] methods.
///
/// Boxed and lifetime-borrowed for the same reason as
/// [`ServerFuture`](https://docs.rs/rushwind-transport/rushwind_transport/type.ServerFuture.html):
/// the trait stays object-safe (`dyn Repository` drives the multi-engine
/// story), and callers can await borrowed futures without `'static`
/// ownership.
pub type RepoFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

/// A list query: filter, ordering, paging, and projection in one bundle.
#[derive(Clone, Debug, Default)]
pub struct ListQuery {
    /// The predicate; `None` selects everything in scope.
    pub filter: Option<FilterExpr>,
    /// The ordering; the default is primary key ascending.
    pub sort: Sort,
    /// The paging strategy.
    pub paging: Paging,
    /// The requested projection; `None` returns full rows.
    pub mask: Option<FieldMask>,
}

impl ListQuery {
    /// A page-number query over everything in scope.
    pub fn page(page: u32, size: u32) -> Self {
        Self {
            paging: Paging::Page { page, size },
            ..Self::default()
        }
    }

    /// Attaches a filter, builder style.
    pub fn filtered(mut self, filter: FilterExpr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Attaches an ordering, builder style.
    pub fn ordered(mut self, sort: Sort) -> Self {
        self.sort = sort;
        self
    }

    /// Validates the whole query against a schema.
    pub fn validate(&self, schema: &Schema) -> Result<(), StorageError> {
        self.paging.validate()?;
        if let Some(filter) = &self.filter {
            filter.validate(schema)?;
        }
        self.sort.validate(schema)?;
        // Token paging streams by primary key; an explicit sort would be
        // silently ignored, so it is rejected instead.
        if matches!(self.paging, Paging::Token { .. }) && !self.sort.is_default() {
            return Err(StorageError::InvalidQuery(
                "token paging orders by primary key; drop the explicit sort".into(),
            ));
        }
        Ok(())
    }
}

/// The generic data-access contract — one interface, many engines.
///
/// Implementations are constructed bound to one [`Schema`]; the trait's
/// methods are then the full CRUD surface. Every method takes the
/// [`QueryCtx`] whose viewer scope is the permission boundary.
///
/// # Conformance
///
/// Adapter crates assert the full contract by invoking
/// `rushwind_testkit::rushwind_storage_conformance_suite!` from their
/// integration-test target; an engine is only conformant when the entire
/// suite passes.
/// The generic data-access contract — one interface, many engines.
///
/// Implementations are constructed bound to one [`Schema`]; the trait's
/// methods are then the full CRUD surface. Every method takes the
/// [`QueryCtx`] **by value** — cloning it is cheap (the auditor sits behind
/// an `Arc`), and the borrowed-future signature stays as simple as the
/// transport contract's single-reference shape. The ctx's viewer scope is
/// the permission boundary.
///
/// # Conformance
///
/// Adapter crates assert the full contract by invoking
/// `rushwind_testkit::rushwind_storage_conformance_suite!` from their
/// integration-test target; an engine is only conformant when the entire
/// suite passes.
pub trait Repository: Send + Sync {
    /// The schema this repository is bound to.
    fn schema(&self) -> &Schema;

    /// Reads one row by primary key; `Ok(None)` for missing **or**
    /// out-of-scope rows.
    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>>;

    /// Lists one page of rows.
    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>>;

    /// Counts the rows matching `filter` (within the viewer's scope).
    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64>;

    /// Whether at least one row matches `filter`. The default defers to
    /// [`Repository::count`]; engines with a cheap `EXISTS` override it.
    fn exists(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, bool> {
        Box::pin(async move { self.count(ctx, filter).await.map(|n| n > 0) })
    }

    /// Inserts one row; the implementation backfills the primary key and
    /// returns the full stored row.
    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record>;

    /// Inserts many rows atomically: either every row lands or none does.
    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>>;

    /// Patches one row by primary key and returns the updated row; missing
    /// or out-of-scope rows yield [`StorageError::NotFound`].
    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record>;

    /// Inserts the row, or — when a row with the same primary key is
    /// visible in scope — updates it, returning the stored row.
    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record>;

    /// Deletes one row by primary key; missing or out-of-scope rows yield
    /// [`StorageError::NotFound`].
    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()>;

    /// The optional recursive-CTE fast path for tree traversals (see
    /// [`RecursiveCte`]); the tree layer probes it and falls back to
    /// its contract-level composition when absent. Defaults to `None`.
    ///
    /// A **decorator must keep the default**: a forwarded fast path
    /// would answer queries the decorator never saw — soft-delete's
    /// tombstone filter and the cache's scope-keyed entries only
    /// apply on the paths that pass through the decorator. Only bare
    /// engines override this.
    fn recursive_cte(&self) -> Option<&dyn RecursiveCte> {
        None
    }
}

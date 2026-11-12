//! The optional recursive-CTE fast path for tree traversals — the
//! capability a SQL engine offers the tree layer
//! (`rushwind-storage-tree`) so deep sweeps run as one recursive query
//! instead of one list per level.
//!
//! The contract-level composition in `rushwind-storage-tree` stays the
//! semantic baseline: whatever an engine answers through this trait
//! must match what the per-level walks answer for the same tree — same
//! ordering (depth, then primary key), same cycle refusal, same scope
//! enforcement. The engine's own viewer scope and tenant axis apply
//! inside the recursive query, exactly as they do on every other read
//! path.

use crate::context::QueryCtx;
use crate::record::Record;
use crate::repository::RepoFuture;

/// The recursive-CTE fast path for tree queries.
pub trait RecursiveCte: Send + Sync {
    /// The subtree below `root`, root excluded: `(row, depth)` pairs
    /// with depth 1 = direct child, ordered depth-then-primary-key —
    /// the composition path's breadth-first order. `parent_column`
    /// names the hierarchy column (the tree layer's configured one).
    /// Viewer scope and the tenant axis are enforced inside the
    /// recursive query; a parent cycle is
    /// [`StorageError::InvalidQuery`], never an infinite loop.
    fn descendants<'a>(
        &'a self,
        ctx: &'a QueryCtx,
        parent_column: &'a str,
        root: i64,
        max_depth: Option<u32>,
    ) -> RepoFuture<'a, Vec<(Record, u32)>>;

    /// The ancestor chain above `id`, `id` excluded, root first.
    /// `parent_column` names the hierarchy column. A dangling parent
    /// ends the chain like a root; a parent cycle is
    /// [`StorageError::InvalidQuery`].
    fn ancestors<'a>(
        &'a self,
        ctx: &'a QueryCtx,
        parent_column: &'a str,
        id: i64,
    ) -> RepoFuture<'a, Vec<Record>>;
}

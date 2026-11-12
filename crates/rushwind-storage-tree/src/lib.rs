//! Tree traversal over any [`Repository`].
//!
//! The convention: a table is hierarchical when it declares a `parent_id`
//! integer column (`NULL` = root). On top of that single column this crate
//! expresses the whole tree vocabulary with contract-level queries only —
//! `children`/`roots` are filtered lists, `ancestors` is a parent-chain
//! walk, `subtree` is a level-by-level breadth-first sweep — so **every**
//! engine (in-memory, SQL, MongoDB, decorated, …) supports it with zero
//! extra machinery.
//!
//! The trade is deliberate: deep subtrees cost one list per level on the
//! composition path, and the walks carry a cycle guard because the
//! contract cannot forbid a row from parenting itself. Engines carrying
//! the contract's [`rushwind_storage::RecursiveCte`] capability answer the
//! deep walks (`subtree`/`ancestors`/`is_ancestor`) as **one recursive
//! query** instead — the SQL engines' fast path, with scope and tenant
//! enforced inside the CTE and identical answers pinned by the same
//! suites. A bare engine only: a decorator forwarding the capability
//! would bypass its own rewriting, so the probe stays `None` there by
//! contract.
//! A cycle is [`StorageError::InvalidQuery`], never an infinite loop; a
//! dangling parent id ends the walk like a root.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeSet;

use rushwind_storage::{
    FilterExpr, ListQuery, Op, QueryCtx, Record, Repository, StorageError, Value,
};

/// The conventional parent column name.
pub const PARENT_COLUMN: &str = "parent_id";

/// Tree traversal bound to one repository (and one viewer context per
/// call). The default parent column is [`PARENT_COLUMN`].
pub struct Tree<'a> {
    repo: &'a dyn Repository,
    parent_column: &'a str,
}

/// One node of a [`Tree::subtree`] sweep: the record plus its depth below
/// the queried root (1 = direct child).
#[derive(Clone, Debug, PartialEq)]
pub struct SubtreeNode {
    /// Depth below the queried id; direct children are depth 1.
    pub depth: u32,
    /// The row itself.
    pub record: Record,
}

impl<'a> Tree<'a> {
    /// Binds traversal to a repository using [`PARENT_COLUMN`].
    pub fn new(repo: &'a dyn Repository) -> Self {
        Self {
            repo,
            parent_column: PARENT_COLUMN,
        }
    }

    /// Binds traversal with an explicit parent column name.
    pub fn with_column(repo: &'a dyn Repository, parent_column: &'a str) -> Self {
        Self {
            repo,
            parent_column,
        }
    }

    fn parent_of(&self, row: &Record) -> Option<i64> {
        row.get(self.parent_column).and_then(Value::as_i64)
    }

    fn id_of(&self, row: &Record) -> Option<i64> {
        row.get(&self.repo.schema().primary_key)
            .and_then(Value::as_i64)
    }

    async fn rows_with_parent(
        &self,
        ctx: &QueryCtx,
        parent: Option<i64>,
    ) -> Result<Vec<Record>, StorageError> {
        let (op, values) = match parent {
            Some(id) => (Op::Eq, vec![Value::Int(id)]),
            None => (Op::IsNull, vec![]),
        };
        let query = ListQuery {
            filter: Some(FilterExpr::cond(self.parent_column, op, values)),
            ..ListQuery::default()
        };
        self.repo
            .list(ctx.clone(), &query)
            .await
            .map(|page| page.items)
    }

    /// The row's direct children, primary key ordered.
    pub async fn children(&self, ctx: &QueryCtx, id: i64) -> Result<Vec<Record>, StorageError> {
        self.rows_with_parent(ctx, Some(id)).await
    }

    /// The roots: rows whose parent is `NULL`.
    pub async fn roots(&self, ctx: &QueryCtx) -> Result<Vec<Record>, StorageError> {
        self.rows_with_parent(ctx, None).await
    }

    /// The row's ancestor chain, root first, excluding the row itself.
    /// A dangling parent id ends the walk; a parent cycle is
    /// [`StorageError::InvalidQuery`].
    pub async fn ancestors(&self, ctx: &QueryCtx, id: i64) -> Result<Vec<Record>, StorageError> {
        // The fast path: one recursive query for the whole chain (see the
        // subtree note). The composition below stays the baseline.
        if let Some(fast) = self.repo.recursive_cte() {
            return fast.ancestors(ctx, self.parent_column, id).await;
        }
        let mut chain: Vec<Record> = Vec::new();
        let mut visited: BTreeSet<i64> = BTreeSet::new();
        visited.insert(id);
        let mut cursor = id;
        loop {
            let Some(row) = self.repo.get(ctx.clone(), Value::Int(cursor)).await? else {
                break; // dangling id: the chain ends here, like a root
            };
            let Some(parent) = self.parent_of(&row) else {
                break; // NULL parent: a root
            };
            if !visited.insert(parent) {
                return Err(StorageError::InvalidQuery(format!(
                    "parent cycle detected at id {parent}"
                )));
            }
            match self.repo.get(ctx.clone(), Value::Int(parent)).await? {
                Some(parent_row) => chain.push(parent_row),
                None => break, // dangling parent
            }
            cursor = parent;
        }
        chain.reverse();
        Ok(chain)
    }

    /// Whether `ancestor` sits on `descendant`'s parent chain (strictly
    /// above it — a row is not its own ancestor).
    pub async fn is_ancestor(
        &self,
        ctx: &QueryCtx,
        ancestor: i64,
        descendant: i64,
    ) -> Result<bool, StorageError> {
        // The fast path: the whole chain in one recursive query, membership
        // checked on the returned rows. The composition below stays the
        // baseline.
        if let Some(fast) = self.repo.recursive_cte() {
            return Ok(fast
                .ancestors(ctx, self.parent_column, descendant)
                .await?
                .iter()
                .any(|row| self.id_of(row) == Some(ancestor)));
        }
        let mut visited: BTreeSet<i64> = BTreeSet::new();
        visited.insert(descendant);
        let mut cursor = descendant;
        loop {
            let Some(row) = self.repo.get(ctx.clone(), Value::Int(cursor)).await? else {
                return Ok(false);
            };
            let Some(parent) = self.parent_of(&row) else {
                return Ok(false);
            };
            if parent == ancestor {
                return Ok(true);
            }
            if !visited.insert(parent) {
                return Err(StorageError::InvalidQuery(format!(
                    "parent cycle detected at id {parent}"
                )));
            }
            cursor = parent;
        }
    }

    /// The breadth-first subtree below `id`, level by level, **excluding
    /// the node itself**. `max_depth` caps the sweep (`Some(1)` = direct
    /// children, `None` = the whole branch); cycles are
    /// [`StorageError::InvalidQuery`].
    pub async fn subtree(
        &self,
        ctx: &QueryCtx,
        id: i64,
        max_depth: Option<u32>,
    ) -> Result<Vec<SubtreeNode>, StorageError> {
        // The fast path: an engine carrying the recursive-CTE capability
        // answers the whole sweep in one query, scope and tenant enforced
        // inside the CTE. The composition below stays the baseline — the
        // capability's answers must match it exactly, which the engine
        // suites pin.
        if let Some(fast) = self.repo.recursive_cte() {
            return fast
                .descendants(ctx, self.parent_column, id, max_depth)
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|(record, depth)| SubtreeNode { depth, record })
                        .collect()
                });
        }
        let mut found: Vec<SubtreeNode> = Vec::new();
        let mut visited: BTreeSet<i64> = BTreeSet::new();
        visited.insert(id);
        let mut frontier = vec![id];
        let mut depth: u32 = 0;
        while !frontier.is_empty() {
            if max_depth.is_some_and(|cap| depth >= cap) {
                break;
            }
            depth += 1;
            // One list per level: the filter is a set membership test.
            let filter = FilterExpr::cond(
                self.parent_column,
                Op::In,
                frontier.iter().copied().map(Value::Int),
            );
            let query = ListQuery {
                filter: Some(filter),
                ..ListQuery::default()
            };
            let page = self.repo.list(ctx.clone(), &query).await?;
            let mut next: Vec<i64> = Vec::new();
            for row in page.items {
                let Some(row_id) = self.id_of(&row) else {
                    return Err(StorageError::Backend(
                        "a tree row lost its primary key".into(),
                    ));
                };
                if !visited.insert(row_id) {
                    return Err(StorageError::InvalidQuery(format!(
                        "parent cycle detected at id {row_id}"
                    )));
                }
                next.push(row_id);
                found.push(SubtreeNode { depth, record: row });
            }
            frontier = next;
        }
        Ok(found)
    }
}

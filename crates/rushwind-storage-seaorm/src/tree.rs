//! The recursive-CTE fast path for tree traversals — the engine's
//! implementation of the contract's [`RecursiveCte`] capability: deep
//! sweeps and ancestor chains as one recursive query each, with the
//! viewer scope and tenant axis enforced inside the CTE itself.
//!
//! Shape (descendants — the ancestor face mirrors it upward):
//!
//! ```sql
//! WITH RECURSIVE rushwind_tree(id, depth) AS (
//!   SELECT id, 0 FROM widgets WHERE id = {root}      -- the anchor: it
//!       -- never surfaces, so it needs no scope predicate
//!   UNION ALL
//!   SELECT widgets.id, rushwind_tree.depth + 1
//!   FROM widgets JOIN rushwind_tree ON widgets.parent_id = rushwind_tree.id
//!     AND rushwind_tree.depth < {scoped_count + 1}   -- the spin guard
//!     [AND rushwind_tree.depth < {max_depth}]
//!     AND <scope> AND <tenant>                       -- every surfaced
//! )                                                  -- row scope-checked
//! SELECT widgets.*, rushwind_tree.depth AS rushwind_depth
//! FROM widgets JOIN rushwind_tree ON widgets.id = rushwind_tree.id
//! WHERE widgets.id != {root}
//! ORDER BY rushwind_tree.depth, widgets.id           -- composition order
//! ```
//!
//! Cycle honesty: `UNION ALL` over a parent cycle would spin forever,
//! so the recursion carries a depth cap — the viewer-scoped row count
//! plus one, which every acyclic path fits (a path of depth *d*
//! touches *d + 1* distinct rows). Detection is exact and cheap:
//! under one-parent-per-row data, two paths reaching the same id is
//! precisely a cycle, so a duplicate id in the result is
//! [`StorageError::InvalidQuery`] — the same refusal the per-level
//! composition gives. The same deduplication discipline runs on the
//! ancestor face.
//!
//! Scope mapping: the anchor row is exempt (it only anchors, never
//! surfaces — matching the composition, whose walk never reads the
//! root row itself); every surfaced row is scope-checked on its own
//! read, matching the per-level list. `ancestors` scope-checks the
//! seed row too, because its composition reads `id` itself first — an
//! out-of-scope id answers an empty chain there.

use sea_orm::sea_query::{
    Alias, CommonTableExpression, Condition, Expr, ExprTrait, JoinType, Query, SelectStatement,
    UnionType, WithClause,
};
use sea_orm::ConnectionTrait;

use rushwind_storage::{
    QueryCtx, Record, RecursiveCte, RepoFuture, Repository, Schema as ContractSchema, StorageError,
};

use crate::{ScopeSql, SeaRepo, TenantSql};

/// The depth column alias on the final select — namespaced so a table
/// column can never collide with it.
const DEPTH_COLUMN: &str = "rushwind_depth";
/// The CTE's name.
const TREE_CTE: &str = "rushwind_tree";

impl RecursiveCte for SeaRepo {
    fn descendants<'a>(
        &'a self,
        ctx: &'a QueryCtx,
        parent_column: &'a str,
        root: i64,
        max_depth: Option<u32>,
    ) -> RepoFuture<'a, Vec<(Record, u32)>> {
        Box::pin(async move {
            let scope = self.scope_sql(ctx)?;
            if matches!(scope, ScopeSql::Deny) {
                return Ok(Vec::new());
            }
            let tenant = TenantSql::resolve(&self.schema, ctx)?;
            let predicate = tree_predicate(&scope, &tenant);
            let cap = self.count(ctx.clone(), None).await? as i64 + 1;
            let statement = descendants_statement(
                &self.schema,
                parent_column,
                root,
                max_depth,
                cap,
                &predicate,
            );
            let rows = self.query_tree_rows(&statement).await?;
            detect_cycle(&rows, &self.schema.primary_key)?;
            Ok(rows)
        })
    }

    fn ancestors<'a>(
        &'a self,
        ctx: &'a QueryCtx,
        parent_column: &'a str,
        id: i64,
    ) -> RepoFuture<'a, Vec<Record>> {
        Box::pin(async move {
            let scope = self.scope_sql(ctx)?;
            if matches!(scope, ScopeSql::Deny) {
                return Ok(Vec::new());
            }
            let tenant = TenantSql::resolve(&self.schema, ctx)?;
            let predicate = tree_predicate(&scope, &tenant);
            let cap = self.count(ctx.clone(), None).await? as i64 + 1;
            let statement = ancestors_statement(&self.schema, parent_column, id, cap, &predicate);
            let rows = self.query_tree_rows(&statement).await?;
            detect_cycle(&rows, &self.schema.primary_key)?;
            Ok(rows.into_iter().map(|(record, _)| record).collect())
        })
    }
}

impl SeaRepo {
    /// Runs a mapped tree query: the base columns map through the
    /// schema, the aliased depth column rides alongside as an i64.
    async fn query_tree_rows(
        &self,
        select: &SelectStatement,
    ) -> Result<Vec<(Record, u32)>, StorageError> {
        let result = self
            .db
            .query_all_raw(self.statement(select))
            .await
            .map_err(|e| self.map_err(e))?;
        let columns: Vec<String> = self
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        result
            .iter()
            .map(|row| {
                let record = self.row_to_record(row, &columns)?;
                let depth = row
                    .try_get_by::<i64, _>(DEPTH_COLUMN)
                    .map_err(|e| self.map_err(e))?;
                Ok((record, std::cmp::Ord::max(depth, 0) as u32))
            })
            .collect()
    }
}

/// The scope + tenant predicate conjoined — what every surfaced row
/// must satisfy, applied on the recursion's reads.
fn tree_predicate(scope: &ScopeSql, tenant: &TenantSql) -> Condition {
    let mut combined = Condition::all();
    if let ScopeSql::Pass(Some(scope_cond)) = scope {
        combined = combined.add(scope_cond.clone());
    }
    if let Some(tenant_cond) = tenant.predicate() {
        combined = combined.add(tenant_cond);
    }
    combined
}

/// Builds the downward recursive query: the anchor, the scoped
/// recursion, the depth-ordered final select with the root excluded.
pub(crate) fn descendants_statement(
    schema: &ContractSchema,
    parent_column: &str,
    root: i64,
    max_depth: Option<u32>,
    cap: i64,
    predicate: &Condition,
) -> SelectStatement {
    let table = Alias::new(&schema.table);
    let pk = Alias::new(&schema.primary_key);
    let parent = Alias::new(parent_column);
    let cte = Alias::new(TREE_CTE);
    let cte_depth = Expr::col((Alias::new(TREE_CTE), Alias::new("depth")));

    // Seed: the anchor row only — it never surfaces, so it needs no
    // scope; the recursion scope-checks everything that does.
    let mut seed = Query::select();
    seed.column(pk.clone()).expr(Expr::val(0i64));
    seed.from(table.clone());
    seed.and_where(Expr::col(pk.clone()).eq(root));

    // Step: rows whose parent is already in the CTE — scope-checked on
    // their own read, spin-guarded, depth-capped. A surfaced node sits
    // at `depth + 1`, admitted only when that stays within max_depth.
    let mut step = Query::select();
    step.expr(Expr::col((table.clone(), pk.clone())));
    step.expr(cte_depth.clone().add(1));
    step.from(table.clone());
    // SQLite forbids a recursive member from referencing its own CTE
    // inside a subquery — the direct join is the portable reference.
    step.join(
        JoinType::InnerJoin,
        cte.clone(),
        Condition::all().add(
            Expr::col((table.clone(), parent.clone()))
                .equals((Alias::new(TREE_CTE), Alias::new("id"))),
        ),
    );
    step.and_where(cte_depth.clone().lt(cap));
    if let Some(max) = max_depth {
        step.and_where(cte_depth.clone().lt(max as i64));
    }
    step.cond_where(predicate.clone());

    // Final: the surfaced rows with their depths, root excluded, in
    // the composition's (depth, primary key) order.
    let mut select = Query::select();
    for column in &schema.columns {
        select.column((table.clone(), Alias::new(&column.name)));
    }
    select.expr_as(cte_depth.clone(), Alias::new(DEPTH_COLUMN));
    select.from(table.clone());
    select.join(
        JoinType::InnerJoin,
        cte.clone(),
        Condition::all().add(
            Expr::col((table.clone(), pk.clone())).equals((Alias::new(TREE_CTE), Alias::new("id"))),
        ),
    );
    select.and_where(Expr::col((table.clone(), pk.clone())).ne(root));
    select.order_by_expr(cte_depth.clone(), sea_orm::sea_query::Order::Asc);
    select.order_by_expr(
        Expr::col((table.clone(), pk)),
        sea_orm::sea_query::Order::Asc,
    );
    select.with_cte(with_recursive(cte, seed, step));
    select
}

/// Builds the upward recursive query: the seed is the direct parent of
/// `id` (scope-checked — the composition reads `id` itself first), the
/// recursion walks parent by parent, and the final select orders
/// root-first by descending depth. A dangling parent never joins a
/// base row, so the chain ends there.
pub(crate) fn ancestors_statement(
    schema: &ContractSchema,
    parent_column: &str,
    id: i64,
    cap: i64,
    predicate: &Condition,
) -> SelectStatement {
    let table = Alias::new(&schema.table);
    let pk = Alias::new(&schema.primary_key);
    let parent = Alias::new(parent_column);
    let cte = Alias::new(TREE_CTE);
    let cte_depth = Expr::col((Alias::new(TREE_CTE), Alias::new("depth")));

    // Seed: the direct parent, if it exists and is in scope. The seed
    // reads `id`'s own row — exactly the composition's first `get`.
    let mut seed = Query::select();
    seed.column(parent.clone()).expr(Expr::val(0i64));
    seed.from(table.clone());
    seed.and_where(Expr::col(pk.clone()).eq(id));
    seed.and_where(Expr::col(parent.clone()).is_not_null());
    seed.cond_where(predicate.clone());

    // Step: parents of rows already in the CTE, each scope-checked on
    // its own read, spin-guarded, NULL parents excluded.
    let mut step = Query::select();
    step.expr(Expr::col((table.clone(), parent.clone())));
    step.expr(cte_depth.clone().add(1));
    step.from(table.clone());
    // The direct join, for the same SQLite recursion rule.
    step.join(
        JoinType::InnerJoin,
        cte.clone(),
        Condition::all().add(
            Expr::col((table.clone(), pk.clone())).equals((Alias::new(TREE_CTE), Alias::new("id"))),
        ),
    );
    step.and_where(Expr::col((table.clone(), parent.clone())).is_not_null());
    step.and_where(cte_depth.clone().lt(cap));
    step.cond_where(predicate.clone());

    // Final: the ancestor rows, root first (the largest depth is the
    // most distant ancestor).
    let mut select = Query::select();
    for column in &schema.columns {
        select.column((table.clone(), Alias::new(&column.name)));
    }
    select.expr_as(cte_depth.clone(), Alias::new(DEPTH_COLUMN));
    select.from(table.clone());
    select.join(
        JoinType::InnerJoin,
        cte.clone(),
        Condition::all().add(
            Expr::col((table.clone(), pk.clone())).equals((Alias::new(TREE_CTE), Alias::new("id"))),
        ),
    );
    select.order_by_expr(cte_depth, sea_orm::sea_query::Order::Desc);
    select.order_by_expr(Expr::col((table, pk)), sea_orm::sea_query::Order::Asc);
    select.with_cte(with_recursive(cte, seed, step));
    select
}

/// The `WITH RECURSIVE rushwind_tree(id, depth) AS (seed UNION ALL
/// step)` clause.
fn with_recursive(cte_name: Alias, seed: SelectStatement, step: SelectStatement) -> WithClause {
    let mut cte = CommonTableExpression::new();
    cte.table_name(cte_name);
    cte.columns([Alias::new("id"), Alias::new("depth")]);
    let mut unioned = seed;
    unioned.union(UnionType::All, step);
    cte.query(unioned);
    let mut clause = WithClause::new();
    clause.recursive(true);
    clause.cte(cte);
    clause
}

/// The exact cycle check: one-parent-per-row data makes two paths to
/// the same id precisely a cycle.
fn detect_cycle(rows: &[(Record, u32)], pk: &str) -> Result<(), StorageError> {
    let mut seen = std::collections::BTreeSet::new();
    for (record, _) in rows {
        let Some(id) = record.get(pk).and_then(rushwind_storage::Value::as_i64) else {
            return Err(StorageError::Backend(
                "a tree row lost its primary key".into(),
            ));
        };
        if !seen.insert(id) {
            return Err(StorageError::InvalidQuery(format!(
                "parent cycle detected at id {id}"
            )));
        }
    }
    Ok(())
}

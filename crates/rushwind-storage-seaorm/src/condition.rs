//! The predicate face: viewer scope, tenant axis, and the filter tree
//! rendered into `sea_query::Condition`s — the counterpart of the
//! in-memory engine's evaluator, pinned together by the conformance suite.

use sea_orm::sea_query::{Alias, Condition, Expr, ExprTrait, Func};

use rushwind_storage::{
    FilterNode, Op, QueryCtx, Record, StorageError, TenancyDecision, Value, Viewer,
};

use crate::Schema;

/// The viewer scope rendered for SQL: denied (empty everything) or a
/// conjoinable predicate.
pub(crate) enum ScopeSql {
    /// The viewer may see nothing; engines short-circuit.
    Deny,
    /// The extra predicate to AND in, if any.
    Pass(Option<Condition>),
}

/// The tenant axis resolved for one operation: the schema's tenant column
/// plus the viewer's [`TenancyDecision`] against it.
///
/// Mirrors the in-memory reference engine's `TenantRule`: resolution is
/// fail-closed — a tenant-scoped table queried by an unidentified viewer
/// errors with [`StorageError::MissingViewer`] from *every* repository
/// path, before any statement is sent.
#[derive(Clone, Debug)]
pub(crate) enum TenantSql {
    /// No tenant predicate anywhere: the table declares no tenant column,
    /// or the viewer is platform/system.
    Pass,
    /// Conjoin `column = id` on every path and force-stamp writes with id.
    Enforce { column: String, id: i64 },
}

impl TenantSql {
    /// Resolves the schema's tenant column against the viewer.
    pub(crate) fn resolve(schema: &Schema, ctx: &QueryCtx) -> Result<Self, StorageError> {
        Ok(
            match ctx
                .viewer
                .tenant_decision(schema.tenant_column.as_deref())?
            {
                TenancyDecision::Unscoped | TenancyDecision::Pass => TenantSql::Pass,
                TenancyDecision::Enforce(id) => TenantSql::Enforce {
                    // Enforce only arises when the schema declares the
                    // column; the fallback keeps the conventional name so
                    // the stamp cannot silently miss.
                    column: schema
                        .tenant_column
                        .clone()
                        .unwrap_or_else(|| Viewer::TENANT_COLUMN.to_owned()),
                    id,
                },
            },
        )
    }

    /// The predicate to AND into every query's WHERE clause, if any.
    pub(crate) fn predicate(&self) -> Option<Condition> {
        match self {
            TenantSql::Pass => None,
            TenantSql::Enforce { column, id } => {
                Some(Condition::all().add(Expr::col(Alias::new(column.as_str())).eq(*id)))
            }
        }
    }

    /// Force-stamps the tenant column when the viewer is a tenant business
    /// viewer — a tenant actor cannot address another tenant by writing the
    /// column itself. Platform and unscoped writes keep the payload value.
    pub(crate) fn stamp(&self, row: &mut Record) {
        if let TenantSql::Enforce { column, id } = self {
            row.insert(column.as_str(), Value::Int(*id));
        }
    }
}

pub(crate) fn expect_id(id: &Value) -> Result<i64, StorageError> {
    id.as_i64()
        .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
}

/// Binds a contract [`Value`] as a SQL parameter.
pub(crate) fn bind(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::val(Option::<i64>::None),
        Value::Bool(b) => Expr::val(*b),
        Value::Int(i) => Expr::val(*i),
        Value::Real(f) => Expr::val(*f),
        Value::Text(s) => Expr::val(s.clone()),
    }
}

/// Translates a filter node into a SQL condition — the counterpart of the
/// in-memory engine's evaluator; the conformance suite pins them together.
pub(crate) fn translate(node: &FilterNode) -> Result<Condition, StorageError> {
    match node {
        FilterNode::All(children) => {
            let mut cond = Condition::all();
            for child in children {
                cond = cond.add(translate(child)?);
            }
            Ok(cond)
        }
        FilterNode::Any(children) => {
            let mut cond = Condition::any();
            for child in children {
                cond = cond.add(translate(child)?);
            }
            Ok(cond)
        }
        FilterNode::Cond(cond) => {
            let col = Expr::col(Alias::new(&cond.field));
            let first = || cond.values.first().cloned().unwrap_or(Value::Null);
            let pattern = |fmt: fn(&str) -> String| match first().as_str() {
                Some(s) => fmt(s),
                None => String::new(),
            };
            let expr: Expr = match cond.op {
                Op::Eq => col.eq(bind(&first())),
                Op::NotEq => col.ne(bind(&first())),
                Op::Gt => col.gt(bind(&first())),
                Op::Gte => col.gte(bind(&first())),
                Op::Lt => col.lt(bind(&first())),
                Op::Lte => col.lte(bind(&first())),
                Op::In => col.is_in(cond.values.iter().map(bind).collect::<Vec<Expr>>()),
                Op::NotIn => col.is_not_in(cond.values.iter().map(bind).collect::<Vec<Expr>>()),
                Op::Like => col.like(pattern(|s| s.to_owned())),
                Op::NotLike => col.not_like(pattern(|s| s.to_owned())),
                // No ILIKE in SQLite/MySQL: LOWER on the column, folded in Rust.
                Op::Ilike => Expr::from(Func::lower(col)).like(pattern(|s| s.to_lowercase())),
                Op::IsNull => col.is_null(),
                Op::IsNotNull => col.is_not_null(),
                Op::Between => {
                    let lo = cond.values.first().cloned().unwrap_or(Value::Null);
                    let hi = cond.values.get(1).cloned().unwrap_or(Value::Null);
                    col.between(bind(&lo), bind(&hi))
                }
                Op::NotBetween => {
                    let lo = cond.values.first().cloned().unwrap_or(Value::Null);
                    let hi = cond.values.get(1).cloned().unwrap_or(Value::Null);
                    col.not_between(bind(&lo), bind(&hi))
                }
                Op::Contains => col.like(pattern(|s| format!("%{s}%"))),
                Op::StartsWith => col.like(pattern(|s| format!("{s}%"))),
                Op::EndsWith => col.like(pattern(|s| format!("%{s}"))),
            };
            Ok(expr.into())
        }
    }
}

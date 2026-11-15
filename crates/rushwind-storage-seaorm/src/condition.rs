//! The predicate face: viewer scope, tenant axis, and the filter tree
//! rendered into `sea_query::Condition`s — the counterpart of the
//! in-memory engine's evaluator, pinned together by the conformance suite.

use sea_orm::sea_query::{Alias, Condition, Expr, ExprTrait, Func};
use sea_orm::DbBackend;

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

/// Binds a contract [`Value`] as a SQL parameter. JSON kinds bind as
/// their canonical JSON text — the shape every dialect's JSON column
/// accepts (MySQL `JSON`, PostgreSQL `JSONB`, SQLite's TEXT-stored
/// documents).
pub(crate) fn bind(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::val(Option::<i64>::None),
        Value::Bool(b) => Expr::val(*b),
        Value::Int(i) => Expr::val(*i),
        Value::Real(f) => Expr::val(*f),
        Value::Text(s) => Expr::val(s.clone()),
        Value::Array(items) => {
            Expr::val(rushwind_storage::json::encode(&Value::Array(items.clone())))
        }
        Value::Json(text) => Expr::val(text.clone()),
    }
}

/// Translates a filter node into a SQL condition — the counterpart of the
/// in-memory engine's evaluator; the conformance suite pins them together.
/// The backend shapes the JSON operators (each dialect speaks its own
/// JSON face; the rest of the tree renders identically everywhere).
pub(crate) fn translate(node: &FilterNode, backend: DbBackend) -> Result<Condition, StorageError> {
    match node {
        FilterNode::All(children) => {
            let mut cond = Condition::all();
            for child in children {
                cond = cond.add(translate(child, backend)?);
            }
            Ok(cond)
        }
        FilterNode::Any(children) => {
            let mut cond = Condition::any();
            for child in children {
                cond = cond.add(translate(child, backend)?);
            }
            Ok(cond)
        }
        FilterNode::Cond(cond) => {
            // The JSON containment pair render per dialect through
            // their own face (a plain Condition), not the shared
            // expression table below.
            if matches!(cond.op, Op::JsonContains | Op::ArrayContains) {
                let operand = cond.values.first().cloned().unwrap_or(Value::Null);
                return json_condition(&cond.field, cond.op, &operand, backend);
            }
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
                // Same folding for case-insensitive equality.
                Op::Iexact => Expr::from(Func::lower(col)).eq(pattern(|s| s.to_lowercase())),
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
                // The JSON containment pair returned above, per dialect.
                Op::JsonContains | Op::ArrayContains => unreachable!("json faces handled above"),
            };
            Ok(expr.into())
        }
    }
}

/// The dialect's JSON containment face — `JSON_CONTAINS` semantics
/// (equality, object key-subset, array element membership).
///
/// - MySQL: the native `JSON_CONTAINS`.
/// - PostgreSQL: `jsonb @>` — the same containment shape.
/// - SQLite: the needle decomposes at translate time (it is a query
///   constant) into whole-document equality, top-level element
///   membership over `json_each` (the `type` column keeps `true`
///   apart from `1`), and a flat key-subset over `json_extract`.
///   Needles nested deeper than that (documents inside arrays of
///   arrays, key values that are themselves containers) have no
///   native SQLite face — they refuse as `Unsupported` rather than
///   silently narrowing.
fn json_condition(
    field: &str,
    op: Op,
    operand: &Value,
    backend: DbBackend,
) -> Result<Condition, StorageError> {
    match op {
        Op::JsonContains => json_contains_sql(field, operand, backend),
        _ => array_contains_sql(field, operand, backend),
    }
}

fn json_contains_sql(
    field: &str,
    needle: &Value,
    backend: DbBackend,
) -> Result<Condition, StorageError> {
    let Some(text) = needle.as_json() else {
        return Err(StorageError::InvalidQuery(
            "JSON_CONTAINS takes a json operand".into(),
        ));
    };
    match backend {
        DbBackend::MySql => Ok(Condition::all().add(Expr::cust_with_values(
            format!("JSON_CONTAINS(\"{field}\", ?)"),
            [text],
        ))),
        DbBackend::Postgres => Ok(Condition::all().add(Expr::cust_with_values(
            format!("\"{field}\" @> CAST(? AS jsonb)"),
            [text],
        ))),
        _ => sqlite_contains_sql(field, text),
    }
}

/// The dialect's array-element-membership face — the needle rides the
/// same JSON containment machinery with the element as the candidate.
fn array_contains_sql(
    field: &str,
    element: &Value,
    backend: DbBackend,
) -> Result<Condition, StorageError> {
    let json_text = rushwind_storage::json::encode(element);
    match backend {
        DbBackend::MySql => Ok(Condition::all().add(Expr::cust_with_values(
            format!("JSON_CONTAINS(\"{field}\", ?)"),
            [json_text],
        ))),
        DbBackend::Postgres => Ok(Condition::all().add(Expr::cust_with_values(
            format!("\"{field}\" @> CAST(? AS jsonb)"),
            [json_text],
        ))),
        _ => sqlite_element_sql(field, element),
    }
}

/// The needle's parsed shape for the SQLite decomposition.
enum SqliteNeedle {
    /// A scalar (the membership bind is the native value).
    Scalar,
    /// An object (flat key-subset over json_extract).
    Object(Vec<(String, Value)>),
    /// An array (elements ride json_each by their own shape).
    Array,
}

/// Classifies the needle for the SQLite decomposition; object keys
/// must be path-safe barewords (the json_extract path spells them).
fn sqlite_needle(text: &str) -> Result<SqliteNeedle, StorageError> {
    let Some(parsed) = rushwind_storage::json::parse(text) else {
        return Err(StorageError::InvalidQuery(
            "JSON_CONTAINS operand is not valid JSON".into(),
        ));
    };
    Ok(match parsed {
        rushwind_storage::json::Json::Obj(entries) => {
            let mut flat = Vec::new();
            for (key, value) in entries {
                if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return Err(StorageError::Unsupported(
                        "SQLite JSON_CONTAINS over keys outside [A-Za-z0-9_]".into(),
                    ));
                }
                let Some(value) = json_of_needle_value(&value) else {
                    return Err(StorageError::Unsupported(
                        "SQLite JSON_CONTAINS over container key values".into(),
                    ));
                };
                flat.push((key, value));
            }
            SqliteNeedle::Object(flat)
        }
        rushwind_storage::json::Json::Arr(_) => SqliteNeedle::Array,
        _ => SqliteNeedle::Scalar,
    })
}

/// Converts a parsed needle value into the contract value whose bind
/// matches SQLite's json_extract/json_each rendering — `None` for
/// containers (the flat subset does not recurse into them).
fn json_of_needle_value(value: &rushwind_storage::json::Json) -> Option<Value> {
    match value {
        rushwind_storage::json::Json::Null => Some(Value::Null),
        rushwind_storage::json::Json::Bool(b) => Some(Value::Bool(*b)),
        rushwind_storage::json::Json::Num(n) => Some(Value::Real(*n)),
        rushwind_storage::json::Json::Str(s) => Some(Value::Text(s.clone())),
        rushwind_storage::json::Json::Arr(_) | rushwind_storage::json::Json::Obj(_) => None,
    }
}

/// The SQLite containment face: equality, top-level element membership
/// (the needle rides json_each with a type-guarded bind), and — for
/// object needles — the flat key-subset over json_extract.
fn sqlite_contains_sql(field: &str, needle_text: &str) -> Result<Condition, StorageError> {
    let needle = sqlite_needle(needle_text)?;
    // Whole-document equality always applies.
    let mut any = Condition::any().add(Expr::cust_with_values(
        format!("json(\"{field}\") = json(?)"),
        [needle_text.to_string()],
    ));
    // Top-level element membership: the element bind matches
    // json_each.value's rendering for the element's own shape, guarded
    // by the type column.
    match &needle {
        SqliteNeedle::Object(_) => {}
        SqliteNeedle::Scalar => {
            let parsed = rushwind_storage::json::parse(needle_text);
            if let Some(value) = parsed.as_ref().and_then(json_of_needle_value) {
                any = any.add(sqlite_each_membership(field, &value));
            }
        }
        SqliteNeedle::Array => {
            // The needle array itself may sit as one element.
            any = any.add(Expr::cust_with_values(
                format!(
                    "EXISTS (SELECT 1 FROM json_each(\"{field}\") WHERE json_each.type = 'array' AND json_each.value = ?)"
                ),
                [needle_text.to_string()],
            ));
        }
    }
    // Object needles: the flat key-subset.
    if let SqliteNeedle::Object(entries) = &needle {
        let mut subset = Condition::all();
        for (key, value) in entries {
            subset = subset.add(Expr::cust_with_values(
                format!("json_extract(\"{field}\", '$.{key}') = ?"),
                [sea_value(value)],
            ));
        }
        any = any.add(subset);
    }
    Ok(Condition::all()
        .add(Expr::cust(format!("json_valid(\"{field}\")")))
        .add(any))
}

/// One json_each membership arm: the element's bind plus the type
/// guard that keeps JSON's `true`/`1` and `1`/`1.0` distinctions
/// honest on SQLite's dynamically typed values.
fn sqlite_each_membership(field: &str, element: &Value) -> Condition {
    let types = match element {
        Value::Bool(b) => {
            if *b {
                "'true'"
            } else {
                "'false'"
            }
        }
        Value::Int(_) => "'integer'",
        Value::Real(_) => "'real'",
        Value::Text(_) => "'text'",
        _ => "",
    };
    Condition::all().add(Expr::cust_with_values(
        format!(
            "EXISTS (SELECT 1 FROM json_each(\"{field}\") WHERE json_each.type IN ({types}) AND json_each.value = ?)"
        ),
        [sea_value(element)],
    ))
}

/// Converts a contract value into sea-query's parameter model for the
/// cust faces that bind raw values inside hand-written SQL.
fn sea_value(value: &Value) -> sea_orm::sea_query::Value {
    match value {
        Value::Null => Option::<i64>::None.into(),
        Value::Bool(b) => (*b).into(),
        Value::Int(i) => (*i).into(),
        Value::Real(f) => (*f).into(),
        Value::Text(s) => s.clone().into(),
        Value::Array(_) | Value::Json(_) => unreachable!("scalar bind"),
    }
}

/// The SQLite array-element-membership face (the scalar element's own
/// bind, type-guarded).
fn sqlite_element_sql(field: &str, element: &Value) -> Result<Condition, StorageError> {
    match element {
        Value::Null | Value::Array(_) | Value::Json(_) => Err(StorageError::InvalidQuery(
            "ARRAY_CONTAINS takes a scalar operand".into(),
        )),
        _ => Ok(sqlite_each_membership(field, element)),
    }
}

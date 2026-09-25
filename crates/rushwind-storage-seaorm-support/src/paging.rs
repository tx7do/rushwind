//! The `PagingRequest` → SeaORM select assembly.
//!
//! Deployment request types (their own generated `pagination` messages)
//! plug in through [`PagingInput`] — one impl per deployment converts
//! its generated shape into the owned [`Params`], and every repository
//! call site keeps passing its request verbatim:
//!
//! ```ignore
//! impl PagingInput for proto::proto::pagination::PagingRequest {
//!     fn paging_params(&self) -> Params {
//!         Params {
//!             query: match &self.filtering_type {
//!                 Some(paging_request::FilteringType::Query(q)) => Some(q.clone()),
//!                 _ => None,
//!             },
//!             order_by: self.order_by.clone(),
//!             sorting: self
//!                 .sorting
//!                 .iter()
//!                 .filter(|s| !s.field.is_empty())
//!                 .map(|s| Sorting { field: s.field.clone(), desc: s.direction == 1 })
//!                 .collect(),
//!             page: self.page,
//!             page_size: self.page_size,
//!             offset: self.offset,
//!             limit: self.limit,
//!             no_paging: self.no_paging.unwrap_or(false),
//!         }
//!     }
//! }
//! ```

use sea_orm::sea_query::{Alias, BinOper, Condition, Expr, ExprTrait, Func, SimpleExpr};
use sea_orm::Value as QValue;
use sea_orm::{DatabaseConnection, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Select};

use rushwind_http_binding::envelope::StatusError;

use crate::db_err;

/// The resolved paging knobs — the owned, deployment-agnostic shape the
/// pipeline consumes.
#[derive(Debug, Clone, Default)]
pub struct Params {
    /// The `query` JSON filter string (the rust-utils query_parser
    /// syntax: `{"field":"val","field___icontains":"val2"}`).
    pub query: Option<String>,
    /// The `orderBy` spelling: a JSON array of fields or a plain
    /// comma/field string; `-field` marks descending.
    pub order_by: Option<String>,
    /// The structured `sorting` list, paired with the request's own
    /// ordering.
    pub sorting: Vec<Sorting>,
    /// Page number (1-based).
    pub page: Option<u32>,
    /// Page size.
    pub page_size: Option<u32>,
    /// Explicit offset (wins over page slicing when present).
    pub offset: Option<u64>,
    /// Explicit limit (wins over page slicing when present).
    pub limit: Option<u32>,
    /// No slicing at all — every matching row.
    pub no_paging: bool,
}

/// One structured ordering entry; `desc` mirrors the wire's
/// `Direction::DESC`.
#[derive(Debug, Clone)]
pub struct Sorting {
    pub field: String,
    pub desc: bool,
}

/// The request side of the pipeline: anything that can resolve itself
/// into [`Params`]. Blanket-impl'd for `Params` so framework callers
/// pass params directly.
pub trait PagingInput {
    fn paging_params(&self) -> Params;
}

impl PagingInput for Params {
    fn paging_params(&self) -> Params {
        self.clone()
    }
}

/// The value-binding kind of a filter column (filter values arrive as
/// JSON strings; the backend needs matching literal types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Number,
    Bool,
    Text,
}

/// The value-binding kind of one entity column, read off the entity's
/// own schema at runtime — a column absent from the entity (or an
/// entity-less name) binds as text. Integer-family columns bind
/// numeric, `Boolean` binds bool.
pub fn column_kind<E>(field: &str) -> Kind
where
    E: sea_orm::EntityTrait,
{
    use sea_orm::entity::ColumnTrait as _;
    let mut kind = Kind::Text;
    for col in <E::Column as sea_orm::Iterable>::iter() {
        if sea_orm::sea_query::Iden::to_string(&col) == field {
            kind = match col.def().get_column_type() {
                sea_orm::sea_query::ColumnType::TinyInteger
                | sea_orm::sea_query::ColumnType::SmallInteger
                | sea_orm::sea_query::ColumnType::Integer
                | sea_orm::sea_query::ColumnType::BigInteger
                | sea_orm::sea_query::ColumnType::TinyUnsigned
                | sea_orm::sea_query::ColumnType::SmallUnsigned
                | sea_orm::sea_query::ColumnType::Unsigned
                | sea_orm::sea_query::ColumnType::BigUnsigned => Kind::Number,
                sea_orm::sea_query::ColumnType::Boolean => Kind::Bool,
                _ => Kind::Text,
            };
            break;
        }
    }
    kind
}

fn value_of(kind: Kind, value: &str) -> QValue {
    match kind {
        Kind::Number => value
            .parse::<i64>()
            .map(|n| QValue::BigInt(Some(n)))
            .unwrap_or_else(|_| QValue::String(Some(value.to_string()))),
        Kind::Bool => QValue::Bool(Some(value == "true")),
        Kind::Text => QValue::String(Some(value.to_string())),
    }
}

fn cmp(field: &str, op: BinOper, value: &str, kind: Kind) -> SimpleExpr {
    Expr::col(Alias::new(field)).binary(op, SimpleExpr::Value(value_of(kind, value)))
}

fn lower_like(field: &str, pattern: &str) -> SimpleExpr {
    let col = Func::cust("lower").arg(Expr::col(Alias::new(field)));
    let pat = Func::cust("lower").arg(SimpleExpr::Value(QValue::String(Some(pattern.to_string()))));
    col.binary(BinOper::Like, pat)
}

fn split_values(value: &str) -> Vec<String> {
    rust_utils::query_parser::split_query_values(value)
        .into_iter()
        .map(String::from)
        .collect()
}

fn in_list(field: &str, values: &[String], kind: Kind) -> Option<SimpleExpr> {
    match values.len() {
        0 => None,
        1 => Some(cmp(field, BinOper::Equal, &values[0], kind)),
        _ => Some(
            Expr::col(Alias::new(field)).binary(
                BinOper::In,
                SimpleExpr::Tuple(
                    values
                        .iter()
                        .map(|v| SimpleExpr::Value(value_of(kind, v)))
                        .collect(),
                ),
            ),
        ),
    }
}

fn filter_condition(kind: Kind, field: &str, op: &str, value: &str) -> Option<Condition> {
    use rust_utils::query_parser as qp;
    // A numeric column with a non-numeric literal (the front-end echoes
    // unresolved ids as the string "undefined") cannot bind — drop the
    // condition instead of failing the whole query on a PG type error.
    if kind == Kind::Number && value.parse::<i64>().is_err() {
        return None;
    }
    let cond = match op {
        "" | qp::FILTER_EXACT => Condition::all().add(cmp(field, BinOper::Equal, value, kind)),
        qp::FILTER_NOT | qp::FILTER_NOT_IN => {
            let expr = in_list(field, &split_values(value), kind)?;
            Condition::all().add(Expr::not(expr))
        }
        qp::FILTER_IN => Condition::all().add(in_list(field, &split_values(value), kind)?),
        qp::FILTER_GT => Condition::all().add(cmp(field, BinOper::GreaterThan, value, kind)),
        qp::FILTER_GTE => {
            Condition::all().add(cmp(field, BinOper::GreaterThanOrEqual, value, kind))
        }
        qp::FILTER_LT => Condition::all().add(cmp(field, BinOper::SmallerThan, value, kind)),
        qp::FILTER_LTE => {
            Condition::all().add(cmp(field, BinOper::SmallerThanOrEqual, value, kind))
        }
        qp::FILTER_RANGE => {
            let values = split_values(value);
            if values.len() != 2 {
                return None;
            }
            Condition::all()
                .add(cmp(field, BinOper::GreaterThanOrEqual, &values[0], kind))
                .add(cmp(field, BinOper::SmallerThanOrEqual, &values[1], kind))
        }
        qp::FILTER_IS_NULL => Condition::all().add(Expr::col(Alias::new(field)).is_null()),
        qp::FILTER_NOT_IS_NULL => Condition::all().add(Expr::col(Alias::new(field)).is_not_null()),
        qp::FILTER_CONTAINS => {
            Condition::all().add(Expr::col(Alias::new(field)).like(format!("%{value}%")))
        }
        qp::FILTER_ICONTAINS => Condition::all().add(lower_like(field, &format!("%{value}%"))),
        qp::FILTER_STARTS_WITH => {
            Condition::all().add(Expr::col(Alias::new(field)).like(format!("{value}%")))
        }
        qp::FILTER_ISTARTSWITH => Condition::all().add(lower_like(field, &format!("{value}%"))),
        qp::FILTER_ENDS_WITH => {
            Condition::all().add(Expr::col(Alias::new(field)).like(format!("%{value}")))
        }
        qp::FILTER_IENDSWITH => Condition::all().add(lower_like(field, &format!("%{value}"))),
        qp::FILTER_IEXACT => Condition::all().add(lower_like(field, value)),
        // The gorm layer silently drops regex/search (operator-matrix D1/D2).
        qp::FILTER_REGEX | qp::FILTER_IREGEX | qp::FILTER_SEARCH => return None,
        _ => return None,
    };
    Some(cond)
}

/// The paged fetch envelope shared by every repository listing: rows of
/// the assembled select plus the total matching the SAME predicate set
/// (the row count itself under `no_paging`). `base` carries the fixed
/// predicates and ordering; the request's filtering rides the total too
/// — the deployments' copies counted the bare base, so a filtered list
/// reported the unfiltered total (the extraction's SQLite suite caught
/// it; the fixed semantic is pinned by `tests/sqlite.rs`).
pub async fn fetch_paged<E>(
    db: &DatabaseConnection,
    base: Select<E>,
    req: &impl PagingInput,
) -> Result<(Vec<E::Model>, u64), StatusError>
where
    E: sea_orm::EntityTrait,
    E::Model: sea_orm::FromQueryResult + Send + Sync + 'static,
{
    let params = req.paging_params();
    let assembled = apply_filters_and_order(base, &params);
    let total = if params.no_paging {
        None
    } else {
        // count() clears ordering internally, so an ordered select is safe.
        Some(assembled.clone().count(db).await.map_err(db_err)?)
    };
    let rows = apply_slicing(assembled, &params)
        .all(db)
        .await
        .map_err(db_err)?;
    let total = total.unwrap_or(rows.len() as u64);
    Ok((rows, total))
}

/// Applies the paging params to a select: filter conditions, ordering
/// (falling back to `id`) and page slicing.
pub fn apply<E>(select: Select<E>, req: &impl PagingInput) -> Select<E>
where
    E: sea_orm::EntityTrait,
{
    let params = req.paging_params();
    apply_slicing(apply_filters_and_order(select, &params), &params)
}

/// The predicate + ordering pass — the slice-free shape the total
/// counts run over.
fn apply_filters_and_order<E>(mut select: Select<E>, req: &Params) -> Select<E>
where
    E: sea_orm::EntityTrait,
{
    if let Some(query) = &req.query {
        let mut conditions: Vec<Condition> = Vec::new();
        let _ = rust_utils::query_parser::parse_filter_json_string(query, |field, op, value| {
            if let Some(cond) = filter_condition(column_kind::<E>(field), field, op, value) {
                conditions.push(cond);
            }
        });
        for cond in conditions {
            select = select.filter(cond);
        }
    }

    let mut order_specs: Vec<(String, bool)> = Vec::new();
    if let Some(order_by) = &req.order_by {
        if let Ok(fields) = serde_json::from_str::<Vec<String>>(order_by) {
            for field in fields {
                rust_utils::query_parser::parse_order_by_field(&field, |f, desc| {
                    order_specs.push((f.to_string(), desc));
                });
            }
        } else {
            let _ = rust_utils::query_parser::parse_order_by_string(order_by, |f, desc| {
                order_specs.push((f.to_string(), desc));
            });
        }
    }
    for sorting in &req.sorting {
        order_specs.push((
            rust_utils::stringcase::to_snake_case(&sorting.field),
            sorting.desc,
        ));
    }
    if order_specs.is_empty() {
        order_specs.push(("id".to_string(), false));
    }
    for (field, desc) in &order_specs {
        // The wire names are camelCase (the front-end's field names);
        // order only by real entity columns in their snake_case form —
        // anything else would ORDER BY a nonexistent column.
        let snake = rust_utils::stringcase::to_snake_case(field);
        let known = <E::Column as sea_orm::Iterable>::iter()
            .any(|c| sea_orm::sea_query::Iden::to_string(&c) == snake);
        if !known {
            continue;
        }
        let col = Expr::col(Alias::new(&snake));
        select = if *desc {
            select.order_by(col, sea_orm::Order::Desc)
        } else {
            select.order_by(col, sea_orm::Order::Asc)
        };
    }

    select
}

/// The slicing pass (page/offset/none) — applied after counting.
fn apply_slicing<E>(mut select: Select<E>, req: &Params) -> Select<E>
where
    E: sea_orm::EntityTrait,
{
    let page = std::cmp::max(req.page.unwrap_or(1) as u64, 1);
    let page_size = std::cmp::max(req.page_size.unwrap_or(10) as u64, 1);
    let (offset, limit) = if req.no_paging {
        (0, u64::MAX)
    } else if req.offset.is_some() || req.limit.is_some() {
        (
            req.offset.unwrap_or(0),
            std::cmp::max(req.limit.unwrap_or(10) as u64, 1),
        )
    } else {
        ((page - 1) * page_size, page_size)
    };
    if !req.no_paging {
        select = select.offset(offset).limit(limit);
    }

    select
}

#[cfg(test)]
mod tests {
    use super::filter_condition;
    use super::{column_kind, Kind};
    use rust_utils::query_parser;

    // A schema probe entity for the runtime column-kind oracle.
    mod todo {
        use sea_orm::entity::prelude::*;

        #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
        #[sea_orm(table_name = "todos")]
        pub struct Model {
            #[sea_orm(primary_key)]
            pub id: i32,
            pub title: String,
            pub priority: i32,
            pub done: bool,
        }

        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}

        impl ActiveModelBehavior for ActiveModel {}
    }

    #[test]
    fn numeric_column_drops_non_numeric_filter_values() {
        // The "undefined" echo from the front-end must not poison the SQL.
        assert!(filter_condition(Kind::Number, "recipient_user_id", "", "undefined").is_none());
        assert!(filter_condition(Kind::Number, "id", query_parser::FILTER_EXACT, "12").is_some());
        // Text columns keep arbitrary literals.
        assert!(filter_condition(Kind::Text, "status", "", "RECEIVED").is_some());
    }

    #[test]
    fn column_kinds_read_off_the_entity_schema() {
        assert_eq!(column_kind::<todo::Entity>("id"), Kind::Number);
        assert_eq!(column_kind::<todo::Entity>("done"), Kind::Bool);
        assert_eq!(column_kind::<todo::Entity>("title"), Kind::Text);
        // Entity-less names bind as text.
        assert_eq!(column_kind::<todo::Entity>("nope"), Kind::Text);
    }
}

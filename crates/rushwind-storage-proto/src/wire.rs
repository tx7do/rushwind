//! Wire → contract conversions with the full operator taxonomy mapped.

use rushwind_storage::{
    FieldMask, ListQuery, Op, Paging, Sort, SortDir, SortField, StorageError, Value, MAX_LIMIT,
};

use crate::v1::FilterExpr as ProtoFilterExpr;
use crate::v1::{
    pagination_request, paging_request, ExprType, FilterCondition, Operator, PaginationRequest,
    PagingRequest, SortDirection,
};

type ContractFilterExpr = rushwind_storage::FilterExpr;

/// Parses a protojson `FilterExpr` document into a contract filter.
///
/// Field names follow protojson: lowerCamelCase (`istartsWith`) and the
/// original snake_case (`istarts_with`) are both accepted; enum values are
/// their proto member names (`"OPERATOR_UNSPECIFIED"`, `"EQ"`, …).
///
/// The parse routes through [`crate::cleanjson`]: the
/// `google.protobuf.Value` operands deserialize via `deserialize_any`,
/// which serde_json's `arbitrary_precision` feature — enabled
/// workspace-wide by the script engines' dependency trees — would
/// otherwise hand to the visitor as a tagged map instead of a number.
pub fn filter_expr_from_json(json: &str) -> Result<ContractFilterExpr, StorageError> {
    let value: serde_json::Value = serde_json_error(serde_json::from_str(json))?;
    let proto: ProtoFilterExpr =
        serde_json_error(<ProtoFilterExpr as serde::Deserialize>::deserialize(
            crate::cleanjson::CleanJson::new(&value),
        ))?;
    filter_expr_from_proto(&proto)
}

/// Parses a protojson `PagingRequest` document into a contract list query.
///
/// Routed through [`crate::cleanjson`] like
/// [`filter_expr_from_json`], for the same reason.
pub fn list_query_from_json(json: &str) -> Result<ListQuery, StorageError> {
    let value: serde_json::Value = serde_json_error(serde_json::from_str(json))?;
    let proto: PagingRequest =
        serde_json_error(<PagingRequest as serde::Deserialize>::deserialize(
            crate::cleanjson::CleanJson::new(&value),
        ))?;
    list_query_from_proto(&proto)
}

/// Converts a generated `FilterExpr` into a contract filter.
///
/// An unspecified group type is treated as `AND`; leaves carry the operator
/// mapping of the taxonomy table.
pub fn filter_expr_from_proto(proto: &ProtoFilterExpr) -> Result<ContractFilterExpr, StorageError> {
    let mut children = Vec::new();
    for condition in &proto.conditions {
        children.push(condition_to_filter(condition)?);
    }
    for group in &proto.groups {
        children.push(filter_expr_from_proto(group)?);
    }
    Ok(match proto.r#type() {
        ExprType::Or => ContractFilterExpr::any(children),
        _ => ContractFilterExpr::all(children),
    })
}

/// Converts the generated pagination oneof into a contract paging strategy.
///
/// `NoPaging` maps to an offset window spanning [`MAX_LIMIT`] — the largest
/// fetch the contract allows; engines enforce the bound regardless.
pub fn paging_from_proto(proto: &PaginationRequest) -> Result<Paging, StorageError> {
    Ok(match proto.pagination_type.as_ref() {
        Some(pagination_request::PaginationType::PageBased(p)) => Paging::Page {
            page: to_u32(p.page),
            size: to_u32(p.page_size),
        },
        Some(pagination_request::PaginationType::OffsetBased(p)) => Paging::Offset {
            offset: to_u64(p.offset),
            limit: to_u32(p.limit),
        },
        Some(pagination_request::PaginationType::TokenBased(p)) => Paging::Token {
            token: p.token.clone(),
            limit: to_u32(p.page_size),
        },
        Some(pagination_request::PaginationType::NoPaging(_)) => Paging::Offset {
            offset: 0,
            limit: MAX_LIMIT,
        },
        None => Paging::default(),
    })
}

/// Converts the generated sorting list into a contract ordering; an
/// unspecified direction defaults to ascending.
pub fn sorting_from_proto(sorting: &[crate::v1::Sorting]) -> Result<Sort, StorageError> {
    Ok(Sort {
        fields: sorting
            .iter()
            .map(|term| {
                Ok(SortField {
                    field: term.field.clone(),
                    dir: match term.direction() {
                        SortDirection::Desc => SortDir::Desc,
                        _ => SortDir::Asc,
                    },
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
    })
}

/// Converts the whole generated request envelope into a contract list query.
pub fn list_query_from_proto(proto: &PagingRequest) -> Result<ListQuery, StorageError> {
    Ok(ListQuery {
        paging: match &proto.pagination_type {
            Some(pagination) => paging_from_proto(pagination)?,
            None => Paging::default(),
        },
        filter: match proto.filter.as_ref() {
            Some(paging_request::Filter::FilterExpr(filter)) => {
                Some(filter_expr_from_proto(filter)?)
            }
            None => None,
        },
        sort: sorting_from_proto(&proto.sorting)?,
        mask: proto
            .field_mask
            .as_ref()
            .map(|mask| FieldMask::of(mask.paths.iter().cloned())),
    })
}

// ---- leaves ----------------------------------------------------------------

fn condition_to_filter(condition: &FilterCondition) -> Result<ContractFilterExpr, StorageError> {
    if !condition.date_part.is_empty() {
        return Err(unsupported("date_part"));
    }
    if !condition.json_path.is_empty() {
        return Err(unsupported("json_path"));
    }
    let field = condition.field.as_str();
    let op = condition.op();
    let mut values = Vec::new();
    if let Some(value) = &condition.value {
        values.push(value_to_contract(value)?);
    }
    values.extend(
        condition
            .values
            .iter()
            .map(value_to_contract)
            .collect::<Result<Vec<_>, StorageError>>()?,
    );

    let cond = |op: Op, values: Vec<Value>| ContractFilterExpr::cond(field, op, values);

    Ok(match op {
        Operator::Unspecified => {
            return Err(StorageError::InvalidQuery(
                "wire operator is OPERATOR_UNSPECIFIED".into(),
            ))
        }
        Operator::Eq | Operator::Exact => cond(Op::Eq, values),
        Operator::Neq => cond(Op::NotEq, values),
        Operator::Gt => cond(Op::Gt, values),
        Operator::Gte => cond(Op::Gte, values),
        Operator::Lt => cond(Op::Lt, values),
        Operator::Lte => cond(Op::Lte, values),
        Operator::Like => cond(Op::Like, values),
        Operator::NotLike => cond(Op::NotLike, values),
        Operator::Ilike => cond(Op::Ilike, values),
        Operator::In => cond(Op::In, values),
        Operator::Nin => cond(Op::NotIn, values),
        Operator::IsNull => cond(Op::IsNull, Vec::new()),
        Operator::IsNotNull => cond(Op::IsNotNull, Vec::new()),
        Operator::Between => cond(Op::Between, values),
        Operator::Contains => cond(Op::Contains, values),
        Operator::StartsWith => cond(Op::StartsWith, values),
        Operator::EndsWith => cond(Op::EndsWith, values),
        // The Django-lookup convenience family folds into case-insensitive
        // LIKE patterns, which every engine already speaks.
        Operator::Icontains => cond(Op::Ilike, wrap_pattern(&values, |s| format!("%{s}%"))),
        Operator::IstartsWith => cond(Op::Ilike, wrap_pattern(&values, |s| format!("{s}%"))),
        Operator::IendsWith => cond(Op::Ilike, wrap_pattern(&values, |s| format!("%{s}"))),
        // Case-insensitive equality is its own contract operator — the
        // SQL engines render LOWER(col) = LOWER(?), MongoDB an anchored
        // case-folded regex.
        Operator::Iexact => cond(Op::Iexact, values),
        Operator::Regexp
        | Operator::Iregexp
        | Operator::JsonContains
        | Operator::ArrayContains
        | Operator::Exists
        | Operator::Search => return Err(unsupported(op_name(op))),
    })
}

/// Wraps text operands into the given LIKE pattern; non-text operands pass
/// through so the engine's kind validation produces the error.
fn wrap_pattern(values: &[Value], fmt: fn(&str) -> String) -> Vec<Value> {
    values
        .iter()
        .map(|value| match value {
            Value::Text(s) => Value::Text(fmt(s)),
            other => other.clone(),
        })
        .collect()
}

fn value_to_contract(value: &pbjson_types::Value) -> Result<Value, StorageError> {
    use pbjson_types::value::Kind;
    match &value.kind {
        Some(Kind::BoolValue(b)) => Ok(Value::Bool(*b)),
        Some(Kind::NumberValue(n)) => {
            if n.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(n) {
                Ok(Value::Int(*n as i64))
            } else {
                Ok(Value::Real(*n))
            }
        }
        Some(Kind::StringValue(s)) => Ok(Value::Text(s.clone())),
        Some(Kind::NullValue(_)) | None => Ok(Value::Null),
        Some(Kind::ListValue(_)) | Some(Kind::StructValue(_)) => {
            Err(unsupported("composite (list/object) wire values"))
        }
    }
}

fn op_name(op: Operator) -> &'static str {
    match op {
        Operator::Regexp => "REGEXP",
        Operator::Iregexp => "IREGEXP",
        Operator::JsonContains => "JSON_CONTAINS",
        Operator::ArrayContains => "ARRAY_CONTAINS",
        Operator::Exists => "EXISTS",
        Operator::Search => "SEARCH",
        Operator::Iexact => "IEXACT",
        _ => "OPERATOR",
    }
}

fn unsupported(name: &str) -> StorageError {
    StorageError::Unsupported(format!(
        "wire operator/feature {name} has no relational equivalent in the Rust core"
    ))
}

fn serde_json_error<T>(result: Result<T, serde_json::Error>) -> Result<T, StorageError> {
    result.map_err(|e| StorageError::InvalidQuery(format!("protojson: {e}")))
}

fn to_u32(v: i32) -> u32 {
    u32::try_from(v).unwrap_or(0)
}

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

// The node types are referenced only by tests and the conversions above.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{FilterCondition, PageBasedPagination, PaginationRequest};

    #[test]
    fn operator_names_cover_the_taxonomy() {
        // Sanity: the mapping reaches every member of the 29-operator enum.
        for op in [
            Operator::Unspecified,
            Operator::Eq,
            Operator::Neq,
            Operator::Gt,
            Operator::Gte,
            Operator::Lt,
            Operator::Lte,
            Operator::Like,
            Operator::Ilike,
            Operator::NotLike,
            Operator::In,
            Operator::Nin,
            Operator::IsNull,
            Operator::IsNotNull,
            Operator::Between,
            Operator::Regexp,
            Operator::Iregexp,
            Operator::Contains,
            Operator::StartsWith,
            Operator::EndsWith,
            Operator::Icontains,
            Operator::IstartsWith,
            Operator::IendsWith,
            Operator::JsonContains,
            Operator::ArrayContains,
            Operator::Exists,
            Operator::Search,
            Operator::Exact,
            Operator::Iexact,
        ] {
            let condition = FilterCondition {
                field: "age".into(),
                op: op.into(),
                value: Some(pbjson_types::value::Kind::NumberValue(1.0).into()),
                values: Vec::new(),
                date_part: String::new(),
                json_path: String::new(),
            };
            // Every member must map to either a contract condition or a
            // typed Unsupported/InvalidQuery error — never panic.
            let _ = condition_to_filter(&condition);
        }
    }

    #[test]
    fn page_based_conversion() {
        let proto = PaginationRequest {
            pagination_type: Some(pagination_request::PaginationType::PageBased(
                PageBasedPagination {
                    page: 2,
                    page_size: 10,
                },
            )),
        };
        match paging_from_proto(&proto).expect("converts") {
            Paging::Page { page, size } => {
                assert_eq!((page, size), (2, 10));
            }
            other => panic!("unexpected paging: {other:?}"),
        }
    }
}

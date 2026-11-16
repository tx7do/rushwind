//! The query translation layer — pure functions, unit-tested offline.
//!
//! Contract filter trees compile to Elasticsearch `bool` queries; SQL
//! pattern operators (`LIKE` family) become `wildcard` clauses with the
//! SQL wildcards (`%`→`*`, `_`→`?`) transposed and everything else
//! escaped; the case-insensitive family sets `case_insensitive`.

use serde_json::{json, Value as Json};

use rushwind_storage::{Condition, FilterNode, Op, Sort, SortDir, Value};

/// Maps a contract column name onto its Elasticsearch field name — text
/// columns are queried and sorted through their `.keyword` sub-field so
/// term/wildcard matching and ordering are exact, never analyzed.
pub type FieldMapper<'a> = &'a dyn Fn(&str) -> String;

/// Escapes Elasticsearch wildcard metacharacters so a literal operand
/// cannot widen into a pattern; `transposed` re-marks the SQL wildcards
/// that were deliberately carried over.
fn wildcard_body(sql_pattern: &str) -> String {
    let mut out = String::with_capacity(sql_pattern.len());
    for ch in sql_pattern.chars() {
        match ch {
            '\\' | '*' | '?' => {
                out.push('\\');
                out.push(ch);
            }
            '%' => out.push('*'),
            '_' => out.push('?'),
            other => out.push(other),
        }
    }
    out
}

fn wildcard_clause(field: String, sql_pattern: &str, fold: bool) -> Json {
    json!({ "wildcard": { field: {
        "value": wildcard_body(sql_pattern),
        "case_insensitive": fold,
    } } })
}

fn pattern_operand(condition: &Condition) -> Result<String, rushwind_storage::StorageError> {
    match condition.values.first() {
        Some(Value::Text(s)) => Ok(s.clone()),
        _ => Err(rushwind_storage::StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        )),
    }
}

/// The query JSON for one leaf condition.
pub(crate) fn condition_json(
    condition: &Condition,
    mapped: FieldMapper,
) -> Result<Json, rushwind_storage::StorageError> {
    // The point-path face: key steps join the field's dotted path
    // (Elasticsearch walks objects natively); numeric steps have no
    // face — arrays are implicit in ES, there is no positional
    // indexing to translate — so they refuse rather than silently
    // matching every element.
    let field = match &condition.json_path {
        Some(path) => {
            if path.split('.').any(|step| step.parse::<usize>().is_ok()) {
                return Err(rushwind_storage::StorageError::Unsupported(
                    "json_path array indexes have no Elasticsearch face".into(),
                ));
            }
            mapped(&format!("{}.{}", condition.field, path))
        }
        None => mapped(&condition.field),
    };
    let field_borrow = field.as_str();
    let first = || condition.values.first().cloned().unwrap_or(Value::Null);
    let scalar = |value: &Value| match value {
        Value::Null => Json::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Real(f) => json!(f),
        Value::Text(s) => json!(s),
        // JSON kinds ride the wire shape: arrays as JSON arrays (a
        // term query matches any element — Elasticsearch's native
        // containment face), documents as parsed objects.
        Value::Array(items) => {
            json!(items.iter().map(json_value).collect::<Vec<Json>>())
        }
        Value::Json(text) => serde_json::from_str(text).unwrap_or(Json::String(text.clone())),
    };
    let term = || json!({ "term": { field_borrow: scalar(&first()) } });
    // (lower, inclusive), (upper, inclusive): one-sided ops pass None.
    let range = |lower: Option<(Value, bool)>, upper: Option<(Value, bool)>| {
        let mut bounds = serde_json::Map::new();
        if let Some((value, inclusive)) = lower {
            bounds.insert(if inclusive { "gte" } else { "gt" }.into(), scalar(&value));
        }
        if let Some((value, inclusive)) = upper {
            bounds.insert(if inclusive { "lte" } else { "lt" }.into(), scalar(&value));
        }
        json!({ "range": { field_borrow: bounds } })
    };
    let first_bound = |inclusive: bool| Some((first(), inclusive));
    let second_bound = |inclusive: bool| {
        Some((
            condition.values.get(1).cloned().unwrap_or(Value::Null),
            inclusive,
        ))
    };
    Ok(match condition.op {
        Op::Eq => term(),
        Op::NotEq => json!({ "bool": { "must_not": [term()] } }),
        Op::Gt => range(first_bound(false), None),
        Op::Gte => range(first_bound(true), None),
        Op::Lt => range(None, first_bound(false)),
        Op::Lte => range(None, first_bound(true)),
        Op::In => {
            json!({
                "terms": { field_borrow: condition.values.iter().map(scalar).collect::<Vec<_>>() }
            })
        }
        Op::NotIn => json!({ "bool": { "must_not": [
            { "terms": { field_borrow: condition.values.iter().map(scalar).collect::<Vec<_>>() } }
        ] } }),
        Op::IsNull => {
            json!({ "bool": { "must_not": [ { "exists": { "field": field_borrow } } ] } })
        }
        Op::IsNotNull => json!({ "exists": { "field": field_borrow } }),
        Op::Between => range(first_bound(true), second_bound(true)),
        Op::NotBetween => {
            json!({ "bool": { "must_not": [range(first_bound(true), second_bound(true))] } })
        }
        Op::Like => wildcard_clause(field_borrow.to_owned(), &pattern_operand(condition)?, false),
        Op::NotLike => json!({ "bool": { "must_not": [
            wildcard_clause(field_borrow.to_owned(), &pattern_operand(condition)?, false)
        ] } }),
        Op::Ilike => wildcard_clause(field_borrow.to_owned(), &pattern_operand(condition)?, true),
        // The `term` query's own case-insensitive flag — exact folding,
        // not a wildcard.
        Op::Iexact => json!({
            "term": { field_borrow: { "value": scalar(&first()), "case_insensitive": true } }
        }),
        Op::Contains => {
            let text = pattern_operand(condition)?;
            wildcard_clause(field_borrow.to_owned(), &format!("%{text}%"), false)
        }
        Op::StartsWith => {
            let text = pattern_operand(condition)?;
            wildcard_clause(field_borrow.to_owned(), &format!("{text}%"), false)
        }
        Op::EndsWith => {
            let text = pattern_operand(condition)?;
            wildcard_clause(field_borrow.to_owned(), &format!("%{text}"), false)
        }
        // Elasticsearch's term face IS the array containment face: a
        // term matches any element of an array field. ARRAY_CONTAINS
        // rides it directly; JSON_CONTAINS rides equality — the term
        // on the scalar/document/element. The object key-subset
        // containment has no Elasticsearch query face without nested
        // mappings — the crate docs declare the delta.
        Op::ArrayContains => json!({ "term": { field_borrow: scalar(&first()) } }),
        Op::JsonContains => term(),
        // The regex family: Lucene's regexp matches the WHOLE term, so
        // the contract's unanchored semantics render as a per-top-
        // level-alternative `.*alt.*` wrapper. The anchors have no
        // Lucene face — they refuse rather than silently changing
        // position semantics.
        Op::Regexp | Op::Iregexp => {
            let owned = first();
            let Some(pattern) = owned.as_str() else {
                return Err(rushwind_storage::StorageError::InvalidQuery(
                    "the regex operators need one text pattern operand".into(),
                ));
            };
            let ast = rushwind_storage::regex::parse(pattern).map_err(|error| {
                rushwind_storage::StorageError::InvalidQuery(format!(
                    "regexp pattern at byte {}: {}",
                    error.position, error.message
                ))
            })?;
            if has_anchor(&ast) {
                return Err(rushwind_storage::StorageError::Unsupported(
                    "the ^ and $ anchors have no Elasticsearch regexp face".into(),
                ));
            }
            let wrapped: Vec<String> = ast
                .top_level_alternatives()
                .iter()
                .map(|alternative| format!(".*{}.*", render_verbatim(alternative)))
                .collect();
            let lucene = wrapped.join("|");
            json!({
                "regexp": { field_borrow: {
                    "value": lucene,
                    "case_insensitive": condition.op == Op::Iregexp,
                } }
            })
        }
    })
}

/// Whether any node in the tree is an anchor.
fn has_anchor(ast: &rushwind_storage::regex::Ast) -> bool {
    use rushwind_storage::regex::Ast;
    match ast {
        Ast::Start | Ast::End => true,
        Ast::Concat(nodes) | Ast::Alt(nodes) => nodes.iter().any(has_anchor),
        Ast::Repeat { node, .. } => has_anchor(node),
        _ => false,
    }
}

/// Renders the AST verbatim — every construct the contract's subset
/// parses rides Lucene's syntax unchanged (the parser refused the
/// dialect-specific shorthands at the edge).
fn render_verbatim(ast: &rushwind_storage::regex::Ast) -> String {
    use rushwind_storage::regex::Ast;
    match ast {
        Ast::Empty => String::new(),
        Ast::Char(c) => c.to_string(),
        Ast::Any => ".".to_string(),
        Ast::Class { ranges, negated } => {
            let mut out = String::from("[");
            if *negated {
                out.push('^');
            }
            for (low, high) in ranges {
                if low == high {
                    out.push(*low);
                } else {
                    out.push(*low);
                    out.push('-');
                    out.push(*high);
                }
            }
            out.push(']');
            out
        }
        Ast::Concat(nodes) => nodes.iter().map(render_verbatim).collect(),
        Ast::Alt(branches) => {
            let rendered: Vec<String> = branches.iter().map(render_verbatim).collect();
            rendered.join("|")
        }
        Ast::Repeat { node, min, max } => {
            let inner = render_verbatim(node);
            let quantifier = match (min, max) {
                (0, None) => "*".to_string(),
                (1, None) => "+".to_string(),
                (0, Some(1)) => "?".to_string(),
                (min, None) => format!("{{{min},}}"),
                (min, Some(max)) if min == max => format!("{{{min}}}"),
                (min, Some(max)) => format!("{{{min},{max}}}"),
            };
            // Multi-char nodes quote into a group so the quantifier
            // binds the whole node.
            if matches!(node.as_ref(), Ast::Char(_) | Ast::Any | Ast::Class { .. }) {
                format!("{inner}{quantifier}")
            } else {
                format!("({inner}){quantifier}")
            }
        }
        Ast::Start | Ast::End => String::new(),
    }
}

/// One contract value out to its JSON wire shape — the array face of
/// [`scalar`] generalized (nested arrays recurse; documents parse
/// their canonical text).
pub(crate) fn json_value(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Real(f) => json!(f),
        Value::Text(s) => json!(s),
        Value::Array(items) => Json::Array(items.iter().map(json_value).collect()),
        Value::Json(text) => serde_json::from_str(text).unwrap_or(Json::String(text.clone())),
    }
}

/// The query JSON for a filter-tree node (`AND`/`OR` nest as bool
/// must/should); the constraint-free group is `match_all`.
pub(crate) fn node_json(
    node: &FilterNode,
    mapped: FieldMapper,
) -> Result<Json, rushwind_storage::StorageError> {
    Ok(match node {
        FilterNode::All(children) if children.is_empty() => json!({ "match_all": {} }),
        FilterNode::All(children) => json!({ "bool": { "must":
            children
                .iter()
                .map(|child| node_json(child, mapped))
                .collect::<Result<Vec<_>, _>>()?
        } }),
        FilterNode::Any(children) => json!({ "bool": { "should":
            children
                .iter()
                .map(|child| node_json(child, mapped))
                .collect::<Result<Vec<_>, _>>()?
        } }),
        FilterNode::Cond(condition) => condition_json(condition, mapped)?,
    })
}

/// The search request body: query, sort (query terms then the primary key
/// as the deterministic tiebreaker), paging, and the `_source` projection.
pub(crate) fn search_body(
    query_json: &Json,
    sort: &Sort,
    primary_key: &str,
    mapped: FieldMapper,
    from: Option<u64>,
    size: u64,
    source_columns: &[String],
) -> Json {
    let mut sort_terms: Vec<Json> = sort
        .fields
        .iter()
        .map(|term| {
            json!({ mapped(&term.field):
                match term.dir { SortDir::Asc => "asc", SortDir::Desc => "desc" } })
        })
        .collect();
    sort_terms.push(json!({ mapped(primary_key): "asc" }));
    json!({
        "query": query_json,
        "sort": sort_terms,
        "from": from.unwrap_or(0),
        "size": size,
        "_source": source_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_storage::{Condition, SortField};

    fn mapped(column: &str) -> String {
        if column == "name" {
            format!("{column}.keyword")
        } else {
            column.to_owned()
        }
    }

    fn leaf(field: &str, op: Op, values: Vec<Value>) -> Json {
        condition_json(&Condition::new(field, op, values), &mapped).expect("translates")
    }

    #[test]
    fn comparison_terms_and_ranges() {
        assert_eq!(
            leaf("age", Op::Eq, vec![Value::Int(7)]),
            json!({ "term": { "age": 7 } })
        );
        assert_eq!(
            leaf("age", Op::NotEq, vec![Value::Int(7)]),
            json!({ "bool": { "must_not": [ { "term": { "age": 7 } } ] } })
        );
        assert_eq!(
            leaf("age", Op::Between, vec![Value::Int(5), Value::Int(25)]),
            json!({ "range": { "age": { "gte": 5, "lte": 25 } } })
        );
        assert_eq!(
            leaf("age", Op::Lt, vec![Value::Int(3)]),
            json!({ "range": { "age": { "lt": 3 } } })
        );
        assert_eq!(
            leaf("age", Op::Gte, vec![Value::Int(3)]),
            json!({ "range": { "age": { "gte": 3 } } })
        );
        assert_eq!(
            leaf("age", Op::NotBetween, vec![Value::Int(5), Value::Int(25)]),
            json!({ "bool": { "must_not": [
                { "range": { "age": { "gte": 5, "lte": 25 } } }
            ] } })
        );
    }

    #[test]
    fn null_and_set_operators() {
        assert_eq!(
            leaf("score", Op::IsNull, vec![]),
            json!({ "bool": { "must_not": [ { "exists": { "field": "score" } } ] } })
        );
        assert_eq!(
            leaf("score", Op::IsNotNull, vec![]),
            json!({ "exists": { "field": "score" } })
        );
        assert_eq!(
            leaf("age", Op::In, vec![Value::Int(1), Value::Int(2)]),
            json!({ "terms": { "age": [1, 2] } })
        );
    }

    #[test]
    fn like_family_transposes_wildcards_and_escapes() {
        // % -> *, _ -> ?, everything else escaped.
        let like = leaf("name", Op::Like, vec![Value::Text("%a_c".into())]);
        assert_eq!(
            like,
            json!({ "wildcard": { "name.keyword": { "value": "*a?c", "case_insensitive": false } } })
        );
        // Dot is not a wildcard metacharacter in Elasticsearch; only the
        // SQL wildcards transpose.
        let dots = leaf("name", Op::Contains, vec![Value::Text("a.c".into())]);
        assert_eq!(
            dots,
            json!({ "wildcard": { "name.keyword": { "value": "*a.c*", "case_insensitive": false } } })
        );
        let ilike = leaf("name", Op::Ilike, vec![Value::Text("%E%".into())]);
        assert_eq!(
            ilike,
            json!({ "wildcard": { "name.keyword": { "value": "*E*", "case_insensitive": true } } })
        );
    }

    #[test]
    fn groups_nest_as_bool_must_and_should() {
        let tree = FilterNode::Any(vec![
            FilterNode::Cond(Condition::new("age", Op::Eq, [Value::Int(1)])),
            FilterNode::All(vec![
                FilterNode::Cond(Condition::new("age", Op::Gte, [Value::Int(10)])),
                FilterNode::Cond(Condition::new("unit_id", Op::Eq, [Value::Int(20)])),
            ]),
        ]);
        assert_eq!(
            node_json(&tree, &mapped).expect("translates"),
            json!({ "bool": { "should": [
                { "term": { "age": 1 } },
                { "bool": { "must": [
                    { "range": { "age": { "gte": 10 } } },
                    { "term": { "unit_id": 20 } },
                ] } },
            ] } })
        );
        assert_eq!(
            node_json(&FilterNode::All(vec![]), &mapped).expect("translates"),
            json!({ "match_all": {} })
        );
    }

    #[test]
    fn search_body_sorts_with_pk_tiebreaker() {
        let body = search_body(
            &json!({ "match_all": {} }),
            &Sort {
                fields: vec![SortField {
                    field: "age".into(),
                    dir: SortDir::Desc,
                }],
            },
            "id",
            &mapped,
            Some(20),
            10,
            &["id".into(), "name".into()],
        );
        assert_eq!(
            body,
            json!({
                "query": { "match_all": {} },
                "sort": [ { "age": "desc" }, { "id": "asc" } ],
                "from": 20,
                "size": 10,
                "_source": ["id", "name"],
            })
        );
    }
}

//! The filter-tree → BSON translation: the MongoDB counterpart of the
//! SQL adapter's `translate`, unit-tested entirely offline.
//!
//! Operator mapping — SQL-parallel where MongoDB has one, derived where it
//! does not:
//!
//! | contract op | MongoDB |
//! |:---|:---|
//! | `Eq` / `NotEq` | `{f: v}` / `{f: {$ne: v}}` |
//! | `Gt` `Gte` `Lt` `Lte` | `{$gt}` `{$gte}` `{$lt}` `{$lte}` |
//! | `In` / `NotIn` | `{$in}` / `{$nin}` |
//! | `IsNull` / `IsNotNull` | `{f: null}` / `{f: {$ne: null}}` |
//! | `Between` / `NotBetween` | `{$gte,$lte}` / `$or` of the two outside arms |
//! | `Like` family | `$regex` (SQL wildcards compiled to anchored regex, escaped) |
//! | `Ilike` family | same, with the `i` flag |
//!
//! `AND`/`OR` groups become `$and`/`$or`; a constraint-free group is `{}`.

use mongodb::bson::{doc, Bson, Document, Regex};

use rushwind_storage::{Condition, FilterNode, Op, Schema, Sort, SortDir, StorageError, Value};

/// Converts a contract value into BSON. Arrays encode element-wise;
/// JSON documents parse strictly (see [`bson_of_json`]) — the write
/// face: validate_row has already refused unparseable documents, so
/// the fallback never fires for honest payloads.
pub(crate) fn value_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Int(i) => Bson::Int64(*i),
        Value::Real(f) => Bson::Double(*f),
        Value::Text(s) => Bson::String(s.clone()),
        Value::Array(items) => Bson::Array(items.iter().map(value_to_bson).collect()),
        Value::Json(text) => bson_of_json(text).unwrap_or(Bson::Null),
    }
}

/// Parses a JSON document strictly and converts it to BSON — the
/// query operands' face, where a failed parse must refuse (MongoDB
/// reads an empty document as "matches every embedded document", the
/// silent narrowing the live suite caught). Top-level arrays and
/// scalars ride their own BSON kinds.
pub(crate) fn bson_of_json(text: &str) -> Result<Bson, StorageError> {
    let parsed = rushwind_storage::json::parse(text).ok_or_else(|| {
        StorageError::InvalidQuery(format!("the JSON document is not valid JSON: {text:?}"))
    })?;
    Ok(json_model_to_bson(&parsed))
}

/// One parsed JSON model value out to BSON (objects keep their field
/// order; whole-number reals ride as Int64 — the contract's number
/// model).
fn json_model_to_bson(model: &rushwind_storage::json::Json) -> Bson {
    use rushwind_storage::json::Json;
    match model {
        Json::Null => Bson::Null,
        Json::Bool(b) => Bson::Boolean(*b),
        Json::Num(n) => {
            if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                Bson::Int64(*n as i64)
            } else {
                Bson::Double(*n)
            }
        }
        Json::Str(s) => Bson::String(s.clone()),
        Json::Arr(items) => Bson::Array(items.iter().map(json_model_to_bson).collect()),
        Json::Obj(entries) => {
            let mut document = Document::new();
            for (key, value) in entries {
                document.insert(key.clone(), json_model_to_bson(value));
            }
            Bson::Document(document)
        }
    }
}

/// Reads one column back out of a stored document according to the
/// schema's declared kind; a missing field is `NULL`, as everywhere else.
pub(crate) fn bson_to_value(
    schema: &Schema,
    column: &str,
    value: Option<&Bson>,
) -> Result<Value, StorageError> {
    let kind = schema
        .column(column)
        .ok_or_else(|| StorageError::InvalidQuery(format!("unknown column {column:?}")))?
        .kind;
    let Some(bson) = value else {
        return Ok(Value::Null);
    };
    // A declared column may hold NULL regardless of its kind; the kind
    // check below is only for *present* values.
    if matches!(bson, Bson::Null) {
        return Ok(Value::Null);
    }
    let kind_name = |bson: &Bson| match bson {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Boolean(_) => "boolean",
        Bson::Null => "null",
        Bson::Int32(_) => "int32",
        Bson::Int64(_) => "int64",
        _ => "other",
    };
    let mismatches = |expected: &str| {
        StorageError::Backend(format!(
            "column {column:?} holds {}, expected {expected}",
            kind_name(bson)
        ))
    };
    Ok(match kind {
        rushwind_storage::ColumnKind::Bool => {
            Value::Bool(bson.as_bool().ok_or_else(|| mismatches("a boolean"))?)
        }
        rushwind_storage::ColumnKind::Int => {
            Value::Int(bson.as_i64().ok_or_else(|| mismatches("an integer"))?)
        }
        rushwind_storage::ColumnKind::Real => {
            Value::Real(bson.as_f64().ok_or_else(|| mismatches("a number"))?)
        }
        // A JSON column reads back typed: arrays typed element-wise,
        // objects as canonical documents, scalars native.
        rushwind_storage::ColumnKind::Json => bson_to_json_value(bson),
        rushwind_storage::ColumnKind::Text => Value::Text(
            bson.as_str()
                .ok_or_else(|| mismatches("a string"))?
                .to_owned(),
        ),
    })
}

/// Escapes regex metacharacters so a literal operand can never widen into a
/// pattern.
fn escape_regex(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(
            ch,
            '.' | '*' | '+' | '?' | '^' | '$' | '{' | '}' | '(' | ')' | '|' | '[' | ']' | '\\'
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Compiles an SQL `LIKE` pattern (SQL wildcards, no escapes) into an
/// anchored regex: `%` → `.*`, `_` → `.`, everything else literal.
fn like_to_regex(pattern: &str, fold_case: bool) -> Bson {
    let mut body = String::with_capacity(pattern.len());
    for ch in pattern.chars() {
        match ch {
            '%' => body.push_str(".*"),
            '_' => body.push('.'),
            other => body.push_str(&escape_regex(&other.to_string())),
        }
    }
    Bson::RegularExpression(Regex {
        pattern: format!("^{body}$"),
        options: if fold_case { "i".into() } else { String::new() },
    })
}

/// The case-insensitive derived family folds to unanchored/anchored regex
/// with the `i` flag: `contains` / `starts with` / `ends with`.
fn derived_regex(value: &Value, shape: fn(&str) -> String) -> Result<Bson, StorageError> {
    let Some(text) = value.as_str() else {
        return Err(StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        ));
    };
    Ok(Bson::RegularExpression(Regex {
        pattern: shape(&escape_regex(text)),
        options: "i".into(),
    }))
}

fn like_regex(condition: &Condition) -> Result<Bson, StorageError> {
    let Some(pattern) = condition.values.first().and_then(Value::as_str) else {
        return Err(StorageError::InvalidQuery(
            "LIKE needs one text pattern operand".into(),
        ));
    };
    Ok(like_to_regex(pattern, false))
}

/// Translates one leaf condition into its MongoDB predicate document.
pub(crate) fn condition_to_doc(condition: &Condition) -> Result<Document, StorageError> {
    use Value::Null;
    // The point-path face: mongo's dotted paths walk documents AND
    // array indexes natively, so the field and the path simply join.
    let field = match &condition.json_path {
        Some(path) => format!("{}.{}", condition.field, path),
        None => condition.field.clone(),
    };
    let field = field.as_str();
    let first = || condition.values.first().cloned().unwrap_or(Null);
    let text_shape = |fmt: fn(&str) -> String| match first() {
        Value::Text(s) => Ok(derived_regex(&Value::Text(s), fmt)?),
        _ => Err(StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        )),
    };
    Ok(match condition.op {
        Op::Eq => doc! { field: value_to_bson(&first()) },
        Op::NotEq => doc! { field: { "$ne": value_to_bson(&first()) } },
        Op::Gt => doc! { field: { "$gt": value_to_bson(&first()) } },
        Op::Gte => doc! { field: { "$gte": value_to_bson(&first()) } },
        Op::Lt => doc! { field: { "$lt": value_to_bson(&first()) } },
        Op::Lte => doc! { field: { "$lte": value_to_bson(&first()) } },
        Op::In => {
            doc! {
                field: { "$in": condition.values.iter().map(value_to_bson).collect::<Vec<_>>() }
            }
        }
        Op::NotIn => {
            doc! {
                field: { "$nin": condition.values.iter().map(value_to_bson).collect::<Vec<_>>() }
            }
        }
        // The point-path face's null is "the path does not resolve" —
        // MongoDB's `{path: null}` skips rows whose path traverses an
        // array (the element-wise semantics never see the missing
        // key), so the face spells the two disjuncts: absent path, or
        // a present-but-null value (the live suite caught it).
        Op::IsNull if condition.json_path.is_some() => doc! {
            "$or": [
                { field: { "$exists": false } },
                { field: Bson::Null },
            ]
        },
        Op::IsNull => doc! { field: Bson::Null },
        // The point-path face's $ne null needs $exists alongside it:
        // MongoDB's null matching is element-wise over arrays — a
        // `meta.tags ≠ null` alone matches rows whose meta is an
        // array of scalars (the live suite caught it); exists pins
        // the path's presence, ne-null pins the non-null value.
        Op::IsNotNull if condition.json_path.is_some() => doc! {
            field: { "$exists": true, "$ne": Bson::Null }
        },
        Op::IsNotNull => doc! { field: { "$ne": Bson::Null } },
        Op::Between => doc! { field: {
            "$gte": value_to_bson(&first()),
            "$lte": value_to_bson(&condition.values.get(1).cloned().unwrap_or(Null)),
        } },
        Op::NotBetween => doc! {
            "$or": [
                { field: { "$lt": value_to_bson(&first()) } },
                {
                    field: {
                        "$gt": value_to_bson(&condition.values.get(1).cloned().unwrap_or(Null))
                    }
                },
            ]
        },
        Op::Like => doc! { field: { "$regex": like_regex(condition)? } },
        Op::NotLike => doc! { field: { "$not": like_regex(condition)? } },
        Op::Ilike => {
            let owned = first();
            let Some(pattern) = owned.as_str() else {
                return Err(StorageError::InvalidQuery(
                    "ILIKE needs one text pattern operand".into(),
                ));
            };
            doc! { field: { "$regex": like_to_regex(pattern, true) } }
        }
        Op::Contains => doc! { field: { "$regex": text_shape(|s| s.to_owned())? } },
        Op::StartsWith => doc! { field: { "$regex": text_shape(|s| format!("^{s}"))? } },
        Op::EndsWith => doc! { field: { "$regex": text_shape(|s| format!("{s}$"))? } },
        Op::Iexact => {
            let owned = first();
            let Some(text) = owned.as_str() else {
                return Err(StorageError::InvalidQuery(
                    "IEXACT needs one text operand".into(),
                ));
            };
            doc! { field: { "$regex": Bson::RegularExpression(Regex {
                pattern: format!("^{}$", escape_regex(text)),
                options: "i".into(),
            }) } }
        }
        // MongoDB's equality face IS the containment face for arrays:
        // `{field: element}` matches arrays holding the element.
        // JSON_CONTAINS rides the same shape — the operand document
        // matches exact equality or array membership. The object
        // key-subset containment MySQL's JSON_CONTAINS carries has no
        // MongoDB query face (aggregation-only) — the crate docs
        // declare the delta. The JSON operands parse strictly — a
        // failed parse refuses (an empty document would match every
        // embedded document, the silent narrowing the live suite
        // caught).
        Op::ArrayContains => doc! {
            field: bson_of_json(&rushwind_storage::json::encode(&first()))?
        },
        Op::JsonContains => doc! {
            field: bson_of_json(&rushwind_storage::json::encode(&first()))?
        },
        // The regex family rides PCRE verbatim — the contract's
        // documented subset renders identically there (the anchors
        // and classes included); IREGEXP flips mongo's own
        // case-insensitive option.
        Op::Regexp => {
            let owned = first();
            let Some(pattern) = owned.as_str() else {
                return Err(StorageError::InvalidQuery(
                    "REGEXP needs one text pattern operand".into(),
                ));
            };
            doc! { field: { "$regex": pattern } }
        }
        Op::Iregexp => {
            let owned = first();
            let Some(pattern) = owned.as_str() else {
                return Err(StorageError::InvalidQuery(
                    "IREGEXP needs one text pattern operand".into(),
                ));
            };
            doc! { field: { "$regex": pattern, "$options": "i" } }
        }
    })
}

/// One stored BSON value back into the contract's kinds — arrays
/// typed element-wise, documents as canonical JSON text.
pub(crate) fn bson_to_json_value(bson: &Bson) -> Value {
    match bson {
        Bson::Null => Value::Null,
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Int32(i) => Value::Int(*i as i64),
        Bson::Int64(i) => Value::Int(*i),
        Bson::Double(f) => Value::Real(*f),
        Bson::String(s) => Value::Text(s.clone()),
        Bson::Array(items) => Value::Array(items.iter().map(bson_to_json_value).collect()),
        Bson::Document(document) => Value::Json(document.to_string()),
        // Other BSON shapes (DateTime, ObjectId, …) ride their
        // extended-JSON text form.
        other => Value::Json(other.clone().into_relaxed_extjson().to_string()),
    }
}

/// Translates a filter-tree node; `All`/`Any` become `$and`/`$or`, and an
/// empty group is the empty (match-all) document.
pub(crate) fn node_to_doc(node: &FilterNode) -> Result<Document, StorageError> {
    Ok(match node {
        FilterNode::All(children) => {
            if children.is_empty() {
                doc! {}
            } else {
                let parts = children
                    .iter()
                    .map(node_to_doc)
                    .collect::<Result<Vec<_>, StorageError>>()?;
                doc! { "$and": parts }
            }
        }
        FilterNode::Any(children) => {
            let parts = children
                .iter()
                .map(node_to_doc)
                .collect::<Result<Vec<_>, StorageError>>()?;
            doc! { "$or": parts }
        }
        FilterNode::Cond(condition) => condition_to_doc(condition)?,
    })
}

/// The find() sort document: the query's terms, primary key ascending as
/// the deterministic tiebreaker.
pub(crate) fn sort_to_doc(sort: &Sort, primary_key: &str) -> Document {
    let mut doc = Document::new();
    if sort.is_default() {
        doc.insert(primary_key, 1i32);
    } else {
        for term in &sort.fields {
            let dir = match term.dir {
                SortDir::Asc => 1i32,
                SortDir::Desc => -1i32,
            };
            doc.insert(term.field.as_str(), dir);
        }
        doc.insert(primary_key, 1i32);
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_storage::{ColumnKind, Condition};

    fn schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", ColumnKind::Text)
            .column("age", ColumnKind::Int)
            .column("score", ColumnKind::Real)
            .column("owner_id", ColumnKind::Int)
            .build()
            .expect("valid schema")
    }

    fn leaf(field: &str, op: Op, values: Vec<Value>) -> Document {
        condition_to_doc(&Condition::new(field, op, values)).expect("translates")
    }

    /// Pulls (pattern, options) out of either `$regex` holder shape.
    fn regex_of(document: &Document, field: &str, key: &str) -> (String, String) {
        match document.get(field) {
            Some(Bson::Document(inner)) => match inner.get(key) {
                Some(Bson::RegularExpression(re)) => (re.pattern.clone(), re.options.clone()),
                Some(other) => panic!("{key} is not a regex: {other:?}"),
                None => panic!("{key} missing from the operator document"),
            },
            Some(Bson::RegularExpression(re)) => (re.pattern.clone(), re.options.clone()),
            other => panic!("{field} is not a regex holder: {other:?}"),
        }
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(
            leaf("age", Op::Eq, vec![Value::Int(7)]),
            doc! { "age": 7i64 }
        );
        assert_eq!(
            "age",
            leaf("age", Op::NotEq, vec![Value::Int(7)])
                .get_document("age")
                .map(|_| "age")
                .unwrap_or_default()
        );
        let ne = leaf("age", Op::NotEq, vec![Value::Int(7)]);
        assert_eq!(
            ne.get_document("age").expect("doc").get_i64("$ne"),
            Ok(7i64)
        );
        let gte = leaf("age", Op::Gte, vec![Value::Int(7)]);
        assert_eq!(
            gte.get_document("age").expect("doc").get_i64("$gte"),
            Ok(7i64)
        );
        let lt = leaf("score", Op::Lt, vec![Value::Real(2.5)]);
        assert_eq!(
            lt.get_document("score").expect("doc").get_f64("$lt"),
            Ok(2.5)
        );
    }

    #[test]
    fn set_null_and_range_operators() {
        let set = leaf("age", Op::In, vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(
            set.get_document("age")
                .expect("doc")
                .get_array("$in")
                .expect("array"),
            &vec![Bson::Int64(1), Bson::Int64(2)]
        );
        assert_eq!(
            leaf("score", Op::IsNull, vec![]).get("score"),
            Some(&Bson::Null)
        );
        let not_null = leaf("score", Op::IsNotNull, vec![]);
        assert_eq!(
            not_null.get_document("score").expect("doc").get("$ne"),
            Some(&Bson::Null)
        );
        let between = leaf("age", Op::Between, vec![Value::Int(5), Value::Int(25)]);
        let range = between.get_document("age").expect("doc");
        assert_eq!(range.get_i64("$gte"), Ok(5i64));
        assert_eq!(range.get_i64("$lte"), Ok(25i64));
        let outside = leaf("age", Op::NotBetween, vec![Value::Int(5), Value::Int(25)]);
        assert!(
            outside.get_array("$or").is_ok(),
            "not-between is the $or of the two arms"
        );
    }

    #[test]
    fn like_family_compiles_wildcards_into_anchored_regex() {
        let (pattern, options) = regex_of(
            &leaf("name", Op::Like, vec![Value::Text("%a_c".into())]),
            "name",
            "$regex",
        );
        assert_eq!(pattern, r"^.*a.c$");
        assert_eq!(options, "");

        // Metacharacters in the operand stay literal.
        let (pattern, _) = regex_of(
            &leaf("name", Op::Contains, vec![Value::Text("a.c".into())]),
            "name",
            "$regex",
        );
        assert_eq!(pattern, r"a\.c");

        let (pattern, _) = regex_of(
            &leaf("name", Op::StartsWith, vec![Value::Text("bo".into())]),
            "name",
            "$regex",
        );
        assert_eq!(pattern, r"^bo");

        let not = leaf("name", Op::NotLike, vec![Value::Text("%x".into())]);
        assert!(not.get_document("name").expect("doc").get("$not").is_some());
    }

    #[test]
    fn ilike_sets_the_case_insensitive_flag() {
        let (pattern, options) = regex_of(
            &leaf("name", Op::Ilike, vec![Value::Text("%AM%".into())]),
            "name",
            "$regex",
        );
        assert_eq!(pattern, r"^.*AM.*$");
        assert_eq!(options, "i");
    }

    #[test]
    fn iexact_anchors_with_the_case_insensitive_flag() {
        let (pattern, options) = regex_of(
            &leaf("name", Op::Iexact, vec![Value::Text("CHARLIE".into())]),
            "name",
            "$regex",
        );
        assert_eq!(pattern, r"^CHARLIE$");
        assert_eq!(options, "i");
    }

    #[test]
    fn groups_translate_to_and_or() {
        let tree = FilterNode::Any(vec![
            FilterNode::Cond(Condition::new("age", Op::Eq, [Value::Int(1)])),
            FilterNode::All(vec![
                FilterNode::Cond(Condition::new("age", Op::Gte, [Value::Int(10)])),
                FilterNode::Cond(Condition::new("unit_id", Op::Eq, [Value::Int(20)])),
            ]),
        ]);
        let translated = node_to_doc(&tree).expect("translates");
        let arms = translated.get_array("$or").expect("or arms");
        assert_eq!(arms.len(), 2);
        assert!(
            arms[1]
                .as_document()
                .expect("doc")
                .get_array("$and")
                .is_ok(),
            "the second OR arm is the nested AND group"
        );
        // The constraint-free group is the match-all document.
        assert_eq!(
            node_to_doc(&FilterNode::All(vec![])).expect("translates"),
            doc! {}
        );
    }

    #[test]
    fn sort_document_defaults_to_primary_key_ascending() {
        assert_eq!(sort_to_doc(&Sort::default(), "id"), doc! { "id": 1i32 });
        assert_eq!(
            sort_to_doc(&Sort::by("age", SortDir::Desc), "id"),
            doc! { "age": -1i32, "id": 1i32 }
        );
    }

    #[test]
    fn values_roundtrip_through_the_declared_kinds() {
        let schema = schema();
        let stored = doc! {
            "id": 3i64,
            "name": "bolt",
            "age": Bson::Null,
            "score": 2.5,
            "missing": "ignored",
        };
        let read = |col: &str| bson_to_value(&schema, col, stored.get(col)).expect("reads");
        assert_eq!(read("id"), Value::Int(3));
        assert_eq!(read("name"), Value::Text("bolt".into()));
        assert_eq!(read("age"), Value::Null);
        assert_eq!(read("score"), Value::Real(2.5));
        // A column absent from the document is NULL, same as in the engines.
        assert_eq!(read("owner_id"), Value::Null);
        // A kind mismatch is a backend error, not a silent wrong value.
        assert!(bson_to_value(&schema, "name", Some(&Bson::Int64(1))).is_err());
    }
}

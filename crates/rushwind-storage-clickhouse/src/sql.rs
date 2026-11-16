//! SQL generation for ClickHouse — pure functions, unit-tested offline.
//!
//! ClickHouse speaks dialect-flavored SQL over plain HTTP. The generator
//! covers exactly the contract's statement surface: DDL (MergeTree,
//! `ORDER BY` the primary key), INSERT, mutation-synced UPDATE/DELETE
//! (`SETTINGS mutations_sync = 1` so reads-after-writes hold), and the
//! SELECT shape (projection, WHERE, ORDER BY, LIMIT/OFFSET, cursor).

use rushwind_storage::{
    ColumnKind, Condition, FilterNode, ListQuery, Op, Paging, Schema, SortDir, StorageError, Value,
};

/// Escapes a ClickHouse string literal body (single-quoted on both ends).
pub(crate) fn escape_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(ch),
        }
    }
    out
}

/// A contract [`Value`] as a SQL literal.
pub(crate) fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "1" } else { "0" }.into(),
        Value::Int(i) => i.to_string(),
        // Whole-number reals must keep a decimal point: ClickHouse would
        // otherwise read them as the integer literal they resemble.
        Value::Real(f) => {
            let text = f.to_string();
            if text.contains(['.', 'e', 'E']) || text.contains("inf") || text.contains("NaN") {
                text
            } else {
                // Whole-number reals: format the *number* with one decimal
                // (formatting the string with `.1` would truncate it).
                format!("{f:.1}")
            }
        }
        Value::Text(s) => format!("'{}'", escape_string(s)),
        // JSON kinds store as their canonical text (a JSON column's
        // ClickHouse face is a String column holding the document).
        Value::Array(items) => format!(
            "'{}'",
            escape_string(&rushwind_storage::json::encode(&Value::Array(
                items.clone()
            )))
        ),
        Value::Json(text) => format!("'{}'", escape_string(text)),
    }
}

/// Backquoted identifier, shared with the engine's hand-built WHERE
/// fragments (the tenant predicate).
pub(crate) fn identifier(name: &str) -> String {
    format!("`{name}`")
}

/// The column type for a declared kind: everything nullable — absent
/// fields are NULL, the same doctrine as every other engine.
fn column_type(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Bool => "Nullable(UInt8)",
        ColumnKind::Int => "Nullable(Int64)",
        ColumnKind::Real => "Nullable(Float64)",
        ColumnKind::Text => "Nullable(String)",
        // JSON columns store their canonical text.
        ColumnKind::Json => "Nullable(String)",
    }
}

/// `CREATE TABLE IF NOT EXISTS` with a MergeTree engine ordered by the
/// primary key — ClickHouse's closest analogue of a rowid-ordered heap.
pub(crate) fn create_table_sql(schema: &Schema) -> String {
    let mut columns = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        // The sorting key cannot be Nullable: the primary key column is
        // declared bare (Int64 still matches the contract's i64 ids).
        let kind = if column.name == schema.primary_key {
            match column.kind {
                ColumnKind::Bool => "UInt8",
                ColumnKind::Int => "Int64",
                ColumnKind::Real => "Float64",
                ColumnKind::Text | ColumnKind::Json => "String",
            }
        } else {
            column_type(column.kind)
        };
        columns.push(format!("{} {}", identifier(&column.name), kind));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree ORDER BY {}",
        identifier(&schema.table),
        columns.join(", "),
        identifier(&schema.primary_key)
    )
}

/// INSERT with an explicit column list; unlisted columns become their
/// type default (NULL for nullable columns).
pub(crate) fn insert_sql(schema: &Schema, row: &Record) -> Result<String, StorageError> {
    let pk = &schema.primary_key;
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for column in &schema.columns {
        if column.name == *pk && matches!(row.get(pk.as_str()), None | Some(Value::Null)) {
            continue; // the caller lets the engine assign… ClickHouse cannot,
                      // so the caller must have backfilled; skip NULLs anyway
        }
        columns.push(identifier(&column.name));
        values.push(match row.get(&column.name) {
            Some(value) => literal(value),
            None => "NULL".into(),
        });
    }
    Ok(format!(
        "INSERT INTO {} ({}) VALUES ({})",
        identifier(&schema.table),
        columns.join(", "),
        values.join(", ")
    ))
}

/// Mutation-synced UPDATE — `SETTINGS mutations_sync = 1` makes the ALTER
/// return only after the mutation applied, so reads-after-writes hold.
pub(crate) fn update_sql(
    schema: &Schema,
    id: i64,
    sets: &[(String, Value)],
    extra_filter: Option<&FilterNode>,
) -> Result<String, StorageError> {
    if sets.is_empty() {
        return Err(StorageError::InvalidQuery(
            "update patches cannot be empty".into(),
        ));
    }
    let assignments = sets
        .iter()
        .map(|(column, value)| format!("{} = {}", identifier(column), literal(value)))
        .collect::<Vec<_>>()
        .join(", ");
    let mut cond = Condition::new(schema.primary_key.as_str(), Op::Eq, [Value::Int(id)]);
    if let Some(extra) = extra_filter {
        // Conjoin the scope predicate: the WHERE is AND-of-both.
        return Ok(format!(
            "ALTER TABLE {} UPDATE {} WHERE {} AND {} SETTINGS mutations_sync = 1",
            identifier(&schema.table),
            assignments,
            condition_sql(&cond)?,
            node_sql(extra)?
        ));
    }
    let _ = &mut cond;
    Ok(format!(
        "ALTER TABLE {} UPDATE {} WHERE {} SETTINGS mutations_sync = 1",
        identifier(&schema.table),
        assignments,
        condition_sql(&cond)?
    ))
}

/// Mutation-synced DELETE, scope-conjoined like [`update_sql`].
pub(crate) fn delete_sql(
    schema: &Schema,
    id: i64,
    extra_filter: Option<&FilterNode>,
) -> Result<String, StorageError> {
    let cond = Condition::new(schema.primary_key.as_str(), Op::Eq, [Value::Int(id)]);
    Ok(match extra_filter {
        Some(extra) => format!(
            "ALTER TABLE {} DELETE WHERE {} AND {} SETTINGS mutations_sync = 1",
            identifier(&schema.table),
            condition_sql(&cond)?,
            node_sql(extra)?
        ),
        None => format!(
            "ALTER TABLE {} DELETE WHERE {} SETTINGS mutations_sync = 1",
            identifier(&schema.table),
            condition_sql(&cond)?
        ),
    })
}

/// The WHERE fragment for a leaf condition.
pub(crate) fn condition_sql(condition: &Condition) -> Result<String, StorageError> {
    // The JSON faces render their own fragments (the extraction or
    // containment machinery replaces the plain column reference).
    if condition.json_path.is_some() {
        return json_path_sql(condition);
    }
    if condition.op == Op::ArrayContains {
        return array_contains_sql(condition);
    }
    if condition.op == Op::JsonContains {
        return json_contains_sql(condition);
    }
    let column = identifier(&condition.field);
    let first = || condition.values.first().cloned().unwrap_or(Value::Null);
    let pattern = |fmt: fn(&str) -> String| match first() {
        Value::Text(s) => Ok(fmt(&escape_string(&s))),
        _ => Err(StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        )),
    };
    Ok(match condition.op {
        // The JSON faces returned above; the arms below keep the
        // match exhaustive.
        Op::JsonContains | Op::ArrayContains => {
            unreachable!("json faces handled above")
        }
        // The regex family rides RE2 verbatim (match() is an
        // unanchored search — the contract's semantics); IREGEXP
        // carries the inline (?i) flag.
        Op::Regexp => format!("match({}, '{}')", column, pattern(|s| s.to_owned())?),
        Op::Iregexp => format!("match({}, '(?i){}')", column, pattern(|s| s.to_owned())?),
        Op::Eq => format!("{} = {}", column, literal(&first())),
        Op::NotEq => format!("{} != {}", column, literal(&first())),
        Op::Gt => format!("{} > {}", column, literal(&first())),
        Op::Gte => format!("{} >= {}", column, literal(&first())),
        Op::Lt => format!("{} < {}", column, literal(&first())),
        Op::Lte => format!("{} <= {}", column, literal(&first())),
        Op::In => format!(
            "{} IN ({})",
            column,
            condition
                .values
                .iter()
                .map(literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Op::NotIn => format!(
            "{} NOT IN ({})",
            column,
            condition
                .values
                .iter()
                .map(literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Op::IsNull => format!("{} IS NULL", column),
        Op::IsNotNull => format!("{} IS NOT NULL", column),
        Op::Between => format!(
            "{} BETWEEN {} AND {}",
            column,
            literal(&first()),
            literal(&condition.values.get(1).cloned().unwrap_or(Value::Null))
        ),
        Op::NotBetween => format!(
            "{} NOT BETWEEN {} AND {}",
            column,
            literal(&first()),
            literal(&condition.values.get(1).cloned().unwrap_or(Value::Null))
        ),
        Op::Like => format!("{} LIKE '{}'", column, pattern(|s| s.to_owned())?),
        Op::NotLike => format!("{} NOT LIKE '{}'", column, pattern(|s| s.to_owned())?),
        Op::Ilike => format!("{} ILIKE '{}'", column, pattern(|s| s.to_lowercase())?),

        // lowerUTF8 is ClickHouse's Unicode-aware LOWER; the operand
        // folds on the Rust side, as every SQL engine here does.
        Op::Iexact => format!(
            "lowerUTF8({}) = '{}'",
            column,
            pattern(|s| s.to_lowercase())?
        ),
        Op::Contains => format!("{} LIKE '%{}%'", column, pattern(|s| s.to_owned())?),
        Op::StartsWith => format!("{} LIKE '{}%'", column, pattern(|s| s.to_owned())?),
        Op::EndsWith => format!("{} LIKE '%{}'", column, pattern(|s| s.to_owned())?),
    })
}

/// The WHERE fragment for a filter-tree node (`AND`/`OR` groups nest).
pub(crate) fn node_sql(node: &FilterNode) -> Result<String, StorageError> {
    Ok(match node {
        FilterNode::All(children) if children.is_empty() => "1".into(),
        FilterNode::All(children) => {
            let parts = children
                .iter()
                .map(part_sql)
                .collect::<Result<Vec<_>, StorageError>>()?;
            format!("({})", parts.join(" AND "))
        }
        FilterNode::Any(children) => {
            let parts = children
                .iter()
                .map(part_sql)
                .collect::<Result<Vec<_>, StorageError>>()?;
            format!("({})", parts.join(" OR "))
        }
        FilterNode::Cond(condition) => condition_sql(condition)?,
    })
}

fn part_sql(node: &FilterNode) -> Result<String, StorageError> {
    // Nested groups re-parenthesize; leaves come through bare.
    node_sql(node)
}

/// The page-of-rows SELECT.
pub(crate) fn select_sql(
    schema: &Schema,
    columns: &[String],
    where_clause: Option<&str>,
    query: &ListQuery,
) -> Result<String, StorageError> {
    let pk = schema.primary_key.as_str();

    // Assemble the full WHERE up front: the caller's clause plus the token
    // cursor (a primary-key range), since WHERE must precede ORDER BY.
    let mut conditions: Vec<String> = Vec::new();
    if let Some(where_clause) = where_clause {
        conditions.push(where_clause.to_owned());
    }
    if let Paging::Token { token, .. } = &query.paging {
        if !token.is_empty() {
            let last = rushwind_storage::decode_cursor(token)?;
            conditions.push(format!("{} > {}", identifier(pk), last));
        }
    }

    let mut sql = format!(
        "SELECT {} FROM {}",
        columns
            .iter()
            .map(|column| identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        identifier(&schema.table)
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    if query.sort.is_default() {
        sql.push_str(&format!(" ORDER BY {} ASC", identifier(pk)));
    } else {
        let terms = query
            .sort
            .fields
            .iter()
            .map(|term| {
                let dir = match term.dir {
                    SortDir::Asc => "ASC",
                    SortDir::Desc => "DESC",
                };
                format!("{} {}", identifier(&term.field), dir)
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" ORDER BY {terms}"));
    }
    let limit = query.paging.limit();
    match &query.paging {
        // The cursor predicate was already folded into the WHERE clause;
        // peek one row past the page to learn whether the stream continues.
        Paging::Token { .. } => {
            sql.push_str(&format!(" LIMIT {}", limit as u64 + 1));
        }
        Paging::Page { page, .. } => {
            sql.push_str(&format!(
                " LIMIT {} OFFSET {}",
                limit,
                (u64::from(*page - 1)) * u64::from(limit)
            ));
        }
        Paging::Offset { offset, .. } => {
            sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
        }
    }
    Ok(sql)
}

/// The matching-row count.
pub(crate) fn count_sql(
    schema: &Schema,
    where_clause: Option<&str>,
) -> Result<String, StorageError> {
    Ok(match where_clause {
        Some(where_clause) => format!(
            "SELECT count() FROM {} WHERE {}",
            identifier(&schema.table),
            where_clause
        ),
        None => format!("SELECT count() FROM {}", identifier(&schema.table)),
    })
}

use rushwind_storage::Record;

// ---- the JSON faces -----------------------------------------------------

/// The JSON path varargs: dotted steps become keys (quoted) and array
/// indexes (bare numbers) — the shape `JSONExtractRaw`/`JSONHas` walk.
/// The steps are validate-checked barewords and indexes, so quoting is
/// safe.
fn json_path_args(path: &str) -> String {
    let parts: Vec<String> = path
        .split('.')
        .map(|step| {
            if step.parse::<usize>().is_ok() {
                step.to_string()
            } else {
                format!("'{}'", escape_string(step))
            }
        })
        .collect();
    parts.join(", ")
}

/// The point-path face: `JSONExtractRaw` extracts the subvalue's
/// canonical text (the stored document is engine-canonicalized, so the
/// raw slice and the contract's canonical operand text compare
/// verbatim); the pattern family extracts `JSONExtractString` (the
/// unquoted text patterns apply to); the numeric range family extracts
/// `JSONExtractFloat` guarded by `JSONHas` (a missing path would
/// otherwise read as 0 and false-positive the low bounds); IsNull rides
/// `JSONHas` with a coalesce for the NULL column itself.
fn json_path_sql(condition: &Condition) -> Result<String, StorageError> {
    let path = condition
        .json_path
        .as_deref()
        .ok_or_else(|| StorageError::InvalidQuery("json_path missing".into()))?;
    let args = json_path_args(path);
    // The JSON functions demand a non-Nullable argument (ClickHouse's
    // type synthesis rejects Nullable-wrapped arrays), and the
    // declared column is Nullable(String) — coalesce hands NULL and
    // the functions a harmless empty string, which every predicate
    // below reads as "the path is absent" (NULL rows stay invisible
    // through the guards and the empty comparisons).
    let column = identifier(&condition.field);
    let doc = format!("coalesce({column}, '')");
    let raw = format!("JSONExtractRaw({doc}, {args})");
    let has = format!("JSONHas({doc}, {args})");
    let text = format!("JSONExtractString({doc}, {args})");
    let float = format!("JSONExtractFloat({doc}, {args})");
    let first = || condition.values.first().cloned().unwrap_or(Value::Null);
    let canonical = || {
        format!(
            "'{}'",
            escape_string(&rushwind_storage::json::encode(&first()))
        )
    };
    let pattern = |fmt: fn(&str) -> String| match first() {
        Value::Text(s) => Ok(fmt(&escape_string(&s))),
        _ => Err(StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        )),
    };
    let numeric = |value: &Value| match value {
        Value::Int(i) => Ok(i.to_string()),
        Value::Real(f) => Ok({
            let text = f.to_string();
            if text.contains(['.', 'e', 'E']) {
                text
            } else {
                format!("{f:.1}")
            }
        }),
        _ => Err(StorageError::InvalidQuery(
            "numeric range operators need numeric operands over a json_path".into(),
        )),
    };
    Ok(match condition.op {
        Op::Eq => format!("{raw} = {}", canonical()),
        Op::NotEq => format!("{raw} != {} OR coalesce({has}, 0) = 0", canonical()),
        Op::In => {
            let items: Vec<String> = condition
                .values
                .iter()
                .map(|value| {
                    format!(
                        "'{}'",
                        escape_string(&rushwind_storage::json::encode(value))
                    )
                })
                .collect();
            format!("{raw} IN ({})", items.join(", "))
        }
        Op::NotIn => {
            let items: Vec<String> = condition
                .values
                .iter()
                .map(|value| {
                    format!(
                        "'{}'",
                        escape_string(&rushwind_storage::json::encode(value))
                    )
                })
                .collect();
            format!(
                "{raw} NOT IN ({}) OR coalesce({has}, 0) = 0",
                items.join(", ")
            )
        }
        Op::Gt => format!(
            "coalesce({has}, 0) != 0 AND {float} > {}",
            numeric(&first())?
        ),
        Op::Gte => format!(
            "coalesce({has}, 0) != 0 AND {float} >= {}",
            numeric(&first())?
        ),
        Op::Lt => format!(
            "coalesce({has}, 0) != 0 AND {float} < {}",
            numeric(&first())?
        ),
        Op::Lte => format!(
            "coalesce({has}, 0) != 0 AND {float} <= {}",
            numeric(&first())?
        ),
        Op::Between => {
            let hi = condition.values.get(1).cloned().unwrap_or(Value::Null);
            format!(
                "coalesce({has}, 0) != 0 AND {float} > {} AND {float} < {}",
                numeric(&first())?,
                numeric(&hi)?
            )
        }
        Op::NotBetween => {
            let hi = condition.values.get(1).cloned().unwrap_or(Value::Null);
            format!(
                "(coalesce({has}, 0) = 0 OR {float} < {} OR {float} > {})",
                numeric(&first())?,
                numeric(&hi)?
            )
        }
        Op::Like => format!("{text} LIKE '{}'", pattern(|s| s.to_owned())?),
        Op::NotLike => format!("{text} NOT LIKE '{}'", pattern(|s| s.to_owned())?),
        Op::Ilike => format!(
            "lowerUTF8({text}) LIKE '{}'",
            pattern(|s| s.to_lowercase())?
        ),
        Op::Iexact => format!(
            "lowerUTF8({text}) = '{}'",
            match first() {
                Value::Text(s) => escape_string(&s.to_lowercase()),
                _ => String::new(),
            }
        ),
        Op::Contains => format!("{text} LIKE '{}'", pattern(|s| format!("%{s}%"))?),
        Op::StartsWith => format!("{text} LIKE '{}'", pattern(|s| format!("{s}%"))?),
        Op::EndsWith => format!("{text} LIKE '{}'", pattern(|s| format!("%{s}"))?),
        // The regex family rides RE2 verbatim (match() is an
        // unanchored search — the contract's semantics); IREGEXP
        // carries the inline (?i) flag.
        Op::Regexp => format!("match({}, '{}')", column, pattern(|s| s.to_owned())?,),
        Op::Iregexp => format!("match({}, '(?i){}')", column, pattern(|s| s.to_owned())?,),
        Op::IsNull => format!("coalesce({has}, 0) = 0"),
        Op::IsNotNull => format!("coalesce({has}, 0) != 0"),
        // The containment pair never reach here (the contract refuses
        // them with a json_path).
        _ => {
            return Err(StorageError::Unsupported(
                "the containment pair runs on the whole document — no json_path".into(),
            ))
        }
    })
}

/// The array-membership face: `has(JSONExtractArrayRaw(...))` —
/// non-array columns (objects, NULL, scalars) read empty, so the
/// array-type guard is structural; membership only, never whole-value
/// equality.
fn array_contains_sql(condition: &Condition) -> Result<String, StorageError> {
    let element = condition.values.first().cloned().unwrap_or(Value::Null);
    let canonical = rushwind_storage::json::encode(&element);
    let doc = format!("coalesce({}, '')", identifier(&condition.field));
    Ok(format!(
        "isValidJSON({doc}) AND has(JSONExtractArrayRaw({doc}), '{}')",
        escape_string(&canonical)
    ))
}

/// The containment face, decomposed over ClickHouse's JSON functions —
/// the same honest shape the SQLite face renders: whole-document
/// equality, the flat key-subset for object needles (each key's
/// `JSONExtractRaw` against the value's canonical text), and top-level
/// element membership over `JSONExtractArrayRaw` (which on a non-array
/// column reads empty — the array-type guard comes free). Deeper
/// containment refuses at the contract's validate; the stored
/// documents are engine-canonicalized, so raw slices and canonical
/// operand texts compare verbatim.
fn json_contains_sql(condition: &Condition) -> Result<String, StorageError> {
    let needle = condition.values.first().cloned().unwrap_or(Value::Null);
    let canonical = rushwind_storage::json::encode(&needle);
    let quoted = format!("'{}'", escape_string(&canonical));
    let column = identifier(&condition.field);
    // The coalesce keeps the JSON functions off the Nullable type
    // synthesis (the live suite caught the Array-inside-Nullable
    // error); NULL rows fail isValidJSON and stay invisible.
    let doc = format!("coalesce({}, '')", column);
    let mut arms = vec![format!("{doc} = {quoted}")];
    // The flat key-subset for object needles — every key must extract
    // and compare (an AND group inside the OR of containment arms).
    if let Some(entries) = object_entries(&needle) {
        let subset: Vec<String> = entries
            .iter()
            .map(|(key, value)| {
                format!(
                    "JSONExtractRaw({doc}, '{}') = '{}'",
                    escape_string(key),
                    escape_string(&rushwind_storage::json::encode(value))
                )
            })
            .collect();
        if !subset.is_empty() {
            arms.push(format!("({})", subset.join(" AND ")));
        }
    }
    // Top-level element membership — non-array columns read empty, so
    // the guard is structural.
    arms.push(format!("has(JSONExtractArrayRaw({doc}), {quoted})"));
    Ok(format!("isValidJSON({doc}) AND ({})", arms.join(" OR ")))
}

/// The needle object's flat entries; non-objects read empty (the
/// subset arms simply don't render).
fn object_entries(needle: &Value) -> Option<Vec<(String, Value)>> {
    let rushwind_storage::json::Json::Obj(entries) =
        rushwind_storage::json::parse(needle.as_json()?)?
    else {
        return None;
    };
    Some(
        entries
            .iter()
            .map(|(key, value)| (key.clone(), json_to_value(value)))
            .collect(),
    )
}

/// One parsed JSON model value back into the contract's kinds (the
/// needle values ride the canonical encoder).
fn json_to_value(value: &rushwind_storage::json::Json) -> Value {
    match value {
        rushwind_storage::json::Json::Null => Value::Null,
        rushwind_storage::json::Json::Bool(b) => Value::Bool(*b),
        rushwind_storage::json::Json::Num(n) => Value::Real(*n),
        rushwind_storage::json::Json::Str(s) => Value::Text(s.clone()),
        rushwind_storage::json::Json::Arr(items) => {
            Value::Array(items.iter().map(json_to_value).collect())
        }
        rushwind_storage::json::Json::Obj(_) => {
            Value::Json(rushwind_storage::json::encode_model(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", ColumnKind::Text)
            .column("age", ColumnKind::Int)
            .column("score", ColumnKind::Real)
            .column("meta", ColumnKind::Json)
            .build()
            .expect("valid schema")
    }

    #[test]
    fn regexp_rides_re2_verbatim() {
        let sql = condition_sql(&Condition::new(
            "name",
            Op::Regexp,
            [Value::Text("^a[0-9]+$".into())],
        ))
        .expect("renders");
        assert_eq!(sql, "match(`name`, '^a[0-9]+$')");
        let sql = condition_sql(&Condition::new(
            "name",
            Op::Iregexp,
            [Value::Text("^a[0-9]+$".into())],
        ))
        .expect("renders");
        assert_eq!(sql, "match(`name`, '(?i)^a[0-9]+$')");
    }

    #[test]
    fn array_contains_rides_json_extract_array_raw() {
        let sql = condition_sql(&Condition::new(
            "meta",
            Op::ArrayContains,
            [Value::Text("red".into())],
        ))
        .expect("renders");
        assert_eq!(
            sql,
            "isValidJSON(coalesce(`meta`, '')) AND has(JSONExtractArrayRaw(coalesce(`meta`, '')), '\"red\"')"
        );
    }

    #[test]
    fn json_contains_decomposes_the_object_needle() {
        let sql = condition_sql(&Condition::new(
            "meta",
            Op::JsonContains,
            [Value::Json(r#"{"k":1,"kind":"two"}"#.to_string())],
        ))
        .expect("renders");
        // Equality, the flat key-subset, and the array-membership arm
        // under the validity guard.
        assert!(
            sql.starts_with("isValidJSON(coalesce(`meta`, '')) AND ("),
            "{sql}"
        );
        assert!(
            sql.contains("coalesce(`meta`, '') = '{\"k\":1,\"kind\":\"two\"}'"),
            "{sql}"
        );
        assert!(
            sql.contains(
                "JSONExtractRaw(coalesce(`meta`, ''), 'k') = '1' AND JSONExtractRaw(coalesce(`meta`, ''), 'kind') = '\"two\"'"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("has(JSONExtractArrayRaw(coalesce(`meta`, '')),"),
            "{sql}"
        );
    }

    #[test]
    fn json_contains_scalar_needle_rides_membership() {
        let sql = condition_sql(&Condition::new(
            "meta",
            Op::JsonContains,
            [Value::Json("5".to_string())],
        ))
        .expect("renders");
        assert!(sql.contains("coalesce(`meta`, '') = '5'"), "{sql}");
        assert!(
            sql.contains("has(JSONExtractArrayRaw(coalesce(`meta`, '')), '5')"),
            "{sql}"
        );
    }

    #[test]
    fn json_path_extracts_raw_and_compares_canonical() {
        let sql =
            condition_sql(&Condition::new("meta", Op::Eq, [Value::Int(1)]).with_json_path("k"))
                .expect("renders");
        assert_eq!(sql, "JSONExtractRaw(coalesce(`meta`, ''), 'k') = '1'");

        let sql = condition_sql(
            &Condition::new("meta", Op::Eq, [Value::Text("x".into())]).with_json_path("a.0.b"),
        )
        .expect("renders");
        assert_eq!(
            sql,
            "JSONExtractRaw(coalesce(`meta`, ''), 'a', 0, 'b') = '\"x\"'"
        );
    }

    #[test]
    fn json_path_null_rides_json_has_with_coalesce() {
        let sql = condition_sql(&Condition::new("meta", Op::IsNull, []).with_json_path("tags"))
            .expect("renders");
        assert_eq!(
            sql,
            "coalesce(JSONHas(coalesce(`meta`, ''), 'tags'), 0) = 0"
        );
        let sql = condition_sql(&Condition::new("meta", Op::IsNotNull, []).with_json_path("tags"))
            .expect("renders");
        assert_eq!(
            sql,
            "coalesce(JSONHas(coalesce(`meta`, ''), 'tags'), 0) != 0"
        );
    }

    #[test]
    fn json_path_patterns_extract_strings_and_ranges_guard() {
        let sql = condition_sql(
            &Condition::new("meta", Op::Contains, [Value::Text("wo".into())])
                .with_json_path("kind"),
        )
        .expect("renders");
        assert_eq!(
            sql,
            "JSONExtractString(coalesce(`meta`, ''), 'kind') LIKE '%wo%'"
        );
        let sql =
            condition_sql(&Condition::new("meta", Op::Gt, [Value::Int(5)]).with_json_path("k"))
                .expect("renders");
        assert_eq!(
            sql,
            "coalesce(JSONHas(coalesce(`meta`, ''), 'k'), 0) != 0 AND JSONExtractFloat(coalesce(`meta`, ''), 'k') > 5"
        );
    }

    #[test]
    fn ddl_pins_mergetree_ordered_by_pk() {
        let sql = create_table_sql(&schema());
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `widgets` (`id` Int64, `name` Nullable(String), \
             `age` Nullable(Int64), `score` Nullable(Float64), \
             `meta` Nullable(String)) ENGINE = MergeTree ORDER BY `id`"
        );
    }

    #[test]
    fn string_literals_escape() {
        assert_eq!(escape_string("it's a \\"), "it\\'s a \\\\");
        assert_eq!(literal(&Value::Text("it's".into())), "'it\\'s'");
    }

    #[test]
    fn insert_lists_columns_and_literals() {
        let row = Record::new()
            .set("id", 3i64)
            .set("name", "it's")
            .set("score", 2.5);
        let sql = insert_sql(&schema(), &row).expect("builds");
        assert_eq!(
            sql,
            "INSERT INTO `widgets` (`id`, `name`, `age`, `score`, `meta`) VALUES (3, 'it\\'s', NULL, 2.5, NULL)"
        );
    }

    #[test]
    fn real_literals_always_carry_a_decimal_point() {
        assert_eq!(literal(&Value::Real(2.0)), "2.0");
        assert_eq!(literal(&Value::Int(2)), "2");
    }

    #[test]
    fn mutations_carry_the_sync_setting() {
        let schema = schema();
        let update =
            update_sql(&schema, 7, &[("age".into(), Value::Int(9))], None).expect("builds");
        assert_eq!(
            update,
            "ALTER TABLE `widgets` UPDATE `age` = 9 WHERE `id` = 7 SETTINGS mutations_sync = 1"
        );
        let delete = delete_sql(&schema, 7, None).expect("builds");
        assert_eq!(
            delete,
            "ALTER TABLE `widgets` DELETE WHERE `id` = 7 SETTINGS mutations_sync = 1"
        );
    }

    #[test]
    fn filter_tree_translates_with_nesting() {
        let tree = FilterNode::Any(vec![
            FilterNode::Cond(Condition::new("age", Op::Gte, [Value::Int(10)])),
            FilterNode::All(vec![
                FilterNode::Cond(Condition::new(
                    "name",
                    Op::Ilike,
                    [Value::Text("%E%".into())],
                )),
                FilterNode::Cond(Condition::new("score", Op::IsNull, [])),
            ]),
        ]);
        assert_eq!(
            node_sql(&tree).expect("translates"),
            "(`age` >= 10 OR (`name` ILIKE '%e%' AND `score` IS NULL))"
        );
    }

    #[test]
    fn select_covers_paging_and_cursor() {
        let schema = schema();
        let page = select_sql(
            &schema,
            &["id".into(), "name".into()],
            None,
            &ListQuery::page(2, 10),
        )
        .expect("builds");
        assert_eq!(
            page,
            "SELECT `id`, `name` FROM `widgets` ORDER BY `id` ASC LIMIT 10 OFFSET 10"
        );

        let token = ListQuery {
            paging: Paging::Token {
                token: rushwind_storage::encode_cursor(42),
                limit: 3,
            },
            ..ListQuery::default()
        };
        let cursor = select_sql(&schema, &["id".into()], None, &token).expect("builds");
        assert_eq!(
            cursor,
            "SELECT `id` FROM `widgets` WHERE `id` > 42 ORDER BY `id` ASC LIMIT 4"
        );
    }
}

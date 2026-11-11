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
                ColumnKind::Text => "String",
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
    let column = identifier(&condition.field);
    let first = || condition.values.first().cloned().unwrap_or(Value::Null);
    let pattern = |fmt: fn(&str) -> String| match first() {
        Value::Text(s) => Ok(fmt(&escape_string(&s))),
        _ => Err(StorageError::InvalidQuery(
            "pattern operators apply only to text operands".into(),
        )),
    };
    Ok(match condition.op {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", ColumnKind::Text)
            .column("age", ColumnKind::Int)
            .column("score", ColumnKind::Real)
            .build()
            .expect("valid schema")
    }

    #[test]
    fn ddl_pins_mergetree_ordered_by_pk() {
        let sql = create_table_sql(&schema());
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `widgets` (`id` Int64, `name` Nullable(String), \
             `age` Nullable(Int64), `score` Nullable(Float64)) ENGINE = MergeTree ORDER BY `id`"
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
            "INSERT INTO `widgets` (`id`, `name`, `age`, `score`) VALUES (3, 'it\\'s', NULL, 2.5)"
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

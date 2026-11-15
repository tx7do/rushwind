//! CQL literal formatting — pure functions, unit-tested offline.
//!
//! The adapter binds values as inlined literals (Cassandra's native
//! serialization goes through the driver's typed bind path; the suite's
//! partitions are small, so literal simplicity wins).

use rushwind_storage::Value;

/// Escapes a CQL string literal body (single quotes doubled).
pub fn escape_string(text: &str) -> String {
    text.replace('\'', "''")
}

/// A contract [`Value`] as a CQL literal.
pub fn literal(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "null".into(),
        Some(Value::Bool(b)) => if *b { "true" } else { "false" }.into(),
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::Real(f)) => {
            let text = f.to_string();
            if text.contains(['.', 'e', 'E']) || text.contains("inf") || text.contains("NaN") {
                text
            } else {
                // Whole-number reals: format the *number* with one decimal
                // (formatting the string with `.1` would truncate it).
                format!("{f:.1}")
            }
        }
        Some(Value::Text(s)) => format!("'{}'", escape_string(s)),
        // JSON kinds store as their canonical text (a JSON column's
        // CQL face is a text column holding the document).
        Some(Value::Array(items)) => {
            format!(
                "'{}'",
                escape_string(&rushwind_storage::json::encode(&Value::Array(
                    items.clone()
                )))
            )
        }
        Some(Value::Json(text)) => format!("'{}'", escape_string(text)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literals_double_their_quotes() {
        assert_eq!(escape_string("it's"), "it''s");
        assert_eq!(literal(Some(&Value::Text("it's".into()))), "'it''s'");
    }

    #[test]
    fn nulls_and_booleans_spell_cql_style() {
        assert_eq!(literal(None), "null");
        assert_eq!(literal(Some(&Value::Null)), "null");
        assert_eq!(literal(Some(&Value::Bool(true))), "true");
        assert_eq!(literal(Some(&Value::Bool(false))), "false");
    }

    #[test]
    fn reals_keep_a_decimal_point() {
        assert_eq!(literal(Some(&Value::Real(2.0))), "2.0");
        assert_eq!(literal(Some(&Value::Int(2))), "2");
    }
}

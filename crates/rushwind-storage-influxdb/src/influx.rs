//! Line-protocol and InfluxQL generation — pure functions, unit-tested
//! offline.

use rushwind_storage::{Record, Value};

/// Escapes a measurement, tag value or field key for line protocol
/// (commas, spaces, equals signs, and — critically — line breaks).
///
/// A raw `\n` or `\r` inside any token would terminate the point mid-line
/// and let the remainder be parsed as a second, attacker-chosen point
/// under the writer's credentials; the escapes keep one physical line per
/// point.
pub fn escape_line_token(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            ',' | ' ' | '=' => {
                out.push('\\');
                out.push(ch);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}

/// Escapes a string inside an InfluxQL single-quoted literal.
pub fn escape_query_string(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\'', "''")
}

/// Escapes a text field value inside a line-protocol quoted string: the
/// parser honors `\\`, `\"` and `\n`/`\r` escapes inside the quotes, so
/// the backslash goes first and a literal line break becomes its
/// two-character escape — a raw newline would terminate the point and
/// append a second, attacker-chosen point under the writer's credentials.
fn escape_field_string(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// The line-protocol point for a materialized row: the primary key is a
/// **tag** (series identity), other non-null columns are fields, and the
/// timestamp is pinned to epoch 0 so same-id writes overwrite in place.
/// The tag-form of the primary key (a series-identifying string).
fn display_id(value: &Value) -> String {
    // The pk is an integer by contract; other widths degrade to their
    // scalar text.
    match value {
        Value::Int(i) => i.to_string(),
        Value::Real(f) => format!("{f}"),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

/// The line-protocol point for a materialized row: the primary key is a
/// **tag** (series identity), other non-null columns are fields, and the
/// timestamp is pinned to epoch 0 so same-id writes overwrite in place.
pub fn line(measurement: &str, row: &Record) -> String {
    let mut tags = String::new();
    let mut fields = Vec::new();
    for (column, value) in row.iter() {
        if column == "id" {
            tags.push_str(",id=");
            tags.push_str(&escape_line_token(&display_id(value)));
            continue;
        }
        let field = match value {
            Value::Null => continue, // absent fields read back as NULL
            Value::Bool(b) => format!("{}={}", escape_line_token(column), b),
            Value::Int(i) => format!("{}={}i", escape_line_token(column), i),
            Value::Real(f) => format!("{}={}", escape_line_token(column), f),
            Value::Text(s) => format!(
                "{}=\"{}\"",
                escape_line_token(column),
                escape_field_string(s)
            ),
        };
        fields.push(field);
    }
    if fields.is_empty() {
        // InfluxDB requires at least one field; keep a sentinel so the
        // series (and its NULL fields) still exists.
        fields.push("_present=true".to_owned());
    }
    format!(
        "{measurement}{tags} {fields} 0",
        measurement = escape_line_token(measurement),
        tags = tags,
        fields = fields.join(",")
    )
}

/// `SELECT JSON * FROM measurement` — the full-scan the adapter filters
/// and orders in memory.
pub fn select_all(measurement: &str) -> String {
    format!("SELECT * FROM \"{}\"", escape_query_string(measurement))
}

/// The InfluxQL delete for one series: `DELETE FROM m WHERE "id" = '…'`.
pub fn delete_by_id(measurement: &str, primary_key: &str, id: i64) -> String {
    format!(
        "DELETE FROM \"{}\" WHERE \"{}\" = '{}'",
        escape_query_string(measurement),
        escape_query_string(primary_key),
        id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Record {
        Record::new()
            .set("id", 3i64)
            .set("name", "it's bolt")
            .set("age", 7i64)
            .set("score", Value::Null)
    }

    #[test]
    fn line_protocol_pins_series_and_fields() {
        let line = line("widgets", &sample());
        assert_eq!(line, "widgets,id=3 age=7i,name=\"it's bolt\" 0");
        // Null columns are omitted; they read back as NULL.
        assert!(!line.contains("score"));
    }

    #[test]
    fn line_protocol_escapes_tokens_and_strings() {
        let row = Record::new()
            .set("id", 1i64)
            .set("name", "a b,c=d")
            .set("age", 1i64);
        let line = line("my table", &row);
        assert_eq!(line, "my\\ table,id=1 age=1i,name=\"a b,c=d\" 0");
    }

    #[test]
    fn text_values_cannot_smuggle_a_second_line() {
        // A tenant-controlled text value carrying a raw newline used to
        // terminate the point and append a forged second point under the
        // writer's credentials; the escape keeps exactly one physical line.
        let row = Record::new()
            .set("id", 1i64)
            .set("name", "legit\nage=9i fake=\"pwned\" m2 b")
            .set("age", 1i64);
        let line = line("widgets", &row);
        assert!(
            !line.contains('\n') && !line.contains('\r'),
            "a text value must never break the point into two lines: {line:?}"
        );
        assert_eq!(
            line,
            "widgets,id=1 age=1i,\
             name=\"legit\\nage=9i fake=\\\"pwned\\\" m2 b\" 0"
        );
    }

    #[test]
    fn line_tokens_cannot_smuggle_a_second_line() {
        // Measurement names, tag values and field keys ride the same rule:
        // no raw line break may reach the wire.
        assert_eq!(escape_line_token("a\nb\rc"), "a\\nb\\rc");
        let row = Record::new()
            .set("id", Value::Text("x\ny".into()))
            .set("age", 1i64);
        let line = line("wea\napons", &row);
        assert!(
            !line.contains('\n'),
            "a token must never break the point into two lines: {line:?}"
        );
        assert_eq!(line, "wea\\napons,id=x\\ny age=1i 0");
    }

    #[test]
    fn queries_quote_identifiers_and_strings() {
        assert_eq!(select_all("my table"), "SELECT * FROM \"my table\"");
        assert_eq!(
            delete_by_id("widgets", "id", 9),
            "DELETE FROM \"widgets\" WHERE \"id\" = '9'"
        );
    }
}

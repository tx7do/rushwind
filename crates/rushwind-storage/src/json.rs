//! A minimal, dependency-free JSON reader — the contract's containment
//! semantics for [`crate::Value::Json`] need to parse JSON, and the
//! contract layer stays dependency-free by doctrine, so the reader is
//! hand-written here (the house precedent: the LIKE evaluator in
//! `filter.rs` is likewise hand-written).
//!
//! Numbers parse as `f64` — containment compares values, and JSON's
//! number model maps naturally onto f64 for membership decisions. Text
//! comparison is by code points. The reader accepts exactly the JSON
//! grammar (RFC 8259) with no extensions; malformed input reads as
//! `None` and the containment face treats it as "not contained".

/// One parsed JSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    /// `null`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// A number (parsed as f64; containment compares numerically).
    Num(f64),
    /// A string.
    Str(String),
    /// An array.
    Arr(Vec<Json>),
    /// An object.
    Obj(Vec<(String, Json)>),
}

/// Encodes a contract [`crate::Value`] as canonical minified JSON
/// text — the write face of a JSON column and the operand encoding
/// the JSON operators carry on the wire ([`crate::Value::Json`]
/// documents embed verbatim; arrays encode element-wise).
pub fn encode(value: &crate::Value) -> String {
    match value {
        crate::Value::Null => "null".to_string(),
        crate::Value::Bool(b) => b.to_string(),
        crate::Value::Int(i) => i.to_string(),
        crate::Value::Real(f) => encode_number(*f),
        crate::Value::Text(s) => encode_string(s),
        crate::Value::Array(items) => {
            let rendered: Vec<String> = items.iter().map(encode).collect();
            format!("[{}]", rendered.join(","))
        }
        crate::Value::Json(text) => text.clone(),
    }
}

/// Encodes one parsed JSON model value as canonical minified text —
/// the canonical face of a document extracted mid-path (objects need
/// their text to ride back as [`crate::Value::Json`]).
pub fn encode_model(value: &Json) -> String {
    match value {
        Json::Null => "null".to_string(),
        Json::Bool(b) => b.to_string(),
        Json::Num(n) => encode_number(*n),
        Json::Str(s) => encode_string(s),
        Json::Arr(items) => {
            let rendered: Vec<String> = items.iter().map(encode_model).collect();
            format!("[{}]", rendered.join(","))
        }
        Json::Obj(entries) => {
            let rendered: Vec<String> = entries
                .iter()
                .map(|(key, value)| format!("{}:{}", encode_string(key), encode_model(value)))
                .collect();
            format!("{{{}}}", rendered.join(","))
        }
    }
}

/// Renders an f64 without a spurious `.0`-style tail — integral
/// values stay integral in JSON (the contract's round trip keeps
/// Int/Real apart).
fn encode_number(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

/// Encodes one JSON string literal (quotes, backslashes, and the
/// control characters the JSON grammar names).
fn encode_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Parses one JSON document (leading and trailing whitespace allowed;
/// trailing garbage fails).
pub fn parse(text: &str) -> Option<Json> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        pos: 0,
    };
    parser.skip_ws();
    let value = parser.value()?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return None;
    }
    Some(value)
}

/// The MySQL `JSON_CONTAINS` semantics: the candidate is contained when
/// it equals the document, or — for objects — every candidate key is
/// present with a contained value (key-subset), or — for arrays — some
/// element contains it. Scalars contain only by equality.
pub fn contains(hay: &Json, needle: &Json) -> bool {
    if hay == needle {
        return true;
    }
    match (hay, needle) {
        // Objects contain object candidates by key-subset with
        // contained values; non-object candidates by some value
        // containing them.
        (Json::Obj(entries), Json::Obj(needed)) => needed.iter().all(|(key, value)| {
            entries
                .iter()
                .any(|(hay_key, hay_value)| hay_key == key && contains(hay_value, value))
        }),
        (Json::Obj(entries), needle) => entries
            .iter()
            .any(|(_, hay_value)| contains(hay_value, needle)),
        // Arrays contain array candidates when every candidate element
        // is contained by some element; other candidates by some
        // element containing them.
        (Json::Arr(items), Json::Arr(needed)) => needed
            .iter()
            .all(|needed| items.iter().any(|item| contains(item, needed))),
        (Json::Arr(items), needle) => items.iter().any(|item| contains(item, needle)),
        _ => false,
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, word: &str) -> bool {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Option<Json> {
        self.skip_ws();
        match self.peek()? {
            b'n' => self.eat("null").then_some(Json::Null),
            b't' => self.eat("true").then_some(Json::Bool(true)),
            b'f' => self.eat("false").then_some(Json::Bool(false)),
            b'"' => self.string().map(Json::Str),
            b'[' => self.array(),
            b'{' => self.object(),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn string(&mut self) -> Option<String> {
        if self.peek()? != b'"' {
            return None;
        }
        self.pos += 1;
        let mut out = String::new();
        loop {
            let byte = self.peek()?;
            self.pos += 1;
            match byte {
                b'"' => return Some(out),
                b'\\' => {
                    let escape = self.peek()?;
                    self.pos += 1;
                    match escape {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => out.push(self.unicode_escape()?),
                        _ => return None,
                    }
                }
                _ => {
                    // Multi-byte UTF-8 rides through verbatim.
                    let start = self.pos - 1;
                    let len = utf8_len(byte)?;
                    self.pos = start + len;
                    out.push_str(std::str::from_utf8(self.bytes.get(start..self.pos)?).ok()?);
                }
            }
        }
    }

    /// The `\uXXXX` escape, including surrogate pairs.
    fn unicode_escape(&mut self) -> Option<char> {
        let first = self.hex4()?;
        if (0xD800..0xDC00).contains(&first) {
            // High surrogate: the low surrogate must follow.
            if !self.eat("\\u") {
                return None;
            }
            let second = self.hex4()?;
            if !(0xDC00..0xE000).contains(&second) {
                return None;
            }
            let combined = 0x10000 + ((first - 0xD800) << 10) + (second - 0xDC00);
            char::from_u32(combined)
        } else {
            char::from_u32(first)
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let slice = self.bytes.get(self.pos..self.pos + 4)?;
        let text = std::str::from_utf8(slice).ok()?;
        let value = u32::from_str_radix(text, 16).ok()?;
        self.pos += 4;
        Some(value)
    }

    fn number(&mut self) -> Option<Json> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        // Integer part.
        match self.peek()? {
            b'0' => self.pos += 1,
            b'1'..=b'9' => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return None,
        }
        // Fraction.
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        // Exponent.
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).ok()?;
        text.parse::<f64>().ok().map(Json::Num)
    }

    fn array(&mut self) -> Option<Json> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Some(Json::Arr(items));
        }
        loop {
            let value = self.value()?;
            items.push(value);
            self.skip_ws();
            match self.peek()? {
                b',' => {
                    self.pos += 1;
                }
                b']' => {
                    self.pos += 1;
                    return Some(Json::Arr(items));
                }
                _ => return None,
            }
        }
    }

    fn object(&mut self) -> Option<Json> {
        self.pos += 1; // '{'
        let mut entries = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Some(Json::Obj(entries));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            if self.peek()? != b':' {
                return None;
            }
            self.pos += 1;
            let value = self.value()?;
            entries.push((key, value));
            self.skip_ws();
            match self.peek()? {
                b',' => {
                    self.pos += 1;
                }
                b'}' => {
                    self.pos += 1;
                    return Some(Json::Obj(entries));
                }
                _ => return None,
            }
        }
    }
}

/// The UTF-8 sequence length for a lead byte; `None` for invalid leads.
fn utf8_len(lead: u8) -> Option<usize> {
    match lead {
        0x00..=0x7F => Some(1),
        0xC0..=0xDF => Some(2),
        0xE0..=0xEF => Some(3),
        0xF0..=0xF7 => Some(4),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_grammar() {
        assert_eq!(parse("null"), Some(Json::Null));
        assert_eq!(parse(" true "), Some(Json::Bool(true)));
        assert_eq!(parse("-2.5e2"), Some(Json::Num(-250.0)));
        assert_eq!(
            parse("\"a\\nb\\u0041\\ud83d\\ude00\""),
            Some(Json::Str("a\nbA😀".to_string()))
        );
        assert_eq!(
            parse("[1, \"two\", {\"three\": 3}]"),
            Some(Json::Arr(vec![
                Json::Num(1.0),
                Json::Str("two".to_string()),
                Json::Obj(vec![("three".to_string(), Json::Num(3.0))]),
            ]))
        );
        assert_eq!(
            parse("{\"a\": {}, \"b\": []}"),
            Some(Json::Obj(vec![
                ("a".to_string(), Json::Obj(vec![])),
                ("b".to_string(), Json::Arr(vec![])),
            ]))
        );
    }

    #[test]
    fn malformed_reads_none() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("{"), None);
        assert_eq!(parse("[1,]"), None);
        assert_eq!(parse("{\"a\" 1}"), None);
        assert_eq!(parse("1 2"), None);
        assert_eq!(parse("\"\\u12\""), None);
        assert_eq!(parse("nul"), None);
        assert_eq!(parse("01"), None);
        assert_eq!(parse("1."), None);
        assert_eq!(parse("1e"), None);
    }

    #[test]
    fn encodes_the_contract_kinds() {
        use crate::Value;
        assert_eq!(encode(&Value::Null), "null");
        assert_eq!(encode(&Value::Bool(true)), "true");
        assert_eq!(encode(&Value::Int(7)), "7");
        assert_eq!(encode(&Value::Real(1.5)), "1.5");
        assert_eq!(
            encode(&Value::Text("a\"b\\c\nd".into())),
            "\"a\\\"b\\\\c\\nd\""
        );
        assert_eq!(
            encode(&Value::Array(vec![
                Value::Int(1),
                Value::Text("two".into()),
                Value::Array(vec![]),
                Value::Json("{\"k\":1}".into()),
            ])),
            "[1,\"two\",[],{\"k\":1}]"
        );
        assert_eq!(encode(&Value::Json("[1,2]".into())), "[1,2]");
    }

    #[test]
    fn containment_follows_mysql_json_contains() {
        let hay = parse(r#"{"a": [1, {"b": "x"}], "c": 2}"#).expect("hay");
        // Exact equality.
        assert!(contains(
            &hay,
            &parse(r#"{"a": [1, {"b": "x"}], "c": 2}"#).expect("needle")
        ));
        // Object key-subset with contained values.
        assert!(contains(&hay, &parse(r#"{"c": 2}"#).expect("needle")));
        assert!(contains(&hay, &parse(r#"{"a": [1]}"#).expect("needle")));
        // Scalars and arrays ride the object's values.
        assert!(contains(&hay, &parse("2").expect("needle")));
        assert!(contains(&hay, &parse("[1]").expect("needle")));
        // A top-level key the object lacks misses even when a value
        // holds it.
        assert!(!contains(&hay, &parse(r#"{"b": "x"}"#).expect("needle")));
        // Misses.
        assert!(!contains(&hay, &parse("3").expect("needle")));
        assert!(!contains(
            &hay,
            &parse(r#"{"c": 2, "d": 4}"#).expect("needle")
        ));

        let arr = parse(r#"[1, "two", [3]]"#).expect("arr");
        assert!(contains(&arr, &parse("1").expect("needle")));
        assert!(contains(&arr, &parse("[3]").expect("needle")));
        // Array candidates: every element contained by some element.
        assert!(contains(&arr, &parse(r#"[1, [3]]"#).expect("needle")));
        // Deep: the element [3] contains 3, so the candidate [1, 3]
        // is contained (every element rides some element).
        assert!(contains(&arr, &parse("3").expect("needle")));
        assert!(contains(&arr, &parse(r#"[1, 3]"#).expect("needle")));
    }
}

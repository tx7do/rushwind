//! The AIP-flavored text filter language.
//!
//! Filter strings come in the Google AIP-160 style —
//! `name = "bolt" AND (age > 10 OR score <= 2.5)` — alongside protojson.
//! This module parses that text into the same [`FilterExpr`] tree the wire
//! layer produces, with the operator complements of the contract's
//! [`Op`] table:
//!
//! | text | operator |
//! |:---|:---|
//! | `=` | [`Op::Eq`] |
//! | `!=`, `<>` | [`Op::NotEq`] |
//! | `>`, `>=`, `<`, `<=` | [`Op::Gt`], [`Op::Gte`], [`Op::Lt`], [`Op::Lte`] |
//! | `IN (…)` / `NOT IN (…)` | [`Op::In`] / [`Op::NotIn`] |
//!
//! `AND` binds tighter than `OR`; juxtaposed comparisons conjoin
//! (implicit `AND`, as in AIP). Keywords are case-insensitive. Values are
//! quoted strings, numbers, `true`/`false`/`null`, or barewords (read as
//! text, AIP-style). `NOT` negates comparisons whose operator has an exact
//! complement and flips group combinators; negated pattern operators
//! (`contains` and friends) are rejected — the contract has no
//! `NotContains`.

use crate::error::StorageError;
use crate::filter::{Condition, FilterExpr, FilterNode, Op};
use crate::value::Value;

impl FilterExpr {
    /// Parses an AIP-160-style filter string into a filter expression.
    ///
    /// ```ignore
    /// let filter = FilterExpr::from_aip(r#"name = "bolt" AND age >= 3"#)?;
    /// ```
    pub fn from_aip(input: &str) -> Result<Self, StorageError> {
        let mut p = Aip {
            bytes: input.as_bytes(),
            pos: 0,
        };
        p.skip_ws();
        let expr = p.parse_or()?;
        p.skip_ws();
        if p.pos != p.bytes.len() {
            return Err(p.err("trailing content after the filter expression"));
        }
        Ok(expr)
    }
}

struct Aip<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Aip<'a> {
    fn err(&self, message: &str) -> StorageError {
        StorageError::InvalidQuery(format!("AIP filter at byte {}: {message}", self.pos))
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, token: &str) -> bool {
        if self.bytes[self.pos..].starts_with(token.as_bytes()) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    /// Case-insensitive keyword match with a bareword-boundary check.
    fn keyword(&mut self, word: &str) -> bool {
        let end = self.pos + word.len();
        if !self.bytes[self.pos..]
            .get(..word.len())
            .is_some_and(|slice| slice.eq_ignore_ascii_case(word.as_bytes()))
        {
            return false;
        }
        if matches!(self.bytes.get(end), Some(&b) if is_bareword(b)) {
            return false;
        }
        self.pos = end;
        true
    }

    // ---- grammar -----------------------------------------------------------

    fn parse_or(&mut self) -> Result<FilterExpr, StorageError> {
        let mut children = vec![self.parse_and()?];
        self.skip_ws();
        while self.keyword("OR") {
            children.push(self.parse_and()?);
            self.skip_ws();
        }
        Ok(match children.len() {
            1 => children.pop().expect("checked length"),
            _ => FilterExpr::any(children),
        })
    }

    fn parse_and(&mut self) -> Result<FilterExpr, StorageError> {
        let mut children = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                None | Some(b')') => break,
                Some(_) if self.peek_is_keyword("OR") => break,
                Some(_) => {
                    // Consume an explicit AND, then the factor that follows
                    // — or treat the factor itself as an implicit AND.
                    let _ = self.keyword("AND");
                    children.push(self.parse_factor()?);
                }
            }
        }
        Ok(match children.len() {
            0 => FilterExpr::matches_all(),
            1 => children.pop().expect("checked length"),
            _ => FilterExpr::all(children),
        })
    }

    fn peek_is_keyword(&self, word: &str) -> bool {
        match self.bytes[self.pos..].get(..word.len()) {
            Some(slice) if slice.eq_ignore_ascii_case(word.as_bytes()) => !matches!(
                self.bytes.get(self.pos + word.len()),
                Some(&b) if is_bareword(b)
            ),
            _ => false,
        }
    }

    fn parse_factor(&mut self) -> Result<FilterExpr, StorageError> {
        self.skip_ws();
        if self.peek() == Some(b'(') {
            self.pos += 1;
            let inner = self.parse_or()?;
            self.skip_ws();
            if !self.eat(")") {
                return Err(self.err("expected ')'"));
            }
            return Ok(inner);
        }
        let start = self.pos;
        if self.keyword("NOT") {
            self.skip_ws();
            let inner = self.parse_factor()?;
            return negate(inner.node()).map_err(|_| {
                StorageError::InvalidQuery(format!(
                    "AIP filter at byte {start}: NOT on this operand has no complement operator"
                ))
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<FilterExpr, StorageError> {
        let field = self
            .bareword()
            .ok_or_else(|| self.err("expected a field name"))?;
        self.skip_ws();

        if self.peek_is_keyword("IN") {
            self.pos += "IN".len();
            return self.value_list(&field, Op::In);
        }
        if self.peek_is_keyword("NOT") {
            let mark = self.pos;
            self.pos += "NOT".len();
            self.skip_ws();
            if !self.peek_is_keyword("IN") {
                self.pos = mark;
                return Err(self.err("expected IN after NOT"));
            }
            self.pos += "IN".len();
            return self.value_list(&field, Op::NotIn);
        }

        let op = if self.eat("<=") {
            Op::Lte
        } else if self.eat(">=") {
            Op::Gte
        } else if self.eat("!=") || self.eat("<>") {
            Op::NotEq
        } else if self.eat("<") {
            Op::Lt
        } else if self.eat(">") {
            Op::Gt
        } else if self.eat("=") {
            Op::Eq
        } else {
            return Err(self.err("expected a comparison operator"));
        };
        self.skip_ws();
        let value = self.value()?;
        Ok(FilterExpr::cond(field, op, [value]))
    }

    fn value_list(&mut self, field: &str, op: Op) -> Result<FilterExpr, StorageError> {
        self.skip_ws();
        if !self.eat("(") {
            return Err(self.err("expected '(' after IN"));
        }
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(b')') && values.is_empty() {
                return Err(self.err("IN needs at least one value"));
            }
            values.push(self.value()?);
            self.skip_ws();
            if self.eat(",") {
                continue;
            }
            if self.eat(")") {
                return Ok(FilterExpr::cond(field, op, values));
            }
            return Err(self.err("expected ',' or ')' in the value list"));
        }
    }

    // ---- tokens ------------------------------------------------------------

    fn bareword(&mut self) -> Option<String> {
        let start = self.pos;
        if !matches!(self.peek(), Some(b) if b.is_ascii_alphabetic() || b == b'_') {
            return None;
        }
        while matches!(self.peek(), Some(b) if is_bareword(b)) {
            self.pos += 1;
        }
        Some(
            std::str::from_utf8(&self.bytes[start..self.pos])
                .ok()?
                .to_owned(),
        )
    }

    fn value(&mut self) -> Result<Value, StorageError> {
        self.skip_ws();
        match self.peek() {
            Some(quote @ (b'"' | b'\'')) => {
                self.pos += 1;
                let mut out = String::new();
                loop {
                    let Some(b) = self.peek() else {
                        return Err(self.err("unterminated string"));
                    };
                    if b == quote {
                        self.pos += 1;
                        return Ok(Value::Text(out));
                    }
                    if b == b'\\' {
                        self.pos += 1;
                        let Some(esc) = self.peek() else {
                            return Err(self.err("unterminated escape"));
                        };
                        self.pos += 1;
                        match esc {
                            b'n' => out.push('\n'),
                            b't' => out.push('\t'),
                            b'r' => out.push('\r'),
                            other => out.push(other as char),
                        }
                        continue;
                    }
                    let rest = std::str::from_utf8(&self.bytes[self.pos..])
                        .map_err(|_| self.err("invalid UTF-8"))?;
                    let ch = rest.chars().next().expect("non-empty");
                    out.push(ch);
                    self.pos += ch.len_utf8();
                }
            }
            Some(b) if b.is_ascii_digit() || b == b'-' || b == b'+' => {
                let start = self.pos;
                if matches!(self.peek(), Some(b'-' | b'+')) {
                    self.pos += 1;
                }
                let mut is_float = false;
                while let Some(b) = self.peek() {
                    match b {
                        b'0'..=b'9' => self.pos += 1,
                        b'.' => {
                            is_float = true;
                            self.pos += 1;
                        }
                        _ => break,
                    }
                }
                let text = std::str::from_utf8(&self.bytes[start..self.pos])
                    .map_err(|_| self.err("invalid number"))?;
                if is_float {
                    text.parse::<f64>()
                        .map(Value::Real)
                        .map_err(|_| self.err("invalid number"))
                } else {
                    text.parse::<i64>()
                        .map(Value::Int)
                        .or_else(|_| text.parse::<f64>().map(Value::Real))
                        .map_err(|_| self.err("invalid number"))
                }
            }
            Some(_) => {
                let word = self
                    .bareword()
                    .ok_or_else(|| self.err("expected a value"))?;
                match word.to_ascii_lowercase().as_str() {
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    "null" => Ok(Value::Null),
                    _ => Ok(Value::Text(word)),
                }
            }
            None => Err(self.err("expected a value")),
        }
    }
}

fn is_bareword(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-'
}

/// The complement table: every operator with an exact negation flips to it;
/// groups swap their combinator; pattern operators have no complement and
/// are reported as unsupported.
fn negate(node: &FilterNode) -> Result<FilterExpr, StorageError> {
    Ok(match node {
        FilterNode::All(children) => {
            let negated = children
                .iter()
                .map(negate)
                .collect::<Result<Vec<_>, StorageError>>()?;
            FilterExpr::any(negated)
        }
        FilterNode::Any(children) => {
            let negated = children
                .iter()
                .map(negate)
                .collect::<Result<Vec<_>, StorageError>>()?;
            FilterExpr::all(negated)
        }
        FilterNode::Cond(condition) => {
            let op = match condition.op {
                Op::Eq => Op::NotEq,
                Op::NotEq => Op::Eq,
                Op::Gt => Op::Lte,
                Op::Gte => Op::Lt,
                Op::Lt => Op::Gte,
                Op::Lte => Op::Gt,
                Op::In => Op::NotIn,
                Op::NotIn => Op::In,
                Op::Like => Op::NotLike,
                Op::NotLike => Op::Like,
                Op::IsNull => Op::IsNotNull,
                Op::IsNotNull => Op::IsNull,
                Op::Between => Op::NotBetween,
                Op::NotBetween => Op::Between,
                Op::Ilike | Op::Iexact | Op::Contains | Op::StartsWith | Op::EndsWith => {
                    return Err(StorageError::Unsupported(
                        "negated pattern operators".into(),
                    ))
                }
            };
            FilterExpr::from(FilterNode::Cond(Condition {
                field: condition.field.clone(),
                op,
                values: condition.values.clone(),
            }))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_comparison() {
        let filter = FilterExpr::from_aip("age >= 3").expect("parses");
        assert_eq!(
            filter.node(),
            &FilterNode::Cond(Condition::new("age", Op::Gte, [Value::Int(3)]))
        );
    }

    #[test]
    fn and_binds_tighter_than_or() {
        let filter = FilterExpr::from_aip("a = 1 OR b = 2 AND c = 3").expect("parses");
        match filter.node() {
            FilterNode::Any(children) => {
                assert_eq!(children.len(), 2, "OR splits top level");
                assert!(
                    matches!(&children[1], FilterNode::All(_)),
                    "the second OR arm is the AND group"
                );
            }
            other => panic!("unexpected tree: {other:?}"),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        let filter = FilterExpr::from_aip("(a = 1 OR b = 2) AND c = 3").expect("parses");
        match filter.node() {
            FilterNode::All(children) => {
                assert!(matches!(&children[0], FilterNode::Any(_)));
            }
            other => panic!("unexpected tree: {other:?}"),
        }
    }

    #[test]
    fn juxtaposition_is_implicit_and() {
        let filter = FilterExpr::from_aip("a = 1 b = 2").expect("parses");
        assert!(matches!(filter.node(), FilterNode::All(children) if children.len() == 2));
    }

    #[test]
    fn in_and_not_in_lists() {
        let filter = FilterExpr::from_aip("name IN (\"a\", 'b')").expect("parses");
        assert!(matches!(
            filter.node(),
            FilterNode::Cond(c) if c.op == Op::In && c.values.len() == 2
        ));
        let filter = FilterExpr::from_aip("age NOT IN (1, 2)").expect("parses");
        assert!(matches!(
            filter.node(),
            FilterNode::Cond(c) if c.op == Op::NotIn && c.values.len() == 2
        ));
    }

    #[test]
    fn keywords_are_case_insensitive() {
        let filter = FilterExpr::from_aip("a = 1 and b = 2 or c = 3").expect("parses");
        assert!(matches!(filter.node(), FilterNode::Any(_)));
    }

    #[test]
    fn values_have_kinds() {
        let filter =
            FilterExpr::from_aip("a = \"hi there\" b = 3 c = 2.5 d = true e = null f = bare")
                .expect("parses");
        match filter.node() {
            FilterNode::All(children) => {
                let expected = [
                    Value::Text("hi there".into()),
                    Value::Int(3),
                    Value::Real(2.5),
                    Value::Bool(true),
                    Value::Null,
                    Value::Text("bare".into()),
                ];
                for (child, want) in children.iter().zip(expected) {
                    assert!(
                        matches!(child, FilterNode::Cond(c) if c.values[0] == want),
                        "value mismatch"
                    );
                }
            }
            other => panic!("unexpected tree: {other:?}"),
        }
    }

    #[test]
    fn not_negates_the_comparison() {
        let filter = FilterExpr::from_aip("NOT age > 3").expect("parses");
        assert!(matches!(
            filter.node(),
            FilterNode::Cond(c) if c.op == Op::Lte
        ));
        let filter = FilterExpr::from_aip("NOT age IN (1, 2)").expect("parses");
        assert!(matches!(
            filter.node(),
            FilterNode::Cond(c) if c.op == Op::NotIn
        ));
    }

    #[test]
    fn not_flips_groups() {
        let filter = FilterExpr::from_aip("NOT (a = 1 OR b = 2)").expect("parses");
        assert!(matches!(filter.node(), FilterNode::All(_)));
    }

    #[test]
    fn syntax_errors_carry_positions() {
        let err = FilterExpr::from_aip("a = ").expect_err("must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
        let err = FilterExpr::from_aip("a = 1 )").expect_err("must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }
}

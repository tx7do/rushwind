//! The filter expression tree.
//!
//! The `FilterExpr`\/`FilterCondition` vocabulary: leaf
//! conditions pair a field with one operator and its operand values, and
//! leaves compose through `AND`/`OR` groups nesting to arbitrary depth.
//! Engines translate the tree into their native predicate language — the
//! conformance suite pins that translation against a Rust-side reference
//! evaluator.

use crate::error::StorageError;
use crate::schema::{ColumnKind, Schema};
use crate::value::Value;
use std::cmp::Ordering;

/// A comparison operator.
///
/// String-pattern operators (`Like`, `Ilike`, `Contains`, `StartsWith`,
/// `EndsWith`) use SQL semantics: `%` matches any sequence and `_` matches
/// exactly one character; the `I`-prefixed forms are case-insensitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Equal.
    Eq,
    /// Not equal.
    NotEq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Membership in a set (one or more operands).
    In,
    /// Exclusion from a set (one or more operands).
    NotIn,
    /// Case-sensitive SQL `LIKE` pattern (one operand).
    Like,
    /// Negated case-sensitive `LIKE` (one operand).
    NotLike,
    /// Case-insensitive `LIKE` (one operand).
    Ilike,
    /// Case-insensitive equality (one operand) — `Eq` under Unicode
    /// case folding, the same folding the `I`-prefixed pattern family
    /// uses.
    Iexact,
    /// The field is `NULL` (no operands).
    IsNull,
    /// The field is not `NULL` (no operands).
    IsNotNull,
    /// Inclusive range (exactly two operands: low, high).
    Between,
    /// Outside the inclusive range (exactly two operands: low, high).
    NotBetween,
    /// Substring match (one operand).
    Contains,
    /// Prefix match (one operand).
    StartsWith,
    /// Suffix match (one operand).
    EndsWith,
}

/// The operand-count requirement of an [`Op`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arity {
    /// No operands.
    Zero,
    /// Exactly one operand.
    One,
    /// Exactly two operands.
    Two,
    /// At least one operand.
    Many,
}

impl Op {
    /// The operand-count requirement of this operator.
    pub fn arity(self) -> Arity {
        match self {
            Op::IsNull | Op::IsNotNull => Arity::Zero,
            Op::Eq
            | Op::NotEq
            | Op::Gt
            | Op::Gte
            | Op::Lt
            | Op::Lte
            | Op::Like
            | Op::NotLike
            | Op::Ilike
            | Op::Iexact
            | Op::Contains
            | Op::StartsWith
            | Op::EndsWith => Arity::One,
            Op::Between | Op::NotBetween => Arity::Two,
            Op::In | Op::NotIn => Arity::Many,
        }
    }

    /// Returns `true` when `len` operands satisfy this operator's arity.
    pub fn arity_ok(self, len: usize) -> bool {
        match self.arity() {
            Arity::Zero => len == 0,
            Arity::One => len == 1,
            Arity::Two => len == 2,
            Arity::Many => len >= 1,
        }
    }

    /// Returns `true` when the operator only applies to text columns.
    pub fn is_text_only(self) -> bool {
        matches!(
            self,
            Op::Like
                | Op::NotLike
                | Op::Ilike
                | Op::Iexact
                | Op::Contains
                | Op::StartsWith
                | Op::EndsWith
        )
    }
}

/// One leaf condition: `field op values`.
#[derive(Clone, Debug, PartialEq)]
pub struct Condition {
    /// The field being tested (a column name of the schema).
    pub field: String,
    /// The operator.
    pub op: Op,
    /// The operands.
    pub values: Vec<Value>,
}

impl Condition {
    /// Builds a leaf condition.
    pub fn new(field: impl Into<String>, op: Op, values: impl IntoIterator<Item = Value>) -> Self {
        Self {
            field: field.into(),
            op,
            values: values.into_iter().collect(),
        }
    }
}

/// A node of the filter tree: a leaf condition or an `AND`/`OR` group.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterNode {
    /// A leaf condition.
    Cond(Condition),
    /// True when every child holds.
    All(Vec<FilterNode>),
    /// True when any child holds.
    Any(Vec<FilterNode>),
}

/// A validated-at-the-edge query predicate.
///
/// The default value is the always-true predicate (no filtering).
#[derive(Clone, Debug, PartialEq)]
pub struct FilterExpr(FilterNode);

impl Default for FilterExpr {
    fn default() -> Self {
        Self::matches_all()
    }
}

impl From<FilterNode> for FilterExpr {
    fn from(node: FilterNode) -> Self {
        Self(node)
    }
}

impl FilterExpr {
    /// The always-true predicate.
    pub fn matches_all() -> Self {
        Self(FilterNode::All(Vec::new()))
    }

    /// Wraps a single leaf condition.
    pub fn cond(field: impl Into<String>, op: Op, values: impl IntoIterator<Item = Value>) -> Self {
        Self(FilterNode::Cond(Condition::new(field, op, values)))
    }

    /// Conjoins several expressions.
    pub fn all(exprs: impl IntoIterator<Item = FilterExpr>) -> Self {
        Self(FilterNode::All(exprs.into_iter().map(|e| e.0).collect()))
    }

    /// Disjoins several expressions.
    pub fn any(exprs: impl IntoIterator<Item = FilterExpr>) -> Self {
        Self(FilterNode::Any(exprs.into_iter().map(|e| e.0).collect()))
    }

    /// Returns `true` when the predicate imposes no constraint.
    pub fn is_matches_all(&self) -> bool {
        matches!(&self.0, FilterNode::All(children) if children.is_empty())
    }

    /// The root node, for engines walking the tree.
    pub fn node(&self) -> &FilterNode {
        &self.0
    }

    /// Evaluates the predicate against a row — the contract's reference
    /// semantics (the same evaluator the in-memory engine and the conformance
    /// suite pin). Engines that cannot push a filter down (Cassandra's
    /// query model, InfluxDB's schema) fetch candidates and evaluate this.
    /// A field absent from the row is NULL, exactly as in SQL.
    pub fn matches(&self, row: &crate::record::Record) -> bool {
        Self::eval_node(self.node(), row)
    }

    fn eval_node(node: &FilterNode, row: &crate::record::Record) -> bool {
        match node {
            FilterNode::All(children) => children.iter().all(|c| Self::eval_node(c, row)),
            FilterNode::Any(children) => children.iter().any(|c| Self::eval_node(c, row)),
            FilterNode::Cond(condition) => Self::eval_cond(condition, row),
        }
    }

    fn eval_cond(condition: &Condition, row: &crate::record::Record) -> bool {
        let value = row.get(&condition.field).cloned().unwrap_or(Value::Null);
        let first = || condition.values.first().cloned().unwrap_or(Value::Null);
        let text_arg = |fmt: fn(&str) -> String| match first().as_str() {
            Some(s) => Value::Text(fmt(s)),
            None => Value::Null,
        };
        let cmp = |other: &Value| value.compare(other);
        match condition.op {
            Op::Eq => cmp(&first()) == Ordering::Equal,
            Op::NotEq => cmp(&first()) != Ordering::Equal,
            Op::Gt => cmp(&first()) == Ordering::Greater,
            Op::Gte => cmp(&first()) != Ordering::Less,
            Op::Lt => cmp(&first()) == Ordering::Less,
            Op::Lte => cmp(&first()) != Ordering::Greater,
            Op::In => condition.values.iter().any(|v| cmp(v) == Ordering::Equal),
            Op::NotIn => !condition.values.iter().any(|v| cmp(v) == Ordering::Equal),
            Op::Like => like(&value, &first(), false),
            Op::NotLike => !like(&value, &first(), false),
            Op::Ilike => like(&value, &first(), true),
            Op::Iexact => match (value.as_str(), first().as_str()) {
                (Some(a), Some(b)) => a.to_lowercase() == b.to_lowercase(),
                _ => false,
            },
            Op::IsNull => value.is_null(),
            Op::IsNotNull => !value.is_null(),
            Op::Between => {
                cmp(&first()) != Ordering::Less
                    && cmp(&condition.values.get(1).cloned().unwrap_or(Value::Null))
                        != Ordering::Greater
            }
            Op::NotBetween => {
                cmp(&first()) == Ordering::Less
                    || cmp(&condition.values.get(1).cloned().unwrap_or(Value::Null))
                        == Ordering::Greater
            }
            Op::Contains => like(&value, &text_arg(|s| format!("%{s}%")), false),
            Op::StartsWith => like(&value, &text_arg(|s| format!("{s}%")), false),
            Op::EndsWith => like(&value, &text_arg(|s| format!("%{s}")), false),
        }
    }

    /// Checks the expression against a schema: every leaf must name a
    /// declared column, honor its operator's arity, respect kind
    /// compatibility (pattern operators are text-only), and carry operand
    /// kinds the column accepts.
    pub fn validate(&self, schema: &Schema) -> Result<(), StorageError> {
        fn walk(node: &FilterNode, schema: &Schema) -> Result<(), StorageError> {
            match node {
                FilterNode::All(children) | FilterNode::Any(children) => {
                    children.iter().try_for_each(|c| walk(c, schema))
                }
                FilterNode::Cond(cond) => {
                    let column = schema.column(&cond.field).ok_or_else(|| {
                        StorageError::InvalidQuery(format!(
                            "unknown column {:?} in table {:?}",
                            cond.field, schema.table
                        ))
                    })?;
                    if !cond.op.arity_ok(cond.values.len()) {
                        return Err(StorageError::InvalidQuery(format!(
                            "operator {:#?} on {:?} expects {:?} operands, got {}",
                            cond.op,
                            cond.field,
                            cond.op.arity(),
                            cond.values.len()
                        )));
                    }
                    if cond.op.is_text_only() && column.kind != ColumnKind::Text {
                        return Err(StorageError::InvalidQuery(format!(
                            "operator {:#?} applies only to text columns, but {:?} is not",
                            cond.op, cond.field
                        )));
                    }
                    for value in &cond.values {
                        if !column.kind.accepts(value.type_name()) {
                            return Err(StorageError::InvalidQuery(format!(
                                "column {:?} of kind {:#?} does not accept a {} operand",
                                cond.field,
                                column.kind,
                                value.type_name()
                            )));
                        }
                    }
                    Ok(())
                }
            }
        }
        walk(&self.0, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::builder("t", "id")
            .column("name", ColumnKind::Text)
            .column("age", ColumnKind::Int)
            .build()
            .expect("valid schema")
    }

    #[test]
    fn default_matches_all() {
        assert!(FilterExpr::default().is_matches_all());
    }

    #[test]
    fn unknown_column_is_rejected() {
        let err = FilterExpr::cond("nope", Op::Eq, [Value::Int(1)])
            .validate(&schema())
            .expect_err("unknown column must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }

    #[test]
    fn arity_is_validated() {
        let err = FilterExpr::cond("age", Op::Between, [Value::Int(1)])
            .validate(&schema())
            .expect_err("between needs two operands");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }

    #[test]
    fn pattern_ops_are_text_only() {
        let err = FilterExpr::cond("age", Op::Contains, [Value::Int(1)])
            .validate(&schema())
            .expect_err("contains on int must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }

    #[test]
    fn kind_mismatch_is_rejected() {
        let err = FilterExpr::cond("age", Op::Eq, [Value::Text("x".to_owned())])
            .validate(&schema())
            .expect_err("text operand on int column must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }

    #[test]
    fn nested_groups_validate_recursively() {
        let expr = FilterExpr::any([
            FilterExpr::cond("name", Op::Like, [Value::Text("b%".to_owned())]),
            FilterExpr::all([
                FilterExpr::cond("age", Op::Gte, [Value::Int(1)]),
                FilterExpr::cond("age", Op::Lt, [Value::Int(9)]),
            ]),
        ]);
        expr.validate(&schema()).expect("valid expression");
    }
}

/// SQL `LIKE` semantics: `%` any sequence, `_` one character, no escapes;
/// `fold` switches to case-insensitive matching.
fn like(value: &Value, pattern: &Value, fold: bool) -> bool {
    let (Some(text), Some(pattern)) = (value.as_str(), pattern.as_str()) else {
        return false;
    };
    let norm = |s: &str| {
        if fold {
            s.to_lowercase().chars().collect::<Vec<char>>()
        } else {
            s.chars().collect()
        }
    };
    let (t, p) = (norm(text), norm(pattern));
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod matches_tests {
    use super::*;
    use crate::record::Record;

    #[test]
    fn evaluator_covers_the_operator_table() {
        let row = Record::new()
            .set("name", "Bravo")
            .set("age", 10i64)
            .set("score", Value::Null);

        assert!(FilterExpr::cond("age", Op::Gte, [Value::Int(10)]).matches(&row));
        assert!(!FilterExpr::cond("age", Op::Gt, [Value::Int(10)]).matches(&row));
        assert!(FilterExpr::cond("name", Op::Eq, [Value::Text("Bravo".into())]).matches(&row));
        assert!(FilterExpr::cond("name", Op::Contains, [Value::Text("avo".into())]).matches(&row));
        assert!(FilterExpr::cond("name", Op::Ilike, [Value::Text("%br%".into())]).matches(&row));
        assert!(FilterExpr::cond("score", Op::IsNull, []).matches(&row));
        assert!(!FilterExpr::cond("score", Op::IsNotNull, []).matches(&row));
        assert!(FilterExpr::cond("absent", Op::IsNull, []).matches(&row));
        assert!(FilterExpr::cond("age", Op::In, [Value::Int(1), Value::Int(10)]).matches(&row));
        assert!(
            FilterExpr::cond("age", Op::Between, [Value::Int(5), Value::Int(15)]).matches(&row)
        );

        let group = FilterExpr::any([
            FilterExpr::cond("age", Op::Lt, [Value::Int(5)]),
            FilterExpr::all([
                FilterExpr::cond("age", Op::Gte, [Value::Int(10)]),
                FilterExpr::cond("name", Op::StartsWith, [Value::Text("Br".into())]),
            ]),
        ]);
        assert!(group.matches(&row));
    }
}

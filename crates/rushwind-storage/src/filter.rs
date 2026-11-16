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
    /// The column's JSON contains the operand JSON — MySQL's
    /// `JSON_CONTAINS` semantics (one operand, kind [`Value::Json`]):
    /// equality, or every operand key present with a contained value
    /// (objects), or some element containing it (arrays).
    JsonContains,
    /// The column's array contains the operand as an element (one
    /// operand, a scalar).
    ArrayContains,
    /// The text matches the operand's regular expression —
    /// unanchored contains semantics over the contract's own
    /// documented subset (the dependency-free engine in `regex`;
    /// one Text operand, case-sensitive).
    Regexp,
    /// The Unicode case-folded face of [`Op::Regexp`] (the same
    /// folding `Iexact` uses).
    Iregexp,
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
            | Op::EndsWith
            | Op::JsonContains
            | Op::ArrayContains
            | Op::Regexp
            | Op::Iregexp => Arity::One,
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
                | Op::Regexp
                | Op::Iregexp
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
    /// The dotted path inside the column's JSON document (`a.b.0` —
    /// bareword keys and numeric array indexes). `None` targets the
    /// column itself; a path targets the value the path extracts (a
    /// missing path reads NULL, exactly as an absent column does).
    pub json_path: Option<String>,
}

impl Condition {
    /// Builds a leaf condition.
    pub fn new(field: impl Into<String>, op: Op, values: impl IntoIterator<Item = Value>) -> Self {
        Self {
            field: field.into(),
            op,
            values: values.into_iter().collect(),
            json_path: None,
        }
    }

    /// Targets the dotted path inside the column's JSON document
    /// instead of the column itself.
    pub fn with_json_path(mut self, path: impl Into<String>) -> Self {
        self.json_path = Some(path.into());
        self
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
        let mut value = row.get(&condition.field).cloned().unwrap_or(Value::Null);
        if let Some(path) = &condition.json_path {
            value = extract_json_path(&value, path).unwrap_or(Value::Null);
        }
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
            Op::Regexp | Op::Iregexp => {
                let owned = first();
                let (Some(text), Some(pattern)) = (value.as_str(), owned.as_str()) else {
                    return false;
                };
                // The pattern compiled at validate; the belt-and-braces
                // face reads a failed parse as a miss, never a panic.
                match crate::regex::parse(pattern) {
                    Ok(ast) => crate::regex::is_match(&ast, text, condition.op == Op::Iregexp),
                    Err(_) => false,
                }
            }
            Op::JsonContains => json_contains(&value, &first()),
            Op::ArrayContains => {
                let Some(items) = value.as_array() else {
                    return false;
                };
                let Some(operand) = condition.values.first() else {
                    return false;
                };
                items
                    .iter()
                    .any(|item| item.compare(operand) == Ordering::Equal)
            }
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
                    // The pattern family applies to text columns — or
                    // to whatever a json_path extracts (the extracted
                    // value's kind lives in the document, not the
                    // schema; the operand kind check below keeps the
                    // operands textual).
                    if cond.op.is_text_only()
                        && column.kind != ColumnKind::Text
                        && cond.json_path.is_none()
                    {
                        return Err(StorageError::InvalidQuery(format!(
                            "operator {:#?} applies only to text columns, but {:?} is not",
                            cond.op, cond.field
                        )));
                    }
                    if let Some(path) = &cond.json_path {
                        if column.kind != ColumnKind::Json {
                            return Err(StorageError::InvalidQuery(format!(
                                "json_path targets column {:?} of kind {:#?} — only json columns carry documents",
                                cond.field, column.kind
                            )));
                        }
                        if path_steps(path).is_none() {
                            return Err(StorageError::InvalidQuery(format!(
                                "json_path {path:?} is not a dotted path of bareword keys and array indexes"
                            )));
                        }
                    }
                    if cond.json_path.is_some()
                        && matches!(cond.op, Op::JsonContains | Op::ArrayContains)
                    {
                        return Err(StorageError::InvalidQuery(
                            "JSON_CONTAINS/ARRAY_CONTAINS run on the whole document — no json_path"
                                .into(),
                        ));
                    }
                    if matches!(cond.op, Op::JsonContains | Op::ArrayContains)
                        && column.kind != ColumnKind::Json
                    {
                        return Err(StorageError::InvalidQuery(format!(
                            "operator {:#?} applies only to json columns, but {:?} is not",
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
                        // The regex family compiles its pattern at the
                        // edge — a malformed pattern refuses with its
                        // parse position, never at row evaluation.
                        if matches!(cond.op, Op::Regexp | Op::Iregexp) {
                            let Some(pattern) = cond.values.first().and_then(Value::as_str) else {
                                return Err(StorageError::InvalidQuery(format!(
                                    "operator {:#?} on {:?} takes one text operand (the pattern)",
                                    cond.op, cond.field
                                )));
                            };
                            if let Err(error) = crate::regex::parse(pattern) {
                                return Err(StorageError::InvalidQuery(format!(
                                    "regexp pattern at byte {}: {}",
                                    error.position, error.message
                                )));
                            }
                        }
                        // The JSON containment pair pin their operand
                        // kinds: JSON_CONTAINS takes a JSON document,
                        // ARRAY_CONTAINS a scalar element.
                        if cond.op == Op::JsonContains && value.type_name() != "json" {
                            return Err(StorageError::InvalidQuery(format!(
                                "operator JSON_CONTAINS on {:?} takes a json operand, got {}",
                                cond.field,
                                value.type_name()
                            )));
                        }
                        if cond.op == Op::ArrayContains
                            && matches!(value.type_name(), "null" | "json" | "array")
                        {
                            return Err(StorageError::InvalidQuery(format!(
                                "operator ARRAY_CONTAINS on {:?} takes a scalar operand, got {}",
                                cond.field,
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
/// The contract's JSON containment reference semantics — MySQL's
/// `JSON_CONTAINS`: the operand is contained when it equals the
/// column's JSON, or every operand key is present with a contained
/// value (objects), or some element contains it (arrays). The column
/// carries a [`Value::Json`] document (parsed by the contract's own
/// dependency-free reader) or a [`Value::Array`]; the operand is a
/// [`Value::Json`].
fn json_contains(column: &Value, operand: &Value) -> bool {
    let Some(needle_text) = operand.as_json() else {
        return false;
    };
    let Some(needle) = crate::json::parse(needle_text) else {
        return false;
    };
    let hay = match column {
        Value::Json(text) => crate::json::parse(text),
        Value::Array(items) => items
            .iter()
            .map(json_of_value)
            .collect::<Option<Vec<crate::json::Json>>>()
            .map(crate::json::Json::Arr),
        _ => None,
    };
    let Some(hay) = hay else {
        return false;
    };
    crate::json::contains(&hay, &needle)
}

/// Parses a dotted JSON path: bareword keys and numeric array
/// indexes (`a.b.0`); `None` for anything else.
fn path_steps(path: &str) -> Option<Vec<Step>> {
    if path.is_empty() {
        return None;
    }
    let mut steps = Vec::new();
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        if let Ok(index) = segment.parse::<usize>() {
            steps.push(Step::Index(index));
        } else if segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            steps.push(Step::Key(segment.to_string()));
        } else {
            return None;
        }
    }
    Some(steps)
}

/// One path step.
enum Step {
    Key(String),
    Index(usize),
}

/// Extracts the value a JSON path names: objects walk bareword keys
/// (the canonical text parses through the contract's own reader),
/// arrays walk numeric indexes; a missing step is `None` — the
/// caller reads it as NULL, exactly as an absent column.
fn extract_json_path(value: &Value, path: &str) -> Option<Value> {
    let steps = path_steps(path)?;
    let mut current = value.clone();
    for step in steps {
        current = match (step, current) {
            (Step::Key(key), Value::Json(text)) => {
                let crate::json::Json::Obj(entries) = crate::json::parse(&text)? else {
                    return None;
                };
                let (_, found) = entries.iter().find(|(k, _)| k == &key)?;
                json_model_to_value(found)
            }
            (Step::Index(index), Value::Array(items)) => items.into_iter().nth(index)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Converts one parsed JSON model value back into the contract's
/// kinds (objects ride their canonical text; the model's encoder
/// renders it).
fn json_model_to_value(value: &crate::json::Json) -> Value {
    match value {
        crate::json::Json::Null => Value::Null,
        crate::json::Json::Bool(b) => Value::Bool(*b),
        crate::json::Json::Num(n) => Value::Real(*n),
        crate::json::Json::Str(s) => Value::Text(s.clone()),
        crate::json::Json::Arr(items) => {
            Value::Array(items.iter().map(json_model_to_value).collect())
        }
        crate::json::Json::Obj(_) => Value::Json(crate::json::encode_model(value)),
    }
}

/// Converts a contract value into the JSON reader's model. `Value::Json`
/// documents embed by parse; scalars map naturally (numbers onto f64 —
/// the containment number model).
fn json_of_value(value: &Value) -> Option<crate::json::Json> {
    match value {
        Value::Null => Some(crate::json::Json::Null),
        Value::Bool(b) => Some(crate::json::Json::Bool(*b)),
        Value::Int(i) => Some(crate::json::Json::Num(*i as f64)),
        Value::Real(f) => Some(crate::json::Json::Num(*f)),
        Value::Text(s) => Some(crate::json::Json::Str(s.clone())),
        Value::Array(items) => Some(crate::json::Json::Arr(
            items
                .iter()
                .map(json_of_value)
                .collect::<Option<Vec<crate::json::Json>>>()?,
        )),
        Value::Json(text) => crate::json::parse(text),
    }
}

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

//! Dynamically typed scalar values shared across the storage contract.

use std::cmp::Ordering;

/// A dynamically typed scalar value.
///
/// The contract deals in rows that are not known at compile time (filters,
/// cursors, primary keys all arrive as data), so scalar values cross the
/// trait boundary in this closed enum instead of generics. Engines map it
/// onto their native parameter types.
///
/// `PartialEq` is the *exact* equality: `Value::Int(1)` does not equal
/// `Value::Real(1.0)`. Range comparisons use [`Value::compare`], which
/// treats the two numeric variants uniformly.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Absence of a value (SQL `NULL`).
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed 64-bit integer.
    Int(i64),
    /// A 64-bit float.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// An ordered array of values (a JSON array column's payload).
    Array(Vec<Value>),
    /// A JSON document, carried as its canonical minified text (a JSON
    /// object column's payload). Equality is the canonical text's —
    /// callers compare equal-shaped documents by their minified forms.
    Json(String),
}

impl Value {
    /// Returns `true` when the value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The value kind's name, used in diagnostics.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Real(_) => "real",
            Value::Text(_) => "text",
            Value::Array(_) => "array",
            Value::Json(_) => "json",
        }
    }

    /// Total ordering used by range filters (`Gt`, `Between`, …) and
    /// in-memory engines.
    ///
    /// Values of different kinds order by kind rank — `Null` < `Bool` <
    /// numeric (`Int`/`Real`, compared as numbers) < `Text` < `Array` <
    /// `Json` — so the order is always total even across kinds. Inside a
    /// kind the natural order applies (arrays lexicographically by
    /// element; JSON by its canonical text).
    pub fn compare(&self, other: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Real(_) => 2,
                Value::Text(_) => 3,
                Value::Array(_) => 4,
                Value::Json(_) => 5,
            }
        }
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Int(a), Value::Real(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Real(a), Value::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Array(a), Value::Array(b)) => {
                for (x, y) in a.iter().zip(b.iter()) {
                    let ordering = x.compare(y);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                a.len().cmp(&b.len())
            }
            (Value::Json(a), Value::Json(b)) => a.cmp(b),
            (a, b) => rank(a).cmp(&rank(b)),
        }
    }

    /// Extracts the integer payload, if this is [`Value::Int`].
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Extracts the text payload, if this is [`Value::Text`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Extracts the float payload, if this is [`Value::Real`].
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            _ => None,
        }
    }

    /// Extracts the boolean payload, if this is [`Value::Bool`].
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Extracts the element slice, if this is [`Value::Array`].
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }

    /// Extracts the canonical JSON text, if this is [`Value::Json`].
    pub fn as_json(&self) -> Option<&str> {
        match self {
            Value::Json(text) => Some(text),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

macro_rules! impl_int_from {
    ($($ty:ty),+ $(,)?) => {
        $(impl From<$ty> for Value {
            fn from(v: $ty) -> Self {
                Value::Int(i64::from(v))
            }
        })+
    };
}

impl_int_from!(i8, i16, i32, u8, u16, u32);

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Real(f64::from(v))
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl From<&[Value]> for Value {
    fn from(v: &[Value]) -> Self {
        Value::Array(v.to_vec())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_kinds_compare_uniformly() {
        assert_eq!(Value::Int(1).compare(&Value::Real(1.0)), Ordering::Equal);
        assert_eq!(Value::Real(0.5).compare(&Value::Int(1)), Ordering::Less);
        assert_eq!(Value::Int(2).compare(&Value::Real(1.5)), Ordering::Greater);
    }

    #[test]
    fn kinds_rank_total_across_families() {
        assert_eq!(Value::Null.compare(&Value::Bool(true)), Ordering::Less);
        assert_eq!(Value::Bool(false).compare(&Value::Int(0)), Ordering::Less);
        assert_eq!(
            Value::Int(i64::MAX).compare(&Value::Text("a".to_owned())),
            Ordering::Less
        );
        assert_eq!(
            Value::Text("z".to_owned()).compare(&Value::Null),
            Ordering::Greater
        );
    }

    #[test]
    fn option_maps_to_null() {
        assert_eq!(Value::from(None::<i64>), Value::Null);
        assert_eq!(Value::from(Some(7i32)), Value::Int(7));
    }
}

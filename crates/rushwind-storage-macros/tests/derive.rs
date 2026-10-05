//! The mapper pair: ToRecord / FromRecord roundtrips, kind mismatches,
//! the Option<_> ↔ NULL mapping, and the extended type matrix — widened
//! integers, f32, string enums via `as_text`, custom conversions via
//! `with`, and column renames.

use std::str::FromStr;

use rushwind_storage::{FromRecord, Record, StorageError, ToRecord, Value};
use rushwind_storage_macros::{FromRecord, ToRecord};

#[derive(Debug, PartialEq, ToRecord, FromRecord)]
struct User {
    id: i64,
    name: String,
    score: Option<f64>,
    active: bool,
}

#[test]
fn roundtrips_through_the_record_view() {
    let user = User {
        id: 7,
        name: "bolt".into(),
        score: Some(2.5),
        active: true,
    };
    let record = user.to_record();
    assert_eq!(record.get("id"), Some(&Value::Int(7)));
    assert_eq!(record.get("name").and_then(Value::as_str), Some("bolt"));
    assert_eq!(record.get("score"), Some(&Value::Real(2.5)));
    assert_eq!(record.get("active"), Some(&Value::Bool(true)));

    let back = User::from_record(&record).expect("reads back");
    assert_eq!(back, user);
}

#[test]
fn none_maps_to_null_and_back() {
    let user = User {
        id: 1,
        name: "anon".into(),
        score: None,
        active: false,
    };
    let record = user.to_record();
    assert_eq!(record.get("score"), Some(&Value::Null));

    let back = User::from_record(&record).expect("reads back");
    assert_eq!(back.score, None);
    // A missing field behaves like NULL for optional fields.
    let mut sparse = Record::new();
    sparse.insert("id", 1i64);
    sparse.insert("name", "anon");
    sparse.insert("active", false);
    let back = User::from_record(&sparse).expect("reads back");
    assert_eq!(back.score, None);
}

#[test]
fn missing_required_fields_are_invalid_query() {
    let record = Record::new().set("name", "half");
    let err = User::from_record(&record).expect_err("id is missing");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("id")));
}

#[test]
fn kind_mismatches_are_invalid_query() {
    let record = Record::new()
        .set("id", 1i64)
        .set("name", 42i64)
        .set("active", true);
    let err = User::from_record(&record).expect_err("name carries an int");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("name")));
}

// ---------------------------------------------------------------------
// Widened integers and f32
// ---------------------------------------------------------------------

#[derive(Debug, PartialEq, ToRecord, FromRecord)]
struct Widths {
    tiny: i8,
    small: i16,
    medium: i32,
    unsigned: u16,
    ratio: f32,
    maybe_count: Option<u8>,
}

#[test]
fn widened_integers_and_f32_roundtrip() {
    let widths = Widths {
        tiny: -3,
        small: 300,
        medium: 70_000,
        unsigned: 65_000,
        ratio: 0.5,
        maybe_count: Some(9),
    };
    let record = widths.to_record();
    assert_eq!(record.get("tiny"), Some(&Value::Int(-3)));
    assert_eq!(record.get("unsigned"), Some(&Value::Int(65_000)));
    assert_eq!(record.get("ratio"), Some(&Value::Real(0.5)));
    assert_eq!(record.get("maybe_count"), Some(&Value::Int(9)));

    let back = Widths::from_record(&record).expect("reads back");
    assert_eq!(back, widths);
}

#[test]
fn widened_reads_cast_narrow_again() {
    let mut record = Record::new();
    record.insert("tiny", -3i64);
    record.insert("small", 300i64);
    record.insert("medium", 70_000i64);
    record.insert("unsigned", 65_000i64);
    record.insert("ratio", 0.5f64);
    let back = Widths::from_record(&record).expect("reads back");
    assert_eq!(back.tiny, -3i8);
    assert_eq!(back.ratio, 0.5f32);
}

// ---------------------------------------------------------------------
// String enums via `as_text`
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Draft,
    Published,
}

impl Status {
    const fn as_str(&self) -> &'static str {
        match self {
            Status::Draft => "draft",
            Status::Published => "published",
        }
    }
}

impl FromStr for Status {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "draft" => Ok(Status::Draft),
            "published" => Ok(Status::Published),
            other => Err(format!("unknown status {other:?}")),
        }
    }
}

#[derive(Debug, PartialEq, ToRecord, FromRecord)]
struct Article {
    title: String,
    #[record(as_text)]
    status: Status,
    #[record(as_text)]
    previous: Option<Status>,
}

#[test]
fn string_enums_ride_text_and_roundtrip() {
    let article = Article {
        title: "hello".into(),
        status: Status::Published,
        previous: Some(Status::Draft),
    };
    let record = article.to_record();
    assert_eq!(
        record.get("status").and_then(Value::as_str),
        Some("published")
    );
    assert_eq!(
        record.get("previous").and_then(Value::as_str),
        Some("draft")
    );

    let back = Article::from_record(&record).expect("reads back");
    assert_eq!(back, article);
}

#[test]
fn enum_parse_failures_name_the_field_and_the_text() {
    let record = Article {
        title: "t".into(),
        status: Status::Draft,
        previous: None,
    }
    .to_record();
    let mut record = record;
    record.insert("status", "archived");
    let err = Article::from_record(&record).expect_err("archived is not a Status");
    assert!(
        matches!(
            err,
            StorageError::InvalidQuery(ref m) if m.contains("status") && m.contains("archived")
        ),
        "{err}"
    );

    record.insert("status", 7i64);
    let err = Article::from_record(&record).expect_err("an int is not text");
    assert!(
        matches!(err, StorageError::InvalidQuery(ref m) if m.contains("status")),
        "{err}"
    );
}

// ---------------------------------------------------------------------
// The `with` escape hatch
// ---------------------------------------------------------------------

/// A JSON-ish column the user's crate owns — deliberately hand-rolled
/// so the macro crate itself stays dependency-free.
#[derive(Debug, Clone, PartialEq)]
struct Tags(Vec<String>);

mod tags {
    use crate::Tags;
    use rushwind_storage::{StorageError, Value};

    pub fn to_value(tags: &Tags) -> Value {
        Value::Text(tags.0.join(","))
    }

    pub fn from_value(value: &Value) -> Result<Tags, StorageError> {
        match value {
            Value::Text(s) => Ok(Tags(
                s.split(',')
                    .filter(|tag| !tag.is_empty())
                    .map(str::to_owned)
                    .collect(),
            )),
            other => Err(StorageError::InvalidQuery(format!(
                "tags expect text, got {}",
                other.type_name()
            ))),
        }
    }
}

/// Unix-seconds timestamps are the admin's time carrier: i64 in, i64 out.
mod unix_seconds {
    use rushwind_storage::{StorageError, Value};

    pub fn to_value(secs: &i64) -> Value {
        Value::Int(*secs)
    }

    pub fn from_value(value: &Value) -> Result<i64, StorageError> {
        value.as_i64().ok_or_else(|| {
            StorageError::InvalidQuery(format!(
                "unix seconds expect int, got {}",
                value.type_name()
            ))
        })
    }
}

#[derive(Debug, PartialEq, ToRecord, FromRecord)]
struct Post {
    #[record(with = "tags")]
    tags: Tags,
    #[record(with = "tags")]
    hidden_tags: Option<Tags>,
    #[record(with = "unix_seconds")]
    created_at: i64,
    #[record(rename = "updated_seconds")]
    updated_at: i64,
}

#[test]
fn custom_conversions_roundtrip_through_their_module() {
    let post = Post {
        tags: Tags(vec!["rust".into(), "storage".into()]),
        hidden_tags: Some(Tags(vec!["quiet".into()])),
        created_at: 1_900_000_000,
        updated_at: 1_900_000_100,
    };
    let record = post.to_record();
    assert_eq!(
        record.get("tags").and_then(Value::as_str),
        Some("rust,storage")
    );
    assert_eq!(
        record.get("hidden_tags").and_then(Value::as_str),
        Some("quiet")
    );
    assert_eq!(record.get("created_at"), Some(&Value::Int(1_900_000_000)));
    // The rename decides the column, not the Rust field name.
    assert_eq!(
        record.get("updated_seconds"),
        Some(&Value::Int(1_900_000_100))
    );
    assert!(record.get("updated_at").is_none());

    let back = Post::from_record(&record).expect("reads back");
    assert_eq!(back, post);
}

#[test]
fn custom_conversion_errors_surface_as_invalid_query() {
    let mut record = Post {
        tags: Tags(vec![]),
        hidden_tags: None,
        created_at: 1,
        updated_at: 2,
    }
    .to_record();
    record.insert("created_at", "not-a-number");
    let err = Post::from_record(&record).expect_err("text is not unix seconds");
    // The `with` module owns its message — it cannot know the field
    // name, so the module's own words are the contract here.
    assert!(
        matches!(
            err,
            StorageError::InvalidQuery(ref m) if m.contains("unix seconds") && m.contains("text")
        ),
        "{err}"
    );
}

#[test]
fn renamed_columns_drive_the_read_face_too() {
    let mut record = Record::new();
    record.insert("tags", "solo");
    record.insert("created_at", 5i64);
    record.insert("updated_seconds", 6i64);
    let post = Post::from_record(&record).expect("reads back");
    assert_eq!(post.tags, Tags(vec!["solo".into()]));
    assert_eq!(post.updated_at, 6);
}

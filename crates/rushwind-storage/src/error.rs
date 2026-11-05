//! The storage error taxonomy.

use std::fmt;

/// The error taxonomy every storage engine surfaces.
///
/// Mirrors [`ServerError`](https://docs.rs/rushwind-transport/rushwind_transport/enum.ServerError.html)
/// in spirit: a small closed set the caller can branch on, with driver detail
/// carried in a string payload rather than a generic error box.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// The requested row does not exist **or** exists outside the viewer's
    /// scope — the two are deliberately indistinguishable.
    NotFound,
    /// The query itself is malformed: unknown column, operator arity
    /// mismatch, invalid paging bounds, unparsable token, `Token` paging
    /// combined with a custom sort, …
    InvalidQuery(String),
    /// The write violates a store-side constraint (duplicate key, foreign
    /// key, uniqueness).
    Conflict(String),
    /// The table is tenant-scoped and the query arrived with no viewer
    /// identity attached — tenancy enforcement fails closed rather than
    /// silently dropping the tenant predicate. The port of the reference
    /// port's missing-viewer error.
    MissingViewer,
    /// The engine does not support the requested capability.
    Unsupported(String),
    /// The operation exceeded its budget.
    Timeout,
    /// A driver-level failure that does not fit the other variants; the
    /// payload is a human-readable description, never a secret.
    Backend(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "row not found"),
            Self::InvalidQuery(detail) => write!(f, "invalid query: {detail}"),
            Self::Conflict(detail) => write!(f, "conflict: {detail}"),
            Self::Unsupported(detail) => write!(f, "unsupported: {detail}"),
            Self::MissingViewer => write!(
                f,
                "missing viewer identity: the table is tenant-scoped and the query carries no tenant"
            ),
            Self::Timeout => write!(f, "operation timed out"),
            Self::Backend(detail) => write!(f, "backend failure: {detail}"),
        }
    }
}

impl std::error::Error for StorageError {}

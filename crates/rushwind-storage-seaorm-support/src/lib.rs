//! The typed-entity service bridge over SeaORM — the layer the
//! reference deployments' repositories and mappers shared by
//! copy-paste, extracted once.
//!
//! Three faces:
//!
//! * [`paging`] — the deployment corpus's `PagingRequest` select
//!   assembly: the `query` JSON filter syntax (rust-utils query_parser,
//!   `__`-suffixed operators, snake-cased fields), the `orderBy`
//!   JSON-array/plain spellings plus the structured `sorting` list, and
//!   the page/offset/none slicing modes. Filter values bind with the
//!   entity column's own schema type (numeric/bool/text read off the
//!   entity at runtime); PG lacks ILIKE in sea-query, so
//!   case-insensitive matching goes through `lower(x) LIKE lower(y)`.
//!   Deployment request types plug in through the [`paging::PagingInput`]
//!   trait — one impl per deployment, zero call-site churn.
//! * [`db_err`] — the SeaORM failure → Unknown envelope mapping.
//! * [`time`] — the entity-time ↔ proto-Timestamp conversions
//!   (SeaORM columns carry chrono types; the protojson face carries
//!   pbjson WKTs).
//!
//! Semantics are anchored to the two deployments' shared implementation;
//! the one deliberate tightening: ordering only lands on real entity
//! columns (an unknown ORDER BY name is skipped instead of emitting SQL
//! against a nonexistent column — the guard one deployment grew and the
//! other had not yet received).

#![forbid(unsafe_code)]

pub mod paging;
pub mod time;

use rushwind_http_binding::envelope::{internal_error, StatusError};

/// Repository-layer DB failure → the Unknown envelope (500, empty
/// reason) — the shape every repository `map_err`s through.
pub fn db_err(e: sea_orm::DbErr) -> StatusError {
    internal_error(format!("db: {e}"))
}

/// A plain failure string → the same Unknown envelope shape.
pub fn internal_error_msg(message: &str) -> StatusError {
    internal_error(message.to_string())
}

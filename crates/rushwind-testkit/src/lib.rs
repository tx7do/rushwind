//! Conformance suite for [`Server`] implementations.
//!
//! Every adapter crate (`rushwind-transport-*`) MUST invoke
//! [`rushwind_conformance_suite!`] from its integration-test target; a
//! transport is only conformant when the entire suite passes. The suite has
//! two halves:
//!
//! - **Orchestrator semantics** — run automatically against the built-in
//!   probe servers; these pin [`App`](rushwind_core::App) to its documented
//!   lifecycle (cascading shutdown, phase ordering, deadline enforcement,
//!   panic isolation, ill-behaved-server abandonment).
//! - **Adapter behavior** — run against the server produced by the
//!   `$factory` expression; these pin the adapter-side obligations of the
//!   contract (endpoint resolution, cooperative shutdown, teardown
//!   execution).
//!
//! # Wiring it up
//!
//! The suite's generated tests use `#[tokio::test]`, so the consuming crate
//! needs `tokio` as a dev-dependency:
//!
//! ```toml
//! [dev-dependencies]
//! rushwind-testkit = "0.0.1"
//! tokio = { version = "1", features = ["macros", "rt", "time"] }
//! ```
//!
//! Then, in the adapter crate's `tests/conformance.rs`, define the factory
//! at the test-crate root and reference it by crate-rooted path — the
//! suite expands into a nested module where a bare identifier does not
//! resolve:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rushwind_transport::Server;
//!
//! fn my_server() -> Arc<dyn Server> {
//!     Arc::new(MyServer::new())
//! }
//!
//! rushwind_testkit::rushwind_conformance_suite!(crate::my_server);
//! ```
//!
//! A `cargo test` with this suite green is the definition of a conformant
//! transport; CI enforces it for every adapter crate.
//!
//! # Re-exports
//!
//! [`rushwind_core`] and [`rushwind_transport`] are re-exported so the
//! generated tests resolve their dependencies through `$crate` without
//! additional imports in the consuming crate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod conformance;
#[cfg(feature = "lock")]
pub mod lock_conformance;
#[cfg(feature = "password")]
pub mod password_conformance;
pub mod probe;
#[cfg(feature = "storage")]
pub mod storage_conformance;
#[cfg(feature = "vector")]
pub mod vector_conformance;

pub use rushwind_core;
#[cfg(feature = "lock")]
pub use rushwind_lock;
#[cfg(feature = "password")]
pub use rushwind_password;
#[cfg(feature = "storage")]
pub use rushwind_storage;
pub use rushwind_transport;
#[cfg(feature = "vector")]
pub use rushwind_vector;

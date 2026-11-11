//! The password conformance suite.
//!
//! [`rushwind_password_conformance_suite!`] expands to a module of
//! `#[test]` functions pinning the [`PasswordHasher`](rushwind_password::PasswordHasher)
//! contract on a concrete engine: round-trips, the three verify outcomes
//! (`Ok(true)` / `Ok(false)` / `Err(InvalidHash)`), salt uniqueness, and
//! the needs-rehash faces. The suite is deliberately engine-agnostic —
//! the same tests gate the argon2, bcrypt, scrypt, and pbkdf2 engines.
//!
//! # Wiring it up
//!
//! The consuming crate needs the feature-gated testkit:
//!
//! ```toml
//! [dev-dependencies]
//! rushwind-testkit = { version = "0.0.1", features = ["password"] }
//! ```
//!
//! Then, in the engine crate's `tests/conformance.rs`, define a factory
//! at the test-crate root and reference it by crate-rooted path — the
//! suite expands into nested modules where a bare identifier does not
//! resolve:
//!
//! ```ignore
//! use std::sync::Arc;
//! use rushwind_password::PasswordHasher;
//!
//! fn hasher() -> Arc<dyn PasswordHasher> {
//!     Arc::new(MyEngine::with_fast_parameters().expect("valid"))
//! }
//!
//! rushwind_testkit::rushwind_password_conformance_suite!(crate::hasher);
//! ```
//!
//! `$factory` may run a **fast-but-valid** cost policy — the suite pins
//! semantics, not costs; the engine's own tests pin its defaults and
//! policy thresholds.

/// Generates the password conformance suite for a
/// [`PasswordHasher`](rushwind_password::PasswordHasher) implementation.
///
/// `$factory` must be a **crate-rooted path** to a function of type
/// `fn() -> Arc<dyn PasswordHasher>` producing the engine under test
/// (e.g. `crate::hasher`).
#[macro_export]
macro_rules! rushwind_password_conformance_suite {
    ($factory:expr) => {
        mod rushwind_password_conformance {
            //! The shared password conformance tests; each function pins
            //! one slice of the password contract semantics.

            use std::sync::Arc;

            use $crate::rushwind_password::{PasswordError, PasswordHasher};

            fn hasher() -> Arc<dyn PasswordHasher> {
                $factory()
            }

            /// Hash then verify round-trips; a wrong password reads as
            /// `Ok(false)`, not an error.
            #[test]
            fn hash_then_verify_round_trips() {
                let hasher = hasher();
                let stored = hasher.hash("correct horse").expect("hash must succeed");
                assert_eq!(
                    hasher
                        .verify("correct horse", &stored)
                        .expect("verify must succeed"),
                    true
                );
                assert_eq!(
                    hasher
                        .verify("staple battery", &stored)
                        .expect("verify must succeed"),
                    false,
                    "a wrong password is Ok(false), not an error"
                );
            }

            /// Hashing is salted: two calls on one password produce
            /// different stored strings.
            #[test]
            fn hashes_are_salted() {
                let hasher = hasher();
                let first = hasher.hash("same password").expect("hash must succeed");
                let second = hasher.hash("same password").expect("hash must succeed");
                assert_ne!(
                    first, second,
                    "a reused salt would leak equality across stores"
                );
            }

            /// A fresh hash needs no rehash — the engine's own policy
            /// minted it.
            #[test]
            fn fresh_hash_needs_no_rehash() {
                let hasher = hasher();
                let stored = hasher.hash("fresh").expect("hash must succeed");
                assert!(
                    !hasher.needs_rehash(&stored),
                    "a hash the policy itself minted must not read as weaker"
                );
            }

            /// A malformed stored string is an `Err(InvalidHash)`, not a
            /// wrong password — operations must be able to tell a corrupt
            /// row from a failed login.
            #[test]
            fn malformed_hash_is_invalid_hash() {
                let hasher = hasher();
                assert!(
                    matches!(
                        hasher.verify("anything", "not-a-hash"),
                        Err(PasswordError::InvalidHash(_))
                    ),
                    "garbage must not audit as a wrong password"
                );
            }

            /// `needs_rehash` on an unparseable string answers `true` —
            /// it cannot stay in the store.
            #[test]
            fn needs_rehash_on_garbage_is_true() {
                let hasher = hasher();
                assert!(hasher.needs_rehash("not-a-hash"));
            }

            /// The empty password is contract-valid input; Unicode
            /// round-trips. Both are caller-policy questions, not engine
            /// refusals.
            #[test]
            fn empty_and_unicode_passwords_round_trip() {
                let hasher = hasher();
                for password in ["", "风-🚀-p@ss"] {
                    let stored = hasher.hash(password).expect("hash must succeed");
                    assert_eq!(
                        hasher
                            .verify(password, &stored)
                            .expect("verify must succeed"),
                        true,
                        "the password itself must verify"
                    );
                    assert!(!(hasher.verify("x", &stored).expect("verify must succeed")));
                }
            }

            /// A tampered digest verifies `false`: flipping a byte inside
            /// the stored digest turns the check into a mismatch, never
            /// into a success or a panic.
            #[test]
            fn tampered_digest_verifies_false() {
                let hasher = hasher();
                let stored = hasher.hash("tamper target").expect("hash must succeed");
                let last = stored.chars().last().expect("hash is non-empty");
                let replacement = if last == 'A' { 'B' } else { 'A' };
                let tampered = format!("{}{replacement}", &stored[..stored.len() - 1]);
                assert_ne!(tampered, stored, "the tamper must change the string");
                // The tampered digest either verifies false (still
                // parseable) or is an invalid hash (the flip broke the
                // encoding) — both are refusals; success is impossible.
                match hasher.verify("tamper target", &tampered) {
                    Ok(false) => {}
                    Err(PasswordError::InvalidHash(_)) => {}
                    other => panic!("a tampered digest must not verify: {other:?}"),
                }
            }

            /// The stored hash string is not itself a password that
            /// verifies — degenerate input stays `false`.
            #[test]
            fn hash_string_as_password_is_false() {
                let hasher = hasher();
                let stored = hasher.hash("degenerate").expect("hash must succeed");
                assert_eq!(
                    hasher
                        .verify(&stored, &stored)
                        .expect("verify must succeed"),
                    false
                );
            }
        }
    };
}

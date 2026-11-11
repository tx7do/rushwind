//! Password-hashing contract for RushWind: one [`PasswordHasher`] face over
//! the PHC string format, with one engine crate per algorithm.
//!
//! # The core shapes
//!
//! A hash is a **self-describing PHC string** (`$argon2id$v=19$m=19456,t=2,
//! p=1$<salt>$<digest>`) — algorithm, cost parameters and salt all ride in
//! the string, so a stored hash verifies without the engine re-deriving any
//! parameter from configuration. [`PasswordHasher::verify`] drives the
//! verification with the parameters embedded in the stored hash, never the
//! engine's current policy: a stored hash that predates a cost bump must
//! still verify, and [`PasswordHasher::needs_rehash`] is the only place the
//! current policy speaks.
//!
//! The three outcomes a caller must keep apart:
//!
//! - `verify` → `Ok(true)` — the password matches.
//! - `verify` → `Ok(false)` — well-formed hash, wrong password. A normal
//!   outcome of a check, not an error.
//! - `verify` → `Err(InvalidHash)` — the stored string is not a hash this
//!   engine can parse. That is an operations incident (a corrupt or foreign
//!   row), and collapsing it into `false` would auditably look like a wrong
//!   password.
//!
//! Hashing is **synchronous CPU work** (tens of milliseconds at production
//! cost parameters, deliberately): the trait takes no async machinery so the
//! cost lands where the caller runs it. On a tokio thread pool, wrap calls
//! in `spawn_blocking`.
//!
//! # The upgrade loop
//!
//! [`PasswordHasher::needs_rehash`] answers one question: is this stored
//! hash weaker than the engine's current policy (or unparseable)? The
//! canonical flow rehashes after a *successful* verify — never on a failed
//! one, which would let an attacker rewrite stored hashes by guessing:
//!
//! ```ignore
//! if hasher.verify(password, stored)? {
//!     if hasher.needs_rehash(stored) {
//!         let fresh = hasher.hash(password)?;
//!         store.replace(user_id, fresh);
//!     }
//! }
//! ```
//!
//! # Engines
//!
//! Engines live in `rushwind-password-*` crates and implement
//! [`PasswordHasher`] over their native implementation:
//!
//! | Crate | Algorithm | PHC id |
//! |:---|:---|:---|
//! | `rushwind-password-argon2` | Argon2id | `argon2id` |
//! | `rushwind-password-bcrypt` | bcrypt | `bcrypt` (MCF `$2b$` wire) |
//! | `rushwind-password-scrypt` | scrypt | `scrypt` |
//! | `rushwind-password-pbkdf2` | PBKDF2-HMAC-SHA256 | `pbkdf2` |
//!
//! Every engine passes the shared conformance suite
//! (`rushwind_testkit::rushwind_password_conformance_suite!`); the
//! suite pins the semantics above, and each engine's own tests pin its
//! cost-parameter faces (defaults, `needs_rehash` policy thresholds, and
//! the algorithm's storage-format edges).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

/// Errors surfaced by password engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum PasswordError {
    /// The engine could not produce a hash (entropy or derivation
    /// failure on the hashing side). Verification never returns this
    /// variant.
    Hash(String),
    /// The stored string is not a well-formed hash this engine recognizes
    /// — corrupt row, truncated column, or a hash minted by a foreign
    /// scheme. Distinct from a wrong password (`Ok(false)`).
    InvalidHash(String),
    /// An engine constructor received a cost policy the algorithm
    /// rejects (out-of-range cost, rounds below the floor, …). Nothing
    /// was hashed or stored — fix the policy and retry.
    InvalidParams(String),
}

impl std::fmt::Display for PasswordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash(msg) => write!(f, "password hashing failed: {msg}"),
            Self::InvalidHash(msg) => write!(f, "stored value is not a valid hash: {msg}"),
            Self::InvalidParams(msg) => write!(f, "invalid cost parameters: {msg}"),
        }
    }
}

impl std::error::Error for PasswordError {}

/// The password-hashing contract. Engines must be callable through
/// shared references (`&self`).
///
/// All three methods are synchronous: hashing is CPU-bound by design,
/// and the PHC string carries every parameter verification needs.
pub trait PasswordHasher: Send + Sync {
    /// Hashes the password into a self-describing PHC string with a
    /// fresh random salt. Two calls on the same password produce
    /// different strings — the salt is not reusable.
    ///
    /// The empty password is a valid input at the contract level
    /// (caller-side policy decides whether to forbid it); every
    /// conformant engine must round-trip it.
    ///
    /// # Errors
    ///
    /// [`PasswordError::Hash`] when the engine cannot complete the
    /// derivation.
    fn hash(&self, password: &str) -> Result<String, PasswordError>;

    /// Verifies the password against a stored hash string. The
    /// parameters embedded in the stored hash drive the verification —
    /// the engine's current cost policy does not participate.
    ///
    /// # Errors
    ///
    /// [`PasswordError::InvalidHash`] when `hash` is not a well-formed
    /// hash this engine can parse. A wrong password against a
    /// well-formed hash is `Ok(false)`, never an error.
    fn verify(&self, password: &str, hash: &str) -> Result<bool, PasswordError>;

    /// Reports whether the stored hash is weaker than the engine's
    /// current cost policy and should be upgraded the next time its
    /// password verifies successfully. Unparseable strings answer
    /// `true` (they cannot stay in the store); strings whose embedded
    /// parameters already meet the policy answer `false`.
    fn needs_rehash(&self, hash: &str) -> bool;

    /// The PHC algorithm identifier this engine mints (`argon2id`,
    /// `bcrypt`, `scrypt`, `pbkdf2`, …). Informational — verification
    /// dispatches on the stored hash itself, not on this id.
    fn algorithm(&self) -> &'static str;
}

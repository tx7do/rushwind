//! Distributed-lock contract for RushWind: TTL'd acquire with
//! ownership tokens, ownership-checked release and extend.
//!
//! # The core shapes
//!
//! [`LockManager::acquire`] is **try-once** semantics — the atomic
//! test-and-set of a lock: `Ok(Some(handle))` when the key was free,
//! `Ok(None)` when it is held. Waiting is the caller's retry loop
//! (with whatever backoff policy fits); a contract-level "wait for the
//! lock" hides exactly the liveness questions (how long, what
//! backoff, cancellation) that belong in the open.
//!
//! Every acquire takes an **explicit TTL**; there is no forever-lock.
//! A lock nobody renews evaporates when its TTL lapses — the
//! crash-safety of the whole scheme rests on it, which is why a
//! zero-or-negative TTL refuses with [`LockError::InvalidTtl`] instead
//! of meaning "no expiry".
//!
//! # Ownership
//!
//! A [`LockHandle`] carries the engine-minted **token**. Release and
//! extend succeed only for the token that acquired the lock: a holder
//! whose TTL already lapsed (another worker took the key) gets
//! `Ok(false)` — its mutual exclusion already lapsed — never a
//! surprise delete of the successor's lock. `Ok(false)` on release is
//! the signal the caller did racy work and should treat the section's
//! output accordingly.
//!
//! # Engines
//!
//! Engines live in `rushwind-lock-*` crates:
//!
//! | Crate | Backing |
//! |:---|:---|
//! | `rushwind-lock-memory` | in-process map (single-process only) |
//! | `rushwind-lock-redis` | Redis `SET NX PX` + token-checked Lua release |
//!
//! Every engine passes the shared conformance suite
//! (`rushwind_testkit::rushwind_lock_conformance_suite!`).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Future type used across the lock contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by lock engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum LockError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The TTL is not usable for a lock (zero or negative). Locks are
    /// always TTL'd — the liveness of a crashed holder depends on it.
    InvalidTtl(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "lock operation failed: {msg}"),
            Self::InvalidTtl(msg) => write!(f, "invalid lock ttl: {msg}"),
        }
    }
}

impl std::error::Error for LockError {}

/// A held lock: the key and the engine-minted ownership token. Pass it
/// back to [`LockManager::release`] / [`LockManager::extend`]; those
/// succeed only while this token still owns the key.
///
/// The token is opaque and engine-chosen (random for the redis engine,
/// so ownership survives process restarts). Scheduling renewal before
/// the TTL lapses is the caller's business — the handle carries no
/// expiry clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockHandle {
    /// The locked key.
    pub key: String,
    /// The ownership token; treats it as opaque.
    pub token: String,
}

/// The lock contract. Engines must be callable through shared
/// references (`&self`).
pub trait LockManager: Send + Sync {
    /// Tries once to take the key for `ttl`: `Ok(Some)` when it was
    /// free, `Ok(None)` when held (or taken by a still-live TTL).
    ///
    /// # Errors
    ///
    /// [`LockError::InvalidTtl`] when `ttl` is zero or negative;
    /// [`LockError::Failed`] on engine failure.
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<LockHandle>, LockError>>;

    /// Releases the lock **only when `lock`'s token still owns the
    /// key**: `Ok(true)` freed, `Ok(false)` when the TTL already
    /// lapsed (or another holder owns it now) — never a delete of
    /// somebody else's lock.
    ///
    /// # Errors
    ///
    /// [`LockError::Failed`] on engine failure.
    fn release<'a>(&'a self, lock: &'a LockHandle) -> BoxFuture<'a, Result<bool, LockError>>;

    /// Pushes the lock's expiry a full `ttl` into the future — the
    /// renewal call for sections longer than their initial budget.
    /// `Ok(true)` extended; `Ok(false)` when the token no longer owns
    /// the key (the caller must re-acquire).
    ///
    /// # Errors
    ///
    /// [`LockError::InvalidTtl`] when `ttl` is zero or negative;
    /// [`LockError::Failed`] on engine failure.
    fn extend<'a>(
        &'a self,
        lock: &'a LockHandle,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LockError>>;

    /// Releases engine resources.
    fn close(&self) -> BoxFuture<'_, Result<(), LockError>>;
}

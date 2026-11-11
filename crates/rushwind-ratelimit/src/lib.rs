//! Rate-limiting contract for RushWind: an algorithm-agnostic
//! [`Limiter`] interface. Concrete implementations (token bucket,
//! BBR, ...) live in `rushwind-ratelimit-*` crates and implement this
//! trait so business code depends only on the contract.
//!
//! # Deferred engine: sentinel
//!
//! A Sentinel engine is deferred: building one means reimplementing
//! Sentinel's core (no maintained Rust SDK offers an equivalent to
//! wrap) — token calculation, reject/throttle behaviors, warm-up,
//! statistic windows. The direct-
//! threshold + reject rule shape maps 1:1 onto the token-bucket
//! engine (burst = threshold) when that rule shape is all a caller
//! needs.
//!
//! # The core shapes
//!
//! [`Limiter::allow`] returns `Ok(false)` when the request is
//! rejected and `Ok(true)`
//! when permitted — rejections are normal outcomes, not errors.
//! The closed-limiter vs exhausted-limiter distinction is not
//! carried: a closed limiter rejects like an exhausted one.
//!
//! [`Limiter::wait`] blocks until permitted or cancelled;
//! cancellation rides the future — dropping the
//! [`Limiter::wait`] future is the cancellation. A permanently
//! exhausted limiter (e.g. a closed one) still returns
//! [`RateLimitError::Limited`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

/// Future type used across the ratelimit contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A monotonic clock the engines read time through — `Arc<dyn Fn() ->
/// Instant>`. The default is [`Instant::now`]; engines accept an
/// injected one (`with_clock`) so tests drive refill accounting and
/// window rotation by advancing a fake instead of sleeping real
/// durations.
pub type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The wall clock every engine runs on when none is injected.
pub fn system_clock() -> Clock {
    Arc::new(Instant::now)
}

/// Errors surfaced by rate limiters.
#[derive(Debug)]
#[non_exhaustive]
pub enum RateLimitError {
    /// The rate limit is permanently exhausted (e.g. the limiter was
    /// closed).
    Limited,
    /// The engine could not complete the operation.
    Failed(String),
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limited => write!(f, "ratelimit: rate limit exceeded"),
            Self::Failed(msg) => write!(f, "ratelimit operation failed: {msg}"),
        }
    }
}

impl std::error::Error for RateLimitError {}

/// The core rate-limiting contract. Implementations
/// must be callable through shared references (`&self`) and safe for
/// concurrent use.
pub trait Limiter: Send + Sync {
    /// Attempts to consume one unit without blocking. `Ok(false)` —
    /// the rate limit is exceeded; `Ok(true)` — permitted.
    fn allow(&self) -> bool;

    /// Waits until a unit is available. `Err(Limited)` only when the
    /// limiter is permanently exhausted; cancellation rides the
    /// future.
    fn wait<'a>(&'a self) -> BoxFuture<'a, Result<(), RateLimitError>>;

    /// Releases engine resources. Waiters blocked in [`Limiter::wait`]
    /// resolve with [`RateLimitError::Limited`].
    fn close(&self);
}

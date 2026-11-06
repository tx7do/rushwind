//! Token-bucket rate limiter for the RushWind ratelimit contract.
//!
//! Tokens refill at a fixed rate (`rate` per second) up to the burst
//! capacity; every request consumes one. An empty bucket rejects
//! ([`TokenBucket::allow`]) or delays ([`TokenBucket::wait`]).
//!
//! Virtual-clock accounting: tokens are computed
//! from the elapsed time since the last take, so a limiter idle for a
//! long period has a full burst available immediately, and Wait's
//! delay is the exact deficit divided by the rate.
//!
//! # Divergences
//!
//! - Waking concurrent Waiters when a *take* makes tokens available:
//!   here each Wait recomputes its own delay from the shared state,
//!   which serves the same purpose without the broadcast. A Close
//!   does wake every sleeping Waiter (via a notification) so it
//!   answers `Limited` immediately.
//! - No clock injection (`WithClock`): tests use real short
//!   durations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_ratelimit::{BoxFuture, Limiter, RateLimitError};

struct State {
    tokens: f64,
    last: Instant,
    closed: bool,
}

/// A token-bucket rate limiter.
pub struct TokenBucket {
    state: Mutex<State>,
    rate: f64,
    burst: f64,
    /// Wakes sleeping `wait`ers when [`Limiter::close`] fires.
    close_notify: tokio::sync::Notify,
}

/// The token-bucket engine's settings.
#[derive(Debug, Clone)]
pub struct TokenBucketOptions {
    /// The sustained refill rate, in tokens per second.
    pub rate: f64,
    /// The bucket capacity — the instantaneous burst.
    pub burst: f64,
}

impl TokenBucket {
    /// Builds a limiter starting full. Fails when `rate` or `burst`
    /// is not strictly positive.
    pub fn new(options: TokenBucketOptions) -> Result<Self, RateLimitError> {
        if options.rate <= 0.0 || options.burst <= 0.0 {
            return Err(RateLimitError::Failed(
                "rate and burst must be > 0".to_string(),
            ));
        }
        Ok(Self {
            state: Mutex::new(State {
                tokens: options.burst,
                last: Instant::now(),
                closed: false,
            }),
            rate: options.rate,
            burst: options.burst,
            close_notify: tokio::sync::Notify::new(),
        })
    }

    /// Attempts to consume one token without blocking.
    /// `false` when the bucket is empty or the limiter is
    /// closed.
    pub fn allow(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        self.take_locked(&mut state, 1.0).wait.is_zero()
    }

    /// Consumes `n` tokens, returning the duration to wait when the
    /// bucket cannot cover the request yet.
    /// Replenishes tokens from elapsed time first.
    fn take_locked(&self, state: &mut State, n: f64) -> Take {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens = (state.tokens + elapsed * self.rate).min(self.burst);
            state.last = now;
        }

        if state.tokens >= n {
            state.tokens -= n;
            return Take {
                wait: Duration::ZERO,
            };
        }

        let deficit = n - state.tokens;
        Take {
            // An extreme `rate` can push the quotient past the
            // Duration representation ceiling; `from_secs_f64` would
            // panic there, and an unrepresentable wait simply means
            // "not for the foreseeable future".
            wait: Duration::try_from_secs_f64(deficit / self.rate).unwrap_or(Duration::MAX),
        }
    }

    /// The most tokens the bucket can hold — the configured burst.
    pub fn burst(&self) -> f64 {
        self.burst
    }

    /// The sustained refill rate — tokens per second.
    pub fn rate(&self) -> f64 {
        self.rate
    }
}

/// The outcome of a token take: how long until the tokens are
/// available.
struct Take {
    wait: Duration,
}

impl Limiter for TokenBucket {
    fn allow(&self) -> bool {
        TokenBucket::allow(self)
    }

    fn wait<'a>(&'a self) -> BoxFuture<'a, Result<(), RateLimitError>> {
        Box::pin(async move {
            loop {
                let wait = {
                    let mut state = self.state.lock().unwrap();
                    if state.closed {
                        return Err(RateLimitError::Limited);
                    }
                    self.take_locked(&mut state, 1.0).wait
                };
                if wait.is_zero() {
                    return Ok(());
                }
                // The close notification cuts the sleep short: a
                // closed limiter answers its waiters with `Limited`
                // right away instead of after the full (possibly
                // astronomically long) delay.
                let closed = self.close_notify.notified();
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = closed => {}
                }
            }
        })
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.close_notify.notify_waiters();
    }
}

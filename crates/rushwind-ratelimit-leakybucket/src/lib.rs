//! Leaky-bucket rate limiter for the RushWind ratelimit contract —
//! the pacing shape: every admission pours one unit into a backlog
//! that drains at a constant `drain_rate`; a full backlog refuses.
//!
//! The distinction from the token bucket is where the slack lives. A
//! token bucket **banks** credit while idle (a full burst available
//! after a long quiet period); the leaky bucket banks nothing — its
//! slack is the *empty backlog*, and steady-state output is paced at
//! exactly `drain_rate` no matter how the requests arrived. Callers
//! who want "no more than N per second, smoothed" reach here; callers
//! who want "N per second with a burst allowance" reach the token
//! bucket.
//!
//! Virtual-clock accounting: the backlog level is computed from the
//! elapsed time since the last touch, so an idle limiter has drained
//! to empty and admits instantly — once; the second immediate request
//! already waits `1/drain_rate`.
//!
//! # Divergences
//!
//! - Clock injection is [`LeakyBucket::with_clock`]: drain accounting
//!   and deficit computation read the injected clock, so tests
//!   advance a fake instead of sleeping. The `wait` sleep still rides
//!   tokio's real timer — wait-path tests use real short durations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_ratelimit::{system_clock, BoxFuture, Clock, Limiter, RateLimitError};

struct State {
    level: f64,
    last: Instant,
    closed: bool,
}

/// A leaky-bucket (pacing) rate limiter.
pub struct LeakyBucket {
    state: Mutex<State>,
    capacity: f64,
    drain_rate: f64,
    /// The clock drain accounting reads — injectable for tests.
    clock: Clock,
    /// Wakes sleeping `wait`ers when [`Limiter::close`] fires.
    close_notify: tokio::sync::Notify,
}

/// The leaky-bucket engine's settings.
#[derive(Debug, Clone)]
pub struct LeakyBucketOptions {
    /// The backlog capacity, in units. Must be at least 1 — a bucket
    /// that cannot hold a single admission would refuse everything.
    pub capacity: f64,
    /// The constant drain rate, in units per second.
    pub drain_rate: f64,
}

impl LeakyBucket {
    /// Builds a limiter starting empty. Fails when `drain_rate` is
    /// not strictly positive or `capacity` is below 1.
    pub fn new(options: LeakyBucketOptions) -> Result<Self, RateLimitError> {
        Self::with_clock(options, system_clock())
    }

    /// Builds a limiter whose drain accounting reads `clock` — the
    /// test seam: a fake clock advanced by hand replaces the real
    /// sleeps drain tests would otherwise need. The `wait` sleep
    /// still rides tokio's real timer.
    pub fn with_clock(options: LeakyBucketOptions, clock: Clock) -> Result<Self, RateLimitError> {
        if options.drain_rate <= 0.0 {
            return Err(RateLimitError::Failed("drain_rate must be > 0".to_string()));
        }
        if options.capacity < 1.0 {
            return Err(RateLimitError::Failed(
                "capacity must be >= 1: a bucket that cannot hold one admission refuses everything"
                    .to_string(),
            ));
        }
        Ok(Self {
            state: Mutex::new(State {
                level: 0.0,
                last: clock(),
                closed: false,
            }),
            capacity: options.capacity,
            drain_rate: options.drain_rate,
            clock,
            close_notify: tokio::sync::Notify::new(),
        })
    }

    /// Drains the backlog to `now` under the lock. The `now` is passed
    /// in so every read in one decision observes the same instant.
    fn drain_locked(state: &mut State, now: Instant, drain_rate: f64) {
        let elapsed = now.duration_since(state.last).as_secs_f64();
        if elapsed > 0.0 {
            state.level = (state.level - elapsed * drain_rate).max(0.0);
            state.last = now;
        }
    }

    /// The backlog capacity — the maximum in-flight backlog.
    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    /// The constant drain rate — units per second.
    pub fn drain_rate(&self) -> f64 {
        self.drain_rate
    }

    /// Attempts to pour one unit into the backlog without blocking.
    /// `false` when the backlog is full or the limiter is closed.
    pub fn allow(&self) -> bool {
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        Self::drain_locked(&mut state, now, self.drain_rate);
        if state.level + 1.0 <= self.capacity {
            state.level += 1.0;
            true
        } else {
            false
        }
    }
}

impl Limiter for LeakyBucket {
    fn allow(&self) -> bool {
        LeakyBucket::allow(self)
    }

    fn wait<'a>(&'a self) -> BoxFuture<'a, Result<(), RateLimitError>> {
        Box::pin(async move {
            loop {
                let wait = {
                    let now = (self.clock)();
                    let mut state = self.state.lock().unwrap();
                    if state.closed {
                        return Err(RateLimitError::Limited);
                    }
                    Self::drain_locked(&mut state, now, self.drain_rate);
                    if state.level + 1.0 <= self.capacity {
                        state.level += 1.0;
                        return Ok(());
                    }
                    // The deficit above the capacity drains at the
                    // constant rate; that long is the wait.
                    let deficit = state.level + 1.0 - self.capacity;
                    // An extreme `drain_rate` can push the quotient
                    // past the Duration representation ceiling;
                    // `from_secs_f64` would panic there, and an
                    // unrepresentable wait simply means "not for the
                    // foreseeable future".
                    Duration::try_from_secs_f64(deficit / self.drain_rate).unwrap_or(Duration::MAX)
                };
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

//! Sliding-window-log rate limiter for the RushWind ratelimit contract.
//!
//! At most `limit` admissions per trailing `window`: every admission is
//! logged with its timestamp, and a request is admitted only while the
//! log holds fewer than `limit` entries younger than the window. The
//! boundary is exact — unlike a fixed window there is no double-budget
//! seam at the wall-clock edge, and unlike the token bucket there is
//! nothing to bank: `limit` requests in any trailing window is the
//! whole contract, however idle the limiter was before.
//!
//! Memory is bounded by the admitted cadence, not by time: the log
//! never holds more than `limit` entries (older ones are pruned on
//! every admission attempt), so `limit` is also the memory budget.
//!
//! # Divergences
//!
//! - Clock injection is [`SlidingWindow::with_clock`]: pruning and the
//!   admission accounting read the injected clock, so tests advance a
//!   fake instead of sleeping. The `wait` sleep still rides tokio's
//!   real timer — wait-path tests use real short durations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_ratelimit::{system_clock, BoxFuture, Clock, Limiter, RateLimitError};

struct State {
    events: VecDeque<Instant>,
    closed: bool,
}

/// A sliding-window-log rate limiter.
pub struct SlidingWindow {
    state: Mutex<State>,
    limit: usize,
    window: Duration,
    /// The clock pruning and accounting read — injectable for tests.
    clock: Clock,
    /// Wakes sleeping `wait`ers when [`Limiter::close`] fires.
    close_notify: tokio::sync::Notify,
}

/// The sliding-window engine's settings.
#[derive(Debug, Clone)]
pub struct SlidingWindowOptions {
    /// The maximum admissions per trailing window. Also the memory
    /// bound: the log never holds more entries than this.
    pub limit: usize,
    /// The trailing window the limit counts over.
    pub window: Duration,
}

impl SlidingWindow {
    /// Builds a limiter. Fails when `limit` is zero or `window` is
    /// zero — either would be a limiter that admits nothing or one
    /// whose window never passes.
    pub fn new(options: SlidingWindowOptions) -> Result<Self, RateLimitError> {
        Self::with_clock(options, system_clock())
    }

    /// Builds a limiter whose pruning and accounting read `clock` —
    /// the test seam: a fake clock advanced by hand replaces the real
    /// sleeps window tests would otherwise need. The `wait` sleep
    /// still rides tokio's real timer.
    pub fn with_clock(options: SlidingWindowOptions, clock: Clock) -> Result<Self, RateLimitError> {
        if options.limit == 0 {
            return Err(RateLimitError::Failed("limit must be > 0".to_string()));
        }
        if options.window.is_zero() {
            return Err(RateLimitError::Failed("window must be > 0".to_string()));
        }
        Ok(Self {
            state: Mutex::new(State {
                events: VecDeque::new(),
                closed: false,
            }),
            limit: options.limit,
            window: options.window,
            clock,
            close_notify: tokio::sync::Notify::new(),
        })
    }

    /// Drops the entries that have slid out of the window, under the
    /// lock. The `now` is passed in so every read in one decision
    /// observes the same instant.
    fn prune_locked(state: &mut State, now: Instant, window: Duration) {
        while let Some(front) = state.events.front() {
            if now.duration_since(*front) >= window {
                state.events.pop_front();
            } else {
                break;
            }
        }
    }

    /// The maximum admissions per trailing window.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The trailing window the limit counts over.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Attempts to consume one admission slot without blocking.
    /// `false` when the window is full or the limiter is closed.
    pub fn allow(&self) -> bool {
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        Self::prune_locked(&mut state, now, self.window);
        if state.events.len() < self.limit {
            state.events.push_back(now);
            true
        } else {
            false
        }
    }
}

impl Limiter for SlidingWindow {
    fn allow(&self) -> bool {
        SlidingWindow::allow(self)
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
                    Self::prune_locked(&mut state, now, self.window);
                    if state.events.len() < self.limit {
                        state.events.push_back(now);
                        return Ok(());
                    }
                    // The oldest in-window entry holds the slot: it
                    // slides out exactly `window` after its timestamp.
                    state
                        .events
                        .front()
                        .and_then(|front| front.checked_add(self.window))
                        .and_then(|frees_at| frees_at.checked_duration_since(now))
                        .unwrap_or(Duration::ZERO)
                };
                // The close notification cuts the sleep short: a closed
                // limiter answers its waiters with `Limited` right away
                // instead of after the full window.
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

//! Hystrix-style circuit breaker for the RushWind circuitbreaker
//! contract.
//!
//! The Netflix Hystrix model over a fixed-length sliding window:
//!
//! - **Closed**: requests flow; the window tracks success/failure
//!   counts. When the error rate meets
//!   [`HystrixOptions::error_threshold`] and the window's request
//!   volume meets [`HystrixOptions::request_volume_threshold`], the
//!   breaker trips open.
//! - **Open**: everything rejects. After
//!   [`HystrixOptions::sleep_window`] the breaker moves to half-open.
//! - **Half-open**: a single trial request is admitted; success
//!   closes (and resets the buckets), failure re-opens and restarts
//!   the sleep timer.
//!
//! `State()` performs the Open → HalfOpen transition lazily, so
//! observing the state also advances the machine.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_circuitbreaker::{CircuitBreaker, CircuitError, State as ContractState};

#[derive(Clone)]
struct Bucket {
    requests: u64,
    errors: u64,
}

struct HystrixState {
    buckets: Vec<Bucket>,
    last_rotate: Instant,
    state: ContractState,
    opened_at: Instant,
    half_open_in: bool,
    /// When the in-flight half-open trial was admitted. A trial whose
    /// outcome never arrives (the caller dropped the future, panicked
    /// between `allow` and `mark`, or forgot the mark) is reclaimed
    /// after one sleep window, or the breaker stays stuck in half-open
    /// rejecting everything forever.
    half_open_at: Option<Instant>,
    closed: bool,
}

/// The Hystrix engine's settings.
#[derive(Debug, Clone)]
pub struct HystrixOptions {
    /// The error rate (0–1] that trips the breaker. Default 0.50.
    pub error_threshold: f64,
    /// The minimum window requests before the breaker can trip.
    /// Default 20.
    pub request_volume_threshold: u64,
    /// How long the breaker stays open before half-opening.
    /// Default 5 s.
    pub sleep_window: Duration,
    /// The sliding-window length. Default 10 s.
    pub window: Duration,
    /// The number of buckets in the window. Default 10.
    pub bucket_count: usize,
}

impl Default for HystrixOptions {
    fn default() -> Self {
        Self {
            error_threshold: 0.50,
            request_volume_threshold: 20,
            sleep_window: Duration::from_secs(5),
            window: Duration::from_secs(10),
            bucket_count: 10,
        }
    }
}

/// A Hystrix-style circuit breaker.
pub struct HystrixBreaker {
    state: Mutex<HystrixState>,
    error_threshold: f64,
    request_volume_threshold: u64,
    sleep_window: Duration,
    bucket_duration: Duration,
    start: Instant,
}

impl HystrixBreaker {
    /// Builds a Hystrix-style breaker with the default options.
    pub fn new() -> Self {
        Self::with_options(HystrixOptions::default())
    }

    /// Builds a Hystrix-style breaker with explicit options.
    pub fn with_options(options: HystrixOptions) -> Self {
        let bucket_duration = options.window / options.bucket_count as u32;
        Self {
            state: Mutex::new(HystrixState {
                buckets: vec![
                    Bucket {
                        requests: 0,
                        errors: 0,
                    };
                    options.bucket_count
                ],
                last_rotate: Instant::now(),
                state: ContractState::Closed,
                opened_at: Instant::now(),
                half_open_in: false,
                half_open_at: None,
                closed: false,
            }),
            error_threshold: options.error_threshold,
            request_volume_threshold: options.request_volume_threshold,
            sleep_window: options.sleep_window,
            bucket_duration,
            start: Instant::now(),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, CircuitError> {
        #[derive(serde::Deserialize)]
        struct HystrixSettings {
            error_threshold: Option<f64>,
            request_volume_threshold: Option<u64>,
            sleep_window_ms: Option<u64>,
            window_ms: Option<u64>,
            bucket_count: Option<usize>,
        }
        let settings: HystrixSettings = serde_json::from_value(settings)
            .map_err(|e| CircuitError::Failed(format!("settings parse: {e}")))?;
        let defaults = HystrixOptions::default();
        Ok(Self::with_options(HystrixOptions {
            error_threshold: settings.error_threshold.unwrap_or(defaults.error_threshold),
            request_volume_threshold: settings
                .request_volume_threshold
                .unwrap_or(defaults.request_volume_threshold),
            sleep_window: Duration::from_millis(
                settings
                    .sleep_window_ms
                    .unwrap_or(defaults.sleep_window.as_millis() as u64),
            ),
            window: Duration::from_millis(
                settings
                    .window_ms
                    .unwrap_or(defaults.window.as_millis() as u64),
            ),
            bucket_count: settings.bucket_count.unwrap_or(defaults.bucket_count),
        }))
    }

    /// Admits or rejects a request: closed flows normally; open flows
    /// again once
    /// the sleep window elapsed (moving to half-open with a single
    /// trial in flight); a trial already in flight rejects.
    fn allow_locked(
        &self,
        state: &mut HystrixState,
        now: Instant,
    ) -> std::result::Result<(), CircuitError> {
        if state.closed {
            return Err(CircuitError::Open);
        }
        match state.state {
            ContractState::Open => {
                if now.duration_since(state.opened_at) >= self.sleep_window {
                    state.state = ContractState::HalfOpen;
                    state.half_open_in = true;
                    state.half_open_at = Some(now);
                } else {
                    return Err(CircuitError::Open);
                }
            }
            ContractState::HalfOpen => {
                if state.half_open_in {
                    // One trial at a time — unless the trial in flight
                    // is lost: after a full sleep window without a mark,
                    // reclaim the slot so the breaker can keep serving
                    // trials instead of rejecting everything forever.
                    let trial_lost = state
                        .half_open_at
                        .map(|at| now.duration_since(at) >= self.sleep_window)
                        .unwrap_or(true);
                    if !trial_lost {
                        return Err(CircuitError::Open);
                    }
                }
                state.half_open_in = true;
                state.half_open_at = Some(now);
            }
            ContractState::Closed => {}
        }
        Ok(())
    }

    /// Evaluates the window: trip open when the error rate
    /// meets the threshold after enough volume.
    fn evaluate_locked(&self, state: &mut HystrixState) {
        if state.state != ContractState::Closed {
            return;
        }
        let (mut total_requests, mut total_errors) = (0u64, 0u64);
        for bucket in &state.buckets {
            total_requests += bucket.requests;
            total_errors += bucket.errors;
        }
        if total_requests < self.request_volume_threshold || total_requests == 0 {
            return;
        }
        let error_rate = total_errors as f64 / total_requests as f64;
        if error_rate >= self.error_threshold {
            state.state = ContractState::Open;
            state.opened_at = Instant::now();
        }
    }

    fn rotate_locked(&self, state: &mut HystrixState, now: Instant) {
        let elapsed = now.duration_since(state.last_rotate);
        let steps = (elapsed.as_nanos() / self.bucket_duration.as_nanos()) as usize;
        if steps == 0 {
            return;
        }
        let n = state.buckets.len();
        if steps >= n {
            state.buckets.fill(Bucket {
                requests: 0,
                errors: 0,
            });
        } else {
            for offset in 0..steps {
                let idx = (self.bucket_index(state, now) + n - steps + offset + 1 + n) % n;
                state.buckets[idx] = Bucket {
                    requests: 0,
                    errors: 0,
                };
            }
        }
        state.last_rotate = now;
    }

    fn bucket_index(&self, state: &HystrixState, now: Instant) -> usize {
        let elapsed = now.duration_since(self.start);
        let bucket_nanos = self.bucket_duration.as_nanos();
        (elapsed.as_nanos() / bucket_nanos) as usize % state.buckets.len()
    }

    fn reset_buckets_locked(state: &mut HystrixState) {
        state.buckets.fill(Bucket {
            requests: 0,
            errors: 0,
        });
    }
}

impl Default for HystrixBreaker {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreaker for HystrixBreaker {
    fn allow(&self) -> std::result::Result<(), CircuitError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(CircuitError::Open);
        }
        let now = Instant::now();
        self.rotate_locked(&mut state, now);
        self.allow_locked(&mut state, now)
    }

    /// A half-open trial success recovers: the state closes and the
    /// buckets reset. Otherwise the outcome feeds the sliding window
    /// and may trip the breaker.
    fn mark_success(&self) {
        let mut state = self.state.lock().unwrap();
        self.rotate_locked(&mut state, Instant::now());

        if state.state == ContractState::HalfOpen {
            // Trial succeeded — recover.
            state.state = ContractState::Closed;
            state.half_open_in = false;
            state.half_open_at = None;
            Self::reset_buckets_locked(&mut state);
            return;
        }

        let index = self.bucket_index(&state, Instant::now());
        state.buckets[index].requests += 1;
        self.evaluate_locked(&mut state);
    }

    /// A half-open trial failure re-opens and restarts the sleep
    /// timer; otherwise the outcome feeds the window and may trip.
    fn mark_failure(&self) {
        let mut state = self.state.lock().unwrap();
        self.rotate_locked(&mut state, Instant::now());

        if state.state == ContractState::HalfOpen {
            // Trial failed — re-open.
            state.state = ContractState::Open;
            state.opened_at = Instant::now();
            state.half_open_in = false;
            state.half_open_at = None;
            return;
        }

        let index = self.bucket_index(&state, Instant::now());
        state.buckets[index].requests += 1;
        state.buckets[index].errors += 1;
        self.evaluate_locked(&mut state);
    }

    /// The current state, with the lazy Open → HalfOpen transition
    /// applied.
    fn state(&self) -> ContractState {
        let mut state = self.state.lock().unwrap();
        if state.state == ContractState::Open && state.opened_at.elapsed() >= self.sleep_window {
            state.state = ContractState::HalfOpen;
        }
        state.state
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

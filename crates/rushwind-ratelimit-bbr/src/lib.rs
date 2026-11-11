//! BBR-inspired adaptive rate limiter for the RushWind ratelimit
//! contract.
//!
//! Unlike a fixed-rate limiter, the BBR limiter estimates the system's
//! maximum sustainable throughput from observed latency and inflight
//! counts, then adjusts the pass-through rate. A sliding window of
//! buckets tracks completions and their RTTs;
//! `maxInflight = window / minRTT * cpuThreshold` caps the concurrent
//! requests.
//!
//! # The lifecycle
//!
//! [`BbrLimiter::allow`] admits a request (incrementing the inflight
//! count) or rejects it at capacity; the caller reports completion
//! with [`BbrLimiter::done`], feeding the RTT back into the window
//! (the `Allow`/`Done` pairing). [`Limiter::wait`] polls `allow`
//! every ten milliseconds.
//!
//! # Divergences
//!
//! - `Done(rtt)` is inherent to the BBR type (it does not fit
//!   the base `Limiter` contract); so is
//!   [`BbrLimiter::done`]. `MaxInflight` becomes
//!   [`BbrLimiter::max_inflight`], same semantics.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_ratelimit::{system_clock, BoxFuture, Clock, Limiter, RateLimitError};

/// The CPU/load threshold above which requests throttle. Default 0.80.
const DEFAULT_CPU_THRESHOLD: f64 = 0.80;
/// The sliding-window length. Default 10 s.
const DEFAULT_WINDOW: Duration = Duration::from_secs(10);
/// The number of buckets in the window. Default 40.
const DEFAULT_BUCKET_COUNT: usize = 40;
/// The minimum allowed QPS even under heavy load. Default 1.0.
const DEFAULT_MIN_QPS: f64 = 1.0;

struct Config {
    cpu_threshold: f64,
    window: Duration,
    bucket_count: usize,
    min_qps: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cpu_threshold: DEFAULT_CPU_THRESHOLD,
            window: DEFAULT_WINDOW,
            bucket_count: DEFAULT_BUCKET_COUNT,
            min_qps: DEFAULT_MIN_QPS,
        }
    }
}

/// The BBR engine's settings.
#[derive(Debug, Clone)]
pub struct BbrOptions {
    /// The CPU/load threshold (0–1). Default 0.80.
    pub cpu_threshold: f64,
    /// The sliding-window length. Default 10 s.
    pub window: Duration,
    /// The number of buckets within the window. Default 40.
    pub bucket_count: usize,
    /// The minimum allowed QPS. Default 1.0.
    pub min_qps: f64,
}

impl Default for BbrOptions {
    fn default() -> Self {
        Self {
            cpu_threshold: DEFAULT_CPU_THRESHOLD,
            window: DEFAULT_WINDOW,
            bucket_count: DEFAULT_BUCKET_COUNT,
            min_qps: DEFAULT_MIN_QPS,
        }
    }
}

#[derive(Clone)]
struct Bucket {
    count: u64,
    total_rtt_nanos: u64,
}

struct State {
    buckets: Vec<Bucket>,
    last_bucket_time: Instant,
    inflight: u64,
    max_inflight: u64,
    closed: bool,
}

/// A BBR adaptive rate limiter.
pub struct BbrLimiter {
    state: Mutex<State>,
    config: Config,
    bucket_duration: Duration,
    /// The monotonic anchor the bucket ring indexes against.
    start: Instant,
    /// The clock window rotation reads — injectable for tests.
    clock: Clock,
}

impl BbrLimiter {
    /// Builds a BBR adaptive limiter with the default options.
    pub fn new() -> Self {
        Self::with_options(BbrOptions::default())
    }

    /// Builds a BBR adaptive limiter with explicit options.
    pub fn with_options(options: BbrOptions) -> Self {
        Self::with_clock(options, system_clock())
    }

    /// Builds a BBR adaptive limiter whose window rotation reads
    /// `clock` — the test seam: a fake clock advanced by hand drives
    /// bucket rotation and window expiry without real sleeps. The
    /// `wait` poll sleep still rides tokio's real timer.
    pub fn with_clock(options: BbrOptions, clock: Clock) -> Self {
        let config = Config {
            cpu_threshold: options.cpu_threshold,
            window: options.window,
            bucket_count: options.bucket_count,
            min_qps: options.min_qps,
        };
        let bucket_duration = config.window / config.bucket_count as u32;
        let now = clock();
        Self {
            state: Mutex::new(State {
                buckets: vec![
                    Bucket {
                        count: 0,
                        total_rtt_nanos: 0,
                    };
                    config.bucket_count
                ],
                last_bucket_time: now,
                inflight: 0,
                max_inflight: 0,
                closed: false,
            }),
            config,
            bucket_duration,
            start: now,
            clock,
        }
    }

    /// Attempts to admit a request. When admitted,
    /// the caller reports completion with [`BbrLimiter::done`].
    pub fn allow(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        self.rotate_locked(&mut state, (self.clock)());

        let max_qps = self.estimate_max_qps_locked(&state);
        let mut max_inflight = (max_qps * self.config.cpu_threshold) as u64;
        if max_inflight < 1 {
            max_inflight = 1;
        }
        state.max_inflight = max_inflight;

        if state.inflight >= max_inflight {
            return false;
        }
        state.inflight += 1;
        true
    }

    /// Marks the completion of a previously admitted request — `rtt`
    /// is the request's end-to-end latency.
    pub fn done(&self, rtt: Duration) {
        let mut state = self.state.lock().unwrap();
        state.inflight = state.inflight.saturating_sub(1);
        let now = (self.clock)();
        self.rotate_locked(&mut state, now);

        let index = self.current_bucket_index(now);
        state.buckets[index].count += 1;
        state.buckets[index].total_rtt_nanos += rtt.as_nanos() as u64;
    }

    /// The most recently computed inflight limit.
    pub fn max_inflight(&self) -> u64 {
        self.state.lock().unwrap().max_inflight
    }

    /// Advances the bucket ring to `now`, clearing buckets that fell
    /// out of the window.
    fn rotate_locked(&self, state: &mut State, now: Instant) {
        let elapsed = now.duration_since(state.last_bucket_time);
        let steps = (elapsed.as_nanos() / self.bucket_duration.as_nanos()) as usize;
        if steps == 0 {
            return;
        }
        let n = state.buckets.len();
        if steps >= n {
            let cleared: Vec<Bucket> = (0..state.buckets.len())
                .map(|_| Bucket {
                    count: 0,
                    total_rtt_nanos: 0,
                })
                .collect();
            state.buckets = cleared;
        } else {
            for offset in 0..steps {
                let current = self.current_bucket_index(now);
                let idx = (current + n - steps + offset + 1) % n;
                state.buckets[idx] = Bucket {
                    count: 0,
                    total_rtt_nanos: 0,
                };
            }
        }
        state.last_bucket_time = now;
    }

    /// The ring index for `now`, anchored at the limiter's
    /// construction — the buckets only compare relative ages, so the
    /// missing absolute epoch is irrelevant.
    fn current_bucket_index(&self, now: Instant) -> usize {
        let elapsed = now.duration_since(self.start);
        let bucket_nanos = self.bucket_duration.as_nanos();
        (elapsed.as_nanos() / bucket_nanos) as usize % self.config.bucket_count
    }

    /// The estimated maximum QPS from the sliding window:
    /// `maxQPS = windowSize / minRTT`, floored at the configured
    /// minimum.
    fn estimate_max_qps_locked(&self, state: &State) -> f64 {
        let mut total_count: u64 = 0;
        let mut min_rtt_nanos: Option<u64> = None;
        for bucket in &state.buckets {
            if bucket.count > 0 {
                total_count += bucket.count;
                let avg_rtt = bucket.total_rtt_nanos / bucket.count;
                match min_rtt_nanos {
                    Some(current) if avg_rtt >= current => {}
                    _ => min_rtt_nanos = Some(avg_rtt),
                }
            }
        }

        let Some(min_rtt_nanos) = min_rtt_nanos else {
            return self.config.min_qps;
        };
        if min_rtt_nanos == 0 {
            return self.config.min_qps;
        }

        let window_secs = self.config.window.as_secs_f64();
        let mut qps = (total_count as f64 / window_secs).max(self.config.min_qps);

        let min_rtt_secs = min_rtt_nanos as f64 / 1_000_000_000.0;
        let max_pass = window_secs / min_rtt_secs;
        if max_pass < qps {
            qps = max_pass;
        }

        qps.max(self.config.min_qps)
    }
}

impl Default for BbrLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Limiter for BbrLimiter {
    fn allow(&self) -> bool {
        BbrLimiter::allow(self)
    }

    fn wait<'a>(&'a self) -> BoxFuture<'a, Result<(), RateLimitError>> {
        Box::pin(async move {
            loop {
                if self.state.lock().unwrap().closed {
                    return Err(RateLimitError::Limited);
                }
                if BbrLimiter::allow(self) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

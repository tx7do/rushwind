//! Vegas-style circuit breaker for the RushWind circuitbreaker
//! contract.
//!
//! Inspired by TCP Vegas congestion control: the breaker compares the
//! observed RTT against a baseline (the minimum observed). When the
//! inflation `(currentRTT - baseRTT) / baseRTT` exceeds
//! [`VegasOptions::alpha`], the circuit degrades to open; when it
//! recovers under [`VegasOptions::beta`], it heals through half-open.
//! This detects downstream degradation early — before hard failures —
//! from latency inflation alone.
//!
//! The primary input is [`VegasBreaker::record_latency`]; outlier
//! samples below `min_rtt` / above `max_rtt` are filtered, and the
//! first `warmup_samples` records only prime the baseline before
//! evaluation starts (the warmup gate). `mark_failure` treats a
//! repeated failure as maximal latency by design, and a half-open
//! trial confirming recovery closes the circuit.
//!
//! One admission nuance keeps the healing path reachable: an open
//! circuit admits a probe every [`VegasOptions::probe_stride`]
//! `allow` calls. Full rejection would leave `execute`-style callers
//! unable to produce the latency sample inflation recovery needs, and
//! the circuit would stay open forever. A manually `close()`d breaker
//! rejects probes too — the operator override is absolute.
//!
//! # Testing
//!
//! The engine is in-process; its conformance suite runs as ordinary
//! unit tests.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::Duration;

use rushwind_circuitbreaker::{CircuitBreaker, CircuitError, State};

/// The degrade threshold: RTT inflation over `alpha` opens. Default
/// 0.5.
const DEFAULT_ALPHA: f64 = 0.5;
/// The heal threshold: inflation under `beta` heals. Default 0.3.
const DEFAULT_BETA: f64 = 0.3;
/// The samples recorded before evaluation begins. Default 10.
const DEFAULT_WARMUP_SAMPLES: u64 = 10;
/// The minimum plausible RTT; smaller samples are filtered. Default
/// 1 ms.
const DEFAULT_MIN_RTT: Duration = Duration::from_millis(1);
/// The maximum plausible RTT; larger samples are filtered. Default
/// 30 s.
const DEFAULT_MAX_RTT: Duration = Duration::from_secs(30);
/// How often an open circuit admits a probe. Default 16.
const DEFAULT_PROBE_STRIDE: u64 = 16;
/// The exponential smoothing weight on the previous current RTT —
/// the `0.875 / 0.125` EWMA pair.
const SMOOTHING_PREVIOUS: f64 = 0.875;
const SMOOTHING_NEW: f64 = 0.125;

/// The Vegas engine's settings.
#[derive(Debug, Clone)]
pub struct VegasOptions {
    /// Degrade when `(currentRTT - baseRTT) / baseRTT > alpha`.
    /// Default 0.5.
    pub alpha: f64,
    /// Heal when inflation drops under `beta`; `beta < alpha` gives
    /// hysteresis. Default 0.3.
    pub beta: f64,
    /// The samples recorded before evaluation begins. Default 10.
    pub warmup_samples: u64,
    /// The minimum plausible RTT. Default 1 ms.
    pub min_rtt: Duration,
    /// The maximum plausible RTT (outlier filter). Default 30 s.
    pub max_rtt: Duration,
    /// Every `probe_stride`-th [`CircuitBreaker::allow`] call on an
    /// open circuit is admitted as a probe. Without one, a caller
    /// driving the breaker through `execute` can never produce the
    /// latency sample the healing path needs — full rejection leaves
    /// inflation frozen above `alpha` and the circuit open forever.
    /// Default 16.
    pub probe_stride: u64,
}

impl Default for VegasOptions {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            beta: DEFAULT_BETA,
            warmup_samples: DEFAULT_WARMUP_SAMPLES,
            min_rtt: DEFAULT_MIN_RTT,
            max_rtt: DEFAULT_MAX_RTT,
            probe_stride: DEFAULT_PROBE_STRIDE,
        }
    }
}

struct VegasState {
    base_rtt_nanos: u64,
    current_rtt_nanos: u64,
    sample_count: u64,
    state: rushwind_circuitbreaker::State,
    closed: bool,
    /// Counts `allow` calls made while open; every `probe_stride`-th
    /// one is admitted as a probe.
    probe_gate: u64,
}

/// A Vegas-inspired latency-based circuit breaker.
pub struct VegasBreaker {
    state: Mutex<VegasState>,
    alpha: f64,
    beta: f64,
    warmup_samples: u64,
    min_rtt_nanos: u64,
    max_rtt_nanos: u64,
    probe_stride: u64,
}

impl VegasBreaker {
    /// Builds a Vegas breaker with the default options.
    pub fn new() -> Self {
        Self::with_options(VegasOptions::default())
    }

    /// Builds a Vegas breaker with explicit options.
    pub fn with_options(options: VegasOptions) -> Self {
        Self {
            state: Mutex::new(VegasState {
                base_rtt_nanos: 0,
                current_rtt_nanos: 0,
                sample_count: 0,
                state: State::Closed,
                closed: false,
                probe_gate: 0,
            }),
            alpha: options.alpha,
            beta: options.beta,
            warmup_samples: options.warmup_samples,
            min_rtt_nanos: options.min_rtt.as_nanos() as u64,
            max_rtt_nanos: options.max_rtt.as_nanos() as u64,
            probe_stride: options.probe_stride.max(1),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, CircuitError> {
        #[derive(serde::Deserialize)]
        struct VegasSettings {
            alpha: Option<f64>,
            beta: Option<f64>,
            warmup_samples: Option<u64>,
            min_rtt_ms: Option<u64>,
            max_rtt_ms: Option<u64>,
            probe_stride: Option<u64>,
        }
        let settings: VegasSettings = serde_json::from_value(settings)
            .map_err(|e| CircuitError::Failed(format!("settings parse: {e}")))?;
        let defaults = VegasOptions::default();
        Ok(Self::with_options(VegasOptions {
            alpha: settings.alpha.unwrap_or(defaults.alpha),
            beta: settings.beta.unwrap_or(defaults.beta),
            warmup_samples: settings.warmup_samples.unwrap_or(defaults.warmup_samples),
            min_rtt: Duration::from_millis(settings.min_rtt_ms.unwrap_or(1)),
            max_rtt: Duration::from_millis(settings.max_rtt_ms.unwrap_or(30_000)),
            probe_stride: settings.probe_stride.unwrap_or(defaults.probe_stride),
        }))
    }

    /// Reports the latency of a completed request — the primary input
    /// for the Vegas algorithm. A half-open trial confirming recovery
    /// closes the circuit here.
    pub fn record_latency(&self, rtt: Duration) {
        let mut state = self.state.lock().unwrap();
        if state.state == State::HalfOpen {
            state.state = rushwind_circuitbreaker::State::Closed;
        }
        self.update_rtt(&mut state, rtt);
        self.evaluate(&mut state);
    }

    /// The current baseline RTT, for observability.
    pub fn base_rtt(&self) -> Duration {
        Duration::from_nanos(self.state.lock().unwrap().base_rtt_nanos)
    }

    /// The smoothed current RTT, for observability.
    pub fn current_rtt(&self) -> Duration {
        Duration::from_nanos(self.state.lock().unwrap().current_rtt_nanos)
    }

    /// The current RTT inflation ratio (0 when the baseline is not
    /// established), for observability.
    pub fn inflation(&self) -> f64 {
        let state = self.state.lock().unwrap();
        if state.base_rtt_nanos == 0 {
            return 0.0;
        }
        ((state.current_rtt_nanos as f64 - state.base_rtt_nanos as f64)
            / state.base_rtt_nanos as f64)
            .max(0.0)
    }

    /// The configured maximum plausible RTT as a duration.
    fn max_rtt(&self) -> Duration {
        Duration::from_nanos(self.max_rtt_nanos)
    }

    /// Updates the RTT state: filter outliers, count the sample,
    /// smooth the current RTT (0.875 previous + 0.125 new), and lower
    /// the baseline to new minima.
    fn update_rtt(&self, state: &mut VegasState, rtt: Duration) {
        let rtt_nanos = rtt.as_nanos() as u64;
        if rtt_nanos < self.min_rtt_nanos || rtt_nanos > self.max_rtt_nanos {
            return;
        }
        state.sample_count += 1;
        state.current_rtt_nanos = if state.current_rtt_nanos == 0 {
            rtt_nanos
        } else {
            (state.current_rtt_nanos as f64 * SMOOTHING_PREVIOUS + rtt_nanos as f64 * SMOOTHING_NEW)
                as u64
        };
        if state.base_rtt_nanos == 0 || rtt_nanos < state.base_rtt_nanos {
            state.base_rtt_nanos = rtt_nanos;
        }
    }

    /// Evaluates the breaker: gate on warmup, then drive the state
    /// machine on the inflation ratio.
    fn evaluate(&self, state: &mut VegasState) {
        if state.sample_count < self.warmup_samples || state.base_rtt_nanos == 0 {
            return;
        }
        let inflation = (state.current_rtt_nanos as f64 - state.base_rtt_nanos as f64)
            / state.base_rtt_nanos as f64;

        match state.state {
            State::Closed => {
                if inflation > self.alpha {
                    state.state = rushwind_circuitbreaker::State::Open;
                }
            }
            State::Open => {
                // Vegas heals on latency recovery rather than a fixed
                // timer, transitioning through HalfOpen.
                if inflation < self.beta {
                    state.state = rushwind_circuitbreaker::State::HalfOpen;
                }
            }
            State::HalfOpen => {
                if inflation < self.beta {
                    state.state = rushwind_circuitbreaker::State::Closed;
                } else if inflation > self.alpha {
                    state.state = rushwind_circuitbreaker::State::Open;
                }
            }
        }
    }
}

impl Default for VegasBreaker {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreaker for VegasBreaker {
    fn allow(&self) -> std::result::Result<(), CircuitError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(CircuitError::Open);
        }
        if state.state == rushwind_circuitbreaker::State::Open {
            // An open circuit still admits a probe every
            // `probe_stride` calls: the healing path runs on
            // `record_latency` samples, and a caller driving the
            // breaker through `execute` can only produce one by being
            // admitted. Without the probe the inflation stays frozen
            // above `alpha` and the circuit never heals.
            state.probe_gate = state.probe_gate.wrapping_add(1);
            if state.probe_gate % self.probe_stride != 0 {
                return Err(CircuitError::Open);
            }
        }
        Ok(())
    }

    /// Records a zero latency — filtered as an outlier under the
    /// default minimum RTT, so this is effectively a no-op for the
    /// RTT state; prefer
    /// [`VegasBreaker::record_latency`].
    fn mark_success(&self) {
        self.record_latency(Duration::ZERO);
    }

    /// A half-open trial failure re-opens; repeated failures degrade —
    /// treated as maximal latency.
    fn mark_failure(&self) {
        let mut state = self.state.lock().unwrap();
        if state.state == State::HalfOpen {
            state.state = rushwind_circuitbreaker::State::Open;
            return;
        }
        self.update_rtt(&mut state, self.max_rtt());
        self.evaluate(&mut state);
    }

    fn state(&self) -> rushwind_circuitbreaker::State {
        self.state.lock().unwrap().state
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

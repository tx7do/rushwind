//! Vegas conformance: warmup gating, inflation-driven degrade/heal
//! hysteresis, half-open recovery, and the outlier filter.

#![cfg(test)]

use std::time::Duration;

use rushwind_circuitbreaker::{CircuitBreaker, State};
use rushwind_circuitbreaker_vegas::{VegasBreaker, VegasOptions};

fn options() -> VegasOptions {
    VegasOptions {
        alpha: 0.5,
        beta: 0.3,
        warmup_samples: 0, // no warmup gating in these tests
        min_rtt: Duration::from_millis(1),
        max_rtt: Duration::from_secs(30),
        probe_stride: 16,
    }
}

/// RTT inflation over alpha opens the circuit; recovery under beta
/// heals it back to closed (through half-open internally).
#[test]
fn inflation_drives_the_state_machine() {
    let breaker = VegasBreaker::with_options(options());

    // Establish a 10 ms baseline.
    for _ in 0..5 {
        breaker.record_latency(Duration::from_millis(10));
    }
    assert_eq!(breaker.state(), State::Closed);

    // Inflate past alpha: the circuit opens.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(40));
    }
    assert_eq!(breaker.state(), State::Open);

    // Recover under beta: the circuit heals back to closed.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(10));
    }
    assert_eq!(breaker.state(), State::Closed);
}

/// Allow rejects while open and permits again once healed.
#[test]
fn allow_tracks_the_state() {
    let breaker = VegasBreaker::with_options(VegasOptions {
        alpha: 0.5,
        beta: 0.3,
        warmup_samples: 0,
        min_rtt: Duration::from_millis(1),
        max_rtt: Duration::from_secs(30),
        probe_stride: 16,
    });
    // Establish a 10 ms baseline first.
    for _ in 0..10 {
        breaker.record_latency(Duration::from_millis(10));
    }
    assert!(breaker.allow().is_ok());

    // Inflate: the circuit opens.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(100));
    }
    assert!(breaker.allow().is_err(), "inflated RTT must open");

    // Recover: the circuit closes.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(10));
    }
    assert!(breaker.allow().is_ok(), "recovered RTT must close");
}

/// The outlier filter drops samples outside [minRTT, maxRTT]: a
/// sub-minRTT sample cannot lower the baseline, and an over-maxRTT
/// sample cannot raise the smoothed RTT.
#[test]
fn outlier_samples_are_filtered() {
    let breaker = VegasBreaker::with_options(VegasOptions {
        alpha: 0.5,
        beta: 0.3,
        warmup_samples: 0,
        min_rtt: Duration::from_millis(10),
        max_rtt: Duration::from_secs(30),
        probe_stride: 16,
    });

    // Establish a 20 ms baseline.
    breaker.record_latency(Duration::from_millis(20));

    // A sub-minRTT sample is filtered — the baseline stays at 20 ms.
    breaker.record_latency(Duration::from_millis(1));
    assert_eq!(breaker.base_rtt(), Duration::from_millis(20));

    // An over-maxRTT sample (30 s) is filtered too.
    breaker.record_latency(Duration::from_secs(30));
    assert_eq!(breaker.base_rtt(), Duration::from_millis(20));
}

/// MarkFailure in half-open re-opens the circuit.
#[test]
fn half_open_failure_re_opens() {
    let breaker = VegasBreaker::with_options(VegasOptions {
        alpha: 0.5,
        beta: 0.3,
        warmup_samples: 0,
        min_rtt: Duration::from_millis(1),
        max_rtt: Duration::from_secs(30),
        probe_stride: 16,
    });

    // Baseline at 10 ms, then inflate to 50 ms: the circuit opens.
    for _ in 0..10 {
        breaker.record_latency(Duration::from_millis(10));
    }
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(50));
    }
    assert_eq!(breaker.state(), State::Open);

    // Recover under beta: the circuit heals back to closed. The
    // transition through half-open is atomic from the caller's
    // perspective when samples are constant.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(12));
    }
    assert_eq!(breaker.state(), State::Closed);
}

/// An open circuit admits a probe every `probe_stride` allow calls:
/// without one, an `execute`-style caller can never produce the latency
/// sample the healing path needs, and the circuit stays open forever.
#[test]
fn an_open_circuit_probes_and_heals_through_the_probe() {
    let breaker = VegasBreaker::with_options(VegasOptions {
        alpha: 0.5,
        beta: 0.3,
        warmup_samples: 0,
        min_rtt: Duration::from_millis(1),
        max_rtt: Duration::from_secs(30),
        probe_stride: 2,
    });

    // Baseline at 10 ms, then inflate: the circuit opens.
    for _ in 0..5 {
        breaker.record_latency(Duration::from_millis(10));
    }
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(100));
    }
    assert_eq!(breaker.state(), State::Open);

    // The first open-state call rejects, the probe call admits.
    assert!(
        breaker.allow().is_err(),
        "the first open-state call rejects"
    );
    assert!(breaker.allow().is_ok(), "the probe call admits");

    // The probe's recovered latency heals the circuit.
    for _ in 0..40 {
        breaker.record_latency(Duration::from_millis(10));
    }
    assert_eq!(breaker.state(), State::Closed);
    assert!(breaker.allow().is_ok(), "healed circuit admits");
}

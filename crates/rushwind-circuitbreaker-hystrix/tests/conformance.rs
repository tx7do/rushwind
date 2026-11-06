//! Hystrix conformance: volume-threshold gating, error-rate tripping,
//! sleep-window half-open, single-trial recovery, and close semantics.

#![cfg(test)]

use std::time::Duration;

use rushwind_circuitbreaker::CircuitBreaker;
use rushwind_circuitbreaker_hystrix::HystrixBreaker;
use rushwind_circuitbreaker_hystrix::HystrixOptions;

fn breaker() -> HystrixBreaker {
    HystrixBreaker::with_options(HystrixOptions {
        request_volume_threshold: 5,
        error_threshold: 0.5,
        sleep_window: Duration::from_millis(100),
        window: Duration::from_secs(10),
        bucket_count: 10,
    })
}

/// Below the request-volume threshold, the breaker stays closed
/// regardless of error rate.
#[test]
fn volume_threshold_gates_trip() {
    let b = breaker();
    // 4 failures — below the threshold of 5.
    for _ in 0..4 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Closed);
}

/// Error rate meeting the threshold trips the breaker open.
#[test]
fn error_rate_trips_open() {
    let b = breaker();
    // 5 failures = 100% error rate at 5 volume.
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);
}

/// While open, requests are rejected. After the sleep window, the
/// breaker half-opens for a trial.
#[test]
fn open_rejects_then_half_opens() {
    let b = breaker();
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);

    // Immediately after: still rejects.
    assert!(b.allow().is_err(), "open circuit must reject");

    // After the sleep window: half-open allows a trial.
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(b.state(), rushwind_circuitbreaker::State::HalfOpen);
}

/// A half-open trial success closes the circuit and resets the
/// buckets.
#[test]
fn half_open_success_closes() {
    let b = breaker();
    // Trip open.
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);

    // Sleep past the window, then trial.
    std::thread::sleep(Duration::from_millis(150));
    b.allow().expect("trial must be allowed");
    b.mark_success();

    // The circuit is closed and the buckets are reset.
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Closed);

    // A fresh failure cycle starts from clean state.
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);
}

/// A half-open trial failure re-opens.
#[test]
fn half_open_failure_re_opens() {
    let b = breaker();
    // Trip open with failures.
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);

    // Sleep past the window.
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(b.state(), rushwind_circuitbreaker::State::HalfOpen);

    // Trial fails — re-open and restart the sleep timer.
    b.allow().expect("trial must be allowed");
    b.mark_failure();
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);
    assert!(b.allow().is_err(), "the re-opened circuit rejects");
}

/// A half-open trial whose outcome never arrives (the caller dropped
/// the future, panicked between allow and mark, or forgot the mark) is
/// reclaimed after one sleep window — the breaker admits a fresh trial
/// instead of rejecting everything forever.
#[test]
fn a_lost_half_open_trial_is_reclaimed() {
    let b = breaker();
    // Trip open with failures.
    for _ in 0..5 {
        b.allow().expect("allow must succeed");
        b.mark_failure();
    }
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);

    // Sleep past the window: half-open, one trial admitted.
    std::thread::sleep(Duration::from_millis(150));
    b.allow().expect("trial must be allowed");
    assert!(b.allow().is_err(), "a second concurrent trial rejects");

    // The trial never reports. Sleep past the sleep window again: the
    // lost trial is reclaimed and a fresh one admits.
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(b.state(), rushwind_circuitbreaker::State::HalfOpen);
    b.allow()
        .expect("the reclaimed slot must admit a fresh trial");
}

//! Leaky-bucket conformance: the burst-shaped backlog, the pacing,
//! close semantics, and the invalid-config guards.

#![cfg(test)]

use std::time::Duration;

use rushwind_ratelimit::Limiter;
use rushwind_ratelimit_leakybucket::{LeakyBucket, LeakyBucketOptions};

fn limiter(capacity: f64, drain_rate: f64) -> LeakyBucket {
    LeakyBucket::new(LeakyBucketOptions {
        capacity,
        drain_rate,
    })
    .expect("valid config")
}

/// The initial backlog is empty: `capacity` admissions go through in a
/// burst (they fill the backlog), then refusals.
#[test]
fn backlog_fills_then_refuses() {
    let limiter = limiter(3.0, 1.0);
    assert!(limiter.allow(), "backlog unit 1");
    assert!(limiter.allow(), "backlog unit 2");
    assert!(limiter.allow(), "backlog unit 3");
    assert!(!limiter.allow(), "the full backlog refuses");
}

/// The output is paced: after the backlog fills, every admission costs
/// a full drain tick, and consecutive waits accumulate — N waits cost
/// N ticks, so the steady-state rate is exactly `drain_rate` no matter
/// how fast the requests arrive.
#[tokio::test]
async fn wait_paces_at_the_drain_rate() {
    let limiter = limiter(2.0, 50.0);

    assert!(limiter.allow(), "backlog unit 1");
    assert!(limiter.allow(), "backlog unit 2 — the backlog is full");

    let started = std::time::Instant::now();
    limiter.wait().await.expect("wait 1");
    let first = started.elapsed();
    let started = std::time::Instant::now();
    limiter.wait().await.expect("wait 2");
    let second = started.elapsed();

    assert!(
        first >= Duration::from_millis(15),
        "each wait drains one unit at 50/s, took {first:?}"
    );
    assert!(
        second >= Duration::from_millis(15),
        "the second wait costs another full tick — pacing, not banking, took {second:?}"
    );
}

/// Under sustained load the throughput is the drain rate: the burst
/// buys a head start, and from then on every extra unit waits its
/// tick — the backlog never spends faster than it drains.
#[tokio::test]
async fn sustained_load_is_capped_at_the_drain_rate() {
    let limiter = limiter(2.0, 50.0);

    assert!(limiter.allow());
    assert!(limiter.allow());

    let started = std::time::Instant::now();
    for i in 0..3 {
        limiter.wait().await.expect("wait under load");
        let _ = i;
    }
    let total = started.elapsed();
    assert!(
        total >= Duration::from_millis(45),
        "three units at 50/s must take at least three ticks, took {total:?}"
    );
}

/// Close refuses allow and wakes waiters with `Limited`.
#[tokio::test]
async fn close_refuses_and_wakes_waiters() {
    let limiter = std::sync::Arc::new(limiter(1.0, 0.5));
    assert!(limiter.allow(), "the backlog's single unit");

    let waiter = {
        let limiter = std::sync::Arc::clone(&limiter);
        tokio::spawn(async move { limiter.wait().await })
    };
    // Let the waiter reach its sleep (the drain window here is seconds).
    tokio::time::sleep(Duration::from_millis(20)).await;

    limiter.close();

    let result = waiter.await.expect("waiter task joins");
    assert!(
        matches!(result, Err(rushwind_ratelimit::RateLimitError::Limited)),
        "a closed limiter answers its waiter with Limited, got {result:?}"
    );
    assert!(!limiter.allow(), "a closed limiter refuses");
}

/// An invalid configuration fails construction.
#[test]
fn invalid_config_fails() {
    assert!(LeakyBucket::new(LeakyBucketOptions {
        capacity: 10.0,
        drain_rate: 0.0,
    })
    .is_err());
    // A bucket that cannot hold a single admission refuses everything.
    assert!(LeakyBucket::new(LeakyBucketOptions {
        capacity: 0.5,
        drain_rate: 10.0,
    })
    .is_err());
}

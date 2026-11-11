//! Sliding-window conformance: the limit holds inside the window, the
//! boundary is exact (no fixed-window double budget), close semantics,
//! and the invalid-config guards.

#![cfg(test)]

use std::time::Duration;

use rushwind_ratelimit::Limiter;
use rushwind_ratelimit_slidingwindow::{SlidingWindow, SlidingWindowOptions};

fn limiter(limit: usize, window: Duration) -> SlidingWindow {
    SlidingWindow::new(SlidingWindowOptions { limit, window }).expect("valid config")
}

/// The limit holds: exactly `limit` admissions inside one window, then
/// refusals.
#[test]
fn limit_holds_inside_the_window() {
    let limiter = limiter(3, Duration::from_millis(200));
    assert!(limiter.allow(), "admission 1");
    assert!(limiter.allow(), "admission 2");
    assert!(limiter.allow(), "admission 3");
    assert!(!limiter.allow(), "the window is full");
    assert!(!limiter.allow(), "still full — nothing slid out");
}

/// The boundary is exact: a sliding window never hands out the
/// fixed-window double budget (the seam where two adjacent fixed
/// windows together admit 2×limit), and each entry slides out exactly
/// `window` after its own timestamp.
#[test]
fn the_boundary_is_exact_not_a_fixed_window() {
    let limiter = limiter(2, Duration::from_millis(100));

    assert!(limiter.allow(), "admitted at t0");
    // Stagger the second admission so the two entries slide at
    // different instants.
    std::thread::sleep(Duration::from_millis(60));
    assert!(limiter.allow(), "admitted at t60");
    assert!(
        !limiter.allow(),
        "half a window later both original admissions are still in the log"
    );

    std::thread::sleep(Duration::from_millis(45));
    assert!(
        limiter.allow(),
        "at t105 the t0 admission has slid out of the log"
    );
    assert!(
        !limiter.allow(),
        "the t60 admission has not yet — only the expired slot freed"
    );
}

/// Wait blocks until the oldest in-window admission slides out, then
/// succeeds.
#[tokio::test]
async fn wait_acquires_after_the_window_slides() {
    let limiter = limiter(1, Duration::from_millis(50));
    assert!(limiter.allow(), "the window's single admission");

    let started = std::time::Instant::now();
    limiter.wait().await.expect("wait must acquire");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "wait must block at least until the window slides, took {elapsed:?}"
    );
}

/// Close refuses allow and wakes waiters with `Limited`.
#[tokio::test]
async fn close_refuses_and_wakes_waiters() {
    let limiter = std::sync::Arc::new(limiter(1, Duration::from_millis(5_000)));
    assert!(limiter.allow(), "the window's single admission");

    let waiter = {
        let limiter = std::sync::Arc::clone(&limiter);
        tokio::spawn(async move { limiter.wait().await })
    };
    // Let the waiter reach its sleep (the window here is seconds).
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
    assert!(SlidingWindow::new(SlidingWindowOptions {
        limit: 0,
        window: Duration::from_secs(1)
    })
    .is_err());
    assert!(SlidingWindow::new(SlidingWindowOptions {
        limit: 10,
        window: Duration::ZERO
    })
    .is_err());
}

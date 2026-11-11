//! Clock-injection pins: window rotation driven by an advanced fake
//! clock — no real sleeps.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushwind_ratelimit::Clock;
use rushwind_ratelimit_bbr::{BbrLimiter, BbrOptions};

/// A hand-advanced monotonic clock.
struct FakeClock(Mutex<Instant>);

impl FakeClock {
    fn shared() -> Arc<Self> {
        Arc::new(Self(Mutex::new(Instant::now())))
    }

    fn advance(&self, by: Duration) {
        *self.0.lock().expect("fake clock") += by;
    }

    fn clock(self: &Arc<Self>) -> Clock {
        let inner = Arc::clone(self);
        Arc::new(move || *inner.0.lock().expect("fake clock"))
    }
}

fn limiter(clock: Clock) -> BbrLimiter {
    BbrLimiter::with_clock(
        BbrOptions {
            cpu_threshold: 0.8,
            window: Duration::from_secs(1),
            bucket_count: 4,
            min_qps: 1.0,
        },
        clock,
    )
}

/// A window that fully lapses on the injected clock forgets its
/// completions: the QPS estimate falls back to the floor and
/// `max_inflight` with it — no real second waited.
#[test]
fn clock_rotation_expires_the_window() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock());

    // Seed the window with fast completions: with RTTs of 1 ms the
    // estimate `window / minRTT` is far above the floor.
    for _ in 0..10 {
        assert!(limiter.allow());
        limiter.done(Duration::from_millis(1));
    }
    let hot = limiter.max_inflight();
    assert!(
        hot > 1,
        "fast completions raise the cap above the floor (got {hot})"
    );

    // Advance past the whole window: every bucket lapses, the estimate
    // resets, the cap falls back to the floor of one.
    fake.advance(Duration::from_secs(2));
    assert!(limiter.allow());
    limiter.done(Duration::from_millis(1));
    assert_eq!(
        limiter.max_inflight(),
        1,
        "a lapsed window forgets its completions"
    );
}

/// Completions land in the bucket the injected clock addresses:
/// mid-window samples and post-window samples do not collide into one
/// bucket's average.
#[test]
fn clock_buckets_advance_with_the_clock() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock());

    // Ten fast completions now, then the clock moves a full window:
    // a slow completion afterwards is the only sample left standing.
    for _ in 0..10 {
        assert!(limiter.allow());
        limiter.done(Duration::from_millis(1));
    }
    let hot = limiter.max_inflight();
    assert!(hot > 1, "ten fast completions raise the cap (got {hot})");

    fake.advance(Duration::from_millis(1_200));
    assert!(limiter.allow());
    limiter.done(Duration::from_millis(500));
    assert!(
        limiter.max_inflight() < hot,
        "a lone 500ms-RTT sample must throttle below the fast window (got {} < {hot})",
        limiter.max_inflight()
    );
}

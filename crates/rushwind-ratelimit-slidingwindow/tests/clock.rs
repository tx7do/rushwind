//! Clock-injection pins: window sliding driven by an advanced fake
//! clock — no real sleeps.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushwind_ratelimit::Clock;
use rushwind_ratelimit_slidingwindow::{SlidingWindow, SlidingWindowOptions};

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

fn limiter(clock: Clock, limit: usize, window: Duration) -> SlidingWindow {
    SlidingWindow::with_clock(SlidingWindowOptions { limit, window }, clock)
        .expect("parameters valid")
}

/// The log slides exactly at the window boundary on the injected
/// clock: each entry expires at `timestamp + window`, not before —
/// and only then.
#[test]
fn clock_driven_window_slide() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock(), 2, Duration::from_millis(100));

    assert!(limiter.allow(), "admitted at t0");
    fake.advance(Duration::from_millis(50));
    assert!(limiter.allow(), "admitted at t50");
    assert!(!limiter.allow(), "the window is full");

    fake.advance(Duration::from_millis(49));
    assert!(
        !limiter.allow(),
        "at t99 the t0 entry is one millisecond short of the boundary"
    );

    fake.advance(Duration::from_millis(1));
    assert!(limiter.allow(), "at t100 the t0 entry slid out exactly now");
    assert!(
        !limiter.allow(),
        "the t50 admission has not yet slid — the budget went to the new arrival"
    );
}

/// Half-steps accumulate nothing: unlike the token bucket there is no
/// fractional credit — a refusal leaves no budget behind, and the
/// budget after expiry is `limit`, not the elapsed time's worth.
#[test]
fn clock_driven_no_banking() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock(), 1, Duration::from_millis(100));

    assert!(limiter.allow());

    // Three quarter-windows pass with no admissions: nothing banks.
    for _ in 0..3 {
        fake.advance(Duration::from_millis(25));
        assert!(
            !limiter.allow(),
            "no credit accrues while the entry is in the log"
        );
    }

    fake.advance(Duration::from_millis(25));
    assert!(limiter.allow(), "the entry expired at exactly t+100");
    assert!(!limiter.allow(), "the budget is one again, not five");
}

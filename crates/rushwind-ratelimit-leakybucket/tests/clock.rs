//! Clock-injection pins: drain accounting driven by an advanced fake
//! clock — no real sleeps.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushwind_ratelimit::Clock;
use rushwind_ratelimit_leakybucket::{LeakyBucket, LeakyBucketOptions};

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

fn limiter(clock: Clock, capacity: f64, drain_rate: f64) -> LeakyBucket {
    LeakyBucket::with_clock(
        LeakyBucketOptions {
            capacity,
            drain_rate,
        },
        clock,
    )
    .expect("parameters valid")
}

/// Drain is elapsed-driven on the injected clock: a full backlog
/// refuses until the clock advances past a drain tick, then admits
/// again — no real time passed in this test.
#[test]
fn clock_driven_drain() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock(), 1.0, 10.0);

    assert!(limiter.allow());
    assert!(!limiter.allow(), "the full backlog refuses");

    fake.advance(Duration::from_millis(50));
    assert!(!limiter.allow(), "half a tick drains half a unit — refused");

    fake.advance(Duration::from_millis(60));
    assert!(limiter.allow(), "a full tick drains one unit");
}

/// Drain caps at empty on the injected clock: an idle limiter never
/// goes negative, so credit does not accumulate while idle.
#[test]
fn clock_driven_drain_caps_at_empty() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock(), 2.0, 100.0);

    assert!(limiter.allow());
    assert!(limiter.allow());
    assert!(!limiter.allow(), "backlog full");

    fake.advance(Duration::from_secs(10));
    assert!(limiter.allow(), "drained to empty: unit 1");
    assert!(
        limiter.allow(),
        "unit 2 — the drained backlog, not banked credit"
    );
    assert!(
        !limiter.allow(),
        "ten idle seconds still drain only to empty — the third admission has nothing to draw on"
    );
}

/// The admitted pace is one per drain tick: with a unit capacity the
/// bucket holds exactly one in-flight unit, and each full tick admits
/// exactly one more — no matter how far the clock advances between
/// attempts (drain caps at empty, so idle never banks a second unit).
#[test]
fn the_admitted_pace_is_one_per_tick() {
    let fake = FakeClock::shared();
    let limiter = limiter(fake.clock(), 1.0, 10.0);

    assert!(limiter.allow(), "the empty bucket admits instantly");
    assert!(!limiter.allow(), "the unit-capacity backlog is full");

    fake.advance(Duration::from_millis(100));
    assert!(limiter.allow());
    assert!(!limiter.allow());
    fake.advance(Duration::from_millis(100));
    assert!(limiter.allow());
    assert!(!limiter.allow());
}

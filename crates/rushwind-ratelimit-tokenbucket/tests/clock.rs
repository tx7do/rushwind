//! Clock-injection pins: refill accounting driven by an advanced fake
//! clock — no real sleeps.

#![cfg(test)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rushwind_ratelimit::Clock;
use rushwind_ratelimit_tokenbucket::{TokenBucket, TokenBucketOptions};

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

fn bucket(clock: Clock, rate: f64, burst: f64) -> TokenBucket {
    TokenBucket::with_clock(TokenBucketOptions { rate, burst }, clock).expect("parameters valid")
}

/// Refill is elapsed-driven on the injected clock: an emptied bucket
/// refuses until the clock advances past the refill tick, then admits
/// again — no real time passed in this test.
#[test]
fn clock_driven_refill() {
    let fake = FakeClock::shared();
    let bucket = bucket(fake.clock(), 10.0, 1.0);

    assert!(bucket.allow(), "starts full");
    assert!(!bucket.allow(), "burst of one is spent");

    fake.advance(Duration::from_millis(50));
    assert!(!bucket.allow(), "half a tick is half a token — refused");

    fake.advance(Duration::from_millis(60));
    assert!(bucket.allow(), "a full tick refills one token");
}

/// Refill caps at burst on the injected clock: an idle bucket never
/// banks more than its burst.
#[test]
fn clock_driven_refill_caps_at_burst() {
    let fake = FakeClock::shared();
    let bucket = bucket(fake.clock(), 100.0, 2.0);

    assert!(bucket.allow());
    assert!(bucket.allow());
    assert!(!bucket.allow(), "burst spent");

    fake.advance(Duration::from_secs(10));
    assert!(bucket.allow());
    assert!(bucket.allow());
    assert!(!bucket.allow(), "ten idle seconds still cap at two tokens");
}

/// Fractional tokens accumulate on the injected clock: two half-ticks
/// make one admission.
#[test]
fn clock_driven_fractional_accumulation() {
    let fake = FakeClock::shared();
    let bucket = bucket(fake.clock(), 1.0, 1.0);

    assert!(bucket.allow());
    assert!(!bucket.allow());

    fake.advance(Duration::from_millis(500));
    assert!(!bucket.allow(), "0.5 tokens is not an admission");

    fake.advance(Duration::from_millis(500));
    assert!(bucket.allow(), "two half-ticks accumulate to one token");
}

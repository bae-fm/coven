//! Wall-clock source, injected so consumers read "now" deterministically in
//! tests.
//!
//! Production wires [`SystemClock`] (real `Utc::now()`); tests construct a
//! deterministic fake ([`FixedClock`] / [`SteppingClock`]) and pass it to the
//! unit under test.
//!
//! Sibling to [`crate::sync::hlc::Hlc`]'s `wall_clock`: that one returns `u64`
//! millis for hybrid-logical-clock math, this one returns `DateTime<Utc>` for
//! `created_at` / `updated_at` / expiry comparisons. Both are injected wall
//! sources rooted at the same composition point; neither subsumes the other.

use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Wall-clock source. Returns a full `DateTime<Utc>`; callers derive
/// `.timestamp()` / `.to_rfc3339()` as they need.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Shared handle to a clock. Held by `Clone` types (`Database`,
/// `LibraryManager`) so they clone the handle, not the implementation.
pub type ClockRef = Arc<dyn Clock>;

/// Production clock: real wall time.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

// Test clock fakes are exposed to downstream crates' tests via the `test-utils`
// feature, so any crate that consumes `Clock` tests against the same fakes
// instead of mirroring them.
#[cfg(any(test, feature = "test-utils"))]
pub use fakes::{FixedClock, SteppingClock};

#[cfg(any(test, feature = "test-utils"))]
mod fakes {
    use super::*;
    use chrono::Duration;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Every `now()` returns the same instant.
    pub struct FixedClock(pub DateTime<Utc>);

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    /// Advances a fixed delta per `now()` call: the first call returns `start`,
    /// the next `start + step`, and so on. For tests that assert ordering.
    pub struct SteppingClock {
        start: DateTime<Utc>,
        step: Duration,
        calls: AtomicU64,
    }

    impl SteppingClock {
        pub fn new(start: DateTime<Utc>, step: Duration) -> Self {
            Self {
                start,
                step,
                calls: AtomicU64::new(0),
            }
        }
    }

    impl Clock for SteppingClock {
        fn now(&self) -> DateTime<Utc> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            self.start + self.step * (n as i32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn fixed_clock_returns_same_instant() {
        let instant = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock = FixedClock(instant);
        assert_eq!(clock.now(), instant);
        assert_eq!(clock.now(), instant);
    }

    #[test]
    fn stepping_clock_advances_one_step_per_call() {
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock = SteppingClock::new(start, Duration::seconds(10));
        assert_eq!(clock.now(), start);
        assert_eq!(clock.now(), start + Duration::seconds(10));
        assert_eq!(clock.now(), start + Duration::seconds(20));
    }

    #[test]
    fn clock_is_usable_behind_the_shared_handle() {
        let instant = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock: ClockRef = Arc::new(FixedClock(instant));
        assert_eq!(clock.now(), instant);
    }
}

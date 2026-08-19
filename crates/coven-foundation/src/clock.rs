//! Wall-clock source, injected so consumers read "now" deterministically in
//! tests.
//!
//! Production wires [`SystemClock`] (real `Utc::now()`); tests construct a
//! deterministic fake ([`FixedClock`] / [`ClosureClock`]) and pass it to the
//! unit under test.
//!
//! The hybrid logical clock retains this same clock and derives epoch
//! milliseconds from it. Other consumers use the full
//! `DateTime<Utc>` for `created_at`, `updated_at`, and expiry comparisons.

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Wall-clock source. Returns a full `DateTime<Utc>`; callers derive
/// `.timestamp()` / `.to_rfc3339()` as they need.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Shared handle to a clock. Held by `Clone` types (`CovenHandle`,
/// `CovenReadHandle`) so they clone the handle, not the implementation.
pub type ClockRef = Arc<dyn Clock>;

/// A [`ClockRef`] reading epoch milliseconds from a test-supplied source, for
/// the hybrid-logical-clock tests that think in milliseconds rather than
/// `DateTime`s.
#[cfg(any(test, feature = "test-utils"))]
pub fn clock_from_millis(source: impl Fn() -> u64 + Send + Sync + 'static) -> ClockRef {
    Arc::new(ClosureClock(move || {
        let millis: i64 = source().try_into().expect("test clock millis fit in i64");
        DateTime::from_timestamp_millis(millis).expect("valid test clock instant")
    }))
}

/// Elapsed real time, for measuring how long a piece of work took.
///
/// Deliberately not a [`Clock`]. A clock answers "what instant is this" for
/// values the store keeps — commit stamps, `created_at`, expiry comparisons —
/// and tests pin it so those come out deterministic. Measuring a duration needs
/// the opposite guarantees: a reading that only moves forward, never jumps when
/// the system clock is corrected, and reports the real elapsed time even while a
/// pinned clock says no time has passed. Both live here so ambient time has one
/// owner.
///
/// Nothing durable derives from a stopwatch — only diagnostics read one — so how
/// long a run took never changes what it produces.
pub struct Stopwatch(Instant);

impl Stopwatch {
    /// Starts measuring from now.
    pub fn start() -> Self {
        Self(Instant::now())
    }

    /// Real time since [`start`](Self::start).
    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

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
pub use fakes::{ClosureClock, FixedClock};

#[cfg(any(test, feature = "test-utils"))]
mod fakes {
    use super::*;

    /// Delegates each read to a supplied test function.
    pub struct ClosureClock<F>(pub F);

    impl<F> Clock for ClosureClock<F>
    where
        F: Fn() -> DateTime<Utc> + Send + Sync,
    {
        fn now(&self) -> DateTime<Utc> {
            (self.0)()
        }
    }

    /// Every `now()` returns the same instant.
    pub struct FixedClock(pub DateTime<Utc>);

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn a_stopwatch_advances_with_real_time_under_a_pinned_clock() {
        let instant = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock = FixedClock(instant);
        let stopwatch = Stopwatch::start();

        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(clock.now(), instant);
        assert!(stopwatch.elapsed() >= Duration::from_millis(5));
    }

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
    fn clock_is_usable_behind_the_shared_handle() {
        let instant = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let clock: ClockRef = Arc::new(FixedClock(instant));
        assert_eq!(clock.now(), instant);
    }

    #[test]
    fn closure_clock_reads_the_supplied_source_each_time() {
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let calls = AtomicU64::new(0);
        let clock = ClosureClock(|| {
            let seconds = calls.fetch_add(1, Ordering::SeqCst) as i64;
            start + chrono::Duration::seconds(seconds)
        });

        assert_eq!(clock.now(), start);
        assert_eq!(clock.now(), start + chrono::Duration::seconds(1));
    }
}

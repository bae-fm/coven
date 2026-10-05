//! The wall clock that timestamps use (§7.2); the only reader of system time.

use std::sync::Arc;
use std::time::SystemTime;

/// The wall clock that timestamps use (§7.2).
///
/// Returning `SystemTime` preserves pre-epoch and out-of-range readings so the
/// timestamp layer can reject them when converting to 48-bit milliseconds.
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
}

/// A shared wall clock, supplied when the store opens.
pub type ClockRef = Arc<dyn Clock>;

/// The system wall clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock that returns the instant supplied by a test until it is set again.
#[cfg(feature = "test-utils")]
pub struct FixedClock(std::sync::RwLock<SystemTime>);

#[cfg(feature = "test-utils")]
impl FixedClock {
    /// A clock initially set to `now`.
    pub fn new(now: SystemTime) -> Self {
        Self(std::sync::RwLock::new(now))
    }

    /// Set the time every subsequent read returns, including a backward jump.
    pub fn set(&self, now: SystemTime) {
        *self.0.write().expect("fixed clock lock poisoned") = now;
    }
}

#[cfg(feature = "test-utils")]
impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        *self.0.read().expect("fixed clock lock poisoned")
    }
}

/// A clock that calls a supplied function on every read.
#[cfg(feature = "test-utils")]
pub struct ClosureClock<F>(
    /// The function called on each read.
    pub F,
);

#[cfg(feature = "test-utils")]
impl<F: Fn() -> SystemTime + Send + Sync> Clock for ClosureClock<F> {
    fn now(&self) -> SystemTime {
        (self.0)()
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;

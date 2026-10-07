//! The wall clock that timestamps use (§7.2); the only reader of system time.

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{future::Future, pin::Pin};

/// The wall clock that timestamps use (§7.2).
///
/// Returning `SystemTime` preserves pre-epoch and out-of-range readings so the
/// timestamp layer can reject them when converting to 48-bit milliseconds.
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
    /// Wait for an elapsed duration. System clocks use the runtime's monotonic
    /// timer; controlled clocks wake only when their supplied time advances.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
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
pub struct FixedClock(tokio::sync::watch::Sender<FixedTime>);

#[cfg(feature = "test-utils")]
struct FixedTime {
    wall: SystemTime,
    elapsed: Duration,
}

#[cfg(feature = "test-utils")]
impl FixedClock {
    /// A clock initially set to `now`.
    pub fn new(now: SystemTime) -> Self {
        Self(
            tokio::sync::watch::channel(FixedTime {
                wall: now,
                elapsed: Duration::ZERO,
            })
            .0,
        )
    }

    /// Set the time every subsequent read returns, including a backward jump.
    pub fn set(&self, now: SystemTime) {
        self.0.send_modify(|time| {
            if let Ok(elapsed) = now.duration_since(time.wall) {
                time.elapsed += elapsed;
            }
            time.wall = now;
        });
    }
}

#[cfg(feature = "test-utils")]
impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0.borrow().wall
    }
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let mut time = self.0.subscribe();
        let deadline = time.borrow().elapsed + duration;
        Box::pin(async move {
            time.wait_for(|time| time.elapsed >= deadline)
                .await
                .expect("clock owner retains its sender");
        })
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

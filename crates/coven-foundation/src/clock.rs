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
    /// Monotonic time since an arbitrary origin, counting time the system
    /// spent asleep (§10, §15). Readings never decrease within a process; only
    /// differences between two readings of one clock mean anything.
    fn elapsed(&self) -> Duration;
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
    fn elapsed(&self) -> Duration {
        boot_time()
    }
}

/// Linux and Android count suspension only in `CLOCK_BOOTTIME`.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn boot_time() -> Duration {
    timespec(rustix::time::clock_gettime(rustix::time::ClockId::Boottime))
}

/// Darwin's `CLOCK_MONOTONIC` keeps advancing while the system sleeps,
/// unlike `CLOCK_UPTIME_RAW` and `std::time::Instant`.
#[cfg(target_vendor = "apple")]
fn boot_time() -> Duration {
    timespec(rustix::time::clock_gettime(
        rustix::time::ClockId::Monotonic,
    ))
}

/// Other Unix systems: `CLOCK_MONOTONIC`, whose treatment of suspension is
/// the platform's own.
#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
fn boot_time() -> Duration {
    timespec(rustix::time::clock_gettime(
        rustix::time::ClockId::Monotonic,
    ))
}

#[cfg(unix)]
fn timespec(time: rustix::time::Timespec) -> Duration {
    Duration::new(
        u64::try_from(time.tv_sec).expect("monotonic seconds are nonnegative"),
        u32::try_from(time.tv_nsec).expect("nanoseconds below one second"),
    )
}

/// Windows interrupt time counts sleep and hibernation, in 100 ns units.
#[cfg(windows)]
fn boot_time() -> Duration {
    let mut ticks = 0u64;
    // SAFETY: the out pointer refers to a live, writable u64.
    unsafe { windows_sys::Win32::System::WindowsProgramming::QueryInterruptTime(&mut ticks) };
    Duration::from_nanos(ticks.saturating_mul(100))
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
    /// Forward settings advance it, as sleep would; backward ones do not.
    fn elapsed(&self) -> Duration {
        self.0.borrow().elapsed
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
    /// A supplied wall reading has no monotonic counterpart: it never advances.
    fn elapsed(&self) -> Duration {
        Duration::ZERO
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;

//! The wall clock that timestamps use (§7.2); the only reader of system time.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{future::Future, pin::Pin};

// The system's wall-clock deadline timer, one per platform; elsewhere a
// wait for a wall-clock time fails.
#[cfg(target_vendor = "apple")]
#[path = "clock_apple.rs"]
mod wall_timer;
#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "clock_linux.rs"]
mod wall_timer;
#[cfg(windows)]
#[path = "clock_windows.rs"]
mod wall_timer;

/// The wall clock that timestamps use (§7.2).
///
/// Returning `SystemTime` preserves pre-epoch and out-of-range readings so the
/// timestamp layer can reject them when converting to 48-bit milliseconds.
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
    /// Monotonic time since an arbitrary origin, counting time the system
    /// spent asleep (§16.6). Readings never decrease within a process; only
    /// differences between two readings of one clock mean anything.
    fn elapsed(&self) -> Duration;
    /// Wait for an elapsed duration. System clocks use the runtime's monotonic
    /// timer; controlled clocks wake only when their supplied time advances.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
    /// Wait until the wall clock reads `at` or later, however it gets
    /// there: by running, or by being set past it (§13.2). A time already
    /// reached ends the wait at once, and a clock set back moves the wait's
    /// end with it. The clock itself says when it reaches `at`; nothing reads
    /// it again on a timer. A clock that can't say fails the wait.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>>;
}

/// A clock couldn't wait for a wall-clock time (§13.2): the system refused
/// its timer, or the clock has none.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("coven can't wait for a wall-clock time: {0}")]
pub struct WallTimerError(String);

impl WallTimerError {
    /// The failure, as its text.
    pub fn new(failure: impl std::fmt::Display) -> Self {
        Self(failure.to_string())
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
    /// The system's own timer for an absolute wall-clock time, which the
    /// system moves when its clock is set. A time at or before the epoch
    /// has passed: no timer is armed for it, since Linux's timer can't hold
    /// one.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
        if at <= UNIX_EPOCH {
            return Box::pin(std::future::ready(Ok(())));
        }
        wall_timer_sleep(at)
    }
}

#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android", windows))]
fn wall_timer_sleep(
    at: SystemTime,
) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
    Box::pin(wall_timer::sleep_until(at))
}

/// Elsewhere, as on the BSDs, coven has no timer the system moves with its
/// clock, and fails the wait rather than read the clock again on a timer.
#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android", windows)))]
fn wall_timer_sleep(
    _at: SystemTime,
) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
    Box::pin(std::future::ready(Err(WallTimerError::new(
        "this platform has no wall-clock timer coven uses",
    ))))
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
    timespec(rustix::time::clock_gettime(rustix::time::ClockId::Monotonic))
}

/// Other Unix systems: `CLOCK_MONOTONIC`, whose treatment of suspension is
/// the platform's own.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))))]
fn boot_time() -> Duration {
    timespec(rustix::time::clock_gettime(rustix::time::ClockId::Monotonic))
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

/// `future`'s output, or `None` once `limit` of real time passes first: a
/// test waiting on the system fails instead of hanging, whatever clock the
/// store under it reads.
#[cfg(feature = "test-utils")]
pub async fn within<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    tokio::time::timeout(limit, future).await.ok()
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
        Self(tokio::sync::watch::channel(FixedTime { wall: now, elapsed: Duration::ZERO }).0)
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
    /// Wakes when a setting reaches `at`; one before it, forward or back,
    /// does not.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
        let mut time = self.0.subscribe();
        Box::pin(async move {
            time.wait_for(|time| time.wall >= at).await.expect("clock owner retains its sender");
            Ok(())
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
    /// Nothing says when the function's reading reaches a time.
    fn sleep_until(
        &self,
        _at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
        Box::pin(std::future::ready(Err(WallTimerError::new(
            "a closure clock has no timer for its readings",
        ))))
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;

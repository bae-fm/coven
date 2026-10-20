use coven_foundation::clock::{Clock, ClockRef};
use std::time::Duration;

/// Preserve the fixture's timestamps while polling without production delays:
/// retry delays and cooldowns, up to five minutes, pass a thousand times
/// faster; their timers, not the fixed clock, report them over. The
/// 30-second idle interval keeps its length, so passes run when asked.
pub(crate) struct PollingClock {
    clock: ClockRef,
    /// Once set, waits other than the idle interval end only when the test
    /// moves the fixed clock past them, so what a failure reports stays
    /// until the test lets its retry run.
    held: std::sync::atomic::AtomicBool,
}
impl PollingClock {
    pub(crate) fn new(clock: ClockRef) -> Self {
        Self {
            clock,
            held: std::sync::atomic::AtomicBool::new(false),
        }
    }
    /// End later waits by the fixed clock alone; earlier ones keep their
    /// shortened timers.
    pub(crate) fn hold_waits(&self) {
        self.held.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
impl Clock for PollingClock {
    fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }
    fn elapsed(&self) -> Duration {
        self.clock.elapsed()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let idle = Duration::from_secs(30);
        if duration == idle {
            return Box::pin(tokio::time::sleep(duration));
        }
        if self.held.load(std::sync::atomic::Ordering::Relaxed) {
            return self.clock.sleep(duration);
        }
        Box::pin(tokio::time::sleep(
            if duration <= Duration::from_secs(300) {
                duration / 1000
            } else {
                duration
            },
        ))
    }
}

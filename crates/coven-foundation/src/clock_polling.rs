use super::{Clock, FixedClock, WallTimerError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// A fixed clock whose waits end only as its time reaches them. A backoff or
/// cooldown the code waits out passes as the code arms it: this clock moves
/// the fixed clock to the wait's end, as the wait's passing would, so retries
/// run without real time and their timestamps are the waits the code chose.
/// A deadline, which work races, and the idle interval end only when the test
/// moves the clock, as does every wait once `hold_waits` is set. It records
/// each `wait_out` it is given.
pub struct PollingClock {
    clock: Arc<FixedClock>,
    /// Once set, waits end only when the test moves the fixed clock past
    /// them, so what a failure reports stays until the test lets its retry
    /// run.
    held: AtomicBool,
    waited: Mutex<Vec<Duration>>,
}

impl PollingClock {
    /// A polling clock over `clock`, which it moves.
    pub fn new(clock: Arc<FixedClock>) -> Self {
        Self { clock, held: AtomicBool::new(false), waited: Mutex::new(Vec::new()) }
    }
    /// End later waits by the test's moves of the fixed clock alone; a wait
    /// armed before ends as its own move reaches it.
    pub fn hold_waits(&self) {
        self.held.store(true, Ordering::SeqCst);
    }
    /// Pass later waits as the code arms them again; a wait armed while
    /// they were held still ends only when the fixed clock reaches it.
    pub fn release_waits(&self) {
        self.held.store(false, Ordering::SeqCst);
    }
    /// Whether waits are held.
    pub fn holds_waits(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }
    /// Each backoff, cooldown or spacing the code armed, in order.
    pub fn waited(&self) -> Vec<Duration> {
        crate::locks::lock_unpoisoned(&self.waited).clone()
    }
}

impl Clock for PollingClock {
    fn now(&self) -> SystemTime {
        self.clock.now()
    }
    fn elapsed(&self) -> Duration {
        self.clock.elapsed()
    }
    fn deadline(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.clock.deadline(duration)
    }
    fn wait_out(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        crate::locks::lock_unpoisoned(&self.waited).push(wait);
        let woken = self.clock.wait_out(wait);
        if self.held.load(Ordering::SeqCst) {
            return woken;
        }
        let end = self.clock.elapsed() + wait;
        let clock = self.clock.clone();
        Box::pin(async move {
            let left = end.saturating_sub(clock.elapsed());
            if !left.is_zero() {
                clock.advance(left);
            }
            woken.await;
        })
    }
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>> {
        self.clock.sleep_until(at)
    }
}

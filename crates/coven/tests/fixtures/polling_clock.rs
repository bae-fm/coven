use coven_foundation::clock::{Clock, FixedClock};
use std::sync::Arc;
use std::time::Duration;

/// The fixture's fixed clock, whose waits end only as its time reaches
/// them. A backoff or cooldown the code waits out passes as the code arms
/// it: this clock moves the fixed clock to the wait's end, as the wait's
/// passing would, so retries run without real time and their timestamps
/// are the waits the code chose. A deadline, which work races, and the
/// idle interval end only when the test moves the clock, as does every
/// wait once `hold_waits` is set.
pub(crate) struct PollingClock {
    clock: Arc<FixedClock>,
    /// Once set, waits end only when the test moves the fixed clock past
    /// them, so what a failure reports stays until the test lets its retry
    /// run.
    held: std::sync::atomic::AtomicBool,
}
impl PollingClock {
    pub(crate) fn new(clock: Arc<FixedClock>) -> Self {
        Self { clock, held: std::sync::atomic::AtomicBool::new(false) }
    }
    /// End later waits by the test's moves of the fixed clock alone; a wait
    /// armed before ends as its own move reaches it.
    pub(crate) fn hold_waits(&self) {
        self.held.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Pass later waits as the code arms them again; a wait armed while
    /// they were held still ends only when the fixed clock reaches it.
    pub(crate) fn release_waits(&self) {
        self.held.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    /// Whether waits are held.
    pub(crate) fn holds_waits(&self) -> bool {
        self.held.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl Clock for PollingClock {
    fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }
    fn elapsed(&self) -> Duration {
        self.clock.elapsed()
    }
    fn deadline(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.clock.deadline(duration)
    }
    fn wait_out(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let woken = self.clock.wait_out(wait);
        if self.held.load(std::sync::atomic::Ordering::SeqCst) {
            return woken;
        }
        let end = self.clock.elapsed() + wait;
        let clock = self.clock.clone();
        Box::pin(async move {
            let left = end.saturating_sub(clock.elapsed());
            if !left.is_zero() {
                clock.set(clock.now() + left);
            }
            woken.await;
        })
    }
    fn sleep_until(
        &self,
        at: std::time::SystemTime,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), coven_foundation::clock::WallTimerError>>
                + Send,
        >,
    > {
        self.clock.sleep_until(at)
    }
}

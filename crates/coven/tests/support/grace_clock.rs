//! A wall clock that moves with real time and can jump ahead, so snapshots
//! age past retention's week of grace between passes (coven.md §16.6).
//! Elapsed time is real, so coven's timing lines report real durations.

use coven::Clock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) struct GraceClock {
    origin: Instant,
    ahead: AtomicU64,
}

impl GraceClock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { origin: Instant::now(), ahead: AtomicU64::new(0) })
    }

    /// Jump past retention's week of grace.
    pub(crate) fn pass_grace(&self) {
        self.ahead.fetch_add(8 * 24 * 60 * 60, Ordering::SeqCst);
    }
}

impl Clock for GraceClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH
            + Duration::from_secs(1_000_000 + self.ahead.load(Ordering::SeqCst))
            + self.origin.elapsed()
    }
    fn elapsed(&self) -> Duration {
        self.origin.elapsed()
    }
    fn sleep_until(
        &self,
        _at: SystemTime,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), coven::WallTimerError>> + Send>,
    > {
        Box::pin(std::future::ready(Err(coven::WallTimerError::NoTimer)))
    }
}

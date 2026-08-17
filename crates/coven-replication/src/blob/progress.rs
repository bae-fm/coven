use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const TRANSFER_PROGRESS_TICK: Duration = Duration::from_millis(300);

/// Coalesces a transfer's byte callbacks and exposes them at the UI cadence.
/// The storage stream only updates one atomic counter; no callback frequency can
/// flood a platform event bridge.
pub(crate) struct TransferProgress {
    latest: Arc<AtomicU64>,
    forwarded: u64,
    ticker: tokio::time::Interval,
}

impl TransferProgress {
    pub(crate) fn new() -> Self {
        let mut ticker = tokio::time::interval(TRANSFER_PROGRESS_TICK);
        ticker.reset_after(TRANSFER_PROGRESS_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            latest: Arc::new(AtomicU64::new(0)),
            forwarded: 0,
            ticker,
        }
    }

    pub(crate) fn callback(&self) -> Arc<dyn Fn(u64) + Send + Sync> {
        let latest = Arc::clone(&self.latest);
        Arc::new(move |bytes| latest.store(bytes, Ordering::Relaxed))
    }

    pub(crate) async fn changed(&mut self) -> u64 {
        loop {
            self.ticker.tick().await;
            let latest = self.latest.load(Ordering::Relaxed);
            if latest != self.forwarded {
                self.forwarded = latest;
                return latest;
            }
        }
    }

    /// Return the exact terminal total once, even when the storage callback's
    /// last observed chunk has not reached its next cadence tick.
    pub(crate) fn finish(&mut self, total: u64) -> Option<u64> {
        if self.forwarded == total {
            None
        } else {
            self.forwarded = total;
            Some(total)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rapid_buffer_callbacks_coalesce_at_the_transfer_cadence() {
        let started = tokio::time::Instant::now();
        let mut progress = TransferProgress::new();
        let callback = progress.callback();
        callback(1);
        callback(2);
        callback(3);

        assert_eq!(progress.changed().await, 3);
        assert!(started.elapsed() >= Duration::from_millis(250));
        assert_eq!(progress.finish(4), Some(4));
        assert_eq!(progress.finish(4), None);
    }
}

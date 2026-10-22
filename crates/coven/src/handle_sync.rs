//! Starting, stopping and requesting sync passes on the handle's loop (E5).
use crate::*;

impl CovenHandle {
    /// Start syncing; the first pass builds an absent client from custody
    /// credentials and backs off like any failed pass. Already running or
    /// unconfigured stores do nothing.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        self.storage.start().await
    }
    /// Finish the active pass and transfers, then release the provider client.
    /// Keys remain unlocked for the handle session. Completion publishes Stopped,
    /// or Disconnected without configured storage; release failures publish Failed.
    pub fn stop_sync(&self) {
        self.storage.stop();
    }
    /// Request a pass while started. Requests during a pass, and local writes
    /// committed during it, retain one more pass; the pass's own bookkeeping
    /// retains none. A failed connection's backoff and a provider cooldown
    /// hold requests until they end.
    pub fn sync_now(&self) {
        self.storage.sync_now();
    }
    /// Tell coven the device's network changed: it became available, or
    /// moved to another network. Every backoff after an attempt that
    /// received no provider response ends: the connection's (`Offline`),
    /// operations', uploads' and downloads'; a pass runs now. Backoffs
    /// after a provider's answer, and provider cooldowns, hold. Reports
    /// coalesce as `sync_now` requests do.
    pub fn network_changed(&self) {
        self.storage.network_changed();
    }
    /// The person asked to retry everything now, such as by pulling to
    /// refresh. Every ordinary backoff ends, answered or not: the
    /// connection's, operations', uploads' and downloads'; a pass runs
    /// now. Provider cooldowns hold, and failures that never retry stay. An
    /// ended wait keeps its failure count, so a retry that fails again waits
    /// the next longer delay. Requests coalesce as `sync_now` requests do.
    ///
    /// The retry takes effect at once; the returned future resolves when the
    /// first pass after it ends: `Synced`, or `Failed` with why. A pass held
    /// by a provider cooldown answers `CoolingDown` with when it ends, and
    /// one held by a failure only the person can clear answers `Failed` with
    /// it, both at once. Stopped sync, or a closed handle, answers
    /// `NotSyncing`. Dropping the future leaves the retry in place.
    pub fn retry_now(&self) -> RetryAnswer {
        self.storage.retry_now()
    }
    /// Observe the current status and every subsequent status change.
    pub fn subscribe_sync_status(&self) -> tokio::sync::watch::Receiver<SyncStatus> {
        self.storage.subscribe()
    }
}

#[cfg(test)]
#[path = "handle_sync_tests.rs"]
mod tests;

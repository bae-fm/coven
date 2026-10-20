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
    /// moved to another network. A backoff after an attempt that received
    /// no provider response (`Offline`) ends and a pass runs now. Backoffs
    /// after a provider's answer, and provider cooldowns, hold. Reports
    /// coalesce as `sync_now` requests do.
    pub fn network_changed(&self) {
        self.storage.network_changed();
    }
    /// Observe the current status and every subsequent status change.
    pub fn subscribe_sync_status(&self) -> tokio::sync::watch::Receiver<SyncStatus> {
        self.storage.subscribe()
    }
}

#[cfg(test)]
#[path = "handle_sync_tests.rs"]
mod tests;

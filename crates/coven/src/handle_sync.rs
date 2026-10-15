//! Starting, stopping and requesting sync passes on the handle's loop (E5).
use crate::*;

impl CovenHandle {
    /// Start syncing; the first pass builds an absent client from custody
    /// credentials and backs off like any failed pass. Already running or
    /// unconfigured stores do nothing.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        self.sync.start().await
    }
    /// Finish the active pass and transfers, then release the provider client.
    /// Keys remain unlocked for the handle session. Completion publishes Stopped,
    /// or Disconnected without configured storage; release failures publish Failed.
    pub fn stop_sync(&self) {
        self.sync.stop();
    }
    /// Request a pass while started. Requests during a pass, and local writes
    /// committed during it, retain one more pass; the pass's own bookkeeping
    /// retains none. A failed connection's backoff and a provider cooldown
    /// hold requests until they end.
    pub fn sync_now(&self) {
        self.sync.sync_now();
    }
    /// Observe the current status and every subsequent status change.
    pub fn subscribe_sync_status(&self) -> tokio::sync::watch::Receiver<SyncStatus> {
        self.sync.subscribe()
    }
}

#[cfg(test)]
#[path = "handle_sync_tests.rs"]
mod tests;

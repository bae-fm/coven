//! Starting, stopping and requesting sync passes on the handle's loop (E5).
use crate::*;

impl CovenHandle {
    /// Start syncing; the first pass builds an absent client from custody
    /// credentials and backs off like any failed pass. Already running or
    /// unconfigured stores do nothing.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        self.storage.start().await
    }
    /// Stop syncing: `coven_retry_effect` says sync isn't running and the
    /// waiting retries are answered `NotSyncing` first; then the active pass
    /// and transfers finish, the provider client is released and the
    /// location recorded as custody holds it, and this returns. Once it has,
    /// no pass runs until sync starts again. Keys remain unlocked for the
    /// handle session. A `start_sync` asked meanwhile starts after it, and
    /// dropping this future leaves stopping to finish. Stopping changes
    /// nothing the sync state reads; a release failure is recorded as the
    /// last pass's failure and returned. A failure writing either row has
    /// failed the handle; stopping goes on and the failure is returned.
    pub async fn stop_sync(&self) -> Result<(), SyncError> {
        self.storage.stop().await
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
    /// received no provider response ends: the connection's (offline),
    /// operations' and uploads'; a pass runs now. Backoffs
    /// after a provider's answer, and provider cooldowns, hold. Reports
    /// coalesce as `sync_now` requests do.
    pub fn network_changed(&self) {
        self.storage.network_changed();
    }
    /// The person asked to retry everything now, such as by pulling to
    /// refresh. Every ordinary backoff ends, answered or not: the
    /// connection's, operations' and uploads'; a pass runs now. Provider cooldowns hold, and failures that never retry stay. An
    /// ended wait keeps its failure count, so a retry that fails again waits
    /// the next longer delay. Requests coalesce as `sync_now` requests do.
    ///
    /// The retry takes effect once this returns, with the answer as a
    /// future, which resolves at once as `coven_retry_effect` says when no
    /// pass runs for it: `CoolingDown` with when the provider cooldown ends,
    /// `Failed` with a failure only the person can clear, or `NotSyncing`
    /// while sync is stopped. Otherwise it resolves when the first pass
    /// after the retry ends: `Synced`, or `Failed` with why. A closed handle
    /// answers `NotSyncing`. Dropping the future leaves the retry in place.
    /// Deciding the answer writes the retry effect's row; a failed write has
    /// failed the handle, and is returned.
    pub async fn retry_now(&self) -> Result<RetryAnswer, SyncError> {
        self.storage.retry_now().await
    }
    /// Follow `coven_retry_effect` with a live query, as an app does (E5).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_retry_effect(&self) -> coven_database::test_utils::Followed<RetryEffect> {
        RetryEffect::follow(&self.database)
    }
    /// Run idle passes at most `interval` apart, instead of every 30
    /// seconds, until the returned guard drops, such as while a screen
    /// waits for another device. With several guards held, the shortest
    /// interval wins; once the last drops, idle passes return to every 30
    /// seconds. The next idle pass comes `interval` after the last pass
    /// ended, so one that is already due runs at once. Connection backoff
    /// and provider cooldowns still hold every pass. The request outlives
    /// stopping and starting sync, and does nothing while sync is stopped.
    pub fn sync_often(&self, interval: std::time::Duration) -> SyncOftenGuard {
        self.storage.sync_often(interval)
    }
}

#[cfg(test)]
#[path = "handle_sync_tests.rs"]
mod tests;

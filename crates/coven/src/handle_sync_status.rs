//! The sync state an app shows, read from coven's own rows (§19.1, E5).
use crate::*;

impl CovenHandle {
    /// The sync state and when sync last finished, computed from
    /// `coven_sync_status`, `coven_retry_effect`, the send queue, the upload
    /// queue, the files moving and the pending records in one committed
    /// snapshot (§19.1).
    pub async fn sync_status(&self) -> CovenResult<SyncStatus> {
        coven_sync::sync_status(&self.database).await
    }
    /// The same pre-built query, live (E4): its first value is the current
    /// state.
    pub fn subscribe_sync_status(&self) -> LiveQuery<SyncStatus> {
        self.database.subscribe_sync_facts(coven_sync::SyncStatus::computed)
    }
    /// Follow the sync state with `subscribe_sync_status`, as an app does (E5).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_sync_status(&self) -> coven_database::test_utils::Followed<SyncStatus> {
        coven_database::test_utils::Followed::new(self.subscribe_sync_status())
    }
    /// Follow `coven_sync_status.last_pass` with a live query, as an app's
    /// SQL does (E4): `None` until a pass ends on this device.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_last_pass(&self) -> coven_database::test_utils::Followed<Option<LastPass>> {
        LastPass::follow(&self.database)
    }
}

#[cfg(test)]
#[path = "handle_sync_status_tests.rs"]
mod tests;

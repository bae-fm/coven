//! The sync state an app shows, read from coven's own rows (§19.1, E5).
use crate::*;

impl CovenHandle {
    /// The sync state and when sync last finished, as `coven_sync_state`
    /// shows them in one committed snapshot (§19.1). An app's screen reads
    /// the view in its own live query instead.
    pub async fn sync_status(&self) -> CovenResult<SyncStatus> {
        coven_sync::sync_status(&self.database).await.map_err(CovenError::from)
    }
    /// Follow the sync state with a live query of `coven_sync_state`, as an
    /// app does (E4, E5).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_sync_status(&self) -> coven_database::test_utils::Followed<SyncStatus, CovenError> {
        coven_database::test_utils::Followed::new(
            self.database.subscribe(|sql| SyncStatus::read(sql).map_err(CovenError::from)),
        )
    }
    /// Follow `coven_sync_status.last_pass` with a live query, as an app's
    /// SQL does (E4): `None` until a pass ends on this device.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_last_pass(
        &self,
    ) -> coven_database::test_utils::Followed<Option<LastPass>, DbError> {
        LastPass::follow(&self.database)
    }
}

#[cfg(test)]
#[path = "handle_sync_status_tests.rs"]
mod tests;

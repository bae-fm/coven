//! Deleting a store from this device without losing unsent work (E1).

use crate::*;

/// What `delete_store` does with writes and files not yet uploaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IfUnsent {
    /// Refuse with `StoreDeletionError::Unsent`, deleting nothing.
    Refuse,
    /// Delete them with the store.
    Discard,
}

impl CovenHandle {
    /// Deletes this store from this device. With storage set up, it first
    /// refuses unsent writes and files unless `unsent` discards them, then
    /// publishes this device's removal (§13) so peers stop reading its logs;
    /// either failure deletes nothing. It then closes this handle and deletes
    /// as `Coven::discard_store`, which finishes a deletion failing after that.
    pub async fn delete_store(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError> {
        if self.codes.connection().await?.is_some() {
            let work = self.database.unsent().await.map_err(SyncError::from)?;
            if unsent == IfUnsent::Refuse && !work.is_empty() {
                return Err(StoreDeletionError::Unsent(work));
            }
            // The removal needs a connection; a started loop's pass makes one.
            self.sync.start().await?;
            self.sync
                .subscribe()
                .wait_for(|status| !matches!(status, SyncStatus::Syncing))
                .await
                .map_err(|_| SyncError::from(DbError::StoreClosed))?;
            self.operations.remove_device(self.device).await?;
        }
        self.close().await?;
        let directory = self.directory.clone();
        let keychain = self.keychain.clone();
        crate::coven::blocking(move || crate::coven::delete(&directory, &keychain)).await
    }
}

#[cfg(test)]
#[path = "handle_delete_tests.rs"]
mod tests;

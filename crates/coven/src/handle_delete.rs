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
    /// The writes and files this device committed and has not uploaded, from
    /// one committed snapshot: what deleting the store would lose. With storage
    /// set up, `delete_store(IfUnsent::Refuse)` refuses with these counts.
    /// Available while disconnected.
    pub async fn unsent_work(&self) -> CovenResult<UnsentWork> {
        self.database.unsent().await
    }

    /// The same pre-built query, live (E4).
    pub fn subscribe_unsent_work(&self) -> LiveQuery<UnsentWork> {
        self.database.subscribe_unsent()
    }

    /// Deletes this store from this device. With storage set up, it first
    /// refuses unsent writes and files unless `unsent` discards them, then
    /// publishes this device's removal (§14.1), which cuts off its later slots;
    /// either failure deletes nothing. It then closes this handle and deletes
    /// as `Coven::discard_store`, which finishes a deletion failing after that.
    /// Only this device leaves: the person's restore code and S3 key in iCloud
    /// Keychain stay for their other devices (§13.1).
    pub async fn delete_store(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError> {
        if self.storage.configured().await? {
            let work = self.unsent_work().await.map_err(SyncError::from)?;
            if unsent == IfUnsent::Refuse && !work.is_empty() {
                return Err(StoreDeletionError::Unsent(work));
            }
            // The removal needs a connection; this session may not have one.
            self.storage.connected().await?;
            self.remove_device(self.device).await?;
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

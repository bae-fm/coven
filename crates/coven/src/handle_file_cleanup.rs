//! Deleting removed devices' stored files at the app's request (§17.10, E5).

use crate::*;

impl CovenHandle {
    /// Delete the stored files of removed devices that nothing refers to
    /// (§17.10): list again the file folder of every removed device this
    /// device is responsible for, then delete each file found that no
    /// retained row, write or snapshot names, once every other active device
    /// has published a write that read past the listing; later passes delete
    /// the rest as those writes arrive. A removed device can keep
    /// uploading while its provider access still works, but nothing it
    /// writes after its removal counts, so this is safe to run at any time.
    /// Returns once the listing and deletions finish, or with the failure
    /// that stopped them; `SyncError::SnapshotPending` while a snapshot of
    /// this device's is still being published.
    pub async fn clean_up_removed_files(&self) -> Result<(), SyncError> {
        Box::pin(self.storage.clean_up_removed_files()).await
    }
}

#[cfg(test)]
#[path = "handle_file_cleanup_tests.rs"]
mod tests;

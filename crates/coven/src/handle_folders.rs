//! Provider folder selection using the handle's pending sign-in.

use super::*;

impl CovenHandle {
    /// The signed-in person's default OneDrive root and setup location.
    /// Call authenticate first; tokens stay in coven and settings are unchanged.
    pub async fn onedrive_root(&self) -> Result<StorageFolder, StorageSetupError> {
        self.storage.onedrive_root().await
    }

    /// Every immediate child folder of this OneDrive location, across every page.
    pub async fn onedrive_folders(
        &self,
        parent: &StorageConfig,
    ) -> Result<Vec<StorageFolder>, StorageSetupError> {
        self.storage.onedrive_folders(parent).await
    }

    /// Create this exact name in the chosen OneDrive parent; occupied names fail.
    /// Returns a setup location without committing it. Close waits for creation,
    /// including when the awaiting app drops its future.
    pub async fn create_onedrive_folder(
        &self,
        parent: &StorageConfig,
        name: &str,
    ) -> Result<StorageFolder, StorageSetupError> {
        self.storage.create_onedrive_folder(parent, name).await
    }

    /// Every shared Dropbox namespace the signed-in account can access.
    /// Requires Full Dropbox sign-in; each returned location goes directly to setup.
    pub async fn dropbox_shared_folders(&self) -> Result<Vec<StorageFolder>, StorageSetupError> {
        self.storage.dropbox_shared_folders().await
    }
}

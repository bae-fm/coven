//! Folder choices borrow the pending sign-in without publishing credentials or settings.

use super::*;
use coven_storage::providers::StorageFolder;

impl StorageConnections {
    async fn folders<T, F, Fut>(
        &self,
        provider: CloudProvider,
        request: F,
    ) -> Result<T, StorageSetupError>
    where
        F: FnOnce(ProviderAuth) -> Fut,
        Fut: std::future::Future<Output = Result<T, StorageError>>,
    {
        let auth = {
            let held = self.authentication.lock().await;
            let sign_in = held.as_ref().ok_or(OAuthError::Reauthorize(provider))?;
            crate::authentication::check_provider(sign_in, provider)?;
            ProviderAuth::OAuth {
                tokens: sign_in.tokens().map_err(setup_error)?,
                custody: sign_in.clone(),
            }
        };
        Ok(request(auth).await?)
    }

    pub(crate) async fn onedrive_root(&self) -> Result<StorageFolder, StorageSetupError> {
        let _call = self.lock_open().await?;
        self.folders(CloudProvider::OneDrive, |auth| {
            self.connector.onedrive_root(auth, self.waits.clone())
        })
        .await
    }

    pub(crate) async fn onedrive_folders(
        &self,
        parent: &StorageConfig,
    ) -> Result<Vec<StorageFolder>, StorageSetupError> {
        let _call = self.lock_open().await?;
        self.folders(CloudProvider::OneDrive, |auth| {
            self.connector.onedrive_folders(parent, auth, self.waits.clone())
        })
        .await
    }

    pub(crate) async fn create_onedrive_folder(
        self: &Arc<Self>,
        parent: &StorageConfig,
        name: &str,
    ) -> Result<StorageFolder, StorageSetupError> {
        let parent = parent.clone();
        let name = name.to_owned();
        self.call(move |owner| async move {
            owner
                .folders(CloudProvider::OneDrive, |auth| {
                    owner.connector.create_onedrive_folder(
                        &parent,
                        &name,
                        auth,
                        owner.waits.clone(),
                    )
                })
                .await
        })
        .await
    }

    pub(crate) async fn dropbox_shared_folders(
        &self,
    ) -> Result<Vec<StorageFolder>, StorageSetupError> {
        let _call = self.lock_open().await?;
        self.folders(CloudProvider::Dropbox, |auth| {
            self.connector.dropbox_shared_folders(auth, self.waits.clone())
        })
        .await
    }
}

#[cfg(test)]
#[path = "storage_folders_tests.rs"]
mod tests;

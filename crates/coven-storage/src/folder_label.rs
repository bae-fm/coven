//! A folder's name and web address, as the account a connection signs in
//! with names them (§20.2): display labels each connection reads again, so
//! a device shows the folder as its provider names it now.

use crate::{
    DropboxLocation, ProviderOps, Storage, StorageConfig, StorageConnection, StorageError,
};

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    /// This connection's location with its folder's label as the provider
    /// answers it now: a Dropbox shared folder's or a OneDrive folder's.
    /// Other locations have none and send no request. A failed read keeps
    /// its provider cause.
    pub async fn labelled_config(&self) -> Result<StorageConfig, StorageError> {
        let mut config = self.config();
        match &mut config {
            StorageConfig::Dropbox {
                location: DropboxLocation::SharedFolder { .. },
                folder_label,
                ..
            }
            | StorageConfig::OneDrive { folder_label, .. } => {
                let read = self
                    .resent(move || async move {
                        self.cooling()?;
                        let read = self
                            .send(crate::RequestKind::Status, 0, self.provider.folder_label())
                            .await;
                        self.observed(read).await
                    })
                    .await?;
                *folder_label = Some(read);
            }
            StorageConfig::Dropbox { location: DropboxLocation::AppFolder, .. }
            | StorageConfig::S3 { .. }
            | StorageConfig::CloudKit { .. } => {}
        }
        Ok(config)
    }
}

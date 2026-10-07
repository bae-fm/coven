//! Provider composition for new-device bootstrap.

use crate::*;
use coven_storage::{providers::*, RestoreStorage, Storage, StorageCredentials};
use std::sync::Arc;

pub(crate) fn connect(
    data: &RestoreStorage,
    device: DeviceId,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<Arc<dyn Storage>, StorageError> {
    data.validate()?;
    let config = data.location.clone();
    match (&data.location, &data.credentials) {
        (StorageConfig::S3 { .. }, StorageCredentials::S3(credentials)) => Ok(Arc::new(
            S3Storage::new(config, credentials.clone(), clock, ids)?,
        )),
        (StorageConfig::CloudKit { .. }, StorageCredentials::CloudKit) => {
            Ok(Arc::new(CloudKitStorage::new(
                config,
                cloudkit.ok_or(StorageError::InvalidConfiguration(
                    "CloudKit requires the app's bridge",
                ))?,
            )?))
        }
        (_, StorageCredentials::OAuth(tokens)) => {
            let session = OAuthSession::new(config.provider(), tokens.clone(), clock)?;
            match config.provider() {
                CloudProvider::GoogleDrive => {
                    Ok(Arc::new(GoogleDriveStorage::new(config, device, session)?))
                }
                CloudProvider::Dropbox => Ok(Arc::new(DropboxStorage::new(config, session)?)),
                CloudProvider::OneDrive => Ok(Arc::new(OneDriveStorage::new(config, session)?)),
                _ => Err(StorageError::InvalidConfiguration(
                    "provider does not use OAuth",
                )),
            }
        }
        _ => Err(StorageError::InvalidConfiguration(
            "credentials do not match provider",
        )),
    }
}

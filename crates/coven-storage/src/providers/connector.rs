//! Provider construction from an explicit location and device credentials.

use super::{
    CloudKitOps, CloudKitStorage, DropboxStorage, GoogleDriveStorage, OAuthSession,
    OneDriveStorage, S3Storage,
};
use crate::{Storage, StorageConfig, StorageCredentials, StorageError, StorageFailure};
use coven_foundation::{
    clock::ClockRef,
    id_source::{DeviceId, IdSourceRef},
};
use std::sync::Arc;

/// Constructs a connection without committing settings or credentials. Apps and
/// tests can supply another connector while using the same setup and sync owners.
#[async_trait::async_trait]
pub trait StorageConnector: Send + Sync {
    /// Construct the selected adapter; the caller verifies it before publication.
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<dyn Storage>, StorageError>;
}

/// The provider composition root, with every clock, id source and native bridge
/// supplied by the application opening root.
pub struct ProviderConnector {
    clock: ClockRef,
    ids: IdSourceRef,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
}

impl ProviderConnector {
    /// Retain the capabilities used to construct subsequent connections.
    pub fn new(clock: ClockRef, ids: IdSourceRef, cloudkit: Option<Arc<dyn CloudKitOps>>) -> Self {
        Self {
            clock,
            ids,
            cloudkit,
        }
    }
}

#[async_trait::async_trait]
impl StorageConnector for ProviderConnector {
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<dyn Storage>, StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: credentials.clone(),
        }
        .validate()?;
        match (&config, credentials) {
            (StorageConfig::S3 { .. }, StorageCredentials::S3(credentials)) => Ok(Arc::new(
                S3Storage::new(config, credentials, self.clock.clone(), self.ids.clone())?,
            )),
            (StorageConfig::GoogleDrive { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Ok(Arc::new(GoogleDriveStorage::new(config, device, session)?))
            }
            (StorageConfig::Dropbox { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Ok(Arc::new(DropboxStorage::new(config, session)?))
            }
            (StorageConfig::OneDrive { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Ok(Arc::new(OneDriveStorage::new(config, session)?))
            }
            (StorageConfig::CloudKit { .. }, StorageCredentials::CloudKit) => {
                let ops = self.cloudkit.clone().ok_or(
                    StorageFailure::InvalidConfiguration.with_source("CloudKit bridge is absent"),
                )?;
                Ok(Arc::new(CloudKitStorage::new(config, ops)?))
            }
            _ => Err(StorageFailure::InvalidConfiguration
                .with_source("credentials do not match the provider")),
        }
    }
}

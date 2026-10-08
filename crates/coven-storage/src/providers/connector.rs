//! Provider construction from an explicit location and device credentials.

use super::{
    CloudKitOps, CloudKitStorage, DropboxStorage, GoogleDriveStorage, OAuthSession,
    OneDriveStorage, S3Storage,
};
use crate::{
    ProviderOps, StorageConfig, StorageConnection, StorageCredentials, StorageError, StorageFailure,
};
use coven_foundation::{
    clock::ClockRef,
    id_source::{DeviceId, IdSourceRef},
};
use std::sync::Arc;

/// Constructs built-in provider connections without committing settings or credentials.
/// Shared by coven's opening, setup and sync owners. Tests replace it with an
/// in-memory provider through coven's `test-utils` builder hook.
#[async_trait::async_trait]
pub trait StorageConnector: Send + Sync {
    /// Validate settings and wrap the selected native provider once. The caller
    /// verifies the connection before publication.
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<StorageConnection>, StorageError>;
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
    ) -> Result<Arc<StorageConnection>, StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: credentials.clone(),
        }
        .validate()?;
        let provider: Arc<dyn ProviderOps> = match (&config, credentials) {
            (StorageConfig::S3 { .. }, StorageCredentials::S3(credentials)) => Arc::new(
                S3Storage::new(config, credentials, self.clock.clone(), self.ids.clone())?,
            ),
            (StorageConfig::GoogleDrive { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Arc::new(GoogleDriveStorage::new(config, device, session)?)
            }
            (StorageConfig::Dropbox { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Arc::new(DropboxStorage::new(config, session)?)
            }
            (StorageConfig::OneDrive { .. }, StorageCredentials::OAuth(tokens)) => {
                let session = OAuthSession::new(config.provider(), tokens, self.clock.clone())?;
                Arc::new(OneDriveStorage::new(config, session)?)
            }
            (StorageConfig::CloudKit { .. }, StorageCredentials::CloudKit) => {
                let ops = self.cloudkit.clone().ok_or(
                    StorageFailure::InvalidConfiguration.with_source("CloudKit bridge is absent"),
                )?;
                Arc::new(CloudKitStorage::new(config, ops)?)
            }
            _ => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("credentials do not match the provider"))
            }
        };
        Ok(Arc::new(StorageConnection::from_provider(provider)))
    }
}

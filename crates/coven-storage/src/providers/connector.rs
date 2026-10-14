//! Provider construction from an explicit location and device credentials.

use super::{DropboxStorage, GoogleDriveStorage, OAuthSession, OneDriveStorage, S3Storage};
use crate::{
    ProviderOps, S3Credentials, S3Detection, StorageConfig, StorageConnection, StorageCredentials,
    StorageError, StorageFailure,
};
use coven_foundation::{
    clock::ClockRef,
    id_source::{DeviceId, IdSourceRef, StoreId},
};
use std::sync::Arc;

/// Constructs provider connections without committing settings or credentials.
/// Tests replace the connection boundary with in-memory storage.
#[async_trait::async_trait]
pub trait StorageConnector: Send + Sync {
    /// Create the owner's CloudKit location from the entitled container.
    /// A connector without native CloudKit support refuses this capability.
    async fn prepare_cloudkit(
        &self,
        _container: &str,
        _store: StoreId,
    ) -> Result<StorageConfig, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Validate settings and wrap the selected native provider once. The caller
    /// verifies the connection before publication.
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<StorageConnection>, StorageError>;
    /// Probe an S3 location's service (§4), writing only fresh objects under
    /// the store's `.coven-check` prefix and deleting them, also on failure.
    /// A connector without native S3 refuses this capability.
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
    ) -> Result<S3Detection, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Before a store's first setup at an S3 location whose member keys reach
    /// the whole bucket, refuse a bucket holding anything outside the store's
    /// folder with `OwnBucketRequired { reason: Occupied }` (§4). Reads only.
    /// A connector without native S3 refuses this capability.
    async fn check_new_s3_store(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Key administration for an S3 location's service, holding this admin
    /// access for one operation (§4). A custom profile, or a service without
    /// a minter, is refused: its keys are made in the console.
    fn s3_key_admin(
        &self,
        _config: StorageConfig,
        _admin: crate::AdminCredential,
    ) -> Result<Arc<dyn crate::AccessKeyAdmin>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
}

/// The provider composition root retains the supplied clock and id source.
pub struct ProviderConnector {
    clock: ClockRef,
    ids: IdSourceRef,
}
impl ProviderConnector {
    /// Retain the capabilities used to construct subsequent connections.
    pub fn new(clock: ClockRef, ids: IdSourceRef) -> Self {
        Self { clock, ids }
    }
}

#[async_trait::async_trait]
impl StorageConnector for ProviderConnector {
    async fn prepare_cloudkit(
        &self,
        container: &str,
        store: StoreId,
    ) -> Result<StorageConfig, StorageError> {
        #[cfg(target_vendor = "apple")]
        {
            super::cloudkit::prepare(container, store).await
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            let _ = (container, store);
            Err(StorageFailure::ProviderUnavailable.into())
        }
    }
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
                Arc::new(OneDriveStorage::new(config, session, self.ids.clone())?)
            }
            (StorageConfig::CloudKit { .. }, StorageCredentials::CloudKit) => {
                #[cfg(target_vendor = "apple")]
                {
                    Arc::new(super::CloudKitStorage::new(config, self.ids.clone()).await?)
                }
                #[cfg(not(target_vendor = "apple"))]
                {
                    return Err(StorageFailure::ProviderUnavailable.into());
                }
            }
            _ => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("credentials do not match the provider"))
            }
        };
        Ok(Arc::new(StorageConnection::from_provider(provider)))
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
    ) -> Result<S3Detection, StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: StorageCredentials::S3(credentials.clone()),
        }
        .validate()?;
        super::detect_s3(
            config,
            credentials,
            store,
            self.clock.clone(),
            self.ids.clone(),
        )
        .await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
    ) -> Result<(), StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: StorageCredentials::S3(credentials.clone()),
        }
        .validate()?;
        super::check_new_s3_store(
            config,
            credentials,
            store,
            self.clock.clone(),
            self.ids.clone(),
        )
        .await
    }
    fn s3_key_admin(
        &self,
        config: StorageConfig,
        admin: crate::AdminCredential,
    ) -> Result<Arc<dyn crate::AccessKeyAdmin>, StorageError> {
        config.validate()?;
        super::s3::minting::key_admin(
            config,
            admin,
            super::s3::minting::AdminEndpoints::production(),
            self.clock.clone(),
            self.ids.clone(),
        )
    }
}

#[cfg(all(test, not(target_vendor = "apple")))]
#[path = "connector_tests.rs"]
mod tests;

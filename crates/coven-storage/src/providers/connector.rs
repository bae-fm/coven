//! Provider construction from an explicit location and device credentials.

use super::{
    oauth_folders::OAuthFolders, DropboxStorage, OAuthSession, OneDriveStorage, S3Storage,
    StorageFolder,
};
use crate::{
    ProviderAuth, ProviderOps, S3Credentials, S3Detection, StorageConfig, StorageConnection,
    StorageCredentials, StorageError, StorageFailure,
};
use coven_foundation::{
    clock::ClockRef,
    id_source::{IdSourceRef, StoreId},
};
use std::sync::Arc;

/// Constructs provider connections without committing settings or credentials.
/// Tests replace the connection boundary with in-memory storage.
#[async_trait::async_trait]
pub trait StorageConnector: Send + Sync {
    /// Read the signed-in account's display label before recording a member's location.
    async fn account_label(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<String, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read the signed-in OneDrive root.
    async fn onedrive_root(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read all immediate OneDrive child folders.
    async fn onedrive_folders(
        &self,
        _parent: &StorageConfig,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Create a OneDrive folder without renaming or replacing an occupied name.
    async fn create_onedrive_folder(
        &self,
        _parent: &StorageConfig,
        _name: &str,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read all shared Dropbox namespaces.
    async fn dropbox_shared_folders(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

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
    /// verifies the connection before publication. Every request waits for
    /// the store's shared `cooldown`; a rejected OAuth token is refreshed in
    /// the sign-in's custody (§20.2).
    async fn connect(
        &self,
        config: StorageConfig,
        auth: ProviderAuth,
        publications: Arc<dyn crate::PublicationGate>,
        cooldown: Arc<dyn crate::Cooldown>,
        transfers: Arc<crate::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError>;
    /// Probe an S3 location's service (§20.7), writing only fresh objects under
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
    /// folder or the `coven-bucket` marker (§20.7). Reads only.
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
    /// access for one operation (§20.10). A custom profile, or a service without
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
    #[cfg(any(test, feature = "test-utils"))]
    folder_api: Option<String>,
}
impl ProviderConnector {
    /// Retain the capabilities used to construct subsequent connections.
    pub fn new(clock: ClockRef, ids: IdSourceRef) -> Self {
        Self {
            clock,
            ids,
            #[cfg(any(test, feature = "test-utils"))]
            folder_api: None,
        }
    }
    /// Send account and folder calls to an in-process provider API in application tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_folder_api(mut self, api: String) -> Self {
        self.folder_api = Some(api);
        self
    }
    async fn with_folders<T, F, Fut>(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
        request: F,
    ) -> Result<T, StorageError>
    where
        F: FnOnce(OAuthFolders) -> Fut,
        Fut: std::future::Future<Output = Result<T, StorageError>>,
    {
        let ProviderAuth::OAuth { tokens, custody } = auth else {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("account and folder requests require OAuth sign-in"));
        };
        let access = tokens.access;
        let api = match access {
            crate::OAuthAccess::Dropbox(_) => "https://api.dropboxapi.com/2",
            crate::OAuthAccess::OneDrive => "https://graph.microsoft.com/v1.0",
        }
        .to_owned();
        #[cfg(any(test, feature = "test-utils"))]
        let api = self.folder_api.clone().unwrap_or(api);
        let session = OAuthSession::new(access.provider(), tokens, custody, self.clock.clone())?;
        request(OAuthFolders::new(session, access, api, cooldown)).await
    }
}

#[async_trait::async_trait]
impl StorageConnector for ProviderConnector {
    async fn account_label(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<String, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.account_label().await
        })
        .await
    }

    /// Read the signed-in OneDrive root.
    async fn onedrive_root(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.onedrive_root().await
        })
        .await
    }

    /// Read all immediate OneDrive child folders.
    async fn onedrive_folders(
        &self,
        parent: &StorageConfig,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.onedrive_folders(parent).await
        })
        .await
    }

    /// Create a OneDrive folder without renaming or replacing an occupied name.
    async fn create_onedrive_folder(
        &self,
        parent: &StorageConfig,
        name: &str,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.create_onedrive_folder(parent, name).await
        })
        .await
    }

    /// Read all shared Dropbox namespaces.
    async fn dropbox_shared_folders(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.dropbox_shared_folders().await
        })
        .await
    }

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
        auth: ProviderAuth,
        publications: Arc<dyn crate::PublicationGate>,
        cooldown: Arc<dyn crate::Cooldown>,
        transfers: Arc<crate::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: auth.credentials(),
        }
        .validate()?;
        let provider = config.provider();
        let session =
            |tokens, custody| OAuthSession::new(provider, tokens, custody, self.clock.clone());
        let provider: Arc<dyn ProviderOps> = match (&config, auth) {
            (StorageConfig::S3 { .. }, ProviderAuth::S3(credentials)) => Arc::new(S3Storage::new(
                config,
                credentials,
                self.clock.clone(),
                self.ids.clone(),
                cooldown.clone(),
            )?),
            (StorageConfig::Dropbox { .. }, ProviderAuth::OAuth { tokens, custody }) => {
                let session = session(tokens, custody)?;
                Arc::new(DropboxStorage::new(config, session)?)
            }
            (StorageConfig::OneDrive { .. }, ProviderAuth::OAuth { tokens, custody }) => {
                let session = session(tokens, custody)?;
                Arc::new(OneDriveStorage::new(config, session, self.ids.clone())?)
            }
            (StorageConfig::CloudKit { .. }, ProviderAuth::CloudKit) => {
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
        Ok(Arc::new(StorageConnection::with_gate(
            provider,
            publications,
            cooldown,
            transfers,
        )))
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

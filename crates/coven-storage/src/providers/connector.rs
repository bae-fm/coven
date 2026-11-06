//! Provider construction from an explicit location and device credentials.

use super::{
    http_client::HttpClient, oauth_folders::OAuthFolders, DropboxStorage, OAuthSession,
    OneDriveStorage, S3Storage, StorageFolder,
};
use crate::{
    ProviderAuth, ProviderOps, S3Credentials, S3DetectionRun, S3DetectionRuns, StorageConfig,
    StorageConnection, StorageCredentials, StorageError, StorageFailure,
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

    /// Make a folder named `name` at the top of the signed-in Dropbox and
    /// share it, refusing a name already taken.
    async fn create_dropbox_shared_folder(
        &self,
        _name: &str,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
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
    /// the sign-in's custody (§20.2). A setup call makes its own
    /// connection beside it (`StorageConnection::during_setup`) for its
    /// requests to draw on its waits (§20.8).
    /// The connection writes for `device`: what it stages sits in that
    /// device's reserved folder (D10).
    async fn connect(
        &self,
        config: StorageConfig,
        auth: ProviderAuth,
        device: DeviceId,
        publications: Arc<dyn crate::PublicationGate>,
        cooldown: Arc<dyn crate::Cooldown>,
        transfers: Arc<crate::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError>;
    /// Probe an S3 location's service (§20.7), writing only fresh objects under
    /// the store's `.coven-check/<run>/` prefix for `runs.run` and deleting
    /// them, also on failure, then removing what each of `runs.earlier`
    /// left there. The caller recorded `runs.run` before calling; the answer
    /// names the runs whose probes and uploads are confirmed gone.
    /// A connector without native S3 refuses this capability.
    /// A rate-limited probe is sent again while setup's `waits` allow (§20.8).
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
        runs: &S3DetectionRuns,
        _waits: &crate::SetupWaits,
    ) -> S3DetectionRun {
        S3DetectionRun::unwritten(runs, Err(StorageFailure::ProviderUnavailable.into()))
    }
    /// Before a store's first setup at a location it needs to itself (a
    /// Dropbox shared folder, a OneDrive folder, or an S3 bucket), refuse a
    /// location whose root holds anything outside the store's folder or the
    /// `coven-location` marker (§20.3).
    /// Reads only. A connector without that provider refuses this capability.
    /// A rate-limited listing is sent again while setup's `waits` allow.
    async fn check_new_store(
        &self,
        _config: StorageConfig,
        _auth: ProviderAuth,
        _store: StoreId,
        _waits: &crate::SetupWaits,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
}

/// The provider composition root retains the supplied clock and id source,
/// and the one HTTP client every connection it makes sends through.
pub struct ProviderConnector {
    clock: ClockRef,
    ids: IdSourceRef,
    /// Building it fails only when TLS cannot be set up; each call needing it
    /// then returns that failure.
    http: Result<HttpClient, Arc<super::http_client::TlsSetupError>>,
    #[cfg(any(test, feature = "test-utils"))]
    folder_api: Option<String>,
}
impl ProviderConnector {
    /// Retain the capabilities used to construct subsequent connections.
    pub fn new(clock: ClockRef, ids: IdSourceRef) -> Self {
        Self {
            http: HttpClient::new(clock.clone()).map_err(Arc::new),
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
    /// Delete every object and abort every unfinished upload in an S3
    /// location's bucket, once its root shows it holds only stores, with
    /// this connector's clock, ids and HTTP client (api.md E14). A
    /// development tool outside the sync protocol.
    #[cfg(feature = "dev-tools")]
    pub async fn empty_s3_bucket(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
    ) -> Result<super::EmptiedS3Bucket, super::EmptyS3BucketError> {
        let http = self
            .http
            .clone()
            .map_err(|source| super::http_client::tls_failure(crate::CloudProvider::S3, source))
            .map_err(super::EmptyS3BucketError::Storage)?;
        super::empty_s3_bucket(config, credentials, self.clock.clone(), self.ids.clone(), http)
            .await
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
        let api = super::oauth_api(access.provider()).expect("an OAuth provider").to_owned();
        #[cfg(any(test, feature = "test-utils"))]
        let api = self.folder_api.clone().unwrap_or(api);
        let session = OAuthSession::new(
            access.provider(),
            tokens,
            custody,
            self.clock.clone(),
            self.http
                .clone()
                .map_err(|source| super::http_client::tls_failure(access.provider(), source))?,
        )?;
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
        self.with_folders(auth, cooldown, |folders| async move { folders.account_label().await })
            .await
    }

    /// Read the signed-in OneDrive root.
    async fn onedrive_root(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move { folders.onedrive_root().await })
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

    /// Make a folder named `name` at the top of the signed-in Dropbox and
    /// share it, refusing a name already taken.
    async fn create_dropbox_shared_folder(
        &self,
        name: &str,
        auth: ProviderAuth,
        cooldown: Arc<dyn crate::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.with_folders(auth, cooldown, |folders| async move {
            folders.create_dropbox_shared_folder(name).await
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
        device: DeviceId,
        publications: Arc<dyn crate::PublicationGate>,
        cooldown: Arc<dyn crate::Cooldown>,
        transfers: Arc<crate::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        crate::ConnectionCredentials { location: config.clone(), credentials: auth.credentials() }
            .validate()?;
        let provider = config.provider();
        let http = self
            .http
            .clone()
            .map_err(|source| super::http_client::tls_failure(provider, source))?;
        let session = |tokens, custody| {
            OAuthSession::new(provider, tokens, custody, self.clock.clone(), http.clone())
        };
        let provider: Arc<dyn ProviderOps> = match (&config, auth) {
            (StorageConfig::S3 { .. }, ProviderAuth::S3(credentials)) => Arc::new(S3Storage::new(
                config,
                credentials,
                self.clock.clone(),
                self.ids.clone(),
                cooldown.clone(),
                http.clone(),
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
        let connection = StorageConnection::with_gate(
            provider,
            device,
            publications,
            cooldown,
            transfers,
            self.clock.clone(),
        );
        Ok(Arc::new(connection))
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        runs: &S3DetectionRuns,
        waits: &crate::SetupWaits,
    ) -> S3DetectionRun {
        let valid = crate::ConnectionCredentials {
            location: config.clone(),
            credentials: StorageCredentials::S3(credentials.clone()),
        }
        .validate();
        let http = self
            .http
            .clone()
            .map_err(|source| super::http_client::tls_failure(crate::CloudProvider::S3, source));
        let http = match valid.and(http) {
            Ok(http) => http,
            Err(error) => return S3DetectionRun::unwritten(runs, Err(error)),
        };
        super::detect_s3(
            config,
            credentials,
            store,
            runs,
            self.clock.clone(),
            self.ids.clone(),
            http,
            waits,
        )
        .await
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: ProviderAuth,
        store: StoreId,
        waits: &crate::SetupWaits,
    ) -> Result<(), StorageError> {
        crate::ConnectionCredentials { location: config.clone(), credentials: auth.credentials() }
            .validate()?;
        if !config.holds_one_store() {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("the location holds several stores, so nothing checks it"));
        }
        let provider = config.provider();
        let http = self
            .http
            .clone()
            .map_err(|source| super::http_client::tls_failure(provider, source))?;
        let session = |tokens, custody| {
            OAuthSession::new(provider, tokens, custody, self.clock.clone(), http.clone())
        };
        // Each page is one listing request: a rate-limited one stored nothing.
        let clock = self.clock.as_ref();
        let holds_only = match (&config, auth) {
            (StorageConfig::S3 { .. }, ProviderAuth::S3(credentials)) => {
                return super::check_new_s3_store(
                    config,
                    credentials,
                    store,
                    self.clock.clone(),
                    self.ids.clone(),
                    http,
                    waits,
                )
                .await
            }
            (StorageConfig::Dropbox { .. }, ProviderAuth::OAuth { tokens, custody }) => {
                let dropbox = DropboxStorage::new(config, session(tokens, custody)?)?;
                waits.resend(clock, || dropbox.holds_only(store)).await?
            }
            (StorageConfig::OneDrive { .. }, ProviderAuth::OAuth { tokens, custody }) => {
                let onedrive =
                    OneDriveStorage::new(config, session(tokens, custody)?, self.ids.clone())?;
                waits.resend(clock, || onedrive.holds_only(store)).await?
            }
            _ => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("credentials do not match the provider"))
            }
        };
        if !holds_only {
            return Err(StorageFailure::OwnLocationRequired.into());
        }
        Ok(())
    }
}

/// Why `provider`'s requests can't be sent: the connector's HTTP client could
/// not set up TLS.

#[cfg(all(test, not(target_vendor = "apple")))]
#[path = "connector_tests.rs"]
mod tests;

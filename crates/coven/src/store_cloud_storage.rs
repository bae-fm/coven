use std::sync::Arc;

use crate::store_security::StoreSecurity;
use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;
use coven_storage::cloud::setup::StorageSetupError;
use coven_storage::cloud::CloudHomeFactory;
#[cfg(any(test, feature = "test-utils"))]
use coven_storage::cloud::ExactCloudHome;
use coven_storage::{BlobChunking, CloudCipher, CloudSyncConnection};

pub(crate) struct PreparedCloudHomeCredentials {
    custody: Arc<dyn coven_keys::keys::CloudHomeCredentialCustody>,
    staged: std::sync::Mutex<Option<Arc<coven_keys::keys::StagedCloudHomeCredentials>>>,
}

impl PreparedCloudHomeCredentials {
    pub(crate) fn commit(&self) -> Result<(), coven_keys::keys::KeyError> {
        match &*self.staged.lock().expect("lock prepared credentials") {
            Some(staged) => staged.commit(),
            None => Ok(()),
        }
    }

    pub(crate) fn rollback(&self) -> Result<(), coven_keys::keys::KeyError> {
        match &*self.staged.lock().expect("lock prepared credentials") {
            Some(staged) => staged.rollback(),
            None => Ok(()),
        }
    }

    pub(crate) fn finish(&self) {
        self.staged
            .lock()
            .expect("lock prepared credentials")
            .take();
    }
}

impl coven_keys::keys::CloudHomeCredentialCustody for PreparedCloudHomeCredentials {
    fn unlock(
        &self,
    ) -> Result<Option<coven_keys::keys::CloudHomeCredentials>, coven_keys::keys::KeyError> {
        self.custody.unlock()
    }

    fn persist(
        &self,
        credentials: &coven_keys::keys::CloudHomeCredentials,
    ) -> Result<(), coven_keys::keys::KeyError> {
        self.custody.persist(credentials)
    }
}

impl Drop for PreparedCloudHomeCredentials {
    fn drop(&mut self) {
        if let Some(staged) = self
            .staged
            .get_mut()
            .expect("lock prepared credentials")
            .take()
        {
            if let Err(error) = staged.rollback() {
                tracing::error!("failed to roll back uncompleted cloud credentials: {error}");
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoreCloudStorage {
    security: StoreSecurity,
    cloud_homes: CloudHomeFactory,
    credentials: coven_keys::keys::CloudHomeCredentialsOwner,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    blob_chunking: BlobChunking,
}

impl StoreCloudStorage {
    pub(crate) fn new(
        security: StoreSecurity,
        cloud_homes: CloudHomeFactory,
        credentials: coven_keys::keys::CloudHomeCredentialsOwner,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: BlobChunking,
    ) -> Self {
        Self {
            security,
            cloud_homes,
            credentials,
            clock,
            cloudkit_ops,
            blob_chunking,
        }
    }

    pub(crate) async fn open(
        &self,
        config: &Config,
        cipher: Option<CloudCipher>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        self.admit(config, cloudkit_ops)?.open(cipher).await
    }

    pub(crate) fn prepare_credentials(
        &self,
        proposed: Option<coven_keys::keys::CloudHomeCredentials>,
    ) -> Arc<PreparedCloudHomeCredentials> {
        let staged = self.credentials.stage(proposed);
        Arc::new(PreparedCloudHomeCredentials {
            custody: staged.clone(),
            staged: std::sync::Mutex::new(Some(staged)),
        })
    }

    #[cfg(feature = "oauth-providers")]
    pub(crate) async fn prepare_oauth_cloud_home(
        &self,
        cloud_home: coven_foundation::config::CloudHomeConfig,
        store_name: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<coven_storage::cloud::PreparedOAuthCloudHome, coven_storage::cloud::SetupError>
    {
        self.cloud_homes
            .prepare_oauth_cloud_home(cloud_home, store_name, cancel, self.clock.as_ref())
            .await
    }

    pub(crate) async fn open_prepared(
        &self,
        config: &Config,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
        credentials: Arc<PreparedCloudHomeCredentials>,
        master_keys: Arc<crate::store_security::PreparedCloudHomeKey>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        coven_storage::cloud::setup::require_exact_slot_capabilities_config(config)?;
        let home = self
            .cloud_homes
            .create(
                config,
                self.clock.clone(),
                cloudkit_ops.or_else(|| self.cloudkit_ops.clone()),
                credentials,
            )
            .await?;
        self.security.open_cloud_storage_with_master_keys(
            config,
            Arc::from(home),
            None,
            self.blob_chunking,
            master_keys,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_prepared_home(
        &self,
        config: &Config,
        home: Arc<dyn ExactCloudHome>,
        master_keys: Arc<crate::store_security::PreparedCloudHomeKey>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        self.security.open_cloud_storage_with_master_keys(
            config,
            home,
            None,
            self.blob_chunking,
            master_keys,
        )
    }

    pub(crate) async fn probe(&self, config: &Config) -> Result<(), StorageSetupError> {
        coven_storage::cloud::setup::require_exact_slot_capabilities_config(config)?;
        let home = self
            .cloud_homes
            .create(
                config,
                self.clock.clone(),
                self.cloudkit_ops.clone(),
                self.credentials.current(),
            )
            .await?;
        home.probe().await.map_err(StorageSetupError::from)
    }

    pub(crate) fn admit<'storage, 'config>(
        &'storage self,
        config: &'config Config,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<AdmittedStoreCloudConfig<'storage, 'config>, StorageSetupError> {
        coven_storage::cloud::setup::require_exact_slot_capabilities_config(config)?;
        Ok(AdmittedStoreCloudConfig {
            storage: self,
            config,
            cloudkit_ops,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn admit_home<'storage, 'config>(
        &'storage self,
        config: &'config Config,
        home: Arc<dyn ExactCloudHome>,
    ) -> Result<AdmittedStoreCloudHome<'storage, 'config>, StorageSetupError> {
        Ok(AdmittedStoreCloudHome {
            storage: self,
            config,
            home,
        })
    }

    async fn open_admitted_config(
        &self,
        config: &Config,
        cipher: Option<CloudCipher>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        let home = self
            .cloud_homes
            .create(
                config,
                self.clock.clone(),
                cloudkit_ops.or_else(|| self.cloudkit_ops.clone()),
                self.credentials.current(),
            )
            .await?;
        self.security
            .open_cloud_storage(config, Arc::from(home), cipher, self.blob_chunking)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn open_admitted_home(
        &self,
        config: &Config,
        home: Arc<dyn ExactCloudHome>,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        self.security
            .open_cloud_storage(config, home, cipher, self.blob_chunking)
    }
}

pub(crate) struct AdmittedStoreCloudConfig<'storage, 'config> {
    storage: &'storage StoreCloudStorage,
    config: &'config Config,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
}

impl AdmittedStoreCloudConfig<'_, '_> {
    pub(crate) async fn open(
        self,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        self.storage
            .open_admitted_config(self.config, cipher, self.cloudkit_ops)
            .await
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) struct AdmittedStoreCloudHome<'storage, 'config> {
    storage: &'storage StoreCloudStorage,
    config: &'config Config,
    home: Arc<dyn ExactCloudHome>,
}

#[cfg(any(test, feature = "test-utils"))]
impl AdmittedStoreCloudHome<'_, '_> {
    pub(crate) fn open(
        self,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncConnection, StorageSetupError> {
        self.storage
            .open_admitted_home(self.config, self.home, cipher)
    }
}

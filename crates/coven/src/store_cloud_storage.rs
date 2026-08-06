use std::sync::Arc;

use crate::storage::cloud::setup::StorageSetupError;
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
use crate::storage::cloud::CloudHomeFactory;
use crate::storage::{BlobChunking, CloudCipher, CloudSyncStorage};
use crate::store_security::StoreSecurity;
use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;

#[derive(Clone)]
pub(crate) struct StoreCloudStorage {
    security: StoreSecurity,
    cloud_homes: CloudHomeFactory,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    blob_chunking: BlobChunking,
}

impl StoreCloudStorage {
    pub(crate) fn new(
        security: StoreSecurity,
        cloud_homes: CloudHomeFactory,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: BlobChunking,
    ) -> Self {
        Self {
            security,
            cloud_homes,
            clock,
            cloudkit_ops,
            blob_chunking,
        }
    }

    pub(crate) async fn open(
        &self,
        config: &Config,
        cipher: Option<CloudCipher>,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<CloudSyncStorage, StorageSetupError> {
        self.admit(config, cloudkit_ops)?.open(cipher).await
    }

    pub(crate) fn admit<'storage, 'config>(
        &'storage self,
        config: &'config Config,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<AdmittedStoreCloudConfig<'storage, 'config>, StorageSetupError> {
        crate::storage::cloud::setup::require_exact_slot_capabilities_config(config)?;
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
        home: Arc<dyn CloudHome>,
    ) -> Result<AdmittedStoreCloudHome<'storage, 'config>, StorageSetupError> {
        crate::storage::cloud::setup::require_exact_slot_capabilities_home(
            home.clone(),
            config.cloud_home.provider.clone(),
        )?;
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
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<CloudSyncStorage, StorageSetupError> {
        let home = self
            .cloud_homes
            .create(
                config,
                self.clock.clone(),
                cloudkit_ops.or_else(|| self.cloudkit_ops.clone()),
            )
            .await?;
        self.security
            .open_cloud_storage(config, Arc::from(home), cipher, self.blob_chunking)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn open_admitted_home(
        &self,
        config: &Config,
        home: Arc<dyn CloudHome>,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncStorage, StorageSetupError> {
        self.security
            .open_cloud_storage(config, home, cipher, self.blob_chunking)
    }
}

pub(crate) struct AdmittedStoreCloudConfig<'storage, 'config> {
    storage: &'storage StoreCloudStorage,
    config: &'config Config,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
}

impl AdmittedStoreCloudConfig<'_, '_> {
    pub(crate) async fn open(
        self,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncStorage, StorageSetupError> {
        self.storage
            .open_admitted_config(self.config, cipher, self.cloudkit_ops)
            .await
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) struct AdmittedStoreCloudHome<'storage, 'config> {
    storage: &'storage StoreCloudStorage,
    config: &'config Config,
    home: Arc<dyn CloudHome>,
}

#[cfg(any(test, feature = "test-utils"))]
impl AdmittedStoreCloudHome<'_, '_> {
    pub(crate) fn open(
        self,
        cipher: Option<CloudCipher>,
    ) -> Result<CloudSyncStorage, StorageSetupError> {
        self.storage
            .open_admitted_home(self.config, self.home, cipher)
    }
}

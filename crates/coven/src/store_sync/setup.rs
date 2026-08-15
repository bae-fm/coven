use super::*;
use crate::cloud_home_setup::{CloudHomeRollbackError, CloudHomeSetupError, ConnectedCloudHome};
use crate::store_cloud_storage::PreparedCloudHomeCredentials;
use crate::store_security::PreparedCloudHomeKey;
use coven_foundation::config::{CloudHomeConfig, CloudProvider};
use coven_keys::keys::CloudHomeCredentials;

impl StoreSync {
    pub(crate) async fn setup_s3(
        &self,
        mut cloud_home: CloudHomeConfig,
        access_key: String,
        secret_key: String,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        cloud_home.provider = Some(CloudProvider::S3);
        self.setup_configured_cloud_home(
            cloud_home,
            Some(CloudHomeCredentials::S3 {
                access_key,
                secret_key,
            }),
            None,
        )
        .await
    }

    pub(crate) async fn setup_cloudkit(
        &self,
        mut cloud_home: CloudHomeConfig,
        cloudkit_ops: Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        cloud_home.provider = Some(CloudProvider::CloudKit);
        self.setup_configured_cloud_home(cloud_home, None, Some(cloudkit_ops))
            .await
    }

    #[cfg(feature = "oauth-providers")]
    pub(crate) async fn setup_oauth(
        &self,
        cloud_home: CloudHomeConfig,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let _lifecycle = self.lifecycle.lock().await;
        let current = self.config();
        let prepared = self
            .cloud_storage
            .prepare_oauth_cloud_home(cloud_home, &current.store_name, cancel)
            .await
            .map_err(|error| CloudHomeSetupError::Connection(Box::new(SyncError::Setup(error))))?;
        self.setup_prepared_oauth_cloud_home_under_lock(prepared)
            .await
    }

    #[cfg(feature = "oauth-providers")]
    async fn setup_prepared_oauth_cloud_home_under_lock(
        &self,
        prepared: coven_storage::cloud::PreparedOAuthCloudHome,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        self.setup_configured_cloud_home_under_lock(
            prepared.cloud_home,
            Some(prepared.credentials),
            None,
        )
        .await
    }

    #[cfg(all(test, feature = "oauth-providers"))]
    pub(crate) async fn setup_prepared_oauth_cloud_home_for_test(
        &self,
        prepared: coven_storage::cloud::PreparedOAuthCloudHome,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.setup_prepared_oauth_cloud_home_under_lock(prepared)
            .await
    }

    async fn setup_configured_cloud_home(
        &self,
        cloud_home: CloudHomeConfig,
        proposed_credentials: Option<CloudHomeCredentials>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.setup_configured_cloud_home_under_lock(cloud_home, proposed_credentials, cloudkit_ops)
            .await
    }

    async fn setup_configured_cloud_home_under_lock(
        &self,
        cloud_home: CloudHomeConfig,
        proposed_credentials: Option<CloudHomeCredentials>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let mut config = self.config();
        config.cloud_home = cloud_home.clone();
        let key = self
            .security
            .prepare_cloud_home_key(cloud_home.storage)
            .map_err(|error| CloudHomeSetupError::MasterKey(Box::new(error)))?;
        let credentials = self.cloud_storage.prepare_credentials(proposed_credentials);
        let storage = match self
            .cloud_storage
            .open_prepared(&config, cloudkit_ops, credentials.clone(), key.clone())
            .await
        {
            Ok(storage) => Arc::new(storage),
            Err(error) => {
                let failure =
                    CloudHomeSetupError::Connection(Box::new(Self::map_storage_setup_error(error)));
                return Err(failure.with_rollback(rollback(&key, &credentials)));
            }
        };
        self.install_prepared_cloud_home(config, storage, cloud_home, &key, &credentials)
            .await
    }

    async fn install_prepared_cloud_home(
        &self,
        config: Config,
        storage: Arc<CloudSyncConnection>,
        cloud_home: CloudHomeConfig,
        key: &Arc<PreparedCloudHomeKey>,
        credentials: &Arc<PreparedCloudHomeCredentials>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let initialization = match self
            .prepare_storage_initialization(
                config,
                storage,
                SyncDriver::Loop,
                SyncKeyCustody::Prepared(key.clone()),
            )
            .await
        {
            Ok(initialization) => initialization,
            Err(error) => {
                let failure = CloudHomeSetupError::Connection(Box::new(error));
                return Err(failure.with_rollback(rollback(key, credentials)));
            }
        };

        if let Err(error) = key.commit() {
            let failure = CloudHomeSetupError::Commit {
                subject: "master key",
                source: Box::new(error),
            };
            return Err(failure.with_rollback(rollback(key, credentials)));
        }
        if let Err(error) = credentials.commit() {
            let failure = CloudHomeSetupError::Commit {
                subject: "credentials",
                source: Box::new(error),
            };
            return Err(failure.with_rollback(rollback(key, credentials)));
        }
        let prepared = match self.initialize_storage(initialization).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let failure = CloudHomeSetupError::Connection(Box::new(error));
                return Err(failure.with_rollback(rollback(key, credentials)));
            }
        };
        self.stop_current();
        prepared.install(self);

        key.finish();
        credentials.finish();
        Ok(ConnectedCloudHome {
            cloud_home,
            key_state: key.state(),
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn setup_with_test_home(
        &self,
        cloud_home: CloudHomeConfig,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        proposed_credentials: Option<CloudHomeCredentials>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut config = self.config();
        config.cloud_home = cloud_home.clone();
        let key = self
            .security
            .prepare_cloud_home_key(cloud_home.storage)
            .map_err(|error| CloudHomeSetupError::MasterKey(Box::new(error)))?;
        let credentials = self.cloud_storage.prepare_credentials(proposed_credentials);
        let storage = match self
            .cloud_storage
            .open_prepared_home(&config, home, key.clone())
        {
            Ok(storage) => Arc::new(storage),
            Err(error) => {
                let failure =
                    CloudHomeSetupError::Connection(Box::new(Self::map_storage_setup_error(error)));
                return Err(failure.with_rollback(rollback(&key, &credentials)));
            }
        };
        self.install_prepared_cloud_home(config, storage, cloud_home, &key, &credentials)
            .await
    }
}

fn rollback(
    key: &PreparedCloudHomeKey,
    credentials: &PreparedCloudHomeCredentials,
) -> Result<(), CloudHomeRollbackError> {
    let credentials_error = credentials.rollback().err();
    let master_key_error = key.rollback().err();
    match (credentials_error, master_key_error) {
        (None, None) => Ok(()),
        (Some(error), None) => Err(CloudHomeRollbackError::Credentials(error)),
        (None, Some(error)) => Err(CloudHomeRollbackError::MasterKey(error)),
        (Some(credentials), Some(master_key)) => Err(CloudHomeRollbackError::Both {
            credentials,
            master_key,
        }),
    }
}

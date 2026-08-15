use std::sync::Arc;

use coven_foundation::config::Config;
#[cfg(test)]
use coven_foundation::config::HomeStorage;
use coven_keys::encryption::{EncryptionService, MasterKeyring, SealError};
use coven_keys::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, IdentityError, KeyError, MasterKeyCustody,
    MasterKeyError, StoreKeys, UserKeypair,
};
use coven_storage::cloud::ExactCloudHome;
use coven_storage::{BlobChunking, BlobPathScheme, CloudCipher, CloudSyncConnection};

/// Whether the selected cloud-home storage needs an available master key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloudHomeKeyState {
    NotRequired,
    Available,
    Locked,
}

pub(crate) struct PreparedCloudHomeKey {
    state: CloudHomeKeyState,
    custody: Arc<dyn MasterKeyCustody>,
    staged: std::sync::Mutex<Option<Arc<coven_keys::keys::StagedMasterKeyCustody>>>,
}

pub(crate) enum SyncKeyCustody {
    Current,
    Prepared(Arc<PreparedCloudHomeKey>),
}

impl PreparedCloudHomeKey {
    pub(crate) fn state(&self) -> CloudHomeKeyState {
        self.state
    }

    pub(crate) fn commit(&self) -> Result<(), KeyError> {
        match &*self.staged.lock().expect("lock prepared master key") {
            Some(staged) => staged.commit(),
            None => Ok(()),
        }
    }

    pub(crate) fn rollback(&self) -> Result<(), KeyError> {
        match &*self.staged.lock().expect("lock prepared master key") {
            Some(staged) => staged.rollback(),
            None => Ok(()),
        }
    }

    pub(crate) fn finish(&self) {
        self.staged.lock().expect("lock prepared master key").take();
    }
}

impl MasterKeyCustody for PreparedCloudHomeKey {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.custody.unlock()
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        self.custody.persist(keyring)
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.custody.forget()
    }
}

impl Drop for PreparedCloudHomeKey {
    fn drop(&mut self) {
        if let Some(staged) = self
            .staged
            .get_mut()
            .expect("lock prepared master key")
            .take()
        {
            if let Err(error) = staged.rollback() {
                tracing::error!("failed to roll back uncompleted cloud-home master key: {error}");
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoreSecurity {
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl StoreSecurity {
    pub(crate) fn new(
        keys: StoreKeys,
        master_keys: Arc<dyn MasterKeyCustody>,
        identity: Arc<dyn DeviceIdentityCustody>,
        store_dir: coven_foundation::store_dir::StoreDir,
    ) -> Self {
        Self {
            keys,
            master_keys,
            identity,
            store_dir,
        }
    }

    pub(crate) async fn prepare_sync_components(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<CloudSyncConnection>,
        initialization: coven_replication::sync::cycle::StoreInitialization,
        key_custody: SyncKeyCustody,
    ) -> Result<coven_replication::sync::cycle::PreparedSyncComponents, crate::store_sync::SyncError>
    {
        let master_keys: Arc<dyn MasterKeyCustody> = match key_custody {
            SyncKeyCustody::Current => self.master_keys.clone(),
            SyncKeyCustody::Prepared(master_keys) => master_keys,
        };
        let routing_encryption = if storage.is_plaintext() {
            None
        } else {
            let keyring = master_keys
                .unlock()?
                .ok_or(coven_keys::keys::RoutingEncryptionError::NotEstablished)?;
            Some(EncryptionService::from(keyring))
        };
        coven_replication::sync::cycle::PreparedSyncComponents::prepare(
            database,
            self.store_dir.clone(),
            storage,
            self.required_identity()?,
            initialization,
            routing_encryption,
            master_keys,
        )
        .await
        .map_err(crate::store_sync::SyncError::from)
    }

    pub(crate) async fn load_store(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
    ) -> Result<coven_replication::sync::Store, crate::store_sync::SyncError> {
        coven_replication::sync::Store::load(
            database,
            storage,
            self.store_dir.clone(),
            self.required_identity()?,
        )
        .await
        .map_err(crate::store_sync::SyncError::from)
    }

    pub(crate) async fn export_activated_device_continuation(
        &self,
        database: &coven_database::StoreDatabase,
    ) -> Result<coven_protocol::recovery::ActivatedContinuation, crate::store_sync::SyncError> {
        let identity = self.required_identity()?;
        Ok(database
            .export_activated_device_continuation(&identity)
            .await?)
    }

    pub(crate) fn import_master_key(&self, serialized: &str) -> Result<(), MasterKeyError> {
        let keyring = MasterKeyring::from_serialized(serialized)?;
        self.master_keys.persist(&keyring)?;
        Ok(())
    }

    pub(crate) fn cloud_home_key_state(
        &self,
        storage: coven_foundation::config::HomeStorage,
    ) -> Result<CloudHomeKeyState, KeyError> {
        if storage.is_browsable() {
            return Ok(CloudHomeKeyState::NotRequired);
        }
        Ok(match self.master_keys.unlock()? {
            Some(_) => CloudHomeKeyState::Available,
            None => CloudHomeKeyState::Locked,
        })
    }

    pub(crate) fn forget_master_key(&self) -> Result<(), KeyError> {
        self.master_keys.forget()
    }

    pub(crate) fn prepare_cloud_home_key(
        &self,
        storage: coven_foundation::config::HomeStorage,
    ) -> Result<Arc<PreparedCloudHomeKey>, MasterKeyError> {
        if storage.is_browsable() {
            return Ok(Arc::new(PreparedCloudHomeKey {
                state: CloudHomeKeyState::NotRequired,
                custody: self.master_keys.clone(),
                staged: std::sync::Mutex::new(None),
            }));
        }
        if self.master_keys.unlock()?.is_some() {
            return Ok(Arc::new(PreparedCloudHomeKey {
                state: CloudHomeKeyState::Available,
                custody: self.master_keys.clone(),
                staged: std::sync::Mutex::new(None),
            }));
        }
        let staged = coven_keys::keys::StagedMasterKeyCustody::new(
            self.master_keys.clone(),
            MasterKeyring::generate(),
        )?;
        Ok(Arc::new(PreparedCloudHomeKey {
            state: CloudHomeKeyState::Available,
            custody: staged.clone(),
            staged: std::sync::Mutex::new(Some(staged)),
        }))
    }

    pub(crate) fn initialize_identity(&self) -> Result<String, IdentityError> {
        if self.identity.unlock()?.is_some() {
            return Err(IdentityError::AlreadyEstablished);
        }
        let identity = UserKeypair::generate();
        self.identity.persist(&identity)?;
        Ok(coven_keys::keys::public_key_hex(&identity))
    }

    fn required_identity(&self) -> Result<UserKeypair, KeyError> {
        coven_keys::keys::require_identity(self.identity.as_ref())
    }

    pub(crate) fn required_identity_public_key_hex(&self) -> Result<String, KeyError> {
        Ok(coven_keys::keys::public_key_hex(&self.required_identity()?))
    }

    pub(crate) fn identity_public_key(&self) -> Result<Option<[u8; 32]>, KeyError> {
        Ok(self
            .identity
            .unlock()?
            .map(|identity| identity.public_key()))
    }

    pub(crate) fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.keys.set_host_secret(name, value)
    }

    pub(crate) fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.keys.get_host_secret(name)
    }

    pub(crate) fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.keys.delete_host_secret(name)
    }

    pub(crate) fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let keyring = self.master_keys.unlock()?.ok_or(SealError::Locked)?;
        Ok(EncryptionService::from(keyring).seal_app_data(plaintext, aad))
    }

    pub(crate) fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let keyring = self.master_keys.unlock()?.ok_or(SealError::Locked)?;
        EncryptionService::from(keyring).open_app_data(sealed, aad)
    }

    pub(crate) fn open_cloud_storage(
        &self,
        config: &Config,
        home: Arc<dyn ExactCloudHome>,
        cipher: Option<CloudCipher>,
        blob_chunking: BlobChunking,
    ) -> Result<CloudSyncConnection, coven_storage::cloud::setup::StorageSetupError> {
        self.open_cloud_storage_with_master_keys(
            config,
            home,
            cipher,
            blob_chunking,
            self.master_keys.clone(),
        )
    }

    pub(crate) fn open_cloud_storage_with_master_keys(
        &self,
        config: &Config,
        home: Arc<dyn ExactCloudHome>,
        cipher: Option<CloudCipher>,
        blob_chunking: BlobChunking,
        master_keys: Arc<dyn MasterKeyCustody>,
    ) -> Result<CloudSyncConnection, coven_storage::cloud::setup::StorageSetupError> {
        let cipher = match cipher {
            Some(cipher) => cipher,
            None if config.cloud_home.storage.is_browsable() => CloudCipher::Plaintext,
            None => {
                let keyring = master_keys
                    .unlock()?
                    .ok_or(coven_storage::cloud::setup::StorageSetupError::NoEncryptionKey)?;
                CloudCipher::Encrypted(keyring.into())
            }
        };
        let identity = self.required_identity()?;
        Ok(CloudSyncConnection::new(
            home,
            cipher,
            BlobPathScheme::for_storage(config.cloud_home.storage),
            config.store_id.clone(),
            identity,
        )
        .with_blob_chunking(blob_chunking))
    }

    #[cfg(test)]
    pub(crate) fn cloud_cipher_fingerprint_for_test(
        &self,
        storage: HomeStorage,
    ) -> Result<Option<String>, coven_storage::cloud::setup::StorageSetupError> {
        if storage.is_browsable() {
            return Ok(None);
        }
        let keyring = self
            .master_keys
            .unlock()?
            .ok_or(coven_storage::cloud::setup::StorageSetupError::NoEncryptionKey)?;
        Ok(Some(EncryptionService::from(keyring).fingerprint()))
    }

    pub(crate) fn generate_restore_code(
        &self,
        config: &Config,
        store_root: coven_protocol::store_commit::StoreRootRef,
        founder_pubkey: String,
        membership_floor: coven_protocol::membership::MembershipFloor,
        authority: coven_protocol::recovery::RestoreAuthority,
    ) -> Result<String, coven_storage::cloud::setup::SetupError> {
        use coven_domain::restoration::{encode_restore_code, RestoreCode, RESTORE_CODE_VERSION};
        use coven_storage::cloud::CloudHomeJoinInfo;

        let cloud_provider = config.cloud_home.provider.as_ref().ok_or_else(|| {
            coven_storage::cloud::setup::SetupError::Configuration(
                "No cloud provider configured. Set up sync first.".to_string(),
            )
        })?;
        let encryption_key = if config.cloud_home.storage.is_opaque() {
            Some(
                self.master_keys
                    .unlock()?
                    .ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError::Configuration(
                            "No encryption key found".to_string(),
                        )
                    })?
                    .to_serialized(),
            )
        } else {
            None
        };

        let provider = match cloud_provider {
            coven_foundation::config::CloudProvider::S3 => {
                let credentials = self.keys.get_cloud_home_credentials()?.ok_or_else(|| {
                    coven_storage::cloud::setup::SetupError::Configuration(
                        "No S3 credentials found in keyring".to_string(),
                    )
                })?;
                let (access_key, secret_key) = match credentials {
                    CloudHomeCredentials::S3 {
                        access_key,
                        secret_key,
                    } => (access_key, secret_key),
                    _ => {
                        return Err(coven_storage::cloud::setup::SetupError::Configuration(
                            "Expected S3 credentials but found different type".to_string(),
                        ))
                    }
                };
                CloudHomeJoinInfo::S3 {
                    bucket: config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError::Configuration(
                            "S3 bucket not configured".to_string(),
                        )
                    })?,
                    region: config.cloud_home.s3_region.clone().ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError::Configuration(
                            "S3 region not configured".to_string(),
                        )
                    })?,
                    endpoint: config.cloud_home.s3_endpoint.clone(),
                    key_prefix: config.cloud_home.s3_key_prefix.clone(),
                    access_key,
                    secret_key,
                }
            }
            coven_foundation::config::CloudProvider::CloudKit => {
                if config.cloud_home.cloudkit_owner_name.is_some()
                    || config.cloud_home.cloudkit_zone_name.is_some()
                {
                    return Err(coven_storage::cloud::setup::SetupError::Configuration(
                        "This store was joined through a CloudKit share; only the store's owner can create a restore code.".to_string(),
                    ));
                }
                CloudHomeJoinInfo::CloudKit
            }
            coven_foundation::config::CloudProvider::GoogleDrive => {
                CloudHomeJoinInfo::GoogleDrive {
                    folder_id: config
                        .cloud_home
                        .google_drive_folder_id
                        .clone()
                        .ok_or_else(|| {
                            coven_storage::cloud::setup::SetupError::Configuration(
                                "Google Drive folder ID not configured".to_string(),
                            )
                        })?,
                }
            }
            coven_foundation::config::CloudProvider::Dropbox => {
                CloudHomeJoinInfo::Dropbox {
                    folder_path: config.cloud_home.dropbox_folder_path.clone().ok_or_else(
                        || {
                            coven_storage::cloud::setup::SetupError::Configuration(
                                "Dropbox folder path not configured".to_string(),
                            )
                        },
                    )?,
                }
            }
            coven_foundation::config::CloudProvider::OneDrive => CloudHomeJoinInfo::OneDrive {
                drive_id: config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                    coven_storage::cloud::setup::SetupError::Configuration(
                        "OneDrive drive ID not configured".to_string(),
                    )
                })?,
                folder_id: config
                    .cloud_home
                    .onedrive_folder_id
                    .clone()
                    .ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError::Configuration(
                            "OneDrive folder ID not configured".to_string(),
                        )
                    })?,
            },
        };

        Ok(encode_restore_code(&RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: config.store_id.clone(),
            ek: encryption_key,
            name: config.store_name.clone(),
            provider,
            store_root,
            founder_pubkey,
            membership_floor,
            authority,
        }))
    }
}

#[cfg(test)]
#[path = "store_security_tests.rs"]
mod tests;

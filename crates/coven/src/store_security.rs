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

    pub(crate) async fn initialize_sync_components(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<CloudSyncConnection>,
        initialization: coven_replication::sync::cycle::StoreInitialization,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<coven_replication::sync::cycle::SyncComponents, crate::store_sync::SyncError> {
        coven_replication::sync::cycle::PreparedSyncComponents::prepare(
            database,
            self.store_dir.clone(),
            storage,
            self.required_identity()?,
            initialization,
            routing_encryption,
        )
        .await
        .map_err(crate::store_sync::SyncError::from)?
        .initialize()
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
    ) -> Result<coven_protocol::recovery::ActivatedContinuation, coven_database::DbError> {
        let identity = self
            .required_identity()
            .map_err(|error| coven_database::DbError::Message(error.to_string()))?;
        database
            .export_activated_device_continuation(&identity)
            .await
    }

    pub(crate) fn initialize_master_key(&self) -> Result<String, MasterKeyError> {
        if self.master_keys.unlock()?.is_some() {
            return Err(MasterKeyError::AlreadyEstablished);
        }
        let keyring = MasterKeyring::generate();
        self.master_keys.persist(&keyring)?;
        Ok(keyring.fingerprint())
    }

    pub(crate) fn import_master_key(&self, serialized: &str) -> Result<String, MasterKeyError> {
        let keyring = MasterKeyring::from_serialized(serialized)?;
        self.master_keys.persist(&keyring)?;
        Ok(keyring.fingerprint())
    }

    pub(crate) fn master_key_fingerprint(&self) -> Result<Option<String>, KeyError> {
        Ok(self
            .master_keys
            .unlock()?
            .map(|keyring| keyring.fingerprint()))
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
        let cipher = match cipher {
            Some(cipher) => cipher,
            None if config.cloud_home.storage.is_browsable() => CloudCipher::Plaintext,
            None => {
                let keyring = self
                    .master_keys
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
            coven_storage::cloud::setup::SetupError(
                "No cloud provider configured. Set up sync first.".to_string(),
            )
        })?;
        let encryption_key = if config.cloud_home.storage.is_opaque() {
            Some(
                self.master_keys
                    .unlock()
                    .map_err(|error| {
                        coven_storage::cloud::setup::SetupError(format!(
                            "Failed to read master key: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError(
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
                let credentials = self
                    .keys
                    .get_cloud_home_credentials()
                    .map_err(|error| {
                        coven_storage::cloud::setup::SetupError(format!(
                            "Failed to read cloud credentials: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError(
                            "No S3 credentials found in keyring".to_string(),
                        )
                    })?;
                let (access_key, secret_key) = match credentials {
                    CloudHomeCredentials::S3 {
                        access_key,
                        secret_key,
                    } => (access_key, secret_key),
                    _ => {
                        return Err(coven_storage::cloud::setup::SetupError(
                            "Expected S3 credentials but found different type".to_string(),
                        ))
                    }
                };
                CloudHomeJoinInfo::S3 {
                    bucket: config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError(
                            "S3 bucket not configured".to_string(),
                        )
                    })?,
                    region: config.cloud_home.s3_region.clone().ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError(
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
                    return Err(coven_storage::cloud::setup::SetupError(
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
                            coven_storage::cloud::setup::SetupError(
                                "Google Drive folder ID not configured".to_string(),
                            )
                        })?,
                }
            }
            coven_foundation::config::CloudProvider::Dropbox => {
                CloudHomeJoinInfo::Dropbox {
                    folder_path: config.cloud_home.dropbox_folder_path.clone().ok_or_else(
                        || {
                            coven_storage::cloud::setup::SetupError(
                                "Dropbox folder path not configured".to_string(),
                            )
                        },
                    )?,
                }
            }
            coven_foundation::config::CloudProvider::OneDrive => CloudHomeJoinInfo::OneDrive {
                drive_id: config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                    coven_storage::cloud::setup::SetupError(
                        "OneDrive drive ID not configured".to_string(),
                    )
                })?,
                folder_id: config
                    .cloud_home
                    .onedrive_folder_id
                    .clone()
                    .ok_or_else(|| {
                        coven_storage::cloud::setup::SetupError(
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

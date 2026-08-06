use std::sync::Arc;

use crate::config::{Config, HomeStorage};
use crate::encryption::{EncryptionService, MasterKeyring, SealError};
use crate::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, IdentityError, KeyError, MasterKeyCustody,
    MasterKeyError, RoutingEncryptionError, StoreKeys, UserKeypair,
};
use crate::storage::cloud::CloudHome;
use crate::storage::{BlobChunking, BlobPathScheme, CloudCipher, CloudSyncStorage};

pub(crate) struct EstablishedStoreIdentity {
    keypair: UserKeypair,
}

impl EstablishedStoreIdentity {
    pub(crate) fn public_key_hex(&self) -> String {
        crate::keys::public_key_hex(&self.keypair)
    }

    pub(crate) async fn initialize_sync_components(
        &self,
        database: crate::database::StoreDatabase,
        store_dir: crate::store_dir::StoreDir,
        local_blob_access: crate::sync::store::blob::LocalStoreBlobAccess,
        storage: Arc<CloudSyncStorage>,
        initialization: crate::sync::cycle::StoreInitialization,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<crate::sync::cycle::SyncComponents, crate::sync::cycle::InitSyncError> {
        crate::sync::cycle::PreparedSyncComponents::prepare(
            database,
            store_dir,
            local_blob_access,
            storage,
            self.keypair.clone(),
            initialization,
            routing_encryption,
        )
        .await?
        .initialize()
        .await
    }

    pub(crate) async fn load_store(
        &self,
        database: crate::database::StoreDatabase,
        storage: Arc<dyn crate::storage::SyncStorage>,
        store_dir: crate::store_dir::StoreDir,
    ) -> Result<crate::sync::Store, crate::sync::store::StoreError> {
        crate::sync::Store::load(database, storage, store_dir, self.keypair.clone()).await
    }

    pub(crate) async fn export_activated_device_continuation(
        &self,
        database: &crate::database::StoreDatabase,
    ) -> Result<crate::protocol::recovery::ActivatedContinuation, crate::database::DbError> {
        database
            .export_activated_device_continuation(&self.keypair)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct StoreSecurity {
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
}

impl StoreSecurity {
    pub(crate) fn new(
        keys: StoreKeys,
        master_keys: Arc<dyn MasterKeyCustody>,
        identity: Arc<dyn DeviceIdentityCustody>,
    ) -> Self {
        Self {
            keys,
            master_keys,
            identity,
        }
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
        Ok(crate::keys::public_key_hex(&identity))
    }

    pub(crate) fn established_identity(&self) -> Result<EstablishedStoreIdentity, KeyError> {
        Ok(EstablishedStoreIdentity {
            keypair: crate::keys::require_identity(self.identity.as_ref())?,
        })
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

    fn app_data_cipher(&self) -> Result<EncryptionService, SealError> {
        let keyring = self.master_keys.unlock()?.ok_or(SealError::Locked)?;
        Ok(EncryptionService::from(keyring))
    }

    pub(crate) fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        Ok(self.app_data_cipher()?.seal_app_data(plaintext, aad))
    }

    pub(crate) fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        self.app_data_cipher()?.open_app_data(sealed, aad)
    }

    pub(crate) fn routing_encryption(
        &self,
        has_scoped_graph: bool,
    ) -> Result<Option<EncryptionService>, RoutingEncryptionError> {
        if !has_scoped_graph {
            return Ok(None);
        }
        let keyring = self
            .master_keys
            .unlock()?
            .ok_or(RoutingEncryptionError::NotEstablished)?;
        Ok(Some(EncryptionService::from(keyring)))
    }

    pub(crate) fn resolve_cloud_cipher(
        &self,
        storage: HomeStorage,
    ) -> Result<CloudCipher, crate::store_sync::SyncError> {
        if storage.is_browsable() {
            return Ok(CloudCipher::Plaintext);
        }
        let keyring = self.master_keys.unlock()?;
        CloudCipher::for_storage(storage, keyring.map(Into::into))
            .ok_or(crate::store_sync::SyncError::MasterKeyNotEstablished)
    }

    pub(crate) fn open_cloud_storage(
        &self,
        config: &Config,
        home: Arc<dyn CloudHome>,
        cipher: Option<CloudCipher>,
        blob_chunking: BlobChunking,
    ) -> Result<CloudSyncStorage, crate::storage::cloud::setup::StorageSetupError> {
        let cipher = match cipher {
            Some(cipher) => cipher,
            None => self.cloud_cipher(config)?,
        };
        let identity = self.established_identity()?;
        Ok(CloudSyncStorage::new(
            home,
            cipher,
            BlobPathScheme::for_storage(config.cloud_home.storage),
            config.store_id.clone(),
            identity.keypair,
        )?
        .with_blob_chunking(blob_chunking))
    }

    fn cloud_cipher(
        &self,
        config: &Config,
    ) -> Result<CloudCipher, crate::storage::cloud::setup::StorageSetupError> {
        if config.cloud_home.storage.is_browsable() {
            return Ok(CloudCipher::Plaintext);
        }
        let keyring = self
            .master_keys
            .unlock()?
            .ok_or(crate::storage::cloud::setup::StorageSetupError::NoEncryptionKey)?;
        Ok(CloudCipher::Encrypted(keyring.into()))
    }

    pub(crate) fn generate_restore_code(
        &self,
        config: &Config,
        store_root: crate::protocol::store_commit::StoreRootRef,
        founder_pubkey: String,
        membership_floor: crate::protocol::membership::MembershipFloor,
        authority: crate::protocol::recovery::RestoreAuthority,
    ) -> Result<String, crate::storage::cloud::setup::SetupError> {
        use crate::restoration::{encode_restore_code, RestoreCode, RESTORE_CODE_VERSION};
        use crate::storage::cloud::CloudHomeJoinInfo;

        let cloud_provider = config.cloud_home.provider.as_ref().ok_or_else(|| {
            crate::storage::cloud::setup::SetupError(
                "No cloud provider configured. Set up sync first.".to_string(),
            )
        })?;
        let encryption_key = if config.cloud_home.storage.is_opaque() {
            Some(
                self.master_keys
                    .unlock()
                    .map_err(|error| {
                        crate::storage::cloud::setup::SetupError(format!(
                            "Failed to read master key: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
                            "No encryption key found".to_string(),
                        )
                    })?
                    .to_serialized(),
            )
        } else {
            None
        };

        let provider = match cloud_provider {
            crate::config::CloudProvider::S3 => {
                let credentials = self
                    .keys
                    .get_cloud_home_credentials()
                    .map_err(|error| {
                        crate::storage::cloud::setup::SetupError(format!(
                            "Failed to read cloud credentials: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
                            "No S3 credentials found in keyring".to_string(),
                        )
                    })?;
                let (access_key, secret_key) = match credentials {
                    CloudHomeCredentials::S3 {
                        access_key,
                        secret_key,
                    } => (access_key, secret_key),
                    _ => {
                        return Err(crate::storage::cloud::setup::SetupError(
                            "Expected S3 credentials but found different type".to_string(),
                        ))
                    }
                };
                CloudHomeJoinInfo::S3 {
                    bucket: config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
                            "S3 bucket not configured".to_string(),
                        )
                    })?,
                    region: config.cloud_home.s3_region.clone().ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
                            "S3 region not configured".to_string(),
                        )
                    })?,
                    endpoint: config.cloud_home.s3_endpoint.clone(),
                    key_prefix: config.cloud_home.s3_key_prefix.clone(),
                    access_key,
                    secret_key,
                }
            }
            crate::config::CloudProvider::CloudKit => {
                if config.cloud_home.cloudkit_owner_name.is_some()
                    || config.cloud_home.cloudkit_zone_name.is_some()
                {
                    return Err(crate::storage::cloud::setup::SetupError(
                        "This store was joined through a CloudKit share; only the store's owner can create a restore code.".to_string(),
                    ));
                }
                CloudHomeJoinInfo::CloudKit
            }
            crate::config::CloudProvider::GoogleDrive => CloudHomeJoinInfo::GoogleDrive {
                folder_id: config
                    .cloud_home
                    .google_drive_folder_id
                    .clone()
                    .ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
                            "Google Drive folder ID not configured".to_string(),
                        )
                    })?,
            },
            crate::config::CloudProvider::Dropbox => {
                CloudHomeJoinInfo::Dropbox {
                    folder_path: config.cloud_home.dropbox_folder_path.clone().ok_or_else(
                        || {
                            crate::storage::cloud::setup::SetupError(
                                "Dropbox folder path not configured".to_string(),
                            )
                        },
                    )?,
                }
            }
            crate::config::CloudProvider::OneDrive => CloudHomeJoinInfo::OneDrive {
                drive_id: config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                    crate::storage::cloud::setup::SetupError(
                        "OneDrive drive ID not configured".to_string(),
                    )
                })?,
                folder_id: config
                    .cloud_home
                    .onedrive_folder_id
                    .clone()
                    .ok_or_else(|| {
                        crate::storage::cloud::setup::SetupError(
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

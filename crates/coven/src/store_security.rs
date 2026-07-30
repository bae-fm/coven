use std::sync::Arc;

use crate::config::{Config, HomeStorage};
use crate::encryption::{EncryptionService, MasterKeyring, SealError};
use crate::keys::{
    DeviceIdentityCustody, IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys,
    UserKeypair,
};
use crate::storage::cloud::{CloudHome, CloudHomeError};
use crate::storage::{BlobChunking, CloudCipher, CloudSyncStorage};

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

    pub(crate) fn require_identity(&self) -> Result<UserKeypair, KeyError> {
        crate::keys::require_identity(self.identity.as_ref())
    }

    pub(crate) fn identity_public_key(&self) -> Result<Option<[u8; 32]>, KeyError> {
        crate::keys::identity_public_key(self.identity.as_ref())
    }

    pub(crate) fn adopt_key_rotation(
        &self,
        cipher: &dyn crate::storage::CloudCipherAccess,
        encryption: &EncryptionService,
    ) -> Result<String, KeyError> {
        cipher.adopt_key_rotation(encryption, self.master_keys.as_ref())
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
    ) -> Result<Option<EncryptionService>, crate::database::DbError> {
        if !has_scoped_graph {
            return Ok(None);
        }
        let keyring = self
            .master_keys
            .unlock()
            .map_err(|error| {
                crate::database::DbError::Message(format!(
                    "unlock Store key for row routing: {error}"
                ))
            })?
            .ok_or_else(|| {
                crate::database::DbError::Message(
                    "Merge scoped write requires an established Store key".to_string(),
                )
            })?;
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

    pub(crate) async fn create_cloud_home(
        &self,
        config: &Config,
        clock: crate::clock::ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<Box<dyn CloudHome>, CloudHomeError> {
        crate::storage::cloud::create_cloud_home_with_cloudkit(
            config,
            &self.keys,
            clock,
            cloudkit_ops,
        )
        .await
    }

    pub(crate) async fn create_sync_storage(
        &self,
        config: &Config,
        cipher: Option<CloudCipher>,
        clock: crate::clock::ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: BlobChunking,
    ) -> Result<CloudSyncStorage, crate::storage::cloud::setup::StorageSetupError> {
        crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
            config,
            &self.keys,
            self.master_keys.as_ref(),
            self.identity.as_ref(),
            cipher,
            clock,
            cloudkit_ops,
            blob_chunking,
        )
        .await
    }

    pub(crate) fn create_sync_storage_with_home(
        &self,
        config: &Config,
        home: Arc<dyn CloudHome>,
        cipher: Option<CloudCipher>,
        blob_chunking: BlobChunking,
    ) -> Result<CloudSyncStorage, crate::storage::cloud::setup::StorageSetupError> {
        crate::storage::cloud::setup::create_sync_storage_with_home(
            config,
            self.master_keys.as_ref(),
            self.identity.as_ref(),
            home,
            cipher,
            blob_chunking,
        )
    }

    pub(crate) fn generate_restore_code(
        &self,
        config: &Config,
        store_root: crate::protocol::store_commit::StoreRootRef,
        founder_pubkey: String,
        membership_floor: crate::joining::MembershipFloor,
        authority: crate::restoration::RestoreAuthority,
    ) -> Result<String, crate::storage::cloud::setup::SetupError> {
        crate::storage::cloud::setup::generate_restore_code(
            config,
            &self.keys,
            self.master_keys.as_ref(),
            store_root,
            founder_pubkey,
            membership_floor,
            authority,
        )
    }
}

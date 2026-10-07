//! Restore-code generation and credential replacement (§12.1, §20.9).

use crate::SyncError;
use coven_crypto::{
    custody::{MemberKeyCustody, StoreKeychain},
    SecretBytes, SecretText,
};
use coven_database::{Database, DbError};
use coven_format::codes::RestoreCode;
use coven_storage::{RestoreStorage, S3Credentials, Storage, StorageCredentials, StorageSettings};
use std::sync::Arc;

/// Owns code generation and credential changes for one open handle. Closing
/// waits for updates holding the lock, even when their app futures were dropped.
#[derive(Clone)]
pub struct RestoreCodes {
    inner: Arc<tokio::sync::Mutex<Option<RestoreCodesInner>>>,
}
struct RestoreCodesInner {
    database: Database,
    identity: Arc<dyn MemberKeyCustody>,
    keychain: Arc<StoreKeychain>,
    settings: StorageSettings,
    storage: Option<Arc<dyn Storage>>,
    operations: crate::Operations,
}

impl RestoreCodes {
    /// Compose at the facade's opening boundary. The settings and credentials
    /// are also the narrow persisted connection contract used by storage setup.
    pub fn new(
        database: Database,
        identity: Arc<dyn MemberKeyCustody>,
        keychain: Arc<StoreKeychain>,
        settings: StorageSettings,
        storage: Option<Arc<dyn Storage>>,
        operations: crate::Operations,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Some(RestoreCodesInner {
                database,
                identity,
                keychain,
                settings,
                storage,
                operations,
            }))),
        }
    }

    /// Encode this member's current identity and committed provider credentials.
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        Ok(owner.code().await?.to_text()?.to_string())
    }

    /// Install a manually supplied S3 key, publish its id, then return the code.
    /// Requires connected storage. Credentials remain committed if publication
    /// fails: the remote entry may already exist. Retry with the same key to
    /// finish publication; queued entry bytes and numbers are reused.
    /// A removal records this key for deletion even if its entry loses in replay.
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<String, SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            owner.storage.as_ref().ok_or(SyncError::NoStorage)?;
            let mut code = owner.code().await?;
            let mut data = RestoreStorage::decode(code.storage.as_bytes())?;
            data.credentials = StorageCredentials::S3(S3Credentials {
                access_key_id: access_key_id.clone(),
                secret_access_key,
            });
            code.storage = data.encode()?;
            owner.install(&code).await?;
            owner
                .operations
                .set_access(coven_format::MemberAccess::S3AccessKey { access_key_id })
                .await?;
            Ok(code.to_text()?.to_string())
        }))
        .await
    }

    /// Keep only the credentials from a code for this store and this member.
    /// A provider location change is refused; this call cannot redirect a store.
    /// Disconnected handles commit custody for their next connection.
    pub async fn update_credentials(&self, code: &str) -> Result<(), SyncError> {
        let replacement = crate::read_restore_code(code)?;
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            let mut current = owner.code().await?;
            if current.store != replacement.store {
                return Err(SyncError::WrongStore {
                    expected: current.store,
                    actual: replacement.store,
                });
            }
            let expected = current.member_keys.member_id();
            let actual = replacement.member_keys.member_id();
            if expected != actual
                || current.member_keys.sealing_public_key()
                    != replacement.member_keys.sealing_public_key()
            {
                return Err(SyncError::WrongMember { expected, actual });
            }
            let old = RestoreStorage::decode(current.storage.as_bytes())?;
            let new = RestoreStorage::decode(replacement.storage.as_bytes())?;
            if old.location != new.location {
                return Err(coven_storage::StorageError::InvitationMismatch.into());
            }
            current.storage = replacement.storage;
            owner.install(&current).await
        }))
        .await
    }

    /// Finish the current update, then release custody and connection owners.
    /// Updates that have not acquired the lock receive StoreClosed.
    pub async fn close(&self) {
        self.inner.lock().await.take();
    }
}

impl RestoreCodesInner {
    async fn code(&self) -> Result<RestoreCode, SyncError> {
        let local = self.database.local_store_log().await?;
        let store = local.log.replay.state.store.ok_or(SyncError::NoStorage)?;
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        let bytes = self
            .keychain
            .storage_credentials()?
            .ok_or(SyncError::NoStorage)?;
        let credentials = StorageCredentials::decode(bytes.as_bytes())?;
        let member_keys = self
            .identity
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        Ok(RestoreCode {
            store: store.id,
            name: store.name,
            member_keys,
            storage: RestoreStorage {
                location,
                credentials,
            }
            .encode()?,
        })
    }

    async fn install(&self, code: &RestoreCode) -> Result<(), SyncError> {
        let Some(storage) = self.storage.as_deref() else {
            return commit_restore_code(&self.keychain, code);
        };
        let current = self.code().await?;
        let previous = RestoreStorage::decode(current.storage.as_bytes())?;
        let next = RestoreStorage::decode(code.storage.as_bytes())?;
        if storage.config() != next.location {
            return Err(coven_storage::StorageError::InvitationMismatch.into());
        }
        commit_restore_code(&self.keychain, code)?;
        if let Err(operation) = install_session(storage, next.credentials).await {
            let mut error = SyncError::Storage(operation);
            if let Err(cleanup) = commit_restore_code(&self.keychain, &current) {
                error = cleanup_error(error, cleanup);
            }
            if let Err(cleanup) = install_session(storage, previous.credentials).await {
                error = cleanup_error(error, cleanup.into());
            }
            return Err(error);
        }
        Ok(())
    }
}

async fn install_session(
    storage: &dyn Storage,
    credentials: StorageCredentials,
) -> Result<(), coven_storage::StorageError> {
    match credentials {
        StorageCredentials::S3(keys) => storage.set_s3_credentials(keys).await,
        StorageCredentials::OAuth(tokens) => storage.set_oauth_tokens(tokens).await,
        StorageCredentials::CloudKit => Ok(()),
    }
}

/// Commit the device credentials and, on Apple, the matching restore code.
/// Bootstrap and storage setup use the same operation as credential updates;
/// errors restore both previous values and retain every rollback failure.
pub fn commit_restore_code(keychain: &StoreKeychain, code: &RestoreCode) -> Result<(), SyncError> {
    let data = RestoreStorage::decode(code.storage.as_bytes())?;
    let credentials = data.credentials.encode()?;
    let encoded = SecretBytes::new(code.to_bytes()?.to_vec());
    let prior_credentials = keychain.storage_credentials()?;
    let prior_code = if keychain.supports_synced_restore_codes() {
        keychain.synced_restore_code()?
    } else {
        None
    };
    let write = || -> Result<(), SyncError> {
        if keychain.supports_synced_restore_codes() {
            keychain.set_synced_restore_code(&encoded)?;
        }
        keychain.set_storage_credentials(&credentials)?;
        Ok(())
    };
    if let Err(mut operation) = write() {
        let rollback = match prior_credentials {
            Some(bytes) => keychain.set_storage_credentials(&bytes),
            None => keychain.delete_storage_credentials(),
        };
        if let Err(cleanup) = rollback {
            operation = cleanup_error(operation, cleanup.into());
        }
        if keychain.supports_synced_restore_codes() {
            let rollback = match prior_code {
                Some(bytes) => keychain.set_synced_restore_code(&bytes),
                None => keychain.delete_synced_restore_code(),
            };
            if let Err(cleanup) = rollback {
                operation = cleanup_error(operation, cleanup.into());
            }
        }
        return Err(operation);
    }
    Ok(())
}

fn cleanup_error(operation: SyncError, cleanup: SyncError) -> SyncError {
    SyncError::Cleanup {
        operation: Box::new(operation),
        cleanup: Box::new(cleanup),
    }
}

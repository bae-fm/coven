//! Restore-code generation and credential replacement (§12.1, E9).

use crate::SyncError;
use coven_crypto::{
    custody::{MemberKeyCustody, StoreKeychain},
    SecretBytes, SecretText,
};
use coven_database::{Database, DbError};
use coven_format::codes::RestoreCode;
use coven_storage::{
    ConnectionCredentials, RestoreStorage, S3Credentials, Storage, StorageConfig,
    StorageCredentials, StorageSettings,
};
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
    operations: crate::Operations,
    oauth: Option<coven_storage::providers::OAuthFlow>,
}

impl RestoreCodes {
    /// Compose at the facade's opening boundary. The settings and credentials
    /// are also the narrow persisted connection contract used by storage setup.
    pub fn new(
        database: Database,
        identity: Arc<dyn MemberKeyCustody>,
        keychain: Arc<StoreKeychain>,
        settings: StorageSettings,
        operations: crate::Operations,
        oauth: Option<coven_storage::providers::OAuthFlow>,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Some(RestoreCodesInner {
                database,
                identity,
                keychain,
                settings,
                operations,
                oauth,
            }))),
        }
    }

    /// Encode this member's identity, location and S3 key where required.
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
            let mut data = owner.connection()?.ok_or(SyncError::NoStorage)?;
            data.credentials = StorageCredentials::S3(S3Credentials {
                access_key_id: access_key_id.clone(),
                secret_access_key,
            });
            owner.install(data, true).await?;
            let text = owner.code().await?.to_text()?.to_string();
            owner
                .operations
                .set_access(coven_format::MemberAccess::S3AccessKey { access_key_id })
                .await?;
            Ok(text)
        }))
        .await
    }

    /// Keep only the S3 key from a code for this store and this member.
    /// A provider location change is refused; this call cannot redirect a store.
    /// Disconnected handles commit custody for their next connection.
    pub async fn update_credentials(&self, code: &str) -> Result<(), SyncError> {
        let replacement = crate::read_restore_code(code)?;
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            let current = owner.code().await?;
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
            let old = owner.connection()?.ok_or(SyncError::NoStorage)?;
            let new = RestoreStorage::decode(replacement.storage.as_bytes())?;
            if &old.location != new.location() {
                return Err(coven_storage::StorageFailure::InvitationMismatch.into());
            }
            let RestoreStorage::S3 {
                location,
                credentials,
            } = new
            else {
                return Err(coven_storage::StorageFailure::InvalidConfiguration
                    .with_source("account restore codes contain no credentials to replace")
                    .into());
            };
            owner
                .install(
                    ConnectionCredentials {
                        location,
                        credentials: StorageCredentials::S3(credentials),
                    },
                    false,
                )
                .await
        }))
        .await
    }

    /// Read committed provider data without exposing custody or a connection.
    pub async fn connection(&self) -> Result<Option<ConnectionCredentials>, SyncError> {
        let guard = self.inner.lock().await;
        guard.as_ref().ok_or(DbError::StoreClosed)?.connection()
    }

    /// Remove local credentials before disconnecting; failed removal changes no connection.
    pub async fn forget_credentials(&self) -> Result<(), SyncError> {
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .ok_or(DbError::StoreClosed)?
            .keychain
            .delete_storage_credentials()?;
        Ok(())
    }

    /// Reserve the credential owner until remote setup either commits or fails.
    /// The returned commit includes the synced restore code and location, and its
    /// compensation remains available until store keys and the origin are applied.
    pub(crate) async fn prepare_setup(
        &self,
        data: ConnectionCredentials,
        initial_name: String,
    ) -> Result<crate::StorageCommit, SyncError> {
        data.validate()?;
        let guard = self.inner.clone().lock_owned().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let local = owner.database.local_store_log().await?;
        let code = RestoreCode {
            store: local.store,
            name: local
                .log
                .replay
                .state
                .store
                .map_or(initial_name, |store| store.name),
            member_keys: owner
                .identity
                .unlock()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?,
            storage: RestoreStorage::from_connection(&data).encode()?,
        };
        let previous = SavedConnection {
            location: owner.settings.read()?,
            credentials: owner.keychain.storage_credentials()?,
            code: if owner.keychain.supports_synced_restore_codes() {
                owner.keychain.synced_restore_code()?
            } else {
                None
            },
        };
        Ok(Box::new(move || {
            let owner = guard.as_ref().expect("reserved credential owner");
            commit_credentials(&owner.keychain, &data.credentials, Some(&code))?;
            if let Err(error) = owner.settings.commit(&data.location) {
                return Err(match owner.restore_connection(previous) {
                    Ok(()) => error.into(),
                    Err(cleanup) => cleanup_error(error.into(), cleanup),
                });
            }
            Ok(Box::new(move || {
                guard
                    .as_ref()
                    .expect("reserved credential owner")
                    .restore_connection(previous)
            }))
        }))
    }

    /// Refresh expired credentials before a connection or sync pass. Commit
    /// refreshed tokens before installing the live session; the code stays unchanged.
    pub async fn refresh_if_expired(&self, now: std::time::SystemTime) -> Result<(), SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let Some(location) = owner.settings.read()? else {
            return Ok(());
        };
        let Some(bytes) = owner.keychain.storage_credentials()? else {
            return Ok(());
        };
        let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(bytes.as_bytes())?
        else {
            return Ok(());
        };
        if tokens.expires_at.is_none_or(|expiry| now < expiry) {
            return Ok(());
        }
        let clients = owner.oauth.as_ref().ok_or(
            coven_storage::StorageFailure::InvalidConfiguration
                .with_source("OAuth clients are absent"),
        )?;
        let tokens = clients
            .refresh(location.provider(), &tokens)
            .await
            .map_err(|error| match error {
                coven_storage::providers::OAuthError::Storage(error) => error,
                error => coven_storage::StorageError::Provider {
                    provider: location.provider(),
                    failure: coven_storage::StorageFailure::Authentication,
                    source: Box::new(error),
                },
            })?;
        owner
            .install(
                ConnectionCredentials {
                    location,
                    credentials: StorageCredentials::OAuth(tokens),
                },
                false,
            )
            .await
    }

    /// Finish the current update, then release custody and connection owners.
    /// Updates that have not acquired the lock receive StoreClosed.
    pub async fn close(&self) {
        self.inner.lock().await.take();
    }
}

impl RestoreCodesInner {
    fn restore_connection(&self, previous: SavedConnection) -> Result<(), SyncError> {
        let mut failure = None;
        let credentials = match previous.credentials {
            Some(bytes) => self.keychain.set_storage_credentials(&bytes),
            None => self.keychain.delete_storage_credentials(),
        }
        .map_err(SyncError::from);
        let code = if self.keychain.supports_synced_restore_codes() {
            match previous.code {
                Some(bytes) => self.keychain.set_synced_restore_code(&bytes),
                None => self.keychain.delete_synced_restore_code(),
            }
            .map_err(SyncError::from)
        } else {
            Ok(())
        };
        let location = match previous.location {
            Some(location) => self.settings.commit(&location),
            None => self.settings.remove(),
        }
        .map_err(SyncError::from);
        for result in [credentials, code, location] {
            if let Err(error) = result {
                failure = Some(match failure {
                    Some(operation) => cleanup_error(operation, error),
                    None => error,
                });
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn connection(&self) -> Result<Option<ConnectionCredentials>, SyncError> {
        let Some(location) = self.settings.read()? else {
            return Ok(None);
        };
        let Some(bytes) = self.keychain.storage_credentials()? else {
            return Ok(None);
        };
        let data = ConnectionCredentials {
            location,
            credentials: StorageCredentials::decode(bytes.as_bytes())?,
        };
        data.validate()?;
        Ok(Some(data))
    }

    async fn code(&self) -> Result<RestoreCode, SyncError> {
        let local = self.database.local_store_log().await?;
        let store = local.log.replay.state.store.ok_or(SyncError::NoStorage)?;
        let data = self.connection()?.ok_or(SyncError::NoStorage)?;
        let member_keys = self
            .identity
            .unlock()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        Ok(RestoreCode {
            store: store.id,
            name: store.name,
            member_keys,
            storage: RestoreStorage::from_connection(&data).encode()?,
        })
    }

    async fn install(
        &self,
        next: ConnectionCredentials,
        require_connected: bool,
    ) -> Result<(), SyncError> {
        next.validate()?;
        let previous = self.connection()?.ok_or(SyncError::NoStorage)?;
        let codes = if matches!(next.credentials, StorageCredentials::S3(_)) {
            let current = self.code().await?;
            let code = RestoreCode {
                store: current.store,
                name: current.name.clone(),
                member_keys: current.member_keys.clone(),
                storage: RestoreStorage::from_connection(&next).encode()?,
            };
            Some((current, code))
        } else {
            None
        };
        let previous_credentials = previous.credentials.clone();
        let credentials = next.credentials.clone();
        let keychain = self.keychain.clone();
        self.operations
            .install_credentials(
                previous,
                next,
                require_connected,
                Box::new(move || {
                    commit_credentials(
                        &keychain,
                        &credentials,
                        codes.as_ref().map(|(_, next)| next),
                    )?;
                    Ok(Box::new(move || {
                        commit_credentials(
                            &keychain,
                            &previous_credentials,
                            codes.as_ref().map(|(previous, _)| previous),
                        )
                    }))
                }),
            )
            .await
    }
}

pub(crate) async fn install_session(
    storage: &dyn Storage,
    credentials: StorageCredentials,
) -> Result<(), coven_storage::StorageError> {
    match credentials {
        StorageCredentials::S3(keys) => storage.set_s3_credentials(keys).await,
        StorageCredentials::OAuth(tokens) => storage.set_oauth_tokens(tokens).await,
        StorageCredentials::CloudKit => Ok(()),
    }
}

/// Commit device credentials and an optional changed restore code. OAuth refresh
/// supplies no code: it neither unlocks member keys nor touches synced custody.
/// Bootstrap, setup and S3 replacement supply the code. Errors restore every
/// value this call changed and retain every rollback failure.
pub fn commit_credentials(
    keychain: &StoreKeychain,
    credentials: &StorageCredentials,
    code: Option<&RestoreCode>,
) -> Result<(), SyncError> {
    let credentials = credentials.encode()?;
    let encoded = match code {
        Some(code) if keychain.supports_synced_restore_codes() => {
            Some(SecretBytes::new(code.to_bytes()?.to_vec()))
        }
        _ => None,
    };
    let prior_credentials = keychain.storage_credentials()?;
    let prior_code = if encoded.is_some() {
        keychain.synced_restore_code()?
    } else {
        None
    };
    let write = || -> Result<(), SyncError> {
        if let Some(encoded) = &encoded {
            keychain.set_synced_restore_code(encoded)?;
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
        if encoded.is_some() {
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

struct SavedConnection {
    location: Option<StorageConfig>,
    credentials: Option<SecretBytes>,
    code: Option<SecretBytes>,
}

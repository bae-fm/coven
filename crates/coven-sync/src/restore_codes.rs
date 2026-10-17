//! Restore-code generation and credential replacement (§12.1, E9).

use crate::{
    commit_credentials, credential_custody::cleanup_error, CredentialRollback, DeviceCredentials,
    SessionSignIn, SyncError,
};
use coven_crypto::{
    custody::{KeySession, StoreKeychain},
    MemberIdentity, SecretText,
};
use coven_database::{Database, DbError};
use coven_format::codes::RestoreCode;
use coven_storage::{
    ConnectionCredentials, S3Credentials, Storage, StorageConfig, StorageCredentials,
    StorageSettings,
};
use std::sync::Arc;

/// Owns code generation and credential changes for one open handle. Closing
/// waits for updates holding the lock, even when their app futures were dropped.
#[derive(Clone)]
pub struct RestoreCodes {
    inner: Arc<tokio::sync::Mutex<Option<RestoreCodesInner>>>,
    credentials: Arc<DeviceCredentials>,
}
struct RestoreCodesInner {
    database: Database,
    identity: Arc<KeySession<MemberIdentity>>,
    credentials: Arc<DeviceCredentials>,
    settings: StorageSettings,
    operations: crate::Operations,
}

impl RestoreCodes {
    /// The recorded location survives disconnecting its credentials.
    pub async fn location(&self) -> Result<Option<coven_storage::StorageConfig>, SyncError> {
        let owner = self.inner.lock().await;
        let owner = owner.as_ref().ok_or(coven_database::DbError::StoreClosed)?;
        Ok(owner.settings.read()?)
    }

    /// Compose at the facade's opening boundary. The settings and credentials
    /// are also the narrow persisted connection contract used by storage setup.
    pub fn new(
        database: Database,
        identity: Arc<KeySession<MemberIdentity>>,
        credentials: Arc<DeviceCredentials>,
        settings: StorageSettings,
        operations: crate::Operations,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Some(RestoreCodesInner {
                database,
                identity,
                credentials: credentials.clone(),
                settings,
                operations,
            }))),
            credentials,
        }
    }

    /// How a connection reaches storage with these committed credentials: a
    /// rejected OAuth token refreshes in this installation's device custody,
    /// which takes no lock held by credential changes (§4).
    pub fn provider_auth(&self, credentials: StorageCredentials) -> coven_storage::ProviderAuth {
        coven_storage::ProviderAuth::new(credentials, self.credentials.clone())
    }

    /// Encode this member's stable id and current private pair, store id and
    /// name, and storage location only. The pair is the held one replay
    /// binds now; without it the call reports `MemberKeysReplaced` rather
    /// than exporting a retired pair (§12.1).
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        Ok(owner.code().await?.to_text()?.to_string())
    }

    /// Replace this member's pair and return the code for that exact result.
    /// Credential changes and close wait for this owned operation.
    pub async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            let local = owner.database.local_store_log().await?;
            let store = local.log.replay.state.store.ok_or(SyncError::NoStorage)?;
            let location = owner
                .settings
                .read()?
                .ok_or(SyncError::NoStorage)?
                .encode()?;
            let identity = owner.operations.replace_member_keys().await?;
            let code = RestoreCode {
                store: store.id,
                name: store.name,
                member: identity.member(),
                member_keys: identity.keys().clone(),
                storage: location,
            };
            Ok(SecretText::new(code.to_text()?.to_string()))
        }))
        .await
    }

    /// Install a manually supplied S3 key and publish its id; the code stays unchanged.
    /// Requires connected storage. Credentials remain committed if publication
    /// fails: the remote entry may already exist. Retry with the same key to
    /// finish publication; queued entry bytes and numbers are reused.
    /// A removal records this key for deletion even if its entry loses in replay.
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            owner
                .install_s3(
                    S3Credentials {
                        access_key_id: access_key_id.clone(),
                        secret_access_key,
                    },
                    true,
                )
                .await?;
            owner
                .operations
                .set_access(coven_format::MemberAccess::S3AccessKey {
                    access_key_id,
                    principal: coven_format::S3Principal::Console,
                })
                .await?;
            Ok(())
        }))
        .await
    }

    /// Keep this member's separately supplied S3 key without changing identity,
    /// location or code. Disconnected handles retain it for their next connection.
    pub async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            owner
                .install_s3(
                    S3Credentials {
                        access_key_id,
                        secret_access_key,
                    },
                    false,
                )
                .await
        }))
        .await
    }

    /// Keep a key an admin delivered to this member, unless this device already
    /// uses it. Custody commits it before the connection does (§13).
    pub(crate) async fn adopt_delivered_key(
        &self,
        key: coven_format::access_key_delivery::DeliveredKey,
    ) -> Result<(), SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            if let Some(ConnectionCredentials {
                credentials: StorageCredentials::S3(current),
                ..
            }) = owner.connection()?
            {
                if current.access_key_id == key.access_key_id {
                    return Ok(());
                }
            }
            owner
                .install_s3(
                    S3Credentials {
                        access_key_id: key.access_key_id,
                        secret_access_key: key.secret_access_key,
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

    /// Commit a verified OAuth upgrade before installing it on the live connection.
    /// The existing credential operation rolls custody back if installation fails.
    pub async fn install_oauth(&self, tokens: coven_storage::OAuthTokens) -> Result<(), SyncError> {
        let inner = self.inner.clone();
        crate::files::join(tokio::spawn(async move {
            let guard = inner.lock().await;
            let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
            let location = owner.settings.read()?.ok_or(SyncError::NoStorage)?;
            owner
                .install(
                    ConnectionCredentials {
                        location,
                        credentials: StorageCredentials::OAuth(tokens),
                    },
                    true,
                )
                .await
        }))
        .await
    }

    /// Remove local credentials before disconnecting; failed removal changes no connection.
    pub async fn forget_credentials(&self) -> Result<(), SyncError> {
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .ok_or(DbError::StoreClosed)?
            .credentials
            .write(|keychain| Ok(keychain.delete_storage_credentials()?))
    }

    /// Reserve the credential owner until remote setup either commits or fails.
    /// The returned commit includes the synced restore code and location, and its
    /// compensation remains available until store keys and the origin are applied.
    /// An OAuth setup commits `sign_in`'s current tokens, which then refresh in
    /// device custody; a rollback holds them in the session again.
    pub(crate) async fn prepare_setup(
        &self,
        data: ConnectionCredentials,
        sign_in: Option<Arc<SessionSignIn>>,
        initial_name: String,
    ) -> Result<(crate::StorageCommit, SecretText), SyncError> {
        data.validate()?;
        let guard = self.inner.clone().lock_owned().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let local = owner.database.local_store_log().await?;
        let identity = owner
            .identity
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        // The code names the stable id its unchanged private-pair blob belongs to (D13).
        let code = RestoreCode {
            store: local.store,
            name: local
                .log
                .replay
                .state
                .store
                .map_or(initial_name, |store| store.name),
            member: identity.member(),
            member_keys: identity.keys().clone(),
            storage: data.location.encode()?,
        };
        let text = SecretText::new(code.to_text()?.to_string());
        let previous_location = owner.settings.read()?;
        Ok((
            Box::new(move || {
                let owner = guard.as_ref().expect("reserved credential owner");
                let previous = match &sign_in {
                    Some(session) => session.publish(&owner.credentials, |keychain, tokens| {
                        commit_credentials(
                            keychain,
                            &StorageCredentials::OAuth(tokens.clone()),
                            Some(&code),
                        )
                    })?,
                    None => owner.credentials.write(|keychain| {
                        commit_credentials(keychain, &data.credentials, Some(&code))
                    })?,
                };
                let rollback = move |owner: &RestoreCodesInner, previous| {
                    let result = owner.restore_connection(previous_location, previous);
                    if let Some(session) = &sign_in {
                        session.unpublish();
                    }
                    result
                };
                if let Err(error) = owner.settings.commit(&data.location) {
                    return Err(match rollback(owner, previous) {
                        Ok(()) => error.into(),
                        Err(cleanup) => cleanup_error(error.into(), cleanup),
                    });
                }
                Ok(Box::new(move || {
                    rollback(guard.as_ref().expect("reserved credential owner"), previous)
                }))
            }),
            text,
        ))
    }

    /// Finish the current update, then release custody and connection owners.
    /// Updates that have not acquired the lock receive StoreClosed.
    pub async fn close(&self) {
        self.inner.lock().await.take();
    }
}

impl RestoreCodesInner {
    fn restore_connection(
        &self,
        location: Option<StorageConfig>,
        credentials: CredentialRollback,
    ) -> Result<(), SyncError> {
        let credentials = self
            .credentials
            .write(|keychain| credentials.restore(keychain));
        let location = match location {
            Some(location) => self.settings.commit(&location),
            None => self.settings.remove(),
        }
        .map_err(SyncError::from);
        match (credentials, location) {
            (Ok(()), result) | (result, Ok(())) => result,
            (Err(operation), Err(cleanup)) => Err(cleanup_error(operation, cleanup)),
        }
    }

    fn connection(&self) -> Result<Option<ConnectionCredentials>, SyncError> {
        self.credentials.connection(&self.settings)
    }

    async fn code(&self) -> Result<RestoreCode, SyncError> {
        let local = self.database.local_store_log().await?;
        let state = &local.log.replay.state;
        let store = state.store.clone().ok_or(SyncError::NoStorage)?;
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        let identity = self
            .identity
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let current = crate::effects::member(state, &identity.member())
            .ok_or(SyncError::Removed)?
            .keys;
        // A kept self-replacement's candidate is current before custody
        // installs it; a retired pair never is.
        let selected = identity
            .activate(&current)
            .ok_or(SyncError::MemberKeysReplaced {
                member: identity.member(),
            })?;
        Ok(RestoreCode {
            store: store.id,
            name: store.name,
            member: identity.member(),
            member_keys: selected.keys().clone(),
            storage: location.encode()?,
        })
    }

    async fn install_s3(
        &self,
        credentials: S3Credentials,
        require_connected: bool,
    ) -> Result<(), SyncError> {
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        self.install(
            ConnectionCredentials {
                location,
                credentials: StorageCredentials::S3(credentials),
            },
            require_connected,
        )
        .await
    }

    async fn install(
        &self,
        next: ConnectionCredentials,
        require_connected: bool,
    ) -> Result<(), SyncError> {
        next.validate()?;
        let previous = self.connection()?;
        let credentials = next.credentials.clone();
        let custody = self.credentials.clone();
        self.operations
            .install_credentials(
                previous,
                next,
                require_connected,
                Box::new(move || {
                    let previous = custody
                        .write(|keychain| commit_credentials(keychain, &credentials, None))?;
                    Ok(Box::new(move || {
                        custody.write(|keychain| previous.restore(keychain))
                    }))
                }),
            )
            .await
    }
}

/// Where an owner keeps this store's restore code beside its credentials.
/// Member-key replacement refreshes it once custody installs the new pair;
/// storage-credential changes leave it unchanged (§12.1, E9). Every store-log
/// owner states which it has, so none skips the refresh by omission.
pub enum RestoreCodeCustody {
    /// The device keychain holding this store's credentials. It syncs the
    /// code to the person's other Apple devices where it supports synced
    /// entries, and keeps nothing elsewhere.
    Keychain {
        /// The store's committed device credentials and their keychain.
        credentials: Arc<DeviceCredentials>,
        /// The settings naming the store's location.
        settings: StorageSettings,
    },
    /// No code is kept: an owner that never replaces member keys, such as
    /// bootstrap's loader, whose opened store keeps its own code.
    Unkept,
}

impl RestoreCodeCustody {
    /// Keep the code for `identity`'s active pair. Nothing is kept before
    /// storage has a location.
    pub(crate) fn refresh(
        &self,
        local: &coven_database::LocalStoreLog,
        identity: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let Self::Keychain {
            credentials,
            settings,
        } = self
        else {
            return Ok(());
        };
        let Some(store) = local.log.replay.state.store.clone() else {
            return Ok(());
        };
        let Some(location) = settings.read()? else {
            return Ok(());
        };
        let code = RestoreCode {
            store: store.id,
            name: store.name,
            member: identity.member(),
            member_keys: identity.keys().clone(),
            storage: location.encode()?,
        };
        credentials.write(|keychain| {
            if keychain.supports_synced_entries() {
                keychain
                    .set_synced_restore_code(&coven_crypto::SecretBytes::new(
                        code.to_bytes()?.to_vec(),
                    ))
                    .map_err(SyncError::from)?;
            }
            Ok(())
        })
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

/// Read the saved provider location and custody credentials without opening a
/// database. Shared by normal sync, read-only file access and damaged-database
/// recovery. Absence means storage is not configured on this installation.
pub fn read_connection(
    settings: &StorageSettings,
    keychain: &StoreKeychain,
) -> Result<Option<ConnectionCredentials>, SyncError> {
    let Some(location) = settings.read()? else {
        return Ok(None);
    };
    let Some(bytes) = keychain.storage_credentials()? else {
        return Ok(None);
    };
    let data = ConnectionCredentials {
        location,
        credentials: StorageCredentials::decode(bytes.as_bytes())?,
    };
    data.validate()?;
    Ok(Some(data))
}

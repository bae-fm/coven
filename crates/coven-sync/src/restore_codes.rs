//! Restore-code generation and credential replacement (§13.1, E9).

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
    store: coven_foundation::id_source::StoreId,
    database: Database,
    identity: Arc<KeySession<MemberIdentity>>,
    credentials: Arc<DeviceCredentials>,
    settings: StorageSettings,
    /// The connected sequence owner; a disconnected handle has none.
    sync: Option<Arc<crate::SequenceSync>>,
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
        store: coven_foundation::id_source::StoreId,
        database: Database,
        identity: Arc<KeySession<MemberIdentity>>,
        credentials: Arc<DeviceCredentials>,
        settings: StorageSettings,
        sync: Option<Arc<crate::SequenceSync>>,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Some(RestoreCodesInner {
                store,
                database,
                identity,
                credentials: credentials.clone(),
                settings,
                sync,
            }))),
            credentials,
        }
    }

    /// How a connection reaches storage with these committed credentials: a
    /// rejected OAuth token refreshes in this installation's device custody,
    /// which takes no lock held by credential changes (§20.2).
    pub fn provider_auth(&self, credentials: StorageCredentials) -> coven_storage::ProviderAuth {
        coven_storage::ProviderAuth::new(credentials, self.credentials.clone())
    }

    /// Encode this member's stable id and current private pair, store id and
    /// storage location only. The pair is the held one the applied
    /// history binds now; without it the call reports `MemberKeysReplaced` rather
    /// than exporting a retired pair (§13.1).
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
            // Reject missing location before publishing a new binding.
            owner.settings.read()?.ok_or(SyncError::NoStorage)?;
            let sync = owner.sync.as_ref().ok_or(SyncError::NoStorage)?;
            sync.replace_member_keys().await?;
            Ok(sync
                .finish_member_keys(true, |identity, store| owner.refresh_code(identity, store))
                .await?
                .expect("the initiating replacement owns code completion"))
        }))
        .await
    }

    /// Finish the code step of a self-replacement resumed by a sync pass (§11.5).
    pub(crate) async fn resume_member_keys(&self) -> Result<(), SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let sync = owner.sync.as_ref().ok_or(SyncError::NoStorage)?;
        sync.finish_member_keys(false, |identity, store| owner.refresh_code(identity, store))
            .await?;
        Ok(())
    }

    /// Install a manually supplied S3 key and publish its id; the code stays unchanged.
    /// Requires connected storage. Credentials remain committed if publication
    /// fails: the remote entry may already exist. Retry with the same key to
    /// finish publication; queued entry bytes and numbers are reused.
    /// A removal records this key for deletion even if its entry is refused.
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
                .sync
                .as_ref()
                .ok_or(SyncError::NoStorage)?
                .record_access_key(access_key_id)
                .await?;
            Ok(())
        }))
        .await
    }

    /// Keep this member's separately supplied S3 key without changing identity,
    /// location or code. Disconnected handles retain it for their next connection.
    /// A member whose removal applied gets `Removed` (§14.1).
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
    /// uses it. Custody commits it before the connection does (§14.3).
    pub async fn adopt_delivered_key(
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
            .write(|keychains| keychains.delete_storage_credentials())
    }

    /// Reserve the credential owner until remote setup either commits or fails.
    /// The returned commit includes the synced restore code and location, and its
    /// compensation remains available until store keys and the origin are applied.
    /// An OAuth setup commits `sign_in`'s current tokens, which then refresh in
    /// device custody; a rollback holds them in the session again. Reconnecting
    /// with device custody keeps its current tokens, including any refresh.
    pub async fn prepare_setup(
        &self,
        data: ConnectionCredentials,
        sign_in: Option<Arc<SessionSignIn>>,
    ) -> Result<(crate::StorageCommit, SecretText), SyncError> {
        data.validate()?;
        let guard = self.inner.clone().lock_owned().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let identity = owner
            .identity
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        // The code names the stable id its unchanged private-pair blob belongs to (D13).
        let code = RestoreCode {
            store: owner.store,
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
                        let credentials = match &data.credentials {
                            StorageCredentials::OAuth(_) => {
                                StorageCredentials::OAuth(crate::sign_in::held_tokens(keychain)?)
                            }
                            credentials => credentials.clone(),
                        };
                        commit_credentials(keychain, &credentials, Some(&code))
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
    fn refresh_code(
        &self,
        identity: &MemberIdentity,
        store: &coven_database::StoreIdentity,
    ) -> Result<SecretText, SyncError> {
        let code = RestoreCode {
            store: store.id,
            member: identity.member(),
            member_keys: identity.keys().clone(),
            storage: self
                .settings
                .read()?
                .ok_or(SyncError::NoStorage)?
                .encode()?,
        };
        let text = SecretText::new(code.to_text()?.to_string());
        self.credentials
            .write(|keychains| keychains.refresh_restore_code(&code))?;
        Ok(text)
    }

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
        let state = self.database.store_state().await?;
        let store = state.store.clone().ok_or(SyncError::NoStorage)?;
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        let identity = self
            .identity
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let current = crate::effects::member(&state, &identity.member())
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
        let commit: crate::StorageCommit = Box::new(move || {
            let previous =
                custody.write(|keychain| commit_credentials(keychain, &credentials, None))?;
            Ok(Box::new(move || {
                custody.write(|keychain| previous.restore(keychain))
            }))
        });
        match &self.sync {
            Some(sync) => sync.install_credentials(previous, next, commit).await,
            None if require_connected => Err(SyncError::NoStorage),
            // Without a connection, no pass can apply a removal meanwhile.
            None => {
                refuse_after_leaving(&self.database, &self.identity).await?;
                commit().map(drop)
            }
        }
    }
}

/// Refuse a credential change once this member's removal has applied. The
/// person has no storage access left to change, and saving an S3 key would
/// write it back into the synced keychain their removal emptied (§14.1).
pub(crate) async fn refuse_after_leaving(
    database: &Database,
    identity: &KeySession<MemberIdentity>,
) -> Result<(), SyncError> {
    let member = identity
        .read()?
        .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?
        .member();
    if crate::effects::left(&database.store_state().await?, &member) {
        return Err(SyncError::Removed);
    }
    Ok(())
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

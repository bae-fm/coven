//! Restore-code generation and credential replacement (§12.1, E9).

use crate::{commit_credentials, credential_custody::cleanup_error, CredentialRollback, SyncError};
use coven_crypto::{
    custody::{KeySession, StoreKeychain},
    MemberKeys, SecretText,
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
}
struct RestoreCodesInner {
    database: Database,
    identity: Arc<KeySession<MemberKeys>>,
    keychain: Arc<StoreKeychain>,
    settings: StorageSettings,
    operations: crate::Operations,
    oauth: Option<coven_storage::providers::OAuthFlow>,
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
        identity: Arc<KeySession<MemberKeys>>,
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

    /// Encode this member's identity, store id and name, and storage location only.
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        Ok(owner.code().await?.to_text()?.to_string())
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
                .set_access(coven_format::MemberAccess::S3AccessKey { access_key_id })
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
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?,
            storage: data.location.encode()?,
        };
        let previous_location = owner.settings.read()?;
        Ok(Box::new(move || {
            let owner = guard.as_ref().expect("reserved credential owner");
            let previous = commit_credentials(&owner.keychain, &data.credentials, Some(&code))?;
            if let Err(error) = owner.settings.commit(&data.location) {
                return Err(
                    match owner.restore_connection(previous_location, previous) {
                        Ok(()) => error.into(),
                        Err(cleanup) => cleanup_error(error.into(), cleanup),
                    },
                );
            }
            Ok(Box::new(move || {
                guard
                    .as_ref()
                    .expect("reserved credential owner")
                    .restore_connection(previous_location, previous)
            }))
        }))
    }

    /// Refresh expired credentials before a connection or sync pass. Commit
    /// refreshed tokens before installing the live session; the code stays unchanged.
    pub async fn refresh_if_expired(&self, now: std::time::SystemTime) -> Result<(), SyncError> {
        let guard = self.inner.lock().await;
        let owner = guard.as_ref().ok_or(DbError::StoreClosed)?;
        let Some(mut data) = owner.connection()? else {
            return Ok(());
        };
        if let Some(credentials) = refreshed_credentials(&data, owner.oauth.as_ref(), now).await? {
            data.credentials = credentials;
            owner.install(data, false).await?;
        }
        Ok(())
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
        let credentials = credentials.restore(&self.keychain);
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
        read_connection(&self.settings, &self.keychain)
    }

    async fn code(&self) -> Result<RestoreCode, SyncError> {
        let local = self.database.local_store_log().await?;
        let store = local.log.replay.state.store.ok_or(SyncError::NoStorage)?;
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        let member_keys = self
            .identity
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        Ok(RestoreCode {
            store: store.id,
            name: store.name,
            member_keys,
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
        let keychain = self.keychain.clone();
        self.operations
            .install_credentials(
                previous,
                next,
                require_connected,
                Box::new(move || {
                    let previous = commit_credentials(&keychain, &credentials, None)?;
                    Ok(Box::new(move || previous.restore(&keychain)))
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

/// Refresh expired OAuth credentials without changing custody or a live client.
/// The caller commits the replacement before installing it; recovery can do so
/// before opening SQLite, while normal sync serializes it with live operations.
pub async fn refreshed_credentials(
    data: &ConnectionCredentials,
    oauth: Option<&coven_storage::providers::OAuthFlow>,
    now: std::time::SystemTime,
) -> Result<Option<StorageCredentials>, SyncError> {
    let StorageCredentials::OAuth(tokens) = &data.credentials else {
        return Ok(None);
    };
    if tokens.expires_at.is_none_or(|expiry| now < expiry) {
        return Ok(None);
    }
    let clients = oauth.ok_or(
        coven_storage::StorageFailure::InvalidConfiguration.with_source("OAuth clients are absent"),
    )?;
    let tokens = clients
        .refresh(data.location.provider(), tokens)
        .await
        .map_err(|error| match error {
            coven_storage::providers::OAuthError::Storage(error) => error,
            error => coven_storage::StorageError::Provider {
                provider: data.location.provider(),
                failure: coven_storage::StorageFailure::Authentication,
                source: Box::new(error),
            },
        })?;
    Ok(Some(StorageCredentials::OAuth(tokens)))
}

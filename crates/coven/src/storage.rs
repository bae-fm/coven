//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_crypto::custody::KeySession;
use coven_storage::{
    providers::{OAuthFlow, StorageConnector},
    ConnectionCredentials, S3Credentials, Storage, StorageCredentials,
};
use std::{future::Future, sync::Arc};

/// Whether this device's custody holds opened store keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreKeyState {
    /// Store keys are held by custody.
    Available,
    /// Store keys must be opened from this member's sealed copy.
    Locked,
}

/// The configured location and the result of opening this device's keys.
#[derive(Clone, Debug)]
pub struct ConnectedStorage {
    /// The connected provider location.
    pub storage: StorageConfig,
    /// This device's key availability.
    pub key_state: StoreKeyState,
}

pub(crate) struct StorageConnections {
    codes: coven_sync::RestoreCodes,
    initial_name: String,
    keys: std::sync::Mutex<Option<Arc<KeySession<StoreKeyring>>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    authentication: tokio::sync::Mutex<Option<crate::authentication::Authentication>>,
    sync: coven_sync::SyncLoop,
    store: StoreId,
    device: DeviceId,
    ids: IdSourceRef,
    clock: ClockRef,
    calls: tokio::sync::Mutex<()>,
}

impl StorageConnections {
    pub(crate) async fn prepare_invite(&self) -> Result<(), SyncError> {
        // Keep sign-in cancellable. Only credential installation and the existing
        // invitation operation retain their work after their awaiting future drops.
        let _call = self.lock_open().await?;
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        match location {
            StorageConfig::Dropbox {
                location: DropboxLocation::AppFolder,
            } => {
                return Err(StorageFailure::DropboxAppFolderNotShareable.into());
            }
            StorageConfig::GoogleDrive { .. } => {
                let required = OAuthAccess::GoogleDrive(GoogleDriveAccess::Full);
                let flow = self.oauth.as_ref().ok_or_else(|| {
                    SyncError::from(StorageFailure::AccessNotRegistered { required })
                })?;
                flow.require_access(required)
                    .map_err(|error| oauth_error(required.provider(), error))?;
                let data = self.connection().await?.ok_or(SyncError::NoStorage)?;
                let StorageCredentials::OAuth(previous) = &data.credentials else {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("Drive credentials must be OAuth")
                        .into());
                };
                if !previous.access.permits(required) {
                    flow.require_client(previous)
                        .map_err(|error| oauth_error(required.provider(), error))?;
                    let before = self
                        .connector
                        .connect(data.location.clone(), data.credentials.clone(), self.device)
                        .await?;
                    let account = before.account().await?;
                    let mut tokens = flow
                        .authenticate(required)
                        .await
                        .map_err(|error| oauth_error(required.provider(), error))?;
                    if tokens.refresh_token.is_none() {
                        tokens.refresh_token = previous.refresh_token.clone();
                    }
                    let candidate = self
                        .connector
                        .connect(
                            data.location,
                            StorageCredentials::OAuth(tokens.clone()),
                            self.device,
                        )
                        .await?;
                    if candidate.account().await? != account {
                        return Err(StorageFailure::AccountMismatch.into());
                    }
                    self.codes.install_oauth(tokens).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) fn new(
        codes: coven_sync::RestoreCodes,
        initial_name: String,
        keys: Arc<KeySession<StoreKeyring>>,
        connector: Arc<dyn StorageConnector>,
        oauth: Option<OAuthFlow>,
        authentication: Option<crate::authentication::Authentication>,
        sync: coven_sync::SyncLoop,
        store: StoreId,
        device: DeviceId,
        ids: IdSourceRef,
        clock: ClockRef,
    ) -> Self {
        Self {
            codes,
            initial_name,
            keys: std::sync::Mutex::new(Some(keys)),
            connector,
            oauth,
            authentication: tokio::sync::Mutex::new(authentication),
            sync,
            store,
            device,
            ids,
            clock,
            calls: tokio::sync::Mutex::new(()),
        }
    }

    async fn lock_open(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, KeyError> {
        let call = self.calls.lock().await;
        self.keys
            .lock()
            .expect("store keys lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?;
        Ok(call)
    }

    // The task owns both the call and its lock until it finishes, even if the
    // awaiting app future is dropped. Sign-in deliberately does not spawn.
    async fn call<T, E, F>(
        self: &Arc<Self>,
        run: impl FnOnce(Arc<Self>) -> F + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<KeyError> + Send + 'static,
        F: Future<Output = Result<T, E>> + Send,
    {
        let owner = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.lock_open().await?;
            run(owner.clone()).await
        }))
        .await
    }

    pub(crate) fn key_state(&self) -> Result<StoreKeyState, KeyError> {
        Ok(
            if self
                .keys
                .lock()
                .expect("store keys lock poisoned")
                .as_ref()
                .ok_or(KeyError::StoreClosed)?
                .read()?
                .is_some()
            {
                StoreKeyState::Available
            } else {
                StoreKeyState::Locked
            },
        )
    }

    pub(crate) async fn setup_s3(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        if storage.provider() != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("expected S3 storage")
                .into());
        }
        self.setup(
            storage,
            device_name,
            StorageCredentials::S3(S3Credentials {
                access_key_id,
                secret_access_key,
            }),
        )
        .await
    }

    pub(crate) async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError> {
        let _call = self.lock_open().await?;
        let flow = self
            .oauth
            .as_ref()
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        *self.authentication.lock().await = Some(crate::authentication::Authentication { tokens });
        Ok(())
    }

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let provider = storage.provider();
            if !matches!(
                provider,
                CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
            ) {
                return Err(OAuthError::Unavailable(provider).into());
            }
            owner.check_location(&storage).await?;
            let mut held = owner.authentication.lock().await;
            let credentials = match held.as_mut() {
                Some(authentication) => {
                    authentication
                        .credentials(
                            provider,
                            owner
                                .oauth
                                .as_ref()
                                .ok_or(OAuthError::Unavailable(provider))?,
                            owner.clock.now(),
                        )
                        .await?
                }
                None => {
                    let data = owner
                        .connection()
                        .await
                        .map_err(setup_error)?
                        .ok_or(OAuthError::Reauthorize(provider))?;
                    if data.location.provider() != provider {
                        return Err(OAuthError::Reauthorize(provider).into());
                    }
                    data.credentials
                }
            };
            let result = owner
                .setup_connection(storage, &device_name, credentials)
                .await?;
            held.take();
            Ok(result)
        })
        .await
    }

    pub(crate) async fn setup_cloudkit(
        self: &Arc<Self>,
        container: &str,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let container = container.to_owned();
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let storage = match owner.codes.location().await.map_err(setup_error)? {
                Some(storage) => {
                    if !matches!(&storage, StorageConfig::CloudKit { container: recorded, .. } if *recorded == container) {
                        return Err(StorageSetupError::LocationChangeUnsupported);
                    }
                    storage
                }
                None => owner.connector.prepare_cloudkit(&container, owner.store).await?,
            };
            owner.setup_connection(storage, &device_name, StorageCredentials::CloudKit).await
        }).await
    }

    async fn setup(
        self: &Arc<Self>,
        config: StorageConfig,
        device_name: &str,
        credentials: StorageCredentials,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            owner.check_location(&config).await?;
            owner
                .setup_connection(config, &device_name, credentials)
                .await
        })
        .await
    }

    async fn check_location(&self, config: &StorageConfig) -> Result<(), StorageSetupError> {
        if self
            .codes
            .location()
            .await
            .map_err(setup_error)?
            .is_some_and(|recorded| &recorded != config)
        {
            return Err(StorageSetupError::LocationChangeUnsupported);
        }
        Ok(())
    }

    async fn setup_connection(
        &self,
        config: StorageConfig,
        device_name: &str,
        credentials: StorageCredentials,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let storage = self
            .connector
            .connect(config.clone(), credentials.clone(), self.device)
            .await?;
        let path = ObjectPath::file(self.store, self.device, FileId(self.ids.new_id()));
        let plaintext = b"coven storage check";
        let header = coven_format::file::FileHeader::new(plaintext.len() as u64);
        let key = coven_crypto::FileKey::generate()
            .map_err(|error| StorageSetupError::Internal(Box::new(error)))?;
        let mut bytes = header.encode().to_vec();
        bytes.extend(
            header
                .seal_chunk(&key, path.as_str(), 0, plaintext)
                .map_err(|error| StorageSetupError::Internal(Box::new(error)))?,
        );
        coven_storage::check_provider(storage.as_ref(), &path, &bytes).await?;
        let access = match &credentials {
            StorageCredentials::S3(keys) => coven_format::MemberAccess::S3AccessKey {
                access_key_id: keys.access_key_id.clone(),
            },
            _ => coven_format::MemberAccess::ProviderAccount(storage.account().await?),
        };
        self.sync
            .setup(
                storage,
                access,
                device_name.into(),
                ConnectionCredentials {
                    location: config.clone(),
                    credentials,
                },
                self.initial_name.clone(),
            )
            .await
            .map_err(setup_error)?;
        Ok(ConnectedStorage {
            storage: config,
            key_state: StoreKeyState::Available,
        })
    }

    async fn connection(&self) -> Result<Option<ConnectionCredentials>, SyncError> {
        self.codes.refresh_if_expired(self.clock.now()).await?;
        self.codes.connection().await
    }

    pub(crate) async fn unlock(self: &Arc<Self>) -> Result<ConnectedStorage, StoreKeyUnlockError> {
        self.call(|owner| async move {
            let data = owner
                .connection()
                .await
                .map_err(unlock_error)?
                .ok_or(StoreKeyUnlockError::NoStorage)?;
            let config = data.location;
            let storage = owner
                .connector
                .connect(config.clone(), data.credentials, owner.device)
                .await?;
            owner.sync.unlock(storage).await.map_err(unlock_error)?;
            Ok(ConnectedStorage {
                storage: config,
                key_state: StoreKeyState::Available,
            })
        })
        .await
    }

    pub(crate) async fn disconnect(self: &Arc<Self>) -> Result<(), SyncError> {
        self.call(|owner| async move {
            owner.sync.forget_storage().await?;
            owner.authentication.lock().await.take();
            Ok(())
        })
        .await
    }

    pub(crate) async fn forget_store_keys(self: &Arc<Self>) -> Result<(), KeyError> {
        self.call(|owner| async move {
            owner
                .sync
                .forget_store_keys()
                .await
                .map_err(|error| match error {
                    SyncError::SecureStorage(error) => error,
                    SyncError::Database(DbError::StoreClosed) => KeyError::StoreClosed,
                    error => KeyError::Unavailable(Box::new(error)),
                })
        })
        .await
    }

    pub(crate) async fn close(&self) {
        let _call = self.calls.lock().await;
        let keys = self.keys.lock().expect("store keys lock poisoned").take();
        self.authentication.lock().await.take();
        drop(keys);
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

fn oauth_error(provider: CloudProvider, error: OAuthError) -> SyncError {
    match error {
        OAuthError::Storage(error) => error.into(),
        error => coven_storage::StorageError::Provider {
            provider,
            failure: StorageFailure::Authentication,
            source: Box::new(error),
        }
        .into(),
    }
}

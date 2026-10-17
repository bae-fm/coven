//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_crypto::custody::KeySession;
use coven_storage::{
    providers::{OAuthClients, OAuthFlow, StorageConnector},
    ConnectionCredentials, ProviderAuth, S3Credentials, Storage, StorageCredentials,
};
use coven_sync::SessionSignIn;
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

/// Storage setup's committed connection and this member's restore code.
#[derive(Clone, Debug)]
pub struct StorageSetup {
    /// The location and availability of its store keys.
    pub connection: ConnectedStorage,
    /// The code for restoring this member on another installation.
    pub restore_code: SecretText,
}

pub(crate) struct StorageConnections {
    codes: coven_sync::RestoreCodes,
    initial_name: String,
    keys: std::sync::Mutex<Option<Arc<KeySession<StoreKeyring>>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    /// The app's clients, which refresh a session sign-in.
    clients: Option<OAuthClients>,
    /// The latest sign-in, held for this session until setup publishes it.
    authentication: tokio::sync::Mutex<Option<Arc<SessionSignIn>>>,
    sync: coven_sync::SyncLoop,
    store: StoreId,
    device: DeviceId,
    ids: IdSourceRef,
    clock: ClockRef,
    send: Arc<coven_sync::SendGate>,
    waits: Arc<coven_sync::RetryWaits>,
    calls: tokio::sync::Mutex<()>,
}

impl StorageConnections {
    /// Refuse an invite the location cannot share before any provider change.
    pub(crate) async fn prepare_invite(&self) -> Result<(), SyncError> {
        let _call = self.lock_open().await?;
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        if matches!(
            location,
            StorageConfig::Dropbox {
                location: DropboxLocation::AppFolder,
            }
        ) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        Ok(())
    }
    pub(crate) fn new(
        codes: coven_sync::RestoreCodes,
        initial_name: String,
        keys: Arc<KeySession<StoreKeyring>>,
        connector: Arc<dyn StorageConnector>,
        oauth: Option<OAuthFlow>,
        clients: Option<OAuthClients>,
        authentication: Option<Arc<SessionSignIn>>,
        sync: coven_sync::SyncLoop,
        store: StoreId,
        device: DeviceId,
        ids: IdSourceRef,
        clock: ClockRef,
        send: Arc<coven_sync::SendGate>,
        waits: Arc<coven_sync::RetryWaits>,
    ) -> Self {
        Self {
            codes,
            initial_name,
            keys: std::sync::Mutex::new(Some(keys)),
            connector,
            oauth,
            clients,
            authentication: tokio::sync::Mutex::new(authentication),
            sync,
            store,
            device,
            ids,
            clock,
            send,
            waits,
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

    pub(crate) async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError> {
        let _call = self.lock_open().await?;
        let flow = self
            .oauth
            .as_ref()
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        *self.authentication.lock().await = Some(SessionSignIn::new(tokens, self.clients.clone()));
        Ok(())
    }

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError> {
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let provider = storage.provider();
            if !matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) {
                return Err(OAuthError::Unavailable(provider).into());
            }
            owner.check_location(&storage).await?;
            let mut held = owner.authentication.lock().await;
            let (auth, sign_in) = match held.as_ref() {
                Some(sign_in) => {
                    crate::authentication::check_provider(sign_in, provider)?;
                    let auth = ProviderAuth::OAuth {
                        tokens: sign_in.tokens().map_err(setup_error)?,
                        custody: sign_in.clone(),
                    };
                    (auth, Some(sign_in.clone()))
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
                    (owner.codes.provider_auth(data.credentials), None)
                }
            };
            let result = owner
                .setup_connection(storage, &device_name, auth, sign_in)
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
    ) -> Result<StorageSetup, StorageSetupError> {
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
            owner.setup_connection(storage, &device_name, ProviderAuth::CloudKit, None).await
        }).await
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

    /// Check and publish `config` reached through `auth`. A new OAuth
    /// sign-in is `sign_in`, the session custody `auth` refreshes in until
    /// setup commits it to device custody.
    async fn setup_connection(
        &self,
        config: StorageConfig,
        device_name: &str,
        auth: ProviderAuth,
        sign_in: Option<Arc<SessionSignIn>>,
    ) -> Result<StorageSetup, StorageSetupError> {
        let credentials = auth.credentials();
        let storage = self
            .connector
            .connect(config.clone(), auth, self.send.clone(), self.waits.clone())
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
        {
            let _probe = self.send.provider_check(path.clone());
            coven_storage::check_provider(storage.as_ref(), &path, &bytes).await?;
        }
        let access = match &credentials {
            StorageCredentials::S3(keys) => coven_format::MemberAccess::S3AccessKey {
                access_key_id: keys.access_key_id.clone(),
                principal: coven_format::S3Principal::Console,
            },
            _ => coven_format::MemberAccess::ProviderAccount(storage.account().await?),
        };
        let restore_code = self
            .sync
            .setup(
                storage,
                access,
                device_name.into(),
                ConnectionCredentials {
                    location: config.clone(),
                    credentials,
                },
                sign_in,
                self.initial_name.clone(),
            )
            .await
            .map_err(setup_error)?;
        Ok(StorageSetup {
            connection: ConnectedStorage {
                storage: config,
                key_state: StoreKeyState::Available,
            },
            restore_code,
        })
    }

    async fn connection(&self) -> Result<Option<ConnectionCredentials>, SyncError> {
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
                .connect(
                    config.clone(),
                    owner.codes.provider_auth(data.credentials),
                    owner.send.clone(),
                    owner.waits.clone(),
                )
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

#[path = "storage_s3.rs"]
mod s3;

#[path = "storage_admin.rs"]
mod admin;
pub use admin::GuidedAccessKey;
#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

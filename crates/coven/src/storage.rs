//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_storage::{
    providers::{OAuthFlow, StorageConnector},
    ConnectionCredentials, S3Credentials, StorageCredentials,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

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
    keys: std::sync::Mutex<Option<Arc<dyn StoreKeyCustody>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    authentication: tokio::sync::Mutex<Option<crate::authentication::Authentication>>,
    sync: coven_sync::SyncLoop,
    device: DeviceId,
    ids: IdSourceRef,
    clock: ClockRef,
    calls: tokio::sync::Mutex<()>,
    closed: AtomicBool,
}

impl StorageConnections {
    pub(crate) fn new(
        codes: coven_sync::RestoreCodes,
        initial_name: String,
        keys: Arc<dyn StoreKeyCustody>,
        connector: Arc<dyn StorageConnector>,
        oauth: Option<OAuthFlow>,
        authentication: Option<crate::authentication::Authentication>,
        sync: coven_sync::SyncLoop,
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
            device,
            ids,
            clock,
            calls: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
        }
    }

    fn check_open(&self) -> Result<(), KeyError> {
        if self.closed.load(Ordering::Acquire) {
            Err(KeyError::StoreClosed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn key_state(&self) -> Result<StoreKeyState, KeyError> {
        self.check_open()?;
        Ok(
            if self
                .keys
                .lock()
                .expect("store keys lock poisoned")
                .as_ref()
                .ok_or(KeyError::StoreClosed)?
                .unlock()?
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

    pub(crate) async fn authenticate(
        &self,
        provider: CloudProvider,
    ) -> Result<(), StorageSetupError> {
        let _call = self.calls.lock().await;
        self.check_open()?;
        let flow = self
            .oauth
            .as_ref()
            .ok_or(OAuthError::Unavailable(provider))?;
        let tokens = flow.authenticate(provider).await?;
        *self.authentication.lock().await =
            Some(crate::authentication::Authentication { provider, tokens });
        Ok(())
    }

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let owner = self.clone();
        let device_name = device_name.to_owned();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
            let provider = storage.provider();
            if !matches!(
                provider,
                CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
            ) {
                return Err(OAuthError::Unavailable(provider).into());
            }
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
        }))
        .await
    }

    pub(crate) async fn setup_cloudkit(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        if storage.provider() != CloudProvider::CloudKit {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("expected CloudKit storage")
                .into());
        }
        self.setup(storage, device_name, StorageCredentials::CloudKit)
            .await
    }

    async fn setup(
        self: &Arc<Self>,
        config: StorageConfig,
        device_name: &str,
        credentials: StorageCredentials,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        let owner = self.clone();
        let device_name = device_name.to_owned();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
            owner
                .setup_connection(config, &device_name, credentials)
                .await
        }))
        .await
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
        let path = ObjectPath::file(self.device, FileId(self.ids.new_id()));
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
        let owner = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
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
        }))
        .await
    }

    pub(crate) async fn disconnect(self: &Arc<Self>) -> Result<(), SyncError> {
        let owner = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
            owner.sync.forget_storage().await?;
            owner.authentication.lock().await.take();
            Ok(())
        }))
        .await
    }

    pub(crate) async fn forget_store_keys(self: &Arc<Self>) -> Result<(), KeyError> {
        let owner = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
            owner
                .sync
                .forget_store_keys()
                .await
                .map_err(|error| match error {
                    SyncError::SecureStorage(error) => error,
                    SyncError::Database(DbError::StoreClosed) => KeyError::StoreClosed,
                    error => KeyError::Unavailable(Box::new(error)),
                })
        }))
        .await
    }

    pub(crate) async fn close(&self) {
        let _call = self.calls.lock().await;
        self.closed.store(true, Ordering::Release);
        self.authentication.lock().await.take();
        self.keys.lock().expect("store keys lock poisoned").take();
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

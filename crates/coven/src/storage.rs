//! Application storage setup, credential custody and connection control.

use crate::*;
use coven_storage::{
    providers::StorageConnector, RestoreStorage, S3Credentials, StorageCredentials,
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

/// Opening this member's sealed store keys failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreKeyUnlockError {
    /// No stored provider configuration or credentials are available.
    #[error("no storage configured")]
    NoStorage,
    /// This installation has no member identity.
    #[error("member keys are missing")]
    MemberKeysMissing,
    /// The provider refused or failed a request.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// An authenticated key could not be opened.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// Custody refused to read or persist keys.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Store-log validation or local work failed, preserving its cause.
    #[error("opening store keys: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) struct StorageConnections {
    codes: coven_sync::RestoreCodes,
    initial_name: String,
    keys: std::sync::Mutex<Option<Arc<dyn StoreKeyCustody>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthClients>,
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
        oauth: Option<OAuthClients>,
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
            return Err(StorageError::InvalidConfiguration("expected S3 storage").into());
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

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        self.check_open()?;
        let clients =
            self.oauth
                .as_ref()
                .ok_or(coven_storage::providers::OAuthError::Unavailable(
                    storage.provider(),
                ))?;
        let tokens = clients.authorize(storage.provider(), cancel).await?;
        self.setup(storage, device_name, StorageCredentials::OAuth(tokens))
            .await
    }

    pub(crate) async fn setup_cloudkit(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError> {
        if storage.provider() != CloudProvider::CloudKit {
            return Err(StorageError::InvalidConfiguration("expected CloudKit storage").into());
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
            let storage = owner
                .connector
                .connect(config.clone(), credentials.clone(), owner.device)
                .await?;
            let access = match &credentials {
                StorageCredentials::S3(keys) => coven_format::MemberAccess::S3AccessKey {
                    access_key_id: keys.access_key_id.clone(),
                },
                _ => coven_format::MemberAccess::ProviderAccount(storage.account().await?),
            };
            owner
                .sync
                .setup(
                    storage,
                    access,
                    device_name,
                    RestoreStorage {
                        location: config.clone(),
                        credentials,
                    },
                    owner.initial_name.clone(),
                )
                .await
                .map_err(setup_error)?;
            Ok(ConnectedStorage {
                storage: config,
                key_state: StoreKeyState::Available,
            })
        }))
        .await
    }

    async fn connection(&self) -> Result<Option<RestoreStorage>, SyncError> {
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
            owner.sync.forget_storage().await
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

    pub(crate) async fn probe(self: &Arc<Self>, config: &StorageConfig) -> Result<(), SyncError> {
        let owner = self.clone();
        let config = config.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.calls.lock().await;
            owner.check_open()?;
            let data = owner.connection().await?.ok_or(SyncError::NoStorage)?;
            let storage = owner
                .connector
                .connect(config.clone(), data.credentials, owner.device)
                .await?;
            let path = ObjectPath::file(owner.device, FileId(owner.ids.new_id()));
            let plaintext = b"coven storage probe";
            let header = coven_format::file::FileHeader::new(plaintext.len() as u64);
            let key = coven_crypto::FileKey::generate()?;
            let mut bytes = header.encode().to_vec();
            bytes.extend(
                header
                    .seal_chunk(&key, path.as_str(), 0, plaintext)
                    .map_err(|error| SyncFailure::Other(Arc::new(error)))?,
            );
            Ok(storage.probe(&path, &bytes).await?)
        }))
        .await
    }

    pub(crate) async fn close(&self) {
        let _call = self.calls.lock().await;
        self.closed.store(true, Ordering::Release);
        self.keys.lock().expect("store keys lock poisoned").take();
    }
}

fn setup_error(error: SyncError) -> StorageSetupError {
    match error {
        SyncError::Setup(error) => *error,
        SyncError::MissingMemberKeys => StorageSetupError::MemberKeysMissing,
        SyncError::SecureStorage(error) => StorageSetupError::SecureStorage(error),
        SyncError::Storage(error) => StorageSetupError::Storage(error),
        error => StorageSetupError::Internal(Box::new(error)),
    }
}

fn unlock_error(error: SyncError) -> StoreKeyUnlockError {
    match error {
        SyncError::NoStorage => StoreKeyUnlockError::NoStorage,
        SyncError::MissingMemberKeys => StoreKeyUnlockError::MemberKeysMissing,
        SyncError::SecureStorage(error) => StoreKeyUnlockError::SecureStorage(error),
        SyncError::Crypto(error) => StoreKeyUnlockError::Crypto(error),
        SyncError::Storage(error) => StoreKeyUnlockError::Storage(error),
        error => StoreKeyUnlockError::Other(Box::new(error)),
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

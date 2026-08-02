use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::blob::transition::{MakeLocalError, MakeRemoteError};
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::coven::StoreOpenGuard;
use crate::database::{DbError, StoreDatabase};
use crate::encryption::EncryptionService;
use crate::keys::KeyError;
use crate::storage::cloud::setup::{SetupError, StorageSetupError};
use crate::storage::cloud::CloudHome;
use crate::storage::cloud::CloudHomeError;
use crate::storage::{BlobChunking, BlobPathScheme, CloudSyncStorage, StorageError, SyncStorage};
use crate::store_security::StoreSecurity;
use crate::sync::cycle::{InitSyncError, PreparedSyncComponents, SyncComponents};
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobAccess};
use crate::sync::sync_loop::{SyncLoopError, SyncLoopHandle, SyncLoopStatus};
use crate::sync::BlobCacheError;
use crate::sync::Store;

pub(crate) type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("sync is not configured")]
    NotConfigured,
    #[error("sync loop is not running")]
    LoopNotRunning,
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
    #[error("no master key is established for this opaque store (locked, or never initialized)")]
    MasterKeyNotEstablished,
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("failed to create sync storage: {0}")]
    StorageSetup(StorageSetupError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("sync initialization error: {0}")]
    Init(#[from] InitSyncError),
    #[error("Store protocol state: {0}")]
    Protocol(String),
    #[error("Store operation: {0}")]
    Store(#[from] crate::sync::store::StoreError),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("membership error: {0}")]
    Membership(Box<crate::sync::store::MembershipOpsError>),
    #[error("circle operation: {0}")]
    Circle(#[from] crate::sync::store::CircleOperationError),
    #[error("device join: {0}")]
    DeviceJoin(#[from] crate::DeviceJoinError),
    #[error("device join transport: {0}")]
    DeviceJoinTransport(#[from] crate::sync::store::DeviceJoinTransportError),
    #[error("invalid join request code: {0}")]
    InvalidJoinRequest(String),
    #[error("invalid Store membership operation code: {0}")]
    InvalidMembershipOperationCode(String),
    #[error("Store device exclusion: {0}")]
    DeviceExclusion(String),
    #[error("Store Owner promotion: {0}")]
    OwnerPromotion(String),
    #[error("{0}")]
    Database(#[from] DbError),
    #[error("blob upload drain failed: {0}")]
    BlobUpload(DbError),
    #[error("sync loop error: {0}")]
    Loop(SyncLoopError),
}

impl From<crate::sync::store::MembershipOpsError> for SyncError {
    fn from(error: crate::sync::store::MembershipOpsError) -> Self {
        Self::Membership(Box::new(error))
    }
}

/// Who runs sync cycles for a connected cloud.
///
/// Production always connects under `Loop`: a background thread runs cycles on a
/// timer and on trigger, and an operation that queues work for a cycle to carry
/// out refuses once that thread is gone. `Caller` is the test-only alternative —
/// no thread exists and the host drives sync itself, so its own `drain_uploads`
/// is the only drain that runs and cannot lose a race with a cycle's.
enum SyncDriver {
    Loop,
    #[cfg(any(test, feature = "test-utils"))]
    Caller,
}

/// The store's sync connection.
///
/// Three questions that used to travel as one: whether a connection is installed
/// at all, whether it has a cloud to talk to, and — within `WithCloud` — who runs
/// its cycles. Each capability asks for exactly the state it needs. Reading the
/// connected cloud's path scheme needs only a cloud. Queueing a blob transition
/// needs a driver that will carry it out. A Circle write command needs the loop
/// *thread* itself, because that thread services the command channel.
enum SyncConnection {
    /// No connection installed: none was ever made, or the last was discarded.
    Disconnected,
    /// Connected with no cloud attached — either no provider is configured, or
    /// `stop_sync` released the one that was. `start_sync` rebuilds from config.
    WithoutCloud,
    /// Connected over a cloud provider.
    WithCloud {
        sync: Arc<SyncLoopHandle>,
        storage: Arc<dyn SyncStorage>,
        driver: SyncDriver,
    },
}

impl SyncConnection {
    /// The connected cloud's sync, whether or not anything is currently driving
    /// it. For reads and lookups that only need the connection's own facts.
    fn cloud_sync(&self) -> Option<Arc<SyncLoopHandle>> {
        match self {
            Self::WithCloud { sync, .. } => Some(Arc::clone(sync)),
            _ => None,
        }
    }

    /// The connected cloud's sync, when something is driving it. A connection
    /// whose loop thread has gone has nothing to carry out the work an operation
    /// would queue, so it is not driven and every such operation refuses.
    fn driven_sync(&self) -> Option<Arc<SyncLoopHandle>> {
        match self {
            Self::WithCloud { sync, driver, .. } => match driver {
                SyncDriver::Loop => sync.is_running().then(|| Arc::clone(sync)),
                #[cfg(any(test, feature = "test-utils"))]
                SyncDriver::Caller => Some(Arc::clone(sync)),
            },
            _ => None,
        }
    }

    fn storage(&self) -> Option<Arc<dyn SyncStorage>> {
        match self {
            Self::WithCloud { storage, .. } => Some(Arc::clone(storage)),
            _ => None,
        }
    }

    fn is_connected(&self) -> bool {
        !matches!(self, Self::Disconnected)
    }

    fn stop(self) -> Result<(), SyncError> {
        if let Self::WithCloud { sync, .. } = self {
            sync.stop().map_err(SyncError::Loop)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct StoreSync {
    config_provider: ConfigProvider,
    security: StoreSecurity,
    database: StoreDatabase,
    store_dir: crate::store_dir::StoreDir,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    open_guard: Arc<StoreOpenGuard>,
    blob_chunking: BlobChunking,
    local_blob_access: LocalStoreBlobAccess,
    local_blob_transitions: crate::blob::transition::LocalBlobTransitions,
    connection: Arc<RwLock<SyncConnection>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
}

impl StoreSync {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config_provider: ConfigProvider,
        security: StoreSecurity,
        database: StoreDatabase,
        store_dir: crate::store_dir::StoreDir,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        blob_chunking: BlobChunking,
        local_blob_access: LocalStoreBlobAccess,
        local_blob_transitions: crate::blob::transition::LocalBlobTransitions,
    ) -> Self {
        Self {
            config_provider,
            security,
            database,
            store_dir,
            clock,
            cloudkit_ops,
            observer,
            open_guard,
            blob_chunking,
            local_blob_access,
            local_blob_transitions,
            connection: Arc::new(RwLock::new(SyncConnection::Disconnected)),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            status_tx: tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
        }
    }

    fn config(&self) -> Config {
        (self.config_provider)()
    }

    fn cloud_sync(&self) -> Option<Arc<SyncLoopHandle>> {
        self.connection.read().unwrap().cloud_sync()
    }

    fn driven_sync(&self) -> Option<Arc<SyncLoopHandle>> {
        self.connection.read().unwrap().driven_sync()
    }

    #[cfg(test)]
    pub(crate) fn cloud_sync_for_test(&self) -> Option<Arc<SyncLoopHandle>> {
        self.cloud_sync()
    }

    fn storage(&self) -> Option<Arc<dyn SyncStorage>> {
        self.connection.read().unwrap().storage()
    }

    fn install_cloud(
        &self,
        sync: Arc<SyncLoopHandle>,
        storage: Arc<dyn SyncStorage>,
        driver: SyncDriver,
    ) {
        *self.connection.write().unwrap() = SyncConnection::WithCloud {
            sync,
            storage,
            driver,
        };
    }

    fn install_without_cloud(&self) {
        *self.connection.write().unwrap() = SyncConnection::WithoutCloud;
    }

    fn take_connection(&self) -> SyncConnection {
        std::mem::replace(
            &mut *self.connection.write().unwrap(),
            SyncConnection::Disconnected,
        )
    }

    async fn build_connection(
        &self,
        config: Config,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<(), SyncError> {
        if config.cloud_home.provider.is_none() {
            self.install_without_cloud();
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        }

        crate::storage::cloud::setup::require_exact_slot_capabilities_config(&config)
            .map_err(SyncError::StorageSetup)?;
        let routing_encryption = self
            .security
            .routing_encryption(self.database.has_scoped_graph())?;
        let cipher = self
            .security
            .resolve_cloud_cipher(config.cloud_home.storage)?;
        let cloud_home = self
            .security
            .create_cloud_home(&config, self.clock.clone(), cloudkit_ops)
            .await?;
        let cloud_home: Arc<dyn CloudHome> = Arc::from(cloud_home);
        let storage = Arc::new(
            self.security
                .create_sync_storage_with_home(
                    &config,
                    cloud_home.clone(),
                    Some(cipher),
                    self.blob_chunking,
                )
                .map_err(|error| match error {
                    crate::storage::cloud::setup::StorageSetupError::Key(error) => {
                        SyncError::Key(error)
                    }
                    error => SyncError::StorageSetup(error),
                })?,
        );
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn SyncStorage> = storage;
        let sync = self.build_sync(components, config, storage.clone(), routing_encryption);
        sync.start().map_err(SyncError::Loop)?;
        info!("Sync loop started");
        self.install_cloud(sync, storage, SyncDriver::Loop);
        Ok(())
    }

    async fn initialize_components(
        &self,
        storage: Arc<CloudSyncStorage>,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<SyncComponents, SyncError> {
        let initialization = match self.database.local_store_root_ref().await? {
            Some(expected_store_root) => crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root,
            },
            None => crate::sync::cycle::StoreInitialization::CreateStore,
        };
        PreparedSyncComponents::prepare(
            self.database.clone(),
            self.local_blob_access.clone(),
            storage,
            initialization,
            routing_encryption,
        )
        .await
        .map_err(SyncError::from)?
        .initialize()
        .await
        .map_err(SyncError::from)
    }

    /// Assemble the connected sync over `storage`. The loop thread is a separate
    /// decision the caller makes: production starts it, a caller-driven test
    /// connection leaves it unstarted.
    fn build_sync(
        &self,
        components: SyncComponents,
        config: Config,
        storage: Arc<dyn SyncStorage>,
        routing_encryption: Option<EncryptionService>,
    ) -> Arc<SyncLoopHandle> {
        let blob_transitions = crate::blob::transition::ConnectedBlobTransitions::new(
            self.local_blob_transitions.clone(),
            self.local_blob_access.connect(storage),
            routing_encryption,
            self.observer.clone(),
        );
        Arc::new(SyncLoopHandle::new(
            components,
            blob_transitions,
            self.security.clone(),
            self.clock.clone(),
            config,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
        ))
    }

    async fn replace_connection(
        &self,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<(), SyncError> {
        let config = self.config();
        if config.cloud_home.provider.is_some() {
            crate::storage::cloud::setup::require_exact_slot_capabilities_config(&config)
                .map_err(SyncError::StorageSetup)?;
        }
        let previous = self.take_connection();
        previous.stop()?;
        self.build_connection(config, cloudkit_ops).await
    }

    pub(crate) async fn connect(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_connection(self.cloudkit_ops.clone()).await?;
        info!("store sync connected");
        Ok(())
    }

    pub(crate) async fn connect_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_connection(Some(cloudkit_ops)).await?;
        info!("store sync connected with CloudKit driver");
        Ok(())
    }

    /// Build a connection over an injected cloud home, replacing whatever was
    /// connected before. The returned sync is assembled but unstarted; the caller
    /// decides whether a loop thread or the caller itself drives it, and installs
    /// it accordingly.
    #[cfg(any(test, feature = "test-utils"))]
    async fn build_test_connection(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
    ) -> Result<(Arc<SyncLoopHandle>, Arc<dyn SyncStorage>), SyncError> {
        let config = self.config();
        crate::storage::cloud::setup::require_exact_slot_capabilities_home(
            home.clone(),
            config.cloud_home.provider.clone(),
        )
        .map_err(SyncError::StorageSetup)?;
        let previous = self.take_connection();
        previous.stop()?;
        let routing_encryption = self
            .security
            .routing_encryption(self.database.has_scoped_graph())?;
        let storage = Arc::new(
            self.security
                .create_sync_storage_with_home(
                    &config,
                    home.clone(),
                    Some(cipher),
                    self.blob_chunking,
                )
                .map_err(|error| match error {
                    crate::storage::cloud::setup::StorageSetupError::Key(error) => {
                        SyncError::Key(error)
                    }
                    error => SyncError::StorageSetup(error),
                })?,
        );
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn SyncStorage> = storage;
        let sync = self.build_sync(components, config, storage.clone(), routing_encryption);
        Ok((sync, storage))
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn replace_with_test_home(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let (sync, storage) = self.build_test_connection(home, cipher).await?;
        sync.start().map_err(SyncError::Loop)?;
        info!("Sync loop started");
        self.install_cloud(sync, storage, SyncDriver::Loop);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_with_test_home(home, cipher).await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home_caller_driven(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        let (sync, storage) = self.build_test_connection(home, cipher).await?;
        self.install_cloud(sync, storage, SyncDriver::Caller);
        info!(
            "store sync connected over an injected test cloud home; the caller drives its cycles"
        );
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home_custody(
        &self,
        home: Arc<dyn CloudHome>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        let cipher = self
            .security
            .resolve_cloud_cipher(self.config().cloud_home.storage)?;
        self.replace_with_test_home(home, cipher).await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    pub(crate) async fn start(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        if !self.connection.read().unwrap().is_connected() {
            debug!("start_sync: no provider connected; nothing to start");
            return Ok(());
        }
        self.replace_connection(self.cloudkit_ops.clone()).await
    }

    pub(crate) fn stop(&self) {
        let connection = self.take_connection();
        let was_connected = connection.is_connected();
        if let Err(stop_error) = connection.stop() {
            error!("stop_sync failed: {stop_error}");
        }
        if was_connected {
            self.install_without_cloud();
        } else {
            debug!("stop_sync: no provider connected; nothing to stop");
        }
    }

    pub(crate) fn disconnect(&self) {
        let connection = self.take_connection();
        if let Err(stop_error) = connection.stop() {
            error!("disconnect_sync failed to stop sync: {stop_error}");
        }
        info!("store sync disconnected");
    }

    pub(crate) fn trigger(&self) {
        match self.cloud_sync() {
            Some(sync_loop) => sync_loop.trigger(),
            None => debug!("sync_now: no cloud connection; sync wake ignored"),
        }
    }

    pub(crate) fn is_syncing(&self) -> bool {
        self.cloud_sync().is_some_and(|handle| handle.is_running())
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connection.read().unwrap().is_connected()
    }

    pub(crate) fn subscribe_status(&self) -> watch::Receiver<SyncLoopStatus> {
        self.status_tx.subscribe()
    }

    pub(crate) fn host_write_blob_staging(
        &self,
    ) -> Option<crate::sync::store::HostWriteBlobStaging> {
        let store = self.cloud_sync()?.store();
        Some(
            store
                .host_write_blob_staging(tokio::runtime::Handle::current(), self.store_dir.clone()),
        )
    }

    #[cfg(test)]
    pub(crate) async fn create_test_store(
        &self,
        store_id: &str,
        signer: crate::keys::UserKeypair,
    ) -> Result<crate::sync::test_helpers::TestStore, String> {
        crate::sync::test_helpers::TestStore::create_with_database(
            self.database.clone(),
            store_id,
            signer,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_test_store(
        &self,
        store: &crate::sync::test_helpers::TestStore,
    ) -> Result<bool, String> {
        store
            .publish_pending_store_database(&self.database, &self.store_dir)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn pull_test_store(
        &self,
        store: &crate::sync::test_helpers::TestStore,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            crate::sync::store::StorePullResult,
        ),
        crate::sync::store::StorePullError,
    > {
        let device = store
            .open_into_store_database(&self.database)
            .await
            .map_err(|error| {
                crate::sync::store::StorePullError::Membership(
                    crate::sync::store::StorePullMembershipError::Message(error),
                )
            })?;
        let routing_encryption = crate::encryption::EncryptionService::from_key([42; 32]);
        let mut authorization = device
            .store
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
        let result = authorization
            .pull(&self.store_dir, Some(&routing_encryption))
            .await
            .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
        let sequences = result
            .frontier
            .iter()
            .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
            .collect();
        Ok((sequences, result))
    }

    #[cfg(test)]
    pub(crate) async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), DbError> {
        self.database
            .latest_materialized_commit_coordinate_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) fn arm_pull_after_remote_commit_for_test(
        &self,
        device_id: String,
        sequence: u64,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.database
            .arm_test_pause(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id,
                seq: sequence,
            })
    }

    async fn blob_storage(&self) -> Result<Option<Arc<dyn SyncStorage>>, BlobCacheError> {
        if let Some(storage) = self.storage() {
            return Ok(Some(storage));
        }
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(None);
        }
        let storage = self
            .security
            .create_sync_storage(
                &config,
                None,
                self.clock.clone(),
                self.cloudkit_ops.clone(),
                self.blob_chunking,
            )
            .await?;
        Ok(Some(Arc::new(storage)))
    }

    pub(crate) async fn blob_access(&self) -> Result<StoreBlobAccess, BlobCacheError> {
        match self.blob_storage().await? {
            Some(storage) => Ok(StoreBlobAccess::remote(
                self.local_blob_access.connect(storage),
            )),
            None => Ok(StoreBlobAccess::local(self.local_blob_access.clone())),
        }
    }

    pub(crate) fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let (scheme, uploader) = match self.cloud_sync() {
            Some(sync_loop) => (
                sync_loop.blob_path_scheme(),
                Some(sync_loop.self_uploader()),
            ),
            None => {
                let scheme = BlobPathScheme::for_storage(self.config().cloud_home.storage);
                let uploader = self
                    .security
                    .identity_public_key()
                    .map_err(|error| {
                        StorageError::Storage(format!("read this store's identity: {error}"))
                    })?
                    .map(hex::encode);
                (scheme, uploader)
            }
        };
        CloudSyncStorage::blob_key(
            scheme,
            &blob.namespace,
            uploader.as_deref(),
            &blob.id,
            blob.cloud_path.as_deref(),
        )
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        let sync_loop = self.driven_sync().ok_or(MakeRemoteError::SyncNotReady)?;
        sync_loop.make_remote(root_table, root_id, pin).await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        let sync_loop = self.driven_sync().ok_or(MakeRemoteError::SyncNotReady)?;
        sync_loop.cancel_make_remote(root_table, root_id).await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        let sync_loop = self.driven_sync().ok_or(MakeLocalError::SyncNotReady)?;
        sync_loop
            .make_local(root_table, root_id, dest, cancel)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<crate::blob::upload::DrainOutcome, SyncError> {
        self.driven_sync()
            .ok_or(SyncError::LoopNotRunning)?
            .drain_uploads()
            .await
            .map_err(SyncError::BlobUpload)
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, SyncError> {
        self.driven_sync()
            .ok_or(SyncError::LoopNotRunning)?
            .discard_blocked_write(write_id)
            .await
            .map_err(SyncError::from)
    }

    pub(crate) fn is_command_configured(&self) -> bool {
        self.cloud_sync().is_some() || self.config().cloud_home.provider.is_some()
    }

    pub(crate) fn command_config(&self) -> Config {
        self.cloud_sync()
            .map(|handle| handle.config().clone())
            .unwrap_or_else(|| self.config())
    }

    pub(crate) async fn store_for_command(
        &self,
        identity: &crate::keys::UserKeypair,
    ) -> Result<Store, SyncError> {
        let config = self.command_config();
        let storage = match self.storage() {
            Some(storage) => storage,
            None => {
                let storage = self
                    .security
                    .create_sync_storage(
                        &config,
                        None,
                        self.clock.clone(),
                        self.cloudkit_ops.clone(),
                        self.blob_chunking,
                    )
                    .await
                    .map_err(SyncError::StorageSetup)?;
                Arc::new(storage)
            }
        };
        Store::load(self.database.clone(), storage, identity.clone())
            .await
            .map_err(SyncError::from)
    }

    #[cfg(test)]
    pub(crate) fn loop_uses_connected_storage_for_test(&self) -> bool {
        match (self.storage(), self.cloud_sync()) {
            (Some(storage), Some(sync_loop)) => sync_loop.uses_storage_for_test(&storage),
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_remote_storage_for_test(&self) -> bool {
        self.storage().is_some()
    }

    pub(crate) fn active_store(&self) -> Result<Arc<Store>, SyncError> {
        Ok(self.driven_sync().ok_or(SyncError::LoopNotRunning)?.store())
    }

    pub(crate) fn active_membership(&self) -> Result<ActiveMembershipSync, SyncError> {
        Ok(ActiveMembershipSync {
            loop_handle: self.driven_sync().ok_or(SyncError::LoopNotRunning)?,
        })
    }

    /// Circle *writes* are dispatched to the loop thread and executed there, so
    /// this is the one capability that needs the thread itself rather than a
    /// driven connection — and it says which of the two is missing.
    pub(crate) fn active_circles(&self) -> Result<ActiveCircleSync, crate::CircleError> {
        let sync = self.cloud_sync().ok_or(crate::CircleError::NotConfigured)?;
        if !sync.is_running() {
            return Err(crate::CircleError::LoopNotRunning);
        }
        Ok(ActiveCircleSync { loop_handle: sync })
    }
}

pub(crate) struct ActiveMembershipSync {
    loop_handle: Arc<SyncLoopHandle>,
}

impl ActiveMembershipSync {
    pub(crate) fn is_encrypted(&self) -> bool {
        self.loop_handle.current_encryption().is_some()
    }

    pub(crate) fn store_name(&self) -> &str {
        &self.loop_handle.config().store_name
    }

    pub(crate) async fn invite(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
    ) -> Result<crate::joining::InviteCode, crate::sync::store::MembershipOpsError> {
        self.loop_handle
            .invite_member(public_key_hex, invitee_email, role, self.store_name())
            .await
    }

    pub(crate) async fn remove(
        &self,
        public_key_hex: &str,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        self.loop_handle.remove_member(public_key_hex).await
    }

    pub(crate) async fn resolve(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), crate::sync::store::MembershipOpsError> {
        self.loop_handle.resolve_membership_conflict(choice).await
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusionProposalRef, SyncError> {
        self.loop_handle
            .store()
            .propose_device_exclusion_for_device(device_id)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .store()
            .cancel_device_exclusion_proposal(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .store()
            .finalize_device_exclusion_proposal(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, SyncError> {
        self.loop_handle
            .store()
            .begin_owner_promotion_for_device(device_id)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionAcceptance, SyncError> {
        self.loop_handle
            .store()
            .accept_owner_promotion(request)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), SyncError> {
        let encryption = self
            .loop_handle
            .current_encryption()
            .ok_or(SyncError::NotEncryptedHome)?;
        self.loop_handle
            .store()
            .finalize_owner_promotion(&encryption, acceptance)
            .await
            .map(|_| ())
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }
}

pub(crate) struct ActiveCircleSync {
    loop_handle: Arc<SyncLoopHandle>,
}

impl ActiveCircleSync {
    pub(crate) async fn create(
        &self,
        name: &str,
    ) -> Result<crate::CircleId, crate::sync::store::CircleOperationError> {
        self.loop_handle.create_circle(name).await
    }

    pub(crate) async fn rename(
        &self,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.rename_circle(circle_id, name).await
    }

    pub(crate) async fn add_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .add_circle_member(circle_id, member_pubkey, role)
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.loop_handle
            .remove_circle_member(circle_id, member_pubkey)
            .await
    }

    pub(crate) async fn resolve(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    pub(crate) async fn cancel_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.loop_handle.cancel_circle_epoch_close(circle_id).await
    }

    pub(crate) async fn exclude_close_device(
        &self,
        circle_id: crate::CircleId,
        device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .exclude_circle_close_device(circle_id, device_id)
            .await
    }

    pub(crate) async fn delete(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.delete_circle(circle_id).await
    }

    pub(crate) async fn retry(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.retry_circle_operation(operation_id).await
    }

    pub(crate) async fn discard(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .discard_circle_operation(operation_id)
            .await
    }

    pub(crate) async fn close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::sync::store::CircleOperationError> {
        self.loop_handle.circle_close_status(circle_id).await
    }
}

#[cfg(test)]
#[path = "store_sync/tests.rs"]
mod tests;

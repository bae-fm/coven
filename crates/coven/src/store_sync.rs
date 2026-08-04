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
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
use crate::storage::cloud::CloudHomeError;
use crate::storage::{BlobChunking, BlobPathScheme, CloudSyncStorage, StorageError, SyncStorage};
use crate::store_security::StoreSecurity;
use crate::sync::cycle::{InitSyncError, SyncComponents};
use crate::sync::store::blob::{
    LocalStoreBlobAccess, RemoteBlobSource, RemoteStoreBlobAccess, StoreBlobAccess,
};
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
    read_only_blob_storage: crate::store_blobs::ReadOnlyBlobStorage,
    local_blob_transitions: crate::blob::transition::LocalBlobTransitions,
    state: Arc<RwLock<SyncConnection>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    #[cfg(test)]
    stopped_loops: Arc<std::sync::atomic::AtomicU64>,
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
        read_only_blob_storage: crate::store_blobs::ReadOnlyBlobStorage,
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
            read_only_blob_storage,
            local_blob_transitions,
            state: Arc::new(RwLock::new(SyncConnection::Disconnected)),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            status_tx: tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            #[cfg(test)]
            stopped_loops: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub(crate) async fn blob_access(&self) -> Result<StoreBlobAccess, BlobCacheError> {
        let storage = {
            let connection = self.state.read().expect("read Store sync connection");
            match &*connection {
                SyncConnection::WithCloud { storage, .. } => Some(Arc::clone(storage)),
                _ => None,
            }
        };
        match storage {
            Some(storage) => Ok(StoreBlobAccess::remote(RemoteStoreBlobAccess::new(
                self.local_blob_access.clone(),
                RemoteBlobSource::current(self.database.clone(), storage),
            ))),
            None => self
                .read_only_blob_storage
                .access(self.local_blob_access.clone())
                .await
                .map_err(Into::into),
        }
    }

    fn install_cloud(
        &self,
        sync: Arc<SyncLoopHandle>,
        storage: Arc<dyn SyncStorage>,
        driver: SyncDriver,
    ) {
        *self.state.write().expect("write Store sync connection") = SyncConnection::WithCloud {
            sync,
            storage,
            driver,
        };
    }

    fn install_without_cloud(&self) {
        *self.state.write().expect("write Store sync connection") = SyncConnection::WithoutCloud;
    }

    fn stop_current(&self) -> Result<bool, SyncError> {
        let previous = std::mem::replace(
            &mut *self.state.write().expect("write Store sync connection"),
            SyncConnection::Disconnected,
        );
        let was_connected = !matches!(previous, SyncConnection::Disconnected);
        if let SyncConnection::WithCloud { sync, .. } = previous {
            sync.stop().map_err(SyncError::Loop)?;
            #[cfg(test)]
            self.stopped_loops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(was_connected)
    }

    pub(crate) fn is_connected(&self) -> bool {
        !matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::Disconnected
        )
    }

    pub(crate) fn trigger(&self) {
        match self.connected() {
            Some(connection) => connection.trigger(),
            None => debug!("sync_now: no cloud connection; sync wake ignored"),
        }
    }

    pub(crate) fn is_syncing(&self) -> bool {
        self.connected()
            .is_some_and(|connection| connection.is_running())
    }

    fn has_cloud(&self) -> bool {
        matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::WithCloud { .. }
        )
    }

    fn connected(&self) -> Option<ConnectedSyncOperation> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => Some(ConnectedSyncOperation {
                loop_handle: Arc::clone(sync),
            }),
            _ => None,
        }
    }

    fn active(&self) -> Option<ActiveSyncOperation> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, driver, .. } => {
                let driven = match driver {
                    SyncDriver::Loop => sync.is_running(),
                    #[cfg(any(test, feature = "test-utils"))]
                    SyncDriver::Caller => true,
                };
                driven.then(|| ActiveSyncOperation {
                    loop_handle: Arc::clone(sync),
                })
            }
            _ => None,
        }
    }

    async fn connected_command(&self) -> Option<Result<StoreCommand, SyncError>> {
        let storage = {
            let connection = self.state.read().expect("read Store sync connection");
            match &*connection {
                SyncConnection::WithCloud { storage, .. } => Some(Arc::clone(storage)),
                _ => None,
            }
        }?;
        let identity = match self.security.established_identity() {
            Ok(identity) => identity,
            Err(error) => return Some(Err(error.into())),
        };
        Some(
            identity
                .load_store(self.database.clone(), storage)
                .await
                .map(StoreCommand::new)
                .map_err(SyncError::from),
        )
    }

    #[cfg(test)]
    fn loop_uses_connected_storage(&self) -> bool {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, storage, .. } => sync.uses_storage_for_test(storage),
            _ => false,
        }
    }

    #[cfg(test)]
    fn stopped_loop_count(&self) -> u64 {
        self.stopped_loops.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    fn stop_loop(&self) -> Result<(), SyncError> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => sync.stop().map_err(SyncError::Loop),
            _ => Err(SyncError::LoopNotRunning),
        }
    }
}

struct ConnectedSyncOperation {
    loop_handle: Arc<SyncLoopHandle>,
}

impl ConnectedSyncOperation {
    fn trigger(&self) {
        self.loop_handle.trigger();
    }

    fn is_running(&self) -> bool {
        self.loop_handle.is_running()
    }

    fn config(&self) -> Config {
        self.loop_handle.config().clone()
    }

    fn blob_path_scheme(&self) -> BlobPathScheme {
        self.loop_handle.blob_path_scheme()
    }

    fn uploader(&self) -> String {
        self.loop_handle.self_uploader()
    }

    fn host_write_blob_staging(
        &self,
        store_dir: crate::store_dir::StoreDir,
    ) -> crate::sync::store::HostWriteBlobStaging {
        self.loop_handle
            .host_write_blob_staging(tokio::runtime::Handle::current(), store_dir)
    }

    #[cfg(test)]
    fn uses_store_dir(&self, store_dir: &crate::store_dir::StoreDir) -> bool {
        self.loop_handle.uses_store_dir_for_test(store_dir)
    }

    #[cfg(test)]
    fn adopt_key_rotation(&self, encryption: EncryptionService) -> Result<(), SyncError> {
        self.loop_handle
            .adopt_key_rotation_for_test(encryption)
            .map(|_| ())
            .map_err(SyncError::from)
    }

    #[cfg(test)]
    fn encryption_generation(&self) -> Option<u64> {
        self.loop_handle.encryption_generation_for_test()
    }

    #[cfg(test)]
    fn open_sealed_blob(
        &self,
        bytes: &[u8],
        context: &[u8],
    ) -> Result<(crate::encryption::KeyFingerprint, Vec<u8>), StorageError> {
        self.loop_handle
            .open_sealed_blob_for_test(bytes, context)
            .map_err(StorageError::Storage)
    }
}

struct ActiveSyncOperation {
    loop_handle: Arc<SyncLoopHandle>,
}

pub(crate) struct StoreCommand {
    store: Store,
}

impl StoreCommand {
    fn new(store: Store) -> Self {
        Self { store }
    }

    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, crate::sync::store::MembershipOpsError>
    {
        self.store.members().await
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, crate::sync::store::MembershipOpsError> {
        self.store.membership_conflict().await
    }

    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<
        crate::sync::store::owner::StoreRestoreMembership,
        crate::sync::store::MembershipOpsError,
    > {
        self.store.restore_membership().await
    }
}

impl ActiveSyncOperation {
    async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        self.loop_handle.make_remote(root_table, root_id, pin).await
    }

    async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.loop_handle
            .cancel_make_remote(root_table, root_id)
            .await
    }

    async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        self.loop_handle
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    async fn drain_uploads(&self) -> Result<crate::blob::upload::DrainOutcome, DbError> {
        self.loop_handle.drain_uploads().await
    }

    async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        self.loop_handle.discard_blocked_write(write_id).await
    }

    fn membership(self) -> ActiveMembershipSync {
        ActiveMembershipSync {
            loop_handle: self.loop_handle,
        }
    }

    fn circles(self) -> Result<ActiveCircleSync, crate::CircleError> {
        if !self.loop_handle.is_running() {
            return Err(crate::CircleError::LoopNotRunning);
        }
        Ok(ActiveCircleSync {
            loop_handle: self.loop_handle,
        })
    }

    async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle
            .begin_device_join_bundle(member_pubkey)
            .await
    }

    async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle
            .drive_device_join(bundle, policy, access_administrator, timing)
            .await
    }

    async fn cancel_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::sync::store::DeviceJoinTransportError>
    {
        self.loop_handle
            .cancel_device_join_transport(bundle, timing)
            .await
    }

    async fn abandon_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinAbandonment, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle.abandon_device_join_transport(bundle).await
    }

    async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, crate::DeviceJoinError> {
        self.loop_handle.begin_device_join(member_pubkey).await
    }

    async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, crate::DeviceJoinError> {
        self.loop_handle.abandon_device_join(offer).await
    }

    async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, crate::DeviceJoinError> {
        self.loop_handle
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, crate::DeviceJoinError> {
        self.loop_handle.accept_device_registration(request).await
    }

    async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, crate::DeviceJoinError> {
        self.loop_handle
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, crate::DeviceJoinError> {
        self.loop_handle
            .complete_device_provider_admission(readiness)
            .await
    }

    async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, crate::DeviceJoinError> {
        self.loop_handle.finalize_device_join(completion).await
    }

    async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, crate::DeviceJoinError> {
        self.loop_handle.cancel_device_join(attempt).await
    }

    async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .close_device_provider_admission(cancellation)
            .await
    }

    async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await
    }

    async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .revoke_joining_device_writes(cancellation, executor)
            .await
    }

    async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.loop_handle.activate_device_join_cleanup(receipt).await
    }

    async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), crate::DeviceJoinError> {
        self.loop_handle
            .complete_owner_device_join_cleanup(activation)
            .await
            .map(|_| ())
    }
}

impl StoreSync {
    fn config(&self) -> Config {
        (self.config_provider)()
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
        let storage = Arc::new(
            self.security
                .create_sync_storage(
                    &config,
                    Some(cipher),
                    self.clock.clone(),
                    cloudkit_ops,
                    self.blob_chunking,
                )
                .await
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
        self.security
            .established_identity()?
            .initialize_sync_components(
                self.database.clone(),
                self.local_blob_access.clone(),
                storage,
                initialization,
                routing_encryption,
            )
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
            RemoteStoreBlobAccess::new(
                self.local_blob_access.clone(),
                RemoteBlobSource::current(self.database.clone(), storage),
            ),
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
        self.stop_current()?;
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

    /// Replace the current connection with one over an injected cloud home.
    /// This lifetime owner constructs and installs both the loop and its storage;
    /// callers select only who drives cycles.
    #[cfg(any(test, feature = "test-utils"))]
    async fn replace_with_test_home(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
        driver: SyncDriver,
    ) -> Result<(), SyncError> {
        let config = self.config();
        crate::storage::cloud::setup::require_exact_slot_capabilities_home(
            home.clone(),
            config.cloud_home.provider.clone(),
        )
        .map_err(SyncError::StorageSetup)?;
        self.stop_current()?;
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
        if matches!(&driver, SyncDriver::Loop) {
            sync.start().map_err(SyncError::Loop)?;
            info!("Sync loop started");
        }
        self.install_cloud(sync, storage, driver);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_with_test_home(home, cipher, SyncDriver::Loop)
            .await?;
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
        self.replace_with_test_home(home, cipher, SyncDriver::Caller)
            .await?;
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
        self.replace_with_test_home(home, cipher, SyncDriver::Loop)
            .await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    pub(crate) async fn start(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        if !self.is_connected() {
            debug!("start_sync: no provider connected; nothing to start");
            return Ok(());
        }
        self.replace_connection(self.cloudkit_ops.clone()).await
    }

    pub(crate) fn stop(&self) {
        let was_connected = match self.stop_current() {
            Ok(was_connected) => was_connected,
            Err(stop_error) => {
                error!("stop_sync failed: {stop_error}");
                false
            }
        };
        if was_connected {
            self.install_without_cloud();
        } else {
            debug!("stop_sync: no provider connected; nothing to stop");
        }
    }

    pub(crate) fn disconnect(&self) {
        if let Err(stop_error) = self.stop_current() {
            error!("disconnect_sync failed to stop sync: {stop_error}");
        }
        info!("store sync disconnected");
    }

    pub(crate) fn subscribe_status(&self) -> watch::Receiver<SyncLoopStatus> {
        self.status_tx.subscribe()
    }

    pub(crate) fn host_write_blob_staging(
        &self,
    ) -> Option<crate::sync::store::HostWriteBlobStaging> {
        Some(
            self.connected()?
                .host_write_blob_staging(self.store_dir.clone()),
        )
    }

    #[cfg(test)]
    pub(crate) async fn create_test_store(
        &self,
        store_id: &str,
        signer: crate::keys::UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<std::sync::Arc<crate::sync::test_helpers::TestStore>, String> {
        crate::sync::test_helpers::TestStore::create_with_database(
            self.database.clone(),
            store_id,
            signer,
            home,
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

    pub(crate) fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let (scheme, uploader) = match self.connected() {
            Some(connection) => (connection.blob_path_scheme(), Some(connection.uploader())),
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
        self.active()
            .ok_or(MakeRemoteError::SyncNotReady)?
            .make_remote(root_table, root_id, pin)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.active()
            .ok_or(MakeRemoteError::SyncNotReady)?
            .cancel_make_remote(root_table, root_id)
            .await?;
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
        self.active()
            .ok_or(MakeLocalError::SyncNotReady)?
            .make_local(root_table, root_id, dest, cancel)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<crate::blob::upload::DrainOutcome, SyncError> {
        self.active()
            .ok_or(SyncError::LoopNotRunning)?
            .drain_uploads()
            .await
            .map_err(SyncError::BlobUpload)
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, SyncError> {
        self.active()
            .ok_or(SyncError::LoopNotRunning)?
            .discard_blocked_write(write_id)
            .await
            .map_err(SyncError::from)
    }

    pub(crate) fn is_command_configured(&self) -> bool {
        self.has_cloud() || self.config().cloud_home.provider.is_some()
    }

    pub(crate) fn command_config(&self) -> Config {
        self.connected()
            .map(|connection| connection.config())
            .unwrap_or_else(|| self.config())
    }

    pub(crate) async fn command(&self) -> Result<StoreCommand, SyncError> {
        if let Some(command) = self.connected_command().await {
            return command;
        }
        let config = self.command_config();
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
        self.security
            .established_identity()?
            .load_store(self.database.clone(), Arc::new(storage))
            .await
            .map(StoreCommand::new)
            .map_err(SyncError::from)
    }

    #[cfg(test)]
    pub(crate) fn loop_uses_connected_storage_for_test(&self) -> bool {
        self.loop_uses_connected_storage()
    }

    #[cfg(test)]
    pub(crate) fn stopped_loop_count_for_test(&self) -> u64 {
        self.stopped_loop_count()
    }

    #[cfg(test)]
    pub(crate) fn stop_loop_for_test(&self) -> Result<(), SyncError> {
        self.stop_loop()
    }

    #[cfg(test)]
    pub(crate) fn connected_store_id_for_test(&self) -> Option<String> {
        Some(self.connected()?.config().store_id)
    }

    #[cfg(test)]
    pub(crate) fn connected_uses_store_dir_for_test(
        &self,
        store_dir: &crate::store_dir::StoreDir,
    ) -> bool {
        self.connected()
            .is_some_and(|connection| connection.uses_store_dir(store_dir))
    }

    #[cfg(test)]
    pub(crate) fn connected_blob_path_scheme_for_test(&self) -> Option<BlobPathScheme> {
        Some(self.connected()?.blob_path_scheme())
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: EncryptionService,
    ) -> Result<(), SyncError> {
        self.connected()
            .ok_or(SyncError::LoopNotRunning)?
            .adopt_key_rotation(encryption)
    }

    #[cfg(test)]
    pub(crate) fn encryption_generation_for_test(&self) -> Option<u64> {
        self.connected()?.encryption_generation()
    }

    #[cfg(test)]
    pub(crate) fn open_sealed_blob_for_test(
        &self,
        bytes: &[u8],
        context: &[u8],
    ) -> Result<(crate::encryption::KeyFingerprint, Vec<u8>), StorageError> {
        self.connected()
            .ok_or_else(|| StorageError::Storage("sync connection is not installed".to_string()))?
            .open_sealed_blob(bytes, context)
    }

    #[cfg(test)]
    pub(crate) fn has_remote_storage_for_test(&self) -> bool {
        self.has_cloud()
    }

    pub(crate) fn active_membership(&self) -> Result<ActiveMembershipSync, SyncError> {
        Ok(self.active().ok_or(SyncError::LoopNotRunning)?.membership())
    }

    /// Circle *writes* are dispatched to the loop thread and executed there, so
    /// this is the one capability that needs the thread itself rather than a
    /// driven connection — and it says which of the two is missing.
    pub(crate) fn active_circles(&self) -> Result<ActiveCircleSync, crate::CircleError> {
        self.active()
            .ok_or(crate::CircleError::NotConfigured)?
            .circles()
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .begin_device_join_bundle(member_pubkey)
            .await?)
    }

    pub(crate) async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .drive_device_join(bundle, policy, access_administrator, timing)
            .await?)
    }

    pub(crate) async fn cancel_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .cancel_device_join_transport(bundle, timing)
            .await?)
    }

    pub(crate) async fn abandon_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .abandon_device_join_transport(bundle)
            .await?)
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .begin_device_join(member_pubkey)
            .await?)
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .abandon_device_join(offer)
            .await?)
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .authorize_device_provider_access(request, access_administrator)
            .await?)
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .accept_device_registration(request)
            .await?)
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .publish_device_provider_challenge(bootstrap)
            .await?)
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .complete_device_provider_admission(readiness)
            .await?)
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .finalize_device_join(completion)
            .await?)
    }

    pub(crate) async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .cancel_device_join(attempt)
            .await?)
    }

    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .close_device_provider_admission(cancellation)
            .await?)
    }

    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .revoke_joining_device_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        Ok(self
            .active()
            .ok_or(SyncError::LoopNotRunning)?
            .activate_device_join_cleanup(receipt)
            .await?)
    }

    pub(crate) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), SyncError> {
        self.active()
            .ok_or(SyncError::LoopNotRunning)?
            .complete_owner_device_join_cleanup(activation)
            .await?;
        Ok(())
    }
}

pub(crate) struct ActiveMembershipSync {
    loop_handle: Arc<SyncLoopHandle>,
}

impl ActiveMembershipSync {
    pub(crate) fn is_encrypted(&self) -> bool {
        self.loop_handle.is_encrypted()
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
            .propose_device_exclusion(device_id)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .cancel_device_exclusion(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .finalize_device_exclusion(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, SyncError> {
        self.loop_handle
            .begin_owner_promotion(device_id)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionAcceptance, SyncError> {
        self.loop_handle
            .accept_owner_promotion(request)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .finalize_owner_promotion(acceptance)
            .await
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

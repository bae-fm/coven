//! High-level sync manager: lifecycle, membership, status.
//!
//! Owns the sync lifecycle — cloud home + sync loop — and starts/stops it when
//! a provider is connected/disconnected, no app restart required. The host
//! supplies the config snapshot, keys, encryption, database, clock, and blob
//! handling; coven drives the rest.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{error, info};

use crate::blob::transition::{self, MakeLocalError, MakeRemoteError};
use crate::blob::BlobTransitionObserver;
use crate::clock::ClockRef;
use crate::config::{Config, HomeStorage};
use crate::coven::StoreOpenGuard;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::{DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys};
use crate::storage::cloud::setup::{SetupError, StorageSetupError};
use crate::storage::cloud::{CloudHome, CloudHomeError};
#[cfg(any(test, feature = "test-utils"))]
use crate::sync::cloud_storage::BlobPathScheme;
use crate::sync::cloud_storage::CloudCipher;
#[cfg(any(test, feature = "test-utils"))]
use crate::sync::cloud_storage::CloudSyncStorage;
use crate::sync::cycle::{InitSyncError, SyncComponents};
/// `MemberInfo` lives next to `MemberRole` in the membership module; coven's
/// public path reaches it through here (re-exported from `lib.rs`).
pub(crate) use crate::sync::membership::MemberInfo;
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::store::{Store, StoreDatabase};
use crate::sync::sync_loop::{SyncLoopError, SyncLoopHandle, SyncLoopStatus};

/// Supplies the host's current config for building the next connection. Starting
/// a loop captures one snapshot; commands on that running loop use its immutable
/// store identity, representation, provider settings, and directory even if the
/// host's next config changes meanwhile.
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

/// High-level sync manager.
///
/// Holds the store's master-key custody. The at-rest cipher is resolved from
/// it per [`start_sync`](Self::start_sync) call for an opaque home; a
/// store with scoped rows also loads generation 1 for stable row routing,
/// independent of the home's storage representation.
pub(crate) struct SyncManager {
    config_provider: ConfigProvider,
    key_service: StoreKeys,
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    database: StoreDatabase,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    /// How this installation chunks blobs and how wide its range requests are.
    blob_chunking: crate::sync::cloud_storage::BlobChunking,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The store-directory lock, cloned into every sync loop this manager
    /// starts so the loop's thread keeps it alive for the whole of its final
    /// cycle. The lock releases when the last writer — a running loop, else the
    /// handle — is gone, never while a detached loop thread is still writing.
    open_guard: Arc<StoreOpenGuard>,

    /// The current status value the [`CovenHandle`](crate::CovenHandle) owns, cloned
    /// into every sync loop this manager starts so a subscription outlives the
    /// loop restarts a reconnect performs.
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,

    // Mutable sync state — updated when providers are connected/disconnected
    sync_loop_handle: RwLock<Option<Arc<SyncLoopHandle>>>,
    cloud_home: RwLock<Option<Arc<dyn CloudHome>>>,

    /// Serializes the membership operations that mint or rotate the store key —
    /// invite (wraps the key to a new member) and remove (mints a fresh key and
    /// re-wraps it to everyone remaining). Each clones the live cipher at entry
    /// and builds a new keyring on top of it; without this, two rapid ops on one
    /// device would both clone the same base generation and prepare competing
    /// membership authorities. Held for the whole operation so the second waits
    /// and builds on the first's committed state.
    member_ops_lock: tokio::sync::Mutex<()>,
}

impl SyncManager {
    async fn storage_for_command(
        &self,
        config: &Config,
        active_loop: Option<&Arc<SyncLoopHandle>>,
    ) -> Result<Arc<crate::sync::cloud_storage::CloudSyncStorage>, SyncError> {
        if let Some(active_loop) = active_loop {
            return Ok(active_loop.storage().clone());
        }
        let storage = match self.cloud_home() {
            Some(home) => crate::storage::cloud::setup::create_sync_storage_with_home(
                config,
                self.custody.as_ref(),
                self.identity_custody.as_ref(),
                home,
                None,
                self.blob_chunking,
            ),
            None => {
                crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
                    config,
                    &self.key_service,
                    self.custody.as_ref(),
                    self.identity_custody.as_ref(),
                    None,
                    self.clock.clone(),
                    self.cloudkit_ops.clone(),
                    self.blob_chunking,
                )
                .await
            }
        }
        .map_err(SyncError::StorageSetup)?;
        Ok(Arc::new(storage))
    }

    /// Build the manager off the owned [`Database`]. Session initialization takes
    /// the database's already-seeded register clock into [`SyncComponents`], and
    /// every connected command reads that captured clock from the installed loop.
    ///
    /// Construction is infallible and synchronous: seeding already happened in
    /// the open path. The manager is built lazily, only once a provider is
    /// connected.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        custody: Arc<dyn MasterKeyCustody>,
        identity_custody: Arc<dyn DeviceIdentityCustody>,
        db: Database,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
        blob_chunking: crate::sync::cloud_storage::BlobChunking,
    ) -> Self {
        Self {
            config_provider,
            key_service,
            custody,
            identity_custody,
            database: StoreDatabase::from_database(db),
            clock,
            cloudkit_ops,
            blob_chunking,
            observer,
            open_guard,
            status_tx,
            sync_loop_handle: RwLock::new(None),
            cloud_home: RwLock::new(None),
            member_ops_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn cloud_home(&self) -> Option<Arc<dyn CloudHome>> {
        self.cloud_home.read().unwrap().clone()
    }

    fn db(&self) -> &Database {
        self.database.sqlite()
    }

    pub(crate) fn sync_loop_handle(&self) -> Option<Arc<SyncLoopHandle>> {
        self.sync_loop_handle.read().unwrap().clone()
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: coven_core::WriteId,
    ) -> Result<Vec<coven_core::WriteId>, SyncError> {
        let loop_handle = self.sync_loop_handle().ok_or(SyncError::LoopNotRunning)?;
        let identity = crate::keys::require_identity(self.identity_custody.as_ref())?;
        let device_id = self
            .database
            .sqlite()
            .get_protocol_state(coven_core::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or_else(|| {
                SyncError::Protocol("local Store device identity is absent".to_string())
            })?;
        Store::load(self.database.clone(), Arc::clone(loop_handle.storage()))
            .await
            .map_err(|error| SyncError::Protocol(error.to_string()))?
            .discard_blocked_write(&device_id, &identity, write_id)
            .await
            .map_err(SyncError::from)
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// Resolve the home's at-rest cipher from `storage` and this manager's
    /// custody. A browsable home never consults custody — it stores in the
    /// clear regardless. An opaque home unlocks the master keyring; no key
    /// established (a locked store, or a browsable/opaque storage mismatch)
    /// is [`SyncError::MasterKeyNotEstablished`], surfaced here rather than
    /// deep inside the home build. The single custody→cipher decision both
    /// [`start_sync`](Self::start_sync) and the test-home connect path share.
    fn resolve_cipher(&self, storage: HomeStorage) -> Result<CloudCipher, SyncError> {
        if storage.is_browsable() {
            Ok(CloudCipher::Plaintext)
        } else {
            let keyring = self.custody.unlock()?;
            CloudCipher::for_storage(storage, keyring.map(Into::into))
                .ok_or(SyncError::MasterKeyNotEstablished)
        }
    }

    fn routing_encryption(&self) -> Result<Option<EncryptionService>, SyncError> {
        self.db()
            .gates()
            .has_scoped_graph()
            .then(|| crate::handle::routing_encryption_from_custody(self.custody.as_ref()))
            .transpose()
            .map_err(SyncError::from)
    }

    /// Initialize cloud home and sync loop from current config.
    /// Called at startup (if already configured) and after connecting a provider.
    ///
    /// Two outcomes are success: a configured provider whose home builds and whose
    /// loop starts, and a store with no configured provider that legitimately
    /// starts no loop — the latter is a logged `Ok(())` no-op. A cloud-home build
    /// that *fails* (missing credentials, a bad provider config) is an `Err`, not
    /// "no provider connected": the caller must not install a manager that reports
    /// success with nothing started.
    pub(crate) async fn start_sync(&self) -> Result<(), SyncError> {
        let config = (self.config_provider)();

        if config.cloud_home.provider.is_none() {
            self.stop_sync()?;
            // Not a failure: a store with no configured provider starts no
            // cloud home or sync loop.
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        }

        crate::storage::cloud::setup::require_exact_slot_capabilities_config(&config)
            .map_err(SyncError::StorageSetup)?;

        let routing_encryption = self.routing_encryption()?;

        self.stop_current_connection()?;

        // The home's at-rest cipher, resolved fresh on every start so a
        // stop/start picks up whatever custody now holds. Built once here so
        // the sync loop and storage share one instance — a member removal
        // rotates the key in place through it.
        let cipher = self.resolve_cipher(config.cloud_home.storage)?;

        // Build the cloud home. A failure here is a real fault — surface it so the
        // caller never installs a manager that started nothing.
        let cloud_home = crate::storage::cloud::create_cloud_home_with_cloudkit(
            &config,
            &self.key_service,
            self.clock.clone(),
            self.cloudkit_ops.clone(),
        )
        .await
        .map_err(SyncError::from)?;
        let cloud_home: Arc<dyn CloudHome> = Arc::from(cloud_home);

        // Initialize sync loop. The synced-table set is owned by the Database, so
        // init_sync reads it from there rather than from a separately-held copy.
        // Sync is enabled here, so `None` means a real startup failure (no synced
        // tables, storage/keypair/auth/membership bootstrap) that init_sync already
        // logged — surface it so the caller never installs a manager whose loop
        // never started.
        //
        // Connect never mints a device identity: a locked agent with no
        // identity established must fail here with `KeyError::NoDeviceIdentity`,
        // not silently forge one.
        let storage = crate::storage::cloud::setup::create_sync_storage_with_home(
            &config,
            self.custody.as_ref(),
            self.identity_custody.as_ref(),
            cloud_home.clone(),
            Some(cipher.clone()),
            self.blob_chunking,
        )
        .map_err(|error| match error {
            crate::storage::cloud::setup::StorageSetupError::Key(error) => SyncError::Key(error),
            error => SyncError::StorageSetup(error),
        })?;

        let initialization = self.store_initialization().await?;
        let components = crate::sync::cycle::init_sync_over_storage(
            &self.database,
            storage,
            initialization,
            routing_encryption,
        )
        .await
        .map_err(SyncError::from)?;

        let _handle = self.install_sync_loop(components, config)?;
        *self.cloud_home.write().unwrap() = Some(cloud_home);

        Ok(())
    }

    /// Build the sync-loop handle off `components`, start it, and install it. The
    /// shared install tail of [`start_sync`](Self::start_sync) and the test-only
    /// [`start_sync_with_home`](Self::start_sync_with_home): both reach it only
    /// after the bootstrap has produced [`SyncComponents`], so the loop handle is
    /// installed whole, never on a half-built bootstrap.
    fn install_sync_loop(
        &self,
        components: SyncComponents,
        config: Config,
    ) -> Result<Arc<SyncLoopHandle>, SyncError> {
        let handle = Arc::new(SyncLoopHandle::new(
            components,
            self.custody.clone(),
            self.clock.clone(),
            config,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
        ));
        handle.start().map_err(SyncError::Loop)?;

        info!("Sync loop started");
        *self.sync_loop_handle.write().unwrap() = Some(handle.clone());
        Ok(handle)
    }

    /// Test-only: stand the sync loop over an injected `home`/`cipher` instead of
    /// building the cloud home from config via `create_cloud_home`.
    ///
    /// The counterpart of [`start_sync`](Self::start_sync) for a host's
    /// integration tests, which drive coven over a mock [`CloudHome`] no provider
    /// match would ever produce. It skips the config-provider gate — the injected
    /// home IS the enablement, there are no real credentials to check — installs
    /// the home, builds a [`CloudSyncStorage`] over it under the supplied `cipher`
    /// (and the config's blob-path scheme), runs the same bootstrap
    /// [`init_sync`](crate::sync::cycle::init_sync) does via
    /// [`init_sync_over_storage`](crate::sync::cycle::init_sync_over_storage), and
    /// starts the loop. A bootstrap failure is an `Err`, the same fail-loud
    /// discipline `start_sync` keeps — and commit-whole: the home and loop handle
    /// are installed only after the keypair load and bootstrap both succeed, so a
    /// failure leaves nothing installed.
    ///
    /// After this returns, the connected loop's storage is reachable via
    /// [`sync_loop_handle`](Self::sync_loop_handle)`().storage()`, so the handle's
    /// read path serves blobs over the same injected home with no separate hook.
    ///
    /// Like [`start_sync`](Self::start_sync), this never mints a device
    /// identity: the caller must establish one under this manager's identity
    /// custody first, or this fails with `KeyError::NoDeviceIdentity`.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn start_sync_with_home(
        &self,
        home: std::sync::Arc<dyn CloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        self.start_sync_with_home_parts(home, cipher).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn start_sync_with_home_parts(
        &self,
        home: std::sync::Arc<dyn CloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        let config = (self.config_provider)();
        crate::storage::cloud::setup::require_exact_slot_capabilities_home(
            home.clone(),
            config.cloud_home.provider.clone(),
        )
        .map_err(SyncError::StorageSetup)?;
        let routing_encryption = self.routing_encryption()?;
        self.stop_current_connection()?;

        let keypair = crate::keys::require_identity(self.identity_custody.as_ref())?;
        let blob_paths = if cipher.is_plaintext() {
            BlobPathScheme::Plain
        } else {
            BlobPathScheme::Hashed
        };
        let storage = CloudSyncStorage::new(
            home.clone(),
            cipher.clone(),
            blob_paths,
            config.store_id.clone(),
            keypair,
        )?
        .with_blob_chunking(self.blob_chunking);
        let initialization = self.store_initialization().await?;
        let components = crate::sync::cycle::init_sync_over_storage(
            &self.database,
            storage,
            initialization,
            routing_encryption,
        )
        .await
        .map_err(SyncError::from)?;

        let _handle = self.install_sync_loop(components, config)?;
        *self.cloud_home.write().unwrap() = Some(home);

        Ok(())
    }

    async fn store_initialization(
        &self,
    ) -> Result<crate::sync::cycle::StoreInitialization, SyncError> {
        let Some(expected_store_root) = self.database.local_store_root_ref().await? else {
            return Ok(crate::sync::cycle::StoreInitialization::CreateStore);
        };
        Ok(crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root,
        })
    }

    /// Test-only: stand the sync loop over an injected `home` while resolving the
    /// at-rest cipher from custody exactly as [`start_sync`](Self::start_sync)
    /// does — the counterpart of [`start_sync_with_home`](Self::start_sync_with_home)
    /// for proving the established master key is the one sealing traffic. Unlike
    /// that method, which takes the cipher explicitly and never consults custody,
    /// this drives the real [`resolve_cipher`](Self::resolve_cipher) path: an
    /// opaque home with no key established fails [`SyncError::MasterKeyNotEstablished`]
    /// before the loop starts.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn start_sync_with_test_home_custody(
        &self,
        home: std::sync::Arc<dyn CloudHome>,
    ) -> Result<(), SyncError> {
        let storage = (self.config_provider)().cloud_home.storage;
        let cipher = self.resolve_cipher(storage)?;
        self.start_sync_with_home(home, cipher).await
    }

    /// Tear down the sync loop and cloud home.
    pub(crate) fn stop_sync(&self) -> Result<(), SyncError> {
        let stop_result = self.stop_current_loop();
        *self.sync_loop_handle.write().unwrap() = None;
        *self.cloud_home.write().unwrap() = None;

        match stop_result {
            Ok(()) => {
                info!("Sync loop stopped");
                Ok(())
            }
            Err(error) => {
                error!("Sync loop stop failed: {error}");
                Err(error)
            }
        }
    }

    fn stop_current_loop(&self) -> Result<(), SyncError> {
        let handle = self.sync_loop_handle.write().unwrap().take();
        if let Some(handle) = handle {
            handle.stop().map_err(SyncError::Loop)?;
        }
        Ok(())
    }

    fn stop_current_connection(&self) -> Result<(), SyncError> {
        let stop_result = self.stop_current_loop();
        *self.cloud_home.write().unwrap() = None;
        stop_result
    }

    // =========================================================================
    // Status / config queries
    // =========================================================================

    pub(crate) fn is_sync_ready(&self) -> bool {
        self.sync_loop_handle
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.is_running())
    }

    pub(crate) fn trigger_sync(&self) {
        if let Some(ref sync_loop) = *self.sync_loop_handle.read().unwrap() {
            sync_loop.trigger();
        }
    }

    // =========================================================================
    // Blob locality transitions (make_remote / make_local / cancel_make_remote)
    // =========================================================================

    /// Make `(root_table, root_id)` Remote (Local → Remote): enqueue an upload per
    /// user-provided blob from its external file and record the make_remote intent,
    /// then return. The drain uploads each and flips the gate true on the last (see
    /// [`crate::blob::transition::make_remote`]); the gate flip re-emits the subtree,
    /// the cycle's inline push uploads the root's host-provided blobs, and
    /// `on_root_made_remote` fires. `pin` keeps the uploaded blobs in coven's cache
    /// as pinned (offline) copies.
    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        if !self.is_sync_ready() {
            return Err(MakeRemoteError::SyncNotReady);
        }
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(MakeRemoteError::SyncNotReady)?;
        transition::make_remote(
            &self.database,
            sync_loop.store_dir(),
            sync_loop.hlc(),
            root_table,
            root_id,
            pin,
        )
        .await?;
        self.trigger_sync();
        Ok(())
    }

    /// Cancel an in-flight make_remote of `(root_table, root_id)`: clear its intent
    /// and pending uploads and tombstone any blob that already landed. The gate never
    /// flips, so the root stays Local.
    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        if !self.is_sync_ready() {
            return Err(MakeRemoteError::SyncNotReady);
        }
        transition::cancel_make_remote(self.db(), root_table, root_id).await?;
        self.trigger_sync();
        Ok(())
    }

    /// Make `(root_table, root_id)` Local (Remote → Local): bring each blob back to a
    /// local file durability-first — a user-provided blob to the path named in `dest`
    /// (blob id → destination path), a host-provided blob to coven's local store (no
    /// dest) — then flip the gate false, register the user-provided external refs,
    /// and enqueue the cloud deletes in one atomic commit. Awaitable; `cancel` aborts
    /// before the commit (the root stays Remote). `dest` carries user-provided ids
    /// only. Per-blob materialize progress and the completion event reach the
    /// observer this manager was built with.
    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<(), MakeLocalError> {
        if !self.is_sync_ready() {
            return Err(MakeLocalError::SyncNotReady);
        }
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(MakeLocalError::SyncNotReady)?;
        let storage: &dyn SyncStorage = &**sync_loop.storage();
        transition::make_local(
            &self.database,
            storage,
            sync_loop.store_dir(),
            sync_loop.hlc(),
            routing_encryption,
            self.observer.as_deref(),
            root_table,
            root_id,
            dest,
            cancel,
        )
        .await?;
        self.trigger_sync();
        Ok(())
    }

    // =========================================================================
    // Keys / codes
    // =========================================================================

    // =========================================================================
    // Membership
    // =========================================================================

    pub(crate) async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        let active_loop = self.sync_loop_handle();
        let config = active_loop
            .as_ref()
            .map(|handle| handle.config().clone())
            .unwrap_or_else(|| (self.config_provider)());
        if active_loop.is_none() && config.cloud_home.provider.is_none() {
            info!("get_members: sync not configured; returning no members");
            return Ok(Vec::new());
        }
        let storage = self
            .storage_for_command(&config, active_loop.as_ref())
            .await?;

        let user_pubkey = crate::keys::identity_public_key(self.identity_custody.as_ref())?;
        Store::load(self.database.clone(), storage)
            .await?
            .members(user_pubkey.as_ref().map(|key| key.as_slice()))
            .await
            .map_err(SyncError::from)
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, SyncError> {
        let active_loop = self.sync_loop_handle();
        let config = active_loop
            .as_ref()
            .map(|handle| handle.config().clone())
            .unwrap_or_else(|| (self.config_provider)());
        if active_loop.is_none() && config.cloud_home.provider.is_none() {
            return Err(SyncError::NotConfigured);
        }
        let storage = self
            .storage_for_command(&config, active_loop.as_ref())
            .await?;
        let user_pubkey = crate::keys::identity_public_key(self.identity_custody.as_ref())?;
        Store::load(self.database.clone(), storage)
            .await?
            .membership_conflict(user_pubkey.as_ref().map(|key| key.as_slice()))
            .await
            .map_err(SyncError::from)
    }

    /// Build a restore code for this store: fetch the current membership-head
    /// floor from the cloud and mint the code from it, so the restorer can seed
    /// its watermark from mint-time
    /// state rather than accepting any signed head as a fresh device would
    /// otherwise have to. Requires a connected provider — unlike the old,
    /// storage-free `generate_restore_code`, minting a trustworthy floor is a
    /// network read, not a pure function of local config and keyring state.
    pub(crate) async fn generate_restore_code(&self) -> Result<String, SyncError> {
        let active_loop = self.sync_loop_handle();
        let config = active_loop
            .as_ref()
            .map(|handle| handle.config().clone())
            .unwrap_or_else(|| (self.config_provider)());
        if active_loop.is_none() && config.cloud_home.provider.is_none() {
            return Err(SyncError::NotConfigured);
        }
        let storage = self
            .storage_for_command(&config, active_loop.as_ref())
            .await?;

        let restore_membership = Store::load(self.database.clone(), storage)
            .await?
            .restore_membership()
            .await
            .map_err(SyncError::from)?;
        let identity = crate::keys::require_identity(self.identity_custody.as_ref())?;
        let authority = crate::sync::restore_code::RestoreAuthority::ActivatedContinuation(
            self.database
                .export_activated_device_continuation(&identity)
                .await?,
        );

        crate::storage::cloud::setup::generate_restore_code(
            &config,
            &self.key_service,
            self.custody.as_ref(),
            restore_membership.store_root,
            restore_membership.founder_pubkey,
            restore_membership.membership_floor,
            authority,
        )
        .map_err(SyncError::from)
    }

    pub(crate) fn invite_member<'a>(
        &'a self,
        public_key_hex: &'a str,
        invitee_email: Option<&'a str>,
        role: MemberRole,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, SyncError>> + Send + 'a>>
    {
        Box::pin(async move {
            // Serialize with any other key-minting/rotating member op on this device.
            let _member_ops = self.member_ops_lock.lock().await;

            let sync_loop = self
                .sync_loop_handle
                .read()
                .unwrap()
                .clone()
                .ok_or(SyncError::LoopNotRunning)?;

            // Inviting a member wraps the store key to them, which only an encrypted
            // home has. Refuse before touching the membership chain.
            if sync_loop.current_encryption().is_none() {
                return Err(SyncError::NotEncryptedHome);
            }
            let store_name = sync_loop.config().store_name.clone();
            let invite_code = sync_loop
                .invite_member(public_key_hex, invitee_email, role, &store_name)
                .await
                .map_err(SyncError::from)?;

            Ok(crate::join_code::encode(&invite_code))
        })
    }

    pub(crate) fn remove_member<'a>(
        &'a self,
        public_key_hex: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, SyncError>> + Send + 'a>>
    {
        Box::pin(async move {
            // Serialize with any other key-minting/rotating member op on this device,
            // so a second removal builds on this one's committed state rather than
            // cloning the same base cipher and preparing competing rotations.
            let _member_ops = self.member_ops_lock.lock().await;

            let sync_loop = self
                .sync_loop_handle
                .read()
                .unwrap()
                .clone()
                .ok_or(SyncError::LoopNotRunning)?;

            // Removing a member rotates the store key, which only an encrypted home
            // has. Refuse up front so a plaintext home never mutates the membership
            // chain or re-wraps keys before the rotation fails.
            if sync_loop.current_encryption().is_none() {
                return Err(SyncError::NotEncryptedHome);
            }

            // Removing a member commits the cloud key rotation and then adopts the
            // rotated key into this device's keyring and live cipher. The host records
            // the returned fingerprint and that a key is stored in its own config; an
            // adoption failure surfaces as its own membership variant naming the
            // half-applied state and its remedies — and, structurally, this device
            // seals nothing new for the cloud until one of those remedies adopts it
            // (`pending_rotation`, shared with the sync loop this same store runs).
            let outcome = sync_loop.remove_member(public_key_hex).await;

            let fingerprint = outcome.map_err(SyncError::from)?;
            Ok(fingerprint)
        })
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), SyncError> {
        let _member_ops = self.member_ops_lock.lock().await;
        let sync_loop = self.sync_loop_handle().ok_or(SyncError::LoopNotRunning)?;
        sync_loop
            .resolve_membership_conflict(choice)
            .await
            .map_err(SyncError::from)
    }

    /// The local identity's pubkey and the current active Store member set — the
    /// inputs the Circle read queries share.
    async fn circle_query_inputs(
        &self,
    ) -> Result<(String, std::collections::BTreeSet<String>), crate::CircleError> {
        let identity = crate::keys::require_identity(self.identity_custody.as_ref())
            .map_err(|error| crate::CircleError::Identity(error.to_string()))?;
        let identity_pubkey = crate::keys::public_key_hex(&identity);
        let store_members = self
            .get_members()
            .await
            .map_err(crate::CircleError::from)?
            .into_iter()
            .map(|member| member.pubkey)
            .collect();
        Ok((identity_pubkey, store_members))
    }

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::CircleId, crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .create_circle(name)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .rename_circle(circle_id, name)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn add_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .add_circle_member(circle_id, member_pubkey, role)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .remove_circle_member(circle_id, member_pubkey)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .resolve_circle_control(circle_id, chosen)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .cancel_circle_epoch_close(circle_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: crate::CircleId,
        excluded_device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .exclude_circle_close_device(circle_id, excluded_device_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .delete_circle(circle_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .retry_circle_operation(operation_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .discard_circle_operation(operation_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn circle_close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::CircleError> {
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(crate::CircleError::LoopNotRunning)?;
        sync_loop
            .circle_close_status(circle_id)
            .await
            .map_err(crate::CircleError::from)
    }

    pub(crate) async fn list_circles(&self) -> Result<Vec<crate::Circle>, crate::CircleError> {
        let (identity_pubkey, store_members) = self.circle_query_inputs().await?;
        self.database
            .circle_states(&identity_pubkey, store_members)
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }

    pub(crate) async fn circle_members(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<Vec<crate::CircleMemberInfo>, crate::CircleError> {
        let (identity_pubkey, store_members) = self.circle_query_inputs().await?;
        self.database
            .get_circle_members(circle_id, &identity_pubkey, store_members)
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }

    pub(crate) async fn circle_operations(
        &self,
    ) -> Result<Vec<crate::CircleOperationInfo>, crate::CircleError> {
        self.database
            .get_circle_operations()
            .await
            .map_err(|error| crate::CircleError::Protocol(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::clock::SystemClock;
    use crate::config::CloudProvider;
    use crate::coven::StoreOpenGuard;
    use crate::encryption::MasterKeyring;
    use crate::keys::{test_keyring, KeyError, StoreKeys};
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::CloudHomeJoinInfo;
    use crate::store_dir::StoreDir;
    use std::sync::Arc;

    struct NoImmutableCopyHome;

    #[async_trait::async_trait]
    impl CloudHome for NoImmutableCopyHome {
        async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn open_multipart<'a>(
            &'a self,
            _key: &str,
            _total_len: u64,
        ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        fn multipart_threshold(&self) -> u64 {
            panic!("incapable home must be rejected before I/O")
        }

        async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn read_range(
            &self,
            _key: &str,
            _start: u64,
            _end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }

        async fn set_access(
            &self,
            _desired: crate::storage::cloud::CloudAccessState,
        ) -> Result<crate::storage::cloud::CloudAccessOutcome, CloudHomeError> {
            panic!("incapable home must be rejected before I/O")
        }
    }

    /// A custody that never has a master key established — `unlock` always
    /// returns `None`. For tests exercising a locked/unestablished store, or
    /// a browsable home where custody is never consulted at all.
    struct NoKeyCustody;

    impl MasterKeyCustody for NoKeyCustody {
        fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
            Ok(None)
        }
        fn persist(&self, _keyring: &MasterKeyring) -> Result<(), KeyError> {
            Ok(())
        }
        fn forget(&self) -> Result<(), KeyError> {
            Ok(())
        }
    }

    /// The identity sibling of [`NoKeyCustody`]: `unlock` always returns
    /// `None`, for tests exercising a store with no identity established.
    struct NoIdentityCustody;

    impl DeviceIdentityCustody for NoIdentityCustody {
        fn unlock(&self) -> Result<Option<crate::keys::UserKeypair>, KeyError> {
            Ok(None)
        }
        fn persist(&self, _keypair: &crate::keys::UserKeypair) -> Result<(), KeyError> {
            Ok(())
        }
        fn forget(&self) -> Result<(), KeyError> {
            Ok(())
        }
    }

    /// A ready-to-use, already-established identity custody for tests whose
    /// focus is elsewhere (blob transitions, membership, restore-code
    /// generation) — seeded in-memory so it needs no keyring registration.
    fn established_identity_custody() -> Arc<dyn DeviceIdentityCustody> {
        crate::identity_custody::IdentityCustody::InMemory(crate::keys::UserKeypair::generate())
            .resolve("unused-store-id", &StoreDir::new("unused-store-dir"))
    }

    async fn start_sync_with_home_in_its_own_task(
        manager: Arc<SyncManager>,
        home: Arc<dyn CloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        tokio::spawn(async move { manager.start_sync_with_home(home, cipher).await })
            .await
            .expect("join injected-home startup task")
    }

    #[tokio::test]
    async fn get_members_surfaces_malformed_cloud_credentials() {
        test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path());
        let store_id = "sync-enabled-malformed-credentials";
        let key_service = StoreKeys::new(store_id.to_string());
        key_service
            .cloud_home_credentials_entry_for_test()
            .expect("create credentials entry")
            .set_password("{")
            .expect("write malformed credentials");
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: None,
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };
        let config = crate::sync::join::build_config(
            store_id,
            "device",
            &store_dir,
            "store",
            &join_info,
            &CloudCipher::Plaintext,
        );
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            key_service,
            Arc::new(NoKeyCustody),
            established_identity_custody(),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        );

        let error = match manager.get_members().await {
            Ok(_) => panic!("malformed stored credentials must fail"),
            Err(error) => error,
        };
        // The typed CloudHomeError survives up through StorageSetup to the public
        // SyncError surface — not flattened into a string — so its retryability
        // verdict is still readable: malformed credentials are a configuration
        // fault the user must fix, not a transient retry.
        let SyncError::StorageSetup(StorageSetupError::CloudHome(cloud_home_error)) = &error else {
            panic!("expected StorageSetup(CloudHome(_)), got {error:?}");
        };
        assert!(matches!(cloud_home_error, CloudHomeError::Configuration(_)));
        assert!(!cloud_home_error.is_retryable());
        assert!(error
            .to_string()
            .contains("malformed cloud home credentials JSON"));
    }

    #[tokio::test]
    async fn start_sync_rejects_an_opaque_home_without_a_master_key() {
        test_keyring::install();
        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let mut config = Config::with_defaults(
            "lib-opaque-no-encryption".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        );
        // Opaque storage (the default) with a configured provider but no
        // established master key is a locked-store contradiction — custody's
        // `unlock` returns `None`.
        config.cloud_home.provider = Some(CloudProvider::S3);
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new("lib-opaque-no-encryption".to_string()),
            Arc::new(NoKeyCustody),
            established_identity_custody(),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        );

        let error = manager
            .start_sync()
            .await
            .expect_err("opaque home without an established master key must fail");
        assert!(
            matches!(error, SyncError::MasterKeyNotEstablished),
            "expected MasterKeyNotEstablished, got {error:?}"
        );
    }

    #[tokio::test]
    async fn immutable_copy_admission_refuses_before_stopping_the_active_loop() {
        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let config = Arc::new(RwLock::new(Config::with_defaults(
            "immutable-admission-before-stop".to_string(),
            "test-device".to_string(),
            store_dir,
            "Blob Store".to_string(),
        )));
        let db = crate::sync::test_helpers::open_test_db_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::HostProvided,
            crate::CacheFill::CacheLazy,
        ));
        let manager = Arc::new(SyncManager::new(
            {
                let config = config.clone();
                Arc::new(move || config.read().expect("read config").clone())
            },
            StoreKeys::new("immutable-admission-before-stop".to_string()),
            Arc::new(NoKeyCustody),
            established_identity_custody(),
            db,
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        ));
        start_sync_with_home_in_its_own_task(
            manager.clone(),
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Plaintext,
        )
        .await
        .expect("install active loop");
        let active_loop = manager.sync_loop_handle().expect("active loop");

        {
            let mut config = config.write().expect("write config");
            config.cloud_home.provider = Some(CloudProvider::S3);
            config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
            config.cloud_home.s3_exact_slots = None;
        }
        let error = manager
            .start_sync()
            .await
            .expect_err("unsupported immutable-copy provider is refused");

        assert!(matches!(
            error,
            SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
                provider: CloudProvider::S3,
            })
        ));
        assert!(active_loop.is_running());
        assert!(manager.cloud_home().is_some());

        let error = start_sync_with_home_in_its_own_task(
            manager.clone(),
            Arc::new(NoImmutableCopyHome),
            CloudCipher::Plaintext,
        )
        .await
        .expect_err("injected home without immutable-copy storage is refused");
        assert!(matches!(
            error,
            SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
                provider: CloudProvider::S3,
            })
        ));
        assert!(active_loop.is_running());
        assert!(manager.cloud_home().is_some());
    }

    #[tokio::test]
    async fn start_sync_with_home_stops_the_previous_loop_before_replacement() {
        test_keyring::install();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let config = Config::with_defaults(
            "lib-manager-restart".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        );
        let manager = Arc::new(SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new("lib-manager-restart".to_string()),
            Arc::new(NoKeyCustody),
            established_identity_custody(),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        ));

        let home = Arc::new(InMemoryCloudHome::new());
        start_sync_with_home_in_its_own_task(manager.clone(), home.clone(), CloudCipher::Plaintext)
            .await
            .expect("first test home starts");
        let first_loop = manager
            .sync_loop_handle()
            .expect("first loop handle installed");
        assert!(first_loop.is_running(), "first loop starts running");

        start_sync_with_home_in_its_own_task(manager.clone(), home, CloudCipher::Plaintext)
            .await
            .expect("replacement test home starts");
        let replacement_loop = manager
            .sync_loop_handle()
            .expect("replacement loop handle installed");

        assert!(
            !first_loop.is_running(),
            "starting sync again stops the previous loop before replacement",
        );
        assert!(
            replacement_loop.is_running(),
            "replacement loop remains running",
        );
    }

    #[tokio::test]
    async fn failed_restart_leaves_no_stale_cloud_home() {
        test_keyring::install();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let config = Arc::new(RwLock::new(Config::with_defaults(
            "lib-manager-failed-restart".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        )));
        let manager = Arc::new(SyncManager::new(
            {
                let config = config.clone();
                Arc::new(move || config.read().unwrap().clone())
            },
            StoreKeys::new("lib-manager-failed-restart".to_string()),
            // An established master key so the opaque default storage passes
            // the cipher precondition and the restart fails at the home build
            // itself.
            crate::custody::KeyCustody::InMemory(MasterKeyring::generate()).resolve(
                "lib-manager-failed-restart",
                &StoreDir::new("unused-store-dir"),
            ),
            established_identity_custody(),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        ));

        start_sync_with_home_in_its_own_task(
            manager.clone(),
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Plaintext,
        )
        .await
        .expect("injected home starts");
        assert!(manager.cloud_home().is_some(), "injected home is installed");

        config.write().unwrap().cloud_home.provider = Some(CloudProvider::S3);
        let error = manager
            .start_sync()
            .await
            .expect_err("invalid configured provider fails restart");
        assert!(
            error.to_string().contains("failed to build cloud home"),
            "restart failure surfaces the provider setup error: {error}",
        );
        assert!(
            manager.sync_loop_handle().is_none(),
            "failed restart leaves no loop installed",
        );
        assert!(
            manager.cloud_home().is_none(),
            "failed restart must not leave the previous cloud home installed",
        );
    }

    /// The `NoDeviceIdentity` sibling of
    /// `start_sync_rejects_an_opaque_home_without_a_master_key`: connecting
    /// with a configured home but no device identity established must fail
    /// typed, with nothing installed — never silently mint one. Browsable
    /// storage so the master-key precondition is out of the way and this
    /// isolates the identity check.
    #[tokio::test]
    async fn start_sync_rejects_a_connect_with_no_device_identity_established() {
        test_keyring::install();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let store_id = "lib-no-device-identity".to_string();
        let key_service = StoreKeys::new(store_id.clone());
        key_service
            .set_cloud_home_credentials(&crate::keys::CloudHomeCredentials::S3 {
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
            })
            .expect("seed S3 credentials");

        let mut config = Config::with_defaults(
            store_id.clone(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.storage = HomeStorage::Browsable;
        config.cloud_home.s3_bucket = Some("bucket".to_string());
        config.cloud_home.s3_region = Some("us-east-1".to_string());

        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            key_service,
            Arc::new(NoKeyCustody),
            Arc::new(NoIdentityCustody),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        );

        let error = manager
            .start_sync()
            .await
            .expect_err("no device identity established must fail the connect");
        assert!(
            matches!(error, SyncError::Key(KeyError::NoDeviceIdentity)),
            "got {error:?}"
        );
        assert!(
            manager.sync_loop_handle().is_none(),
            "a failed connect installs no loop",
        );
        assert!(
            manager.cloud_home().is_none(),
            "a failed connect installs no cloud home",
        );
    }

    #[tokio::test]
    async fn browsable_test_home_with_a_foreign_founder_installs_nothing() {
        test_keyring::install();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let store_id = "lib-foreign-browsable-founder";
        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Browsable;
        let home = Arc::new(InMemoryCloudHome::new());
        let attacker = crate::keys::UserKeypair::generate();
        let attacker_storage = CloudSyncStorage::new(
            home.clone(),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            store_id,
            attacker.clone(),
        )
        .expect("build attacker storage");
        let attacker_db = crate::sync::test_helpers::open_test_db();
        crate::sync::test_helpers::create_exact_test_store(
            &attacker_db,
            &attacker_storage,
            store_id,
            &attacker,
        )
        .await
        .expect("publish attacker Store root");

        let victim = crate::keys::UserKeypair::generate();
        let db = crate::sync::test_helpers::open_test_db();
        let manager = Arc::new(SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new(store_id.to_string()),
            Arc::new(NoKeyCustody),
            crate::identity_custody::IdentityCustody::InMemory(victim)
                .resolve(store_id, &store_dir),
            db.clone(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        ));

        let error =
            start_sync_with_home_in_its_own_task(manager.clone(), home, CloudCipher::Plaintext)
                .await
                .expect_err("foreign founder must prevent sync startup");
        assert!(
            matches!(error, SyncError::Init(InitSyncError::StoreProtocolRoot(_))),
            "unexpected startup error: {error:?}",
        );
        assert!(manager.sync_loop_handle().is_none());
        assert!(manager.cloud_home().is_none());
        assert_eq!(
            db.get_protocol_state(crate::sync::store::OWNER_PUBKEY_STATE_KEY)
                .await
                .unwrap(),
            None,
        );
    }

    /// Key material a connect resolves from custody is never cached across
    /// connects — `start_sync` re-derives the cipher fresh every call via
    /// `resolve_cipher`, this manager's single
    /// custody→cipher decision. Persists key A, resolves, swaps what the SAME
    /// custody instance serves to key B (a rotation outside any manager call
    /// — the way a host's own key-rotation flow would), and resolves again:
    /// the second resolution reflects B, not a value cached from the first.
    ///
    /// This drives `resolve_cipher` directly rather than a full
    /// connect/disconnect/reconnect through an opaque home: an opaque store's
    /// membership chain is founded and pinned to the local device on first
    /// connect, so swapping its master key outright (rather than through the
    /// real in-place rotation `remove_member` performs, which also re-wraps
    /// existing membership content) would desync a live home — an unrelated
    /// concern to what this test pins. `resolve_cipher` is the exact
    /// mechanism `start_sync` and the custody-resolving test-home connect
    /// path share, so calling it twice with custody mutated in between is the
    /// real unit behind "reconnect uses new material," without wading into
    /// membership bootstrap.
    #[test]
    fn resolve_cipher_never_caches_reflects_whatever_custody_now_serves() {
        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let store_id = "lib-resolve-cipher-fresh";
        let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir);
        let key_a = MasterKeyring::generate();
        custody.persist(&key_a).expect("establish key A");

        let config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new(store_id.to_string()),
            custody.clone(),
            established_identity_custody(),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            crate::sync::cloud_storage::BlobChunking::DEFAULT,
        );

        let fingerprint_a = match manager
            .resolve_cipher(crate::config::HomeStorage::Opaque)
            .expect("resolve the cipher custody serves for key A")
        {
            CloudCipher::Encrypted(enc) => enc.fingerprint(),
            CloudCipher::Plaintext => panic!("opaque storage must resolve an encrypted cipher"),
        };
        assert_eq!(fingerprint_a, key_a.fingerprint());

        // Swap what the SAME custody instance serves — outside any manager
        // call, the way a host's own key-rotation flow would.
        let key_b = MasterKeyring::generate();
        custody
            .persist(&key_b)
            .expect("rotate custody's served key to B");

        let fingerprint_b = match manager
            .resolve_cipher(crate::config::HomeStorage::Opaque)
            .expect("resolve the cipher custody serves for key B")
        {
            CloudCipher::Encrypted(enc) => enc.fingerprint(),
            CloudCipher::Plaintext => panic!("opaque storage must resolve an encrypted cipher"),
        };
        assert_eq!(
            fingerprint_b,
            key_b.fingerprint(),
            "the second resolution must reflect key B, not a value cached from the first call",
        );
        assert_ne!(
            fingerprint_a, fingerprint_b,
            "the two resolutions must differ — custody actually served different material",
        );
    }
}

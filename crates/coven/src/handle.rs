//! The native data handle: one object a host constructs once that owns coven's
//! pieces and exposes the whole data interface as methods.
//!
//! coven owns the store's data — SQL rows and blobs, on disk first, cloud
//! optional. A host (a desktop/mobile app) talks to coven through this one
//! handle and never assembles coven's internals by hand or hands them back to
//! coven on every call. The handle holds the [`Database`], the [`StoreDir`],
//! the keys, and — once a cloud provider is connected — the [`SyncManager`]; the
//! caller passes only descriptors (a [`BlobRef`], SQL, a config) and coven does
//! its own plumbing.
//!
//! It is the native counterpart of the browser `CovenStore` in `coven-wasm`:
//! same role, different substrate. The native stack runs on tokio with a
//! [`SyncManager`], `Send + Sync` throughout.
//!
//! ## What it owns
//!
//! - **Rows** — the [`Database`] (coven already owns the connection). The host
//!   runs its app SQL through [`sql`](CovenHandle::sql) and row+blob batches
//!   through [`write`](CovenHandle::write).
//! - **Blobs** — the [`StoreDir`] the blob engine reads/writes, plus the
//!   credentials to build a read [`SyncStorage`] on a cloud miss. Read, ranged
//!   read, store, register external, pin/unpin, the locality transitions, and the
//!   upload drain are methods here.
//! - **Sync** — built lazily by [`connect_sync`](CovenHandle::connect_sync) when a
//!   cloud provider is connected. A store with no cloud home never builds a
//!   [`SyncManager`] and only ever holds Local blobs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::blob::cache::BlobCacheError;
use crate::blob::transition::{MakeLocalError, MakeRemoteError};
use crate::blob::upload::DrainOutcome;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::coven::StoreOpenGuard;
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring, SealError};
use crate::keys::{DeviceKeys, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys};
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;
#[cfg(any(test, feature = "test-utils"))]
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::membership::MemberRole;
use crate::sync::storage::{StorageError, SyncStorage};
use crate::sync::sync_loop::SyncLoopStatus;
use crate::sync::sync_manager::MemberInfo;
use crate::sync::sync_manager::{ConfigProvider, SyncError, SyncManager};

/// A Remote blob read needs sync storage; if building it from config fails
/// (missing credentials or cloud configuration) the read surfaces that as a
/// configuration fault, not a disk I/O error. `BlobCacheError` lives in
/// `coven-core` and cannot name `coven`'s `StorageSetupError`, so the typed error
/// is rendered to its message at this crate boundary.
impl From<crate::storage::cloud::setup::StorageSetupError> for BlobCacheError {
    fn from(e: crate::storage::cloud::setup::StorageSetupError) -> Self {
        BlobCacheError::StorageSetup(e.to_string())
    }
}

/// The cipher a store's app-data sealing runs under, resolved from `custody`.
///
/// A store whose custody unlocks `None` has no key to seal under or open with,
/// which is [`SealError::Locked`] — the same discipline the sync engine's cipher
/// resolution keeps, where an opaque home with no established key refuses to
/// start rather than inventing one.
///
/// Shared by [`CovenHandle`] and [`CovenReadHandle`](crate::CovenReadHandle) so
/// both resolve the identical keyring the identical way; a payload one seals, the
/// other opens.
pub(crate) fn app_data_cipher(
    custody: &dyn MasterKeyCustody,
) -> Result<EncryptionService, SealError> {
    let keyring = custody.unlock()?.ok_or(SealError::Locked)?;
    Ok(EncryptionService::from(keyring))
}

/// The native handle over one coven store.
///
/// Open it once with [`Coven::builder`](crate::Coven::builder), then call methods. Cheap to
/// [`clone`](Clone) — every field is shared (an `Arc`, a `Clone` handle, or a
/// reference-counted lock), so a clone drives the same database, sync manager,
/// and storage as the original.
///
/// # Using the handle
///
/// The host builds the handle once at startup and then only calls methods on it
/// — it never assembles coven's internals by hand or hands them back to coven on
/// every call. Rows go through the connection coven owns; blobs go through the
/// handle's read/store methods; sync is optional.
///
/// ```no_run
/// # use coven::{BlobRef, CovenHandle};
/// # async fn use_store(handle: &CovenHandle, cover: &BlobRef)
/// #     -> Result<(), Box<dyn std::error::Error>> {
/// // Rows: run app SQL on the connection coven owns.
/// let note_count: i64 = handle
///     .sql(|sql| {
///         sql.tx()
///             .query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
///             .map_err(coven::CovenError::from)
///     })
///     .await?;
///
/// // Blobs: read by descriptor. coven resolves locality — the user's own file,
/// // its local store, the cache, or a cloud fetch — and hands back plaintext.
/// let bytes: Vec<u8> = handle.read_blob(cover).await?;
///
/// // Sync is optional. Connect a provider, then drive it; a store with no
/// // cloud home never calls these and stays fully usable on-device.
/// handle.connect_sync().await?;
/// handle.sync_now();
/// # let _ = note_count;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CovenHandle {
    db: Database,

    /// A read-only companion connection on the same WAL database, opened at
    /// [`open`](crate::CovenBuilder::open) after the writer's migrations completed.
    /// Backs [`sql_read`](Self::sql_read): a pure read runs here on its own
    /// connection thread, concurrent with the writer's thread rather than queued
    /// behind it, and attaches no changeset session. `Database` is `Clone` (clones
    /// share one connection thread), so every [`CovenHandle`] clone shares this one
    /// reader — many readers coexist with the single writer under WAL, each seeing
    /// the last committed state.
    read_db: Database,
    stamper: crate::sync::hlc::UpdatedAtStamper,
    store_dir: StoreDir,

    /// Supplies the host's current config on demand. coven reads it fresh each
    /// call so a host with reactive config sees changes without rebuilding the
    /// handle. The same provider the [`SyncManager`] reads from.
    config_provider: ConfigProvider,
    key_service: StoreKeys,

    /// The store's master-key custody, resolved once at
    /// [`open`](crate::CovenBuilder::open) from the builder's
    /// [`KeyCustody`](crate::KeyCustody) selection. Every master-key read and
    /// write in the handle and the sync engine goes through this — coven never
    /// touches a crypto type directly.
    key_custody: Arc<dyn MasterKeyCustody>,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,

    /// Host bookkeeping for blob transitions (upload progress, materialize
    /// progress, completion). Passed to the [`SyncManager`] and to the upload
    /// drain. `None` for a host that doesn't surface transition progress.
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// Holds the native store-directory lock for this handle and every clone,
    /// and is cloned into each [`SyncManager`] so a running sync loop keeps the
    /// lock alive until its own thread exits — the lock's lifetime tracks the
    /// last writer, not the host's drop timing.
    open_guard: Arc<StoreOpenGuard>,

    /// Built lazily by [`connect_sync`](Self::connect_sync) when a provider is
    /// connected; `None` for a home-less, all-Local store. Shared behind a lock
    /// so a connect/disconnect mutates it in place without rebuilding the handle.
    sync: Arc<RwLock<Option<Arc<SyncManager>>>>,

    /// Serializes async lifecycle replacement so concurrent connects/restarts
    /// cannot each start a loop and race to install the survivor.
    sync_lifecycle: Arc<tokio::sync::Mutex<()>>,

    /// The sync-status broadcast this handle owns. Every [`SyncManager`] it builds
    /// clones this sender into its sync loop, so a
    /// [`subscribe_sync_status`](Self::subscribe_sync_status) receiver keeps
    /// receiving across a reconnect — which drops the old manager and loop and
    /// builds new ones, but reuses this same channel. A subscription created
    /// before any provider is connected is valid and starts receiving once a loop
    /// runs.
    sync_status_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
}

impl CovenHandle {
    /// Build the handle over an already-open [`Database`] and the store's
    /// directory. Does no I/O and builds no sync manager — a home-less store is
    /// fully usable (rows + Local blobs) without one. Call
    /// [`connect_sync`](Self::connect_sync) when a cloud provider is connected.
    ///
    /// `config_provider` is read fresh on every call that needs the current
    /// config (the cloud-home selection, the blob-path scheme), so the host can
    /// reconnect a provider without rebuilding the handle. `observer` carries the
    /// host's transition bookkeeping; pass `None` if it surfaces none.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db: Database,
        read_db: Database,
        stamper: crate::sync::hlc::UpdatedAtStamper,
        store_dir: StoreDir,
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        key_custody: Arc<dyn MasterKeyCustody>,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
    ) -> Self {
        Self {
            db,
            read_db,
            stamper,
            store_dir,
            config_provider,
            key_service,
            key_custody,
            clock,
            cloudkit_ops,
            observer,
            open_guard,
            sync: Arc::new(RwLock::new(None)),
            sync_lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            // Capacity 16: a subscriber more than 16 statuses behind observes a
            // lag error and misses the gap. Documented on `SyncLoopStatus` — its
            // `row_changes` is a refresh hint, so a lagged subscriber re-reads by
            // primary key rather than relying on a complete stream.
            sync_status_tx: tokio::sync::broadcast::channel(16).0,
        }
    }

    fn config(&self) -> Config {
        (self.config_provider)()
    }

    // =========================================================================
    // Rows
    // =========================================================================

    /// The owned [`Database`]. Public row access goes through
    /// [`CovenHandle::sql`] and [`CovenHandle::write`]; coven internals use this
    /// to reach row-level helpers.
    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    /// The read-only companion [`Database`] backing [`sql_read`](Self::sql_read).
    /// A pure read runs against this connection, concurrent with the writer.
    pub(crate) fn read_db(&self) -> &Database {
        &self.read_db
    }

    pub(crate) fn stamper(&self) -> crate::sync::hlc::UpdatedAtStamper {
        self.stamper.clone()
    }

    pub(crate) fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// The connected [`SyncManager`], or `None` for a home-less store or one
    /// whose provider has not been connected yet. The host reaches sync-engine
    /// operations not surfaced as handle methods (membership, invite/remove,
    /// status) through this.
    pub(crate) fn sync_manager(&self) -> Option<Arc<SyncManager>> {
        self.sync.read().unwrap().clone()
    }

    /// Subscribe to the sync loop's [`SyncLoopStatus`] stream. The channel is
    /// owned by this handle, not the loop, so the receiver keeps working across a
    /// reconnect and may be created before any provider is connected (it starts
    /// receiving once a loop runs). Infallible for that reason — there is no loop
    /// state to check.
    ///
    /// Delivery is a bounded broadcast (capacity 16): a receiver that falls more
    /// than 16 statuses behind observes a lag error and misses the gap. Since a
    /// `Succeeded` status's `row_changes` is a refresh hint, a lagged subscriber
    /// re-reads the affected rows by primary key rather than treating the stream as
    /// complete.
    pub fn subscribe_sync_status(&self) -> tokio::sync::broadcast::Receiver<SyncLoopStatus> {
        self.sync_status_tx.subscribe()
    }

    /// Build the [`SyncManager`] for a connected cloud provider, start its sync
    /// loop, and install it. Returns the started manager, or an error if the cloud
    /// home fails to build — in which case nothing is installed, so the handle
    /// never holds a manager that reports success with nothing started.
    ///
    /// The at-rest cipher is resolved from the handle's custody per start: an
    /// opaque home unlocks the master keyring (failing with
    /// [`SyncError::MasterKeyNotEstablished`] if none is established), a
    /// browsable one never consults custody. Reconnecting a provider rebuilds
    /// the manager — the [`Database`] keeps the seeded register clock across
    /// the rebuild, so only the cloud home + loop are replaced.
    pub async fn connect_sync(&self) -> Result<(), SyncError> {
        self.build_and_install_sync(self.cloudkit_ops.clone(), |manager| async move {
            manager.start_sync().await
        })
        .await?;
        info!("coven handle: sync manager connected");
        Ok(())
    }

    pub async fn connect_sync_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        self.build_and_install_sync(Some(cloudkit_ops), |manager| async move {
            manager.start_sync().await
        })
        .await?;
        info!("coven handle: sync manager connected with CloudKit driver");
        Ok(())
    }

    /// Build a [`SyncManager`], start its loop via `start`, and install it — the
    /// shared construct-and-install both [`connect_sync`](Self::connect_sync) and
    /// the test-only
    /// [`connect_sync_with_test_home`](Self::connect_sync_with_test_home) run.
    ///
    /// Start before installing: a failed start (the cloud home fails to build, or a
    /// test home's bootstrap fails) returns its error with nothing installed, so the
    /// handle is left home-less rather than holding a manager whose loop never
    /// started.
    async fn build_and_install_sync<F, Fut>(
        &self,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        start: F,
    ) -> Result<Arc<SyncManager>, SyncError>
    where
        F: FnOnce(Arc<SyncManager>) -> Fut,
        Fut: std::future::Future<Output = Result<(), SyncError>>,
    {
        let _lifecycle = self.sync_lifecycle.lock().await;
        let previous = self.sync.write().unwrap().take();
        if let Some(manager) = previous {
            manager.stop_sync()?;
        }

        let manager = Arc::new(SyncManager::new(
            self.config_provider.clone(),
            self.key_service.clone(),
            self.key_custody.clone(),
            self.db.clone(),
            self.clock.clone(),
            cloudkit_ops,
            self.observer.clone(),
            self.open_guard.clone(),
            self.sync_status_tx.clone(),
        ));
        start(manager.clone()).await?;
        *self.sync.write().unwrap() = Some(manager.clone());
        Ok(manager)
    }

    /// Test-only: connect a started [`SyncManager`] over an injected [`CloudHome`]
    /// instead of one built from [`Config`], so a host's integration tests drive
    /// the real make-Remote / make-Local / upload-drain and read paths over a mock
    /// cloud with no live provider.
    ///
    /// The test counterpart of [`connect_sync`](Self::connect_sync): it stands the
    /// manager over `home`/`cipher` through
    /// [`SyncManager::start_sync_with_home`], starts the loop, and installs it with
    /// the same start-before-install discipline — a failed connect leaves the
    /// handle home-less rather than holding a manager whose loop never started.
    /// The injected `cipher` is the at-rest protection directly — the manager's
    /// custody is never consulted on this path.
    ///
    /// The read path needs no separate hook: [`blob_storage`](Self::blob_storage)
    /// serves reads from the connected loop's own [`CloudSyncStorage`], which here
    /// wraps the injected `home`, so [`read_blob`](Self::read_blob) /
    /// [`pin`](Self::pin) resolve a Remote miss against the same test home the
    /// drain writes to.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn connect_sync_with_test_home(
        &self,
        home: Arc<dyn CloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        self.build_and_install_sync(self.cloudkit_ops.clone(), move |manager| async move {
            manager.start_sync_with_home(home, cipher).await
        })
        .await?;
        info!("coven handle: sync manager connected over an injected test cloud home");
        Ok(())
    }

    /// Test-only: connect over an injected [`CloudHome`] while resolving the
    /// at-rest cipher from custody the way production
    /// [`connect_sync`](Self::connect_sync) does, instead of taking an explicit
    /// cipher like [`connect_sync_with_test_home`](Self::connect_sync_with_test_home).
    ///
    /// Where that method injects the cipher and never touches custody, this drives
    /// [`SyncManager::start_sync_with_test_home_custody`], which unlocks the master
    /// keyring through the store's custody exactly as `start_sync` would — so a
    /// test can establish a key, connect over a mock home, and prove the traffic
    /// is sealed under that key. An opaque home with no key established fails
    /// [`SyncError::MasterKeyNotEstablished`] before the loop starts.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn connect_sync_with_test_home_custody(
        &self,
        home: Arc<dyn CloudHome>,
    ) -> Result<(), SyncError> {
        self.build_and_install_sync(self.cloudkit_ops.clone(), move |manager| async move {
            manager.start_sync_with_test_home_custody(home).await
        })
        .await?;
        info!(
            "coven handle: sync manager connected over an injected test cloud home with custody-resolved cipher"
        );
        Ok(())
    }

    /// Start (or restart) the sync loop of the installed [`SyncManager`]. A no-op
    /// when no provider is connected — a home-less store has nothing to start.
    /// Errors if the installed manager's cloud home fails to build.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        let _lifecycle = self.sync_lifecycle.lock().await;
        match self.sync_manager() {
            Some(manager) => manager.start_sync().await,
            None => {
                debug!("start_sync: no provider connected; nothing to start");
                Ok(())
            }
        }
    }

    /// Stop the sync loop after the in-flight cycle, keeping the installed
    /// manager so [`start_sync`](Self::start_sync) can resume it. A no-op when no
    /// provider is connected.
    pub fn stop_sync(&self) {
        match self.sync_manager() {
            Some(manager) => {
                if let Err(stop_error) = manager.stop_sync() {
                    error!("stop_sync failed: {stop_error}");
                }
            }
            None => debug!("stop_sync: no provider connected; nothing to stop"),
        }
    }

    /// Disconnect the provider entirely: stop the loop and drop the installed
    /// [`SyncManager`]. The store becomes home-less until the next
    /// [`connect_sync`](Self::connect_sync).
    pub fn disconnect_sync(&self) {
        if let Some(manager) = self.sync_manager() {
            if let Err(stop_error) = manager.stop_sync() {
                error!("disconnect_sync failed to stop sync: {stop_error}");
            }
        }
        *self.sync.write().unwrap() = None;
        info!("coven handle: sync manager disconnected");
    }

    /// Wake the sync loop to run a cycle now rather than at the next idle tick. A
    /// no-op when no provider is connected.
    pub fn sync_now(&self) {
        match self.sync_manager() {
            Some(manager) => manager.trigger_sync(),
            None => debug!("sync_now: no provider connected; sync wake ignored"),
        }
    }

    /// Whether the sync loop is running. `false` for a home-less store.
    pub fn is_syncing(&self) -> bool {
        self.sync_manager()
            .is_some_and(|manager| manager.is_sync_ready())
    }

    /// Whether a [`SyncManager`] is installed — a provider is connected. Distinct
    /// from [`is_syncing`](Self::is_syncing), which additionally requires the loop
    /// to be running: this is the predicate a host uses for "has a cloud home"
    /// without the loop-ready condition.
    pub fn is_connected(&self) -> bool {
        self.sync_manager().is_some()
    }

    // =========================================================================
    // Master-key lifecycle
    // =========================================================================

    /// Generate this store's master key and establish it under the handle's
    /// custody. Errors with [`MasterKeyError::AlreadyEstablished`] if custody
    /// already unlocks one — coven never generates over an existing key, so a
    /// corrupt (present-but-unreadable) entry is never silently overwritten
    /// either, since custody's `unlock` surfaces that as `Err`, not `None`.
    /// The only place coven ever generates a master key. Returns its
    /// fingerprint for the host to record in its own config.
    pub fn initialize_master_key(&self) -> Result<String, MasterKeyError> {
        if self.key_custody.unlock()?.is_some() {
            return Err(MasterKeyError::AlreadyEstablished);
        }
        let keyring = MasterKeyring::generate();
        self.key_custody.persist(&keyring)?;
        Ok(keyring.fingerprint())
    }

    /// Import a master key a host already holds — a pasted restore/invite
    /// key, or a keyring migrated from elsewhere — and establish it under the
    /// handle's custody, replacing whatever custody already holds. Accepts
    /// both the keyring JSON and the legacy 64-hex single-key formats.
    /// Returns its fingerprint for the host to record in its own config.
    pub fn import_master_key(&self, serialized: &str) -> Result<String, MasterKeyError> {
        let keyring = MasterKeyring::from_serialized(serialized)?;
        self.key_custody.persist(&keyring)?;
        Ok(keyring.fingerprint())
    }

    /// Remove the master key from custody — a host's lock/sign-out flow. `Ok`
    /// whether or not one was established.
    pub fn forget_master_key(&self) -> Result<(), KeyError> {
        self.key_custody.forget()
    }

    /// The established master key's fingerprint, or `None` if custody has
    /// never had one established (or is locked, for a policy where that's
    /// representable).
    pub fn master_key_fingerprint(&self) -> Result<Option<String>, KeyError> {
        Ok(self.key_custody.unlock()?.map(|k| k.fingerprint()))
    }

    // =========================================================================
    // App-data sealing
    // =========================================================================

    /// Seal `plaintext` under the store's current master-key generation, for a
    /// host to store in its own rows — a password entry's payload, an API token.
    /// coven's at-rest encryption is cloud-side; the local database is plaintext
    /// SQLite, so a host with a secret to keep in a row seals it here first.
    ///
    /// The output records the generation it was sealed under, so it stays
    /// openable after any number of key rotations. `aad` binds the ciphertext to
    /// its context — the owning row's primary key, say — and
    /// [`open_app_data`](Self::open_app_data) with a different `aad` fails, so a
    /// payload moved to another row does not silently open there.
    ///
    /// [`SealError::Locked`] if the store has no established master key, the same
    /// gate [`connect_sync`](Self::connect_sync) applies before it seals cloud
    /// traffic.
    pub fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        Ok(app_data_cipher(self.key_custody.as_ref())?.seal_app_data(plaintext, aad))
    }

    /// Open a payload [`seal_app_data`](Self::seal_app_data) produced, under
    /// whichever generation it names — a rotated keyring still opens everything
    /// it sealed before rotating.
    ///
    /// [`SealError::Locked`] if the store is locked; a wrong `aad`, a tampered
    /// payload, an unreadable version, or a generation this store's keyring lacks
    /// each surface their own typed error.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        app_data_cipher(self.key_custody.as_ref())?.open_app_data(sealed, aad)
    }

    // =========================================================================
    // Blobs
    // =========================================================================

    /// The read [`SyncStorage`] for coven's locality-aware read, or `None` for a
    /// home-less store: `Some(home)` when a provider is connected, `None` when
    /// none is. coven reaches storage only on a cloud miss — a Remote blob not yet
    /// cached. A Local blob (the only kind a home-less store has) is served from
    /// its external ref or the local store without ever touching storage, so a
    /// home-less read passes `None` and the cache layer surfaces
    /// [`BlobCacheError::NoCloudHome`] only if a Remote blob ever reaches the miss
    /// path — a real fault, not masked.
    ///
    /// A provider that IS configured but whose storage fails to build (missing
    /// credentials, a bad cipher) surfaces that error rather than reporting
    /// home-less.
    ///
    /// When a [`SyncManager`] is connected and its loop is running, the read
    /// reuses that loop's own [`CloudSyncStorage`] rather than rebuilding one from
    /// config — so a read and the loop's writes share the exact home + cipher (and
    /// a key rotation the loop applies in place is seen here on the next read), and
    /// a test home injected via
    /// [`connect_sync_with_test_home`](Self::connect_sync_with_test_home) is served
    /// from with no separate hook. A manager connected but not yet running its loop
    /// still wraps the manager's stored home; only a home-less store builds from
    /// config when a provider is configured.
    async fn blob_storage(
        &self,
    ) -> Result<Option<Arc<dyn SyncStorage>>, crate::storage::cloud::setup::StorageSetupError> {
        if let Some(manager) = self.sync_manager() {
            if let Some(loop_handle) = manager.sync_loop_handle() {
                let storage: Arc<dyn SyncStorage> = loop_handle.storage().clone();
                return Ok(Some(storage));
            }
            if let Some(home) = manager.cloud_home() {
                let config = self.config();
                let storage = crate::storage::cloud::setup::create_sync_storage_with_home(
                    &config,
                    self.key_custody.as_ref(),
                    home,
                    None,
                )?;
                return Ok(Some(Arc::new(storage)));
            }
        }
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(None);
        }
        let storage = crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
            &config,
            &self.key_service,
            self.key_custody.as_ref(),
            None,
            self.clock.clone(),
            self.cloudkit_ops.clone(),
        )
        .await?;
        Ok(Some(Arc::new(storage)))
    }

    /// Read a blob's whole plaintext through coven's locality-aware read: served
    /// from the user's file (Local user-provided), coven's local store (Local
    /// host-provided), the pinned/evictable cache on a Remote hit, or fetched
    /// from the cloud (into the cache) on a Remote miss. The host passes only the
    /// [`BlobRef`]; coven holds the database, the directory, and the storage.
    pub async fn read_blob(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobCacheError> {
        let storage = self.blob_storage().await?;
        crate::blob::cache::read_blob(&self.db, &self.store_dir, storage.as_deref(), blob).await
    }

    /// Serve `len` plaintext bytes of a blob starting at `offset`, for streaming
    /// or seeking without loading the whole file. `source_size` is the blob's
    /// plaintext length (the row that owns the blob carries it), used to bound the
    /// range. The ranged sibling of [`read_blob`](Self::read_blob).
    pub async fn open_blob_stream(
        &self,
        blob: &BlobRef,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, BlobCacheError> {
        let storage = self.blob_storage().await?;
        crate::blob::cache::open_blob_stream(
            &self.db,
            &self.store_dir,
            storage.as_deref(),
            blob,
            source_size,
            offset,
            len,
        )
        .await
    }

    /// Pin a Remote blob set for offline: coven fetches each into the protected
    /// cache (`storage/pinned/`) — from the evictable cache if already there, else
    /// the cloud — exempt from the size budget. Idempotent.
    pub async fn pin(&self, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
        let storage = self.blob_storage().await?;
        crate::blob::cache::pin(&self.db, &self.store_dir, storage.as_deref(), blobs).await
    }

    /// Unpin a Remote blob set: coven moves each from `storage/pinned/` to the
    /// evictable `storage/cache/` (still readable, now droppable). No cloud read.
    pub async fn unpin(&self, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
        crate::blob::cache::unpin(&self.store_dir, blobs).await
    }

    /// The cloud object key a blob's bytes live at, derived under the connected
    /// home's path scheme (`Hashed` → `{namespace}/{ab}/{cd}/{id}`, `Plain` →
    /// `{namespace}/{cloud_path}`). coven owns this derivation — the host passes a
    /// [`BlobRef`] and never reconstructs the cloud layout. The host enqueues a
    /// blob's cloud removal under this key (its delete drains to a tombstone; see
    /// [`crate::blob::delete`]), and a test asserts an upload's key matches the
    /// read key with it. A `Plain` home with no `cloud_path` is a surfaced error.
    pub fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let scheme = BlobPathScheme::for_storage(self.config().cloud_home.storage);
        // This device's own blobs key under its own public key (the host asks for
        // the key of a blob it uploaded).
        let uploader = DeviceKeys::get_user_public_key()
            .map_err(|e| StorageError::Storage(format!("read device public key: {e}")))?
            .map(hex::encode);
        CloudSyncStorage::blob_key(
            scheme,
            &blob.namespace,
            uploader.as_deref(),
            &blob.id,
            blob.cloud_path.as_deref(),
        )
    }

    /// Whether every blob in `blobs` is pinned for offline — present in coven's
    /// kept cache folder (`storage/pinned/`). The host answers "is this release
    /// kept offline" through this instead of stat-ing coven's cache layout itself.
    /// An empty set is vacuously pinned. A blob not pinned (in the evictable cache
    /// or absent) makes the whole set unpinned; an existence-check failure is
    /// surfaced, never read as "not pinned".
    pub async fn is_pinned(&self, blobs: &[BlobRef]) -> Result<bool, BlobCacheError> {
        for blob in blobs {
            if !crate::blob::cache::is_pinned(&self.store_dir, &blob.namespace, &blob.id).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Drop a blob's every on-device copy: its cache copies (the kept
    /// `storage/pinned/` and the evictable `storage/cache/` folders) and its
    /// local-store copy (`storage/local/`). The host calls this when a blob is
    /// genuinely being deleted, so coven leaves nothing on disk regardless of the
    /// blob's locality — a Remote blob's cache copy, a host-provided Local blob's
    /// local-store copy, or both across a transition. An absent file in any folder
    /// is the expected case, not an error (the blob lived in at most one of them).
    ///
    /// This removes the only on-device bytes of a Local blob, so it is for the
    /// delete path only — a Remote blob's cloud copy is tombstoned separately via
    /// [`blob_cloud_key`](Self::blob_cloud_key).
    pub async fn evict_blob(&self, blob: &BlobRef) -> Result<(), BlobCacheError> {
        crate::blob::cache::drop_all_local_copies(&self.store_dir, &blob.namespace, &blob.id).await
    }

    /// Make `(root_table, root_id)` Remote (Local → Remote): enqueue an upload per
    /// user-provided blob from its external file and record the make_remote
    /// intent, then return. The drain uploads each and flips the gate true on the
    /// last; the gate flip re-emits the subtree and the cycle's inline push
    /// uploads host-provided blobs. `pin` keeps the uploaded blobs in the cache as
    /// pinned offline copies. Errors with [`MakeRemoteError::SyncNotReady`] when no
    /// provider is connected.
    pub async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        match self.sync_manager() {
            Some(manager) => manager.make_remote(root_table, root_id, pin).await,
            None => Err(MakeRemoteError::SyncNotReady),
        }
    }

    /// Cancel an in-flight make_remote of `(root_table, root_id)`: clear its intent
    /// and pending uploads and tombstone any blob already in the cloud. The gate
    /// never flips, so the root stays Local. Errors with
    /// [`MakeRemoteError::SyncNotReady`] when no provider is connected.
    pub async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        match self.sync_manager() {
            Some(manager) => manager.cancel_make_remote(root_table, root_id).await,
            None => Err(MakeRemoteError::SyncNotReady),
        }
    }

    /// Make `(root_table, root_id)` Local (Remote → Local): bring each blob back to
    /// a local file durability-first — a user-provided blob to the path named in
    /// `dest` (blob id → destination path), a host-provided blob to coven's local
    /// store (no dest) — then flip the gate false, register the external refs, and
    /// enqueue the cloud deletes in one atomic commit. `cancel` aborts before the
    /// commit (the root stays Remote). Errors with [`MakeLocalError::SyncNotReady`]
    /// when no provider is connected.
    pub async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        match self.sync_manager() {
            Some(manager) => manager.make_local(root_table, root_id, dest, cancel).await,
            None => Err(MakeLocalError::SyncNotReady),
        }
    }

    /// Drain pending blob uploads now: read each local file, seal it under its
    /// scope, write it to the cloud, and keep a `retain_pinned` entry's plaintext
    /// in the protected cache. Returns the [`DrainOutcome`].
    ///
    /// The sync loop drains each cycle; this drives a drain directly off the
    /// connected home, against coven's own register clock and the handle's
    /// observer. Errors when no provider is connected (there is no cloud to write
    /// to).
    pub async fn drain_uploads(&self) -> Result<DrainOutcome, SyncError> {
        let manager = self.sync_manager().ok_or(SyncError::NotConfigured)?;
        let hlc = self.db.hlc();
        let sync_loop = manager
            .sync_loop_handle()
            .ok_or(SyncError::LoopNotRunning)?;
        let storage = sync_loop.storage().clone();
        let cipher = sync_loop.cipher().clone();
        crate::blob::upload::drain_uploads(
            &self.db,
            storage.cloud_home(),
            cipher.as_ref(),
            &self.config().store_id,
            &self.store_dir,
            self.clock.as_ref(),
            &hlc,
            self.observer.as_deref(),
        )
        .await
        .map_err(SyncError::BlobUpload)
    }

    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, crate::DbError> {
        self.db.get_cache_budget(namespace).await
    }

    pub async fn set_cache_budget(
        &self,
        namespace: &str,
        max_bytes: u64,
    ) -> Result<(), crate::DbError> {
        self.db.set_cache_budget(namespace, max_bytes).await
    }

    pub fn get_user_pubkey(&self) -> Result<Option<String>, SyncError> {
        DeviceKeys::get_user_public_key()
            .map(|opt| opt.map(hex::encode))
            .map_err(SyncError::from)
    }

    pub fn generate_restore_code(&self) -> Result<String, SyncError> {
        crate::storage::cloud::setup::generate_restore_code(
            &self.config(),
            &self.key_service,
            self.key_custody.as_ref(),
        )
        .map_err(SyncError::from)
    }

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        let manager = self.sync_manager().ok_or(SyncError::NotConfigured)?;
        manager.get_members().await
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
    ) -> Result<String, SyncError> {
        let manager = self.sync_manager().ok_or(SyncError::NotConfigured)?;
        manager
            .invite_member(public_key_hex, invitee_email, role)
            .await
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, SyncError> {
        let manager = self.sync_manager().ok_or(SyncError::NotConfigured)?;
        manager.remove_member(public_key_hex).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{BlobScope, CacheFill, Provenance};
    use crate::clock::SystemClock;
    use crate::config::{CloudProvider, Config, HomeStorage};
    use crate::encryption::EncryptionService;
    use crate::keys::{test_keyring, StoreKeys};
    use crate::storage::cloud::cloudkit::{CloudKitOps, CloudKitScope, CloudKitShare};
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::CloudHomeError;
    use crate::sync::cloud_storage::CloudCipher;
    use crate::sync::sync_manager::{ConfigProvider, SyncError};
    use crate::sync::test_helpers::{plant_blob_row, read_test_db, temp_store_dir};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    struct TestCloudKitOps {
        store: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
    }

    /// A ready-to-use custody for tests that build a [`CovenHandle`] directly
    /// (bypassing the builder) and never exercise master-key lifecycle
    /// methods — the blob/storage/status tests in this module. Seeded
    /// in-memory so it needs no keyring registration.
    fn test_key_custody() -> Arc<dyn crate::keys::MasterKeyCustody> {
        crate::custody::KeyCustody::InMemory(crate::encryption::MasterKeyring::generate()).resolve(
            "unused-store-id",
            &crate::store_dir::StoreDir::new("unused-store-dir"),
        )
    }

    #[tokio::test]
    async fn read_blob_with_unbuildable_storage_is_a_typed_setup_error_not_io() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let mut config = Config::with_defaults(
            "lib-setup-error".to_string(),
            "device".to_string(),
            store_dir.clone(),
            "Test".to_string(),
        );
        // A provider is selected but its bucket is unset, so the read path cannot
        // build sync storage. That is a configuration fault the user must fix — it
        // must reach the caller as StorageSetup, not be mislabeled as a disk I/O
        // error the way the old catch-all Io variant did.
        config.cloud_home.provider = Some(CloudProvider::S3);
        let config_provider: ConfigProvider = Arc::new(move || config.clone());
        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-setup-error".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        let blob = BlobRef {
            namespace: "release_files".to_string(),
            id: "anyblob0".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        let err = handle
            .read_blob(&blob)
            .await
            .expect_err("no sync storage can be built from the broken config");
        assert!(
            matches!(err, BlobCacheError::StorageSetup(_)),
            "got {err:?}"
        );
    }

    fn test_handle(store_id: &str, store_dir: StoreDir, db: Database) -> CovenHandle {
        test_handle_with_custody(store_id, store_dir, db, test_key_custody())
    }

    fn test_handle_with_custody(
        store_id: &str,
        store_dir: StoreDir,
        db: Database,
        key_custody: Arc<dyn crate::keys::MasterKeyCustody>,
    ) -> CovenHandle {
        let config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = Arc::new(move || config.clone());
        CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new(store_id.to_string()),
            key_custody,
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        )
    }

    impl TestCloudKitOps {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            }
        }
    }

    impl CloudKitOps for TestCloudKitOps {
        fn write_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
            data: Vec<u8>,
        ) -> Result<(), CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .insert((scope.clone(), key.to_string()), data);
            Ok(())
        }

        fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .get(&(scope.clone(), key.to_string()))
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
        }

        fn list_records(
            &self,
            scope: &CloudKitScope,
            prefix: &str,
        ) -> Result<Vec<String>, CloudHomeError> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .keys()
                .filter(|(stored_scope, key)| stored_scope == scope && key.starts_with(prefix))
                .map(|(_, key)| key.clone())
                .collect())
        }

        fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .remove(&(scope.clone(), key.to_string()));
            Ok(())
        }

        fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .contains_key(&(scope.clone(), key.to_string())))
        }

        fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
            Ok(CloudKitShare {
                share_url: format!("coven-test-share-{member_pubkey}"),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            })
        }

        fn revoke_share(&self, _member_pubkey: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }

        fn accept_share(&self, _share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
            Ok(CloudKitShare {
                share_url: "coven-test-share".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            })
        }
    }

    /// `connect_sync_with_test_home` stands a real `SyncManager` over an injected
    /// `InMemoryCloudHome` and routes BOTH the upload drain and the read path
    /// through it: a blob enqueued for upload drains to the home through the
    /// handle, and a subsequent `read_blob` resolves the Remote miss back out of
    /// the same home — end to end, with the host supplying only the home + cipher.
    // The user keypair is one process-wide keyring account, so the guard is held
    // across this test's awaits to keep a parallel test from deleting it mid-run
    // (sound here: a `#[tokio::test]` is a single-task current-thread runtime, so
    // the blocking `std` lock never deadlocks against another task on this runtime).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_home_drives_drain_and_read_through_the_handle() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (tmp, store_dir) = temp_store_dir();
        // `note_photos` carries a blob in the `images` namespace so the read path can
        // resolve a planted row up to its gated `notes` root (the gate that decides
        // Local vs Remote).
        let db = read_test_db("images");

        // A browsable home: plaintext at rest, readable `{namespace}/{cloud_path}`
        // blob keys. The cipher passed below is the matching `Plaintext`.
        let mut config = Config::with_defaults(
            "lib-test".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Browsable;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };

        let stamper = db.stamper();
        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: this test never calls `sql_read`, and the test db is
            // `:memory:` (no shareable read-only companion), so the writer clone
            // stands in.
            db.clone(),
            stamper,
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-test".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        // Inject the mock home; the host hands over only the home + cipher.
        let home = Arc::new(InMemoryCloudHome::new());
        handle
            .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
            .await
            .expect("connect over the injected test home");

        // A blob's plaintext on disk, enqueued for upload through coven's real
        // queue API (drives the same enqueue path production make_remote uses; no
        // backing row, so the drain's completion check finds no gated root and just
        // clears the row — a plain upload).
        let plaintext = b"cover-art-bytes-for-the-test-home".to_vec();
        let source = tmp.path().join("cover-source.jpg");
        std::fs::write(&source, &plaintext).expect("write source file");
        let cloud_key = "images/cover.jpg"; // {namespace}/{cloud_path} under the plain scheme.
        db.enqueue_upload(
            "cover-1",
            cloud_key,
            Some(source.to_str().expect("temp source path is valid UTF-8")),
            BlobScope::Master,
            false,
            "2024-01-01T00:00:00Z",
        )
        .await
        .expect("enqueue the upload");

        // Drain through the handle: the upload lands in the injected home verbatim.
        let outcome = handle
            .drain_uploads()
            .await
            .expect("drain through the handle");
        assert_eq!(outcome.uploaded, 1, "the one queued blob uploaded");
        assert_eq!(
            home.get(cloud_key).as_deref(),
            Some(plaintext.as_slice()),
            "the blob landed in the injected home at its readable key, plaintext at rest",
        );

        // The drain above ran the plain-upload path (no backing row) deliberately. A
        // real read happens after a make_remote flipped the blob's gated root Remote,
        // so plant that Remote state now (a gated `notes` root with the `note_photos`
        // child carrying this id) so the read resolves the blob's locality to Remote
        // and fetches it back out of the home (rather than failing locality resolution).
        plant_blob_row(&db, "cover-1", true, plaintext.len() as u64).await;

        // Read through the handle: a Remote miss resolves back out of the same home.
        let blob = BlobRef {
            namespace: "images".to_string(),
            id: "cover-1".to_string(),
            scope: BlobScope::Master,
            cloud_path: Some("cover.jpg".to_string()),
            provenance: Provenance::UserProvided,
            fill: CacheFill::CacheLazy,
        };
        let read = handle
            .read_blob(&blob)
            .await
            .expect("read through the handle");
        assert_eq!(
            read, plaintext,
            "read_blob fetched the blob's plaintext from the injected test home",
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn connected_manager_reuses_cloud_home_for_loop_storage() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");

        let mut config = Config::with_defaults(
            "lib-cloudkit-home-reuse".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::CloudKit);
        config.cloud_home.storage = HomeStorage::Browsable;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };

        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-cloudkit-home-reuse".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            Some(Arc::new(TestCloudKitOps::new())),
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        handle
            .connect_sync()
            .await
            .expect("connect sync over the test CloudKit driver");

        let manager = handle
            .sync_manager()
            .expect("connect_sync installs a manager");
        let stored_home = manager.cloud_home().expect("manager stores cloud home");
        let loop_handle = manager
            .sync_loop_handle()
            .expect("connect_sync starts the sync loop");

        assert!(
            std::ptr::addr_eq(stored_home.as_ref(), loop_handle.storage().cloud_home()),
            "the sync loop storage must wrap the same cloud home stored on the manager",
        );
    }

    /// A read-only handle holds no sync loop, so every cloud-miss read builds
    /// storage fresh from config via the `cipher: None` fallback
    /// (`create_sync_storage_with_cloudkit` → `build_cloud_cipher`) — which
    /// must resolve the same cipher a writer sealed under. Sealing directly
    /// through storage built the identical way (bypassing the local-store
    /// staging a `CovenHandle::write` would leave, which a same-store-dir
    /// reader could serve from without ever touching the cloud) forces the
    /// read through an actual cloud GET + decrypt.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn read_only_handle_resolves_an_encrypted_cipher_through_custody() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let store_id = "ro-encrypted-custody-test";
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");

        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::CloudKit);
        config.cloud_home.storage = HomeStorage::Opaque;

        let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir);
        custody
            .persist(&crate::encryption::MasterKeyring::generate())
            .expect("establish a master key");

        let ops = Arc::new(TestCloudKitOps::new());
        let key_service = StoreKeys::new(store_id.to_string());

        // Seal and upload directly through storage built the same way a
        // production write path would build it — resolving the cipher from
        // custody via the `cipher: None` fallback.
        let writer_storage = crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
            &config,
            &key_service,
            custody.as_ref(),
            None,
            Arc::new(SystemClock),
            Some(ops.clone()),
        )
        .await
        .expect("build cloud storage resolving the cipher from custody");
        let plaintext = b"encrypted-cloud-blob-for-the-read-only-handle".to_vec();
        crate::sync::storage::SyncStorage::put_blob(
            &writer_storage,
            "images",
            "cover-1",
            BlobScope::Master,
            None,
            plaintext.clone(),
        )
        .await
        .expect("seal and upload the blob");

        plant_blob_row(&db, "cover-1", true, plaintext.len() as u64).await;

        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let reader = crate::read_handle::CovenReadHandle::new(
            db,
            store_dir,
            config_provider,
            key_service,
            custody,
            Arc::new(SystemClock),
            Some(ops),
        );

        let blob = BlobRef {
            namespace: "images".to_string(),
            id: "cover-1".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::UserProvided,
            fill: CacheFill::CacheLazy,
        };
        let read = reader
            .read_blob(&blob)
            .await
            .expect("the read-only handle resolves the same cipher through custody");
        assert_eq!(
            read, plaintext,
            "the blob decrypts back to its original plaintext",
        );
    }

    #[tokio::test]
    async fn sync_not_configured_is_typed() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-no-sync", store_dir, db);

        let result = handle.get_members().await;

        assert!(matches!(result, Err(SyncError::NotConfigured)));
    }

    /// `initialize_master_key` is the only place coven ever generates a
    /// master key, and it refuses to run again once one is established —
    /// coven never generates over an existing key.
    #[tokio::test]
    async fn initialize_master_key_refuses_a_second_call() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("lib-init-master-key-twice", &store_dir);
        let handle = test_handle_with_custody("lib-init-master-key-twice", store_dir, db, custody);

        let fingerprint = handle
            .initialize_master_key()
            .expect("the first call establishes a master key");
        assert!(!fingerprint.is_empty());
        assert_eq!(
            handle.master_key_fingerprint().unwrap(),
            Some(fingerprint),
            "master_key_fingerprint reflects what initialize_master_key just established",
        );

        let error = handle
            .initialize_master_key()
            .expect_err("a second call must refuse rather than generate over an existing key");
        assert!(matches!(
            error,
            crate::keys::MasterKeyError::AlreadyEstablished
        ));
    }

    /// The end-to-end proof that `initialize_master_key` establishes the key
    /// that actually seals cloud traffic. A keyring-custody store initializes a
    /// master key, connects over an injected opaque `InMemoryCloudHome` through
    /// the custody-resolving connect path — no cipher is injected; the manager
    /// unlocks the key exactly as production `start_sync` does — then enqueues
    /// and drains a blob. The bytes at rest in the home are ciphertext, never
    /// the plaintext (the assertion a browsable/plaintext home would fail),
    /// while `read_blob` decrypts them back. Only the established key sealing the
    /// upload makes both hold.
    // The user keypair is one process-wide keyring account, so the guard is held
    // across this test's awaits to keep a parallel test from deleting it mid-run.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn initialize_master_key_seals_cloud_traffic_the_custody_path_reads_back() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let store_id = "lib-init-master-key-seals-traffic";

        // Opaque storage: the master key established below seals every object at
        // rest. A configured provider is unnecessary — the injected test home is
        // the enablement.
        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Opaque;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };

        let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir);
        let handle = CovenHandle::new(
            db.clone(),
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new(store_id.to_string()),
            custody,
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        handle
            .initialize_master_key()
            .expect("establish the master key before connecting");

        // Connect over the injected home through the custody path: the manager
        // resolves the cipher from the just-established key, never an injected
        // one. An opaque home with no key would fail here with
        // `MasterKeyNotEstablished`.
        let home = Arc::new(InMemoryCloudHome::new());
        handle
            .connect_sync_with_test_home_custody(home.clone())
            .await
            .expect("connect over the injected opaque home, resolving the cipher from custody");

        // Enqueue this device's own blob under the hashed key an opaque home uses
        // (`{namespace}/{uploader}/{ab}/{cd}/{id}`) so the drain seals it and the
        // read resolves the same uploader to fetch it back. The keypair exists
        // now that connecting bootstrapped it.
        let uploader = hex::encode(
            DeviceKeys::get_user_public_key()
                .expect("read this device's public key")
                .expect("a device keypair exists after connecting"),
        );
        let cloud_key = StoreDir::uploader_hashed_key("images", &uploader, "cover-1")
            .expect("build the hashed cloud key");
        let plaintext = b"cover-art-sealed-under-the-established-master-key".to_vec();
        let source = tmp.path().join("cover-source.jpg");
        std::fs::write(&source, &plaintext).expect("write source file");
        db.enqueue_upload(
            "cover-1",
            &cloud_key,
            Some(source.to_str().expect("temp source path is valid UTF-8")),
            BlobScope::Master,
            false,
            "2024-01-01T00:00:00Z",
        )
        .await
        .expect("enqueue the upload");

        let outcome = handle
            .drain_uploads()
            .await
            .expect("drain through the handle");
        assert_eq!(outcome.uploaded, 1, "the one queued blob uploaded");

        // At rest the object is ciphertext: the stored bytes are not the
        // plaintext, and no object in the home holds the plaintext verbatim.
        let at_rest = home.get(&cloud_key).expect("the blob landed in the home");
        assert_ne!(
            at_rest, plaintext,
            "the master key sealed the upload — the bytes at rest are not the plaintext",
        );
        assert!(
            home.keys()
                .iter()
                .all(|k| home.get(k).as_deref() != Some(plaintext.as_slice())),
            "no object in the home holds the plaintext",
        );

        // Plant the Remote locality, then read back: the read resolves the
        // uploader off the home and decrypts through the same custody-resolved
        // cipher.
        plant_blob_row(&db, "cover-1", true, plaintext.len() as u64).await;
        let blob = BlobRef {
            namespace: "images".to_string(),
            id: "cover-1".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::UserProvided,
            fill: CacheFill::CacheLazy,
        };
        let read = handle
            .read_blob(&blob)
            .await
            .expect("read through the handle");
        assert_eq!(
            read, plaintext,
            "read_blob decrypts the sealed blob back to its original plaintext",
        );
    }

    /// `import_master_key` accepts both formats `MasterKeyring::from_serialized`
    /// does — the legacy 64-hex single key and the keyring JSON — and returns
    /// the fingerprint of whichever generation it just established.
    #[tokio::test]
    async fn import_master_key_accepts_legacy_hex_and_keyring_json() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-import-master-key", store_dir, db);

        let raw_hex = hex::encode([0x22u8; 32]);
        let hex_fingerprint = handle
            .import_master_key(&raw_hex)
            .expect("import the legacy raw-hex format");
        assert_eq!(
            hex_fingerprint,
            EncryptionService::from_key([0x22u8; 32]).fingerprint(),
        );
        assert_eq!(
            handle.master_key_fingerprint().unwrap(),
            Some(hex_fingerprint),
        );

        let keyring = crate::encryption::MasterKeyring::generate();
        let json_fingerprint = handle
            .import_master_key(&keyring.to_serialized())
            .expect("import the keyring JSON format");
        assert_eq!(json_fingerprint, keyring.fingerprint());
        assert_eq!(
            handle.master_key_fingerprint().unwrap(),
            Some(json_fingerprint),
            "the second import replaces the first",
        );
    }

    /// `forget_master_key` removes an established key, and is `Ok` whether or
    /// not one was established — a host's lock/sign-out flow.
    #[tokio::test]
    async fn forget_master_key_clears_an_established_key_and_is_idempotent() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-forget-master-key", store_dir, db);

        assert!(handle.master_key_fingerprint().unwrap().is_some());
        handle
            .forget_master_key()
            .expect("forget an established key");
        assert!(handle.master_key_fingerprint().unwrap().is_none());
        handle
            .forget_master_key()
            .expect("forgetting an already-absent key is not an error");
    }

    // =========================================================================
    // App-data sealing
    // =========================================================================

    /// The host-facing round trip over a keyring-custody store: what the handle
    /// seals under the store's established master key, the same handle opens —
    /// and a payload presented with a different `aad` than it was bound to does
    /// not open, so a value lifted into another row stays shut.
    #[tokio::test]
    async fn seal_and_open_app_data_round_trip_through_the_handle() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let store_id = "lib-app-data-round-trip";
        let handle = test_handle_with_custody(
            store_id,
            store_dir.clone(),
            db,
            crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir),
        );
        handle
            .initialize_master_key()
            .expect("establish the store's master key");

        let sealed = handle
            .seal_app_data(b"entry-secret", b"row-42")
            .expect("seal under the established key");
        assert_ne!(
            sealed, b"entry-secret",
            "the sealed payload is not the plaintext",
        );

        assert_eq!(
            handle.open_app_data(&sealed, b"row-42").unwrap(),
            b"entry-secret",
            "the handle opens what it sealed",
        );

        let error = handle
            .open_app_data(&sealed, b"row-99")
            .expect_err("a different aad must not open the payload");
        assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
    }

    /// A read-only handle over the same store opens what the writer sealed: it
    /// resolves the same master keyring through its own custody (the same
    /// `store_id` keyring account), so a secondary reader — a File Provider
    /// extension, a second process — reads the host's sealed rows.
    #[tokio::test]
    async fn open_app_data_round_trips_through_the_read_handle() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let store_id = "lib-app-data-read-handle";

        let writer = test_handle_with_custody(
            store_id,
            store_dir.clone(),
            db.clone(),
            crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir),
        );
        writer
            .initialize_master_key()
            .expect("establish the store's master key");
        let sealed = writer
            .seal_app_data(b"read-me-back", b"ctx")
            .expect("seal through the write handle");

        let config_provider: ConfigProvider = {
            let config = Config::with_defaults(
                store_id.to_string(),
                "test-device".to_string(),
                store_dir.clone(),
                "Test Store".to_string(),
            );
            Arc::new(move || config.clone())
        };
        let reader = crate::read_handle::CovenReadHandle::new(
            db,
            store_dir.clone(),
            config_provider,
            StoreKeys::new(store_id.to_string()),
            crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir),
            Arc::new(SystemClock),
            None,
        );

        assert_eq!(
            reader.open_app_data(&sealed, b"ctx").unwrap(),
            b"read-me-back",
            "the read handle opens what the write handle sealed",
        );
    }

    /// A store whose custody holds no master key has nothing to seal under and
    /// nothing to open with. Both directions refuse with `Locked` rather than
    /// inventing a key — the app-data counterpart of the sync engine's
    /// `MasterKeyNotEstablished` gate. Here the store is genuinely never
    /// initialized: a real keyring custody whose account holds no key.
    #[tokio::test]
    async fn app_data_is_locked_when_no_master_key_is_established() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let store_id = "lib-app-data-locked";
        let handle = test_handle_with_custody(
            store_id,
            store_dir.clone(),
            db,
            crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir),
        );
        assert!(
            handle.master_key_fingerprint().unwrap().is_none(),
            "the store starts with no established master key",
        );

        let seal_error = handle
            .seal_app_data(b"nothing to seal under", b"ctx")
            .expect_err("sealing a locked store must refuse");
        assert!(matches!(seal_error, SealError::Locked), "{seal_error:?}");

        let open_error = handle
            .open_app_data(b"nothing to open with", b"ctx")
            .expect_err("opening on a locked store must refuse");
        assert!(matches!(open_error, SealError::Locked), "{open_error:?}");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn plaintext_membership_operations_are_typed() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-plaintext-membership", store_dir, db);
        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect plaintext home");

        let public_key_hex = hex::encode(crate::keys::UserKeypair::generate().public_key());
        let invite = handle
            .invite_member(&public_key_hex, None, MemberRole::Member)
            .await;
        let remove = handle.remove_member(&public_key_hex).await;

        assert!(matches!(invite, Err(SyncError::NotEncryptedHome)));
        assert!(matches!(remove, Err(SyncError::NotEncryptedHome)));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn reconnect_sync_stops_the_previous_loop() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let config = Config::with_defaults(
            "lib-reconnect-loop".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-reconnect-loop".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("first connect over injected home");
        let first_loop = handle
            .sync_manager()
            .expect("first manager installed")
            .sync_loop_handle()
            .expect("first loop installed");
        assert!(first_loop.is_running(), "first loop starts running");

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("second connect over injected home");
        let replacement_loop = handle
            .sync_manager()
            .expect("replacement manager installed")
            .sync_loop_handle()
            .expect("replacement loop installed");

        assert!(
            !first_loop.is_running(),
            "reconnect must stop the old loop before installing a replacement",
        );
        assert!(
            replacement_loop.is_running(),
            "reconnect leaves the replacement loop running",
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn stopped_installed_loop_blocks_blob_transitions() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let config = Config::with_defaults(
            "lib-stopped-loop-readiness".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-stopped-loop-readiness".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");
        let manager = handle.sync_manager().expect("manager installed");
        let loop_handle = manager.sync_loop_handle().expect("loop installed");

        loop_handle.stop().expect("stop installed loop");

        let make_remote = manager.make_remote("notes", "note-1", false).await;
        assert!(matches!(make_remote, Err(MakeRemoteError::SyncNotReady)));

        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let make_local = manager
            .make_local("notes", "note-1", &HashMap::new(), &cancel_rx)
            .await;
        assert!(matches!(make_local, Err(MakeLocalError::SyncNotReady)));
    }

    // The user keypair is one process-wide keyring account, so the guard is held
    // across this test's awaits to keep a parallel test from deleting it mid-run
    // (sound here: a `#[tokio::test]` is a single-task current-thread runtime, so
    // the blocking `std` lock never deadlocks against another task on this runtime).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn host_drain_uses_the_installed_sync_loop_cipher_lock() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");

        let config = Config::with_defaults(
            "lib-test".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };

        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new("lib-test".to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );

        let home = Arc::new(InMemoryCloudHome::new());
        handle
            .connect_sync_with_test_home(
                home.clone(),
                CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            )
            .await
            .expect("connect encrypted injected home");
        let manager = handle.sync_manager().expect("sync manager installed");
        let loop_handle = manager.sync_loop_handle().expect("sync loop installed");
        *loop_handle.cipher().write().unwrap() = CloudCipher::Plaintext;

        let plaintext = b"plain-drain-bytes-after-loop-cipher-mutation".to_vec();
        let source = tmp.path().join("plain-source.jpg");
        std::fs::write(&source, &plaintext).expect("write source file");
        let cloud_key = "images/ab/cd/plain-cover";
        db.enqueue_upload(
            "plain-cover",
            cloud_key,
            Some(source.to_str().expect("temp source path is valid UTF-8")),
            BlobScope::Master,
            false,
            "2024-01-01T00:00:00Z",
        )
        .await
        .expect("enqueue the upload");

        let outcome = handle
            .drain_uploads()
            .await
            .expect("drain through the handle");
        assert_eq!(outcome.uploaded, 1, "the queued blob uploaded");
        assert_eq!(
            home.get(cloud_key).as_deref(),
            Some(plaintext.as_slice()),
            "the host-driven drain used the mutated loop cipher lock",
        );
    }

    fn status_test_handle(store_id: &str) -> (tempfile::TempDir, CovenHandle) {
        let (tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: these tests never call `sql_read`, and the test db is
            // `:memory:` (unique per connection, no shareable read-only companion),
            // so the writer clone stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            StoreKeys::new(store_id.to_string()),
            test_key_custody(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
        );
        (tmp, handle)
    }

    /// The loop emits `Started` when a cycle begins and a terminal status when it
    /// ends, so a host can show a sync in progress and then its outcome.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn subscribed_host_sees_started_then_a_terminal_status() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, handle) = status_test_handle("lib-status-syncing");
        let mut rx = handle.subscribe_sync_status();

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");

        let start = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("a start status arrives within the timeout")
            .expect("the status channel is open");
        assert!(
            matches!(start, SyncLoopStatus::Started),
            "the first status of a cycle marks it in progress",
        );

        let done = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("a completion status arrives within the timeout")
            .expect("the status channel is open");
        // The cycle over an empty plaintext home succeeds — a terminal Succeeded,
        // never a Failed.
        assert!(
            matches!(done, SyncLoopStatus::Succeeded(_)),
            "a successful cycle ends with Succeeded, got {done:?}",
        );
    }

    /// A subscription created before any provider is connected keeps receiving
    /// across a reconnect — the channel is owned by the handle, not the loop that
    /// a reconnect replaces. Under a per-loop channel the receiver would observe
    /// `Closed` after the reconnect dropped the first loop's sender.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn subscription_survives_a_reconnect() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, handle) = status_test_handle("lib-status-reconnect");

        // Subscribe before any provider is connected — valid because the channel
        // is handle-owned.
        let mut rx = handle.subscribe_sync_status();

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("first connect");
        // Reconnect immediately: this drops the first loop and starts a second one
        // over a fresh home before the first loop's startup delay elapses.
        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("reconnect");

        let status = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("a status arrives from the post-reconnect loop")
            .expect("a reconnect does not close the handle-owned status channel");
        assert!(
            matches!(status, SyncLoopStatus::Started),
            "the received status is a cycle start marker, got {status:?}",
        );
    }
}

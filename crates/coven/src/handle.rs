//! The native data handle: one object a host constructs once that owns coven's
//! pieces and exposes the whole data interface as methods.
//!
//! coven owns the library's data — SQL rows and blobs, on disk first, cloud
//! optional. A host (a desktop/mobile app) talks to coven through this one
//! handle and never assembles coven's internals by hand or hands them back to
//! coven on every call. The handle holds the [`Database`], the [`LibraryDir`],
//! the keys, and — once a cloud provider is connected — the [`SyncManager`]; the
//! caller passes only descriptors (a [`BlobRef`], SQL, a config) and coven does
//! its own plumbing.
//!
//! It is the native counterpart of the browser `CovenLibrary` in `coven-wasm`:
//! same role, different substrate. The native stack runs on tokio with a
//! [`SyncManager`], `Send + Sync` throughout.
//!
//! ## What it owns
//!
//! - **Rows** — the [`Database`] (coven already owns the connection). The host
//!   runs its app SQL through [`sql`](CovenHandle::sql) and row+blob batches
//!   through [`write`](CovenHandle::write).
//! - **Blobs** — the [`LibraryDir`] the blob engine reads/writes, plus the
//!   credentials to build a read [`SyncStorage`] on a cloud miss. Read, ranged
//!   read, store, register external, pin/unpin, the locality transitions, and the
//!   upload drain are methods here.
//! - **Sync** — built lazily by [`connect_sync`](CovenHandle::connect_sync) when a
//!   cloud provider is connected. A library with no cloud home never builds a
//!   [`SyncManager`] and only ever holds Local blobs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, info};

use crate::blob::cache::BlobCacheError;
use crate::blob::transition::{MakeLocalError, MakeRemoteError};
use crate::blob::upload::DrainOutcome;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::KeyService;
use crate::library_dir::LibraryDir;
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
#[cfg(any(test, feature = "test-utils"))]
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::membership::MemberRole;
use crate::sync::storage::{StorageError, SyncStorage};
use crate::sync::sync_loop::SyncLoopStatus;
use crate::sync::sync_manager::MemberInfo;
use crate::sync::sync_manager::{ConfigProvider, SyncManager};

/// The native handle over one coven library.
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
/// # async fn use_library(handle: &CovenHandle, cover: &BlobRef)
/// #     -> Result<(), Box<dyn std::error::Error>> {
/// // Rows: run app SQL on the connection coven owns.
/// let note_count: i64 = handle
///     .sql(|sql| {
///         sql.connection()
///             .query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
///             .map_err(coven::CovenError::from)
///     })
///     .await?;
///
/// // Blobs: read by descriptor. coven resolves locality — the user's own file,
/// // its local store, the cache, or a cloud fetch — and hands back plaintext.
/// let bytes: Vec<u8> = handle.read_blob(cover).await?;
///
/// // Sync is optional. Connect a provider, then drive it; a library with no
/// // cloud home never calls these and stays fully usable on-device.
/// handle.connect_sync(None).await?;
/// handle.sync_now();
/// # let _ = note_count;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CovenHandle {
    db: Database,
    stamper: crate::sync::hlc::UpdatedAtStamper,
    library_dir: LibraryDir,

    /// Supplies the host's current config on demand. coven reads it fresh each
    /// call so a host with reactive config sees changes without rebuilding the
    /// handle. The same provider the [`SyncManager`] reads from.
    config_provider: ConfigProvider,
    key_service: KeyService,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,

    /// Host bookkeeping for blob transitions (upload progress, materialize
    /// progress, completion). Passed to the [`SyncManager`] and to the upload
    /// drain. `None` for a host that doesn't surface transition progress.
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// Built lazily by [`connect_sync`](Self::connect_sync) when a provider is
    /// connected; `None` for a home-less, all-Local library. Shared behind a lock
    /// so a connect/disconnect mutates it in place without rebuilding the handle.
    sync: Arc<RwLock<Option<Arc<SyncManager>>>>,
}

impl CovenHandle {
    /// Build the handle over an already-open [`Database`] and the library's
    /// directory. Does no I/O and builds no sync manager — a home-less library is
    /// fully usable (rows + Local blobs) without one. Call
    /// [`connect_sync`](Self::connect_sync) when a cloud provider is connected.
    ///
    /// `config_provider` is read fresh on every call that needs the current
    /// config (the cloud-home selection, the blob-path scheme), so the host can
    /// reconnect a provider without rebuilding the handle. `observer` carries the
    /// host's transition bookkeeping; pass `None` if it surfaces none.
    pub(crate) fn new(
        db: Database,
        stamper: crate::sync::hlc::UpdatedAtStamper,
        library_dir: LibraryDir,
        config_provider: ConfigProvider,
        key_service: KeyService,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
    ) -> Self {
        Self {
            db,
            stamper,
            library_dir,
            config_provider,
            key_service,
            clock,
            cloudkit_ops,
            observer,
            sync: Arc::new(RwLock::new(None)),
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

    pub(crate) fn stamper(&self) -> crate::sync::hlc::UpdatedAtStamper {
        self.stamper.clone()
    }

    pub(crate) fn library_dir(&self) -> LibraryDir {
        self.library_dir.clone()
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// The connected [`SyncManager`], or `None` for a home-less library or one
    /// whose provider has not been connected yet. The host reaches sync-engine
    /// operations not surfaced as handle methods (membership, invite/remove,
    /// status) through this.
    pub(crate) fn sync_manager(&self) -> Option<Arc<SyncManager>> {
        self.sync.read().unwrap().clone()
    }

    pub fn subscribe_sync_status(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<SyncLoopStatus>, String> {
        let manager = self
            .sync_manager()
            .ok_or_else(|| "sync is not configured".to_string())?;
        let loop_handle = manager
            .sync_loop_handle()
            .ok_or_else(|| "sync loop is not running".to_string())?;
        Ok(loop_handle.subscribe())
    }

    /// Build the [`SyncManager`] for a connected cloud provider, start its sync
    /// loop, and install it. Returns the started manager, or an error if the cloud
    /// home fails to build — in which case nothing is installed, so the handle
    /// never holds a manager that reports success with nothing started.
    ///
    /// `encryption_service` is `Some` for an opaque home (sealed under the library
    /// key) and `None` for a browsable one (stored in the clear). Reconnecting a
    /// provider rebuilds the manager — the [`Database`] keeps the seeded register
    /// clock across the rebuild, so only the cloud home + loop are replaced.
    pub async fn connect_sync(
        &self,
        encryption_service: Option<EncryptionService>,
    ) -> Result<(), String> {
        self.build_and_install_sync(encryption_service, |manager| async move {
            manager.start_sync().await
        })
        .await?;
        info!("coven handle: sync manager connected");
        Ok(())
    }

    pub async fn connect_sync_with_cloudkit(
        &self,
        encryption_service: Option<EncryptionService>,
        cloudkit_ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), String> {
        let manager = Arc::new(SyncManager::new(
            self.config_provider.clone(),
            self.key_service.clone(),
            encryption_service,
            self.db.clone(),
            self.clock.clone(),
            Some(cloudkit_ops),
            self.observer.clone(),
        ));
        manager.start_sync().await?;
        *self.sync.write().unwrap() = Some(manager);
        info!("coven handle: sync manager connected with CloudKit driver");
        Ok(())
    }

    /// Build a [`SyncManager`], start its loop via `start`, and install it — the
    /// shared construct-and-install both [`connect_sync`](Self::connect_sync) and
    /// the test-only
    /// [`connect_sync_with_test_home`](Self::connect_sync_with_test_home) run,
    /// parameterized by the `encryption_service` the manager reports and which
    /// start method `start` invokes.
    ///
    /// Start before installing: a failed start (the cloud home fails to build, or a
    /// test home's bootstrap fails) returns its error with nothing installed, so the
    /// handle is left home-less rather than holding a manager whose loop never
    /// started.
    async fn build_and_install_sync<F, Fut>(
        &self,
        encryption_service: Option<EncryptionService>,
        start: F,
    ) -> Result<Arc<SyncManager>, String>
    where
        F: FnOnce(Arc<SyncManager>) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let manager = Arc::new(SyncManager::new(
            self.config_provider.clone(),
            self.key_service.clone(),
            encryption_service,
            self.db.clone(),
            self.clock.clone(),
            self.cloudkit_ops.clone(),
            self.observer.clone(),
        ));
        start(manager.clone()).await?;
        *self.sync.write().unwrap() = Some(manager.clone());
        Ok(manager)
    }

    /// Test-only: connect a started [`SyncManager`] over an injected [`CloudHome`]
    /// instead of one built from [`Config`] via `create_cloud_home`, so a host's
    /// integration tests drive the real make-Remote / make-Local / upload-drain and
    /// read paths over a mock cloud with no live provider.
    ///
    /// The test counterpart of [`connect_sync`](Self::connect_sync): it stands the
    /// manager over `home`/`cipher` through
    /// [`SyncManager::start_sync_with_home`], starts the loop, and installs it with
    /// the same start-before-install discipline — a failed connect leaves the
    /// handle home-less rather than holding a manager whose loop never started. The
    /// encryption service the manager reports (for `blob_cipher` / membership) is
    /// taken from the injected `cipher`, the single source of at-rest protection on
    /// the test path.
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
    ) -> Result<(), String> {
        // The test supplies encryption through the cipher, not a separate service;
        // derive the manager's service from it so `blob_cipher` and the membership
        // path agree with the at-rest protection the loop and storage seal under.
        let encryption_service = match &cipher {
            CloudCipher::Encrypted(service) => Some(service.clone()),
            CloudCipher::Plaintext => None,
        };
        self.build_and_install_sync(encryption_service, move |manager| async move {
            manager.start_sync_with_home(home, cipher).await
        })
        .await?;
        info!("coven handle: sync manager connected over an injected test cloud home");
        Ok(())
    }

    /// Start (or restart) the sync loop of the installed [`SyncManager`]. A no-op
    /// when no provider is connected — a home-less library has nothing to start.
    /// Errors if the installed manager's cloud home fails to build.
    pub async fn start_sync(&self) -> Result<(), String> {
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
            Some(manager) => manager.stop_sync(),
            None => debug!("stop_sync: no provider connected; nothing to stop"),
        }
    }

    /// Disconnect the provider entirely: stop the loop and drop the installed
    /// [`SyncManager`]. The library becomes home-less until the next
    /// [`connect_sync`](Self::connect_sync).
    pub fn disconnect_sync(&self) {
        if let Some(manager) = self.sync_manager() {
            manager.stop_sync();
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

    /// Whether the sync loop is running. `false` for a home-less library.
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

    /// The active [`EncryptionService`] for the connected opaque home, or `None`
    /// for a home-less library or a connected browsable home (stored in the
    /// clear).
    pub fn encryption_service(&self) -> Option<EncryptionService> {
        self.sync_manager().and_then(|m| m.encryption_service())
    }

    // =========================================================================
    // Blobs
    // =========================================================================

    /// The read [`SyncStorage`] for coven's locality-aware read, or `None` for a
    /// home-less library: `Some(home)` when a provider is connected, `None` when
    /// none is. coven reaches storage only on a cloud miss — a Remote blob not yet
    /// cached. A Local blob (the only kind a home-less library has) is served from
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
    /// (sync configured but not enabled) falls back to building from config, as
    /// does a home-less library (provider `None` ⇒ `None`).
    async fn blob_storage(&self) -> Result<Option<Arc<dyn SyncStorage>>, String> {
        if let Some(manager) = self.sync_manager() {
            if let Some(loop_handle) = manager.sync_loop_handle() {
                let storage: Arc<dyn SyncStorage> = loop_handle.storage().clone();
                return Ok(Some(storage));
            }
        }
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(None);
        }
        let storage = crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
            &config,
            &self.key_service,
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
        let storage = self.blob_storage().await.map_err(BlobCacheError::Io)?;
        crate::blob::cache::read_blob(&self.db, &self.library_dir, storage.as_deref(), blob).await
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
        let storage = self.blob_storage().await.map_err(BlobCacheError::Io)?;
        crate::blob::cache::open_blob_stream(
            &self.db,
            &self.library_dir,
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
        let storage = self.blob_storage().await.map_err(BlobCacheError::Io)?;
        crate::blob::cache::pin(&self.db, &self.library_dir, storage.as_deref(), blobs).await
    }

    /// Unpin a Remote blob set: coven moves each from `storage/pinned/` to the
    /// evictable `storage/cache/` (still readable, now droppable). No cloud read.
    pub async fn unpin(&self, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
        crate::blob::cache::unpin(&self.library_dir, blobs).await
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
        CloudSyncStorage::blob_key(
            scheme,
            &blob.namespace,
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
            if !crate::blob::cache::is_pinned(&self.library_dir, &blob.namespace, &blob.id).await? {
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
        crate::blob::cache::drop_all_local_copies(&self.library_dir, &blob.namespace, &blob.id)
            .await
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
    pub async fn drain_uploads(&self) -> Result<DrainOutcome, String> {
        let manager = self
            .sync_manager()
            .ok_or("drain_uploads: no provider connected")?;
        let cloud_home = manager
            .cloud_home()
            .ok_or("drain_uploads: no cloud home connected")?;
        let cipher = manager
            .blob_cipher()
            .ok_or("drain_uploads: no blob cipher (locked library)")?;
        let cipher = RwLock::new(cipher);
        let hlc = self.db.hlc();
        crate::blob::upload::drain_uploads(
            &self.db,
            cloud_home.as_ref(),
            &cipher,
            &self.library_dir,
            self.clock.as_ref(),
            &hlc,
            self.observer.as_deref(),
        )
        .await
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

    pub async fn mint_item_key(&self, item_id: &str) -> Result<[u8; 32], crate::DbError> {
        self.db.mint_item_key(item_id).await
    }

    pub async fn item_key(&self, item_id: &str) -> Result<Option<[u8; 32]>, crate::DbError> {
        self.db.item_key(item_id).await
    }

    pub fn get_user_pubkey(&self) -> Result<Option<String>, String> {
        self.key_service
            .get_user_public_key()
            .map(|opt| opt.map(hex::encode))
            .map_err(|e| format!("Failed to read user public key: {e}"))
    }

    pub fn generate_restore_code(&self) -> Result<String, String> {
        crate::storage::cloud::setup::generate_restore_code(&self.config(), &self.key_service)
            .map_err(|e| e.to_string())
    }

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, String> {
        let manager = self
            .sync_manager()
            .ok_or_else(|| "sync is not configured".to_string())?;
        manager.get_members().await
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
    ) -> Result<String, String> {
        let manager = self
            .sync_manager()
            .ok_or_else(|| "sync is not configured".to_string())?;
        manager
            .invite_member(public_key_hex, invitee_email, role)
            .await
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, String> {
        let manager = self
            .sync_manager()
            .ok_or_else(|| "sync is not configured".to_string())?;
        manager.remove_member(public_key_hex).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{BlobScope, CacheFill, Provenance};
    use crate::clock::SystemClock;
    use crate::config::{Config, HomeStorage};
    use crate::keys::{test_keyring, KeyService};
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::CloudCipher;
    use crate::sync::sync_manager::ConfigProvider;
    use crate::sync::test_helpers::{plant_blob_row, read_test_db, temp_library_dir};

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

        let (tmp, library_dir) = temp_library_dir();
        // `note_photos` carries a blob in the `images` namespace so the read path can
        // resolve a planted row up to its gated `notes` root (the gate that decides
        // Local vs Remote).
        let db = read_test_db("images");

        // A browsable home: plaintext at rest, readable `{namespace}/{cloud_path}`
        // blob keys. The cipher passed below is the matching `Plaintext`.
        let mut config = Config::with_defaults(
            "lib-test".to_string(),
            "test-device".to_string(),
            library_dir.clone(),
            "Test Library".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Browsable;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };

        let stamper = db.stamper();
        let handle = CovenHandle::new(
            db.clone(),
            stamper,
            library_dir,
            config_provider,
            KeyService::new("lib-test".to_string()),
            Arc::new(SystemClock),
            None,
            None,
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
        plant_blob_row(&db, "cover-1", true).await;

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
}

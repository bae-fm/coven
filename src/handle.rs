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
//! It is the native counterpart of the browser [`CovenLibrary`](crate::wasm_facade::CovenLibrary):
//! same role, different substrate. The browser stack runs single-threaded on the
//! event loop with a [`WasmSyncRuntime`](crate::sync::wasm_runtime::WasmSyncRuntime);
//! the native stack runs on tokio with a [`SyncManager`], `Send + Sync`
//! throughout. The blob engine ([`crate::blob`]) and the row store
//! ([`Database`]) are shared between them; the sync lifecycle is not, which is
//! why this is a distinct type rather than a shared core both wrap.
//!
//! ## What it owns
//!
//! - **Rows** — the [`Database`] (coven already owns the connection). The host
//!   runs its app SQL through [`database`](CovenHandle::database)`().call(|conn| …)`.
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
use crate::blob::local_files::LocalBlobError;
use crate::blob::transition::{MakeLocalError, MakeRemoteError};
use crate::blob::upload::DrainOutcome;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::KeyService;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::setup::create_sync_storage;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::storage::{StorageError, SyncStorage};
use crate::sync::sync_manager::{ConfigProvider, SyncManager};

/// The native handle over one coven library.
///
/// Construct it once with [`new`](Self::new), then call methods. Cheap to
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
/// # use coven::{blob::BlobRef, CovenHandle};
/// # async fn use_library(handle: &CovenHandle, cover: &BlobRef)
/// #     -> Result<(), Box<dyn std::error::Error>> {
/// // Rows: run app SQL on the connection coven owns.
/// let note_count: i64 = handle
///     .database()
///     .call(|conn| {
///         conn.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
///             .map_err(coven::database::DbError::from)
///     })
///     .await?;
///
/// // Blobs: read by descriptor. coven resolves locality — the user's own file,
/// // its local store, the cache, or a cloud fetch — and hands back plaintext.
/// let bytes: Vec<u8> = handle.read_blob(cover).await?;
/// // Store host-provided bytes that coven then owns.
/// handle.store_blob("images", "release-1", &bytes).await?;
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
    library_dir: LibraryDir,

    /// Supplies the host's current config on demand. coven reads it fresh each
    /// call so a host with reactive config sees changes without rebuilding the
    /// handle. The same provider the [`SyncManager`] reads from.
    config_provider: ConfigProvider,
    key_service: KeyService,
    clock: ClockRef,

    /// Host bookkeeping for blob transitions (upload progress, materialize
    /// progress, completion). Passed to the [`SyncManager`] and to the upload
    /// drain. `None` for a host that doesn't surface transition progress.
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// Built lazily by [`connect_sync`](Self::connect_sync) when a provider is
    /// connected; `None` for a home-less, all-Local library. Shared behind a lock
    /// so a connect/disconnect mutates it in place without rebuilding the handle.
    sync: Arc<RwLock<Option<Arc<SyncManager>>>>,

    /// A test-only read [`SyncStorage`] a host injects via
    /// [`set_test_storage`](Self::set_test_storage), so a host's tests route their
    /// blob reads through the handle against a storage backed by an injected mock
    /// cloud home instead of one built from [`Config`]. When set it takes
    /// precedence in [`blob_storage`](Self::blob_storage); production never has it.
    #[cfg(any(test, feature = "test-utils"))]
    test_storage: Arc<RwLock<Option<Arc<dyn SyncStorage>>>>,
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
    pub fn new(
        db: Database,
        library_dir: LibraryDir,
        config_provider: ConfigProvider,
        key_service: KeyService,
        clock: ClockRef,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
    ) -> Self {
        Self {
            db,
            library_dir,
            config_provider,
            key_service,
            clock,
            observer,
            sync: Arc::new(RwLock::new(None)),
            #[cfg(any(test, feature = "test-utils"))]
            test_storage: Arc::new(RwLock::new(None)),
        }
    }

    fn config(&self) -> Config {
        (self.config_provider)()
    }

    // =========================================================================
    // Rows
    // =========================================================================

    /// The owned [`Database`]. The host runs its app SQL through
    /// [`Database::call`] and reaches coven's row-level helpers (cache budgets,
    /// external blob refs, item keys) on it. coven owns the connection; this is
    /// how the host reaches it.
    pub fn database(&self) -> &Database {
        &self.db
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// The connected [`SyncManager`], or `None` for a home-less library or one
    /// whose provider has not been connected yet. The host reaches sync-engine
    /// operations not surfaced as handle methods (membership, invite/remove,
    /// status) through this.
    pub fn sync_manager(&self) -> Option<Arc<SyncManager>> {
        self.sync.read().unwrap().clone()
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
    ) -> Result<Arc<SyncManager>, String> {
        let manager = Arc::new(SyncManager::new(
            self.config_provider.clone(),
            self.key_service.clone(),
            encryption_service,
            self.db.clone(),
            self.clock.clone(),
            self.observer.clone(),
        ));
        // Start before installing: a cloud-home build failure must leave the handle
        // home-less, not holding a dead manager.
        manager.start_sync().await?;
        *self.sync.write().unwrap() = Some(manager.clone());
        info!("coven handle: sync manager connected");
        Ok(manager)
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
    /// home-less. A test storage injected via
    /// [`set_test_storage`](Self::set_test_storage) takes precedence.
    async fn blob_storage(&self) -> Result<Option<Arc<dyn SyncStorage>>, String> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(storage) = self.test_storage.read().unwrap().clone() {
            return Ok(Some(storage));
        }
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(None);
        }
        let storage =
            create_sync_storage(&config, &self.key_service, None, self.clock.clone()).await?;
        Ok(Some(Arc::new(storage)))
    }

    /// Inject a read [`SyncStorage`] a host's tests route blob reads through,
    /// bypassing the [`Config`]-built cloud home so a test can supply storage
    /// backed by a mock cloud home the handle could not otherwise see. Takes
    /// precedence in [`blob_storage`](Self::blob_storage). Test-only seam;
    /// production never sets it.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_test_storage(&self, storage: Arc<dyn SyncStorage>) {
        *self.test_storage.write().unwrap() = Some(storage);
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

    /// Store a host-provided blob's bytes in coven's local store
    /// (`storage/local/<namespace>/<id>`). coven owns the copy from here: it
    /// serves it locally while the blob is Local and moves it into the cache when
    /// the blob is made Remote. The host writes the blob-bearing row separately.
    pub async fn store_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> Result<(), LocalBlobError> {
        crate::blob::local_files::store(&self.library_dir, namespace, id, bytes).await
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
        crate::blob::cache::drop_cached_blob(&self.library_dir, &blob.namespace, &blob.id).await?;
        crate::blob::local_files::drop_blob(&self.library_dir, &blob.namespace, &blob.id).await?;
        Ok(())
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
}

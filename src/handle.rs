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

use async_trait::async_trait;
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
use crate::sync::storage::{DeviceHead, MinSchemaVersion, StorageError, SyncStorage};
use crate::sync::sync_manager::{ConfigProvider, SyncManager};

/// The native handle over one coven library.
///
/// Construct it once with [`new`](Self::new), then call methods. Cheap to
/// [`clone`](Clone) — every field is shared (an `Arc`, a `Clone` handle, or a
/// reference-counted lock), so a clone drives the same database, sync manager,
/// and storage as the original.
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
    /// loop, and install it. Returns the started manager.
    ///
    /// `encryption_service` is `Some` for an opaque home (sealed under the library
    /// key) and `None` for a browsable one (stored in the clear). Reconnecting a
    /// provider rebuilds the manager — the [`Database`] keeps the seeded register
    /// clock across the rebuild, so only the cloud home + loop are replaced.
    pub async fn connect_sync(
        &self,
        encryption_service: Option<EncryptionService>,
    ) -> Arc<SyncManager> {
        let manager = Arc::new(SyncManager::new(
            self.config_provider.clone(),
            self.key_service.clone(),
            encryption_service,
            self.db.clone(),
            self.clock.clone(),
            self.observer.clone(),
        ));
        manager.start_sync().await;
        *self.sync.write().unwrap() = Some(manager.clone());
        info!("coven handle: sync manager connected");
        manager
    }

    /// Start (or restart) the sync loop of the installed [`SyncManager`]. A no-op
    /// when no provider is connected — a home-less library has nothing to start.
    pub async fn start_sync(&self) {
        match self.sync_manager() {
            Some(manager) => manager.start_sync().await,
            None => debug!("start_sync: no provider connected; nothing to start"),
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

    /// A [`SyncStorage`] for coven's locality-aware read: the configured cloud
    /// home when a provider is connected, else an [`OfflineSyncStorage`] stub.
    ///
    /// coven reaches storage only on a cloud miss — a Remote blob not yet cached.
    /// A Local blob (the only kind a home-less library has) is served from its
    /// external ref or the local store without ever touching it, so the stub
    /// stands in for a uniform read path. A provider that IS configured but whose
    /// storage fails to build (missing credentials, a bad cipher) surfaces that
    /// error rather than masking it as offline — a Remote read against the stub
    /// would otherwise report a misleading "no cloud home".
    async fn blob_storage(&self) -> Result<Box<dyn SyncStorage>, String> {
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(Box::new(OfflineSyncStorage));
        }
        let storage =
            create_sync_storage(&config, &self.key_service, None, self.clock.clone()).await?;
        Ok(Box::new(storage))
    }

    /// Read a blob's whole plaintext through coven's locality-aware read: served
    /// from the user's file (Local user-provided), coven's local store (Local
    /// host-provided), the pinned/evictable cache on a Remote hit, or fetched
    /// from the cloud (into the cache) on a Remote miss. The host passes only the
    /// [`BlobRef`]; coven holds the database, the directory, and the storage.
    pub async fn read_blob(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobCacheError> {
        let storage = self.blob_storage().await.map_err(BlobCacheError::Io)?;
        crate::blob::cache::read_blob(&self.db, &self.library_dir, storage.as_ref(), blob).await
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
            storage.as_ref(),
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
        crate::blob::cache::pin(&self.db, &self.library_dir, storage.as_ref(), blobs).await
    }

    /// Unpin a Remote blob set: coven moves each from `storage/pinned/` to the
    /// evictable `storage/cache/` (still readable, now droppable). No cloud read.
    pub async fn unpin(&self, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
        crate::blob::cache::unpin(&self.library_dir, blobs).await
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

/// A [`SyncStorage`] with no backing cloud home: every operation errors. The
/// storage argument to coven's locality-aware read for a home-less library, where
/// only the never-reached cloud-miss branch would call it. Reaching a method means
/// a Remote blob was read with no home — a real fault, surfaced rather than masked.
struct OfflineSyncStorage;

impl OfflineSyncStorage {
    fn err() -> StorageError {
        StorageError::S3("no cloud home configured".to_string())
    }
}

#[async_trait]
impl SyncStorage for OfflineSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        Err(Self::err())
    }
    async fn get_changeset(&self, _device_id: &str, _seq: u64) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn put_changeset(
        &self,
        _device_id: &str,
        _seq: u64,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn put_head(
        &self,
        _device_id: &str,
        _seq: u64,
        _snapshot_seq: Option<u64>,
        _timestamp: &str,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn put_blob(
        &self,
        _namespace: &str,
        _id: &str,
        _scope: crate::blob::ResolvedScope,
        _cloud_path: Option<&str>,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_blob(
        &self,
        _namespace: &str,
        _id: &str,
        _scope: crate::blob::ResolvedScope,
        _cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn read_blob_range(
        &self,
        _namespace: &str,
        _id: &str,
        _scope: crate::blob::ResolvedScope,
        _cloud_path: Option<&str>,
        _source_size: u64,
        _offset: u64,
        _len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn put_snapshot(
        &self,
        _author: &str,
        _seq: u64,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_snapshot(&self, _author: &str, _seq: u64) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn delete_changeset(&self, _device_id: &str, _seq: u64) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn list_changesets(&self, _device_id: &str) -> Result<Vec<u64>, StorageError> {
        Err(Self::err())
    }
    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        Err(Self::err())
    }
    async fn set_min_schema_version(&self, _version: u32) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn put_membership_entry(
        &self,
        _author_pubkey: &str,
        _seq: u64,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_membership_entry(
        &self,
        _author_pubkey: &str,
        _seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        Err(Self::err())
    }
    async fn put_wrapped_key(
        &self,
        _user_pubkey: &str,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_wrapped_key(&self, _user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn delete_wrapped_key(&self, _user_pubkey: &str) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn put_snapshot_meta(
        &self,
        _author: &str,
        _seq: u64,
        _data: Vec<u8>,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_snapshot_meta(&self, _author: &str, _seq: u64) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn put_snapshot_pointer(&self, _data: Vec<u8>) -> Result<(), StorageError> {
        Err(Self::err())
    }
    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
        Err(Self::err())
    }
    async fn list_own_snapshot_generations(&self, _author: &str) -> Result<Vec<u64>, StorageError> {
        Err(Self::err())
    }
    async fn delete_snapshot_generation(
        &self,
        _author: &str,
        _seq: u64,
    ) -> Result<(), StorageError> {
        Err(Self::err())
    }
}

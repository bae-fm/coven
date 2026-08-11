//! The data handle: one object a host constructs once that owns coven's
//! pieces and exposes the whole data interface as methods.
//!
//! coven owns the store's data — SQL rows and blobs, on disk first, cloud
//! optional. A host (a desktop/mobile app) talks to coven through this one
//! handle and never assembles coven's internals by hand or hands them back to
//! coven on every call. The handle delegates to retained owners for rows,
//! blobs, sync, security, membership, joining, recovery, and Circles; the
//! caller passes only descriptors (a [`BlobRef`], SQL, or a config).
//!
//! The stack runs on Tokio and is `Send + Sync` throughout.
//!
//! ## What it owns
//!
//! - **Rows** — SQL execution and row-and-blob writes.
//! - **Blobs** — exact row-bound reads, cache policy, locality transitions, and
//!   upload visibility.
//! - **Sync** — connection lifecycle, status, and explicit synchronization.
//! - **Security** — key custody, device identity, host secrets, and app-data
//!   sealing.
//! - **Membership, joining, recovery, and Circles** — their complete host
//!   workflows, each behind its retained domain owner.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::store_blobs::StoreBlobAccess;
use crate::store_blobs::StoreBlobs;
use crate::store_circles::StoreCircles;
use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_joining::StoreJoining;
use crate::store_membership::StoreMembership;
use crate::store_recovery::StoreRecovery;
use crate::store_rows::StoreRows;
use crate::store_security::StoreSecurity;
use crate::store_sync::{ConfigProvider, StoreSync, SyncError};
use coven_database::{Database, DbError, StoreDatabase};
use coven_foundation::clock::ClockRef;
use coven_foundation::store_dir::StoreDir;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_keys::encryption::SealError;
use coven_keys::keys::{
    DeviceIdentityCustody, IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys,
};
use coven_protocol::blob::DrainOutcome;
use coven_protocol::blob::{BlobRef, BlobTransitionObserver, RowBlobRef};
use coven_protocol::membership::MemberInfo;
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::StorageError;
use coven_replication::blob::transition::{LocalBlobTransitions, MakeLocalError, MakeRemoteError};
use coven_replication::sync::store::blob::{LocalStoreBlobAccess, StoreBlobCache};
use coven_replication::sync::sync_loop::SyncLoopStatus;
use coven_replication::sync::{BlobCacheError, BlobStream};
#[cfg(any(test, feature = "test-utils"))]
use coven_storage::cloud::ExactCloudHome;
#[cfg(any(test, feature = "test-utils"))]
use coven_storage::CloudCipher;
use tokio::sync::watch;

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
/// The handle over one coven store.
///
/// Open it once with [`Coven::builder`](crate::Coven::builder), then call methods. Cheap to
/// [`clone`](Clone) — every field is shared (an `Arc`, a `Clone` handle, or a
/// reference-counted lock), so a clone drives the same retained owners as the
/// original.
///
/// # Using the handle
///
/// The host builds the handle once at startup and then only calls methods on it
/// — it never assembles coven's internals by hand or hands them back to coven on
/// every call. Rows go through the connection coven owns; blobs go through the
/// handle's read/store methods; sync is optional.
///
/// ```no_run
/// # use coven::{CovenHandle, RowBlobRef};
/// # async fn use_store(handle: &CovenHandle, cover: &RowBlobRef)
/// #     -> Result<(), Box<dyn std::error::Error>> {
/// // Rows: run app SQL on the connection coven owns.
/// let note_count = handle
///     .sql(|sql| {
///         sql.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
///             .map_err(coven::CovenError::from)
///     })
///     .await?;
/// let note_count: i64 = note_count.value;
///
/// // Blobs: read an exact row version. coven resolves locality — the user's own
/// // file, its local store, the cache, or a cloud fetch — and returns plaintext.
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
    rows: StoreRows,
    blobs: StoreBlobs,
    security: StoreSecurity,
    sync: StoreSync,
    membership: StoreMembership,
    joining: StoreJoining,
    recovery: StoreRecovery,
    circles: StoreCircles,
}

impl CovenHandle {
    /// Build the handle over an already-open [`Database`] and the store's
    /// directory. Does no I/O and opens no sync connection — a home-less store
    /// is fully usable (rows + Local blobs). Call
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
        store_dir: StoreDir,
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        key_custody: Arc<dyn MasterKeyCustody>,
        identity_custody: Arc<dyn DeviceIdentityCustody>,
        oauth_clients: coven_storage::oauth::OAuthClients,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        blob_chunking: coven_storage::BlobChunking,
    ) -> Self {
        let database = StoreDatabase::from_database(db);
        let cloud_homes =
            coven_storage::cloud::CloudHomeFactory::new(key_service.clone(), oauth_clients);
        let security = StoreSecurity::new(key_service, key_custody.clone(), identity_custody);
        let cloud_storage = StoreCloudStorage::new(
            security.clone(),
            cloud_homes,
            clock.clone(),
            cloudkit_ops,
            blob_chunking,
        );
        let blob_cache = StoreBlobCache::new(database.clone(), store_dir.clone());
        let local_blob_access =
            LocalStoreBlobAccess::new(database.clone(), store_dir.clone(), blob_cache);
        let blob_access = StoreBlobAccess::new(
            database.clone(),
            config_provider.clone(),
            cloud_storage.clone(),
            local_blob_access.clone(),
        );
        let read_database = StoreDatabase::from_database(read_db);
        let local_blob_transitions = LocalBlobTransitions::new(database.clone(), store_dir.clone());
        let sync = StoreSync::new(
            config_provider,
            security.clone(),
            key_custody.clone(),
            database.clone(),
            store_dir.clone(),
            clock,
            observer,
            open_guard,
            cloud_storage,
            local_blob_access.clone(),
            blob_access.clone(),
            local_blob_transitions,
        );
        let rows = StoreRows::new(
            coven_database::StoreRowWrites::new(database.clone()),
            read_database,
            key_custody,
            sync.clone(),
        );
        let blobs = StoreBlobs::new(database.clone(), blob_access, local_blob_access);
        let membership = StoreMembership::new(sync.clone());
        let joining = StoreJoining::new(database.clone(), membership.clone(), sync.clone());
        let recovery = StoreRecovery::new(database.clone(), security.clone(), sync.clone());
        let circles = StoreCircles::new(
            database.clone(),
            membership.clone(),
            security.clone(),
            sync.clone(),
        );
        Self {
            rows,
            blobs,
            security,
            sync,
            membership,
            joining,
            recovery,
            circles,
        }
    }

    pub async fn sql<F, R>(&self, sql: F) -> crate::CovenResult<crate::WriteReceipt<R>>
    where
        F: for<'context, 'connection> FnOnce(
                crate::SqlContext<'context, 'connection>,
            ) -> crate::CovenResult<R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.rows.sql(sql).await
    }

    pub async fn sql_read<F, R>(&self, read: F) -> crate::CovenResult<R>
    where
        F: for<'connection> FnOnce(crate::SqlReadContext<'connection>) -> crate::CovenResult<R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.rows.read(read).await
    }

    pub async fn write<F, S, R>(
        &self,
        build: F,
        sql: S,
    ) -> crate::CovenResult<crate::WriteReceipt<R>>
    where
        F: FnOnce(&mut crate::WriteBatch) -> crate::CovenResult<()> + Send + 'static,
        S: for<'context, 'connection> FnOnce(
                crate::SqlContext<'context, 'connection>,
            ) -> crate::CovenResult<R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.rows.write(build, sql).await
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// Subscribe to the sync loop's [`SyncLoopStatus`] stream. The channel is
    /// owned by this handle, not the loop, so the receiver keeps working across a
    /// reconnect and may be created before any provider is connected (it starts
    /// receiving once a loop runs). Infallible for that reason — there is no loop
    /// state to check.
    ///
    /// The receiver immediately contains the current value. Intermediate values
    /// may be coalesced; `Synchronized.row_changes` is a refresh hint rather than a
    /// complete change stream.
    pub fn subscribe_sync_status(&self) -> tokio::sync::watch::Receiver<SyncLoopStatus> {
        self.sync.subscribe_status()
    }

    /// Writes that have shared rows and have not reached a published position.
    pub async fn pending_writes(&self) -> Result<Vec<crate::PendingWrite>, crate::CovenError> {
        self.rows
            .pending_writes()
            .await
            .map_err(crate::CovenError::from)
    }

    /// Writes stopped by a semantic publication fault and awaiting an explicit
    /// retry or discard decision.
    pub async fn blocked_writes(&self) -> Result<Vec<crate::PendingWrite>, crate::CovenError> {
        self.rows
            .blocked_writes()
            .await
            .map_err(crate::CovenError::from)
    }

    /// Requeue one blocked write for full production validation. A connected
    /// sync loop is woken after the durable transition.
    pub async fn retry_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::CovenError> {
        self.rows.retry_blocked_write(write_id).await
    }

    /// Atomically discard a blocked write and reverse every later unpublished
    /// shared write whose working-row state depends on it.
    pub async fn discard_blocked_write(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::CovenError> {
        self.rows.discard_blocked_write(write_id).await
    }

    /// Read the current durable status of one write.
    pub async fn write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<crate::WriteStatus, crate::CovenError> {
        self.rows
            .write_status(write_id)
            .await
            .map_err(crate::CovenError::from)
    }

    /// Subscribe to one write's current durable status. The initial value is
    /// reconstructed from SQLite before the receiver is returned.
    pub async fn subscribe_write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<tokio::sync::watch::Receiver<crate::WriteStatus>, crate::CovenError> {
        self.rows
            .subscribe_write_status(write_id)
            .await
            .map_err(crate::CovenError::from)
    }

    /// Build the connected cloud storage, start its sync loop, and install the
    /// connection. If the cloud home fails to build, no connection is installed.
    ///
    /// The at-rest cipher is resolved from the handle's custody per start: an
    /// opaque home unlocks the master keyring (failing with
    /// [`SyncError::MasterKeyNotEstablished`] if none is established), a
    /// browsable one never consults custody. Reconnecting a provider replaces
    /// the cloud home and loop while retaining the Store database and clock.
    pub async fn connect_sync(&self) -> Result<(), SyncError> {
        self.sync.connect().await
    }

    /// Build and probe the cloud home described by `config` without installing
    /// it as this handle's sync connection. Hosts use this to validate proposed
    /// provider settings before committing them to their config source.
    pub async fn probe_cloud_home(&self, config: &crate::Config) -> Result<(), SyncError> {
        self.sync.probe_cloud_home(config).await
    }

    pub async fn connect_sync_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        self.sync.connect_with_cloudkit(cloudkit_ops).await
    }

    /// Test-only: connect a started sync loop over an injected [`ExactCloudHome`]
    /// instead of one built from [`crate::Config`], so a host's integration tests drive
    /// the real make-Remote / make-Local / upload-drain and read paths over a mock
    /// cloud with no live provider.
    ///
    /// The test counterpart of [`connect_sync`](Self::connect_sync): it builds
    /// storage over `home`/`cipher`, starts the loop, and installs the connection
    /// only after startup succeeds. The injected `cipher` is the at-rest
    /// protection directly; custody is never consulted on this path.
    ///
    /// The read path needs no separate hook: `blob_storage`
    /// serves reads from the connected loop's own `CloudSyncConnection`, which here
    /// wraps the injected `home`, so [`read_blob`](Self::read_blob) /
    /// [`pin`](Self::pin) resolve a Remote miss against the same test home the
    /// drain writes to.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn connect_sync_with_test_home(
        &self,
        home: Arc<dyn ExactCloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        self.sync.connect_with_test_home(home, cipher).await
    }

    /// Test-only: connect over an injected [`ExactCloudHome`] exactly as
    /// [`connect_sync_with_test_home`](Self::connect_sync_with_test_home) does,
    /// but start no background loop — the caller drives sync itself.
    ///
    /// The loop-started connect and an explicit
    /// [`drain_uploads`](Self::drain_uploads) are two drainers of one queue. Both
    /// see the same rows, both succeed, and whichever loses reports an empty
    /// queue — a host asserting on its own drain's count reads that as "nothing
    /// was queued" and fails intermittently. Here no cycle exists to race: the
    /// host's `drain_uploads` is the only drain, its count is the whole truth,
    /// and [`is_syncing`](Self::is_syncing) stays `false` for the connection's
    /// whole life.
    ///
    /// Everything a connected store can do is available — `make_remote`,
    /// `make_local`, the drain, membership — because none of it needs the loop
    /// thread. Circle *writes* are the exception: they are dispatched to that
    /// thread, so they refuse with
    /// [`CircleError::LoopNotRunning`](crate::CircleError::LoopNotRunning) here.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn connect_sync_with_test_home_caller_driven(
        &self,
        home: Arc<dyn ExactCloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        self.sync
            .connect_with_test_home_caller_driven(home, cipher)
            .await
    }

    /// Test-only: connect over an injected [`ExactCloudHome`] while resolving the
    /// at-rest cipher from custody the way production
    /// [`connect_sync`](Self::connect_sync) does, instead of taking an explicit
    /// cipher like [`connect_sync_with_test_home`](Self::connect_sync_with_test_home).
    ///
    /// Where that method injects the cipher and never touches custody, this drives
    /// the same connection path as production, which unlocks the master keyring
    /// through the store's custody exactly as `start_sync` would — so a
    /// test can establish a key, connect over a mock home, and prove the traffic
    /// is sealed under that key. An opaque home with no key established fails
    /// [`SyncError::MasterKeyNotEstablished`] before the loop starts.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn connect_sync_with_test_home_custody(
        &self,
        home: Arc<dyn ExactCloudHome>,
    ) -> Result<(), SyncError> {
        self.sync.connect_with_test_home_custody(home).await
    }

    /// Start (or restart) the sync loop of the installed connection. A no-op
    /// when no provider is connected — a home-less store has nothing to start.
    /// Errors if the connected cloud home fails to build.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        self.sync.start().await
    }

    /// Stop the sync loop after the in-flight cycle while keeping the provider
    /// connected so [`start_sync`](Self::start_sync) can resume it. A no-op when
    /// no provider is connected.
    ///
    /// The material a running loop resolved from custody (the master keyring,
    /// the device signing identity) is cached only inside that loop for as
    /// long as it runs — nowhere else in the handle — and this is where it is
    /// purged. A subsequent [`start_sync`](Self::start_sync)/
    /// [`connect_sync`](Self::connect_sync) re-resolves fresh from whatever
    /// custody now serves, so a host's lock flow that stops sync as part of
    /// locking, then later reconnects, never resumes on stale material.
    pub fn stop_sync(&self) {
        self.sync.stop()
    }

    /// Disconnect the provider entirely: stop the loop and drop the connection.
    /// The store becomes home-less until the next
    /// [`connect_sync`](Self::connect_sync).
    ///
    /// Carries the same purge as [`stop_sync`](Self::stop_sync), so nothing about
    /// the previous connection — including which custody it resolved material
    /// from — survives into the next connect.
    pub fn disconnect_sync(&self) {
        self.sync.disconnect()
    }

    /// Wake the sync loop to run a cycle now rather than at the next idle tick. A
    /// no-op when no provider is connected.
    pub fn sync_now(&self) {
        self.sync.trigger()
    }

    /// Whether the sync loop is running. `false` for a home-less store.
    pub fn is_syncing(&self) -> bool {
        self.sync.is_syncing()
    }

    /// Whether a provider connection is installed. Distinct
    /// from [`is_syncing`](Self::is_syncing), which additionally requires the loop
    /// to be running: this is the predicate a host uses for "has a cloud home"
    /// without the loop-ready condition.
    pub fn is_connected(&self) -> bool {
        self.sync.is_connected()
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
        self.security.initialize_master_key()
    }

    /// Import a serialized master keyring a host already holds and establish it
    /// under the handle's custody, replacing whatever custody already holds.
    /// Returns its fingerprint for the host to record in its own config.
    pub fn import_master_key(&self, serialized: &str) -> Result<String, MasterKeyError> {
        self.security.import_master_key(serialized)
    }

    /// The established master key's fingerprint, or `None` if custody has
    /// never had one established (or is locked, for a policy where that's
    /// representable).
    pub fn master_key_fingerprint(&self) -> Result<Option<String>, KeyError> {
        self.security.master_key_fingerprint()
    }

    // =========================================================================
    // Identity lifecycle
    // =========================================================================

    /// Generate this store's signing identity and establish it under the
    /// handle's identity custody. Errors with
    /// [`IdentityError::AlreadyEstablished`] if custody already unlocks one —
    /// coven never generates over an existing identity. The counterpart of
    /// [`initialize_master_key`](Self::initialize_master_key) for a store a
    /// host is creating fresh (not joining or restoring, which each establish
    /// their own identity as part of what they do). Returns the established
    /// public key, hex-encoded.
    pub fn initialize_identity(&self) -> Result<String, IdentityError> {
        self.security.initialize_identity()
    }

    // =========================================================================
    // Host secrets
    // =========================================================================

    /// Set a host's own store-scoped secret — an API token, a service
    /// credential — under the same platform keyring, and the same access
    /// policy, as coven's own key material. `name` identifies the secret
    /// within the store; coven owns the account rendering and the entry's
    /// protection class. [`KeyError::InvalidSecretName`] if `name` collides
    /// with one of coven's own reserved slot names, is empty, or contains
    /// `:`.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.security.set_host_secret(name, value)
    }

    /// Read a host secret set by [`set_host_secret`](Self::set_host_secret),
    /// `None` if never set. A present-but-empty entry is corrupt, not
    /// absent — the same discipline coven's own key reads apply.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.security.host_secret(name)
    }

    /// Remove a host secret. `Ok` whether or not one was set.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.security.delete_host_secret(name)
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
        self.security.seal_app_data(plaintext, aad)
    }

    /// Open a payload [`seal_app_data`](Self::seal_app_data) produced, under
    /// whichever generation it names — a rotated keyring still opens everything
    /// it sealed before rotating.
    ///
    /// [`SealError::Locked`] if the store is locked; a wrong `aad`, a tampered
    /// payload, an unreadable version, or a generation this store's keyring lacks
    /// each surface their own typed error.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        self.security.open_app_data(sealed, aad)
    }

    // =========================================================================
    // Blobs
    // =========================================================================

    /// Capture the exact current blob-bearing row version. Blob operations use
    /// this row-bound value so a later row replacement cannot redirect a read.
    pub async fn row_blob_ref(&self, table: &str, row_id: &str) -> Result<RowBlobRef, DbError> {
        self.blobs.row_blob_ref(table, row_id).await
    }

    /// Read a blob's whole plaintext through coven's locality-aware read: served
    /// from the user's file (Local user-provided), coven's local store (Local
    /// host-provided), the pinned/evictable cache on a Remote hit, or fetched
    /// from the cloud (into the cache) on a Remote miss. The host passes the
    /// [`RowBlobRef`] captured from [`row_blob_ref`](Self::row_blob_ref); coven
    /// holds the database, directory, and storage.
    pub async fn read_blob(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.blobs.read(blob).await
    }

    /// Ensure the exact current row blob plaintext is durable on this device.
    /// Remote blobs materialize into their locator-keyed cache path; Local and
    /// pending-remote blobs exact-verify their authoritative local source.
    pub async fn materialize_row_blob(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.blobs.materialize(blob).await
    }

    /// Open an exact row blob's plaintext for ranged reading, for streaming or
    /// seeking without loading the whole file. The ranged sibling of
    /// [`read_blob`](Self::read_blob), which stays the one-shot whole read.
    ///
    /// Opening resolves the blob's locality, proves the plaintext's size and
    /// content hash against the row, and holds the open file; every
    /// [`BlobStream::read_at`] then costs only the bytes it returns. Hold the
    /// stream for as long as the host is reading that blob — a stream per opened
    /// file, not per range — since re-opening re-proves the whole blob.
    pub async fn open_blob_stream(&self, blob: &RowBlobRef) -> Result<BlobStream, BlobCacheError> {
        self.blobs.open_stream(blob).await
    }

    /// Pin a Remote blob set for offline: coven fetches each into the protected
    /// cache (`storage/pinned/`) — from the evictable cache if already there, else
    /// the cloud — exempt from the size budget. Idempotent.
    pub async fn pin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.blobs.pin(blobs).await
    }

    /// Unpin a Remote blob set: coven moves each from `storage/pinned/` to the
    /// evictable `storage/cache/` (still readable, now droppable). No cloud read.
    pub async fn unpin(&self, blobs: &[RowBlobRef]) -> Result<(), BlobCacheError> {
        self.blobs.unpin(blobs).await
    }

    /// The cloud object key a blob's bytes live at, derived under the connected
    /// home's path scheme (`Hashed` → `{namespace}/{ab}/{cd}/{id}`, `Plain` →
    /// `{namespace}/{cloud_path}`).
    ///
    /// Read-only: coven owns this derivation and every operation that needs a key
    /// derives its own (a delete resolves it from the stored ref), so nothing a
    /// host calls takes one back. It exists so a host can *observe* the key coven
    /// would use — asserting an upload landed where a read looks for it, or
    /// naming an object in a diagnostic — without reimplementing the layout and
    /// drifting from it.
    ///
    /// A `Plain` home whose `cloud_path` is absent, or does not name the blob it
    /// carries, is a surfaced error — see `CloudSyncConnection::blob_key`.
    pub fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        self.sync.blob_cloud_key(blob)
    }

    /// Whether every blob in `blobs` is pinned for offline — present in coven's
    /// kept cache folder (`storage/pinned/`). The host answers "is this release
    /// kept offline" through this instead of stat-ing coven's cache layout itself.
    /// An empty set is vacuously pinned. A blob not pinned (in the evictable cache
    /// or absent) makes the whole set unpinned; an existence-check failure is
    /// surfaced, never read as "not pinned".
    pub async fn is_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.blobs.all_pinned(blobs).await
    }

    /// Remove one Remote blob's re-fetchable on-device cache copies from both
    /// `storage/pinned/` and `storage/cache/`. This never touches the local store,
    /// whose bytes may be the only usable copy owned by an unpublished write.
    /// It does not delete the cloud blob or its carrying row; a later read can
    /// fetch the bytes again.
    pub async fn evict_blob(&self, blob: &RowBlobRef) -> Result<(), BlobCacheError> {
        self.blobs.evict(blob).await
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
        self.sync.make_remote(root_table, root_id, pin).await
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
        self.sync.cancel_make_remote(root_table, root_id).await
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
        self.sync
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    /// Every upload the durable queue is holding, oldest first.
    ///
    /// An upload appears here the moment [`make_remote`](Self::make_remote)
    /// enqueues it — before any transfer is attempted, and whether or not sync
    /// is connected — and stays until its publication activates or its
    /// cancellation clears it. The queue is a table in the store database, so
    /// this survives restarts: a host can render "waiting to upload" without
    /// having observed the transfer that will do it.
    ///
    /// This is a read; nothing here starts or advances a transfer. Compare
    /// [`drain_uploads`](Self::drain_uploads), which does the work.
    ///
    /// To ask whether a *root* still has a transition running, prefer
    /// [`make_remote_progress`](Self::make_remote_progress): the queue empties
    /// before the transition ends.
    pub async fn queued_uploads(&self) -> Result<Vec<crate::QueuedUpload>, crate::DbError> {
        self.blobs.queued_uploads().await
    }

    /// The queued uploads belonging to one gated root.
    ///
    /// The filter runs in SQL, so asking about one root does not decode every
    /// other queued upload in the store. A host answers "is anything still
    /// waiting to upload for this row?" from whether this is empty — but see
    /// [`make_remote_progress`](Self::make_remote_progress) for whether the
    /// transition itself has finished, which outlasts its uploads.
    pub async fn queued_uploads_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<crate::QueuedUpload>, crate::DbError> {
        self.blobs
            .queued_uploads_for_root(root_table, root_id)
            .await
    }

    /// Where the user's own file for a row's blob lives on disk, or `None`
    /// when the row has no external registration.
    ///
    /// This is the read that mirrors
    /// [`SqlContext::register_external_blob`](crate::SqlContext::register_external_blob):
    /// a host that needs the original file itself — to re-read its tags, to
    /// find an artifact it produced — asks here rather than reading coven's
    /// copy, because for a user-provided blob there is no copy.
    ///
    /// `None` means no registration, which is an ordinary answer: a row whose
    /// blobs coven copies, or one whose registration was cleared, has no user
    /// file to name. A registration that disagrees with the row it belongs to
    /// is an error, not a `None`.
    pub async fn external_blob(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<Option<crate::ExternalBlob>, crate::DbError> {
        self.blobs.external_blob(table, row_id).await
    }

    /// Every cloud tombstone the durable queue is holding, oldest first.
    ///
    /// A tombstone is queued by
    /// [`SqlContext::enqueue_blob_delete`](crate::SqlContext::enqueue_blob_delete)
    /// and stays until a sync cycle carries the removal out, so this reports
    /// removals still owed to the cloud across restarts.
    pub async fn queued_deletes(&self) -> Result<Vec<crate::QueuedDelete>, crate::DbError> {
        self.blobs.queued_deletes().await
    }

    /// How far the make-remote for one gated root has got, or `None` when that
    /// root has none running.
    ///
    /// This outlasts the root's queued uploads. Once the last upload lands its
    /// queue rows are consumed, but the transition is not finished until the
    /// Store write publishing it activates — so a root can have no queued
    /// uploads and still be mid-transition, reported here as
    /// [`MakeRemoteProgress::Publishing`](crate::MakeRemoteProgress).
    pub async fn make_remote_progress(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<crate::MakeRemoteProgress>, crate::DbError> {
        self.blobs.make_remote_progress(root_table, root_id).await
    }

    /// Drain pending blob uploads now: read each local file, seal it under its
    /// scope, write it to the cloud, and keep a `retain_pinned` entry's plaintext
    /// in the protected cache.
    ///
    /// The sync loop drains each cycle; this drives a drain directly off the
    /// connected home, against coven's own register clock and the handle's
    /// observer. Errors when no provider is connected (there is no cloud to write
    /// to).
    ///
    /// The [`DrainOutcome`] says what the pass found, not just how much it moved:
    /// an empty queue, a queue held entirely in retry backoff, and a paused one
    /// are each their own answer rather than a zero count. A host that connects
    /// with a running loop has *two* drainers of one queue and gets whichever
    /// answer the race leaves it; `connect_sync_with_test_home_caller_driven`
    /// (test builds only) connects without a loop, so this call is the only
    /// drain.
    pub async fn drain_uploads(&self) -> Result<DrainOutcome, SyncError> {
        self.sync.drain_uploads().await
    }

    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, crate::DbError> {
        self.blobs.cache_budget(namespace).await
    }

    pub async fn set_cache_budget(
        &self,
        namespace: &str,
        max_bytes: u64,
    ) -> Result<(), crate::DbError> {
        self.blobs.set_cache_budget(namespace, max_bytes).await
    }

    /// Generate a restore code, seeded with the store's current membership-head
    /// floor read from the cloud. Requires a connected provider because minting
    /// a trustworthy floor is a network read, not a pure function of local
    /// config and keyring state — a restore code minted without one would carry
    /// no protection against a storage provider replaying an older, otherwise
    /// validly signed membership state to the device that redeems it.
    pub async fn generate_restore_code(&self) -> Result<String, SyncError> {
        self.recovery.generate_restore_code().await
    }

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        self.membership.members().await
    }

    pub async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, SyncError> {
        self.membership.conflict().await
    }

    /// Admit the device that generated `join_request_code`, and return the one
    /// payload that device needs: its invite code and this attempt's transport
    /// bundle.
    ///
    /// The joining device generates its join request first and shows it here —
    /// the offer is signed for that device's key, so it cannot be minted
    /// before this device knows it.
    pub async fn begin_device_invite(
        &self,
        join_request_code: &str,
        role: MemberRole,
    ) -> Result<coven_domain::joining::DeviceJoinInvite, SyncError> {
        self.joining.begin_invite(join_request_code, role).await
    }

    /// Drive the admitting side of a join this device issued, publishing each
    /// artifact it produces and waiting for the joining device's.
    ///
    /// Returns when the attempt reaches an end this side owns: its activation,
    /// or the abandonment that ended it early.
    pub async fn drive_device_join(
        &self,
        invite: &coven_domain::joining::DeviceJoinInvite,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        self.sync
            .drive_device_join(&invite.bundle, policy, access_administrator, timing)
            .await
    }

    /// Cancel an invited join and carry the unwind to its activated cleanup,
    /// publishing each artifact the joining device needs to close its own side.
    ///
    /// Which attempt this cancels comes from this device's own owner journal,
    /// which is what decided it. Retry the whole call if it fails: a Store
    /// commit that loses a race with this handle's sync loop is refused before
    /// it persists, and the unwind resumes from where its journal stands.
    ///
    /// The counterpart for a host delivering artifacts itself is
    /// [`cancel_device_join`](Self::cancel_device_join), which produces the
    /// cancellation and hands it back rather than publishing it.
    pub async fn cancel_device_invite(
        &self,
        invite: &crate::DeviceJoinInvite,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        self.sync
            .cancel_device_join_transport(&invite.bundle, timing)
            .await
    }

    /// Give up on an invited join and publish the abandonment, so a joining
    /// device waiting on its next artifact learns the join is over.
    ///
    /// The counterpart for a host delivering artifacts itself is
    /// [`abandon_device_join`](Self::abandon_device_join).
    pub async fn abandon_device_invite(
        &self,
        invite: &crate::DeviceJoinInvite,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        self.sync
            .abandon_device_join_transport(&invite.bundle)
            .await
    }

    pub async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        self.sync.begin_device_join(member_pubkey).await
    }

    pub async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        self.sync.abandon_device_join(offer).await
    }

    pub async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        self.sync
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub async fn accept_device_registration_request(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        self.sync.accept_device_registration(request).await
    }

    pub async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        self.sync.publish_device_provider_challenge(bootstrap).await
    }

    pub async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        self.sync
            .complete_device_provider_admission(readiness)
            .await
    }

    pub async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        self.sync.finalize_device_join(completion).await
    }

    pub async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, SyncError> {
        self.sync.cancel_device_join(attempt).await
    }

    pub async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        self.sync
            .close_device_provider_admission(cancellation)
            .await
    }

    pub async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        revocation_executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        self.sync
            .revoke_device_provider_admission_writes(cancellation, revocation_executor)
            .await
    }

    pub async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        revocation_executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, SyncError> {
        self.sync
            .revoke_joining_device_writes(cancellation, revocation_executor)
            .await
    }

    pub async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        self.sync.activate_device_join_cleanup(receipt).await
    }

    pub async fn complete_cancelled_device_join(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), SyncError> {
        self.sync
            .complete_owner_device_join_cleanup(activation)
            .await
    }

    pub async fn device_join_status(
        &self,
        attempt_id: crate::DeviceJoinAttemptId,
        role: crate::DeviceJoinRole,
    ) -> Result<Option<crate::DeviceJoinStatus>, SyncError> {
        self.joining.status(attempt_id, role).await
    }

    pub async fn resume_device_joins(&self) -> Result<Vec<crate::DeviceJoinAction>, SyncError> {
        self.joining.resumable_actions().await
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
    ) -> Result<String, SyncError> {
        self.membership
            .invite(public_key_hex, invitee_email, role)
            .await
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, SyncError> {
        self.membership.remove(public_key_hex).await
    }

    pub async fn resolve_membership_conflict(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), SyncError> {
        self.membership.resolve_conflict(choice).await
    }

    /// Propose excluding one Store device and return the code that identifies
    /// the exact activated proposal.
    pub async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<String, SyncError> {
        self.membership.propose_device_exclusion(device_id).await
    }

    /// Cancel the exact Store-device exclusion proposal carried by `proposal_code`.
    pub async fn cancel_device_exclusion(&self, proposal_code: &str) -> Result<(), SyncError> {
        self.membership.cancel_device_exclusion(proposal_code).await
    }

    /// Finalize the exact Store-device exclusion proposal carried by `proposal_code`.
    pub async fn finalize_device_exclusion(&self, proposal_code: &str) -> Result<(), SyncError> {
        self.membership
            .finalize_device_exclusion(proposal_code)
            .await
    }

    /// Begin transferring Store ownership to an active device and return the
    /// request code that device must accept.
    pub async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<String, SyncError> {
        self.membership.begin_owner_promotion(device_id).await
    }

    /// Accept an Owner-promotion request and return the acceptance code the
    /// existing Owner must finalize.
    pub async fn accept_owner_promotion(&self, request_code: &str) -> Result<String, SyncError> {
        self.membership.accept_owner_promotion(request_code).await
    }

    /// Finalize the Owner-promotion acceptance carried by `acceptance_code`.
    pub async fn finalize_owner_promotion(&self, acceptance_code: &str) -> Result<(), SyncError> {
        self.membership
            .finalize_owner_promotion(acceptance_code)
            .await
    }

    /// The Circle application surface: create, lifecycle, inspection, and typed
    /// [`CircleError`](crate::CircleError). A borrowed namespace with no state of
    /// its own.
    pub fn circles(&self) -> crate::Circles<'_> {
        crate::Circles::new(&self.circles)
    }

    // =========================================================================
    // Rows
    // =========================================================================

    #[cfg(test)]
    pub(crate) async fn create_test_store(
        &self,
        store_id: &str,
        signer: coven_keys::keys::UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<std::sync::Arc<coven_replication::sync::test_helpers::TestStore>, String> {
        self.sync.create_test_store(store_id, signer, home).await
    }

    #[cfg(test)]
    pub(crate) async fn install_test_active_circle(
        &self,
        label: &str,
    ) -> Result<crate::CircleId, coven_database::DbError> {
        self.circles.install_test_active_circle(label).await
    }

    #[cfg(test)]
    pub(crate) async fn publish_test_store(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
    ) -> Result<bool, String> {
        self.sync.publish_test_store(store).await
    }

    #[cfg(test)]
    pub(crate) async fn pull_test_store(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        coven_replication::sync::store::StorePullResult,
    ) {
        self.sync
            .pull_test_store(store)
            .await
            .expect("pull exact test Store")
    }

    #[cfg(test)]
    pub(crate) async fn store_write_partition_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<u8>, coven_database::DbError> {
        self.rows.store_write_partition_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_lease_count_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<i64, coven_database::DbError> {
        self.rows.write_blob_lease_count_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, coven_database::DbError> {
        self.rows
            .cleanup_intent_count_for_test(namespace, blob_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn coven_table_exists_for_test(
        &self,
        table: coven_database::DatabaseTestTable,
    ) -> Result<bool, coven_database::DbError> {
        self.rows.coven_table_exists_for_test(table).await
    }

    #[cfg(test)]
    pub(crate) async fn install_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), coven_database::DbError> {
        self.rows
            .install_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn remove_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), coven_database::DbError> {
        self.rows
            .remove_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_facts_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<String, coven_database::DbError> {
        self.rows.write_blob_facts_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn execute_sql_with_blob_staging_for_test(
        &self,
        blob_staging: Option<Box<dyn coven_database::AudienceBlobMoveStaging>>,
        sql: String,
    ) -> crate::CovenResult<crate::WriteReceipt<()>> {
        self.rows
            .execute_sql_with_blob_staging_for_test(blob_staging, sql)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), coven_database::DbError> {
        self.sync
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
        self.sync
            .arm_pull_after_remote_commit_for_test(device_id, sequence)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_test_join_snapshot(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
        owner: &coven_keys::keys::UserKeypair,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), String> {
        self.joining
            .prepare_test_join_snapshot(store, owner, snapshot_path)
            .await
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;

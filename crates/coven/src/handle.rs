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

use crate::blob::transition::{
    BlobTransitionJournal, LocalBlobTransitions, MakeLocalError, MakeRemoteError,
};
use crate::blob::upload::DrainOutcome;
use crate::blob::{BlobRef, BlobTransitionObserver, RowBlobRef};
use crate::clock::ClockRef;
use crate::coven::StoreOpenGuard;
use crate::database::{Database, DbError, StoreDatabase};
use crate::encryption::SealError;
use crate::keys::{
    DeviceIdentityCustody, IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys,
};
use crate::protocol::membership::MemberInfo;
use crate::protocol::membership::MemberRole;
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::CloudCipher;
use crate::storage::StorageError;
use crate::store_blobs::{ConnectedBlobStorage, StoreBlobReads, StoreBlobs};
use crate::store_circles::StoreCircles;
use crate::store_dir::StoreDir;
use crate::store_joining::StoreJoining;
use crate::store_membership::StoreMembership;
use crate::store_recovery::StoreRecovery;
use crate::store_rows::StoreRows;
use crate::store_security::StoreSecurity;
use crate::store_sync::{ConfigProvider, StoreSync, SyncError};
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobCache};
use crate::sync::sync_loop::SyncLoopStatus;
use crate::sync::{BlobCacheError, BlobStream};
use tokio::sync::watch;

/// A Remote blob read needs sync storage; if building it from config fails
/// (missing credentials or cloud configuration) the read surfaces that as a
/// configuration fault, not a disk I/O error. The cache error preserves the
/// setup failure's message at this API boundary.
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
        stamper: crate::sync::hlc::UpdatedAtStamper,
        store_dir: StoreDir,
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        key_custody: Arc<dyn MasterKeyCustody>,
        identity_custody: Arc<dyn DeviceIdentityCustody>,
        oauth_clients: crate::oauth::OAuthClients,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        blob_chunking: crate::storage::BlobChunking,
    ) -> Self {
        let database = StoreDatabase::from_database(db);
        let read_database = StoreDatabase::from_database(read_db);
        let cloud_homes =
            crate::storage::cloud::CloudHomeFactory::new(key_service.clone(), oauth_clients);
        let security = StoreSecurity::new(
            key_service.clone(),
            key_custody.clone(),
            identity_custody.clone(),
            cloud_homes,
        );
        let blob_cache = StoreBlobCache::new(database.clone(), store_dir.clone());
        let local_blob_access =
            LocalStoreBlobAccess::new(database.clone(), store_dir.clone(), blob_cache.clone());
        let local_blob_transitions = LocalBlobTransitions::new(
            BlobTransitionJournal::new(database.clone()),
            store_dir.clone(),
        );
        let sync = StoreSync::new(
            config_provider,
            security.clone(),
            database.clone(),
            store_dir.clone(),
            clock,
            cloudkit_ops,
            observer,
            open_guard,
            blob_chunking,
            local_blob_access.clone(),
            local_blob_transitions,
        );
        let rows = StoreRows::new(
            database.clone(),
            read_database,
            store_dir.clone(),
            stamper,
            security.clone(),
            sync.clone(),
        );
        let blob_reads = StoreBlobReads::new(
            local_blob_access,
            blob_cache,
            ConnectedBlobStorage::new(sync.clone()),
        );
        let blobs = StoreBlobs::new(database.clone(), blob_reads, sync.clone());
        let membership = StoreMembership::new(security.clone(), sync.clone());
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

    // =========================================================================
    // Rows
    // =========================================================================

    #[cfg(test)]
    pub(crate) async fn create_test_store(
        &self,
        store_id: &str,
        signer: crate::keys::UserKeypair,
    ) -> Result<crate::sync::test_helpers::TestStore, String> {
        self.sync.create_test_store(store_id, signer).await
    }

    #[cfg(test)]
    pub(crate) async fn install_test_active_circle(
        &self,
        label: &str,
    ) -> Result<crate::CircleId, crate::database::DbError> {
        self.circles.install_test_active_circle(label).await
    }

    #[cfg(test)]
    pub(crate) async fn publish_test_store(
        &self,
        store: &crate::sync::test_helpers::TestStore,
    ) -> Result<bool, String> {
        self.sync.publish_test_store(store).await
    }

    #[cfg(test)]
    pub(crate) async fn pull_test_store(
        &self,
        store: &crate::sync::test_helpers::TestStore,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        self.sync
            .pull_test_store(store)
            .await
            .expect("pull exact test Store")
    }

    #[cfg(test)]
    pub(crate) async fn write_changeset_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<Vec<u8>, crate::database::DbError> {
        self.rows.write_changeset_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_lease_count_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<i64, crate::database::DbError> {
        self.rows.write_blob_lease_count_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_intent_count_for_test(
        &self,
        namespace: String,
        blob_id: String,
    ) -> Result<i64, crate::database::DbError> {
        self.rows
            .cleanup_intent_count_for_test(namespace, blob_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn coven_table_exists_for_test(
        &self,
        table: crate::database::DatabaseTestTable,
    ) -> Result<bool, crate::database::DbError> {
        self.rows.coven_table_exists_for_test(table).await
    }

    #[cfg(test)]
    pub(crate) async fn install_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), crate::database::DbError> {
        self.rows
            .install_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn remove_store_write_failure_trigger_for_test(
        &self,
    ) -> Result<(), crate::database::DbError> {
        self.rows
            .remove_store_write_failure_trigger_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_blob_facts_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<String, crate::database::DbError> {
        self.rows.write_blob_facts_for_test(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn execute_sql_with_blob_staging_for_test(
        &self,
        blob_staging: Option<crate::sync::store::HostWriteBlobStaging>,
        sql: String,
    ) -> crate::CovenResult<crate::WriteReceipt<()>> {
        self.rows
            .execute_sql_with_blob_staging_for_test(blob_staging, sql)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), crate::database::DbError> {
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
        store: &crate::sync::test_helpers::TestStore,
        owner: &crate::keys::UserKeypair,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), String> {
        self.joining
            .prepare_test_join_snapshot(store, owner, snapshot_path)
            .await
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

    pub async fn connect_sync_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        self.sync.connect_with_cloudkit(cloudkit_ops).await
    }

    /// Test-only: connect a started sync loop over an injected [`CloudHome`]
    /// instead of one built from [`Config`], so a host's integration tests drive
    /// the real make-Remote / make-Local / upload-drain and read paths over a mock
    /// cloud with no live provider.
    ///
    /// The test counterpart of [`connect_sync`](Self::connect_sync): it builds
    /// storage over `home`/`cipher`, starts the loop, and installs the connection
    /// only after startup succeeds. The injected `cipher` is the at-rest
    /// protection directly; custody is never consulted on this path.
    ///
    /// The read path needs no separate hook: `blob_storage`
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
        self.sync.connect_with_test_home(home, cipher).await
    }

    /// Test-only: connect over an injected [`CloudHome`] while resolving the
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
        home: Arc<dyn CloudHome>,
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
    /// carries, is a surfaced error — see [`CloudSyncStorage::blob_key`].
    pub fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        self.blobs.cloud_key(blob)
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
        self.blobs.make_remote(root_table, root_id, pin).await
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
        self.blobs.cancel_make_remote(root_table, root_id).await
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
        self.blobs
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    /// Drain pending blob uploads now: read each local file, seal it under its
    /// scope, write it to the cloud, and keep a `retain_pinned` entry's plaintext
    /// in the protected cache. Returns the [`DrainOutcome`].
    ///
    /// The sync loop drains each cycle; this drives a drain directly off the
    /// connected home, against coven's own register clock and the handle's
    /// observer. Errors when no provider is connected (there is no cloud to write
    /// to).
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

    pub async fn drain_uploads(&self) -> Result<DrainOutcome, SyncError> {
        self.blobs.drain_uploads().await
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
    ) -> Result<crate::joining::DeviceJoinInvite, SyncError> {
        self.joining.begin_invite(join_request_code, role).await
    }

    /// Drive the admitting side of a join this device issued, publishing each
    /// artifact it produces and waiting for the joining device's.
    ///
    /// Returns when the attempt reaches an end this side owns: its activation,
    /// or the abandonment that ended it early.
    pub async fn drive_device_join(
        &self,
        invite: &crate::joining::DeviceJoinInvite,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        self.joining
            .drive(invite, policy, access_administrator, timing)
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
        self.joining.cancel_invite(invite, timing).await
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
        self.joining.abandon_invite(invite).await
    }

    pub async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        self.joining.begin(member_pubkey).await
    }

    pub async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        self.joining.abandon(offer).await
    }

    pub async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        self.joining
            .authorize_provider_access(request, access_administrator)
            .await
    }

    pub async fn accept_device_registration_request(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        self.joining.accept_registration(request).await
    }

    pub async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        self.joining.publish_provider_challenge(bootstrap).await
    }

    pub async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        self.joining.complete_provider_admission(readiness).await
    }

    pub async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        self.joining.finalize(completion).await
    }

    pub async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, SyncError> {
        self.joining.cancel(attempt).await
    }

    pub async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        self.joining.close_provider_admission(cancellation).await
    }

    pub async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        revocation_executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        self.joining
            .revoke_provider_writes(cancellation, revocation_executor)
            .await
    }

    pub async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        revocation_executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, SyncError> {
        self.joining
            .revoke_joiner_writes(cancellation, revocation_executor)
            .await
    }

    pub async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        self.joining.activate_cleanup(receipt).await
    }

    pub async fn complete_cancelled_device_join(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), SyncError> {
        self.joining.complete_cancelled(activation).await
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{CacheFill, Provenance};
    use crate::clock::SystemClock;
    use crate::config::{CloudProvider, Config, HomeStorage};
    use crate::encryption::EncryptionService;
    use crate::keys::{test_keyring, StoreKeys};
    use crate::storage::cloud::cloudkit::{
        CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps,
        CloudKitProviderIdentity, CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope,
        CloudKitShare,
    };
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::CloudHomeError;
    use crate::storage::{BlobPathScheme, CloudCipher};
    use crate::store_sync::{ConfigProvider, SyncError};
    use crate::sync::test_helpers::{
        open_test_db_with_blob, plant_blob_row, read_test_db, temp_store_dir, TestStore,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, RwLock};
    use std::time::Duration;

    type TestCloudKitCoordinate = (CloudKitScope, String);
    type TestCloudKitObject = (Vec<u8>, u64);

    struct TestCloudKitOps {
        store: Mutex<HashMap<TestCloudKitCoordinate, TestCloudKitObject>>,
        shares: Mutex<HashMap<String, CloudKitShare>>,
        batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
        next_batch: AtomicUsize,
    }

    /// A ready-to-use custody for tests that build a [`CovenHandle`] directly
    /// (bypassing the builder) and never exercise master-key lifecycle
    /// methods — the blob/storage/status tests in this module. Seeded
    /// in-memory so it needs no keyring registration.
    fn test_store_keys(store_id: &str) -> StoreKeys {
        crate::keys::test_keyring::install();
        StoreKeys::bind(store_id.to_string())
    }

    fn test_key_custody() -> Arc<dyn crate::keys::MasterKeyCustody> {
        let store_keys = test_store_keys("unused-store-id");
        crate::custody::KeyCustody::InMemory(crate::encryption::MasterKeyring::generate()).resolve(
            &store_keys,
            &crate::store_dir::StoreDir::new("unused-store-dir"),
        )
    }

    /// A ready-to-use identity custody for the same tests, seeded in-memory
    /// so it needs no keyring registration — the identity sibling of
    /// [`test_key_custody`].
    fn test_identity_custody() -> Arc<dyn DeviceIdentityCustody> {
        let store_keys = test_store_keys("unused-store-id");
        crate::identity_custody::IdentityCustody::InMemory(crate::keys::UserKeypair::generate())
            .resolve(
                &store_keys,
                &crate::store_dir::StoreDir::new("unused-store-dir"),
            )
    }

    fn host_blob_test_db(namespace: &str) -> Database {
        open_test_db_with_blob(
            crate::sync::session::BlobDecl::new(
                namespace,
                Provenance::HostProvided,
                CacheFill::CacheLazy,
            )
            .with_cloud_path_column("cloud_path"),
        )
    }

    struct PausedUploadDrain {
        paused: std::sync::atomic::AtomicBool,
        reached: tokio::sync::Notify,
    }

    impl PausedUploadDrain {
        fn new() -> Self {
            Self {
                paused: std::sync::atomic::AtomicBool::new(true),
                reached: tokio::sync::Notify::new(),
            }
        }

        fn resume(&self) {
            self.paused
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::blob::BlobTransitionObserver for PausedUploadDrain {
        async fn on_blob_upload_started(&self, _blob_id: &str) {}

        async fn on_blob_uploaded(&self, _blob_id: &str) {}

        async fn on_blob_upload_failed(&self, _blob_id: &str, _error: &str) {}

        fn should_skip_uploads(&self) -> bool {
            let paused = self.paused.load(std::sync::atomic::Ordering::SeqCst);
            if paused {
                self.reached.notify_one();
            }
            paused
        }
    }

    async fn queue_host_blob(
        handle: &CovenHandle,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
        remote: bool,
    ) -> crate::WriteId {
        let note_id = format!("note-{id}");
        let id = id.to_string();
        let cloud_path = cloud_path.to_string();
        let bytes = bytes.to_vec();
        let size = bytes.len() as i64;
        let hash = crate::blob::content_hash(&bytes);
        let write = handle
            .write(
                {
                    let id = id.clone();
                    let bytes = bytes.clone();
                    move |batch| {
                        batch.put_blob("images", id, bytes);
                        Ok(())
                    }
                },
                {
                    let id = id.clone();
                    move |sql| {
                        let stamp = sql.stamp();
                        sql.execute(
                            "INSERT INTO notes \
                             (id, title, shared, _updated_at, created_at) \
                             VALUES (?1, 'blob owner', ?2, ?3, '2026-01-01')",
                            rusqlite::params![note_id, remote as i64, stamp],
                        )?;
                        sql.execute(
                            "INSERT INTO note_photos \
                             (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
                             VALUES (?1, ?2, 'cover', ?3, ?4, ?5, '2026-01-01', ?6)",
                            rusqlite::params![id, note_id, size, hash, stamp, cloud_path],
                        )?;
                        Ok(())
                    }
                },
            )
            .await
            .expect("queue host blob write");
        write.write_id
    }

    async fn wait_for_host_blob_publication(
        handle: &CovenHandle,
        id: &str,
        write_id: &crate::WriteId,
    ) -> RowBlobRef {
        let mut status = handle
            .subscribe_write_status(write_id)
            .await
            .expect("subscribe to host blob publication");
        handle.sync_now();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let current = status.borrow().clone();
                match current {
                    crate::WriteStatus::Published(_) => break,
                    crate::WriteStatus::Pending | crate::WriteStatus::Publishing => status
                        .changed()
                        .await
                        .expect("write status channel remains open"),
                    other => panic!("host blob write did not publish: {other:?}"),
                }
            }
        })
        .await
        .expect("host blob publishes");
        handle
            .row_blob_ref("note_photos", id)
            .await
            .expect("capture published host blob row")
    }

    async fn publish_host_blob(
        handle: &CovenHandle,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> RowBlobRef {
        let write_id = queue_host_blob(handle, id, cloud_path, bytes, true).await;
        wait_for_host_blob_publication(handle, id, &write_id).await
    }

    #[tokio::test]
    async fn read_blob_with_unbuildable_storage_is_a_typed_setup_error_not_io() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");
        let mut config = Config::with_defaults(
            "lib-setup-error".to_string(),
            "device".to_string(),
            store_dir.clone(),
            "Test".to_string(),
        );
        // A provider is selected but its bucket is unset, so the read path cannot
        // build sync storage. That is a configuration fault the user must fix — it
        // must reach the caller as StorageSetup, not be mislabeled as disk I/O.
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
            StoreKeys::bind("lib-setup-error".to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        plant_blob_row(&db, "anyblob0", false, b"typed setup error").await;
        let blob = db
            .row_blob_ref("note_photos", "anyblob0")
            .await
            .expect("capture local blob row");
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
        test_handle_with_custody_and_storage(
            store_id,
            store_dir,
            db,
            test_key_custody(),
            HomeStorage::Browsable,
        )
    }

    fn test_handle_with_custody(
        store_id: &str,
        store_dir: StoreDir,
        db: Database,
        key_custody: crate::custody::KeyCustody,
    ) -> CovenHandle {
        let store_keys = test_store_keys(store_id);
        let key_custody = key_custody.resolve(&store_keys, &store_dir);
        test_handle_with_custody_and_storage(
            store_id,
            store_dir,
            db,
            key_custody,
            HomeStorage::Opaque,
        )
    }

    fn test_handle_with_custody_and_storage(
        store_id: &str,
        store_dir: StoreDir,
        db: Database,
        key_custody: Arc<dyn crate::keys::MasterKeyCustody>,
        storage: HomeStorage,
    ) -> CovenHandle {
        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = storage;
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
            StoreKeys::bind(store_id.to_string()),
            key_custody,
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        )
    }

    impl TestCloudKitOps {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
                shares: Mutex::new(HashMap::new()),
                batches: Mutex::new(HashMap::new()),
                next_batch: AtomicUsize::new(0),
            }
        }
    }

    impl CloudKitOps for TestCloudKitOps {
        fn provider_identity(
            &self,
            scope: &CloudKitScope,
        ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
            let (owner_name, zone_name) = match scope {
                CloudKitScope::Private => ("test-owner", "test-zone"),
                CloudKitScope::Shared {
                    owner_name,
                    zone_name,
                } => (owner_name.as_str(), zone_name.as_str()),
            };
            Ok(CloudKitProviderIdentity {
                container_id: "iCloud.test.coven".to_string(),
                environment: crate::CloudKitEnvironment::Development,
                owner_name: owner_name.to_string(),
                zone_name: zone_name.to_string(),
                current_user_record_name: "test-user".to_string(),
            })
        }

        fn accepted_read_write_share(
            &self,
            _scope: &CloudKitScope,
        ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
            Err(CloudHomeError::NotFound(
                "accepted CloudKit share".to_string(),
            ))
        }

        fn write_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
            data: Vec<u8>,
        ) -> Result<(), CloudHomeError> {
            let mut store = self.store.lock().unwrap();
            let coordinate = (scope.clone(), key.to_string());
            let version = store.get(&coordinate).map_or(1, |(_, version)| version + 1);
            store.insert(coordinate, (data, version));
            Ok(())
        }

        fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .get(&(scope.clone(), key.to_string()))
                .map(|(bytes, _)| bytes.clone())
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

        fn read_versioned_record(
            &self,
            scope: &CloudKitScope,
            key: &str,
        ) -> Result<crate::storage::cloud::CloudVersionedObject, CloudHomeError> {
            let store = self.store.lock().unwrap();
            let (bytes, version) = store
                .get(&(scope.clone(), key.to_string()))
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
            Ok(crate::storage::cloud::CloudVersionedObject {
                bytes: bytes.clone(),
                version: crate::storage::cloud::CloudObjectVersion::from_provider(
                    version.to_string(),
                )?,
            })
        }

        fn begin_atomic_create(
            &self,
            _scope: &CloudKitScope,
        ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
            let batch = CloudKitAtomicCreateBatch::from_provider(format!(
                "handle-batch-{}",
                self.next_batch.fetch_add(1, Ordering::SeqCst)
            ))?;
            self.batches
                .lock()
                .unwrap()
                .insert(batch.as_provider().to_string(), Vec::new());
            Ok(batch)
        }

        fn stage_atomic_create_record(
            &self,
            _scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
            create: CloudKitRecordCreate,
        ) -> Result<(), CloudHomeError> {
            self.batches
                .lock()
                .unwrap()
                .get_mut(batch.as_provider())
                .ok_or_else(|| CloudHomeError::NotFound(batch.as_provider().to_string()))?
                .push(create);
            Ok(())
        }

        fn commit_atomic_create(
            &self,
            scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
        ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
            let mut batches = self.batches.lock().unwrap();
            let creates = batches
                .get(batch.as_provider())
                .ok_or_else(|| CloudHomeError::NotFound(batch.as_provider().to_string()))?;
            let mut store = self.store.lock().unwrap();
            for create in creates {
                if store.contains_key(&(scope.clone(), create.key.clone())) {
                    return Err(CloudHomeError::AlreadyExists(create.key.clone()));
                }
            }
            let creates = batches
                .remove(batch.as_provider())
                .expect("validated handle CloudKit batch disappeared");
            let mut created = Vec::with_capacity(creates.len());
            for create in creates {
                store.insert((scope.clone(), create.key.clone()), (create.data, 1));
                created.push(CloudKitRecordVersion {
                    key: create.key,
                    version: crate::storage::cloud::CloudObjectVersion::from_provider(
                        "1".to_string(),
                    )?,
                });
            }
            Ok(created)
        }

        fn discard_atomic_create(
            &self,
            _scope: &CloudKitScope,
            batch: &CloudKitAtomicCreateBatch,
        ) -> Result<(), CloudHomeError> {
            self.batches.lock().unwrap().remove(batch.as_provider());
            Ok(())
        }

        fn delete_record_versions(
            &self,
            scope: &CloudKitScope,
            exact_records: &[CloudKitRecordVersion],
        ) -> Result<(), CloudHomeError> {
            let mut store = self.store.lock().unwrap();
            for record in exact_records {
                let coordinate = (scope.clone(), record.key.clone());
                let (_, version) = store
                    .get(&coordinate)
                    .ok_or_else(|| CloudHomeError::NotFound(record.key.clone()))?;
                if version.to_string() != record.version.as_provider() {
                    return Err(CloudHomeError::Transport(format!(
                        "handle CloudKit record {:?} changed before exact deletion",
                        record.key
                    )));
                }
            }
            for record in exact_records {
                store.remove(&(scope.clone(), record.key.clone()));
            }
            Ok(())
        }

        fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
            let share = CloudKitShare {
                share_url: format!("coven-test-share-{member_pubkey}"),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            };
            self.shares
                .lock()
                .unwrap()
                .insert(member_pubkey.to_string(), share.clone());
            Ok(share)
        }

        fn share_for_member(
            &self,
            member_pubkey: &str,
        ) -> Result<Option<CloudKitShare>, CloudHomeError> {
            Ok(self.shares.lock().unwrap().get(member_pubkey).cloned())
        }

        fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError> {
            self.shares.lock().unwrap().remove(member_pubkey);
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

    /// `connect_sync_with_test_home` starts the production sync loop over an injected
    /// `InMemoryCloudHome`. A host write creates a pending exact Store row/blob;
    /// the public drain uploads its prepared blob object, the next cycle publishes
    /// the row with its exact locator, and `read_blob` uses that row-bound locator
    /// to read the same object through the handle.
    #[tokio::test]
    async fn test_home_drives_drain_and_read_through_the_handle() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_test_home_drives_drain_and_read_through_the_handle())
                    .await
                    .expect("test-home handle task");
            })
            .await;
    }

    async fn run_test_home_drives_drain_and_read_through_the_handle() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        // `note_photos` carries a blob in the `images` namespace so the read path can
        // resolve a planted row up to its gated `notes` root (the gate that decides
        // Local vs Remote).
        let db = host_blob_test_db("images");

        // Pre-create the exact Store in the same home the handle will connect to,
        // with the same signing identity and cipher.
        let mut config = Config::with_defaults(
            "lib-test".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Opaque;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let upload_pause = Arc::new(PausedUploadDrain::new());
        let signer = crate::keys::UserKeypair::generate();
        let store = TestStore::create(&db, "lib-test", signer.clone())
            .await
            .expect("create exact test Store");
        let store_keys = test_store_keys("lib-test");
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(signer)
            .resolve(&store_keys, &store_dir);

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
            store_keys,
            test_key_custody(),
            identity_custody,
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            Some(upload_pause.clone()),
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        // Inject the mock home; the host hands over only the home + cipher.
        let home = store.home.clone();
        handle
            .connect_sync_with_test_home(
                home.clone(),
                CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            )
            .await
            .expect("connect over the injected test home");

        // The loop prepares the exact blob upload from the pending Store write,
        // then the observer pauses before it can drain the queue itself.
        let plaintext = b"cover-art-bytes-for-the-test-home".to_vec();
        queue_host_blob(&handle, "cover-1", "cover-cover-1.jpg", &plaintext, false).await;
        handle
            .make_remote("notes", "note-cover-1", false)
            .await
            .expect("queue the exact row/blob transition");
        tokio::time::timeout(Duration::from_secs(20), upload_pause.reached.notified())
            .await
            .expect("the loop reaches the paused upload drain");
        let local = handle
            .row_blob_ref("note_photos", "cover-1")
            .await
            .expect("capture Local row while upload is paused");
        assert!(
            matches!(local.authority(), crate::blob::RowBlobAuthority::Local),
            "the row stays Local until the exact upload completes",
        );
        assert!(local.stored().is_none());

        upload_pause.resume();
        let outcome = handle
            .drain_uploads()
            .await
            .expect("drain the prepared exact blob through the public handle");
        assert_eq!(outcome.uploaded, 1);
        assert!(outcome.yielded_for_publish);
        assert!(outcome.failures.failures().is_empty());

        let blob = handle
            .row_blob_ref("note_photos", "cover-1")
            .await
            .expect("capture Remote row after exact upload");
        let object = blob
            .stored()
            .expect("published blob has exact storage")
            .object();
        let exact = home
            .clone()
            .exact_slot_storage()
            .expect("test home supports exact object slots");
        let at_rest = exact
            .read_at(object.slot())
            .await
            .expect("the exact blob object exists");
        assert!(
            !at_rest.is_empty(),
            "the exact blob object contains its sealed payload",
        );

        // The published `RowBlobRef` carries the exact remote object and authority;
        // the read resolves it through the same connected home.
        let read = handle
            .read_blob(&blob)
            .await
            .expect("read through the handle");
        assert_eq!(
            read, plaintext,
            "read_blob fetched the blob's plaintext from the injected test home",
        );
    }

    /// The chunk size a host sets through
    /// [`CovenBuilder::blob_chunking`](crate::CovenBuilder::blob_chunking) is what
    /// the connected sync storage seals under. The receipt is the stored object's
    /// own header: it names the configured size, so the setting decides how little
    /// a later ranged read can fetch. A connect path that builds its connection or its
    /// storage on `BlobChunking::DEFAULT` instead seals at 64 KiB and this fails.
    #[tokio::test]
    async fn connected_seal_honors_the_handles_configured_blob_chunking() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_connected_seal_honors_the_handles_configured_chunking(),
                )
                .await
                .expect("configured-chunking handle task");
            })
            .await;
    }

    async fn run_connected_seal_honors_the_handles_configured_chunking() {
        test_keyring::install();

        // Distinctive on both axes: neither number is `BlobChunking::DEFAULT`'s
        // (64 KiB chunk, 1 MiB window), so a dropped configuration is visible
        // rather than coinciding with the default.
        const CHUNK: u32 = 4096;
        let chunking = crate::storage::BlobChunking::new(
            std::num::NonZeroU32::new(CHUNK).expect("nonzero chunk"),
            std::num::NonZeroU64::new(1 << 16).expect("nonzero window"),
        );

        let (_tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");

        let mut config = Config::with_defaults(
            "lib-chunking".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.storage = HomeStorage::Opaque;
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let signer = crate::keys::UserKeypair::generate();
        let store = TestStore::create(&db, "lib-chunking", signer.clone())
            .await
            .expect("create exact test Store");
        let store_keys = test_store_keys("lib-chunking");
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(signer)
            .resolve(&store_keys, &store_dir);
        // Holds the loop off the upload queue so this test's explicit
        // `drain_uploads` is the call that seals the object.
        let upload_pause = Arc::new(PausedUploadDrain::new());

        let handle = CovenHandle::new(
            db.clone(),
            // `read_db`: this test never calls `sql_read`, and the test db is
            // `:memory:` (no shareable read-only companion), so the writer clone
            // stands in.
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            store_keys,
            test_key_custody(),
            identity_custody,
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            Some(upload_pause.clone()),
            StoreOpenGuard::acquire_for_test(&store_dir),
            chunking,
        );

        let home = store.home.clone();
        handle
            .connect_sync_with_test_home(
                home.clone(),
                CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            )
            .await
            .expect("connect over the injected test home");

        // Several chunks' worth of plaintext, so the configured size frames the
        // object rather than fitting inside one chunk either way.
        let plaintext: Vec<u8> = (0..3 * CHUNK as usize + 17)
            .map(|value| (value % 251) as u8)
            .collect();
        queue_host_blob(&handle, "cover-1", "cover-cover-1.jpg", &plaintext, false).await;
        handle
            .make_remote("notes", "note-cover-1", false)
            .await
            .expect("queue the exact row/blob transition");
        tokio::time::timeout(Duration::from_secs(20), upload_pause.reached.notified())
            .await
            .expect("the loop reaches the paused upload drain");

        upload_pause.resume();
        let outcome = handle
            .drain_uploads()
            .await
            .expect("drain the prepared exact blob through the public handle");
        assert_eq!(outcome.uploaded, 1);
        assert!(outcome.failures.failures().is_empty());

        let blob = handle
            .row_blob_ref("note_photos", "cover-1")
            .await
            .expect("capture Remote row after exact upload");
        let object = blob
            .stored()
            .expect("published blob has exact storage")
            .object();
        let exact = home
            .clone()
            .exact_slot_storage()
            .expect("test home supports exact object slots");
        let at_rest = exact
            .read_at(object.slot())
            .await
            .expect("the exact blob object exists");

        // `[key tag][header][chunks]` — the header the sealer wrote is what every
        // later reader frames the object by.
        let header =
            crate::encryption::SealedBlobHeader::parse(&at_rest[crate::storage::KEY_TAG_LEN..])
                .expect("stored blob carries a sealed header");
        assert_eq!(
            header.chunk_size().get(),
            CHUNK,
            "the sealed blob is framed at the chunking the handle was built with",
        );
        assert_eq!(header.plaintext_len(), plaintext.len() as u64);

        let read = handle
            .read_blob(&blob)
            .await
            .expect("read through the handle");
        assert_eq!(
            read, plaintext,
            "the blob sealed at the configured chunk size reads back whole",
        );
    }

    #[tokio::test]
    async fn connected_sync_reuses_connection_storage_for_loop() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");

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
            StoreKeys::bind("lib-cloudkit-home-reuse".to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            Some(Arc::new(TestCloudKitOps::new())),
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        handle
            .connect_sync()
            .await
            .expect("connect sync over the test CloudKit driver");

        assert!(
            handle.sync.loop_uses_connected_storage_for_test(),
            "StoreSync and its sync loop must retain the same storage instance",
        );
    }

    /// A read-only handle holds no sync loop, so every cloud-miss read builds
    /// storage fresh from config via the `cipher: None` path. The writer publishes
    /// a host-provided row and exact encrypted blob through the normal Store path;
    /// publication releases its local staging bytes, forcing the reader to use the
    /// row's exact cloud locator and resolve the same cipher through custody.
    #[tokio::test]
    async fn read_only_handle_resolves_an_encrypted_cipher_through_custody() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_read_only_handle_resolves_an_encrypted_cipher_through_custody(),
                )
                .await
                .expect("encrypted read-only handle task");
            })
            .await;
    }

    async fn run_read_only_handle_resolves_an_encrypted_cipher_through_custody() {
        test_keyring::install();

        let store_id = "ro-encrypted-custody-test";
        let (_tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");

        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::CloudKit);
        config.cloud_home.storage = HomeStorage::Opaque;

        let key_service = test_store_keys(store_id);
        let custody = crate::custody::KeyCustody::Keyring.resolve(&key_service, &store_dir);
        custody
            .persist(&crate::encryption::MasterKeyring::generate())
            .expect("establish a master key");

        // Exact opaque blob locators bind their uploader registration, so establish
        // the writer's signing identity before connecting storage.
        let identity_custody =
            crate::identity_custody::IdentityCustody::Keyring.resolve(&key_service, &store_dir);
        identity_custody
            .persist(&crate::keys::UserKeypair::generate())
            .expect("establish this store's signing identity");

        let ops = Arc::new(TestCloudKitOps::new());
        let config_provider: ConfigProvider = {
            let config = config.clone();
            Arc::new(move || config.clone())
        };
        let writer = CovenHandle::new(
            db.clone(),
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            key_service.clone(),
            custody.clone(),
            identity_custody.clone(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            Some(ops.clone()),
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );
        writer
            .connect_sync_with_cloudkit(ops.clone())
            .await
            .expect("connect encrypted CloudKit writer");
        let plaintext = b"encrypted-cloud-blob-for-the-read-only-handle".to_vec();
        let blob = publish_host_blob(&writer, "cover-1", "cover-cover-1.jpg", &plaintext).await;

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
            identity_custody,
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            Some(ops),
            crate::storage::BlobChunking::DEFAULT,
        );

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
        let handle = test_handle_with_custody(
            "lib-init-master-key-twice",
            store_dir,
            db,
            crate::custody::KeyCustody::Keyring,
        );

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
    /// the custody-resolving connect path — no cipher is injected; StoreSync
    /// unlocks the key exactly as production `start_sync` does — then enqueues
    /// and drains a blob. The bytes at rest in the home are ciphertext, never
    /// the plaintext (the assertion a browsable/plaintext home would fail),
    /// while `read_blob` decrypts them back. Only the established key sealing the
    /// upload makes both hold.
    #[tokio::test]
    async fn initialize_master_key_seals_cloud_traffic_the_custody_path_reads_back() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(Box::pin(
                    run_initialize_master_key_seals_cloud_traffic_the_custody_path_reads_back(),
                ))
                .await
                .expect("master-key cloud traffic test task");
            })
            .await;
    }

    async fn run_initialize_master_key_seals_cloud_traffic_the_custody_path_reads_back() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");
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

        let store_keys = test_store_keys(store_id);
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        let identity_custody =
            crate::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
        let handle = CovenHandle::new(
            db.clone(),
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            store_keys,
            custody,
            identity_custody,
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        handle
            .initialize_master_key()
            .expect("establish the master key before connecting");
        handle
            .initialize_identity()
            .expect("establish this store's identity before connecting");

        // Connect over the injected home through the custody path: StoreSync
        // resolves the cipher from the just-established key, never an injected
        // one. An opaque home with no key would fail here with
        // `MasterKeyNotEstablished`.
        let home = Arc::new(InMemoryCloudHome::new());
        let connect_handle = handle.clone();
        let connect_home = home.clone();
        tokio::task::spawn_local(async move {
            connect_handle
                .connect_sync_with_test_home_custody(connect_home)
                .await
        })
        .await
        .expect("join custody-resolved connection")
        .expect("connect over the injected opaque home, resolving the cipher from custody");

        // Publish a host-provided row and exact blob under the opaque home. The
        // resulting row reference carries its uploader authority and stored slot.
        let plaintext = b"cover-art-sealed-under-the-established-master-key".to_vec();
        let blob = publish_host_blob(&handle, "cover-1", "cover-cover-1.jpg", &plaintext).await;
        let cloud_key = blob
            .stored()
            .expect("published blob has exact storage")
            .object()
            .slot()
            .logical_key();

        // At rest the object is ciphertext: the stored bytes are not the
        // plaintext, and no object in the home holds the plaintext verbatim.
        let at_rest = home.get(cloud_key).expect("the blob landed in the home");
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

        // Read back through the row's activated exact locator and the same
        // custody-resolved cipher.
        let read = handle
            .read_blob(&blob)
            .await
            .expect("read through the handle");
        assert_eq!(
            read, plaintext,
            "read_blob decrypts the sealed blob back to its original plaintext",
        );
    }

    #[tokio::test]
    async fn import_master_key_rejects_raw_hex() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-import-master-key", store_dir, db);

        let raw_hex = hex::encode([0x22u8; 32]);
        assert!(handle.import_master_key(&raw_hex).is_err());
    }

    #[tokio::test]
    async fn import_master_key_accepts_the_current_serialized_keyring() {
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-import-master-key", store_dir, db);

        let keyring = crate::encryption::MasterKeyring::generate();
        let imported_fingerprint = handle
            .import_master_key(&keyring.to_serialized())
            .expect("import the serialized keyring");
        assert_eq!(imported_fingerprint, keyring.fingerprint());
        assert_eq!(
            handle.master_key_fingerprint().unwrap(),
            Some(imported_fingerprint),
        );
    }

    // =========================================================================
    // Identity lifecycle
    // =========================================================================

    /// A handle over a real (keyring-backed) identity custody, for tests that
    /// need to prove something about a store's *own* keyring account rather
    /// than the shared in-memory `test_identity_custody`.
    fn test_handle_with_real_identity(
        store_id: &str,
        store_dir: StoreDir,
        db: Database,
    ) -> CovenHandle {
        let config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let config_provider: ConfigProvider = Arc::new(move || config.clone());
        let store_keys = test_store_keys(store_id);
        let identity_custody =
            crate::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
        CovenHandle::new(
            db.clone(),
            db.clone(),
            db.stamper(),
            store_dir.clone(),
            config_provider,
            store_keys,
            test_key_custody(),
            identity_custody,
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        )
    }

    /// `initialize_identity` is the only place coven ever generates a
    /// store's signing identity, and it refuses to run again once one is
    /// established — coven never generates over an existing identity. The
    /// identity sibling of `initialize_master_key_refuses_a_second_call`.
    #[tokio::test]
    async fn initialize_identity_refuses_a_second_call() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle_with_real_identity("lib-init-identity-twice", store_dir, db);

        let pubkey = handle
            .initialize_identity()
            .expect("the first call establishes an identity");
        assert!(!pubkey.is_empty());

        let error = handle
            .initialize_identity()
            .expect_err("a second call must refuse rather than generate over an existing identity");
        assert!(matches!(
            error,
            crate::keys::IdentityError::AlreadyEstablished
        ));
    }

    /// Creating two stores on one device establishes two different
    /// identities — each store's `initialize_identity` generates its own
    /// keypair, under its own keyring account, independent of the other.
    #[tokio::test]
    async fn creating_two_stores_yields_two_different_identities() {
        test_keyring::install();
        let (_tmp_a, store_dir_a) = temp_store_dir();
        let (_tmp_b, store_dir_b) = temp_store_dir();
        let handle_a = test_handle_with_real_identity(
            "lib-two-stores-identity-a",
            store_dir_a,
            read_test_db("images"),
        );
        let handle_b = test_handle_with_real_identity(
            "lib-two-stores-identity-b",
            store_dir_b,
            read_test_db("images"),
        );

        let pubkey_a = handle_a
            .initialize_identity()
            .expect("establish store a's identity");
        let pubkey_b = handle_b
            .initialize_identity()
            .expect("establish store b's identity");

        assert_ne!(
            pubkey_a, pubkey_b,
            "two stores on one device must not share an identity",
        );
    }

    // =========================================================================
    // Host secrets
    // =========================================================================

    /// The host-facing round trip: `set_host_secret` / `host_secret` /
    /// `delete_host_secret` through the handle, with an absent secret
    /// reading `None` both before it's ever set and after it's deleted.
    #[tokio::test]
    async fn host_secret_round_trips_through_the_handle() {
        test_keyring::install();
        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-host-secret-round-trip", store_dir, db);

        assert_eq!(
            handle.host_secret("discogs_api_key").expect("get"),
            None,
            "an unset host secret reads as absent",
        );

        handle
            .set_host_secret("discogs_api_key", "the-discogs-key")
            .expect("set");
        assert_eq!(
            handle.host_secret("discogs_api_key").expect("get"),
            Some("the-discogs-key".to_string()),
        );

        handle
            .delete_host_secret("discogs_api_key")
            .expect("delete");
        assert_eq!(
            handle
                .host_secret("discogs_api_key")
                .expect("get after delete"),
            None,
        );
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
            crate::custody::KeyCustody::Keyring,
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
            crate::custody::KeyCustody::Keyring,
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
        let store_keys = test_store_keys(store_id);
        let key_custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        let reader = crate::read_handle::CovenReadHandle::new(
            db,
            store_dir.clone(),
            config_provider,
            store_keys,
            key_custody,
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            crate::storage::BlobChunking::DEFAULT,
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
            crate::custody::KeyCustody::Keyring,
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
    async fn plaintext_membership_operations_are_typed() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_plaintext_membership_operations_are_typed())
                    .await
                    .expect("plaintext membership test task");
            })
            .await;
    }

    async fn run_plaintext_membership_operations_are_typed() {
        await_test_orchestration(tokio::spawn(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = read_test_db("images");
            let handle = test_handle("lib-plaintext-membership", store_dir, db);
            handle
                .connect_sync_with_test_home(
                    Arc::new(InMemoryCloudHome::new()),
                    CloudCipher::Plaintext,
                )
                .await
                .expect("connect plaintext home");

            let public_key_hex = hex::encode(crate::keys::UserKeypair::generate().public_key());
            let invite = handle
                .invite_member(&public_key_hex, None, MemberRole::Member)
                .await;
            let remove = handle.remove_member(&public_key_hex).await;
            let circle = handle.circles().create("Household").await;

            assert!(matches!(invite, Err(SyncError::NotEncryptedHome)));
            assert!(matches!(remove, Err(SyncError::NotEncryptedHome)));
            assert!(
                matches!(&circle, Err(crate::CircleError::BrowsableStorage)),
                "{circle:?}"
            );
        }))
        .await;
    }

    async fn await_test_orchestration(task: tokio::task::JoinHandle<()>) {
        task.await.expect("test orchestration task completes");
    }

    #[tokio::test]
    async fn create_circle_returns_after_merge_activation_is_materialized() {
        await_test_orchestration(tokio::spawn(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = read_test_db("images");
            let keyring = crate::encryption::MasterKeyring::generate();
            let handle = test_handle_with_custody(
                "lib-create-circle-merge",
                store_dir,
                db.clone(),
                crate::custody::KeyCustody::InMemory(keyring.clone()),
            );
            handle
                .connect_sync_with_test_home(
                    Arc::new(InMemoryCloudHome::new()),
                    CloudCipher::Encrypted(EncryptionService::from(keyring)),
                )
                .await
                .expect("connect encrypted Merge home");

            let circle_id = handle
                .circles()
                .create("Household")
                .await
                .expect("create and activate circle");

            handle
                .circles()
                .rename(circle_id, "Household money")
                .await
                .expect("rename and activate circle");

            assert_eq!(
                handle.circles().list().await.expect("read active circles"),
                vec![crate::Circle {
                    id: circle_id,
                    name: Some("Household money".to_string()),
                    role: Some(crate::CircleRole::Owner),
                    state: crate::CircleState::Active,
                }]
            );
            assert_eq!(
                handle
                    .circles()
                    .members(circle_id)
                    .await
                    .expect("read active Circle members"),
                vec![crate::CircleMemberInfo {
                    pubkey: crate::keys::public_key_hex(
                        &handle
                            .security
                            .require_identity()
                            .expect("read test identity"),
                    ),
                    role: crate::CircleRole::Owner,
                    is_self: true,
                }]
            );
            let identity = handle
                .security
                .require_identity()
                .expect("read test identity");
            assert!(StoreDatabase::from_database(db.clone())
                .get_circle_members(
                    circle_id,
                    &crate::keys::public_key_hex(&identity),
                    std::collections::BTreeSet::new(),
                )
                .await
                .expect("intersect Circle roster with an empty Store membership")
                .is_empty());
            assert!(handle
                .circles()
                .operations()
                .await
                .expect("read completed circle operations")
                .is_empty());

            let counts = db
                .test_sql(move |database| database.circle_state_counts(circle_id))
                .await
                .expect("read activated circle state");
            assert_eq!(counts, (2, 2, 0));
        }))
        .await;
    }

    /// The `circles()` namespace round-trips through the running loop across
    /// derived states: create and rename land `Active`, read back through `list`;
    /// deletion lands `Deleted`. Each write dispatches through the loop-thread
    /// command channel and each state is read back through the public list surface.
    #[tokio::test]
    async fn circles_namespace_round_trips_across_states() {
        await_test_orchestration(tokio::spawn(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = read_test_db("images");
            let keyring = crate::encryption::MasterKeyring::generate();
            let handle = test_handle_with_custody(
                "lib-circles-namespace",
                store_dir,
                db.clone(),
                crate::custody::KeyCustody::InMemory(keyring.clone()),
            );
            handle
                .connect_sync_with_test_home(
                    Arc::new(InMemoryCloudHome::new()),
                    CloudCipher::Encrypted(EncryptionService::from(keyring)),
                )
                .await
                .expect("connect encrypted home");

            let circles = handle.circles();
            let circle_id = circles.create("Family").await.expect("create the Circle");
            circles
                .rename(circle_id, "Household")
                .await
                .expect("rename the Circle");

            let state_of = |list: Vec<crate::Circle>| {
                list.into_iter()
                    .find(|circle| circle.id == circle_id)
                    .expect("the Circle is listed")
                    .state
            };
            assert_eq!(
                state_of(circles.list().await.expect("list after rename")),
                crate::CircleState::Active,
            );

            circles.delete(circle_id).await.expect("delete the Circle");
            assert_eq!(
                state_of(circles.list().await.expect("list after delete")),
                crate::CircleState::Deleted,
            );
        }))
        .await;
    }

    /// Every Circle write command reaches the loop thread through its own
    /// `SyncCommand` dispatch arm and returns a reply. Each is fired at a state
    /// that refuses it: the three close/resolve commands come back with distinct
    /// typed errors naming the forwarded circle id (which also proves each arm
    /// forwards to the *right* components method — a swapped arm would return a
    /// different typed error); retry, remove, and add come back carrying the
    /// forwarded operation or circle id in their message. A wrong-field or
    /// wrong-method bug in any arm would surface here.
    #[tokio::test]
    async fn circle_write_commands_dispatch_through_their_command_arms() {
        await_test_orchestration(tokio::spawn(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = read_test_db("images");
            let keyring = crate::encryption::MasterKeyring::generate();
            let handle = test_handle_with_custody(
                "lib-circles-dispatch",
                store_dir,
                db,
                crate::custody::KeyCustody::InMemory(keyring.clone()),
            );
            handle
                .connect_sync_with_test_home(
                    Arc::new(InMemoryCloudHome::new()),
                    CloudCipher::Encrypted(EncryptionService::from(keyring)),
                )
                .await
                .expect("connect encrypted home");

            let circles = handle.circles();
            let circle_id = circles.create("Family").await.expect("create the Circle");
            let member = hex::encode(crate::keys::UserKeypair::generate().public_key());

            // Distinct typed refusals: a swapped arm would return a different one.
            assert!(
                matches!(
                    circles.cancel_close(circle_id).await,
                    Err(crate::CircleError::NoCloseToCancel { circle_id: refused })
                        if refused == circle_id
                ),
                "cancel_close dispatches to its arm and returns NoCloseToCancel"
            );
            let device = "aa"
                .repeat(32)
                .parse::<crate::StoreDeviceId>()
                .expect("device id");
            assert!(
                matches!(
                    circles.exclude_close_device(circle_id, device).await,
                    Err(crate::CircleError::NoCloseToExclude { circle_id: refused })
                        if refused == circle_id
                ),
                "exclude_close_device dispatches to its arm and returns NoCloseToExclude"
            );
            assert!(
                matches!(
                    circles
                        .resolve(circle_id, crate::CircleControlCoord::placeholder(1))
                        .await,
                    Err(crate::CircleError::NotConflicted { circle_id: refused })
                        if refused == circle_id
                ),
                "resolve dispatches to its arm and returns NotConflicted"
            );

            // The remaining three carry the forwarded id in their message.
            let retry = circles
                .retry_operation(crate::CircleOperationId::placeholder("dispatch-op-seed"))
                .await;
            assert!(
                matches!(&retry, Err(crate::CircleError::Protocol(message))
                    if message.contains("dispatch-op-seed")),
                "retry_operation forwards the operation id: {retry:?}"
            );

            let discard = circles
                .discard_operation(crate::CircleOperationId::placeholder("dispatch-op-seed"))
                .await;
            assert!(
                matches!(&discard, Err(crate::CircleError::Protocol(message))
                    if message.contains("dispatch-op-seed")),
                "discard_operation forwards the operation id: {discard:?}"
            );

            let absent_circle = crate::CircleId::from_bytes([9u8; 16]);
            let remove = circles.remove_member(absent_circle, &member).await;
            assert!(
                matches!(&remove, Err(crate::CircleError::Protocol(message))
                    if message.contains(&absent_circle.to_string())),
                "remove_member forwards the circle id: {remove:?}"
            );

            let add = circles.add_member(circle_id, &member).await;
            assert!(
                add.is_err(),
                "add_member dispatches to its arm and returns a reply: {add:?}"
            );
        }))
        .await;
    }

    #[tokio::test]
    async fn reconnect_sync_stops_the_previous_loop() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_reconnect_sync_stops_the_previous_loop())
                    .await
                    .expect("sync reconnect test task");
            })
            .await;
    }

    async fn run_reconnect_sync_stops_the_previous_loop() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let mut config = Config::with_defaults(
            "lib-reconnect-loop".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
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
            StoreKeys::bind("lib-reconnect-loop".to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        let home = Arc::new(InMemoryCloudHome::new());
        handle
            .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
            .await
            .expect("first connect over injected home");
        let first_loop = handle
            .sync
            .active_loop_for_test()
            .expect("first loop installed");
        assert!(first_loop.is_running(), "first loop starts running");

        handle
            .connect_sync_with_test_home(home, CloudCipher::Plaintext)
            .await
            .expect("second connect over injected home");
        let replacement_loop = handle
            .sync
            .active_loop_for_test()
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
    async fn stopped_installed_loop_blocks_blob_transitions() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_stopped_installed_loop_blocks_blob_transitions())
                    .await
                    .expect("stopped-loop readiness test task");
            })
            .await;
    }

    async fn run_stopped_installed_loop_blocks_blob_transitions() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let mut config = Config::with_defaults(
            "lib-stopped-loop-readiness".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
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
            StoreKeys::bind("lib-stopped-loop-readiness".to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");
        let loop_handle = handle.sync.active_loop_for_test().expect("loop installed");

        loop_handle.stop().expect("stop installed loop");

        let make_remote = handle.make_remote("notes", "note-1", false).await;
        assert!(matches!(make_remote, Err(MakeRemoteError::SyncNotReady)));

        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let make_local = handle
            .make_local("notes", "note-1", &HashMap::new(), &cancel_rx)
            .await;
        assert!(matches!(make_local, Err(MakeLocalError::SyncNotReady)));
    }

    #[tokio::test]
    async fn encrypted_session_keeps_its_binding_after_config_changes() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_encrypted_session_keeps_its_binding_after_config_changes(),
                )
                .await
                .expect("encrypted-session binding test task");
            })
            .await;
    }

    async fn run_encrypted_session_keeps_its_binding_after_config_changes() {
        test_keyring::install();

        let (tmp, store_dir) = temp_store_dir();
        let db = host_blob_test_db("images");

        let config = Config::with_defaults(
            "lib-test".to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
        let live_config = Arc::new(RwLock::new(config));
        let config_provider: ConfigProvider = {
            let live_config = live_config.clone();
            Arc::new(move || {
                live_config
                    .read()
                    .expect("test config lock is not poisoned")
                    .clone()
            })
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
            StoreKeys::bind("lib-test".to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );

        let home = Arc::new(InMemoryCloudHome::new());
        handle
            .connect_sync_with_test_home(
                home.clone(),
                CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            )
            .await
            .expect("connect encrypted injected home");
        let loop_handle = handle
            .sync
            .active_loop_for_test()
            .expect("sync loop installed");

        {
            let mut next_config = live_config
                .write()
                .expect("test config lock is not poisoned");
            next_config.store_id = "next-lib".to_string();
            next_config.store_dir = StoreDir::new(tmp.path().join("next-store"));
            next_config.cloud_home.storage = HomeStorage::Browsable;
        }

        assert_eq!(loop_handle.config().store_id, "lib-test");
        assert_eq!(loop_handle.store_dir(), &store_dir);
        assert!(matches!(
            loop_handle.blob_path_scheme(),
            BlobPathScheme::Hashed
        ));

        let rotated = EncryptionService::from_key([7u8; 32])
            .with_appended_generation(2, [8u8; 32])
            .expect("append generation");
        loop_handle
            .adopt_key_rotation_for_test(rotated)
            .expect("adopt encrypted generation");
        assert_eq!(
            loop_handle
                .current_encryption()
                .expect("session remains encrypted")
                .current_generation(),
            2,
        );

        let plaintext = b"encrypted-drain-bytes-after-key-rotation".to_vec();
        let blob = publish_host_blob(&handle, "plain-cover", "plain-cover", &plaintext).await;
        let cloud_key = blob
            .stored()
            .expect("published blob has exact storage")
            .object()
            .slot()
            .logical_key();
        let stored = home.get(cloud_key).expect("uploaded cloud object");
        assert_ne!(
            stored.as_slice(),
            plaintext.as_slice(),
            "an encrypted session must never upload plaintext cloud bytes",
        );

        let aad_context = |store_id: &str| {
            let mut context = Vec::new();
            context.extend_from_slice(&(store_id.len() as u64).to_le_bytes());
            context.extend_from_slice(store_id.as_bytes());
            context.extend_from_slice(&(cloud_key.len() as u64).to_le_bytes());
            context.extend_from_slice(cloud_key.as_bytes());
            context
        };
        let encryption = loop_handle
            .current_encryption()
            .expect("session remains encrypted");
        let (fingerprint, header, chunks) =
            crate::storage::split_sealed_blob(&stored).expect("stored blob layout");
        assert_eq!(fingerprint, encryption.seal_key_fingerprint());
        assert_eq!(
            encryption
                .blob_opener(header, &aad_context("lib-test"))
                .open_chunks(0..header.chunk_count(), chunks)
                .expect("open with the installed session binding"),
            plaintext,
        );
        assert!(
            encryption
                .blob_opener(header, &aad_context("next-lib"))
                .open_chunks(0..header.chunk_count(), chunks)
                .is_err(),
            "a later config must not change the installed session's store binding",
        );
    }

    fn status_test_handle(store_id: &str) -> (tempfile::TempDir, CovenHandle) {
        let (tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let mut config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            store_dir.clone(),
            "Test Store".to_string(),
        );
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
            StoreKeys::bind(store_id.to_string()),
            test_key_custody(),
            test_identity_custody(),
            crate::oauth::OAuthClients::empty(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            crate::storage::BlobChunking::DEFAULT,
        );
        (tmp, handle)
    }

    /// The current state starts offline, moves through storage checking and
    /// publication, then reports synchronization.
    #[tokio::test]
    async fn subscribed_host_sees_offline_checking_publishing_then_synchronized() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_subscribed_host_sees_offline_checking_publishing_then_synchronized(),
                )
                .await
                .expect("sync status sequence test task");
            })
            .await;
    }

    async fn run_subscribed_host_sees_offline_checking_publishing_then_synchronized() {
        test_keyring::install();

        let (_tmp, handle) = status_test_handle("lib-status-syncing");
        let mut rx = handle.subscribe_sync_status();
        assert_eq!(format!("{:?}", *rx.borrow()), "Offline");

        let home = InMemoryCloudHome::new();
        let (probe_reached, release_probe) = home.pause_next_probe();
        handle
            .connect_sync_with_test_home(Arc::new(home.clone()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");

        tokio::time::timeout(Duration::from_secs(20), probe_reached.notified())
            .await
            .expect("the reachability probe reaches its test pause");
        assert_eq!(format!("{:?}", *rx.borrow()), "CheckingStorage");

        let (publication_reached, release_publication) = home.pause_after_exact_create_call(1);
        release_probe.notify_one();
        tokio::time::timeout(Duration::from_secs(20), publication_reached.notified())
            .await
            .expect("publication reaches its test pause");
        let publishing = rx.borrow().clone();
        assert_eq!(format!("{publishing:?}"), "Publishing");

        release_publication.notify_one();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if matches!(&*rx.borrow(), SyncLoopStatus::Synchronized(_)) {
                    break;
                }
                rx.changed().await.expect("the status channel remains open");
            }
        })
        .await
        .expect("a synchronized status arrives within the timeout");
        let done = rx.borrow().clone();
        assert!(
            format!("{done:?}").starts_with("Synchronized("),
            "a successful cycle ends synchronized, got {done:?}",
        );
    }

    #[tokio::test]
    async fn transport_failure_after_reachability_probe_returns_to_offline() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_transport_failure_after_reachability_probe_returns_to_offline(),
                )
                .await
                .expect("transport failure status test task");
            })
            .await;
    }

    async fn run_transport_failure_after_reachability_probe_returns_to_offline() {
        test_keyring::install();

        let (_tmp, handle) = status_test_handle("lib-status-cycle-transport");
        let mut rx = handle.subscribe_sync_status();
        let home = InMemoryCloudHome::new();
        let (probe_reached, release_probe) = home.pause_next_probe();
        handle
            .connect_sync_with_test_home(Arc::new(home.clone()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");

        tokio::time::timeout(Duration::from_secs(20), probe_reached.notified())
            .await
            .expect("the reachability probe reaches the provider");
        assert_eq!(format!("{:?}", *rx.borrow()), "CheckingStorage");
        home.arm_write_failures();
        release_probe.notify_one();

        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                rx.changed().await.expect("the status channel remains open");
                match rx.borrow().clone() {
                    SyncLoopStatus::CheckingStorage | SyncLoopStatus::Publishing => {}
                    SyncLoopStatus::Offline => break,
                    status => {
                        panic!("a provider transport failure must end offline, got {status:?}")
                    }
                }
            }
        })
        .await
        .expect("the failed cycle publishes a terminal status");
    }

    /// A subscription created before any provider is connected keeps receiving
    /// across a reconnect — the channel is owned by the handle, not the loop that
    /// a reconnect replaces. Under a per-loop channel the receiver would observe
    /// `Closed` after the reconnect dropped the first loop's sender.
    #[tokio::test]
    async fn subscription_survives_a_reconnect() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_subscription_survives_a_reconnect())
                    .await
                    .expect("status subscription reconnect test task");
            })
            .await;
    }

    async fn run_subscription_survives_a_reconnect() {
        test_keyring::install();

        let (_tmp, handle) = status_test_handle("lib-status-reconnect");

        // Subscribe before any provider is connected — valid because the channel
        // is handle-owned.
        let mut rx = handle.subscribe_sync_status();
        let home = Arc::new(InMemoryCloudHome::new());

        handle
            .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
            .await
            .expect("first connect");
        // Reconnect immediately: this drops the first loop and starts a second one
        // over the same store home before the first loop's startup delay elapses.
        handle
            .connect_sync_with_test_home(home, CloudCipher::Plaintext)
            .await
            .expect("reconnect");

        tokio::time::timeout(Duration::from_secs(20), rx.changed())
            .await
            .expect("a status arrives from the post-reconnect loop")
            .expect("a reconnect does not close the handle-owned status channel");
        let status = rx.borrow().clone();
        assert!(
            matches!(
                status,
                SyncLoopStatus::CheckingStorage | SyncLoopStatus::Publishing
            ),
            "the received status is a cycle start marker, got {status:?}",
        );
    }

    /// `stop_sync` keeps the provider connection so `start_sync` can resume it;
    /// `disconnect_sync` drops it outright. The resolved cipher and device
    /// keypair live only inside the active loop, so nothing resolved by the
    /// connection survives disconnection. A later `connect_sync` resolves fresh
    /// material from custody.
    #[tokio::test]
    async fn disconnect_sync_drops_the_connection_not_just_the_loop() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_disconnect_sync_drops_the_connection_not_just_the_loop(),
                )
                .await
                .expect("disconnect-connection test task");
            })
            .await;
    }

    async fn run_disconnect_sync_drops_the_connection_not_just_the_loop() {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db("images");
        let handle = test_handle("lib-disconnect-drops-connection", store_dir, db);

        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect over injected home");
        assert!(handle.sync.is_connected(), "connect installs a connection");

        handle.stop_sync();
        assert!(
            handle.sync.is_connected(),
            "stop_sync keeps the connection installed so start_sync can resume it",
        );

        handle.disconnect_sync();
        assert!(
            !handle.sync.is_connected(),
            "disconnect_sync drops the connection entirely — nothing it \
             cached survives past this call",
        );
    }
}

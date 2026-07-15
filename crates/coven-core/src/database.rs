//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with the sync bookkeeping
//! beside it. Every database access — the host's app SQL, coven's bookkeeping,
//! changeset capture and apply — runs against that one connection, so access is
//! serialized.
//!
//! Hosts open coven with [`crate::Coven::builder`] and run app SQL through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`].

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::error;
use tracing::warn;

use crate::blob::decl::BlobDecls;
use crate::blob::{BlobRef, Provenance};
use crate::db::{
    apply_coven_schema, is_reserved_table_name, ExternalBlob, OutboxEntry, OutboxOperation,
};
use crate::encryption::EncryptionService;
use crate::migration::{run_migrations_in_transaction, Migration, MigrationError};
use crate::sync::gate::{self, Gates};
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY, MAX_FUTURE_SKEW_MS};
use crate::sync::membership::{SerialAuthorizationState, SerialMembershipState};
use crate::sync::routing_contract::SyncRoutingContract;
use crate::sync::session::SyncedTable;
use crate::sync::store_commit::{
    CommitFrontier, CommitPosition, ObjectHash, SnapshotMeta, StoreAck, StoreBatchCommit,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreDeviceRegistrationState, StoreProtocolRoot, StoreSerialHead, SERIAL_STREAM_ID,
};
use crate::write::{
    AffectedRow, PendingBranch, PendingBranchId, PendingWrite, PublishedPosition, WriteId,
    WriteReceipt, WriteResolution, WriteStatus,
};
use crate::WritePolicy;

pub const LOCAL_DEVICE_ID_STATE_KEY: &str = "local_device_id";
pub const WRITE_POLICY_STATE_KEY: &str = "write_policy";
const SYNC_ROUTING_CONTRACT_STATE_KEY: &str = "sync_routing_contract";
pub const SYNC_ROUTING_HASH_STATE_KEY: &str = "sync_routing_hash";
const COVEN_INITIALIZED_STATE_KEY: &str = "coven_initialized";
const COVEN_INITIALIZED_STATE_VALUE: &str = "1";
pub const STORE_ROOT_HASH_STATE_KEY: &str = "store_root_hash";
pub const LAST_SNAPSHOT_HASH_STATE_KEY: &str = "last_snapshot_hash";
pub const LAST_SNAPSHOT_FRONTIER_STATE_KEY: &str = "last_snapshot_frontier";
pub const LAST_SNAPSHOT_POSITION_STATE_KEY: &str = "last_snapshot_position";
pub const SERIAL_MEMBERSHIP_STATE_KEY: &str = "serial_membership_state";
pub const SERIAL_KEY_GENERATION_STATE_KEY: &str = "serial_key_generation";
const GATE_BASELINE_SCHEMA: &str = "coven_gate_empty";

fn authorize_host_sql(context: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};

    if context
        .database_name
        .is_some_and(|name| name.eq_ignore_ascii_case(GATE_BASELINE_SCHEMA))
        || matches!(
            context.action,
            AuthAction::Detach { database_name }
                if database_name.eq_ignore_ascii_case(GATE_BASELINE_SCHEMA)
        )
        || matches!(
            context.action,
            AuthAction::Pragma { pragma_name, .. }
                if pragma_name.eq_ignore_ascii_case("database_list")
        )
    {
        Authorization::Deny
    } else {
        Authorization::Allow
    }
}

/// An error from the owned database.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Message(String),
    #[error("database error: Store protocol root hash is absent")]
    StoreRootHashMissing,
    #[error("database error: Store protocol root hash is invalid: {reason}")]
    StoreRootHashInvalid { reason: String },
}

impl DbError {
    pub(crate) fn into_message(self) -> String {
        match self {
            Self::Message(message) => message,
            Self::StoreRootHashMissing => "Store protocol root hash is absent".to_string(),
            Self::StoreRootHashInvalid { reason } => {
                format!("Store protocol root hash is invalid: {reason}")
            }
        }
    }

    fn missing_store_root_hash() -> Self {
        Self::StoreRootHashMissing
    }

    fn invalid_store_root_hash(reason: String) -> Self {
        Self::StoreRootHashInvalid { reason }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::Message(e.to_string())
    }
}

/// Why opening the database failed. Splits a migration-ladder failure from every
/// other open-time database error so the [`MigrationError`] a host acts on —
/// [`MigrationError::SchemaTooNew`], whose remedy is "update the app" — stays
/// matchable at the open boundary instead of being flattened into a
/// [`DbError`] string.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Migration(#[from] MigrationError),
    #[error(transparent)]
    Db(#[from] DbError),
}

#[derive(Clone)]
struct DatabaseState {
    write_policy: WritePolicy,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    /// The host's blob-tombstone convergence window, read by the tombstone GC to
    /// age each tombstone's `deleted_at`. Host config carried here alongside
    /// `synced_tables` so the sync layer reads it from the one owner rather than
    /// threading a separately-passed copy that could diverge.
    blob_tombstone_grace: chrono::Duration,
    /// How many blob transfers each transfer loop runs at once, read by the upload
    /// drain and the pin loop (both hold `&Database`). Open-time host config carried
    /// here for the same single-owner reason as `blob_tombstone_grace`.
    transfer_limits: crate::blob::TransferLimits,
    /// Serializes complete membership-chain loads that share this database, so a
    /// load cannot return an older chain after another load commits a newer floor.
    membership_load: Arc<tokio::sync::Mutex<()>>,
    /// Serializes construction and execution of the one local membership mutation
    /// whose exact signed bytes are held in `outbound_membership_mutation`.
    membership_mutation: Arc<tokio::sync::Mutex<()>>,
    /// Serializes staging and publication of the one exact snapshot generation
    /// held in `outbound_store_snapshot`.
    snapshot_publication: Arc<tokio::sync::Mutex<()>>,
    /// Serializes the full durable-intent to filesystem-deletion to intent-removal
    /// operation across every clone of this database.
    local_blob_cleanup: Arc<tokio::sync::Mutex<()>>,
    write_ids: crate::id_provider::IdRef,
    write_statuses:
        Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>>,
    #[cfg(any(test, feature = "test-utils"))]
    test_pause_points: Arc<TestPausePoints<DatabaseTestPoint>>,
}

/// Test-only checkpoints reached by database operations whose ordering matters.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseTestPoint {
    LocalBlobCleanupRequested,
    LocalBlobCleanupAcquired,
    LocalBlobCleanupBeforeFilesystem { namespace: String, blob_id: String },
    LocalBlobCleanupFinished,
    PullAfterRemoteCommit { device_id: String, seq: u64 },
}

#[cfg(any(test, feature = "test-utils"))]
struct ArmedTestPause<K> {
    point: K,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-utils"))]
struct TestPauseState<K> {
    armed: Option<ArmedTestPause<K>>,
    observers: Vec<tokio::sync::mpsc::UnboundedSender<K>>,
}

#[cfg(any(test, feature = "test-utils"))]
struct TestPausePoints<K> {
    state: std::sync::Mutex<TestPauseState<K>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<K> Default for TestPausePoints<K> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(TestPauseState {
                armed: None,
                observers: Vec::new(),
            }),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<K: Clone + PartialEq> TestPausePoints<K> {
    fn arm(&self, point: K) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let prior = self
            .state
            .lock()
            .expect("database test pause mutex poisoned")
            .armed
            .replace(ArmedTestPause {
                point,
                reached: reached.clone(),
                resume: resume.clone(),
            });
        assert!(prior.is_none(), "database test pause already armed");
        (reached, resume)
    }

    fn observe(&self) -> tokio::sync::mpsc::UnboundedReceiver<K> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("database test pause mutex poisoned")
            .observers
            .push(sender);
        receiver
    }

    async fn reach(&self, point: K) {
        let pause = {
            let mut state = self
                .state
                .lock()
                .expect("database test pause mutex poisoned");
            state
                .observers
                .retain(|observer| observer.send(point.clone()).is_ok());
            if state
                .armed
                .as_ref()
                .is_some_and(|pause| pause.point == point)
            {
                state.armed.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
    }
}

/// The owned SQLite connection and the sync bookkeeping resolved beside it at
/// open. One connection thread owns this for the connection's whole life; every
/// database access runs against it, so access is serialized. Changeset capture is
/// per-transaction — [`Database::run_store_write_transaction_on`] attaches a
/// session for the span of one host write and drains it into the existing write records —
/// so no capture state lives on the core between calls.
///
/// `DatabaseCore` holds only `Send` fields (a `rusqlite::Connection`, which is
/// `Send`, plus `Arc`s and a `u32`), so it is `Send` by construction — the
/// connection thread receives it by value across a single `thread::spawn` with no
/// manual `unsafe impl`.
struct DatabaseCore {
    conn: Connection,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: crate::blob::TransferLimits,
    write_policy: WritePolicy,
}

#[derive(Clone, Copy)]
enum CovenMetadataOpen {
    Detect,
    InitializeVerifiedSnapshot,
}

fn serialized_write_policy(write_policy: WritePolicy) -> Result<String, DbError> {
    serde_json::to_string(&write_policy)
        .map_err(|error| DbError::Message(format!("serialize Store write policy: {error}")))
}

fn initialize_write_policy(conn: &Connection, write_policy: WritePolicy) -> Result<(), DbError> {
    let serialized = serialized_write_policy(write_policy)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (WRITE_POLICY_STATE_KEY, serialized),
    )
    .map_err(DbError::from)?;
    Ok(())
}

fn protocol_state_exists(conn: &Connection) -> Result<bool, DbError> {
    conn.query_row(
        "SELECT EXISTS(\
             SELECT 1 FROM main.sqlite_schema \
             WHERE type = 'table' AND name = 'protocol_state'\
         )",
        [],
        |row| row.get(0),
    )
    .map_err(DbError::from)
}

fn has_coven_initialization_marker(conn: &Connection) -> Result<bool, DbError> {
    if !protocol_state_exists(conn)? {
        return Ok(false);
    }
    let marker = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_INITIALIZED_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?;
    match marker.as_deref() {
        None => Ok(false),
        Some(COVEN_INITIALIZED_STATE_VALUE) => Ok(true),
        Some(value) => Err(DbError::Message(format!(
            "Store database has invalid Coven initialization marker {value:?}"
        ))),
    }
}

fn initialize_coven_metadata_on(
    conn: &Connection,
    write_policy: WritePolicy,
    sync_routing_contract: &SyncRoutingContract,
    install_routing_schema: bool,
) -> Result<(), DbError> {
    apply_coven_schema(conn).map_err(DbError::from)?;
    if install_routing_schema {
        crate::db::apply_coven_routing_schema(conn).map_err(DbError::from)?;
    }
    initialize_write_policy(conn, write_policy)?;
    let contract_json =
        String::from_utf8(sync_routing_contract.bytes().to_vec()).map_err(|error| {
            DbError::Message(format!("encode sync-routing contract metadata: {error}"))
        })?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (SYNC_ROUTING_CONTRACT_STATE_KEY, contract_json),
    )
    .map_err(DbError::from)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (
            SYNC_ROUTING_HASH_STATE_KEY,
            sync_routing_contract.hash().to_string(),
        ),
    )
    .map_err(DbError::from)?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
        (COVEN_INITIALIZED_STATE_KEY, COVEN_INITIALIZED_STATE_VALUE),
    )
    .map(|_| ())
    .map_err(DbError::from)
}

fn load_coven_metadata(
    conn: &Connection,
    write_policy: WritePolicy,
) -> Result<SyncRoutingContract, DbError> {
    if !has_coven_initialization_marker(conn)? {
        return Err(DbError::Message(
            "Store database is missing required Coven initialization metadata".to_string(),
        ));
    }
    validate_write_policy(conn, write_policy)?;
    let contract_bytes = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_CONTRACT_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_contract metadata".to_string(),
            )
        })?;
    let contract = SyncRoutingContract::from_bytes(contract_bytes.as_bytes()).map_err(|error| {
        DbError::Message(format!("Store sync-routing contract is invalid: {error}"))
    })?;
    let stored_hash = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_HASH_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message(
                "Store database is missing required sync_routing_hash metadata".to_string(),
            )
        })?;
    let stored_hash: ObjectHash = stored_hash.parse().map_err(|error| {
        DbError::Message(format!(
            "Store sync_routing_hash metadata is invalid: {error}"
        ))
    })?;
    if stored_hash != contract.hash() {
        return Err(DbError::Message(format!(
            "Store sync-routing contract hashes to {}, but metadata records {stored_hash}",
            contract.hash(),
        )));
    }
    Ok(contract)
}

fn validate_sync_routing_contract(
    pinned: &SyncRoutingContract,
    resolved: &SyncRoutingContract,
) -> Result<(), DbError> {
    if pinned.bytes() != resolved.bytes() || pinned.hash() != resolved.hash() {
        return Err(DbError::Message(format!(
            "Store sync-routing hash is {}, but open resolved {}",
            pinned.hash(),
            resolved.hash(),
        )));
    }
    Ok(())
}

fn validate_write_policy(conn: &Connection, requested: WritePolicy) -> Result<(), DbError> {
    let stored = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [WRITE_POLICY_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message("Store database is missing required write_policy metadata".to_string())
        })?;
    let stored: WritePolicy = serde_json::from_str(&stored).map_err(|error| {
        DbError::Message(format!("Store write_policy metadata is invalid: {error}"))
    })?;
    if stored != requested {
        return Err(DbError::Message(format!(
            "Store write policy is {stored:?}, but open requested {requested:?}"
        )));
    }
    Ok(())
}

impl DatabaseCore {
    fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen,
    ) -> Result<(Self, DatabaseState, UpdatedAtStamper), OpenError> {
        let mut conn = open_connection(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

        let initialized = match metadata_open {
            CovenMetadataOpen::InitializeVerifiedSnapshot => false,
            CovenMetadataOpen::Detect => has_coven_initialization_marker(&conn)?,
        };
        let pinned_routing_contract = initialized
            .then(|| load_coven_metadata(&conn, write_policy))
            .transpose()?;
        let (schema_version, sync_routing_contract, gates, blob_decls) = {
            let tx = conn.transaction().map_err(DbError::from)?;
            let outcome = (|| -> Result<_, OpenError> {
                let schema_version = run_migrations_in_transaction(&tx, migrations)?;

                // The host ladder and routing validation share this transaction.
                // A pending migration that changes confidentiality topology cannot
                // leave either its DDL or `user_version` advance committed.
                validate_host_synced_tables(&tx, &synced_tables)?;
                let resolved = SyncRoutingContract::from_connection(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if let Some(pinned) = &pinned_routing_contract {
                    validate_sync_routing_contract(pinned, &resolved)?;
                } else {
                    initialize_coven_metadata_on(
                        &tx,
                        write_policy,
                        &resolved,
                        write_policy == WritePolicy::MergeConcurrent && resolved.has_scoped_graph(),
                    )?;
                }
                let gates = Gates::from_tables(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let blob_decls = BlobDecls::from_tables(&tx, &synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                Ok((schema_version, resolved, gates, blob_decls))
            })();
            match outcome {
                Ok(initialized) => {
                    tx.commit().map_err(DbError::from)?;
                    initialized
                }
                Err(error) => return Err(error),
            }
        };
        let sync_routing_hash = sync_routing_contract.hash();
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (LOCAL_DEVICE_ID_STATE_KEY, hlc.device_id()),
        )
        .map_err(DbError::from)?;

        // Seed the register clock so a restart cannot mint a stamp behind a value
        // already on disk. Floor = max(persisted high-water, max synced-row
        // `_updated_at`).
        let persisted = conn
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [HIGHWATER_STATE_KEY],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?;
        seed_from(&hlc, persisted, "HLC high-water mark in protocol_state")?;
        let seed_wall_ms = hlc.wall_now_ms();
        let seed_bound_ms = seed_wall_ms.saturating_add(MAX_FUTURE_SKEW_MS);
        let on_disk = scan_max_updated_at(&conn, &synced_tables, seed_bound_ms)?;
        seed_from(&hlc, on_disk, "`_updated_at` in synced tables")?;

        let stamper = UpdatedAtStamper::new(hlc.clone());
        let synced_tables = Arc::new(synced_tables);
        let gates = Arc::new(gates);
        let blob_decls = Arc::new(blob_decls);
        blob_decls
            .install_cleanup_guards(&conn)
            .map_err(|e| DbError::Message(e.to_string()))?;
        gate::attach_empty_clone(&conn, &gates)
            .map_err(|error| DbError::Message(format!("install host transaction gate: {error}")))?;
        let core = DatabaseCore {
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
        };
        let state = core.state();

        Ok((core, state, stamper))
    }

    /// Open the connection at `path` read-only: a `SQLITE_OPEN_READONLY`
    /// connection resolving the same gate/blob models a writer open resolves, but
    /// running no migration ladder and no schema/bookkeeping writes. It refuses a
    /// db a newer binary migrated past this one (the writer's `SchemaTooNew`
    /// policy), since its models must understand the on-disk schema. Backs
    /// [`Database::open_read_only`]; see it for why a reader takes no store lock.
    fn open_read_only(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Self, DatabaseState), OpenError> {
        let conn = open_connection_read_only(path)?;
        // `foreign_keys` is a per-connection runtime setting, not a write to the db
        // file, so it is allowed on a read-only connection; keeping it on matches the
        // writer's relational view. A read never inserts, so it enforces nothing new.
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
        // Open against the on-disk schema exactly as the writer left it: run no
        // migration ladder (that writes), but refuse a schema newer than this binary
        // knows — the same policy `run_migrations` applies — because the gate and blob
        // models below are resolved against a schema this binary must understand.
        let schema_version = crate::migration::ensure_schema_supported(&conn, migrations)?;

        // Reads only (PRAGMA table_info): assert the host tables the writer created
        // still present the synced-table contract, so a wrong schema fails loud at
        // open rather than mid-read.
        validate_host_synced_tables(&conn, &synced_tables)?;
        let pinned_routing_contract = load_coven_metadata(&conn, write_policy)?;
        let sync_routing_contract = SyncRoutingContract::from_connection(&conn, &synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        validate_sync_routing_contract(&pinned_routing_contract, &sync_routing_contract)?;
        let sync_routing_hash = sync_routing_contract.hash();

        let synced_tables = Arc::new(synced_tables);

        // No register-clock seeding: a reader never mints an `_updated_at`, so it has
        // no stamp to keep ahead of on-disk values.
        let gates = Arc::new(
            Gates::from_tables(&conn, &synced_tables)
                .map_err(|e| DbError::Message(e.to_string()))?,
        );
        let blob_decls = Arc::new(
            BlobDecls::from_tables(&conn, &synced_tables)
                .map_err(|e| DbError::Message(e.to_string()))?,
        );
        let core = DatabaseCore {
            conn,
            hlc,
            synced_tables,
            schema_version,
            sync_routing_hash,
            gates,
            blob_decls,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
        };
        let state = core.state();
        Ok((core, state))
    }

    fn state(&self) -> DatabaseState {
        DatabaseState {
            write_policy: self.write_policy,
            hlc: self.hlc.clone(),
            synced_tables: self.synced_tables.clone(),
            schema_version: self.schema_version,
            sync_routing_hash: self.sync_routing_hash,
            gates: self.gates.clone(),
            blob_decls: self.blob_decls.clone(),
            blob_tombstone_grace: self.blob_tombstone_grace,
            transfer_limits: self.transfer_limits,
            membership_load: Arc::new(tokio::sync::Mutex::new(())),
            membership_mutation: Arc::new(tokio::sync::Mutex::new(())),
            snapshot_publication: Arc::new(tokio::sync::Mutex::new(())),
            local_blob_cleanup: Arc::new(tokio::sync::Mutex::new(())),
            write_ids: Arc::new(crate::id_provider::UuidProvider),
            write_statuses: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(any(test, feature = "test-utils"))]
            test_pause_points: Arc::new(TestPausePoints::default()),
        }
    }

    fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// A unit of work for the connection thread: a caller's closure to run against
/// the owned core, or the sentinel the last [`Database`] clone sends as it drops
/// to stop the thread. A `Run` closure carries its own reply channel, so it
/// returns `()` — [`Database::on_connection_thread`] builds it to capture the
/// caller's result and send it back.
enum DbJob {
    Run(Box<dyn FnOnce(&mut DatabaseCore) + Send>),
    Stop,
}

/// The connection thread's send channel and join handle, shared by every
/// [`Database`] clone through an `Arc`. The last clone to drop shuts the thread
/// down and joins it, so the connection closes on the thread that owned it before
/// control returns to the dropper.
struct ConnectionThread {
    jobs: tokio::sync::mpsc::UnboundedSender<DbJob>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ConnectionThread {
    fn drop(&mut self) {
        // Reached only when the last `Database` clone drops — no other clone can
        // still be sending. Queue `Stop` behind whatever jobs are already in
        // flight so the thread drains them, exits, and drops the core (closing the
        // connection). A send error means the thread already stopped.
        let _ = self.jobs.send(DbJob::Stop);
        let handle = match self.join.take() {
            Some(handle) => handle,
            None => return,
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            // A `Database` handle roams freely across async tasks, so this last
            // drop can land inside a runtime task. Joining here would block that
            // worker until the queue drains — the very stall this thread exists to
            // remove. Detach instead: the thread drains its queue, drops the core,
            // and exits on its own. Every queued job's effect is durable, so a
            // thread left unjoined loses nothing; only the deterministic close
            // moves off this task.
            drop(handle);
        } else {
            // Sync context — tests, process teardown — where there is no worker to
            // stall. Join for a deterministic shutdown: the connection is fully
            // closed before we return. Jobs run under `catch_unwind`, so the thread
            // never unwinds from a caller's closure; a join error is a real fault
            // (a panic in the core's own drop) and is surfaced, not swallowed.
            if handle.join().is_err() {
                error!("database connection thread panicked");
            }
        }
    }
}

/// Own the connection on this thread and run each queued job in send order until
/// `Stop`. The channel's FIFO is the serialization the `tokio::Mutex` used to
/// provide, and the connection never leaves this thread.
fn run_connection_thread(
    mut core: DatabaseCore,
    mut jobs: tokio::sync::mpsc::UnboundedReceiver<DbJob>,
) {
    while let Some(job) = jobs.blocking_recv() {
        match job {
            DbJob::Run(f) => f(&mut core),
            DbJob::Stop => break,
        }
    }
    // Loop exited: drop `core` here — closing the connection on the thread that
    // has owned it throughout.
}

/// A handle to the owned database. Cloneable; every clone sends work to the one
/// connection thread over the same channel, so access serializes as the channel's
/// FIFO.
#[derive(Clone)]
pub struct Database {
    thread: Arc<ConnectionThread>,
    state: DatabaseState,
}

pub(crate) struct PreparedStoreWrite {
    pub write_id: WriteId,
    pub changeset: Vec<u8>,
    pub partitions: PreparedStoreWritePartitions,
    pub inverse_changeset: Vec<u8>,
    pub base: StoreWriteBase,
    pub blob_facts: StoreWriteBlobFacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStoreWritePartitions {
    pub store: Option<gate::AudiencePartition>,
    pub circles: Vec<gate::AudiencePartition>,
}

#[derive(Clone, Copy)]
enum StoreWriteRouting<'a> {
    Unscoped,
    MergeScoped(&'a EncryptionService),
    SerialScoped,
}

impl PreparedStoreWritePartitions {
    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &gate::AudiencePartition> {
        self.store.iter().chain(self.circles.iter())
    }
}

pub(crate) struct SerialStoreBranchPreparationWork {
    pub branch_id: PendingBranchId,
    pub base: Option<CommitPosition>,
    pub writes: Vec<PreparedStoreWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StoreWriteBase {
    MergeConcurrent {
        dependencies: BTreeMap<String, CommitPosition>,
    },
    Serial {
        branch_id: PendingBranchId,
        base: Option<CommitPosition>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreWriteBlobFacts {
    pub blobs: Vec<StoreWriteBlobFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provenance", rename_all = "snake_case")]
pub(crate) enum StoreWriteBlobFact {
    UserProvided {
        blob: BlobRef,
        state: StoreWriteUserBlobState,
    },
    HostProvided {
        blob: BlobRef,
        size: u64,
        state: StoreWriteHostBlobState,
    },
}

impl StoreWriteBlobFact {
    pub(crate) fn blob(&self) -> &BlobRef {
        match self {
            Self::UserProvided { blob, .. } | Self::HostProvided { blob, .. } => blob,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoreWriteUserBlobState {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum StoreWriteHostBlobState {
    Ordinary,
    MakeRemote {
        root_table: String,
        root_id: String,
        retain_pinned: bool,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ExactProtocolObject<T> {
    pub value: T,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStoreWriteCommit {
    pub package_bytes: Vec<u8>,
    pub commit: ExactProtocolObject<StoreBatchCommit>,
    pub head: ExactProtocolObject<StoreDeviceHead>,
    pub blob_manifest: StoreBlobManifest,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedSerialStoreWriteCommit {
    pub package_bytes: Vec<u8>,
    pub commit: ExactProtocolObject<StoreBatchCommit>,
    pub blob_manifest: StoreBlobManifest,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedSerialStoreBranch {
    pub branch_id: PendingBranchId,
    pub base: Option<CommitPosition>,
    pub writes: Vec<PreparedSerialStoreWriteCommit>,
    pub head: ExactProtocolObject<StoreSerialHead>,
}

#[derive(Debug, Clone)]
pub(crate) struct UnresolvedSerialBranch {
    pub branch_id: PendingBranchId,
    pub base: Option<CommitPosition>,
    pub conflicted: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundStoreAck {
    pub revision: u64,
    pub ack_hash: ObjectHash,
    pub previous_ack_hash: Option<ObjectHash>,
    pub ack_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableProtocolObject {
    pub semantic_hash: ObjectHash,
    pub bytes: Vec<u8>,
    pub published: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableDeviceRegistration {
    pub revision: u64,
    pub registration_hash: ObjectHash,
    pub previous_registration_hash: Option<ObjectHash>,
    pub state: StoreDeviceRegistrationState,
    pub registration_bytes: Vec<u8>,
    pub activation_commit_bytes: Option<Vec<u8>>,
    pub activation_head_bytes: Option<Vec<u8>>,
    pub published: bool,
}

struct StoredDeviceRegistrationActivation {
    registration_bytes: Vec<u8>,
    commit_bytes: Option<Vec<u8>>,
    head_bytes: Option<Vec<u8>>,
    published: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableMembershipMutation {
    pub intent_hash: ObjectHash,
    pub plan_bytes: Vec<u8>,
    pub progress_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableSnapshotPublication {
    pub snapshot_hash: ObjectHash,
    pub image_hash: ObjectHash,
    pub image_bytes: Vec<u8>,
    pub meta_bytes: Vec<u8>,
}

pub(crate) struct StoreWritePreparation {
    pub write_id: WriteId,
    pub package_bytes: Vec<u8>,
    pub commit: StoreBatchCommit,
    pub head: StoreDeviceHead,
    pub blob_manifest: StoreBlobManifest,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

pub(crate) struct SerialStoreWritePreparation {
    pub branch_id: PendingBranchId,
    pub base: Option<CommitPosition>,
    pub writes: Vec<SerialStoreWritePreparationEntry>,
    pub head: StoreSerialHead,
}

pub(crate) struct SerialStoreWritePreparationEntry {
    pub write_id: WriteId,
    pub package_bytes: Vec<u8>,
    pub commit: StoreBatchCommit,
    pub blob_manifest: StoreBlobManifest,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PreparedStoreWriteState {
    MergeConcurrent {
        commit_bytes: Vec<u8>,
        head_bytes: Vec<u8>,
        blob_manifest: StoreBlobManifest,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
    SerialPreparing,
    Serial {
        commit_bytes: Vec<u8>,
        tip_head_bytes: Option<Vec<u8>>,
        blob_manifest: StoreBlobManifest,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreBlobManifest {
    pub blobs: Vec<BlobRef>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreBatchLocalCleanup {
    pub drops: Vec<crate::sync::service::DeferredLocalBlobDrop>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreBatchCompletion {
    pub consumed_make_remote_intents: Vec<StoreConsumedMakeRemoteIntent>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreConsumedMakeRemoteIntent {
    pub root_table: String,
    pub root_id: String,
}

fn validate_host_synced_tables(
    conn: &Connection,
    synced_tables: &[SyncedTable],
) -> Result<(), DbError> {
    // Which table owns each declared blob namespace, so a second table claiming an
    // already-owned namespace is caught here. A blob's namespace is part of its
    // address; two tables sharing one makes `row_for_blob_in_namespace` resolve to
    // whichever the hash map iterates first.
    let mut namespace_owner: HashMap<&str, &str> = HashMap::new();
    let mut table_by_sqlite_name: HashMap<String, &str> = HashMap::new();
    for table in synced_tables {
        let name = table.name();
        if name.is_empty() {
            return Err(DbError::Message(
                "synced table name must not be empty".to_string(),
            ));
        }
        if is_reserved_table_name(name) {
            return Err(DbError::Message(format!(
                "synced table {name:?} is reserved by coven"
            )));
        }
        let sqlite_name = name.to_ascii_lowercase();
        if let Some(prior) = table_by_sqlite_name.insert(sqlite_name, name) {
            return Err(DbError::Message(format!(
                "synced tables {prior:?} and {name:?} are declared as the same SQLite table more than once"
            )));
        }
        if let Some(live_name) = canonical_table_name(conn, name)? {
            if live_name != name {
                return Err(DbError::Message(format!(
                    "synced table {name:?} does not use the live schema's exact spelling {live_name:?}"
                )));
            }
        }
        validate_synced_table_contract(conn, name)?;
        validate_existing_row_identities(conn, table)?;
        if let Some(decl) = table.blob() {
            let namespace = decl.namespace.as_str();
            if let Some(prior) = namespace_owner.insert(namespace, name) {
                return Err(DbError::Message(format!(
                    "synced tables {prior:?} and {name:?} both declare blob namespace \
                     {namespace:?}; a namespace must be owned by exactly one table"
                )));
            }
        }
    }
    Ok(())
}

/// Return the live `main`-schema spelling SQLite resolves for `table`.
/// SQLite table identifiers compare case-insensitively, while coven dispatches
/// changesets by their exact table name, so open requires declarations to use
/// this canonical spelling.
fn canonical_table_name(conn: &Connection, table: &str) -> Result<Option<String>, DbError> {
    conn.query_row(
        "SELECT name FROM main.sqlite_schema \
         WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
        [table],
        |row| row.get(0),
    )
    .optional()
    .map_err(DbError::from)
}

fn validate_existing_row_identities(conn: &Connection, table: &SyncedTable) -> Result<(), DbError> {
    if table.row_identity() == crate::sync::session::RowIdentity::SharedKey {
        return Ok(());
    }
    let sql = format!(
        "SELECT id FROM {}",
        crate::sync::session::quote_ident(table.name())
    );
    let mut statement = conn.prepare(&sql).map_err(DbError::from)?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?;
    for id in ids {
        let id = id.map_err(DbError::from)?;
        crate::sync::session::validate_row_identity(table.name(), table.row_identity(), &id)
            .map_err(|error| DbError::Message(error.to_string()))?;
    }
    Ok(())
}

/// One column of a table's `PRAGMA table_info`. `position` is the column ordinal
/// — the index a session changeset reports for that column, so the pk's position
/// is what the by-position apply path reads. `pk` is 0 for a non-key column or its
/// 1-based rank within the primary key.
struct ColumnInfo {
    position: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    pk: i64,
}

/// Enforce the synced-table contract ([`crate::sync::session::SyncedTable`]) on
/// `table`'s live schema: the table declared STRICT; a single primary key
/// column, named `id`, declared TEXT, at column 0; and an `_updated_at` column
/// declared TEXT NOT NULL. A violation is an open error naming the table and the
/// requirement it broke, so the integrator learns it on their own device instead
/// of a peer's pull failing on the row.
fn validate_synced_table_contract(conn: &Connection, table: &str) -> Result<(), DbError> {
    match table_is_strict(conn, table)? {
        None => {
            return Err(DbError::Message(format!(
                "synced table {table:?} is declared in `synced_tables` but no migration \
                 creates it — add a `CREATE TABLE {table} (...) STRICT` to the schema \
                 migrations, or remove the declaration"
            )));
        }
        Some(false) => {
            return Err(DbError::Message(format!(
                "synced table {table:?} is not declared STRICT; the sync contract assumes typed \
                 columns (apply preserves storage classes peer-to-peer, LWW arbitration renders \
                 values to strings for comparison), which STRICT enforces at the insert — declare \
                 it STRICT: `CREATE TABLE {table} (...) STRICT`"
            )));
        }
        Some(true) => {}
    }

    let sql = format!(
        "PRAGMA table_info({})",
        crate::sync::session::quote_ident(table)
    );
    let mut stmt = conn.prepare(&sql).map_err(DbError::from)?;
    let mut columns = Vec::new();
    let rows = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                position: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
                declared_type: row.get::<_, String>(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                pk: row.get::<_, i64>(5)?,
            })
        })
        .map_err(DbError::from)?;
    for row in rows {
        columns.push(row.map_err(DbError::from)?);
    }

    let pk_columns: Vec<&ColumnInfo> = columns.iter().filter(|c| c.pk > 0).collect();
    let pk = match pk_columns.as_slice() {
        [single] => *single,
        [] => {
            return Err(DbError::Message(format!(
                "synced table {table:?} has no primary key; the contract requires a single \
                 `id` TEXT primary key at column 0"
            )))
        }
        _ => {
            let names: Vec<&str> = pk_columns.iter().map(|c| c.name.as_str()).collect();
            return Err(DbError::Message(format!(
                "synced table {table:?} has a composite primary key {names:?}; the contract \
                 requires a single `id` TEXT primary key at column 0"
            )));
        }
    };
    if pk.name != "id" {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key is {:?}, not `id`; the contract requires the \
             primary key to be the `id` column",
            pk.name
        )));
    }
    if pk.position != 0 {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key `id` is at column {}, not column 0; the \
             contract requires `id` to be the first column",
            pk.position
        )));
    }
    if !declared_as_text(&pk.declared_type) {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key `id` is declared {:?}, not TEXT; the contract \
             requires an `id` TEXT primary key",
            pk.declared_type
        )));
    }

    let updated_at = columns
        .iter()
        .find(|c| c.name == "_updated_at")
        .ok_or_else(|| {
            DbError::Message(format!(
                "synced table {table:?} has no `_updated_at` column; the contract requires \
                 `_updated_at TEXT NOT NULL`"
            ))
        })?;
    if !declared_as_text(&updated_at.declared_type) {
        return Err(DbError::Message(format!(
            "synced table {table:?} column `_updated_at` is declared {:?}, not TEXT; the \
             contract requires `_updated_at TEXT NOT NULL`",
            updated_at.declared_type
        )));
    }
    if !updated_at.not_null {
        return Err(DbError::Message(format!(
            "synced table {table:?} column `_updated_at` is nullable; the contract requires \
             `_updated_at TEXT NOT NULL`"
        )));
    }

    Ok(())
}

/// Whether a `PRAGMA table_info` declared type is TEXT, case-insensitively. SQL
/// keywords are case-insensitive, so `text` and `TEXT` both satisfy the contract;
/// any other declared type (or none) does not.
fn declared_as_text(declared_type: &str) -> bool {
    declared_type.eq_ignore_ascii_case("TEXT")
}

/// Whether `table` (in the `main` schema) is declared STRICT, via `PRAGMA
/// table_list`'s `strict` column (SQLite 3.37+) — the schema-level flag itself,
/// not `sqlite_master.sql` text, which a hand-formatted `CREATE TABLE` could spell
/// many ways. `None` means the table doesn't exist in `main` at all — a declared
/// synced table no migration created — which the caller reports as its own
/// contract error rather than folding into "not STRICT".
fn table_is_strict(conn: &Connection, table: &str) -> Result<Option<bool>, DbError> {
    let sql = format!(
        "PRAGMA table_list({})",
        crate::sync::session::quote_ident(table)
    );
    let mut stmt = conn.prepare(&sql).map_err(DbError::from)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(5)?))
        })
        .map_err(DbError::from)?;
    for row in rows {
        let (schema, strict) = row.map_err(DbError::from)?;
        if schema == "main" {
            return Ok(Some(strict != 0));
        }
    }
    Ok(None)
}

impl Database {
    #[doc(hidden)]
    pub fn new_write_id(&self) -> WriteId {
        WriteId::from_generated(self.state.write_ids.new_id())
    }

    fn notify_write_status_in(
        statuses: &Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>>,
        write_id: &WriteId,
        status: WriteStatus,
    ) {
        let senders = statuses.lock().expect("write status mutex poisoned");
        if let Some(sender) = senders.get(write_id) {
            sender.send_replace(status);
        }
    }

    fn set_write_status_on(
        conn: &Connection,
        write_id: &WriteId,
        status: &WriteStatus,
    ) -> Result<(), DbError> {
        let status = serde_json::to_string(status)
            .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
        let updated = conn
            .execute(
                "UPDATE store_writes SET status = ?2 WHERE write_id = ?1",
                rusqlite::params![write_id.as_str(), status],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!("write {write_id} does not exist")));
        }
        Ok(())
    }

    pub(crate) async fn set_write_status(
        &self,
        write_id: &WriteId,
        status: WriteStatus,
    ) -> Result<(), DbError> {
        let stored_id = write_id.clone();
        let stored_status = status.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            Self::set_write_status_on(conn, &stored_id, &stored_status)?;
            Self::notify_write_status_in(&statuses, &stored_id, stored_status);
            Ok(())
        })
        .await
    }

    pub async fn write_status(&self, write_id: &WriteId) -> Result<WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))
        })
        .await
    }

    pub async fn pending_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, status, affected_rows FROM store_writes
                     WHERE status IN ('\"pending\"', '\"publishing\"')
                        OR json_extract(status, '$.blocked') IS NOT NULL
                        OR json_extract(status, '$.conflict') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows.map(|row| {
                let (write_id, status, affected_rows) = row.map_err(DbError::from)?;
                Ok(PendingWrite {
                    write_id: WriteId::from_generated(write_id),
                    status: serde_json::from_str(&status).map_err(|error| {
                        DbError::Message(format!("pending write status: {error}"))
                    })?,
                    affected_rows: serde_json::from_str(&affected_rows).map_err(|error| {
                        DbError::Message(format!("pending affected rows: {error}"))
                    })?,
                })
            })
            .collect()
        })
        .await
    }

    /// Writes whose semantic publication fault requires an explicit host action.
    pub async fn blocked_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        Ok(self
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, WriteStatus::Blocked(_)))
            .collect())
    }

    /// Return one blocked write, or its whole Serial branch, to production
    /// publication. The next preparation attempt revalidates every captured fact;
    /// another semantic failure records a fresh `Blocked` status.
    pub async fn retry_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, raw_base, prepared): (String, String, Option<String>) = tx
                .query_row(
                    "SELECT status, base, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} status: {error}")))?;
            if !matches!(status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!("write {write_id} is not blocked")));
            }
            let base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} base: {error}")))?;

            let mut retried = Vec::new();
            match base {
                StoreWriteBase::MergeConcurrent { .. } => {
                    if let Some(raw_prepared) = prepared.as_deref() {
                        let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                            .map_err(|error| {
                                DbError::Message(format!("blocked write {write_id} preparation: {error}"))
                            })?;
                        if !matches!(prepared, PreparedStoreWriteState::MergeConcurrent { .. }) {
                            return Err(DbError::Message(format!(
                                "blocked MergeConcurrent write {write_id} has Serial preparation"
                            )));
                        }
                    }
                    let next = if prepared.is_some() {
                        WriteStatus::Publishing
                    } else {
                        WriteStatus::Pending
                    };
                    let next_json = serde_json::to_string(&next)
                        .map_err(|error| DbError::Message(format!("serialize retry status: {error}")))?;
                    let updated = tx
                        .execute(
                            "UPDATE store_writes SET status = ?2
                             WHERE write_id = ?1 AND json_extract(status, '$.blocked') IS NOT NULL",
                            rusqlite::params![write_id.as_str(), next_json],
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(format!(
                            "blocked write {write_id} changed during retry"
                        )));
                    }
                    retried.push((write_id, next));
                }
                StoreWriteBase::Serial {
                    branch_id,
                    base: branch_base,
                } => {
                    if prepared.is_some() {
                        return Err(DbError::Message(format!(
                            "blocked Serial branch {} retains publication preparation",
                            branch_id.first_write_id()
                        )));
                    }
                    let expected_base = StoreWriteBase::Serial {
                        branch_id: branch_id.clone(),
                        base: branch_base,
                    };
                    let mut statement = tx
                        .prepare(
                            "SELECT write_id, status, base, prepared FROM store_writes
                             WHERE base = ?1
                               AND status != '\"local_only\"'
                               AND json_extract(status, '$.published') IS NULL
                               AND json_extract(status, '$.resolved') IS NULL
                             ORDER BY ordinal",
                        )
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([&raw_base], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        })
                        .map_err(DbError::from)?;
                    let mut branch_write_ids = Vec::new();
                    for row in rows {
                        let (stored_id, raw_status, raw_base, prepared) =
                            row.map_err(DbError::from)?;
                        let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                            .map_err(|error| DbError::Message(format!("Serial retry base: {error}")))?;
                        if stored_base != expected_base {
                            return Err(DbError::Message(
                                "Serial database contains more than one unresolved branch"
                                    .to_string(),
                            ));
                        }
                        let stored_status: WriteStatus = serde_json::from_str(&raw_status)
                            .map_err(|error| DbError::Message(format!("Serial retry status: {error}")))?;
                        if !matches!(stored_status, WriteStatus::Pending | WriteStatus::Blocked(_))
                            || prepared.is_some()
                        {
                            return Err(DbError::Message(format!(
                                "Serial branch write {stored_id} is not pending or blocked without preparation"
                            )));
                        }
                        if matches!(stored_status, WriteStatus::Blocked(_)) {
                            branch_write_ids.push(WriteId::from_generated(stored_id));
                        }
                    }
                    drop(statement);
                    let pending = serde_json::to_string(&WriteStatus::Pending)
                        .map_err(|error| DbError::Message(format!("serialize retry status: {error}")))?;
                    for branch_write_id in branch_write_ids {
                        let updated = tx
                            .execute(
                                "UPDATE store_writes SET status = ?2
                                 WHERE write_id = ?1
                                   AND json_extract(status, '$.blocked') IS NOT NULL
                                   AND prepared IS NULL",
                                rusqlite::params![branch_write_id.as_str(), &pending],
                            )
                            .map_err(DbError::from)?;
                        if updated != 1 {
                            return Err(DbError::Message(format!(
                                "blocked Serial write {branch_write_id} changed during retry"
                            )));
                        }
                        retried.push((branch_write_id, WriteStatus::Pending));
                    }
                }
            }
            tx.commit().map_err(DbError::from)?;
            let retried_ids = retried
                .iter()
                .map(|(write_id, _)| write_id.clone())
                .collect();
            for (write_id, status) in retried {
                Self::notify_write_status_in(&statuses, &write_id, status);
            }
            Ok(retried_ids)
        })
        .await
    }

    /// Atomically reverse a blocked write and every later unpublished shared
    /// write whose working-row state depends on it.
    pub async fn discard_blocked_write(&self, write_id: &WriteId) -> Result<Vec<WriteId>, DbError> {
        let write_id = write_id.clone();
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, target_ordinal): (String, i64) = tx
                .query_row(
                    "SELECT status, ordinal FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let target_status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("blocked write {write_id} status: {error}")))?;
            if !matches!(target_status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!("write {write_id} is not blocked")));
            }

            let mut statement = tx
                .prepare(
                    "SELECT write_id, status, inverse_changeset FROM store_writes
                     WHERE ordinal >= ?1
                       AND status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([target_ordinal], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut discarded = Vec::new();
            for row in rows {
                let (stored_id, raw_status, inverse) = row.map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::Message(format!("discard write status: {error}")))?;
                if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!(
                        "write {stored_id} after blocked write {write_id} has non-discardable status {status:?}"
                    )));
                }
                discarded.push((WriteId::from_generated(stored_id), inverse));
            }
            drop(statement);
            if discarded.first().map(|(stored_id, _)| stored_id) != Some(&write_id) {
                return Err(DbError::Message(format!(
                    "blocked write {write_id} is absent from its unpublished suffix"
                )));
            }
            let schema = Arc::new(crate::sync::conflict::TableSchema::from_db(
                &tx,
                &synced_tables,
            )?);
            for (_, inverse) in discarded.iter().rev() {
                let inverse = crate::sync::apply::ValidatedChangeset::new(
                    inverse,
                    schema.clone(),
                )
                .map_err(|error| DbError::Message(format!("invalid blocked-write inverse: {error}")))?;
                crate::sync::apply::apply_changeset_strict_on(&tx, inverse, &[])
                    .map_err(|error| DbError::Message(format!("reverse blocked-write suffix: {error}")))?;
            }
            let discarded_ids: Vec<_> = discarded
                .into_iter()
                .map(|(write_id, _)| write_id)
                .collect();
            let resolution = WriteResolution::Discarded;
            Self::resolve_unpublished_writes_on(&tx, &discarded_ids, &resolution)?;
            tx.commit().map_err(DbError::from)?;
            let status = WriteStatus::Resolved(resolution);
            for discarded_id in &discarded_ids {
                Self::notify_write_status_in(&statuses, discarded_id, status.clone());
            }
            Ok(discarded_ids)
        })
        .await
    }

    pub async fn pending_branches(&self) -> Result<Option<PendingBranch>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, status, affected_rows, base FROM store_writes
                     WHERE status IN ('\"pending\"', '\"publishing\"')
                        OR json_extract(status, '$.blocked') IS NOT NULL
                        OR json_extract(status, '$.conflict') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut records = Vec::new();
            for row in rows {
                let (write_id, status, affected_rows, base) = row.map_err(DbError::from)?;
                records.push((
                    PendingWrite {
                        write_id: WriteId::from_generated(write_id),
                        status: serde_json::from_str(&status).map_err(|error| {
                            DbError::Message(format!("pending write status: {error}"))
                        })?,
                        affected_rows: serde_json::from_str(&affected_rows).map_err(|error| {
                            DbError::Message(format!("pending affected rows: {error}"))
                        })?,
                    },
                    serde_json::from_str::<StoreWriteBase>(&base).map_err(|error| {
                        DbError::Message(format!("pending write base: {error}"))
                    })?,
                ));
            }
            drop(statement);

            let mut conflict = None;
            for (write, base) in &records {
                let WriteStatus::Conflict(candidate) = &write.status else {
                    continue;
                };
                let StoreWriteBase::Serial {
                    branch_id,
                    base: stored_base,
                } = base
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base carries a Serial conflict".to_string(),
                    ));
                };
                if branch_id != &candidate.branch_id || stored_base != &candidate.base {
                    return Err(DbError::Message(
                        "Serial conflict status differs from its durable branch base".to_string(),
                    ));
                }
                match &conflict {
                    None => conflict = Some(candidate.clone()),
                    Some(existing) if existing == candidate => {}
                    Some(_) => {
                        return Err(DbError::Message(
                            "Serial database contains more than one conflict branch".to_string(),
                        ))
                    }
                }
            }
            let Some(conflict) = conflict else {
                return Ok(None);
            };
            let expected_base = StoreWriteBase::Serial {
                branch_id: conflict.branch_id.clone(),
                base: conflict.base.clone(),
            };
            let mut writes = Vec::new();
            for (write, base) in records {
                if matches!(base, StoreWriteBase::MergeConcurrent { .. }) {
                    continue;
                }
                if base != expected_base {
                    return Err(DbError::Message(
                        "Serial database contains more than one unresolved branch".to_string(),
                    ));
                }
                if !matches!(
                    &write.status,
                    WriteStatus::Conflict(_) | WriteStatus::Pending
                ) {
                    return Err(DbError::Message(format!(
                        "conflicted Serial branch write {} has non-resolvable status {:?}",
                        write.write_id, write.status
                    )));
                }
                writes.push(write);
            }
            Ok(Some(PendingBranch {
                branch_id: conflict.branch_id,
                base: conflict.base,
                current: conflict.current,
                writes,
            }))
        })
        .await
    }

    pub(crate) async fn unresolved_serial_branch(
        &self,
    ) -> Result<Option<UnresolvedSerialBranch>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT base, status FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                       AND json_type(base, '$.serial') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let mut branch = None;
            for row in rows {
                let (raw_base, raw_status) = row.map_err(DbError::from)?;
                let StoreWriteBase::Serial { branch_id, base } = serde_json::from_str(&raw_base)
                    .map_err(|error| {
                        DbError::Message(format!("unresolved Serial base: {error}"))
                    })?
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base reached a Serial branch query".to_string(),
                    ));
                };
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("unresolved Serial status: {error}"))
                })?;
                match &mut branch {
                    None => {
                        branch = Some(UnresolvedSerialBranch {
                            branch_id,
                            base,
                            conflicted: matches!(status, WriteStatus::Conflict(_)),
                        });
                    }
                    Some(existing) if existing.branch_id == branch_id && existing.base == base => {
                        existing.conflicted |= matches!(status, WriteStatus::Conflict(_));
                    }
                    Some(_) => {
                        return Err(DbError::Message(
                            "Serial database contains more than one unresolved branch".to_string(),
                        ));
                    }
                }
            }
            Ok(branch)
        })
        .await
    }

    pub async fn subscribe_write_status(
        &self,
        write_id: &WriteId,
    ) -> Result<tokio::sync::watch::Receiver<WriteStatus>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let current: WriteStatus = serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))?;
            let mut senders = statuses.lock().expect("write status mutex poisoned");
            let sender = senders
                .entry(write_id)
                .or_insert_with(|| tokio::sync::watch::channel(current.clone()).0);
            sender.send_replace(current);
            Ok(sender.subscribe())
        })
        .await
    }

    /// Lock the complete membership load transaction for this database handle
    /// and every clone that shares its state.
    pub(crate) async fn lock_membership_load(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.state.membership_load.clone().lock_owned().await
    }

    pub(crate) async fn lock_membership_mutation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.state.membership_mutation.clone().lock_owned().await
    }

    pub(crate) async fn lock_snapshot_publication(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.state.snapshot_publication.clone().lock_owned().await
    }

    pub(crate) async fn lock_local_blob_cleanup(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.state.local_blob_cleanup.clone().lock_owned().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn arm_test_pause(
        &self,
        point: DatabaseTestPoint,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        self.state.test_pause_points.arm(point)
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn observe_test_points(&self) -> tokio::sync::mpsc::UnboundedReceiver<DatabaseTestPoint> {
        self.state.test_pause_points.observe()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn reach_test_point(&self, point: DatabaseTestPoint) {
        self.state.test_pause_points.reach(point).await;
    }

    /// Open and own the connection at `path`.
    ///
    /// Runs the host migration ladder and validates its final sync-routing
    /// contract in one transaction. A fresh database creates Coven metadata in
    /// that transaction; an initialized database commits only when the final
    /// contract exactly matches its pinned bytes. Then seeds the register clock
    /// from on-disk rows. Returns the handle plus the non-optional `_updated_at`
    /// stamper the host binds into every synced-row write.
    ///
    pub fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
            Arc::new(hlc),
            migrations,
            CovenMetadataOpen::Detect,
        )
    }

    pub(crate) fn open_initialized_store(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
            Arc::new(hlc),
            migrations,
            CovenMetadataOpen::InitializeVerifiedSnapshot,
        )
    }

    /// Open with a caller-supplied register clock instead of a fresh
    /// system-wall-clock one. Lets a test inject an [`Hlc`] over a controlled
    /// wall clock to exercise the skew/restart-seeding guarantees, sharing the
    /// production open path (migration, seed, session) so the test drives the
    /// real unit.
    ///
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
            hlc,
            migrations,
            CovenMetadataOpen::Detect,
        )
    }

    fn open_with_hlc_and_coven_metadata(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen,
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let (core, state, stamper) = DatabaseCore::open(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
            hlc,
            migrations,
            metadata_open,
        )?;

        let database = {
            let (jobs_tx, jobs_rx) = tokio::sync::mpsc::unbounded_channel::<DbJob>();
            let join = std::thread::Builder::new()
                .name("coven-db".to_string())
                .spawn(move || run_connection_thread(core, jobs_rx))
                .map_err(|e| DbError::Message(format!("spawn database connection thread: {e}")))?;
            Database {
                thread: Arc::new(ConnectionThread {
                    jobs: jobs_tx,
                    join: Some(join),
                }),
                state,
            }
        };

        Ok((database, stamper))
    }

    /// Open the store at `path` read-only for a same-store secondary reader
    /// (e.g. a separate process reading while another holds the writer open).
    ///
    /// Distinct from [`Database::open`] in three ways, all so the reader never
    /// mutates shared state a concurrent writer owns: the connection is
    /// `SQLITE_OPEN_READONLY`; no migration ladder or bookkeeping DDL runs (it
    /// opens against the schema the writer left, and refuses one newer than this
    /// binary knows — the writer's `SchemaTooNew` policy); and it returns no
    /// stamper, because a reader mints no `_updated_at`. Reads are safe across
    /// processes because the writer opens the db in WAL mode, so a reader observes
    /// committed rows while the writer commits more.
    ///
    /// The caller takes no store open-lock for a read-only open: the exclusive
    /// advisory lock guards against a second *writer*, and a read-only connection
    /// cannot write, so multiple readers and one writer coexist under WAL.
    pub fn open_read_only(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        write_policy: WritePolicy,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
        let (core, state) = DatabaseCore::open_read_only(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            write_policy,
            Arc::new(hlc),
            migrations,
        )?;
        let (jobs_tx, jobs_rx) = tokio::sync::mpsc::unbounded_channel::<DbJob>();
        let join = std::thread::Builder::new()
            .name("coven-db-ro".to_string())
            .spawn(move || run_connection_thread(core, jobs_rx))
            .map_err(|e| DbError::Message(format!("spawn database connection thread: {e}")))?;
        Ok(Database {
            thread: Arc::new(ConnectionThread {
                jobs: jobs_tx,
                join: Some(join),
            }),
            state,
        })
    }

    /// The host's declared synced-table set, the single owner of which tables
    /// participate in changeset sync. Each journaled write's capture session
    /// attaches exactly these, the register-clock seed scanned these, and the
    /// gate/apply operate over these — so the sync layer reads the set from here
    /// instead of carrying a separately-passed copy that could silently diverge.
    pub fn synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    /// The host's blob-tombstone convergence window. The tombstone GC ages each
    /// tombstone's `deleted_at` against this to decide when a deleted blob may be
    /// erased. Fixed for this handle's life.
    pub fn blob_tombstone_grace(&self) -> chrono::Duration {
        self.state.blob_tombstone_grace
    }

    /// How many blob transfers each transfer loop may run at once. Read by the
    /// upload drain ([`crate::blob::upload::drain_uploads`]) and the pin loop
    /// ([`crate::blob::cache::pin`]). Fixed for this handle's life.
    pub fn transfer_limits(&self) -> crate::blob::TransferLimits {
        self.state.transfer_limits
    }

    pub fn write_policy(&self) -> WritePolicy {
        self.state.write_policy
    }

    /// The gate model resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    #[doc(hidden)]
    pub fn gates(&self) -> Arc<Gates> {
        self.state.gates.clone()
    }

    /// Blob declarations resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    #[doc(hidden)]
    pub fn blob_decls(&self) -> Arc<BlobDecls> {
        self.state.blob_decls.clone()
    }

    /// The applied synced-schema version — `PRAGMA user_version` after the
    /// migration ladder ran at open. This is the single source of the wire
    /// `schema_version`: every outgoing changeset is stamped with it, the pull
    /// gates compare incoming changesets and the min-floor against it, and the
    /// snapshot meta carries it. A device cannot stamp a version it has not
    /// migrated to. Cached because migrations run only at open, so the value is
    /// fixed for the handle's life.
    pub fn schema_version(&self) -> u32 {
        self.state.schema_version
    }

    /// Hash of the declarations and live schema shape that decide row routing
    /// and confidentiality for this Store.
    pub fn sync_routing_hash(&self) -> ObjectHash {
        self.state.sync_routing_hash
    }

    /// The shared register clock. coven's sync layer records pulled rows as its
    /// floor and stamps envelopes off it; it is the same `Arc<Hlc>` the stamper wraps.
    pub fn hlc(&self) -> Arc<Hlc> {
        self.state.hlc.clone()
    }

    pub fn stamper(&self) -> UpdatedAtStamper {
        UpdatedAtStamper::new(self.state.hlc.clone())
    }

    /// The receiver's current wall-clock millis, read from this database's
    /// register clock. The pull reads it once and passes it down to bound an
    /// incoming `_updated_at`'s physical component (a grossly-future stamp must not
    /// win last-writer-wins or ratchet the clock).
    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.state.hlc.wall_now_ms()
    }

    /// Run `f` against the connection and await the result.
    ///
    /// This is how coven runs bookkeeping, gating reads, raw test writes, and
    /// apply — anything that needs `&Connection`. Public host writes and coven
    /// transitions that mutate synced rows wrap their write in a
    /// [`Self::run_internal_store_write_transaction_on`] transaction (still through
    /// `call`) so it lands in the pending-changeset journal.
    ///
    /// Hands `f` to the connection thread and awaits its reply, so the SQL runs
    /// off the async executor.
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| f(core.connection()))
            .await
    }

    /// Send `f` to the connection thread, run it against the owned core there, and
    /// await its result. A panic in `f` is caught on the connection thread (so it
    /// cannot unwind the thread and take the connection with it) and resumed on
    /// this task, matching the pre-thread behavior where the closure panicked
    /// directly on the caller.
    ///
    /// Cancellation: once dispatched, `f` runs to completion on the connection
    /// thread regardless of whether the caller is still awaiting. If the caller is
    /// cancelled between the thread committing and this reply resolving, it never
    /// observes the result even though the effect landed — the same "the operation
    /// may have committed" contract any network call carries. This is deliberate:
    /// the durable database state is the source of truth, and a caller must treat
    /// a cancelled call as possibly-committed. Follow-ups that matter beyond that
    /// durable state — observer notifications, publish triggers — are not driven
    /// off this return value; the sync cycle re-derives them from durable state.
    async fn on_connection_thread<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut DatabaseCore) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let job = DbJob::Run(Box::new(move |core| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(core)));
            // The caller may have been cancelled and dropped `reply_rx`; a failed
            // send is that normal outcome, not an error.
            let _ = reply_tx.send(outcome);
        }));
        if self.thread.jobs.send(job).is_err() {
            panic!("database connection thread stopped before a call completed");
        }
        match reply_rx.await {
            Ok(Ok(value)) => value,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => {
                panic!("database connection thread dropped a call's reply without responding")
            }
        }
    }

    fn start_host_change_journal_on<'c>(
        conn: &'c Connection,
        synced_tables: &[SyncedTable],
    ) -> Result<rusqlite::session::Session<'c>, DbError> {
        attach_session(conn, synced_tables)
    }

    fn drain_host_change_journal_on(
        session: &mut rusqlite::session::Session<'_>,
    ) -> Result<Vec<u8>, DbError> {
        capture_changeset(session)
    }

    fn invert_changeset(changeset: &[u8]) -> Result<Vec<u8>, DbError> {
        if changeset.is_empty() {
            return Ok(Vec::new());
        }
        let mut inverse = Vec::new();
        rusqlite::session::invert_strm(&mut &changeset[..], &mut inverse).map_err(DbError::from)?;
        Ok(inverse)
    }

    fn run_host_sql_on<R, E>(conn: &Connection, f: impl FnOnce() -> Result<R, E>) -> Result<R, E>
    where
        E: From<DbError>,
    {
        conn.authorizer(Some(authorize_host_sql))
            .map_err(DbError::from)
            .map_err(E::from)?;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        if let Err(error) = conn.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        ) {
            panic!("failed to remove host SQL gate-baseline guard: {error}");
        }
        match outcome {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn capture_host_changes_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        f: impl FnOnce() -> Result<R, E>,
    ) -> Result<(R, Vec<u8>), E>
    where
        E: From<DbError>,
    {
        let mut journal =
            Self::start_host_change_journal_on(conn, synced_tables).map_err(E::from)?;
        let value = Self::run_host_sql_on(conn, f)?;
        let captured = Self::drain_host_change_journal_on(&mut journal).map_err(E::from)?;
        crate::sync::session::validate_changeset_row_identities(&captured, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))
            .map_err(E::from)?;
        Ok((value, captured))
    }

    fn capture_store_write_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        changeset: &[u8],
        gates: &Gates,
        blob_decls: &BlobDecls,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let changes = crate::changeset::walk(changeset)
            .map_err(|error| DbError::Message(format!("read Store write blobs: {error}")))?;
        let mut facts = BTreeMap::new();
        for change in changes {
            let Some((blob, size)) = blob_decls
                .publication_blob_from_change(tx, &change)
                .map_err(|error| DbError::Message(format!("capture Store write blob: {error}")))?
            else {
                continue;
            };
            let pk = change.pk().ok_or_else(|| {
                DbError::Message(format!(
                    "blob-bearing Store write row in {:?} has no primary key",
                    change.table
                ))
            })?;
            let fact = match blob.provenance {
                Provenance::UserProvided => {
                    let local = tx
                        .query_row(
                            "SELECT 1 FROM local_blob_refs WHERE blob_id = ?1",
                            [&blob.id],
                            |_| Ok(()),
                        )
                        .optional()
                        .map_err(DbError::from)?
                        .is_some();
                    StoreWriteBlobFact::UserProvided {
                        blob,
                        state: if local {
                            StoreWriteUserBlobState::Local
                        } else {
                            StoreWriteUserBlobState::Remote
                        },
                    }
                }
                Provenance::HostProvided => {
                    let state =
                        match gates
                            .resolve_root_of(tx, &change.table, pk)
                            .map_err(|error| {
                                DbError::Message(format!("resolve Store write blob root: {error}"))
                            })? {
                            Some((root_table, root_id)) => {
                                match Self::make_remote_intent_retain_pinned(
                                    tx,
                                    &root_table,
                                    &root_id,
                                )? {
                                    Some(retain_pinned) => StoreWriteHostBlobState::MakeRemote {
                                        root_table,
                                        root_id,
                                        retain_pinned,
                                    },
                                    None => StoreWriteHostBlobState::Ordinary,
                                }
                            }
                            None => StoreWriteHostBlobState::Ordinary,
                        };
                    StoreWriteBlobFact::HostProvided { blob, size, state }
                }
            };
            let key = (fact.blob().namespace.clone(), fact.blob().id.clone());
            if let Some(prior) = facts.insert(key.clone(), fact.clone()) {
                if prior != fact {
                    return Err(DbError::Message(format!(
                        "Store write gives blob {}/{} conflicting publication facts",
                        key.0, key.1
                    )));
                }
            }
        }
        Ok(StoreWriteBlobFacts {
            blobs: facts.into_values().collect(),
        })
    }

    fn capture_partition_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        partitions: &[gate::AudiencePartition],
        gates: &Gates,
        blob_decls: &BlobDecls,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let mut facts = BTreeMap::new();
        for partition in partitions {
            for fact in Self::capture_store_write_blob_facts_on(
                tx,
                &partition.changeset,
                gates,
                blob_decls,
            )?
            .blobs
            {
                let key = (fact.blob().namespace.clone(), fact.blob().id.clone());
                if let Some(prior) = facts.insert(key.clone(), fact.clone()) {
                    if prior != fact {
                        return Err(DbError::Message(format!(
                            "audience partitions give blob {}/{} conflicting publication facts",
                            key.0, key.1
                        )));
                    }
                }
            }
        }
        Ok(StoreWriteBlobFacts {
            blobs: facts.into_values().collect(),
        })
    }

    fn partition_captured_write_on(
        tx: &rusqlite::Transaction<'_>,
        captured: &[u8],
        gates: &Gates,
        write_policy: WritePolicy,
        routing: StoreWriteRouting<'_>,
    ) -> Result<Vec<gate::AudiencePartition>, DbError> {
        match routing {
            StoreWriteRouting::MergeScoped(encryption) => {
                let store_root_hash = Self::required_store_root_hash_on(tx)?;
                let key = crate::sync::circle::derive_row_routing_key(encryption, store_root_hash)
                    .map_err(|error| {
                        DbError::Message(format!("derive row routing key: {error}"))
                    })?;
                let routing_changeset = gate::capture_routing_changes(tx, captured, gates, &key)
                    .map_err(|error| {
                        DbError::Message(format!("capture scoped routing changes: {error}"))
                    })?;
                gate::partition_outbound(tx, captured, &routing_changeset, gates, write_policy)
                    .map_err(|error| {
                        DbError::Message(format!("partition scoped host transaction: {error}"))
                    })
            }
            StoreWriteRouting::SerialScoped => gate::partition_outbound(
                tx,
                captured,
                &gate::RoutingChanges::empty(),
                gates,
                write_policy,
            )
            .map_err(|error| {
                DbError::Message(format!("partition scoped host transaction: {error}"))
            }),
            StoreWriteRouting::Unscoped => {
                let changeset = gate::gate_outbound(tx, captured, gates)
                    .map_err(|error| DbError::Message(format!("gate host transaction: {error}")))?;
                Ok((!changeset.is_empty())
                    .then_some(gate::AudiencePartition {
                        audience: crate::sync::circle::Audience::Store,
                        control: None,
                        changeset,
                    })
                    .into_iter()
                    .collect())
            }
        }
    }

    fn store_write_routing<'a>(
        gates: &Gates,
        write_policy: WritePolicy,
        routing_encryption: Option<&'a EncryptionService>,
    ) -> Result<StoreWriteRouting<'a>, DbError> {
        if !gates.has_scoped_graph() {
            return Ok(StoreWriteRouting::Unscoped);
        }
        match write_policy {
            WritePolicy::MergeConcurrent => routing_encryption
                .map(StoreWriteRouting::MergeScoped)
                .ok_or_else(|| {
                    DbError::Message(
                        "Merge scoped write requires the Store generation-1 routing key"
                            .to_string(),
                    )
                }),
            WritePolicy::Serial => Ok(StoreWriteRouting::SerialScoped),
        }
    }

    pub(crate) fn validate_store_write_routing(
        gates: &Gates,
        write_policy: WritePolicy,
        routing_encryption: Option<&EncryptionService>,
    ) -> Result<(), DbError> {
        Self::store_write_routing(gates, write_policy, routing_encryption).map(drop)
    }

    fn insert_store_write_on(
        tx: &rusqlite::Transaction<'_>,
        write_id: &WriteId,
        partitions: &[gate::AudiencePartition],
        inverse_changeset: &[u8],
        base: &StoreWriteBase,
        blob_facts: &StoreWriteBlobFacts,
        rows_changed: u64,
    ) -> Result<WriteStatus, DbError> {
        let affected_rows = if partitions.is_empty() {
            // A tripwire, not a routine event. An empty capture from a transaction
            // that also CHANGED NO ROWS is a pure read left on the write path —
            // warn so it gets moved to the journal-free read path
            // (`CovenHandle::sql_read`). An empty capture
            // from a transaction that DID change rows is a device-local-table
            // write (those tables aren't in the session) — a supported, routine
            // pattern that stays on `sql()` silently. The one case this misses:
            // a conditional write to a synced table that no-op'd this cycle (an
            // idempotent INSERT OR IGNORE re-run) also changed no rows and warns;
            // legitimate but rare, tolerated.
            if rows_changed == 0 {
                warn!("journaled sql transaction changed nothing; pure reads belong on sql_read");
                // Debug builds name the offender: the backtrace runs through the
                // host's monomorphized closure, whose symbol carries the call
                // site's module path. Captured only when the warn fires.
                #[cfg(debug_assertions)]
                warn!(
                    "zero-change sql transaction backtrace:\n{}",
                    std::backtrace::Backtrace::force_capture()
                );
            }
            Vec::new()
        } else {
            let mut affected = Vec::new();
            for partition in partitions {
                affected.extend(
                    crate::changeset::walk(&partition.changeset)
                        .map_err(|error| {
                            DbError::Message(format!("read affected write rows: {error}"))
                        })?
                        .into_iter()
                        .filter(|row| !gate::is_routing_table(&row.table))
                        .map(|row| {
                            let primary_key = row.pk().map(str::to_owned).ok_or_else(|| {
                                DbError::Message(format!(
                                    "shared write row in {:?} has no primary key",
                                    row.table
                                ))
                            })?;
                            Ok(AffectedRow {
                                table: row.table,
                                primary_key,
                            })
                        })
                        .collect::<Result<Vec<_>, DbError>>()?,
                );
            }
            affected.sort();
            affected.dedup();
            affected
        };
        let status = if partitions.is_empty() {
            WriteStatus::LocalOnly
        } else {
            WriteStatus::Pending
        };
        let base = serde_json::to_string(base)
            .map_err(|error| DbError::Message(format!("serialize pending Store base: {error}")))?;
        let status_json = serde_json::to_string(&status)
            .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
        let affected_rows = serde_json::to_string(&affected_rows)
            .map_err(|error| DbError::Message(format!("serialize affected rows: {error}")))?;
        let blob_facts_json = serde_json::to_string(blob_facts).map_err(|error| {
            DbError::Message(format!("serialize Store write blob facts: {error}"))
        })?;
        let store_changeset = partitions
            .iter()
            .find(|partition| partition.audience == crate::sync::circle::Audience::Store)
            .map(|partition| partition.changeset.as_slice())
            .unwrap_or_default();
        tx.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                write_id.as_str(),
                status_json,
                affected_rows,
                store_changeset,
                inverse_changeset,
                base,
                blob_facts_json,
            ],
        )
        .map_err(DbError::from)?;
        for partition in partitions {
            let audience = match partition.audience {
                crate::sync::circle::Audience::Store => "store".to_string(),
                crate::sync::circle::Audience::Local => {
                    return Err(DbError::Message(
                        "Local audience must not enter the durable outbound journal".to_string(),
                    ));
                }
                crate::sync::circle::Audience::Circle(circle_id) => circle_id.to_string(),
            };
            let control = partition
                .control
                .as_ref()
                .map(gate::CirclePartitionControl::stored_json);
            tx.execute(
                "INSERT INTO store_write_partitions
                 (write_id, audience, control_coord, changeset)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![write_id.as_str(), audience, control, partition.changeset,],
            )
            .map_err(DbError::from)?;
        }
        if status == WriteStatus::Pending {
            for fact in &blob_facts.blobs {
                let StoreWriteBlobFact::HostProvided { blob, .. } = fact else {
                    continue;
                };
                tx.execute(
                    "INSERT INTO store_write_blob_leases (write_id, namespace, blob_id) \
                     VALUES (?1, ?2, ?3)",
                    (write_id.as_str(), &blob.namespace, &blob.id),
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(status)
    }

    pub fn run_store_write_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        gates: &Gates,
        blob_decls: &BlobDecls,
        write_policy: WritePolicy,
        routing_encryption: Option<&EncryptionService>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<WriteReceipt<R>, E>
    where
        E: From<DbError>,
    {
        (|| {
            let routing = Self::store_write_routing(gates, write_policy, routing_encryption)
                .map_err(E::from)?;
            let changes_before = conn.total_changes();
            let tx = conn
                .unchecked_transaction()
                .map_err(DbError::from)
                .map_err(E::from)?;
            let (value, captured) = Self::capture_host_changes_on(&tx, synced_tables, || f(&tx))?;
            let partitions =
                Self::partition_captured_write_on(&tx, &captured, gates, write_policy, routing)
                    .map_err(E::from)?;
            let blob_facts =
                Self::capture_partition_blob_facts_on(&tx, &partitions, gates, blob_decls)
                    .map_err(E::from)?;
            let rows_changed = conn.total_changes().saturating_sub(changes_before);
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)
                .map_err(E::from)?;
            let inverse_changeset = Self::invert_changeset(&captured).map_err(E::from)?;
            let base = match write_policy {
                WritePolicy::MergeConcurrent => StoreWriteBase::MergeConcurrent {
                    dependencies: Self::materialized_frontier_on(&tx, Some(&local_device_id))
                        .map_err(E::from)?,
                },
                WritePolicy::Serial => {
                    let existing: Option<String> = tx
                        .query_row(
                            "SELECT base FROM store_writes
                             WHERE status != '\"local_only\"'
                               AND json_extract(status, '$.published') IS NULL
                               AND json_extract(status, '$.resolved') IS NULL
                               AND json_type(base, '$.serial') IS NOT NULL
                             ORDER BY ordinal LIMIT 1",
                            [],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(DbError::from)
                        .map_err(E::from)?;
                    match existing {
                        Some(existing) => serde_json::from_str(&existing)
                            .map_err(|error| {
                                DbError::Message(format!("pending serial branch base: {error}"))
                            })
                            .map_err(E::from)?,
                        None => StoreWriteBase::Serial {
                            branch_id: PendingBranchId::from_first_write(write_id.clone()),
                            base: Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)
                                .map_err(E::from)?,
                        },
                    }
                }
            };
            let status = Self::insert_store_write_on(
                &tx,
                &write_id,
                &partitions,
                &inverse_changeset,
                &base,
                &blob_facts,
                rows_changed,
            )
            .map_err(E::from)?;
            tx.commit().map_err(DbError::from).map_err(E::from)?;
            Ok(WriteReceipt {
                value,
                write_id,
                status,
            })
        })()
    }

    pub fn run_internal_store_write_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        write_policy: WritePolicy,
        routing_encryption: Option<&EncryptionService>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        let gates = Gates::from_tables(conn, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))
            .map_err(E::from)?;
        let blob_decls = BlobDecls::from_tables(conn, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))
            .map_err(E::from)?;
        Self::run_store_write_transaction_on(
            conn,
            synced_tables,
            &gates,
            &blob_decls,
            write_policy,
            routing_encryption,
            write_id,
            f,
        )
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn prepare_store_write(&self) -> Result<Option<PreparedStoreWrite>, DbError> {
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let stored = conn
                .query_row(
                "SELECT write_id, changeset, inverse_changeset, base, blob_facts FROM store_writes
                 WHERE status = '\"pending\"'
                   AND ordinal = (
                       SELECT MIN(ordinal) FROM store_writes
                       WHERE status != '\"local_only\"'
                         AND json_extract(status, '$.published') IS NULL
                         AND json_extract(status, '$.resolved') IS NULL
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM store_writes WHERE prepared IS NOT NULL
                   )
                 ORDER BY ordinal LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
                .optional()
                .map_err(DbError::from)?;
            let Some((write_id, changeset, inverse_changeset, base, blob_facts)) = stored else {
                return Ok(None);
            };
            let partitions = Self::prepared_store_write_partitions_on(
                conn,
                &write_id,
                &changeset,
                write_policy,
            )?;
            Ok(Some(PreparedStoreWrite {
                write_id: WriteId::from_generated(write_id),
                changeset,
                partitions,
                inverse_changeset,
                base: serde_json::from_str(&base)
                    .map_err(|error| DbError::Message(format!("pending write base: {error}")))?,
                blob_facts: serde_json::from_str(&blob_facts).map_err(|error| {
                    DbError::Message(format!("pending write blob facts: {error}"))
                })?,
            }))
        })
        .await
    }

    fn prepared_store_write_partitions_on(
        conn: &Connection,
        write_id: &str,
        stored_store_changeset: &[u8],
        write_policy: WritePolicy,
    ) -> Result<PreparedStoreWritePartitions, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions
                 WHERE write_id = ?1
                 ORDER BY CASE audience WHEN 'store' THEN 0 ELSE 1 END,
                          audience, control_coord",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([write_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut store = None;
        let mut circles = Vec::new();
        for row in rows {
            let (audience, control, changeset) = row.map_err(DbError::from)?;
            if audience == "store" {
                if control.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} Store partition carries a Circle control"
                    )));
                }
                if store.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} carries more than one Store partition"
                    )));
                }
                store = Some(gate::AudiencePartition {
                    audience: crate::sync::circle::Audience::Store,
                    control: None,
                    changeset,
                });
                continue;
            }
            let circle_id = audience
                .parse::<crate::sync::circle::CircleId>()
                .map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} has invalid audience {audience:?}: {error}"
                    ))
                })?;
            let control_json = control.ok_or_else(|| {
                DbError::Message(format!(
                    "pending write {write_id} Circle {circle_id} has no control coordinate"
                ))
            })?;
            let control =
                gate::CirclePartitionControl::from_stored_json(control_json).map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} Circle {circle_id} control coordinate: {error}"
                    ))
                })?;
            let control_policy = match control.coordinate() {
                crate::sync::circle::CircleControlCoord::MergeConcurrent { .. } => {
                    WritePolicy::MergeConcurrent
                }
                crate::sync::circle::CircleControlCoord::Serial { .. } => WritePolicy::Serial,
            };
            if control_policy != write_policy {
                return Err(DbError::Message(format!(
                    "pending write {write_id} Circle {circle_id} control uses {control_policy:?}, database uses {write_policy:?}"
                )));
            }
            circles.push(gate::AudiencePartition {
                audience: crate::sync::circle::Audience::Circle(circle_id),
                control: Some(control),
                changeset,
            });
        }
        drop(statement);
        match &store {
            Some(partition) if partition.changeset != stored_store_changeset => {
                return Err(DbError::Message(format!(
                    "pending write {write_id} Store partition differs from store_writes.changeset"
                )));
            }
            None if !stored_store_changeset.is_empty() => {
                return Err(DbError::Message(format!(
                    "pending write {write_id} has a store_writes.changeset without a Store partition"
                )));
            }
            Some(_) | None => {}
        }
        if store.is_none() && circles.is_empty() {
            return Err(DbError::Message(format!(
                "pending write {write_id} has no durable audience partitions"
            )));
        }
        Ok(PreparedStoreWritePartitions { store, circles })
    }

    pub(crate) async fn reserve_serial_store_branch(
        &self,
    ) -> Result<Option<SerialStoreBranchPreparationWork>, DbError> {
        if self.write_policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial branch reservation requires the Serial write policy".to_string(),
            ));
        }
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, changeset, inverse_changeset, base, blob_facts, status, prepared
                     FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row.map_err(DbError::from)?);
            }
            drop(statement);
            let Some(first) = records.first() else {
                tx.commit().map_err(DbError::from)?;
                return Ok(None);
            };
            let first_base: StoreWriteBase = serde_json::from_str(&first.3)
                .map_err(|error| DbError::Message(format!("pending Serial write base: {error}")))?;
            let StoreWriteBase::Serial { branch_id, base } = first_base else {
                return Err(DbError::Message(
                    "MergeConcurrent write exists in a Serial database".to_string(),
                ));
            };
            let preparing = serde_json::to_string(&PreparedStoreWriteState::SerialPreparing)
                .map_err(|error| DbError::Message(format!("serialize Serial reservation: {error}")))?;
            let publishing = serde_json::to_string(&WriteStatus::Publishing)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
            let mut writes = Vec::new();
            let mut newly_reserved = Vec::new();
            for (write_id, changeset, inverse_changeset, raw_base, blob_facts, status, prepared) in
                records
            {
                let parsed_base: StoreWriteBase = serde_json::from_str(&raw_base)
                    .map_err(|error| DbError::Message(format!("pending Serial write base: {error}")))?;
                if parsed_base
                    != (StoreWriteBase::Serial {
                        branch_id: branch_id.clone(),
                        base: base.clone(),
                    })
                {
                    break;
                }
                match (status.as_str(), prepared.as_deref()) {
                    ("\"pending\"", None) => {
                        let updated = tx
                            .execute(
                                "UPDATE store_writes SET status = ?2, prepared = ?3
                                 WHERE write_id = ?1 AND status = '\"pending\"' AND prepared IS NULL",
                                rusqlite::params![&write_id, &publishing, &preparing],
                            )
                            .map_err(DbError::from)?;
                        if updated != 1 {
                            return Err(DbError::Message(format!(
                                "Serial write {write_id} lost branch reservation"
                            )));
                        }
                        newly_reserved.push(WriteId::from_generated(write_id.clone()));
                    }
                    ("\"publishing\"", Some(stored)) if stored == preparing => {}
                    ("\"publishing\"", Some(_)) => {
                        tx.commit().map_err(DbError::from)?;
                        return Ok(None);
                    }
                    _ => {
                        tx.commit().map_err(DbError::from)?;
                        return Ok(None);
                    }
                }
                let partitions = Self::prepared_store_write_partitions_on(
                    &tx,
                    &write_id,
                    &changeset,
                    WritePolicy::Serial,
                )?;
                writes.push(PreparedStoreWrite {
                    write_id: WriteId::from_generated(write_id),
                    changeset,
                    partitions,
                    inverse_changeset,
                    base: parsed_base,
                    blob_facts: serde_json::from_str(&blob_facts).map_err(|error| {
                        DbError::Message(format!("pending Serial write blob facts: {error}"))
                    })?,
                });
            }
            if writes.is_empty() {
                return Err(DbError::Message("Serial branch reservation is empty".to_string()));
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in newly_reserved {
                Self::notify_write_status_in(&statuses, &write_id, WriteStatus::Publishing);
            }
            Ok(Some(SerialStoreBranchPreparationWork {
                branch_id,
                base,
                writes,
            }))
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn enqueue_store_changeset_for_test(
        &self,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let write_id = self.new_write_id();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let base = match write_policy {
                WritePolicy::MergeConcurrent => StoreWriteBase::MergeConcurrent {
                    dependencies: Self::materialized_frontier_on(&tx, Some(&local_device_id))?,
                },
                WritePolicy::Serial => StoreWriteBase::Serial {
                    branch_id: PendingBranchId::from_first_write(write_id.clone()),
                    base: Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)?,
                },
            };
            let inverse_changeset = Self::invert_changeset(&changeset)?;
            let partitions = vec![gate::AudiencePartition {
                audience: crate::sync::circle::Audience::Store,
                control: None,
                changeset,
            }];
            let blob_facts =
                Self::capture_partition_blob_facts_on(&tx, &partitions, &gates, &blob_decls)?;
            Self::insert_store_write_on(
                &tx,
                &write_id,
                &partitions,
                &inverse_changeset,
                &base,
                &blob_facts,
                1,
            )?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn prepare_store_write_commit(
        &self,
        stage: StoreWritePreparation,
    ) -> Result<(), DbError> {
        let write_id = stage.write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if stage.commit.device_id != local_device_id || stage.head.device_id != local_device_id
            {
                return Err(DbError::Message(format!(
                    "prepared Store write belongs to {:?}/{:?}, local device is {:?}",
                    stage.commit.device_id, stage.head.device_id, local_device_id
                )));
            }
            let store_root_hash = Self::required_store_root_hash_on(&tx)?;
            stage
                .commit
                .verify_at(
                    store_root_hash,
                    WritePolicy::MergeConcurrent,
                    &local_device_id,
                    stage.commit.seq(),
                )
                .map_err(|error| {
                    DbError::Message(format!("verify prepared Store commit: {error}"))
                })?;
            stage
                .commit
                .verify_store_package(&stage.package_bytes)
                .map_err(|error| {
                    DbError::Message(format!("verify prepared Store package: {error}"))
                })?;
            if stage.commit.write_id != stage.write_id {
                return Err(DbError::Message(
                    "prepared write id differs from signed commit".to_string(),
                ));
            }
            let (stored_changeset, stored_base, stored_status, stored_preparation): (
                Vec<u8>,
                String,
                String,
                Option<String>,
            ) = tx
                .query_row(
                    "SELECT changeset, base, status, prepared
                     FROM store_writes WHERE write_id = ?1",
                    [stage.write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(DbError::from)?;
            if stored_status != "\"pending\"" || stored_preparation.is_some() {
                return Err(DbError::Message(format!(
                    "write {} is not an unprepared pending write",
                    stage.write_id
                )));
            }
            if stored_changeset != stage.package_bytes {
                return Err(DbError::Message(format!(
                    "prepared package differs from write {} changeset",
                    stage.write_id
                )));
            }
            let stored_base: StoreWriteBase =
                serde_json::from_str(&stored_base).map_err(|error| {
                    DbError::Message(format!("write {} base: {error}", stage.write_id))
                })?;
            let StoreWriteBase::MergeConcurrent {
                dependencies: stored_dependencies,
            } = stored_base
            else {
                return Err(DbError::Message(format!(
                    "serial write {} reached MergeConcurrent preparation",
                    stage.write_id
                )));
            };
            if stored_dependencies
                != *stage.commit.merge_dependencies().map_err(|error| {
                    DbError::Message(format!("prepared Store commit policy: {error}"))
                })?
            {
                return Err(DbError::Message(format!(
                    "prepared commit dependencies differ from write {}",
                    stage.write_id
                )));
            }
            let another_prepared: Option<String> = tx
                .query_row(
                    "SELECT write_id FROM store_writes
                     WHERE prepared IS NOT NULL AND write_id != ?1
                     ORDER BY ordinal LIMIT 1",
                    [stage.write_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(other_write_id) = another_prepared {
                return Err(DbError::Message(format!(
                    "write {other_write_id} already owns Store publication"
                )));
            }
            let expected_position = stage.commit.position();
            if stage.head.position.as_ref() != Some(&expected_position) {
                return Err(DbError::Message(
                    "outbound Store head does not activate its commit".to_string(),
                ));
            }
            let head_bytes = stage.head.to_bytes();
            let parsed_head = StoreDeviceHead::parse_at(
                &head_bytes,
                store_root_hash,
                &local_device_id,
                stage.commit.seq(),
            )
            .map_err(|error| DbError::Message(format!("verify prepared Store head: {error}")))?;
            if parsed_head != stage.head {
                return Err(DbError::Message(
                    "prepared Store head changed during encoding".to_string(),
                ));
            }

            let durable_predecessor = Self::latest_position_for_device_on(&tx, &local_device_id)?;
            let expected_seq = durable_predecessor
                .as_ref()
                .map_or(1, |position| position.seq.saturating_add(1));
            let expected_hash = durable_predecessor.map(|position| position.commit_hash);
            if stage.commit.seq() != expected_seq
                || stage.commit.previous_commit_hash() != expected_hash
            {
                return Err(DbError::Message(format!(
                    "outbound Store commit is {}/{:?}, expected {expected_seq}/{expected_hash:?}",
                    stage.commit.seq(),
                    stage.commit.previous_commit_hash()
                )));
            }

            let prepared = PreparedStoreWriteState::MergeConcurrent {
                commit_bytes: stage.commit.to_bytes(),
                head_bytes,
                blob_manifest: stage.blob_manifest,
                local_cleanup: stage.local_cleanup,
                completion: stage.completion,
            };
            let prepared = serde_json::to_string(&prepared).map_err(|error| {
                DbError::Message(format!("serialize prepared Store write: {error}"))
            })?;
            let status = serde_json::to_string(&WriteStatus::Publishing)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
            let updated = tx
                .execute(
                    "UPDATE store_writes SET prepared = ?2, status = ?3
                     WHERE write_id = ?1 AND prepared IS NULL AND status = '\"pending\"'",
                    rusqlite::params![stage.write_id.as_str(), prepared, status],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(format!(
                    "write {} lost pending preparation ownership",
                    stage.write_id
                )));
            }
            tx.commit().map_err(DbError::from)?;
            Self::notify_write_status_in(&statuses, &write_id, WriteStatus::Publishing);
            Ok(())
        })
        .await
    }

    pub(crate) async fn prepare_serial_store_branch_commit(
        &self,
        stage: SerialStoreWritePreparation,
    ) -> Result<(), DbError> {
        if self.write_policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial branch preparation requires the Serial write policy".to_string(),
            ));
        }
        if stage.writes.is_empty() {
            return Err(DbError::Message(
                "prepared Serial branch is empty".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let store_root_hash = Self::required_store_root_hash_on(&tx)?;
            let expected_base = StoreWriteBase::Serial {
                branch_id: stage.branch_id.clone(),
                base: stage.base.clone(),
            };
            let head_bytes = stage.head.to_bytes();
            let parsed_head =
                StoreSerialHead::parse(&head_bytes, store_root_hash).map_err(|error| {
                    DbError::Message(format!("verify prepared Serial head: {error}"))
                })?;
            if parsed_head != stage.head {
                return Err(DbError::Message(
                    "prepared Serial head changed during encoding".to_string(),
                ));
            }
            let tip = stage.writes.last().expect("nonempty checked above");
            if stage.head.commit.as_ref() != Some(&tip.commit.position())
                || stage.head.tip_write_id.as_ref() != Some(&tip.write_id)
            {
                return Err(DbError::Message(
                    "prepared Serial head does not activate the branch tip".to_string(),
                ));
            }
            let mut predecessor = stage.base.clone();
            for (index, write) in stage.writes.iter().enumerate() {
                if write.commit.write_id != write.write_id
                    || write.commit.device_id != local_device_id
                {
                    return Err(DbError::Message(format!(
                        "prepared Serial write {} identity differs from its commit",
                        write.write_id
                    )));
                }
                let expected_seq = predecessor
                    .as_ref()
                    .map_or(1, |position| position.seq.saturating_add(1));
                let expected_hash = predecessor.as_ref().map(|position| position.commit_hash);
                write
                    .commit
                    .verify_at(
                        store_root_hash,
                        WritePolicy::Serial,
                        SERIAL_STREAM_ID,
                        expected_seq,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Serial commit: {error}"))
                    })?;
                write
                    .commit
                    .verify_store_package(&write.package_bytes)
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Serial package: {error}"))
                    })?;
                if write.commit.previous_commit_hash() != expected_hash {
                    return Err(DbError::Message(format!(
                        "prepared Serial commit {} has the wrong predecessor",
                        write.write_id
                    )));
                }
                let (stored_changeset, stored_base, status, prepared): (
                    Vec<u8>,
                    String,
                    String,
                    String,
                ) = tx
                    .query_row(
                        "SELECT changeset, base, status, prepared FROM store_writes
                         WHERE write_id = ?1",
                        [write.write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .map_err(DbError::from)?;
                let stored_base: StoreWriteBase = serde_json::from_str(&stored_base)
                    .map_err(|error| DbError::Message(format!("stored Serial base: {error}")))?;
                let stored_prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                    .map_err(|error| {
                        DbError::Message(format!("stored Serial reservation: {error}"))
                    })?;
                if stored_changeset != write.package_bytes
                    || stored_base != expected_base
                    || status != "\"publishing\""
                    || !matches!(stored_prepared, PreparedStoreWriteState::SerialPreparing)
                {
                    return Err(DbError::Message(format!(
                        "Serial write {} no longer owns its exact branch reservation",
                        write.write_id
                    )));
                }
                let tip_head_bytes = (index + 1 == stage.writes.len()).then(|| head_bytes.clone());
                let durable = PreparedStoreWriteState::Serial {
                    commit_bytes: write.commit.to_bytes(),
                    tip_head_bytes,
                    blob_manifest: write.blob_manifest.clone(),
                    local_cleanup: write.local_cleanup.clone(),
                    completion: write.completion.clone(),
                };
                let durable = serde_json::to_string(&durable).map_err(|error| {
                    DbError::Message(format!("serialize prepared Serial write: {error}"))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND status = '\"publishing\"'",
                        rusqlite::params![write.write_id.as_str(), durable],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "Serial write {} lost exact preparation ownership",
                        write.write_id
                    )));
                }
                predecessor = Some(write.commit.position());
            }
            let reserved_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM store_writes
                     WHERE status = '\"publishing\"' AND json_type(base, '$.serial') IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if reserved_count
                != i64::try_from(stage.writes.len()).map_err(|_| {
                    DbError::Message("Serial branch length exceeds SQLite integer".into())
                })?
            {
                return Err(DbError::Message(format!(
                    "prepared Serial branch contains {} writes but {reserved_count} are reserved",
                    stage.writes.len()
                )));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    fn write_ids_matching_serial_base<I>(
        rows: I,
        expected_base: &StoreWriteBase,
    ) -> Result<Vec<WriteId>, DbError>
    where
        I: IntoIterator<Item = rusqlite::Result<(String, String)>>,
    {
        let mut write_ids = Vec::new();
        for row in rows {
            let (write_id, raw_base) = row.map_err(DbError::from)?;
            let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("stored Serial base: {error}")))?;
            if &stored_base == expected_base {
                write_ids.push(WriteId::from_generated(write_id));
            }
        }
        Ok(write_ids)
    }

    fn rebase_unprepared_serial_branch_on(
        tx: &rusqlite::Transaction<'_>,
        predecessor: Option<CommitPosition>,
        activated: CommitPosition,
    ) -> Result<(), DbError> {
        let mut statement = tx
            .prepare(
                "SELECT write_id, status, base, prepared FROM store_writes
                 WHERE status != '\"local_only\"'
                   AND json_extract(status, '$.published') IS NULL
                   AND json_extract(status, '$.resolved') IS NULL
                   AND json_type(base, '$.serial') IS NOT NULL
                 ORDER BY ordinal",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut branch_base = None;
        let mut branch_len = 0_usize;
        for row in rows {
            let (write_id, raw_status, raw_base, prepared) = row.map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("Serial branch status: {error}")))?;
            let base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("Serial branch base: {error}")))?;
            if branch_base.as_ref().is_some_and(|stored| stored != &base) {
                return Err(DbError::Message(
                    "Serial database contains more than one unresolved branch".to_string(),
                ));
            }
            branch_base.get_or_insert(base);
            if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_))
                || prepared.is_some()
            {
                return Err(DbError::Message(format!(
                    "Serial branch write {write_id} cannot be rebased during registration activation"
                )));
            }
            branch_len = branch_len.checked_add(1).ok_or_else(|| {
                DbError::Message("Serial branch length exceeds usize".to_string())
            })?;
        }
        drop(statement);
        let Some(StoreWriteBase::Serial { branch_id, base }) = branch_base else {
            return Ok(());
        };
        if base != predecessor {
            return Ok(());
        }
        let old_base = StoreWriteBase::Serial {
            branch_id: branch_id.clone(),
            base,
        };
        let rebased = StoreWriteBase::Serial {
            branch_id,
            base: Some(activated),
        };
        let updated = tx
            .execute(
                "UPDATE store_writes SET base = ?2
                 WHERE base = ?1 AND prepared IS NULL
                   AND (status = '\"pending\"' OR json_extract(status, '$.blocked') IS NOT NULL)",
                rusqlite::params![
                    serde_json::to_string(&old_base).map_err(|error| DbError::Message(format!(
                        "serialize prior Serial branch base: {error}"
                    )))?,
                    serde_json::to_string(&rebased).map_err(|error| DbError::Message(format!(
                        "serialize rebased Serial branch: {error}"
                    )))?,
                ],
            )
            .map_err(DbError::from)?;
        if updated != branch_len {
            return Err(DbError::Message(
                "Serial branch changed during registration activation".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn release_serial_store_branch_reservation(
        &self,
        branch_id: PendingBranchId,
        base: Option<CommitPosition>,
        status: WriteStatus,
    ) -> Result<(), DbError> {
        if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "Serial preparation can only return a reservation to pending or blocked"
                    .to_string(),
            ));
        }
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let expected_base = StoreWriteBase::Serial { branch_id, base };
            let preparing = serde_json::to_string(&PreparedStoreWriteState::SerialPreparing)
                .map_err(|error| {
                    DbError::Message(format!("serialize Serial reservation: {error}"))
                })?;
            let status_json = serde_json::to_string(&status)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, base FROM store_writes
                     WHERE status = '\"publishing\"' AND prepared = ?1
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([&preparing], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let write_ids = Self::write_ids_matching_serial_base(rows, &expected_base)?;
            drop(statement);
            if write_ids.is_empty() {
                return Err(DbError::Message(
                    "reserved Serial branch disappeared during preparation".to_string(),
                ));
            }
            for write_id in &write_ids {
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET status = ?2, prepared = NULL
                         WHERE write_id = ?1 AND status = '\"publishing\"' AND prepared = ?3",
                        rusqlite::params![write_id.as_str(), &status_json, &preparing],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "reserved Serial write {write_id} disappeared during release"
                    )));
                }
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn oldest_prepared_store_write(
        &self,
    ) -> Result<Option<PreparedStoreWriteCommit>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT write_id, changeset, base, prepared FROM store_writes
                 WHERE prepared IS NOT NULL
                   AND status IN ('\"pending\"', '\"publishing\"')
                 ORDER BY ordinal LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(write_id, package_bytes, base, prepared)| {
                let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                    .map_err(|error| DbError::Message(format!("prepared Store write: {error}")))?;
                let PreparedStoreWriteState::MergeConcurrent {
                    commit_bytes,
                    head_bytes,
                    blob_manifest,
                    ..
                } = prepared
                else {
                    return Err(DbError::Message(
                        "serial branch reached MergeConcurrent publication".to_string(),
                    ));
                };
                let commit: StoreBatchCommit = serde_json::from_slice(&commit_bytes)
                    .map_err(|error| DbError::Message(format!("prepared Store commit: {error}")))?;
                let head: StoreDeviceHead = serde_json::from_slice(&head_bytes)
                    .map_err(|error| DbError::Message(format!("prepared Store head: {error}")))?;
                let write_id = WriteId::from_generated(write_id);
                if commit.write_id != write_id {
                    return Err(DbError::Message(
                        "prepared write id differs from signed commit".to_string(),
                    ));
                }
                let base: StoreWriteBase = serde_json::from_str(&base)
                    .map_err(|error| DbError::Message(format!("prepared write base: {error}")))?;
                let StoreWriteBase::MergeConcurrent { dependencies } = base else {
                    return Err(DbError::Message(
                        "serial base reached MergeConcurrent publication".to_string(),
                    ));
                };
                if *commit.merge_dependencies().map_err(|error| {
                    DbError::Message(format!("prepared Store commit policy: {error}"))
                })? != dependencies
                {
                    return Err(DbError::Message(
                        "prepared commit differs from its write dependency frontier".to_string(),
                    ));
                }
                Ok(PreparedStoreWriteCommit {
                    package_bytes,
                    commit: ExactProtocolObject {
                        value: commit,
                        bytes: commit_bytes,
                    },
                    head: ExactProtocolObject {
                        value: head,
                        bytes: head_bytes,
                    },
                    blob_manifest,
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn prepared_serial_store_branch(
        &self,
    ) -> Result<Option<PreparedSerialStoreBranch>, DbError> {
        self.call(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, changeset, base, prepared FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut branch_id = None;
            let mut base = None;
            let mut writes = Vec::new();
            let mut head = None;
            for row in rows {
                let (write_id, package_bytes, raw_base, prepared) = row.map_err(DbError::from)?;
                let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                    .map_err(|error| DbError::Message(format!("prepared Serial write: {error}")))?;
                if matches!(prepared, PreparedStoreWriteState::SerialPreparing) {
                    if writes.is_empty() {
                        return Ok(None);
                    }
                    return Err(DbError::Message(
                        "Serial branch mixes reserved and exact prepared writes".to_string(),
                    ));
                }
                let PreparedStoreWriteState::Serial {
                    commit_bytes,
                    tip_head_bytes,
                    blob_manifest,
                    ..
                } = prepared
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent write reached Serial publication".to_string(),
                    ));
                };
                let StoreWriteBase::Serial {
                    branch_id: row_branch_id,
                    base: row_base,
                } = serde_json::from_str(&raw_base)
                    .map_err(|error| DbError::Message(format!("prepared Serial base: {error}")))?
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base reached Serial publication".to_string(),
                    ));
                };
                if branch_id
                    .as_ref()
                    .is_some_and(|value| value != &row_branch_id)
                    || base.as_ref().is_some_and(|value| value != &row_base)
                {
                    return Err(DbError::Message(
                        "prepared Serial writes do not share one branch base".to_string(),
                    ));
                }
                branch_id.get_or_insert(row_branch_id);
                base.get_or_insert(row_base);
                let commit: StoreBatchCommit =
                    serde_json::from_slice(&commit_bytes).map_err(|error| {
                        DbError::Message(format!("prepared Serial commit: {error}"))
                    })?;
                if commit.write_id.as_str() != write_id {
                    return Err(DbError::Message(
                        "prepared Serial write id differs from signed commit".to_string(),
                    ));
                }
                if let Some(head_bytes) = tip_head_bytes {
                    if head.is_some() {
                        return Err(DbError::Message(
                            "prepared Serial branch has more than one tip head".to_string(),
                        ));
                    }
                    let value: StoreSerialHead =
                        serde_json::from_slice(&head_bytes).map_err(|error| {
                            DbError::Message(format!("prepared Serial head: {error}"))
                        })?;
                    head = Some(ExactProtocolObject {
                        value,
                        bytes: head_bytes,
                    });
                }
                writes.push(PreparedSerialStoreWriteCommit {
                    package_bytes,
                    commit: ExactProtocolObject {
                        value: commit,
                        bytes: commit_bytes,
                    },
                    blob_manifest,
                });
            }
            if writes.is_empty() {
                return Ok(None);
            }
            let head = head.ok_or_else(|| {
                DbError::Message("prepared Serial branch has no activating tip head".to_string())
            })?;
            if writes.last().map(|write| write.commit.value.position()) != head.value.commit.clone()
            {
                return Err(DbError::Message(
                    "prepared Serial head does not activate the final commit".to_string(),
                ));
            }
            Ok(Some(PreparedSerialStoreBranch {
                branch_id: branch_id.expect("nonempty branch"),
                base: base.expect("nonempty branch"),
                writes,
                head,
            }))
        })
        .await
    }

    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<CommitPosition>, DbError> {
        self.call(|conn| {
            let device_id: String = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            Self::latest_position_for_device_on(conn, &device_id)
        })
        .await
    }

    #[doc(hidden)]
    pub async fn latest_outbound_store_position(&self) -> Result<Option<CommitPosition>, DbError> {
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let stream_id = match write_policy {
                WritePolicy::MergeConcurrent => conn
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = ?1",
                        [LOCAL_DEVICE_ID_STATE_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(DbError::from)?,
                WritePolicy::Serial => SERIAL_STREAM_ID.to_string(),
            };
            Self::latest_position_for_device_on(conn, &stream_id)
        })
        .await
    }

    pub(crate) async fn complete_prepared_store_write(
        &self,
        position: CommitPosition,
    ) -> Result<(), DbError> {
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let prepared_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM store_writes WHERE prepared IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if prepared_count != 1 {
                return Err(DbError::Message(format!(
                    "Store publication expected one prepared write, found {prepared_count}"
                )));
            }
            let (stored_write_id, prepared): (String, String) = tx
                .query_row(
                    "SELECT write_id, prepared FROM store_writes
                     WHERE prepared IS NOT NULL ORDER BY ordinal LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                .map_err(|error| DbError::Message(format!("prepared Store write: {error}")))?;
            let PreparedStoreWriteState::MergeConcurrent {
                commit_bytes,
                local_cleanup,
                completion,
                ..
            } = prepared
            else {
                return Err(DbError::Message(
                    "serial branch reached MergeConcurrent completion".to_string(),
                ));
            };
            let store_root_hash = Self::required_store_root_hash_on(&tx)?;
            let commit = StoreBatchCommit::parse_at(
                &commit_bytes,
                store_root_hash,
                WritePolicy::MergeConcurrent,
                &local_device_id,
                position.seq,
            )
            .map_err(|error| DbError::Message(format!("outbound commit: {error}")))?;
            if commit.position() != position {
                return Err(DbError::Message(format!(
                    "prepared Store write is {:?}, completion named {:?}",
                    commit.position(),
                    position
                )));
            }
            if commit.write_id.as_str() != stored_write_id {
                return Err(DbError::Message(
                    "prepared write id differs from signed commit".to_string(),
                ));
            }
            Self::record_materialized_commit_on(&tx, &commit)?;
            for drop in local_cleanup.drops {
                tx.execute(
                    "INSERT INTO published_blob_drop_intents
                     (seq, namespace, blob_id, size, disposition)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(seq, namespace, blob_id) DO NOTHING",
                    rusqlite::params![
                        Self::sequence_to_sqlite(&local_device_id, position.seq)?,
                        drop.namespace,
                        drop.id,
                        i64::try_from(drop.size).map_err(|_| DbError::Message(
                            "outbound local cleanup size exceeds SQLite integer".to_string()
                        ))?,
                        drop.disposition.as_db(),
                    ],
                )
                .map_err(DbError::from)?;
            }
            for intent in completion.consumed_make_remote_intents {
                Self::delete_make_remote_intent_on(&tx, &intent.root_table, &intent.root_id)?;
            }
            tx.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [stored_write_id.as_str()],
            )
            .map_err(DbError::from)?;
            let cleared = tx
                .execute(
                    "UPDATE store_writes SET prepared = NULL
                     WHERE write_id = ?1 AND prepared IS NOT NULL",
                    [stored_write_id.as_str()],
                )
                .map_err(DbError::from)?;
            if cleared != 1 {
                return Err(DbError::Message(
                    "prepared Store write disappeared".to_string(),
                ));
            }
            let write_id = commit.write_id;
            let status = WriteStatus::Published(PublishedPosition::MergeConcurrent {
                device_id: local_device_id,
                position,
            });
            Self::set_write_status_on(&tx, &write_id, &status)?;
            tx.commit().map_err(DbError::from)?;
            Self::notify_write_status_in(&statuses, &write_id, status);
            Ok(())
        })
        .await
    }

    pub(crate) async fn mark_serial_branch_conflict(
        &self,
        branch_id: PendingBranchId,
        base: Option<CommitPosition>,
        current: Option<CommitPosition>,
    ) -> Result<(), DbError> {
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let expected_base = StoreWriteBase::Serial {
                branch_id: branch_id.clone(),
                base: base.clone(),
            };
            let conflict = crate::SerializationConflict {
                branch_id: branch_id.clone(),
                base,
                current,
            };
            let status = WriteStatus::Conflict(conflict);
            let status_json = serde_json::to_string(&status)
                .map_err(|error| DbError::Message(format!("serialize Serial conflict: {error}")))?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, base FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let write_ids = Self::write_ids_matching_serial_base(rows, &expected_base)?;
            drop(statement);
            if write_ids.is_empty() {
                return Err(DbError::Message(format!(
                    "Serial branch {:?} has no pending writes",
                    branch_id.first_write_id()
                )));
            }
            for write_id in &write_ids {
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET status = ?2, prepared = NULL WHERE write_id = ?1",
                        rusqlite::params![write_id.as_str(), &status_json],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "Serial conflict write {write_id} disappeared"
                    )));
                }
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn complete_prepared_serial_branch(
        &self,
        activated: CommitPosition,
        tip_write_id: WriteId,
    ) -> Result<u64, DbError> {
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let store_root_hash = Self::required_store_root_hash_on(&tx)?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, prepared, base FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut completed = Vec::new();
            let mut completed_base = None;
            for row in rows {
                let (stored_write_id, prepared, raw_base) = row.map_err(DbError::from)?;
                let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                    .map_err(|error| DbError::Message(format!("prepared Serial base: {error}")))?;
                match &completed_base {
                    Some(expected) if expected != &stored_base => {
                        return Err(DbError::Message(
                            "prepared Serial branch contains inconsistent bases".to_string(),
                        ));
                    }
                    None => completed_base = Some(stored_base),
                    Some(_) => {}
                }
                let PreparedStoreWriteState::Serial {
                    commit_bytes,
                    local_cleanup,
                    completion,
                    ..
                } = serde_json::from_str(&prepared)
                    .map_err(|error| DbError::Message(format!("prepared Serial write: {error}")))?
                else {
                    return Err(DbError::Message(
                        "non-Serial write reached Serial completion".to_string(),
                    ));
                };
                let commit: StoreBatchCommit =
                    serde_json::from_slice(&commit_bytes).map_err(|error| {
                        DbError::Message(format!("prepared Serial commit: {error}"))
                    })?;
                commit
                    .verify_at(
                        store_root_hash,
                        WritePolicy::Serial,
                        SERIAL_STREAM_ID,
                        commit.seq(),
                    )
                    .map_err(|error| {
                        DbError::Message(format!("outbound Serial commit: {error}"))
                    })?;
                if commit.write_id.as_str() != stored_write_id {
                    return Err(DbError::Message(
                        "prepared Serial write id differs from signed commit".to_string(),
                    ));
                }
                Self::record_materialized_commit_on(&tx, &commit)?;
                for drop in local_cleanup.drops {
                    tx.execute(
                        "INSERT INTO published_blob_drop_intents
                         (seq, namespace, blob_id, size, disposition)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(seq, namespace, blob_id) DO NOTHING",
                        rusqlite::params![
                            Self::sequence_to_sqlite(SERIAL_STREAM_ID, commit.seq())?,
                            drop.namespace,
                            drop.id,
                            i64::try_from(drop.size).map_err(|_| DbError::Message(
                                "outbound local cleanup size exceeds SQLite integer".to_string()
                            ))?,
                            drop.disposition.as_db(),
                        ],
                    )
                    .map_err(DbError::from)?;
                }
                for intent in completion.consumed_make_remote_intents {
                    Self::delete_make_remote_intent_on(&tx, &intent.root_table, &intent.root_id)?;
                }
                let write_id = commit.write_id.clone();
                tx.execute(
                    "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                    [write_id.as_str()],
                )
                .map_err(DbError::from)?;
                tx.execute(
                    "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                    [write_id.as_str()],
                )
                .map_err(DbError::from)?;
                let status = WriteStatus::Published(PublishedPosition::Serial {
                    position: commit.position(),
                });
                Self::set_write_status_on(&tx, &write_id, &status)?;
                completed.push((write_id, status, commit.position()));
            }
            drop(statement);
            let Some((final_write_id, _, final_position)) = completed.last() else {
                return Err(DbError::Message(
                    "prepared Serial branch is absent".to_string(),
                ));
            };
            if final_write_id != &tip_write_id || final_position != &activated {
                return Err(DbError::Message(
                    "activated Serial head differs from the prepared branch tip".to_string(),
                ));
            }
            let completed_base = completed_base.ok_or_else(|| {
                DbError::Message("prepared Serial branch base is absent".to_string())
            })?;
            let suffix_first: Option<String> = tx
                .query_row(
                    "SELECT write_id FROM store_writes
                     WHERE status = '\"pending\"' AND prepared IS NULL AND base = ?1
                     ORDER BY ordinal LIMIT 1",
                    [serde_json::to_string(&completed_base).map_err(|error| {
                        DbError::Message(format!("serialize Serial base: {error}"))
                    })?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(suffix_first) = suffix_first {
                let rebased = StoreWriteBase::Serial {
                    branch_id: PendingBranchId::from_first_write(WriteId::from_generated(
                        suffix_first,
                    )),
                    base: Some(activated.clone()),
                };
                tx.execute(
                    "UPDATE store_writes SET base = ?2
                     WHERE status = '\"pending\"' AND prepared IS NULL AND base = ?1",
                    rusqlite::params![
                        serde_json::to_string(&completed_base).map_err(
                            |error| DbError::Message(format!(
                                "serialize completed Serial base: {error}"
                            ))
                        )?,
                        serde_json::to_string(&rebased).map_err(|error| DbError::Message(
                            format!("serialize rebased Serial suffix: {error}")
                        ))?,
                    ],
                )
                .map_err(DbError::from)?;
            }
            let count = u64::try_from(completed.len())
                .map_err(|_| DbError::Message("Serial completion count exceeds u64".to_string()))?;
            tx.commit().map_err(DbError::from)?;
            for (write_id, status, _) in completed {
                Self::notify_write_status_in(&statuses, &write_id, status);
            }
            Ok(count)
        })
        .await
    }

    fn apply_serial_resolution_on(
        tx: &rusqlite::Transaction<'_>,
        synced_tables: &[SyncedTable],
        branch_id: &PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
    ) -> Result<Vec<WriteId>, DbError> {
        let schema = Arc::new(crate::sync::conflict::TableSchema::from_db(
            tx,
            synced_tables,
        )?);
        let mut statement = tx
            .prepare(
                "SELECT write_id, status, inverse_changeset, base, prepared FROM store_writes
                 WHERE status != '\"local_only\"'
                   AND json_extract(status, '$.published') IS NULL
                   AND json_extract(status, '$.resolved') IS NULL
                   AND json_type(base, '$.serial') IS NOT NULL
                 ORDER BY ordinal",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut branch = Vec::new();
        let mut branch_base = None;
        let mut saw_conflict = false;
        for row in rows {
            let (write_id, status, inverse, raw_base, prepared) = row.map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&status)
                .map_err(|error| DbError::Message(format!("Serial branch status: {error}")))?;
            let base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("Serial branch base: {error}")))?;
            let StoreWriteBase::Serial {
                branch_id: stored_branch_id,
                base,
            } = base
            else {
                return Err(DbError::Message(
                    "MergeConcurrent base reached Serial resolution".to_string(),
                ));
            };
            if &stored_branch_id != branch_id {
                return Err(DbError::Message(
                    "Serial database contains more than one unresolved branch".to_string(),
                ));
            }
            match status {
                WriteStatus::Conflict(conflict) => {
                    if conflict.branch_id != stored_branch_id || conflict.base != base {
                        return Err(DbError::Message(
                            "Serial conflict status differs from its durable branch base"
                                .to_string(),
                        ));
                    }
                    saw_conflict = true;
                }
                WriteStatus::Pending if prepared.is_none() => {}
                status => {
                    return Err(DbError::Message(format!(
                        "conflicted Serial branch write {write_id} has non-resolvable status {status:?}"
                    )))
                }
            }
            if prepared.is_some() {
                return Err(DbError::Message(
                    "conflicted Serial branch contains prepared publication state".to_string(),
                ));
            }
            if branch_base.as_ref().is_some_and(|stored| stored != &base) {
                return Err(DbError::Message(
                    "Serial conflict branch has inconsistent bases".to_string(),
                ));
            }
            branch_base.get_or_insert(base);
            branch.push((WriteId::from_generated(write_id), inverse));
        }
        drop(statement);
        if branch.is_empty() || !saw_conflict {
            return Err(DbError::Message(format!(
                "Serial branch {} is not conflicted",
                branch_id.first_write_id()
            )));
        }
        let branch_base = branch_base.expect("nonempty branch has a base value");
        let durable_base = Self::latest_position_for_device_on(tx, SERIAL_STREAM_ID)?;
        if durable_base != branch_base {
            return Err(DbError::Message(format!(
                "local Serial position {durable_base:?} differs from branch base {branch_base:?}"
            )));
        }
        for (_, inverse) in branch.iter().rev() {
            let inverse = crate::sync::apply::ValidatedChangeset::new(inverse, schema.clone())
                .map_err(|error| DbError::Message(format!("invalid Serial inverse: {error}")))?;
            crate::sync::apply::apply_changeset_strict_on(tx, inverse, &[])
                .map_err(|error| DbError::Message(format!("reverse Serial branch: {error}")))?;
        }
        let mut predecessor = branch_base;
        for resolution in plan.commits {
            let expected_seq = predecessor
                .as_ref()
                .map_or(1, |position| position.seq.saturating_add(1));
            let expected_hash = predecessor.as_ref().map(|position| position.commit_hash);
            if resolution.commit.seq() != expected_seq
                || resolution.commit.previous_commit_hash() != expected_hash
            {
                return Err(DbError::Message(format!(
                    "Serial resolution commit {} does not follow the branch base",
                    resolution.commit.seq()
                )));
            }
            if let Some(package) = resolution.package {
                let changeset = crate::sync::apply::ValidatedChangeset::new(
                    package,
                    schema.clone(),
                )
                .map_err(|error| {
                    DbError::Message(format!("invalid Serial resolution changeset: {error}"))
                })?;
                crate::sync::apply::apply_changeset_strict_on(tx, changeset, &resolution.uploads)
                    .map_err(|error| {
                    DbError::Message(format!(
                        "apply Serial resolution commit {}: {error}",
                        resolution.commit.seq()
                    ))
                })?;
                let blob_decls = BlobDecls::from_tables(tx, synced_tables)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                for intent in resolution.cleanup {
                    crate::blob::local_cleanup::record_if_unreferenced_on(
                        tx,
                        &blob_decls,
                        &intent,
                    )?;
                }
            }
            Self::record_activated_store_device_registrations_on(
                tx,
                &resolution.commit,
                &resolution.registrations,
            )?;
            Self::record_materialized_serial_commit_on(
                tx,
                &resolution.commit,
                &resolution.authorization_after.membership,
                resolution.authorization_after.key_generation,
            )?;
            predecessor = Some(resolution.commit.position());
        }
        if predecessor != plan.head.commit {
            return Err(DbError::Message(
                "Serial resolution commits do not reach the verified global head".to_string(),
            ));
        }
        Ok(branch.into_iter().map(|(write_id, _)| write_id).collect())
    }

    fn resolve_unpublished_writes_on(
        tx: &rusqlite::Transaction<'_>,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            tx.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            Self::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn discard_pending_serial_branch(
        &self,
        branch_id: PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
    ) -> Result<(), DbError> {
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let write_ids =
                Self::apply_serial_resolution_on(&tx, &synced_tables, &branch_id, plan)?;
            let resolution = WriteResolution::Discarded;
            Self::resolve_unpublished_writes_on(&tx, &write_ids, &resolution)?;
            tx.commit().map_err(DbError::from)?;
            let status = WriteStatus::Resolved(resolution);
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }

    #[doc(hidden)]
    pub async fn replace_pending_serial_branch<R, E, F>(
        &self,
        branch_id: PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
        replacement_write_id: WriteId,
        f: F,
    ) -> Result<WriteReceipt<R>, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E> + Send + 'static,
    {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let statuses = self.state.write_statuses.clone();
        let outcome = self
            .call(move |conn| {
                Ok((|| {
                    let tx = conn
                        .unchecked_transaction()
                        .map_err(DbError::from)
                        .map_err(E::from)?;
                    let old_write_ids =
                        Self::apply_serial_resolution_on(&tx, &synced_tables, &branch_id, plan)
                            .map_err(E::from)?;
                    let changes_before = tx.total_changes();
                    let (value, captured) =
                        Self::capture_host_changes_on(&tx, &synced_tables, || f(&tx))?;
                    let partitions = Self::partition_captured_write_on(
                        &tx,
                        &captured,
                        &gates,
                        WritePolicy::Serial,
                        StoreWriteRouting::SerialScoped,
                    )
                    .map_err(E::from)?;
                    let blob_facts = Self::capture_partition_blob_facts_on(
                        &tx,
                        &partitions,
                        &gates,
                        &blob_decls,
                    )
                    .map_err(E::from)?;
                    let rows_changed = tx.total_changes().saturating_sub(changes_before);
                    let inverse_changeset = Self::invert_changeset(&captured).map_err(E::from)?;
                    let base = StoreWriteBase::Serial {
                        branch_id: PendingBranchId::from_first_write(replacement_write_id.clone()),
                        base: Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)
                            .map_err(E::from)?,
                    };
                    let status = Self::insert_store_write_on(
                        &tx,
                        &replacement_write_id,
                        &partitions,
                        &inverse_changeset,
                        &base,
                        &blob_facts,
                        rows_changed,
                    )
                    .map_err(E::from)?;
                    let resolution = WriteResolution::Replaced {
                        replacement: replacement_write_id.clone(),
                    };
                    Self::resolve_unpublished_writes_on(&tx, &old_write_ids, &resolution)
                        .map_err(E::from)?;
                    tx.commit().map_err(DbError::from).map_err(E::from)?;
                    let old_status = WriteStatus::Resolved(resolution);
                    for write_id in old_write_ids {
                        Self::notify_write_status_in(&statuses, &write_id, old_status.clone());
                    }
                    Ok(WriteReceipt {
                        value,
                        write_id: replacement_write_id,
                        status,
                    })
                })())
            })
            .await
            .map_err(E::from)?;
        outcome
    }

    pub(crate) async fn latest_local_store_ack(
        &self,
    ) -> Result<Option<(u64, ObjectHash)>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT revision, ack_hash FROM (\
                   SELECT revision, ack_hash FROM outbound_store_acks \
                   UNION ALL \
                   SELECT revision, ack_hash FROM published_store_acks\
                 ) ORDER BY revision DESC LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(revision, hash)| {
                Ok((
                    Self::sequence_from_sqlite("Store acknowledgement", revision)?,
                    hash.parse().map_err(|error| {
                        DbError::Message(format!("Store acknowledgement hash: {error}"))
                    })?,
                ))
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_store_ack(&self, ack: StoreAck) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let store_root_hash = Self::required_store_root_hash_on(&tx)?;
            let device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            StoreAck::parse_at(&ack.to_bytes(), store_root_hash, &device_id, ack.revision)
                .map_err(|error| DbError::Message(format!("verify staged Store acknowledgement: {error}")))?;
            let previous = tx
                .query_row(
                    "SELECT revision, ack_hash FROM published_store_acks \
                     ORDER BY revision DESC LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            let expected_revision = previous.as_ref().map_or(1, |(revision, _)| revision + 1);
            let expected_previous = previous
                .map(|(_, hash)| {
                    hash.parse::<ObjectHash>().map_err(|error| {
                        DbError::Message(format!("published Store acknowledgement hash: {error}"))
                    })
                })
                .transpose()?;
            if i64::try_from(ack.revision).ok() != Some(expected_revision)
                || ack.previous_ack_hash != expected_previous
            {
                return Err(DbError::Message(format!(
                    "Store acknowledgement does not extend local chain at revision {expected_revision}"
                )));
            }
            let bytes = ack.to_bytes();
            tx.execute(
                "INSERT INTO outbound_store_acks \
                 (revision, ack_hash, previous_ack_hash, ack_bytes) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    Self::sequence_to_sqlite("Store acknowledgement", ack.revision)?,
                    ack.ack_hash().to_string(),
                    ack.previous_ack_hash.map(|hash| hash.to_string()),
                    bytes,
                ],
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn oldest_outbound_store_ack(
        &self,
    ) -> Result<Option<OutboundStoreAck>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT revision, ack_hash, previous_ack_hash, ack_bytes \
                 FROM outbound_store_acks ORDER BY revision LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(revision, ack_hash, previous_ack_hash, ack_bytes)| {
                Ok(OutboundStoreAck {
                    revision: Self::sequence_from_sqlite("Store acknowledgement", revision)?,
                    ack_hash: ack_hash.parse().map_err(|error| {
                        DbError::Message(format!("outbound Store acknowledgement hash: {error}"))
                    })?,
                    previous_ack_hash: previous_ack_hash
                        .map(|hash| {
                            hash.parse().map_err(|error| {
                                DbError::Message(format!(
                                    "outbound Store previous acknowledgement hash: {error}"
                                ))
                            })
                        })
                        .transpose()?,
                    ack_bytes,
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn complete_outbound_store_ack(
        &self,
        revision: u64,
        ack_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let deleted = tx
                .execute(
                    "DELETE FROM outbound_store_acks WHERE revision = ?1 AND ack_hash = ?2",
                    (
                        Self::sequence_to_sqlite("Store acknowledgement", revision)?,
                        ack_hash.to_string(),
                    ),
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "outbound Store acknowledgement disappeared".to_string(),
                ));
            }
            tx.execute(
                "INSERT INTO published_store_acks (revision, ack_hash) VALUES (?1, ?2)",
                (
                    Self::sequence_to_sqlite("Store acknowledgement", revision)?,
                    ack_hash.to_string(),
                ),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<DurableMembershipMutation>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT intent_hash, plan_bytes, progress_bytes \
                 FROM outbound_membership_mutation WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(hash, plan_bytes, progress_bytes)| {
                let intent_hash: ObjectHash = hash.parse().map_err(|error| {
                    DbError::Message(format!("membership intent hash: {error}"))
                })?;
                if ObjectHash::digest(&plan_bytes) != intent_hash {
                    return Err(DbError::Message(
                        "membership intent hash differs from its exact plan bytes".to_string(),
                    ));
                }
                Ok(DurableMembershipMutation {
                    intent_hash,
                    plan_bytes,
                    progress_bytes,
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_membership_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
    ) -> Result<ObjectHash, DbError> {
        self.call(move |conn| {
            let intent_hash = ObjectHash::digest(&plan_bytes);
            let existing = conn
                .query_row(
                    "SELECT intent_hash, plan_bytes FROM outbound_membership_mutation \
                     WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some((existing_hash, existing_plan)) = existing {
                if existing_hash == intent_hash.to_string() && existing_plan == plan_bytes {
                    return Ok(intent_hash);
                }
                return Err(DbError::Message(
                    "a different membership mutation is already pending".to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO outbound_membership_mutation \
                 (singleton, intent_hash, plan_bytes, progress_bytes) \
                 VALUES (1, ?1, ?2, ?3)",
                rusqlite::params![intent_hash.to_string(), plan_bytes, progress_bytes],
            )
            .map_err(DbError::from)?;
            Ok(intent_hash)
        })
        .await
    }

    pub(crate) async fn update_membership_mutation_progress(
        &self,
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let updated = conn
                .execute(
                    "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                     WHERE singleton = 1 AND intent_hash = ?2",
                    rusqlite::params![progress_bytes, intent_hash.to_string()],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "membership mutation ownership row is absent or changed".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn complete_membership_mutation(
        &self,
        intent_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let deleted = conn
                .execute(
                    "DELETE FROM outbound_membership_mutation \
                     WHERE singleton = 1 AND intent_hash = ?1",
                    [intent_hash.to_string()],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "membership mutation ownership row is absent or changed".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn outbound_snapshot_publication(
        &self,
    ) -> Result<Option<DurableSnapshotPublication>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT snapshot_hash, image_hash, image_bytes, meta_bytes \
                 FROM outbound_store_snapshot WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(snapshot_hash, image_hash, image_bytes, meta_bytes)| {
                let snapshot_hash = snapshot_hash.parse().map_err(|error| {
                    DbError::Message(format!("outbound snapshot hash: {error}"))
                })?;
                let image_hash: ObjectHash = image_hash.parse().map_err(|error| {
                    DbError::Message(format!("outbound snapshot image hash: {error}"))
                })?;
                if ObjectHash::digest(&image_bytes) != image_hash {
                    return Err(DbError::Message(
                        "outbound snapshot image hash differs from its exact bytes".to_string(),
                    ));
                }
                Ok(DurableSnapshotPublication {
                    snapshot_hash,
                    image_hash,
                    image_bytes,
                    meta_bytes,
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_snapshot_publication(
        &self,
        snapshot_hash: ObjectHash,
        image_hash: ObjectHash,
        image_bytes: Vec<u8>,
        meta_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            if ObjectHash::digest(&image_bytes) != image_hash {
                return Err(DbError::Message(
                    "staged snapshot image hash differs from its exact bytes".to_string(),
                ));
            }
            let existing = conn
                .query_row(
                    "SELECT snapshot_hash, image_hash, image_bytes, meta_bytes \
                     FROM outbound_store_snapshot WHERE singleton = 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some((stored_snapshot, stored_image, stored_image_bytes, stored_meta_bytes)) =
                existing
            {
                if stored_snapshot == snapshot_hash.to_string()
                    && stored_image == image_hash.to_string()
                    && stored_image_bytes == image_bytes
                    && stored_meta_bytes == meta_bytes
                {
                    return Ok(());
                }
                return Err(DbError::Message(
                    "a different snapshot publication is already pending".to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO outbound_store_snapshot \
                 (singleton, snapshot_hash, image_hash, image_bytes, meta_bytes) \
                 VALUES (1, ?1, ?2, ?3, ?4)",
                rusqlite::params![
                    snapshot_hash.to_string(),
                    image_hash.to_string(),
                    image_bytes,
                    meta_bytes,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn complete_snapshot_publication(
        &self,
        snapshot_hash: ObjectHash,
    ) -> Result<(), DbError> {
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (stored_image_hash, image_bytes, meta_bytes): (String, Vec<u8>, Vec<u8>) = tx
                .query_row(
                    "SELECT image_hash, image_bytes, meta_bytes \
                     FROM outbound_store_snapshot \
                     WHERE singleton = 1 AND snapshot_hash = ?1",
                    [snapshot_hash.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            let image_hash: ObjectHash = stored_image_hash.parse().map_err(|error| {
                DbError::Message(format!("outbound snapshot image hash: {error}"))
            })?;
            if ObjectHash::digest(&image_bytes) != image_hash {
                return Err(DbError::Message(
                    "outbound snapshot image hash differs from its exact bytes".to_string(),
                ));
            }
            let unverified: SnapshotMeta =
                serde_json::from_slice(&meta_bytes).map_err(|error| {
                    DbError::Message(format!("outbound snapshot metadata: {error}"))
                })?;
            let meta = SnapshotMeta::parse_at(
                &meta_bytes,
                unverified.store_root_hash,
                &unverified.author_pubkey,
                snapshot_hash,
            )
            .map_err(|error| {
                DbError::Message(format!("verify outbound snapshot metadata: {error}"))
            })?;
            if meta.image_hash != image_hash {
                return Err(DbError::Message(
                    "outbound snapshot metadata names different image bytes".to_string(),
                ));
            }
            if meta.coverage.policy() != write_policy {
                return Err(DbError::Message(format!(
                    "outbound snapshot coverage uses {:?}, database uses {write_policy:?}",
                    meta.coverage.policy()
                )));
            }
            let frontier = serde_json::to_string(&meta.coverage).map_err(|error| {
                DbError::Message(format!("serialize snapshot frontier: {error}"))
            })?;
            for (key, value) in [
                (LAST_SNAPSHOT_HASH_STATE_KEY, snapshot_hash.to_string()),
                ("last_snapshot_time", meta.created_at.clone()),
                (LAST_SNAPSHOT_FRONTIER_STATE_KEY, frontier),
            ] {
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )
                .map_err(DbError::from)?;
            }
            let snapshot_position = match &meta.coverage {
                CommitFrontier::MergeConcurrent(coverage) => {
                    let local_device_id: String = tx
                        .query_row(
                            "SELECT value FROM protocol_state WHERE key = ?1",
                            [LOCAL_DEVICE_ID_STATE_KEY],
                            |row| row.get(0),
                        )
                        .map_err(DbError::from)?;
                    coverage.get(&local_device_id)
                }
                CommitFrontier::Serial(position) => position.as_ref(),
            };
            match snapshot_position {
                Some(position) => {
                    let encoded = serde_json::to_string(position).map_err(|error| {
                        DbError::Message(format!("serialize snapshot position: {error}"))
                    })?;
                    tx.execute(
                        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        (LAST_SNAPSHOT_POSITION_STATE_KEY, encoded),
                    )
                    .map_err(DbError::from)?;
                }
                None => {
                    tx.execute(
                        "DELETE FROM protocol_state WHERE key = ?1",
                        [LAST_SNAPSHOT_POSITION_STATE_KEY],
                    )
                    .map_err(DbError::from)?;
                }
            }
            let deleted = tx
                .execute(
                    "DELETE FROM outbound_store_snapshot \
                     WHERE singleton = 1 AND snapshot_hash = ?1",
                    [snapshot_hash.to_string()],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "outbound snapshot ownership row is absent or changed".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn local_store_protocol_root(
        &self,
    ) -> Result<Option<DurableProtocolObject>, DbError> {
        self.call(|conn| {
            conn.query_row(
                "SELECT store_root_hash, store_protocol_root_bytes, published \
                 FROM local_store_protocol_root WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|(hash, bytes, published)| {
                Ok(DurableProtocolObject {
                    semantic_hash: hash.parse().map_err(|error| {
                        DbError::Message(format!("local store protocol root hash: {error}"))
                    })?,
                    bytes,
                    published: match published {
                        0 => false,
                        1 => true,
                        value => {
                            return Err(DbError::Message(format!(
                                "local store protocol root has invalid published value {value}"
                            )))
                        }
                    },
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_store_protocol_root(
        &self,
        store_protocol_root: StoreProtocolRoot,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let bytes = store_protocol_root.to_bytes();
            let parsed = StoreProtocolRoot::parse(&bytes)
                .map_err(|error| DbError::Message(format!("verify staged store protocol root: {error}")))?;
            if parsed != store_protocol_root {
                return Err(DbError::Message(
                    "staged store protocol root changed during encoding".to_string(),
                ));
            }
            let hash = store_protocol_root.object_hash();
            let existing = conn
                .query_row(
                    "SELECT store_root_hash, store_protocol_root_bytes FROM local_store_protocol_root \
                     WHERE singleton = 1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some((existing_hash, existing_bytes)) = existing {
                if existing_hash == hash.to_string() && existing_bytes == bytes {
                    return Ok(());
                }
                return Err(DbError::Message(
                    "local store protocol root already owns different bytes".to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO local_store_protocol_root \
                 (singleton, store_root_hash, store_protocol_root_bytes, published) VALUES (1, ?1, ?2, 0)",
                (hash.to_string(), bytes),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn complete_store_protocol_root(
        &self,
        store_root_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let updated = tx
                .execute(
                    "UPDATE local_store_protocol_root SET published = 1 \
                     WHERE singleton = 1 AND store_root_hash = ?1",
                    [store_root_hash.to_string()],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "local store protocol root ownership row is absent".to_string(),
                ));
            }
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (STORE_ROOT_HASH_STATE_KEY, store_root_hash.to_string()),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn latest_local_store_device_registration(
        &self,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.read_local_store_device_registration(
            "SELECT revision, registration_hash, previous_registration_hash, state, \
                    registration_bytes, activation_commit_bytes, activation_head_bytes, published \
             FROM local_store_device_registration ORDER BY revision DESC LIMIT 1",
        )
        .await
    }

    pub(crate) async fn oldest_unpublished_store_device_registration(
        &self,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.read_local_store_device_registration(
            "SELECT revision, registration_hash, previous_registration_hash, state, \
                    registration_bytes, activation_commit_bytes, activation_head_bytes, published \
             FROM local_store_device_registration WHERE published = 0 \
             ORDER BY revision LIMIT 1",
        )
        .await
    }

    async fn read_local_store_device_registration(
        &self,
        sql: &'static str,
    ) -> Result<Option<DurableDeviceRegistration>, DbError> {
        self.call(move |conn| {
            conn.query_row(sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })
            .optional()
            .map_err(DbError::from)?
            .map(
                |(
                    revision,
                    hash,
                    previous_hash,
                    state,
                    bytes,
                    activation_commit_bytes,
                    activation_head_bytes,
                    published,
                )| {
                    let revision = u64::try_from(revision).map_err(|_| {
                        DbError::Message(format!(
                            "local Store device registration has invalid revision {revision}"
                        ))
                    })?;
                    if revision == 0 {
                        return Err(DbError::Message(
                            "local Store device registration has revision zero".to_string(),
                        ));
                    }
                    Ok(DurableDeviceRegistration {
                        revision,
                        registration_hash: hash.parse().map_err(|error| {
                            DbError::Message(format!(
                                "local Store device registration hash: {error}"
                            ))
                        })?,
                        previous_registration_hash: previous_hash
                            .map(|hash| {
                                hash.parse().map_err(|error| {
                                    DbError::Message(format!(
                                        "local Store device previous registration hash: {error}"
                                    ))
                                })
                            })
                            .transpose()?,
                        state: match state.as_str() {
                            "active" => StoreDeviceRegistrationState::Active,
                            "retired" => StoreDeviceRegistrationState::Retired,
                            _ => {
                                return Err(DbError::Message(format!(
                                    "local Store device registration has invalid state {state:?}"
                                )))
                            }
                        },
                        registration_bytes: bytes,
                        activation_commit_bytes,
                        activation_head_bytes,
                        published: match published {
                            0 => false,
                            1 => true,
                            value => {
                                return Err(DbError::Message(format!(
                            "local Store device registration has invalid published value {value}"
                        )))
                            }
                        },
                    })
                },
            )
            .transpose()
        })
        .await
    }

    pub(crate) async fn stage_store_device_registration(
        &self,
        registration: StoreDeviceRegistration,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let store_root_hash = Self::required_store_root_hash_on(conn)?;
            let device_id: String = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let bytes = registration.to_bytes();
            let parsed = StoreDeviceRegistration::parse_at(
                &bytes,
                store_root_hash,
                &device_id,
                registration.revision,
            )
            .map_err(|error| {
                DbError::Message(format!("verify Store device registration: {error}"))
            })?;
            if parsed != registration {
                return Err(DbError::Message(
                    "Store device registration changed during verification".to_string(),
                ));
            }
            let hash = registration.registration_hash();
            let existing = conn
                .query_row(
                    "SELECT revision, registration_hash, state, registration_bytes, published \
                     FROM local_store_device_registration ORDER BY revision DESC LIMIT 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some((revision, existing_hash, state, existing_bytes, published)) = existing {
                let revision = u64::try_from(revision).map_err(|_| {
                    DbError::Message(
                        "local Store device registration revision is negative".to_string(),
                    )
                })?;
                if revision == registration.revision {
                    if existing_hash == hash.to_string() && existing_bytes == bytes {
                        return Ok(());
                    }
                    return Err(DbError::Message(format!(
                        "local Store device registration revision {revision} owns different bytes"
                    )));
                }
                let previous = StoreDeviceRegistration::parse_at(
                    &existing_bytes,
                    store_root_hash,
                    &device_id,
                    revision,
                )
                .map_err(|error| {
                    DbError::Message(format!(
                        "verify previous Store device registration: {error}"
                    ))
                })?;
                if previous.registration_hash().to_string() != existing_hash
                    || previous.author_pubkey != registration.author_pubkey
                {
                    return Err(DbError::Message(
                        "Store device registration successor must retain the exact author chain"
                            .to_string(),
                    ));
                }
                if published != 1 {
                    return Err(DbError::Message(format!(
                        "local Store device registration revision {revision} is not published"
                    )));
                }
                if registration.revision != revision + 1
                    || registration.previous_registration_hash
                        != Some(existing_hash.parse().map_err(|error| {
                            DbError::Message(format!(
                                "previous Store device registration hash: {error}"
                            ))
                        })?)
                    || state != "active"
                    || registration.state != StoreDeviceRegistrationState::Retired
                {
                    return Err(DbError::Message(
                        "Store device registration must transition from published Active to Retired"
                            .to_string(),
                    ));
                }
            } else if registration.revision != 1
                || registration.previous_registration_hash.is_some()
                || registration.state != StoreDeviceRegistrationState::Active
            {
                return Err(DbError::Message(
                    "first Store device registration must be revision 1 Active".to_string(),
                ));
            }
            let revision = i64::try_from(registration.revision).map_err(|_| {
                DbError::Message(
                    "Store device registration revision exceeds SQLite INTEGER".to_string(),
                )
            })?;
            let state = match registration.state {
                StoreDeviceRegistrationState::Active => "active",
                StoreDeviceRegistrationState::Retired => "retired",
            };
            conn.execute(
                "INSERT INTO local_store_device_registration \
                 (revision, registration_hash, previous_registration_hash, state, \
                  registration_bytes, activation_commit_bytes, activation_head_bytes, published) \
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, 0)",
                (
                    revision,
                    hash.to_string(),
                    registration
                        .previous_registration_hash
                        .map(|hash| hash.to_string()),
                    state,
                    bytes,
                ),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn stage_merge_store_device_registration_activation(
        &self,
        revision: u64,
        registration_hash: ObjectHash,
        commit: StoreBatchCommit,
        head: StoreDeviceHead,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            if commit.policy() != WritePolicy::MergeConcurrent
                || head.position.as_ref() != Some(&commit.position())
            {
                return Err(DbError::Message(
                    "Merge Store device registration activation has an invalid commit/head"
                        .to_string(),
                ));
            }
            Self::stage_store_device_registration_activation_on(
                conn,
                revision,
                registration_hash,
                &commit,
                &head.to_bytes(),
            )
        })
        .await
    }

    pub(crate) async fn stage_serial_store_device_registration_activation(
        &self,
        revision: u64,
        registration_hash: ObjectHash,
        commit: StoreBatchCommit,
        head: StoreSerialHead,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            if commit.policy() != WritePolicy::Serial
                || head.commit.as_ref() != Some(&commit.position())
            {
                return Err(DbError::Message(
                    "Serial Store device registration activation has an invalid commit/head"
                        .to_string(),
                ));
            }
            Self::stage_store_device_registration_activation_on(
                conn,
                revision,
                registration_hash,
                &commit,
                &head.to_bytes(),
            )
        })
        .await
    }

    fn stage_store_device_registration_activation_on(
        conn: &Connection,
        revision: u64,
        registration_hash: ObjectHash,
        commit: &StoreBatchCommit,
        head_bytes: &[u8],
    ) -> Result<(), DbError> {
        let revision = i64::try_from(revision).map_err(|_| {
            DbError::Message(
                "Store device registration revision exceeds SQLite INTEGER".to_string(),
            )
        })?;
        let stored = conn
            .query_row(
                "SELECT registration_bytes, activation_commit_bytes, activation_head_bytes, published \
                 FROM local_store_device_registration \
                 WHERE revision = ?1 AND registration_hash = ?2",
                (revision, registration_hash.to_string()),
                |row| {
                    Ok(StoredDeviceRegistrationActivation {
                        registration_bytes: row.get(0)?,
                        commit_bytes: row.get(1)?,
                        head_bytes: row.get(2)?,
                        published: row.get(3)?,
                    })
                },
            )
            .map_err(DbError::from)?;
        let registration = StoreDeviceRegistration::parse_at(
            &stored.registration_bytes,
            commit.store_root_hash,
            &commit.device_id,
            revision as u64,
        )
        .map_err(|error| {
            DbError::Message(format!("verify local Store device registration: {error}"))
        })?;
        let reference = StoreDeviceRegistrationRef::from_registration(&registration);
        if commit.device_registrations.as_slice() != [reference]
            || commit.author_pubkey != registration.author_pubkey
            || commit.control.is_some()
            || !commit.circle_controls.is_empty()
            || commit.store_package.is_some()
            || !commit.circle_packages.is_empty()
        {
            return Err(DbError::Message(
                "Store device registration activation is not an exact control-only batch"
                    .to_string(),
            ));
        }
        let commit_bytes = commit.to_bytes();
        match (stored.commit_bytes, stored.head_bytes, stored.published) {
            (None, None, 0) => {
                conn.execute(
                    "UPDATE local_store_device_registration \
                     SET activation_commit_bytes = ?3, activation_head_bytes = ?4 \
                     WHERE revision = ?1 AND registration_hash = ?2 AND published = 0",
                    rusqlite::params![
                        revision,
                        registration_hash.to_string(),
                        commit_bytes,
                        head_bytes,
                    ],
                )
                .map_err(DbError::from)?;
                Ok(())
            }
            (Some(existing_commit), Some(existing_head), 0)
                if existing_commit == commit_bytes && existing_head == head_bytes =>
            {
                Ok(())
            }
            (Some(existing_commit), Some(existing_head), 1)
                if existing_commit == commit_bytes && existing_head == head_bytes =>
            {
                Ok(())
            }
            _ => Err(DbError::Message(
                "Store device registration owns different activation bytes".to_string(),
            )),
        }
    }

    pub(crate) async fn complete_merge_store_device_registration_activation(
        &self,
        revision: u64,
        registration_hash: ObjectHash,
        commit: StoreBatchCommit,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let registration = Self::owned_registration_for_activation_on(
                &tx,
                revision,
                registration_hash,
                &commit,
            )?;
            Self::record_activated_store_device_registrations_on(&tx, &commit, &[registration])?;
            Self::record_materialized_commit_on(&tx, &commit)?;
            Self::mark_store_device_registration_published_on(&tx, revision, registration_hash)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn complete_serial_store_device_registration_activation(
        &self,
        revision: u64,
        registration_hash: ObjectHash,
        commit: StoreBatchCommit,
        authorization: SerialAuthorizationState,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let registration = Self::owned_registration_for_activation_on(
                &tx,
                revision,
                registration_hash,
                &commit,
            )?;
            Self::record_activated_store_device_registrations_on(&tx, &commit, &[registration])?;
            Self::record_materialized_serial_commit_on(
                &tx,
                &commit,
                &authorization.membership,
                authorization.key_generation,
            )?;
            let predecessor = commit
                .previous_commit_hash()
                .map(|commit_hash| CommitPosition {
                    seq: commit.seq() - 1,
                    commit_hash,
                });
            Self::rebase_unprepared_serial_branch_on(&tx, predecessor, commit.position())?;
            Self::mark_store_device_registration_published_on(&tx, revision, registration_hash)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    fn owned_registration_for_activation_on(
        tx: &rusqlite::Transaction<'_>,
        revision: u64,
        registration_hash: ObjectHash,
        commit: &StoreBatchCommit,
    ) -> Result<StoreDeviceRegistration, DbError> {
        let revision_sql = i64::try_from(revision).map_err(|_| {
            DbError::Message(
                "Store device registration revision exceeds SQLite INTEGER".to_string(),
            )
        })?;
        let (registration_bytes, activation_commit_bytes): (Vec<u8>, Vec<u8>) = tx
            .query_row(
                "SELECT registration_bytes, activation_commit_bytes \
                 FROM local_store_device_registration \
                 WHERE revision = ?1 AND registration_hash = ?2 AND published = 0",
                (revision_sql, registration_hash.to_string()),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if activation_commit_bytes != commit.to_bytes() {
            return Err(DbError::Message(
                "Store device registration activation commit differs from durable bytes"
                    .to_string(),
            ));
        }
        StoreDeviceRegistration::parse_at(
            &registration_bytes,
            commit.store_root_hash,
            &commit.device_id,
            revision,
        )
        .map_err(|error| {
            DbError::Message(format!("verify owned Store device registration: {error}"))
        })
    }

    fn mark_store_device_registration_published_on(
        tx: &rusqlite::Transaction<'_>,
        revision: u64,
        registration_hash: ObjectHash,
    ) -> Result<(), DbError> {
        let revision = i64::try_from(revision).map_err(|_| {
            DbError::Message(
                "Store device registration revision exceeds SQLite INTEGER".to_string(),
            )
        })?;
        let updated = tx
            .execute(
                "UPDATE local_store_device_registration SET published = 1 \
                 WHERE revision = ?1 AND registration_hash = ?2 AND published = 0 \
                   AND activation_commit_bytes IS NOT NULL AND activation_head_bytes IS NOT NULL",
                (revision, registration_hash.to_string()),
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "local Store device registration activation row is absent".to_string(),
            ));
        }
        Ok(())
    }

    // ---- Bookkeeping: protocol_state ----

    fn required_store_root_hash_on(conn: &Connection) -> Result<ObjectHash, DbError> {
        let raw = conn
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [STORE_ROOT_HASH_STATE_KEY],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .ok_or_else(DbError::missing_store_root_hash)?;
        raw.parse::<ObjectHash>()
            .map_err(|error| DbError::invalid_store_root_hash(error.to_string()))
    }

    pub(crate) async fn required_store_root_hash(&self) -> Result<ObjectHash, DbError> {
        self.call(Self::required_store_root_hash_on).await
    }

    pub(crate) async fn required_store_root_hash_mapped<E>(
        &self,
        missing: impl FnOnce() -> E,
        invalid: impl FnOnce(String) -> E,
        other: impl FnOnce(DbError) -> E,
    ) -> Result<ObjectHash, E> {
        self.required_store_root_hash()
            .await
            .map_err(|error| match error {
                DbError::StoreRootHashMissing => missing(),
                DbError::StoreRootHashInvalid { reason } => invalid(reason),
                error @ DbError::Message(_) => other(error),
            })
    }

    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [key],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn delete_protocol_state(&self, key: &str) -> Result<(), DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    /// `namespace`'s device-local cache-size budget in bytes, or `None` if the host
    /// has not set one for it. `None` means unlimited — eviction is off for that
    /// namespace and its cache grows without bound; the host opts a namespace into a
    /// budget by calling [`Self::set_cache_budget`]. Budgets are per namespace so a
    /// small namespace (`covers`) is never wiped by pressure from a big one
    /// (`release_files`): each evicts against its own budget. Stored as a single
    /// decimal value under [`crate::blob::cache::cache_budget_state_key`] in
    /// `protocol_state` (config, not per-blob accounting — the cache's truth is still the
    /// folder on disk).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        match self.get_protocol_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|e| {
                DbError::Message(format!(
                    "cache budget for {namespace:?} in protocol_state is not a byte count: {e}"
                ))
            }),
            None => Ok(None),
        }
    }

    /// Set `namespace`'s device-local cache-size budget in bytes. Once set, a populate
    /// into that namespace's cache that pushes `storage/cache/<namespace>/` over this
    /// total evicts its oldest files (by mtime) back under it; `pinned/` is never
    /// counted or touched, and another namespace's files are never walked. Stored
    /// under [`crate::blob::cache::cache_budget_state_key`] in `protocol_state`.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        self.set_protocol_state(&key, &max_bytes.to_string()).await
    }

    // ---- Bookkeeping: blob_uploaders (which device uploaded a blob) ----

    /// The hex public key of the device that uploaded blob `(namespace, id)`, or
    /// `None` if this device has never recorded one. The read dispatch consults it
    /// to key a blob under its uploader's prefix; a `None` is a missing dispatch
    /// key the read surfaces loud, never a cue to scan an untrusted listing.
    pub(crate) async fn blob_uploader(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<String>, DbError> {
        let (namespace, id) = (namespace.to_string(), id.to_string());
        self.call(move |conn| {
            conn.query_row(
                "SELECT uploader FROM blob_uploaders WHERE namespace = ?1 AND blob_id = ?2",
                (namespace, id),
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
    }

    /// Record blob `(namespace, id)`'s uploader on `conn`, composable inside a
    /// caller's transaction so the record commits atomically with the changeset
    /// apply it belongs to (never a later repair). Idempotent — re-recording the
    /// same uploader (a changeset re-applied after an FK-deferred retry) is a
    /// no-op; a later, authoritative uploader (a re-upload by a different member)
    /// overwrites.
    pub(crate) fn record_blob_uploader_on(
        conn: &Connection,
        namespace: &str,
        id: &str,
        uploader: &str,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT INTO blob_uploaders (namespace, blob_id, uploader) VALUES (?1, ?2, ?3) \
             ON CONFLICT(namespace, blob_id) DO UPDATE SET uploader = excluded.uploader",
            (namespace, id, uploader),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Record blob `(namespace, id)`'s uploader outside any caller transaction —
    /// used by the inline host-provided upload, which records this device as the
    /// uploader for a blob it just sealed to the cloud. Recording an authoritative
    /// fact (who uploaded), not repairing wrong state.
    pub(crate) async fn record_blob_uploader(
        &self,
        namespace: &str,
        id: &str,
        uploader: &str,
    ) -> Result<(), DbError> {
        let (namespace, id, uploader) =
            (namespace.to_string(), id.to_string(), uploader.to_string());
        self.call(move |conn| Self::record_blob_uploader_on(conn, &namespace, &id, &uploader))
            .await
    }

    // ---- Materialized Store commit ledger ----

    pub(crate) async fn materialized_frontier(
        &self,
    ) -> Result<BTreeMap<String, CommitPosition>, DbError> {
        self.call(|conn| Self::materialized_frontier_on(conn, None))
            .await
    }

    pub(crate) async fn exact_materialized_hash(
        &self,
        device_id: &str,
        seq: u64,
    ) -> Result<Option<ObjectHash>, DbError> {
        let device_id = device_id.to_string();
        self.call(move |conn| Self::materialized_position_on(conn, &device_id, seq))
            .await
    }

    pub(crate) async fn snapshot_coverage_frontier(
        &self,
    ) -> Result<BTreeMap<String, CommitPosition>, DbError> {
        self.call(|conn| {
            let mut stmt = conn
                .prepare("SELECT device_id, seq, commit_hash FROM snapshot_coverage")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut frontier = BTreeMap::new();
            for row in rows {
                let (device_id, seq, hash) = row.map_err(DbError::from)?;
                frontier.insert(
                    device_id.clone(),
                    CommitPosition {
                        seq: Self::sequence_from_sqlite(&device_id, seq)?,
                        commit_hash: hash.parse().map_err(|error| {
                            DbError::Message(format!("snapshot coverage hash: {error}"))
                        })?,
                    },
                );
            }
            Ok(frontier)
        })
        .await
    }

    pub(crate) fn materialized_frontier_on(
        conn: &Connection,
        exclude_device: Option<&str>,
    ) -> Result<BTreeMap<String, CommitPosition>, DbError> {
        let mut frontier = BTreeMap::new();
        let mut stmt = conn
            .prepare(
                "SELECT m.device_id, m.seq, m.commit_hash \
                 FROM materialized_commits m \
                 JOIN (SELECT device_id, MAX(seq) AS seq FROM materialized_commits \
                       GROUP BY device_id) latest \
                   ON latest.device_id = m.device_id AND latest.seq = m.seq",
            )
            .map_err(DbError::from)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        for row in rows {
            let (device_id, seq, hash) = row.map_err(DbError::from)?;
            if exclude_device == Some(device_id.as_str()) {
                continue;
            }
            frontier.insert(
                device_id.clone(),
                CommitPosition {
                    seq: Self::sequence_from_sqlite(&device_id, seq)?,
                    commit_hash: hash.parse().map_err(|error| {
                        DbError::Message(format!("materialized commit hash: {error}"))
                    })?,
                },
            );
        }

        let mut coverage = conn
            .prepare("SELECT device_id, seq, commit_hash FROM snapshot_coverage")
            .map_err(DbError::from)?;
        let rows = coverage
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        for row in rows {
            let (device_id, seq, hash) = row.map_err(DbError::from)?;
            if exclude_device == Some(device_id.as_str()) {
                continue;
            }
            let position = CommitPosition {
                seq: Self::sequence_from_sqlite(&device_id, seq)?,
                commit_hash: hash.parse().map_err(|error| {
                    DbError::Message(format!("snapshot coverage hash: {error}"))
                })?,
            };
            if frontier
                .get(&device_id)
                .is_none_or(|current| current.seq < position.seq)
            {
                frontier.insert(device_id, position);
            }
        }
        Ok(frontier)
    }

    pub(crate) fn materialized_position_on(
        conn: &Connection,
        device_id: &str,
        seq: u64,
    ) -> Result<Option<ObjectHash>, DbError> {
        let seq = Self::sequence_to_sqlite(device_id, seq)?;
        conn.query_row(
            "SELECT commit_hash FROM materialized_commits \
             WHERE device_id = ?1 AND seq = ?2",
            (device_id, seq),
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DbError::from)?
        .map(|hash| {
            hash.parse()
                .map_err(|error| DbError::Message(format!("materialized commit hash: {error}")))
        })
        .transpose()
    }

    fn latest_position_for_device_on(
        conn: &Connection,
        device_id: &str,
    ) -> Result<Option<CommitPosition>, DbError> {
        let materialized = conn
            .query_row(
                "SELECT seq, commit_hash FROM materialized_commits
                 WHERE device_id = ?1 ORDER BY seq DESC LIMIT 1",
                [device_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let coverage = conn
            .query_row(
                "SELECT seq, commit_hash FROM snapshot_coverage WHERE device_id = ?1",
                [device_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let positions = [materialized, coverage]
            .into_iter()
            .flatten()
            .map(|(seq, hash)| {
                Ok(CommitPosition {
                    seq: Self::sequence_from_sqlite(device_id, seq)?,
                    commit_hash: hash.parse().map_err(|error| {
                        DbError::Message(format!("latest Store position hash: {error}"))
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        if positions.len() == 2
            && positions[0].seq == positions[1].seq
            && positions[0].commit_hash != positions[1].commit_hash
        {
            return Err(DbError::Message(format!(
                "materialized ledger and snapshot coverage fork {device_id:?} at sequence {}",
                positions[0].seq
            )));
        }
        Ok(positions.into_iter().max_by_key(|position| position.seq))
    }

    pub(crate) fn record_activated_store_device_registrations_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        registrations: &[StoreDeviceRegistration],
    ) -> Result<(), DbError> {
        if registrations.len() != commit.device_registrations.len() {
            return Err(DbError::Message(
                "Store device registration activation count differs from the signed commit"
                    .to_string(),
            ));
        }
        let stream_id = commit.order.stream_id(&commit.device_id);
        let seq = Self::sequence_to_sqlite(stream_id, commit.seq())?;
        let commit_hash = commit.commit_hash().to_string();
        for reference in &commit.device_registrations {
            let registration = registrations
                .iter()
                .find(|registration| {
                    registration.device_id == reference.device_id
                        && registration.revision == reference.revision
                })
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Store commit is missing registration bytes for {:?} revision {}",
                        reference.device_id, reference.revision
                    ))
                })?;
            reference
                .verify_registration(registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
            if registration.store_root_hash != commit.store_root_hash
                || registration.author_pubkey != commit.author_pubkey
            {
                return Err(DbError::Message(format!(
                    "Store registration {:?} revision {} is not signed by its activating commit author",
                    registration.device_id, registration.revision
                )));
            }
            let existing = conn
                .query_row(
                    "SELECT revision, registration_hash, previous_registration_hash, state, \
                            author_pubkey, registration_bytes, stream_id, seq, commit_hash \
                     FROM store_device_registration_activations \
                     WHERE device_id = ?1 ORDER BY revision DESC LIMIT 1",
                    [&registration.device_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, Vec<u8>>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, String>(8)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            let registration_bytes = registration.to_bytes();
            let state = match registration.state {
                StoreDeviceRegistrationState::Active => "active",
                StoreDeviceRegistrationState::Retired => "retired",
            };
            let previous_hash = registration
                .previous_registration_hash
                .map(|hash| hash.to_string());
            if let Some((
                existing_revision,
                existing_hash,
                existing_previous,
                existing_state,
                existing_author,
                existing_bytes,
                existing_stream,
                existing_seq,
                existing_commit,
            )) = existing
            {
                let existing_revision = u64::try_from(existing_revision).map_err(|_| {
                    DbError::Message(
                        "activated Store device registration revision is negative".to_string(),
                    )
                })?;
                if existing_revision == registration.revision {
                    if existing_hash == reference.registration_hash.to_string()
                        && existing_previous == previous_hash
                        && existing_state == state
                        && existing_author == registration.author_pubkey
                        && existing_bytes == registration_bytes
                        && existing_stream == stream_id
                        && existing_seq == seq
                        && existing_commit == commit_hash
                    {
                        continue;
                    }
                    return Err(DbError::Message(format!(
                        "Store device registration {:?} revision {} has a different activation",
                        registration.device_id, registration.revision
                    )));
                }
                let expected_revision = existing_revision.checked_add(1).ok_or_else(|| {
                    DbError::Message("Store device registration revision overflow".to_string())
                })?;
                if registration.revision != expected_revision
                    || previous_hash.as_deref() != Some(existing_hash.as_str())
                    || registration.author_pubkey != existing_author
                    || existing_state != "active"
                    || registration.state != StoreDeviceRegistrationState::Retired
                {
                    return Err(DbError::Message(format!(
                        "Store device registration {:?} revision {} does not extend its activated chain",
                        registration.device_id, registration.revision
                    )));
                }
            } else if registration.revision != 1
                || registration.previous_registration_hash.is_some()
                || registration.state != StoreDeviceRegistrationState::Active
            {
                return Err(DbError::Message(format!(
                    "Store device registration {:?} must begin with revision 1 Active",
                    registration.device_id
                )));
            }
            conn.execute(
                "INSERT INTO store_device_registration_activations \
                 (device_id, revision, registration_hash, previous_registration_hash, state, \
                  author_pubkey, registration_bytes, stream_id, seq, commit_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    registration.device_id,
                    i64::try_from(registration.revision).map_err(|_| DbError::Message(
                        "Store device registration revision exceeds SQLite INTEGER".to_string()
                    ))?,
                    reference.registration_hash.to_string(),
                    previous_hash,
                    state,
                    registration.author_pubkey,
                    registration_bytes,
                    stream_id,
                    seq,
                    commit_hash,
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) async fn activated_store_device_registrations(
        &self,
    ) -> Result<Vec<StoreDeviceRegistration>, DbError> {
        let store_root_hash = self.required_store_root_hash().await?;
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT device_id, revision, registration_bytes \
                     FROM store_device_registration_activations \
                     ORDER BY device_id, revision",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows.map(|row| {
                let (device_id, revision, bytes) = row.map_err(DbError::from)?;
                let revision = u64::try_from(revision).map_err(|_| {
                    DbError::Message(
                        "activated Store device registration revision is negative".to_string(),
                    )
                })?;
                StoreDeviceRegistration::parse_at(&bytes, store_root_hash, &device_id, revision)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "activated Store device registration {device_id:?}/{revision}: {error}"
                        ))
                    })
            })
            .collect()
        })
        .await
    }

    pub(crate) fn record_materialized_commit_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
    ) -> Result<(), DbError> {
        let actual_hash = commit.commit_hash();
        let stream_id = commit.order.stream_id(&commit.device_id);
        let predecessor = if commit.seq() == 1 {
            None
        } else if let Some(hash) =
            Self::materialized_position_on(conn, stream_id, commit.seq() - 1)?
        {
            Some(hash)
        } else {
            conn.query_row(
                "SELECT commit_hash FROM snapshot_coverage \
                 WHERE device_id = ?1 AND seq = ?2",
                (
                    stream_id,
                    Self::sequence_to_sqlite(stream_id, commit.seq() - 1)?,
                ),
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|hash| {
                hash.parse()
                    .map_err(|error| DbError::Message(format!("snapshot coverage hash: {error}")))
            })
            .transpose()?
        };
        if predecessor != commit.previous_commit_hash() {
            return Err(DbError::Message(format!(
                "Store commit {}/{} names predecessor {:?}, durable predecessor is {:?}",
                stream_id,
                commit.seq(),
                commit.previous_commit_hash(),
                predecessor
            )));
        }
        let seq = Self::sequence_to_sqlite(stream_id, commit.seq())?;
        conn.execute(
            "INSERT INTO materialized_commits (device_id, seq, commit_hash) \
             VALUES (?1, ?2, ?3)",
            (stream_id, seq, actual_hash.to_string()),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    pub(crate) fn record_materialized_serial_commit_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        membership: &SerialMembershipState,
        key_generation: u64,
    ) -> Result<(), DbError> {
        if commit.policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial membership state cannot accompany a MergeConcurrent commit".to_string(),
            ));
        }
        Self::record_materialized_commit_on(conn, commit)?;
        let membership = serde_json::to_string(membership).map_err(|error| {
            DbError::Message(format!("serialize Serial membership state: {error}"))
        })?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SERIAL_MEMBERSHIP_STATE_KEY, membership),
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (SERIAL_KEY_GENERATION_STATE_KEY, key_generation.to_string()),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    pub async fn serial_membership_state(&self) -> Result<Option<SerialMembershipState>, DbError> {
        let Some(raw) = self.get_protocol_state(SERIAL_MEMBERSHIP_STATE_KEY).await? else {
            return Ok(None);
        };
        serde_json::from_str(&raw)
            .map(Some)
            .map_err(|error| DbError::Message(format!("parse Serial membership state: {error}")))
    }

    pub async fn serial_key_generation(&self) -> Result<Option<u64>, DbError> {
        let Some(raw) = self
            .get_protocol_state(SERIAL_KEY_GENERATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };
        raw.parse::<u64>()
            .map(Some)
            .map_err(|error| DbError::Message(format!("parse Serial key generation: {error}")))
    }

    pub(crate) async fn install_serial_root_authorization(
        &self,
        founder_pubkey: String,
        authorization: SerialAuthorizationState,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            if Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)?.is_some() {
                return Err(DbError::Message(
                    "cannot install founder-only Serial authorization after a materialized commit"
                        .to_string(),
                ));
            }
            let existing_state: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM protocol_state WHERE key IN (?1, ?2, ?3)",
                    rusqlite::params![
                        crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY,
                        SERIAL_MEMBERSHIP_STATE_KEY,
                        SERIAL_KEY_GENERATION_STATE_KEY,
                    ],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if existing_state != 0 {
                return Err(DbError::Message(
                    "cannot install founder-only Serial authorization over existing state"
                        .to_string(),
                ));
            }
            let membership = serde_json::to_string(&authorization.membership).map_err(|error| {
                DbError::Message(format!(
                    "serialize Serial founder membership state: {error}"
                ))
            })?;
            for (key, value) in [
                (
                    crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY,
                    founder_pubkey,
                ),
                (SERIAL_MEMBERSHIP_STATE_KEY, membership),
                (
                    SERIAL_KEY_GENERATION_STATE_KEY,
                    authorization.key_generation.to_string(),
                ),
            ] {
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )
                .map_err(DbError::from)?;
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn materialize_serial_control_commit(
        &self,
        commit: StoreBatchCommit,
        authorization_after: SerialAuthorizationState,
    ) -> Result<(), DbError> {
        if commit.control.is_none() || commit.store_package.is_some() {
            return Err(DbError::Message(
                "Serial control materialization requires a control-only Store batch".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            Self::record_materialized_serial_commit_on(
                &tx,
                &commit,
                &authorization_after.membership,
                authorization_after.key_generation,
            )?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn install_serial_authorization_at_position(
        &self,
        expected: CommitPosition,
        authorization: SerialAuthorizationState,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let actual = Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)?;
            if actual.as_ref() != Some(&expected) {
                return Err(DbError::Message(format!(
                    "cannot install Serial authorization at {expected:?}; durable position is {actual:?}"
                )));
            }
            let membership = serde_json::to_string(&authorization.membership).map_err(|error| {
                DbError::Message(format!("serialize Serial membership state: {error}"))
            })?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (SERIAL_MEMBERSHIP_STATE_KEY, membership),
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (
                    SERIAL_KEY_GENERATION_STATE_KEY,
                    authorization.key_generation.to_string(),
                ),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn install_bootstrap_state(
        &self,
        coverage: &CommitFrontier,
        snapshot_hash: ObjectHash,
        store_root_hash: ObjectHash,
    ) -> Result<(), DbError> {
        if coverage.policy() != self.write_policy() {
            return Err(DbError::Message(format!(
                "snapshot coverage uses {:?}, database uses {:?}",
                coverage.policy(),
                self.write_policy()
            )));
        }
        let coverage = coverage.clone().into_positions();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            tx.execute("DELETE FROM snapshot_coverage", [])
                .map_err(DbError::from)?;
            for (device_id, position) in coverage {
                tx.execute(
                    "INSERT INTO snapshot_coverage \
                     (device_id, seq, commit_hash, snapshot_hash) VALUES (?1, ?2, ?3, ?4)",
                    (
                        &device_id,
                        Self::sequence_to_sqlite(&device_id, position.seq)?,
                        position.commit_hash.to_string(),
                        snapshot_hash.to_string(),
                    ),
                )
                .map_err(DbError::from)?;
            }
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (STORE_ROOT_HASH_STATE_KEY, store_root_hash.to_string()),
            )
            .map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (LAST_SNAPSHOT_HASH_STATE_KEY, snapshot_hash.to_string()),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    fn sequence_from_sqlite(device_id: &str, value: i64) -> Result<u64, DbError> {
        let value = u64::try_from(value).map_err(|_| {
            DbError::Message(format!(
                "Store position for {device_id:?} contains negative sequence {value}"
            ))
        })?;
        if value == 0 {
            return Err(DbError::Message(format!(
                "Store position for {device_id:?} contains sequence zero"
            )));
        }
        Ok(value)
    }

    fn sequence_to_sqlite(device_id: &str, value: u64) -> Result<i64, DbError> {
        if value == 0 {
            return Err(DbError::Message(format!(
                "Store position for {device_id:?} cannot use sequence zero"
            )));
        }
        i64::try_from(value).map_err(|_| {
            DbError::Message(format!(
                "Store position for {device_id:?} exceeds SQLite INTEGER"
            ))
        })
    }

    // ---- Cloud outbox ----

    /// Enqueue a blob upload. `scope` names which key the blob is encrypted
    /// under (master or a derived scope); coven persists it on the row and
    /// resolves it to a key at drain, long after the enqueue site is gone.
    /// At most one upload per `(operation, cloud_key)`; a re-enqueue for the same key
    /// overwrites the row's source path, scope, and pin choice with this call's values
    /// (latest enqueue decides). Queuing an upload also cancels any pending delete of
    /// the same key — latest intent wins, so a re-upload isn't tombstoned in the same
    /// cycle.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_upload(
        &self,
        file_id: &str,
        cloud_key: &str,
        source_path: Option<&str>,
        scope: crate::blob::BlobScope,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        let (file_id, cloud_key, source_path, created_at) = (
            file_id.to_string(),
            cloud_key.to_string(),
            source_path.map(str::to_string),
            created_at.to_string(),
        );
        self.call(move |conn| {
            Self::enqueue_upload_on(
                conn,
                &file_id,
                &cloud_key,
                source_path.as_deref(),
                scope,
                retain_pinned,
                &created_at,
            )
        })
        .await
    }

    /// Transaction-composable form of [`enqueue_upload`](Self::enqueue_upload):
    /// runs on a connection the host already holds inside a
    /// [`call`](Self::call) closure, so the host can commit a row's upload
    /// intent atomically with the row itself (e.g. an import that must either
    /// land with its uploads queued or not land at all).
    pub fn enqueue_upload_on(
        conn: &Connection,
        file_id: &str,
        cloud_key: &str,
        source_path: Option<&str>,
        scope: crate::blob::BlobScope,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        // Latest intent wins: queuing an upload for a key cancels a pending delete
        // of the same key, so a re-upload and a stale delete can't both be live in
        // one cycle (which the upload-then-delete phase split would otherwise
        // resolve by deleting the freshly re-uploaded blob). Runs on the caller's
        // connection so a transactional import stays atomic with this cancel.
        conn.execute(
            "DELETE FROM cloud_outbox WHERE operation = 'delete' AND cloud_key = ?1",
            [cloud_key],
        )
        .map_err(DbError::from)?;
        // Latest enqueue wins for the row's parameters too, not just its existence. A
        // second make_remote on the same still-Local root re-registers the source path
        // and pin choice; the queued row must adopt them, or the drain would upload the
        // stale path (retrying a dead path forever) or miss the new pin. Reset the
        // attempt counter and backoff so a corrected path retries immediately rather
        // than waiting out the failed old path's window.
        conn.execute(
            "INSERT INTO cloud_outbox \
             (operation, file_id, cloud_key, source_path, scope, retain_pinned, created_at) \
             VALUES ('upload', ?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(operation, cloud_key) DO UPDATE SET \
                 source_path = excluded.source_path, \
                 scope = excluded.scope, \
                 retain_pinned = excluded.retain_pinned, \
                 attempt_count = 0, \
                 last_error = NULL, \
                 last_attempt_at = NULL",
            (
                file_id,
                cloud_key,
                source_path,
                scope.to_outbox_str(),
                retain_pinned,
                created_at,
            ),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Enqueue a blob delete. The next sync cycle's drain turns it into a signed
    /// cloud tombstone, and a later GC reclaims the blob once the convergence grace
    /// has passed (see [`crate::blob::delete`]). Idempotent on `(operation,
    /// cloud_key)`. Queuing a delete also cancels any pending upload of the same key
    /// — latest intent wins.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_delete(&self, cloud_key: &str, created_at: &str) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        self.call(move |conn| Self::enqueue_delete_on(conn, &cloud_key, &created_at))
            .await
    }

    /// Transaction-composable form of [`enqueue_delete`](Self::enqueue_delete):
    /// runs on a connection the host already holds inside a [`call`](Self::call)
    /// closure, so coven's `make_local` can flip a root's gate false and enqueue its
    /// blob deletes in one transaction — the tombstone can't be lost to a crash
    /// between the flip and a separate enqueue. A delete touches no key, so it
    /// carries no scope (the column is NULL for a delete row).
    pub fn enqueue_delete_on(
        conn: &Connection,
        cloud_key: &str,
        created_at: &str,
    ) -> Result<(), DbError> {
        // Latest intent wins: queuing a delete cancels a pending upload of the same
        // key (the mirror of `enqueue_upload_on`), so an enqueued-then-deleted blob
        // isn't uploaded only to be tombstoned in the same cycle. It also drops a
        // pending tombstone-cancel for the key: a fresh delete wants the blob
        // tombstoned, so a leftover cancel (which would remove that tombstone) must
        // not survive to undo it.
        conn.execute(
            "DELETE FROM cloud_outbox \
             WHERE operation IN ('upload', 'cancel') AND cloud_key = ?1",
            [cloud_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox \
             (operation, cloud_key, scope, created_at) \
             VALUES ('delete', ?1, NULL, ?2)",
            (cloud_key, created_at),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Enqueue a durable tombstone-cancel for `cloud_key`: the tombstone-cancel drain
    /// ([`crate::blob::delete::drain_tombstone_cancels`]) retries removing the
    /// tombstone until it is gone, so a re-uploaded blob is never reclaimed by a GC
    /// that outraces an inline cancel. Backs a failed inline cancel on every upload
    /// path. Idempotent on `(operation, cloud_key)`.
    pub async fn enqueue_cancel(&self, cloud_key: &str, created_at: &str) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        self.call(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO cloud_outbox (operation, cloud_key, scope, created_at) \
                 VALUES ('cancel', ?1, NULL, ?2)",
                (cloud_key, created_at),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Pending upload entries, oldest first. The host reads these to drive its
    /// own upload-status UI; coven's sync loop reads them to do the uploads.
    pub async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("upload").await
    }

    /// Pending delete entries, oldest first.
    pub async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("delete").await
    }

    /// Pending tombstone-cancel entries, oldest first. Each names a `cloud_key`
    /// whose tombstone must be removed because the blob was re-uploaded; the
    /// tombstone-cancel drain reads these and retries the removal until it lands
    /// (see [`crate::blob::delete::drain_tombstone_cancels`]).
    pub async fn get_pending_cloud_cancels(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("cancel").await
    }

    async fn pending_outbox(&self, op_str: &'static str) -> Result<Vec<OutboxEntry>, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT id, operation, file_id, cloud_key, source_path, scope, \
                            retain_pinned, attempt_count, last_attempt_at \
                     FROM cloud_outbox WHERE operation = ?1 ORDER BY id",
                )
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([op_str], row_to_outbox_entry)
                .map_err(DbError::from)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(DbError::from)?);
            }
            Ok(out)
        })
        .await
    }

    /// Remove an outbox entry by id (after the upload or delete completed).
    pub async fn remove_cloud_outbox_entry(&self, id: i64) -> Result<(), DbError> {
        self.call(move |conn| {
            conn.execute("DELETE FROM cloud_outbox WHERE id = ?1", [id])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    /// Clear the retry-backoff timestamp on failed uploads so the next cycle
    /// retries them immediately. Backs a host "retry now" action.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn reset_cloud_outbox_backoff(&self) -> Result<(), DbError> {
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox SET last_attempt_at = NULL \
                 WHERE operation = 'upload' AND attempt_count > 0",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Record a failed upload attempt (bumps `attempt_count`, stores the error
    /// and the time). The entry stays queued for retry.
    pub async fn record_cloud_upload_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let (error, attempted_at) = (error.to_string(), attempted_at.to_string());
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox \
                 SET attempt_count = attempt_count + 1, last_error = ?1, last_attempt_at = ?2 \
                 WHERE id = ?3",
                (error, attempted_at, id),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Record a failed delete/cancel attempt (bumps `attempt_count`, stores the
    /// error and the time), scoped to `operation` so an id collision with the
    /// other kind of row cannot mutate it. Shared by
    /// [`record_cloud_delete_failure`](Self::record_cloud_delete_failure) and
    /// [`record_cloud_cancel_failure`](Self::record_cloud_cancel_failure), which
    /// differ only in which operation they're scoped to.
    async fn record_cloud_outbox_failure(
        &self,
        id: i64,
        operation: &'static str,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let (error, attempted_at) = (error.to_string(), attempted_at.to_string());
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox \
                 SET attempt_count = attempt_count + 1, last_error = ?1, last_attempt_at = ?2 \
                 WHERE id = ?3 AND operation = ?4",
                (error, attempted_at, id, operation),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Record a failed delete/tombstone attempt (bumps `attempt_count`, stores the
    /// error and the time). Scoped to delete rows so an id collision with another
    /// operation cannot mutate the wrong kind of outbox entry.
    pub async fn record_cloud_delete_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        self.record_cloud_outbox_failure(id, "delete", error, attempted_at)
            .await
    }

    /// Record a failed tombstone-cancel retry (bumps `attempt_count`, stores the
    /// error and the time). Scoped to cancel rows so an id collision with another
    /// operation cannot mutate the wrong kind of outbox entry.
    pub async fn record_cloud_cancel_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        self.record_cloud_outbox_failure(id, "cancel", error, attempted_at)
            .await
    }

    // ---- Local blob refs (external user files) ----

    /// Register an external blob ref: map `blob_id` to the user-owned file at
    /// `path`, whose plaintext length is `size`. Insert-or-replace on `blob_id`, so
    /// a relocate (the user picks a new folder; the host recomputes each path and
    /// re-registers) overwrites the prior row. coven reads this file but does not
    /// own it; a read validates it by presence + size (see
    /// [`crate::db`]'s `local_blob_refs` and [`crate::blob::cache::read_blob`]).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn register_external_blob(
        &self,
        blob_id: &str,
        namespace: &str,
        path: &Path,
        size: u64,
    ) -> Result<(), DbError> {
        let (blob_id, namespace, path) = (
            blob_id.to_string(),
            namespace.to_string(),
            path.to_path_buf(),
        );
        self.call(move |conn| {
            Self::register_external_blob_on(conn, &blob_id, &namespace, &path, size)
        })
        .await
    }

    /// Transaction-composable form of
    /// [`register_external_blob`](Self::register_external_blob): runs on a
    /// connection the host already holds inside a [`call`](Self::call) closure, so
    /// coven's `make_local` can flip a root's gate false and register the external
    /// refs for its now-local user files in one transaction. Insert-or-replace on
    /// `blob_id`, so a relocate overwrites the prior row.
    pub fn register_external_blob_on(
        conn: &Connection,
        blob_id: &str,
        namespace: &str,
        path: &Path,
        size: u64,
    ) -> Result<(), DbError> {
        let path = path.to_str().ok_or_else(|| {
            DbError::Message(format!(
                "external blob path for {blob_id} is not valid UTF-8: {path:?}"
            ))
        })?;
        conn.execute(
            "INSERT INTO local_blob_refs (blob_id, namespace, path, size) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(blob_id) DO UPDATE SET \
                 namespace = excluded.namespace, \
                 path = excluded.path, \
                 size = excluded.size",
            (blob_id, namespace, path, size as i64),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Remove the external blob ref for `blob_id`, so the blob resolves through the
    /// normal cache/cloud path again. A no-op if no ref is registered.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn clear_external_blob(&self, blob_id: &str) -> Result<(), DbError> {
        let blob_id = blob_id.to_string();
        self.call(move |conn| Self::clear_external_blob_on(conn, &blob_id))
            .await
    }

    /// Transaction-composable form of
    /// [`clear_external_blob`](Self::clear_external_blob): runs on a connection the
    /// host already holds inside a [`call`](Self::call) closure, so coven's
    /// `make_remote` completion can flip a root's gate true and drop the external
    /// refs for its now-cloud-backed blobs in one transaction. A no-op if no ref is
    /// registered.
    pub fn clear_external_blob_on(conn: &Connection, blob_id: &str) -> Result<(), DbError> {
        conn.execute("DELETE FROM local_blob_refs WHERE blob_id = ?1", [blob_id])
            .map(|_| ())
            .map_err(DbError::from)
    }

    // ---- Blob make-Remote intents (device-local in-flight make_remote markers) ----

    /// Record an in-flight make_remote of `(root_table, root_id)` as a durable
    /// marker. Transaction-composable: coven's `make_remote` inserts this in the same
    /// transaction that enqueues the root's user-provided blob uploads or flips a
    /// host-provided-only root, so an in-flight make_remote is a durable, atomic fact.
    /// `retain_pinned` is consumed by inline host-provided uploads, which have no
    /// upload outbox row to carry the pin choice.
    pub fn insert_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        retain_pinned: bool,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT OR REPLACE INTO blob_make_remote_intents \
             (root_table, root_id, retain_pinned) VALUES (?1, ?2, ?3)",
            (root_table, root_id, retain_pinned),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Remove the make_remote intent for `(root_table, root_id)`.
    /// Transaction-composable so it commits with the gate flip (on completion) or
    /// with the cancel cleanup.
    pub fn delete_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), DbError> {
        conn.execute(
            "DELETE FROM blob_make_remote_intents WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Whether a make_remote is in flight for `(root_table, root_id)`. Read inside
    /// the upload drain's completion check to tell a make_remote to finish (`true`)
    /// from an orphan upload of a cancelled make_remote to tombstone (`false`).
    /// Synchronous (runs on a connection the caller already holds).
    pub fn make_remote_intent_exists(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        conn.query_row(
            "SELECT 1 FROM blob_make_remote_intents \
             WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(DbError::from)
    }

    pub fn make_remote_intent_retain_pinned(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<bool>, DbError> {
        conn.query_row(
            "SELECT retain_pinned FROM blob_make_remote_intents \
             WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
            |r| r.get::<_, bool>(0),
        )
        .optional()
        .map_err(DbError::from)
    }

    /// The external user-owned file `blob_id` resolves to, or `None` when no ref is
    /// registered (the blob is host-provided local-store, cache, or cloud — not a
    /// user-provided external file). The locality-aware read
    /// ([`crate::blob::cache::read_blob`]) consults this before dispatching on the
    /// blob's locality.
    pub async fn external_blob(&self, blob_id: &str) -> Result<Option<ExternalBlob>, DbError> {
        let blob_id = blob_id.to_string();
        self.call(move |conn| {
            conn.query_row(
                "SELECT path, size FROM local_blob_refs WHERE blob_id = ?1",
                [blob_id],
                |r| {
                    Ok(ExternalBlob {
                        path: std::path::PathBuf::from(r.get::<_, String>(0)?),
                        size: r.get::<_, i64>(1)? as u64,
                    })
                },
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
    }
}

/// Map a `cloud_outbox` row to an [`OutboxEntry`]. Column order matches the
/// SELECT in [`Database::pending_outbox`]. The flat row reads back as one
/// [`OutboxOperation`] variant or the other, built from the columns that belong
/// to that operation — the rest are NULL and unread.
fn row_to_outbox_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    let op_tag: String = r.get(1)?;
    let operation = match op_tag.as_str() {
        "upload" => {
            // A present scope must parse — a present-but-unparseable scope is a
            // corrupt row. An upload always wrote one (the column is non-NULL
            // for an upload), so its absence is also corruption.
            let scope_str: String = r.get(5)?;
            let scope = crate::blob::BlobScope::from_outbox_str(&scope_str)
                .unwrap_or_else(|| panic!("invalid cloud_outbox.scope: {scope_str:?}"));
            OutboxOperation::Upload {
                file_id: r.get(2)?,
                source_path: r.get(4)?,
                scope,
                retain_pinned: r.get(6)?,
            }
        }
        "delete" => OutboxOperation::Delete,
        "cancel" => OutboxOperation::Cancel,
        other => panic!("invalid cloud_outbox.operation: {other:?}"),
    };
    Ok(OutboxEntry {
        id: r.get(0)?,
        cloud_key: r.get(3)?,
        attempt_count: r.get(7)?,
        last_attempt_at: r.get(8)?,
        operation,
    })
}

/// Seed the register clock from one candidate floor, if present. A present but
/// unparseable value is a corrupt register and aborts; `None` contributes no
/// floor.
fn seed_from(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), DbError> {
    if let Some(stamp) = value {
        let floor = Timestamp::parse(&stamp)
            .ok_or_else(|| DbError::Message(format!("corrupt {context}: {stamp:?}")))?;
        hlc.seed(&floor);
    }
    Ok(())
}

/// Greatest `_updated_at` within the restart seed's honest future bound, scanned
/// across every synced table. A registered table that does not exist is a host
/// integration error and surfaces as `Err`.
fn scan_max_updated_at(
    conn: &Connection,
    synced_tables: &[SyncedTable],
    seed_bound_ms: u64,
) -> Result<Option<String>, DbError> {
    let mut overall: Option<String> = None;
    let seed_bound = format!("{seed_bound_ms:013}");
    for t in synced_tables {
        let sql = format!(
            "SELECT MAX(_updated_at) FROM {} WHERE substr(_updated_at, 1, 13) <= ?1",
            crate::sync::session::quote_ident(t.name())
        );
        let value: Option<String> = conn
            .query_row(&sql, [&seed_bound], |r| r.get::<_, Option<String>>(0))
            .map_err(|e| {
                DbError::Message(format!(
                    "register-floor scan over synced table {}: {e}",
                    t.name()
                ))
            })?;
        if let Some(v) = value {
            overall = Some(match overall {
                Some(cur) if cur >= v => cur,
                _ => v,
            });
        }
    }
    Ok(overall)
}

/// Create a capture session and attach every synced table, so a journaled
/// transaction records changes to exactly those tables.
fn attach_session<'c>(
    conn: &'c Connection,
    synced_tables: &[SyncedTable],
) -> Result<rusqlite::session::Session<'c>, DbError> {
    let mut session = rusqlite::session::Session::new(conn)
        .map_err(|e| DbError::Message(format!("failed to create capture session: {e}")))?;
    for t in synced_tables {
        session.attach(Some(t.name())).map_err(|e| {
            DbError::Message(format!(
                "failed to attach synced table {} to session: {e}",
                t.name()
            ))
        })?;
    }
    Ok(session)
}

/// Drain a journal session's recorded changes into a changeset. The caller drops
/// the session right after (it lives only for the span of one journaled
/// transaction), so there is nothing to reset.
fn capture_changeset(session: &mut rusqlite::session::Session<'_>) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    session
        .changeset_strm(&mut buf)
        .map(|()| buf)
        .map_err(DbError::from)
}

fn open_connection(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    // WAL so a read-only connection in another process can read committed rows while
    // this writer commits. The mode is stored in the db header and persists, so a
    // later read-only open finds the db already in WAL.
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .map_err(DbError::from)?;
    Ok(conn)
}

/// Open a `SQLITE_OPEN_READONLY` connection for [`Database::open_read_only`].
/// `NO_MUTEX` because coven serializes every access
/// on its one connection thread; the connection sets no journal mode (a read-only
/// connection cannot, and the writer already put the db in WAL).
fn open_connection_read_only(path: &Path) -> Result<Connection, DbError> {
    use rusqlite::OpenFlags;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;
    Connection::open_with_flags(path, flags).map_err(DbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::delete::BLOB_TOMBSTONE_GRACE;

    fn notes_migration() -> Migration {
        Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )
    }

    fn things_migration() -> Migration {
        Migration::sql(
            1,
            "things",
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )
    }

    fn things_table(identity: crate::sync::session::RowIdentity) -> SyncedTable {
        SyncedTable::new("things", identity)
    }

    #[tokio::test]
    async fn required_store_root_hash_rejects_missing_and_malformed_state() {
        let (db, _) = Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                "notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "required-store-root".to_string(),
            &[notes_migration()],
        )
        .expect("open database");

        let missing = db
            .required_store_root_hash()
            .await
            .expect_err("missing Store root must fail");
        assert!(matches!(missing, DbError::StoreRootHashMissing));

        db.set_protocol_state(STORE_ROOT_HASH_STATE_KEY, "not-a-root-hash")
            .await
            .expect("write malformed Store root");
        let malformed = db
            .required_store_root_hash()
            .await
            .expect_err("malformed Store root must fail");
        assert!(matches!(
            malformed,
            DbError::StoreRootHashInvalid { reason } if !reason.is_empty()
        ));

        let expected = ObjectHash::digest(b"required Store root");
        db.set_protocol_state(STORE_ROOT_HASH_STATE_KEY, &expected.to_string())
            .await
            .expect("write Store root");
        assert_eq!(db.required_store_root_hash().await.unwrap(), expected);
    }

    #[test]
    fn fresh_open_rolls_back_host_schema_and_coven_metadata_when_routing_is_invalid() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("fresh-routing-failure.sqlite");
        let result = Database::open(
            &path,
            vec![things_table(crate::sync::session::RowIdentity::SharedKey)],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "fresh-routing-failure".to_string(),
            &[Migration::sql(
                1,
                "invalid routing",
                "CREATE TABLE local_parents (id TEXT PRIMARY KEY) STRICT;
                 CREATE TABLE things (
                    id TEXT PRIMARY KEY,
                    local_parent_id TEXT NOT NULL REFERENCES local_parents(id),
                    _updated_at TEXT NOT NULL
                 ) STRICT;",
            )],
        );
        let error = match result {
            Ok(_) => panic!("fresh open must reject a synced-to-local foreign key"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("local_parents"), "{error}");

        let conn = Connection::open(&path).expect("inspect rolled-back database");
        let user_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version");
        let durable_tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .expect("durable tables");
        assert_eq!(user_version, 0);
        assert_eq!(durable_tables, 0);
    }

    #[test]
    fn initialized_open_commits_ordinary_migration_without_changing_routing_contract() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("ordinary-migration.sqlite");
        let migrations = [things_migration()];
        let (database, _) = Database::open(
            &path,
            vec![things_table(crate::sync::session::RowIdentity::SharedKey)],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "ordinary-first-open".to_string(),
            &migrations,
        )
        .expect("initial open");
        let pinned_hash = database.sync_routing_hash();
        drop(database);

        let migrations = [
            things_migration(),
            Migration::sql(
                2,
                "ordinary column and index",
                "ALTER TABLE things ADD COLUMN ordinary TEXT DEFAULT 'ordinary';
                 CREATE INDEX things_ordinary ON things(ordinary);",
            ),
        ];
        let (database, _) = Database::open(
            &path,
            vec![things_table(crate::sync::session::RowIdentity::SharedKey)],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "ordinary-second-open".to_string(),
            &migrations,
        )
        .expect("ordinary migration open");
        assert_eq!(database.schema_version(), 2);
        assert_eq!(database.sync_routing_hash(), pinned_hash);
        drop(database);

        let conn = Connection::open(&path).expect("inspect migrated database");
        let ordinary_ordinal: i64 = conn
            .query_row(
                "SELECT cid FROM pragma_table_info('things') WHERE name = 'ordinary'",
                [],
                |row| row.get(0),
            )
            .expect("ordinary column");
        assert_eq!(ordinary_ordinal, 3);
    }

    #[test]
    fn initialized_open_rolls_back_routing_migration_and_user_version() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("routing-migration.sqlite");
        let v1 = || {
            Migration::sql(
                1,
                "gated things",
                "CREATE TABLE things (
                    id TEXT PRIMARY KEY,
                    audience TEXT COLLATE BINARY NOT NULL,
                    _updated_at TEXT NOT NULL
                 ) STRICT;",
            )
        };
        let table = || {
            SyncedTable::new("things", crate::sync::session::RowIdentity::SharedKey)
                .gated_by("audience")
        };
        let (database, _) = Database::open(
            &path,
            vec![table()],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "routing-first-open".to_string(),
            &[v1()],
        )
        .expect("initial open");
        drop(database);

        let v2 = Migration::sql(
            2,
            "change audience collation",
            "CREATE TABLE things_next (
                id TEXT PRIMARY KEY,
                audience TEXT COLLATE NOCASE NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO things_next SELECT * FROM things;
             DROP TABLE things;
             ALTER TABLE things_next RENAME TO things;",
        );
        let result = Database::open(
            &path,
            vec![table()],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "routing-second-open".to_string(),
            &[v1(), v2],
        );
        let error = match result {
            Ok(_) => panic!("routing migration must not commit"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("sync-routing hash"), "{error}");

        let conn = Connection::open(&path).expect("inspect rolled-back migration");
        let user_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version");
        let things_next: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'things_next'",
                [],
                |row| row.get(0),
            )
            .expect("things_next presence");
        let (_, collation, _, _, _) = conn
            .column_metadata(None::<&str>, "things", "audience")
            .expect("audience metadata");
        assert_eq!(user_version, 1);
        assert_eq!(things_next, 0);
        assert_eq!(collation.unwrap().to_bytes(), b"BINARY");
    }

    #[test]
    fn first_open_rolls_back_host_migration_when_gate_model_is_invalid() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("invalid-gate-migration.sqlite");
        let migration = Migration::sql(
            1,
            "composite gate relation",
            "CREATE TABLE parents (
                id TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL,
                UNIQUE (id, code)
             ) STRICT;
             CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL,
                parent_code TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (parent_id, parent_code) REFERENCES parents(id, code)
             ) STRICT;",
        );
        let tables = vec![
            SyncedTable::new("parents", crate::sync::session::RowIdentity::SharedKey)
                .gated_by("shared"),
            SyncedTable::new("children", crate::sync::session::RowIdentity::SharedKey),
        ];

        let error = match Database::open(
            &path,
            tables,
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "invalid-gate-open".to_string(),
            &[migration],
        ) {
            Ok(_) => panic!("an invalid gate model must reject the open"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("composite foreign key"),
            "{error}"
        );

        let conn = Connection::open(&path).expect("inspect rejected database");
        let user_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version");
        assert_eq!(user_version, 0);
        let host_tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('parents', 'children')",
                [],
                |row| row.get(0),
            )
            .expect("host table count");
        assert_eq!(host_tables, 0);
    }

    #[test]
    fn sqlite_session_representation_preserves_upsert_but_loses_primary_key_update_intent() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO things VALUES ('old', 'base', '0000000001000-0000-writer');",
        )
        .expect("schema and seed");
        let tables = vec![things_table(crate::sync::session::RowIdentity::SharedKey)];

        let mut primary_key_session = attach_session(&conn, &tables).expect("attach session");
        let primary_key_tx = conn.unchecked_transaction().expect("transaction");
        primary_key_tx
            .execute(
                "UPDATE things SET id = 'new', _updated_at = '0000000002000-0000-writer' WHERE id = 'old'",
                [],
            )
            .expect("update primary key");
        let primary_key_changes =
            capture_changeset(&mut primary_key_session).expect("capture primary-key update");
        drop(primary_key_tx);
        drop(primary_key_session);
        let primary_key_changes =
            crate::changeset::walk(&primary_key_changes).expect("walk primary-key update");
        assert_eq!(
            primary_key_changes
                .iter()
                .map(|change| (change.op, change.pk()))
                .collect::<Vec<_>>(),
            vec![
                (crate::changeset::ChangeOp::Insert, Some("new")),
                (crate::changeset::ChangeOp::Delete, Some("old")),
            ]
        );

        let mut upsert_session = attach_session(&conn, &tables).expect("attach session");
        let upsert_tx = conn.unchecked_transaction().expect("transaction");
        upsert_tx
            .execute(
                "INSERT INTO things VALUES ('old', 'upserted', '0000000003000-0000-writer')
                 ON CONFLICT(id) DO UPDATE SET
                    body = excluded.body,
                    _updated_at = excluded._updated_at",
                [],
            )
            .expect("same-id upsert");
        let upsert_changes = capture_changeset(&mut upsert_session).expect("capture upsert");
        drop(upsert_tx);
        let upsert_changes = crate::changeset::walk(&upsert_changes).expect("walk upsert");
        assert_eq!(upsert_changes.len(), 1);
        assert_eq!(upsert_changes[0].op, crate::changeset::ChangeOp::Update);
        assert_eq!(upsert_changes[0].pk(), Some("old"));
    }

    #[test]
    fn writer_and_read_only_open_reject_existing_invalid_independent_uuid() {
        let writer_error = match Database::open(
            Path::new(":memory:"),
            vec![things_table(
                crate::sync::session::RowIdentity::IndependentUuid,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "invalid-uuid-writer".to_string(),
            &[Migration::sql(
                1,
                "things",
                "CREATE TABLE things (
                    id TEXT PRIMARY KEY,
                    body TEXT NOT NULL,
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 INSERT INTO things VALUES ('2', 'invalid', '0000000001000-0000-seed');",
            )],
        ) {
            Ok(_) => panic!("writer open must reject an existing non-UUID id"),
            Err(error) => error.to_string(),
        };
        assert!(
            writer_error.contains("things") && writer_error.contains("\"2\""),
            "writer error identifies the table and value: {writer_error}",
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("read-only-invalid.sqlite");
        let (writer, _) = Database::open(
            &path,
            vec![things_table(crate::sync::session::RowIdentity::SharedKey)],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "invalid-uuid-seed".to_string(),
            &[Migration::sql(
                1,
                "things",
                "CREATE TABLE things (
                    id TEXT PRIMARY KEY,
                    body TEXT NOT NULL,
                    _updated_at TEXT NOT NULL
                 ) STRICT;
                 INSERT INTO things VALUES ('2', 'invalid', '0000000001000-0000-seed');",
            )],
        )
        .expect("seed database under its declared SharedKey contract");
        drop(writer);

        let reader_error = match Database::open_read_only(
            &path,
            vec![things_table(
                crate::sync::session::RowIdentity::IndependentUuid,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "invalid-uuid-reader".to_string(),
            &[things_migration()],
        ) {
            Ok(_) => panic!("read-only open must reject an existing non-UUID id"),
            Err(error) => error.to_string(),
        };
        assert!(
            reader_error.contains("things") && reader_error.contains("\"2\""),
            "reader error identifies the table and value: {reader_error}",
        );
    }

    #[test]
    fn database_open_rejects_duplicate_synced_table_declarations() {
        let error = match Database::open(
            Path::new(":memory:"),
            vec![
                things_table(crate::sync::session::RowIdentity::SharedKey),
                things_table(crate::sync::session::RowIdentity::IndependentUuid),
            ],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "duplicate-things".to_string(),
            &[things_migration()],
        ) {
            Ok(_) => panic!("one table cannot have two identity declarations"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("things") && error.contains("declared"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn invalid_host_identity_rolls_back_rows_and_preserves_existing_write() {
        let tables = vec![things_table(
            crate::sync::session::RowIdentity::IndependentUuid,
        )];
        let (db, _) = Database::open(
            Path::new(":memory:"),
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "invalid-host-identity".to_string(),
            &[things_migration()],
        )
        .expect("open");
        let existing_changeset = vec![0x45, 0x58, 0x41, 0x43, 0x54];
        let existing_for_insert = existing_changeset.clone();
        db.call(move |conn| {
            conn.execute(
                "INSERT INTO store_writes
                 (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
                 VALUES (
                    'existing-write', '\"pending\"', '[]', ?1, ?1,
                    '{\"merge_concurrent\":{\"dependencies\":{}}}',
                    '{\"blobs\":[]}'
                 )",
                [existing_for_insert],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("seed existing write records");

        let write_id = db.new_write_id();
        let result = db
            .call(move |conn| {
                Database::run_internal_store_write_transaction_on(
                    conn,
                    &tables,
                    crate::WritePolicy::MergeConcurrent,
                    None,
                    write_id,
                    |tx| {
                        tx.execute(
                            "INSERT INTO things VALUES (?1, 'valid', '0000000002000-0000-writer')",
                            ["f47ac10b-58cc-4372-a567-0e02b2c3d479"],
                        )?;
                        tx.execute(
                        "INSERT INTO things VALUES ('2', 'invalid', '0000000002001-0000-writer')",
                        [],
                    )?;
                        Ok::<_, DbError>(())
                    },
                )
            })
            .await;
        let error = result.expect_err("invalid UUID must reject the host transaction");
        assert!(error.to_string().contains("things") && error.to_string().contains("2"));

        db.call(move |conn| {
            let row_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM things", [], |row| row.get(0))
                .map_err(DbError::from)?;
            let pending = conn
                .prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(DbError::from)?;
            assert_eq!(row_count, 0);
            assert_eq!(pending, vec![existing_changeset]);
            Ok(())
        })
        .await
        .expect("inspect rollback");
    }

    #[tokio::test]
    async fn valid_identity_changes_updates_and_upserts_succeed_but_invalid_new_uuid_rolls_back() {
        let tables = vec![things_table(
            crate::sync::session::RowIdentity::IndependentUuid,
        )];
        let (db, _) = Database::open(
            Path::new(":memory:"),
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "host-identity-changes".to_string(),
            &[things_migration()],
        )
        .expect("open");
        let original = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        db.call(move |conn| {
            conn.execute(
                "INSERT INTO things VALUES (?1, 'base', '0000000001000-0000-writer')",
                [original],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("seed row");

        let update_tables = tables.clone();
        let renamed = "01890a5d-ac96-774b-bcce-b302099c3f74";
        let update_write_id = db.new_write_id();
        db.call(move |conn| {
            Database::run_internal_store_write_transaction_on(
                conn,
                &update_tables,
                crate::WritePolicy::MergeConcurrent,
                None,
                update_write_id,
                |tx| {
                tx.execute(
                    "UPDATE things SET id = ?1, _updated_at = '0000000002000-0000-writer' WHERE id = ?2",
                    [renamed, original],
                )?;
                Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("valid primary-key change succeeds");

        let replace_tables = tables.clone();
        let replaced = "8b1a9953-c461-4e20-8c66-826115d53552";
        let replace_write_id = db.new_write_id();
        db.call(move |conn| {
            Database::run_internal_store_write_transaction_on(
                conn,
                &replace_tables,
                crate::WritePolicy::MergeConcurrent,
                None,
                replace_write_id,
                |tx| {
                    tx.execute("DELETE FROM things WHERE id = ?1", [renamed])?;
                    tx.execute(
                        "INSERT INTO things VALUES (?1, 'replaced', '0000000003000-0000-writer')",
                        [replaced],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("explicit delete and insert succeeds");

        let ordinary_tables = tables.clone();
        let ordinary_write_id = db.new_write_id();
        db.call(move |conn| {
            Database::run_internal_store_write_transaction_on(
                conn,
                &ordinary_tables,
                crate::WritePolicy::MergeConcurrent,
                None,
                ordinary_write_id,
                |tx| {
                tx.execute(
                    "UPDATE things SET body = 'ordinary', _updated_at = '0000000004000-0000-writer' WHERE id = ?1",
                    [replaced],
                )?;
                tx.execute(
                    "INSERT INTO things VALUES (?1, 'upserted', '0000000005000-0000-writer')
                     ON CONFLICT(id) DO UPDATE SET body = excluded.body, _updated_at = excluded._updated_at",
                    [replaced],
                )?;
                Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("ordinary update and same-id upsert succeed");

        let pending_before = db
            .call(|conn| {
                conn.prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
                    .and_then(|mut statement| {
                        statement
                            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .map_err(DbError::from)
            })
            .await
            .expect("read existing write records");
        assert_eq!(pending_before.len(), 3);

        let invalid_tables = tables;
        let invalid_write_id = db.new_write_id();
        let invalid = db
            .call(move |conn| {
                Database::run_internal_store_write_transaction_on(
                    conn,
                    &invalid_tables,
                    crate::WritePolicy::MergeConcurrent,
                    None,
                    invalid_write_id,
                    |tx| {
                        tx.execute(
                            "UPDATE things SET id = 'not-a-uuid', _updated_at = '0000000006000-0000-writer' WHERE id = ?1",
                            [replaced],
                        )?;
                        Ok::<_, DbError>(())
                    },
                )
            })
            .await;
        let error = invalid.expect_err("invalid new UUID rejects the primary-key change");
        assert!(error.to_string().contains("not-a-uuid"));

        db.call(move |conn| {
            let row = conn
                .query_row("SELECT id, body FROM things", [], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let pending_after = conn
                .prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(DbError::from)?;
            assert_eq!(row, (replaced.to_string(), "upserted".to_string()));
            assert_eq!(pending_after, pending_before);
            Ok(())
        })
        .await
        .expect("invalid identity change rolls back row and write records");
    }

    /// A SQL closure that blocks for a while must not stall other tasks on the
    /// same runtime, because jobs run on the dedicated connection thread rather
    /// than the async executor. On a current-thread runtime the scheduler has to
    /// poll the spawned DB call before it can resume us; if that call ran its
    /// blocking closure inline on the executor thread, this single `yield_now`
    /// would not return until the closure finished. With the closure on its own
    /// thread we resume immediately, long before it completes.
    #[tokio::test]
    async fn slow_db_call_does_not_block_the_executor() {
        use std::time::{Duration, Instant};

        let (db, _stamper) = Database::open(
            Path::new(":memory:"),
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "liveness".to_string(),
            &[],
        )
        .expect("open database");

        let slow_db = db.clone();
        let slow = tokio::spawn(async move {
            slow_db
                .call(|conn| {
                    std::thread::sleep(Duration::from_millis(500));
                    conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))
                        .map_err(DbError::from)
                })
                .await
        });

        let start = Instant::now();
        tokio::task::yield_now().await;
        let stalled = start.elapsed();

        assert!(
            stalled < Duration::from_millis(250),
            "unrelated task stalled {stalled:?} behind the slow DB call — the executor was blocked",
        );

        let value = slow
            .await
            .expect("slow DB task joins")
            .expect("slow DB call succeeds");
        assert_eq!(value, 1, "the slow DB call still returns its result");
    }

    /// Dropping the last handle from inside a runtime task must not block that
    /// task on the connection thread's queue, and a job already dispatched must
    /// still run to completion. The drop detaches the thread in async context, so
    /// it returns at once; the detached thread finishes the queued job (its effect
    /// is durable) and exits on its own.
    #[tokio::test]
    async fn dropping_last_handle_in_async_context_does_not_stall_but_job_still_lands() {
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("db.sqlite");
        let marker = dir.path().join("marker");

        let (db, _stamper) = Database::open(
            &db_path,
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "drop-async".to_string(),
            &[],
        )
        .expect("open");

        // Dispatch a slow job to the connection thread, then release the clone
        // tied to it so the job is in flight with no handle awaiting it: spawn the
        // call, let it dispatch, then abort the task so its `Database` clone drops
        // while the job still runs. The job writes a marker file when it finishes.
        let job_db = db.clone();
        let job_marker = marker.clone();
        let task = tokio::spawn(async move {
            let _ = job_db
                .call(move |_conn| {
                    std::thread::sleep(Duration::from_millis(300));
                    std::fs::write(&job_marker, b"landed")
                        .map_err(|e| DbError::Message(e.to_string()))
                })
                .await;
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        // `db` is now the last clone; dropping it inside this runtime task must
        // detach (not join) so it returns without waiting out the queued job.
        let drop_start = Instant::now();
        drop(db);
        let drop_elapsed = drop_start.elapsed();
        assert!(
            drop_elapsed < Duration::from_millis(200),
            "dropping the last handle stalled {drop_elapsed:?} — it joined the connection thread \
             instead of detaching",
        );

        // The detached thread still runs the already-dispatched job to completion.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !marker.exists() {
            assert!(
                Instant::now() < deadline,
                "the dispatched job's effect never landed after the last handle dropped",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn database_open_rejects_empty_device_id() {
        let result = Database::open(
            Path::new(":memory:"),
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            String::new(),
            &[],
        );
        let error = match result {
            Ok(_) => panic!("empty device_id must be rejected"),
            Err(error) => error.to_string(),
        };

        assert!(
            error.contains("device_id") && error.contains("empty"),
            "error names the empty device id: {error}",
        );
    }

    #[test]
    fn host_sql_authorizer_is_removed_after_success_error_and_panic() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "ATTACH ':memory:' AS coven_gate_empty; \
             CREATE TABLE coven_gate_empty.baseline (id TEXT PRIMARY KEY) STRICT; \
             INSERT INTO coven_gate_empty.baseline VALUES ('guarded');",
        )
        .expect("attach guarded schema");
        let assert_guard_removed = || {
            let id: String = conn
                .query_row("SELECT id FROM coven_gate_empty.baseline", [], |row| {
                    row.get(0)
                })
                .expect("internal SQL can address the baseline after host SQL");
            assert_eq!(id, "guarded");
        };

        Database::run_host_sql_on(&conn, || Ok::<_, DbError>(())).expect("successful host SQL");
        assert_guard_removed();

        let error =
            Database::run_host_sql_on(&conn, || Err::<(), _>(DbError::Message("host".into())));
        assert!(error.is_err());
        assert_guard_removed();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Database::run_host_sql_on(&conn, || -> Result<(), DbError> { panic!("host panic") })
                .expect("panicking host SQL closure never returns");
        }));
        assert!(panic.is_err());
        assert_guard_removed();
    }

    #[tokio::test]
    async fn database_open_rejects_host_declared_reserved_tables() {
        for table_name in ["cloud_outbox", "protocol_state"] {
            let result = Database::open(
                Path::new(":memory:"),
                vec![SyncedTable::new(
                    table_name,
                    crate::sync::session::RowIdentity::SharedKey,
                )],
                BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                format!("reserved-{table_name}"),
                &[notes_migration()],
            );
            let error = match result {
                Ok(_) => panic!("reserved table {table_name} must be rejected"),
                Err(error) => error.to_string(),
            };

            assert!(
                error.contains(table_name),
                "error names reserved table {table_name}: {error}",
            );
        }
    }

    #[tokio::test]
    async fn database_open_rejects_empty_synced_table_name() {
        let result = Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                "",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "empty-synced-table".to_string(),
            &[notes_migration()],
        );
        let error = match result {
            Ok(_) => panic!("empty synced table name must be rejected"),
            Err(error) => error.to_string(),
        };

        assert!(
            error.contains("empty"),
            "error names empty synced table: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_accepts_normal_host_synced_table() {
        Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                "notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "normal-synced-table".to_string(),
            &[notes_migration()],
        )
        .expect("normal host table opens");
    }

    /// Open with a single host migration and the given synced-table set, expecting
    /// the contract validation to refuse the open. Returns the error text so a test
    /// asserts it names the offending table and the violated requirement.
    fn open_contract_error(
        migration_sql: &'static str,
        tables: Vec<SyncedTable>,
        device_id: &str,
    ) -> String {
        let result = Database::open(
            Path::new(":memory:"),
            tables,
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            device_id.to_string(),
            &[Migration::sql(1, "contract", migration_sql)],
        );
        match result {
            Ok(_) => panic!("open must reject the synced-table contract violation"),
            Err(error) => error.to_string(),
        }
    }

    #[tokio::test]
    async fn database_open_rejects_integer_primary_key() {
        let error = open_contract_error(
            "CREATE TABLE things (id INTEGER PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "integer-pk",
        );
        assert!(
            error.contains("things") && error.contains("TEXT"),
            "error names the table and the TEXT requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_primary_key_not_at_column_zero() {
        let error = open_contract_error(
            "CREATE TABLE things (body TEXT NOT NULL, id TEXT PRIMARY KEY, \
             _updated_at TEXT NOT NULL) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "pk-not-first",
        );
        assert!(
            error.contains("things") && error.contains("column 0"),
            "error names the table and the column-0 requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_primary_key_named_other_than_id() {
        let error = open_contract_error(
            "CREATE TABLE things (thing_id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "pk-misnamed",
        );
        assert!(
            error.contains("things") && error.contains("`id`"),
            "error names the table and the `id` requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_composite_primary_key() {
        let error = open_contract_error(
            "CREATE TABLE things (id TEXT NOT NULL, part TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, PRIMARY KEY (id, part)) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "composite-pk",
        );
        assert!(
            error.contains("things") && error.contains("composite"),
            "error names the table and the single-primary-key requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_nullable_updated_at() {
        let error = open_contract_error(
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "nullable-updated-at",
        );
        assert!(
            error.contains("things") && error.contains("_updated_at"),
            "error names the table and the `_updated_at` requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_non_strict_synced_table() {
        let error = open_contract_error(
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL);",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "non-strict",
        );
        assert!(
            error.contains("things") && error.contains("STRICT"),
            "error names the table and the STRICT requirement: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_declared_table_no_migration_creates() {
        let error = open_contract_error(
            "CREATE TABLE other (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "declared-never-created",
        );
        assert!(
            error.contains("things") && error.contains("no migration creates it"),
            "error says the declared table was never created: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_synced_table_spelling_that_differs_from_live_schema() {
        let error = open_contract_error(
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            vec![SyncedTable::new(
                "Things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            "case-variant-table-name",
        );
        assert!(
            error.contains("Things") && error.contains("things") && error.contains("exact"),
            "error names both spellings and requires the live spelling: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_rejects_case_variant_duplicate_synced_tables() {
        let error = open_contract_error(
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            vec![
                SyncedTable::new("things", crate::sync::session::RowIdentity::SharedKey),
                SyncedTable::new("THINGS", crate::sync::session::RowIdentity::IndependentUuid),
            ],
            "case-variant-duplicate-table",
        );
        assert!(
            error.contains("things")
                && error.contains("THINGS")
                && error.contains("more than once"),
            "error names both duplicate declarations: {error}",
        );
    }

    #[tokio::test]
    async fn database_open_accepts_strict_synced_table() {
        Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "strict-synced-table".to_string(),
            &[Migration::sql(
                1,
                "contract",
                "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
            )],
        )
        .expect("a STRICT synced table satisfying the rest of the contract opens");
    }

    #[tokio::test]
    async fn database_open_ignores_undeclared_non_strict_local_table() {
        // `things` is declared and STRICT; `scratch` is a host-local table never
        // passed to `synced_tables` — its own business, not coven's, so it stays
        // non-strict with no open error.
        Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                "things",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "undeclared-local-table".to_string(),
            &[Migration::sql(
                1,
                "contract",
                "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT; \
                 CREATE TABLE scratch (id INTEGER PRIMARY KEY, note TEXT);",
            )],
        )
        .expect("an undeclared non-strict local table is not coven's business");
    }

    #[tokio::test]
    async fn database_open_rejects_duplicate_blob_namespace() {
        let blob = |namespace| {
            crate::sync::session::BlobDecl::new(
                namespace,
                crate::blob::Provenance::HostProvided,
                crate::blob::CacheFill::CacheLazy,
            )
        };
        let error = open_contract_error(
            "CREATE TABLE covers (id TEXT PRIMARY KEY, size INTEGER NOT NULL, \
             hash TEXT, _updated_at TEXT NOT NULL) STRICT;\
             CREATE TABLE thumbs (id TEXT PRIMARY KEY, size INTEGER NOT NULL, \
             hash TEXT, _updated_at TEXT NOT NULL) STRICT;",
            vec![
                SyncedTable::new("covers", crate::sync::session::RowIdentity::SharedKey)
                    .carries_blob(blob("images")),
                SyncedTable::new("thumbs", crate::sync::session::RowIdentity::SharedKey)
                    .carries_blob(blob("images")),
            ],
            "dup-namespace",
        );
        assert!(
            error.contains("covers") && error.contains("thumbs") && error.contains("images"),
            "error names both tables and the shared blob namespace: {error}",
        );
    }

    #[tokio::test]
    async fn fresh_open_creates_canonical_make_remote_intent_retain_pinned_column() {
        let (db, _stamper) = Database::open(
            Path::new(":memory:"),
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "test-device".to_string(),
            &[],
        )
        .expect("open database");

        let column = db
            .call(|conn| {
                let mut stmt = conn
                    .prepare("PRAGMA table_info(blob_make_remote_intents)")
                    .map_err(DbError::from)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    })
                    .map_err(DbError::from)?;
                for row in rows {
                    let (name, notnull, default_value) = row.map_err(DbError::from)?;
                    if name == "retain_pinned" {
                        return Ok(Some((notnull, default_value)));
                    }
                }
                Ok(None)
            })
            .await
            .expect("read make_remote intent schema")
            .expect("retain_pinned column exists");

        assert_eq!(column.0, 1, "retain_pinned must be NOT NULL");
        assert_eq!(
            column.1.as_deref(),
            Some("0"),
            "retain_pinned must default to 0",
        );
    }

    #[tokio::test]
    async fn serial_pending_branch_survives_reopen_with_exact_base_and_inverses() {
        let temp = tempfile::tempdir().expect("temporary Store");
        let path = temp.path().join("serial.db");
        let tables = vec![SyncedTable::new(
            "notes",
            crate::sync::session::RowIdentity::SharedKey,
        )];
        let migrations = vec![notes_migration()];
        let (db, _) = Database::open(
            &path,
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "serial-device".to_string(),
            &migrations,
        )
        .expect("open serial Store");
        for (write_id, sql) in [
            (
                "serial-write-1",
                "INSERT INTO notes VALUES ('n1', 'first', '0000000001000-0000-serial')",
            ),
            (
                "serial-write-2",
                "UPDATE notes SET body = 'second', _updated_at = '0000000002000-0000-serial' WHERE id = 'n1'",
            ),
        ] {
            let tables = tables.clone();
            let write_id = WriteId::from_generated(write_id.to_string());
            db.call(move |conn| {
                Database::run_internal_store_write_transaction_on(
                    conn,
                    &tables,
                    crate::WritePolicy::Serial,
                    None,
                    write_id,
                    |tx| tx.execute_batch(sql).map_err(DbError::from),
                )
            })
            .await
            .expect("commit provisional serial write");
        }
        drop(db);

        let (reopened, _) = Database::open(
            &path,
            tables,
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "serial-device".to_string(),
            &migrations,
        )
        .expect("reopen serial Store");
        let rows = reopened
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT write_id, inverse_changeset, base
                         FROM store_writes ORDER BY ordinal",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(rows)
            })
            .await
            .expect("read reopened branch");
        assert_eq!(rows.len(), 2);
        for (write_id, inverse, base) in rows {
            assert!(!inverse.is_empty(), "{write_id} retains its inverse");
            let base: StoreWriteBase = serde_json::from_str(&base).expect("serial base");
            assert_eq!(
                base,
                StoreWriteBase::Serial {
                    branch_id: PendingBranchId::from_first_write(WriteId::from_generated(
                        "serial-write-1".to_string(),
                    )),
                    base: None,
                }
            );
        }
    }

    const RESTART_CIRCLE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn restart_circle_coord(policy: WritePolicy) -> String {
        match policy {
            WritePolicy::MergeConcurrent => serde_json::json!({
                "merge_concurrent": {
                    "device_id": "restart-control-device",
                    "author_pubkey": "restart-owner",
                    "author_owner_grant": "11".repeat(32),
                    "seq": 1,
                    "control_hash": "22".repeat(32)
                }
            }),
            WritePolicy::Serial => serde_json::json!({
                "serial": {
                    "author_pubkey": "restart-owner",
                    "generation": 1,
                    "control_hash": "33".repeat(32)
                }
            }),
        }
        .to_string()
    }

    async fn capture_scoped_write_then_reopen(
        policy: WritePolicy,
        name: &str,
    ) -> (
        tempfile::TempDir,
        Database,
        Vec<(String, Option<String>, Vec<u8>)>,
    ) {
        let temp = tempfile::tempdir().expect("temporary scoped Store");
        let path = temp.path().join(format!("{name}.db"));
        let tables =
            vec![
                SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
                    .scoped_by("audience"),
            ];
        let migrations = vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )];
        let (db, _) = Database::open(
            &path,
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            policy,
            format!("{name}-device"),
            &migrations,
        )
        .expect("open scoped Store");
        let control = restart_circle_coord(policy);
        db.call(move |conn| {
            conn.execute(
                "INSERT INTO protocol_state (key, value)
                 VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (
                    STORE_ROOT_HASH_STATE_KEY,
                    ObjectHash::digest(b"restart-scoped-root").to_string(),
                ),
            )
            .map_err(DbError::from)?;
            conn.execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, 'restart-control-device', 1, ?3, X'01')",
                (RESTART_CIRCLE_ID, &control, "44".repeat(32)),
            )
            .map_err(DbError::from)?;
            conn.execute(
                "INSERT INTO circle_access_cache
                 (circle_id, control_coord, owner_pubkey, disposition, access_bytes)
                 VALUES (?1, ?2, 'restart-owner', 'active', X'02')",
                (RESTART_CIRCLE_ID, &control),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("seed active Circle authority");

        let gates = db.gates();
        let blob_decls = db.blob_decls();
        let write_id = db.new_write_id();
        let routing =
            (policy == WritePolicy::MergeConcurrent).then(|| EncryptionService::from_key([7; 32]));
        let capture_tables = tables.clone();
        db.call(move |conn| {
            Database::run_store_write_transaction_on(
                conn,
                &capture_tables,
                &gates,
                &blob_decls,
                policy,
                routing.as_ref(),
                write_id,
                |tx| {
                    tx.execute(
                        "INSERT INTO accounts (id, audience, _updated_at)
                         VALUES ('store-account', NULL, '0000000001000-0000-restart')",
                        [],
                    )?;
                    tx.execute(
                        "INSERT INTO accounts (id, audience, _updated_at)
                         VALUES ('circle-account', ?1, '0000000001001-0000-restart')",
                        [RESTART_CIRCLE_ID],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("capture Store and Circle partitions");
        let expected = db
            .call(|conn| {
                conn.prepare(
                    "SELECT audience, control_coord, changeset
                     FROM store_write_partitions
                     ORDER BY CASE audience WHEN 'store' THEN 0 ELSE 1 END,
                              audience, control_coord",
                )
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                            ))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(DbError::from)
            })
            .await
            .expect("read exact persisted audience partitions");
        assert_eq!(expected.len(), 2);
        drop(db);

        let (reopened, _) = Database::open(
            &path,
            tables,
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            policy,
            format!("{name}-device"),
            &migrations,
        )
        .expect("reopen scoped Store");
        (temp, reopened, expected)
    }

    fn assert_prepared_partitions(
        actual: &PreparedStoreWritePartitions,
        expected: &[(String, Option<String>, Vec<u8>)],
    ) {
        let actual = actual
            .iter()
            .map(|partition| {
                let audience = match partition.audience {
                    crate::sync::circle::Audience::Store => "store".to_string(),
                    crate::sync::circle::Audience::Circle(circle) => circle.to_string(),
                    crate::sync::circle::Audience::Local => {
                        panic!("Local audience entered Store preparation")
                    }
                };
                (
                    audience,
                    partition.control.as_ref().map(|control| {
                        let parsed = serde_json::from_str(control.stored_json())
                            .expect("parse stored control");
                        assert_eq!(control.coordinate(), &parsed);
                        control.stored_json().to_string()
                    }),
                    partition.changeset.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn merge_preparation_reloads_exact_scoped_partitions_after_restart() {
        let (_temp, reopened, expected) =
            capture_scoped_write_then_reopen(WritePolicy::MergeConcurrent, "merge-restart").await;
        let prepared = reopened
            .prepare_store_write()
            .await
            .expect("prepare restarted Merge write")
            .expect("pending Merge write");

        assert_prepared_partitions(&prepared.partitions, &expected);
    }

    #[tokio::test]
    async fn serial_preparation_reloads_exact_scoped_partitions_after_restart() {
        let (_temp, reopened, expected) =
            capture_scoped_write_then_reopen(WritePolicy::Serial, "serial-restart").await;
        let branch = reopened
            .reserve_serial_store_branch()
            .await
            .expect("reserve restarted Serial branch")
            .expect("pending Serial branch");
        assert_eq!(branch.writes.len(), 1);

        assert_prepared_partitions(&branch.writes[0].partitions, &expected);
    }
}

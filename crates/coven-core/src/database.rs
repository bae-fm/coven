//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with the sync bookkeeping
//! beside it. Every database access — the host's app SQL, coven's bookkeeping,
//! changeset capture and apply — runs against that one connection, so access is
//! serialized.
//!
//! Native hosts open coven with [`crate::Coven::builder`] and run app SQL through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`]. Platform crates
//! choose how a SQLite connection is opened and held.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::debug;
// `error!` fires only from the native connection thread's shutdown path.
#[cfg(not(target_arch = "wasm32"))]
use tracing::error;

use crate::blob::decl::BlobDecls;
use crate::db::{
    apply_coven_schema, is_reserved_table_name, ExternalBlob, OutboxEntry, OutboxOperation,
};
use crate::migration::{run_migrations, Migration, MigrationError};
use crate::sync::gate::{self, Gates};
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY, MAX_FUTURE_SKEW_MS};
use crate::sync::session::SyncedTable;

/// An error from the owned database.
#[derive(Debug, thiserror::Error)]
#[error("database error: {0}")]
pub struct DbError(pub String);

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError(e.to_string())
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
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    /// The host's blob-tombstone convergence window, read by the tombstone GC to
    /// age each tombstone's `deleted_at`. Host config carried here alongside
    /// `synced_tables` so the sync layer reads it from the one owner rather than
    /// threading a separately-passed copy that could diverge.
    blob_tombstone_grace: chrono::Duration,
}

/// The owned SQLite connection and the sync bookkeeping resolved beside it at
/// open. One connection thread owns this for the connection's whole life; every
/// database access runs against it, so access is serialized. Changeset capture is
/// per-transaction — [`Database::run_pending_journaled_transaction_on`] attaches a
/// session for the span of one host write and drains it into the pending journal —
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
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
}

impl DatabaseCore {
    fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Self, DatabaseState, UpdatedAtStamper), OpenError> {
        let conn = open_connection(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

        // coven's bookkeeping tables and the coven-owned `item_keys` synced table
        // first (declarative, idempotent, run every open), then the host's synced
        // schema ladder over `PRAGMA user_version`. The two are kept separate by
        // sync-visibility: coven's bookkeeping rows are stripped on snapshot, so a
        // versioned ledger can't track them; the host's synced ladder version rides
        // inside the snapshot's DB header.
        apply_coven_schema(&conn).map_err(DbError::from)?;
        migrate_bookkeeping_schema(&conn)?;
        let schema_version = run_migrations(&conn, migrations)?;

        // Enforce the synced-table contract against the live host schema, before
        // `item_keys` is appended below: each host table must present the pk shape
        // and `_updated_at` the capture/gate/apply paths assume. Validating here —
        // after the migration ladder built the tables, on the integrator's own
        // device — turns a wrong shape into an open error naming the table, rather
        // than a peer's cursor wedging forever at pull time. `item_keys` is coven's
        // own and keyed by `item_id`, so it is excluded by running before the push.
        validate_host_synced_tables(&conn, &synced_tables)?;

        // `item_keys` is coven-owned but is library-global content every member
        // needs, so coven — not the host — declares it synced. Injecting it here
        // is the single point that puts it on every path that reads the set: each
        // journaled write's capture session attaches it, the snapshot preserves it,
        // and the gate and apply operate over it. The host never sees or declares
        // it. A coven-reserved table name the host cannot declare, so this appends
        // unconditionally.
        let mut synced_tables = synced_tables;
        synced_tables.push(SyncedTable::new(crate::db::ITEM_KEYS_TABLE));

        // Seed the register clock so a restart cannot mint a stamp behind a value
        // already on disk. Floor = max(persisted high-water, max synced-row
        // `_updated_at`).
        let persisted = conn
            .query_row(
                "SELECT value FROM sync_state WHERE key = ?1",
                [HIGHWATER_STATE_KEY],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?;
        seed_from(&hlc, persisted, "HLC high-water mark in sync_state")?;
        let seed_wall_ms = hlc.wall_now_ms();
        let seed_bound_ms = seed_wall_ms.saturating_add(MAX_FUTURE_SKEW_MS);
        let on_disk = scan_max_updated_at(&conn, &synced_tables, seed_bound_ms)?;
        seed_from(&hlc, on_disk, "`_updated_at` in synced tables")?;

        let stamper = UpdatedAtStamper::new(hlc.clone());
        let synced_tables = Arc::new(synced_tables);
        let gates = Arc::new(
            Gates::from_tables(&conn, &synced_tables).map_err(|e| DbError(e.to_string()))?,
        );
        let blob_decls = Arc::new(
            BlobDecls::from_tables(&conn, &synced_tables).map_err(|e| DbError(e.to_string()))?,
        );
        let core = DatabaseCore {
            conn,
            hlc,
            synced_tables,
            schema_version,
            gates,
            blob_decls,
            blob_tombstone_grace,
        };
        let state = core.state();

        Ok((core, state, stamper))
    }

    fn state(&self) -> DatabaseState {
        DatabaseState {
            hlc: self.hlc.clone(),
            synced_tables: self.synced_tables.clone(),
            schema_version: self.schema_version,
            gates: self.gates.clone(),
            blob_decls: self.blob_decls.clone(),
            blob_tombstone_grace: self.blob_tombstone_grace,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn connection(&self) -> &Connection {
        &self.conn
    }

    #[cfg(target_arch = "wasm32")]
    fn with_connection<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        f(&self.conn)
    }
}

/// A unit of work for the connection thread: a caller's closure to run against
/// the owned core, or the sentinel the last [`Database`] clone sends as it drops
/// to stop the thread. A `Run` closure carries its own reply channel, so it
/// returns `()` — [`Database::on_connection_thread`] builds it to capture the
/// caller's result and send it back.
#[cfg(not(target_arch = "wasm32"))]
enum DbJob {
    Run(Box<dyn FnOnce(&mut DatabaseCore) + Send>),
    Stop,
}

/// The connection thread's send channel and join handle, shared by every
/// [`Database`] clone through an `Arc`. The last clone to drop shuts the thread
/// down and joins it, so the connection closes on the thread that owned it before
/// control returns to the dropper.
#[cfg(not(target_arch = "wasm32"))]
struct ConnectionThread {
    jobs: tokio::sync::mpsc::UnboundedSender<DbJob>,
    join: Option<std::thread::JoinHandle<()>>,
}

#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Database {
    thread: Arc<ConnectionThread>,
    state: DatabaseState,
}

/// A handle to the owned database. Cloneable; every clone shares the one
/// connection through the same `Rc`.
///
/// The browser is single-threaded (the connection lives on one Worker), so this
/// holds the core directly behind `Rc<RefCell<…>>` instead of talking to a
/// thread.
#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct Database {
    core: std::rc::Rc<std::cell::RefCell<DatabaseCore>>,
    state: DatabaseState,
}

pub(crate) enum PendingChangesetBatch {
    Empty,
    Pending { max_id: i64, changeset: Vec<u8> },
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
    for table in synced_tables {
        let name = table.name();
        if name.is_empty() {
            return Err(DbError("synced table name must not be empty".to_string()));
        }
        if is_reserved_table_name(name) {
            return Err(DbError(format!(
                "synced table {name:?} is reserved by coven"
            )));
        }
        validate_synced_table_contract(conn, name)?;
        if let Some(decl) = table.blob() {
            let namespace = decl.namespace.as_str();
            if let Some(prior) = namespace_owner.insert(namespace, name) {
                return Err(DbError(format!(
                    "synced tables {prior:?} and {name:?} both declare blob namespace \
                     {namespace:?}; a namespace must be owned by exactly one table"
                )));
            }
        }
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
/// `table`'s live schema: a single primary key column, named `id`, declared TEXT,
/// at column 0; and an `_updated_at` column declared TEXT NOT NULL. A violation is
/// an open error naming the table and the requirement it broke, so the integrator
/// learns it on their own device instead of a peer's pull failing on the row.
fn validate_synced_table_contract(conn: &Connection, table: &str) -> Result<(), DbError> {
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
            return Err(DbError(format!(
                "synced table {table:?} has no primary key; the contract requires a single \
                 `id` TEXT primary key at column 0"
            )))
        }
        _ => {
            let names: Vec<&str> = pk_columns.iter().map(|c| c.name.as_str()).collect();
            return Err(DbError(format!(
                "synced table {table:?} has a composite primary key {names:?}; the contract \
                 requires a single `id` TEXT primary key at column 0"
            )));
        }
    };
    if pk.name != "id" {
        return Err(DbError(format!(
            "synced table {table:?} primary key is {:?}, not `id`; the contract requires the \
             primary key to be the `id` column",
            pk.name
        )));
    }
    if pk.position != 0 {
        return Err(DbError(format!(
            "synced table {table:?} primary key `id` is at column {}, not column 0; the \
             contract requires `id` to be the first column",
            pk.position
        )));
    }
    if !declared_as_text(&pk.declared_type) {
        return Err(DbError(format!(
            "synced table {table:?} primary key `id` is declared {:?}, not TEXT; the contract \
             requires an `id` TEXT primary key",
            pk.declared_type
        )));
    }

    let updated_at = columns
        .iter()
        .find(|c| c.name == "_updated_at")
        .ok_or_else(|| {
            DbError(format!(
                "synced table {table:?} has no `_updated_at` column; the contract requires \
                 `_updated_at TEXT NOT NULL`"
            ))
        })?;
    if !declared_as_text(&updated_at.declared_type) {
        return Err(DbError(format!(
            "synced table {table:?} column `_updated_at` is declared {:?}, not TEXT; the \
             contract requires `_updated_at TEXT NOT NULL`",
            updated_at.declared_type
        )));
    }
    if !updated_at.not_null {
        return Err(DbError(format!(
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

impl Database {
    /// Open and own the connection at `path`.
    ///
    /// Runs coven's bookkeeping migration, then the host's synced-schema migration
    /// ladder (`migrations`) over `PRAGMA user_version`, and seeds the register
    /// clock off the on-disk rows. Returns the handle plus the non-optional
    /// `_updated_at` stamper the host binds into every synced-row write.
    ///
    pub fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let hlc = Hlc::try_new(device_id).map_err(|e| DbError(format!("device_id {e}")))?;
        Self::open_with_hlc(
            path,
            synced_tables,
            blob_tombstone_grace,
            Arc::new(hlc),
            migrations,
        )
    }

    /// Open with a caller-supplied register clock instead of a fresh
    /// system-wall-clock one. Lets a test inject an [`Hlc`] over a controlled
    /// wall clock to exercise the skew/restart-seeding guarantees, sharing the
    /// production open path (migration, seed, session) so the test drives the
    /// real unit.
    ///
    pub(crate) fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let (core, state, stamper) =
            DatabaseCore::open(path, synced_tables, blob_tombstone_grace, hlc, migrations)?;

        #[cfg(not(target_arch = "wasm32"))]
        let database = {
            let (jobs_tx, jobs_rx) = tokio::sync::mpsc::unbounded_channel::<DbJob>();
            let join = std::thread::Builder::new()
                .name("coven-db".to_string())
                .spawn(move || run_connection_thread(core, jobs_rx))
                .map_err(|e| DbError(format!("spawn database connection thread: {e}")))?;
            Database {
                thread: Arc::new(ConnectionThread {
                    jobs: jobs_tx,
                    join: Some(join),
                }),
                state,
            }
        };

        #[cfg(target_arch = "wasm32")]
        let database = Database {
            core: std::rc::Rc::new(std::cell::RefCell::new(core)),
            state,
        };

        Ok((database, stamper))
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

    /// The gate model resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    pub(crate) fn gates(&self) -> Arc<Gates> {
        self.state.gates.clone()
    }

    /// Blob declarations resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    pub(crate) fn blob_decls(&self) -> Arc<BlobDecls> {
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
    /// [`Self::run_pending_journaled_transaction_on`] transaction (still through
    /// `call`) so it lands in the pending-changeset journal.
    ///
    /// Native hands `f` to the connection thread and awaits its reply, so the SQL
    /// runs off the async executor; wasm runs `f` directly on the borrowed
    /// connection on this Worker and returns a ready future, so it needs no `Send`
    /// — the closure never leaves the thread.
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
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

    /// See the native `call`. wasm borrows the connection, runs `f`, drops the
    /// borrow, and returns a ready future — the borrow never spans an `.await`.
    #[cfg(target_arch = "wasm32")]
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + 'static,
        R: 'static,
    {
        let core = self.core.borrow();
        core.with_connection(f)
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

    fn insert_pending_changeset_on(
        tx: &rusqlite::Transaction<'_>,
        changeset: &[u8],
    ) -> Result<(), DbError> {
        if changeset.is_empty() {
            debug!("journaled transaction produced no synced changes");
            return Ok(());
        }
        tx.execute(
            "INSERT INTO pending_changesets (changeset) VALUES (?1)",
            [changeset],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    pub fn run_pending_journaled_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        let mut journal =
            Self::start_host_change_journal_on(conn, synced_tables).map_err(E::from)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(DbError::from)
            .map_err(E::from)?;
        let value = f(&tx)?;
        let changeset = Self::drain_host_change_journal_on(&mut journal).map_err(E::from)?;
        Self::insert_pending_changeset_on(&tx, &changeset).map_err(E::from)?;
        tx.commit().map_err(DbError::from).map_err(E::from)?;
        Ok(value)
    }

    pub(crate) async fn pending_changeset_batch(&self) -> Result<PendingChangesetBatch, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare("SELECT id, changeset FROM pending_changesets ORDER BY id")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(DbError::from)?;
            let mut max_id = None;
            let mut changesets = Vec::new();
            for row in rows {
                let (id, changeset) = row.map_err(DbError::from)?;
                max_id = Some(id);
                changesets.push(changeset);
            }
            if changesets.iter().any(Vec::is_empty) {
                return Err(DbError(
                    "pending_changesets contains an empty changeset".to_string(),
                ));
            }
            let changeset = match changesets.as_slice() {
                [] => return Ok(PendingChangesetBatch::Empty),
                [single] => single.clone(),
                _ => gate::combine_changesets(conn, &changesets)
                    .map_err(|e| DbError(format!("combine pending changesets: {e}")))?,
            };
            let max_id = max_id.expect("non-empty pending changeset batch has a max id");
            Ok(PendingChangesetBatch::Pending { max_id, changeset })
        })
        .await
    }

    pub(crate) async fn clear_pending_changesets_through(
        &self,
        max_id: i64,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            conn.execute("DELETE FROM pending_changesets WHERE id <= ?1", [max_id])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    // ---- Bookkeeping: sync_state ----

    pub(crate) async fn get_sync_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.query_row("SELECT value FROM sync_state WHERE key = ?1", [key], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO sync_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
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
    /// `sync_state` (config, not per-blob accounting — the cache's truth is still the
    /// folder on disk).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        match self.get_sync_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|e| {
                DbError(format!(
                    "cache budget for {namespace:?} in sync_state is not a byte count: {e}"
                ))
            }),
            None => Ok(None),
        }
    }

    /// Set `namespace`'s device-local cache-size budget in bytes. Once set, a populate
    /// into that namespace's cache that pushes `storage/cache/<namespace>/` over this
    /// total evicts its oldest files (by mtime) back under it; `pinned/` is never
    /// counted or touched, and another namespace's files are never walked. Stored
    /// under [`crate::blob::cache::cache_budget_state_key`] in `sync_state`.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        self.set_sync_state(&key, &max_bytes.to_string()).await
    }

    // ---- Bookkeeping: blob_uploaders (which device uploaded a blob) ----

    /// The hex public key of the device that uploaded blob `(namespace, id)`, or
    /// `None` if this device has never recorded one. The read dispatch consults it
    /// to key a blob under its uploader's prefix; a `None` sends the read to the
    /// listing-scan fallback ([`SyncStorage::find_blob_uploader`]).
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
    /// no-op; a later, authoritative uploader (a re-upload, or a scan that found
    /// the object) overwrites.
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

    /// Record a discovered uploader outside any caller transaction — used when a
    /// read miss's listing scan finds which prefix holds a blob, so the next read
    /// skips the scan. Recording a fact just observed, not repairing wrong state.
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

    /// Forget blob `(namespace, id)`'s recorded uploader. The read path calls this
    /// when the recorded prefix returns NotFound — its copy was legitimately
    /// reclaimed — so a fresh listing scan can find another member's surviving copy
    /// and record it. A no-op when no record exists.
    pub(crate) async fn forget_blob_uploader(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<(), DbError> {
        let (namespace, id) = (namespace.to_string(), id.to_string());
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM blob_uploaders WHERE namespace = ?1 AND blob_id = ?2",
                (namespace, id),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    // ---- Bookkeeping: sync_cursors ----

    pub(crate) async fn get_all_sync_cursors(&self) -> Result<HashMap<String, u64>, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare("SELECT device_id, last_seq FROM sync_cursors")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
                })
                .map_err(DbError::from)?;
            let mut out = HashMap::new();
            for row in rows {
                let (id, seq) = row.map_err(DbError::from)?;
                out.insert(id, seq);
            }
            Ok(out)
        })
        .await
    }

    pub async fn set_sync_cursor(&self, device_id: &str, seq: u64) -> Result<(), DbError> {
        let device_id = device_id.to_string();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO sync_cursors (device_id, last_seq) VALUES (?1, ?2) \
                 ON CONFLICT(device_id) DO UPDATE SET last_seq = excluded.last_seq",
                (device_id, seq as i64),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    // ---- Item keys ----

    /// Mint a random per-item content key and store it in the synced `item_keys`
    /// table, keyed by `item_id`. Idempotent: a no-op if the item already has a
    /// key (a re-mint must not rotate it out from under blobs already encrypted
    /// under it).
    ///
    /// The INSERT runs inside a
    /// [`Self::run_pending_journaled_transaction_on`] transaction, so it lands in
    /// the pending-changeset journal a running cycle pushes from and it replays to
    /// every member; the `_updated_at` HLC stamp satisfies the synced-table
    /// contract. Because `item_keys` is in the synced set, the row also survives a
    /// snapshot bootstrap. Returns the item's key (the freshly minted one, or the
    /// existing one if it was already present).
    ///
    /// `item_id` must be unique at creation. coven does not coordinate two devices
    /// independently minting the SAME `item_id` while offline: they would generate
    /// different random keys, last-writer-wins on the `item_keys` row keeps one,
    /// and any blobs the loser already encrypted under its key become
    /// undecryptable. Hosts avoid this by minting at item-creation time, where the
    /// id is freshly allocated and the item does not yet exist on another device.
    pub async fn mint_item_key(&self, item_id: &str) -> Result<[u8; 32], DbError> {
        let item_id = item_id.to_string();
        let new_key = crate::encryption::generate_random_key();
        let updated_at = self.state.hlc.now().to_string();
        let tables = self.synced_tables().to_vec();
        self.call(move |conn| {
            Self::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                Self::mint_item_key_on(tx, &item_id, new_key, &updated_at)
            })
        })
        .await
    }

    /// Transaction-composable form of [`mint_item_key`](Self::mint_item_key):
    /// runs on a connection the host already holds inside a
    /// [`call`](Self::call) closure, so the host can mint an item's key
    /// atomically with the item's own rows. The caller supplies the random
    /// key ([`crate::encryption::generate_random_key`]) and the `_updated_at`
    /// stamp (the host's [`crate::sync::hlc::UpdatedAtStamper`], which shares
    /// this database's HLC register).
    pub fn mint_item_key_on(
        conn: &Connection,
        item_id: &str,
        new_key: [u8; 32],
        updated_at: &str,
    ) -> Result<[u8; 32], DbError> {
        conn.execute(
            "INSERT OR IGNORE INTO item_keys (item_id, key, _updated_at) \
             VALUES (?1, ?2, ?3)",
            (item_id, new_key.to_vec(), updated_at),
        )
        .map_err(DbError::from)?;
        // Read back the stored key — `INSERT OR IGNORE` keeps the existing
        // row, so on a re-mint this returns the original key, not `new_key`.
        read_item_key(conn, item_id)?
            .ok_or_else(|| DbError(format!("item_keys row absent after mint for {item_id}")))
    }

    /// The content key for `item_id` from the synced `item_keys` table, or `None`
    /// if no key has been minted for it. A local SELECT — the row arrives via the
    /// changeset (for members) or the snapshot (for a bootstrapped joiner).
    pub async fn item_key(&self, item_id: &str) -> Result<Option<[u8; 32]>, DbError> {
        let item_id = item_id.to_string();
        self.call(move |conn| read_item_key(conn, &item_id)).await
    }

    /// Resolve a public [`crate::blob::BlobScope`] to the internal
    /// [`crate::blob::ResolvedScope`] storage and encryption consume. `Master`
    /// and `Derived` pass through unchanged; `Item(id)` looks up the item's key
    /// in `item_keys`. This is the single resolution point shared by the three
    /// blob paths (changeset push, changeset pull, outbox drain).
    ///
    /// A missing `item_keys` row is a host bug — the host must mint the key (and
    /// let it sync) before tagging a blob with that item. coven surfaces it as an
    /// error rather than silently falling back to the master key, which would
    /// encrypt the blob so no share recipient could ever read it.
    pub async fn resolve_blob_scope(
        &self,
        scope: crate::blob::BlobScope,
    ) -> Result<crate::blob::ResolvedScope, DbError> {
        use crate::blob::{BlobScope, ResolvedScope};
        match scope {
            BlobScope::Master => Ok(ResolvedScope::Master),
            BlobScope::Derived(s) => Ok(ResolvedScope::Derived(s)),
            BlobScope::Item(item_id) => match self.item_key(&item_id).await? {
                Some(key) => Ok(ResolvedScope::Key(key)),
                None => Err(DbError(format!(
                    "no item key for {item_id:?}: a blob was scoped to an item with no minted key \
                     (mint_item_key must run and sync before tagging the blob)"
                ))),
            },
        }
    }

    // ---- Cloud outbox ----

    /// Enqueue a blob upload. `scope` names which key the blob is encrypted
    /// under (master, a derived scope, or a coven-managed item); coven persists
    /// it on the row and resolves it to a key at drain — looking up the
    /// `item_keys` row for an [`crate::blob::BlobScope::Item`] scope — long after
    /// the enqueue site is gone. Idempotent on `(operation, cloud_key)`. Queuing an
    /// upload also cancels any pending delete of the same key — latest intent wins,
    /// so a re-upload isn't tombstoned in the same cycle.
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
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox \
             (operation, file_id, cloud_key, source_path, scope, retain_pinned, created_at) \
             VALUES ('upload', ?1, ?2, ?3, ?4, ?5, ?6)",
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

    /// Record a failed delete/tombstone attempt (bumps `attempt_count`, stores the
    /// error and the time). Scoped to delete rows so an id collision with another
    /// operation cannot mutate the wrong kind of outbox entry.
    pub async fn record_cloud_delete_failure(
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
                 WHERE id = ?3 AND operation = 'delete'",
                (error, attempted_at, id),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
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
            DbError(format!(
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

/// Read the 32-byte content key for `item_id` from `item_keys`, or `None` if no
/// row exists. A stored key of the wrong length is a corrupt DB and surfaces as a
/// [`DbError`] — `mint_item_key` only ever writes 32 bytes, so this names the
/// offending row rather than failing later as an opaque decrypt error.
fn read_item_key(conn: &Connection, item_id: &str) -> Result<Option<[u8; 32]>, DbError> {
    let stored: Option<Vec<u8>> = conn
        .query_row(
            "SELECT key FROM item_keys WHERE item_id = ?1",
            [item_id],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(DbError::from)?;
    match stored {
        None => Ok(None),
        Some(bytes) => {
            let len = bytes.len();
            bytes.try_into().map(Some).map_err(|_| {
                DbError(format!(
                    "item_keys.key for {item_id} is {len} bytes, not 32"
                ))
            })
        }
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
            .ok_or_else(|| DbError(format!("corrupt {context}: {stamp:?}")))?;
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
                DbError(format!(
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
        .map_err(|e| DbError(format!("failed to create capture session: {e}")))?;
    for t in synced_tables {
        session.attach(Some(t.name())).map_err(|e| {
            DbError(format!(
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

pub type PlatformConnectionOpener = fn(&Path) -> Result<Connection, DbError>;

#[cfg(not(target_arch = "wasm32"))]
static PLATFORM_CONNECTION_OPENER: std::sync::OnceLock<PlatformConnectionOpener> =
    std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static PLATFORM_CONNECTION_OPENER: std::cell::Cell<Option<PlatformConnectionOpener>> =
        std::cell::Cell::new(None);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn register_platform_connection_opener(opener: PlatformConnectionOpener) {
    let _ = PLATFORM_CONNECTION_OPENER.set(opener);
}

#[cfg(target_arch = "wasm32")]
pub fn register_platform_connection_opener(opener: PlatformConnectionOpener) {
    PLATFORM_CONNECTION_OPENER.with(|slot| slot.set(Some(opener)));
}

#[cfg(not(target_arch = "wasm32"))]
pub fn open_native_connection(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .map_err(DbError::from)?;
    Ok(conn)
}

#[cfg(not(target_arch = "wasm32"))]
fn open_connection(path: &Path) -> Result<Connection, DbError> {
    if let Some(opener) = PLATFORM_CONNECTION_OPENER.get() {
        opener(path)
    } else {
        #[cfg(any(test, feature = "test-utils"))]
        {
            open_native_connection(path)
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            Err(DbError(
                "platform SQLite connection opener is not registered".to_string(),
            ))
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn open_connection(path: &Path) -> Result<Connection, DbError> {
    PLATFORM_CONNECTION_OPENER
        .with(|slot| slot.get())
        .ok_or_else(|| DbError("platform SQLite connection opener is not registered".to_string()))?(
        path,
    )
}

fn migrate_bookkeeping_schema(conn: &Connection) -> Result<(), DbError> {
    if !table_has_column(conn, "published_blob_drop_intents", "size")? {
        conn.execute(
            "ALTER TABLE published_blob_drop_intents ADD COLUMN size INTEGER",
            [],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, DbError> {
    let sql = format!(
        "PRAGMA table_info({})",
        crate::sync::session::quote_ident(table)
    );
    let mut stmt = conn.prepare(&sql).map_err(DbError::from)?;
    let mut rows = stmt.query([]).map_err(DbError::from)?;
    while let Some(row) = rows.next().map_err(DbError::from)? {
        let name: String = row.get(1).map_err(DbError::from)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
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
            );",
        )
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
                    std::fs::write(&job_marker, b"landed").map_err(|e| DbError(e.to_string()))
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

    #[tokio::test]
    async fn database_open_rejects_host_declared_reserved_tables() {
        for table_name in [crate::db::ITEM_KEYS_TABLE, "sync_state"] {
            let result = Database::open(
                Path::new(":memory:"),
                vec![SyncedTable::new(table_name)],
                BLOB_TOMBSTONE_GRACE,
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
            vec![SyncedTable::new("")],
            BLOB_TOMBSTONE_GRACE,
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
            vec![SyncedTable::new("notes")],
            BLOB_TOMBSTONE_GRACE,
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
            "CREATE TABLE things (id INTEGER PRIMARY KEY, _updated_at TEXT NOT NULL);",
            vec![SyncedTable::new("things")],
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
             _updated_at TEXT NOT NULL);",
            vec![SyncedTable::new("things")],
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
            "CREATE TABLE things (thing_id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL);",
            vec![SyncedTable::new("things")],
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
             _updated_at TEXT NOT NULL, PRIMARY KEY (id, part));",
            vec![SyncedTable::new("things")],
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
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT);",
            vec![SyncedTable::new("things")],
            "nullable-updated-at",
        );
        assert!(
            error.contains("things") && error.contains("_updated_at"),
            "error names the table and the `_updated_at` requirement: {error}",
        );
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
             _updated_at TEXT NOT NULL);\
             CREATE TABLE thumbs (id TEXT PRIMARY KEY, size INTEGER NOT NULL, \
             _updated_at TEXT NOT NULL);",
            vec![
                SyncedTable::new("covers").carries_blob(blob("images")),
                SyncedTable::new("thumbs").carries_blob(blob("images")),
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
}

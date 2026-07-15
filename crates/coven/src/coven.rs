//! Top-level API: open one handle and drive rows, blobs, sync, and
//! membership through it.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::Connection;
use tracing::{debug, warn};

use crate::blob::local_files::LocalBlobError;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::{ClockRef, SystemClock};
use crate::config::Config;
use crate::custody::KeyCustody;
use crate::database::{Database, DbError, OpenError};
use crate::handle::CovenHandle;
use crate::identity_custody::IdentityCustody;
use crate::keys::StoreKeys;
use crate::migration::{Migration, MigrationError};
use crate::store_dir::PathTokenError;
use crate::sync::hlc::UpdatedAtStamper;
use crate::sync::session::SyncedTable;
use crate::sync::sync_manager::ConfigProvider;
use coven_core::{WriteId, WritePolicy, WriteReceipt};

pub type CovenResult<T> = Result<T, CovenError>;

const LOCAL_STAGE_MARKER: &str = ".coven-stage-";

#[derive(Debug, thiserror::Error)]
pub enum CovenError {
    #[error("database error: {0}")]
    Database(#[from] DbError),
    #[error("migration error: {0}")]
    Migration(MigrationError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("blob error: {0}")]
    Blob(String),
    #[error("unsafe blob path: {0}")]
    UnsafeBlobPath(#[from] PathTokenError),
    #[error("malformed local path: {0}")]
    MalformedPath(String),
    #[error("the write SQL closure panicked")]
    WriteClosurePanicked,
    #[error("synced_tables must be set before opening a coven store")]
    MissingSyncedTables,
    #[error("migrations must be set before opening a coven store")]
    MissingMigrations,
    #[error("write_policy must be set before opening a coven store")]
    MissingWritePolicy,
    #[error("Serial branch resolution failed: {0}")]
    SerialResolution(String),
    #[error("blob_tombstone_grace must be a positive duration")]
    InvalidBlobTombstoneGrace,
    #[error("blob {namespace}/{id} is still referenced by a row after the write")]
    BlobStillReferenced { namespace: String, id: String },
    #[error("blob {namespace}/{id} is already referenced by a row")]
    BlobAlreadyReferenced { namespace: String, id: String },
    #[error("blob {namespace}/{id} is owned by an unpublished write")]
    BlobOwnedByPendingWrite { namespace: String, id: String },
    #[error("store is already open: {}", store_dir.display())]
    AlreadyOpen { store_dir: PathBuf },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<OpenError> for CovenError {
    fn from(value: OpenError) -> Self {
        match value {
            OpenError::Migration(e) => CovenError::Migration(e),
            OpenError::Db(e) => CovenError::Database(e),
        }
    }
}

impl From<LocalBlobError> for CovenError {
    fn from(value: LocalBlobError) -> Self {
        CovenError::Blob(value.to_string())
    }
}

#[derive(Clone)]
pub struct CovenConfig(ConfigProvider);

impl CovenConfig {
    fn current(&self) -> Config {
        (self.0)()
    }

    fn provider(&self) -> ConfigProvider {
        self.0.clone()
    }
}

impl From<Config> for CovenConfig {
    fn from(value: Config) -> Self {
        let config = value;
        Self(Arc::new(move || config.clone()))
    }
}

impl<F> From<F> for CovenConfig
where
    F: Fn() -> Config + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        Self(Arc::new(value))
    }
}

pub struct Coven;

impl Coven {
    pub fn builder(config: impl Into<CovenConfig>) -> CovenBuilder {
        let config = config.into();
        let current = config.current();
        CovenBuilder {
            config,
            synced_tables: None,
            migrations: None,
            write_policy: None,
            blob_tombstone_grace: crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            max_concurrent_uploads: NonZeroUsize::MIN,
            max_concurrent_downloads: NonZeroUsize::MIN,
            clock: Arc::new(SystemClock),
            key_service: StoreKeys::new(current.store_id),
            key_custody: KeyCustody::Keyring,
            identity_custody: IdentityCustody::Keyring,
            cloudkit_ops: None,
            observer: None,
        }
    }
}

pub struct CovenBuilder {
    config: CovenConfig,
    synced_tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    write_policy: Option<WritePolicy>,
    blob_tombstone_grace: chrono::Duration,
    max_concurrent_uploads: NonZeroUsize,
    max_concurrent_downloads: NonZeroUsize,
    clock: ClockRef,
    key_service: StoreKeys,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
}

/// The single-writer store lock: an exclusive advisory lock on
/// `<store>/.coven-lock`, held for the life of a full [`open`](CovenBuilder::open)
/// handle (and its running sync loop). A second full open of the same store is
/// refused with [`CovenError::AlreadyOpen`] while the lock is held — the invariant
/// that keeps two writers from racing the same db and blob store.
///
/// # Read-only opens take no lock
///
/// [`open_read_only`](CovenBuilder::open_read_only) deliberately does **not** touch
/// this lock. The lock is exclusive, so a shared lock on the same file would block
/// against a writer that already holds it (and vice versa) — a reader could never
/// coexist with the writer it exists to read alongside. But a read-only open needs
/// no lock at all: the lock guards against a second *writer*, and a read-only handle
/// holds a `SQLITE_OPEN_READONLY` connection that cannot write. So a read-only open
/// skips the guard entirely. Cross-process safety comes from WAL mode (a reader sees
/// committed rows while the writer commits more), not from this lock; the blob cache
/// a reader may populate is per-device scratch written atomically (temp + rename), so
/// a reader and the writer touching the same cache file never tear it. This lets one
/// writer and any number of read-only readers coexist on one store.
pub(crate) struct StoreOpenGuard {
    _file: std::fs::File,
}

impl StoreOpenGuard {
    /// Acquire the guard for a test, panicking on refusal.
    #[cfg(test)]
    pub(crate) fn acquire_for_test(store_dir: &crate::store_dir::StoreDir) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::acquire(store_dir).expect("acquire store open guard"))
    }

    pub(crate) fn acquire(store_dir: &crate::store_dir::StoreDir) -> CovenResult<Self> {
        let db_path = store_dir.db_path();
        let Some(dir) = db_path.parent() else {
            return Err(CovenError::MalformedPath(format!(
                "store database path has no parent: {}",
                db_path.display()
            )));
        };
        std::fs::create_dir_all(dir)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(".coven-lock"))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(CovenError::AlreadyOpen {
                store_dir: dir.to_path_buf(),
            }),
            Err(std::fs::TryLockError::Error(error)) => Err(CovenError::Io(error)),
        }
    }
}

impl CovenBuilder {
    pub fn write_policy(mut self, policy: WritePolicy) -> Self {
        self.write_policy = Some(policy);
        self
    }

    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.synced_tables = Some(tables);
        self
    }

    /// How long a deleted blob is kept after its tombstone is written before the
    /// tombstone GC erases it: the cross-device convergence window. Defaults to
    /// [`crate::blob::delete::BLOB_TOMBSTONE_GRACE`]. Must be positive — a
    /// zero-or-negative grace is refused at [`open`](Self::open), since it would
    /// let the GC erase a blob a lagging peer still references.
    pub fn blob_tombstone_grace(mut self, grace: chrono::Duration) -> Self {
        self.blob_tombstone_grace = grace;
        self
    }

    /// How many blob uploads the sync cycle's upload drain runs at once. Defaults
    /// to one (fully serial). A [`NonZeroUsize`] so a zero — which would leave the
    /// drain admitting nothing and never completing — cannot be set.
    pub fn max_concurrent_uploads(mut self, n: NonZeroUsize) -> Self {
        self.max_concurrent_uploads = n;
        self
    }

    /// How many blob downloads a [`pin`](CovenHandle::pin) call fetches at once.
    /// Defaults to one (fully serial). A [`NonZeroUsize`] so a zero — which would
    /// leave the pin loop admitting nothing and never completing — cannot be set.
    pub fn max_concurrent_downloads(mut self, n: NonZeroUsize) -> Self {
        self.max_concurrent_downloads = n;
        self
    }

    /// The host's synced-schema migration ladder, applied over `PRAGMA
    /// user_version` at open. The top version is the wire `schema_version` every
    /// changeset is stamped with.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = Some(migrations);
        self
    }

    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.clock = clock;
        self
    }

    pub fn key_service(mut self, key_service: StoreKeys) -> Self {
        self.key_service = key_service;
        self
    }

    /// How the store's master key is protected: the OS keyring (the
    /// default), a passphrase-wrapped file, an in-memory session value, or a
    /// host's own [`MasterKeyCustody`](crate::MasterKeyCustody) implementation.
    /// coven builds every cipher internally from what this custody supplies —
    /// the host never touches a crypto type.
    pub fn key_custody(mut self, custody: KeyCustody) -> Self {
        self.key_custody = custody;
        self
    }

    /// How this store's device-signing identity is protected: the OS keyring
    /// (the default), a passphrase-wrapped file, an in-memory session value,
    /// or a host's own
    /// [`DeviceIdentityCustody`](crate::DeviceIdentityCustody) implementation.
    /// Selected next to [`key_custody`](Self::key_custody) — the identity is
    /// scoped to this store, established as part of creating, joining, or
    /// restoring it (see [`CovenHandle::initialize_identity`]).
    pub fn identity_custody(mut self, custody: IdentityCustody) -> Self {
        self.identity_custody = custody;
        self
    }

    pub fn cloudkit_ops(
        mut self,
        ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    ) -> Self {
        self.cloudkit_ops = Some(ops);
        self
    }

    pub fn apply_cloudkit_ops(
        mut self,
        ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Self {
        self.cloudkit_ops = ops;
        self
    }

    pub fn observer(mut self, observer: Arc<dyn BlobTransitionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Open the store, returning the [`CovenHandle`].
    ///
    /// Opening performs no keyring interaction: it opens the database, runs
    /// migrations, and resolves the master-key custody selection to a value
    /// (constructing the trait object, never calling its `unlock`) — a locked
    /// agent (no OS keyring session, no established master key or device
    /// identity) can `open()` a store and use it fully for rows and Local
    /// blobs. The first read of any key happens lazily, at the specific call
    /// that needs it ([`CovenHandle::connect_sync`],
    /// [`CovenHandle::master_key_fingerprint`], and similar).
    pub fn open(self) -> CovenResult<CovenHandle> {
        let config = self.config.current();
        let tables = self.synced_tables.ok_or(CovenError::MissingSyncedTables)?;
        let migrations = self.migrations.ok_or(CovenError::MissingMigrations)?;
        let write_policy = self.write_policy.ok_or(CovenError::MissingWritePolicy)?;
        if self.blob_tombstone_grace <= chrono::Duration::zero() {
            return Err(CovenError::InvalidBlobTombstoneGrace);
        }
        let db_path = config.store_dir.db_path();
        let provider = self.config.provider();
        let store_dir = config.store_dir.clone();
        let transfer_limits = crate::blob::TransferLimits {
            uploads: self.max_concurrent_uploads,
            downloads: self.max_concurrent_downloads,
        };
        let open_guard = Arc::new(StoreOpenGuard::acquire(&store_dir)?);
        remove_orphaned_local_blob_temps(&store_dir, std::time::SystemTime::now())?;
        let (db, stamper) = Database::open(
            &db_path,
            tables.clone(),
            self.blob_tombstone_grace,
            transfer_limits,
            write_policy,
            config.device_id.clone(),
            &migrations,
        )?;
        // A second, read-only connection on the same WAL database, opened after the
        // writer completed its migrations so the schema exists. It backs
        // [`CovenHandle::sql_read`]: a pure read runs here on its own connection
        // thread, concurrent with the writer rather than queued behind it, and
        // attaches no changeset session. Opening it fails `open()` loudly — there is
        // no full handle without its read path.
        let read_db = Database::open_read_only(
            &db_path,
            tables,
            self.blob_tombstone_grace,
            transfer_limits,
            write_policy,
            config.device_id.clone(),
            &migrations,
        )?;
        let key_custody = self.key_custody.resolve(&config.store_id, &store_dir);
        let identity_custody = self.identity_custody.resolve(&config.store_id, &store_dir);
        Ok(CovenHandle::new(
            db,
            read_db,
            stamper,
            store_dir,
            provider,
            self.key_service,
            key_custody,
            identity_custody,
            self.clock,
            self.cloudkit_ops,
            self.observer,
            open_guard,
        ))
    }

    /// Open the store read-only for a same-store secondary reader: a separate
    /// process (or a second handle) that must read rows and blobs while another
    /// handle holds the full [`open`](Self::open). Returns a [`CovenReadHandle`],
    /// whose surface is reads only — SQL queries and blob reads — with no write,
    /// sync, migration, or stamp API by construction.
    ///
    /// Unlike [`open`](Self::open) this takes no store lock (see
    /// [`StoreOpenGuard`]): it succeeds while a writer holds the exclusive lock,
    /// and any number of read-only opens coexist. It opens a `SQLITE_OPEN_READONLY`
    /// connection against the schema on disk, running no migration ladder — but it
    /// refuses a db a newer binary migrated past what this binary supports
    /// ([`CovenError::Migration`] with [`MigrationError::SchemaTooNew`]), the same
    /// policy the writer enforces. It runs no orphan-temp cleanup either (that is a
    /// write the lock-holding writer owns).
    ///
    /// Cross-process reads are safe because the writer opens the db in WAL mode; a
    /// blob read that misses locally fetches from the cloud into the per-device
    /// cache (files written atomically), which is device scratch and touches no
    /// synced state.
    pub fn open_read_only(self) -> CovenResult<crate::read_handle::CovenReadHandle> {
        let config = self.config.current();
        let tables = self.synced_tables.ok_or(CovenError::MissingSyncedTables)?;
        let migrations = self.migrations.ok_or(CovenError::MissingMigrations)?;
        let write_policy = self.write_policy.ok_or(CovenError::MissingWritePolicy)?;
        let db_path = config.store_dir.db_path();
        let provider = self.config.provider();
        let store_dir = config.store_dir.clone();
        // No StoreOpenGuard and no orphan-temp cleanup: both are writer concerns
        // (see StoreOpenGuard). A reader must not take the exclusive lock the
        // writer holds, nor write the filesystem the writer owns.
        let db = Database::open_read_only(
            &db_path,
            tables,
            self.blob_tombstone_grace,
            crate::blob::TransferLimits {
                uploads: self.max_concurrent_uploads,
                downloads: self.max_concurrent_downloads,
            },
            write_policy,
            config.device_id.clone(),
            &migrations,
        )?;
        let key_custody = self.key_custody.resolve(&config.store_id, &store_dir);
        let identity_custody = self.identity_custody.resolve(&config.store_id, &store_dir);
        Ok(crate::read_handle::CovenReadHandle::new(
            db,
            store_dir,
            provider,
            self.key_service,
            key_custody,
            identity_custody,
            self.clock,
            self.cloudkit_ops,
        ))
    }
}

/// Host SQL inside one journaled write transaction.
///
/// The context exposes SQL operations, but never the underlying SQLite
/// connection or transaction. In particular, a host cannot remove coven's SQL
/// authorizer and address its attached gate baseline:
///
/// ```compile_fail
/// # use coven::{CovenError, CovenHandle};
/// # async fn cannot_remove_guard(handle: &CovenHandle) -> Result<(), CovenError> {
/// handle.sql(|sql| {
///     sql.tx().authorizer(
///         None::<fn(coven::rusqlite::hooks::AuthContext<'_>)
///             -> coven::rusqlite::hooks::Authorization>,
///     )?;
///     Ok(())
/// }).await?;
/// # Ok(())
/// # }
/// ```
pub struct SqlContext<'ctx, 'conn> {
    tx: &'ctx rusqlite::Transaction<'conn>,
    stamper: UpdatedAtStamper,
}

impl<'ctx, 'conn> SqlContext<'ctx, 'conn> {
    pub(crate) fn new(tx: &'ctx rusqlite::Transaction<'conn>, stamper: UpdatedAtStamper) -> Self {
        Self { tx, stamper }
    }

    /// Execute one SQL statement with bound parameters.
    pub fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.tx.execute(sql, params)
    }

    /// Execute one or more semicolon-separated SQL statements.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.tx.execute_batch(sql)
    }

    /// Query exactly one row and map it to a host value.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.tx.query_row(sql, params, map)
    }

    /// Query any number of rows and collect their mapped host values.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.tx.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub fn stamp(&self) -> String {
        self.stamper.stamp()
    }
}

type WriteSql<R> =
    Box<dyn for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send>;

pub struct WriteBatch {
    new_blobs: Vec<NewBlob>,
    deleted_blobs: Vec<BlobRef>,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            new_blobs: Vec::new(),
            deleted_blobs: Vec::new(),
        }
    }

    pub fn put_blob(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) {
        self.new_blobs.push(NewBlob {
            namespace: namespace.into(),
            id: id.into(),
            bytes: bytes.into(),
        });
    }

    pub fn delete_blob(&mut self, blob: BlobRef) {
        self.deleted_blobs.push(blob);
    }
}

struct NewBlob {
    namespace: String,
    id: String,
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct StagedBlob {
    pub namespace: String,
    pub id: String,
    pub staged: PathBuf,
    pub final_path: PathBuf,
}

impl CovenHandle {
    async fn prepare_pending_branch_resolution(
        &self,
        branch_id: &coven_core::PendingBranchId,
    ) -> CovenResult<crate::sync::store_pull::SerialResolutionPlan> {
        let branch = self
            .db()
            .pending_branches()
            .await?
            .into_iter()
            .find(|branch| &branch.branch_id == branch_id)
            .ok_or_else(|| {
                CovenError::SerialResolution(format!(
                    "branch {} is not conflicted",
                    branch_id.first_write_id()
                ))
            })?;
        let manager = self
            .sync_manager()
            .ok_or_else(|| CovenError::SerialResolution("sync is not connected".to_string()))?;
        manager
            .prepare_serial_resolution(branch.base, &self.store_dir())
            .await
            .map_err(|error| CovenError::SerialResolution(error.to_string()))
    }

    pub async fn sql<F, R>(&self, f: F) -> CovenResult<WriteReceipt<R>>
    where
        F: for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let stamper = self.stamper();
        let tables = self.db().synced_tables().to_vec();
        let gates = self.db().gates();
        let blob_decls = self.db().blob_decls();
        let write_policy = self.db().write_policy();
        let routing_encryption = (write_policy == WritePolicy::MergeConcurrent
            && gates.has_scoped_graph())
        .then(|| self.routing_encryption())
        .transpose()?;
        let write_id = self.db().new_write_id();
        let outcome = self
            .db()
            .call(move |conn| {
                Ok(Database::run_store_write_transaction_on(
                    conn,
                    &tables,
                    &gates,
                    &blob_decls,
                    write_policy,
                    routing_encryption.as_ref(),
                    write_id,
                    |tx| f(SqlContext::new(tx, stamper)),
                ))
            })
            .await
            .map_err(CovenError::from)?;
        outcome
    }

    pub async fn discard_pending_branch(
        &self,
        branch_id: coven_core::PendingBranchId,
    ) -> CovenResult<()> {
        let plan = self.prepare_pending_branch_resolution(&branch_id).await?;
        self.db()
            .discard_pending_serial_branch(branch_id, plan)
            .await?;
        self.sync_now();
        Ok(())
    }

    pub async fn replace_pending_branch<F, R>(
        &self,
        branch_id: coven_core::PendingBranchId,
        f: F,
    ) -> CovenResult<WriteReceipt<R>>
    where
        F: for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let plan = self.prepare_pending_branch_resolution(&branch_id).await?;
        let write_id = self.db().new_write_id();
        let stamper = self.stamper();
        let receipt = self
            .db()
            .replace_pending_serial_branch(branch_id, plan, write_id, move |tx| {
                f(SqlContext::new(tx, stamper))
            })
            .await?;
        self.sync_now();
        Ok(receipt)
    }

    /// Run a pure read against this handle's read-only companion connection and
    /// await the result.
    ///
    /// Unlike [`sql`](Self::sql), this attaches no changeset session and opens no
    /// journaled transaction: a read pays nothing for capture it would produce
    /// nothing for, and runs on a separate `SQLITE_OPEN_READONLY` connection thread
    /// (concurrent with the writer, not queued behind it). The connection is
    /// read-only, so an `INSERT`/`UPDATE`/`DELETE`/DDL in the closure is refused by
    /// SQLite — a read cannot silently escape the sync journal because it cannot
    /// write at all. The closure receives the `&Connection` directly, so a host
    /// closure written against
    /// [`CovenReadHandle::sql_read`](crate::CovenReadHandle::sql_read) runs
    /// unchanged here.
    ///
    /// Read-your-writes holds for committed writes: a `sql_read` issued after an
    /// awaited [`sql`](Self::sql) or [`write`](Self::write) sees that data (the WAL
    /// reader reads the last committed state). It may not see another task's write
    /// that has not yet committed.
    pub async fn sql_read<F, R>(&self, f: F) -> CovenResult<R>
    where
        F: FnOnce(&rusqlite::Connection) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let outcome = self
            .read_db()
            .call(move |conn| Ok(f(conn)))
            .await
            .map_err(CovenError::from)?;
        outcome
    }

    pub async fn write<F, S, R>(&self, f: F, sql: S) -> CovenResult<WriteReceipt<R>>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let mut batch = WriteBatch::new();
        f(&mut batch)?;
        let sql: WriteSql<R> = Box::new(sql);
        let staged = self.stage_blobs(batch.new_blobs).await?;
        let staged_paths = staged
            .iter()
            .map(|blob| blob.staged.clone())
            .collect::<Vec<_>>();
        let tables = self.db().synced_tables().to_vec();
        let db = self.db().clone();
        let stamper = self.stamper();
        let gates = self.db().gates();
        let blob_decls = self.db().blob_decls();
        let write_policy = self.db().write_policy();
        let routing_encryption = (write_policy == WritePolicy::MergeConcurrent
            && gates.has_scoped_graph())
        .then(|| self.routing_encryption())
        .transpose()?;
        let write_id = self.db().new_write_id();
        let deleted = batch.deleted_blobs;
        let store_dir = self.store_dir();
        let outcome = match db
            .call(move |conn| {
                Ok(run_write_batch_on_connection(
                    conn,
                    stamper,
                    store_dir,
                    staged,
                    deleted,
                    tables,
                    gates,
                    blob_decls,
                    write_policy,
                    routing_encryption,
                    write_id,
                    sql,
                ))
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                remove_staged_paths(&staged_paths).await;
                return Err(CovenError::from(error));
            }
        };
        match outcome {
            Ok(receipt) => {
                if let Err(error) =
                    crate::blob::local_cleanup::drain(self.db(), &self.store_dir()).await
                {
                    warn!(
                        error = %error,
                        "failed to drain local blob cleanup intents after write commit"
                    );
                }
                Ok(receipt)
            }
            Err(error) => {
                remove_staged_paths(&staged_paths).await;
                Err(error)
            }
        }
    }

    async fn stage_blobs(&self, blobs: Vec<NewBlob>) -> CovenResult<Vec<StagedBlob>> {
        let mut staged = Vec::new();
        for blob in blobs {
            let final_path = self
                .store_dir()
                .local_blob_path(&blob.namespace, &blob.id)?;
            let staged_path = local_stage_temp_path(&final_path)?;
            if let Err(e) = crate::local_blob::write_atomic(&staged_path, &blob.bytes).await {
                remove_staged_files(&staged).await;
                return Err(CovenError::Blob(e));
            }
            staged.push(StagedBlob {
                namespace: blob.namespace,
                id: blob.id,
                staged: staged_path,
                final_path,
            });
        }
        Ok(staged)
    }
}

fn local_stage_temp_path(final_path: &Path) -> CovenResult<PathBuf> {
    let file_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CovenError::MalformedPath(format!(
                "local blob path has no file name: {}",
                final_path.display()
            ))
        })?;
    Ok(final_path.with_file_name(format!(
        "{file_name}{LOCAL_STAGE_MARKER}{}",
        uuid::Uuid::new_v4()
    )))
}

/// Remove temp debris a crashed prior process left in the blob folders, before the
/// handle serves any read. Two temp conventions:
///
/// - Every atomic write (`write_atomic`/`copy_atomic`) in any blob folder — the local
///   store, `cache/`, `pinned/` — leaves a `.tmp.<uuid>` sibling until its
///   fsync-then-rename commits it. A crash orphans that temp. Only those older than
///   this open (`process_start`) are crash leftovers; a temp a concurrent write is
///   mid-producing is newer and is left in place so the sweep can never delete a live
///   write's rename target.
/// - `storage/local/` additionally holds write staging (`.coven-stage-<uuid>`). Open
///   holds the exclusive store lock, so every staging temp here is a crash leftover
///   — all are removed, no age check.
fn remove_orphaned_local_blob_temps(
    store_dir: &crate::store_dir::StoreDir,
    process_start: std::time::SystemTime,
) -> CovenResult<()> {
    let storage = store_dir.storage_dir();
    remove_orphaned_temps_in_dir(&storage.join("local"), &is_local_stage_temp, None)?;
    for folder in ["local", "cache", "pinned"] {
        remove_orphaned_temps_in_dir(
            &storage.join(folder),
            &crate::local_blob::is_temp_blob_path,
            Some(process_start),
        )?;
    }
    Ok(())
}

/// Recursively remove temp files under `dir` that `is_temp` recognizes. With
/// `older_than = Some(cutoff)` a temp is removed only when its modification time is
/// before `cutoff` — a temp at or after it is a live write, left untouched; `None`
/// removes every recognized temp regardless of age.
fn remove_orphaned_temps_in_dir(
    dir: &Path,
    is_temp: &dyn Fn(&Path) -> bool,
    older_than: Option<std::time::SystemTime>,
) -> CovenResult<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!(
                path = %dir.display(),
                "blob directory absent during orphaned temp cleanup"
            );
            return Ok(());
        }
        Err(error) => return Err(CovenError::Io(error)),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            remove_orphaned_temps_in_dir(&path, is_temp, older_than)?;
        } else if file_type.is_file() && is_temp(&path) {
            if let Some(cutoff) = older_than {
                let modified = entry.metadata()?.modified()?;
                if modified >= cutoff {
                    debug!(
                        path = %path.display(),
                        "leaving fresh blob temp created at or after process start"
                    );
                    continue;
                }
            }
            remove_file_if_present(&path)?;
        } else if file_type.is_file() && path.file_name().and_then(|name| name.to_str()).is_none() {
            debug!(
                path = %path.display(),
                "skipping blob path with non-utf8 file name during orphaned temp cleanup"
            );
        }
    }
    Ok(())
}

fn is_local_stage_temp(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains(LOCAL_STAGE_MARKER))
}

async fn remove_staged_files(staged: &[StagedBlob]) {
    let paths = staged
        .iter()
        .map(|blob| blob.staged.clone())
        .collect::<Vec<_>>();
    remove_staged_paths(&paths).await;
}

async fn remove_staged_paths(paths: &[PathBuf]) {
    for path in paths {
        remove_staged_path(path).await;
    }
}

async fn remove_staged_path(path: &Path) {
    if let Err(error) = crate::local_blob::remove_file(path).await {
        warn!(
            path = %path.display(),
            error = %error,
            "failed to remove staged local blob"
        );
    }
}

fn remove_file_if_present(path: &Path) -> CovenResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!(
                path = %path.display(),
                "file already absent during local blob cleanup"
            );
            Ok(())
        }
        Err(error) => Err(CovenError::Io(error)),
    }
}

fn sync_parent_dir(path: &Path) -> CovenResult<()> {
    let parent = path.parent().ok_or_else(|| {
        CovenError::MalformedPath(format!("path has no parent directory: {}", path.display()))
    })?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn run_write_batch_on_connection<R>(
    conn: &Connection,
    stamper: UpdatedAtStamper,
    store_dir: crate::store_dir::StoreDir,
    staged: Vec<StagedBlob>,
    deleted: Vec<BlobRef>,
    tables: Vec<SyncedTable>,
    gates: Arc<crate::sync::gate::Gates>,
    decls: Arc<crate::blob::decl::BlobDecls>,
    write_policy: WritePolicy,
    routing_encryption: Option<crate::encryption::EncryptionService>,
    write_id: WriteId,
    sql: WriteSql<R>,
) -> CovenResult<WriteReceipt<R>> {
    let mut moved = Vec::new();
    let result = Database::run_store_write_transaction_on(
        conn,
        &tables,
        &gates,
        &decls,
        write_policy,
        routing_encryption.as_ref(),
        write_id,
        |tx| -> CovenResult<R> {
            for blob in &staged {
                match decls.row_for_blob_in_namespace(tx, &blob.namespace, &blob.id) {
                    Ok(Some(_)) => {
                        return Err(CovenError::BlobAlreadyReferenced {
                            namespace: blob.namespace.clone(),
                            id: blob.id.clone(),
                        });
                    }
                    Ok(None) => {}
                    Err(e) => return Err(CovenError::Blob(e.to_string())),
                }
                let leased = tx
                    .query_row(
                        "SELECT EXISTS(\
                             SELECT 1 FROM store_write_blob_leases \
                             WHERE namespace = ?1 AND blob_id = ?2\
                         )",
                        (&blob.namespace, &blob.id),
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(CovenError::from)?;
                if leased {
                    return Err(CovenError::BlobOwnedByPendingWrite {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
                if let Some(parent) = blob.final_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        CovenError::Blob(format!(
                            "create local blob parent {}: {e}",
                            parent.display()
                        ))
                    })?;
                }
                std::fs::rename(&blob.staged, &blob.final_path).map_err(|e| {
                    CovenError::Blob(format!(
                        "install staged blob {} -> {}: {e}",
                        blob.staged.display(),
                        blob.final_path.display()
                    ))
                })?;
                moved.push(blob.clone());
                sync_parent_dir(&blob.final_path).map_err(|e| {
                    CovenError::Blob(format!(
                        "sync local blob parent after installing {}: {e}",
                        blob.final_path.display()
                    ))
                })?;
            }

            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                sql(SqlContext::new(tx, stamper))
            })) {
                Ok(Ok(value)) => {
                    for blob in &deleted {
                        let _ = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
                        let _ = store_dir.pinned_blob_path(&blob.namespace, &blob.id)?;
                        let _ = store_dir.cache_blob_path(&blob.namespace, &blob.id)?;
                        let intent = crate::blob::local_cleanup::LocalBlobCleanupIntent::new(
                            &blob.namespace,
                            &blob.id,
                        );
                        if !crate::blob::local_cleanup::record_if_unreferenced_on(
                            tx, &decls, &intent,
                        )? {
                            return Err(CovenError::BlobStillReferenced {
                                namespace: blob.namespace.clone(),
                                id: blob.id.clone(),
                            });
                        }
                    }
                    Ok(value)
                }
                Ok(Err(error)) => Err(error),
                Err(_) => Err(CovenError::WriteClosurePanicked),
            }
        },
    );
    match result {
        Ok(value) => Ok(value),
        Err(error) => rollback_write_batch(error, moved),
    }
}

fn rollback_write_batch<R>(error: CovenError, moved: Vec<StagedBlob>) -> CovenResult<R> {
    for blob in moved.iter().rev() {
        if let Err(e) = std::fs::remove_file(&blob.final_path) {
            warn!(
                namespace = %blob.namespace,
                blob_id = %blob.id,
                path = %blob.final_path.display(),
                error = %e,
                "failed to remove installed local blob after write rollback"
            );
        }
    }
    Err(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{BlobScope, CacheFill, Provenance};
    use crate::config::Config;
    use crate::keys::test_keyring;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
    };
    use crate::store_dir::StoreDir;
    use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
    use crate::sync::hlc::Hlc;
    use crate::sync::session::BlobDecl;
    use crate::sync::storage::SyncStorage;
    use crate::sync::test_helpers::run_test_cycle as drive_test_cycle;
    use crate::sync::test_helpers::{
        capture_bytes, pull_into, query_text, row_exists, MockSyncStorage,
    };
    use async_trait::async_trait;
    use rusqlite::params;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::RwLock;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::sync::Notify;

    fn config(dir: StoreDir) -> Config {
        Config::with_defaults(
            "lib-test".to_string(),
            "device-test".to_string(),
            dir,
            "Test".to_string(),
        )
    }

    fn media_files_decl() -> BlobDecl {
        BlobDecl::new(
            "media-files",
            Provenance::HostProvided,
            CacheFill::CacheLazy,
        )
        .with_id_column("blob_id")
    }

    fn files_table() -> SyncedTable {
        SyncedTable::new("files", coven_core::RowIdentity::SharedKey)
            .carries_blob(media_files_decl())
    }

    fn remote_root_files_table() -> SyncedTable {
        SyncedTable::new("files", coven_core::RowIdentity::SharedKey)
            .remote_root()
            .carries_blob(media_files_decl())
    }

    fn files_migration() -> Migration {
        Migration::sql(
            1,
            "test-schema",
            "CREATE TABLE files (
                id TEXT PRIMARY KEY,
                blob_id TEXT,
                size INTEGER NOT NULL,
                hash TEXT,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )
    }

    fn gated_roots_table() -> SyncedTable {
        SyncedTable::new("roots", coven_core::RowIdentity::SharedKey).gated_by("shared")
    }

    fn gated_roots_migration() -> Migration {
        Migration::sql(
            1,
            "gated-roots",
            "CREATE TABLE roots (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )
    }

    fn open_gated_roots_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let handle = Coven::builder(config(StoreDir::new(tmp.path())))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("open gated handle");
        (tmp, handle)
    }

    fn open_gated_roots_at(dir: StoreDir, write_policy: WritePolicy) -> CovenResult<CovenHandle> {
        Coven::builder(config(dir))
            .write_policy(write_policy)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
    }

    fn precreate_database(dir: &StoreDir, sql: &str) {
        let conn = rusqlite::Connection::open(dir.db_path()).expect("precreate store database");
        conn.execute_batch(sql)
            .expect("seed pre-existing database state");
    }

    #[test]
    fn serial_store_opens_without_a_provider_or_coordination() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let handle = Coven::builder(config(StoreDir::new(tmp.path())))
            .write_policy(WritePolicy::Serial)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("open local Serial store");

        assert!(matches!(
            *handle.subscribe_sync_status().borrow(),
            crate::SyncLoopStatus::Offline
        ));
    }

    #[test]
    fn precreated_empty_sqlite_file_initializes_coven_metadata() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        precreate_database(&dir, "");

        let handle = open_gated_roots_at(dir, WritePolicy::Serial)
            .expect("initialize Coven in an empty SQLite database");

        assert_eq!(handle.db().write_policy(), WritePolicy::Serial);
    }

    #[test]
    fn existing_host_tables_without_a_coven_marker_initialize_coven_metadata() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        precreate_database(
            &dir,
            "CREATE TABLE roots (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             PRAGMA user_version = 1;",
        );

        let handle = open_gated_roots_at(dir, WritePolicy::Serial)
            .expect("initialize Coven beside an existing host schema");

        assert_eq!(handle.db().write_policy(), WritePolicy::Serial);
    }

    #[test]
    fn interrupted_coven_schema_without_a_marker_is_initialized_atomically() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        precreate_database(
            &dir,
            "CREATE TABLE protocol_state (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) STRICT;",
        );

        let handle = open_gated_roots_at(dir, WritePolicy::Serial)
            .expect("finish an uncommitted Coven schema initialization");

        assert_eq!(handle.db().write_policy(), WritePolicy::Serial);
    }

    #[tokio::test]
    async fn reopen_refuses_a_write_policy_that_differs_from_the_created_store() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let serial = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::Serial)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("create Serial store");
        drop(serial);

        let wrong = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open();
        if let Ok(wrong) = wrong {
            wrong
                .sql(|sql| {
                    sql.execute(
                        "INSERT INTO roots (id, title, shared, _updated_at) \
                         VALUES ('wrong-policy', 'wrong', 1, \
                                 '0000000001000-0000-device-test')",
                        [],
                    )?;
                    Ok(())
                })
                .await
                .expect("the mismatched open currently permits a host write");
            panic!("a store must reject a different write policy at open");
        }

        let reopened = Coven::builder(config(dir))
            .write_policy(WritePolicy::Serial)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("the store reopens with its selected policy");
        assert!(reopened.db().pending_writes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reopen_refuses_a_store_missing_required_write_policy_metadata() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::Serial)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("create Serial store");
        handle
            .db()
            .call(|conn| {
                let marker: String = conn
                    .query_row(
                        "SELECT value FROM protocol_state WHERE key = 'coven_initialized'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(crate::database::DbError::from)?;
                assert_eq!(marker, "1");
                conn.execute("DELETE FROM protocol_state WHERE key = 'write_policy'", [])
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("remove required policy metadata");
        drop(handle);

        let reopened = Coven::builder(config(dir))
            .write_policy(WritePolicy::Serial)
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open();
        assert!(matches!(reopened, Err(CovenError::Database(_))));
    }

    #[tokio::test]
    async fn host_sql_cannot_discover_or_mutate_the_gate_baseline() {
        let (_tmp, handle) = open_gated_roots_handle();

        let discovery = handle
            .sql(|sql| {
                sql.query("PRAGMA database_list", [], |row| row.get::<_, String>(1))
                    .map_err(CovenError::from)
            })
            .await;
        assert!(
            discovery.is_err(),
            "host SQL must not enumerate coven's attached gate baseline",
        );

        let mutation = handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO coven_gate_empty.roots \
                     (id, title, shared, _updated_at) \
                     VALUES ('root-1', 'Private', 1, '0000000002000-0000-device-test')",
                    [],
                )?;
                Ok(())
            })
            .await;
        assert!(
            mutation.is_err(),
            "host SQL must not address coven's attached gate baseline",
        );

        handle
            .sql(|sql| {
                sql.execute_batch(
                    "CREATE TABLE host_local (id TEXT PRIMARY KEY, value TEXT) STRICT; \
                     INSERT INTO host_local VALUES ('local-1', 'kept'); \
                     INSERT INTO roots (id, title, shared, _updated_at) \
                     VALUES ('root-1', 'Private', 0, \
                             '0000000001000-0000-device-test');",
                )?;
                Ok(())
            })
            .await
            .expect("arbitrary host-schema SQL remains available");
        let published = handle
            .sql(|sql| {
                sql.execute(
                    "UPDATE roots SET shared = 1, \
                     _updated_at = '0000000002000-0000-device-test' \
                     WHERE id = 'root-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .expect("flip root visible");
        let write_id = published.write_id.clone();
        let changeset = handle
            .db()
            .call(move |conn| {
                conn.query_row(
                    "SELECT changeset FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("load gated changeset");
        let rows = coven_core::changeset::walk(&changeset).expect("walk gated changeset");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op, coven_core::changeset::ChangeOp::Insert);
        assert_eq!(rows[0].pk(), Some("root-1"));
        assert_eq!(rows[0].col(1), Some("Private"));
        assert_eq!(rows[0].col(2), Some("1"));
        assert_eq!(rows[0].col(3), Some("0000000002000-0000-device-test"));
    }

    fn open_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    #[tokio::test]
    async fn second_open_of_one_store_is_refused_until_the_first_handle_drops() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let first = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("first open succeeds");
        let clone = first.clone();

        let second = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            second,
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ));

        drop(first);

        let still_locked = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            still_locked,
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ));

        drop(clone);

        Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open succeeds after the first handle drops");
    }

    #[tokio::test]
    async fn a_zero_or_negative_blob_tombstone_grace_is_refused_at_open() {
        for grace in [chrono::Duration::zero(), chrono::Duration::seconds(-1)] {
            let tmp = tempfile::tempdir().expect("temp dir");
            let dir = StoreDir::new(tmp.path());
            let result = Coven::builder(config(dir))
                .write_policy(WritePolicy::MergeConcurrent)
                .synced_tables(vec![files_table()])
                .migrations(vec![files_migration()])
                .blob_tombstone_grace(grace)
                .open();
            assert!(
                matches!(result, Err(CovenError::InvalidBlobTombstoneGrace)),
                "grace {grace:?} must be refused at open",
            );
        }
    }

    #[tokio::test]
    async fn a_positive_blob_tombstone_grace_opens() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .blob_tombstone_grace(chrono::Duration::hours(1))
            .open()
            .expect("a positive grace opens");
    }

    fn open_remote_root_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    fn open_files_handle_in(dir: StoreDir) -> CovenHandle {
        try_open(&dir).expect("open handle")
    }

    async fn run_test_cycle(
        storage: &MockSyncStorage,
        handle: &CovenHandle,
        keypair: &crate::keys::UserKeypair,
    ) {
        let cipher = RwLock::new(CloudCipher::Plaintext);
        let hlc = Hlc::new("device-test".to_string());
        crate::sync::test_helpers::bind_mock_store_protocol(handle.db(), storage, "device-test")
            .await;
        handle
            .db()
            .set_protocol_state(
                crate::database::LAST_SNAPSHOT_HASH_STATE_KEY,
                &crate::sync::store_commit::ObjectHash::digest(b"existing-test-snapshot")
                    .to_string(),
            )
            .await
            .expect("seed Store snapshot state");
        drive_test_cycle(
            storage,
            "lib-test",
            "device-test",
            &hlc,
            &SystemClock,
            handle.db(),
            &cipher,
            &PendingRotation::none(),
            keypair,
            None,
            &handle.store_dir(),
            None,
            None,
        )
        .await
        .expect("sync cycle");
    }

    async fn merge_test_storage(keypair: &crate::keys::UserKeypair) -> MockSyncStorage {
        let storage = MockSyncStorage::with_store_and_keypair("lib-test", keypair.clone());
        crate::sync::test_helpers::publish_test_founder_membership(&storage, "lib-test", keypair)
            .await;
        storage
    }

    async fn publish_current_writes(handle: &CovenHandle) {
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, handle, &keypair).await;
    }

    #[tokio::test]
    async fn write_survives_reopen_before_sync_cycle() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = open_files_handle_in(dir.clone());
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-before-reopen', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("write before reopen");
        drop(handle);

        let reopened = open_files_handle_in(dir);
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, &reopened, &keypair).await;

        let (_peer_tmp, peer) = open_files_handle();
        pull_into(peer.db(), &storage, "peer", &peer.store_dir()).await;
        assert_eq!(
            query_text(
                peer.db(),
                "SELECT id FROM files WHERE id = 'file-before-reopen'"
            )
            .await,
            "file-before-reopen",
        );
    }

    #[tokio::test]
    async fn separate_host_transactions_publish_as_separate_store_commits_after_restart() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = open_files_handle_in(dir.clone());
        let mut write_ids = Vec::new();
        for id in ["file-pending-a", "file-pending-b"] {
            let receipt = handle
                .sql(move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                        (id, sql.stamp()),
                    )?;
                    Ok(())
                })
                .await
                .expect("write before reopen");
            assert_eq!(receipt.status, coven_core::WriteStatus::Pending);
            write_ids.push(receipt.write_id);
        }
        drop(handle);

        let reopened = open_files_handle_in(dir);
        let pending = reopened.pending_writes().await.expect("pending writes");
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].write_id, write_ids[0]);
        assert_eq!(pending[1].write_id, write_ids[1]);
        let mut first_status = reopened
            .subscribe_write_status(&write_ids[0])
            .await
            .expect("subscribe after restart");
        assert_eq!(*first_status.borrow(), coven_core::WriteStatus::Pending);
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, &reopened, &keypair).await;

        first_status.changed().await.expect("published status");
        assert!(matches!(
            &*first_status.borrow(),
            coven_core::WriteStatus::Published(position) if position.position().seq == 2
        ));
        assert!(matches!(
            reopened.write_status(&write_ids[1]).await.expect("second status"),
            coven_core::WriteStatus::Published(position) if position.position().seq == 3
        ));
        assert!(reopened
            .pending_writes()
            .await
            .expect("published writes are not pending")
            .is_empty());

        let (_peer_tmp, peer) = open_files_handle();
        pull_into(peer.db(), &storage, "peer", &peer.store_dir()).await;
        assert!(row_exists(peer.db(), "SELECT 1 FROM files WHERE id = 'file-pending-a'").await);
        assert!(row_exists(peer.db(), "SELECT 1 FROM files WHERE id = 'file-pending-b'").await);
    }

    #[tokio::test]
    async fn device_local_transaction_is_local_only_and_never_pending() {
        let (_tmp, handle) = open_files_handle();
        let receipt = handle
            .sql(|sql| {
                sql.execute_batch(
                    "CREATE TABLE local_notes (id TEXT PRIMARY KEY, body TEXT) STRICT;
                     INSERT INTO local_notes VALUES ('local-1', 'private');",
                )?;
                Ok("saved")
            })
            .await
            .expect("local transaction");

        assert_eq!(receipt.value, "saved");
        assert_eq!(receipt.status, coven_core::WriteStatus::LocalOnly);
        assert_eq!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("durable local status"),
            coven_core::WriteStatus::LocalOnly
        );
        assert!(handle
            .pending_writes()
            .await
            .expect("pending writes")
            .is_empty());
        let local_write_id = receipt.write_id.clone();
        let lease_count = handle
            .db()
            .call(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM store_write_blob_leases WHERE write_id = ?1",
                    [local_write_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("count local-only blob leases");
        assert_eq!(lease_count, 0);

        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, &handle, &keypair).await;
        assert_eq!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("local status after sync"),
            coven_core::WriteStatus::LocalOnly
        );
        assert!(handle
            .pending_writes()
            .await
            .expect("pending writes after sync")
            .is_empty());
    }

    #[tokio::test]
    async fn mixed_transaction_tracks_and_publishes_only_shared_rows() {
        let (_tmp, handle) = open_files_handle();
        let receipt = handle
            .sql(|sql| {
                sql.execute_batch(
                    "CREATE TABLE local_notes (id TEXT PRIMARY KEY, body TEXT) STRICT;
                     INSERT INTO local_notes VALUES ('local-1', 'private');",
                )?;
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at)
                     VALUES ('shared-1', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("mixed transaction");

        assert_eq!(receipt.status, coven_core::WriteStatus::Pending);
        let pending = handle.pending_writes().await.expect("pending mixed write");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].write_id, receipt.write_id);
        assert_eq!(
            pending[0].affected_rows,
            vec![coven_core::AffectedRow {
                table: "files".to_string(),
                primary_key: "shared-1".to_string(),
            }]
        );

        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, &handle, &keypair).await;
        assert!(matches!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("published mixed write"),
            coven_core::WriteStatus::Published(position) if position.position().seq == 2
        ));

        let (_peer_tmp, peer) = open_files_handle();
        pull_into(peer.db(), &storage, "peer", &peer.store_dir()).await;
        assert!(row_exists(peer.db(), "SELECT 1 FROM files WHERE id = 'shared-1'").await);
        assert!(
            !row_exists(
                peer.db(),
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'local_notes'"
            )
            .await
        );
        assert!(
            row_exists(
                handle.db(),
                "SELECT 1 FROM local_notes WHERE id = 'local-1'"
            )
            .await
        );
    }

    #[tokio::test]
    async fn delete_survives_reopen_before_sync_cycle() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = open_files_handle_in(dir.clone());
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-delete-reopen', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("insert before first cycle");
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, &handle, &keypair).await;

        let (_peer_tmp, peer) = open_files_handle();
        pull_into(peer.db(), &storage, "peer", &peer.store_dir()).await;
        assert!(
            row_exists(
                peer.db(),
                "SELECT 1 FROM files WHERE id = 'file-delete-reopen'"
            )
            .await,
            "the peer receives the insert before the delete",
        );

        handle
            .sql(|sql| {
                sql.execute("DELETE FROM files WHERE id = 'file-delete-reopen'", [])?;
                Ok(())
            })
            .await
            .expect("delete before reopen");
        drop(handle);

        let reopened = open_files_handle_in(dir);
        run_test_cycle(&storage, &reopened, &keypair).await;

        pull_into(peer.db(), &storage, "peer", &peer.store_dir()).await;
        assert!(
            !row_exists(
                peer.db(),
                "SELECT 1 FROM files WHERE id = 'file-delete-reopen'"
            )
            .await,
            "the delete changeset reaches the peer after reopening",
        );
    }

    #[tokio::test]
    async fn pending_write_drains_only_after_changeset_push() {
        let (_tmp, handle) = open_files_handle();
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-retry-publish', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("write before failed push");
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        crate::sync::test_helpers::bind_mock_store_protocol(handle.db(), &storage, "device-test")
            .await;
        storage.fail_next_changeset_puts(1);

        let first = drive_test_cycle(
            &storage,
            "lib-test",
            "device-test",
            &Hlc::new("device-test".to_string()),
            &SystemClock,
            handle.db(),
            &RwLock::new(CloudCipher::Plaintext),
            &PendingRotation::none(),
            &keypair,
            None,
            &handle.store_dir(),
            None,
            None,
        )
        .await;
        assert!(
            first
                .as_ref()
                .is_err_and(|error| error.contains("forced Store package append failure")),
            "the first cycle must report the append failure while preserving its outbox: {first:?}",
        );
        assert!(crate::sync::store_objects::load_commit_slot(
            &storage,
            storage.store_root_hash(),
            "device-test",
            2,
        )
        .await
        .expect("inspect Store commit slot after failed append")
        .is_none());

        run_test_cycle(&storage, &handle, &keypair).await;
        assert!(
            crate::sync::store_objects::load_commit_slot(
                &storage,
                storage.store_root_hash(),
                "device-test",
                2,
            )
            .await
            .expect("inspect Store commit slot after retry")
            .is_some(),
            "the pending write is published as an immutable Store commit after retry",
        );
    }

    async fn write_raw_file(path: &std::path::Path, bytes: &[u8]) {
        crate::local_blob::write_atomic(path, bytes)
            .await
            .expect("write file");
    }

    async fn cleanup_intent_count(handle: &CovenHandle, namespace: &str, id: &str) -> i64 {
        let namespace = namespace.to_string();
        let id = id.to_string();
        handle
            .sql(move |sql| {
                sql.query_row(
                    "SELECT count(*) FROM local_cleanup_intents \
                         WHERE namespace = ?1 AND blob_id = ?2",
                    params![namespace, id],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("count cleanup intents")
            .value
    }

    #[tokio::test]
    async fn builder_open_runs_coven_and_host_migrations() {
        let (_tmp, handle) = open_files_handle();
        let has_coven_table: i64 = handle
            .sql(|sql| {
                sql.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'protocol_state'",
                    [],
                    |row| row.get(0),
                ).map_err(CovenError::from)
            })
            .await
            .expect("query coven table")
            .value;
        let has_host_table: i64 = handle
            .sql(|sql| {
                sql.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'files'",
                    [],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("query host table")
            .value;
        assert_eq!(has_coven_table, 1);
        assert_eq!(has_host_table, 1);
    }

    #[tokio::test]
    async fn open_of_a_too_new_db_yields_the_matchable_migration_variant() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());

        // Open with a two-step ladder so the db lands at synced-schema version 2.
        let ahead = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![
                files_migration(),
                Migration::sql(2, "add-extra", "CREATE TABLE extra (id TEXT PRIMARY KEY)"),
            ])
            .open()
            .expect("open at version 2");
        drop(ahead);

        // Reopen with only the first step: an older binary meeting a db a newer one
        // already migrated. The remedy is "update the app", so the host must be able
        // to match the specific variant rather than string-scrape a DbError.
        let reopened = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            reopened,
            Err(CovenError::Migration(MigrationError::SchemaTooNew {
                current: 2,
                supported: 1
            }))
        ));
    }

    #[tokio::test]
    async fn sql_reads_writes_and_stamps() {
        let (_tmp, handle) = open_files_handle();
        let id = "file-sql".to_string();
        handle
            .sql(move |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    params![id, sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("insert through sql");
        let count: i64 = handle
            .sql(|sql| {
                sql.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count rows")
            .value;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn sql_surfaces_sqlite_constraint_typed() {
        let (_tmp, handle) = open_files_handle();
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    params!["duplicate-id", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");

        let result: CovenResult<WriteReceipt<()>> = handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    params!["duplicate-id", sql.stamp()],
                )?;
                Ok(())
            })
            .await;

        assert!(matches!(result, Err(CovenError::Sqlite(_))));
    }

    #[tokio::test]
    async fn sql_read_sees_a_committed_write() {
        let (_tmp, handle) = open_files_handle();
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-read-your-write', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("insert through sql");

        // The read runs on the separate read-only connection; it must observe the
        // just-committed write (WAL read-your-writes for committed data).
        let id: String = handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT id FROM files WHERE id = 'file-read-your-write'",
                    [],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read the committed row back through sql_read");
        assert_eq!(id, "file-read-your-write");
    }

    #[tokio::test]
    async fn sql_read_cannot_write() {
        let (_tmp, handle) = open_files_handle();
        let result: CovenResult<()> = handle
            .sql_read(|conn| {
                conn.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-read-only', NULL, 0, '0')",
                    [],
                )
                .map_err(CovenError::from)?;
                Ok(())
            })
            .await;
        // The companion connection is SQLITE_OPEN_READONLY, so the write is refused
        // at the SQLite layer with the readonly code — a read cannot escape the sync
        // journal because it cannot write at all.
        match result {
            Err(CovenError::Sqlite(rusqlite::Error::SqliteFailure(err, _))) => {
                assert_eq!(err.code, rusqlite::ErrorCode::ReadOnly);
            }
            other => panic!("expected a readonly sqlite failure, got {other:?}"),
        }

        let count: i64 = handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT count(*) FROM files WHERE id = 'file-read-only'",
                    [],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("count rows through sql_read");
        assert_eq!(count, 0, "the refused write left no row behind");
    }

    #[tokio::test]
    async fn open_on_a_fresh_store_serves_sql_read() {
        // A fresh (empty) directory: `open` runs the writer's migrations, then opens
        // the read connection against the schema they created. A `sql_read` over the
        // host table then succeeds rather than failing on a missing table — proof the
        // read connection opened after, not before, the schema exists.
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open on an empty directory");
        let count: i64 = handle
            .sql_read(|conn| {
                conn.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("sql_read on a fresh store");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn open_removes_orphaned_local_blob_temps() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let final_path = dir
            .local_blob_path("media-files", "tempaaaa")
            .expect("local path");
        let temp = local_stage_temp_path(&final_path).expect("stage temp path");
        write_raw_file(&temp, b"interrupted write").await;

        let _handle = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");

        assert!(
            !temp.exists(),
            "open removes orphaned local blob staging temps"
        );
        assert!(
            !dir.local_blob_path("media-files", "tempaaaa")
                .expect("local path")
                .exists(),
            "the interrupted blob has no committed final file"
        );
    }

    #[tokio::test]
    async fn write_inserts_row_and_host_provided_blob() {
        let (_tmp, handle) = open_files_handle();
        let bytes = b"piece-bytes".to_vec();
        handle
            .write(
                {
                    let bytes = bytes.clone();
                    move |w| {
                        w.put_blob("media-files", "blobaaaa", bytes);
                        Ok(())
                    }
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-1", "blobaaaa", bytes.len() as i64, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write row and blob");
        let path = handle
            .store_dir()
            .local_blob_path("media-files", "blobaaaa")
            .expect("local path");
        assert_eq!(
            std::fs::read(path).expect("read local blob"),
            b"piece-bytes"
        );
    }

    #[tokio::test]
    async fn orphaned_final_blob_is_replaced_by_next_write() {
        let (_tmp, handle) = open_files_handle();
        let path = handle
            .store_dir()
            .local_blob_path("media-files", "orphaaaa")
            .expect("local path");
        write_raw_file(&path, b"orphaned bytes").await;

        handle
            .write(
                |w| {
                    w.put_blob("media-files", "orphaaaa", b"committed bytes".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-orphan", "orphaaaa", 15i64, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write replaces orphaned final blob");

        assert_eq!(
            std::fs::read(path).expect("read committed blob"),
            b"committed bytes"
        );
    }

    #[tokio::test]
    async fn put_blob_rejects_id_already_referenced_by_a_row() {
        let (_tmp, handle) = open_files_handle();
        handle
            .write(
                |w| {
                    w.put_blob("media-files", "dupeaaaa", b"original".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-original", "dupeaaaa", 8i64, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("seed original blob");

        let result: CovenResult<WriteReceipt<()>> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "dupeaaaa", b"replacement".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-replacement", "dupeaaaa", 11i64, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(CovenError::BlobAlreadyReferenced { .. })
        ));
        let path = handle
            .store_dir()
            .local_blob_path("media-files", "dupeaaaa")
            .expect("dupe path");
        assert_eq!(
            std::fs::read(path).expect("read original blob"),
            b"original"
        );
        let replacement_rows: i64 = handle
            .sql(|sql| {
                sql.query_row(
                    "SELECT count(*) FROM files WHERE id = 'file-replacement'",
                    [],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("count replacement rows")
            .value;
        assert_eq!(replacement_rows, 0);
    }

    #[tokio::test]
    async fn remote_root_host_provided_write_reads_staging_through_handle_before_upload() {
        let (_tmp, handle) = open_remote_root_files_handle();
        let expected = b"remote-root-host-provided-staging-bytes".to_vec();
        let bytes = expected.clone();

        let blob = handle
            .write(
                {
                    let bytes = bytes.clone();
                    move |w| {
                        w.put_blob("media-files", "rrhpaaaa", bytes);
                        Ok(())
                    }
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            "file-remote-root",
                            "rrhpaaaa",
                            bytes.len() as i64,
                            sql.stamp()
                        ],
                    )?;
                    Ok(BlobRef {
                        namespace: "media-files".to_string(),
                        id: "rrhpaaaa".to_string(),
                        scope: BlobScope::Master,
                        cloud_path: None,
                        provenance: Provenance::HostProvided,
                        fill: CacheFill::CacheLazy,
                    })
                },
            )
            .await
            .expect("write remote-root row and host-provided blob")
            .value;

        let whole = handle
            .read_blob(&blob)
            .await
            .expect("read_blob serves upload staging before sync upload");
        assert_eq!(
            whole, expected,
            "read_blob returns the bytes written through handle.write",
        );

        let (offset, len) = (12u64, 19u64);
        let range = handle
            .open_blob_stream(&blob, expected.len() as u64, offset, len)
            .await
            .expect("open_blob_stream serves upload staging before sync upload");
        assert_eq!(
            range,
            &expected[offset as usize..(offset + len) as usize],
            "open_blob_stream returns the requested slice of the staged bytes",
        );
    }

    #[tokio::test]
    async fn sql_failure_removes_staged_blob() {
        let (_tmp, handle) = open_files_handle();
        let err = handle
            .write(
                |w| {
                    w.put_blob("media-files", "blobbbbb", b"staged".to_vec());
                    Ok(())
                },
                |_sql| Err::<(), CovenError>(CovenError::Blob("sql failed".to_string())),
            )
            .await
            .expect_err("write fails");
        assert!(err.to_string().contains("sql failed"));
        let path = handle
            .store_dir()
            .local_blob_path("media-files", "blobbbbb")
            .expect("local path");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn blob_stage_failure_does_not_run_sql() {
        let (_tmp, handle) = open_files_handle();
        let result: CovenResult<WriteReceipt<()>> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "..", b"bad".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('should-not-exist', NULL, 0, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await;
        // A blob id that can't form a safe path is its own typed error, not the
        // generic blob catch-all.
        assert!(matches!(result, Err(CovenError::UnsafeBlobPath(_))));
        let count: i64 = handle
            .sql(|sql| {
                sql.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count rows")
            .value;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn replacement_deletes_old_blob_after_sql_drops_reference() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldaaaa", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldaaaa", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldaaaa", b"old")
            .await
            .expect("restore published blob locally");
        let old_ref = BlobRef {
            namespace: "media-files".to_string(),
            id: "oldaaaa".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        handle
            .write(
                move |w| {
                    w.put_blob("media-files", "newaaaa", b"new".to_vec());
                    w.delete_blob(old_ref);
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        "UPDATE files SET blob_id = ?1, size = 3, _updated_at = ?2 WHERE id = 'file-1'",
                        params!["newaaaa", sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("replace blob");
        assert!(!handle
            .store_dir()
            .local_blob_path("media-files", "oldaaaa")
            .expect("old path")
            .exists());
        assert!(handle
            .store_dir()
            .local_blob_path("media-files", "newaaaa")
            .expect("new path")
            .exists());
    }

    async fn queue_replacement_before_sync(
        handle: &CovenHandle,
    ) -> (WriteId, WriteId, std::path::PathBuf) {
        let first = handle
            .write(
                |batch| {
                    batch.put_blob("media-files", "ownedaaa", b"first".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('owned-file', 'ownedaaa', 5, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("queue first blob write");
        let first_path = handle
            .store_dir()
            .local_blob_path("media-files", "ownedaaa")
            .expect("first blob path");
        let first_blob = BlobRef {
            namespace: "media-files".to_string(),
            id: "ownedaaa".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        let second = handle
            .write(
                move |batch| {
                    batch.put_blob("media-files", "ownedbbb", b"second".to_vec());
                    batch.delete_blob(first_blob);
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "UPDATE files SET blob_id = 'ownedbbb', size = 6, _updated_at = ?1 \
                         WHERE id = 'owned-file'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("queue replacement write");
        (first.write_id, second.write_id, first_path)
    }

    async fn assert_replacement_publishes_in_order(
        handle: &CovenHandle,
        first_write: &WriteId,
        second_write: &WriteId,
        first_path: &std::path::Path,
    ) {
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&keypair).await;
        run_test_cycle(&storage, handle, &keypair).await;

        assert!(matches!(
            handle.write_status(first_write).await.expect("first status"),
            coven_core::WriteStatus::Published(position) if position.position().seq == 2
        ));
        assert!(matches!(
            handle.write_status(second_write).await.expect("second status"),
            coven_core::WriteStatus::Published(position) if position.position().seq == 3
        ));
        assert!(storage
            .blob_exists("media-files", "ownedaaa", None)
            .await
            .expect("first cloud blob"));
        assert!(storage
            .blob_exists("media-files", "ownedbbb", None)
            .await
            .expect("second cloud blob"));
        assert_eq!(storage.blob_put_from_file_count(), 2);
        assert!(
            !first_path.exists(),
            "the first write releases its local bytes after publication"
        );
    }

    #[tokio::test]
    async fn pending_write_owns_blob_bytes_until_its_publication() {
        let (_tmp, handle) = open_files_handle();
        let (first_write, second_write, first_path) = queue_replacement_before_sync(&handle).await;

        assert_eq!(
            std::fs::read(&first_path).expect("first write still owns its bytes"),
            b"first"
        );
        let overwrite: CovenResult<WriteReceipt<()>> = handle
            .write(
                |batch| {
                    batch.put_blob("media-files", "ownedaaa", b"overwritten".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('overwrite', 'ownedaaa', 11, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await;
        assert!(matches!(
            overwrite,
            Err(CovenError::BlobOwnedByPendingWrite { .. })
        ));
        assert_eq!(
            std::fs::read(&first_path).expect("lease prevents overwrite"),
            b"first"
        );
        assert_replacement_publishes_in_order(&handle, &first_write, &second_write, &first_path)
            .await;
    }

    #[tokio::test]
    async fn pending_write_blob_ownership_survives_restart() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = open_files_handle_in(dir.clone());
        let (first_write, second_write, first_path) = queue_replacement_before_sync(&handle).await;
        drop(handle);

        let reopened = open_files_handle_in(dir);
        assert_eq!(
            std::fs::read(&first_path).expect("reopened first write still owns its bytes"),
            b"first"
        );
        assert_replacement_publishes_in_order(&reopened, &first_write, &second_write, &first_path)
            .await;
    }

    #[tokio::test]
    async fn cache_eviction_cannot_remove_pending_write_blob_bytes() {
        let (_tmp, handle) = open_files_handle();
        let (_first_write, _second_write, first_path) =
            queue_replacement_before_sync(&handle).await;
        let first_blob = BlobRef {
            namespace: "media-files".to_string(),
            id: "ownedaaa".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };

        handle
            .evict_blob(&first_blob)
            .await
            .expect("evict only re-fetchable cache copies");

        assert_eq!(
            std::fs::read(first_path).expect("pending write still owns local-store bytes"),
            b"first"
        );
    }

    #[tokio::test]
    async fn author_delete_drops_all_local_blob_copies() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldcccc", b"old")
            .await
            .expect("store old");
        let pinned = handle
            .store_dir()
            .pinned_blob_path("media-files", "oldcccc")
            .expect("pinned path");
        let cached = handle
            .store_dir()
            .cache_blob_path("media-files", "oldcccc")
            .expect("cache path");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldcccc", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldcccc", b"old")
            .await
            .expect("restore published blob locally");
        write_raw_file(&pinned, b"pinned").await;
        write_raw_file(&cached, b"cached").await;
        let old_ref = BlobRef {
            namespace: "media-files".to_string(),
            id: "oldcccc".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };

        handle
            .write(
                move |w| {
                    w.delete_blob(old_ref);
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        "UPDATE files SET blob_id = NULL, size = 0, _updated_at = ?1 \
                         WHERE id = 'file-1'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("delete blob");

        assert!(!handle
            .store_dir()
            .local_blob_path("media-files", "oldcccc")
            .expect("local path")
            .exists());
        assert!(!pinned.exists(), "pinned copy is removed");
        assert!(!cached.exists(), "cache copy is removed");
    }

    #[tokio::test]
    async fn failed_local_blob_cleanup_keeps_intent_for_later_drain() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldddddd", b"old")
            .await
            .expect("store old");
        let pinned = handle
            .store_dir()
            .pinned_blob_path("media-files", "oldddddd")
            .expect("pinned path");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldddddd", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldddddd", b"old")
            .await
            .expect("restore published blob locally");
        std::fs::create_dir_all(&pinned).expect("create pinned blocker");
        let old_ref = BlobRef {
            namespace: "media-files".to_string(),
            id: "oldddddd".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };

        handle
            .write(
                move |w| {
                    w.delete_blob(old_ref);
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "UPDATE files SET blob_id = NULL, size = 0, _updated_at = ?1 \
                         WHERE id = 'file-1'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("row delete commits despite cleanup failure");

        assert_eq!(
            cleanup_intent_count(&handle, "media-files", "oldddddd").await,
            1
        );
        assert!(handle
            .store_dir()
            .local_blob_path("media-files", "oldddddd")
            .expect("local path")
            .exists());

        std::fs::remove_dir_all(&pinned).expect("remove pinned blocker");
        handle
            .write(
                |_| Ok(()),
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, NULL, 0, ?2)",
                        params!["drain-trigger", sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("later committed write drains pending cleanup");

        assert_eq!(
            cleanup_intent_count(&handle, "media-files", "oldddddd").await,
            0
        );
        assert!(!handle
            .store_dir()
            .local_blob_path("media-files", "oldddddd")
            .expect("local path")
            .exists());
    }

    #[tokio::test]
    async fn write_drain_preserves_a_blob_still_referenced_after_remote_delete() {
        let (_tmp, handle) = open_files_handle();
        let blob_id = "shared01";
        handle
            .sql(move |sql| {
                sql.execute_batch(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('remote-deletes', 'shared01', 4, \
                             '0000000001000-0000-dev-remote'); \
                     INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('still-live', 'shared01', 4, \
                             '0000000001000-0000-dev-remote');",
                )?;
                Ok(())
            })
            .await
            .expect("seed two rows sharing the blob");

        let local = handle
            .store_dir()
            .local_blob_path("media-files", blob_id)
            .expect("local path");
        let pinned = handle
            .store_dir()
            .pinned_blob_path("media-files", blob_id)
            .expect("pinned path");
        let cached = handle
            .store_dir()
            .cache_blob_path("media-files", blob_id)
            .expect("cache path");
        write_raw_file(&local, b"live").await;
        write_raw_file(&pinned, b"live").await;
        write_raw_file(&cached, b"live").await;

        let (source, _stamper) = Database::open(
            std::path::Path::new(":memory:"),
            vec![files_table()],
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            coven_core::WritePolicy::MergeConcurrent,
            "dev-remote".to_string(),
            &[files_migration()],
        )
        .expect("open remote source");
        capture_bytes(
            &source,
            &["INSERT INTO files (id, blob_id, size, _updated_at) \
                 VALUES ('remote-deletes', 'shared01', 4, \
                         '0000000001000-0000-dev-remote')"],
        )
        .await;
        let remote_delete =
            capture_bytes(&source, &["DELETE FROM files WHERE id = 'remote-deletes'"]).await;
        let storage = Arc::new(MockSyncStorage::new());
        storage.store_changeset("dev-remote", 1, &remote_delete, 1);

        let (commit_reached, resume_pull) = handle.db().arm_test_pause(
            coven_core::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id: "dev-remote".to_string(),
                seq: 1,
            },
        );
        let pull_handle = handle.clone();
        let pull_storage = storage.clone();
        let pull = tokio::spawn(async move {
            pull_into(
                pull_handle.db(),
                pull_storage.as_ref(),
                "device-test",
                &pull_handle.store_dir(),
            )
            .await
        });

        commit_reached.notified().await;
        handle
            .write(
                |_| Ok(()),
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('drain-trigger', NULL, 0, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write drains cleanup queue");
        resume_pull.notify_one();
        pull.await.expect("pull task");

        assert!(
            !row_exists(
                handle.db(),
                "SELECT 1 FROM files WHERE id = 'remote-deletes'"
            )
            .await
        );
        assert!(row_exists(handle.db(), "SELECT 1 FROM files WHERE id = 'still-live'").await);
        assert!(local.exists(), "the live blob's local copy survives");
        assert!(pinned.exists(), "the live blob's pinned copy survives");
        assert!(cached.exists(), "the live blob's cache copy survives");
        assert_eq!(
            cleanup_intent_count(&handle, "media-files", blob_id).await,
            0,
        );
    }

    #[tokio::test]
    async fn replacement_is_rejected_while_sql_still_references_old_blob() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.store_dir(), "media-files", "oldbbbb", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldbbbb", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        let old_ref = BlobRef {
            namespace: "media-files".to_string(),
            id: "oldbbbb".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        let result: CovenResult<WriteReceipt<()>> = handle
            .write(
                move |w| {
                    w.put_blob("media-files", "newbbbb", b"new".to_vec());
                    w.delete_blob(old_ref);
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        "UPDATE files SET _updated_at = ?1 WHERE id = 'file-1'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(CovenError::BlobStillReferenced { .. })
        ));
        assert!(handle
            .store_dir()
            .local_blob_path("media-files", "oldbbbb")
            .expect("old path")
            .exists());
        assert!(!handle
            .store_dir()
            .local_blob_path("media-files", "newbbbb")
            .expect("new path")
            .exists());
    }

    #[tokio::test]
    async fn sql_panic_removes_moved_blob() {
        let (_tmp, handle) = open_files_handle();
        let result: CovenResult<WriteReceipt<()>> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "panicccc", b"new".to_vec());
                    Ok(())
                },
                |_sql| panic!("boom"),
            )
            .await;
        assert!(matches!(result, Err(CovenError::WriteClosurePanicked)));
        assert!(!handle
            .store_dir()
            .local_blob_path("media-files", "panicccc")
            .expect("panic path")
            .exists());
    }

    #[tokio::test]
    async fn concurrent_duplicate_blob_write_does_not_delete_committed_blob() {
        let (_tmp, handle) = open_files_handle();
        let winner = handle.clone();
        let loser = handle.clone();

        let write_winner = tokio::spawn(async move {
            winner
                .write(
                    move |w| {
                        w.put_blob("media-files", "raceblob", b"committed".to_vec());
                        Ok(())
                    },
                    move |sql| {
                        sql.execute(
                            "INSERT INTO files (id, blob_id, size, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4)",
                            params!["winner", "raceblob", 9i64, sql.stamp()],
                        )?;
                        Ok(())
                    },
                )
                .await
        });

        let write_loser = tokio::spawn(async move {
            loser
                .write(
                    move |w| {
                        w.put_blob("media-files", "raceblob", b"rolled-back".to_vec());
                        Ok(())
                    },
                    |_sql| Err::<(), CovenError>(CovenError::Blob("force rollback".to_string())),
                )
                .await
        });

        let winner_result = write_winner.await.expect("winner task");
        let loser_result = write_loser.await.expect("loser task");
        assert!(winner_result.is_ok() || loser_result.is_ok());
        assert!(winner_result.is_err() || loser_result.is_err());

        let path = handle
            .store_dir()
            .local_blob_path("media-files", "raceblob")
            .expect("race path");
        assert_eq!(std::fs::read(path).expect("read race blob"), b"committed");
        let rows: i64 = handle
            .sql(|sql| {
                sql.query_row(
                    "SELECT count(*) FROM files WHERE id = 'winner'",
                    [],
                    |row| row.get(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("count winner row")
            .value;
        assert_eq!(rows, 1);
    }

    /// A [`CloudHome`] that blocks the sync loop inside a cycle on demand. It
    /// delegates every call to an inner [`InMemoryCloudHome`], but once `armed`,
    /// the first cloud operation of a cycle signals `entered` and parks on
    /// `release` until the test wakes it — holding the loop mid-cycle so the test
    /// can observe the store-directory lock while a cycle is in flight.
    ///
    /// Arming happens only after `connect_sync_with_test_home` returns, so the
    /// connect-time bootstrap runs unblocked and only a loop cycle is gated.
    struct GateCloudHome {
        inner: InMemoryCloudHome,
        armed: Arc<AtomicBool>,
        gated: AtomicBool,
        entered: mpsc::UnboundedSender<()>,
        release: Arc<Notify>,
    }

    impl GateCloudHome {
        async fn gate(&self) {
            if self.armed.load(Ordering::Acquire) && !self.gated.swap(true, Ordering::AcqRel) {
                self.entered
                    .send(())
                    .expect("test observes the loop entering a cycle");
                self.release.notified().await;
            }
        }
    }

    #[async_trait]
    impl CloudHome for GateCloudHome {
        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            self.gate().await;
            self.inner.put_object(key, data).await
        }

        async fn open_multipart<'a>(
            &'a self,
            key: &str,
            total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            self.gate().await;
            self.inner.open_multipart(key, total_len).await
        }

        fn multipart_threshold(&self) -> u64 {
            self.inner.multipart_threshold()
        }

        async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.gate().await;
            self.inner.read(key).await
        }

        async fn read_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            self.gate().await;
            self.inner.read_range(key, start, end).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            self.gate().await;
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
            self.gate().await;
            self.inner.delete(key).await
        }

        async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
            self.gate().await;
            self.inner.exists(key).await
        }

        async fn set_access(
            &self,
            desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            self.gate().await;
            self.inner.set_access(desired).await
        }
    }

    fn try_open(dir: &StoreDir) -> CovenResult<CovenHandle> {
        Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
    }

    /// Reopen `dir`, retrying while a still-running sync loop holds the lock.
    /// Retries only the lock refusal; any other open error fails immediately.
    /// Fails the calling test if the lock is not released within the budget.
    async fn open_when_lock_released(dir: &StoreDir) -> CovenHandle {
        for _ in 0..100 {
            match try_open(dir) {
                Ok(handle) => return handle,
                Err(CovenError::AlreadyOpen { .. }) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(other) => panic!("reopen failed for a reason other than the lock: {other}"),
            }
        }
        panic!("store lock never released: reopen kept failing");
    }

    #[tokio::test]
    async fn lock_is_held_until_the_sync_loop_exits_its_cycle() {
        test_keyring::install();

        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        // A store id distinct from every other test's — `try_open`/
        // `open_when_lock_released` below reopen the same directory (the
        // lock is path-scoped, not store-id-scoped) but this test's own
        // identity establishment must not collide with a concurrently
        // running test that also establishes one under the shared default id.
        let handle = Coven::builder(Config::with_defaults(
            "lock-held-until-cycle-exits".to_string(),
            "device-test".to_string(),
            dir.clone(),
            "Test".to_string(),
        ))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");
        handle
            .initialize_identity()
            .expect("establish this store's identity before connecting");

        let armed = Arc::new(AtomicBool::new(false));
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let home = Arc::new(GateCloudHome {
            inner: InMemoryCloudHome::new(),
            armed: armed.clone(),
            gated: AtomicBool::new(false),
            entered: entered_tx,
            release: release.clone(),
        });
        handle
            .connect_sync_with_test_home(home, CloudCipher::Plaintext)
            .await
            .expect("connect over the gating test home");

        // Bootstrap is done; gate the loop's next cycle and wait until it parks
        // mid-cycle inside the cloud home.
        armed.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(30), entered_rx.recv())
            .await
            .expect("sync loop reached a cycle within the timeout")
            .expect("gating home signalled the cycle entry");

        // Drop the last handle while the loop is still mid-cycle. The loop owns a
        // guard clone, so the lock must stay held: a concurrent open is refused.
        drop(handle);
        assert!(
            matches!(
                try_open(&dir),
                Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
            ),
            "a concurrent open must be refused while the sync loop is mid-cycle",
        );

        // Release the cycle; the loop finishes, sees its channels closed, exits,
        // and drops the last guard clone — the lock frees and a reopen succeeds.
        release.notify_one();
        let _reopened = open_when_lock_released(&dir).await;
    }

    #[tokio::test]
    async fn normal_shutdown_releases_the_lock_for_reopen() {
        test_keyring::install();

        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        // A store id distinct from every other test's — see the identical
        // note in `lock_is_held_until_the_sync_loop_exits_its_cycle`.
        let handle = Coven::builder(Config::with_defaults(
            "normal-shutdown-releases-lock".to_string(),
            "device-test".to_string(),
            dir.clone(),
            "Test".to_string(),
        ))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");
        handle
            .initialize_identity()
            .expect("establish this store's identity before connecting");
        handle
            .connect_sync_with_test_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("connect over the test home");

        drop(handle);

        let _reopened = open_when_lock_released(&dir).await;
    }

    /// The open-time orphan sweep clears crash-left atomic-write temps (`.tmp.<uuid>`)
    /// under every blob folder — `cache/`, `pinned/`, and the local store — that
    /// predate this open, while leaving a temp a concurrent write is mid-producing
    /// (newer than process start) and every committed blob untouched. Local-store
    /// staging temps (`.coven-stage-<uuid>`) are still swept regardless of age.
    #[test]
    fn open_time_sweep_clears_stale_blob_temps_but_keeps_fresh_ones() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let ld = StoreDir::new(tmp.path());

        let process_start = std::time::SystemTime::now();
        let stale = process_start - Duration::from_secs(3600);
        let fresh = process_start + Duration::from_secs(3600);

        let write_with_mtime = |path: &Path, mtime: std::time::SystemTime| {
            std::fs::create_dir_all(path.parent().unwrap()).expect("create dir");
            std::fs::write(path, b"x").expect("write file");
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .expect("open to set mtime")
                .set_modified(mtime)
                .expect("set mtime");
        };
        let temp_name = |marker: &str| format!("{marker}{}", uuid::Uuid::new_v4());

        let cache_shard = ld
            .storage_dir()
            .join("cache")
            .join("release_files")
            .join("ab")
            .join("cd");
        let pinned_shard = ld
            .storage_dir()
            .join("pinned")
            .join("photos")
            .join("ef")
            .join("gh");
        let local_ns = ld.storage_dir().join("local").join("audio");

        let stale_cache_temp = cache_shard.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        let fresh_cache_temp = cache_shard.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        // A committed blob with an OLD mtime: not a temp, so age is irrelevant — it must
        // survive, proving the sweep classifies by the temp prefix, not by age.
        let committed_cache = cache_shard.join("blob0aaa");
        let stale_pinned_temp = pinned_shard.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        let fresh_pinned_temp = pinned_shard.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        // The local store's own `write_atomic` leaves `.tmp.<uuid>` temps too; a crash
        // orphans them the same way. A committed local blob (old mtime) must survive.
        let stale_local_temp = local_ns.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        let fresh_local_temp = local_ns.join(temp_name(crate::local_blob::TEMP_BLOB_PREFIX));
        let committed_local = local_ns.join("blob0bbb");
        let stale_local_stage = local_ns.join(temp_name(&format!("blob{LOCAL_STAGE_MARKER}")));

        write_with_mtime(&stale_cache_temp, stale);
        write_with_mtime(&fresh_cache_temp, fresh);
        write_with_mtime(&committed_cache, stale);
        write_with_mtime(&stale_pinned_temp, stale);
        write_with_mtime(&fresh_pinned_temp, fresh);
        write_with_mtime(&stale_local_temp, stale);
        write_with_mtime(&fresh_local_temp, fresh);
        write_with_mtime(&committed_local, stale);
        write_with_mtime(&stale_local_stage, stale);

        remove_orphaned_local_blob_temps(&ld, process_start).expect("open-time orphan sweep");

        assert!(
            !stale_cache_temp.exists(),
            "a cache temp older than process start is a crash leftover, removed",
        );
        assert!(
            !stale_pinned_temp.exists(),
            "a pinned temp older than process start is a crash leftover, removed",
        );
        assert!(
            !stale_local_temp.exists(),
            "a local-store atomic-write temp older than process start is a crash leftover, removed",
        );
        assert!(
            fresh_cache_temp.exists(),
            "a cache temp newer than process start is a live write, left alone",
        );
        assert!(
            fresh_pinned_temp.exists(),
            "a pinned temp newer than process start is a live write, left alone",
        );
        assert!(
            fresh_local_temp.exists(),
            "a local-store temp newer than process start is a live write, left alone",
        );
        assert!(
            committed_cache.exists(),
            "a committed cache blob is never a temp — kept regardless of its mtime",
        );
        assert!(
            committed_local.exists(),
            "a committed local-store blob is never a temp — kept regardless of its mtime",
        );
        assert!(
            !stale_local_stage.exists(),
            "local-store staging temps are still swept at open",
        );
    }

    // ========================================================================
    // Read-only opens (CovenReadHandle)
    // ========================================================================

    fn try_open_read_only(dir: &StoreDir) -> CovenResult<crate::read_handle::CovenReadHandle> {
        Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open_read_only()
    }

    async fn read_files_count(handle: &crate::read_handle::CovenReadHandle) -> i64 {
        handle
            .sql_read(|conn| {
                conn.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count files through the read handle")
    }

    /// The requirement's failing baseline made to pass: a second FULL open is still
    /// refused while the first holds the store, but a read-only open succeeds
    /// against the same held store — it takes no writer lock.
    #[tokio::test]
    async fn read_only_open_succeeds_while_a_full_open_holds_the_store() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let writer = open_files_handle_in(dir.clone());

        // A second full open is refused (the invariant the lock protects).
        assert!(matches!(
            try_open(&dir),
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ));

        // A read-only open succeeds against the very same held store.
        let reader = try_open_read_only(&dir).expect("read-only open succeeds under a full open");
        assert_eq!(read_files_count(&reader).await, 0);

        drop(writer);
    }

    /// Multiple read-only opens coexist with each other and with the writer.
    #[tokio::test]
    async fn multiple_read_only_opens_coexist_with_a_writer() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let writer = open_files_handle_in(dir.clone());

        let reader_a = try_open_read_only(&dir).expect("first read-only open");
        let reader_b = try_open_read_only(&dir).expect("second read-only open");

        assert_eq!(read_files_count(&reader_a).await, 0);
        assert_eq!(read_files_count(&reader_b).await, 0);
        drop(writer);
    }

    /// A read-only handle sees rows the writer committed — both before the reader
    /// opened and after (WAL cross-connection visibility on the one db file).
    #[tokio::test]
    async fn read_only_open_sees_committed_writer_data() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let writer = open_files_handle_in(dir.clone());

        writer
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('row-before-reader', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("writer inserts before the reader opens");

        let reader = try_open_read_only(&dir).expect("read-only open");
        assert_eq!(
            read_files_count(&reader).await,
            1,
            "the reader sees the row committed before it opened",
        );

        // A commit after the reader is open is visible on its next read: coven runs
        // each read as its own transaction, so it never pins an old WAL snapshot.
        writer
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('row-after-reader', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("writer inserts after the reader opened");
        assert_eq!(
            read_files_count(&reader).await,
            2,
            "the reader sees a row the writer committed after the reader opened",
        );
        drop(writer);
    }

    /// A read-only handle has no write API at all — writes are absent by
    /// construction. The one place a raw statement could be run is the
    /// `sql_read` closure's `&Connection`; because the connection is
    /// `SQLITE_OPEN_READONLY`, SQLite refuses the write there rather than
    /// mutating the store.
    #[tokio::test]
    async fn read_only_handle_connection_refuses_writes() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let _writer = open_files_handle_in(dir.clone());
        let reader = try_open_read_only(&dir).expect("read-only open");

        let result: CovenResult<()> = reader
            .sql_read(|conn| {
                conn.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('should-not-write', NULL, 0, 'x')",
                    [],
                )?;
                Ok(())
            })
            .await;
        assert!(
            matches!(result, Err(CovenError::Sqlite(_))),
            "a write through the read-only connection is refused by SQLite: {result:?}",
        );
    }

    /// A read-only open runs no migration ladder, but refuses a db a newer binary
    /// migrated past what this binary knows — the writer's `SchemaTooNew` policy.
    #[tokio::test]
    async fn read_only_open_refuses_a_too_new_schema() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());

        // A newer binary migrates the db to synced-schema version 2.
        let ahead = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![
                files_migration(),
                Migration::sql(2, "add-extra", "CREATE TABLE extra (id TEXT PRIMARY KEY)"),
            ])
            .open()
            .expect("open at version 2");
        drop(ahead);

        // An older binary opens the same db read-only with only the version-1 ladder:
        // it cannot understand the schema, so it refuses with the matchable variant.
        let reopened = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open_read_only();
        assert!(matches!(
            reopened,
            Err(CovenError::Migration(MigrationError::SchemaTooNew {
                current: 2,
                supported: 1
            }))
        ));
    }

    /// End-to-end blob read through the read handle: the writer stores a
    /// host-provided blob on a remote-root table (its bytes land in the local store
    /// as upload staging); the read-only handle resolves the blob's locality and
    /// serves those bytes — no cloud, no writer lock.
    #[tokio::test]
    async fn read_only_handle_reads_a_host_provided_blob() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let writer = Coven::builder(config(dir.clone()))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open writer");

        let bytes = b"read-only-handle-serves-these-blob-bytes".to_vec();
        writer
            .write(
                {
                    let bytes = bytes.clone();
                    move |w| {
                        w.put_blob("media-files", "roblob01", bytes);
                        Ok(())
                    }
                },
                {
                    let len = bytes.len() as i64;
                    move |sql| {
                        sql.execute(
                            "INSERT INTO files (id, blob_id, size, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4)",
                            params!["file-ro", "roblob01", len, sql.stamp()],
                        )?;
                        Ok(())
                    }
                },
            )
            .await
            .expect("writer stores the row and blob");

        let reader = Coven::builder(config(dir))
            .write_policy(WritePolicy::MergeConcurrent)
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open_read_only()
            .expect("read-only open");

        let blob = BlobRef {
            namespace: "media-files".to_string(),
            id: "roblob01".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        let read = reader
            .read_blob(&blob)
            .await
            .expect("read blob via read handle");
        assert_eq!(
            read, bytes,
            "the read handle serves the blob the writer stored"
        );

        // A ranged read through the same handle serves the requested slice.
        let (offset, len) = (5u64, 10u64);
        let range = reader
            .open_blob_stream(&blob, bytes.len() as u64, offset, len)
            .await
            .expect("ranged read via read handle");
        assert_eq!(range, &bytes[offset as usize..(offset + len) as usize]);
        drop(writer);
    }

    /// Concurrent same-path cache populate by two producers (the writer's and the
    /// reader's fetch both writing the same blob into `cache/`) is safe: the atomic
    /// temp-then-rename write means the destination is always one producer's whole
    /// file, never a torn interleave. This is the primitive requirement 6 rests on.
    #[tokio::test]
    async fn concurrent_same_blob_cache_writes_never_tear() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let dest = dir
            .cache_blob_path("media-files", "raceblob")
            .expect("cache path");
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).expect("create cache shard dir");
        }

        // Two full-length payloads that differ everywhere, so any interleave would be
        // detectable as a mixed file. In production both producers write the same
        // blob's bytes; the distinct payloads only make tearing observable.
        let a = vec![b'A'; 64 * 1024];
        let b = vec![b'B'; 64 * 1024];
        let (ra, rb) = tokio::join!(
            crate::local_blob::write_atomic(&dest, &a),
            crate::local_blob::write_atomic(&dest, &b),
        );
        ra.expect("first concurrent cache write");
        rb.expect("second concurrent cache write");

        let final_bytes = crate::local_blob::read(&dest)
            .await
            .expect("read cache file");
        assert!(
            final_bytes == a || final_bytes == b,
            "the cache file is exactly one producer's whole payload, never a torn mix",
        );
    }
}

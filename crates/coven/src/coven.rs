//! Top-level API: open one handle and drive rows, blobs, sync, and
//! membership through it.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::blob::local_files::LocalBlobError;
use crate::blob::BlobTransitionObserver;
use crate::clock::{ClockRef, SystemClock};
use crate::config::{Config, HomeStorage};
use crate::custody::KeyCustody;
use crate::database::{Database, DbError, OpenError};
use crate::handle::CovenHandle;
use crate::identity_custody::IdentityCustody;
use crate::keys::StoreKeys;
use crate::migration::{Migration, MigrationError};
use crate::store_dir::PathTokenError;
use crate::store_sync::ConfigProvider;
use crate::sync::session::SyncedTable;

pub type CovenResult<T> = Result<T, CovenError>;

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
    #[error(
        "write failed: {write}; failed to remove installed local blobs during rollback: {rollback}"
    )]
    WriteRollbackFailed {
        write: Box<CovenError>,
        rollback: String,
    },
    #[error("write failed: {operation}; failed to remove unpublished local blobs: {cleanup}")]
    BlobCleanupFailed {
        operation: Box<CovenError>,
        cleanup: String,
    },
    #[error("synced_tables must be set before opening a coven store")]
    MissingSyncedTables,
    #[error("migrations must be set before opening a coven store")]
    MissingMigrations,
    #[error("candidate resolution failed: {0}")]
    CandidateResolution(String),
    #[error("blob_tombstone_grace must be a positive duration")]
    InvalidBlobTombstoneGrace,
    #[error("browsable cloud storage cannot be used with scoped table {table:?}")]
    BrowsableStorageWithScopedTable { table: String },
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
        CovenBuilder {
            config,
            synced_tables: None,
            migrations: None,
            blob_tombstone_grace: crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            blob_chunking: crate::storage::BlobChunking::DEFAULT,
            max_concurrent_uploads: NonZeroUsize::MIN,
            max_concurrent_downloads: NonZeroUsize::MIN,
            clock: Arc::new(SystemClock),
            key_custody: KeyCustody::Keyring,
            identity_custody: IdentityCustody::Keyring,
            oauth_clients: crate::oauth::OAuthClients::empty(),
            cloudkit_ops: None,
            observer: None,
        }
    }
}

pub struct CovenBuilder {
    config: CovenConfig,
    synced_tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    blob_tombstone_grace: chrono::Duration,
    max_concurrent_uploads: NonZeroUsize,
    max_concurrent_downloads: NonZeroUsize,
    clock: ClockRef,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_clients: crate::oauth::OAuthClients,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    blob_chunking: crate::storage::BlobChunking,
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

    /// How this installation seals and reads blobs: the plaintext chunk size a
    /// blob is sealed at, and how many stored bytes one ranged read spans.
    /// Defaults to [`BlobChunking::DEFAULT`](crate::storage::BlobChunking::DEFAULT).
    ///
    /// The chunk size applies to blobs this installation seals from here on. A
    /// blob already in the cloud records the size it was sealed at in its own
    /// header, and readers honor that, so changing this setting migrates
    /// nothing and installations set differently read each other's blobs.
    pub fn blob_chunking(mut self, chunking: crate::storage::BlobChunking) -> Self {
        self.blob_chunking = chunking;
        self
    }

    /// How many blob uploads the sync cycle's upload drain runs at once. Defaults
    /// to one (one at a time). A [`NonZeroUsize`] so a zero — which would leave the
    /// drain admitting nothing and never completing — cannot be set.
    pub fn max_concurrent_uploads(mut self, n: NonZeroUsize) -> Self {
        self.max_concurrent_uploads = n;
        self
    }

    /// How many blob downloads a [`pin`](CovenHandle::pin) call fetches at once.
    /// Defaults to one (one at a time). A [`NonZeroUsize`] so a zero — which would
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

    /// The OAuth applications this app uses for consumer cloud providers.
    pub fn oauth_clients(mut self, clients: crate::oauth::OAuthClients) -> Self {
        self.oauth_clients = clients;
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
        validate_storage_scope(&config, &tables)?;
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
        store_dir.remove_orphaned_blob_temps(std::time::SystemTime::now())?;
        let (db, stamper) = Database::open(
            &db_path,
            tables.clone(),
            self.blob_tombstone_grace,
            transfer_limits,
            config.device_id.clone(),
            self.clock.clone(),
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
            config.device_id.clone(),
            self.clock.clone(),
            &migrations,
        )?;
        let key_service = StoreKeys::bind(config.store_id.clone());
        let key_custody = self.key_custody.resolve(&key_service, &store_dir);
        let identity_custody = self.identity_custody.resolve(&key_service, &store_dir);
        Ok(CovenHandle::new(
            db,
            read_db,
            stamper,
            store_dir,
            provider,
            key_service,
            key_custody,
            identity_custody,
            self.oauth_clients,
            self.clock,
            self.cloudkit_ops,
            self.observer,
            open_guard,
            self.blob_chunking,
        ))
    }

    /// Open the store read-only for a same-store secondary reader: a separate
    /// process (or a second handle) that must read rows and blobs while another
    /// handle holds the full [`open`](Self::open). Returns a [`crate::CovenReadHandle`],
    /// whose surface is reads only — SQL queries and blob reads — with no write,
    /// sync, migration, or stamp API by construction.
    ///
    /// Unlike [`open`](Self::open) this takes no store lock (see
    /// `StoreOpenGuard`): it succeeds while a writer holds the exclusive lock,
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
        validate_storage_scope(&config, &tables)?;
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
            config.device_id.clone(),
            self.clock.clone(),
            &migrations,
        )?;
        let key_service = StoreKeys::bind(config.store_id.clone());
        let key_custody = self.key_custody.resolve(&key_service, &store_dir);
        let identity_custody = self.identity_custody.resolve(&key_service, &store_dir);
        Ok(crate::read_handle::CovenReadHandle::new(
            db,
            store_dir,
            provider,
            key_service,
            key_custody,
            identity_custody,
            self.oauth_clients,
            self.clock,
            self.cloudkit_ops,
            self.blob_chunking,
        ))
    }
}

fn validate_storage_scope(config: &Config, tables: &[SyncedTable]) -> CovenResult<()> {
    if config.cloud_home.storage == HomeStorage::Browsable {
        if let Some(table) = tables
            .iter()
            .find(|table| table.audience_column().is_some())
        {
            return Err(CovenError::BrowsableStorageWithScopedTable {
                table: table.name().to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{BlobRef, BlobScope, CacheFill, Provenance};
    use crate::config::Config;
    use crate::keys::test_keyring;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
        ExactSlotStorage, ObjectSlot, UploadProgress,
    };
    use crate::storage::CloudCipher;
    use crate::store_dir::StoreDir;
    use crate::sync::session::BlobDecl;
    use crate::sync::test_helpers::TestStore;
    use crate::{WriteId, WriteReceipt};
    use async_trait::async_trait;
    use rusqlite::{params, OptionalExtension};
    use std::sync::atomic::{AtomicBool, Ordering};
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

    async fn query_handle_text(handle: &CovenHandle, sql: &str) -> String {
        let sql = sql.to_string();
        handle
            .sql_read(move |connection| {
                connection
                    .query_row(&sql, [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("query text through the host read capability")
    }

    async fn handle_row_exists(handle: &CovenHandle, sql: &str) -> bool {
        let sql = sql.to_string();
        handle
            .sql_read(move |connection| {
                connection
                    .query_row(&sql, [], |_| Ok(true))
                    .optional()
                    .map(|value| value.unwrap_or(false))
                    .map_err(CovenError::from)
            })
            .await
            .expect("query row existence through the host read capability")
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
        SyncedTable::new("files", crate::RowIdentity::SharedKey).carries_blob(media_files_decl())
    }

    fn remote_root_files_table() -> SyncedTable {
        SyncedTable::new("files", crate::RowIdentity::SharedKey)
            .remote_root()
            .carries_blob(media_files_decl())
    }

    fn scoped_files_table() -> SyncedTable {
        SyncedTable::new("files", crate::RowIdentity::SharedKey)
            .scoped_by("audience")
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

    fn scoped_files_migration() -> Migration {
        Migration::sql(
            1,
            "scoped-files",
            "CREATE TABLE files (
                id TEXT PRIMARY KEY,
                blob_id TEXT,
                size INTEGER NOT NULL,
                hash TEXT,
                audience TEXT,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )
    }

    fn gated_roots_table() -> SyncedTable {
        SyncedTable::new("roots", crate::RowIdentity::SharedKey).gated_by("shared")
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
            .synced_tables(vec![gated_roots_table()])
            .migrations(vec![gated_roots_migration()])
            .open()
            .expect("open gated handle");
        (tmp, handle)
    }

    fn open_gated_roots_at(dir: StoreDir) -> CovenResult<CovenHandle> {
        Coven::builder(config(dir))
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
    fn precreated_empty_sqlite_file_initializes_coven_metadata() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        precreate_database(&dir, "");

        open_gated_roots_at(dir).expect("initialize Coven in an empty SQLite database");
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

        open_gated_roots_at(dir).expect("initialize Coven beside an existing host schema");
    }

    #[test]
    fn interrupted_coven_schema_without_a_marker_is_rejected() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        precreate_database(
            &dir,
            "CREATE TABLE protocol_state (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) STRICT;",
        );

        let error = match open_gated_roots_at(dir) {
            Ok(_) => panic!("partial Coven schema has no valid initialization commit"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            CovenError::Database(DbError::Message(reason))
                if reason.contains("without the required initialization marker")
        ));
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
            .write_changeset_for_test(&write_id)
            .await
            .expect("load gated changeset");
        let rows = crate::changeset::walk(&changeset).expect("walk gated changeset");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op, crate::changeset::ChangeOp::Insert);
        assert_eq!(rows[0].pk(), Some("root-1"));
        assert_eq!(rows[0].col(1), Some("Private"));
        assert_eq!(rows[0].col(2), Some("1"));
        assert_eq!(rows[0].col(3), Some("0000000002000-0000-device-test"));
    }

    fn open_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    #[tokio::test]
    async fn configured_clock_is_the_hlc_wall_source() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let clock = chrono::DateTime::from_timestamp_millis(1_234).expect("valid clock instant");
        let handle = Coven::builder(config(StoreDir::new(tmp.path())))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .clock(Arc::new(crate::clock::FixedClock(clock)))
            .open()
            .expect("open handle");

        let receipt = handle
            .sql(|sql| {
                let stamp = sql.stamp();
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('configured-clock', NULL, 0, ?1)",
                    [&stamp],
                )?;
                Ok(stamp)
            })
            .await
            .expect("write with configured clock");

        assert_eq!(receipt.value, "0000000001234-0000-device-test");
    }

    #[tokio::test]
    async fn second_open_of_one_store_is_refused_until_the_first_handle_drops() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let first = Coven::builder(config(dir.clone()))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("first open succeeds");
        let clone = first.clone();

        let second = Coven::builder(config(dir.clone()))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            second,
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ));

        drop(first);

        let still_locked = Coven::builder(config(dir.clone()))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            still_locked,
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ));

        drop(clone);

        Coven::builder(config(dir))
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
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    fn open_files_handle_in(dir: StoreDir) -> CovenHandle {
        try_open(&dir).expect("open handle")
    }

    async fn run_test_cycle(storage: &TestStore, handle: &CovenHandle) {
        handle
            .publish_test_store(storage)
            .await
            .expect("publish pending Store write");
    }

    async fn merge_test_storage(
        handle: &CovenHandle,
        keypair: &crate::keys::UserKeypair,
    ) -> TestStore {
        handle
            .create_test_store("lib-test", keypair.clone())
            .await
            .expect("create exact test Store")
    }

    async fn publish_current_writes(handle: &CovenHandle) {
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(handle, &keypair).await;
        run_test_cycle(&storage, handle).await;
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
        let storage = merge_test_storage(&reopened, &keypair).await;
        run_test_cycle(&storage, &reopened).await;

        let (_peer_tmp, peer) = open_files_handle();
        peer.pull_test_store(&storage).await;
        assert_eq!(
            query_handle_text(
                &peer,
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
            assert_eq!(receipt.status, crate::WriteStatus::Pending);
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
        assert_eq!(*first_status.borrow(), crate::WriteStatus::Pending);
        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&reopened, &keypair).await;
        run_test_cycle(&storage, &reopened).await;

        first_status.changed().await.expect("published status");
        let first_sequence = match &*first_status.borrow() {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("first host transaction is not published: {status:?}"),
        };
        assert_eq!(
            reopened
                .write_status(&write_ids[1])
                .await
                .expect("second status after first publication"),
            crate::WriteStatus::Pending,
        );
        run_test_cycle(&storage, &reopened).await;
        let second_sequence = match reopened
            .write_status(&write_ids[1])
            .await
            .expect("second status")
        {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("second host transaction is not published: {status:?}"),
        };
        assert_eq!(second_sequence, first_sequence + 1);
        assert!(reopened
            .pending_writes()
            .await
            .expect("published writes are not pending")
            .is_empty());

        let (_peer_tmp, peer) = open_files_handle();
        peer.pull_test_store(&storage).await;
        assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-pending-a'").await);
        assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-pending-b'").await);
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
        assert_eq!(receipt.status, crate::WriteStatus::LocalOnly);
        assert_eq!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("durable local status"),
            crate::WriteStatus::LocalOnly
        );
        assert!(handle
            .pending_writes()
            .await
            .expect("pending writes")
            .is_empty());
        let lease_count = handle
            .write_blob_lease_count_for_test(&receipt.write_id)
            .await
            .expect("count local-only blob leases");
        assert_eq!(lease_count, 0);

        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&handle, &keypair).await;
        run_test_cycle(&storage, &handle).await;
        assert_eq!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("local status after sync"),
            crate::WriteStatus::LocalOnly
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

        assert_eq!(receipt.status, crate::WriteStatus::Pending);
        let pending = handle.pending_writes().await.expect("pending mixed write");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].write_id, receipt.write_id);
        assert_eq!(
            pending[0].affected_rows,
            vec![crate::AffectedRow {
                table: "files".to_string(),
                primary_key: "shared-1".to_string(),
            }]
        );

        let keypair = crate::keys::UserKeypair::generate();
        let storage = merge_test_storage(&handle, &keypair).await;
        run_test_cycle(&storage, &handle).await;
        assert!(matches!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("published mixed write"),
            crate::WriteStatus::Published(_)
        ));

        let (_peer_tmp, peer) = open_files_handle();
        peer.pull_test_store(&storage).await;
        assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'shared-1'").await);
        assert!(
            !handle_row_exists(
                &peer,
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'local_notes'"
            )
            .await
        );
        assert!(handle_row_exists(&handle, "SELECT 1 FROM local_notes WHERE id = 'local-1'").await);
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
        let storage = merge_test_storage(&handle, &keypair).await;
        run_test_cycle(&storage, &handle).await;

        let (_peer_tmp, peer) = open_files_handle();
        peer.pull_test_store(&storage).await;
        assert!(
            handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-delete-reopen'").await,
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
        run_test_cycle(&storage, &reopened).await;

        peer.pull_test_store(&storage).await;
        assert!(
            !handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-delete-reopen'").await,
            "the delete changeset reaches the peer after reopening",
        );
    }

    #[tokio::test]
    async fn pending_write_drains_only_after_changeset_push() {
        let (_tmp, handle) = open_files_handle();
        let receipt = handle
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
        let storage = merge_test_storage(&handle, &keypair).await;
        storage.home.fail_exact_create_before_call(1);

        let first = handle.publish_test_store(&storage).await;
        assert!(
            first
                .as_ref()
                .is_err_and(|error| error.contains("forced failure before exact create")),
            "the first cycle must report the append failure while preserving its outbox: {first:?}",
        );
        assert_eq!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("write status after failed append"),
            crate::WriteStatus::Publishing,
        );

        run_test_cycle(&storage, &handle).await;
        assert!(
            matches!(
                handle
                    .write_status(&receipt.write_id)
                    .await
                    .expect("write status after retry"),
                crate::WriteStatus::Published(_)
            ),
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
        let mut staged = crate::local_blob::AtomicStagedFile::create(&final_path)
            .await
            .expect("allocate local blob stage");
        staged
            .write_bytes(b"interrupted write")
            .await
            .expect("write interrupted stage");
        let temp = staged.leave_unpublished_for_test();

        let _handle = Coven::builder(config(dir.clone()))
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
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        let bytes = b"piece-bytes".to_vec();
        let hash = crate::blob::content_hash(&bytes);
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
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params!["file-1", "blobaaaa", bytes.len() as i64, hash, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write row and blob");
        let path = dir
            .local_blob_path("media-files", "blobaaaa")
            .expect("local path");
        assert_eq!(
            std::fs::read(path).expect("read local blob"),
            b"piece-bytes"
        );
    }

    #[tokio::test]
    async fn orphaned_final_blob_is_replaced_by_next_write() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        let path = dir
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
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "file-orphan",
                            "orphaaaa",
                            15i64,
                            crate::blob::content_hash(b"committed bytes"),
                            sql.stamp()
                        ],
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
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        handle
            .write(
                |w| {
                    w.put_blob("media-files", "dupeaaaa", b"original".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "file-original",
                            "dupeaaaa",
                            8i64,
                            crate::blob::content_hash(b"original"),
                            sql.stamp()
                        ],
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
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "file-replacement",
                            "dupeaaaa",
                            11i64,
                            crate::blob::content_hash(b"replacement"),
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(CovenError::BlobAlreadyReferenced { .. })
        ));
        let path = dir
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
        let hash = crate::blob::content_hash(&bytes);

        handle
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
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "file-remote-root",
                            "rrhpaaaa",
                            bytes.len() as i64,
                            hash,
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write remote-root row and host-provided blob");
        let blob = handle
            .row_blob_ref("files", "file-remote-root")
            .await
            .expect("capture remote-root blob row");

        let whole = handle
            .read_blob(&blob)
            .await
            .expect("read_blob serves upload staging before sync upload");
        assert_eq!(
            whole, expected,
            "read_blob returns the bytes written through handle.write",
        );

        let (offset, len) = (12u64, 19u64);
        let stream = handle
            .open_blob_stream(&blob)
            .await
            .expect("open_blob_stream serves upload staging before sync upload");
        assert_eq!(stream.plaintext_size(), expected.len() as u64);
        let range = stream
            .read_at(offset, len)
            .await
            .expect("read a range from the opened stream");
        assert_eq!(
            range,
            &expected[offset as usize..(offset + len) as usize],
            "the stream returns the requested slice of the staged bytes",
        );
    }

    struct RemoteOnlyStoreBlob {
        _tmp: tempfile::TempDir,
        dir: StoreDir,
        handle: CovenHandle,
        store: TestStore,
        encryption: crate::EncryptionService,
        destination_circle: crate::CircleId,
        source_object: ObjectSlot,
    }

    async fn remote_only_store_blob() -> RemoteOnlyStoreBlob {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let signer = crate::keys::UserKeypair::generate();
        let encryption = crate::EncryptionService::from_key([42; 32]);
        let handle = Coven::builder(config(dir.clone()))
            .synced_tables(vec![scoped_files_table()])
            .migrations(vec![scoped_files_migration()])
            .key_custody(crate::KeyCustody::InMemory(encryption.clone().into()))
            .identity_custody(crate::IdentityCustody::InMemory(signer.clone()))
            .open()
            .expect("open scoped blob store");
        let store = handle
            .create_test_store("lib-test", signer)
            .await
            .expect("create exact test Store");
        let authority =
            rusqlite::Connection::open(dir.db_path()).expect("open Circle authority database");
        let (destination_circle, _) =
            crate::sync::test_helpers::install_test_active_circle(&authority, "blob-circle");
        drop(authority);

        let bytes = b"remote-only-circle-blob".to_vec();
        let hash = crate::blob::content_hash(&bytes);
        handle
            .write(
                {
                    let bytes = bytes.clone();
                    move |batch| {
                        batch.put_blob("media-files", "circleblob", bytes);
                        Ok(())
                    }
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files
                         (id, blob_id, size, hash, audience, _updated_at)
                         VALUES ('circle-file', 'circleblob', ?1, ?2, ?3, ?4)",
                        params![
                            bytes.len() as i64,
                            hash,
                            Option::<String>::None,
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write Store blob");
        handle
            .publish_test_store(&store)
            .await
            .expect("publish Store blob");
        let source_object = handle
            .row_blob_ref("files", "circle-file")
            .await
            .expect("capture published Store blob")
            .stored()
            .expect("published Store blob has exact storage")
            .object()
            .slot()
            .clone();
        std::fs::remove_file(
            dir.local_blob_path("media-files", "circleblob")
                .expect("local blob path"),
        )
        .expect("remove local plaintext to leave a remote-only source");

        RemoteOnlyStoreBlob {
            _tmp: tmp,
            dir,
            handle,
            store,
            encryption,
            destination_circle,
            source_object,
        }
    }

    fn outbound_blob_spools(dir: &StoreDir) -> std::collections::BTreeSet<PathBuf> {
        let path = dir.storage_dir().join("outbound-blobs");
        match std::fs::read_dir(path) {
            Ok(entries) => entries
                .map(|entry| entry.expect("read outbound blob spool entry").path())
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::collections::BTreeSet::new()
            }
            Err(error) => panic!("read outbound blob spools: {error}"),
        }
    }

    #[tokio::test]
    async fn audience_move_requires_remote_only_blob_before_committing_sql() {
        let fixture = remote_only_store_blob().await;
        ExactSlotStorage::delete_at(fixture.store.home.as_ref(), &fixture.source_object)
            .await
            .expect("remove the only remote source");
        fixture
            .handle
            .connect_sync_with_test_home(
                fixture.store.home.clone(),
                CloudCipher::Encrypted(fixture.encryption.clone()),
            )
            .await
            .expect("connect the exact test Store");

        let destination_circle_value = fixture.destination_circle.to_string();
        let result = fixture
            .handle
            .sql(move |sql| {
                sql.execute(
                    "UPDATE files SET audience = ?1, _updated_at = ?2
                     WHERE id = 'circle-file'",
                    params![destination_circle_value, sql.stamp()],
                )?;
                Ok(())
            })
            .await;

        assert!(
            result.is_err(),
            "a missing source blob must abort the audience move before SQLite commit",
        );
        let audience = fixture
            .handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read rolled-back audience");
        assert_eq!(audience, None);
    }

    #[tokio::test]
    async fn blob_audience_move_without_staging_rejects_and_rolls_back_sql() {
        let fixture = remote_only_store_blob().await;
        let destination_circle_value = fixture.destination_circle.to_string();
        let result = fixture
            .handle
            .execute_sql_with_blob_staging_for_test(
                None,
                format!(
                    "UPDATE files SET audience = '{destination_circle_value}',
                 _updated_at = '0000000009000-0000-device-test'
                 WHERE id = 'circle-file'"
                ),
            )
            .await;

        let error = result.expect_err("a blob audience move cannot omit materialization");
        assert!(
            error
                .to_string()
                .contains("BlobMoveRequiresMaterialization"),
            "{error}",
        );
        let audience = fixture
            .handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read rolled-back audience");
        assert_eq!(audience, None);
    }

    #[tokio::test]
    async fn missing_authorized_store_only_blocks_a_move_that_needs_it() {
        let fixture = remote_only_store_blob().await;

        fixture
            .handle
            .execute_sql_with_blob_staging_for_test(
                None,
                "UPDATE files SET _updated_at = '0000000009000-0000-device-test'
                 WHERE id = 'circle-file'"
                    .to_string(),
            )
            .await
            .expect("a write that does not move an audience needs no authorized Store");

        let destination_circle_value = fixture.destination_circle.to_string();
        let error = fixture
            .handle
            .execute_sql_with_blob_staging_for_test(
                None,
                format!(
                    "UPDATE files SET audience = '{destination_circle_value}',
                     _updated_at = '0000000010000-0000-device-test'
                     WHERE id = 'circle-file'"
                ),
            )
            .await
            .expect_err("a remote-only move must surface its adapter error");
        assert!(
            error
                .to_string()
                .contains("audience move staging is unavailable"),
            "{error}",
        );
        let audience = fixture
            .handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read audience after adapter failure");
        assert_eq!(audience, None);
    }

    #[tokio::test]
    async fn journal_failure_removes_only_the_audience_move_spool_and_rolls_back_sql() {
        let fixture = remote_only_store_blob().await;
        fixture
            .handle
            .connect_sync_with_test_home(
                fixture.store.home.clone(),
                CloudCipher::Encrypted(fixture.encryption.clone()),
            )
            .await
            .expect("connect the exact test Store");
        let before = outbound_blob_spools(&fixture.dir);
        rusqlite::Connection::open(fixture.dir.db_path())
            .expect("open database for journal fault")
            .execute_batch(
                "CREATE TRIGGER fail_audience_move_journal
                 BEFORE INSERT ON store_writes
                 BEGIN
                   SELECT RAISE(ABORT, 'injected Store write journal failure');
                 END;",
            )
            .expect("install Store write journal fault");

        let destination_circle_value = fixture.destination_circle.to_string();
        let result = fixture
            .handle
            .sql(move |sql| {
                sql.execute(
                    "UPDATE files SET audience = ?1, _updated_at = ?2
                     WHERE id = 'circle-file'",
                    params![destination_circle_value, sql.stamp()],
                )?;
                Ok(())
            })
            .await;

        assert!(result.is_err(), "the injected journal failure must surface");
        assert_eq!(
            outbound_blob_spools(&fixture.dir),
            before,
            "rollback removes the destination spool created by this attempt",
        );
        let audience = fixture
            .handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read rolled-back audience");
        assert_eq!(audience, None);
    }

    #[tokio::test]
    async fn local_audience_move_rolls_back_its_file_and_reuses_an_exact_leftover() {
        let fixture = remote_only_store_blob().await;
        fixture
            .handle
            .connect_sync_with_test_home(
                fixture.store.home.clone(),
                CloudCipher::Encrypted(fixture.encryption.clone()),
            )
            .await
            .expect("connect the exact test Store");
        let destination = fixture
            .dir
            .local_blob_path("media-files", "circleblob")
            .expect("resolve Local destination");
        let sibling = destination
            .parent()
            .expect("Local destination has a parent")
            .join("unrelated");
        std::fs::create_dir_all(sibling.parent().expect("sibling has a parent"))
            .expect("create Local blob directory");
        std::fs::write(&sibling, b"unrelated").expect("write unrelated Local file");
        rusqlite::Connection::open(fixture.dir.db_path())
            .expect("open database for Local journal fault")
            .execute_batch(
                "CREATE TRIGGER fail_local_audience_move_journal
                 BEFORE INSERT ON store_writes
                 BEGIN
                   SELECT RAISE(ABORT, 'injected Local Store write journal failure');
                 END;",
            )
            .expect("install Local Store write journal fault");

        let result = fixture
            .handle
            .sql(|sql| {
                sql.execute(
                    "UPDATE files SET audience = 'local', _updated_at = ?1
                     WHERE id = 'circle-file'",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await;
        assert!(result.is_err(), "the injected journal failure must surface");
        assert!(
            !destination.exists(),
            "rollback removes the Local file created by this attempt",
        );
        assert_eq!(
            std::fs::read(&sibling).expect("read unrelated Local file"),
            b"unrelated",
        );
        let audience = fixture
            .handle
            .sql_read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read rolled-back Local audience");
        assert_eq!(audience, None);
        assert!(matches!(
            fixture
                .handle
                .row_blob_ref("files", "circle-file")
                .await
                .expect("load blob after failed Local move")
                .authority(),
            crate::blob::RowBlobAuthority::Remote(_)
        ));

        rusqlite::Connection::open(fixture.dir.db_path())
            .expect("open database to remove Local journal fault")
            .execute_batch("DROP TRIGGER fail_local_audience_move_journal")
            .expect("remove Local Store write journal fault");
        std::fs::write(&destination, b"remote-only-circle-blob")
            .expect("model an exact file left by failed cleanup");
        fixture
            .handle
            .sql(|sql| {
                sql.execute(
                    "UPDATE files SET audience = 'local', _updated_at = ?1
                     WHERE id = 'circle-file'",
                    [sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("retry accepts the exact already-materialized Local file");
        assert_eq!(
            std::fs::read(&destination).expect("read retried Local destination"),
            b"remote-only-circle-blob",
        );
        assert_eq!(
            std::fs::read(&sibling).expect("read unrelated file after retry"),
            b"unrelated",
        );
    }

    #[tokio::test]
    async fn audience_move_publishes_from_precommit_spool_after_source_disappears() {
        let fixture = remote_only_store_blob().await;
        fixture
            .handle
            .connect_sync_with_test_home(
                fixture.store.home.clone(),
                CloudCipher::Encrypted(fixture.encryption.clone()),
            )
            .await
            .expect("connect the exact test Store");

        // The fabricated destination Circle names its fixed owner in its roster;
        // that identity must be an active Store member or the Circle would be
        // rotation-required and reject new content.
        fixture
            .handle
            .invite_member(
                &crate::keys::public_key_hex(
                    &crate::sync::test_helpers::test_circle_owner_keypair(),
                ),
                None,
                crate::MemberRole::Member,
            )
            .await
            .expect("register the fabricated Circle roster owner as a Store member");

        let destination_circle_value = fixture.destination_circle.to_string();
        let receipt = fixture
            .handle
            .sql(move |sql| {
                sql.execute(
                    "UPDATE files SET audience = ?1, _updated_at = ?2
                     WHERE id = 'circle-file'",
                    params![destination_circle_value, sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("commit audience move after staging its destination blob");
        let write_id = receipt.write_id.to_string();
        let blob_facts = fixture
            .handle
            .sql_read(move |conn| {
                conn.query_row(
                    "SELECT blob_facts FROM store_writes WHERE write_id = ?1",
                    [write_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(CovenError::from)
            })
            .await
            .expect("read durable move blob facts");
        let blob_facts: serde_json::Value =
            serde_json::from_str(&blob_facts).expect("decode move blob facts");
        let spool_path = blob_facts["blobs"][0]["audience_move"]["remote"]["spool_path"]
            .as_str()
            .expect("move fact records its exact destination spool");
        assert!(
            std::path::Path::new(spool_path).is_file(),
            "the destination spool is durable before the SQL write returns",
        );

        ExactSlotStorage::delete_at(fixture.store.home.as_ref(), &fixture.source_object)
            .await
            .expect("remove source after the move commits");
        fixture
            .handle
            .publish_test_store(&fixture.store)
            .await
            .expect("publish the move from its durable destination spool");
        assert!(
            !std::path::Path::new(spool_path).exists(),
            "prepared-object completion retires the durable precommit spool",
        );
        assert!(
            !fixture
                .handle
                .publish_test_store(&fixture.store)
                .await
                .expect("retry completed move publication"),
            "a completed move has nothing left to publish",
        );
        let published = fixture
            .handle
            .row_blob_ref("files", "circle-file")
            .await
            .expect("capture published destination Circle blob");
        assert_eq!(
            published.authority().audience(),
            crate::Audience::Circle(fixture.destination_circle),
        );
    }

    #[tokio::test]
    async fn public_materialization_survives_store_reopen_without_a_cloud_connection() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(
                    run_public_materialization_survives_store_reopen_without_a_cloud_connection(),
                )
                .await
                .expect("public materialization task");
            })
            .await;
    }

    async fn run_public_materialization_survives_store_reopen_without_a_cloud_connection() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let signer = crate::keys::UserKeypair::generate();
        let keyring = crate::MasterKeyring::from(crate::EncryptionService::from_key([42; 32]));
        let open = || {
            Coven::builder(config(dir.clone()))
                .synced_tables(vec![remote_root_files_table()])
                .migrations(vec![files_migration()])
                .key_custody(crate::KeyCustody::InMemory(keyring.clone()))
                .identity_custody(crate::IdentityCustody::InMemory(signer.clone()))
                .open()
                .expect("open remote-root store")
        };
        let handle = open();
        let store = handle
            .create_test_store("lib-test", signer.clone())
            .await
            .expect("create exact test Store");
        handle
            .connect_sync_with_test_home(
                store.home.clone(),
                CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
            )
            .await
            .expect("connect exact test Store");

        let expected = b"public materialized blob".to_vec();
        let bytes = expected.clone();
        let hash = crate::blob::content_hash(&bytes);
        let receipt = handle
            .write(
                {
                    let bytes = bytes.clone();
                    move |batch| {
                        batch.put_blob("media-files", "materialized-blob", bytes);
                        Ok(())
                    }
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "materialized-row",
                            "materialized-blob",
                            bytes.len() as i64,
                            hash,
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write remote-root blob");
        let mut status = handle
            .subscribe_write_status(&receipt.write_id)
            .await
            .expect("subscribe to materialized write");
        handle.sync_now();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let current = status.borrow().clone();
                match current {
                    crate::WriteStatus::Published(_) => break,
                    crate::WriteStatus::Pending | crate::WriteStatus::Publishing => {
                        status.changed().await.expect("write status remains open")
                    }
                    other => panic!("materialized write did not publish: {other:?}"),
                }
            }
        })
        .await
        .expect("materialized write publishes");
        let reference = handle
            .row_blob_ref("files", "materialized-row")
            .await
            .expect("capture published row blob");
        handle
            .materialize_row_blob(&reference)
            .await
            .expect("materialize through the public handle");
        let locator_hash = reference
            .stored()
            .expect("published row has exact storage")
            .locator()
            .locator_hash();
        let cached = dir
            .cache_blob_path("media-files", locator_hash)
            .expect("exact cache path");
        assert_eq!(std::fs::read(&cached).expect("read exact cache"), expected);

        handle.disconnect_sync();
        drop(handle);
        let reopened = open();
        let reopened_reference = reopened
            .row_blob_ref("files", "materialized-row")
            .await
            .expect("capture reopened row blob");
        reopened
            .materialize_row_blob(&reopened_reference)
            .await
            .expect("reopen verifies the materialized cache without cloud storage");
    }

    #[tokio::test]
    async fn sql_failure_removes_staged_blob() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
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
        let path = dir
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
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        crate::blob::local_files::store(&dir, "media-files", "oldaaaa", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                    params![
                        "file-1",
                        "oldaaaa",
                        crate::blob::content_hash(b"old"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        crate::blob::local_files::store(&dir, "media-files", "oldaaaa", b"old")
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
                        "UPDATE files SET blob_id = ?1, size = 3, hash = ?2, \
                         _updated_at = ?3 WHERE id = 'file-1'",
                        params!["newaaaa", crate::blob::content_hash(b"new"), sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("replace blob");
        assert!(!dir
            .local_blob_path("media-files", "oldaaaa")
            .expect("old path")
            .exists());
        assert!(dir
            .local_blob_path("media-files", "newaaaa")
            .expect("new path")
            .exists());
    }

    async fn queue_replacement_before_sync(
        handle: &CovenHandle,
        store_dir: &StoreDir,
    ) -> (WriteId, WriteId, std::path::PathBuf) {
        let first = handle
            .write(
                |batch| {
                    batch.put_blob("media-files", "ownedaaa", b"first".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('owned-file', 'ownedaaa', 5, ?1, ?2)",
                        params![crate::blob::content_hash(b"first"), sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("queue first blob write");
        let first_path = store_dir
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
                        "UPDATE files SET blob_id = 'ownedbbb', size = 6, hash = ?1, \
                         _updated_at = ?2 \
                         WHERE id = 'owned-file'",
                        params![crate::blob::content_hash(b"second"), sql.stamp()],
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
        let storage = merge_test_storage(handle, &keypair).await;
        run_test_cycle(&storage, handle).await;

        let first_sequence = match handle
            .write_status(first_write)
            .await
            .expect("first status")
        {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("first replacement write is not published: {status:?}"),
        };
        assert_eq!(
            handle
                .write_status(second_write)
                .await
                .expect("second status after first publication"),
            crate::WriteStatus::Pending,
            "one Store publication consumes one pending host transaction",
        );

        run_test_cycle(&storage, handle).await;
        let second_sequence = match handle
            .write_status(second_write)
            .await
            .expect("second status")
        {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("second replacement write is not published: {status:?}"),
        };
        assert_eq!(
            second_sequence,
            first_sequence + 1,
            "replacement writes publish in their host transaction order"
        );
        let blob_objects = storage
            .home
            .keys()
            .into_iter()
            .filter(|key| key.starts_with("media-files/opaque/"))
            .count();
        assert_eq!(blob_objects, 2, "both row versions upload their blob bytes");
        assert!(
            !first_path.exists(),
            "the first write releases its local bytes after publication"
        );
    }

    #[tokio::test]
    async fn pending_write_owns_blob_bytes_until_its_publication() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_pending_write_owns_blob_bytes_until_its_publication())
                    .await
                    .expect("pending-write blob ownership test task");
            })
            .await;
    }

    async fn run_pending_write_owns_blob_bytes_until_its_publication() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        let (first_write, second_write, first_path) =
            queue_replacement_before_sync(&handle, &dir).await;

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
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('overwrite', 'ownedaaa', 11, ?1, ?2)",
                        params![crate::blob::content_hash(b"overwritten"), sql.stamp()],
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
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                tokio::task::spawn_local(run_pending_write_blob_ownership_survives_restart())
                    .await
                    .expect("reopened pending-write blob ownership test task");
            })
            .await;
    }

    async fn run_pending_write_blob_ownership_survives_restart() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        let handle = open_files_handle_in(dir.clone());
        let (first_write, second_write, first_path) =
            queue_replacement_before_sync(&handle, &dir).await;
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
    async fn author_delete_drops_all_local_blob_copies() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        crate::blob::local_files::store(&dir, "media-files", "oldcccc", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                    params![
                        "file-1",
                        "oldcccc",
                        crate::blob::content_hash(b"old"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        let published = handle
            .row_blob_ref("files", "file-1")
            .await
            .expect("capture exact published blob");
        let locator_hash = published
            .stored()
            .expect("published row has exact storage")
            .locator()
            .locator_hash();
        let pinned = dir
            .pinned_blob_path("media-files", locator_hash)
            .expect("pinned path");
        let cached = dir
            .cache_blob_path("media-files", locator_hash)
            .expect("cache path");
        crate::blob::local_files::store(&dir, "media-files", "oldcccc", b"old")
            .await
            .expect("restore published blob locally");
        write_raw_file(&pinned, b"old").await;
        write_raw_file(&cached, b"old").await;
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
                        "UPDATE files SET blob_id = NULL, size = 0, hash = NULL, _updated_at = ?1 \
                         WHERE id = 'file-1'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("delete blob");

        assert!(!dir
            .local_blob_path("media-files", "oldcccc")
            .expect("local path")
            .exists());
        assert!(!pinned.exists(), "pinned copy is removed");
        assert!(!cached.exists(), "cache copy is removed");
    }

    #[tokio::test]
    async fn failed_local_blob_cleanup_keeps_intent_for_later_drain() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        crate::blob::local_files::store(&dir, "media-files", "oldddddd", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                    params![
                        "file-1",
                        "oldddddd",
                        crate::blob::content_hash(b"old"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
        publish_current_writes(&handle).await;
        let published = handle
            .row_blob_ref("files", "file-1")
            .await
            .expect("capture exact published blob");
        let locator_hash = published
            .stored()
            .expect("published row has exact storage")
            .locator()
            .locator_hash();
        let pinned = dir
            .pinned_blob_path("media-files", locator_hash)
            .expect("pinned path");
        crate::blob::local_files::store(&dir, "media-files", "oldddddd", b"old")
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
                        "UPDATE files SET blob_id = NULL, size = 0, hash = NULL, _updated_at = ?1 \
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
            2
        );
        assert!(dir
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
        assert!(!dir
            .local_blob_path("media-files", "oldddddd")
            .expect("local path")
            .exists());
    }

    #[tokio::test]
    async fn write_drain_separates_live_local_source_from_deleted_exact_cache() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        let blob_id = "shared01";
        handle
            .sql(move |sql| {
                let hash = crate::blob::content_hash(b"live");
                for id in ["remote-deletes", "still-live"] {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, 'shared01', 4, ?2, \
                                 '0000000001000-0000-dev-remote')",
                        params![id, &hash],
                    )?;
                }
                Ok(())
            })
            .await
            .expect("seed two rows sharing the blob");

        let local = dir
            .local_blob_path("media-files", blob_id)
            .expect("local path");
        write_raw_file(&local, b"live").await;

        let (_source_tmp, source) = open_files_handle();
        let storage = Arc::new(
            source
                .create_test_store("lib-test", crate::keys::UserKeypair::generate())
                .await
                .expect("create remote exact test Store"),
        );
        source
            .write(
                |batch| {
                    batch.put_blob("media-files", "shared01", b"live".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('remote-deletes', 'shared01', 4, ?1, ?2)",
                        params![crate::blob::content_hash(b"live"), sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("insert remote row");
        source
            .publish_test_store(storage.as_ref())
            .await
            .expect("publish remote insert");
        let remote_reference = source
            .row_blob_ref("files", "remote-deletes")
            .await
            .expect("capture exact remote blob");
        let locator_hash = remote_reference
            .stored()
            .expect("published row has exact storage")
            .locator()
            .locator_hash();
        let pinned = dir
            .pinned_blob_path("media-files", locator_hash)
            .expect("pinned path");
        let cached = dir
            .cache_blob_path("media-files", locator_hash)
            .expect("cache path");
        write_raw_file(&pinned, b"live").await;
        write_raw_file(&cached, b"live").await;
        let delete = source
            .sql(|sql| {
                sql.execute("DELETE FROM files WHERE id = 'remote-deletes'", [])?;
                Ok(())
            })
            .await
            .expect("delete remote row");
        source
            .publish_test_store(storage.as_ref())
            .await
            .expect("publish remote delete");

        assert!(matches!(
            source
                .write_status(&delete.write_id)
                .await
                .expect("remote delete status"),
            crate::WriteStatus::Published(_)
        ));
        let (device_id, sequence) = source
            .latest_materialized_commit_coordinate_for_test()
            .await
            .expect("load remote delete stream coordinate");

        let (commit_reached, resume_pull) =
            handle.arm_pull_after_remote_commit_for_test(device_id, sequence);
        let pull_handle = handle.clone();
        let pull_storage = storage.clone();
        let pull =
            tokio::spawn(async move { pull_handle.pull_test_store(pull_storage.as_ref()).await });

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
            !handle_row_exists(&handle, "SELECT 1 FROM files WHERE id = 'remote-deletes'").await
        );
        assert!(handle_row_exists(&handle, "SELECT 1 FROM files WHERE id = 'still-live'").await);
        assert!(local.exists(), "the live blob's local copy survives");
        assert!(
            !pinned.exists(),
            "the deleted locator's pinned copy is removed"
        );
        assert!(
            !cached.exists(),
            "the deleted locator's cache copy is removed"
        );
        assert_eq!(
            cleanup_intent_count(&handle, "media-files", blob_id).await,
            0,
        );
    }

    #[tokio::test]
    async fn replacement_is_rejected_while_sql_still_references_old_blob() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        crate::blob::local_files::store(&dir, "media-files", "oldbbbb", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                    params![
                        "file-1",
                        "oldbbbb",
                        crate::blob::content_hash(b"old"),
                        sql.stamp()
                    ],
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
        assert!(dir
            .local_blob_path("media-files", "oldbbbb")
            .expect("old path")
            .exists());
        assert!(!dir
            .local_blob_path("media-files", "newbbbb")
            .expect("new path")
            .exists());
    }

    #[tokio::test]
    async fn sql_panic_removes_moved_blob() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
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
        assert!(!dir
            .local_blob_path("media-files", "panicccc")
            .expect("panic path")
            .exists());
    }

    #[tokio::test]
    async fn write_surfaces_a_failed_installed_blob_rollback() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
        let final_path = dir
            .local_blob_path("media-files", "rollback-failure")
            .expect("rollback failure path");
        let obstructed_path = final_path.clone();

        let result: CovenResult<WriteReceipt<()>> = handle
            .write(
                |write| {
                    write.put_blob("media-files", "rollback-failure", b"new".to_vec());
                    Ok(())
                },
                move |_sql| {
                    std::fs::remove_file(&obstructed_path)
                        .expect("replace installed blob with rollback obstruction");
                    std::fs::create_dir(&obstructed_path)
                        .expect("create rollback obstruction directory");
                    Err(CovenError::Blob("force rollback".to_string()))
                },
            )
            .await;

        let error = result.expect_err("the write and its blob rollback both fail");
        assert!(error.to_string().contains("force rollback"), "{error}");
        assert!(
            error
                .to_string()
                .contains("failed to remove installed local blobs during rollback"),
            "{error}"
        );
        assert!(final_path.is_dir(), "rollback obstruction remains visible");
    }

    #[tokio::test]
    async fn concurrent_duplicate_blob_write_does_not_delete_committed_blob() {
        let (tmp, handle) = open_files_handle();
        let dir = StoreDir::new(tmp.path());
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
                            "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![
                                "winner",
                                "raceblob",
                                9i64,
                                crate::blob::content_hash(b"committed"),
                                sql.stamp()
                            ],
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

        let path = dir
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
        fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
            Some(self)
        }

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

    #[async_trait]
    impl ExactSlotStorage for GateCloudHome {
        async fn provider_binding(
            &self,
        ) -> Result<crate::storage::ResolvedProviderBinding, CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::provider_binding(&self.inner).await
        }

        async fn allocate_slot(&self, key: &str) -> Result<ObjectSlot, CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::allocate_slot(&self.inner, key).await
        }

        async fn create_at(
            &self,
            slot: &ObjectSlot,
            body: BlobBody,
            progress: &UploadProgress<'_>,
        ) -> Result<(), CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::create_at(&self.inner, slot, body, progress).await
        }

        async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::read_at(&self.inner, slot).await
        }

        async fn read_range_at(
            &self,
            slot: &ObjectSlot,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::read_range_at(&self.inner, slot, start, end).await
        }

        async fn read_at_to_file(
            &self,
            slot: &ObjectSlot,
            destination: &std::path::Path,
        ) -> Result<(), crate::storage::cloud::CloudFileReadError> {
            self.gate().await;
            ExactSlotStorage::read_at_to_file(&self.inner, slot, destination).await
        }

        async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
            self.gate().await;
            ExactSlotStorage::delete_at(&self.inner, slot).await
        }
    }

    fn try_open(dir: &StoreDir) -> CovenResult<CovenHandle> {
        Coven::builder(config(dir.clone()))
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
        let connect_handle = handle.clone();
        tokio::spawn(async move {
            connect_handle
                .connect_sync_with_test_home(
                    home,
                    CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
                )
                .await
        })
        .await
        .expect("gating test connection task completes")
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
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");
        handle
            .initialize_identity()
            .expect("establish this store's identity before connecting");
        let connect_handle = handle.clone();
        tokio::spawn(async move {
            connect_handle
                .connect_sync_with_test_home(
                    Arc::new(InMemoryCloudHome::new()),
                    CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
                )
                .await
        })
        .await
        .expect("shutdown test connection task completes")
        .expect("connect over the test home");

        drop(handle);

        let _reopened = open_when_lock_released(&dir).await;
    }

    // ========================================================================
    // Read-only opens (CovenReadHandle)
    // ========================================================================

    fn try_open_read_only(dir: &StoreDir) -> CovenResult<crate::read_handle::CovenReadHandle> {
        Coven::builder(config(dir.clone()))
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
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open writer");

        let bytes = b"read-only-handle-serves-these-blob-bytes".to_vec();
        let hash = crate::blob::content_hash(&bytes);
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
                            "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params!["file-ro", "roblob01", len, hash, sql.stamp()],
                        )?;
                        Ok(())
                    }
                },
            )
            .await
            .expect("writer stores the row and blob");

        let reader = Coven::builder(config(dir))
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open_read_only()
            .expect("read-only open");

        let blob = reader
            .row_blob_ref("files", "file-ro")
            .await
            .expect("capture read-only blob row");
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
            .open_blob_stream(&blob)
            .await
            .expect("open a stream via read handle")
            .read_at(offset, len)
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
            .cache_blob_path(
                "media-files",
                crate::protocol::store_commit::ObjectHash::digest(b"raceblob"),
            )
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

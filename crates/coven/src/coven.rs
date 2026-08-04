//! Top-level API: open one handle and drive rows, blobs, sync, and
//! membership through it.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::clock::{ClockRef, SystemClock};
use crate::config::{Config, HomeStorage};
use crate::custody::KeyCustody;
use crate::database::{Database, DbError, OpenError};
use crate::handle::CovenHandle;
use crate::identity_custody::IdentityCustody;
use crate::keys::StoreKeys;
use crate::protocol::blob::BlobTransitionObserver;
use crate::protocol::synced_schema::SyncedTable;
use crate::store_dir::StoreOpenGuard;
use crate::store_dir::{LocalBlobStoreError, PathTokenError};
use crate::store_sync::ConfigProvider;
use crate::{Migration, MigrationError};

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

impl From<LocalBlobStoreError> for CovenError {
    fn from(value: LocalBlobStoreError) -> Self {
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
            blob_tombstone_grace: crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
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

impl From<crate::store_dir::StoreOpenGuardError> for CovenError {
    fn from(error: crate::store_dir::StoreOpenGuardError) -> Self {
        match error {
            crate::store_dir::StoreOpenGuardError::AlreadyOpen { store_dir } => {
                CovenError::AlreadyOpen { store_dir }
            }
            crate::store_dir::StoreOpenGuardError::MalformedPath(message) => {
                CovenError::MalformedPath(message)
            }
            crate::store_dir::StoreOpenGuardError::Io(error) => CovenError::Io(error),
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
    /// [`crate::protocol::blob::BLOB_TOMBSTONE_GRACE`]. Must be positive — a
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
        let transfer_limits = crate::protocol::blob::TransferLimits {
            uploads: self.max_concurrent_uploads,
            downloads: self.max_concurrent_downloads,
        };
        let open_guard = Arc::new(StoreOpenGuard::acquire(&store_dir)?);
        store_dir.remove_orphaned_blob_temps(self.clock.now().into())?;
        let db = Database::open(
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
            crate::protocol::blob::TransferLimits {
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
#[path = "coven_tests.rs"]
mod tests;

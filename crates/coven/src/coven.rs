//! Native top-level API: open one handle and drive rows, blobs, sync, and
//! membership through it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::{debug, warn};

use crate::blob::local_files::LocalBlobError;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::{ClockRef, SystemClock};
use crate::config::Config;
use crate::database::{Database, DbError};
use crate::handle::CovenHandle;
use crate::keys::KeyService;
use crate::library_dir::PathTokenError;
use crate::migration::Migration;
use crate::sync::hlc::UpdatedAtStamper;
use crate::sync::session::SyncedTable;
use crate::sync::sync_manager::ConfigProvider;

pub type CovenResult<T> = Result<T, CovenError>;

const LOCAL_STAGE_MARKER: &str = ".coven-stage-";

#[derive(Debug, thiserror::Error)]
pub enum CovenError {
    #[error("database error: {0}")]
    Database(#[from] DbError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("blob error: {0}")]
    Blob(String),
    #[error("synced_tables must be set before opening a coven library")]
    MissingSyncedTables,
    #[error("migrations must be set before opening a coven library")]
    MissingMigrations,
    #[error("blob {namespace}/{id} is still referenced by a row after the write")]
    BlobStillReferenced { namespace: String, id: String },
    #[error("blob {namespace}/{id} is already referenced by a row")]
    BlobAlreadyReferenced { namespace: String, id: String },
    #[error("library is already open: {}", library_dir.display())]
    AlreadyOpen { library_dir: PathBuf },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<LocalBlobError> for CovenError {
    fn from(value: LocalBlobError) -> Self {
        CovenError::Blob(value.to_string())
    }
}

impl From<PathTokenError> for CovenError {
    fn from(value: PathTokenError) -> Self {
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
            clock: Arc::new(SystemClock),
            key_service: KeyService::new(current.library_id),
            cloudkit_ops: None,
            observer: None,
        }
    }
}

pub struct CovenBuilder {
    config: CovenConfig,
    synced_tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    clock: ClockRef,
    key_service: KeyService,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
}

pub(crate) struct LibraryOpenGuard {
    _file: std::fs::File,
}

impl LibraryOpenGuard {
    pub(crate) fn acquire(library_dir: &crate::library_dir::LibraryDir) -> CovenResult<Self> {
        let db_path = library_dir.db_path();
        let Some(dir) = db_path.parent() else {
            return Err(CovenError::Blob(format!(
                "library database path has no parent: {}",
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
                library_dir: dir.to_path_buf(),
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

    pub fn key_service(mut self, key_service: KeyService) -> Self {
        self.key_service = key_service;
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

    pub fn open(self) -> CovenResult<CovenHandle> {
        crate::install_platform();
        let config = self.config.current();
        let tables = self.synced_tables.ok_or(CovenError::MissingSyncedTables)?;
        let migrations = self.migrations.ok_or(CovenError::MissingMigrations)?;
        let db_path = config.library_dir.db_path();
        let provider = self.config.provider();
        let library_dir = config.library_dir.clone();
        let open_guard = Arc::new(LibraryOpenGuard::acquire(&library_dir)?);
        remove_orphaned_local_blob_temps(&library_dir)?;
        let (db, stamper) =
            Database::open(&db_path, tables, config.device_id.clone(), &migrations)?;
        Ok(CovenHandle::new(
            db,
            stamper,
            library_dir,
            provider,
            self.key_service,
            self.clock,
            self.cloudkit_ops,
            self.observer,
            open_guard,
        ))
    }
}

pub struct SqlContext<'ctx, 'conn> {
    tx: &'ctx rusqlite::Transaction<'conn>,
    stamper: UpdatedAtStamper,
}

impl<'ctx, 'conn> SqlContext<'ctx, 'conn> {
    pub(crate) fn new(tx: &'ctx rusqlite::Transaction<'conn>, stamper: UpdatedAtStamper) -> Self {
        Self { tx, stamper }
    }

    pub fn tx(&self) -> &'ctx rusqlite::Transaction<'conn> {
        self.tx
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

pub(crate) enum WriteDbOutcome<R> {
    Committed(R),
    RolledBack { error: CovenError },
}

impl CovenHandle {
    pub async fn sql<F, R>(&self, f: F) -> CovenResult<R>
    where
        F: for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let stamper = self.stamper();
        self.db()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let value = f(SqlContext::new(&tx, stamper)).map_err(|e| DbError(e.to_string()))?;
                tx.commit().map_err(DbError::from)?;
                Ok(value)
            })
            .await
            .map_err(CovenError::from)
    }

    pub async fn write<F, S, R>(&self, f: F, sql: S) -> CovenResult<R>
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
        let deleted = batch.deleted_blobs;
        let library_dir = self.library_dir();
        let outcome = match db
            .call(move |conn| {
                Ok(run_write_batch_on_connection(
                    conn,
                    stamper,
                    library_dir,
                    staged,
                    deleted,
                    tables,
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
            WriteDbOutcome::Committed(value) => {
                if let Err(error) =
                    drain_local_cleanup_intents(self.db(), &self.library_dir()).await
                {
                    warn!(
                        error = %error,
                        "failed to drain local blob cleanup intents after write commit"
                    );
                }
                Ok(value)
            }
            WriteDbOutcome::RolledBack { error } => {
                remove_staged_paths(&staged_paths).await;
                Err(error)
            }
        }
    }

    async fn stage_blobs(&self, blobs: Vec<NewBlob>) -> CovenResult<Vec<StagedBlob>> {
        let mut staged = Vec::new();
        for blob in blobs {
            let final_path = self
                .library_dir()
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
            CovenError::Blob(format!(
                "local blob path has no file name: {}",
                final_path.display()
            ))
        })?;
    Ok(final_path.with_file_name(format!(
        "{file_name}{LOCAL_STAGE_MARKER}{}",
        uuid::Uuid::new_v4()
    )))
}

fn remove_orphaned_local_blob_temps(
    library_dir: &crate::library_dir::LibraryDir,
) -> CovenResult<()> {
    remove_orphaned_local_blob_temps_in_dir(&library_dir.storage_dir().join("local"))
}

fn remove_orphaned_local_blob_temps_in_dir(dir: &Path) -> CovenResult<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!(
                path = %dir.display(),
                "local blob directory absent during orphaned staging cleanup"
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
            remove_orphaned_local_blob_temps_in_dir(&path)?;
        } else if file_type.is_file() && is_local_stage_temp(&path) {
            remove_file_if_present(&path)?;
        } else if file_type.is_file() && path.file_name().and_then(|name| name.to_str()).is_none() {
            debug!(
                path = %path.display(),
                "skipping local blob path with non-utf8 file name during orphaned staging cleanup"
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
        CovenError::Blob(format!("path has no parent directory: {}", path.display()))
    })?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn record_local_cleanup_intents(
    tx: &rusqlite::Transaction<'_>,
    library_dir: &crate::library_dir::LibraryDir,
    deleted: &[BlobRef],
) -> CovenResult<()> {
    for blob in deleted {
        let _ = library_dir.local_blob_path(&blob.namespace, &blob.id)?;
        let _ = library_dir.pinned_blob_path(&blob.namespace, &blob.id)?;
        let _ = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;
        let pending: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM local_cleanup_intents WHERE namespace = ?1 AND blob_id = ?2",
                rusqlite::params![blob.namespace, blob.id],
                |row| row.get(0),
            )
            .optional()?;
        if pending.is_some() {
            debug!(
                namespace = %blob.namespace,
                blob_id = %blob.id,
                "local blob cleanup intent is already pending"
            );
        } else {
            tx.execute(
                "INSERT INTO local_cleanup_intents (namespace, blob_id) VALUES (?1, ?2)",
                rusqlite::params![blob.namespace, blob.id],
            )?;
        }
    }
    Ok(())
}

async fn drain_local_cleanup_intents(
    db: &Database,
    library_dir: &crate::library_dir::LibraryDir,
) -> CovenResult<()> {
    let intents = db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT namespace, blob_id FROM local_cleanup_intents ORDER BY namespace, blob_id",
            )?;
            let intents = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(intents)
        })
        .await?;

    for (namespace, id) in intents {
        match crate::blob::cache::drop_all_local_copies(library_dir, &namespace, &id).await {
            Ok(()) => {
                let delete_namespace = namespace.clone();
                let delete_id = id.clone();
                if let Err(error) = db
                    .call(move |conn| {
                        conn.execute(
                            "DELETE FROM local_cleanup_intents WHERE namespace = ?1 AND blob_id = ?2",
                            rusqlite::params![delete_namespace, delete_id],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                    })
                    .await
                {
                    warn!(
                        namespace = %namespace,
                        blob_id = %id,
                        error = %error,
                        "failed to clear local blob cleanup intent"
                    );
                }
            }
            Err(error) => {
                warn!(
                    namespace = %namespace,
                    blob_id = %id,
                    error = %error,
                    "local blob cleanup intent remains pending"
                );
            }
        }
    }
    Ok(())
}

fn run_write_batch_on_connection<R>(
    conn: &Connection,
    stamper: UpdatedAtStamper,
    library_dir: crate::library_dir::LibraryDir,
    staged: Vec<StagedBlob>,
    deleted: Vec<BlobRef>,
    tables: Vec<SyncedTable>,
    sql: WriteSql<R>,
) -> WriteDbOutcome<R> {
    let tx = match conn.unchecked_transaction() {
        Ok(tx) => tx,
        Err(e) => {
            return WriteDbOutcome::RolledBack {
                error: CovenError::from(e),
            }
        }
    };
    let mut moved = Vec::new();
    let decls = match crate::blob::decl::BlobDecls::from_tables(&tx, &tables)
        .map_err(|e| CovenError::Blob(e.to_string()))
    {
        Ok(decls) => decls,
        Err(e) => {
            return rollback_write_batch(e, moved);
        }
    };
    for blob in &staged {
        match decls.row_for_blob_in_namespace(&tx, &blob.namespace, &blob.id) {
            Ok(Some(_)) => {
                return rollback_write_batch(
                    CovenError::BlobAlreadyReferenced {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    },
                    moved,
                );
            }
            Ok(None) => {}
            Err(e) => {
                return rollback_write_batch(CovenError::Blob(e.to_string()), moved);
            }
        }
        if let Some(parent) = blob.final_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return rollback_write_batch(
                    CovenError::Blob(format!(
                        "create local blob parent {}: {e}",
                        parent.display()
                    )),
                    moved,
                );
            }
        }
        if let Err(e) = std::fs::rename(&blob.staged, &blob.final_path) {
            return rollback_write_batch(
                CovenError::Blob(format!(
                    "install staged blob {} -> {}: {e}",
                    blob.staged.display(),
                    blob.final_path.display()
                )),
                moved,
            );
        }
        moved.push(blob.clone());
        if let Err(e) = sync_parent_dir(&blob.final_path) {
            return rollback_write_batch(
                CovenError::Blob(format!(
                    "sync local blob parent after installing {}: {e}",
                    blob.final_path.display()
                )),
                moved,
            );
        }
    }

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sql(SqlContext::new(&tx, stamper))
    })) {
        Ok(Ok(value)) => {
            for blob in &deleted {
                match decls.row_for_blob_in_namespace(&tx, &blob.namespace, &blob.id) {
                    Ok(Some(_)) => {
                        return rollback_write_batch(
                            CovenError::BlobStillReferenced {
                                namespace: blob.namespace.clone(),
                                id: blob.id.clone(),
                            },
                            moved,
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return rollback_write_batch(CovenError::Blob(e.to_string()), moved);
                    }
                }
            }
            if let Err(e) = record_local_cleanup_intents(&tx, &library_dir, &deleted) {
                return rollback_write_batch(e, moved);
            }
            if let Err(e) = tx.commit() {
                return rollback_write_batch(CovenError::from(e), moved);
            }
            WriteDbOutcome::Committed(value)
        }
        Ok(Err(error)) => rollback_write_batch(error, moved),
        Err(_) => rollback_write_batch(
            CovenError::Blob("write SQL closure panicked".to_string()),
            moved,
        ),
    }
}

fn rollback_write_batch<R>(error: CovenError, moved: Vec<StagedBlob>) -> WriteDbOutcome<R> {
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
    WriteDbOutcome::RolledBack { error }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blob::{BlobScope, CacheFill, Provenance};
    use crate::config::Config;
    use crate::library_dir::LibraryDir;
    use crate::sync::session::BlobDecl;
    use rusqlite::params;

    fn config(dir: LibraryDir) -> Config {
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
        SyncedTable::new("files").carries_blob(media_files_decl())
    }

    fn remote_root_files_table() -> SyncedTable {
        SyncedTable::new("files")
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
                _updated_at TEXT NOT NULL
            );",
        )
    }

    fn open_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = LibraryDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    #[tokio::test]
    async fn second_open_of_one_library_is_refused_until_the_first_handle_drops() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = LibraryDir::new(tmp.path());
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
            Err(CovenError::AlreadyOpen { library_dir }) if library_dir == tmp.path()
        ));

        drop(first);

        let still_locked = Coven::builder(config(dir.clone()))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open();
        assert!(matches!(
            still_locked,
            Err(CovenError::AlreadyOpen { library_dir }) if library_dir == tmp.path()
        ));

        drop(clone);

        Coven::builder(config(dir))
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open succeeds after the first handle drops");
    }

    fn open_remote_root_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = LibraryDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .synced_tables(vec![remote_root_files_table()])
            .migrations(vec![files_migration()])
            .open()
            .expect("open handle");
        (tmp, handle)
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
                sql.tx()
                    .query_row(
                        "SELECT count(*) FROM local_cleanup_intents \
                         WHERE namespace = ?1 AND blob_id = ?2",
                        params![namespace, id],
                        |row| row.get(0),
                    )
                    .map_err(CovenError::from)
            })
            .await
            .expect("count cleanup intents")
    }

    #[tokio::test]
    async fn builder_open_runs_coven_and_host_migrations() {
        let (_tmp, handle) = open_files_handle();
        let has_coven_table: i64 = handle
            .sql(|sql| {
                sql.tx().query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sync_state'",
                    [],
                    |row| row.get(0),
                ).map_err(CovenError::from)
            })
            .await
            .expect("query coven table");
        let has_host_table: i64 = handle
            .sql(|sql| {
                sql.tx().query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'files'",
                    [],
                    |row| row.get(0),
                ).map_err(CovenError::from)
            })
            .await
            .expect("query host table");
        assert_eq!(has_coven_table, 1);
        assert_eq!(has_host_table, 1);
    }

    #[tokio::test]
    async fn sql_reads_writes_and_stamps() {
        let (_tmp, handle) = open_files_handle();
        let id = "file-sql".to_string();
        handle
            .sql(move |sql| {
                sql.tx().execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    params![id, sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("insert through sql");
        let count: i64 = handle
            .sql(|sql| {
                sql.tx()
                    .query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count rows");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn open_removes_orphaned_local_blob_temps() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = LibraryDir::new(tmp.path());
        let final_path = dir
            .local_blob_path("media-files", "tempaaaa")
            .expect("local path");
        let temp = local_stage_temp_path(&final_path).expect("stage temp path");
        write_raw_file(&temp, b"interrupted write").await;

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
                    sql.tx().execute(
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
            .library_dir()
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
            .library_dir()
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
                    sql.tx().execute(
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
                    sql.tx().execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-original", "dupeaaaa", 8i64, sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("seed original blob");

        let result: CovenResult<()> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "dupeaaaa", b"replacement".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "dupeaaaa")
            .expect("dupe path");
        assert_eq!(
            std::fs::read(path).expect("read original blob"),
            b"original"
        );
        let replacement_rows: i64 = handle
            .sql(|sql| {
                sql.tx()
                    .query_row(
                        "SELECT count(*) FROM files WHERE id = 'file-replacement'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(CovenError::from)
            })
            .await
            .expect("count replacement rows");
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
                    sql.tx().execute(
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
            .expect("write remote-root row and host-provided blob");

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
            .library_dir()
            .local_blob_path("media-files", "blobbbbb")
            .expect("local path");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn blob_stage_failure_does_not_run_sql() {
        let (_tmp, handle) = open_files_handle();
        let result: CovenResult<()> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "..", b"bad".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.tx().execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('should-not-exist', NULL, 0, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await;
        assert!(result.is_err());
        let count: i64 = handle
            .sql(|sql| {
                sql.tx()
                    .query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count rows");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn replacement_deletes_old_blob_after_sql_drops_reference() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.library_dir(), "media-files", "oldaaaa", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.tx().execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldaaaa", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
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
                    sql.tx().execute(
                        "UPDATE files SET blob_id = ?1, size = 3, _updated_at = ?2 WHERE id = 'file-1'",
                        params!["newaaaa", sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("replace blob");
        assert!(!handle
            .library_dir()
            .local_blob_path("media-files", "oldaaaa")
            .expect("old path")
            .exists());
        assert!(handle
            .library_dir()
            .local_blob_path("media-files", "newaaaa")
            .expect("new path")
            .exists());
    }

    #[tokio::test]
    async fn author_delete_drops_all_local_blob_copies() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.library_dir(), "media-files", "oldcccc", b"old")
            .await
            .expect("store old");
        let pinned = handle
            .library_dir()
            .pinned_blob_path("media-files", "oldcccc")
            .expect("pinned path");
        let cached = handle
            .library_dir()
            .cache_blob_path("media-files", "oldcccc")
            .expect("cache path");
        write_raw_file(&pinned, b"pinned").await;
        write_raw_file(&cached, b"cached").await;
        handle
            .sql(|sql| {
                sql.tx().execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldcccc", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
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
                    sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "oldcccc")
            .expect("local path")
            .exists());
        assert!(!pinned.exists(), "pinned copy is removed");
        assert!(!cached.exists(), "cache copy is removed");
    }

    #[tokio::test]
    async fn failed_local_blob_cleanup_keeps_intent_for_later_drain() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.library_dir(), "media-files", "oldddddd", b"old")
            .await
            .expect("store old");
        let pinned = handle
            .library_dir()
            .pinned_blob_path("media-files", "oldddddd")
            .expect("pinned path");
        std::fs::create_dir_all(&pinned).expect("create pinned blocker");
        handle
            .sql(|sql| {
                sql.tx().execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, ?2, 3, ?3)",
                    params!["file-1", "oldddddd", sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("seed row");
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
                    sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "oldddddd")
            .expect("local path")
            .exists());

        std::fs::remove_dir_all(&pinned).expect("remove pinned blocker");
        handle
            .write(
                |_| Ok(()),
                |sql| {
                    sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "oldddddd")
            .expect("local path")
            .exists());
    }

    #[tokio::test]
    async fn replacement_is_rejected_while_sql_still_references_old_blob() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.library_dir(), "media-files", "oldbbbb", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.tx().execute(
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
        let result: CovenResult<()> = handle
            .write(
                move |w| {
                    w.put_blob("media-files", "newbbbb", b"new".to_vec());
                    w.delete_blob(old_ref);
                    Ok(())
                },
                move |sql| {
                    sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "oldbbbb")
            .expect("old path")
            .exists());
        assert!(!handle
            .library_dir()
            .local_blob_path("media-files", "newbbbb")
            .expect("new path")
            .exists());
    }

    #[tokio::test]
    async fn sql_panic_removes_moved_blob() {
        let (_tmp, handle) = open_files_handle();
        let result: CovenResult<()> = handle
            .write(
                |w| {
                    w.put_blob("media-files", "panicccc", b"new".to_vec());
                    Ok(())
                },
                |_sql| panic!("boom"),
            )
            .await;
        assert!(result
            .expect_err("panic is surfaced")
            .to_string()
            .contains("panicked"));
        assert!(!handle
            .library_dir()
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
                        sql.tx().execute(
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
            .library_dir()
            .local_blob_path("media-files", "raceblob")
            .expect("race path");
        assert_eq!(std::fs::read(path).expect("read race blob"), b"committed");
        let rows: i64 = handle
            .sql(|sql| {
                sql.tx()
                    .query_row(
                        "SELECT count(*) FROM files WHERE id = 'winner'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(CovenError::from)
            })
            .await
            .expect("count winner row");
        assert_eq!(rows, 1);
    }
}

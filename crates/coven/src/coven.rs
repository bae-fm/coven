//! Native top-level API: open one handle and drive rows, blobs, sync, and
//! membership through it.

use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;

use crate::blob::local_files::LocalBlobError;
use crate::blob::{BlobRef, BlobTransitionObserver};
use crate::clock::{ClockRef, SystemClock};
use crate::config::Config;
use crate::database::{Database, DbError};
use crate::handle::CovenHandle;
use crate::id_provider::{IdRef, UuidProvider};
use crate::keys::KeyService;
use crate::library_dir::PathTokenError;
use crate::migration::Migration;
use crate::sync::hlc::UpdatedAtStamper;
use crate::sync::session::SyncedTable;
use crate::sync::sync_manager::ConfigProvider;

pub type CovenResult<T> = Result<T, CovenError>;

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
    #[error("write batch did not install a SQL closure")]
    MissingSql,
    #[error("write batch can install only one SQL closure")]
    MultipleSqlClosures,
    #[error("blob {namespace}/{id} is still referenced by a row after the write")]
    BlobStillReferenced { namespace: String, id: String },
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
pub enum CovenConfig {
    Static(Config),
    Provider(ConfigProvider),
}

impl CovenConfig {
    fn current(&self) -> Config {
        match self {
            CovenConfig::Static(config) => config.clone(),
            CovenConfig::Provider(provider) => provider(),
        }
    }

    fn provider(&self) -> ConfigProvider {
        match self {
            CovenConfig::Static(config) => {
                let config = config.clone();
                Arc::new(move || config.clone())
            }
            CovenConfig::Provider(provider) => provider.clone(),
        }
    }
}

impl From<Config> for CovenConfig {
    fn from(value: Config) -> Self {
        CovenConfig::Static(value)
    }
}

impl<F> From<F> for CovenConfig
where
    F: Fn() -> Config + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        CovenConfig::Provider(Arc::new(value))
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
            observer: None,
            stage_blob_ids: Arc::new(UuidProvider),
        }
    }
}

pub struct CovenBuilder {
    config: CovenConfig,
    synced_tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    clock: ClockRef,
    key_service: KeyService,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    stage_blob_ids: IdRef,
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

    pub fn observer(mut self, observer: Arc<dyn BlobTransitionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn stage_blob_ids(mut self, ids: IdRef) -> Self {
        self.stage_blob_ids = ids;
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
        let (db, stamper) =
            Database::open(&db_path, tables, config.device_id.clone(), &migrations)?;
        Ok(CovenHandle::new(
            db,
            stamper,
            library_dir,
            provider,
            self.key_service,
            self.clock,
            self.stage_blob_ids,
            self.observer,
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

    pub fn connection(&self) -> &'ctx Connection {
        self.tx
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

pub struct WriteBatch<R> {
    new_blobs: Vec<NewBlob>,
    deleted_blobs: Vec<BlobRef>,
    sql: Option<WriteSql<R>>,
}

impl<R> WriteBatch<R> {
    fn new() -> Self {
        Self {
            new_blobs: Vec::new(),
            deleted_blobs: Vec::new(),
            sql: None,
        }
    }

    pub fn put_blob(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> PendingBlob {
        let blob = NewBlob {
            namespace: namespace.into(),
            id: id.into(),
            bytes: bytes.into(),
        };
        let pending = PendingBlob {
            id: blob.id.clone(),
        };
        self.new_blobs.push(blob);
        pending
    }

    pub fn delete_blob(&mut self, blob: BlobRef) {
        self.deleted_blobs.push(blob);
    }

    pub fn sql(
        &mut self,
        sql: impl for<'ctx, 'conn> FnOnce(SqlContext<'ctx, 'conn>) -> CovenResult<R> + Send + 'static,
    ) -> CovenResult<()> {
        if self.sql.is_some() {
            return Err(CovenError::MultipleSqlClosures);
        }
        self.sql = Some(Box::new(sql));
        Ok(())
    }
}

pub struct PendingBlob {
    id: String,
}

impl PendingBlob {
    pub fn id(&self) -> &str {
        &self.id
    }
}

struct NewBlob {
    namespace: String,
    id: String,
    bytes: Vec<u8>,
}

pub(crate) struct StagedBlob {
    pub namespace: String,
    pub id: String,
    pub staged: PathBuf,
    pub final_path: PathBuf,
}

struct InstalledBlob {
    blob: StagedBlob,
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

    pub async fn write<F, R>(&self, f: F) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch<R>) -> CovenResult<()> + Send + 'static,
        R: Send + 'static,
    {
        let mut batch = WriteBatch::new();
        f(&mut batch)?;
        let sql = batch.sql.take().ok_or(CovenError::MissingSql)?;
        let staged = self.stage_blobs(batch.new_blobs).await?;
        let staged_paths = staged
            .iter()
            .map(|blob| blob.staged.clone())
            .collect::<Vec<_>>();
        let deleted_after_commit = batch.deleted_blobs.clone();
        let tables = self.db().synced_tables().to_vec();
        let db = self.db().clone();
        let stamper = self.stamper();
        let deleted = batch.deleted_blobs;
        let outcome = match db
            .call(move |conn| {
                Ok(run_write_batch_on_connection(
                    conn, stamper, staged, deleted, tables, sql,
                ))
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                for path in staged_paths {
                    crate::local_blob::remove_file(&path)
                        .await
                        .map_err(CovenError::Blob)?;
                }
                return Err(CovenError::from(error));
            }
        };
        match outcome {
            WriteDbOutcome::Committed(value) => {
                let library_dir = self.library_dir();
                for blob in deleted_after_commit {
                    crate::blob::local_files::drop_blob(&library_dir, &blob.namespace, &blob.id)
                        .await?;
                }
                Ok(value)
            }
            WriteDbOutcome::RolledBack { error } => {
                for path in staged_paths {
                    crate::local_blob::remove_file(&path)
                        .await
                        .map_err(CovenError::Blob)?;
                }
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
            let stage_dir = self
                .library_dir()
                .storage_dir()
                .join("local-staging")
                .join(&blob.namespace);
            let staged_path =
                stage_dir.join(format!("{}.{}", blob.id, self.stage_blob_ids().new_id()));
            if let Err(e) = crate::local_blob::write_atomic(&staged_path, &blob.bytes).await {
                remove_staged_files(&staged).await?;
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

async fn remove_staged_files(staged: &[StagedBlob]) -> CovenResult<()> {
    for blob in staged {
        crate::local_blob::remove_file(&blob.staged)
            .await
            .map_err(CovenError::Blob)?;
    }
    Ok(())
}

fn run_write_batch_on_connection<R>(
    conn: &Connection,
    stamper: UpdatedAtStamper,
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
    for blob in &staged {
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
        if let Err(e) = std::fs::hard_link(&blob.staged, &blob.final_path) {
            return rollback_write_batch(
                CovenError::Blob(format!(
                    "install staged blob {} -> {}: {e}",
                    blob.staged.display(),
                    blob.final_path.display()
                )),
                moved,
            );
        }
        moved.push(InstalledBlob {
            blob: StagedBlob {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
                staged: blob.staged.clone(),
                final_path: blob.final_path.clone(),
            },
        });
        if let Err(e) = std::fs::remove_file(&blob.staged) {
            return rollback_write_batch(
                CovenError::Blob(format!(
                    "remove staged blob {} after install: {e}",
                    blob.staged.display()
                )),
                moved,
            );
        }
    }

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sql(SqlContext::new(&tx, stamper))
    })) {
        Ok(Ok(value)) => {
            let decls = match crate::blob::decl::BlobDecls::from_tables(&tx, &tables)
                .map_err(|e| CovenError::Blob(e.to_string()))
            {
                Ok(decls) => decls,
                Err(e) => {
                    return rollback_write_batch(e, moved);
                }
            };
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

fn rollback_write_batch<R>(error: CovenError, moved: Vec<InstalledBlob>) -> WriteDbOutcome<R> {
    for blob in moved.iter().rev() {
        if let Err(e) = std::fs::remove_file(&blob.blob.final_path) {
            return WriteDbOutcome::RolledBack {
                error: CovenError::Blob(format!(
                    "rollback local blob {}/{} at {}: {e}",
                    blob.blob.namespace,
                    blob.blob.id,
                    blob.blob.final_path.display()
                )),
            };
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

    fn files_table() -> SyncedTable {
        SyncedTable::new("files").carries_blob(
            BlobDecl::new(
                "media-files",
                Provenance::HostProvided,
                CacheFill::CacheLazy,
            )
            .with_id_column("blob_id"),
        )
    }

    fn open_files_handle() -> (tempfile::TempDir, CovenHandle) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = LibraryDir::new(tmp.path());
        let handle = Coven::builder(config(dir))
            .synced_tables(vec![files_table()])
            .migrations(vec![Migration::sql(
                1,
                "test-schema",
                "CREATE TABLE files (
                    id TEXT PRIMARY KEY,
                    blob_id TEXT,
                    size INTEGER NOT NULL,
                    _updated_at TEXT NOT NULL
                );",
            )])
            .open()
            .expect("open handle");
        (tmp, handle)
    }

    #[tokio::test]
    async fn builder_open_runs_coven_and_host_migrations() {
        let (_tmp, handle) = open_files_handle();
        let has_coven_table: i64 = handle
            .sql(|sql| {
                sql.connection().query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sync_state'",
                    [],
                    |row| row.get(0),
                ).map_err(CovenError::from)
            })
            .await
            .expect("query coven table");
        let has_host_table: i64 = handle
            .sql(|sql| {
                sql.connection().query_row(
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
                sql.connection().execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    params![id, sql.stamp()],
                )?;
                Ok(())
            })
            .await
            .expect("insert through sql");
        let count: i64 = handle
            .sql(|sql| {
                sql.connection()
                    .query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                    .map_err(CovenError::from)
            })
            .await
            .expect("count rows");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn write_inserts_row_and_host_provided_blob() {
        let (_tmp, handle) = open_files_handle();
        let bytes = b"piece-bytes".to_vec();
        handle
            .write(move |w| {
                let blob = w.put_blob("media-files", "blobaaaa", bytes.clone());
                let blob_id = blob.id().to_string();
                w.sql(move |sql| {
                    sql.tx().execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params!["file-1", blob_id, bytes.len() as i64, sql.stamp()],
                    )?;
                    Ok(())
                })
            })
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
    async fn sql_failure_removes_staged_blob() {
        let (_tmp, handle) = open_files_handle();
        let err = handle
            .write::<_, ()>(|w| {
                w.put_blob("media-files", "blobbbbb", b"staged".to_vec());
                w.sql(|_sql| Err(CovenError::Blob("sql failed".to_string())))
            })
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
        let result = handle
            .write(|w| {
                w.put_blob("media-files", "..", b"bad".to_vec());
                w.sql(|sql| {
                    sql.connection().execute(
                        "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('should-not-exist', NULL, 0, ?1)",
                        [sql.stamp()],
                    )?;
                    Ok(())
                })
            })
            .await;
        assert!(result.is_err());
        let count: i64 = handle
            .sql(|sql| {
                sql.connection()
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
                sql.connection().execute(
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
            .write(move |w| {
                let new_blob = w.put_blob("media-files", "newaaaa", b"new".to_vec());
                w.delete_blob(old_ref);
                let new_id = new_blob.id().to_string();
                w.sql(move |sql| {
                    sql.connection().execute(
                        "UPDATE files SET blob_id = ?1, size = 3, _updated_at = ?2 WHERE id = 'file-1'",
                        params![new_id, sql.stamp()],
                    )?;
                    Ok(())
                })
            })
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
    async fn replacement_is_rejected_while_sql_still_references_old_blob() {
        let (_tmp, handle) = open_files_handle();
        crate::blob::local_files::store(&handle.library_dir(), "media-files", "oldbbbb", b"old")
            .await
            .expect("store old");
        handle
            .sql(|sql| {
                sql.connection().execute(
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
        let result = handle
            .write(move |w| {
                w.put_blob("media-files", "newbbbb", b"new".to_vec());
                w.delete_blob(old_ref);
                w.sql(move |sql| {
                    sql.connection().execute(
                        "UPDATE files SET _updated_at = ?1 WHERE id = 'file-1'",
                        [sql.stamp()],
                    )?;
                    Ok(())
                })
            })
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
        let result = handle
            .write::<_, ()>(|w| {
                w.put_blob("media-files", "panicccc", b"new".to_vec());
                w.sql(|_sql| panic!("boom"))
            })
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
                .write(move |w| {
                    let blob = w.put_blob("media-files", "raceblob", b"committed".to_vec());
                    let blob_id = blob.id().to_string();
                    w.sql(move |sql| {
                        sql.tx().execute(
                            "INSERT INTO files (id, blob_id, size, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4)",
                            params!["winner", blob_id, 9i64, sql.stamp()],
                        )?;
                        Ok(())
                    })
                })
                .await
        });

        let write_loser = tokio::spawn(async move {
            loser
                .write::<_, ()>(move |w| {
                    w.put_blob("media-files", "raceblob", b"rolled-back".to_vec());
                    w.sql(|_sql| Err(CovenError::Blob("force rollback".to_string())))
                })
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
                sql.connection()
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

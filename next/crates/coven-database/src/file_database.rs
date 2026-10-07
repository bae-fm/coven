//! File metadata operations through the owning database, never through app SQL.

use super::{finish_blocking, Database, DatabaseReadHandle};
use crate::DatabaseChanges;
use crate::{
    file_queue, file_ref, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef,
    FileUpload, RowKey,
};
use coven_crypto::{SecretBytes, SecretText};
use coven_foundation::{
    files::{FileArea, FileName, StoreDir},
    id_source::DeviceId,
};
use std::time::SystemTime;

#[path = "file_cache.rs"]
mod cache;
#[path = "file_keep.rs"]
mod keep;

#[derive(Clone)]
enum FileDatabaseAccess {
    Writer(Database),
    Reader(DatabaseReadHandle),
}

/// File queue and cache access. Read handles can mutate only local cache metadata.
/// Construct beside the database at the application composition root.
#[derive(Clone)]
pub struct FileDatabase {
    access: FileDatabaseAccess,
}

impl FileDatabase {
    /// Use an open writer for uploads and cache operations.
    pub fn new(database: Database) -> Self {
        Self {
            access: FileDatabaseAccess::Writer(database),
        }
    }
    /// Use a read-only application's cache connection; upload mutations are refused.
    pub fn read_only(database: DatabaseReadHandle) -> Self {
        Self {
            access: FileDatabaseAccess::Reader(database),
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        run: impl FnOnce(&DatabaseConnection, &WriteSchema, &StoreDir, DeviceId) -> Result<T, DbError>
            + Send
            + 'static,
    ) -> Result<T, DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || match &owner.access {
                FileDatabaseAccess::Writer(database) => {
                    let slot = database.inner.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    let writer = inner
                        .writer
                        .lock()
                        .expect("writer connection lock poisoned");
                    run(&writer, &inner.write_schema, &inner.directory, inner.device)
                }
                FileDatabaseAccess::Reader(database) => {
                    let slot = database.inner.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    let writer = inner
                        .cache_writer
                        .lock()
                        .expect("cache connection lock poisoned");
                    run(&writer, &inner.schema, &inner.directory, inner.device)
                }
            })
            .await,
        )
    }

    /// Validate a captured reference against one committed row version.
    pub async fn validate(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        self.run(move |db, schema, _, _| db.transaction(|db| file_ref::validate(db, schema, &file)))
            .await
    }
    /// Read the file currently attached to one row.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError> {
        match &self.access {
            FileDatabaseAccess::Writer(db) => db.file_ref(table, key).await,
            FileDatabaseAccess::Reader(db) => db.file_ref(table, key).await,
        }
    }
    /// Open the declared local source and check it before returning.
    pub async fn open_local(
        &self,
        file: &FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
        match &self.access {
            FileDatabaseAccess::Writer(db) => db.open_local_file(file).await,
            FileDatabaseAccess::Reader(db) => db.open_local_file(file).await,
        }
    }
    /// All visible uploaded files whose declaration requests eager downloads.
    pub async fn eager_files(&self) -> Result<Vec<FileRef>, DbError> {
        self.run(|db, schema, _, _| {
            db.transaction(|db| {
                let mut files = Vec::new();
                for declaration in &schema.declarations {
                    let Some(file) = &declaration.files else {
                        continue;
                    };
                    if file.fill != crate::CacheFill::CacheEager {
                        continue;
                    }
                    let table = schema.table(&declaration.name);
                    let keys = db.query(
                        &format!(
                            "SELECT {} FROM main.{} WHERE {} LIKE 'uploaded %'",
                            crate::write_rows::key_columns(table)
                                .iter()
                                .map(|c| crate::sql::identifier(&c.name))
                                .collect::<Vec<_>>()
                                .join(","),
                            crate::sql::identifier(&table.name),
                            crate::sql::identifier(&file.location)
                        ),
                        [],
                        |row| {
                            let mut values = Vec::new();
                            for index in 0..crate::write_rows::key_columns(table).len() {
                                values.push(row.get(index)?)
                            }
                            Ok(RowKey(values))
                        },
                    )?;
                    for key in keys {
                        files.push(file_ref::read(db, schema, &table.name, &key)?)
                    }
                }
                Ok(files)
            })
        })
        .await
    }
    /// Queue every requested current local file atomically. An uploaded reference is a no-op.
    pub async fn enqueue(&self, files: &[FileRef], now: SystemTime) -> Result<(), DbError> {
        self.require_writer()?;
        let files = files.to_vec();
        self.run(move |db, schema, _, device| {
            db.transaction(|db| {
                for file in files {
                    file_ref::validate(db, schema, &file)?;
                    match file.location() {
                        crate::FileLocation::Uploaded => {}
                        crate::FileLocation::OnDevice(found) if found == device => {
                            file_queue::enqueue(db, &file, now)?
                        }
                        crate::FileLocation::OnDevice(_) => {
                            return Err(DbError::FileBytesRequired {
                                table: file.table().into(),
                                key: file.key().clone(),
                            })
                        }
                    }
                }
                Ok(())
            })
        })
        .await
    }
    /// Durable queue, including stored copies whose row changed meanwhile.
    pub async fn uploads(&self) -> Result<Vec<FileUpload>, DbError> {
        self.run(|db, _, _, _| file_queue::read(db)).await
    }
    /// Record the attempt before contacting storage.
    pub async fn begin_attempt(&self, id: i64, now: SystemTime) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET last_attempt_at=?2 WHERE id=?1",
                    (id, crate::user_file::encode_time(now)),
                )?)
            })
        })
        .await
    }
    /// Record a failed attempt's typed payload. Native errors are interpreted by sync.
    pub async fn fail_upload(&self, id: i64, failure: SecretBytes) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET attempts=attempts+1,failure=?2 WHERE id=?1",
                    (id, failure.as_bytes()),
                )?)
            })
        })
        .await
    }
    /// Persist the provider recording after each confirmed change in progress.
    pub async fn record_session(&self, id: i64, session: SecretBytes) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET session=?2 WHERE id=?1 AND fixed IS NOT NULL",
                    (id, session.as_bytes()),
                )?)
            })
        })
        .await
    }
    /// Record publication before the conditional application write.
    pub async fn record_stored(&self, id: i64) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET stored=1,session=NULL WHERE id=?1 AND fixed IS NOT NULL",
                    [id],
                )?)
            })
        })
        .await
    }
    /// Mark all four row columns through an ordinary recorded write, only if the
    /// captured reference still matches. A mismatch records the copy as unused.
    pub async fn finish_upload(
        &self,
        id: i64,
        file: &FileRef,
        location: SecretText,
    ) -> Result<bool, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let database = database.clone();
        let file = file.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let files = crate::file_write::FileWrite::new(
                    &writer,
                    &inner.directory,
                    &inner.write_schema,
                    inner.device,
                    &inner.staging,
                    Vec::new(),
                );
                let result = writer.local_write(
                    &inner.write_schema,
                    inner.device,
                    inner.clock.now(),
                    &files,
                    |sql| {
                        if !writer.query_row(
                            "SELECT stored FROM _coven_file_uploads WHERE id=?1",
                            [id],
                            |r| r.get::<_, bool>(0),
                        )? {
                            return Err(DbError::DamagedDatabase);
                        }
                        match file_ref::validate(&writer, &inner.write_schema, &file) {
                            Err(DbError::FileRefChanged { .. }) => {
                                writer.internal_execute(
                                    "UPDATE _coven_file_uploads SET unused=1 WHERE id=?1",
                                    [id],
                                )?;
                                Ok(false)
                            }
                            Err(error) => Err(error),
                            Ok(()) => {
                                sql.mark_uploaded(&file, &location)?;
                                writer.internal_execute(
                                    "INSERT INTO _coven_file_removals(path)
                                     SELECT path FROM _coven_file_uploads WHERE id=?1",
                                    [id],
                                )?;
                                writer.internal_execute(
                                    "DELETE FROM _coven_file_uploads WHERE id=?1",
                                    [id],
                                )?;
                                Ok(true)
                            }
                        }
                    },
                );
                files.finish(result)
            })
            .await,
        )
    }
    /// Register a spool name before bytes exist. Cancellation retains the lease
    /// until the partial bytes and pending record have been removed.
    pub async fn reserve_upload_bytes(&self, name: FileName) -> Result<FileReservation, DbError> {
        Ok(FileReservation {
            pending: self.reserve(name, FileArea::AppProvided).await?,
        })
    }
    /// Reserve a cache file while it is assembled and checked. Only the writer
    /// can pin; read-only file owners continue to cache individual chunks.
    pub async fn reserve_cache_file(&self, name: FileName) -> Result<CacheReservation, DbError> {
        Ok(CacheReservation {
            pending: self.reserve(name, FileArea::Cache).await?,
        })
    }
    async fn reserve(&self, name: FileName, area: FileArea) -> Result<PendingFile, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let database = database.clone();
        let lease = database.file_tasks.clone().read_owned().await;
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                {
                    let slot = database.inner.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    let writer = inner
                        .writer
                        .lock()
                        .expect("writer connection lock poisoned");
                    writer.transaction(|db| {
                        if db.query_row(
                            "SELECT EXISTS(
                                SELECT 1 FROM _coven_device_files WHERE path=?1 UNION ALL
                                SELECT 1 FROM _coven_file_uploads WHERE path=?1 UNION ALL
                                SELECT 1 FROM _coven_cache WHERE path=?1)",
                            [name.as_str()],
                            |r| r.get::<_, bool>(0),
                        )? {
                            return Err(DbError::FileNameReused { name: name.clone() });
                        }
                        let area = match area {
                            FileArea::AppProvided => "files",
                            FileArea::Cache => "cache",
                        };
                        db.internal_execute(
                            "INSERT INTO _coven_file_removals(area,path) VALUES(?1,?2)",
                            (area, name.as_str()),
                        )?;
                        Ok(())
                    })?;
                    inner
                        .staging
                        .lock()
                        .expect("staging lock poisoned")
                        .insert(name.clone());
                }
                Ok(PendingFile {
                    database,
                    name,
                    lease: Some(lease),
                })
            })
            .await,
        )
    }
    /// Open a database commit observation for application file rows and upload
    /// metadata. Subscribing before reading the work list prevents missed commits.
    pub fn changes(&self) -> DatabaseChanges {
        match &self.access {
            FileDatabaseAccess::Reader(_) => DatabaseChanges {
                commits: crate::observation::CommitSubscription::closed(),
                reads: Vec::new(),
                first: true,
            },
            FileDatabaseAccess::Writer(database) => {
                let slot = database.inner.read().expect("database lock poisoned");
                let Some(inner) = slot.as_ref() else {
                    return DatabaseChanges {
                        commits: crate::observation::CommitSubscription::closed(),
                        reads: Vec::new(),
                        first: true,
                    };
                };
                let reads = inner
                    .write_schema
                    .declarations
                    .iter()
                    .filter_map(|t| {
                        t.files.as_ref().map(|file| crate::observation::TableRead {
                            table: t.name.to_ascii_lowercase(),
                            columns: file
                                .columns()
                                .into_iter()
                                .map(str::to_ascii_lowercase)
                                .collect(),
                            keys: crate::key_scope::KeyScope::All,
                        })
                    })
                    .chain(std::iter::once(crate::observation::TableRead {
                        table: "_coven_file_uploads".into(),
                        columns: ["reference".into()].into(),
                        keys: crate::key_scope::KeyScope::All,
                    }))
                    .collect::<Vec<_>>();
                let commits = inner.observer.subscribe();
                commits.begin();
                commits.finish(reads.clone());
                DatabaseChanges {
                    commits,
                    reads,
                    first: true,
                }
            }
        }
    }
    fn require_writer(&self) -> Result<(), DbError> {
        match self.access {
            FileDatabaseAccess::Writer(_) => Ok(()),
            FileDatabaseAccess::Reader(_) => Err(read_only()),
        }
    }

    /// Cached encrypted chunk or header (-1), read with its eviction protected by SQLite.
    pub async fn cached(&self, file: &FileRef, index: i64) -> Result<Option<Vec<u8>>, DbError> {
        let file = file.clone();
        self.run(move |db, _, directory, _| cache::read(db, directory, &file, index))
            .await
    }
    /// Keep verified ciphertext durably before publishing its cache metadata.
    pub async fn cache_chunk(
        &self,
        file: &FileRef,
        index: i64,
        name: FileName,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        let file = file.clone();
        self.run(move |db, _, directory, _| cache::put(db, directory, &file, index, &name, &bytes))
            .await
    }
    /// Evict unpinned chunks in least-recently-read order, independently per namespace.
    pub async fn trim_cache(&self, namespace: &str) -> Result<(), DbError> {
        let namespace = namespace.to_owned();
        self.run(move |db, _, directory, _| cache::trim(db, directory, &namespace))
            .await
    }
    /// Remove only cached bytes, pinned or not, without any storage operation.
    pub async fn evict(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        self.run(move |db, schema, directory, _| {
            db.transaction(|db| file_ref::validate(db, schema, &file))?;
            cache::evict(db, directory, &file)
        })
        .await
    }
    /// Pin a complete, previously checked cache file atomically if one exists.
    /// The presence check and state change share a transaction with eviction.
    pub async fn pin_complete(&self, file: &FileRef) -> Result<bool, DbError> {
        let file = file.clone();
        self.run(move |db, schema, _, _| {
            db.transaction(|db| {
                file_ref::validate(db, schema, &file)?;
                cache::pin_complete(db, &file)
            })
        })
        .await
    }
    /// Release an uploaded file's budget exemption after checking the reference.
    pub async fn unpin(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        self.run(move |db, schema, _, _| {
            db.transaction(|db| {
                file_ref::validate(db, schema, &file)?;
                cache::unpin(db, &file)
            })
        })
        .await
    }
    /// Whether the uploaded file has a retained whole-file pin.
    pub async fn is_pinned(&self, file: &FileRef) -> Result<bool, DbError> {
        let file = file.clone();
        self.run(move |db, schema, _, _| {
            db.transaction(|db| {
                file_ref::validate(db, schema, &file)?;
                cache::pinned(db, &file)
            })
        })
        .await
    }
    /// Store a namespace's byte budget. Eviction is requested separately by sync.
    pub async fn set_budget(&self, namespace: &str, bytes: u64) -> Result<(), DbError> {
        let namespace = namespace.to_owned();
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                db.internal_execute(
                    "INSERT INTO _coven_cache_budgets(namespace,bytes) VALUES(?1,?2)
                     ON CONFLICT(namespace) DO UPDATE SET bytes=excluded.bytes",
                    (namespace, bytes.to_be_bytes().as_slice()),
                )?;
                Ok(())
            })
        })
        .await
    }
    /// Plaintext bytes absent from this namespace's cache; headers and tags
    /// are excluded from application download progress.
    pub async fn missing_bytes(&self, file: &FileRef) -> Result<u64, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, _| {
            if file.uploaded()?.is_none() {
                return Ok(0);
            }
            let id = cache::id(&file)?;
            if db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_cache
                 WHERE namespace=?1 AND file_id=?2 AND chunk=-2)",
                (file.namespace(), &id),
                |r| r.get::<_, bool>(0),
            )? {
                return Ok(0);
            }
            let cached = db.query_row(
                "SELECT coalesce(sum(size-16),0) FROM _coven_cache
                 WHERE namespace=?1 AND file_id=?2 AND chunk>=0",
                (file.namespace(), &id),
                |r| r.get::<_, i64>(0),
            )?;
            file.plaintext_size()
                .checked_sub(u64::try_from(cached).map_err(|_| DbError::DamagedDatabase)?)
                .ok_or(DbError::DamagedDatabase)
        })
        .await
    }
    /// An absent budget leaves that namespace unlimited.
    pub async fn budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let namespace = namespace.to_owned();
        self.run(move |db, _, _, _| cache::budget(db, &namespace))
            .await
    }
}

/// Bytes reserved for one fixed encrypted upload.
pub struct FileReservation {
    pending: PendingFile,
}
/// Bytes reserved for a verified whole-file cache pin.
pub struct CacheReservation {
    pending: PendingFile,
}
struct PendingFile {
    database: Database,
    name: FileName,
    lease: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}
impl FileReservation {
    /// Commit the fixed identity only after the caller has synced the named bytes.
    pub async fn publish(self, id: i64, identity: SecretBytes) -> Result<(), DbError> {
        let mut pending = self.pending;
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                {
                    let slot = pending
                        .database
                        .inner
                        .read()
                        .expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    let writer = inner
                        .writer
                        .lock()
                        .expect("writer connection lock poisoned");
                    writer.transaction(|db| {
                        changed(db.internal_execute(
                            "UPDATE _coven_file_uploads SET path=?2,fixed=?3
                             WHERE id=?1 AND fixed IS NULL",
                            (id, pending.name.as_str(), identity.as_bytes()),
                        )?)?;
                        db.internal_execute(
                            "DELETE FROM _coven_file_removals WHERE path=?1 AND area='files'",
                            [pending.name.as_str()],
                        )?;
                        Ok(())
                    })?;
                    inner
                        .staging
                        .lock()
                        .expect("staging lock poisoned")
                        .remove(&pending.name);
                }
                pending.lease.take();
                Ok(())
            })
            .await,
        )
    }
}
impl CacheReservation {
    /// Atomically publish a fully synced and content-checked whole file as pinned.
    /// Any older chunk files become recorded removals in the same transaction.
    pub async fn publish(self, file: &FileRef) -> Result<(), DbError> {
        let mut pending = self.pending;
        let file = file.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                {
                    let slot = pending
                        .database
                        .inner
                        .read()
                        .expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    let writer = inner
                        .writer
                        .lock()
                        .expect("writer connection lock poisoned");
                    writer.transaction(|db| {
                        file_ref::validate(db, &inner.write_schema, &file)?;
                        cache::publish(db, &inner.directory, &file, &pending.name)
                    })?;
                    let mut active = inner.staging.lock().expect("staging lock poisoned");
                    active.remove(&pending.name);
                    pending.lease.take();
                    crate::file_removals::FileRemovals::new(&writer, &inner.directory, &active)
                        .finish(Ok::<_, DbError>(()))?;
                }
                Ok(())
            })
            .await,
        )
    }
}
impl Drop for PendingFile {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let database = self.database.clone();
        let name = self.name.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let slot = database.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().expect("reservation holds close guard");
            let writer = inner
                .writer
                .lock()
                .expect("writer connection lock poisoned");
            let mut active = inner.staging.lock().expect("staging lock poisoned");
            active.remove(&name);
            let result =
                crate::file_removals::FileRemovals::new(&writer, &inner.directory, &active)
                    .finish(Ok::<_, DbError>(()));
            drop(active);
            drop(writer);
            if let Err(error) = result {
                panic!("reserved file cleanup failed: {error:?}")
            }
        });
    }
}
fn changed(count: usize) -> Result<(), DbError> {
    if count == 1 {
        Ok(())
    } else {
        Err(DbError::DamagedDatabase)
    }
}
fn read_only() -> DbError {
    DbError::StatementForbidden {
        operation: "upload through read-only handle",
    }
}

#[cfg(test)]
#[path = "file_database_tests.rs"]
mod tests;

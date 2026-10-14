//! File metadata operations through the owning database, never through app SQL.

use super::{finish_blocking, Database, DatabaseInner, DatabaseReadHandle};
use crate::DatabaseChanges;
use crate::{
    file_queue, file_ref, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef,
    FileUpload, RowKey,
};
use coven_crypto::SecretBytes;
use coven_foundation::{
    files::{FileName, StoreDir},
    id_source::DeviceId,
};
use std::sync::{Arc, RwLock};

#[path = "file_cache.rs"]
mod cache;

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

    async fn run<T: Send + 'static, E: From<DbError> + Send + 'static>(
        &self,
        run: impl FnOnce(&DatabaseConnection, &WriteSchema, &StoreDir, DeviceId) -> Result<T, E>
            + Send
            + 'static,
    ) -> Result<T, E> {
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| {
                        inner.access.with_writer(|writer| {
                            run(
                                writer,
                                &inner.access.write_schema,
                                &inner.access.directory,
                                inner.access.device,
                            )
                        })
                    })
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |inner| {
                        inner.with_writer(|writer| {
                            run(writer, &inner.write_schema, &inner.directory, inner.device)
                        })
                    })
                    .await
            }
        }
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
    /// Open and check the local source of a file this device has queued for
    /// upload. `None` means its bytes come from the cache or storage.
    pub async fn open_local(
        &self,
        file: &FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        match &self.access {
            FileDatabaseAccess::Writer(db) => db.open_local_file(file).await,
            FileDatabaseAccess::Reader(db) => db.open_local_file(file).await,
        }
    }
    /// All visible files whose declaration requests eager downloads, except
    /// those this device still has queued: their bytes are already local.
    pub async fn eager_files(&self) -> Result<Vec<FileRef>, DbError> {
        self.run(|db, schema, _, device| {
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
                            "SELECT {} FROM main.{} WHERE {} IS NOT NULL",
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
                        let file = file_ref::read(db, schema, &table.name, &key)?;
                        if file.uploader() != device || !file_queue::queued(db, &file)? {
                            files.push(file);
                        }
                    }
                }
                Ok(files)
            })
        })
        .await
    }
    /// The uploader's replayed state and its own record about `subject`, read
    /// in one committed state: the evidence a confirmed absence is judged
    /// with (§16.1). A device with no registration here has no removal or
    /// replacement either, so it reads as active.
    pub async fn uploader_evidence<P: crate::PendingValue>(
        &self,
        uploader: DeviceId,
        subject: P::Subject,
    ) -> Result<(crate::DeviceState, Option<P>), DbError>
    where
        P::Subject: Send,
    {
        self.run(move |db, _, _, _| {
            db.read_transaction(|| {
                let state = db
                    .query(
                        "SELECT state FROM _coven_devices WHERE device=?1",
                        [uploader.0.to_be_bytes()],
                        |r| crate::store_log_tables::device_state(r.get(0)?),
                    )?
                    .pop()
                    .unwrap_or(crate::DeviceState::Active);
                Ok((state, crate::pending::find::<P>(db, &subject, uploader)?))
            })
        })
        .await
    }
    /// Record this device's first unmet condition for one file it reads,
    /// replacing an earlier reason. Read-only handles keep no records.
    pub async fn record_file_pending<P: crate::PendingValue>(
        &self,
        record: impl FnOnce(DeviceId) -> P + Send + 'static,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let device = self.run(|_, _, _, device| Ok::<_, DbError>(device)).await?;
        database.record_pending(record(device)).await
    }
    /// Clear this device's record about a file once its read completes.
    pub async fn clear_file_pending<S: serde::Serialize + Send + 'static>(
        &self,
        subject: S,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database.clear_pending(subject).await
    }
    /// Durable queue, including files whose row changed after attachment.
    pub async fn uploads(&self) -> Result<Vec<FileUpload>, DbError> {
        self.run(|db, _, _, _| file_queue::read(db)).await
    }
    /// Record a failed attempt's typed payload with its retry wait, or with
    /// none when it waits for the app (§16.5). Native errors are interpreted by sync.
    pub async fn fail_upload(
        &self,
        id: i64,
        failure: SecretBytes,
        retry: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET failure=?2 WHERE id=?1",
                    (id, failure.as_bytes()),
                )?)?;
                let subject = crate::RetrySubject::Upload(id);
                match &retry {
                    Some(wait) => crate::retry::save(db, subject, wait),
                    None => crate::retry::clear(db, subject),
                }
            })
        })
        .await
    }
    /// Read the first-read plaintext hash of one queued chunk, keyed by the
    /// file's fixed reference and owned by the queue whatever the row became.
    pub async fn upload_chunk_hash(
        &self,
        file: &FileRef,
        index: u64,
    ) -> Result<coven_crypto::ContentHash, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, _| file_queue::chunk_hash(db, &file, index))
            .await
    }
    /// Open the source an upload sends, from the facts its queue entry fixed at
    /// attachment; later row changes cannot replace them. `None` means the
    /// file is no longer queued.
    pub async fn open_upload_source(
        &self,
        file: &FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let file = file.clone();
        self.run(move |db, _, directory, _| file_ref::open_queued(db, directory, &file))
            .await
    }

    /// Persist the provider recording after each confirmed change in progress.
    pub async fn record_session(&self, id: i64, session: SecretBytes) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET session=?2 WHERE id=?1",
                    (id, session.as_bytes()),
                )?)
            })
        })
        .await
    }
    /// Retire an upload once storage confirms its complete bytes (§16.5): one
    /// transaction removes the queue row, its chunk hashes and the copy kept
    /// for it. No app row, file reference or write changes, so a row retargeted,
    /// moved or deleted meanwhile keeps its newer state. Afterwards this device
    /// reads the file from the cache or storage.
    pub async fn complete_upload(&self, id: i64) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                inner.access.with_writer(|writer| {
                    let result = writer.transaction(|db| file_queue::complete(db, id));
                    let active = inner.staging.lock().expect("staging lock poisoned");
                    crate::file_removals::FileRemovals::new(
                        writer,
                        &inner.access.directory,
                        &active,
                    )
                    .finish(result)
                })
            })
            .await
    }
    /// Reserve a cache file while it is assembled and checked. Only the writer
    /// can pin; read-only file owners continue to cache individual chunks.
    pub async fn reserve_cache_file(&self, name: FileName) -> Result<CacheReservation, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let lease = database.file_tasks.clone().read_owned().await;
        let resources = database.inner.clone();
        database
            .call(move |inner| {
                inner.access.with_writer(|writer| {
                    writer.transaction(|db| {
                        if db.query_row(
                            "SELECT EXISTS(
                                SELECT 1 FROM _coven_device_files WHERE path=?1 UNION ALL
                                SELECT 1 FROM _coven_cache WHERE path=?1)",
                            [name.as_str()],
                            |r| r.get::<_, bool>(0),
                        )? {
                            return Err(DbError::FileNameReused { name: name.clone() });
                        }
                        db.internal_execute(
                            "INSERT INTO _coven_file_removals(area,path) VALUES(?1,?2)",
                            ("cache", name.as_str()),
                        )?;
                        Ok(())
                    })?;
                    inner
                        .staging
                        .lock()
                        .expect("staging lock poisoned")
                        .insert(name.clone());
                    Ok::<_, DbError>(())
                })?;
                Ok(CacheReservation {
                    database: resources,
                    name,
                    lease: Some(lease),
                })
            })
            .await
    }

    /// Open a database commit observation for application file rows and upload
    /// metadata. Subscribing before reading the work list prevents missed commits.
    pub fn changes(&self) -> DatabaseChanges {
        self.file_changes(false)
    }
    /// Observe the file worker's work: changed application file rows and newly
    /// queued uploads. A confirmed upload retiring its own queue row wakes
    /// nothing (§16.5).
    pub fn queued_work_changes(&self) -> DatabaseChanges {
        self.file_changes(true)
    }
    fn file_changes(&self, queued_only: bool) -> DatabaseChanges {
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
                    .access
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
                            inserted_only: false,
                        })
                    })
                    .chain(std::iter::once(crate::observation::TableRead {
                        table: "_coven_file_uploads".into(),
                        columns: ["reference".into()].into(),
                        keys: crate::key_scope::KeyScope::All,
                        inserted_only: queued_only,
                    }))
                    .collect::<Vec<_>>();
                let commits = inner.access.observer.subscribe();
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
    /// are excluded from application download progress. A file this device
    /// still has queued is wholly local.
    pub async fn missing_bytes(&self, file: &FileRef) -> Result<u64, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, device| {
            if file.uploader() == device && file_queue::queued(db, &file)? {
                return Ok(0);
            }
            let id = cache::id(&file)?;
            if db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_cache
                 WHERE namespace=?1 AND file_id=?2 AND chunk=?3)",
                (file.namespace(), &id, cache::COMPLETE_FILE),
                |r| r.get::<_, bool>(0),
            )? {
                return Ok(0);
            }
            let cached = db.query_row(
                "SELECT coalesce(sum(size-?3),0) FROM _coven_cache
                 WHERE namespace=?1 AND file_id=?2 AND chunk>=0",
                (
                    file.namespace(),
                    &id,
                    coven_crypto::FILE_CHUNK_TAG_LEN as i64,
                ),
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

/// Bytes reserved for a verified whole-file cache pin.
pub struct CacheReservation {
    database: Arc<RwLock<Option<DatabaseInner>>>,
    name: FileName,
    lease: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}
impl CacheReservation {
    /// Atomically publish a fully synced and content-checked whole file as pinned.
    /// Any older chunk files become recorded removals in the same transaction.
    pub async fn publish(self, file: &FileRef) -> Result<(), DbError> {
        let mut pending = self;
        let file = file.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                {
                    let slot = pending.database.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    inner.access.with_writer(|writer| {
                        writer.transaction(|db| {
                            file_ref::validate(db, &inner.access.write_schema, &file)?;
                            cache::publish(db, &inner.access.directory, &file, &pending.name)
                        })?;
                        let mut active = inner.staging.lock().expect("staging lock poisoned");
                        active.remove(&pending.name);
                        pending.lease.take();
                        crate::file_removals::FileRemovals::new(
                            writer,
                            &inner.access.directory,
                            &active,
                        )
                        .finish(Ok::<_, DbError>(()))
                    })?;
                }
                Ok(())
            })
            .await,
        )
    }
}
impl Drop for CacheReservation {
    fn drop(&mut self) {
        let Some(_lease) = self.lease.take() else {
            return;
        };
        let slot = self.database.read().expect("database lock poisoned");
        slot.as_ref()
            .expect("reservation holds close guard")
            .release_file_reservations(std::iter::once(&self.name));
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

//! File metadata operations through the owning database, never through app SQL.

use super::{finish_blocking, Database, DatabaseAccess, DatabaseInner, DatabaseReadHandle};
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
pub(crate) use cache::{id as cache_id, pinned as cache_pinned};

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
    /// Pending eager work: changed references and temporary failures awaiting
    /// their retry wait, from one indexed table (§16.4, SP step 7).
    pub async fn eager_work(&self) -> Result<Vec<FileRef>, DbError> {
        self.run(|db, _, _, _| crate::eager_work::read(db)).await
    }
    /// Settle eager work: downloaded, permanently refused or cancelled.
    pub async fn settle_eager_work(&self, files: Vec<FileRef>) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                files
                    .iter()
                    .try_for_each(|file| crate::eager_work::settle(db, file))
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
    /// replacing an earlier reason, with its retry wait, or with none when it
    /// waits for the app or an update (§16.3, §16.5). Read-only handles keep
    /// no records.
    pub async fn fail_download<P: crate::PendingValue>(
        &self,
        record: impl FnOnce(DeviceId) -> P + Send + 'static,
        subject: crate::RetrySubject,
        wait: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let device = self.run(|_, _, _, device| Ok::<_, DbError>(device)).await?;
        database
            .record_pending_retry(record(device), subject, wait)
            .await
    }
    /// Every download's persisted retry wait (§16.5), read without the
    /// writer. A read-only handle persists none.
    pub async fn download_waits(
        &self,
    ) -> Result<Vec<(crate::RetrySubject, crate::RetryWait)>, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(Vec::new());
        };
        database.download_retry_waits().await
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
    /// Whether the app paused uploads, as persisted (E7).
    pub async fn uploads_paused(&self) -> Result<bool, DbError> {
        self.run(|db, _, _, _| file_queue::paused(db)).await
    }
    /// Persist the app's pause setting. A paused upload keeps its place and
    /// its provider session.
    pub async fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| db.transaction(|db| file_queue::set_paused(db, paused)))
            .await
    }
    /// Each queued upload with this device's pending record about its file,
    /// read in one committed state: the reason an earlier attempt left decides
    /// whether automatic work retries it (§16.5).
    pub async fn upload_work<P: crate::PendingValue>(
        &self,
        subject: impl Fn(&FileRef) -> P::Subject + Send + 'static,
    ) -> Result<Vec<(FileUpload, Option<P>)>, DbError> {
        self.run(move |db, _, _, device| {
            db.read_transaction(|| {
                file_queue::read(db)?
                    .into_iter()
                    .map(|upload| {
                        let record = crate::pending::find::<P>(db, &subject(&upload.file), device)?;
                        Ok((upload, record))
                    })
                    .collect()
            })
        })
        .await
    }
    /// Record a failed attempt as this device's pending record about its file,
    /// with its retry wait or none when it waits for a prerequisite or the app,
    /// in one transaction (§16.5, §19.1).
    pub async fn fail_upload<P: crate::PendingValue>(
        &self,
        id: i64,
        record: impl FnOnce(DeviceId) -> P + Send + 'static,
        retry: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, device| {
            db.transaction(|db| {
                if !db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads WHERE id=?1)",
                    [id],
                    |r| r.get::<_, bool>(0),
                )? {
                    return Err(DbError::Invariant {
                        what: "the upload recording progress is queued",
                    });
                }
                crate::pending::save(db, &record(device))?;
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
    /// file is no longer queued. Its bytes are checked with no writer held.
    pub async fn open_upload_source(
        &self,
        file: &FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let file = file.clone();
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| open_upload_source(&inner.access, &file))
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |access| open_upload_source(access, &file))
                    .await
            }
        }
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
    /// transaction removes the queue row, its chunk hashes, the copy kept for
    /// it and this device's pending record about the file. No app row, file
    /// reference or write changes, so a row retargeted, moved or deleted
    /// meanwhile keeps its newer state. Afterwards this device reads the file
    /// from the cache or storage.
    pub async fn complete_upload<S: serde::Serialize + Send + 'static>(
        &self,
        id: i64,
        subject: S,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                let (result, chosen) = inner.access.with_writer(|writer| {
                    let device = inner.access.device;
                    let schema = &inner.access.write_schema;
                    let result = writer.transaction(|db| {
                        let reference = file_queue::complete(db, id)?;
                        crate::eager_work::uploaded(db, schema, &reference)?;
                        crate::pending::clear(db, &subject, device)
                    });
                    Ok::<_, DbError>((result, super::released(writer, &inner.staging)))
                })?;
                inner.finish_removals(result, chosen)
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

    /// Observe the file worker's work: changed application file rows and newly
    /// queued uploads. A confirmed upload retiring its own queue row wakes
    /// nothing (§16.5). Subscribing before reading the work list prevents
    /// missed commits.
    pub fn queued_work_changes(&self) -> DatabaseChanges {
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
                        inserted_only: true,
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

    /// Cached encrypted chunk or header (-1). Its bytes are read with no
    /// writer held (§3.1); an entry evicted meanwhile reads as not cached.
    pub async fn cached(&self, file: &FileRef, index: i64) -> Result<Option<Vec<u8>>, DbError> {
        let file = file.clone();
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| cache::read(&inner.access, &file, index))
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |access| cache::read(access, &file, index))
                    .await
            }
        }
    }
    /// Keep one verified range of `chunks` consecutive encrypted chunks from
    /// `first`, or the header (`first` = -1), durably before publishing its
    /// cache metadata in one transaction. The bytes are written with no
    /// writer held (§3.1), on a blocking worker that finishes even if the
    /// caller stops waiting.
    pub async fn cache_range(
        &self,
        file: &FileRef,
        first: i64,
        chunks: i64,
        name: FileName,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        let file = file.clone();
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| {
                        cache::put(&inner.access, &file, first, chunks, &name, &bytes)
                    })
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |access| cache::put(access, &file, first, chunks, &name, &bytes))
                    .await
            }
        }
    }
    /// Remove the bytes of cache entries the writer just chose, once it is
    /// free (§3.1, §16.6). Returns the failures, whose records stay for the
    /// next attempt.
    async fn remove_released(
        &self,
        chosen: crate::file_removals::Chosen,
    ) -> Result<Vec<DbError>, DbError> {
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| Ok::<_, DbError>(inner.access.remove_released(chosen)))
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |inner| Ok::<_, DbError>(inner.remove_released(chosen)))
                    .await
            }
        }
    }
    async fn removed(&self, chosen: crate::file_removals::Chosen) -> Result<(), DbError> {
        let failures = self.remove_released(chosen).await?;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::FileCleanup {
                write: Ok(()),
                failures,
            })
        }
    }
    /// Evict unpinned chunks in least-recently-read order, independently per namespace.
    pub async fn trim_cache(&self, namespace: &str) -> Result<(), DbError> {
        let namespace = namespace.to_owned();
        let chosen = self
            .run(move |db, _, _, _| {
                let paths = cache::trim(db, &namespace)?;
                Ok::<_, DbError>(crate::file_removals::choose_cache(db, &paths))
            })
            .await?;
        self.removed(chosen).await
    }
    /// Remove only cached bytes, pinned or not, without any storage operation.
    pub async fn evict(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        let chosen = self
            .run(move |db, schema, _, _| {
                db.transaction(|db| file_ref::validate(db, schema, &file))?;
                let paths = cache::evict(db, &file)?;
                Ok::<_, DbError>(crate::file_removals::choose_cache(db, &paths))
            })
            .await?;
        self.removed(chosen).await
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
            Ok(file.plaintext_size() - crate::cache_progress::covered(db, &file, device)?)
        })
        .await
    }
    /// Reserve eager downloads of these references; a cancelled set gives
    /// way to them (§16.4).
    pub async fn request_eager(&self, files: Vec<FileRef>) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| crate::cache_progress::request_eager(db, &files))
        })
        .await
    }
    /// Commit cancellation of the requested eager set before reporting it.
    pub async fn cancel_eager(&self) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(|db, _, _, _| db.transaction(crate::cache_progress::cancel_eager))
            .await
    }
    /// Retire the requested eager set together once all of it is cached.
    pub async fn retire_eager(&self) -> Result<bool, DbError> {
        self.require_writer()?;
        self.run(|db, _, _, device| {
            db.transaction(|db| crate::cache_progress::retire_eager(db, device))
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
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                {
                    let slot = pending.database.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    // Disk reads finish before the writer is taken: a stalled
                    // read must not hold back app writes (§3.1).
                    let size = cache::whole_size(&inner.access.directory, &file, &pending.name)?;
                    inner
                        .access
                        .with_writer(|writer| {
                            writer.transaction(|db| {
                                file_ref::validate(db, &inner.access.write_schema, &file)?;
                                cache::publish(db, &file, &pending.name, size)
                            })?;
                            inner
                                .staging
                                .lock()
                                .expect("staging lock poisoned")
                                .remove(&pending.name);
                            pending.lease.take();
                            Ok::<_, DbError>(super::released(writer, &inner.staging))
                        })
                        // The entries it replaced go once the writer is free.
                        .and_then(|chosen| inner.finish_removals(Ok::<_, DbError>(()), chosen))?;
                }
                Ok(())
            }))
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
/// Find a queued file's source under the writer, then open and check its
/// bytes once the writer is free: checking reads the whole file (§3.1).
fn open_upload_source<L>(
    access: &DatabaseAccess<L>,
    file: &FileRef,
) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
    let source = access.with_writer(|db| file_ref::queued_source(db, file))?;
    source
        .map(|source| file_ref::open_source(&access.directory, file, source))
        .transpose()
}
fn changed(count: usize) -> Result<(), DbError> {
    if count == 1 {
        Ok(())
    } else {
        Err(DbError::Invariant {
            what: "a queued upload's update changes exactly its row",
        })
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

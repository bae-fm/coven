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
pub(crate) use cache::{
    declare_budgets, id as cache_id, kept as cache_kept, pinned as cache_pinned,
};

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

/// What supplied an already-complete pin without a download.
pub enum PinnedFile {
    /// A complete cache file whose content hash was verified.
    Cached,
    /// A local source still awaiting upload; pinning did not verify its bytes.
    Queued,
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

    /// A read-only query on one committed state. A writable handle reads on
    /// its read pool, so read-only decisions never take the writer (§21.2).
    async fn read<T: Send + 'static, E: From<DbError> + Send + 'static>(
        &self,
        read: impl FnOnce(&DatabaseConnection) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E> {
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| inner.access.with_committed(read))
                    .await
            }
            FileDatabaseAccess::Reader(_) => self.run(move |db, _, _, _| read(db)).await,
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
    /// Pins carried from completed uploads that still wait for a whole copy.
    pub async fn pin_requests(&self) -> Result<Vec<FileRef>, DbError> {
        self.read(cache::pin_requests).await
    }
    /// Drop a carried pin that cannot be kept: refused for good, or its row
    /// no longer names the file.
    pub async fn settle_pin_request(&self, file: FileRef) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| db.transaction(|db| cache::settle_pin(db, &file)))
            .await
    }
    /// The uploader's applied state and, for this device's uploads, its local
    /// record about `subject`, read
    /// in one committed state: the evidence a confirmed absence is judged
    /// with (§17.4). A device with no registration here has no removal or
    /// replacement either, so it reads as active.
    pub async fn uploader_evidence<P: crate::PendingValue>(
        &self,
        uploader: DeviceId,
        subject: P::Subject,
    ) -> Result<(crate::DeviceState, Option<P>), DbError>
    where
        P::Subject: Send,
    {
        self.run(move |db, _, _, device| {
            db.read_transaction(|| {
                let state = db
                    .query(
                        "SELECT state FROM _coven_devices WHERE device=?1",
                        [uploader.0.to_be_bytes()],
                        |r| crate::store_state_tables::device_state(r.get(0)?),
                    )?
                    .pop()
                    .unwrap_or(crate::DeviceState::Active);
                let record = if uploader == device {
                    crate::pending::find::<P>(db, &subject)?
                } else {
                    None
                };
                Ok((state, record))
            })
        })
        .await
    }
    /// Record this device's first unmet condition for one file it reads,
    /// replacing an earlier reason, with its retry wait, or with none when it
    /// waits for the app or an update (§17.5, §6.7). Read-only handles keep
    /// no records.
    pub async fn fail_download<P: crate::PendingValue>(
        &self,
        record: P,
        subject: crate::RetrySubject,
        wait: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(());
        };
        database.record_pending_retry(record, subject, wait).await
    }
    /// The persisted wait for this fixed download; read-only handles keep none.
    pub async fn download_wait(
        &self,
        subject: crate::RetrySubject,
    ) -> Result<Option<crate::RetryWait>, DbError> {
        match &self.access {
            FileDatabaseAccess::Writer(database) => database.retry_wait(subject).await,
            FileDatabaseAccess::Reader(_) => Ok(None),
        }
    }
    /// Every download's persisted retry wait (§6.7), read without the
    /// writer. A read-only handle persists none.
    pub async fn download_waits(
        &self,
    ) -> Result<Vec<(crate::RetrySubject, crate::RetryWait)>, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(Vec::new());
        };
        database.download_retry_waits().await
    }
    /// Clear only conditions the completed read actually checked. The
    /// predicate and deletion share the pending transaction. The record's
    /// download wait and any queued upload wait end with it; return the
    /// upload subject so the caller can discard its in-memory timer.
    pub async fn clear_file_pending<P: crate::PendingValue>(
        &self,
        file: &FileRef,
        subject: P::Subject,
        checked: impl Fn(&P) -> bool + Send + 'static,
    ) -> Result<Option<crate::RetrySubject>, DbError>
    where
        P::Subject: Send + 'static,
    {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(None);
        };
        database
            .clear_pending_if::<P>(file.clone(), subject, checked)
            .await
    }
    /// Record a local source failure only while its upload remains queued.
    /// The retry is selected from the current upload wait and committed with
    /// the record, so concurrent upload completion cannot leave a stale failure.
    pub async fn fail_local_read<P: crate::PendingValue>(
        &self,
        file: &FileRef,
        record: P,
        next: impl FnOnce(Option<crate::RetryWait>) -> Result<crate::RetryWait, DbError>
            + Send
            + 'static,
    ) -> Result<Option<(crate::RetrySubject, crate::RetryWait)>, DbError> {
        let FileDatabaseAccess::Writer(_) = &self.access else {
            return Ok(None);
        };
        let file = file.clone();
        self.run(move |db, _, _, _| {
            db.pending_transaction(|db| {
                let Some(id) = file_queue::upload_id(db, &file)? else {
                    return Ok(None);
                };
                let subject = crate::RetrySubject::Upload(id);
                let wait = next(crate::retry::find(db, subject)?)?;
                crate::pending::save(db, &record)?;
                crate::retry::save(db, subject, &wait)?;
                Ok(Some((subject, wait)))
            })
        })
        .await
    }

    /// Durable queue, including files whose row changed after attachment.
    pub async fn uploads(&self) -> Result<Vec<FileUpload>, DbError> {
        self.read(file_queue::read).await
    }
    /// Whether the app paused uploads, as persisted (E7).
    pub async fn uploads_paused(&self) -> Result<bool, DbError> {
        self.read(file_queue::paused).await
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
    /// whether automatic work retries it (§17.7).
    pub async fn upload_work<P: crate::PendingValue>(
        &self,
        subject: impl Fn(&FileRef) -> P::Subject + Send + 'static,
    ) -> Result<Vec<(FileUpload, Option<P>)>, DbError> {
        self.run(move |db, _, _, _| {
            db.read_transaction(|| {
                file_queue::read(db)?
                    .into_iter()
                    .map(|upload| {
                        let record = crate::pending::find::<P>(db, &subject(&upload.file))?;
                        Ok((upload, record))
                    })
                    .collect()
            })
        })
        .await
    }
    /// Record a failed attempt as this device's pending record about its file,
    /// with its retry wait or none when it waits for a prerequisite or the app,
    /// in one transaction (§17.7, §19.1).
    pub async fn fail_upload<P: crate::PendingValue>(
        &self,
        id: i64,
        record: P,
        retry: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
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
                crate::pending::save(db, &record)?;
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
        self.read(move |db| file_queue::chunk_hash(db, &file, index))
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
    /// Whether completing this queued upload would keep coven's own copy as
    /// the file's cache copy: the queue's source is that copy of app-provided
    /// bytes, and the file is pinned while queued and still named by its row
    /// (§17.6). A user's original is never read again after its upload
    /// (§17.1), so it never answers yes.
    /// Completion decides again in its own transaction; this lets the
    /// uploader write the copy first.
    pub async fn upload_keeps_copy(&self, file: &FileRef) -> Result<bool, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let file = file.clone();
        database
            .call(move |inner| {
                inner.access.with_committed(|db| {
                    let schema = &inner.access.write_schema;
                    Ok(matches!(
                        file_ref::queued_source(db, &file)?,
                        Some(file_ref::QueuedSource::Copy(_))
                    ) && file_queue::pinned(db, &file)?
                        && file_ref::naming(db, schema, &file)?.is_some())
                })
            })
            .await
    }
    /// Retire an upload once storage confirms its complete bytes (§17.7): one
    /// transaction removes the queue row, its chunk hashes, the copy kept for
    /// it and this device's pending record about the file. No app row, file
    /// reference or write changes, so a row retargeted, moved or deleted
    /// meanwhile keeps its newer state. Afterwards this device reads the file
    /// from the cache or storage.
    ///
    /// A file pinned while queued and still named by its row takes `copy`,
    /// the whole encrypted file the uploader wrote from coven's own copy, as
    /// its pinned cache copy in that transaction (§17.6). Without one, the
    /// pin becomes the stored file's carried pin, kept whole from storage. A
    /// copy nothing needs any more is removed with the queue's own.
    pub async fn complete_upload<S: serde::Serialize + Send + 'static>(
        &self,
        id: i64,
        subject: S,
        copy: Option<CacheReservation>,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let resources = database.inner.clone();
        // Its own blocking task, as `CacheReservation::publish` has: the copy
        // is dropped after the database guard on every path, since releasing
        // it takes that lock again.
        finish_blocking(
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                let mut copy = copy;
                let slot = resources.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let completed = inner.call(|inner| complete(inner, id, &subject, copy.as_mut()));
                if let Some(copy) = &mut copy {
                    copy.release(inner);
                }
                completed
            }))
            .await,
        )
    }
    /// Reserve a whole cache file of `file` while it is written and checked.
    /// Only the writer can pin or complete uploads; read-only file owners
    /// continue to cache individual chunks.
    pub async fn reserve_cache_file(
        &self,
        file: &FileRef,
        name: FileName,
    ) -> Result<CacheReservation, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let lease = database.file_tasks.clone().read_owned().await;
        let resources = database.inner.clone();
        let file = file.clone();
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
                    file,
                    name,
                    lease: Some(lease),
                })
            })
            .await
    }

    /// Observe the file worker's work: changed application file rows and newly
    /// queued uploads. A confirmed upload retiring its own queue row wakes
    /// nothing (§17.7). Subscribing before reading the work list prevents
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
    /// writer held (§21.2); an entry evicted meanwhile reads as not cached.
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
    /// writer held (§21.2), on a blocking worker that finishes even if the
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
    /// free (§21.2, Appendix C.3). Returns the failures, whose records stay for the
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
        crate::write_failure::after_cleanup(Ok(()), failures)
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
    /// Pin a complete, previously checked cache file, or a file still queued
    /// here, atomically if one exists. The presence check and state change
    /// share a transaction with eviction and with the upload's completion.
    pub async fn pin_complete(&self, file: &FileRef) -> Result<Option<PinnedFile>, DbError> {
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
                let changed = db.internal_execute(
                    "UPDATE _coven_cache_budgets SET bytes=?2 WHERE namespace=?1",
                    (&namespace, bytes.to_be_bytes().as_slice()),
                )?;
                if changed == 0 {
                    return Err(DbError::UnknownNamespace { namespace });
                }
                Ok(())
            })
        })
        .await
    }
    /// Plaintext bytes absent from this namespace's cache; headers and tags
    /// are excluded from application download progress. A file this device
    /// still has queued is wholly local.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn missing_bytes(&self, file: &FileRef) -> Result<u64, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, device| {
            Ok(file.plaintext_size() - crate::cache_progress::covered(db, &file, device)?)
        })
        .await
    }
    /// The namespace's budget, declared or since set.
    pub async fn budget(&self, namespace: &str) -> Result<u64, DbError> {
        let namespace = namespace.to_owned();
        self.run(move |db, _, _, _| cache::budget(db, &namespace))
            .await
    }
}

/// Bytes reserved for one file's whole encrypted cache file while it is
/// written: a pin's, or an upload's own copy.
pub struct CacheReservation {
    database: Arc<RwLock<Option<DatabaseInner>>>,
    file: FileRef,
    name: FileName,
    lease: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}
impl CacheReservation {
    /// Atomically publish a fully synced and content-checked whole file as pinned.
    /// Any older chunk files become recorded removals in the same transaction.
    pub async fn publish(self) -> Result<(), DbError> {
        let mut pending = self;
        let database = pending.database.clone();
        finish_blocking(
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                {
                    let slot = database.read().expect("database lock poisoned");
                    let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                    // Disk reads finish before the writer is taken: a stalled
                    // read must not hold back app writes (§21.2).
                    let size =
                        cache::whole_size(&inner.access.directory, &pending.file, &pending.name)?;
                    inner
                        .access
                        .with_writer(|writer| {
                            writer.transaction(|db| {
                                file_ref::validate(db, &inner.access.write_schema, &pending.file)?;
                                cache::publish(db, &pending.file, &pending.name, size, true)
                            })?;
                            pending.release(inner);
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
    /// Give up the name once a transaction has decided its bytes: published,
    /// or left to its recorded removal. Releasing again does nothing.
    fn release(&mut self, inner: &DatabaseInner) {
        if self.lease.is_some() {
            inner.release_file_reservations(std::iter::once(&self.name));
            self.lease.take();
        }
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
/// One upload's completion and its cache copy, under the writer (§17.7).
/// The copy's bytes are checked first, with no writer held (§21.2).
fn complete<S: serde::Serialize>(
    inner: &DatabaseInner,
    id: i64,
    subject: &S,
    copy: Option<&mut CacheReservation>,
) -> Result<(), DbError> {
    let size = copy
        .as_deref()
        .map(|copy| cache::whole_size(&inner.access.directory, &copy.file, &copy.name))
        .transpose()?;
    let (result, chosen) = inner.access.with_writer(|writer| {
        let schema = &inner.access.write_schema;
        let result = writer.transaction(|db| {
            let (reference, pinned) = file_queue::complete(db, id)?;
            match copy.as_deref().zip(size) {
                Some((copy, size)) => {
                    if cache::id(&copy.file)? != cache::id(&reference)? {
                        return Err(DbError::Invariant {
                            what: "an upload keeps a cache copy of its own file",
                        });
                    }
                    // A pin keeps a file only while its row names it.
                    if pinned && file_ref::naming(db, schema, &reference)?.is_some() {
                        cache::publish(db, &reference, &copy.name, size, true)?;
                    }
                }
                None => {
                    if pinned {
                        cache::request_pin(db, &reference)?;
                    }
                }
            }
            crate::pending::clear(db, subject)
        });
        // Published, its name is the cache's; otherwise its recorded removal
        // goes with the queue's copy.
        if let Some(copy) = copy {
            copy.release(inner);
        }
        Ok::<_, DbError>((result, super::released(writer, &inner.staging)))
    })?;
    inner.finish_removals(result, chosen)
}
/// Find a queued file's source under the writer, then open and check its
/// bytes once the writer is free: checking reads the whole file (§21.2).
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

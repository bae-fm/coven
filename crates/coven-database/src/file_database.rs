//! File metadata operations through the owning database, never through app SQL.

use super::{finish_blocking, Database, DatabaseAccess, DatabaseInner, DatabaseReadHandle};
use crate::DatabaseChanges;
use crate::{
    file_queue, file_ref, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef,
    FileUpload,
};
use coven_crypto::SecretBytes;
use coven_foundation::{
    files::{FileName, StoreDir},
    id_source::{DeviceId, FileId},
};
use std::sync::{Arc, RwLock};

#[path = "file_cache.rs"]
mod cache;
pub(crate) use cache::{
    declare_budgets, file_id as file_cache_id, id as cache_id, kept as cache_kept,
    remove_entries as remove_cached,
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

    /// The files this process is sending now, which the upload worker marks
    /// as each attempt starts and ends (§17.9).
    pub fn uploads_in_flight(&self) -> crate::InFlight {
        match &self.access {
            FileDatabaseAccess::Writer(database) => database.uploads_in_flight(),
            FileDatabaseAccess::Reader(database) => database.uploads_in_flight(),
        }
    }

    /// The files this process is downloading to keep whole now, which each
    /// pin's download marks as it starts and ends (§17.8).
    pub fn downloads_in_flight(&self) -> crate::InFlight {
        match &self.access {
            FileDatabaseAccess::Writer(database) => database.downloads_in_flight(),
            FileDatabaseAccess::Reader(database) => database.downloads_in_flight(),
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

    /// The file this device has a record of under this id.
    pub async fn file_ref(&self, file: FileId) -> Result<FileRef, DbError> {
        match &self.access {
            FileDatabaseAccess::Writer(db) => db.file_ref(file).await,
            FileDatabaseAccess::Reader(db) => db.file_ref(file).await,
        }
    }
    /// Open and check the local source of a file still on this device only,
    /// while it uploads. `None` means its bytes come from the cache or
    /// storage.
    pub async fn open_local(
        &self,
        file: &FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        match &self.access {
            FileDatabaseAccess::Writer(db) => db.open_local_file(file).await,
            FileDatabaseAccess::Reader(db) => db.open_local_file(file).await,
        }
    }
    /// The upload of a file this device uploads, its move, this device's
    /// record about the upload's bytes and its move's record, read in one
    /// committed state: the local facts its status is judged with (§17.6).
    /// `None` when this device doesn't upload the file.
    pub async fn local_evidence<P: crate::PendingValue>(
        &self,
        file: &FileRef,
        upload_subject: impl FnOnce(&crate::FileLocation) -> P::Subject + Send + 'static,
        move_subject: impl FnOnce(crate::MoveId) -> P::Subject + Send + 'static,
    ) -> Result<Option<LocalEvidence<P>>, DbError>
    where
        P::Subject: Send,
    {
        let file = file.clone();
        self.run(move |db, _, _, device| {
            db.read_transaction(|| {
                if file.place() != (crate::FilePlace::DeviceOnly { device }) {
                    return Ok(None);
                }
                let Some(upload) = file_queue::upload_of(db, file.id())? else {
                    return Ok(None);
                };
                let (location, of) = db.query_row(
                    "SELECT location,move FROM _coven_file_uploads WHERE id=?1",
                    [upload],
                    |r| Ok((r.get::<_, String>(0)?, crate::MoveId(r.get(1)?))),
                )?;
                let location = crate::FileLocation::decode(&location)?;
                let record = crate::pending::find::<P>(db, &upload_subject(&location))?;
                let moving = crate::pending::find::<P>(db, &move_subject(of))?;
                let original = crate::user_file::read(db, file.id())?.map(|original| original.path);
                Ok(Some(LocalEvidence {
                    upload,
                    location,
                    of,
                    original,
                    record,
                    moving,
                }))
            })
        })
        .await
    }
    /// Fail a move because a user's original changed before its upload
    /// (§17.3): none of its files switches, each stays on this device only,
    /// and its uploads retire, their stored bytes becoming deletion
    /// candidates. `record` is the move's own and says which original. A
    /// move that already ended keeps its state.
    pub async fn fail_move<P: crate::PendingValue>(
        &self,
        upload: i64,
        record: P,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.pending_transaction(|db| {
                let Some(of) = record.move_of() else {
                    return Err(DbError::Invariant {
                        what: "a failed move's record names the move",
                    });
                };
                if crate::file_moves::of_upload(db, upload)? != of {
                    return Err(DbError::Invariant {
                        what: "a move fails through one of its own uploads",
                    });
                }
                if crate::file_moves::state(db, of)? != Some(crate::file_moves::MoveState::Moving) {
                    return Ok(());
                }
                crate::file_moves::end_unswitched(db, of, crate::file_moves::MoveState::Failed)?;
                crate::pending::save(db, &record)
            })
        })
        .await
    }
    /// Clear the record `subject` names if `checked` says the read or upload
    /// of `upload`'s original that just succeeded settles it, by the record
    /// and the original's path. The predicate and deletion share the
    /// pending transaction, which also ends the upload's retry wait the
    /// failure armed. Returns the original's path when it cleared.
    pub async fn clear_original<P: crate::PendingValue>(
        &self,
        upload: i64,
        subject: P::Subject,
        checked: impl Fn(&P, &std::path::Path) -> bool + Send + 'static,
    ) -> Result<Option<std::path::PathBuf>, DbError>
    where
        P::Subject: Send + 'static,
    {
        let FileDatabaseAccess::Writer(_) = &self.access else {
            return Ok(None);
        };
        self.run(move |db, _, _, _| {
            db.pending_transaction(|db| {
                let original: Option<Option<Vec<u8>>> = db
                    .query(
                        "SELECT original FROM _coven_file_uploads WHERE id=?1",
                        [upload],
                        |r| r.get(0),
                    )?
                    .pop();
                let Some(Some(original)) = original else {
                    return Ok(None);
                };
                let path = crate::user_file::decode_path(original)?;
                if crate::pending::find::<P>(db, &subject)?
                    .is_some_and(|record| checked(&record, &path))
                {
                    crate::pending::clear(db, &subject)?;
                    crate::retry::clear(db, crate::RetrySubject::Upload(upload))?;
                    return Ok(Some(path));
                }
                Ok(None)
            })
        })
        .await
    }
    /// Retire the uploads no file waits for any more, except those whose
    /// objects are in `in_flight`, whose attempts retire them as they end
    /// (§17.9). Each retired upload's pending record goes with it, and its
    /// stored path becomes a deletion candidate unless something else
    /// protects it (§17.10). Returns the retired queue ids.
    pub async fn retire_unlinked<S: serde::Serialize + Send + 'static>(
        &self,
        in_flight: std::collections::BTreeSet<FileId>,
        subject: impl Fn(&crate::FileLocation) -> S + Send + 'static,
    ) -> Result<Vec<i64>, DbError> {
        // A read-only handle queues nothing and retires nothing.
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(Vec::new());
        };
        // An idle run writes nothing: only work found takes the writer.
        let work = database
            .call(move |inner| {
                inner
                    .access
                    .with_committed(|db| Ok::<_, DbError>(!file_queue::unlinked(db)?.is_empty()))
            })
            .await?;
        if !work {
            return Ok(Vec::new());
        }
        database
            .call(move |inner| {
                // The released copies go once the writer is free (§21.2). A
                // move whose last waiting upload retires switches its files
                // in this write (§17.3).
                inner.with_files(Vec::new(), |writer, files| {
                    writer.local_write(
                        &inner.access.write_schema,
                        inner.access.device,
                        inner.clock.now(),
                        files,
                        |_sql| files.retire_unlinked(&in_flight, subject),
                    )
                })
            })
            .await
    }
    /// Record this device's first unmet condition for the stored bytes a
    /// pin downloads, replacing an earlier reason. It keeps no wait: only the
    /// app's next pin or read tries again (§17.7). Read-only handles keep no
    /// records.
    pub async fn fail_download<P: crate::PendingValue>(&self, record: P) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(());
        };
        database.record_pending(record).await
    }
    /// Clear only conditions the completed download actually checked. The
    /// predicate and deletion share the pending transaction.
    pub async fn clear_download_pending<P: crate::PendingValue>(
        &self,
        subject: P::Subject,
        checked: impl Fn(&P) -> bool + Send + 'static,
    ) -> Result<(), DbError>
    where
        P::Subject: Send + 'static,
    {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Ok(());
        };
        database.clear_checked::<P>(subject, checked).await
    }
    /// Clear only conditions the completed local read actually checked. The
    /// predicate and deletion share the pending transaction. Any queued
    /// upload wait ends with it; return the upload subject so the caller can
    /// discard its in-memory timer.
    pub async fn clear_upload_pending<P: crate::PendingValue>(
        &self,
        object: (DeviceId, FileId),
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
            .clear_pending_if::<P>(object, subject, checked)
            .await
    }
    /// Record a local source failure only while its upload remains queued.
    /// The retry is selected from the current upload wait and committed with
    /// the record, so concurrent upload completion cannot leave a stale failure.
    pub async fn fail_local_read<P: crate::PendingValue>(
        &self,
        file: FileId,
        record: P,
        next: impl FnOnce(Option<crate::RetryWait>) -> Result<crate::RetryWait, DbError>
            + Send
            + 'static,
    ) -> Result<Option<(crate::RetrySubject, crate::RetryWait)>, DbError> {
        let FileDatabaseAccess::Writer(_) = &self.access else {
            return Ok(None);
        };
        self.run(move |db, _, _, _| {
            db.pending_transaction(|db| {
                let Some(id) = file_queue::upload_of(db, file)? else {
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

    /// Durable queue: each upload some file still waits for.
    pub async fn uploads(&self) -> Result<Vec<FileUpload>, DbError> {
        self.read(file_queue::read).await
    }
    /// Every queued upload, those no file waits for included.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn all_uploads(&self) -> Result<Vec<FileUpload>, DbError> {
        self.read(file_queue::read_all).await
    }
    /// The queue ids of the bound uploads, which the pause leaves running
    /// (§17.4).
    pub async fn bound_uploads(&self) -> Result<std::collections::BTreeSet<i64>, DbError> {
        self.read(file_queue::bound).await
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
    /// whether automatic work retries it (§17.9).
    pub async fn upload_work<P: crate::PendingValue>(
        &self,
        subject: impl Fn(&crate::FileLocation) -> P::Subject + Send + 'static,
    ) -> Result<Vec<(FileUpload, Option<P>)>, DbError> {
        self.run(move |db, _, _, _| {
            db.read_transaction(|| {
                file_queue::read(db)?
                    .into_iter()
                    // A stored upload only waits for the rest of its move.
                    .filter(|upload| !upload.stored)
                    .map(|upload| {
                        let record = crate::pending::find::<P>(db, &subject(&upload.location))?;
                        Ok((upload, record))
                    })
                    .collect()
            })
        })
        .await
    }
    /// Record a failed attempt as this device's pending record about its file,
    /// with its retry wait or none when it waits for a prerequisite or the app,
    /// in one transaction (§17.9, §19.1).
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
    /// upload's location and owned by the queue whatever its files became.
    pub async fn upload_chunk_hash(
        &self,
        location: &crate::FileLocation,
        index: u64,
    ) -> Result<coven_crypto::ContentHash, DbError> {
        let location = location.clone();
        self.read(move |db| file_queue::chunk_hash(db, &location, index))
            .await
    }
    /// Open the source an upload sends, from the facts its queue entry fixed
    /// at staging; later row changes cannot replace them. `None` means the
    /// upload is no longer queued. It is opened with no writer held.
    pub async fn open_upload_source(
        &self,
        upload: &FileUpload,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let (id, object, size) = (upload.id, upload.location.object(), upload.size);
        match &self.access {
            FileDatabaseAccess::Writer(database) => {
                database
                    .call(move |inner| open_upload_source(&inner.access, id, object, size))
                    .await
            }
            FileDatabaseAccess::Reader(database) => {
                database
                    .call(move |access| open_upload_source(access, id, object, size))
                    .await
            }
        }
    }

    /// Persist the provider recording after each confirmed change in progress,
    /// with the bytes it records as confirmed, which `coven_uploads` gives the
    /// app (E4). Storage alone reads the recording, so the caller gives both
    /// from the same session.
    #[expect(
        clippy::map_err_ignore,
        reason = "a byte count past SQLite's integers breaks the named invariant"
    )]
    pub async fn record_session(
        &self,
        id: i64,
        session: SecretBytes,
        progress: crate::UploadProgress,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        let count = |bytes: u64| {
            i64::try_from(bytes).map_err(|_| DbError::Invariant {
                what: "an upload's size fits SQLite",
            })
        };
        let confirmed = count(progress.bytes_confirmed)?;
        let total = count(progress.bytes_total)?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                changed(db.internal_execute(
                    "UPDATE _coven_file_uploads SET session=?2,bytes_confirmed=?3,bytes_total=?4
                     WHERE id=?1",
                    (id, session.as_bytes(), confirmed, total),
                )?)
            })
        })
        .await
    }
    /// Complete an upload once storage confirms its complete bytes (§17.9):
    /// one local transaction switches each file the upload stores to the
    /// cloud, as this device's write, and removes the queue row, its chunk
    /// hashes, the copy kept for it and this device's pending record about
    /// it. A file gone meanwhile switches nothing. Afterwards this device
    /// reads the files from the cache or storage. A write that can't be made
    /// now fails the transaction, and the upload stays queued.
    pub async fn complete_upload<S: serde::Serialize + Send + 'static>(
        &self,
        id: i64,
        subject: S,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let subject = serde_json::to_vec(&subject).map_err(DbError::PendingEncoding)?;
        database
            .call(move |inner| complete(inner, id, subject))
            .await
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
                                SELECT 1 FROM _coven_file_uploads WHERE copy=?1 UNION ALL
                                SELECT 1 FROM _coven_pin_requests WHERE copy=?1 UNION ALL
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

    /// Observe the upload worker's work: newly queued uploads, and uploads a
    /// file stopped waiting for. Subscribing before reading the work list
    /// prevents missed commits.
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
                // A newly queued upload, and an upload a file stopped
                // waiting for, which the worker then retires (§17.9).
                let reads = vec![
                    crate::observation::TableRead {
                        table: "_coven_file_uploads".into(),
                        columns: ["location".into()].into(),
                        scope: crate::row_scope::RowScope::All,
                        inserted_only: true,
                    },
                    crate::observation::TableRead {
                        table: "_coven_upload_files".into(),
                        columns: ["file".into()].into(),
                        scope: crate::row_scope::RowScope::All,
                        inserted_only: false,
                    },
                ];
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
                Ok::<_, DbError>(crate::file_removals::choose_cache(&paths))
            })
            .await?;
        self.removed(chosen).await
    }
    /// Remove only cached bytes, pinned or not, without any storage operation.
    pub async fn evict(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        let chosen = self
            .run(move |db, _, _, _| {
                let paths = cache::evict(db, &file)?;
                Ok::<_, DbError>(crate::file_removals::choose_cache(&paths))
            })
            .await?;
        self.removed(chosen).await
    }
    /// Pin a complete, previously checked cache file of a stored file,
    /// atomically if one exists, and return whether it does. The presence
    /// check and state change share a transaction with eviction.
    pub async fn pin_complete(&self, file: &FileRef) -> Result<bool, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, _| db.transaction(|db| cache::pin_complete(db, &file)))
            .await
    }
    /// Release a stored file's budget exemption.
    pub async fn unpin(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        self.run(move |db, _, _, _| db.transaction(|db| cache::unpin(db, &file)))
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
                    return Err(DbError::UndeclaredNamespace { namespace });
                }
                Ok(())
            })
        })
        .await
    }
    /// Plaintext bytes absent from this namespace's cache; headers and tags
    /// are excluded from application download progress. A file only on this
    /// device is wholly local.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn missing_bytes(&self, file: &FileRef) -> Result<u64, DbError> {
        let file = file.clone();
        self.run(move |db, _, _, device| {
            Ok(file.size() - crate::cache_progress::covered(db, &file, device)?)
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
    /// A file no record here names the stored bytes of any more, as one
    /// made device-only or deleted while it downloaded, is `NoLongerStored`,
    /// and its bytes go instead (§17.8).
    pub async fn publish(self) -> Result<(), DbError> {
        let mut pending = self;
        let database = pending.database.clone();
        finish_blocking(
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                let slot = database.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let published = inner
                    .access
                    .with_writer(|writer| {
                        let published = writer.transaction(|db| {
                            cache::publish(db, &pending.file, &pending.name, true)
                        })?;
                        pending.release(inner);
                        Ok::<_, DbError>((published, super::released(writer, inner)))
                    })
                    // The entries it replaced, or its own bytes, go once
                    // the writer is free.
                    .and_then(|(published, chosen)| {
                        inner
                            .finish_removals(Ok::<_, DbError>(()), chosen)
                            .map(|()| published)
                    })?;
                if published {
                    Ok(())
                } else {
                    Err(DbError::NoLongerStored {
                        file: pending.file.id(),
                    })
                }
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
/// One upload's completion (§17.9): a local write that switches the files
/// the upload stores to the cloud, then retires the upload.
fn complete(inner: &DatabaseInner, id: i64, subject: Vec<u8>) -> Result<(), DbError> {
    inner.with_files(Vec::new(), |writer, files| {
        writer.local_write(
            &inner.access.write_schema,
            inner.access.device,
            inner.clock.now(),
            files,
            |_sql| {
                files.complete_upload(
                    id,
                    Box::new(move |db| crate::pending::clear_encoded(db, subject)),
                )
            },
        )
    })
}
/// Find a queued upload's source under the writer, then open it once the
/// writer is free: opening is file IO (§21.2).
fn open_upload_source<L>(
    access: &DatabaseAccess<L>,
    upload: i64,
    object: FileId,
    size: u64,
) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
    let source = access.with_writer(|db| file_ref::queued_source(db, upload))?;
    source
        .map(|source| file_ref::open_source(&access.directory, &object.to_string(), size, source))
        .transpose()
}

impl Database {
    /// Settle what other devices' writes left for this device's files, in
    /// one write of its own (§17.2): delete each record they left with no
    /// reference, and switch each new file their moves made over bytes this
    /// device had stored to the same stored bytes. An audience reloading, or
    /// a pause of this device's writes, leaves its files for a later call.
    pub async fn settle_files(&self) -> Result<(), DbError> {
        self.call(move |inner| {
            let waiting = inner.access.with_committed(|db| {
                db.query_row(
                    "SELECT (EXISTS(SELECT 1 FROM _coven_unreferenced_files)
                             OR EXISTS(SELECT 1 FROM _coven_moved_copies))
                        AND NOT EXISTS(SELECT 1 FROM _coven_write_pause)",
                    [],
                    |r| r.get::<_, bool>(0),
                )
            })?;
            if !waiting {
                return Ok(());
            }
            inner.with_files(Vec::new(), |writer, files| {
                writer.local_write(
                    &inner.access.write_schema,
                    inner.access.device,
                    inner.clock.now(),
                    files,
                    |_sql| files.settle(),
                )
            })
        })
        .await
    }
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

/// What this device knows locally about a file it uploads (§17.6): the
/// upload, where its bytes go, its move, and the records about each.
#[derive(Debug)]
pub struct LocalEvidence<P> {
    /// The upload's queue id.
    pub upload: i64,
    /// Where its bytes go, with their key.
    pub location: crate::FileLocation,
    /// The move it belongs to.
    pub of: crate::MoveId,
    /// The file's original, for a user's file.
    pub original: Option<std::path::PathBuf>,
    /// This device's record about the upload's bytes.
    pub record: Option<P>,
    /// The move's record.
    pub moving: Option<P>,
}

#[path = "file_database_pins.rs"]
mod pins;

#[path = "file_database_copies.rs"]
mod copies;
pub use copies::{CopyReservation, FolderCopy};

#[cfg(test)]
#[path = "file_database_tests.rs"]
mod tests;

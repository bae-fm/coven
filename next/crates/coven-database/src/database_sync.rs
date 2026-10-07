//! Database operations used by synchronization, on the retained connections.

use super::{finish_blocking, Database};
use crate::{CovenResult, DbError};

impl Database {
    /// Keep first-observed times for exactly the writes still waiting after a
    /// download step. Resolved waits are removed in the same transaction.
    pub async fn waiting_writes(
        &self,
        writes: Vec<crate::WriteId>,
    ) -> Result<Vec<(crate::WriteId, std::time::SystemTime)>, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::waiting_write::record(&writer, writes, inner.clock.now())
            })
            .await,
        )
    }
    /// Consume bounded plaintext streams inside one transaction. `authenticate`
    /// runs after every stream ends and must confirm the complete sealed object,
    /// including its signature. Any stream or final-check error rolls back all
    /// rows, losses, file references and positions. No whole write is collected.
    pub async fn apply_downloaded_stream<R, F>(
        &self,
        write: crate::DownloadedWriteStream<R>,
        store_log: coven_format::value::EntryPositions,
        authenticate: F,
    ) -> Result<crate::ApplyOutcome, DbError>
    where
        R: std::io::Read + Send + 'static,
        F: FnOnce() -> Result<(), DbError> + Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
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
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let result = reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            inner.write_schema.prepare(reader)?;
                            // Pin the before view before the streamed transaction changes merge state.
                            reader.schema_version()?;
                            write.apply(
                                &writer,
                                reader,
                                &inner.write_schema,
                                inner.clock.now(),
                                &files,
                                &store_log,
                                authenticate,
                            )
                        })
                    });
                    files.finish(result)
                }));
                drop(writer);
                match result {
                    Ok(result) => result,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            })
            .await,
        )
    }
    /// The provider's opaque upload recording, retained with the fixed seal.
    pub async fn write_upload_session(
        &self,
        write: crate::WriteId,
    ) -> Result<Option<Vec<u8>>, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::upload::session(&writer, write)
            })
            .await,
        )
    }

    /// Record a session before sending and after provider-confirmed progress.
    pub async fn keep_write_upload_session(
        &self,
        write: crate::WriteId,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::upload::keep_session(&writer, write, bytes)
            })
            .await,
        )
    }
    /// Seal the oldest plaintext upload directly into a SQLite BLOB. The first
    /// successful attempt commits all bytes before returning its position. The
    /// callback is skipped on retry; an error or panic rolls back the whole seal.
    /// The committed store-log view and app version select the sealing keys.
    pub async fn prepare_write_upload<F>(&self, seal: F) -> Result<Option<crate::WriteId>, DbError>
    where
        F: FnOnce(
                &crate::StoreLog,
                u32,
                crate::WaitingUpload<'_>,
                &mut dyn FnMut(&[u8]) -> Result<(), DbError>,
            ) -> Result<(), DbError>
            + Send
            + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::upload::prepare(&writer, seal)
                }));
                drop(writer);
                match result {
                    Ok(result) => result,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            })
            .await,
        )
    }
    /// Apply one authenticated download, or report the prerequisite it awaits.
    /// Row changes, merge state, fingerprints and positions commit together.
    pub async fn apply_downloaded(
        &self,
        write: crate::DownloadedWrite,
    ) -> Result<crate::ApplyOutcome, DbError> {
        let database = self.clone();
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
                files.finish(crate::download::apply(
                    &writer,
                    &inner.write_schema,
                    inner.clock.now(),
                    write,
                    &files,
                ))
            })
            .await,
        )
    }

    /// Stream an audience's plaintext snapshot frames from one committed reader
    /// transaction. The consumer can seal and upload each frame as it arrives;
    /// commits on the writer connection continue throughout this call.
    pub async fn write_snapshot<F, E>(
        &self,
        id: coven_format::store_log::SnapshotId,
        emit: F,
    ) -> Result<(), crate::SnapshotWriteError<E>>
    where
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.with_reader(|reader| {
                    reader.read_transaction(|| {
                        crate::snapshot_write::write(reader, &inner.write_schema, id, emit)
                    })
                })
            })
            .await,
        )
    }

    /// Load authenticated snapshots and gap writes supplied by sync. Snapshot
    /// frames and write parts are read in chunks of at most 64 KiB; only one
    /// decoded write is retained at a time. Inputs may arrive in any order.
    ///
    /// Replaces the selected audiences and replays uncovered parts and waiting
    /// uploads in one transaction. Every audience finishes at common positions;
    /// missing history, read errors and failed checks roll the whole load back.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealed bytes stay unchanged.
    pub async fn load_snapshots<R, W>(
        &self,
        snapshots: Vec<(coven_format::store_log::SnapshotId, R)>,
        writes: Vec<crate::DownloadedWriteStream<W>>,
    ) -> Result<(), DbError>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Read + Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                // Reserve a reader before locking the writer: snapshot consumers
                // may be waiting for a commit before releasing their readers.
                let reader = inner.readers.acquire_reader();
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
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            inner.write_schema.prepare(reader)?;
                            crate::snapshot_load::load(
                                &writer,
                                reader,
                                &inner.write_schema,
                                snapshots,
                                writes,
                                &files,
                                (inner.device, inner.clock.now()),
                            )
                        })
                    })?;
                    files.finish(Ok(()))
                }));
                drop(writer);
                match result {
                    Ok(result) => result,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            })
            .await,
        )
    }

    /// Read only the oldest waiting write, streaming plaintext parts or its
    /// already fixed sealed bytes. The callback runs in a committed reader
    /// transaction; its result is `None` when the queue is empty.
    pub async fn read_oldest_upload<F, R, E>(
        &self,
        consume: F,
    ) -> Result<Option<R>, crate::UploadReadError<E>>
    where
        F: FnOnce(crate::WaitingUpload<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.with_reader(|reader| {
                    reader.read_transaction(|| crate::upload::read(reader, consume))
                })
            })
            .await,
        )
    }

    /// Report a successful upload and remove its plaintext and sealed bytes
    /// together. A repeated report returns false; attempts must stay in order.
    pub async fn upload_succeeded(&self, write: coven_merge::WriteId) -> Result<bool, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::upload::succeeded(&writer, write)
            })
            .await,
        )
    }

    /// Record an applied breaking change's snapshot coverage. Returns false on repetition.
    /// Loading the snapshot's rows is a separate operation.
    pub async fn apply_breaking_change(
        &self,
        version: u32,
        included: coven_format::value::WritePositions,
    ) -> Result<bool, DbError> {
        self.apply_boundary(crate::write_boundary::WriteBoundary::SchemaChange {
            version,
            included,
        })
        .await
    }

    /// Record an applied reset's snapshot coverage. Returns false on repetition.
    /// Loading the snapshot's rows is a separate operation.
    pub async fn apply_reset(
        &self,
        entry: crate::EntryId,
        audience: coven_merge::Audience,
        included: coven_format::value::WritePositions,
    ) -> Result<bool, DbError> {
        self.apply_boundary(crate::write_boundary::WriteBoundary::Reset {
            entry,
            audience,
            included,
        })
        .await
    }

    async fn apply_boundary(
        &self,
        boundary: crate::write_boundary::WriteBoundary,
    ) -> Result<bool, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                boundary.record(&writer)
            })
            .await,
        )
    }

    /// Read applied entries and the fixed publication queue under the writer lock.
    pub async fn local_store_log(&self) -> Result<crate::LocalStoreLog, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                Ok(crate::LocalStoreLog {
                    store: inner.directory.id(),
                    device: inner.device,
                    log: crate::store_log_tables::read(&writer)?,
                    upload: crate::store_log_upload::read(&writer)?,
                })
            })
            .await,
        )
    }

    /// Fix the next entry's number, stamp, causal past, sealed bytes and sealed-key
    /// prerequisites together. No storage attempt may precede this commit.
    /// The callback owns sealing values, receives no database capability, and
    /// rejects a change or returns the bytes to retain. Failure reserves no number.
    pub async fn prepare_store_log<F, E>(
        &self,
        author: coven_crypto::MemberId,
        change: coven_format::store_log::StoreChange,
        seal: F,
    ) -> Result<crate::EntryId, E>
    where
        F: FnOnce(
                &crate::StoreLog,
                &coven_format::store_log::StoreLogEntry,
            ) -> Result<crate::SealedStoreLog, E>
            + Send
            + 'static,
        E: From<DbError> + Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::store_log_upload::prepare(
                    &writer,
                    inner.device,
                    inner.clock.now(),
                    author,
                    change,
                    move |log, entry| seal(log, entry).map(|sealed| (sealed, None)),
                )
            })
            .await,
        )
    }

    /// Fix a sealed key copy before its first storage attempt. An existing path
    /// returns its original bytes without calling `seal`, including after reopen.
    /// The callback receives no database capability; failure reserves nothing.
    /// Paths are opaque here: the sync owner chooses recipients from its replay.
    pub async fn prepare_key_upload<F, E>(&self, path: String, seal: F) -> Result<Vec<u8>, E>
    where
        F: FnOnce() -> Result<Vec<u8>, E> + Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::key_upload::prepare(&writer, &path, seal)
            })
            .await,
        )
    }

    /// Retire a fixed sealed copy only after storage accepted it or its path was
    /// already occupied. Retrying retirement is safe after an uncertain reply.
    pub async fn complete_key_upload(&self, path: String) -> Result<(), DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                crate::key_upload::complete(&writer, &path)
            })
            .await,
        )
    }

    /// Commit one entry, its immutable author-view check, every kept/dropped mark,
    /// and sync's whole replay result in one transaction (§9).
    ///
    /// The database stores the supplied result without replaying. Circle deletions
    /// and reversals recompute row visibility in the same transaction. A result for
    /// a stale or different applied-entry set is refused with
    /// [`DbError::StoreLogEntriesChanged`]; changing an applied entry's bytes or
    /// author-view check is refused with [`DbError::StoreLogEntryChanged`].
    pub async fn apply_store_log(
        &self,
        entry: crate::ReplayEntry,
        replay: crate::StoreLogReplay,
    ) -> Result<(), DbError> {
        self.apply_store_log_operations(entry, replay, Vec::new(), Vec::new())
            .await
    }

    /// Applied entries and their complete replay result from one committed snapshot.
    pub async fn store_log(&self) -> CovenResult<crate::StoreLog> {
        self.read(|sql| sql.store_log()).await
    }

    /// The current store key selected by the committed replay (§11), or `None`
    /// before creation. Reads only the selected id, without loading entry history.
    pub async fn current_store_key(
        &self,
    ) -> CovenResult<Option<coven_foundation::id_source::KeyId>> {
        self.read(|sql| sql.current_store_key()).await
    }

    /// Read the schema version, positions and fingerprints from one committed state.
    /// Supply fresh hashers derived from the store or circle keys. Audiences whose
    /// keys are unavailable can be omitted without interrupting sum maintenance.
    pub async fn sync_state(
        &self,
        keys: Vec<(coven_merge::Audience, coven_crypto::FingerprintHasher)>,
    ) -> Result<crate::SyncState, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let schema_version = writer.schema_version()?;
                let positions = crate::download::positions(&writer)?;
                let fingerprints = keys
                    .into_iter()
                    .map(|(audience, key)| {
                        crate::fingerprint::read(&writer, &audience, key)
                            .map(|value| (audience, value))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(crate::SyncState {
                    device: inner.device,
                    store_log: crate::store_log::positions(&writer)?,
                    uploads_pending: writer.query_row(
                        "SELECT EXISTS(SELECT 1 FROM coven_uploads)",
                        [],
                        |r| r.get(0),
                    )?,
                    breaking_version: writer.query_row(
                        "SELECT minimum FROM coven_snapshot_schema WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )?,
                    schema_version,
                    positions,
                    fingerprints,
                })
            })
            .await,
        )
    }
}

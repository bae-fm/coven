//! Database operations used by synchronization, on the retained connections.

use super::Database;
use crate::{CovenResult, DbError};

impl Database {
    /// Original plaintext of an authenticated received or confirmed published header.
    /// This establishes header facts, not the validity of a subsequently read body.
    pub async fn checked_write_header(
        &self,
        id: coven_merge::WriteId,
    ) -> Result<Option<coven_format::write_stream::WriteHeaderFrame>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|db| crate::write_headers::read(db, id))
        })
        .await
    }

    /// Audiences whose selected entry effects still require readable replacement
    /// data. A replay that rebuilds from retained inputs removes its requirement
    /// in the same transaction, so sync need not load that audience again.
    pub async fn reloading_audiences(
        &self,
    ) -> Result<std::collections::BTreeSet<coven_merge::Audience>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(crate::entry_effects::reloading_audiences)
        })
        .await
    }

    /// Complete retained entry or key-copy input, including input not yet applied.
    pub async fn log_inputs(&self, kind: crate::LogKind) -> Result<Vec<crate::LogInput>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|reader| crate::log_input::inputs(reader, kind))
        })
        .await
    }

    /// Contiguous received positions; these never substitute for applied positions.
    pub async fn log_downloads(
        &self,
        kind: crate::LogKind,
    ) -> Result<Vec<(coven_foundation::id_source::DeviceId, u64)>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|reader| crate::log_input::positions(reader, kind))
        })
        .await
    }

    /// Writers known without listing storage: this device, every device a
    /// received entry wrote from or registered (including removed and replaced
    /// ones), and every writer folder remembered from an earlier listing.
    pub async fn known_log_writers(
        &self,
    ) -> Result<std::collections::BTreeSet<coven_foundation::id_source::DeviceId>, DbError> {
        self.call(move |inner| {
            let mut writers = inner.with_store_log(|log| {
                Ok::<_, DbError>(
                    log.replay
                        .state
                        .devices
                        .keys()
                        .copied()
                        .chain(log.entries.iter().map(|entry| entry.entry.position.device))
                        .collect::<std::collections::BTreeSet<_>>(),
                )
            })?;
            writers.insert(inner.access.device);
            inner.access.readers.acquire_reader().with_reader(|reader| {
                for kind in [crate::LogKind::Entry, crate::LogKind::KeyCopy] {
                    writers.extend(
                        crate::log_input::positions(reader, kind)?
                            .into_iter()
                            .map(|(writer, _)| writer),
                    );
                }
                Ok(writers)
            })
        })
        .await
    }

    /// The storage time certified by the last completed catch-up (§15's `S`).
    pub async fn return_checkpoint(
        &self,
    ) -> Result<Option<coven_format::value::StorageTime>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(crate::log_input::checkpoint)
        })
        .await
    }

    /// Save `S` after a completed catch-up whose positions were confirmed
    /// posted. Only a newer time is written; returns whether it was.
    pub async fn save_return_checkpoint(
        &self,
        time: coven_format::value::StorageTime,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::log_input::save_checkpoint(writer, time))
        })
        .await
    }

    /// Retain discovered writers even when their first exact read is still absent.
    pub async fn remember_log_writers(
        &self,
        writers: Vec<coven_foundation::id_source::DeviceId>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::log_input::remember_writers(writer, &writers))
        })
        .await
    }

    /// Retain complete inputs and advance each writer's contiguous received
    /// position in one transaction. Out-of-order completions remain reusable
    /// after a crash; a failure retains none of them.
    pub async fn keep_log_inputs(
        &self,
        kind: crate::LogKind,
        inputs: Vec<crate::LogInput>,
    ) -> Result<std::collections::BTreeMap<coven_foundation::id_source::DeviceId, u64>, DbError>
    {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::log_input::keep(writer, kind, &inputs))
        })
        .await
    }

    /// Current pending records, decoded as the sync owner's typed values.
    pub async fn pending<P: crate::PendingValue>(&self) -> CovenResult<Vec<P>> {
        self.read(|sql| sql.pending()).await
    }

    /// The same pre-built pending query, observing committed changes.
    pub fn subscribe_pending<P: crate::PendingValue>(&self) -> crate::LiveQuery<Vec<P>> {
        self.subscribe(|sql| sql.pending())
    }

    /// Record one local first unmet condition, replacing its previous reason.
    pub async fn record_pending<P: crate::PendingValue>(&self, record: P) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                if record.reported_by() != inner.access.device {
                    return Err(DbError::DamagedDatabase);
                }
                if let Some(id) = record.operation() {
                    if crate::operation::find(writer, id)?.is_none() {
                        return Err(DbError::OperationChanged(id));
                    }
                }
                writer.pending_transaction(|db| crate::pending::save(db, &record))
            })
        })
        .await
    }

    /// Clear this device's observation when its subject advances.
    pub async fn clear_pending<S: serde::Serialize + Send + 'static>(
        &self,
        subject: S,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.pending_transaction(|db| {
                    crate::pending::clear(db, &subject, inner.access.device)
                })
            })
        })
        .await
    }

    /// Replace this device's observations of one completed scan atomically:
    /// its records matching `scanned` become exactly `records`, so a subject
    /// that now checks, or is no longer listed, loses its record in the same
    /// transaction. An unchanged set writes nothing.
    pub async fn replace_local_pending<P: crate::PendingValue>(
        &self,
        scanned: impl Fn(&P) -> bool + Send + 'static,
        records: Vec<P>,
    ) -> Result<(), DbError> {
        use std::collections::BTreeSet;
        self.call(move |inner| {
            let mut identities = BTreeSet::new();
            for record in &records {
                if record.reported_by() != inner.access.device
                    || record.operation().is_some()
                    || !scanned(record)
                    || !identities.insert(record.subject())
                {
                    return Err(DbError::DamagedDatabase);
                }
            }
            let encode = |r: &P| serde_json::to_vec(r).map_err(DbError::PendingEncoding);
            let next = records
                .iter()
                .map(encode)
                .collect::<Result<BTreeSet<_>, _>>()?;
            // The usual unchanged scan reads its class without taking the writer.
            let unchanged = inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|reader| {
                    let current = crate::pending::retained::<P>(reader)?
                        .into_iter()
                        .filter(|r| r.reported_by() == inner.access.device && scanned(r))
                        .map(|r| encode(&r))
                        .collect::<Result<BTreeSet<_>, _>>()?;
                    Ok::<_, DbError>(current == next)
                })?;
            if unchanged {
                return Ok(());
            }
            inner.access.with_writer(|writer| {
                let previous: Vec<_> = crate::pending::retained::<P>(writer)?
                    .into_iter()
                    .filter(|r| r.reported_by() == inner.access.device && scanned(r))
                    .collect();
                if previous
                    .iter()
                    .map(encode)
                    .collect::<Result<BTreeSet<_>, _>>()?
                    == next
                {
                    return Ok(());
                }
                writer.pending_transaction(|db| {
                    for record in &previous {
                        if !identities.contains(record.subject()) {
                            crate::pending::clear(db, record.subject(), inner.access.device)?;
                        }
                    }
                    for record in &records {
                        crate::pending::save(db, record)?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Replace a completed authenticated peer scan atomically, preserving local records.
    /// The sync owner selects reports relevant to this installation before calling.
    pub async fn replace_pending_reports<P: crate::PendingValue>(
        &self,
        reports: Vec<P>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                let changes =
                    crate::pending::Replacement::reports(writer, inner.access.device, reports)?;
                if changes.is_empty() {
                    return Ok(());
                }
                writer.pending_transaction(|db| changes.apply(db))
            })
        })
        .await
    }

    /// Apply the persisted condition's retry policy to this installed version.
    pub async fn pending_can_retry<P: crate::PendingValue>(
        &self,
        subject: P::Subject,
    ) -> Result<bool, DbError>
    where
        P::Subject: Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                crate::pending::retry::<P>(writer, &subject, inner.access.device)
            })
        })
        .await
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
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            inner.with_files(Vec::new(), |writer, files| {
                reader.with_reader(|reader| {
                    reader.read_transaction(|| {
                        inner.access.write_schema.prepare(reader)?;
                        // Pin the before view before the streamed transaction changes merge state.
                        reader.schema_version()?;
                        write.apply(
                            writer,
                            reader,
                            &inner.access.write_schema,
                            files,
                            &store_log,
                            authenticate,
                        )
                    })
                })
            })
        })
        .await
    }
    /// The provider's opaque upload recording, retained with the attempted write.
    pub async fn write_upload_session(
        &self,
        write: crate::WriteId,
    ) -> Result<Option<Vec<u8>>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::upload::session(writer, write))
        })
        .await
    }

    /// Record a session before sending and after provider-confirmed progress.
    pub async fn keep_write_upload_session(
        &self,
        write: crate::WriteId,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::upload::keep_session(writer, write, bytes))
        })
        .await
    }
    /// Fix the oldest write's sealing keys and mark its first attempt atomically.
    /// Selection runs against the committed store log and app version. A retry
    /// keeps the original keys without calling `select`; errors reserve nothing.
    pub async fn prepare_write_upload<F>(
        &self,
        select: F,
    ) -> Result<Option<crate::WriteId>, DbError>
    where
        F: FnOnce(
                &crate::StoreLog,
                u32,
                &coven_format::write_stream::WriteHeaderFrame,
            ) -> Result<coven_format::sealed_write::WriteObjectPrefix, DbError>
            + Send
            + 'static,
    {
        self.call(move |inner| {
            inner.with_store_log(|log| {
                inner
                    .access
                    .with_writer(|writer| crate::upload::prepare(writer, log, select))
            })
        })
        .await
    }
    /// Apply one authenticated download, or report the prerequisite it awaits.
    /// Row changes, merge state, fingerprints and positions commit together.
    pub async fn apply_downloaded(
        &self,
        write: crate::DownloadedWrite,
    ) -> Result<crate::ApplyOutcome, DbError> {
        self.call(move |inner| {
            inner.with_files(Vec::new(), |writer, files| {
                crate::download::apply(writer, &inner.access.write_schema, write, files)
            })
        })
        .await
    }

    /// Read the oldest waiting plaintext and its recorded sealing keys.
    /// The callback runs in a committed reader transaction; its result is `None`
    /// when the queue is empty.
    pub async fn read_oldest_upload<F, R, E>(
        &self,
        consume: F,
    ) -> Result<Option<R>, crate::UploadReadError<E>>
    where
        F: FnOnce(crate::WaitingUpload<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| crate::upload::read(reader, consume))
            })
        })
        .await
    }

    /// Report a successful upload and remove its queue row and session together.
    /// A repeated report returns false; attempts must stay in order.
    pub async fn upload_succeeded(&self, write: coven_merge::WriteId) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::upload::succeeded(writer, write))
        })
        .await
    }

    /// Read the retained decoded log and its queue from a committed reader snapshot.
    /// All clones share the value installed by the replay transaction.
    pub async fn local_store_log(&self) -> Result<crate::LocalStoreLog, DbError> {
        self.call(move |inner| {
            inner.with_store_log(|log| {
                inner.access.readers.acquire_reader().with_reader(|reader| {
                    reader.read_transaction(|| {
                        Ok(crate::LocalStoreLog {
                            store: inner.access.directory.id(),
                            device: inner.access.device,
                            log: log.clone(),
                            upload: crate::store_log_upload::read(reader)?,
                        })
                    })
                })
            })
        })
        .await
    }

    /// Fix the next entry's number, stamp, causal past, sealing key id and sealed-key
    /// prerequisites together. No storage attempt may precede this commit.
    /// The callback owns sealing values, receives no database capability, and
    /// rejects a change or returns the keys to retain. Failure reserves no number.
    /// A store creation must name the directory settings' id and name (§9).
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
                crate::KeyUploadBatch,
            ) -> Result<crate::StoreLogSealing, E>
            + Send
            + 'static,
        E: From<DbError> + Send + 'static,
    {
        self.call(move |inner| {
            inner.with_store_log(|log| {
                inner.access.with_writer(|writer| {
                    crate::store_log_upload::prepare(
                        writer,
                        log,
                        inner.access.directory.id(),
                        &inner.name,
                        inner.access.device,
                        inner.clock.now(),
                        author,
                        change,
                        move |log, entry, copies| {
                            seal(log, entry, copies).map(|sealed| (sealed, None))
                        },
                    )
                })
            })
        })
        .await
    }

    /// Reserve one historical copy with its next writer number and fixed bytes.
    /// The caller chooses a still-unmet need from its latest replay and copy observation.
    /// Once committed this copy must settle even if that need disappears.
    pub async fn prepare_key_upload<F, E>(&self, seal: F) -> Result<crate::KeyUpload, E>
    where
        F: FnOnce(&coven_format::path::ObjectPath) -> Result<Vec<u8>, E> + Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                crate::key_upload::prepare(
                    writer,
                    inner.access.directory.id(),
                    inner.access.device,
                    seal,
                )
            })
        })
        .await
    }

    /// Every unconfirmed fixed copy in writer/number order, including abandoned operations.
    pub async fn key_uploads(&self) -> Result<Vec<crate::KeyUpload>, DbError> {
        self.call(move |inner| inner.access.with_writer(crate::key_upload::read))
            .await
    }

    /// This installation's last confirmed key-copy number, or zero before its
    /// first publication. Reserved but unconfirmed copies do not advance it.
    pub async fn published_key_copy_number(&self) -> Result<u64, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::key_upload::published(writer, inner.access.device))
        })
        .await
    }

    /// Confirm the writer's next copy after storage accepted its exact bytes.
    /// Confirmation and queue retirement commit together; repetitions are idempotent.
    pub async fn complete_key_upload(&self, copy: crate::KeyUpload) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| crate::key_upload::complete(writer, &copy))
        })
        .await
    }

    /// Commit one entry, its first publication time and immutable checks, every
    /// kept/dropped mark, local drop reports and the whole replay result together (§9).
    ///
    /// The database stores the supplied result without replaying. Circle deletions
    /// and reversals recompute row visibility in the same transaction. A result for
    /// a stale or different applied-entry set is refused with
    /// [`DbError::StoreLogEntriesChanged`]; changing an applied entry's bytes or
    /// publication time or check is refused with [`DbError::StoreLogEntryChanged`].
    /// S3 deletion obligations the entry creates commit with it.
    pub async fn apply_store_log<P: crate::PendingValue>(
        &self,
        entry: crate::ReplayEntry,
        replay: crate::StoreLogReplay,
        access_keys: Vec<crate::AccessKeyDeletion>,
    ) -> Result<(), DbError> {
        let checked = self.local_store_log().await?.log;
        self.apply_store_log_batch::<P>(
            checked,
            vec![entry],
            replay,
            Vec::new(),
            Vec::new(),
            access_keys,
            Vec::new(),
        )
        .await
    }

    /// Applied entries and their complete replay result from one committed snapshot.
    pub async fn store_log(&self) -> CovenResult<crate::StoreLog> {
        self.read(|sql| sql.store_log()).await
    }

    /// Save a completed scan's horizon against exactly the replay it checked.
    /// The writer transaction rejects stale inputs and preserves monotonicity.
    /// An unchanged cutoff performs no write and emits no query notification.
    pub async fn advance_store_log_horizon(
        &self,
        checked: crate::StoreLog,
        horizon: std::time::Duration,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.with_store_log(|current| {
                inner.access.with_writer(|writer| {
                    if current.entries != checked.entries || current.replay != checked.replay {
                        return Err(DbError::StoreLogEntriesChanged);
                    }
                    if current.horizon.is_some_and(|previous| horizon <= previous) {
                        return Ok(());
                    }
                    writer.transaction(|database| {
                        database.internal_execute(
                            "UPDATE _coven_finality SET horizon=?1 WHERE singleton=1",
                            [crate::store_log_tables::encode_horizon(horizon)],
                        )?;
                        // Inputs whose boundary entries all became final are released.
                        crate::entry_inputs::release(database)
                    })?;
                    current.horizon = Some(horizon);
                    Ok(())
                })
            })
        })
        .await
    }

    /// Read the schema version, positions and fingerprints from one committed state.
    /// Supply fresh hashers derived from the store or circle keys. Audiences whose
    /// keys are unavailable can be omitted without interrupting sum maintenance.
    pub async fn sync_state(
        &self,
        keys: Vec<(coven_merge::Audience, coven_crypto::FingerprintHasher)>,
    ) -> Result<crate::SyncState, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                let schema_version = writer.schema_version()?;
                let positions = crate::download::positions(writer)?;
                let fingerprints = keys
                    .into_iter()
                    .map(|(audience, key)| {
                        crate::fingerprint::read(writer, &audience, key)
                            .map(|value| (audience, value))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(crate::SyncState {
                    device: inner.access.device,
                    store_log: crate::store_log::positions(writer)?,
                    reloading: writer.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_data_reloads)",
                        [],
                        |r| r.get(0),
                    )?,
                    uploads_pending: writer.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_uploads)",
                        [],
                        |r| r.get(0),
                    )?,
                    breaking_version: writer.query_row(
                        "SELECT publication FROM _coven_snapshot_schema WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )?,
                    schema_version,
                    positions,
                    fingerprints,
                })
            })
        })
        .await
    }
}

#[path = "database_positions.rs"]
mod positions;

#[path = "database_device.rs"]
mod device;
pub use device::IdentityReservations;

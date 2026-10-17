//! Snapshot streams and atomic reload through the database owner.

use super::Database;
use crate::DbError;

impl Database {
    /// Encoded audience parts in counted writes after a snapshot's coverage.
    /// Local originals without a slot do not request a snapshot of unpublished data.
    pub async fn snapshot_part_bytes(
        &self,
        audience: coven_merge::Audience,
        after: u64,
    ) -> Result<u64, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| db.read_transaction(|| {
                let mut bytes = 0u64;
                db.for_each("SELECT h.header FROM _coven_writes w JOIN _coven_write_headers h ON h.device=substr(w.timestamp,9,8) AND h.number=w.number WHERE w.slot>?1",
                    [after.to_be_bytes()], |row| {
                        let header = coven_format::write_stream::WriteHeaderFrame::decode(&row.get::<_, Vec<u8>>(0)?)?;
                        for part in header.parts.iter().filter(|part| part.audience == audience) {
                            bytes = bytes.checked_add(part.plaintext_length).ok_or(DbError::TooLarge {
                                field: "snapshot part bytes", actual: u64::MAX, maximum: u64::MAX - 1,
                            })?;
                        }
                        Ok::<_, DbError>(())
                    })?;
                Ok(bytes)
            }))
        }).await
    }

    /// Headers of waiting plaintext writes, including fixed upload attempts.
    /// Snapshot growth counts these parts before their eventual publication.
    pub async fn waiting_snapshot_headers(
        &self,
    ) -> Result<Vec<coven_format::write_stream::WriteHeaderFrame>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                db.read_transaction(|| {
                    let mut headers = Vec::new();
                    crate::upload::each_plaintext(db, |header, _| {
                        headers.push(header);
                        Ok(())
                    })?;
                    Ok(headers)
                })
            })
        })
        .await
    }

    /// Run the snapshot loader's format, schema and merge checks in a transaction
    /// that always rolls back, returning the uploaded files the snapshot names.
    /// File retention uses this for a snapshot no reload has read; a damaged
    /// snapshot cannot leave rows, metadata or observations behind. Supply its
    /// authenticated sealed prefix alongside the opened plaintext.
    pub async fn validate_snapshot<R: std::io::Read + Send + 'static>(
        &self,
        id: coven_format::admin::SnapshotId,
        prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
        input: R,
        author: coven_foundation::id_source::MemberId,
        authorship: std::sync::Arc<dyn crate::Authorship>,
    ) -> Result<
        std::collections::BTreeSet<(
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::FileId,
        )>,
        DbError,
    > {
        self.call(move |inner| {
            inner.inputs.prepare(|inputs| {
                let prepared = crate::snapshot_preparation::prepare(
                    inputs,
                    &inner.access.write_schema,
                    &id,
                    prefix,
                    input,
                    authorship.as_ref(),
                )?;
                inner.access.with_writer(|writer| {
                    writer.discarded_transaction(|writer| {
                        crate::snapshot_state::create_tables(writer)?;
                        crate::snapshot_load::read(
                            writer,
                            &inner.access.write_schema,
                            prepared,
                            Some(author),
                        )?;
                        super::file_retention::snapshot_references(
                            writer,
                            &id.audience,
                            &inner.access.write_schema,
                        )
                    })
                })
            })
        })
        .await
    }

    /// Stream an audience's plaintext snapshot frames from one committed reader
    /// transaction. `begin` receives metadata before any frame, so the consumer
    /// can write the sealed prefix and seal each subsequent frame as it arrives;
    /// commits on the writer connection continue throughout this call.
    pub async fn write_snapshot<B, F, E>(
        &self,
        id: coven_format::admin::SnapshotId,
        begin: B,
        emit: F,
    ) -> Result<(), crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: Send + 'static,
    {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
                    inner.access.write_schema.prepare(reader)?;
                    crate::snapshot_write::write(
                        reader,
                        &inner.access.write_schema,
                        id,
                        begin,
                        emit,
                    )
                })
            })
        })
        .await
    }

    /// The raise snapshots a breaking migration captured through its marker,
    /// one per audience this device read then (§17.1, §18.1).
    pub async fn raise_captures(&self) -> Result<Vec<crate::RaiseCapture>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| crate::raise_capture::captures(reader, inner.access.device))
        })
        .await
    }

    /// The capture to seal for an audience's raise, right after a completed
    /// catch-up: kept if that catch-up applied nothing past it, otherwise
    /// captured again from the current state through this device's newest
    /// write, which then joins the prefix to confirm (§15, §17.1). None when
    /// a later migration replaced the capture.
    pub async fn prepare_raise_capture(
        &self,
        audience: coven_merge::Audience,
        version: u32,
    ) -> Result<Option<crate::RaiseCapture>, DbError> {
        self.call(move |inner| {
            let device = inner.access.device;
            inner.access.with_writer(|writer| {
                writer.transaction(|writer| {
                    crate::raise_capture::prepare(
                        writer,
                        &inner.access.write_schema,
                        device,
                        &audience,
                        version,
                    )
                })
            })
        })
        .await
    }

    /// The first own write of a snapshot's captured prefix, through its
    /// own position, not yet confirmed stored. Any snapshot, ordinary, raise
    /// or reset, is sent only once this is `None` (§15).
    pub async fn unconfirmed_own_prefix(
        &self,
        through: coven_merge::WriteId,
    ) -> Result<Option<coven_merge::WriteId>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| crate::upload::unconfirmed(reader, through))
        })
        .await
    }

    /// Stream an audience's captured raise snapshot under its published
    /// identity, like `write_snapshot` but from the migration's capture.
    pub async fn write_raise_snapshot<B, F, E>(
        &self,
        id: coven_format::admin::SnapshotId,
        version: u32,
        begin: B,
        emit: F,
    ) -> Result<(), crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: Send + 'static,
    {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
                    inner.access.write_schema.prepare(reader)?;
                    crate::raise_capture::write(
                        reader,
                        &inner.access.write_schema,
                        id,
                        version,
                        begin,
                        emit,
                    )
                })
            })
        })
        .await
    }

    /// Forget a capture whose raise is published or superseded.
    pub async fn discard_raise_capture(
        &self,
        audience: coven_merge::Audience,
        version: u32,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer
                    .transaction(|writer| crate::raise_capture::discard(writer, &audience, version))
            })
        })
        .await
    }

    /// Load authenticated snapshots and gap writes supplied by sync. Each stored
    /// snapshot supplies its identity, authenticated prefix and plaintext. Snapshot
    /// frames and write parts are prepared in bounded records on the input
    /// connection before taking the application writer. Inputs name a fixed
    /// target and consecutive retained slots.
    ///
    /// Replaces the selected audiences and replays uncovered parts and waiting
    /// uploads in one transaction, after parsing each snapshot body once. Every
    /// audience finishes at the common target slot; missing history, read errors and
    /// failed checks roll the whole load back, a body's failure as
    /// `SnapshotError::Body` naming that snapshot. Returns the uploaded files
    /// each stored snapshot names.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealing key ids stay unchanged.
    pub async fn load_snapshots<P: crate::PendingValue, R, W>(
        &self,
        reload: crate::SnapshotReload<R, crate::DownloadedWriteStream<W>>,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Read + Send + 'static,
    {
        self.call(move |inner| {
            inner.inputs.prepare(|inputs| {
                let snapshots =
                    reload
                        .snapshots
                        .into_iter()
                        .map(|source| {
                            Ok(match source {
                                crate::SnapshotSource::Stored {
                                    id,
                                    prefix,
                                    input,
                                    author,
                                } => {
                                    let input = crate::snapshot_preparation::prepare(
                                        inputs,
                                        &inner.access.write_schema,
                                        &id,
                                        prefix.clone(),
                                        input,
                                        reload.authorship.as_ref(),
                                    )
                                    .map_err(|error| crate::SnapshotError::Body {
                                        snapshot: id.clone(),
                                        error: Box::new(error),
                                    })?;
                                    crate::SnapshotSource::Stored {
                                        id,
                                        prefix,
                                        input,
                                        author,
                                    }
                                }
                                crate::SnapshotSource::Empty(audience) => {
                                    crate::SnapshotSource::Empty(audience)
                                }
                                crate::SnapshotSource::Current(audience) => {
                                    crate::SnapshotSource::Current(audience)
                                }
                            })
                        })
                        .collect::<Result<_, DbError>>()?;
                let reload = crate::SnapshotReload {
                    slots: reload
                        .slots
                        .into_iter()
                        .map(|slot| slot.prepare(inputs))
                        .collect::<Result<_, _>>()?,
                    snapshots,
                    target: reload.target,
                    state: reload.state,
                    provider: reload.provider,
                    cutoffs: reload.cutoffs,
                    boundaries: reload.boundaries,
                    operation: reload.operation,
                    authorship: reload.authorship,
                };
                // Reserve a reader before locking the writer: snapshot consumers
                // may be waiting for a commit before releasing their readers.
                let reader = inner.access.readers.acquire_reader();
                inner.with_files(Vec::new(), |writer, files| {
                    reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            inner.access.write_schema.prepare(reader)?;
                            crate::snapshot_load::load::<P>(
                                writer,
                                reader,
                                &inner.access.write_schema,
                                reload,
                                files,
                            )
                        })
                    })
                })
            })
        })
        .await
    }
}

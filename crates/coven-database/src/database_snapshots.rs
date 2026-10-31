//! Snapshot streams and atomic reload through the database owner.

use super::Database;
use crate::DbError;

impl Database {
    /// The sealed bytes a new device replays for `audience` after a
    /// snapshot's coverage (§16.2): each applied writes slot carrying one of
    /// its parts, and for the store every admin, key-copy and announcement
    /// slot too. Local originals without a slot do not request a snapshot of
    /// unpublished data, and slots this installation loaded inside a
    /// snapshot instead of reading count nothing.
    pub async fn snapshot_slot_bytes(
        &self,
        audience: coven_merge::Audience,
        after: u64,
    ) -> Result<u64, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| db.read_transaction(|| {
                let too_large = || DbError::TooLarge {
                    field: "snapshot slot bytes", actual: u64::MAX, maximum: u64::MAX - 1,
                };
                // A slot holding several of the audience's writes counts once.
                let mut slots = std::collections::BTreeMap::new();
                db.for_each("SELECT w.slot,s.size,h.header FROM _coven_writes w JOIN _coven_write_slots s ON s.slot=w.slot JOIN _coven_write_headers h ON h.device=substr(w.timestamp,9,8) AND h.number=w.number WHERE w.slot>?1",
                    [after.to_be_bytes()], |row| {
                        let header = coven_format::write_stream::WriteHeaderFrame::decode(&row.get::<_, Vec<u8>>(2)?)?;
                        if header.parts.iter().any(|part| part.audience == audience) {
                            let size = u64::try_from(row.get::<_, i64>(1)?).map_err(|_negative| DbError::DamagedDatabase)?;
                            slots.insert(row.get::<_, Vec<u8>>(0)?, size);
                        }
                        Ok::<_, DbError>(())
                    })?;
                let mut bytes = slots.into_values().try_fold(0u64, |sum, size| sum.checked_add(size)).ok_or_else(too_large)?;
                if audience == coven_merge::Audience::Store {
                    let records: i64 = db.query_row("SELECT coalesce(sum(length(record)),0) FROM _coven_slot_records WHERE slot>?1",
                        [after.to_be_bytes()], |row| row.get(0))?;
                    bytes = u64::try_from(records).ok().and_then(|records| bytes.checked_add(records)).ok_or_else(too_large)?;
                }
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

    /// Run the snapshot loader's schema and merge checks over a staged
    /// snapshot in a transaction that always rolls back, returning the uploaded
    /// files it names. File retention uses this for a snapshot no reload has
    /// read; a damaged snapshot cannot leave rows, metadata or observations
    /// behind. Supply the records of a body that authenticated; reading them
    /// removes them (§16.2).
    pub async fn validate_snapshot(
        &self,
        id: coven_format::admin::SnapshotId,
        input: crate::StagedRecords,
        author: coven_foundation::id_source::MemberId,
        authorship: std::sync::Arc<dyn crate::Authorship>,
    ) -> Result<
        std::collections::BTreeSet<(
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::FileId,
        )>,
        DbError,
    > {
        let staged = [input.staging(self)?.clone()];
        self.call(move |inner| {
            inner.inputs.prepare(&staged, |inputs| {
                let prepared =
                    crate::snapshot_preparation::prepare(inputs, &input, authorship.as_ref())?;
                inner.access.with_writer(|writer| {
                    writer.discarded_transaction(|writer| {
                        let schema = &inner.access.write_schema;
                        crate::snapshot_state::create_tables(writer)?;
                        crate::snapshot_install::create_tables(writer, schema)?;
                        crate::snapshot_load::read(writer, schema, prepared, Some(author))?;
                        super::file_retention::snapshot_references(writer, schema, &id.audience)
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
    /// one per audience this device read then (§18.3, §11.5).
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
    /// write, which then joins the prefix to confirm (§16.3, §18.5). None when
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
    /// or reset, is sent only once this is `None` (§16.1); a raise starts
    /// only then (§18.5).
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
    /// snapshot supplies its identity and the records staged from its body;
    /// their write evidence is authenticated, and write parts are staged on the
    /// input connection, before taking the application writer. Inputs name a
    /// fixed target and consecutive retained slots.
    ///
    /// Replaces the selected audiences and replays uncovered parts and waiting
    /// uploads in one transaction, reading each staged snapshot once and then
    /// removing its records. Every audience finishes at the common target slot;
    /// missing history, read errors and failed checks roll the whole load back,
    /// a body's failure as `SnapshotError::Body` naming that snapshot, and leave
    /// the staged records for their owner to discard. Returns the uploaded files
    /// each stored snapshot names.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealing key ids stay unchanged.
    pub async fn load_snapshots<P: crate::PendingValue, W>(
        &self,
        reload: crate::SnapshotReload<crate::StagedRecords, crate::DownloadedWriteStream<W>>,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        W: std::io::Read + Send + 'static,
    {
        let staged = reload
            .snapshots
            .iter()
            .filter_map(|source| match source {
                crate::SnapshotSource::Stored { input, .. } => Some(input.staging(self).cloned()),
                _ => None,
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.call(move |inner| {
            let started = inner.clock.elapsed();
            inner.inputs.prepare(&staged, |inputs| {
                let snapshots =
                    reload
                        .snapshots
                        .into_iter()
                        .map(|source| {
                            Ok(match source {
                                crate::SnapshotSource::Stored { id, input, author } => {
                                    let input = crate::snapshot_preparation::prepare(
                                        inputs,
                                        &input,
                                        reload.authorship.as_ref(),
                                    )
                                    .map_err(|error| crate::SnapshotError::Body {
                                        snapshot: id.clone(),
                                        error: Box::new(error),
                                    })?;
                                    crate::SnapshotSource::Stored { id, input, author }
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
                let prepared = inner.clock.elapsed();
                // Reserve a reader before locking the writer: snapshot consumers
                // may be waiting for a commit before releasing their readers.
                let reader = inner.access.readers.acquire_reader();
                let loaded = inner.with_files(Vec::new(), |writer, files| {
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
                });
                tracing::info!(
                    preparing = ?(prepared - started),
                    installing = ?(inner.clock.elapsed() - prepared),
                    "installed snapshots"
                );
                loaded
            })
        })
        .await
    }
}

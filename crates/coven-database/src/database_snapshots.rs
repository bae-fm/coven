//! Snapshot streams and atomic reload through the database owner.

use super::Database;
use crate::DbError;

impl Database {
    /// Headers of waiting plaintext writes, including fixed upload attempts.
    /// Snapshot growth counts these parts before their eventual publication.
    pub async fn waiting_snapshot_headers(
        &self,
    ) -> Result<Vec<coven_format::write_stream::WriteHeaderFrame>, DbError> {
        self.call(move |inner| {
            let reader = inner.readers.acquire_reader();
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

    /// Positions a reload must preserve for the local author's future writes,
    /// and the queue identities whose plaintext the transaction itself replays.
    pub async fn reload_positions(
        &self,
    ) -> Result<
        (
            coven_format::value::WritePositions,
            Vec<coven_merge::WriteId>,
        ),
        DbError,
    > {
        self.call(move |inner| {
            let reader = inner.readers.acquire_reader();
            reader.with_reader(|reader| reader.read_transaction(|| {
                let waiting = reader.query("SELECT device,number FROM _coven_uploads ORDER BY device,number", [], |r| Ok(coven_merge::WriteId {
                    device: coven_foundation::id_source::DeviceId(crate::write_encoding::counter(r.get(0)?)),
                    number: crate::write_encoding::counter(r.get(1)?),
                }))?;
                let mut required = std::collections::BTreeMap::<_, u64>::new();
                reader.for_each("SELECT number,had_read FROM _coven_writes WHERE substr(timestamp,9,8)=?1 ORDER BY number DESC LIMIT 1", [inner.device.0.to_be_bytes().as_slice()], |r| {
                    let number = crate::write_encoding::counter(r.get(0)?);
                    required.insert(inner.device, number);
                    let past = crate::write_encoding::decoded(coven_format::merge_fields::decode_write_positions(&r.get::<_, Vec<u8>>(1)?))?;
                    for id in past.0 { required.entry(id.device).and_modify(|n| *n = (*n).max(id.number)).or_insert(id.number); }
                    Ok::<_, DbError>(())
                })?;
                Ok((coven_format::value::WritePositions(required.into_iter().map(|(device,number)| coven_merge::WriteId { device, number }).collect()), waiting))
            }))
        }).await
    }

    /// Run the snapshot loader's format, schema and merge checks in a transaction
    /// that always rolls back. Sync uses this for loading and file-reference checks;
    /// a damaged snapshot cannot leave rows, metadata or observations behind.
    /// Supply its authenticated sealed prefix alongside the opened plaintext.
    pub async fn validate_snapshot<R: std::io::Read + Send + 'static>(
        &self,
        id: coven_format::store_log::SnapshotId,
        prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
        input: R,
    ) -> Result<
        std::collections::BTreeSet<(
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::FileId,
        )>,
        DbError,
    > {
        self.call(move |inner| {
            inner.with_writer(|writer| {
                writer.read_transaction(|| {
                    crate::snapshot_state::create_tables(writer)?;
                    crate::snapshot_load::read(writer, &inner.write_schema, &id, prefix, input)?;
                    let files =
                        super::file_retention::snapshot_references(writer, &inner.write_schema)
                            .map_err(|error| match error {
                                DbError::DamagedDatabase => crate::snapshot_error::invalid(
                                    "snapshot file location is invalid",
                                ),
                                error => error,
                            })?;
                    Ok(files)
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
        id: coven_format::store_log::SnapshotId,
        begin: B,
        emit: F,
    ) -> Result<(), crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: Send + 'static,
    {
        self.call(move |inner| {
            let reader = inner.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
                    crate::snapshot_write::write(reader, &inner.write_schema, id, begin, emit)
                })
            })
        })
        .await
    }

    /// Load authenticated snapshots and gap writes supplied by sync. Each stored
    /// snapshot supplies its identity, authenticated prefix and plaintext. Snapshot
    /// frames and write parts are read in chunks of at most 64 KiB; only one
    /// decoded write is retained at a time. Inputs may arrive in any order.
    ///
    /// Replaces the selected audiences and replays uncovered parts and waiting
    /// uploads in one transaction. Every audience finishes at common positions;
    /// missing history, read errors and failed checks roll the whole load back.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealing key ids stay unchanged.
    pub async fn load_snapshots<R, W>(
        &self,
        reload: crate::SnapshotReload<R, W>,
    ) -> Result<(), DbError>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Read + Send + 'static,
    {
        self.call(move |inner| {
            // Reserve a reader before locking the writer: snapshot consumers
            // may be waiting for a commit before releasing their readers.
            let reader = inner.readers.acquire_reader();
            inner.with_files(Vec::new(), |writer, files| {
                reader.with_reader(|reader| {
                    reader.read_transaction(|| {
                        inner.write_schema.prepare(reader)?;
                        crate::snapshot_load::load(
                            writer,
                            reader,
                            &inner.write_schema,
                            reload,
                            files,
                            (inner.device, inner.clock.now()),
                        )
                    })
                })?;
                Ok(())
            })
        })
        .await
    }
}

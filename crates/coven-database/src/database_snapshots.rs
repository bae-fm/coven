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

    /// Positions a reload must preserve, and the queue identities whose
    /// plaintext the transaction itself replays. They are this device's applied
    /// positions, which hold its own writes and every past they read; a write
    /// made during staging reads no more (§15, io `staging_past_bounded`).
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
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
                    let waiting = reader.query(
                        "SELECT device,number FROM _coven_uploads ORDER BY device,number",
                        [],
                        |r| {
                            Ok(coven_merge::WriteId {
                                device: coven_foundation::id_source::DeviceId(
                                    crate::write_encoding::counter(r.get(0)?),
                                ),
                                number: crate::write_encoding::counter(r.get(1)?),
                            })
                        },
                    )?;
                    Ok((crate::download::positions(reader)?, waiting))
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
        id: coven_format::store_log::SnapshotId,
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
            inner.access.with_writer(|writer| {
                writer.discarded_transaction(|writer| {
                    crate::snapshot_state::create_tables(writer)?;
                    crate::snapshot_load::read(
                        writer,
                        &inner.access.write_schema,
                        &id,
                        prefix,
                        input,
                        &crate::snapshot_load::SnapshotSigner {
                            authorship: authorship.as_ref(),
                            author: Some(author),
                        },
                    )?;
                    super::file_retention::snapshot_references(
                        writer,
                        &id.audience,
                        &inner.access.write_schema,
                    )
                })
            })
        })
        .await
    }

    /// What preparing a snapshot of `audience` waits for before every covered
    /// write's retirement-window verdict is final (§15). `signer` is this
    /// device's member signing key; `latest` is the end of the last complete
    /// store-log catch-up's receipt plus `WRITE_WINDOW`.
    pub async fn snapshot_verdict_wait(
        &self,
        audience: coven_merge::Audience,
        signer: coven_crypto::SigningKey,
        latest: Option<coven_format::value::StorageTime>,
    ) -> Result<Option<crate::VerdictWait>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
                    crate::write_landing::unsettled(
                        reader,
                        &audience,
                        inner.access.device,
                        &signer,
                        latest,
                    )
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
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| {
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
        id: coven_format::store_log::SnapshotId,
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
                    crate::raise_capture::write(reader, id, version, begin, emit)
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

    /// Every audience's retained historical coverage evidence (§15).
    pub async fn snapshot_witnesses(&self) -> Result<crate::SnapshotWitnesses, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(crate::snapshot_witnesses::read)
        })
        .await
    }

    /// Keep the landing of each checked counting snapshot just listed for
    /// `audience` as historical coverage evidence, which outlives the
    /// snapshot, its body and reopening (§15). `now`, the newest storage time
    /// known, drops evidence a mature witness makes redundant. Unchanged
    /// evidence takes no writer.
    pub async fn record_snapshot_witnesses(
        &self,
        audience: coven_merge::Audience,
        observed: Vec<(
            coven_format::value::WritePositions,
            coven_format::value::StorageTime,
        )>,
        now: Option<coven_format::value::StorageTime>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            let known = inner
                .access
                .readers
                .acquire_reader()
                .with_reader(crate::snapshot_witnesses::read)?;
            if crate::snapshot_witnesses::update(known, &audience, &observed, now).is_none() {
                return Ok(());
            }
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let known = crate::snapshot_witnesses::read(db)?;
                    match crate::snapshot_witnesses::update(known, &audience, &observed, now) {
                        Some(changed) => crate::snapshot_witnesses::write(db, &changed, &audience),
                        None => Ok(()),
                    }
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
    /// uploads in one transaction, parsing each snapshot body once. Every
    /// audience finishes at common positions; missing history, read errors and
    /// failed checks roll the whole load back, a body's failure as
    /// `SnapshotError::Body` naming that snapshot. Returns the uploaded files
    /// each stored snapshot names.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealing key ids stay unchanged.
    pub async fn load_snapshots<P: crate::PendingValue, R, W>(
        &self,
        reload: crate::SnapshotReload<R, W>,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Read + Send + 'static,
    {
        self.call(move |inner| {
            // Reserve a reader before locking the writer: snapshot consumers
            // may be waiting for a commit before releasing their readers.
            let reader = inner.access.readers.acquire_reader();
            inner.with_files(Vec::new(), |writer, files| {
                reader.with_reader(|reader| {
                    reader.read_transaction(|| {
                        inner.access.write_schema.prepare(reader)?;
                        crate::snapshot_load::load::<P, _, _>(
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
        .await
    }
}

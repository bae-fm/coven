//! Snapshot streams and atomic reload through the database owner.

use super::Database;
use crate::DbError;
use coven_foundation::progress::ProgressReport;

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
            inner.access.with_committed(|db| {
                let mut bytes = 0u64;
                db.for_each("SELECT s.size FROM _coven_write_slot_audiences a JOIN _coven_write_slots s ON s.slot=a.slot WHERE a.audience=?1 AND a.slot>?2",
                    rusqlite::params![audience, after.to_be_bytes()], |row| {
                        let size = u64::try_from(row.get::<_, i64>(0)?).map_err(|_negative| DbError::DamagedDatabase)?;
                        bytes = crate::sqlite::add_total("snapshot slot bytes", bytes, size)?;
                        Ok::<_, DbError>(())
                    })?;
                if audience == coven_merge::Audience::Store {
                    let records: i64 = db.query_row("SELECT coalesce(sum(length(record)),0) FROM _coven_slot_records WHERE slot>?1",
                        [after.to_be_bytes()], |row| row.get(0))?;
                    let records = u64::try_from(records).map_err(|_negative| DbError::DamagedDatabase)?;
                    bytes = crate::sqlite::add_total("snapshot slot bytes", bytes, records)?;
                }
                Ok(bytes)
            })
        }).await
    }

    /// Headers of waiting plaintext writes, including fixed upload attempts.
    /// Snapshot growth counts these parts before their eventual publication.
    pub async fn waiting_snapshot_headers(
        &self,
    ) -> Result<Vec<coven_format::write_stream::WriteHeaderFrame>, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|db| {
                let mut headers = Vec::new();
                crate::upload::each_plaintext(db, |header, _| {
                    headers.push(header);
                    Ok(())
                })?;
                Ok(headers)
            })
        })
        .await
    }

    /// Run the snapshot loader's schema and merge checks over a staged
    /// snapshot in a transaction that always rolls back, returning the uploaded
    /// files it names. File retention uses this for a snapshot no reload has
    /// read; a damaged snapshot cannot leave rows, metadata or observations
    /// behind. Supply the staging of a body that authenticated: the check is
    /// its last phase, and removes its records whether it passes or fails
    /// (§16.2).
    pub async fn validate_snapshot(
        &self,
        id: coven_format::admin::SnapshotId,
        staging: crate::SnapshotStaging,
        author: coven_foundation::id_source::MemberId,
        authorship: std::sync::Arc<dyn crate::Authorship>,
    ) -> Result<
        std::collections::BTreeSet<(
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::ObjectId,
        )>,
        DbError,
    > {
        if !staging.staged_by(self) {
            return Err(
                super::input_database::staging::refuse(vec![staging], STAGED_ELSEWHERE).await
            );
        }
        let mut stagings = [staging];
        self.call(move |inner| {
            inner.inputs.read_staged(&mut stagings, |inputs, [staging]| {
                // A failure reading the body names the snapshot.
                let body = |error: DbError| crate::SnapshotError::Body {
                    snapshot: id.clone(),
                    error: Box::new(error),
                    staged: None,
                };
                let prepared = staging.prepare(inputs, authorship.as_ref()).map_err(&body)?;
                inner.access.with_writer(|writer| {
                    writer.discarded_transaction(|writer| {
                        let schema = &inner.access.write_schema;
                        crate::snapshot_state::create_tables(writer)?;
                        crate::snapshot_install::create_tables(writer, schema)?;
                        // Validating runs installing's passes where no
                        // host draws them.
                        let mut progress = ProgressReport::unwatched(inner.clock.clone());
                        crate::snapshot_load::begin_installing(schema, 1, &mut progress);
                        crate::snapshot_load::read(
                            writer,
                            schema,
                            prepared,
                            Some(author),
                            &mut progress,
                        )
                        .map_err(&body)?;
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
    /// commits on the writer connection continue throughout this call. While
    /// this device's raise of the audience waits, it refuses with
    /// [`DbError::RaiseWaiting`]: the snapshot would be written at a schema
    /// version the history doesn't hold yet (§16.2). Returns the snapshot's
    /// breaking version, for its announcement (§16.3).
    pub async fn write_snapshot<B, F, E>(
        &self,
        id: coven_format::admin::SnapshotId,
        begin: B,
        emit: F,
    ) -> Result<u32, crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_committed(|reader| {
                // In the transaction that reads the schema version the
                // snapshot is written at.
                if crate::raise_capture::provisional(reader)?
                    .iter()
                    .any(|(audience, _, _)| *audience == id.audience)
                {
                    return Err(DbError::RaiseWaiting.into());
                }
                crate::write_schema::prepare_evaluators(reader, &inner.access.write_schema)?;
                crate::snapshot_write::write(reader, &inner.access.write_schema, id, begin, emit)
            })
        })
        .await
    }

    /// Stream an audience's plaintext frames as [`Self::write_snapshot`] does,
    /// without refusing while a raise of the audience waits: a rebuild's store
    /// holds such raises and writes the circle's state it counted (§18.3).
    pub(crate) async fn write_counted_snapshot<B, F, E>(
        &self,
        id: coven_format::admin::SnapshotId,
        begin: B,
        emit: F,
    ) -> Result<u32, crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_committed(|reader| {
                crate::write_schema::prepare_evaluators(reader, &inner.access.write_schema)?;
                crate::snapshot_write::write(reader, &inner.access.write_schema, id, begin, emit)
            })
        })
        .await
    }

    /// The version of the newest breaking migration this installation ran,
    /// zero if none: its snapshots' breaking version (§16.3).
    pub async fn breaking_version(&self) -> Result<u32, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(crate::raise_capture::breaking_version)
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
    ) -> Result<u32, crate::SnapshotWriteError<E>>
    where
        B: FnOnce(&coven_format::snapshot::SnapshotHeader) -> Result<(), E> + Send + 'static,
        F: FnMut(Vec<u8>) -> Result<(), E> + Send + 'static,
        E: coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_committed(|reader| {
                crate::write_schema::prepare_evaluators(reader, &inner.access.write_schema)?;
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
    /// snapshot supplies its identity and its staging, of a body whose stream
    /// checked out to its end; their write evidence is authenticated, and write
    /// parts are staged on the input connection, before taking the application
    /// writer. Inputs name a fixed target and consecutive retained slots.
    ///
    /// Replaces the selected audiences and replays uncovered parts and waiting
    /// uploads in one transaction. The load is each staging's last phase: it
    /// reads each staged snapshot once and removes its records, whether it
    /// commits or fails. Every audience finishes at the common target slot;
    /// missing history, read errors and failed checks roll the whole load back,
    /// a body's failure as `SnapshotError::Body` naming that snapshot. Returns
    /// the uploaded files each stored snapshot names.
    /// Supply each readable audience's parts, including those needed by waiting
    /// writes and the device's own earlier writes. Other audiences retain their
    /// merge history. Upload records, numbers and sealing key ids stay unchanged.
    /// `progress`, when given, hears the `Installing` and `InstallingSlots`
    /// steps as they run (§13.4).
    pub async fn load_snapshots<P: crate::PendingValue, W>(
        &self,
        reload: crate::SnapshotReload<crate::SnapshotStaging, crate::DownloadedWriteStream<W>>,
        progress: Option<ProgressReport<crate::LoadStep>>,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        W: std::io::Read + Send + 'static,
    {
        self.load_through::<P, W>(reload, progress, None, None).await
    }

    /// Install `circle`, which this device rebuilt under its migration write
    /// `marker`: its start is the rebuilt snapshot, its raise is captured
    /// through the marker, and its wait for the rebuild ends, all in the one
    /// commit that loads it (§18.3). `reload` is a [`Self::load_snapshots`]
    /// input whose `circle` start is the stored snapshot the rebuild exported.
    pub async fn install_rebuilt<P: crate::PendingValue, W>(
        &self,
        reload: crate::SnapshotReload<crate::SnapshotStaging, crate::DownloadedWriteStream<W>>,
        circle: coven_merge::Audience,
        marker: coven_merge::WriteId,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        W: std::io::Read + Send + 'static,
    {
        self.load_through::<P, W>(reload, None, None, Some((circle, marker))).await
    }

    /// [`Self::load_snapshots`], for a rebuild's store when `store_through`
    /// is given: the store's parts in slots after it aren't read, and a write
    /// above this database's version is refused, not waited for (§18.3).
    pub(crate) async fn load_through<P: crate::PendingValue, W>(
        &self,
        reload: crate::SnapshotReload<crate::SnapshotStaging, crate::DownloadedWriteStream<W>>,
        progress: Option<ProgressReport<crate::LoadStep>>,
        store_through: Option<u64>,
        rebuilt: Option<(coven_merge::Audience, coven_merge::WriteId)>,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        W: std::io::Read + Send + 'static,
    {
        // Each stored source names its staging by its place among them.
        let mut stagings = Vec::new();
        let sources: Vec<crate::SnapshotSource<usize>> = reload
            .snapshots
            .into_iter()
            .map(|source| match source {
                crate::SnapshotSource::Stored { id, input, author } => {
                    stagings.push(input);
                    crate::SnapshotSource::Stored { id, input: stagings.len() - 1, author }
                }
                crate::SnapshotSource::Empty(audience) => crate::SnapshotSource::Empty(audience),
                crate::SnapshotSource::Current(audience) => {
                    crate::SnapshotSource::Current(audience)
                }
            })
            .collect();
        if !stagings.iter().all(|staging| staging.staged_by(self)) {
            return Err(super::input_database::staging::refuse(stagings, STAGED_ELSEWHERE).await);
        }
        let authorship = reload.authorship;
        self.call(move |inner| {
            let started = inner.clock.elapsed();
            let mut progress =
                progress.unwrap_or_else(|| ProgressReport::unwatched(inner.clock.clone()));
            inner.inputs.read_staged(&mut stagings, |inputs, stagings| {
                let snapshots = sources
                    .into_iter()
                    .map(|source| {
                        Ok(match source {
                            crate::SnapshotSource::Stored { id, input, author } => {
                                let input = stagings[input]
                                    .prepare(inputs, authorship.as_ref())
                                    .map_err(|error| crate::SnapshotError::Body {
                                        snapshot: id.clone(),
                                        error: Box::new(error),
                                        staged: None,
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
                    provider_access: reload.provider_access,
                    cutoffs: reload.cutoffs,
                    boundaries: reload.boundaries,
                    operation: reload.operation,
                    rebuilds: reload.rebuilds,
                    authorship,
                };
                let prepared = inner.clock.elapsed();
                // Reserve a reader before locking the writer: snapshot consumers
                // may be waiting for a commit before releasing their readers.
                let reader = inner.access.readers.acquire_reader();
                let loaded = inner.with_files("load a snapshot", |writer, files| {
                    reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            crate::write_schema::prepare_evaluators(
                                reader,
                                &inner.access.write_schema,
                            )?;
                            crate::snapshot_load::load::<P>(
                                writer,
                                reader,
                                &inner.access.write_schema,
                                reload,
                                files,
                                &mut progress,
                                store_through,
                                inner.clock.now(),
                                rebuilt.map(|(circle, marker)| crate::snapshot_load::Rebuilt {
                                    circle,
                                    marker,
                                    operation: inner.raise_operation.as_deref(),
                                }),
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

/// A staging another store made: its records are on that store's input
/// connection, not this one's.
const STAGED_ELSEWHERE: DbError =
    DbError::Invariant { what: "a snapshot is loaded by the store that staged it" };

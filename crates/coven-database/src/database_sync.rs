//! Database operations used by synchronization, on the retained connections.

use super::Database;
use crate::{CovenResult, DbError};

impl Database {
    /// Authenticate and retain original source records covered by an applied slot
    /// or snapshot, without applying their data again or changing source verdicts.
    pub async fn retain_write_sources<R, F>(
        &self,
        write: crate::DownloadedWriteStream<R>,
        authenticate: F,
    ) -> Result<(), DbError>
    where
        R: std::io::Read + Send + 'static,
        F: FnOnce() -> Result<(), DbError> + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| {
                    write.retain_sources(db)?;
                    authenticate()
                })
            })
        })
        .await
    }

    /// Commit one complete slot, its claim number and every readable effect.
    /// Authentication failure rolls back the slot and leaves its predecessor applied.
    pub async fn apply_slot<P, R, F>(
        &self,
        incoming: crate::IncomingSlot<R>,
        authenticate: F,
        operation: Option<crate::OperationUpdate>,
    ) -> Result<crate::ApplyOutcome, DbError>
    where
        P: crate::PendingValue,
        R: std::io::Read + Send + 'static,
        F: FnOnce() -> Result<(), DbError> + Send + 'static,
    {
        self.call(move |inner| {
            if let crate::SlotContent::Admin {
                effect: Some(effect),
                ..
            } = &incoming.content
            {
                if let Some(store) = &effect.state.store {
                    crate::store_state::check_creation(
                        store.id,
                        &store.name,
                        inner.access.directory.id(),
                        &inner.name,
                    )?;
                }
            }
            let reader = inner.access.readers.acquire_reader();
            inner.with_files(Vec::new(), |writer, files| {
                reader.with_reader(|reader| {
                    reader.read_transaction(|| {
                        inner.access.write_schema.prepare(reader)?;
                        reader.schema_version()?;
                        crate::sequence_apply::apply::<P, R>(
                            writer,
                            reader,
                            &inner.access.write_schema,
                            incoming,
                            files,
                            authenticate,
                            operation,
                        )
                    })
                })
            })
        })
        .await
    }
    /// Fix the keys of the oldest original before constructing its batch.
    pub async fn prepare_upload<F>(
        &self,
        select: F,
    ) -> Result<Option<coven_merge::WriteId>, DbError>
    where
        F: FnOnce(
                &crate::StoreState,
                u32,
                &coven_format::write_stream::WriteHeaderFrame,
            ) -> Result<coven_format::sealed_write::WriteObjectPrefix, DbError>
            + Send
            + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                let state = crate::store_state_tables::read(db)?;
                crate::upload::prepare(db, &state, select)
            })
        })
        .await
    }
    /// The captured queue prefix eligible before an unpublished breaking raise.
    pub async fn captured_through(&self) -> Result<Option<u64>, DbError> {
        self.call(|inner| {
            inner
                .access
                .with_committed(crate::raise_capture::captured_through)
        })
        .await
    }

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

    /// Audiences whose counted administrative effects require replacement data.
    /// Loading their replacement rows removes the requirement atomically.
    pub async fn reloading_audiences(
        &self,
    ) -> Result<std::collections::BTreeSet<coven_merge::Audience>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(crate::reload_gate::reloading_audiences)
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

    /// Writes and files this device committed and has not uploaded, from one
    /// committed state.
    pub async fn unsent(&self) -> CovenResult<crate::UnsentWork> {
        self.read(|sql| sql.unsent()).await
    }

    /// The upload queue, its pause setting and each file's recorded provider
    /// session in one committed snapshot, available while disconnected (E7).
    pub async fn upload_queue(&self) -> CovenResult<crate::UploadQueueState> {
        self.read(|sql| sql.upload_queue()).await
    }

    /// The same pre-built upload queue query, observing committed changes.
    pub fn subscribe_upload_queue(&self) -> crate::LiveQuery<crate::UploadQueueState> {
        self.subscribe(|sql| sql.upload_queue())
    }

    /// Record one local first unmet condition, replacing its previous reason.
    pub async fn record_pending<P: crate::PendingValue>(&self, record: P) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                if record.reported_by() != inner.access.device {
                    return Err(DbError::Invariant {
                        what: "a recorded local condition is this device's own",
                    });
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

    /// Clear this device's observation when its subject advances. The usual
    /// case, with nothing recorded, reads without taking the writer.
    pub async fn clear_pending<S: serde::Serialize + Send + 'static>(
        &self,
        subject: S,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            let recorded = inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|reader| {
                    crate::pending::recorded(reader, &subject, inner.access.device)
                })?;
            if !recorded {
                return Ok(());
            }
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
                    return Err(DbError::Invariant {
                        what: "a scan replaces distinct own records it covers",
                    });
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

    /// Membership and audience state of the committed sequence prefix.
    pub async fn store_state(&self) -> CovenResult<crate::StoreState> {
        self.call(|inner| {
            inner
                .access
                .with_committed(|db| Ok(crate::store_state_tables::read(db)?))
        })
        .await
    }

    /// The store prefix whose complete effects have committed.
    pub async fn applied_slot(&self) -> Result<u64, DbError> {
        self.call(|inner| inner.access.with_committed(crate::sequence::applied))
            .await
    }

    /// Claim numbers recorded by every consumed slot, including refusals.
    pub async fn last_claims(&self) -> Result<Vec<coven_format::snapshot::LastClaim>, DbError> {
        self.call(|inner| inner.access.with_committed(crate::sequence::claims))
            .await
    }

    /// The active claim and every durable attempted path. `read_current_attempt`
    /// streams the current attempt's exact bytes.
    pub async fn current_claim(&self) -> Result<Option<crate::Claim>, DbError> {
        self.call(|inner| inner.access.with_committed(crate::claim::current))
            .await
    }

    /// Reserve a claim and seal its first attempt under the writer lock.
    /// Returning the attempt proves the journal transaction has committed.
    pub async fn begin_claim<F, R>(
        &self,
        work: crate::ClaimWork,
        seal: F,
    ) -> Result<crate::Claim, DbError>
    where
        F: FnOnce(u64) -> Result<crate::ClaimAttempt<R>, DbError> + Send + 'static,
        R: std::io::Read + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| {
                    crate::claim::begin(db, inner.access.device, work, seal)
                })
            })
        })
        .await
    }

    /// Stream the current attempt's exact ciphertext from one committed reader.
    pub async fn read_current_attempt<T, E, F>(&self, consume: F) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<DbError> + Send + 'static,
        F: FnOnce(crate::ClaimBytes<'_>) -> Result<T, E> + Send + 'static,
    {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|db| db.read_transaction(|| crate::claim::read_attempt(db, consume)))
        })
        .await
    }

    /// Retain a new envelope only after the prior attempt was proved not landed.
    pub async fn retry_claim<R: std::io::Read + Send + 'static>(
        &self,
        attempt: crate::ClaimAttempt<R>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| crate::claim::retry(db, attempt))
            })
        })
        .await
    }

    /// Settle from checked kept bytes or the floor's authenticated snapshot.
    pub async fn settle_claim(
        &self,
        read: crate::ClaimRead,
    ) -> Result<crate::ClaimSettlement, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| crate::claim::settle(db, read))
            })
        })
        .await
    }

    /// Finish a settled claim and its optional operation transition atomically.
    pub async fn finish_claim(
        &self,
        operation: Option<crate::OperationUpdate>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| {
                    crate::claim::finish(db)?;
                    if let Some(update) = operation {
                        crate::operation::advance(db, &update)?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Detect a foreign use of this installation's id against its full journal.
    pub async fn attempted_claim(
        &self,
        path: coven_format::path::ObjectPath,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_committed(|db| crate::claim::attempted(db, &path))
        })
        .await
    }
}

#[path = "database_device.rs"]
mod device;

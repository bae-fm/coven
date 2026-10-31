//! Database operations used by synchronization, on the retained connections.

use super::Database;
use crate::{CovenResult, DbError};

impl Database {
    /// Transfer metadata of the current durable claim attempt.
    pub async fn claim_upload(&self) -> Result<crate::ClaimUpload, DbError> {
        self.call(|inner| inner.access.with_committed(crate::claim::upload))
            .await
    }
    /// A bounded range of the current attempt, used by a recorded upload.
    pub async fn claim_range(&self, offset: u64, length: usize) -> Result<Vec<u8>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_committed(|db| crate::claim::range(db, offset, length))
        })
        .await
    }
    /// Save provider-confirmed progress before the next part is sent.
    pub async fn save_claim_upload(&self, session: Vec<u8>) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.stream_transaction(|db| crate::claim::save_upload(db, session))
            })
        })
        .await
    }
    /// Fix first-attempt keys for this captured queue prefix. Previously
    /// attempted originals retain their exact format and keys.
    pub async fn prepare_uploads<E, F>(
        &self,
        through: u64,
        select: F,
    ) -> Result<Vec<coven_format::write_stream::WriteHeaderFrame>, E>
    where
        E: From<DbError> + From<coven_format::Error> + Send + 'static,
        F: FnMut(
                &coven_format::write_stream::WriteHeaderFrame,
            ) -> Result<coven_format::sealed_write::WriteObjectPrefix, E>
            + Send
            + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                crate::upload::prepare_batch(writer, inner.access.device, through, select)
            })
        })
        .await
    }

    /// Stream the captured originals through one committed reader. Concurrent
    /// app commits append after this fixed number and remain for the next pass.
    pub async fn read_uploads<E, F>(&self, through: u64, consume: F) -> Result<(), E>
    where
        E: From<DbError> + Send + 'static,
        F: FnMut(crate::WaitingUpload<'_>) -> Result<(), E> + Send + 'static,
    {
        self.call(move |inner| {
            inner.access.readers.acquire_reader().with_reader(|reader| {
                reader.read_transaction(|| {
                    crate::upload::read_batch(reader, inner.access.device, through, consume)
                })
            })
        })
        .await
    }

    /// Authenticate and retain original source records covered by an applied slot
    /// or snapshot, without applying their data again or changing source verdicts.
    #[cfg(test)]
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
            inner.inputs.prepare(&[], |inputs| {
                let write = write.prepare(inputs)?;
                authenticate()?;
                inner
                    .access
                    .with_writer(|writer| writer.stream_transaction(|db| write.retain_sources(db)))
            })
        })
        .await
    }

    /// Commit one complete slot, its claim number and every readable effect.
    /// Authentication failure rolls back the slot and leaves its predecessor applied.
    pub async fn apply_slot<P, R, F>(
        &self,
        incoming: crate::IncomingSlot<crate::DownloadedWriteStream<R>>,
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
                    crate::store_state::check_creation(store.id, inner.access.directory.id())?;
                }
            }
            let started = inner.clock.elapsed();
            inner.inputs.prepare(&[], |inputs| {
                let slot = incoming.slot;
                let incoming = incoming.prepare(inputs)?;
                let prepared = inner.clock.elapsed();
                let reader = inner.access.readers.acquire_reader();
                let applied = inner.with_files(Vec::new(), Vec::new(), |writer, files| {
                    reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            inner.access.write_schema.prepare(reader)?;
                            reader.schema_version()?;
                            crate::sequence_apply::apply::<P>(
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
                });
                tracing::debug!(
                    slot = slot.0,
                    preparing = ?(prepared - started),
                    applying = ?(inner.clock.elapsed() - prepared),
                    "applied slot"
                );
                applied
            })
        })
        .await
    }
    /// Fix the keys of the oldest original before constructing its batch.
    #[cfg(test)]
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
    /// The newest waiting write this device may send now, read in one
    /// committed state with the queue (§6.3): none past an unpublished
    /// breaking raise's capture (§18.5), and none from a write a bound upload
    /// holds on (§17.4). `None` with no write waiting.
    pub async fn sendable_through(&self) -> Result<Option<u64>, DbError> {
        self.call(|inner| {
            inner
                .access
                .with_committed(|db| crate::upload::sendable_through(db, inner.access.device))
        })
        .await
    }

    /// Original plaintext of an authenticated received or confirmed published header.
    /// This establishes header facts, not the validity of a subsequently read body.
    #[cfg(test)]
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

    /// Writes and files this device committed and has not uploaded, from one
    /// committed state.
    pub async fn unsent(&self) -> CovenResult<crate::UnsentWork> {
        self.read(|sql| sql.unsent()).await
    }

    /// The same pre-built unsent-work query, observing committed changes.
    pub fn subscribe_unsent(&self) -> crate::LiveQuery<crate::UnsentWork> {
        self.subscribe(|sql| sql.unsent())
    }

    /// The upload queue, its pause setting and each file's recorded provider
    /// session in one committed snapshot, available while disconnected (E7).
    pub async fn upload_queue(&self) -> CovenResult<crate::UploadQueueState> {
        self.read(|sql| sql.upload_queue()).await
    }

    /// Record one local first unmet condition, replacing its previous reason.
    pub async fn record_pending<P: crate::PendingValue>(&self, record: P) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
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
        self.clear_pending_checked(subject, crate::pending::recorded, |_| Ok(()))
            .await
            .map(drop)
    }

    /// Settle only the condition the consumer actually checked, using the
    /// subject's indexed lookup and the same pending transaction as deletion.
    pub(crate) async fn clear_checked<P: crate::PendingValue>(
        &self,
        subject: P::Subject,
        checked: impl Fn(&P) -> bool + Send + 'static,
    ) -> Result<(), DbError>
    where
        P::Subject: Send + 'static,
    {
        self.clear_pending_checked(
            subject,
            move |db, subject| {
                Ok(crate::pending::find::<P>(db, subject)?.is_some_and(|record| checked(&record)))
            },
            |_| Ok(()),
        )
        .await
        .map(drop)
    }

    /// Settle only the condition the consumer actually checked, as
    /// `clear_checked` does, ending the wait of `object`'s queued upload
    /// with it.
    pub(crate) async fn clear_pending_if<P: crate::PendingValue>(
        &self,
        object: (
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::FileId,
        ),
        subject: P::Subject,
        checked: impl Fn(&P) -> bool + Send + 'static,
    ) -> Result<Option<crate::RetrySubject>, DbError>
    where
        P::Subject: Send + 'static,
    {
        self.clear_pending_checked(
            subject,
            move |db, subject| {
                Ok(crate::pending::find::<P>(db, subject)?.is_some_and(|record| checked(&record)))
            },
            move |db| {
                let retry = crate::file_queue::upload_id(db, object.0, object.1)?
                    .map(crate::RetrySubject::Upload);
                if let Some(retry) = retry {
                    crate::retry::clear(db, retry)?;
                }
                Ok(retry)
            },
        )
        .await
        .map(Option::flatten)
    }

    async fn clear_pending_checked<S, F, T>(
        &self,
        subject: S,
        checked: F,
        settled: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<T, DbError> + Send + 'static,
    ) -> Result<Option<T>, DbError>
    where
        S: serde::Serialize + Send + 'static,
        T: Send + 'static,
        F: Fn(&crate::sqlite::DatabaseConnection, &S) -> Result<bool, DbError> + Send + 'static,
    {
        self.call(move |inner| {
            let should_clear = inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|db| checked(db, &subject))?;
            if !should_clear {
                return Ok(None);
            }
            inner.access.with_writer(|writer| {
                writer.pending_transaction(|db| {
                    // Another writer may have replaced this condition since
                    // the committed read; deletion checks that current record.
                    if checked(db, &subject)? {
                        crate::pending::clear(db, &subject)?;
                        return settled(db).map(Some);
                    }
                    Ok(None)
                })
            })
        })
        .await
    }

    /// Replace this device's observations of one completed scan atomically:
    /// its records matching `scanned` become exactly `records`, so a subject
    /// that now checks, or is no longer listed, loses its record in the same
    /// transaction. An unchanged set writes nothing.
    #[cfg(test)]
    pub async fn replace_local_pending<P: crate::PendingValue>(
        &self,
        scanned: impl Fn(&P) -> bool + Send + 'static,
        records: Vec<P>,
    ) -> Result<(), DbError> {
        use std::collections::BTreeSet;
        self.call(move |inner| {
            let mut identities = BTreeSet::new();
            for record in &records {
                if record.operation().is_some()
                    || !scanned(record)
                    || !identities.insert(record.subject())
                {
                    return Err(DbError::Invariant {
                        what: "a scan replaces distinct records it covers",
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
                    let current = crate::pending::read::<P>(reader)?
                        .into_iter()
                        .filter(|r| scanned(r))
                        .map(|r| encode(&r))
                        .collect::<Result<BTreeSet<_>, _>>()?;
                    Ok::<_, DbError>(current == next)
                })?;
            if unchanged {
                return Ok(());
            }
            inner.access.with_writer(|writer| {
                let previous: Vec<_> = crate::pending::read::<P>(writer)?
                    .into_iter()
                    .filter(|r| scanned(r))
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
                            crate::pending::clear(db, record.subject())?;
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
            inner
                .access
                .with_writer(|writer| crate::pending::retry::<P>(writer, &subject))
        })
        .await
    }

    /// Read the oldest waiting plaintext and its recorded sealing keys.
    /// The callback runs in a committed reader transaction; its result is `None`
    /// when the queue is empty.
    #[cfg(any(test, feature = "test-utils"))]
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

    /// Claim numbers and complete-slot digests recorded by every consumed slot, including refusals.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn last_claims(&self) -> Result<Vec<coven_format::snapshot::LastClaim>, DbError> {
        self.call(|inner| inner.access.with_committed(crate::sequence::claims))
            .await
    }

    /// Original administrative, copy and announcement evidence retained by snapshots.
    pub async fn original_slots(&self) -> Result<Vec<coven_format::floor::OriginalSlot>, DbError> {
        self.call(|inner| {
            inner.access.with_committed(|db| {
                db.query(
                    "SELECT slot,record FROM _coven_slot_records ORDER BY slot",
                    [],
                    |row| {
                        Ok(coven_format::floor::OriginalSlot {
                            slot: coven_merge::Slot(crate::write_encoding::counter(row.get(0)?)),
                            object: row.get(1)?,
                        })
                    },
                )
            })
        })
        .await
    }

    /// Greatest locally reserved claim number, including finished journals.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn claim_counter(&self) -> Result<u64, DbError> {
        self.call(|inner| {
            inner
                .access
                .with_committed(|db| crate::claim::reserved(db, inner.access.device))
        })
        .await
    }

    /// Whether this copy journaled these complete bytes at this exact slot.
    pub async fn attempted_slot(
        &self,
        number: u64,
        path: coven_format::path::ObjectPath,
        digest: [u8; 32],
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_committed(|db| crate::claim::attempted_digest(db, number, &path, &digest))
        })
        .await
    }

    /// The active claim and every durable attempted path. `claim_range`
    /// reads the current attempt's exact bytes.
    pub async fn current_claim(&self) -> Result<Option<crate::Claim>, DbError> {
        self.call(|inner| inner.access.with_committed(crate::claim::current))
            .await
    }

    /// Seal and stage outside the application writer, then atomically reserve
    /// the claim number and retain its complete attempted bytes.
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
            let number = inner
                .access
                .with_committed(|db| crate::claim::reserved(db, inner.access.device))?
                .checked_add(1)
                .ok_or(DbError::ClaimNumberExhausted)?;
            let attempt = seal(number)?;
            inner.inputs.prepare_claim(attempt, |prepared| {
                inner.access.with_writer(|writer| {
                    writer.stream_transaction(|db| {
                        crate::claim::begin(db, inner.access.device, work, number, prepared)
                    })
                })
            })
        })
        .await
    }

    /// Stream the current attempt's exact ciphertext from one committed reader.
    #[cfg(any(test, feature = "test-utils"))]
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
            inner.inputs.prepare_claim(attempt, |prepared| {
                inner.access.with_writer(|writer| {
                    writer.stream_transaction(|db| crate::claim::retry(db, prepared))
                })
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

    /// Whether the open claim attempted this path. Finished journals are deleted.
    #[cfg(any(test, feature = "test-utils"))]
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

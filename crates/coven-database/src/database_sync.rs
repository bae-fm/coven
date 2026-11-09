//! Database operations used by synchronization, on the retained connections.

use super::{Database, Mode};
use crate::internal_schema::retry_effect::{self, RetryState};
use crate::DbError;
use coven_foundation::task::CarriesBug;

impl Database {
    /// Transfer metadata of the current durable claim attempt.
    pub async fn claim_upload(&self) -> Result<crate::ClaimUpload, DbError> {
        self.call(|inner| inner.access.with_committed(crate::claim::upload)).await
    }
    /// A bounded range of the current attempt, used by a recorded upload.
    pub async fn claim_range(&self, offset: u64, length: usize) -> Result<Vec<u8>, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|db| crate::claim::range(db, offset, length))
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
    ) -> Result<crate::PreparedUploads, E>
    where
        E: From<DbError> + From<coven_format::Error> + CarriesBug + Send + 'static,
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

    /// Stream the prepared originals through one committed reader. Concurrent
    /// app commits append after the prepared prefix and remain for the next pass.
    pub async fn read_uploads<E, F>(
        &self,
        prepared: crate::PreparedUploads,
        consume: F,
    ) -> Result<(), E>
    where
        E: From<DbError> + CarriesBug + Send + 'static,
        F: FnMut(crate::WaitingUpload<'_>) -> Result<(), E> + Send + 'static,
    {
        self.call(move |inner| {
            inner
                .access
                .with_committed(|reader| crate::upload::read_batch(reader, prepared, consume))
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
            inner.inputs.prepare(|inputs| {
                let write = write.prepare(inputs)?;
                authenticate()?;
                inner.access.with_writer(|writer| {
                    writer.stream_transaction(|db| {
                        crate::download_stream::retain_sources(
                            db,
                            &inner.access.write_schema,
                            write,
                        )
                    })
                })
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
    ) -> Result<crate::ApplyOutcome, DbError>
    where
        P: crate::PendingValue,
        R: std::io::Read + Send + 'static,
        F: FnOnce() -> Result<(), DbError> + Send + 'static,
    {
        self.call(move |inner| {
            if let crate::SlotContent::Admin { effect: Some(effect), .. } = &incoming.content {
                if let Some(store) = &effect.state.store {
                    crate::store_state::check_creation(store.id, inner.access.directory.id())?;
                }
            }
            let started = inner.clock.elapsed();
            inner.inputs.prepare(|inputs| {
                let slot = incoming.slot;
                let incoming = incoming.prepare(inputs)?;
                let prepared = inner.clock.elapsed();
                let reader = inner.access.readers.acquire_reader();
                let applied = inner.with_files("apply a slot", |writer, files| {
                    reader.with_reader(|reader| {
                        reader.read_transaction(|| {
                            crate::write_schema::prepare_evaluators(
                                reader,
                                &inner.access.write_schema,
                            )?;
                            reader.schema_version()?;
                            crate::sequence_apply::apply::<P>(
                                writer,
                                reader,
                                &inner.access.write_schema,
                                incoming,
                                files,
                                inner.raise_operation.as_deref(),
                                authenticate,
                            )
                        })
                    })
                });
                tracing::debug!(
                    slot = slot.get(),
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
    /// breaking raise's capture or from a write ahead of an audience's
    /// version, whose wait it records (§18.5), and none from a write a bound
    /// upload holds on (§17.4). `None` with no write waiting.
    pub async fn sendable_through<P: crate::PendingValue>(&self) -> Result<Option<u64>, DbError> {
        self.call(|inner| {
            let device = inner.access.device;
            let ahead =
                inner.access.with_committed(|db| crate::raise_capture::first_ahead(db, device))?;
            if ahead.is_none() {
                return inner
                    .access
                    .with_committed(|db| crate::upload::sendable_through(db, device));
            }
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| {
                    crate::raise_capture::record_ahead::<P>(db, device)?;
                    crate::upload::sendable_through(db, device)
                })
            })
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
    pub async fn pending<P: crate::PendingValue>(&self) -> Result<Vec<P>, DbError> {
        self.read(|sql| sql.pending()).await
    }

    /// Writes and files this device committed and has not uploaded, from one
    /// committed state.
    pub async fn unsent(&self) -> Result<crate::UnsentWork, DbError> {
        self.read(|sql| sql.unsent()).await
    }

    /// The same pre-built unsent-work query, observing committed changes.
    pub fn subscribe_unsent<E>(&self) -> crate::LiveQuery<crate::UnsentWork, E>
    where
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.subscribe(|sql| sql.unsent().map_err(E::from))
    }

    /// What the sync state is computed from, in one committed snapshot
    /// (§19.1): `coven_sync_status`'s row, `coven_retry_effect`'s, the send
    /// and upload queues, the files moving and the pending records, decoded
    /// as the sync owner's typed values, given to `compute` in the read.
    pub async fn sync_facts<P, T, E>(
        &self,
        compute: impl FnOnce(crate::SyncFacts<P>) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        P: crate::PendingValue + Clone,
        T: Send + 'static,
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.read(move |sql| compute(sql.sync_facts()?)).await
    }

    /// The same pre-built query, live: it runs again on each committed
    /// change of what it reads, and as a file starts or ends a transfer.
    pub fn subscribe_sync_facts<P, T, E>(
        &self,
        compute: impl Fn(crate::SyncFacts<P>) -> Result<T, E> + Send + Sync + 'static,
    ) -> crate::LiveQuery<T, E>
    where
        P: crate::PendingValue + Clone,
        T: Send + 'static,
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.subscribe(move |sql| compute(sql.sync_facts()?))
    }

    /// The upload queue, its pause setting and each file's recorded provider
    /// session in one committed snapshot, available while disconnected (E7).
    pub async fn upload_queue(&self) -> Result<crate::UploadQueueState, DbError> {
        self.read(|sql| sql.upload_queue()).await
    }

    /// Record, as the store opens, whether a storage location is recorded
    /// here and its credentials held, and that no pass moves, keeping how the
    /// last pass ended and when sync last finished; `finished`, for a store
    /// joining or restoring read to its end, is when (api.md E5). The
    /// provider is as sync names it. A failure fails this handle with its
    /// cause, as losing a pending record does.
    pub async fn record_opened(
        &self,
        location: crate::RecordedLocation<String>,
        finished: Option<std::time::SystemTime>,
    ) -> Result<(), DbError> {
        self.save_sync_status(move |db| {
            crate::internal_schema::sync_status::save_location(db, &location, finished, true)
        })
        .await
    }

    /// Record whether a storage location is recorded here and its
    /// credentials held, as setup, a change of credentials or releasing a
    /// connection finds it; `finished`, for setup, is when it read the store
    /// to its end. A failure fails this handle, as `record_opened`'s does.
    pub async fn record_location(
        &self,
        location: crate::RecordedLocation<String>,
        finished: Option<std::time::SystemTime>,
    ) -> Result<(), DbError> {
        self.save_sync_status(move |db| {
            crate::internal_schema::sync_status::save_location(db, &location, finished, false)
        })
        .await
    }

    /// Record that the running pass read a slot it hadn't, or claimed one.
    /// A failure fails this handle, as `record_opened`'s does.
    pub async fn record_moving(&self) -> Result<(), DbError> {
        self.save_sync_status(crate::internal_schema::sync_status::save_moving).await
    }

    /// Record how the last pass ended, as sync encodes it, when sync failed
    /// outside a pass's end: starting, stopping, or a local failure or panic
    /// of the loop's, in a pass or outside one; and that nothing moves. A
    /// failure fails this handle, as `record_opened`'s does.
    pub async fn record_last_pass(&self, last_pass: Vec<u8>) -> Result<(), DbError> {
        self.save_sync_status(move |db| {
            crate::internal_schema::sync_status::save_last_pass(db, &last_pass)
        })
        .await
    }

    /// Record that a store-wide worker's panic halted sync until the store
    /// reopens, with its failure, as sync encodes it, as how the last pass
    /// ended, and that nothing moves: from here no pass writes the row. A
    /// failure fails this handle, as `record_opened`'s does.
    pub async fn record_halt(&self, last_pass: Vec<u8>) -> Result<(), DbError> {
        self.save_sync_status(move |db| {
            crate::internal_schema::sync_status::save_halt(db, &last_pass)
        })
        .await
    }

    /// Record how the running pass ended, as sync encodes it, and that it
    /// moves no more; `finished`, for a pass that reached the end of the
    /// sequence, is when, which becomes the last synced time unless that is
    /// later. A failure fails this handle, as `record_opened`'s does.
    pub async fn record_pass_end(
        &self,
        last_pass: Vec<u8>,
        finished: Option<std::time::SystemTime>,
    ) -> Result<(), DbError> {
        self.save_sync_status(move |db| {
            crate::internal_schema::sync_status::save_pass_end(db, &last_pass, finished)
        })
        .await
    }

    /// Write `coven_sync_status` in the step that changes it, on the writer.
    async fn save_sync_status(
        &self,
        save: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<(), DbError> + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| inner.access.with_writer(|writer| writer.state_transaction(save)))
            .await
    }

    /// Change what decides the person's retry, `coven_retry_effect`'s row,
    /// then run `then` with the state as committed, in one step on the
    /// writer: no other change to the state, and no other `then`, comes
    /// between them, so a decision `then` makes from the state is the one
    /// the row shows. `change` gets the state as the row holds it; `then`
    /// runs only once the change commits. A failed write fails this handle
    /// with its cause, as losing a pending record does.
    pub async fn change_retry_state<T: Send + 'static>(
        &self,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
        then: impl FnOnce(RetryState) -> T + Send + 'static,
    ) -> Result<T, DbError> {
        self.retry_step(change, then, |_| Ok(())).await
    }

    /// Change the retry state as sync starts, as `change_retry_state` does,
    /// and count the start in `coven_sync_status.starts` in the same
    /// transaction: every start writes the row, even one that holds for the
    /// reason the last one did.
    pub async fn start_retry_state<T: Send + 'static>(
        &self,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
        then: impl FnOnce(RetryState) -> T + Send + 'static,
    ) -> Result<T, DbError> {
        self.retry_step(change, then, crate::internal_schema::sync_status::save_start).await
    }

    async fn retry_step<T: Send + 'static>(
        &self,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
        then: impl FnOnce(RetryState) -> T + Send + 'static,
        also: fn(&crate::sqlite::DatabaseConnection) -> Result<(), DbError>,
    ) -> Result<T, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                let state = writer.state_transaction(|db| {
                    let state = retry_effect::stored(change(retry_effect::read(db)?))?;
                    retry_effect::save(db, state)?;
                    also(db)?;
                    Ok(state)
                })?;
                Ok(then(state))
            })
        })
        .await
    }

    /// Record the provider cooldown's wait, and change the retry state as
    /// `change_retry_state` does, in one transaction: the cooldown that
    /// holds every pass and the effect it gives a retry commit together.
    pub async fn record_cooldown(
        &self,
        wait: crate::RetryWait,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| {
                    crate::retry::save(db, crate::RetrySubject::Provider, &wait)?;
                    let state = retry_effect::stored(change(retry_effect::read(db)?))?;
                    retry_effect::save(db, state)
                })
            })
        })
        .await
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
                writer.state_transaction(|db| crate::pending::save(db, &record))
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
        self.clear_pending_checked(subject, crate::pending::recorded, |_| Ok(())).await.map(drop)
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
        object: (coven_foundation::id_source::DeviceId, coven_foundation::id_source::ObjectId),
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
            let should_clear =
                inner.access.readers.acquire_reader().with_reader(|db| checked(db, &subject))?;
            if !should_clear {
                return Ok(None);
            }
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| {
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
            let encode = |r: &P| serde_json::to_vec(r).map_err(DbError::StateEncoding);
            let next = records.iter().map(encode).collect::<Result<BTreeSet<_>, _>>()?;
            // The usual unchanged scan reads its class without taking the writer.
            let unchanged = inner.access.readers.acquire_reader().with_reader(|reader| {
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
                let previous: Vec<_> =
                    crate::pending::read::<P>(writer)?.into_iter().filter(|r| scanned(r)).collect();
                if previous.iter().map(encode).collect::<Result<BTreeSet<_>, _>>()? == next {
                    return Ok(());
                }
                writer.state_transaction(|db| {
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
            inner.access.with_writer(|writer| crate::pending::retry::<P>(writer, &subject))
        })
        .await
    }

    /// Make every pending record one `build` recorded, as a store opened by
    /// another build of coven finds the records an earlier one wrote (§19.1).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn set_pending_build(&self, build: u128) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| crate::pending::set_build(db, build))
            })
        })
        .await
    }

    /// Read the oldest waiting plaintext.
    /// The callback runs in a committed reader transaction; its result is `None`
    /// when the queue is empty.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn read_oldest_upload<F, R, E>(
        &self,
        consume: F,
    ) -> Result<Option<R>, crate::UploadReadError<E>>
    where
        F: FnOnce(crate::UploadPlaintext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: CarriesBug + Send + 'static,
    {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| crate::upload::read(reader, consume))
            })
        })
        .await
    }

    /// The oldest waiting write and the keys its first attempt fixed, if
    /// any; `None` when the queue is empty.
    #[cfg(test)]
    pub(crate) async fn oldest_upload_keys(
        &self,
    ) -> Result<
        Option<(coven_merge::WriteId, Option<coven_format::sealed_write::WriteObjectPrefix>)>,
        DbError,
    > {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|reader| {
                reader.read_transaction(|| crate::upload::oldest_keys(reader))
            })
        })
        .await
    }

    /// The same state, live (E4), for a test waiting for passes to change
    /// it, such as to apply a raise.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn subscribe_store_state(&self) -> crate::LiveQuery<crate::StoreState, DbError> {
        self.subscribe(|sql| sql.store_state())
    }

    /// The store prefix whose complete effects have committed.
    pub async fn applied_slot(&self) -> Result<u64, DbError> {
        self.call(|inner| {
            #[cfg(any(test, feature = "test-utils"))]
            if inner
                .applied_slot_failures
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |left| left.checked_sub(1),
                )
                .is_ok()
            {
                return Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some("injected applied slot read failure".into()),
                )));
            }
            inner.access.with_committed(crate::sequence::applied)
        })
        .await
    }

    /// Fail the next `count` `applied_slot` reads with a disk error.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn fail_applied_slots(&self, count: usize) {
        self.call(move |inner| {
            inner.applied_slot_failures.store(count, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, DbError>(())
        })
        .await
        .expect("an open store arms its applied slot failures");
    }

    /// Claim numbers and complete-slot digests recorded by every consumed slot, including refusals.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn last_claims(&self) -> Result<Vec<coven_format::snapshot::LastClaim>, DbError> {
        self.call(|inner| inner.access.with_committed(crate::sequence::claims)).await
    }

    /// Original administrative, copy and announcement evidence retained by snapshots.
    pub async fn original_slots(&self) -> Result<Vec<coven_format::floor::OriginalSlot>, DbError> {
        self.call(|inner| {
            inner.access.with_committed(|db| {
                db.query("SELECT slot,record FROM _coven_slot_records ORDER BY slot", [], |row| {
                    Ok(coven_format::floor::OriginalSlot {
                        slot: crate::write_encoding::slot_counter(row.get(0)?)?,
                        object: row.get(1)?,
                    })
                })
            })
        })
        .await
    }

    /// Greatest locally reserved claim number, including finished journals.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn claim_counter(&self) -> Result<u64, DbError> {
        self.call(|inner| {
            inner.access.with_committed(|db| crate::claim::reserved(db, inner.access.device))
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
        self.call(|inner| inner.access.with_committed(crate::claim::current)).await
    }

    /// Seal and stage outside the application writer, then atomically reserve
    /// the claim number and retain its complete attempted bytes.
    /// Returning the attempt proves the journal transaction has committed.
    pub async fn begin_claim<F, R, E>(
        &self,
        work: crate::ClaimWork,
        seal: F,
    ) -> Result<crate::Claim, E>
    where
        F: FnOnce(u64) -> Result<crate::ClaimAttempt<R>, E> + Send + 'static,
        R: std::io::Read + Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        self.call(move |inner| {
            let number = inner
                .access
                .with_committed(|db| crate::claim::reserved(db, inner.access.device))?
                .checked_add(1)
                .ok_or(DbError::ClaimNumberExhausted)?;
            let attempt = seal(number)?;
            Ok(inner.inputs.prepare_claim(attempt, |prepared| {
                inner.access.with_writer(|writer| {
                    writer.stream_transaction(|db| {
                        crate::claim::begin(db, inner.access.device, work, number, prepared)
                    })
                })
            })?)
        })
        .await
    }

    /// Stream the current attempt's exact ciphertext from one committed reader.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn read_current_attempt<T, E, F>(&self, consume: F) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
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
        self.call(move |inner| inner.access.with_committed(|db| crate::claim::attempted(db, &path)))
            .await
    }
}

#[path = "database_device.rs"]
mod device;

impl<M: Mode> Database<M> {
    /// Membership and audience state of the committed sequence prefix.
    pub async fn store_state(&self) -> Result<crate::StoreState, DbError> {
        self.call(|open| M::access(open).with_committed(crate::store_state_tables::read)).await
    }
}

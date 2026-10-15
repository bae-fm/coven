//! One snapshot attempt, from preparation to its confirmed counting landing.
//! Every snapshot, ordinary, raise or reset, is prepared right after this
//! pass's completed catch-up, sampled fresh, and must cover its audience's
//! current newest counting snapshot. It is sent once its own captured prefix
//! is confirmed, and abandoned for a new path once it can no longer count
//! (§15, SP step 8).

use super::{io, snapshot_path, StoreLogSync};
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    snapshot_data::*,
    PendingReason, Prerequisite, SyncError,
};
use coven_database::{OperationRecord, StoreLog};
use coven_format::{
    sealed_snapshot::SnapshotObjectPrefix, store_log::SnapshotId, value::StorageTime,
    value::WritePositions,
};
use coven_merge::Audience;
use coven_storage::StorageFailure;

impl StoreLogSync {
    pub(super) async fn write_snapshot_step(
        &mut self,
        record: &OperationRecord,
        task: SnapshotTask,
    ) -> Result<Progress, SyncError> {
        let SnapshotJob::Write {
            audience,
            device,
            trigger,
            attempt,
            ..
        } = task.job.clone()
        else {
            unreachable!("snapshot writing step")
        };
        if matches!(trigger, SnapshotTrigger::Reset) && record.last_step == 0 {
            self.catch_up().await?;
        }
        if !matches!(trigger, SnapshotTrigger::Raise { .. }) {
            if let Some(id) = self.pending_reload().await? {
                return Err(SyncError::ReloadPending(id));
            }
        }
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        if matches!(trigger, SnapshotTrigger::Reset) {
            crate::effects::Authority::reset(&audience)
                .check(&local.log.replay.state, &member.member())?;
        }
        // Each run first settles what an earlier abandonment left (§15).
        let task = self.settle_abandoned(record, task).await?;
        let published = |attempt: &SnapshotAttempt| SnapshotId {
            audience: audience.clone(),
            device,
            number: attempt.number.get(),
        };
        if let SnapshotTrigger::Raise { version, ref entry } = trigger {
            if entry.is_none()
                && crate::store_log_sync::schema_sync::schema_in_place(
                    &local.log.replay.state,
                    &audience,
                    version,
                )
            {
                if let Some(wait) = self.unsettled(&task)? {
                    return Ok(wait);
                }
                self.discard_snapshot(record, task).await?;
                self.database
                    .discard_raise_capture(audience.clone(), version)
                    .await?;
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
            if record.last_step >= 2 {
                let attempt = attempt.as_ref().ok_or(prepared_attempt())?;
                // Its entry, not yet prepared, carries the stored snapshot's
                // `T_c` and counts only if it lands within a day of it (§9,
                // §15). Past that day the attempt is abandoned and the raise
                // prepared afresh at a new path; an entry already sent that
                // lands late is dropped, which restarts it the same way. Its
                // snapshot landed counting and stays an own snapshot.
                let caught_at = match entry {
                    Some(_) => None,
                    None => {
                        let name =
                            task.temporary
                                .first()
                                .ok_or(coven_database::DbError::Invariant {
                                what:
                                    "a stored raise snapshot keeps its sealed file until its entry",
                            })?;
                        let caught_at =
                            sealed_prefix(&io::SnapshotInput::open(&self.directory, name)?)?
                                .caught_at;
                        if !self.may_count(caught_at) {
                            tracing::debug!("a raise's entry window passed before its entry");
                            return self.abandon_snapshot(record, task, None).await;
                        }
                        Some(caught_at)
                    }
                };
                return self
                    .raise_snapshot_step(record, task, version, published(attempt), caught_at)
                    .await;
            }
            // A later breaking migration replaced this capture: its own
            // raise supersedes this one.
            if !self
                .database
                .raise_captures()
                .await?
                .iter()
                .any(|c| c.audience == audience && c.version == version)
            {
                if let Some(wait) = self.unsettled(&task)? {
                    return Ok(wait);
                }
                self.discard_snapshot(record, task).await?;
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
        }
        match (record.last_step, attempt) {
            (0, _) => {
                self.prepare_snapshot(record, task, &local.log, &member)
                    .await
            }
            (1, Some(attempt)) => self.send_snapshot(record, task, attempt).await,
            (2, Some(attempt)) => {
                let id = published(&attempt);
                self.finish_snapshot(record, task, id).await
            }
            _ => Err(prepared_attempt().into()),
        }
    }

    async fn prepare_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        log: &StoreLog,
        member: &coven_crypto::MemberIdentity,
    ) -> Result<Progress, SyncError> {
        let SnapshotJob::Write {
            audience,
            device,
            trigger,
            ..
        } = task.job.clone()
        else {
            unreachable!("snapshot preparation step")
        };
        self.clear_snapshot_files(record, &mut task).await?;
        if matches!(trigger, SnapshotTrigger::Growth) && !self.snapshot_grew(&audience, log).await?
        {
            if let Some(wait) = self.unsettled(&task)? {
                return Ok(wait);
            }
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        if let SnapshotTrigger::Coverage { writer, number } = trigger {
            let write = coven_merge::WriteId {
                device: writer,
                number,
            };
            if self.currently_covered(&audience, log, write).await? {
                if let Some(wait) = self.unsettled(&task)? {
                    return Ok(wait);
                }
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
        }
        if !self.snapshot_audiences(log)?.contains(&audience) {
            // Coven's own snapshot of an audience this member left has
            // nothing left to do; the app's raise or reset is refused.
            if matches!(
                trigger,
                SnapshotTrigger::Raise { .. } | SnapshotTrigger::Reset
            ) {
                return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
            }
            if let Some(wait) = self.unsettled(&task)? {
                return Ok(wait);
            }
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        if let Some(reason) = self.replacement_holds_snapshots().await? {
            return Ok(Progress::Waiting(reason));
        }
        // Only right after this pass's catch-up read every writer's log
        // through its next-number miss, with nothing pending; its sample from
        // before those reads is `T_c` (§15; io `completed_catchup_covers_old`).
        let Some(caught_at) = self.reads.write_catch_up() else {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::CatchUp,
            )));
        };
        // An attempt whose sample is already more than a day old would be
        // abandoned before sending; a later catch-up samples again.
        if !self.may_count(caught_at) {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::CatchUp,
            )));
        }
        // A raise snapshot is its migration's capture when that catch-up
        // applied nothing past it, and otherwise captured again from the
        // current state (§15, §17.1; io `RaiseHistory`).
        let (raise, positions) = match trigger {
            SnapshotTrigger::Raise { version, .. } => {
                let Some(capture) = self
                    .database
                    .prepare_raise_capture(audience.clone(), version)
                    .await?
                else {
                    if let Some(wait) = self.unsettled(&task)? {
                        return Ok(wait);
                    }
                    self.discard_snapshot(record, task).await?;
                    self.database.finish_operation(record.id).await?;
                    return Ok(Progress::Finished(Output::Unit));
                };
                (Some(version), capture.writes)
            }
            _ => (None, self.database.sync_state(Vec::new()).await?.positions),
        };
        if let Some(wait) = self.behind_current(&audience, log, &positions).await? {
            return Ok(Progress::Waiting(wait));
        }
        let ring = self.store_keys.read()?.ok_or_else(|| {
            SyncError::KeyUnavailable(
                crate::key_introductions::newest(log, &audience)
                    .expect("readable audience has a key"),
            )
        })?;
        // A snapshot's first upload is a first attempt in its audience (§11).
        // Without a usable key it waits; other audiences' sends go on.
        let observation = self.copy_observation()?;
        let key_id = match crate::write_seal::first_attempt(
            log,
            &observation,
            &ring,
            &member.member(),
            &audience,
        ) {
            Ok(key) => key,
            Err(SyncError::KeyUnavailable(key)) => {
                return Ok(Progress::Waiting(PendingReason::KeyUnavailable {
                    audience: audience.clone(),
                    key,
                }))
            }
            Err(error) => return Err(error),
        };
        let key = io::key(&ring, &audience, key_id)?;
        // Every covered write's retirement-window verdict must be final, so
        // a loader holds what the final replay holds (§15, storelog-data
        // `prepared_final`). A raise's capture was kept only if nothing it
        // was taken from has applied since, so this check of the current
        // state covers its verdicts too (io `RaiseHistory`).
        let latest = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .caught_up_window();
        if let Some(wait) = self
            .database
            .snapshot_verdict_wait(
                audience.clone(),
                member.keys().public_keys().signing,
                latest,
            )
            .await?
        {
            return Ok(Progress::Waiting(PendingReason::Waits(match wait {
                coven_database::VerdictWait::Entry(entry) => Prerequisite::EntryFinality(entry),
                coven_database::VerdictWait::CatchUp => Prerequisite::CatchUp,
            })));
        }
        // Each attempt takes a path no earlier attempt used, so an abandoned
        // request that lands later never meets this one (§15).
        let number = self.database.reserve_snapshot_number().await?;
        let id = SnapshotId {
            audience: audience.clone(),
            device,
            number: number.get(),
        };
        let path = snapshot_path(self.directory.id(), &id)?;
        let name = self
            .reserve_snapshot_files(record, &mut task, 1)
            .await?
            .remove(0);
        self.seal_snapshot(
            id.clone(),
            raise,
            (key_id, key),
            caught_at,
            member,
            &path,
            &name,
        )
        .await?;
        set_attempt(
            &mut task,
            Some(SnapshotAttempt {
                number,
                caught_at,
                session: None,
                sent: false,
            }),
        );
        self.save_snapshot_task(record, &task, 1).await?;
        Ok(Progress::Advanced)
    }

    /// Whether the audience's current newest counting snapshot, as its
    /// catalog lists it now, covers `write`: another device's snapshot may
    /// have supplied the coverage retention asked for (§15). An unverified
    /// newest decides nothing, so the request stands.
    async fn currently_covered(
        &self,
        audience: &Audience,
        log: &StoreLog,
        write: coven_merge::WriteId,
    ) -> Result<bool, SyncError> {
        let catalog = self.snapshot_candidates(audience, log).await?;
        Ok(catalog.newest_unknown.is_none()
            && catalog
                .verified
                .iter()
                .find(|snapshot| snapshot.follows_boundary(log))
                .is_some_and(|newest| newest.prefix.writes.covers(write)))
    }

    /// What holds a preparation back from its audience's current newest
    /// counting snapshot: it must cover every position of that snapshot,
    /// read after the catch-up's sample, though a concurrent arrival need not
    /// be covered (§15; io `SnapshotPublication.canPrepare`). A listed prefix
    /// this device has not verified may be that newest, so preparation waits
    /// on it as retention does; its own record names why.
    async fn behind_current(
        &self,
        audience: &Audience,
        log: &StoreLog,
        positions: &WritePositions,
    ) -> Result<Option<PendingReason>, SyncError> {
        let catalog = self.current_snapshot_candidates(audience, log).await?;
        if let Some(unknown) = catalog.newest_unknown {
            return Ok(Some(PendingReason::Waits(Prerequisite::Object(unknown))));
        }
        let Some(newest) = catalog
            .verified
            .iter()
            .find(|snapshot| snapshot.follows_boundary(log))
        else {
            return Ok(None);
        };
        if newest
            .prefix
            .writes
            .0
            .iter()
            .all(|id| positions.covers(*id))
        {
            return Ok(None);
        }
        // A completed catch-up reads every position a counting snapshot
        // lists; one that landed after it waits for the next.
        tracing::debug!(?audience, "snapshot capture is behind the current snapshot");
        Ok(Some(PendingReason::Waits(Prerequisite::CatchUp)))
    }

    /// Each unfinished snapshot attempt's upload expiry, `T_c` plus one day,
    /// for the time-probe schedule (§15, SP step 1).
    pub(crate) async fn snapshot_expiries(&self) -> Result<Vec<StorageTime>, SyncError> {
        let mut expiries = Vec::new();
        for record in self.database.operations().await? {
            if let Data::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        attempt: Some(attempt),
                        ..
                    },
                ..
            }) = Data::read(&record)?
            {
                expiries.extend(
                    attempt
                        .caught_at
                        .checked_add(std::time::Duration::from_secs(StorageTime::DAY_SECONDS)),
                );
            }
        }
        Ok(expiries)
    }

    /// Whether a snapshot sampled at `caught_at` can still count: no storage
    /// time this session observed is past `caught_at` plus one day (§15).
    fn may_count(&self, caught_at: StorageTime) -> bool {
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
            .is_none_or(|now| caught_at.within_day(now))
    }

    /// Whether a snapshot waits for this pass's fresh catch-up before it can
    /// be prepared: a ready write task that has not prepared its attempt, or
    /// a migration's capture whose raise snapshot no task has prepared yet,
    /// which this pass schedules.
    pub(crate) async fn preparation_waits(&self) -> Result<bool, SyncError> {
        let mut prepared = Vec::new();
        for record in self.database.operations().await? {
            let Data::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        audience, trigger, ..
                    },
                ..
            }) = Data::read(&record)?
            else {
                continue;
            };
            if record.last_step == 0 {
                if self.operation_ready(&record).await? {
                    return Ok(true);
                }
            } else if let SnapshotTrigger::Raise { version, .. } = trigger {
                prepared.push((audience, version));
            }
        }
        Ok(self
            .database
            .raise_captures()
            .await?
            .into_iter()
            .any(|capture| !prepared.contains(&(capture.audience, capture.version))))
    }

    /// The operation runner's drive outside a pass: run the catch-up a
    /// waiting preparation follows, sampled fresh, and keep its scope for
    /// the drive (§15). A failed catch-up is recorded as each waiting
    /// preparation's condition and returned with them.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn catch_up_outside_pass(
        &mut self,
        writes: &mut crate::DeviceLogSync,
    ) -> Result<
        Result<
            Option<crate::pass_reads::ReadScope>,
            (SyncError, Vec<(crate::OperationId, crate::PendingRecord)>),
        >,
        SyncError,
    > {
        if self.reads.write_catch_up().is_some() || !self.preparation_waits().await? {
            return Ok(Ok(None));
        }
        let scope = self.begin_pass(writes);
        let Err(error) = self.catch_up_writes(writes).await else {
            return Ok(Ok(Some(scope)));
        };
        let mut waiting = Vec::new();
        for record in self.database.operations().await? {
            if record.last_step == 0
                && matches!(
                    Data::read(&record)?,
                    Data::Snapshots(SnapshotTask {
                        job: SnapshotJob::Write { .. },
                        ..
                    })
                )
                && self.operation_ready(&record).await?
            {
                self.record_operation_error(&record, &error).await?;
                if let Some(pending) = self
                    .database
                    .operation_pending::<crate::PendingRecord>(record.id)
                    .await?
                {
                    waiting.push((record.id, pending));
                }
            }
        }
        Ok(Err((error, waiting)))
    }

    /// Outside a pass, run the catch-up a waiting preparation needs, sampled
    /// fresh, unless the caller's pass already completed one (§15).
    pub(super) async fn catch_up_for_snapshots(
        &mut self,
        writes: &mut crate::DeviceLogSync,
    ) -> Result<(), SyncError> {
        if self.reads.write_catch_up().is_none() && self.preparation_waits().await? {
            self.catch_up_writes(writes).await?;
        }
        Ok(())
    }

    /// §15's `T_c`: a fresh provider sample taken before a complete
    /// catch-up. An older sample could already be more than a day old, so
    /// a catch-up a preparation uses observes storage time first; the device
    /// clock never stands in for it.
    pub(crate) async fn catch_up_time(&self) -> Result<StorageTime, SyncError> {
        self.observe_clock(&self.database.local_store_log().await?)
            .await?;
        // T_c must precede this catch-up's reads, so the pass's earlier first
        // phase cannot serve here.
        self.scan_store_log(true).await?;
        Ok(self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .caught_up()
            .ok_or_else(|| {
                StorageFailure::Protocol.with_source("a complete catch-up recorded no storage time")
            })?)
    }

    /// Send the prepared attempt: abandoned if it can no longer count, held
    /// until every own write through its captured position is confirmed,
    /// and abandoned again if its confirmed landing came too late (§15).
    async fn send_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        attempt: SnapshotAttempt,
    ) -> Result<Progress, SyncError> {
        let SnapshotJob::Write {
            audience, device, ..
        } = task.job.clone()
        else {
            unreachable!("snapshot sending step")
        };
        let path = snapshot_path(
            self.directory.id(),
            &SnapshotId {
                audience,
                device,
                number: attempt.number.get(),
            },
        )?;
        let name = task
            .temporary
            .first()
            .ok_or(coven_database::DbError::Invariant {
                what: "an uploading snapshot has its prepared file",
            })?;
        let input = io::SnapshotInput::open(&self.directory, name)?;
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let prefix = self.check_snapshot_signer(&input).await?;
        // Stopping locally cannot stop an earlier request landing; readers
        // ignore it, and a new attempt takes a new path (§15).
        if !self.may_count(prefix.caught_at) {
            tracing::debug!(
                path = path.as_str(),
                "snapshot attempt expired before sending"
            );
            if let Some(session) = &attempt.session {
                storage.abort_upload(session).await?;
            }
            // An earlier run's request may still land; the record lets this
            // operation delete it if it does (§15). One never sent needs none.
            let abandoned = attempt.sent.then_some(AbandonedAttempt {
                number: attempt.number,
                caught_at: prefix.caught_at,
                landing: AbandonedLanding::Unconfirmed {
                    abandoned_at: self.observed_time()?,
                },
            });
            return self.abandon_snapshot(record, task, abandoned).await;
        }
        // Capture may precede own-upload confirmation, but the snapshot is
        // sent only once every own write it covers is stored, so a loader
        // never mistakes an unpublished part for deleted history (§15; io
        // `snapshot_covers_published`).
        if let Some(own) = prefix.writes.0.iter().find(|id| id.device == device) {
            if let Some(write) = self.database.unconfirmed_own_prefix(*own).await? {
                return Ok(Progress::Waiting(PendingReason::Waits(
                    Prerequisite::Object(crate::write_seal::path(self.directory.id(), write)),
                )));
            }
        }
        if !attempt.sent {
            set_attempt(
                &mut task,
                Some(SnapshotAttempt {
                    sent: true,
                    ..attempt.clone()
                }),
            );
            self.save_snapshot_task(record, &task, 1).await?;
        }
        let recorded = attempt.session.clone();
        let number = attempt.number;
        let mut transfer = SnapshotTransfer {
            sync: self,
            record,
            task: &mut task,
            input: &input,
        };
        match crate::recorded_upload::upload(
            storage,
            &path,
            input.size(),
            attempt.session,
            &mut transfer,
        )
        .await?
        {
            crate::recorded_upload::Uploaded::Occupied(expected) => {
                // A lost reply may have published this snapshot; another copy
                // of this device may also have used its number (§10).
                crate::identity::settle_occupied(storage, &self.send, &path, expected).await?;
                if let Some(session) = &recorded {
                    storage.abort_upload(session).await?;
                }
            }
            crate::recorded_upload::Uploaded::Stored => {}
            crate::recorded_upload::Uploaded::Paused => {
                unreachable!("snapshot uploads keep going")
            }
        }
        let stored = storage
            .status(&path)
            .await?
            .ok_or_else(|| StorageFailure::NotFound.with_source("confirmed snapshot is absent"))?;
        // Its landing time is a provider time this session now knows.
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .observe(&stored, self.clock.elapsed());
        if !prefix.counts(stored.stored_at) {
            tracing::debug!(path = path.as_str(), "snapshot landed too late to count");
            // Abandoned with its landing on record, so the next run deletes
            // it before preparing afresh (§15; io `late_deletion_unseen`).
            let abandoned = AbandonedAttempt {
                number,
                caught_at: prefix.caught_at,
                landing: AbandonedLanding::Late,
            };
            return self.abandon_snapshot(record, task, Some(abandoned)).await;
        }
        self.save_snapshot_task(record, &task, 2).await?;
        Ok(Progress::Advanced)
    }

    /// Give up an attempt that cannot count and prepare afresh after another
    /// catch-up, at a new path. One without a confirmed counting landing is
    /// recorded in the same transaction, so its landing is settled even after
    /// a crash (§15).
    async fn abandon_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        to_settle: Option<AbandonedAttempt>,
    ) -> Result<Progress, SyncError> {
        for name in &task.temporary {
            io::remove(&self.directory, name)?;
        }
        task.temporary.clear();
        set_attempt(&mut task, None);
        let SnapshotJob::Write { abandoned, .. } = &mut task.job else {
            unreachable!("only a snapshot write abandons attempts")
        };
        abandoned.extend(to_settle);
        self.database
            .advance_operation(Data::Snapshots(task).update(record, 0)?)
            .await?;
        Ok(Progress::Advanced)
    }

    /// The newest storage time this session observed, which abandonment
    /// needs: an attempt expires only against an observed time (§15).
    fn observed_time(&self) -> Result<StorageTime, SyncError> {
        Ok(self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
            .ok_or(coven_database::DbError::Invariant {
                what: "an expired snapshot attempt has an observed storage time",
            })?)
    }

    /// A snapshot's both signatures are fixed with its sealed file. Each
    /// request, first or retried, needs that signer to be this member's key
    /// in the snapshot's signed coverage and its current key now: current
    /// custody never sends a file signed under replaced keys (§10, D9).
    /// Returns the signed prefix, with its `T_c` and captured positions.
    async fn check_snapshot_signer(
        &self,
        input: &io::SnapshotInput,
    ) -> Result<SnapshotObjectPrefix, SyncError> {
        let member = self.operation_member()?;
        let local = self.database.local_store_log().await?;
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        let prefix = sealed_prefix(input)?;
        crate::object_author::check_signer(
            &member,
            crate::object_author::recorded_signer(&local.log, &prefix.store_log, &member.member()),
        )?;
        Ok(prefix)
    }

    async fn finish_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        id: SnapshotId,
    ) -> Result<Progress, SyncError> {
        // Every abandoned attempt is settled before the snapshot's work ends.
        if let Some(wait) = self.unsettled(&task)? {
            return Ok(wait);
        }
        if matches!(
            task.job,
            SnapshotJob::Write {
                trigger: SnapshotTrigger::Reset,
                ..
            }
        ) {
            self.clear_snapshot_files(record, &mut task).await?;
            let data = Data::Entry(crate::operation_data::EntryWork {
                intent: crate::operation_data::Intent::Reset { snapshot: id },
                entry: None,
            });
            self.database
                .advance_operation(data.update(record, 3)?)
                .await?;
            return Ok(Progress::Advanced);
        }
        // Confirmed upload ends publication. Retention derives what this
        // snapshot permits deleting from the catalog in a later pass step,
        // so its waits and failures never hold this operation (§18.1).
        self.clear_snapshot_files(record, &mut task).await?;
        self.database.finish_operation(record.id).await?;
        Ok(Progress::Finished(Output::Unit))
    }
}

/// The signed prefix of a prepared snapshot file, with its `T_c`.
fn sealed_prefix(input: &io::SnapshotInput) -> Result<SnapshotObjectPrefix, SyncError> {
    let bytes = input.read_at(
        0,
        (SnapshotObjectPrefix::MAX_SIGNED_PREFIX as u64).min(input.size()) as usize,
    )?;
    let length = SnapshotObjectPrefix::length(&bytes)?;
    let (prefix, _) = SnapshotObjectPrefix::decode_signed(
        bytes
            .get(..length + 64)
            .ok_or(coven_format::Error::Truncated)?,
    )?;
    Ok(prefix)
}

fn prepared_attempt() -> coven_database::DbError {
    coven_database::DbError::Invariant {
        what: "a snapshot write has three steps, each after the first with its attempt",
    }
}

fn set_attempt(task: &mut SnapshotTask, value: Option<SnapshotAttempt>) {
    let SnapshotJob::Write { attempt, .. } = &mut task.job else {
        unreachable!("only a snapshot write has an attempt")
    };
    *attempt = value;
}

impl SnapshotTask {
    /// The published identity of a prepared attempt.
    pub(crate) fn published(&self) -> Option<SnapshotId> {
        match &self.job {
            SnapshotJob::Write {
                audience,
                device,
                attempt: Some(attempt),
                ..
            } => Some(SnapshotId {
                audience: audience.clone(),
                device: *device,
                number: attempt.number.get(),
            }),
            _ => None,
        }
    }
}

struct SnapshotTransfer<'a> {
    sync: &'a StoreLogSync,
    record: &'a OperationRecord,
    task: &'a mut SnapshotTask,
    input: &'a io::SnapshotInput,
}
impl crate::recorded_upload::UploadSource for SnapshotTransfer<'_> {
    type Error = SyncError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, SyncError> {
        self.input.read_at(offset, length)
    }
    async fn save(&mut self, session: coven_storage::UploadSession) -> Result<(), SyncError> {
        let SnapshotJob::Write {
            attempt: Some(attempt),
            ..
        } = &mut self.task.job
        else {
            unreachable!("a sending snapshot has its attempt")
        };
        attempt.session = Some(session);
        self.sync
            .save_snapshot_task(self.record, self.task, 1)
            .await
    }
    fn keep_going(&self) -> bool {
        true
    }
}

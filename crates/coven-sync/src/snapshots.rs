//! Durable snapshot writing, reload and retention, invoked without a sync loop.

use super::StoreLogSync;
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    snapshot_data::*,
    SyncError,
};
use coven_database::{OperationRecord, StoreLog};
use coven_format::{store_log::SnapshotId, value::WritePositions};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

#[path = "snapshot_boundaries.rs"]
mod boundaries;
#[path = "snapshot_catalog.rs"]
pub(super) mod catalog;
#[path = "snapshot_file_retention.rs"]
mod file_retention;
#[path = "snapshot_io.rs"]
mod io;
#[path = "snapshot_reload.rs"]
mod reload;
#[path = "snapshot_retention.rs"]
mod retention;
#[path = "snapshot_return.rs"]
mod return_check;
#[path = "snapshot_seal.rs"]
mod seal;
#[path = "snapshot_write_input.rs"]
mod write_input;

impl StoreLogSync {
    /// Resume retained snapshot work, then write each readable audience whose
    /// encoded parts since its latest signed snapshot prefix exceed the listed
    /// plaintext size, or 1 MiB when it has none (§15). Uploads always use the
    /// recorded disk bytes.
    pub async fn write_snapshots(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        self.resume_snapshots().await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        for audience in self.snapshot_audiences(&local.log)? {
            let record = self
                .start_snapshot_task(SnapshotJob::Write {
                    audience,
                    device: local.device,
                    trigger: SnapshotTrigger::Growth,
                    session: None,
                })
                .await?;
            self.drive_snapshot(record.id).await?;
        }
        Ok(())
    }

    /// Apply the writes that follow a reload's target by next-number discovery
    /// from the loaded positions (§15: a reload stops at its target). Joining,
    /// restore and recovery call this after loading snapshots.
    pub async fn download_later_writes(
        &mut self,
        writes: &mut crate::DeviceLogSync,
    ) -> Result<(), SyncError> {
        let _pass = self.begin_pass(writes);
        writes.download_writes().await?;
        Ok(())
    }

    /// Reload all readable audiences from validated snapshots and authenticated
    /// device writes, preserving queued plaintext, write numbers and key ids (§15).
    /// Joining, missing-history recovery and explicit recovery call this method.
    /// A snapshot that fails its checks is its own pending record; while it
    /// is its audience's current newest, the reload waits on it (§19.1). A
    /// required write this device refused makes the reload wait on that write
    /// until a snapshot covering it lands (§15).
    pub async fn reload_from_snapshots(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        self.resume_snapshots().await?;
        let record = self
            .start_snapshot_task(SnapshotJob::Reload {
                scope: crate::snapshot_data::ReloadScope::All,
                files: None,
            })
            .await?;
        self.drive_snapshot(record.id).await?;
        Ok(())
    }

    /// Delete covered logs, this device's superseded snapshots, and eligible
    /// unreferenced uploaded files under §§15 and 16.5. No timer is installed.
    pub async fn run_retention(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        self.resume_snapshots().await?;
        let record = self.start_snapshot_task(SnapshotJob::Retain).await?;
        self.drive_snapshot(record.id).await?;
        Ok(())
    }

    /// Write and publish a requested audience snapshot regardless of growth.
    /// Reset and schema-change operations record the returned identity only
    /// after this durable publication has completed. A snapshot waiting for
    /// its audience's key stays journaled and returns that pending condition.
    pub async fn write_snapshot(&mut self, audience: Audience) -> Result<SnapshotId, SyncError> {
        let _reads = self.reads.enter();
        self.resume_snapshots().await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        if !self.snapshot_audiences(&local.log)?.contains(&audience) {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        // A snapshot is new data in its audience: replace an exposed key first.
        self.rotate_if_exposed(audience.clone(), &member.member())
            .await?;
        let record = self
            .start_snapshot_task(SnapshotJob::Write {
                audience: audience.clone(),
                device: local.device,
                trigger: SnapshotTrigger::Requested,
                session: None,
            })
            .await?;
        if !self.drive_snapshot(record.id).await? {
            let pending = self
                .database
                .operation_pending::<crate::PendingRecord>(record.id)
                .await?
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            return Err(SyncError::Pending(Box::new(pending)));
        }
        Ok(SnapshotId {
            audience,
            device: local.device,
            number: record
                .id
                .0
                .try_into()
                .map_err(|_| coven_database::DbError::DamagedDatabase)?,
        })
    }

    pub(super) async fn start_snapshot_task(
        &self,
        job: SnapshotJob,
    ) -> Result<OperationRecord, SyncError> {
        for record in self.database.operations().await? {
            if record.started_by == "coven" {
                if let Data::Snapshots(task) = Data::read(&record)? {
                    if task.job.same_work(&job) {
                        return Ok(record);
                    }
                }
            }
        }
        let data = Data::Snapshots(SnapshotTask {
            job,
            temporary: Vec::new(),
        });
        let id = self
            .database
            .start_operation(data.new_operation("coven")?)
            .await?;
        self.snapshot_record(id).await
    }

    pub(super) async fn resume_snapshots(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        loop {
            let mut work = Vec::new();
            for record in self.database.operations().await? {
                if let Data::Snapshots(task) = Data::read(&record)? {
                    if self.operation_can_retry(&record).await?
                        && !matches!(
                            task.job,
                            SnapshotJob::Write {
                                trigger: SnapshotTrigger::Reset,
                                ..
                            }
                        )
                    {
                        work.push((record, task.job));
                    }
                }
            }
            work.sort_by_key(|(_, job)| match job {
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Raise { .. },
                    ..
                } => 0,
                SnapshotJob::Reload { .. } => 1,
                _ => 2,
            });
            let mut advanced = false;
            for (record, job) in work {
                if matches!(
                    job,
                    SnapshotJob::Write {
                        trigger: SnapshotTrigger::Growth | SnapshotTrigger::Requested,
                        ..
                    }
                ) && self.pending_reload().await?.is_some()
                {
                    continue;
                }
                advanced |= self.drive_snapshot(record.id).await?;
            }
            if !advanced {
                return Ok(());
            }
        }
    }

    pub(crate) async fn pending_reload(&self) -> Result<Option<crate::OperationId>, SyncError> {
        self.database
            .first_operation(Data::RELOAD_KIND)
            .await?
            .map(|record| Data::read(&record).map(|_| record.id))
            .transpose()
    }

    pub(super) async fn discard_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
    ) -> Result<(), SyncError> {
        if let SnapshotJob::Write {
            session: Some(session),
            ..
        } = &task.job
        {
            self.storage
                .as_deref()
                .ok_or(SyncError::NoStorage)?
                .abort_upload(session)
                .await?;
        }
        self.clear_snapshot_files(record, &mut task).await
    }

    async fn snapshot_record(&self, id: crate::OperationId) -> Result<OperationRecord, SyncError> {
        self.database
            .operation(id)
            .await?
            .ok_or_else(|| coven_database::DbError::OperationChanged(id).into())
    }

    async fn drive_snapshot(&mut self, id: crate::OperationId) -> Result<bool, SyncError> {
        loop {
            let record = self.snapshot_record(id).await?;
            if !self.operation_can_retry(&record).await? {
                let pending = self
                    .database
                    .operation_pending::<crate::PendingRecord>(id)
                    .await?
                    .ok_or(coven_database::DbError::DamagedDatabase)?;
                return Err(SyncError::Pending(Box::new(pending)));
            }
            let data = Data::read(&record)?;
            let Data::Snapshots(task) = data else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            let progress = match self.snapshot_step(&record, task).await {
                Ok(progress) => progress,
                Err(error) => {
                    self.record_operation_error(&record, &error).await?;
                    return Err(error);
                }
            };
            match progress {
                Progress::Finished(_) => return Ok(true),
                Progress::Waiting(reason) => {
                    self.record_operation_condition(&record, reason).await?;
                    return Ok(false);
                }
                Progress::Advanced => (),
                _ => unreachable!("snapshot steps complete or return a typed failure"),
            }
        }
    }

    pub(crate) async fn snapshot_step(
        &mut self,
        record: &OperationRecord,
        mut task: SnapshotTask,
    ) -> Result<Progress, SyncError> {
        let _reads = self.reads.enter();
        match &task.job {
            SnapshotJob::Write { .. } => self.write_snapshot_step(record, task).await,
            SnapshotJob::Reload { .. } => self.reload_snapshot_step(record, task).await,
            SnapshotJob::Retain => {
                self.clear_snapshot_files(record, &mut task).await?;
                self.retain_snapshot_objects().await?;
                self.database.finish_operation(record.id).await?;
                Ok(Progress::Finished(Output::Unit))
            }
        }
    }

    async fn write_snapshot_step(
        &mut self,
        record: &OperationRecord,
        task: SnapshotTask,
    ) -> Result<Progress, SyncError> {
        let SnapshotJob::Write {
            audience,
            device,
            trigger,
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
        let id = SnapshotId {
            audience: audience.clone(),
            device,
            number: record
                .id
                .0
                .try_into()
                .map_err(|_| coven_database::DbError::DamagedDatabase)?,
        };
        let path = snapshot_path(self.directory.id(), &id)?;
        if let SnapshotTrigger::Raise { version, ref entry } = trigger {
            if entry.is_none()
                && super::schema_sync::schema_in_place(&local.log.replay.state, &audience, version)
            {
                self.discard_snapshot(record, task).await?;
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
            if record.last_step >= 2 {
                return self.raise_snapshot_step(record, task, version, id).await;
            }
        }
        match record.last_step {
            0 => {
                self.prepare_snapshot(record, task, &path, &local.log, &member)
                    .await
            }
            1 => self.upload_snapshot(record, task, &path).await,
            2 => self.finish_snapshot(record, task, id).await,
            _ => Err(coven_database::DbError::DamagedDatabase.into()),
        }
    }

    async fn prepare_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        path: &ObjectPath,
        log: &StoreLog,
        member: &coven_crypto::MemberIdentity,
    ) -> Result<Progress, SyncError> {
        let id = path.snapshot_id().expect("snapshot path");
        self.clear_snapshot_files(record, &mut task).await?;
        if matches!(
            task.job,
            SnapshotJob::Write {
                trigger: SnapshotTrigger::Growth,
                ..
            }
        ) && !self.snapshot_grew(&id.audience, log).await?
        {
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        if !self.snapshot_audiences(log)?.contains(&id.audience) {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        let ring = self.store_keys.read()?.ok_or_else(|| {
            SyncError::KeyUnavailable(
                crate::key_introductions::newest(log, &id.audience)
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
            &id.audience,
        ) {
            Ok(key) => key,
            Err(SyncError::KeyUnavailable(key)) => {
                return Ok(Progress::Waiting(crate::PendingReason::KeyUnavailable {
                    audience: id.audience.clone(),
                    key,
                }))
            }
            Err(error) => return Err(error),
        };
        let key = io::key(&ring, &id.audience, key_id)?;
        let caught_at = self.catch_up_time().await?;
        let name = self
            .reserve_snapshot_files(record, &mut task, 1)
            .await?
            .remove(0);
        self.seal_snapshot(id, key_id, caught_at, key, member, path, &name)
            .await?;
        self.save_snapshot_task(record, &task, 1).await?;
        Ok(Progress::Advanced)
    }

    /// §15's `T_c`: a fresh provider sample taken before a complete
    /// catch-up. An older sample could already be more than a day old, so
    /// every preparation observes storage time first; the device clock never
    /// stands in for it.
    async fn catch_up_time(&self) -> Result<coven_format::value::StorageTime, SyncError> {
        self.observe_clock(&self.database.local_store_log().await?)
            .await?;
        // T_c must precede this catch-up's reads, so the pass's earlier first
        // phase cannot serve here.
        self.scan_store_log(true).await?;
        let sample = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .caught_up()
            .ok_or_else(|| {
                coven_storage::StorageFailure::Protocol
                    .with_source("a complete catch-up recorded no storage time")
            })?;
        Ok(coven_format::value::StorageTime::from_system_time(sample)?)
    }

    async fn snapshot_grew(&self, audience: &Audience, log: &StoreLog) -> Result<bool, SyncError> {
        let candidates = self.current_snapshot_candidates(audience, log).await?;
        let (positions, threshold) = match candidates.candidates.first() {
            Some(latest) => (
                latest.prefix.writes.clone(),
                crate::write_object::checked(
                    &latest.object.path,
                    latest.prefix.plaintext_length(latest.object.size),
                )?,
            ),
            None => (WritePositions(Vec::new()), 1024 * 1024),
        };
        let state = self.database.sync_state(Vec::new()).await?;
        let mut stuck = crate::pending_logs::local_refusals(&self.database).await?;
        let waiting = self.database.waiting_snapshot_headers().await?;
        let waiting_ids: std::collections::BTreeSet<_> =
            waiting.iter().map(|h| h.header.position).collect();
        let mut bytes: u128 = waiting
            .iter()
            .filter(|h| !positions.covers(h.header.position))
            .flat_map(|h| &h.parts)
            .filter(|p| &p.audience == audience)
            .map(|p| u128::from(p.plaintext_length))
            .sum();
        for object in self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::device_logs(self.directory.id()))
            .await?
        {
            let write = object
                .path
                .write_id()
                .ok_or_else(|| catalog::inconsistent("device listing has another path layout"))?;
            if positions.covers(write)
                || !state.positions.covers(write)
                || waiting_ids.contains(&write)
                || stuck
                    .iter()
                    .any(|record| record.blocks(coven_database::LogObject::Write(write)))
            {
                continue;
            }
            let header = match self.open_write_header(&object).await {
                Ok(header) => header,
                Err(SyncError::Damaged(damage)) => {
                    stuck.push(coven_database::LogRefusal {
                        object: coven_database::LogObject::Write(write),
                        failure: (&damage.failure).into(),
                    });
                    continue;
                }
                Err(error) => return Err(error),
            };
            bytes += header
                .parts
                .iter()
                .filter(|p| &p.audience == audience)
                .map(|p| u128::from(p.plaintext_length))
                .sum::<u128>();
        }
        Ok(bytes > u128::from(threshold))
    }

    async fn upload_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        path: &ObjectPath,
    ) -> Result<Progress, SyncError> {
        let SnapshotJob::Write { session, .. } = task.job.clone() else {
            unreachable!("snapshot upload step")
        };
        let name = task
            .temporary
            .first()
            .ok_or(coven_database::DbError::DamagedDatabase)?;
        let input = io::SnapshotInput::open(&self.directory, name)?;
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let recorded = session.clone();
        let mut transfer = SnapshotTransfer {
            sync: self,
            record,
            task: &mut task,
            input: &input,
        };
        if let crate::recorded_upload::Uploaded::Occupied(expected) =
            crate::recorded_upload::upload(storage, path, input.size(), session, &mut transfer)
                .await?
        {
            // A lost reply may have published this snapshot; another copy of
            // this device may also have used its number (§10).
            crate::identity::settle_occupied(storage, &self.send, path, expected).await?;
            if let Some(session) = &recorded {
                storage.abort_upload(session).await?;
            }
        }
        self.save_snapshot_task(record, &task, 2).await?;
        Ok(Progress::Advanced)
    }

    async fn finish_snapshot(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        id: SnapshotId,
    ) -> Result<Progress, SyncError> {
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
        self.retain_snapshot_objects().await?;
        self.clear_snapshot_files(record, &mut task).await?;
        self.database.finish_operation(record.id).await?;
        Ok(Progress::Finished(Output::Unit))
    }

    pub(super) fn snapshot_audiences(
        &self,
        log: &StoreLog,
    ) -> Result<BTreeSet<Audience>, SyncError> {
        audiences_for_member(log, &self.operation_member()?.member())
    }

    pub(super) async fn save_snapshot_task(
        &self,
        record: &OperationRecord,
        task: &SnapshotTask,
        step: u32,
    ) -> Result<(), SyncError> {
        self.database
            .advance_operation(Data::Snapshots(task.clone()).update(record, step)?)
            .await?;
        Ok(())
    }

    /// Reserve a download batch before creating any file. A reload records its
    /// candidate names and log-part names once each, independent of log count.
    pub(super) async fn reserve_snapshot_files(
        &self,
        record: &OperationRecord,
        task: &mut SnapshotTask,
        count: usize,
    ) -> Result<Vec<String>, SyncError> {
        let names: Vec<_> = (task.temporary.len()..task.temporary.len() + count)
            .map(|index| format!("snapshot-{}-{index}", record.id.0))
            .collect();
        task.temporary.extend(names.iter().cloned());
        if count > 0 {
            self.save_snapshot_task(record, task, record.last_step)
                .await?;
        }
        Ok(names)
    }

    pub(super) async fn clear_snapshot_files(
        &self,
        record: &OperationRecord,
        task: &mut SnapshotTask,
    ) -> Result<(), SyncError> {
        for name in &task.temporary {
            io::remove(&self.directory, name)?;
        }
        task.temporary.clear();
        self.save_snapshot_task(record, task, record.last_step)
            .await
    }
}

/// The audiences a member can read and snapshot (§15). Which key seals each
/// snapshot is chosen when sealing, under §11.
pub(super) fn audiences_for_member(
    log: &StoreLog,
    member: &coven_foundation::id_source::MemberId,
) -> Result<BTreeSet<Audience>, SyncError> {
    let state = &log.replay.state;
    if state.members.get(member).is_none_or(|m| m.removed) {
        return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
    }
    let mut audiences = BTreeSet::new();
    if state.store.is_some() {
        audiences.insert(Audience::Store);
    }
    audiences.extend(
        state
            .readable_circles(member)
            .into_iter()
            .map(Audience::Circle),
    );
    Ok(audiences)
}

pub(super) fn snapshot_path(
    store: coven_foundation::id_source::StoreId,
    id: &SnapshotId,
) -> Result<ObjectPath, SyncError> {
    Ok(ObjectPath::snapshot(
        store,
        id.audience.clone(),
        id.device,
        NonZeroU64::new(id.number).ok_or(coven_database::DbError::DamagedDatabase)?,
    ))
}

pub(super) fn include_positions<'a>(
    positions: &mut BTreeMap<DeviceId, u64>,
    writes: impl IntoIterator<Item = &'a coven_merge::WriteId>,
) {
    for write in writes {
        positions
            .entry(write.device)
            .and_modify(|number| *number = (*number).max(write.number))
            .or_insert(write.number);
    }
}

pub(super) fn point(positions: &WritePositions, device: DeviceId) -> u64 {
    positions
        .0
        .iter()
        .find(|id| id.device == device)
        .map_or(0, |id| id.number)
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
            session: recorded, ..
        } = &mut self.task.job
        else {
            unreachable!()
        };
        *recorded = Some(session);
        self.sync
            .save_snapshot_task(self.record, self.task, 1)
            .await
    }
    fn keep_going(&self) -> bool {
        true
    }
}

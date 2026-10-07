//! Durable snapshot writing, reload and retention, invoked without a sync loop.

use super::StoreLogSync;
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    snapshot_data::*,
    SyncError, SyncResults,
};
use coven_database::{OperationRecord, StoreLog};
use coven_format::{
    sealed_snapshot::SnapshotObjectPrefix, store_log::SnapshotId, value::WritePositions,
};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix};
use std::collections::BTreeMap;
use std::num::NonZeroU64;

#[path = "snapshot_boundaries.rs"]
mod boundaries;
#[path = "snapshot_catalog.rs"]
mod catalog;
#[path = "snapshot_file_retention.rs"]
mod file_retention;
#[path = "snapshot_io.rs"]
mod io;
#[path = "snapshot_reload.rs"]
mod reload;
#[path = "snapshot_retention.rs"]
mod retention;
#[path = "snapshot_seal.rs"]
mod seal;
#[path = "snapshot_write_input.rs"]
mod write_input;

impl StoreLogSync {
    /// A signed snapshot prefix may require a reload, but never authorizes rows.
    /// The reload authenticates the complete snapshot and its intervening writes
    /// before its atomic database replacement (§15, §19.1).
    pub(crate) async fn reload_deleted_history(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = SyncResults::default();
        let local = self.database.local_store_log().await?;
        let state = self.database.sync_state(Vec::new()).await?;
        let listed: std::collections::BTreeSet<_> = self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::device_logs())
            .await?
            .into_iter()
            .filter_map(|object| object.path.write_id())
            .collect();
        for audience in self.snapshot_audiences(&local.log)?.into_keys() {
            let catalog = self
                .current_snapshot_candidates(&audience, &local.log, &mut report)
                .await?;
            for covered in &catalog.required_positions.0 {
                let after = point(&state.positions, covered.device);
                if covered.number > after {
                    let present = listed
                        .iter()
                        .filter(|write| {
                            write.device == covered.device
                                && write.number > after
                                && write.number <= covered.number
                        })
                        .count() as u64;
                    if present < covered.number - after {
                        report.append(self.reload_from_snapshots().await?);
                        return Ok(report);
                    }
                }
            }
        }
        Ok(report)
    }

    /// Resume retained snapshot work, then write each readable audience whose
    /// encoded parts since its latest valid snapshot exceed that snapshot's
    /// plaintext size, or 1 MiB when it has none (§15). Returns damaged objects
    /// passed over during selection. Uploads always use the recorded disk bytes.
    pub async fn write_snapshots(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = self.resume_snapshots().await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        for audience in self.snapshot_audiences(&local.log)?.into_keys() {
            let record = self
                .start_snapshot_task(SnapshotJob::Write {
                    audience,
                    device: local.device,
                    trigger: SnapshotTrigger::Growth,
                    session: None,
                })
                .await?;
            self.drive_snapshot(record.id, &mut report).await?;
        }
        Ok(report)
    }

    /// Reload all readable audiences from validated snapshots and authenticated
    /// device writes, keeping queued local write numbers and sealed bytes (§15).
    /// Joining, missing-history recovery and explicit recovery call this method.
    pub async fn reload_from_snapshots(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = self.resume_snapshots().await?;
        let record = self
            .start_snapshot_task(SnapshotJob::Reload {
                scope: crate::snapshot_data::ReloadScope::All,
                files: None,
            })
            .await?;
        self.drive_snapshot(record.id, &mut report).await?;
        Ok(report)
    }

    /// Delete covered logs, this device's superseded snapshots, and eligible
    /// unreferenced uploaded files under §§15 and 16.5. No timer is installed.
    pub async fn run_retention(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = self.resume_snapshots().await?;
        let record = self.start_snapshot_task(SnapshotJob::Retain).await?;
        self.drive_snapshot(record.id, &mut report).await?;
        Ok(report)
    }

    /// Write and publish a requested audience snapshot regardless of growth.
    /// Reset and schema-change operations record the returned identity only
    /// after this durable publication has completed.
    pub async fn write_snapshot(&mut self, audience: Audience) -> Result<SnapshotId, SyncError> {
        self.resume_snapshots().await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        if !self.snapshot_audiences(&local.log)?.contains_key(&audience) {
            return Err(SyncError::PermissionDenied);
        }
        let record = self
            .start_snapshot_task(SnapshotJob::Write {
                audience: audience.clone(),
                device: local.device,
                trigger: SnapshotTrigger::Requested,
                session: None,
            })
            .await?;
        self.drive_snapshot(record.id, &mut SyncResults::default())
            .await?;
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

    pub(super) async fn resume_snapshots(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = SyncResults::default();
        loop {
            let mut work = Vec::new();
            for record in self.database.operations().await? {
                if let Data::Snapshots(task) = Data::read(&record)? {
                    if record.failure.is_none()
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
                advanced |= self.drive_snapshot(record.id, &mut report).await?;
            }
            if !advanced {
                break;
            }
        }
        Ok(report)
    }

    pub(crate) async fn pending_reload(&self) -> Result<Option<crate::OperationId>, SyncError> {
        for record in self.database.operations().await? {
            if matches!(
                Data::read(&record)?,
                Data::Snapshots(SnapshotTask {
                    job: SnapshotJob::Reload { .. },
                    ..
                })
            ) {
                return Ok(Some(record.id));
            }
        }
        Ok(None)
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
            .operations()
            .await?
            .into_iter()
            .find(|r| r.id == id)
            .ok_or_else(|| coven_database::DbError::OperationChanged(id).into())
    }

    async fn drive_snapshot(
        &mut self,
        id: crate::OperationId,
        report: &mut SyncResults,
    ) -> Result<bool, SyncError> {
        loop {
            let record = self.snapshot_record(id).await?;
            let Data::Snapshots(task) = Data::read(&record)? else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            match self.snapshot_step(&record, task, report).await? {
                Progress::Finished(_) => return Ok(true),
                Progress::Waiting => return Ok(false),
                Progress::Advanced => (),
                _ => unreachable!("snapshot steps complete or return a typed failure"),
            }
        }
    }

    pub(crate) async fn snapshot_step(
        &mut self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        report: &mut SyncResults,
    ) -> Result<Progress, SyncError> {
        match task.job.clone() {
            SnapshotJob::Write {
                audience,
                device,
                trigger,
                session,
            } => {
                if !matches!(trigger, SnapshotTrigger::Raise { .. }) {
                    if let Some(id) = self.pending_reload().await? {
                        return Err(SyncError::ReloadPending(id));
                    }
                }
                let local = self.database.local_store_log().await?;
                let member = self.operation_member()?;
                self.check_stopped(&local, &member)?;
                if matches!(trigger, SnapshotTrigger::Reset) {
                    self.require_reset(&local.log.replay.state, &audience, &member.member_id())?;
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
                let path = snapshot_path(&id)?;
                if let SnapshotTrigger::Raise { version, ref entry } = trigger {
                    if entry.is_none() && version.in_place(&local.log.replay.state, &audience) {
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
                        self.clear_snapshot_files(record, &mut task).await?;
                        if matches!(trigger, SnapshotTrigger::Growth) {
                            let candidates = self
                                .current_snapshot_candidates(&audience, &local.log, report)
                                .await?;
                            let latest = self
                                .choose_snapshot(candidates.candidates, record, &mut task, report)
                                .await?;
                            let (positions, threshold) = match latest {
                                Some(latest) => (
                                    SnapshotObjectPrefix::decode(&latest.prefix)?.writes,
                                    io::SnapshotInput::open(&self.directory, &latest.file)?.size(),
                                ),
                                None => (WritePositions(Vec::new()), 1024 * 1024),
                            };
                            let state = self.database.sync_state(Vec::new()).await?;
                            let waiting = self.database.waiting_snapshot_headers().await?;
                            let waiting_ids: std::collections::BTreeSet<_> =
                                waiting.iter().map(|h| h.header.position).collect();
                            let mut bytes: u128 = waiting
                                .iter()
                                .filter(|h| !positions.covers(h.header.position))
                                .flat_map(|h| &h.parts)
                                .filter(|p| p.audience == audience)
                                .map(|p| u128::from(p.plaintext_length))
                                .sum();
                            for object in self
                                .storage
                                .as_deref()
                                .ok_or(SyncError::NoStorage)?
                                .list(&ObjectPrefix::device_logs())
                                .await?
                            {
                                let write = object.path.write_id().ok_or_else(|| {
                                    catalog::inconsistent("device listing has another path layout")
                                })?;
                                if positions.covers(write)
                                    || !state.positions.covers(write)
                                    || waiting_ids.contains(&write)
                                {
                                    continue;
                                }
                                let header = self.open_write_header(&object).await?;
                                bytes += header
                                    .parts
                                    .iter()
                                    .filter(|p| p.audience == audience)
                                    .map(|p| u128::from(p.plaintext_length))
                                    .sum::<u128>();
                            }
                            self.clear_snapshot_files(record, &mut task).await?;
                            if bytes <= u128::from(threshold) {
                                self.database.finish_operation(record.id).await?;
                                return Ok(Progress::Finished(Output::Unit));
                            }
                        }
                        let current = self.snapshot_audiences(&local.log)?;
                        let key_id = *current.get(&audience).ok_or(SyncError::PermissionDenied)?;
                        let ring = self
                            .store_keys
                            .unlock()?
                            .ok_or(SyncError::KeyUnavailable(key_id))?;
                        let key = io::key(&ring, &audience, key_id)?;
                        let name = self.reserve_snapshot_file(record, &mut task).await?;
                        self.seal_snapshot(id, key_id, key, &member, &path, &name)
                            .await?;
                        self.save_snapshot_task(record, &task, 1).await?;
                    }
                    1 => {
                        let name = task
                            .temporary
                            .first()
                            .ok_or(coven_database::DbError::DamagedDatabase)?;
                        let input = io::SnapshotInput::open(&self.directory, name)?;
                        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
                        if storage
                            .list(&ObjectPrefix::audience_snapshots(&audience))
                            .await?
                            .iter()
                            .any(|o| o.path == path)
                        {
                            if let Some(session) = &session {
                                storage.abort_upload(session).await?;
                            }
                        } else {
                            crate::recorded_upload::upload(
                                storage,
                                &path,
                                input.size(),
                                session,
                                &mut SnapshotTransfer {
                                    sync: self,
                                    record,
                                    task: &mut task,
                                    input: &input,
                                },
                            )
                            .await?;
                        }
                        self.save_snapshot_task(record, &task, 2).await?;
                    }
                    2 => {
                        if matches!(trigger, SnapshotTrigger::Reset) {
                            self.clear_snapshot_files(record, &mut task).await?;
                            let data = Data::Entry(crate::operation_data::EntryWork {
                                intent: crate::operation_data::Intent::Reset { snapshot: id },
                                entry: None,
                                removal: None,
                            });
                            self.database
                                .advance_operation(data.update(record, 3)?)
                                .await?;
                            return Ok(Progress::Advanced);
                        }
                        self.retain_snapshot_objects(record, &mut task, report)
                            .await?;
                        self.clear_snapshot_files(record, &mut task).await?;
                        self.database.finish_operation(record.id).await?;
                        return Ok(Progress::Finished(Output::Unit));
                    }
                    _ => return Err(coven_database::DbError::DamagedDatabase.into()),
                }
            }
            SnapshotJob::Reload { .. } => {
                return self.reload_snapshot_step(record, task, report).await
            }
            SnapshotJob::Retain => {
                self.clear_snapshot_files(record, &mut task).await?;
                self.retain_snapshot_objects(record, &mut task, report)
                    .await?;
                self.clear_snapshot_files(record, &mut task).await?;
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Unit));
            }
        }
        Ok(Progress::Advanced)
    }

    pub(super) fn snapshot_audiences(
        &self,
        log: &StoreLog,
    ) -> Result<BTreeMap<Audience, KeyId>, SyncError> {
        audiences_for_member(log, &self.operation_member()?.member_id())
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

    pub(super) async fn reserve_snapshot_file(
        &self,
        record: &OperationRecord,
        task: &mut SnapshotTask,
    ) -> Result<String, SyncError> {
        let name = format!("snapshot-{}-{}", record.id.0, task.temporary.len());
        task.temporary.push(name.clone());
        self.save_snapshot_task(record, task, record.last_step)
            .await?;
        Ok(name)
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

pub(super) fn audiences_for_member(
    log: &StoreLog,
    member: &coven_crypto::MemberId,
) -> Result<BTreeMap<Audience, KeyId>, SyncError> {
    let state = &log.replay.state;
    let mut audiences = BTreeMap::new();
    if state.members.get(member).is_none_or(|m| m.removed) {
        return Err(SyncError::PermissionDenied);
    }
    if let Some(store) = &state.store {
        audiences.insert(Audience::Store, store.key);
    }
    for (id, circle) in &state.circles {
        if !circle.deleted && circle.members.contains(member) {
            audiences.insert(Audience::Circle(*id), circle.key);
        }
    }
    Ok(audiences)
}

pub(super) fn snapshot_path(id: &SnapshotId) -> Result<ObjectPath, SyncError> {
    Ok(ObjectPath::snapshot(
        id.audience.clone(),
        id.device,
        NonZeroU64::new(id.number).ok_or(coven_database::DbError::DamagedDatabase)?,
    ))
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

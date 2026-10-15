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
pub(super) mod boundaries;
#[path = "snapshot_catalog.rs"]
pub(super) mod catalog;
#[path = "snapshot_file_retention.rs"]
mod file_retention;
#[path = "snapshot_file_scan.rs"]
mod file_scan;
#[path = "snapshot_io.rs"]
mod io;
#[path = "snapshot_prefix.rs"]
mod prefix;
#[path = "snapshot_publish.rs"]
mod publish;
#[path = "snapshot_reload.rs"]
mod reload;
#[path = "snapshot_retention.rs"]
mod retention;
#[path = "snapshot_return.rs"]
mod return_check;
#[path = "snapshot_seal.rs"]
mod seal;
#[path = "snapshot_verify.rs"]
mod verify;
#[path = "snapshot_write_input.rs"]
mod write_input;

/// How far driving one snapshot task got.
pub(crate) enum Driven {
    /// It finished, publishing this snapshot if it prepared one.
    Finished(Option<SnapshotId>),
    /// It recorded the condition it waits for.
    Waiting,
    /// A reset's snapshot is published; its operation continues with the
    /// entry that names it (SP step 5).
    Entry,
}

impl StoreLogSync {
    /// SP step 8: request a snapshot of each readable audience whose encoded
    /// parts since its latest signed snapshot prefix exceed the listed
    /// plaintext size, or 1 MiB when it has none (§15), then drive retained
    /// snapshot work. Every preparation follows this pass's completed
    /// catch-up, sampled fresh because a preparation was waiting for it; a
    /// pass that cannot supply one leaves the preparation pending for the
    /// next. Returns whether a reset's snapshot was published, so its entry
    /// can follow. Uploads always use the recorded disk bytes.
    pub async fn write_snapshots(&mut self) -> Result<bool, SyncError> {
        let _reads = self.reads.enter();
        let mut handed = self.resume_snapshots().await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        let tasks = self.database.operations().await?;
        let mut started = false;
        for audience in self.snapshot_audiences(&local.log)? {
            let job = SnapshotJob::Write {
                audience: audience.clone(),
                device: local.device,
                trigger: SnapshotTrigger::Growth,
                attempt: None,
            };
            let mut requested = false;
            for record in &tasks {
                if let Data::Snapshots(task) = Data::read(record)? {
                    requested |= record.started_by == "coven" && task.job.same_work(&job);
                }
            }
            // A task is created only for an audience that needs a snapshot.
            if !requested && self.snapshot_grew(&audience, &local.log).await? {
                self.start_snapshot_task(job).await?;
                started = true;
            }
        }
        if started {
            handed |= self.resume_snapshots().await?;
        }
        Ok(handed)
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

    /// Write and publish a requested audience snapshot regardless of growth,
    /// after a catch-up of `writes` sampled fresh (§15). A snapshot waiting
    /// for its audience's key, a catch-up or its own uploads stays journaled
    /// and returns that pending condition.
    pub async fn write_snapshot(
        &mut self,
        writes: &mut crate::DeviceLogSync,
        audience: Audience,
    ) -> Result<SnapshotId, SyncError> {
        let _pass = self.begin_pass(writes);
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
                audience,
                device: local.device,
                trigger: SnapshotTrigger::Requested,
                attempt: None,
            })
            .await?;
        self.catch_up_for_snapshots(writes).await?;
        match self.drive_snapshot(record.id).await? {
            Driven::Finished(Some(id)) => Ok(id),
            Driven::Waiting => {
                let pending = self
                    .database
                    .operation_pending::<crate::PendingRecord>(record.id)
                    .await?
                    .ok_or(coven_database::DbError::Invariant {
                        what: "an operation not ready has its condition",
                    })?;
                Err(SyncError::Pending(Box::new(pending)))
            }
            Driven::Finished(None) | Driven::Entry => Err(coven_database::DbError::Invariant {
                what: "a requested snapshot publishes or waits",
            }
            .into()),
        }
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

    /// Drive every ready snapshot task, raises first. A reset's snapshot
    /// steps run here too, after this pass's catch-up, and wait while a raise
    /// is pending as its entry would (SP steps 5 and 8). Returns whether a
    /// reset's snapshot was published, so its entry can follow.
    pub(super) async fn resume_snapshots(&mut self) -> Result<bool, SyncError> {
        let _reads = self.reads.enter();
        let mut handed = false;
        loop {
            let mut work = Vec::new();
            let mut raising = false;
            let mut tasks = Vec::new();
            for record in self.database.operations().await? {
                let data = Data::read(&record)?;
                raising |= data.raises_version();
                if let Data::Snapshots(task) = data {
                    tasks.push((record, task.job));
                }
            }
            for (record, job) in tasks {
                let reset = matches!(
                    job,
                    SnapshotJob::Write {
                        trigger: SnapshotTrigger::Reset,
                        ..
                    }
                );
                if !(reset && raising) && self.operation_ready(&record).await? {
                    work.push((record, job));
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
                match self.drive_snapshot(record.id).await? {
                    Driven::Finished(_) => advanced = true,
                    Driven::Entry => (advanced, handed) = (true, true),
                    Driven::Waiting => (),
                }
            }
            if !advanced {
                return Ok(handed);
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
            attempt:
                Some(SnapshotAttempt {
                    session: Some(session),
                    ..
                }),
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

    async fn drive_snapshot(&mut self, id: crate::OperationId) -> Result<Driven, SyncError> {
        loop {
            let record = self.snapshot_record(id).await?;
            let data = Data::read(&record)?;
            let Data::Snapshots(task) = data else {
                return Ok(Driven::Entry);
            };
            if !self.operation_ready(&record).await? {
                let pending = self
                    .database
                    .operation_pending::<crate::PendingRecord>(id)
                    .await?
                    .ok_or(coven_database::DbError::Invariant {
                        what: "an operation not ready has its condition",
                    })?;
                return Err(SyncError::Pending(Box::new(pending)));
            }
            let published = task.published();
            let progress = match self.snapshot_step(&record, task).await {
                Ok(progress) => progress,
                Err(error) => {
                    self.record_operation_error(&record, &error).await?;
                    return Err(error);
                }
            };
            match progress {
                Progress::Finished(_) => return Ok(Driven::Finished(published)),
                Progress::Waiting(reason) => {
                    self.record_operation_condition(&record, reason).await?;
                    return Ok(Driven::Waiting);
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
        NonZeroU64::new(id.number).ok_or(coven_database::DbError::Invariant {
            what: "snapshot numbers start at one",
        })?,
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

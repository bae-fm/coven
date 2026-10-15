//! Publish the durable migration result before letting new-version writes out.

use super::StoreLogSync;
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    snapshot_data::*,
    PendingReason, Prerequisite, SyncError,
};
use coven_database::{OperationRecord, StoreLogState};
use coven_format::store_log::{SnapshotId, StoreChange};
use coven_merge::Audience;

pub(super) fn schema_in_place(state: &StoreLogState, audience: &Audience, version: u32) -> bool {
    state
        .schema
        .get(audience)
        .is_some_and(|v| v.number >= version)
}

impl StoreLogSync {
    /// Supply to `DatabaseBuilder::migration_operation` at the composition root.
    /// Step 1 commits with the migration transaction; the retained publication
    /// completes only after every readable audience and the reload are applied.
    pub fn migration_operation(
        version: u32,
    ) -> Result<coven_database::NewOperation, coven_database::DbError> {
        Data::PublishSchema { version }
            .new_operation("coven")
            .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
    }

    pub(super) async fn schema_publication_step(
        &self,
        record: &OperationRecord,
        version: u32,
    ) -> Result<Progress, SyncError> {
        let local = self.database.local_store_log().await?;
        if local.log.replay.state.store.is_none() {
            return Ok(Progress::Waiting(PendingReason::NoStorage));
        }
        if let Some(id) = self.pending_reload().await? {
            return Ok(Progress::Waiting(self.reload_condition(id).await?));
        }
        if let Some(audience) = self
            .snapshot_audiences(&local.log)?
            .into_iter()
            .find(|a| !schema_in_place(&local.log.replay.state, a, version))
        {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::SchemaPublication { audience, version },
            )));
        }
        self.database.finish_operation(record.id).await?;
        Ok(Progress::Finished(Output::Unit))
    }

    /// The database commits its publication version with the migration
    /// write. That version remains the publication intent across process exits;
    /// audience raises discharge it independently, including a circle whose
    /// first updating member was not the device that raised the store.
    pub(crate) async fn schedule_version_changes(&self) -> Result<(), SyncError> {
        let state = self.database.sync_state(Vec::new()).await?;
        let local = self.database.local_store_log().await?;
        if local.log.replay.state.store.is_none() {
            return Ok(());
        }
        // Intent needs the device's committed membership, not unlocked signing
        // keys. Actual snapshot and entry steps check custody and authority.
        let Some(device) = crate::effects::device(&local.log.replay.state, local.device) else {
            return Ok(());
        };
        if crate::effects::member(&local.log.replay.state, &device.member).is_none() {
            return Ok(());
        }
        crate::write_seal::check_upload_version(&local.log, state.schema_version)?;
        // A breaking migration records its publication intent with its captures.
        if state.breaking_version == 0 {
            return Ok(());
        }
        let records = self.database.operations().await?;
        let mut pending = Vec::new();
        for record in &records {
            if let Data::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        audience,
                        trigger: SnapshotTrigger::Raise { version, .. },
                        ..
                    },
                ..
            }) = Data::read(record)?
            {
                pending.push((audience, version));
            }
        }
        // The migration captured one raise per audience it read, through its marker.
        let mut captures = self.database.raise_captures().await?;
        // The store raise releases writes, so publish the readable circles first.
        captures.sort_by_key(|c| matches!(c.audience, Audience::Store));
        for capture in captures {
            if schema_in_place(&local.log.replay.state, &capture.audience, capture.version) {
                self.database
                    .discard_raise_capture(capture.audience, capture.version)
                    .await?;
            } else if !pending.contains(&(capture.audience.clone(), capture.version)) {
                self.start_snapshot_task(SnapshotJob::Write {
                    audience: capture.audience,
                    device: local.device,
                    trigger: SnapshotTrigger::Raise {
                        version: capture.version,
                        entry: None,
                    },
                    attempt: None,
                })
                .await?;
            }
        }
        Ok(())
    }

    /// SP step 6: discover and apply every writer's new writes, after a
    /// snapshot discovery if a miss cannot be trusted. Snapshots are prepared
    /// only right after a completed one, with a fresh sample taken before its
    /// reads as their `T_c`: while a preparation waits, this catch-up
    /// observes storage time and completes a store-log catch-up first, and
    /// records that sample once it completes (§15).
    pub async fn catch_up_writes(
        &mut self,
        writes: &mut crate::DeviceLogSync,
    ) -> Result<crate::WriteDiscovery, SyncError> {
        // The fresh store-log catch-up also serves the retirement-verdict gate.
        let sample = if self.preparation_waits().await? {
            Some(self.catch_up_time().await?)
        } else {
            None
        };
        let mut discovery = writes.download_writes().await?;
        if discovery.needs_snapshots {
            self.discover_snapshots().await?;
            discovery = writes.download_writes().await?;
        }
        if let Some(sample) = sample.filter(|_| discovery.complete) {
            self.reads.keep_write_catch_up(sample);
        }
        Ok(discovery)
    }

    pub(super) async fn raise_snapshot_step(
        &mut self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        version: u32,
        snapshot: SnapshotId,
    ) -> Result<Progress, SyncError> {
        let mut data = Data::Snapshots(task.clone());
        if record.last_step >= data.entry_step_number(5) {
            self.clear_snapshot_files(record, &mut task).await?;
            self.database
                .discard_raise_capture(snapshot.audience.clone(), version)
                .await?;
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        if data.entry()?.is_some() {
            return self.entry_step(record, data).await;
        }
        let mut local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        let mut ring = self.store_keys.read()?;
        self.update_keys(&local.log, &member, &mut ring).await?;
        // A plain app call may have fixed the next entry before this open ran
        // migrations. Its reserved number must finish before preparing a raise.
        self.publish(&mut local, &member, &mut ring).await?;
        if let Some(upload) = &local.upload {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::Object(crate::store_log_object::path(
                    self.directory.id(),
                    upload.entry.position,
                )),
            )));
        }
        if schema_in_place(&local.log.replay.state, &snapshot.audience, version) {
            self.clear_snapshot_files(record, &mut task).await?;
            self.database
                .discard_raise_capture(snapshot.audience.clone(), version)
                .await?;
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        let observed = self.send.observation()?;
        let record = record.clone();
        self.database
            .prepare_operation_entry(
                member.member(),
                StoreChange::RaiseSchema { version, snapshot },
                move |log, entry, copies| {
                    let sealed = crate::store_log_keys::seal(
                        copies,
                        log,
                        entry,
                        ring.as_ref(),
                        &member,
                        &observed,
                    )?;
                    data.set_entry(Some(entry.clone()))?;
                    Ok::<_, SyncError>((sealed, data.update(&record, data.entry_step_number(1))?))
                },
            )
            .await?;
        Ok(Progress::Advanced)
    }
}

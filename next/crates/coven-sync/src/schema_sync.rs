//! Publish the durable migration result before letting new-version writes out.

use super::StoreLogSync;
use crate::{
    operation_data::Data,
    operations::{Output, Progress},
    snapshot_data::*,
    SyncError, SyncReport,
};
use coven_database::{OperationRecord, StoreLogState};
use coven_format::store_log::{SnapshotId, StoreChange};
use coven_merge::Audience;

impl RaisedVersion {
    pub(crate) fn in_place(self, state: &StoreLogState, audience: &Audience) -> bool {
        match self {
            Self::Schema(version) => state
                .schema
                .get(audience)
                .is_some_and(|v| v.number >= version),
            Self::Format(version) => state
                .format
                .get(audience)
                .is_some_and(|v| v.number >= version),
        }
    }

    fn change(self, snapshot: SnapshotId) -> StoreChange {
        match self {
            Self::Schema(version) => StoreChange::RaiseSchema { version, snapshot },
            Self::Format(version) => StoreChange::RaiseFormat { version, snapshot },
        }
    }
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
        if local.log.replay.state.store.is_none() || self.pending_reload().await?.is_some() {
            return Ok(Progress::Waiting);
        }
        let raised = RaisedVersion::Schema(version);
        if self
            .snapshot_audiences(&local.log)?
            .keys()
            .any(|a| !raised.in_place(&local.log.replay.state, a))
        {
            return Ok(Progress::Waiting);
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
        // Format one is the initial format, with no predecessor to migrate.
        // A later format uses the same snapshot/entry operation; its codecs own
        // both conversion parts, including late writes, rather than marking loss.
        // Intent needs the device's committed membership, not unlocked signing
        // keys. Actual snapshot and entry steps check custody and authority.
        let Some(device) = local.log.replay.state.devices.get(&local.device) else {
            return Ok(());
        };
        if device.removed
            || local
                .log
                .replay
                .state
                .members
                .get(&device.member)
                .is_none_or(|m| m.removed)
        {
            return Ok(());
        }
        crate::write_seal::check_upload_version(&local.log, state.schema_version)?;
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
        let mut audiences: Vec<_> =
            super::snapshots::audiences_for_member(&local.log, &device.member)?
                .into_keys()
                .collect();
        // The store raise releases writes, so publish the readable circles first.
        audiences.sort_by_key(|a| matches!(a, Audience::Store));
        for audience in audiences {
            let mut versions = Vec::new();
            if state.breaking_version > 0 {
                versions.push(RaisedVersion::Schema(state.breaking_version));
            }
            if coven_format::FORMAT_VERSION
                > local
                    .log
                    .replay
                    .state
                    .format
                    .get(&audience)
                    .map_or(1, |v| v.number)
            {
                versions.push(RaisedVersion::Format(coven_format::FORMAT_VERSION));
            }
            for version in versions {
                if !version.in_place(&local.log.replay.state, &audience)
                    && !pending.contains(&(audience.clone(), version))
                {
                    self.start_snapshot_task(SnapshotJob::Write {
                        audience: audience.clone(),
                        device: local.device,
                        trigger: SnapshotTrigger::Raise {
                            version,
                            entry: None,
                        },
                        session: None,
                    })
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn raise_snapshot_step(
        &mut self,
        record: &OperationRecord,
        mut task: SnapshotTask,
        version: RaisedVersion,
        snapshot: SnapshotId,
    ) -> Result<Progress, SyncError> {
        let mut data = Data::Snapshots(task.clone());
        if record.last_step >= data.entry_step_number(5) {
            self.clear_snapshot_files(record, &mut task).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        if data.entry()?.is_some() {
            return self.entry_step(record, data).await;
        }
        let mut local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        let mut ring = self.store_keys.unlock()?;
        let mut report = SyncReport::default();
        self.update_keys(&local.log, &member, &mut ring, &mut report)
            .await?;
        // A plain app call may have fixed the next entry before this open ran
        // migrations. Its reserved number must finish before preparing a raise.
        self.publish(&mut local, &member, &mut ring, &mut report)
            .await?;
        if local.upload.is_some() {
            return Ok(Progress::Waiting);
        }
        if let Some(damaged) = report.damaged_objects.into_iter().next() {
            return Err(damaged.into());
        }
        if version.in_place(&local.log.replay.state, &snapshot.audience) {
            self.clear_snapshot_files(record, &mut task).await?;
            self.database.finish_operation(record.id).await?;
            return Ok(Progress::Finished(Output::Unit));
        }
        let record = record.clone();
        self.database
            .prepare_operation_entry(
                member.member_id(),
                version.change(snapshot),
                move |log, entry| {
                    let sealed = crate::store_log_keys::seal(log, entry, ring.as_ref(), &member)?;
                    data.set_entry(Some(entry.clone()))?;
                    Ok::<_, SyncError>((sealed, data.update(&record, data.entry_step_number(1))?))
                },
            )
            .await?;
        Ok(Progress::Advanced)
    }
}

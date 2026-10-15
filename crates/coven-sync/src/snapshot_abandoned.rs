//! Settling an operation's abandoned snapshot attempts (§15). Stopping
//! locally cannot stop a request already sent from landing, so an attempt
//! abandoned before a counting landing was confirmed is recorded with the
//! abandonment. Its landing that does not count is deleted: no reader
//! selects, loads or counts it (io `late_deletion_unseen`,
//! `abandoned_landing_unseen`). One that counts landed before the
//! abandonment and stays an ordinary snapshot of this device's. An absent
//! path is settled once an observed storage time is more than the listing
//! margin past the abandonment.

use super::{snapshot_path, StoreLogSync};
use crate::{
    finality::LISTING_MARGIN,
    operation_data::Data,
    operations::Progress,
    snapshot_data::{AbandonedAttempt, AbandonedLanding, SnapshotJob, SnapshotTask},
    PendingReason, Prerequisite, SyncError,
};
use coven_database::OperationRecord;
use coven_format::{store_log::SnapshotId, value::StorageTime};
use coven_storage::ObjectPath;

impl StoreLogSync {
    /// Check each abandoned attempt once and drop those now settled, saving
    /// the task at its current step when any is. A failed check or deletion
    /// keeps the record for the next run.
    pub(crate) async fn settle_abandoned(
        &self,
        record: &OperationRecord,
        mut task: SnapshotTask,
    ) -> Result<SnapshotTask, SyncError> {
        let abandoned = abandoned(&task).to_vec();
        if abandoned.is_empty() {
            return Ok(task);
        }
        let mut observed = false;
        let mut remaining = Vec::new();
        for attempt in abandoned.iter() {
            if !self.settle(&task, attempt, &mut observed).await? {
                remaining.push(attempt.clone());
            }
        }
        if remaining.len() != abandoned.len() {
            let SnapshotJob::Write { abandoned, .. } = &mut task.job else {
                unreachable!("only a snapshot write abandons attempts")
            };
            *abandoned = remaining;
            self.database
                .advance_operation(Data::Snapshots(task.clone()).update(record, record.last_step)?)
                .await?;
        }
        Ok(task)
    }

    /// Whether this abandoned attempt is settled: its landing deleted, or
    /// kept because it counts, or its path absent past the margin.
    async fn settle(
        &self,
        task: &SnapshotTask,
        attempt: &AbandonedAttempt,
        observed: &mut bool,
    ) -> Result<bool, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let path = abandoned_path(self, task, attempt)?;
        if let Some(landed) = storage.status(&path).await? {
            self.finality
                .lock()
                .expect("finality lock poisoned")
                .observe(&landed);
            // A landing that counts was stored before the abandonment
            // (`counting_abandoned_landed_before`); readers may select it.
            if !attempt.caught_at.within_day(landed.stored_at) {
                tracing::debug!(
                    path = path.as_str(),
                    "deleting an abandoned snapshot attempt that landed too late to count"
                );
                storage.delete(&path).await?;
            }
            return Ok(true);
        }
        // A confirmed landing now absent was deleted by an earlier run.
        let AbandonedLanding::Unconfirmed { abandoned_at } = attempt.landing else {
            return Ok(true);
        };
        if !self.margin_passed(abandoned_at)
            && !*observed
            && margin_may_have_passed(self, abandoned_at)
        {
            self.observe_clock(&self.database.local_store_log().await?)
                .await?;
            *observed = true;
        }
        Ok(self.margin_passed(abandoned_at))
    }

    /// Whether an observed storage time is more than the listing margin past
    /// the abandonment, so an exact status after it settles an absent path.
    fn margin_passed(&self, abandoned_at: StorageTime) -> bool {
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
            .is_some_and(|now| past_margin(now, abandoned_at))
    }

    /// The wait that keeps a snapshot write from finishing while one of its
    /// abandoned attempts is unsettled: that attempt's path (§15).
    pub(crate) fn unsettled(&self, task: &SnapshotTask) -> Result<Option<Progress>, SyncError> {
        abandoned(task)
            .first()
            .map(|attempt| {
                Ok(Progress::Waiting(PendingReason::Waits(
                    Prerequisite::Object(abandoned_path(self, task, attempt)?),
                )))
            })
            .transpose()
    }
}

fn abandoned(task: &SnapshotTask) -> &[AbandonedAttempt] {
    match &task.job {
        SnapshotJob::Write { abandoned, .. } => abandoned,
        SnapshotJob::Reload { .. } => &[],
    }
}

fn abandoned_path(
    sync: &StoreLogSync,
    task: &SnapshotTask,
    attempt: &AbandonedAttempt,
) -> Result<ObjectPath, SyncError> {
    let SnapshotJob::Write {
        audience, device, ..
    } = &task.job
    else {
        unreachable!("only a snapshot write abandons attempts")
    };
    snapshot_path(
        sync.directory.id(),
        &SnapshotId {
            audience: audience.clone(),
            device: *device,
            number: attempt.number.get(),
        },
    )
}

/// The device clock only schedules an observation; it never settles an
/// absence itself (§9).
fn margin_may_have_passed(sync: &StoreLogSync, abandoned_at: StorageTime) -> bool {
    StorageTime::from_system_time(sync.clock.now()).is_ok_and(|now| past_margin(now, abandoned_at))
}

fn past_margin(now: StorageTime, abandoned_at: StorageTime) -> bool {
    now.duration_since(abandoned_at)
        .is_some_and(|since| since > LISTING_MARGIN)
}

//! Replay-selected boundaries constrain both snapshot choice and late writes.

use super::{snapshot_path, StoreLogSync};
use crate::{snapshot_data::SavedBoundary, SyncError};
use coven_database::{BoundaryCoverage, StoreLog};
use coven_format::{sealed_snapshot::SnapshotObjectPrefix, store_log::SnapshotId, value::EntryId};
use coven_merge::Audience;

pub(super) fn allows(log: &StoreLog, id: &SnapshotId, prefix: &SnapshotObjectPrefix) -> bool {
    log.boundaries()
        .filter(|boundary| boundary.snapshot().audience == id.audience)
        .last()
        .is_none_or(|boundary| {
            boundary.snapshot() == id || prefix.store_log.covers(boundary.entry())
        })
}

pub(super) fn decode(saved: &SavedBoundary) -> Result<BoundaryCoverage, SyncError> {
    let prefix = SnapshotObjectPrefix::decode(&saved.prefix)?;
    Ok(BoundaryCoverage {
        entry: EntryId {
            device: saved.entry.0,
            number: saved.entry.1,
        },
        included: prefix.writes,
    })
}

impl StoreLogSync {
    pub(super) async fn snapshot_boundaries(
        &self,
        log: &StoreLog,
    ) -> Result<Vec<SavedBoundary>, SyncError> {
        let readable = self.snapshot_audiences(log)?;
        let mut saved = Vec::new();
        let boundaries: Vec<_> = log
            .boundaries()
            .filter(|boundary| readable.contains(&boundary.snapshot().audience))
            .collect();
        if boundaries.is_empty() {
            return Ok(saved);
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let objects = storage
            .list(&coven_storage::ObjectPrefix::snapshots(self.directory.id()))
            .await?;
        for boundary in boundaries {
            let id = boundary.snapshot();
            let path = snapshot_path(self.directory.id(), id)?;
            let object = objects
                .iter()
                .find(|object| object.path == path)
                .ok_or(coven_storage::StorageFailure::NotFound)?;
            let candidate = match self.snapshot_prefix(object, log).await? {
                Ok(candidate) => candidate,
                // The named snapshot's wait or refusal is its own record, and
                // the reload that needs it fails with that record (§19.1).
                Err(reason) => {
                    let record = crate::PendingRecord {
                        subject: crate::PendingSubject::Snapshot(id.clone()),
                        reason,
                        reported_by: self.database.local_store_log().await?.device,
                    };
                    self.database.record_pending(record.clone()).await?;
                    return Err(SyncError::Pending(Box::new(record)));
                }
            };
            // A named snapshot that landed late is ignored like any other:
            // the boundary waits as if it had not arrived.
            if !candidate.prefix.counts(super::catalog::landing(object)?) {
                return Err(coven_storage::StorageFailure::NotFound
                    .with_source("the boundary's snapshot landed after its catch-up deadline")
                    .into());
            }
            let clear = candidate.prefix.encode()?;
            saved.push(SavedBoundary {
                entry: (boundary.entry().device, boundary.entry().number),
                prefix: clear,
            });
        }
        Ok(saved)
    }

    pub(super) fn has_snapshot_boundary(&self, log: &StoreLog, audience: &Audience) -> bool {
        log.boundaries()
            .any(|boundary| boundary.snapshot().audience == *audience)
    }
}

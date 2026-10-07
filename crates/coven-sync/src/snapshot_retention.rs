//! Deletion follows authenticated coverage, posted positions and ownership.

use super::{
    catalog::{inconsistent, snapshot_damage},
    snapshot_path, StoreLogSync,
};
use crate::{SyncError, SyncResults};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::{store_log::StoreChange, value::WritePositions};
use coven_foundation::id_source::DeviceId;
use coven_storage::{CloudProvider, ObjectPath, ObjectPrefix};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

impl StoreLogSync {
    pub(super) async fn retain_snapshot_objects(
        &self,
        report: &mut SyncResults,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        let readable: BTreeSet<_> = self.snapshot_audiences(&local.log)?.into_keys().collect();
        let mut coverage = BTreeMap::new();
        let mut superseded = Vec::new();
        let mut pinned = BTreeSet::new();
        for pending in self.database.operations().await? {
            if let crate::operation_data::Data::Entry(crate::operation_data::EntryWork {
                intent: crate::operation_data::Intent::Reset { snapshot },
                ..
            }) = crate::operation_data::Data::read(&pending)?
            {
                pinned.insert(snapshot_path(&snapshot)?);
            }
            if let crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
                job:
                    crate::snapshot_data::SnapshotJob::Write {
                        audience, device, ..
                    },
                ..
            }) = crate::operation_data::Data::read(&pending)?
            {
                pinned.insert(snapshot_path(&coven_format::store_log::SnapshotId {
                    audience,
                    device,
                    number: pending
                        .id
                        .0
                        .try_into()
                        .map_err(|_| coven_database::DbError::DamagedDatabase)?,
                })?);
            }
        }
        for (_, snapshot) in super::boundaries::entries(&local.log) {
            pinned.insert(snapshot_path(snapshot)?);
        }
        for audience in &readable {
            let candidates = self
                .snapshot_candidates(audience, &local.log, report)
                .await?
                .candidates;
            let older: Vec<_> = candidates
                .iter()
                .map(|c| (c.object.path.clone(), c.prefix.writes.clone()))
                .collect();
            let chosen = candidates.into_iter().find(|c| {
                super::boundaries::allows(
                    &local.log,
                    &c.object
                        .path
                        .snapshot_id()
                        .expect("validated snapshot path"),
                    &c.prefix,
                )
            });
            if let Some(chosen) = chosen {
                let prefix = chosen.prefix;
                for (path, positions) in older {
                    if path != chosen.object.path
                        && path.device() == Some(local.device)
                        && !pinned.contains(&path)
                        && positions.0.iter().all(|id| prefix.writes.covers(*id))
                    {
                        superseded.push(path);
                    }
                }
                coverage.insert(audience.clone(), prefix.writes);
            }
        }
        let positions = self.retention_positions(&local.log, report).await?;
        for object in storage.list(&ObjectPrefix::device_logs()).await? {
            let id = object
                .path
                .write_id()
                .ok_or_else(|| inconsistent("device listing has another path layout"))?;
            if !self.can_delete_device(&local.log, local.device, id.device)? {
                continue;
            }
            let header = match self.open_write_header(&object).await {
                Ok(header) => header,
                Err(error) if waiting(&object.path, &error) => return Ok(()),
                Err(error) => {
                    snapshot_damage(report, &object.path, error)?;
                    continue;
                }
            };
            let covered = header
                .parts
                .iter()
                .all(|part| coverage.get(&part.audience).is_some_and(|p| p.covers(id)));
            let posted = local
                .log
                .replay
                .state
                .devices
                .iter()
                .filter(|(_, d)| !d.removed)
                .all(|(device, _)| positions.get(device).is_some_and(|p| p.covers(id)));
            let aged = self
                .clock
                .now()
                .duration_since(object.stored_at)
                .is_ok_and(|age| age >= Duration::from_secs(30 * 24 * 60 * 60));
            if covered && (posted || aged) {
                storage.delete(&object.path).await?;
            }
        }
        for path in superseded {
            storage.delete(&path).await?;
        }
        self.retain_uploaded_files(report).await
    }

    pub(super) fn can_delete_device(
        &self,
        log: &StoreLog,
        own: DeviceId,
        uploader: DeviceId,
    ) -> Result<bool, SyncError> {
        if uploader == own {
            return Ok(true);
        }
        let Some(device) = log.replay.state.devices.get(&uploader) else {
            return Ok(false);
        };
        if !device.removed {
            return Ok(false);
        }
        let me = self.operation_member()?.member_id();
        if device.member == me {
            return Ok(true);
        }
        let removed = log
            .replay
            .state
            .members
            .get(&device.member)
            .is_some_and(|m| m.removed);
        let provider = self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .config()
            .provider();
        let owner = match provider {
            CloudProvider::GoogleDrive => false,
            // S3 access-key ids do not identify their provider account. The
            // creation entry identifies its member; an admin role does not.
            CloudProvider::S3 => log.entries.iter().any(|entry| {
                matches!(entry.entry.change, StoreChange::CreateStore { .. })
                    && log.replay.entries[&entry.entry.position] == EntryOutcome::Kept
                    && entry.entry.author == me
            }),
            _ => self.owns_storage(log, &me),
        };
        Ok(removed && owner)
    }

    async fn retention_positions(
        &self,
        log: &StoreLog,
        report: &mut SyncResults,
    ) -> Result<BTreeMap<DeviceId, WritePositions>, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut positions = BTreeMap::new();
        for object in storage.list(&ObjectPrefix::positions()).await? {
            match self.open_retention_positions(&object.path, log).await {
                Ok((device, writes)) => {
                    positions.insert(device, writes);
                }
                Err(error) => snapshot_damage(report, &object.path, error)?,
            }
        }
        Ok(positions)
    }

    async fn open_retention_positions(
        &self,
        path: &ObjectPath,
        log: &StoreLog,
    ) -> Result<(DeviceId, WritePositions), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let bytes = storage.read(path).await?;
        let ring = self.store_keys.unlock()?;
        let positions = crate::posted_positions::open(&bytes, path, ring.as_ref(), log)?;
        Ok((positions.device, positions.writes))
    }
}

// Retention cannot prove coverage or absence until a write's inputs arrive.
pub(super) fn waiting(path: &ObjectPath, error: &SyncError) -> bool {
    let waiting = matches!(
        error,
        SyncError::KeyUnavailable(_)
            | SyncError::Database(coven_database::DbError::Snapshot(
                coven_database::SnapshotError::WriteWaiting(_)
            ))
    );
    if waiting {
        tracing::debug!(
            path = path.as_str(),
            ?error,
            "waiting write holds back retention"
        );
    }
    waiting
}

//! Deletion follows authenticated coverage, posted positions and ownership.

use super::{
    catalog::{failure, inconsistent},
    snapshot_path, StoreLogSync,
};
use crate::SyncError;
use coven_database::{EntryOutcome, StoreLog};
use coven_format::{
    store_log::StoreChange,
    value::{StorageTime, WritePositions},
};
use coven_foundation::id_source::DeviceId;
use coven_storage::{CloudProvider, ObjectPrefix};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

/// A covered write object whose readers have not all posted past it may go
/// once its storage age reaches thirty days (§15).
const RETENTION_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Whether storage time `now` is at least thirty days after `stored_at`.
fn reached_age(now: Option<StorageTime>, stored_at: StorageTime) -> bool {
    now.and_then(|now| now.duration_since(stored_at))
        .is_some_and(|age| age >= RETENTION_AGE)
}

impl StoreLogSync {
    pub(super) async fn retain_snapshot_objects(&self) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &self.operation_member()?)?;
        let readable = self.snapshot_audiences(&local.log)?;
        let mut coverage = BTreeMap::new();
        let mut superseded = Vec::new();
        let mut pinned = BTreeSet::new();
        for pending in self.database.operations().await? {
            let data = crate::operation_data::Data::read(&pending)?;
            if let crate::operation_data::Data::Entry(crate::operation_data::EntryWork {
                intent: crate::operation_data::Intent::Reset { snapshot },
                ..
            }) = &data
            {
                pinned.insert(snapshot_path(self.directory.id(), snapshot)?);
            }
            if let crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
                job:
                    crate::snapshot_data::SnapshotJob::Write {
                        audience, device, ..
                    },
                ..
            }) = &data
            {
                pinned.insert(snapshot_path(
                    self.directory.id(),
                    &coven_format::store_log::SnapshotId {
                        audience: audience.clone(),
                        device: *device,
                        number: pending
                            .id
                            .0
                            .try_into()
                            .map_err(|_| coven_database::DbError::DamagedDatabase)?,
                    },
                )?);
            }
        }
        for boundary in local.log.boundaries() {
            let snapshot = boundary.snapshot();
            pinned.insert(snapshot_path(self.directory.id(), snapshot)?);
        }
        for audience in &readable {
            let catalog = self.snapshot_candidates(audience, &local.log).await?;
            // Log deletion judges by the current newest counting snapshot:
            // the newest verified prefix, whatever this device found in its
            // body or whether it accepts its key. A version that loads that
            // body selects it and needs every write it leaves out (§15,
            // §19.1; storelog-data `retention_ignores_bodies`).
            if let Some(newest) = catalog
                .verified
                .iter()
                .find(|snapshot| snapshot.follows_boundary(&local.log))
            {
                // A listed prefix this device has not verified, because it
                // waits or this version refused it, may be the newest for a
                // later version, and need writes the chosen snapshot covers:
                // deleting this audience's parts waits until it verifies or
                // is no longer listed (§15, §19.1; `waiting_retention_safe`,
                // storelog-data `holding_safe`, `refused_prefix_holds`). That
                // prefix's record says why. A refused body never holds.
                if catalog.newest_unknown {
                    tracing::debug!(
                        ?audience,
                        "unverified snapshot prefix holds back log retention"
                    );
                } else {
                    coverage.insert(audience.clone(), newest.prefix.writes.clone());
                }
            }
            // An own snapshot goes once a newer one this device can load
            // covers everything it covers.
            let candidates = catalog.candidates;
            let chosen = candidates.iter().find(|c| {
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
                for older in &candidates {
                    if older.object.path != chosen.object.path
                        && older.object.path.device() == Some(local.device)
                        && !pinned.contains(&older.object.path)
                        && older
                            .prefix
                            .writes
                            .0
                            .iter()
                            .all(|id| chosen.prefix.writes.covers(*id))
                    {
                        superseded.push(older.object.path.clone());
                    }
                }
            }
        }
        let positions = self.retention_positions().await?;
        let mut storage_time = self.storage_time();
        let mut observed = false;
        let mut stuck = crate::pending_logs::local_refusals(&self.database).await?;
        for object in storage
            .list(&ObjectPrefix::device_logs(self.directory.id()))
            .await?
        {
            let id = object
                .path
                .write_id()
                .ok_or_else(|| inconsistent("device listing has another path layout"))?;
            if !self.can_delete_device(&local.log, local.device, id.device)? {
                continue;
            }
            if stuck
                .iter()
                .any(|record| record.blocks(coven_database::LogObject::Write(id)))
            {
                continue;
            }
            let header = match self.open_write_header(&object).await {
                Ok(header) => header,
                Err(error) if waiting(&object.path, &error) => return Ok(()),
                // Opening the header recorded a refusal as the write's own
                // record; it stops that log here as well.
                Err(error) => {
                    if let SyncError::Damaged(damage) = &error {
                        stuck.push(coven_database::LogRefusal {
                            object: coven_database::LogObject::Write(id),
                            failure: (&damage.failure).into(),
                        });
                    }
                    failure(&object.path, error)?;
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
                .filter(|(_, d)| d.active())
                .all(|(device, _)| positions.get(device).is_some_and(|p| p.covers(id)));
            // Age compares storage times only: the observed provider time T
            // against the object's first stored time. The device clock only
            // schedules one fresh observation when the known T is too old to
            // decide; it never establishes storage age (§9, §15).
            let mut aged = reached_age(storage_time, object.stored_at);
            if covered && !posted && !aged && !observed && self.age_may_have_passed(&object) {
                self.observe_clock(&local).await?;
                observed = true;
                storage_time = self.storage_time();
                aged = reached_age(storage_time, object.stored_at);
            }
            if covered && (posted || aged) {
                storage.delete(&object.path).await?;
            }
        }
        for path in superseded {
            storage.delete(&path).await?;
        }
        self.retain_uploaded_files().await
    }

    /// The newest provider time this session knows, a lower bound on storage's.
    fn storage_time(&self) -> Option<StorageTime> {
        self.finality
            .lock()
            .expect("finality lock poisoned")
            .sample_time()
    }

    /// Whether the device clock says a write may have reached its storage age,
    /// which schedules an observation only.
    fn age_may_have_passed(&self, object: &coven_storage::StoredObject) -> bool {
        StorageTime::from_system_time(self.clock.now())
            .is_ok_and(|now| reached_age(Some(now), object.stored_at))
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
        if !device.removed() {
            return Ok(false);
        }
        let me = self.operation_member()?.member();
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

    async fn retention_positions(&self) -> Result<BTreeMap<DeviceId, WritePositions>, SyncError> {
        let local = self.database.local_store_log().await?;
        let ring = self.store_keys.read()?;
        let observation = crate::posted_positions::observe(
            &self.database,
            &self.reads,
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            &local,
            ring.as_ref(),
        )
        .await?;
        Ok(observation
            .positions
            .into_iter()
            .map(|(device, positions)| (device, positions.writes))
            .collect())
    }
}

// Retention cannot prove coverage or absence until a write's inputs arrive.
pub(super) fn waiting(path: &coven_storage::ObjectPath, error: &SyncError) -> bool {
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

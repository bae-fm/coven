//! Prove absence across retained data before deleting an uploader's files.

use super::{catalog::snapshot_damage, StoreLogSync};
use crate::{replay_cache::ReplayCache, snapshot_data::SnapshotTask, SyncError, SyncResults};
use coven_database::OperationRecord;
use coven_storage::{ObjectPath, ObjectPrefix};
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    pub(super) async fn retain_uploaded_files(
        &self,
        record: &OperationRecord,
        task: &mut SnapshotTask,
        report: &mut SyncResults,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        // List first: publication fixes an identity in the local queue before
        // storage can list it, and atomically replaces that queue protection
        // with a waiting write. A later upload is outside this deletion set.
        let objects = storage.list(&ObjectPrefix::files()).await?;
        let local = self.database.local_store_log().await?;
        let readable: BTreeSet<_> = self.snapshot_audiences(&local.log)?.into_keys().collect();
        let protected = self.database.retained_files().await?;
        if objects.is_empty() && !protected.uploads.iter().any(|upload| upload.unused) {
            return Ok(());
        }
        let mut references: BTreeSet<_> = protected
            .references
            .into_iter()
            .map(|(device, id)| ObjectPath::file(device, id))
            .collect();
        for upload in &protected.uploads {
            if upload.unused {
                continue;
            }
            if let Some(fixed) = &upload.fixed {
                let (id, _) =
                    crate::files::file_upload::decode_identity(fixed.identity.as_bytes())?;
                let coven_database::FileLocation::OnDevice(device) = upload.file.location() else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                references.insert(ObjectPath::file(device, id));
            }
        }
        let mut audiences = BTreeSet::new();
        for object in storage.list(&ObjectPrefix::snapshots()).await? {
            let id = object
                .path
                .snapshot_id()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if !readable.contains(&id.audience) {
                tracing::debug!(
                    path = object.path.as_str(),
                    "unreadable retained snapshot prevents proving file absence"
                );
                return Ok(());
            }
            audiences.insert(id.audience);
        }
        for audience in audiences {
            let damaged = report.damaged_objects.len();
            let candidates = self
                .snapshot_candidates(&audience, &local.log, report)
                .await?;
            if report.damaged_objects.len() != damaged
                || candidates.listed != candidates.candidates.len()
            {
                tracing::debug!(
                    ?audience,
                    "excluded retained snapshots prevent proving file absence"
                );
                return Ok(());
            }
            for candidate in candidates.candidates {
                let file = self.reserve_snapshot_file(record, task).await?;
                match self.open_snapshot(&candidate, &file).await {
                    Ok(files) => references.extend(
                        files
                            .into_iter()
                            .map(|(device, id)| ObjectPath::file(device, id)),
                    ),
                    Err(SyncError::Database(coven_database::DbError::Snapshot(
                        coven_database::SnapshotError::Schema { .. },
                    ))) => {
                        tracing::debug!(
                            path = candidate.object.path.as_str(),
                            "unsupported retained snapshot prevents proving file absence"
                        );
                        return Ok(());
                    }
                    Err(error) => {
                        snapshot_damage(report, &candidate.object.path, error)?;
                        // Damaged or unsupported data cannot establish absence.
                        return Ok(());
                    }
                }
            }
        }
        let mut replays = ReplayCache::new(&local.log);
        for object in storage.list(&ObjectPrefix::device_logs()).await? {
            let saved = match self
                .open_snapshot_write(&object, &local.log, &mut replays, &readable, record, task)
                .await
            {
                Ok(saved) => saved,
                Err(error) => {
                    snapshot_damage(report, &object.path, error)?;
                    return Ok(());
                }
            };
            let Some(files) = self
                .database
                .write_file_references(self.open_saved_write(&saved)?)
                .await?
            else {
                tracing::debug!(
                    path = object.path.as_str(),
                    "unreadable retained write prevents proving file absence"
                );
                return Ok(());
            };
            references.extend(
                files
                    .into_iter()
                    .map(|(device, id)| ObjectPath::file(device, id)),
            );
        }
        let mut paths: BTreeSet<_> = objects.into_iter().map(|object| object.path).collect();
        let mut unused = BTreeMap::new();
        for upload in protected.uploads.into_iter().filter(|upload| upload.unused) {
            let fixed = upload
                .fixed
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let (id, _) = crate::files::file_upload::decode_identity(fixed.identity.as_bytes())?;
            let coven_database::FileLocation::OnDevice(device) = upload.file.location() else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            let path = ObjectPath::file(device, id);
            paths.insert(path.clone());
            unused.insert(path, upload.id);
        }
        for path in paths {
            let device = path
                .device()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if self.can_delete_device(&local.log, local.device, device)?
                && !references.contains(&path)
            {
                // Idempotent deletion also confirms absence after a lost reply.
                storage.delete(&path).await?;
                if let Some(id) = unused.get(&path) {
                    self.database.retire_unused_file(*id).await?;
                }
            }
        }
        Ok(())
    }
}

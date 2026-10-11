//! Prove absence across retained data before deleting an uploader's files.

use super::{catalog::snapshot_damage, StoreLogSync};
use crate::{replay_cache::ReplayCache, DamagedObject, SyncError};
use coven_storage::{ObjectPath, ObjectPrefix};
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    pub(super) async fn retain_uploaded_files(
        &self,
        damages: &mut Vec<DamagedObject>,
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
            if let Some(identity) = &upload.identity {
                let (id, _) = crate::files::file_upload::decode_identity(identity.as_bytes())?;
                let coven_database::FileLocation::OnDevice(device) = upload.file.location() else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                references.insert(ObjectPath::file(device, id));
            }
        }
        let mut paths: BTreeSet<_> = objects.into_iter().map(|object| object.path).collect();
        let mut unused = BTreeMap::new();
        for upload in protected.uploads.into_iter().filter(|upload| upload.unused) {
            let identity = upload
                .identity
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let (id, _) = crate::files::file_upload::decode_identity(identity.as_bytes())?;
            let coven_database::FileLocation::OnDevice(device) = upload.file.location() else {
                return Err(coven_database::DbError::DamagedDatabase.into());
            };
            let path = ObjectPath::file(device, id);
            paths.insert(path.clone());
            unused.insert(path, upload.id);
        }
        let mut eligible = BTreeSet::new();
        for path in paths {
            let device = path
                .device()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if self.can_delete_device(&local.log, local.device, device)?
                && !references.contains(&path)
            {
                eligible.insert(path);
            }
        }
        if eligible.is_empty() {
            return Ok(());
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
            let damaged = damages.len();
            let candidates = self
                .snapshot_candidates(&audience, &local.log, damages)
                .await?;
            if damages.len() != damaged || candidates.listed != candidates.candidates.len() {
                tracing::debug!(
                    ?audience,
                    "excluded retained snapshots prevent proving file absence"
                );
                return Ok(());
            }
            for candidate in candidates.candidates {
                match self.snapshot_file_references(&candidate).await {
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
                        snapshot_damage(damages, &candidate.object.path, error)?;
                        // Damaged or unsupported data cannot establish absence.
                        return Ok(());
                    }
                }
            }
        }
        eligible.retain(|path| !references.contains(path));
        if eligible.is_empty() {
            return Ok(());
        }
        let mut replays = ReplayCache::new(&local.log);
        let stuck = self.database.sync_state(Vec::new()).await?.stuck;
        for object in storage.list(&ObjectPrefix::device_logs()).await? {
            if eligible.iter().all(|path| references.contains(path)) {
                return Ok(());
            }
            let id = object
                .path
                .write_id()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if stuck
                .iter()
                .any(|record| record.blocks(coven_database::LogObject::Write(id)))
            {
                tracing::debug!(?id, "stuck write prevents proving file absence");
                return Ok(());
            }
            let files = match self
                .retained_write_references(&object, &local.log, &mut replays, &readable)
                .await
            {
                Ok(Some(files)) => files,
                Ok(None) => {
                    tracing::debug!(
                        path = object.path.as_str(),
                        "unreadable or unsupported retained write prevents proving file absence"
                    );
                    return Ok(());
                }
                Err(error) if super::retention::waiting(&object.path, &error) => return Ok(()),
                Err(error) => {
                    if let SyncError::Damaged(damage) = &error {
                        crate::write_object::record_damage(
                            &self.database,
                            storage,
                            &object,
                            &damage.failure,
                        )
                        .await?;
                    }
                    snapshot_damage(damages, &object.path, error)?;
                    return Ok(());
                }
            };
            references.extend(
                files
                    .into_iter()
                    .map(|(device, id)| ObjectPath::file(device, id)),
            );
        }
        for path in eligible {
            if !references.contains(&path) {
                // Idempotent deletion also confirms absence after a lost reply.
                storage.delete(&path).await?;
                if let Some(id) = unused.get(&path) {
                    self.database.retire_unused_file(*id).await?;
                }
            }
        }
        Ok(())
    }

    async fn retained_write_references(
        &self,
        object: &coven_storage::StoredObject,
        log: &coven_database::StoreLog,
        replays: &mut ReplayCache<'_>,
        readable: &BTreeSet<coven_merge::Audience>,
    ) -> Result<Option<crate::pass_reads::FileReferences>, SyncError> {
        if let Some(files) = self.reads.files(object) {
            return Ok(files);
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.read()?;
        let crate::write_object::ReadyWrite {
            opened,
            author,
            parts: mut opens,
        } = crate::write_object::download(
            storage,
            object,
            ring.as_ref(),
            &self.reads,
            log,
            replays,
            || Ok(self.operation_member()?.member_id()),
        )
        .await?
        .map_err(coven_database::SnapshotError::WriteWaiting)
        .map_err(coven_database::DbError::Snapshot)?;
        let ring = ring.as_ref().expect("opened header has its store key");
        for (opens, part) in opens.iter_mut().zip(&opened.header.parts) {
            *opens &= readable.contains(&part.audience);
        }
        if opens.contains(&false) {
            self.reads.keep_files(object, None);
            return Ok(None);
        }
        let (mut sink, input) =
            crate::stream_input::ChannelParts::new(opened.header.clone(), &opens);
        let transfer = async {
            let result =
                crate::write_object::finish(storage, object, ring, &author, opened, &mut sink)
                    .await;
            drop(sink);
            result
        };
        let (transferred, checked) =
            tokio::join!(transfer, self.database.write_file_references(input));
        transferred?;
        let files =
            checked.map_err(|error| crate::write_object::database_failure(&object.path, error))?;
        self.reads.keep_files(object, files.clone());
        Ok(files)
    }
}

//! Prove absence across retained data before deleting an uploader's files.

use super::{prefix::failure, StoreLogSync};
use crate::{replay_cache::ReplayCache, SyncError};
use coven_storage::{ObjectPath, ObjectPrefix};
use std::collections::BTreeSet;

impl StoreLogSync {
    pub(super) async fn retain_uploaded_files(&self) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        // List first: the attaching write fixes a file's path in its row and
        // this device's queue in one transaction, before storage can list it.
        // A later upload is outside this deletion set.
        let objects = storage
            .list(&ObjectPrefix::files(self.directory.id()))
            .await?;
        if objects.is_empty() {
            return Ok(());
        }
        let local = self.database.local_store_log().await?;
        let readable = self.snapshot_audiences(&local.log)?;
        let mut references: BTreeSet<_> = self
            .database
            .retained_files()
            .await?
            .references
            .into_iter()
            .map(|(device, id)| ObjectPath::file(self.directory.id(), device, id))
            .collect();
        let paths: BTreeSet<_> = objects.into_iter().map(|object| object.path).collect();
        let mut eligible = BTreeSet::new();
        for path in paths {
            let device = path.device().ok_or(coven_database::DbError::Invariant {
                what: "a file listing names devices",
            })?;
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
        for object in storage
            .list(&ObjectPrefix::snapshots(self.directory.id()))
            .await?
        {
            let id = object
                .path
                .snapshot_id()
                .ok_or(coven_database::DbError::Invariant {
                    what: "a snapshot listing names snapshots",
                })?;
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
            // A snapshot that waits, was refused or cannot be read is not a
            // candidate; any of them prevents proving absence.
            let candidates = self.snapshot_candidates(&audience, &local.log).await?;
            if candidates.listed != candidates.candidates.len() {
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
                            .map(|(device, id)| ObjectPath::file(self.directory.id(), device, id)),
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
                        self.refuse_snapshot(&candidate.object, error).await?;
                        // Refused data cannot establish absence.
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
        let stuck = crate::pending_logs::local_refusals(&self.database).await?;
        for object in storage
            .list(&ObjectPrefix::device_logs(self.directory.id()))
            .await?
        {
            if eligible.iter().all(|path| references.contains(path)) {
                return Ok(());
            }
            let id = object
                .path
                .write_id()
                .ok_or(coven_database::DbError::Invariant {
                    what: "a write listing names writes",
                })?;
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
                        crate::write_object::refuse(
                            &self.database,
                            storage,
                            crate::object_input::ObjectInput::observe(&self.database, &object)
                                .await?,
                            &damage.failure,
                        )
                        .await?;
                    }
                    failure(&object.path, error)?;
                    return Ok(());
                }
            };
            references.extend(
                files
                    .into_iter()
                    .map(|(device, id)| ObjectPath::file(self.directory.id(), device, id)),
            );
        }
        for path in eligible {
            if !references.contains(&path) {
                // Idempotent deletion also confirms absence after a lost reply.
                storage.delete(&path).await?;
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
        let retained = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        if let Some(Some(files)) = retained.files().await? {
            return Ok(Some(files));
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.read()?;
        let mut stream = retained.stream(storage).await;
        let crate::write_object::ReadyWrite {
            opened,
            signer,
            parts: mut opens,
        } = crate::write_object::download(&mut stream, ring.as_ref(), log, replays, || {
            Ok(self.operation_member()?.member())
        })
        .await?
        .map_err(crate::write_object::Unready::into_snapshot_error)?;
        let ring = ring.as_ref().expect("opened header has its store key");
        for (opens, part) in opens.iter_mut().zip(&opened.header.parts) {
            *opens &= readable.contains(&part.audience);
        }
        if opens.contains(&false) {
            stream.drain().await?;
            retained.keep_files(None).await?;
            return Ok(None);
        }
        let (mut sink, input) = crate::stream_input::ChannelParts::new(
            opened.header.clone(),
            &opens,
            coven_database::Published {
                stored_at: retained.object().stored_at,
                signer,
            },
        );
        let transfer = async {
            let result =
                crate::write_object::finish(&mut stream, ring, &signer, opened, &mut sink).await;
            drop(sink);
            result
        };
        let (transferred, checked) =
            tokio::join!(transfer, self.database.write_file_references(input));
        transferred?;
        let files =
            checked.map_err(|error| crate::write_object::database_failure(&object.path, error))?;
        retained.keep_files(files.clone()).await?;
        Ok(files)
    }
}

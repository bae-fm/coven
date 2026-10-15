//! Delete uploaded files once no retained data names them (§16.5).
//!
//! The database keeps the candidates: paths whose last local protection ended,
//! paths named by objects this device deleted, and what a removed uploader's
//! one-time folder scan found (`snapshot_file_scan.rs`). Retention reads only the candidates of uploaders
//! this device deletes for, checks local protection first, and proves remote
//! absence from checked references kept by object identity. Nothing else
//! lists `files/`.

use super::{prefix::failure, StoreLogSync};
use crate::{replay_cache::ReplayCache, SyncError};
use coven_database::{FileEvidence, StoreLog, UnprovenFiles};
use coven_foundation::id_source::{DeviceId, FileId, KeyId};
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, StoredObject};
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    /// Delete the candidates no retained row, loss, input, queued write,
    /// upload, snapshot or log write names. `logs` is this pass's listing of
    /// retained writes, less the ones it deleted. The first failed deletion
    /// joins `failures` and ends deletion; its candidate stays.
    pub(super) async fn retain_uploaded_files(
        &self,
        logs: &[StoredObject],
        failures: &mut Vec<(ObjectPath, SyncError)>,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let local = self.database.local_store_log().await?;
        let mut uploaders = BTreeSet::from([local.device]);
        for device in local.log.replay.state.devices.keys() {
            if self.can_delete_device(&local.log, local.device, *device)? {
                uploaders.insert(*device);
            }
        }
        self.scan_removed_uploaders(&local.log, &uploaders).await?;
        // Another uploader's files go only once the entries removing it are
        // final: a returning device may still name them (§9, §16.5).
        let dependencies = super::retention::waits::Dependencies::new(&local.log);
        uploaders.retain(|device| {
            *device == local.device
                || dependencies
                    .device(*device)
                    .all(|entry| local.log.is_final(entry))
        });
        let candidates = self.database.file_candidates(uploaders).await?;
        let (protected, open): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|candidate| candidate.protected);
        // Local protection decides first; its release adds the path again.
        self.database
            .settle_file_candidates(
                protected.iter().map(|c| (c.device, c.file)).collect(),
                Vec::new(),
            )
            .await?;
        let open: BTreeSet<_> = open.into_iter().map(|c| (c.device, c.file)).collect();
        if open.is_empty() {
            return Ok(());
        }
        let Some(references) = self.remote_file_references(&local.log, logs, &open).await? else {
            return Ok(());
        };
        let mut deleted = Vec::new();
        for (device, file) in open.difference(&references) {
            // Idempotent deletion also confirms absence after a lost reply.
            let path = ObjectPath::file(self.directory.id(), *device, *file);
            match storage.delete(&path).await {
                Ok(()) => deleted.push((*device, *file)),
                Err(error) => {
                    failures.push((path, error.into()));
                    break;
                }
            }
        }
        self.database
            .settle_file_candidates(Vec::new(), deleted)
            .await?;
        Ok(())
    }

    /// Every path the retained snapshots and log writes name, or `None` when
    /// some retained object cannot prove absence: an unreadable audience, an
    /// excluded or refused snapshot, a refused, waiting or unreadable write.
    /// Checked references and unproven verdicts are kept by object identity,
    /// so no body is read again for this until its prerequisite changes.
    async fn remote_file_references(
        &self,
        log: &StoreLog,
        logs: &[StoredObject],
        open: &BTreeSet<(DeviceId, FileId)>,
    ) -> Result<Option<BTreeSet<(DeviceId, FileId)>>, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let readable = self.snapshot_audiences(log)?;
        let versions = self.database.write_schema_versions().await?;
        let mut references = BTreeSet::new();
        let mut listed: BTreeMap<Audience, BTreeSet<ObjectPath>> = BTreeMap::new();
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
                return Ok(None);
            }
            listed.entry(id.audience).or_default().insert(object.path);
        }
        for (audience, paths) in listed {
            // A snapshot that waits, was refused or cannot be read is not a
            // candidate; any of them prevents proving absence.
            let candidates: Vec<_> = self
                .snapshot_candidates(&audience, log)
                .await?
                .candidates
                .into_iter()
                .filter(|candidate| paths.contains(&candidate.object.path))
                .collect();
            if candidates.len() != paths.len() {
                tracing::debug!(
                    ?audience,
                    "excluded retained snapshots prevent proving file absence"
                );
                return Ok(None);
            }
            for candidate in candidates {
                match self.snapshot_file_references(&candidate).await {
                    Ok(FileEvidence::Complete(files)) => references.extend(files),
                    Ok(FileEvidence::Unproven(unproven)) => {
                        tracing::debug!(
                            path = candidate.object.path.as_str(),
                            ?unproven,
                            "unsupported retained snapshot prevents proving file absence"
                        );
                        return Ok(None);
                    }
                    Err(error) => {
                        self.refuse_snapshot(&candidate.object, error).await?;
                        // Refused data cannot establish absence.
                        return Ok(None);
                    }
                }
            }
        }
        let mut replays = ReplayCache::new(log);
        let stuck = crate::pending_logs::local_refusals(&self.database).await?;
        let ring = self.store_keys.read()?;
        let needed = crate::key_introductions::needed(log);
        let opens = |audience: &Audience, key: KeyId| {
            readable.contains(audience)
                && crate::key_introductions::accepted(ring.as_ref(), &needed, audience, key)
        };
        for object in logs {
            if open.is_subset(&references) {
                break;
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
                return Ok(None);
            }
            match self
                .retained_write_references(object, log, &mut replays, &readable, &opens, &versions)
                .await
            {
                Ok(FileEvidence::Complete(files)) => references.extend(files),
                Ok(FileEvidence::Unproven(unproven)) => {
                    tracing::debug!(
                        path = object.path.as_str(),
                        ?unproven,
                        "unreadable or unsupported retained write prevents proving file absence"
                    );
                    return Ok(None);
                }
                Err(error) if super::retention::waiting(&object.path, &error) => return Ok(None),
                Err(error) => {
                    if let SyncError::Damaged(damage) = &error {
                        crate::write_object::refuse(
                            &self.database,
                            storage,
                            crate::object_input::ObjectInput::observe(&self.database, object)
                                .await?,
                            &damage.failure,
                        )
                        .await?;
                    }
                    failure(&object.path, error)?;
                    return Ok(None);
                }
            }
        }
        Ok(Some(references))
    }

    /// Check the file references of another uploader's write this device
    /// is about to delete, unless they are already checked or proven
    /// unreadable here; deletion then makes them candidates (§16.5).
    pub(super) async fn establish_references(
        &self,
        object: &StoredObject,
        log: &StoreLog,
    ) -> Result<(), SyncError> {
        let retained = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        if retained.files().await?.is_some() {
            return Ok(());
        }
        let readable = self.snapshot_audiences(log)?;
        let versions = self.database.write_schema_versions().await?;
        let ring = self.store_keys.read()?;
        let needed = crate::key_introductions::needed(log);
        let opens = |audience: &Audience, key: KeyId| {
            readable.contains(audience)
                && crate::key_introductions::accepted(ring.as_ref(), &needed, audience, key)
        };
        let mut replays = ReplayCache::new(log);
        self.retained_write_references(object, log, &mut replays, &readable, &opens, &versions)
            .await?;
        Ok(())
    }

    async fn retained_write_references(
        &self,
        object: &StoredObject,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        readable: &BTreeSet<Audience>,
        opens_part: &impl Fn(&Audience, KeyId) -> bool,
        versions: &std::ops::RangeInclusive<u32>,
    ) -> Result<FileEvidence, SyncError> {
        let retained = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        match retained.files().await? {
            Some(evidence @ FileEvidence::Complete(_)) => return Ok(evidence),
            // The same bytes are not read again until one of their unopened
            // parts opens or the app's schema range covers the write (§16.5).
            Some(FileEvidence::Unproven(unproven)) if !unproven.changed(opens_part, versions) => {
                return Ok(FileEvidence::Unproven(unproven))
            }
            _ => (),
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.read()?;
        let mut stream = retained.stream(storage).await;
        let ready = crate::write_object::download(&mut stream, ring.as_ref(), log, replays, || {
            Ok(self.operation_member()?.member())
        })
        .await?;
        let crate::write_object::ReadyWrite {
            opened,
            signer,
            parts: mut opens,
        } = match ready {
            Ok(ready) => ready,
            // A key this device lacks is that key's wait (§11, §16.5).
            Err(crate::write_object::Unready::Key { audience, key }) => {
                let evidence =
                    FileEvidence::Unproven(UnprovenFiles::Parts(BTreeSet::from([(audience, key)])));
                retained.keep_files(evidence.clone()).await?;
                return Ok(evidence);
            }
            Err(unready) => return Err(unready.into_snapshot_error()),
        };
        let ring = ring.as_ref().expect("opened header has its store key");
        for (opens, part) in opens.iter_mut().zip(&opened.header.parts) {
            *opens &= readable.contains(&part.audience);
        }
        if opens.contains(&false) {
            stream.drain().await?;
            let evidence = FileEvidence::Unproven(UnprovenFiles::Parts(opened.unopened(&opens)));
            retained.keep_files(evidence.clone()).await?;
            return Ok(evidence);
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
        let (transferred, checked) = tokio::join!(
            transfer,
            self.database.write_file_references(input, BTreeSet::new())
        );
        transferred?;
        let evidence =
            checked.map_err(|error| crate::write_object::database_failure(&object.path, error))?;
        retained.keep_files(evidence.clone()).await?;
        Ok(evidence)
    }
}

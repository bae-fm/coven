//! Uploaded-file deletion is derived from checked references in retained data.
use super::snapshot_file_scan::{Listing, UploaderScan};
use crate::{
    history::History,
    object_input::ObjectInput,
    sequence_sync::sequence_operations::{Progress, SequenceOperation, Step},
    sequence_sync::{SequenceSync, Session},
    slot_object::{self, SlotBody},
    snapshot_authorship::SnapshotAuthorship,
    snapshot_object::{self, SnapshotObject},
    OperationKind, OperationRejection, PendingReason, PendingRecord, PendingSubject,
    RequiredUpdate, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{DownloadedPartStream, FileEvidence, NewOperation, UnprovenFiles};
use coven_format::{sealed_write::WriteObjectPrefix, sequence::SnapshotAnnouncement};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_storage::{ObjectPath, StorageFailure};
use std::{collections::BTreeSet, sync::Arc};

const SEQUENCE_FILES: &str = "sequence-files";

impl SequenceSync {
    /// The uploaders whose files this device deletes: its own, and each
    /// removed device it is responsible for (§17.8).
    fn file_uploaders(&self, session: &Session, member: &MemberIdentity) -> BTreeSet<DeviceId> {
        session
            .history
            .state
            .devices
            .keys()
            .copied()
            .filter(|uploader| {
                session
                    .history
                    .deletes_for(self.device, member.member(), *uploader)
            })
            .collect()
    }

    /// The local snapshot capture still in progress, if any: its journal owns
    /// references no longer present in rows until publication or awaited undo.
    fn unfinished_capture(
        records: &[coven_database::OperationRecord],
    ) -> Result<Option<coven_database::OperationId>, SyncError> {
        for record in records.iter().filter(|record| record.kind == "sequence") {
            let work: SequenceOperation = serde_json::from_slice(&record.data)?;
            let step = match work.progress {
                Progress::Running(step) | Progress::Undo { step, .. } => step,
            };
            if matches!(
                step,
                Step::Capturing(_)
                    | Step::Uploading(_)
                    | Step::Announcing(_)
                    | Step::Activating { .. }
                    | Step::SnapshotApplied { .. }
            ) {
                return Ok(Some(record.id));
            }
        }
        Ok(None)
    }

    /// The journal row owning file deletion, started when absent.
    async fn files_operation(
        &self,
        existing: Option<&coven_database::OperationRecord>,
    ) -> Result<coven_database::OperationId, SyncError> {
        Ok(match existing {
            Some(record) => record.id,
            None => {
                self.database
                    .start_operation(NewOperation {
                        kind: SEQUENCE_FILES.into(),
                        data: Vec::new(),
                        started_by: "coven".into(),
                        pauses_writes: false,
                    })
                    .await?
            }
        })
    }

    /// A pass's file deletion: list each removed uploader's folder once at
    /// its removal, then delete every unreferenced candidate. A failure is
    /// held by the operation, which retries under backoff (§17.8).
    pub(crate) async fn retain_files(
        &self,
        session: &Session,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let uploaders = self.file_uploaders(session, member);
        let scans = self
            .uploader_scans(&session.history, &uploaders, Listing::AtRemoval)
            .await?;
        let candidates = self.database.file_candidates(uploaders.clone()).await?;
        let records = self.database.operations().await?;
        let existing = records.iter().find(|record| record.kind == SEQUENCE_FILES);
        if scans.is_empty() && candidates.is_empty() && existing.is_none() {
            return Ok(());
        }
        if Self::unfinished_capture(&records)?.is_some() {
            return Ok(());
        }
        if let Some(record) = existing {
            if !self.operation_ready(record, ring).await? {
                return Ok(());
            }
        }
        let id = self.files_operation(existing).await?;
        let result = self
            .waits
            .begin_attempt(id)
            .run(self.delete_files(session, ring, member, uploaders, scans, id))
            .await;
        match result {
            Ok(()) => {
                self.database.finish_operation(id).await?;
                self.waits.operation_advanced(id);
            }
            Err(error) => {
                self.record_operation_error(
                    id,
                    OperationKind::RetainFiles,
                    crate::sequence_sync::OperationFailure::Held(error),
                    ring,
                )
                .await?
            }
        }
        Ok(())
    }

    /// The app's cleanup of removed devices' files (§17.8): list the folder of
    /// every removed device this device is responsible for again, then delete
    /// every file found that nothing refers to. It returns once that is done,
    /// or with the failure that stopped it, which the operation also records.
    /// A device whose removal applied later uploads only files that can never
    /// be referred to, so running this at any time is safe.
    pub async fn clean_up_removed_files(&self) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        if !self.registered(&session) {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let uploaders = self.file_uploaders(&session, &member);
        let scans = self
            .uploader_scans(&session.history, &uploaders, Listing::Every)
            .await?;
        let records = self.database.operations().await?;
        if let Some(capture) = Self::unfinished_capture(&records)? {
            return Err(SyncError::SnapshotPending(capture));
        }
        let existing = records.iter().find(|record| record.kind == SEQUENCE_FILES);
        let id = self.files_operation(existing).await?;
        let result = self
            .waits
            .begin_attempt(id)
            .run(self.delete_files(&session, &ring, &member, uploaders, scans, id))
            .await;
        match result {
            Ok(()) => {
                self.database.finish_operation(id).await?;
                self.waits.operation_advanced(id);
                Ok(())
            }
            Err(error) => {
                self.record_operation_error(
                    id,
                    OperationKind::RetainFiles,
                    crate::sequence_sync::OperationFailure::Returned(&error),
                    &ring,
                )
                .await?;
                Err(error)
            }
        }
    }

    /// List the given folders, then delete each candidate that no retained
    /// slot or snapshot refers to and nothing local protects.
    async fn delete_files(
        &self,
        session: &Session,
        ring: &StoreKeyring,
        member: &MemberIdentity,
        uploaders: BTreeSet<DeviceId>,
        scans: Vec<UploaderScan>,
        id: coven_database::OperationId,
    ) -> Result<(), SyncError> {
        self.scan_uploaders(scans).await?;
        let candidates = self.database.file_candidates(uploaders).await?;
        let protected: Vec<_> = candidates
            .iter()
            .filter(|file| file.protected)
            .map(|file| (file.device, file.file))
            .collect();
        self.database
            .settle_file_candidates(protected, Vec::new())
            .await?;
        if candidates.iter().all(|file| file.protected) {
            return Ok(());
        }
        let mut referenced = BTreeSet::new();
        let first = session
            .floor
            .as_ref()
            .map_or(1, |floor| floor.header.first.0);
        for number in first..=session.through {
            let path = self.path(number)?;
            let input = match ObjectInput::retained(&self.database, &path).await? {
                Some(input) => input,
                None => self.fetch(&path).await?.ok_or(StorageFailure::NotFound)?,
            };
            let evidence = self
                .slot_files(&input, &session.history, ring, member)
                .await?;
            referenced.extend(self.complete_files(evidence, id)?);
            input.release().await?;
        }
        for announcement in session.history.announcements.values() {
            if self
                .database
                .snapshot_absent(announcement.snapshot.clone())
                .await?
            {
                continue;
            }
            let evidence = self
                .snapshot_files(announcement, &session.history, ring)
                .await?;
            referenced.extend(self.complete_files(evidence, id)?);
        }
        // Local protection is checked after the asynchronous reference reads.
        for file in self
            .database
            .file_candidates(candidates.iter().map(|file| file.device).collect())
            .await?
        {
            if file.protected || referenced.contains(&(file.device, file.file)) {
                continue;
            }
            self.storage
                .delete(&ObjectPath::file(self.store(), file.device, file.file))
                .await?;
            self.database
                .settle_file_candidates(Vec::new(), vec![(file.device, file.file)])
                .await?;
        }
        Ok(())
    }

    fn complete_files(
        &self,
        evidence: FileEvidence,
        id: coven_database::OperationId,
    ) -> Result<BTreeSet<(DeviceId, FileId)>, SyncError> {
        match evidence {
            FileEvidence::Complete(files) => Ok(files),
            FileEvidence::Unproven(reason) => {
                let reason = match reason {
                    UnprovenFiles::Parts(parts) => {
                        let (audience, key) =
                            parts.into_iter().next().expect("unread parts name a key");
                        PendingReason::KeyUnavailable { audience, key }
                    }
                    UnprovenFiles::Schema(version) => {
                        PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version })
                    }
                };
                Err(SyncError::Pending(Box::new(PendingRecord::new(
                    PendingSubject::Operation {
                        id,
                        kind: OperationKind::RetainFiles,
                    },
                    reason,
                ))))
            }
        }
    }

    async fn reuse_files(
        &self,
        recorded: Option<FileEvidence>,
        history: &History,
        ring: &StoreKeyring,
    ) -> Result<Option<FileEvidence>, SyncError> {
        let Some(evidence) = recorded else {
            return Ok(None);
        };
        if let FileEvidence::Unproven(reason) = &evidence {
            if reason.changed(
                |audience, key| history.accepted(ring, audience, key),
                &self.database.write_schema_versions().await?,
            ) {
                return Ok(None);
            }
        }
        Ok(Some(evidence))
    }

    pub(crate) async fn slot_files(
        &self,
        input: &ObjectInput,
        history: &History,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<FileEvidence, SyncError> {
        if let Some(evidence) = self
            .reuse_files(input.files().await?, history, ring)
            .await?
        {
            return Ok(evidence);
        }
        input.receive(self.storage.as_ref()).await?;
        let received = slot_object::open(
            input.local_bytes().await?,
            &input.object().path,
            ring,
            history,
            |audience| crate::effects::belongs(&history.state, audience, &member.member()),
        )?;
        let mut files = BTreeSet::new();
        let mut unread = BTreeSet::new();
        let mut schema = None;
        if let (_, SlotBody::Writes(writes)) = received.into_parts() {
            for write in writes {
                let prefix = WriteObjectPrefix::decode(&write.authorship.evidence.prefix)?;
                let skipped = write
                    .header
                    .parts
                    .iter()
                    .zip(&write.parts)
                    .zip(prefix.part_keys)
                    .filter_map(|((header, part), key)| {
                        matches!(part, DownloadedPartStream::Skipped)
                            .then_some((header.audience.clone(), key))
                    })
                    .collect();
                match self.database.write_file_references(write, skipped).await? {
                    FileEvidence::Complete(references) => files.extend(references),
                    FileEvidence::Unproven(UnprovenFiles::Parts(parts)) => unread.extend(parts),
                    FileEvidence::Unproven(UnprovenFiles::Schema(version)) => {
                        schema = Some(version)
                    }
                }
            }
        }
        let evidence = match schema {
            Some(version) => FileEvidence::Unproven(UnprovenFiles::Schema(version)),
            None if !unread.is_empty() => FileEvidence::Unproven(UnprovenFiles::Parts(unread)),
            None => FileEvidence::Complete(files),
        };
        input.keep_files(evidence.clone()).await?;
        Ok(evidence)
    }

    pub(crate) async fn snapshot_files(
        &self,
        announcement: &SnapshotAnnouncement,
        history: &History,
        ring: &StoreKeyring,
    ) -> Result<FileEvidence, SyncError> {
        let path = snapshot_object::path(self.store(), &announcement.snapshot)?;
        let object =
            match SnapshotObject::read_prefix(&self.database, self.storage.as_ref(), &path).await {
                Ok(object) => object,
                Err(SyncError::Storage(error)) if error.failure() == StorageFailure::NotFound => {
                    self.database
                        .record_absent_snapshot(announcement.snapshot.clone(), BTreeSet::new())
                        .await?;
                    return Ok(FileEvidence::Complete(BTreeSet::new()));
                }
                Err(error) => return Err(error),
            };
        if let Some(evidence) = self
            .reuse_files(object.files().await?, history, ring)
            .await?
        {
            return Ok(evidence);
        }
        let prefix = object.prefix();
        let evidence =
            if !history.accepted(ring, &prefix.audience, prefix.key) {
                FileEvidence::Unproven(UnprovenFiles::Parts(BTreeSet::from([(
                    prefix.audience.clone(),
                    prefix.key,
                )])))
            } else if !self.database.write_schema_versions().await?.contains(
                &match prefix.purpose {
                    coven_format::sequence::Purpose::Raise(version) => version,
                    _ => self.database.schema_version().await?,
                },
            ) {
                let coven_format::sequence::Purpose::Raise(version) = prefix.purpose else {
                    unreachable!("current schema is supported");
                };
                FileEvidence::Unproven(UnprovenFiles::Schema(version))
            } else {
                let plaintext = object.plaintext(self.storage.as_ref(), ring).await?;
                let author = object
                    .authenticate(history, &announcement.snapshot, &announcement.digest)
                    .await?;
                if object.prefix().covers != announcement.covers
                    || object.prefix().purpose != announcement.purpose
                {
                    return Err(crate::write_object::damaged(
                        &path,
                        crate::Refusal::WrongIdentity { cause: None },
                    ));
                }
                let authorship = Arc::new(SnapshotAuthorship {
                    store: self.store(),
                    history: history.clone(),
                    ring: ring.clone(),
                });
                let files = self
                    .database
                    .validate_snapshot(
                        announcement.snapshot.clone(),
                        object.prefix().clone(),
                        plaintext,
                        author,
                        authorship,
                    )
                    .await
                    .map_err(|error| crate::write_object::database_failure(&path, error))?;
                FileEvidence::Complete(files)
            };
        object.keep_files(evidence.clone()).await?;
        object.release().await?;
        Ok(evidence)
    }
}

#[cfg(test)]
#[path = "sequence_files_tests.rs"]
mod tests;

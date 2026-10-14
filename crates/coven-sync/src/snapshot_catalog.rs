//! Prefix-only ordering followed by streamed authentication and database checks.

use super::{io, StoreLogSync};
use crate::write_object::{checked, damaged};
use crate::{snapshot_data::SavedSnapshot, Refusal, SyncError};
use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_crypto::{MemberId, ObjectHasher, Signature};
use coven_database::StoreLog;
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};
use coven_format::value::StorageTime;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, StoredObject};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct Candidate {
    pub(super) object: StoredObject,
    pub(super) prefix: SnapshotObjectPrefix,
    author: MemberId,
    signature: Signature,
}

/// A listed snapshot whose signed prefix verified, names this audience and
/// counts. Its positions establish coverage whatever this device found in its
/// body and whether it accepts its key: readers choose by the authenticated
/// prefix (§15, §19.1).
pub(super) struct Verified {
    pub(super) object: StoredObject,
    pub(super) prefix: SnapshotObjectPrefix,
}

pub(super) struct Catalog {
    pub(super) listed: usize,
    /// The verified snapshots this device may load: no body refusal it may
    /// not check again, and a key it accepts. Newest first.
    pub(super) candidates: Vec<Candidate>,
    /// Every verified snapshot, newest first. The newest that follows the
    /// replay's reset/version boundary is the audience's current newest.
    pub(super) verified: Vec<Verified>,
    /// Verified prefixes that follow the replay's reset/version boundary require
    /// history, even when their keys cannot authorize loading their rows.
    pub(super) required_positions: coven_format::value::WritePositions,
    pub(super) unreadable_prefix: bool,
    /// A listed prefix waits, for an update or for its author's registration:
    /// it may be the audience's current newest, so nothing that needs that
    /// newest snapshot can decide until the prefix reads (§15).
    pub(super) newest_unknown: bool,
    /// A listed snapshot in a newer format: work that selects among this
    /// audience's snapshots waits for that update (§15, §17.2).
    pub(super) update: Option<crate::RequiredUpdate>,
}

impl Verified {
    /// Whether the replay's newest reset/version boundary of this audience
    /// admits this snapshot.
    pub(super) fn follows_boundary(&self, log: &StoreLog) -> bool {
        super::boundaries::allows(
            log,
            &self
                .object
                .path
                .snapshot_id()
                .expect("checked snapshot path"),
            &self.prefix,
        )
    }
}

impl StoreLogSync {
    /// List an audience's snapshots and check each signed prefix. The listing
    /// decides each listed snapshot's record (§19.1): a prefix that waits or
    /// fails its check establishes no coverage, and a snapshot whose body was
    /// refused keeps its verified prefix's required history but is never a
    /// candidate again until a newer version permits one more check.
    pub(super) async fn snapshot_candidates(
        &self,
        audience: &Audience,
        log: &StoreLog,
    ) -> Result<Catalog, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        // A snapshot's authorized key stays eligible when replay drops its
        // introduction; an unauthorized or conflicting one never is (§11).
        let keys = crate::key_introductions::needed(log);
        let prefix = ObjectPrefix::audience_snapshots(self.directory.id(), audience);
        let objects = match self.reads.snapshot_listing(prefix.as_str()) {
            Some(objects) => objects,
            None => {
                let objects = storage.list(&prefix).await?;
                self.reads.keep_snapshot_listing(prefix.as_str(), &objects);
                objects
            }
        };
        let listed = objects.len();
        let mut candidates = Vec::new();
        let mut verified = Vec::new();
        let mut unreadable_prefix = false;
        let mut newest_unknown = false;
        let refused = self.refused_snapshots(audience).await?;
        let mut conditions = Vec::new();
        for object in objects {
            let id = object.path.snapshot_id().expect("snapshot path");
            let refusal = refused.iter().find(|(refused, _)| *refused == id);
            if let Some((_, reason)) = refusal {
                conditions.push((id.clone(), reason.clone()));
                // A prefix that never checked was not retained: its refusal
                // stands without reading the same bytes again.
                let input =
                    crate::object_input::ObjectInput::observe(&self.database, &object).await?;
                if input.metadata().await?.is_none() {
                    unreadable_prefix = true;
                    continue;
                }
            }
            let candidate = match self.snapshot_prefix(&object, log).await? {
                Ok(candidate) => candidate,
                // A wait or a refusal: no coverage until the prefix checks.
                Err(reason) => {
                    unreadable_prefix = true;
                    newest_unknown |= matches!(
                        reason,
                        PendingReason::UpdateRequired(_) | PendingReason::Waits(_)
                    );
                    if refusal.is_none() {
                        conditions.push((id, reason));
                    }
                    continue;
                }
            };
            let prefix = &candidate.prefix;
            if prefix.audience != *audience {
                unreadable_prefix = true;
                if refusal.is_none() {
                    let reason = self
                        .snapshot_condition(&object, Refusal::WrongIdentity { cause: None })
                        .await?;
                    conditions.push((id, reason));
                }
                continue;
            }
            // A landing more than a day after its signed catch-up time is
            // ignored for loading, selection and coverage (§15).
            if !prefix.counts(landing(&object)?) {
                tracing::debug!(path = object.path.as_str(), "late snapshot does not count");
                continue;
            }
            verified.push(Verified {
                object: object.clone(),
                prefix: prefix.clone(),
            });
            // A refused body never authorizes loading its rows (§19.1).
            if refusal.is_none() && keys.contains_key(&(audience.clone(), prefix.key)) {
                candidates.push(candidate);
            }
        }
        let update = conditions.iter().find_map(|(_, reason)| match reason {
            PendingReason::UpdateRequired(update) => Some(*update),
            _ => None,
        });
        self.replace_snapshot_conditions(audience, conditions)
            .await?;
        candidates.sort_by(|a, b| newest_first((&a.object, &a.prefix), (&b.object, &b.prefix)));
        verified.sort_by(|a, b| newest_first((&a.object, &a.prefix), (&b.object, &b.prefix)));
        // A snapshot prepared for a concurrent raise can include its author's
        // unpublished migration write. A boundary that excludes that snapshot
        // also excludes its demand for that write's history.
        let mut positions = BTreeMap::new();
        for snapshot in verified
            .iter()
            .filter(|snapshot| snapshot.follows_boundary(log))
        {
            super::include_positions(&mut positions, &snapshot.prefix.writes.0);
        }
        Ok(Catalog {
            listed,
            candidates,
            verified,
            required_positions: coven_format::value::WritePositions(
                positions
                    .into_iter()
                    .map(|(device, number)| coven_merge::WriteId { device, number })
                    .collect(),
            ),
            unreadable_prefix,
            newest_unknown,
            update,
        })
    }

    pub(super) async fn current_snapshot_candidates(
        &self,
        audience: &Audience,
        log: &StoreLog,
    ) -> Result<Catalog, SyncError> {
        let mut catalog = self.snapshot_candidates(audience, log).await?;
        catalog.candidates.retain(|candidate| {
            super::boundaries::allows(
                log,
                &candidate
                    .object
                    .path
                    .snapshot_id()
                    .expect("validated snapshot path"),
                &candidate.prefix,
            )
        });
        Ok(catalog)
    }

    /// This device's refusals of an audience's snapshots that this version
    /// may not check again (§19.1).
    async fn refused_snapshots(
        &self,
        audience: &Audience,
    ) -> Result<Vec<(coven_format::store_log::SnapshotId, PendingReason)>, SyncError> {
        let own = self.database.local_store_log().await?.device;
        let mut refused = Vec::new();
        for record in self.database.pending::<PendingRecord>().await? {
            if record.reported_by != own || !matches!(record.reason, PendingReason::Refused(_)) {
                continue;
            }
            let PendingSubject::Snapshot(id) = &record.subject else {
                continue;
            };
            if id.audience == *audience
                && !self
                    .database
                    .pending_can_retry::<PendingRecord>(record.subject.clone())
                    .await?
            {
                refused.push((id.clone(), record.reason));
            }
        }
        Ok(refused)
    }

    /// This audience's completed listing decides its snapshots' records: one
    /// that now checks, or is no longer listed, loses its record (§19.1).
    async fn replace_snapshot_conditions(
        &self,
        audience: &Audience,
        conditions: Vec<(coven_format::store_log::SnapshotId, PendingReason)>,
    ) -> Result<(), SyncError> {
        let records = if conditions.is_empty() {
            Vec::new()
        } else {
            let reported_by = self.database.local_store_log().await?.device;
            conditions
                .into_iter()
                .map(|(id, reason)| PendingRecord {
                    subject: PendingSubject::Snapshot(id),
                    reason,
                    reported_by,
                })
                .collect()
        };
        let audience = audience.clone();
        self.database
            .replace_local_pending(
                move |record: &PendingRecord| {
                    matches!(&record.subject, PendingSubject::Snapshot(id) if id.audience == audience)
                },
                records,
            )
            .await?;
        Ok(())
    }

    /// Read and authenticate a listed snapshot's signed prefix. The inner
    /// error is its first unmet condition: an author whose registration this
    /// device has not applied waits on the retained prefix bytes, a newer
    /// format waits for an update, and a failed check refuses the snapshot
    /// (§19.1). The outer error is a transport or local failure.
    pub(super) async fn snapshot_prefix(
        &self,
        object: &StoredObject,
        log: &StoreLog,
    ) -> Result<Result<Candidate, PendingReason>, SyncError> {
        match self.check_prefix(object, log).await {
            Ok(checked) => Ok(checked),
            Err(SyncError::UpdateRequired(update)) => {
                Ok(Err(PendingReason::UpdateRequired(update)))
            }
            Err(error) => {
                let failure = failure(&object.path, error)?;
                Ok(Err(self.snapshot_condition(object, failure).await?))
            }
        }
    }

    async fn check_prefix(
        &self,
        object: &StoredObject,
        log: &StoreLog,
    ) -> Result<Result<Candidate, PendingReason>, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let path = &object.path;
        let input = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        let bytes = match input.metadata().await? {
            Some(bytes) => bytes,
            None => {
                input
                    .prefix(
                        storage,
                        SnapshotObjectPrefix::MAX_SIGNED_PREFIX,
                        SnapshotObjectPrefix::signed_length,
                    )
                    .await?
            }
        };
        let length = checked(path, SnapshotObjectPrefix::length(&bytes))?;
        let signed = checked(
            path,
            bytes
                .get(..length + 64)
                .ok_or(coven_format::Error::Truncated),
        )?;
        let (prefix, signature) = checked(path, SnapshotObjectPrefix::decode_signed(signed))?;
        let author = match crate::object_author::member(log, path.device().expect("snapshot path"))
        {
            Ok(author) => author,
            Err(reason) => return Ok(Err(reason)),
        };
        author
            .verify_prefix(path.as_str(), &signed[..length], &signature)
            .map_err(|error| {
                damaged(
                    path,
                    crate::Refusal::Signature {
                        cause: Some(Arc::new(error)),
                    },
                )
            })?;
        if path
            .snapshot_id()
            .is_none_or(|id| id.audience != prefix.audience)
        {
            return Err(damaged(path, crate::Refusal::WrongIdentity { cause: None }));
        }
        input.keep_metadata(bytes).await?;
        Ok(Ok(Candidate {
            object: object.clone(),
            prefix,
            author: author.clone(),
            signature,
        }))
    }

    /// What a failed check means for a listed snapshot (§19.1): a newer format
    /// waits for an update; any other failure refuses its bytes, recorded only
    /// once they are completely downloaded. A reader that stopped at the
    /// prefix finishes the download first, and the ranges are then released.
    async fn snapshot_condition(
        &self,
        object: &StoredObject,
        failure: Refusal,
    ) -> Result<PendingReason, SyncError> {
        if let Some(update) = failure.required_update() {
            tracing::debug!(
                path = object.path.as_str(),
                ?update,
                "snapshot needs a newer coven"
            );
            return Ok(PendingReason::UpdateRequired(update));
        }
        let input = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        if !input.received().await? {
            input
                .stream(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
                .await
                .drain()
                .await?;
        }
        input.release_ranges().await?;
        tracing::warn!(path = object.path.as_str(), error = %failure, "snapshot is refused");
        Ok(PendingReason::Refused(failure))
    }

    /// Record a candidate's refused body as its snapshot's record; the
    /// listing passes it over from now on (§19.1). A body in a newer format
    /// fails the caller with that required update.
    pub(super) async fn refuse_snapshot(
        &self,
        object: &StoredObject,
        error: SyncError,
    ) -> Result<(), SyncError> {
        let failure = failure(&object.path, error)?;
        match self.snapshot_condition(object, failure).await? {
            PendingReason::UpdateRequired(update) => Err(SyncError::UpdateRequired(update)),
            reason => {
                self.database
                    .record_pending(PendingRecord {
                        subject: PendingSubject::Snapshot(
                            object.path.snapshot_id().expect("snapshot path"),
                        ),
                        reason,
                        reported_by: self.database.local_store_log().await?.device,
                    })
                    .await?;
                Ok(())
            }
        }
    }

    /// Own-counter evidence from every listed snapshot (§10): this device's
    /// own snapshot numbers and the signed coverage of its writes and entries.
    /// Rolled-back writes may already be deleted, so coverage is required. A
    /// prefix that cannot be checked yet supplies no evidence; a failed read or
    /// listing supplies none at all.
    pub(crate) async fn snapshot_identity_evidence(
        &self,
        log: &StoreLog,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<(crate::identity::Ends, u64), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut covered = crate::identity::Ends::default();
        let mut own = 0;
        for object in storage
            .list(&ObjectPrefix::snapshots(self.directory.id()))
            .await?
        {
            let Some(id) = object.path.snapshot_id() else {
                continue;
            };
            if id.device == device {
                own = own.max(id.number);
            }
            let prefix = match self.snapshot_prefix(&object, log).await {
                Ok(Ok(candidate)) => candidate.prefix,
                Ok(Err(_)) | Err(SyncError::Damaged(_)) => continue,
                Err(error) => return Err(error),
            };
            for write in prefix.writes.0.iter().filter(|w| w.device == device) {
                covered.writes = covered.writes.max(write.number);
            }
            for entry in prefix.store_log.0.iter().filter(|e| e.device == device) {
                covered.entries = covered.entries.max(entry.number);
            }
        }
        Ok((covered, own))
    }

    pub(super) async fn load_snapshot(
        &self,
        candidates: Vec<Candidate>,
        files: &mut impl Iterator<Item = String>,
    ) -> Result<Option<SavedSnapshot>, SyncError> {
        for candidate in candidates {
            let file = files.next().expect("reserved snapshot candidates");
            match self.open_snapshot(&candidate, &file).await {
                Ok(_) => {
                    return Ok(Some(SavedSnapshot {
                        path: candidate.object.path,
                        file,
                        prefix: candidate.prefix.encode()?,
                    }))
                }
                Err(SyncError::Database(coven_database::DbError::Snapshot(
                    coven_database::SnapshotError::Schema { snapshot, database },
                ))) => {
                    tracing::debug!(
                        path = candidate.object.path.as_str(),
                        snapshot,
                        database,
                        "snapshot schema is outside the app's supported range"
                    );
                }
                Err(error) => self.refuse_snapshot(&candidate.object, error).await?,
            }
            io::remove(&self.directory, &file)?;
        }
        Ok(None)
    }

    pub(super) async fn open_snapshot(
        &self,
        candidate: &Candidate,
        file: &str,
    ) -> Result<(), SyncError> {
        let mut writer = self
            .directory
            .file(
                coven_foundation::files::FileArea::AppProvided,
                &io::name(file)?,
            )
            .create_writer(self.directory.lock_read_only()?)?;
        let retained =
            crate::object_input::ObjectInput::observe(&self.database, &candidate.object).await?;
        self.stream_snapshot(candidate, &retained, &mut writer)
            .await?;
        writer.finish().await?;
        let files = self
            .database
            .validate_snapshot(
                candidate
                    .object
                    .path
                    .snapshot_id()
                    .expect("checked snapshot path"),
                candidate.prefix.clone(),
                io::SnapshotInput::open(&self.directory, file)?,
            )
            .await?;
        retained.keep_files(Some(files)).await?;
        Ok(())
    }

    pub(super) async fn snapshot_file_references(
        &self,
        candidate: &Candidate,
    ) -> Result<crate::pass_reads::FileReferences, SyncError> {
        let retained =
            crate::object_input::ObjectInput::observe(&self.database, &candidate.object).await?;
        if let Some(Some(files)) = retained.files().await? {
            return Ok(files);
        }
        let (mut send, input) = crate::stream_input::StreamInput::channel();
        let transfer = async {
            let result = self.stream_snapshot(candidate, &retained, &mut send).await;
            drop(send);
            result
        };
        let check = self.database.validate_snapshot(
            candidate
                .object
                .path
                .snapshot_id()
                .expect("checked snapshot path"),
            candidate.prefix.clone(),
            input,
        );
        let (transferred, checked) = tokio::join!(transfer, check);
        transferred?;
        let files = checked?;
        retained.keep_files(Some(files.clone())).await?;
        Ok(files)
    }

    async fn stream_snapshot(
        &self,
        candidate: &Candidate,
        input: &crate::object_input::ObjectInput,
        sink: &mut impl io::SnapshotSink,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self
            .store_keys
            .read()?
            .ok_or(SyncError::KeyUnavailable(candidate.prefix.key))?;
        let key = io::key(&ring, &candidate.prefix.audience, candidate.prefix.key)?;
        let prefix = candidate.prefix.encode()?;
        let path = &candidate.object.path;
        let mut stream = input.stream(storage).await;
        let result = async {
            let signed = stream.read_exact(prefix.len() + 64).await?;
            if signed[..prefix.len()] != prefix
                || signed[prefix.len()..] != *candidate.signature.as_bytes()
            {
                return Err(coven_storage::StorageFailure::Protocol
                    .with_source("snapshot bytes disagree with its retained signed prefix")
                    .into());
            }
            let mut hash = ObjectHasher::new();
            hash.update(&signed);
            let end = candidate
                .object
                .size
                .checked_sub(64)
                .ok_or(coven_format::Error::Truncated)?;
            let mut layout = SnapshotObjectLayout::new();
            while stream.position() < end {
                if end - stream.position() < 4 {
                    return Err(coven_format::Error::Truncated.into());
                }
                let mut piece = stream.read_exact(4).await?;
                let length = layout.chunk_length(&piece)?;
                if (length - 4) as u64 > end - stream.position() {
                    return Err(coven_format::Error::Truncated.into());
                }
                piece.extend(stream.read_exact(length - 4).await?);
                hash.update(&piece);
                let index = layout.index();
                let sealed = layout.decode_chunk(&piece)?;
                let plaintext = key.open_object_chunk(path.as_str(), &prefix, 0, index, sealed)?;
                sink.chunk(plaintext).await?;
            }
            if stream.position() != end {
                return Err(coven_format::Error::Truncated.into());
            }
            let signature = layout.read_signature(&stream.read_exact(64).await?)?;
            candidate
                .author
                .verify_object(path.as_str(), &hash.finish(), &signature)?;
            Ok(())
        }
        .await;
        let result = stream.checked(result).await;
        if result.is_ok() {
            stream.drain().await?;
        }
        result
    }
}

/// The writes a snapshot covers, counted over every log.
pub(crate) fn score(prefix: &SnapshotObjectPrefix) -> u128 {
    prefix.writes.0.iter().map(|id| u128::from(id.number)).sum()
}

/// Counting snapshots of one audience, newest first: the most covered writes,
/// then the smaller path by device and number (§15, IO `catalogCurrent`).
pub(crate) fn newest_first(
    (a, a_prefix): (&StoredObject, &SnapshotObjectPrefix),
    (b, b_prefix): (&StoredObject, &SnapshotObjectPrefix),
) -> std::cmp::Ordering {
    let path = |object: &StoredObject| {
        let id = object.path.snapshot_id().expect("checked snapshot path");
        (id.device, id.number)
    };
    score(b_prefix)
        .cmp(&score(a_prefix))
        .then_with(|| path(a).cmp(&path(b)))
}

/// The first complete publication time of a listed snapshot.
pub(crate) fn landing(object: &StoredObject) -> Result<StorageTime, SyncError> {
    StorageTime::from_system_time(object.stored_at).map_err(|_| {
        coven_storage::StorageFailure::Protocol
            .with_source("snapshot publication precedes the Unix epoch")
            .into()
    })
}

pub(super) fn inconsistent(reason: &'static str) -> SyncError {
    coven_database::DbError::Snapshot(coven_database::SnapshotError::Inconsistent(reason)).into()
}

/// The check failure an error reports for this object, or the error itself
/// when it is a transport or local failure rather than a verdict on bytes.
pub(super) fn failure(path: &ObjectPath, error: SyncError) -> Result<Refusal, SyncError> {
    Ok(match error {
        SyncError::Damaged(object) => object.failure,
        SyncError::Crypto(error @ coven_crypto::CryptoError::Unavailable(_)) => {
            return Err(error.into())
        }
        SyncError::Crypto(error @ coven_crypto::CryptoError::Signature) => Refusal::Signature {
            cause: Some(Arc::new(error)),
        },
        SyncError::Crypto(error) => Refusal::Decryption {
            cause: Some(Arc::new(error)),
        },
        SyncError::Format(error) => Refusal::from(error),
        SyncError::Database(error) => match crate::write_object::database_failure(path, error) {
            SyncError::Damaged(object) => object.failure,
            error => return Err(error),
        },
        error => return Err(error),
    })
}

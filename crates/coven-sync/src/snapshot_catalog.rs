//! Prefix-only ordering followed by streamed authentication and database checks.

use super::prefix::Candidate;
use super::{io, StoreLogSync};
use crate::{snapshot_data::SavedSnapshot, Refusal, SyncError};
use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_crypto::ObjectHasher;
use coven_database::StoreLog;
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};
use coven_merge::Audience;
use coven_storage::{ObjectPrefix, StoredObject};
use std::collections::BTreeMap;

/// A listed snapshot whose signed prefix verified, names this audience and
/// counts. Its positions establish coverage whatever this device found in its
/// body and whether it accepts its key: readers choose by the authenticated
/// prefix (§15, §19.1).
pub(super) struct Verified {
    pub(super) object: StoredObject,
    pub(super) prefix: SnapshotObjectPrefix,
    /// What loading its rows waits for: the snapshot itself, whose own record
    /// is this version's refusal of its body, or its key's introduction. None
    /// for a loading candidate.
    pub(super) load_waits: Option<PendingReason>,
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
    /// A listed prefix this device has not verified: it waits, for an update
    /// or for its author's registration, or this version refused it, and a
    /// later version may verify it (§19.1). It establishes no coverage, but
    /// it may be the audience's current newest, so nothing that needs that
    /// newest snapshot can decide until it verifies or is no longer listed
    /// (§15).
    pub(super) newest_unknown: bool,
    /// A listed snapshot in a newer format: work that selects among this
    /// audience's snapshots waits for that update (§15, §17.2).
    pub(super) update: Option<crate::RequiredUpdate>,
}

impl Catalog {
    /// Take the audience's current newest counting snapshot for loading: the
    /// newest verified prefix that follows the replay's reset/version
    /// boundary, whatever this device found in its body or key. Fails with
    /// what loading it waits for; an older snapshot never stands in for it
    /// (§15, §19.1; storelog-data `ReloadWait.choose`).
    pub(super) fn take_current(
        &mut self,
        log: &StoreLog,
    ) -> Result<Option<Candidate>, PendingReason> {
        let Some(newest) = self
            .verified
            .iter()
            .find(|snapshot| snapshot.follows_boundary(log))
        else {
            return Ok(None);
        };
        if let Some(reason) = &newest.load_waits {
            return Err(reason.clone());
        }
        let index = self
            .candidates
            .iter()
            .position(|candidate| candidate.object.path == newest.object.path)
            .expect("a verified snapshot without a load wait is a candidate");
        Ok(Some(self.candidates.swap_remove(index)))
    }
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
                    newest_unknown = true;
                    continue;
                }
            }
            let candidate = match self.snapshot_prefix(&object, log).await? {
                Ok(candidate) => candidate,
                // A wait or a refusal: no coverage until the prefix checks.
                Err(reason) => {
                    newest_unknown = true;
                    if refusal.is_none() {
                        conditions.push((id, reason));
                    }
                    continue;
                }
            };
            let prefix = &candidate.prefix;
            if prefix.audience != *audience {
                newest_unknown = true;
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
            if !prefix.counts(object.stored_at) {
                tracing::debug!(path = object.path.as_str(), "late snapshot does not count");
                continue;
            }
            // A refused body never authorizes loading its rows (§19.1).
            let load_waits = if refusal.is_some() {
                Some(PendingReason::Waits(crate::Prerequisite::Object(
                    object.path.clone(),
                )))
            } else if !keys.contains_key(&(audience.clone(), prefix.key)) {
                Some(PendingReason::KeyUnavailable {
                    audience: audience.clone(),
                    key: prefix.key,
                })
            } else {
                None
            };
            verified.push(Verified {
                object: object.clone(),
                prefix: prefix.clone(),
                load_waits: load_waits.clone(),
            });
            if load_waits.is_none() {
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

    /// Download an audience's current newest snapshot and save its
    /// authenticated plaintext into `file`; the reload transaction parses it
    /// (§15). Bytes that fail authentication are refused, and loading waits on
    /// that snapshot rather than passing it over for an older one: a version
    /// that loads the body selects it (§19.1; storelog-data `ReloadWait.choose`).
    pub(super) async fn download_snapshot(
        &self,
        candidate: Candidate,
        file: String,
    ) -> Result<Result<SavedSnapshot, PendingReason>, SyncError> {
        let error = match self.save_plaintext(&candidate, &file).await {
            Ok(()) => {
                return Ok(Ok(SavedSnapshot {
                    prefix: candidate.prefix.encode()?,
                    object: candidate.object,
                    file,
                }))
            }
            Err(error) => error,
        };
        self.refuse_snapshot(&candidate.object, error).await?;
        io::remove(&self.directory, &file)?;
        Ok(Err(PendingReason::Waits(crate::Prerequisite::Object(
            candidate.object.path,
        ))))
    }

    async fn save_plaintext(&self, candidate: &Candidate, file: &str) -> Result<(), SyncError> {
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
        // Validation holds the database writer; it reads only local input.
        retained
            .receive(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
            .await?;
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
                .signer
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

pub(super) fn inconsistent(reason: &'static str) -> SyncError {
    coven_database::DbError::Snapshot(coven_database::SnapshotError::Inconsistent(reason)).into()
}

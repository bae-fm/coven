//! Prefix-only ordering followed by streamed authentication and database checks.

use super::{io, StoreLogSync};
use crate::write_object::{checked, damaged};
use crate::{
    snapshot_data::{SavedSnapshot, SnapshotTask},
    SyncError, SyncResults,
};
use coven_crypto::{MemberId, ObjectHasher, Signature};
use coven_database::{EntryOutcome, OperationRecord, StoreLog};
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, StoredObject};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Candidate {
    pub(super) object: StoredObject,
    pub(super) prefix: SnapshotObjectPrefix,
    author: MemberId,
    signature: Signature,
}

pub(super) struct Catalog {
    pub(super) listed: usize,
    pub(super) candidates: Vec<Candidate>,
    /// Verified prefixes that follow the replay's reset/version boundary require
    /// history, even when their keys cannot authorize loading their rows.
    pub(super) required_positions: coven_format::value::WritePositions,
    pub(super) unreadable_prefix: bool,
}

impl StoreLogSync {
    pub(super) async fn snapshot_candidates(
        &self,
        audience: &Audience,
        log: &StoreLog,
        report: &mut SyncResults,
    ) -> Result<Catalog, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let keys: BTreeSet<_> = log
            .entries
            .iter()
            .filter(|entry| log.replay.entries[&entry.entry.position] == EntryOutcome::Kept)
            .flat_map(|entry| crate::store_log_keys::introduced(&entry.entry.change))
            .collect();
        let objects = storage
            .list(&ObjectPrefix::audience_snapshots(audience))
            .await?;
        let listed = objects.len();
        let mut candidates = Vec::new();
        let mut positions = BTreeMap::new();
        let mut unreadable_prefix = false;
        for object in objects {
            let candidate = match self.snapshot_prefix(&object, log).await {
                Ok(candidate) => candidate,
                Err(error) => {
                    unreadable_prefix = true;
                    snapshot_damage(report, &object.path, error)?;
                    continue;
                }
            };
            let prefix = &candidate.prefix;
            if prefix.audience != *audience {
                unreadable_prefix = true;
                snapshot_damage(
                    report,
                    &object.path,
                    inconsistent("snapshot path and audience differ"),
                )?;
                continue;
            }
            // A snapshot prepared for a concurrent raise can include its
            // author's unpublished migration write. A boundary that excludes
            // that snapshot also excludes its demand for that write's history.
            if super::boundaries::allows(
                log,
                &object.path.snapshot_id().expect("checked path"),
                prefix,
            ) {
                for id in &prefix.writes.0 {
                    positions
                        .entry(id.device)
                        .and_modify(|n: &mut u64| *n = (*n).max(id.number))
                        .or_insert(id.number);
                }
            }
            if keys.contains(&(audience.clone(), prefix.key)) {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|a, b| {
            score(&b.prefix)
                .cmp(&score(&a.prefix))
                .then_with(|| a.object.path.cmp(&b.object.path))
        });
        Ok(Catalog {
            listed,
            candidates,
            required_positions: coven_format::value::WritePositions(
                positions
                    .into_iter()
                    .map(|(device, number)| coven_merge::WriteId { device, number })
                    .collect(),
            ),
            unreadable_prefix,
        })
    }

    pub(super) async fn current_snapshot_candidates(
        &self,
        audience: &Audience,
        log: &StoreLog,
        report: &mut SyncResults,
    ) -> Result<Catalog, SyncError> {
        let mut catalog = self.snapshot_candidates(audience, log, report).await?;
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

    pub(super) async fn snapshot_prefix(
        &self,
        object: &StoredObject,
        log: &StoreLog,
    ) -> Result<Candidate, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let path = &object.path;
        let author = crate::object_author::member(log, path)?;
        let length = object
            .size
            .min(SnapshotObjectPrefix::MAX_SIGNED_LENGTH as u64) as usize;
        if length == 0 {
            return checked(path, Err(coven_format::Error::Truncated));
        }
        let bytes = io::range(storage, path, 0, length).await?;
        let length = checked(path, SnapshotObjectPrefix::length(&bytes))?;
        let signed = checked(
            path,
            bytes
                .get(..length + 64)
                .ok_or(coven_format::Error::Truncated),
        )?;
        let (prefix, signature) = checked(path, SnapshotObjectPrefix::decode_signed(signed))?;
        author
            .verify_prefix(path.as_str(), &signed[..length], &signature)
            .map_err(|error| damaged(path, crate::ObjectCheckFailure::Signature(error)))?;
        if path
            .snapshot_id()
            .is_none_or(|id| id.audience != prefix.audience)
        {
            return Err(damaged(
                path,
                crate::write_object::invalid("snapshot path and audience differ"),
            ));
        }
        Ok(Candidate {
            object: object.clone(),
            prefix,
            author: author.clone(),
            signature,
        })
    }

    pub(super) async fn choose_snapshot(
        &self,
        candidates: Vec<Candidate>,
        record: &OperationRecord,
        task: &mut SnapshotTask,
        report: &mut SyncResults,
    ) -> Result<Option<SavedSnapshot>, SyncError> {
        for candidate in candidates {
            let file = self.reserve_snapshot_file(record, task).await?;
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
                Err(error) => snapshot_damage(report, &candidate.object.path, error)?,
            }
            io::remove(&self.directory, &file)?;
        }
        Ok(None)
    }

    pub(super) async fn open_snapshot(
        &self,
        candidate: &Candidate,
        file: &str,
    ) -> Result<
        BTreeSet<(
            coven_foundation::id_source::DeviceId,
            coven_foundation::id_source::FileId,
        )>,
        SyncError,
    > {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self
            .store_keys
            .unlock()?
            .ok_or(SyncError::KeyUnavailable(candidate.prefix.key))?;
        let key = io::key(&ring, &candidate.prefix.audience, candidate.prefix.key)?;
        let prefix = candidate.prefix.encode()?;
        let path = &candidate.object.path;
        let mut writer = self
            .directory
            .file(
                coven_foundation::files::FileArea::AppProvided,
                &io::name(file)?,
            )
            .create_writer(self.directory.lock_read_only()?)?;
        let mut hash = ObjectHasher::new();
        hash.update(&prefix);
        hash.update(candidate.signature.as_bytes());
        let mut offset = prefix.len() as u64 + 64;
        let end = candidate
            .object
            .size
            .checked_sub(64)
            .ok_or(coven_format::Error::Truncated)?;
        let mut layout = SnapshotObjectLayout::new();
        while offset < end {
            if end - offset < 4 {
                return Err(coven_format::Error::Truncated.into());
            }
            let mut piece = io::range(storage, path, offset, 4).await?;
            let length = layout.chunk_length(&piece)?;
            if length as u64 > end - offset {
                return Err(coven_format::Error::Truncated.into());
            }
            piece.extend(io::range(storage, path, offset + 4, length - 4).await?);
            hash.update(&piece);
            let index = layout.index();
            let sealed = layout.decode_chunk(&piece)?;
            let plaintext = key.open_object_chunk(path.as_str(), &prefix, 0, index, sealed)?;
            writer.append(&plaintext).await?;
            offset += length as u64;
        }
        if offset != end {
            return Err(coven_format::Error::Truncated.into());
        }
        let signature = layout.read_signature(&io::range(storage, path, end, 64).await?)?;
        candidate
            .author
            .verify_object(path.as_str(), &hash.finish(), &signature)?;
        writer.finish().await?;
        let inspection = self
            .database
            .validate_snapshot(
                path.snapshot_id()
                    .ok_or_else(|| inconsistent("snapshot has another path layout"))?,
                candidate.prefix.clone(),
                io::SnapshotInput::open(&self.directory, file)?,
            )
            .await?;
        Ok(inspection.files)
    }
}

pub(super) fn score(prefix: &SnapshotObjectPrefix) -> u128 {
    prefix.writes.0.iter().map(|id| u128::from(id.number)).sum()
}

pub(super) fn inconsistent(reason: &'static str) -> SyncError {
    coven_database::DbError::Snapshot(coven_database::SnapshotError::Inconsistent(reason)).into()
}

pub(super) fn snapshot_damage(
    report: &mut SyncResults,
    path: &ObjectPath,
    error: SyncError,
) -> Result<(), SyncError> {
    let failure = match error {
        SyncError::Damaged(object) => object.failure,
        SyncError::Crypto(error @ coven_crypto::CryptoError::Unavailable(_)) => {
            return Err(error.into())
        }
        SyncError::Crypto(error @ coven_crypto::CryptoError::Signature) => {
            crate::ObjectCheckFailure::Signature(error)
        }
        SyncError::Crypto(error @ coven_crypto::CryptoError::UnsupportedVersion(_)) => {
            crate::ObjectCheckFailure::Parse(std::sync::Arc::new(error))
        }
        SyncError::Crypto(error) => crate::ObjectCheckFailure::Decryption(error),
        SyncError::Format(error) => crate::ObjectCheckFailure::Parse(std::sync::Arc::new(error)),
        SyncError::Database(coven_database::DbError::Snapshot(
            error @ coven_database::SnapshotError::Read(_),
        )) => {
            return Err(coven_database::DbError::Snapshot(error).into());
        }
        SyncError::Database(coven_database::DbError::Snapshot(
            coven_database::SnapshotError::Format(error),
        )) => crate::ObjectCheckFailure::Parse(std::sync::Arc::new(error)),
        SyncError::Database(coven_database::DbError::Snapshot(error)) => {
            crate::ObjectCheckFailure::Parse(std::sync::Arc::new(error))
        }
        error => return Err(error),
    };
    StoreLogSync::damage(report, path, failure)
}

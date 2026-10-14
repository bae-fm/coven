//! Prefix-only ordering followed by streamed authentication and database checks.

use super::{io, StoreLogSync};
use crate::write_object::{checked, damaged};
use crate::{snapshot_data::SavedSnapshot, DamagedObject, SyncError};
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
        damages: &mut Vec<DamagedObject>,
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
        let mut positions = BTreeMap::new();
        let mut unreadable_prefix = false;
        for object in objects {
            let candidate = match self.snapshot_prefix(&object, log).await {
                Ok(candidate) => candidate,
                Err(error) => {
                    unreadable_prefix = true;
                    snapshot_damage(damages, &object.path, error)?;
                    continue;
                }
            };
            let prefix = &candidate.prefix;
            if prefix.audience != *audience {
                unreadable_prefix = true;
                snapshot_damage(
                    damages,
                    &object.path,
                    damaged(&object.path, crate::Refusal::WrongIdentity { cause: None }),
                )?;
                continue;
            }
            // A landing more than a day after its signed catch-up time is
            // ignored for loading, selection and coverage (§15).
            if !prefix.counts(landing(&object)?) {
                tracing::debug!(path = object.path.as_str(), "late snapshot does not count");
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
                super::include_positions(&mut positions, &prefix.writes.0);
            }
            if keys.contains_key(&(audience.clone(), prefix.key)) {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|a, b| newest_first((&a.object, &a.prefix), (&b.object, &b.prefix)));
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
        damages: &mut Vec<DamagedObject>,
    ) -> Result<Catalog, SyncError> {
        let mut catalog = self.snapshot_candidates(audience, log, damages).await?;
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
        Ok(Candidate {
            object: object.clone(),
            prefix,
            author: author.clone(),
            signature,
        })
    }

    pub(super) async fn load_snapshot(
        &self,
        candidates: Vec<Candidate>,
        files: &mut impl Iterator<Item = String>,
        damages: &mut Vec<DamagedObject>,
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
                Err(error) => snapshot_damage(damages, &candidate.object.path, error)?,
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

pub(super) fn snapshot_damage(
    damages: &mut Vec<DamagedObject>,
    path: &ObjectPath,
    error: SyncError,
) -> Result<(), SyncError> {
    let failure = match error {
        SyncError::Damaged(object) => object.failure,
        SyncError::Crypto(error @ coven_crypto::CryptoError::Unavailable(_)) => {
            return Err(error.into())
        }
        SyncError::Crypto(error @ coven_crypto::CryptoError::Signature) => {
            crate::Refusal::Signature {
                cause: Some(Arc::new(error)),
            }
        }
        SyncError::Crypto(error) => crate::Refusal::Decryption {
            cause: Some(Arc::new(error)),
        },
        SyncError::Format(error) => crate::Refusal::from(error),
        SyncError::Database(error) => match crate::write_object::database_failure(path, error) {
            SyncError::Damaged(object) => object.failure,
            error => return Err(error),
        },
        error => return Err(error),
    };
    StoreLogSync::damage(damages, path, failure)
}

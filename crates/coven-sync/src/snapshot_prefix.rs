//! Read and authenticate a listed snapshot's signed prefix, and decide what a
//! failed check of its prefix or body means for it (§19.1).

use super::StoreLogSync;
use crate::write_object::{checked, damaged};
use crate::{PendingReason, PendingRecord, PendingSubject, Refusal, SyncError};
use coven_crypto::{Signature, SigningKey};
use coven_database::StoreLog;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;
use coven_storage::{ObjectPath, ObjectPrefix, StoredObject};
use std::sync::Arc;

/// A listed snapshot whose signed prefix verified, with its signer.
pub(super) struct Candidate {
    pub(super) object: StoredObject,
    pub(super) prefix: SnapshotObjectPrefix,
    /// The member owning the snapshot's device in its signed view: whatever
    /// the snapshot names that member as writing needs no other evidence.
    pub(super) author: coven_foundation::id_source::MemberId,
    pub(super) signer: SigningKey,
    pub(super) signature: Signature,
}

impl StoreLogSync {
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
        // The signed coverage selects the signer; until verified it is only a
        // lookup hint, which can request entries but establishes nothing (D9).
        let (author, signer) = match crate::object_author::owner(
            log,
            path.store(),
            path.device().expect("snapshot path"),
            &prefix.store_log,
        ) {
            Ok(owner) => owner,
            Err(crate::object_author::Unresolved::Waits(reason)) => return Ok(Err(reason)),
            Err(crate::object_author::Unresolved::Refused(failure)) => {
                return Err(damaged(path, failure))
            }
        };
        signer
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
            author,
            signer,
            signature,
        }))
    }

    /// What a failed check means for a listed snapshot (§19.1): a newer format
    /// waits for an update; any other failure refuses its bytes, recorded only
    /// once they are completely downloaded. A reader that stopped at the
    /// prefix finishes the download first, and the ranges are then released.
    pub(super) async fn snapshot_condition(
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

use super::*;
use crate::blob::locator::{BlobLocator, StoredBlobRef};

/// The ownership change for a candidate whose operation has completed.
/// The operation retains its original claims until any returned target has been
/// deleted, then persists this disposition atomically with its completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingCandidateRelease {
    Retained(RemoteObjectRecord),
    DeleteProtocol(ExactObjectRef),
    DeleteBlob(StoredBlobRef),
}

impl RemoteObjectRecord {
    /// Remove a candidate's pending claim without asserting whether
    /// that exact candidate was accepted. The caller must hold the durable
    /// operation's completion or discard proof and select its exact manifest.
    /// This calculation neither changes the original row nor authorizes a new
    /// publication; the operation owns deletion and the final atomic row update.
    pub fn release_pending_candidate(
        mut self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<PendingCandidateRelease, RemoteObjectRecordError> {
        self.validate()?;
        let retain = match &mut self {
            Self::CandidateCommit(record) => {
                if record.identity != *candidate {
                    return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                }
                match record.state {
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                        false
                    }
                    CandidateCommitState::CleanupPending { .. }
                    | CandidateCommitState::AbsentVerified { .. } => {
                        return Err(RemoteObjectRecordError::InvalidCleanupTransition);
                    }
                }
            }
            Self::CandidateExclusive(record) => {
                if !matches!(
                    record.identity.domain,
                    CandidateExclusiveObjectDomain::StorePackage { .. }
                        | CandidateExclusiveObjectDomain::CirclePackage { .. }
                ) {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
                match &mut record.state {
                    CandidateObjectState::Prepared { ownership }
                    | CandidateObjectState::UploadedVerified { ownership } => {
                        release_pending_owner(ownership, candidate)?
                    }
                    CandidateObjectState::CleanupPending { .. }
                    | CandidateObjectState::AbsentVerified { .. } => {
                        return Err(RemoteObjectRecordError::InvalidCleanupTransition);
                    }
                }
            }
            Self::RetainedAuthority(record) => {
                let RetainedAuthorityObjectDomain::Commit { reference } = &record.identity.domain
                else {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                };
                if reference != candidate {
                    return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                }
                match &record.state {
                    RetainedAuthorityObjectState::UploadedVerified { ownership }
                        if ownership.activated.contains(candidate) =>
                    {
                        true
                    }
                    _ => return Err(RemoteObjectRecordError::InvalidActivation),
                }
            }
            Self::SharedLiveSet(record) => {
                if !matches!(
                    record.identity.domain,
                    SharedLiveSetObjectDomain::StoredBlob
                        | SharedLiveSetObjectDomain::StorePackage { .. }
                        | SharedLiveSetObjectDomain::CirclePackage { .. }
                ) {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
                match &mut record.state {
                    OwnedObjectState::Prepared { ownership } => {
                        release_pending_owner(ownership, candidate)?
                    }
                    OwnedObjectState::UploadedVerified { ownership } => {
                        if !ownership.pending.remove(candidate)
                            && !ownership
                                .activated
                                .contains(&SharedObjectOwner::StoreCommit(candidate.clone()))
                        {
                            return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                        }
                        !ownership.pending.is_empty() || !ownership.activated.is_empty()
                    }
                    OwnedObjectState::RetirementPending { .. } => {
                        return Err(RemoteObjectRecordError::InvalidCleanupTransition);
                    }
                }
            }
        };
        if retain {
            self.validate()?;
            return Ok(PendingCandidateRelease::Retained(self));
        }
        if let Self::SharedLiveSet(record) = &self {
            if matches!(
                record.identity.domain,
                SharedLiveSetObjectDomain::StoredBlob
            ) {
                let bytes = record
                    .payloads
                    .carried_locator_bytes()
                    .ok_or(RemoteObjectRecordError::PayloadPlacement)?;
                return Ok(PendingCandidateRelease::DeleteBlob(StoredBlobRef::new(
                    BlobLocator::parse(bytes)?,
                    record.identity.object.clone(),
                )?));
            }
        }
        Ok(PendingCandidateRelease::DeleteProtocol(
            self.object().clone(),
        ))
    }
}

fn release_pending_owner(
    ownership: &mut PendingCandidateOwnership,
    candidate: &StoreBatchCommitRef,
) -> Result<bool, RemoteObjectRecordError> {
    if !ownership.pending.remove(candidate) {
        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
    }
    Ok(!ownership.pending.is_empty())
}

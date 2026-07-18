//! Closed local publication and ownership state for remote protocol objects.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::circle::CircleId;
use super::storage::ExactObjectRef;
use super::store_commit::{CandidateFamilyId, ObjectHash, StoreBatchCommitRef, StreamActivationId};

const REMOTE_OBJECT_ID_DOMAIN: &[u8] = b"coven.remote-object-id.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RemoteObjectRecord {
    CandidateCommit(CandidateCommitRecord),
    CandidateExclusive(CandidateObjectRecord),
    RetainedAuthority(RetainedAuthorityRecord),
    SharedLiveSet(SharedObjectRecord),
}

impl RemoteObjectRecord {
    pub(crate) fn candidate_commit(
        identity: StoreBatchCommitRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = identity.object.clone();
        let record = Self::CandidateCommit(CandidateCommitRecord {
            identity,
            bytes: RemoteObjectBytes::inline(canonical_signed_bytes, stored_bytes, object)?,
            state: CandidateCommitState::Prepared,
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn candidate_activated_store_head(
        reference: super::store_commit::StoreDeviceHeadRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        let record = Self::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain: RetainedAuthorityObjectDomain::DeviceHead { reference },
                semantic_hash: ObjectHash::digest(&canonical_signed_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::inline(canonical_signed_bytes, stored_bytes, object)?,
            state: RetainedAuthorityObjectState::Prepared {
                ownership: PendingCandidateOwnership {
                    pending: BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn candidate_activated_store_acknowledgement(
        reference: super::store_commit::StoreAckRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        let record = Self::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain: RetainedAuthorityObjectDomain::Acknowledgement { reference },
                semantic_hash: ObjectHash::digest(&canonical_signed_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::inline(canonical_signed_bytes, stored_bytes, object)?,
            state: RetainedAuthorityObjectState::Prepared {
                ownership: PendingCandidateOwnership {
                    pending: BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn snapshot_activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<Self, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            bytes: RemoteObjectBytes::blob(locator_bytes, stored.object().clone())?,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            bytes: RemoteObjectBytes::blob(locator_bytes, stored.object().clone())?,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn merge_blob_activation(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
            || record.bytes.stored().object() != stored.object()
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        match &mut record.state {
            OwnedObjectState::Prepared { ownership } => {
                let mut pending = ownership.pending.clone();
                pending.remove(owner);
                record.state = OwnedObjectState::UploadedVerified {
                    ownership: SharedObjectOwnership {
                        pending,
                        activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                        nonactivated: ownership.nonactivated.clone(),
                    },
                };
            }
            OwnedObjectState::UploadedVerified { ownership } => {
                ownership.pending.remove(owner);
                ownership
                    .activated
                    .insert(SharedObjectOwner::StoreCommit(owner.clone()));
            }
            OwnedObjectState::RetirementPending { former_candidates } => {
                record.state = OwnedObjectState::UploadedVerified {
                    ownership: SharedObjectOwnership {
                        pending: BTreeSet::new(),
                        activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                        nonactivated: former_candidates.clone(),
                    },
                };
            }
        }
        self.validate()
    }

    pub(crate) fn merge_snapshot_owner(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        match &mut record.state {
            OwnedObjectState::UploadedVerified { ownership } => {
                ownership
                    .activated
                    .insert(SharedObjectOwner::Snapshot(owner));
            }
            OwnedObjectState::RetirementPending { former_candidates } => {
                record.state = OwnedObjectState::UploadedVerified {
                    ownership: SharedObjectOwnership {
                        pending: BTreeSet::new(),
                        activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                        nonactivated: former_candidates.clone(),
                    },
                };
            }
            OwnedObjectState::Prepared { .. } => {
                return Err(RemoteObjectRecordError::InvalidActivation);
            }
        }
        self.validate()
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::CandidateCommit(record) => &record.identity.object,
            Self::CandidateExclusive(record) => &record.identity.object,
            Self::RetainedAuthority(record) => &record.identity.object,
            Self::SharedLiveSet(record) => &record.identity.object,
        }
    }

    pub(crate) fn bytes(&self) -> &RemoteObjectBytes {
        match self {
            Self::CandidateCommit(record) => &record.bytes,
            Self::CandidateExclusive(record) => &record.bytes,
            Self::RetainedAuthority(record) => &record.bytes,
            Self::SharedLiveSet(record) => &record.bytes,
        }
    }

    pub(crate) fn object_id(&self) -> ObjectHash {
        remote_object_id(self.object())
    }

    pub(crate) fn is_activated_stored_blob(&self) -> bool {
        matches!(
            self,
            Self::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
                    && matches!(
                        &record.state,
                        OwnedObjectState::UploadedVerified { ownership }
                            if !ownership.activated.is_empty()
                    )
        )
    }

    pub(crate) fn snapshot_owners(&self) -> impl Iterator<Item = &SnapshotObjectOwner> {
        let owners = match self {
            Self::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob =>
            {
                match &record.state {
                    OwnedObjectState::UploadedVerified { ownership } => Some(&ownership.activated),
                    OwnedObjectState::Prepared { .. }
                    | OwnedObjectState::RetirementPending { .. } => None,
                }
            }
            _ => None,
        };
        owners.into_iter().flat_map(|owners| {
            owners.iter().filter_map(|owner| match owner {
                SharedObjectOwner::Snapshot(owner) => Some(owner),
                SharedObjectOwner::StoreCommit(_) => None,
            })
        })
    }

    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        self.bytes().validate()?;
        match self {
            Self::CandidateCommit(record) => {
                let commit: super::store_commit::StoreBatchCommit = serde_json::from_slice(
                    record.bytes.canonical_semantic_bytes(),
                )
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
                record
                    .identity
                    .verify_commit(&commit)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
                match &record.state {
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {}
                    CandidateCommitState::CleanupPending { proof }
                    | CandidateCommitState::AbsentVerified { proof } => {
                        proof.validate_for(&record.identity, &commit)?;
                    }
                }
            }
            Self::CandidateExclusive(record) => {
                record
                    .identity
                    .validate_semantic(record.bytes.canonical_semantic_bytes())?;
                let package = super::audience_package::AudiencePackage::parse(
                    record.bytes.canonical_semantic_bytes(),
                )
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
                match (&record.identity.domain, package.audience()) {
                    (
                        CandidateExclusiveObjectDomain::StorePackage,
                        super::audience_package::PackageAudience::Store,
                    ) => {}
                    (
                        CandidateExclusiveObjectDomain::CirclePackage { circle_id },
                        super::audience_package::PackageAudience::Circle {
                            circle_id: package_circle,
                            ..
                        },
                    ) if circle_id == package_circle => {}
                    _ => return Err(RemoteObjectRecordError::DomainMismatch),
                }
                record.state.validate()?;
            }
            Self::RetainedAuthority(record) => {
                validate_retained_authority_identity(
                    &record.identity,
                    record.bytes.canonical_semantic_bytes(),
                )?;
                if let RetainedAuthorityObjectDomain::DeviceHead { .. } = &record.identity.domain {
                    let head: super::store_commit::StoreDeviceHead = serde_json::from_slice(
                        record.bytes.canonical_semantic_bytes(),
                    )
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
                    let owns_head_commit = match &record.state {
                        RetainedAuthorityObjectState::Prepared { ownership } => {
                            ownership.pending.len() == 1 && ownership.pending.contains(&head.commit)
                        }
                        RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                            ownership.pending.contains(&head.commit)
                                || ownership.activated.contains(&head.commit)
                        }
                        RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                            ensure_candidate_nonactivation(former_candidates, &head.commit).is_ok()
                        }
                    };
                    if !owns_head_commit {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                }
                record.state.validate()?;
            }
            Self::SharedLiveSet(record) => {
                record
                    .identity
                    .validate_semantic(record.bytes.canonical_semantic_bytes())?;
                match record.identity.domain {
                    SharedLiveSetObjectDomain::StoredBlob => {
                        let locator = crate::blob::locator::BlobLocator::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        crate::blob::locator::StoredBlobRef::new(
                            locator,
                            record.identity.object.clone(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                    }
                    SharedLiveSetObjectDomain::StorePackage => {
                        let package = super::audience_package::AudiencePackage::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        if !matches!(
                            package.audience(),
                            super::audience_package::PackageAudience::Store
                        ) {
                            return Err(RemoteObjectRecordError::DomainMismatch);
                        }
                    }
                    SharedLiveSetObjectDomain::CirclePackage => {
                        let package = super::audience_package::AudiencePackage::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        if !matches!(
                            package.audience(),
                            super::audience_package::PackageAudience::Circle { .. }
                        ) {
                            return Err(RemoteObjectRecordError::DomainMismatch);
                        }
                    }
                }
                record.state.validate()?;
            }
        }
        if self.object() != self.bytes().stored().object() {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        Ok(())
    }

    pub(crate) fn into_activated(
        self,
        commit: &StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let activated = match self {
            Self::CandidateCommit(record) => {
                if &record.identity != commit
                    || !matches!(record.state, CandidateCommitState::UploadedVerified)
                {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                Self::RetainedAuthority(RetainedAuthorityRecord {
                    identity: RetainedAuthorityObjectRef {
                        domain: RetainedAuthorityObjectDomain::Commit {
                            reference: record.identity,
                        },
                        semantic_hash: ObjectHash::digest(record.bytes.canonical_semantic_bytes()),
                        object: record.bytes.stored().object().clone(),
                    },
                    bytes: record.bytes,
                    state: RetainedAuthorityObjectState::UploadedVerified {
                        ownership: CandidateOwnership {
                            pending: BTreeSet::new(),
                            activated: BTreeSet::from([commit.clone()]),
                            nonactivated: Vec::new(),
                        },
                    },
                })
            }
            Self::CandidateExclusive(record) => {
                let CandidateObjectState::UploadedVerified { ownership } = &record.state else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if ownership.pending.len() != 1 || !ownership.pending.contains(commit) {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                let domain = match record.identity.domain {
                    CandidateExclusiveObjectDomain::StorePackage => {
                        SharedLiveSetObjectDomain::StorePackage
                    }
                    CandidateExclusiveObjectDomain::CirclePackage { .. } => {
                        SharedLiveSetObjectDomain::CirclePackage
                    }
                };
                Self::SharedLiveSet(SharedObjectRecord {
                    identity: SharedLiveSetObjectRef {
                        domain,
                        semantic_hash: record.identity.semantic_hash,
                        object: record.identity.object,
                    },
                    bytes: record.bytes,
                    state: OwnedObjectState::UploadedVerified {
                        ownership: SharedObjectOwnership {
                            pending: BTreeSet::new(),
                            activated: BTreeSet::from([SharedObjectOwner::StoreCommit(
                                commit.clone(),
                            )]),
                            nonactivated: Vec::new(),
                        },
                    },
                })
            }
            Self::RetainedAuthority(mut record) => {
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut record.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if ownership.pending.remove(commit) {
                    ownership.activated.insert(commit.clone());
                } else if !ownership.activated.contains(commit) {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                Self::RetainedAuthority(record)
            }
            Self::SharedLiveSet(mut record) => {
                match &mut record.state {
                    OwnedObjectState::UploadedVerified { ownership } => {
                        if ownership.pending.remove(commit) {
                            ownership
                                .activated
                                .insert(SharedObjectOwner::StoreCommit(commit.clone()));
                        } else if !ownership
                            .activated
                            .contains(&SharedObjectOwner::StoreCommit(commit.clone()))
                        {
                            return Err(RemoteObjectRecordError::InvalidActivation);
                        }
                    }
                    OwnedObjectState::Prepared { .. } => {
                        return Err(RemoteObjectRecordError::InvalidActivation);
                    }
                    OwnedObjectState::RetirementPending { .. } => {
                        return Err(RemoteObjectRecordError::InvalidActivation);
                    }
                }
                Self::SharedLiveSet(record)
            }
        };
        activated.validate()?;
        Ok(activated)
    }

    pub(crate) fn mark_uploaded_verified(&mut self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::CandidateCommit(record) => match record.state {
                CandidateCommitState::Prepared => {
                    record.state = CandidateCommitState::UploadedVerified;
                }
                CandidateCommitState::UploadedVerified => {}
                CandidateCommitState::CleanupPending { .. }
                | CandidateCommitState::AbsentVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership } => {
                    record.state = CandidateObjectState::UploadedVerified {
                        ownership: ownership.clone(),
                    };
                }
                CandidateObjectState::UploadedVerified { .. } => {}
                CandidateObjectState::CleanupPending { .. }
                | CandidateObjectState::AbsentVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    record.state = RetainedAuthorityObjectState::UploadedVerified {
                        ownership: CandidateOwnership {
                            pending: ownership.pending.clone(),
                            activated: BTreeSet::new(),
                            nonactivated: ownership.nonactivated.clone(),
                        },
                    };
                }
                RetainedAuthorityObjectState::UploadedVerified { .. } => {}
                RetainedAuthorityObjectState::UncreatedVerified { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    record.state = OwnedObjectState::UploadedVerified {
                        ownership: SharedObjectOwnership {
                            pending: ownership.pending.clone(),
                            activated: BTreeSet::new(),
                            nonactivated: ownership.nonactivated.clone(),
                        },
                    };
                }
                OwnedObjectState::UploadedVerified { .. } => {}
                OwnedObjectState::RetirementPending { .. } => {
                    return Err(RemoteObjectRecordError::InvalidUploadTransition);
                }
            },
        }
        self.validate()
    }

    pub(crate) fn begin_candidate_nonactivation(
        &mut self,
        nonactivation: CandidateNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        let candidate = nonactivation.reference()?;
        match self {
            Self::CandidateCommit(record) => {
                if record.identity != candidate {
                    return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                }
                match &record.state {
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                        record.state = CandidateCommitState::CleanupPending {
                            proof: nonactivation.proof,
                        };
                    }
                    CandidateCommitState::CleanupPending { .. }
                    | CandidateCommitState::AbsentVerified { .. } => {}
                }
            }
            Self::CandidateExclusive(record) => match &mut record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        record.state = CandidateObjectState::CleanupPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                CandidateObjectState::CleanupPending { former_candidates }
                | CandidateObjectState::AbsentVerified { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
            Self::RetainedAuthority(record) => match &mut record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    let RetainedAuthorityObjectDomain::DeviceHead { .. } = &record.identity.domain
                    else {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    };
                    let CandidateNonactivationProof::MergeWinner { winner_head } =
                        &nonactivation.proof
                    else {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "a prepared Store head requires a Merge winner proof".to_string(),
                        ));
                    };
                    if winner_head.object.slot() != record.identity.object.slot()
                        || winner_head.object == record.identity.object
                    {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "Merge winner does not occupy the prepared head's exact slot"
                                .to_string(),
                        ));
                    }
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        record.state = RetainedAuthorityObjectState::UncreatedVerified {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                RetainedAuthorityObjectState::UploadedVerified { .. } => {
                    let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                        &record.state
                    else {
                        unreachable!("matched uploaded retained authority")
                    };
                    let mut ownership = ownership.clone();
                    if !ownership.pending.remove(&candidate) {
                        ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                        return Ok(None);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() && ownership.activated.is_empty() {
                        return ProtocolInertObject::new(
                            record.identity.clone(),
                            record.bytes.canonical_semantic_bytes().to_vec(),
                            ownership.nonactivated,
                        )
                        .map(Some);
                    }
                    record.state = RetainedAuthorityObjectState::UploadedVerified { ownership };
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
            Self::SharedLiveSet(record) => match &mut record.state {
                OwnedObjectState::Prepared { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        record.state = OwnedObjectState::RetirementPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                OwnedObjectState::UploadedVerified { ownership } => {
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() && ownership.activated.is_empty() {
                        record.state = OwnedObjectState::RetirementPending {
                            former_candidates: ownership.nonactivated.clone(),
                        };
                    }
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
            },
        }
        self.validate()?;
        Ok(None)
    }

    pub(crate) fn cleanup_target(&self) -> Option<&ExactObjectRef> {
        match self {
            Self::CandidateCommit(CandidateCommitRecord {
                state: CandidateCommitState::CleanupPending { .. },
                ..
            })
            | Self::CandidateExclusive(CandidateObjectRecord {
                state: CandidateObjectState::CleanupPending { .. },
                ..
            }) => Some(self.object()),
            _ => None,
        }
    }

    pub(crate) fn mark_absent_verified(&mut self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::CandidateCommit(record) => match &record.state {
                CandidateCommitState::CleanupPending { proof } => {
                    record.state = CandidateCommitState::AbsentVerified {
                        proof: proof.clone(),
                    };
                }
                CandidateCommitState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::CleanupPending { former_candidates } => {
                    record.state = CandidateObjectState::AbsentVerified {
                        former_candidates: former_candidates.clone(),
                    };
                }
                CandidateObjectState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::RetainedAuthority(_) | Self::SharedLiveSet(_) => {
                return Err(RemoteObjectRecordError::InvalidCleanupTransition);
            }
        }
        self.validate()
    }

    pub(crate) fn candidate_cleanup_complete(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<bool, RemoteObjectRecordError> {
        self.validate()?;
        let contains =
            |former: &[CandidateNonactivation]| -> Result<bool, RemoteObjectRecordError> {
                former
                    .iter()
                    .map(CandidateNonactivation::reference)
                    .try_fold(false, |found, reference| {
                        reference.map(|reference| found || &reference == candidate)
                    })
            };
        match self {
            Self::CandidateCommit(record) => Ok(&record.identity == candidate
                && matches!(record.state, CandidateCommitState::AbsentVerified { .. })),
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                CandidateObjectState::CleanupPending { .. } => Ok(false),
                CandidateObjectState::AbsentVerified { former_candidates } => {
                    contains(former_candidates)
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                    Ok(!ownership.pending.contains(candidate)
                        && !ownership.activated.contains(candidate)
                        && contains(&ownership.nonactivated)?)
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    contains(former_candidates)
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => Ok(!ownership
                    .pending
                    .contains(candidate)
                    && contains(&ownership.nonactivated)?),
                OwnedObjectState::UploadedVerified { ownership } => {
                    Ok(!ownership.pending.contains(candidate)
                        && !ownership
                            .activated
                            .contains(&SharedObjectOwner::StoreCommit(candidate.clone()))
                        && contains(&ownership.nonactivated)?)
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    contains(former_candidates)
                }
            },
        }
    }

    pub(crate) fn candidate_nonactivation_proof(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<Option<&CandidateNonactivationProof>, RemoteObjectRecordError> {
        self.validate()?;
        match self {
            Self::CandidateCommit(record) => {
                if &record.identity != candidate {
                    return Ok(None);
                }
                match &record.state {
                    CandidateCommitState::CleanupPending { proof }
                    | CandidateCommitState::AbsentVerified { proof } => Ok(Some(proof)),
                    CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                        Ok(None)
                    }
                }
            }
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                CandidateObjectState::CleanupPending { former_candidates }
                | CandidateObjectState::AbsentVerified { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                OwnedObjectState::UploadedVerified { ownership } => {
                    find_nonactivation_proof(&ownership.nonactivated, candidate)
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    find_nonactivation_proof(former_candidates, candidate)
                }
            },
        }
    }
}

pub(crate) fn remote_object_id(object: &ExactObjectRef) -> ObjectHash {
    let mut material = REMOTE_OBJECT_ID_DOMAIN.to_vec();
    material.extend(serde_json::to_vec(object).expect("ExactObjectRef serialization cannot fail"));
    ObjectHash::digest(&material)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateObjectRecord {
    pub(crate) identity: CandidateExclusiveTarget,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: CandidateObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateCommitRecord {
    pub(crate) identity: StoreBatchCommitRef,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: CandidateCommitState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedAuthorityRecord {
    pub(crate) identity: RetainedAuthorityObjectRef,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: RetainedAuthorityObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedAuthorityObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: CandidateOwnership,
    },
    UncreatedVerified {
        former_candidates: Vec<CandidateNonactivation>,
    },
}

impl RetainedAuthorityObjectState {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } => ownership.validate(),
            Self::UploadedVerified { ownership } => ownership.validate(),
            Self::UncreatedVerified { former_candidates } => {
                validate_nonactivations(former_candidates)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedObjectRecord {
    pub(crate) identity: SharedLiveSetObjectRef,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: OwnedObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteObjectBytes {
    canonical_semantic_bytes: Vec<u8>,
    stored: RemoteStoredRepresentation,
}

impl RemoteObjectBytes {
    pub(crate) fn inline(
        canonical_semantic_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            canonical_semantic_bytes,
            stored: RemoteStoredRepresentation::Inline {
                bytes: stored_bytes,
                object,
            },
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn blob(
        canonical_semantic_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            canonical_semantic_bytes,
            stored: RemoteStoredRepresentation::Blob { object },
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn canonical_semantic_bytes(&self) -> &[u8] {
        &self.canonical_semantic_bytes
    }

    pub(crate) fn stored(&self) -> &RemoteStoredRepresentation {
        &self.stored
    }

    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match &self.stored {
            RemoteStoredRepresentation::Inline { bytes, object } => object
                .verify(bytes)
                .map_err(|error| RemoteObjectRecordError::StoredBytes(error.to_string())),
            RemoteStoredRepresentation::Blob { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RemoteStoredRepresentation {
    Inline {
        bytes: Vec<u8>,
        object: ExactObjectRef,
    },
    Blob {
        object: ExactObjectRef,
    },
}

impl RemoteStoredRepresentation {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Inline { object, .. } | Self::Blob { object } => object,
        }
    }

    pub(crate) fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes, .. } => Some(bytes),
            Self::Blob { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: PendingCandidateOwnership,
    },
    CleanupPending {
        former_candidates: Vec<CandidateNonactivation>,
    },
    AbsentVerified {
        former_candidates: Vec<CandidateNonactivation>,
    },
}

impl CandidateObjectState {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } | Self::UploadedVerified { ownership } => {
                ownership.validate()
            }
            Self::CleanupPending { former_candidates }
            | Self::AbsentVerified { former_candidates } => {
                validate_nonactivations(former_candidates)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateCommitState {
    Prepared,
    UploadedVerified,
    CleanupPending { proof: CandidateNonactivationProof },
    AbsentVerified { proof: CandidateNonactivationProof },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OwnedObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: SharedObjectOwnership,
    },
    RetirementPending {
        former_candidates: Vec<CandidateNonactivation>,
    },
}

impl OwnedObjectState {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } => ownership.validate(),
            Self::UploadedVerified { ownership } => ownership.validate(),
            Self::RetirementPending { former_candidates } => {
                validate_nonactivations(former_candidates)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingCandidateOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl PendingCandidateOwnership {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() {
            return Err(RemoteObjectRecordError::EmptyPendingOwnership);
        }
        validate_owner_partition(&self.pending, std::iter::empty(), &self.nonactivated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedObjectOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) activated: BTreeSet<SharedObjectOwner>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl SharedObjectOwnership {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() && self.activated.is_empty() {
            Err(RemoteObjectRecordError::EmptyOwnership)
        } else {
            let activated_commits = self.activated.iter().filter_map(|owner| match owner {
                SharedObjectOwner::StoreCommit(commit) => Some(commit),
                SharedObjectOwner::Snapshot(_) => None,
            });
            validate_owner_partition(&self.pending, activated_commits, &self.nonactivated)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) activated: BTreeSet<StoreBatchCommitRef>,
    pub(crate) nonactivated: Vec<CandidateNonactivation>,
}

impl CandidateOwnership {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() && self.activated.is_empty() {
            return Err(RemoteObjectRecordError::EmptyOwnership);
        }
        validate_owner_partition(&self.pending, self.activated.iter(), &self.nonactivated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedObjectOwner {
    StoreCommit(StoreBatchCommitRef),
    Snapshot(SnapshotObjectOwner),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotObjectOwner {
    pub(crate) activation: StreamActivationId,
    pub(crate) sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateExclusiveTarget {
    pub(crate) family: CandidateFamilyId,
    pub(crate) domain: CandidateExclusiveObjectDomain,
    pub(crate) semantic_hash: ObjectHash,
    pub(crate) object: ExactObjectRef,
}

impl CandidateExclusiveTarget {
    fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateExclusiveObjectDomain {
    StorePackage,
    CirclePackage { circle_id: CircleId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedLiveSetObjectRef {
    pub(crate) domain: SharedLiveSetObjectDomain,
    pub(crate) semantic_hash: ObjectHash,
    pub(crate) object: ExactObjectRef,
}

impl SharedLiveSetObjectRef {
    fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedLiveSetObjectDomain {
    StoredBlob,
    StorePackage,
    CirclePackage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedAuthorityObjectRef {
    pub(crate) domain: RetainedAuthorityObjectDomain,
    pub(crate) semantic_hash: ObjectHash,
    pub(crate) object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProtocolInertObject {
    pub(crate) identity: RetainedAuthorityObjectRef,
    pub(crate) canonical_semantic_bytes: Vec<u8>,
    pub(crate) former_candidates: Vec<CandidateNonactivation>,
}

impl ProtocolInertObject {
    fn new(
        identity: RetainedAuthorityObjectRef,
        canonical_semantic_bytes: Vec<u8>,
        former_candidates: Vec<CandidateNonactivation>,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            identity,
            canonical_semantic_bytes,
            former_candidates,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn object_id(&self) -> ObjectHash {
        remote_object_id(&self.identity.object)
    }

    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        validate_retained_authority_identity(&self.identity, &self.canonical_semantic_bytes)?;
        validate_nonactivations(&self.former_candidates)
    }

    pub(crate) fn candidate_nonactivation_proof(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<Option<&CandidateNonactivationProof>, RemoteObjectRecordError> {
        self.validate()?;
        find_nonactivation_proof(&self.former_candidates, candidate)
    }
}

impl RetainedAuthorityObjectRef {
    fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedAuthorityObjectDomain {
    Commit {
        reference: StoreBatchCommitRef,
    },
    DeviceHead {
        reference: super::store_commit::StoreDeviceHeadRef,
    },
    Acknowledgement {
        reference: super::store_commit::StoreAckRef,
    },
}

fn validate_retained_authority_identity(
    identity: &RetainedAuthorityObjectRef,
    canonical_semantic_bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    identity.validate_semantic(canonical_semantic_bytes)?;
    match &identity.domain {
        RetainedAuthorityObjectDomain::Commit { reference } => {
            let commit: super::store_commit::StoreBatchCommit =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_commit(&commit)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceHead { reference } => {
            let head: super::store_commit::StoreDeviceHead =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if head.head_hash() != reference.head_hash || reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::Acknowledgement { reference } => {
            let acknowledgement: super::store_commit::StoreAck =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if acknowledgement.registration != reference.registration
                || acknowledgement.sequence != reference.sequence
                || acknowledgement.ack_hash() != reference.ack_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
    }
    Ok(())
}

pub(crate) use super::store_commit::StoreBatchCommitDeletionTarget;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateNonactivation {
    pub(crate) candidate: StoreBatchCommitDeletionTarget,
    pub(crate) proof: CandidateNonactivationProof,
}

impl CandidateNonactivation {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        let commit: super::store_commit::StoreBatchCommit =
            serde_json::from_slice(&self.candidate.canonical_signed_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        if commit.policy() != self.candidate.coord.policy()
            || commit.seq() != self.candidate.coord.sequence()
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "candidate coordinate differs from its signed bytes".to_string(),
            ));
        }
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            self.candidate.coord.clone(),
            self.candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        self.proof.validate_for(&reference, &commit)
    }

    pub(crate) fn reference(&self) -> Result<StoreBatchCommitRef, RemoteObjectRecordError> {
        let commit: super::store_commit::StoreBatchCommit =
            serde_json::from_slice(&self.candidate.canonical_signed_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        StoreBatchCommitRef::from_commit(
            &commit,
            self.candidate.coord.clone(),
            self.candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateNonactivationProof {
    MergeWinner {
        winner_head: super::store_commit::StoreDeviceHeadRef,
    },
    SerialImmediateSuccessor {
        accepted_suffix: SerialAcceptedSuffix,
        losing_prefix: Vec<StoreBatchCommitDeletionTarget>,
    },
}

impl CandidateNonactivationProof {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::MergeWinner { .. } => Ok(()),
            Self::SerialImmediateSuccessor {
                accepted_suffix,
                losing_prefix,
            } => {
                accepted_suffix.validate()?;
                validate_losing_prefix(accepted_suffix, losing_prefix)
            }
        }
    }

    fn validate_for(
        &self,
        candidate: &StoreBatchCommitRef,
        commit: &super::store_commit::StoreBatchCommit,
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        match self {
            Self::MergeWinner { .. } => {
                if candidate.coord.policy() != crate::WritePolicy::MergeConcurrent {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "Merge winner proof names a non-Merge candidate".to_string(),
                    ));
                }
                Ok(())
            }
            Self::SerialImmediateSuccessor {
                accepted_suffix: _,
                losing_prefix,
            } => {
                if candidate.coord.policy() != crate::WritePolicy::Serial {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "Serial successor proof names a non-Serial candidate".to_string(),
                    ));
                }
                let last = losing_prefix.last().expect("validated nonempty");
                if last.coord != candidate.coord
                    || last.object != candidate.object
                    || last.canonical_signed_bytes != commit.to_bytes()
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "losing Serial prefix does not end at the discarded candidate".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

fn validate_losing_prefix(
    accepted: &SerialAcceptedSuffix,
    losing: &[StoreBatchCommitDeletionTarget],
) -> Result<(), RemoteObjectRecordError> {
    let accepted_first = accepted.commits.first().expect("validated nonempty");
    if losing.is_empty() {
        return Err(RemoteObjectRecordError::InvalidProof(
            "losing Serial prefix is empty".to_string(),
        ));
    }
    let mut predecessor = accepted.predecessor.clone();
    let mut previous = None;
    let mut first = None;
    for target in losing {
        let commit: super::store_commit::StoreBatchCommit =
            serde_json::from_slice(&target.canonical_signed_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let reference =
            StoreBatchCommitRef::from_commit(&commit, target.coord.clone(), target.object.clone())
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        if commit.order.predecessor() != predecessor.as_ref() {
            return Err(RemoteObjectRecordError::InvalidProof(
                "losing Serial prefix has a broken exact predecessor chain".to_string(),
            ));
        }
        let expected_sequence = previous
            .as_ref()
            .map(|previous: &StoreBatchCommitRef| previous.coord.sequence().checked_add(1))
            .unwrap_or(Some(accepted_first.coord.sequence()));
        if expected_sequence != Some(reference.coord.sequence()) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "losing Serial prefix coordinates are not consecutive".to_string(),
            ));
        }
        first.get_or_insert_with(|| reference.clone());
        predecessor = Some(reference.clone());
        previous = Some(reference);
    }
    let first_ref = first.expect("checked nonempty");
    if &first_ref == accepted_first || first_ref.coord.sequence() != accepted_first.coord.sequence()
    {
        return Err(RemoteObjectRecordError::InvalidProof(
            "accepted and losing Serial branches do not have different immediate successors"
                .to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SerialAcceptedSuffix {
    pub(crate) predecessor: Option<StoreBatchCommitRef>,
    pub(crate) commits: Vec<StoreBatchCommitRef>,
    pub(crate) canonical_signed_head_bytes: Vec<u8>,
    pub(crate) observed_version_hash: ObjectHash,
}

impl SerialAcceptedSuffix {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.commits.is_empty() {
            return Err(RemoteObjectRecordError::InvalidProof(
                "accepted Serial suffix is empty".to_string(),
            ));
        }
        let head: super::store_commit::StoreSerialHead =
            serde_json::from_slice(&self.canonical_signed_head_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let Some(last) = self.commits.last() else {
            unreachable!("checked nonempty")
        };
        if !matches!(head.state, super::store_commit::StoreSerialHeadState::Commit { commit, .. } if commit == *last)
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "accepted Serial head does not name the suffix tip".to_string(),
            ));
        }
        if self
            .commits
            .iter()
            .any(|commit| commit.coord.policy() != crate::WritePolicy::Serial)
            || self
                .predecessor
                .as_ref()
                .is_some_and(|predecessor| predecessor.coord.policy() != crate::WritePolicy::Serial)
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "accepted Serial suffix contains a non-Serial coordinate".to_string(),
            ));
        }
        let expected_first_sequence = match &self.predecessor {
            Some(predecessor) => predecessor.coord.sequence().checked_add(1),
            None => Some(1),
        };
        if expected_first_sequence != self.commits.first().map(|commit| commit.coord.sequence()) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "accepted Serial suffix does not begin immediately after its predecessor"
                    .to_string(),
            ));
        }
        for pair in self.commits.windows(2) {
            if pair[0].coord.sequence().checked_add(1) != Some(pair[1].coord.sequence()) {
                return Err(RemoteObjectRecordError::InvalidProof(
                    "accepted Serial suffix coordinates are not consecutive".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_nonactivations(
    nonactivated: &[CandidateNonactivation],
) -> Result<(), RemoteObjectRecordError> {
    if nonactivated.is_empty() {
        return Err(RemoteObjectRecordError::EmptyNonactivation);
    }
    let mut references = BTreeSet::new();
    for candidate in nonactivated {
        candidate.validate()?;
        if !references.insert(candidate.reference()?) {
            return Err(RemoteObjectRecordError::OverlappingOwnership);
        }
    }
    Ok(())
}

fn ensure_candidate_nonactivation(
    former_candidates: &[CandidateNonactivation],
    expected: &StoreBatchCommitRef,
) -> Result<(), RemoteObjectRecordError> {
    for candidate in former_candidates {
        if candidate.reference()? == *expected {
            return Ok(());
        }
    }
    Err(RemoteObjectRecordError::CandidateNonactivationMissing)
}

fn find_nonactivation_proof<'a>(
    former_candidates: &'a [CandidateNonactivation],
    expected: &StoreBatchCommitRef,
) -> Result<Option<&'a CandidateNonactivationProof>, RemoteObjectRecordError> {
    for candidate in former_candidates {
        if candidate.reference()? == *expected {
            return Ok(Some(&candidate.proof));
        }
    }
    Ok(None)
}

fn validate_owner_partition<'a>(
    pending: &BTreeSet<StoreBatchCommitRef>,
    activated: impl Iterator<Item = &'a StoreBatchCommitRef>,
    nonactivated: &[CandidateNonactivation],
) -> Result<(), RemoteObjectRecordError> {
    let activated = activated.cloned().collect::<BTreeSet<_>>();
    let mut former = BTreeSet::new();
    for candidate in nonactivated {
        candidate.validate()?;
        former.insert(candidate.reference()?);
    }
    if pending
        .iter()
        .any(|owner| activated.contains(owner) || former.contains(owner))
        || activated.iter().any(|owner| former.contains(owner))
        || former.len() != nonactivated.len()
    {
        return Err(RemoteObjectRecordError::OverlappingOwnership);
    }
    Ok(())
}

fn validate_semantic_hash(
    expected: ObjectHash,
    bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    let actual = ObjectHash::digest(bytes);
    if actual != expected {
        return Err(RemoteObjectRecordError::SemanticHashMismatch { expected, actual });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemoteObjectRecordError {
    #[error("prepared stored bytes do not match their exact reference: {0}")]
    StoredBytes(String),
    #[error("prepared stored reference differs from the closed identity reference")]
    StoredReferenceMismatch,
    #[error("prepared semantic hash is {actual}, expected {expected}")]
    SemanticHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("pending candidate ownership has no pending candidate")]
    EmptyPendingOwnership,
    #[error("candidate ownership sets overlap")]
    OverlappingOwnership,
    #[error("candidate ownership has no pending or activated owner")]
    EmptyOwnership,
    #[error("prepared canonical bytes do not parse as their claimed domain: {0}")]
    InvalidDomain(String),
    #[error("prepared canonical bytes disagree with their claimed domain")]
    DomainMismatch,
    #[error("remote object is not uploaded for the exact activating commit")]
    InvalidActivation,
    #[error("remote object cannot return to uploaded state after cleanup began")]
    InvalidUploadTransition,
    #[error("candidate nonactivation set is empty")]
    EmptyNonactivation,
    #[error("candidate nonactivation proof is invalid: {0}")]
    InvalidProof(String),
    #[error("candidate does not own this remote object")]
    CandidateOwnerMismatch,
    #[error("remote object does not retain this candidate's nonactivation proof")]
    CandidateNonactivationMissing,
    #[error("remote object is not awaiting exact candidate cleanup")]
    InvalidCleanupTransition,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::locator::{BlobLocator, RemoteAudience};
    use crate::storage::cloud::ObjectSlot;
    use crate::{BlobScope, KeyFingerprint};

    #[test]
    fn serial_accepted_suffix_rejects_merge_commit_coordinates() {
        let commit_bytes = b"accepted commit";
        let commit = StoreBatchCommitRef {
            coord: super::super::store_commit::StoreCommitCoord::MergeConcurrent {
                stream_id: super::super::causal_grants::AuthorStreamId::from_digest(
                    ObjectHash::digest(b"accepted stream"),
                ),
                sequence: 1,
            },
            commit_hash: ObjectHash::digest(commit_bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/commits/accepted.json".to_string())
                    .expect("valid accepted commit slot"),
                commit_bytes.len() as u64,
                ObjectHash::digest(commit_bytes),
            ),
        };
        let registration_bytes = b"accepted author registration";
        let author_registration = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().expect("valid test device id"),
            registration_hash: ObjectHash::digest(registration_bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/registrations/accepted.json".to_string())
                    .expect("valid accepted registration slot"),
                registration_bytes.len() as u64,
                ObjectHash::digest(registration_bytes),
            ),
        };
        let head = super::super::store_commit::StoreSerialHead {
            version: super::super::store_commit::STORE_PROTOCOL_VERSION,
            store_root_hash: ObjectHash::digest(b"accepted Store root"),
            state: super::super::store_commit::StoreSerialHeadState::Commit {
                author_registration,
                commit: commit.clone(),
            },
            signature: "test signature".to_string(),
        };
        let suffix = SerialAcceptedSuffix {
            predecessor: None,
            commits: vec![commit],
            canonical_signed_head_bytes: head.to_bytes(),
            observed_version_hash: ObjectHash::digest(b"observed version"),
        };

        assert!(matches!(
            suffix.validate(),
            Err(RemoteObjectRecordError::InvalidProof(_))
        ));
    }

    #[test]
    fn stored_blob_record_rejects_object_outside_locator_semantic_slot() {
        let uploader_bytes = b"uploader registration";
        let uploader = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().expect("valid test device id"),
            registration_hash: ObjectHash::digest(b"uploader registration semantic bytes"),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/registrations/uploader.json".to_string())
                    .expect("valid uploader registration slot"),
                uploader_bytes.len() as u64,
                ObjectHash::digest(uploader_bytes),
            ),
        };
        let locator = BlobLocator::opaque(
            "covers",
            "cover-a",
            uploader,
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            7,
            ObjectHash::digest(b"cover-a"),
        )
        .expect("valid locator");
        let canonical_semantic_bytes = locator.to_bytes();
        let stored_bytes = b"stored cover".to_vec();
        let object = ExactObjectRef::new(
            ObjectSlot::logical("covers/opaque/wrong-slot".to_string()).expect("valid slot"),
            stored_bytes.len() as u64,
            ObjectHash::digest(&stored_bytes),
        );
        let owner = StoreBatchCommitRef {
            coord: super::super::store_commit::StoreCommitCoord::Serial { sequence: 1 },
            commit_hash: ObjectHash::digest(b"commit semantic bytes"),
            object: ExactObjectRef::new(
                ObjectSlot::logical(format!(
                    "{}.json",
                    super::super::store_commit::commit_semantic_prefix(
                        super::super::store_commit::CandidateFamilyId::from_hash(
                            ObjectHash::digest(b"remote object test candidate family"),
                        ),
                        super::super::store_commit::SERIAL_STREAM_ID,
                        1,
                        ObjectHash::digest(b"commit semantic bytes"),
                    )
                ))
                .expect("valid slot"),
                1,
                ObjectHash::digest(b"commit"),
            ),
        };
        let record = RemoteObjectRecord::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&canonical_semantic_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::inline(canonical_semantic_bytes, stored_bytes, object)
                .expect("valid stored bytes"),
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });

        assert!(matches!(
            record.validate(),
            Err(RemoteObjectRecordError::InvalidDomain(_))
        ));
    }
}

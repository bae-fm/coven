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
    fn candidate_activated_retained_authority(
        domain: RetainedAuthorityObjectDomain,
        semantic_hash: ObjectHash,
        object: ExactObjectRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let record = Self::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain,
                semantic_hash,
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
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceHead { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_store_acknowledgement(
        reference: super::store_commit::StoreAckRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::Acknowledgement { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_device_exclusion_proposal(
        reference: super::store_commit::StoreDeviceExclusionProposalRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        let semantic_hash = ObjectHash::digest(&canonical_signed_bytes);
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceExclusionProposal { reference },
            semantic_hash,
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_device_exclusion_outcome(
        reference: super::store_commit::StoreDeviceExclusionOutcomeRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object().clone();
        let semantic_hash = ObjectHash::digest(&canonical_signed_bytes);
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceExclusionOutcome { reference },
            semantic_hash,
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_reclaim_evidence(
        reference: super::store_reclaim::ReclaimEvidenceRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ReclaimEvidence { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_reclaim_authorization(
        reference: super::store_reclaim::ReclaimAuthorizationRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ReclaimAuthorization { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_reclaim_receipt(
        reference: super::store_reclaim::ReclaimReceiptRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ReclaimReceipt { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
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

    pub(crate) fn validate_reclaimable_store_package(
        &self,
        target: &super::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        self.validate()?;
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        let package = super::audience_package::AudiencePackage::parse(
            record.bytes.canonical_semantic_bytes(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let expected_owner = SharedObjectOwner::StoreCommit(activation.clone());
        let expected_size = u64::try_from(record.bytes.canonical_semantic_bytes().len())
            .map_err(|_| RemoteObjectRecordError::InvalidReclaim)?;
        if !matches!(
            &record.identity.domain,
            SharedLiveSetObjectDomain::StorePackage { reference } if reference == target
        ) || record.identity.semantic_hash != target.content_hash
            || record.identity.object != target.object
            || package.candidate_family() != target.candidate_family
            || package.schema_version() != target.schema_version
            || expected_size != target.changeset_size
            || !matches!(
                &record.state,
                OwnedObjectState::UploadedVerified { ownership }
                    if ownership.pending.is_empty()
                        && ownership.activated.len() == 1
                        && ownership.activated.contains(&expected_owner)
            )
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
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
                validate_candidate_exclusive_identity(
                    &record.identity,
                    record.bytes.canonical_semantic_bytes(),
                )?;
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
                match &record.identity.domain {
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
                    SharedLiveSetObjectDomain::StorePackage { reference } => {
                        validate_package_reference(
                            reference,
                            None,
                            record.bytes.canonical_semantic_bytes(),
                            &record.identity.object,
                        )?;
                    }
                    SharedLiveSetObjectDomain::CirclePackage { reference } => {
                        validate_package_reference(
                            &reference.package,
                            Some(reference),
                            record.bytes.canonical_semantic_bytes(),
                            &record.identity.object,
                        )?;
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
                if let Some(domain) = record.identity.domain.shared_destination() {
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
                } else if let Some(domain) = record.identity.domain.retained_destination() {
                    Self::RetainedAuthority(RetainedAuthorityRecord {
                        identity: RetainedAuthorityObjectRef {
                            domain,
                            semantic_hash: record.identity.semantic_hash,
                            object: record.identity.object,
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
                } else {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
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

    pub(crate) fn add_retained_authority_candidate(
        &mut self,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::RetainedAuthority(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let RetainedAuthorityObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if ownership.activated.contains(&candidate)
            || ownership
                .nonactivated
                .iter()
                .map(CandidateNonactivation::reference)
                .collect::<Result<BTreeSet<_>, _>>()?
                .contains(&candidate)
            || !ownership.pending.insert(candidate)
        {
            return Err(RemoteObjectRecordError::OverlappingOwnership);
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
    StorePackage {
        reference: super::store_commit::StorePackageRef,
    },
    CirclePackage {
        reference: super::store_commit::CirclePackageRef,
    },
    CircleAccessLeaf {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: super::store_commit::CircleAccessLeafObjectRef,
    },
    CircleAccessEnvelope {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: super::store_commit::CircleAccessEnvelopeObjectRef,
    },
    SelfRetirement {
        reference: super::store_commit::StoreDeviceSelfRetirementRef,
    },
}

impl CandidateExclusiveObjectDomain {
    fn family(&self) -> CandidateFamilyId {
        match self {
            Self::StorePackage { reference } => reference.candidate_family,
            Self::CirclePackage { reference } => reference.package.candidate_family,
            Self::CircleAccessLeaf { family, .. } | Self::CircleAccessEnvelope { family, .. } => {
                *family
            }
            Self::SelfRetirement { reference } => reference.candidate_family,
        }
    }

    fn object(&self) -> &ExactObjectRef {
        match self {
            Self::StorePackage { reference } => &reference.object,
            Self::CirclePackage { reference } => &reference.package.object,
            Self::CircleAccessLeaf { reference, .. } => &reference.object,
            Self::CircleAccessEnvelope { reference, .. } => &reference.object,
            Self::SelfRetirement { reference } => &reference.object,
        }
    }

    pub(crate) fn shared_destination(&self) -> Option<SharedLiveSetObjectDomain> {
        match self {
            Self::StorePackage { reference } => Some(SharedLiveSetObjectDomain::StorePackage {
                reference: reference.clone(),
            }),
            Self::CirclePackage { reference } => Some(SharedLiveSetObjectDomain::CirclePackage {
                reference: reference.clone(),
            }),
            Self::CircleAccessLeaf { .. }
            | Self::CircleAccessEnvelope { .. }
            | Self::SelfRetirement { .. } => None,
        }
    }

    pub(crate) fn retained_destination(&self) -> Option<RetainedAuthorityObjectDomain> {
        match self {
            Self::CircleAccessLeaf {
                family,
                circle_id,
                reference,
            } => Some(RetainedAuthorityObjectDomain::CircleAccessLeaf {
                family: *family,
                circle_id: *circle_id,
                reference: reference.clone(),
            }),
            Self::CircleAccessEnvelope {
                family,
                circle_id,
                reference,
            } => Some(RetainedAuthorityObjectDomain::CircleAccessEnvelope {
                family: *family,
                circle_id: *circle_id,
                reference: reference.clone(),
            }),
            Self::SelfRetirement { reference } => {
                Some(RetainedAuthorityObjectDomain::SelfRetirement {
                    reference: reference.clone(),
                })
            }
            Self::StorePackage { .. } | Self::CirclePackage { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateObjectMaterial {
    pub(crate) object: ExactObjectRef,
    pub(crate) canonical_semantic_bytes: Vec<u8>,
    pub(crate) stored_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateObjectGraph {
    family: CandidateFamilyId,
    objects: Vec<CandidateExclusiveObjectDomain>,
}

impl CandidateObjectGraph {
    pub(crate) fn from_commit(
        commit: &super::store_commit::StoreBatchCommit,
    ) -> Result<Self, RemoteObjectRecordError> {
        let manifest = commit
            .verified_candidate_objects()
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let mut objects = Vec::new();
        for candidate in &manifest.objects {
            match candidate {
                super::store_commit::CandidateExclusiveObjectRef::StorePackage(reference) => {
                    objects.push(CandidateExclusiveObjectDomain::StorePackage {
                        reference: reference.clone(),
                    });
                }
                super::store_commit::CandidateExclusiveObjectRef::CirclePackage(reference) => {
                    objects.push(CandidateExclusiveObjectDomain::CirclePackage {
                        reference: reference.clone(),
                    });
                }
                super::store_commit::CandidateExclusiveObjectRef::CircleAccess {
                    circle_id,
                    access,
                } => {
                    objects.push(CandidateExclusiveObjectDomain::CircleAccessLeaf {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: access.leaf.clone(),
                    });
                    objects.push(CandidateExclusiveObjectDomain::CircleAccessEnvelope {
                        family: manifest.family,
                        circle_id: *circle_id,
                        reference: access.envelope.clone(),
                    });
                }
                super::store_commit::CandidateExclusiveObjectRef::SelfRetirement(reference) => {
                    objects.push(CandidateExclusiveObjectDomain::SelfRetirement {
                        reference: reference.clone(),
                    });
                }
            }
        }
        Ok(Self {
            family: manifest.family,
            objects,
        })
    }

    pub(crate) fn exact_objects(&self) -> impl Iterator<Item = &ExactObjectRef> {
        self.objects
            .iter()
            .map(CandidateExclusiveObjectDomain::object)
    }

    pub(crate) fn close(
        self,
        commit: &super::store_commit::StoreBatchCommit,
        owner: &StoreBatchCommitRef,
        materials: Vec<CandidateObjectMaterial>,
    ) -> Result<Vec<RemoteObjectRecord>, RemoteObjectRecordError> {
        owner
            .verify_commit(commit)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        if self.family != commit.candidate_family() {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let mut exact = std::collections::BTreeMap::new();
        for material in materials {
            if exact.insert(material.object.clone(), material).is_some() {
                return Err(RemoteObjectRecordError::DuplicateCandidateObject);
            }
        }
        validate_access_pairs(&self.objects, &exact)?;
        let mut records = Vec::with_capacity(self.objects.len());
        for domain in self.objects {
            let object = domain.object().clone();
            let material = exact
                .remove(&object)
                .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
            let record = RemoteObjectRecord::CandidateExclusive(CandidateObjectRecord {
                identity: CandidateExclusiveTarget {
                    family: domain.family(),
                    domain,
                    semantic_hash: ObjectHash::digest(&material.canonical_semantic_bytes),
                    object: object.clone(),
                },
                bytes: RemoteObjectBytes::inline(
                    material.canonical_semantic_bytes,
                    material.stored_bytes,
                    object,
                )?,
                state: CandidateObjectState::Prepared {
                    ownership: PendingCandidateOwnership {
                        pending: BTreeSet::from([owner.clone()]),
                        nonactivated: Vec::new(),
                    },
                },
            });
            record.validate()?;
            records.push(record);
        }
        if !exact.is_empty() {
            return Err(RemoteObjectRecordError::CandidateObjectInvented);
        }
        Ok(records)
    }
}

fn validate_access_pairs(
    objects: &[CandidateExclusiveObjectDomain],
    materials: &std::collections::BTreeMap<ExactObjectRef, CandidateObjectMaterial>,
) -> Result<(), RemoteObjectRecordError> {
    let mut index = 0;
    while index < objects.len() {
        let CandidateExclusiveObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference: leaf_ref,
        } = &objects[index]
        else {
            index += 1;
            continue;
        };
        let Some(CandidateExclusiveObjectDomain::CircleAccessEnvelope {
            family: envelope_family,
            circle_id: envelope_circle,
            reference: envelope_ref,
        }) = objects.get(index + 1)
        else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        if family != envelope_family
            || circle_id != envelope_circle
            || leaf_ref.owner_pubkey != envelope_ref.owner_pubkey
            || leaf_ref.recipient_slot != envelope_ref.recipient_slot
            || leaf_ref.leaf_id != envelope_ref.leaf_id
            || leaf_ref.leaf_hash != envelope_ref.leaf_hash
        {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let leaf_material = materials
            .get(&leaf_ref.object)
            .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
        let envelope_material = materials
            .get(&envelope_ref.object)
            .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
        let leaf: super::circle_control::CircleAccessLeaf =
            serde_json::from_slice(&leaf_material.canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let envelope: super::circle_control::AccessEnvelope =
            serde_json::from_slice(&envelope_material.canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let leaf_bytes = serde_json::to_vec(&leaf)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        if envelope.value_hash != ObjectHash::digest(&leaf_bytes)
            || ObjectHash::digest(&leaf_material.stored_bytes) != leaf_ref.leaf_hash
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        index += 2;
    }
    Ok(())
}

fn validate_candidate_exclusive_identity(
    identity: &CandidateExclusiveTarget,
    canonical_semantic_bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    identity.validate_semantic(canonical_semantic_bytes)?;
    if identity.family != identity.domain.family() || identity.object != *identity.domain.object() {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    match &identity.domain {
        CandidateExclusiveObjectDomain::StorePackage { reference } => {
            validate_package_reference(reference, None, canonical_semantic_bytes, &identity.object)
        }
        CandidateExclusiveObjectDomain::CirclePackage { reference } => validate_package_reference(
            &reference.package,
            Some(reference),
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference,
        } => validate_circle_access_leaf_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleAccessEnvelope {
            family,
            circle_id,
            reference,
        } => validate_circle_access_envelope_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::SelfRetirement { reference } => {
            validate_self_retirement_identity(reference, canonical_semantic_bytes, &identity.object)
        }
    }
}

fn validate_package_reference(
    reference: &super::store_commit::StorePackageRef,
    circle: Option<&super::store_commit::CirclePackageRef>,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let package = super::audience_package::AudiencePackage::parse(canonical_semantic_bytes)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let size = u64::try_from(canonical_semantic_bytes.len())
        .map_err(|_| RemoteObjectRecordError::DomainMismatch)?;
    if reference.object != *object
        || reference.content_hash != ObjectHash::digest(canonical_semantic_bytes)
        || reference.schema_version != package.schema_version()
        || reference.changeset_size != size
        || reference.candidate_family != package.candidate_family()
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    match (circle, package.audience()) {
        (None, super::audience_package::PackageAudience::Store) => Ok(()),
        (
            Some(reference),
            super::audience_package::PackageAudience::Circle {
                circle_id,
                control,
                key_fingerprint,
            },
        ) if reference.circle_id == *circle_id
            && reference.control == *control
            && reference.key_fingerprint == *key_fingerprint =>
        {
            Ok(())
        }
        _ => Err(RemoteObjectRecordError::DomainMismatch),
    }
}

fn validate_circle_access_leaf_identity(
    family: CandidateFamilyId,
    circle_id: CircleId,
    reference: &super::store_commit::CircleAccessLeafObjectRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let leaf: super::circle_control::CircleAccessLeaf =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let parsed_bytes = serde_json::to_vec(&leaf)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    if parsed_bytes != canonical_semantic_bytes
        || !leaf.verify_signature()
        || leaf.candidate_family != family
        || leaf.circle_id != circle_id
        || leaf.owner_pubkey != reference.owner_pubkey
        || leaf.epoch_id != reference.epoch_id
        || leaf.recipient_slot != reference.recipient_slot
        || leaf.leaf_id != reference.leaf_id
        || reference.leaf_hash != reference.object.stored_hash()
        || reference.object != *object
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

fn validate_circle_access_envelope_identity(
    family: CandidateFamilyId,
    circle_id: CircleId,
    reference: &super::store_commit::CircleAccessEnvelopeObjectRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let envelope: super::circle_control::AccessEnvelope =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let parsed_bytes = serde_json::to_vec(&envelope)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    if parsed_bytes != canonical_semantic_bytes
        || !crate::keys::verify_signature_hex(
            &envelope.owner_pubkey,
            &envelope.signature,
            &envelope.canonical_bytes(),
        )
        || envelope.candidate_family != family
        || envelope.circle_id != circle_id
        || envelope.owner_pubkey != reference.owner_pubkey
        || envelope.recipient_slot != reference.recipient_slot
        || envelope.control_hash != reference.control_hash
        || envelope.leaf_id != reference.leaf_id
        || envelope.leaf_hash != reference.leaf_hash
        || reference.object != *object
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

fn validate_self_retirement_identity(
    reference: &super::store_commit::StoreDeviceSelfRetirementRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let retirement: super::store_commit::StoreDeviceSelfRetirement =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    if retirement.to_bytes() != canonical_semantic_bytes
        || retirement.candidate_family != reference.candidate_family
        || retirement.target != reference.target
        || retirement.retiring_cut != reference.retiring_cut
        || retirement.retirement_hash() != reference.retirement_hash
        || reference.object != *object
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
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
    StorePackage {
        reference: super::store_commit::StorePackageRef,
    },
    CirclePackage {
        reference: super::store_commit::CirclePackageRef,
    },
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
    DeviceExclusionProposal {
        reference: super::store_commit::StoreDeviceExclusionProposalRef,
    },
    DeviceExclusionOutcome {
        reference: super::store_commit::StoreDeviceExclusionOutcomeRef,
    },
    ReclaimEvidence {
        reference: super::store_reclaim::ReclaimEvidenceRef,
    },
    ReclaimAuthorization {
        reference: super::store_reclaim::ReclaimAuthorizationRef,
    },
    ReclaimReceipt {
        reference: super::store_reclaim::ReclaimReceiptRef,
    },
    CircleAccessLeaf {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: super::store_commit::CircleAccessLeafObjectRef,
    },
    CircleAccessEnvelope {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: super::store_commit::CircleAccessEnvelopeObjectRef,
    },
    SelfRetirement {
        reference: super::store_commit::StoreDeviceSelfRetirementRef,
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
        RetainedAuthorityObjectDomain::DeviceExclusionProposal { reference } => {
            let proposal: super::store_commit::StoreDeviceExclusionProposal =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_proposal(&proposal)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceExclusionOutcome { reference } => {
            let outcome: super::store_commit::StoreDeviceExclusionOutcome =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if outcome.proposal() != reference.proposal()
                || outcome.outcome_hash() != reference.outcome_hash()
                || reference.object() != &identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimEvidence { reference } => {
            let value: super::store_reclaim::ReclaimEvidence =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimAuthorization { reference } => {
            let value: super::store_reclaim::ReclaimAuthorization =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_identity(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimReceipt { reference } => {
            let value: super::store_reclaim::ReclaimReceipt =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_identity(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference,
        } => validate_circle_access_leaf_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
        RetainedAuthorityObjectDomain::CircleAccessEnvelope {
            family,
            circle_id,
            reference,
        } => validate_circle_access_envelope_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
        RetainedAuthorityObjectDomain::SelfRetirement { reference } => {
            validate_self_retirement_identity(
                reference,
                canonical_semantic_bytes,
                &identity.object,
            )?;
        }
    }
    Ok(())
}

pub(crate) use super::store_commit::StoreBatchCommitDeletionTarget;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateNonactivation {
    candidate: StoreBatchCommitDeletionTarget,
    proof: CandidateNonactivationProof,
}

impl CandidateNonactivation {
    pub(crate) fn candidate(&self) -> &StoreBatchCommitDeletionTarget {
        &self.candidate
    }

    pub(crate) fn proof(&self) -> &CandidateNonactivationProof {
        &self.proof
    }

    #[cfg(test)]
    pub(crate) fn unverified_for_test(
        candidate: StoreBatchCommitDeletionTarget,
        proof: CandidateNonactivationProof,
    ) -> Self {
        Self { candidate, proof }
    }

    #[cfg(test)]
    pub(crate) fn proof_mut_for_test(&mut self) -> &mut CandidateNonactivationProof {
        &mut self.proof
    }

    /// Checks the shape of a receipt already admitted through
    /// `VerifiedCandidateNonactivation`; it does not recreate the live observation.
    pub(crate) fn validate_durable_shape(
        candidate: &StoreBatchCommitRef,
        commit: &super::store_commit::StoreBatchCommit,
        proof: CandidateNonactivationProof,
    ) -> Result<(), RemoteObjectRecordError> {
        let value = Self {
            candidate: StoreBatchCommitDeletionTarget {
                coord: candidate.coord.clone(),
                object: candidate.object.clone(),
                canonical_signed_bytes: commit.to_bytes(),
            },
            proof,
        };
        value.validate()
    }

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCandidateNonactivation {
    evidence: Box<VerifiedCandidateNonactivationEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VerifiedCandidateNonactivationEvidence {
    Merge {
        durable: CandidateNonactivation,
        winner_commit: StoreBatchCommitRef,
    },
    Serial {
        durable: CandidateNonactivation,
    },
}

impl VerifiedCandidateNonactivation {
    pub(crate) fn merge(
        observation: &super::store_outbound::VerifiedMergeWinner,
        candidate: StoreBatchCommitDeletionTarget,
        author: &super::store_commit::StoreDeviceRegistration,
    ) -> Result<Self, RemoteObjectRecordError> {
        let commit = candidate
            .verify_nonactivation_candidate(observation.store_root_hash(), author)
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let expected = observation.expected();
        let expected_commit = observation.expected_commit();
        let winner = observation.winner();
        let winner_prepared = observation.winner_prepared();
        let winner_commit = observation.winner_commit();
        if expected.store_root_hash != observation.store_root_hash()
            || commit.store_root_hash != observation.store_root_hash()
            || expected.author_registration != commit.author_registration
            || expected.commit.coord != reference.coord
            || expected_commit.author_registration != commit.author_registration
            || expected_commit.order.predecessor() != commit.order.predecessor()
            || winner.store_root_hash != observation.store_root_hash()
            || winner.author_registration != expected.author_registration
            || winner.commit.coord != expected.commit.coord
            || winner.successor.activation != expected.successor.activation
            || winner.successor.predecessor != expected.successor.predecessor
            || winner_prepared.reference().slot() != observation.expected_slot()
            || winner.commit == reference
            || winner.commit.commit_hash != winner_commit.commit_hash()
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "Merge winner observation is not bound to the losing candidate's exact activation point"
                    .to_string(),
            ));
        }
        winner
            .commit
            .verify_commit(winner_commit)
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let value = Self {
            evidence: Box::new(VerifiedCandidateNonactivationEvidence::Merge {
                durable: CandidateNonactivation {
                    candidate,
                    proof: CandidateNonactivationProof::MergeWinner {
                        winner_head: super::store_commit::StoreDeviceHeadRef {
                            head_hash: winner.head_hash(),
                            object: winner_prepared.reference().clone(),
                        },
                    },
                },
                winner_commit: winner.commit.clone(),
            }),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub(crate) fn serial(
        observation: &super::store_pull::VerifiedSerialAcceptedSuffix,
        losing: Vec<(
            StoreBatchCommitDeletionTarget,
            super::store_commit::StoreDeviceRegistration,
        )>,
    ) -> Result<Self, RemoteObjectRecordError> {
        if losing.is_empty() {
            return Err(RemoteObjectRecordError::InvalidProof(
                "losing Serial prefix is empty".to_string(),
            ));
        }
        let mut verified = Vec::with_capacity(losing.len());
        for (target, author) in losing {
            let commit = target
                .verify_nonactivation_candidate(observation.store_root_hash(), &author)
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
            verified.push((target, commit));
        }
        let candidate = verified
            .last()
            .expect("checked nonempty Serial prefix")
            .0
            .clone();
        let value = Self {
            evidence: Box::new(VerifiedCandidateNonactivationEvidence::Serial {
                durable: CandidateNonactivation {
                    candidate,
                    proof: CandidateNonactivationProof::SerialImmediateSuccessor {
                        accepted_suffix: observation.durable().clone(),
                        losing_prefix: verified.into_iter().map(|(target, _)| target).collect(),
                    },
                },
            }),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub(crate) fn candidate_reference(
        &self,
    ) -> Result<StoreBatchCommitRef, RemoteObjectRecordError> {
        self.durable().reference()
    }

    pub(crate) fn proof(&self) -> &CandidateNonactivationProof {
        &self.durable().proof
    }

    pub(crate) fn merge_winner_commit(
        &self,
    ) -> Result<&StoreBatchCommitRef, RemoteObjectRecordError> {
        match self.evidence.as_ref() {
            VerifiedCandidateNonactivationEvidence::Merge { winner_commit, .. } => {
                Ok(winner_commit)
            }
            VerifiedCandidateNonactivationEvidence::Serial { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "Serial candidate nonactivation has no Merge winner".to_string(),
                ))
            }
        }
    }

    pub(crate) fn into_durable(self) -> CandidateNonactivation {
        match *self.evidence {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::Serial { durable } => durable,
        }
    }

    fn durable(&self) -> &CandidateNonactivation {
        match self.evidence.as_ref() {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::Serial { durable } => durable,
        }
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
    #[error("candidate object graph contains the same exact object more than once")]
    DuplicateCandidateObject,
    #[error("candidate object graph material is missing")]
    CandidateObjectMissing,
    #[error("candidate object material is outside the signed graph")]
    CandidateObjectInvented,
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
    #[error("remote object is not the solely-owned activated Store package being reclaimed")]
    InvalidReclaim,
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

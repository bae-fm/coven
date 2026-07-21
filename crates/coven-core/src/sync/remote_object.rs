//! Closed local publication and ownership state for remote protocol objects.

use std::collections::{BTreeMap, BTreeSet};

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
    fn candidate_exclusive_retained_authority(
        family: CandidateFamilyId,
        domain: CandidateExclusiveObjectDomain,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = domain.object().clone();
        let record = Self::CandidateExclusive(CandidateObjectRecord {
            identity: CandidateExclusiveTarget {
                family,
                domain,
                semantic_hash: ObjectHash::digest(&canonical_signed_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::inline(canonical_signed_bytes, stored_bytes, object)?,
            state: CandidateObjectState::Prepared {
                ownership: PendingCandidateOwnership {
                    pending: BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

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

    pub(crate) fn candidate_activated_membership_control_wrapped_store_key(
        reference: super::wrapped_store_key::WrappedStoreKeyRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::MembershipControlWrappedStoreKey { reference },
            ObjectHash::digest(&canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_store_membership_resolution(
        reference: super::membership::StoreMembershipConflictResolutionRef,
        canonical_semantic_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        candidate: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let semantic_hash = ObjectHash::digest(&canonical_semantic_bytes);
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
            semantic_hash,
            object,
            canonical_semantic_bytes,
            stored_bytes,
            candidate,
        )
    }

    pub(crate) fn candidate_exclusive_merge_membership_entry(
        family: CandidateFamilyId,
        reference: super::membership::MembershipEntryRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        Self::candidate_exclusive_retained_authority(
            family,
            CandidateExclusiveObjectDomain::MergeMembershipEntry { family, reference },
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_exclusive_merge_membership_head(
        family: CandidateFamilyId,
        reference: super::membership::MembershipHeadRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        Self::candidate_exclusive_retained_authority(
            family,
            CandidateExclusiveObjectDomain::MergeMembershipHead { family, reference },
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_exclusive_merge_membership_wrapped_store_key(
        family: CandidateFamilyId,
        reference: super::wrapped_store_key::WrappedStoreKeyRef,
        canonical_signed_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        Self::candidate_exclusive_retained_authority(
            family,
            CandidateExclusiveObjectDomain::MergeMembershipWrappedStoreKey { family, reference },
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

    pub(crate) fn snapshot_activated_image(
        image: &super::store_commit::SnapshotImageRef,
        owner: SnapshotObjectOwner,
    ) -> Result<Self, RemoteObjectRecordError> {
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoreSnapshotImage {
                    reference: image.clone(),
                },
                semantic_hash: image.image_hash,
                object: image.object.clone(),
            },
            bytes: RemoteObjectBytes::external_exact(Vec::new(), image.object.clone())?,
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

    pub(crate) fn activated_external_package(
        domain: SharedLiveSetObjectDomain,
        package: &super::audience_package::AudiencePackage,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        if !matches!(
            domain,
            SharedLiveSetObjectDomain::StorePackage { .. }
                | SharedLiveSetObjectDomain::CirclePackage { .. }
        ) {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let canonical_semantic_bytes = package.to_bytes();
        let object = domain.package_object()?.clone();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain,
                semantic_hash: ObjectHash::digest(&canonical_semantic_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::external_exact(canonical_semantic_bytes, object)?,
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
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub(crate) fn merge_package_activation(
        &mut self,
        domain: &SharedLiveSetObjectDomain,
        package: &super::audience_package::AudiencePackage,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        if !matches!(
            domain,
            SharedLiveSetObjectDomain::StorePackage { .. }
                | SharedLiveSetObjectDomain::CirclePackage { .. }
        ) {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let canonical_semantic_bytes = package.to_bytes();
        if &record.identity.domain != domain
            || record.identity.semantic_hash != ObjectHash::digest(&canonical_semantic_bytes)
            || record.identity.object != *domain.package_object()?
            || record.bytes.canonical_semantic_bytes() != canonical_semantic_bytes
            || record.bytes.stored().object() != domain.package_object()?
            || matches!(
                record.bytes.stored(),
                RemoteStoredRepresentation::Blob { .. }
            )
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        merge_store_commit_owner(&mut record.state, owner);
        self.validate()
    }

    pub(crate) fn merge_retained_replay_owner(
        &mut self,
        owner: RetainedReplayOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        ownership
            .activated
            .insert(SharedObjectOwner::RetainedReplay(owner));
        self.validate()
    }

    pub(crate) fn remove_all_retained_replay_owners(
        &mut self,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Ok(());
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Ok(());
        };
        ownership
            .activated
            .retain(|owner| !matches!(owner, SharedObjectOwner::RetainedReplay(_)));
        self.validate()
    }

    pub(crate) fn remove_retained_replay_owner(
        &mut self,
        owner: &RetainedReplayOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !ownership
            .activated
            .remove(&SharedObjectOwner::RetainedReplay(owner.clone()))
        {
            return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
        }
        self.retire_unowned_shared_live_set()?;
        self.validate()
    }

    fn retire_unowned_shared_live_set(&mut self) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
            return Ok(());
        }
        if ownership.nonactivated.is_empty() {
            return Err(RemoteObjectRecordError::EmptyOwnership);
        }
        let former_candidates = ownership.nonactivated.clone();
        let package_domain = match &record.identity.domain {
            SharedLiveSetObjectDomain::StorePackage { reference } => Some((
                reference.candidate_family,
                CandidateExclusiveObjectDomain::StorePackage {
                    reference: reference.clone(),
                },
            )),
            SharedLiveSetObjectDomain::CirclePackage { reference } => Some((
                reference.package.candidate_family,
                CandidateExclusiveObjectDomain::CirclePackage {
                    reference: reference.clone(),
                },
            )),
            SharedLiveSetObjectDomain::StoredBlob => None,
            SharedLiveSetObjectDomain::StoreSnapshotImage { .. } => None,
        };
        if let Some((family, domain)) = package_domain {
            let identity = CandidateExclusiveTarget {
                family,
                domain,
                semantic_hash: record.identity.semantic_hash,
                object: record.identity.object.clone(),
            };
            let bytes = record.bytes.clone();
            *self = Self::CandidateExclusive(CandidateObjectRecord {
                identity,
                bytes,
                state: CandidateObjectState::CleanupPending { former_candidates },
            });
        } else {
            record.state = OwnedObjectState::RetirementPending { former_candidates };
        }
        Ok(())
    }

    pub(crate) fn retract_activated_candidate(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_nonactivation: Option<&VerifiedCandidateHeadNonactivation>,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        if !matches!(
            nonactivation.proof,
            CandidateNonactivationProof::AuthorExclusion { .. }
                | CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "activated candidate retraction requires terminal author exclusion".to_string(),
            ));
        }
        let candidate = nonactivation.reference()?;
        match self {
            Self::RetainedAuthority(record) => {
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut record.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if !ownership.activated.remove(&candidate) {
                    ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                    return Ok(None);
                }
                ownership.nonactivated.push(nonactivation.clone());
                let is_head = matches!(
                    record.identity.domain,
                    RetainedAuthorityObjectDomain::DeviceHead { .. }
                );
                match (is_head, head_nonactivation) {
                    (true, Some(head_nonactivation))
                        if head_nonactivation.candidate == candidate
                            && matches!(
                                &head_nonactivation.head,
                                VerifiedCandidateHead::ExactLateCandidate { object }
                                    if object == &record.identity.object
                            ) => {}
                    (true, _) => {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "uploaded retracted candidate head lacks exact presence evidence"
                                .to_string(),
                        ));
                    }
                    (false, None) => {}
                    (false, Some(_)) => {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "candidate-head evidence reached a non-head activated object"
                                .to_string(),
                        ));
                    }
                }
                if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
                    self.validate()?;
                    return Ok(None);
                }
                if matches!(
                    &record.identity.domain,
                    RetainedAuthorityObjectDomain::Commit { reference }
                        if reference == &candidate
                ) {
                    let bytes = record.bytes.clone();
                    *self = Self::CandidateCommit(CandidateCommitRecord {
                        identity: candidate,
                        bytes,
                        state: CandidateCommitState::CleanupPending {
                            proof: nonactivation.proof,
                        },
                    });
                    self.validate()?;
                    return Ok(None);
                }
                ProtocolInertObject::new(
                    record.identity.clone(),
                    record.bytes.canonical_semantic_bytes().to_vec(),
                    ownership.nonactivated.clone(),
                )
                .map(Some)
            }
            Self::SharedLiveSet(record) => {
                if head_nonactivation.is_some() {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "candidate-head evidence reached a shared activated object".to_string(),
                    ));
                }
                let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if !ownership
                    .activated
                    .remove(&SharedObjectOwner::StoreCommit(candidate.clone()))
                {
                    ensure_candidate_nonactivation(&ownership.nonactivated, &candidate)?;
                    return Ok(None);
                }
                ownership.nonactivated.push(nonactivation);
                self.retire_unowned_shared_live_set()?;
                self.validate()?;
                Ok(None)
            }
            Self::CandidateCommit(_) | Self::CandidateExclusive(_) => {
                Err(RemoteObjectRecordError::InvalidActivation)
            }
        }
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
        merge_shared_owner(&mut record.state, SharedObjectOwner::Snapshot(owner))?;
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
        let ownership = self.activated_store_package_ownership(target, activation)?;
        if !ownership.pending.is_empty() || ownership.activated.len() != 1 {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(())
    }

    pub(crate) fn store_package_is_retained_for_replay(
        &self,
        target: &super::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, RemoteObjectRecordError> {
        let ownership = self.activated_store_package_ownership(target, activation)?;
        Ok(ownership
            .activated
            .iter()
            .any(|owner| matches!(owner, SharedObjectOwner::RetainedReplay(_))))
    }

    fn activated_store_package_ownership<'a>(
        &'a self,
        target: &super::store_commit::StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<&'a SharedObjectOwnership, RemoteObjectRecordError> {
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
        {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &record.state else {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        };
        if !ownership.activated.contains(&expected_owner) {
            return Err(RemoteObjectRecordError::InvalidReclaim);
        }
        Ok(ownership)
    }

    pub(crate) fn snapshot_owners(&self) -> impl Iterator<Item = &SnapshotObjectOwner> {
        let owners = match self {
            Self::SharedLiveSet(record)
                if matches!(
                    record.identity.domain,
                    SharedLiveSetObjectDomain::StoredBlob
                        | SharedLiveSetObjectDomain::StoreSnapshotImage { .. }
                ) =>
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
                SharedObjectOwner::StoreCommit(_) | SharedObjectOwner::RetainedReplay(_) => None,
            })
        })
    }

    pub(crate) fn retained_replay_owners(&self) -> impl Iterator<Item = &RetainedReplayOwner> {
        let owners = match self {
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::UploadedVerified { ownership } => Some(&ownership.activated),
                OwnedObjectState::Prepared { .. } | OwnedObjectState::RetirementPending { .. } => {
                    None
                }
            },
            Self::CandidateCommit(_) | Self::CandidateExclusive(_) | Self::RetainedAuthority(_) => {
                None
            }
        };
        owners.into_iter().flat_map(|owners| {
            owners.iter().filter_map(|owner| match owner {
                SharedObjectOwner::RetainedReplay(owner) => Some(owner),
                SharedObjectOwner::StoreCommit(_) | SharedObjectOwner::Snapshot(_) => None,
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
                if matches!(
                    record.state,
                    RetainedAuthorityObjectState::CleanupPending { .. }
                        | RetainedAuthorityObjectState::AbsentVerified { .. }
                ) && !matches!(
                    record.identity.domain,
                    RetainedAuthorityObjectDomain::StoreMembershipResolution { .. }
                ) {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
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
                        RetainedAuthorityObjectState::CleanupPending { former_candidates }
                        | RetainedAuthorityObjectState::AbsentVerified { former_candidates }
                        | RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
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
                    SharedLiveSetObjectDomain::StoreSnapshotImage { .. } => {}
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

    pub(crate) fn into_observed_activated(
        mut self,
        commit: &StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        self.mark_uploaded_verified()?;
        self.into_activated(commit)
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
                RetainedAuthorityObjectState::CleanupPending { .. }
                | RetainedAuthorityObjectState::AbsentVerified { .. }
                | RetainedAuthorityObjectState::UncreatedVerified { .. } => {
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

    pub(crate) fn merge_retained_authority_activation(
        &mut self,
        expected: &Self,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::RetainedAuthority(expected) = expected else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let RetainedAuthorityObjectState::UploadedVerified {
            ownership: expected_ownership,
        } = &expected.state
        else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        if !expected_ownership.pending.is_empty()
            || !expected_ownership.nonactivated.is_empty()
            || expected_ownership.activated != BTreeSet::from([owner.clone()])
        {
            return Err(RemoteObjectRecordError::InvalidActivation);
        }
        match self {
            Self::CandidateExclusive(current) => {
                if current.identity.domain.retained_destination()
                    != Some(expected.identity.domain.clone())
                    || current.identity.semantic_hash != expected.identity.semantic_hash
                    || current.identity.object != expected.identity.object
                    || current.bytes != expected.bytes
                {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                let owns_activation = match &current.state {
                    CandidateObjectState::Prepared { ownership }
                    | CandidateObjectState::UploadedVerified { ownership } => {
                        ownership.pending == BTreeSet::from([owner.clone()])
                    }
                    CandidateObjectState::CleanupPending { .. }
                    | CandidateObjectState::AbsentVerified { .. } => false,
                };
                if !owns_activation {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                let mut activated = Self::CandidateExclusive(current.clone());
                activated.mark_uploaded_verified()?;
                *self = activated.into_activated(owner)?;
            }
            Self::RetainedAuthority(current) => {
                if current.identity != expected.identity || current.bytes != expected.bytes {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                if matches!(current.state, RetainedAuthorityObjectState::Prepared { .. }) {
                    self.mark_uploaded_verified()?;
                }
                let Self::RetainedAuthority(current) = self else {
                    unreachable!("retained authority remains in its domain")
                };
                let RetainedAuthorityObjectState::UploadedVerified { ownership } =
                    &mut current.state
                else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                ownership.pending.remove(owner);
                ownership.activated.insert(owner.clone());
            }
            Self::CandidateCommit(_) | Self::SharedLiveSet(_) => {
                return Err(RemoteObjectRecordError::DomainMismatch);
            }
        }
        self.validate()
    }

    pub(crate) fn begin_candidate_nonactivation(
        &mut self,
        nonactivation: CandidateNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        self.begin_candidate_nonactivation_with_head_evidence(
            nonactivation,
            CandidateHeadEvidence::OccupiedByProof,
        )
    }

    pub(crate) fn begin_candidate_nonactivation_with_verified_head_nonactivation(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_nonactivation: &VerifiedCandidateHeadNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        if matches!(
            self,
            Self::RetainedAuthority(RetainedAuthorityRecord {
                state: RetainedAuthorityObjectState::UncreatedVerified { .. },
                ..
            })
        ) {
            return self.reconcile_verified_candidate_head_nonactivation(
                &nonactivation,
                head_nonactivation,
            );
        }
        self.begin_candidate_nonactivation_with_head_evidence(
            nonactivation,
            CandidateHeadEvidence::Verified(head_nonactivation),
        )
    }

    fn reconcile_verified_candidate_head_nonactivation(
        &mut self,
        nonactivation: &CandidateNonactivation,
        head_nonactivation: &VerifiedCandidateHeadNonactivation,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        let candidate = nonactivation.reference()?;
        let Self::RetainedAuthority(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        if !matches!(
            record.identity.domain,
            RetainedAuthorityObjectDomain::DeviceHead { .. }
        ) || head_nonactivation.candidate != candidate
            || head_nonactivation.head.object() != &record.identity.object
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence names another prepared object".to_string(),
            ));
        }
        let RetainedAuthorityObjectState::UncreatedVerified { former_candidates } = &record.state
        else {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence reached a nonterminal head state".to_string(),
            ));
        };
        let mut stored = None;
        for former_candidate in former_candidates {
            if former_candidate.reference()? == candidate {
                stored = Some(former_candidate);
                break;
            }
        }
        let stored = stored.ok_or(RemoteObjectRecordError::CandidateOwnerMismatch)?;
        if stored != nonactivation {
            return Err(RemoteObjectRecordError::InvalidProof(
                "fresh excluded-author head evidence differs from its durable proof".to_string(),
            ));
        }
        match &head_nonactivation.head {
            VerifiedCandidateHead::ExactCandidateAbsent { .. } => Ok(None),
            VerifiedCandidateHead::ExactLateCandidate { .. } => ProtocolInertObject::new(
                record.identity.clone(),
                record.bytes.canonical_semantic_bytes().to_vec(),
                former_candidates.clone(),
            )
            .map(Some),
        }
    }

    fn begin_candidate_nonactivation_with_head_evidence(
        &mut self,
        nonactivation: CandidateNonactivation,
        head_evidence: CandidateHeadEvidence<'_>,
    ) -> Result<Option<ProtocolInertObject>, RemoteObjectRecordError> {
        nonactivation.validate()?;
        let candidate = nonactivation.reference()?;
        if matches!(head_evidence, CandidateHeadEvidence::Verified(_))
            && !matches!(
                self,
                Self::RetainedAuthority(RetainedAuthorityRecord {
                    identity: RetainedAuthorityObjectRef {
                        domain: RetainedAuthorityObjectDomain::DeviceHead { .. },
                        ..
                    },
                    ..
                })
            )
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "candidate head absence evidence reached a non-head object".to_string(),
            ));
        }
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
                    match &head_evidence {
                        CandidateHeadEvidence::OccupiedByProof => {
                            let CandidateNonactivationProof::MergeWinner { winner_head } =
                                &nonactivation.proof
                            else {
                                return Err(RemoteObjectRecordError::InvalidProof(
                                    "a prepared Store head requires winner or verified-absence evidence"
                                        .to_string(),
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
                        }
                        CandidateHeadEvidence::Verified(head_nonactivation) => {
                            if !matches!(
                                nonactivation.proof,
                                CandidateNonactivationProof::AuthorExclusion { .. }
                                    | CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                            ) || head_nonactivation.candidate != candidate
                                || head_nonactivation.head.object() != &record.identity.object
                            {
                                return Err(RemoteObjectRecordError::InvalidProof(
                                    "excluded-author head observation names another prepared object"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    if !ownership.pending.remove(&candidate) {
                        return Err(RemoteObjectRecordError::CandidateOwnerMismatch);
                    }
                    ownership.nonactivated.push(nonactivation);
                    if ownership.pending.is_empty() {
                        match head_evidence {
                            CandidateHeadEvidence::Verified(
                                VerifiedCandidateHeadNonactivation {
                                    head: VerifiedCandidateHead::ExactLateCandidate { .. },
                                    ..
                                },
                            ) => {
                                return ProtocolInertObject::new(
                                    record.identity.clone(),
                                    record.bytes.canonical_semantic_bytes().to_vec(),
                                    ownership.nonactivated.clone(),
                                )
                                .map(Some);
                            }
                            CandidateHeadEvidence::OccupiedByProof
                            | CandidateHeadEvidence::Verified(
                                VerifiedCandidateHeadNonactivation {
                                    head: VerifiedCandidateHead::ExactCandidateAbsent { .. },
                                    ..
                                },
                            ) => {
                                record.state = RetainedAuthorityObjectState::UncreatedVerified {
                                    former_candidates: ownership.nonactivated.clone(),
                                };
                            }
                        }
                    }
                }
                RetainedAuthorityObjectState::UploadedVerified { .. } => {
                    if matches!(
                        head_evidence,
                        CandidateHeadEvidence::Verified(VerifiedCandidateHeadNonactivation {
                            head: VerifiedCandidateHead::ExactCandidateAbsent { .. },
                            ..
                        })
                    ) {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "excluded-author head was verified absent but is marked uploaded"
                                .to_string(),
                        ));
                    }
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
                    match uploaded_retained_nonactivation_disposition(
                        &record.identity.domain,
                        ownership,
                    ) {
                        UploadedRetainedNonactivation::Cleanup(former_candidates) => {
                            record.state =
                                RetainedAuthorityObjectState::CleanupPending { former_candidates };
                        }
                        UploadedRetainedNonactivation::Inert(former_candidates) => {
                            return ProtocolInertObject::new(
                                record.identity.clone(),
                                record.bytes.canonical_semantic_bytes().to_vec(),
                                former_candidates,
                            )
                            .map(Some);
                        }
                        UploadedRetainedNonactivation::Retain(ownership) => {
                            record.state =
                                RetainedAuthorityObjectState::UploadedVerified { ownership };
                        }
                    }
                }
                RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    if matches!(
                        head_evidence,
                        CandidateHeadEvidence::Verified(VerifiedCandidateHeadNonactivation {
                            head: VerifiedCandidateHead::ExactLateCandidate { .. },
                            ..
                        })
                    ) {
                        return Err(RemoteObjectRecordError::InvalidProof(
                            "excluded-author head is present but is marked uncreated".to_string(),
                        ));
                    }
                    ensure_candidate_nonactivation(former_candidates, &candidate)?;
                }
                RetainedAuthorityObjectState::CleanupPending { former_candidates }
                | RetainedAuthorityObjectState::AbsentVerified { former_candidates } => {
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
            })
            | Self::RetainedAuthority(RetainedAuthorityRecord {
                state: RetainedAuthorityObjectState::CleanupPending { .. },
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
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::CleanupPending { former_candidates } => {
                    record.state = RetainedAuthorityObjectState::AbsentVerified {
                        former_candidates: former_candidates.clone(),
                    };
                }
                RetainedAuthorityObjectState::AbsentVerified { .. } => {}
                _ => return Err(RemoteObjectRecordError::InvalidCleanupTransition),
            },
            Self::SharedLiveSet(_) => {
                return Err(RemoteObjectRecordError::InvalidCleanupTransition)
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
                RetainedAuthorityObjectState::CleanupPending { .. } => Ok(false),
                RetainedAuthorityObjectState::AbsentVerified { former_candidates } => {
                    contains(former_candidates)
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
                RetainedAuthorityObjectState::CleanupPending { former_candidates }
                | RetainedAuthorityObjectState::AbsentVerified { former_candidates }
                | RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
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

    pub(crate) fn nonactivation_proofs(&self) -> Vec<&CandidateNonactivationProof> {
        match self {
            Self::CandidateCommit(record) => match &record.state {
                CandidateCommitState::CleanupPending { proof }
                | CandidateCommitState::AbsentVerified { proof } => vec![proof],
                CandidateCommitState::Prepared | CandidateCommitState::UploadedVerified => {
                    Vec::new()
                }
            },
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership }
                | CandidateObjectState::UploadedVerified { ownership } => {
                    nonactivation_proofs(&ownership.nonactivated)
                }
                CandidateObjectState::CleanupPending { former_candidates }
                | CandidateObjectState::AbsentVerified { former_candidates } => {
                    nonactivation_proofs(former_candidates)
                }
            },
            Self::RetainedAuthority(record) => match &record.state {
                RetainedAuthorityObjectState::Prepared { ownership } => {
                    nonactivation_proofs(&ownership.nonactivated)
                }
                RetainedAuthorityObjectState::UploadedVerified { ownership } => {
                    nonactivation_proofs(&ownership.nonactivated)
                }
                RetainedAuthorityObjectState::CleanupPending { former_candidates }
                | RetainedAuthorityObjectState::AbsentVerified { former_candidates }
                | RetainedAuthorityObjectState::UncreatedVerified { former_candidates } => {
                    nonactivation_proofs(former_candidates)
                }
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    nonactivation_proofs(&ownership.nonactivated)
                }
                OwnedObjectState::UploadedVerified { ownership } => {
                    nonactivation_proofs(&ownership.nonactivated)
                }
                OwnedObjectState::RetirementPending { former_candidates } => {
                    nonactivation_proofs(former_candidates)
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

enum UploadedRetainedNonactivation {
    Cleanup(Vec<CandidateNonactivation>),
    Inert(Vec<CandidateNonactivation>),
    Retain(CandidateOwnership),
}

fn uploaded_retained_nonactivation_disposition(
    domain: &RetainedAuthorityObjectDomain,
    ownership: CandidateOwnership,
) -> UploadedRetainedNonactivation {
    if !ownership.pending.is_empty() || !ownership.activated.is_empty() {
        return UploadedRetainedNonactivation::Retain(ownership);
    }
    if matches!(
        domain,
        RetainedAuthorityObjectDomain::StoreMembershipResolution { .. }
    ) {
        UploadedRetainedNonactivation::Cleanup(ownership.nonactivated)
    } else {
        UploadedRetainedNonactivation::Inert(ownership.nonactivated)
    }
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
    CleanupPending {
        former_candidates: Vec<CandidateNonactivation>,
    },
    AbsentVerified {
        former_candidates: Vec<CandidateNonactivation>,
    },
    UncreatedVerified {
        former_candidates: Vec<CandidateNonactivation>,
    },
}

impl RetainedAuthorityObjectState {
    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } => ownership.validate(),
            Self::UploadedVerified { ownership } => ownership.validate(),
            Self::CleanupPending { former_candidates }
            | Self::AbsentVerified { former_candidates }
            | Self::UncreatedVerified { former_candidates } => {
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

    pub(crate) fn external_exact(
        canonical_semantic_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            canonical_semantic_bytes,
            stored: RemoteStoredRepresentation::ExternalExact { object },
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
            RemoteStoredRepresentation::Blob { .. }
            | RemoteStoredRepresentation::ExternalExact { .. } => Ok(()),
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
    ExternalExact {
        object: ExactObjectRef,
    },
}

impl RemoteStoredRepresentation {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Inline { object, .. }
            | Self::Blob { object }
            | Self::ExternalExact { object } => object,
        }
    }

    pub(crate) fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes, .. } => Some(bytes),
            Self::Blob { .. } | Self::ExternalExact { .. } => None,
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

fn merge_store_commit_owner(state: &mut OwnedObjectState, owner: &StoreBatchCommitRef) {
    match state {
        OwnedObjectState::Prepared { ownership } => {
            let mut pending = ownership.pending.clone();
            pending.remove(owner);
            *state = OwnedObjectState::UploadedVerified {
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
            *state = OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                    nonactivated: former_candidates.clone(),
                },
            };
        }
    }
}

fn merge_shared_owner(
    state: &mut OwnedObjectState,
    owner: SharedObjectOwner,
) -> Result<(), RemoteObjectRecordError> {
    match state {
        OwnedObjectState::UploadedVerified { ownership } => {
            ownership.activated.insert(owner);
            Ok(())
        }
        OwnedObjectState::RetirementPending { former_candidates } => {
            *state = OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([owner]),
                    nonactivated: former_candidates.clone(),
                },
            };
            Ok(())
        }
        OwnedObjectState::Prepared { .. } => Err(RemoteObjectRecordError::InvalidActivation),
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
                SharedObjectOwner::Snapshot(_) | SharedObjectOwner::RetainedReplay(_) => None,
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
    RetainedReplay(RetainedReplayOwner),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedReplayOwner {
    Commit {
        commit: StoreBatchCommitRef,
        input_hash: ObjectHash,
    },
}

impl RetainedReplayOwner {
    pub(crate) fn commit(&self) -> &StoreBatchCommitRef {
        match self {
            Self::Commit { commit, .. } => commit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotObjectOwner {
    pub(crate) activation: StreamActivationId,
    pub(crate) generation: u64,
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
    MergeMembershipEntry {
        family: CandidateFamilyId,
        reference: super::membership::MembershipEntryRef,
    },
    MergeMembershipHead {
        family: CandidateFamilyId,
        reference: super::membership::MembershipHeadRef,
    },
    MergeMembershipWrappedStoreKey {
        family: CandidateFamilyId,
        reference: super::wrapped_store_key::WrappedStoreKeyRef,
    },
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
            Self::MergeMembershipEntry { family, .. }
            | Self::MergeMembershipHead { family, .. }
            | Self::MergeMembershipWrappedStoreKey { family, .. } => *family,
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
            Self::MergeMembershipEntry { reference, .. } => &reference.object,
            Self::MergeMembershipHead { reference, .. } => &reference.object,
            Self::MergeMembershipWrappedStoreKey { reference, .. } => &reference.object,
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
            Self::MergeMembershipEntry { .. }
            | Self::MergeMembershipHead { .. }
            | Self::MergeMembershipWrappedStoreKey { .. }
            | Self::CircleAccessLeaf { .. }
            | Self::CircleAccessEnvelope { .. }
            | Self::SelfRetirement { .. } => None,
        }
    }

    pub(crate) fn retained_destination(&self) -> Option<RetainedAuthorityObjectDomain> {
        match self {
            Self::MergeMembershipEntry { reference, .. } => {
                Some(RetainedAuthorityObjectDomain::MergeMembershipEntry {
                    reference: reference.clone(),
                })
            }
            Self::MergeMembershipHead { reference, .. } => {
                Some(RetainedAuthorityObjectDomain::MergeMembershipHead {
                    reference: reference.clone(),
                })
            }
            Self::MergeMembershipWrappedStoreKey { reference, .. } => Some(
                RetainedAuthorityObjectDomain::MembershipControlWrappedStoreKey {
                    reference: reference.clone(),
                },
            ),
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

impl SharedLiveSetObjectDomain {
    fn package_object(&self) -> Result<&ExactObjectRef, RemoteObjectRecordError> {
        match self {
            Self::StoredBlob | Self::StoreSnapshotImage { .. } => {
                Err(RemoteObjectRecordError::DomainMismatch)
            }
            Self::StorePackage { reference } => Ok(&reference.object),
            Self::CirclePackage { reference } => Ok(&reference.package.object),
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
        CandidateExclusiveObjectDomain::MergeMembershipEntry { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MergeMembershipEntry {
                        reference: reference.clone(),
                    },
                    semantic_hash: identity.semantic_hash,
                    object: identity.object.clone(),
                },
                canonical_semantic_bytes,
            )
        }
        CandidateExclusiveObjectDomain::MergeMembershipHead { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MergeMembershipHead {
                        reference: reference.clone(),
                    },
                    semantic_hash: identity.semantic_hash,
                    object: identity.object.clone(),
                },
                canonical_semantic_bytes,
            )
        }
        CandidateExclusiveObjectDomain::MergeMembershipWrappedStoreKey { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MembershipControlWrappedStoreKey {
                        reference: reference.clone(),
                    },
                    semantic_hash: identity.semantic_hash,
                    object: identity.object.clone(),
                },
                canonical_semantic_bytes,
            )
        }
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
        match &self.domain {
            SharedLiveSetObjectDomain::StoreSnapshotImage { reference }
                if bytes.is_empty()
                    && self.semantic_hash == reference.image_hash
                    && self.object == reference.object =>
            {
                Ok(())
            }
            SharedLiveSetObjectDomain::StoreSnapshotImage { .. } => {
                Err(RemoteObjectRecordError::StoredReferenceMismatch)
            }
            _ => validate_semantic_hash(self.semantic_hash, bytes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedLiveSetObjectDomain {
    StoredBlob,
    StoreSnapshotImage {
        reference: super::store_commit::SnapshotImageRef,
    },
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

    pub(crate) fn nonactivation_proofs(&self) -> Vec<&CandidateNonactivationProof> {
        nonactivation_proofs(&self.former_candidates)
    }

    pub(crate) fn is_terminal_head_for(
        &self,
        candidate: &StoreBatchCommitRef,
        object: &ExactObjectRef,
    ) -> Result<bool, RemoteObjectRecordError> {
        self.validate()?;
        let head: super::store_commit::StoreDeviceHead =
            serde_json::from_slice(&self.canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        Ok(self.identity.object == *object
            && head.commit == *candidate
            && matches!(
                &self.identity.domain,
                RetainedAuthorityObjectDomain::DeviceHead { reference }
                    if reference.object == *object
            )
            && matches!(
                self.candidate_nonactivation_proof(candidate)?,
                Some(
                    CandidateNonactivationProof::AuthorExclusion { .. }
                        | CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                )
            ))
    }
}

fn nonactivation_proofs(
    candidates: &[CandidateNonactivation],
) -> Vec<&CandidateNonactivationProof> {
    candidates
        .iter()
        .map(CandidateNonactivation::proof)
        .collect()
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
    MembershipControlWrappedStoreKey {
        reference: super::wrapped_store_key::WrappedStoreKeyRef,
    },
    StoreMembershipResolution {
        reference: super::membership::StoreMembershipConflictResolutionRef,
    },
    MergeMembershipEntry {
        reference: super::membership::MembershipEntryRef,
    },
    MergeMembershipHead {
        reference: super::membership::MembershipHeadRef,
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
        RetainedAuthorityObjectDomain::MembershipControlWrappedStoreKey { reference } => {
            let wrapped: super::wrapped_store_key::WrappedStoreKey =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .validate_value(&wrapped, canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::StoreMembershipResolution { reference } => {
            let resolution: super::membership::StoreMembershipConflictResolution =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            let expected_key = format!(
                "{}.json",
                super::store_commit::membership_resolution_semantic_prefix(
                    reference.conflict_hash,
                    &reference.resolver_pubkey,
                    reference.resolution_hash,
                )
            );
            if resolution.conflict_hash != reference.conflict_hash
                || resolution.resolver_pubkey != reference.resolver_pubkey
                || resolution.resolution_hash() != reference.resolution_hash
                || reference.object != identity.object
                || reference.object.slot().logical_key() != expected_key
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipEntry { reference } => {
            let entry: super::membership::MembershipEntry =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if entry.coord() != reference.coord || reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipHead { reference } => {
            let head: super::membership::AuthorHead =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if head.entry_coord() != reference.coord
                || head.head_hash() != reference.head_hash
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCandidateHeadNonactivation {
    candidate: StoreBatchCommitRef,
    head: VerifiedCandidateHead,
}

impl VerifiedCandidateHeadNonactivation {
    pub(crate) fn head(&self) -> &VerifiedCandidateHead {
        &self.head
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VerifiedCandidateHead {
    ExactCandidateAbsent { object: ExactObjectRef },
    ExactLateCandidate { object: ExactObjectRef },
}

impl VerifiedCandidateHead {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::ExactCandidateAbsent { object } | Self::ExactLateCandidate { object } => object,
        }
    }
}

enum CandidateHeadEvidence<'a> {
    OccupiedByProof,
    Verified(&'a VerifiedCandidateHeadNonactivation),
}

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

    pub(crate) fn from_durable_parts(
        candidate: &StoreBatchCommitRef,
        commit: &super::store_commit::StoreBatchCommit,
        proof: CandidateNonactivationProof,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            candidate: StoreBatchCommitDeletionTarget {
                coord: candidate.coord.clone(),
                object: candidate.object.clone(),
                canonical_signed_bytes: commit.to_bytes(),
            },
            proof,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
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

#[derive(Debug, Clone)]
pub(crate) struct VerifiedDependencyRetractionAuthority {
    durable: CandidateNonactivation,
}

impl VerifiedDependencyRetractionAuthority {
    pub(crate) fn after_live_authority_check(
        durable: CandidateNonactivation,
    ) -> Result<Self, RemoteObjectRecordError> {
        durable.validate()?;
        if !matches!(
            durable.proof(),
            CandidateNonactivationProof::MergeDependencyRetraction { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "dependent retraction authority carries another proof family".to_string(),
            ));
        }
        Ok(Self { durable })
    }
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
    AuthorExclusion {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
    },
    MembershipGrantRevocation {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
    },
    DependencyRetraction {
        durable: CandidateNonactivation,
        head_nonactivation: VerifiedCandidateHeadNonactivation,
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

    pub(crate) fn author_exclusion(
        observation: &super::store_pull::VerifiedAuthorExclusionActivation,
        candidate: StoreBatchCommitDeletionTarget,
    ) -> Result<Self, RemoteObjectRecordError> {
        let commit = candidate
            .verify_nonactivation_candidate(
                observation.store_root_hash(),
                observation.target_registration(),
            )
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let candidate_reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        if commit.author_registration != *observation.target()
            || observation.exclusion().proposal.target != commit.author_registration
            || observation.candidate() != &candidate_reference
        {
            return Err(RemoteObjectRecordError::InvalidProof(
                "author exclusion targets another candidate registration".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(VerifiedCandidateNonactivationEvidence::AuthorExclusion {
                durable: CandidateNonactivation {
                    candidate,
                    proof: CandidateNonactivationProof::AuthorExclusion {
                        exclusion: observation.exclusion().clone(),
                        accepted_cut: observation.accepted_cut().clone(),
                        activation_head: observation.activation_head().clone(),
                    },
                },
                head_nonactivation: VerifiedCandidateHeadNonactivation {
                    candidate: candidate_reference,
                    head: observation.candidate_head().clone(),
                },
            }),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub(crate) fn membership_grant_revocation(
        observation: &super::store_pull::VerifiedMembershipGrantRevocationActivation,
        candidate: StoreBatchCommitDeletionTarget,
    ) -> Result<Self, RemoteObjectRecordError> {
        let commit = candidate
            .verify_nonactivation_candidate(
                observation.store_root_hash(),
                observation.candidate_author(),
            )
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let candidate_reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        if observation.candidate() != &candidate_reference {
            return Err(RemoteObjectRecordError::InvalidProof(
                "membership-grant revocation names another candidate".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                    durable: CandidateNonactivation {
                        candidate,
                        proof: CandidateNonactivationProof::MergeMembershipGrantRevocation {
                            grant_id: observation.grant_id().clone(),
                            membership: observation.membership().clone(),
                            activation_commit: observation.activation_commit().clone(),
                            activation_head: observation.activation_head().clone(),
                        },
                    },
                    head_nonactivation: VerifiedCandidateHeadNonactivation {
                        candidate: candidate_reference,
                        head: observation.candidate_head().clone(),
                    },
                },
            ),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub(crate) fn dependency_retraction(
        dependency: &Self,
        candidate: StoreBatchCommitDeletionTarget,
        author: &super::store_commit::StoreDeviceRegistration,
        activation_head_object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        if !matches!(
            dependency.evidence.as_ref(),
            VerifiedCandidateNonactivationEvidence::AuthorExclusion { .. }
                | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation { .. }
                | VerifiedCandidateNonactivationEvidence::DependencyRetraction { .. }
        ) {
            return Err(RemoteObjectRecordError::InvalidProof(
                "dependent retraction does not descend from terminal evidence".to_string(),
            ));
        }
        let commit = candidate
            .verify_nonactivation_candidate(author.store_root.store_root_hash, author)
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let candidate_reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let dependency_reference = dependency.candidate_reference()?;
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                    durable: CandidateNonactivation {
                        candidate,
                        proof: CandidateNonactivationProof::MergeDependencyRetraction {
                            dependency: dependency_reference,
                            dependency_nonactivation: Box::new(dependency.durable().clone()),
                        },
                    },
                    head_nonactivation: VerifiedCandidateHeadNonactivation {
                        candidate: candidate_reference,
                        head: VerifiedCandidateHead::ExactLateCandidate {
                            object: activation_head_object,
                        },
                    },
                },
            ),
        };
        value.durable().validate()?;
        Ok(value)
    }

    pub(crate) fn from_verified_dependency_retraction_authority(
        authority: VerifiedDependencyRetractionAuthority,
        candidate: StoreBatchCommitDeletionTarget,
        author: &super::store_commit::StoreDeviceRegistration,
        activation_head_object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let commit = candidate
            .verify_nonactivation_candidate(author.store_root.store_root_hash, author)
            .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        let candidate_reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))?;
        if authority.durable.candidate != candidate {
            return Err(RemoteObjectRecordError::InvalidProof(
                "verified dependent retraction authority names another candidate".to_string(),
            ));
        }
        let value = Self {
            evidence: Box::new(
                VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                    durable: authority.durable,
                    head_nonactivation: VerifiedCandidateHeadNonactivation {
                        candidate: candidate_reference,
                        head: VerifiedCandidateHead::ExactLateCandidate {
                            object: activation_head_object,
                        },
                    },
                },
            ),
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
            VerifiedCandidateNonactivationEvidence::AuthorExclusion { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "author-exclusion nonactivation has no Merge slot winner".to_string(),
                ))
            }
            VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "membership-grant revocation has no Merge slot winner".to_string(),
                ))
            }
            VerifiedCandidateNonactivationEvidence::DependencyRetraction { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "dependent retraction has no Merge slot winner".to_string(),
                ))
            }
        }
    }

    pub(crate) fn into_durable(self) -> CandidateNonactivation {
        match *self.evidence {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::Serial { durable }
            | VerifiedCandidateNonactivationEvidence::AuthorExclusion { durable, .. }
            | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable, ..
            }
            | VerifiedCandidateNonactivationEvidence::DependencyRetraction { durable, .. } => {
                durable
            }
        }
    }

    pub(crate) fn into_terminal_head_nonactivation(
        self,
    ) -> Result<(CandidateNonactivation, VerifiedCandidateHeadNonactivation), RemoteObjectRecordError>
    {
        match *self.evidence {
            VerifiedCandidateNonactivationEvidence::AuthorExclusion {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::DependencyRetraction {
                durable,
                head_nonactivation,
            } => Ok((durable, head_nonactivation)),
            VerifiedCandidateNonactivationEvidence::Merge { .. }
            | VerifiedCandidateNonactivationEvidence::Serial { .. } => {
                Err(RemoteObjectRecordError::InvalidProof(
                    "candidate nonactivation is not verified by an excluded-author head observation"
                        .to_string(),
                ))
            }
        }
    }

    fn durable(&self) -> &CandidateNonactivation {
        match self.evidence.as_ref() {
            VerifiedCandidateNonactivationEvidence::Merge { durable, .. }
            | VerifiedCandidateNonactivationEvidence::Serial { durable }
            | VerifiedCandidateNonactivationEvidence::AuthorExclusion { durable, .. }
            | VerifiedCandidateNonactivationEvidence::MembershipGrantRevocation {
                durable, ..
            }
            | VerifiedCandidateNonactivationEvidence::DependencyRetraction { durable, .. } => {
                durable
            }
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
    AuthorExclusion {
        exclusion: super::store_commit::StoreDeviceExclusionRef,
        accepted_cut: BTreeMap<super::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
        activation_head: super::store_commit::StoreDeviceHeadRef,
    },
    MergeMembershipGrantRevocation {
        grant_id: super::membership::MembershipGrantId,
        membership: super::circle_control::MergeStoreMembershipStateRef,
        activation_commit: StoreBatchCommitRef,
        activation_head: super::store_commit::StoreDeviceHeadRef,
    },
    MergeDependencyRetraction {
        dependency: StoreBatchCommitRef,
        dependency_nonactivation: Box<CandidateNonactivation>,
    },
}

impl CandidateNonactivationProof {
    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::MergeWinner { .. } => Ok(()),
            Self::SerialImmediateSuccessor {
                accepted_suffix,
                losing_prefix,
            } => {
                accepted_suffix.validate()?;
                validate_losing_prefix(accepted_suffix, losing_prefix)
            }
            Self::AuthorExclusion { accepted_cut, .. } => {
                super::store_commit::validate_store_history_cut(
                    &super::store_commit::StoreHistoryCut::merge_concurrent(accepted_cut.clone()),
                )
                .map_err(|error| RemoteObjectRecordError::InvalidProof(error.to_string()))
            }
            Self::MergeMembershipGrantRevocation {
                membership,
                activation_commit,
                ..
            } => {
                if !membership.heads.windows(2).all(|pair| pair[0] < pair[1])
                    || !membership
                        .resolutions
                        .windows(2)
                        .all(|pair| pair[0] < pair[1])
                    || activation_commit.coord.policy() != crate::WritePolicy::MergeConcurrent
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "membership-grant revocation names a noncanonical membership state"
                            .to_string(),
                    ));
                }
                Ok(())
            }
            Self::MergeDependencyRetraction {
                dependency,
                dependency_nonactivation,
            } => {
                dependency_nonactivation.validate()?;
                if dependency.coord.policy() != crate::WritePolicy::MergeConcurrent
                    || dependency_nonactivation.reference()? != *dependency
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "dependent retraction names another exact dependency".to_string(),
                    ));
                }
                Ok(())
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
            Self::AuthorExclusion {
                exclusion,
                accepted_cut,
                ..
            } => {
                if candidate.coord.policy() != crate::WritePolicy::MergeConcurrent
                    || commit.author_registration != exclusion.proposal.target
                {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "author exclusion names another candidate author or policy".to_string(),
                    ));
                }
                let expected_stream =
                    super::store_commit::StreamActivation::device_authorized_stream_id(
                        commit.store_root_hash,
                        &commit.author_registration,
                        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                let super::store_commit::StoreCommitCoord::MergeConcurrent {
                    stream_id,
                    sequence,
                } = candidate.coord
                else {
                    unreachable!("checked Merge candidate policy")
                };
                let beyond_cutoff = match accepted_cut.get(&expected_stream) {
                    Some(reference) => sequence > reference.coord.sequence(),
                    None => true,
                };
                if stream_id != expected_stream || !beyond_cutoff {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "candidate is not strictly beyond its excluded author cutoff".to_string(),
                    ));
                }
                Ok(())
            }
            Self::MergeMembershipGrantRevocation { .. } => {
                if candidate.coord.policy() != crate::WritePolicy::MergeConcurrent {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "membership-grant revocation names a non-Merge candidate".to_string(),
                    ));
                }
                Ok(())
            }
            Self::MergeDependencyRetraction { dependency, .. } => {
                if candidate.coord.policy() != crate::WritePolicy::MergeConcurrent {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "dependent retraction names a non-Merge candidate".to_string(),
                    ));
                }
                let mut direct = commit
                    .order
                    .dependencies()
                    .into_iter()
                    .flat_map(|dependencies| dependencies.values())
                    .collect::<BTreeSet<_>>();
                if let Some(predecessor) = commit.order.predecessor() {
                    direct.insert(predecessor);
                }
                if !direct.contains(dependency) {
                    return Err(RemoteObjectRecordError::InvalidProof(
                        "dependent retraction proof is not an exact direct dependency".to_string(),
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
    use crate::{BlobScope, KeyFingerprint, WriteId};

    fn test_commit_ref(label: &str, sequence: u64) -> StoreBatchCommitRef {
        let commit_hash = ObjectHash::digest(format!("{label} semantic commit").as_bytes());
        let stored = format!("{label} stored commit");
        StoreBatchCommitRef {
            coord: super::super::store_commit::StoreCommitCoord::Serial { sequence },
            commit_hash,
            object: ExactObjectRef::new(
                ObjectSlot::logical(format!("store-v1/commits/{label}.json"))
                    .expect("valid test commit slot"),
                stored.len() as u64,
                ObjectHash::digest(stored.as_bytes()),
            ),
        }
    }

    fn test_store_package(
        owner: &StoreBatchCommitRef,
    ) -> (
        super::super::store_commit::StorePackageRef,
        super::super::audience_package::AudiencePackage,
    ) {
        let family = CandidateFamilyId::from_hash(ObjectHash::digest(b"test package family"));
        let package = super::super::audience_package::AudiencePackage::store(
            ObjectHash::digest(b"test Store root"),
            family,
            WriteId::from_generated("test-package-write".to_string()),
            owner.coord.clone(),
            1,
            b"changeset".to_vec(),
            Vec::new(),
        )
        .expect("valid test package");
        let semantic = package.to_bytes();
        let stored = b"encrypted test package";
        let reference = super::super::store_commit::StorePackageRef {
            candidate_family: family,
            content_hash: ObjectHash::digest(&semantic),
            schema_version: package.schema_version(),
            changeset_size: semantic.len() as u64,
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/packages/test.pkg".to_string())
                    .expect("valid test package slot"),
                stored.len() as u64,
                ObjectHash::digest(stored),
            ),
        };
        (reference, package)
    }

    fn test_stored_blob(label: &str) -> crate::blob::locator::StoredBlobRef {
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
            label,
            uploader,
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            7,
            ObjectHash::digest(label.as_bytes()),
        )
        .expect("valid locator");
        let stored = format!("stored {label}");
        let semantic_key = locator.semantic_key();
        crate::blob::locator::StoredBlobRef::new(
            locator,
            ExactObjectRef::new(
                ObjectSlot::opaque(semantic_key, format!("physical-{label}"))
                    .expect("valid blob slot"),
                stored.len() as u64,
                ObjectHash::digest(stored.as_bytes()),
            ),
        )
        .expect("valid stored blob")
    }

    fn test_membership_resolution() -> (
        super::super::membership::StoreMembershipConflictResolutionRef,
        Vec<u8>,
    ) {
        let conflict_hash = ObjectHash::digest(b"remote-object membership conflict");
        let resolver_pubkey = "22".repeat(crate::keys::SIGN_PUBLICKEYBYTES);
        let replacement_grant = super::super::membership::derive_store_resolution_grant(
            &conflict_hash,
            &resolver_pubkey,
        );
        let registration_bytes = b"resolution registration";
        let registration = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: "33".repeat(32).parse().expect("valid resolution device id"),
            registration_hash: ObjectHash::digest(registration_bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/devices/resolution-registration.json".to_string())
                    .expect("valid resolution registration slot"),
                registration_bytes.len() as u64,
                ObjectHash::digest(registration_bytes),
            ),
        };
        let membership = super::super::store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: ObjectSlot::logical(
                "store-v1/membership/heads/resolver/replacement/stream/1.json".to_string(),
            )
            .expect("valid resolution membership slot"),
        };
        let recovery = super::super::store_commit::GrantStreamAnchor::OwnerRecovery {
            first_slot: ObjectSlot::logical(
                "store-v1/recovery/resolver/replacement/1.json".to_string(),
            )
            .expect("valid resolution recovery slot"),
        };
        let resolution = super::super::membership::StoreMembershipConflictResolution {
            version: super::super::store_commit::STORE_PROTOCOL_VERSION,
            store_root_hash: ObjectHash::digest(b"remote-object resolution Store root"),
            conflict_hash,
            conflicting_heads: Vec::new(),
            retired_owner_grants: BTreeSet::new(),
            retirement_barriers: BTreeMap::new(),
            resolver_pubkey: resolver_pubkey.clone(),
            resolver_branch_heads: Vec::new(),
            replacement_grant: replacement_grant.clone(),
            replacement_membership: membership.clone(),
            replacement_acceptance: super::super::store_commit::OwnerConflictResolutionAcceptance {
                store_root_hash: ObjectHash::digest(b"remote-object resolution Store root"),
                owner_grant: replacement_grant,
                owner_registration: registration,
                provider: super::super::storage::ProviderDeviceBinding {
                    principal: super::super::storage::ProviderPrincipalId::CustomS3Credential {
                        access_key_id_hash: ObjectHash::digest(b"resolution provider credential"),
                    },
                },
                membership,
                recovery,
                device_state: super::super::store_commit::StoreDeviceStateRef::MergeConcurrent {
                    frontier: super::super::store_commit::CommitFrontier::MergeConcurrent(
                        BTreeMap::new(),
                    ),
                    recovery: Vec::new(),
                    state_hash: ObjectHash::digest(b"resolution device state"),
                },
                signature: "resolution acceptance signature".to_string(),
            },
            signature: "resolution signature".to_string(),
        };
        let canonical = serde_json::to_vec(&resolution).expect("serialize membership resolution");
        let resolution_hash = resolution.resolution_hash();
        let object = ExactObjectRef::new(
            ObjectSlot::logical(format!(
                "{}.json",
                super::super::store_commit::membership_resolution_semantic_prefix(
                    conflict_hash,
                    &resolver_pubkey,
                    resolution_hash,
                )
            ))
            .expect("valid membership resolution slot"),
            canonical.len() as u64,
            ObjectHash::digest(&canonical),
        );
        (resolution.resolution_ref(object), canonical)
    }

    fn test_membership_resolution_record(
        reference: super::super::membership::StoreMembershipConflictResolutionRef,
        canonical: Vec<u8>,
        candidate: StoreBatchCommitRef,
    ) -> Result<RemoteObjectRecord, RemoteObjectRecordError> {
        let object = reference.object.clone();
        RemoteObjectRecord::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
            ObjectHash::digest(&canonical),
            object,
            canonical.clone(),
            canonical,
            candidate,
        )
    }

    fn activate_test_retained_authority(
        mut record: RemoteObjectRecord,
        owner: &StoreBatchCommitRef,
    ) -> RemoteObjectRecord {
        record
            .mark_uploaded_verified()
            .expect("mark retained authority uploaded");
        record
            .into_activated(owner)
            .expect("activate retained authority")
    }

    #[test]
    fn pulled_retained_authority_merges_an_exact_additional_commit_owner() {
        let (reference, canonical) = test_membership_resolution();
        let first = test_commit_ref("first-resolution-owner", 1);
        let second = test_commit_ref("second-resolution-owner", 1);
        let mut existing = activate_test_retained_authority(
            test_membership_resolution_record(reference.clone(), canonical.clone(), first.clone())
                .expect("prepare first retained authority"),
            &first,
        );
        let expected = activate_test_retained_authority(
            test_membership_resolution_record(reference, canonical, second.clone())
                .expect("prepare second retained authority"),
            &second,
        );

        existing
            .merge_retained_authority_activation(&expected, &second)
            .expect("merge pulled retained authority activation");

        let RemoteObjectRecord::RetainedAuthority(record) = existing else {
            panic!("merged membership resolution changed domain")
        };
        let RetainedAuthorityObjectState::UploadedVerified { ownership } = record.state else {
            panic!("merged membership resolution lost uploaded state")
        };
        assert_eq!(ownership.activated, BTreeSet::from([first, second]));
        assert!(ownership.pending.is_empty());
        assert!(ownership.nonactivated.is_empty());
    }

    #[test]
    fn external_package_keeps_exact_ciphertext_identity_and_idempotent_replay_owner() {
        let commit = test_commit_ref("external-package", 1);
        let (reference, package) = test_store_package(&commit);
        let domain = SharedLiveSetObjectDomain::StorePackage {
            reference: reference.clone(),
        };
        let mut record = RemoteObjectRecord::activated_external_package(
            domain.clone(),
            &package,
            commit.clone(),
        )
        .expect("activate external package");
        let replay = RetainedReplayOwner::Commit {
            commit: commit.clone(),
            input_hash: ObjectHash::digest(b"retained input"),
        };

        record
            .merge_retained_replay_owner(replay.clone())
            .expect("pin external package");
        record
            .merge_retained_replay_owner(replay.clone())
            .expect("repeat exact pin");

        assert!(matches!(
            record.bytes().stored(),
            RemoteStoredRepresentation::ExternalExact { object }
                if object == &reference.object
        ));
        assert_eq!(
            record.retained_replay_owners().collect::<Vec<_>>(),
            vec![&replay]
        );
        assert!(record
            .validate_reclaimable_store_package(&reference, &commit)
            .is_err());

        let mut wrong_plaintext = record.clone();
        let RemoteObjectRecord::SharedLiveSet(inner) = &mut wrong_plaintext else {
            unreachable!("constructed shared package")
        };
        inner.bytes.canonical_semantic_bytes.push(b' ');
        assert!(wrong_plaintext.validate().is_err());

        let mut wrong_reference = record;
        let RemoteObjectRecord::SharedLiveSet(inner) = &mut wrong_reference else {
            unreachable!("constructed shared package")
        };
        inner.identity.domain = domain;
        inner.identity.object = test_commit_ref("wrong-package", 2).object;
        assert!(wrong_reference.validate().is_err());
    }

    #[test]
    fn shared_blob_retains_each_commit_owner_independently() {
        let blob = test_stored_blob("shared-blob");
        let first = test_commit_ref("first-blob-owner", 1);
        let second = test_commit_ref("second-blob-owner", 2);
        let first_replay = RetainedReplayOwner::Commit {
            commit: first.clone(),
            input_hash: ObjectHash::digest(b"first retained input"),
        };
        let second_replay = RetainedReplayOwner::Commit {
            commit: second.clone(),
            input_hash: ObjectHash::digest(b"second retained input"),
        };
        let mut record =
            RemoteObjectRecord::activated_blob(&blob, first.clone()).expect("activate shared blob");
        record
            .merge_blob_activation(&blob, &second)
            .expect("activate second blob owner");
        record
            .merge_retained_replay_owner(first_replay.clone())
            .expect("pin first retained input");
        record
            .merge_retained_replay_owner(second_replay.clone())
            .expect("pin second retained input");

        assert_eq!(
            record
                .retained_replay_owners()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([first_replay, second_replay])
        );
        assert!(record.validate().is_ok());
    }

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

    #[test]
    fn serial_membership_wrap_is_candidate_activated_retained_authority() {
        let owner_key = crate::keys::UserKeypair::generate();
        let recipient_key = crate::keys::UserKeypair::generate();
        let owner_pubkey = hex::encode(owner_key.public_key());
        let recipient_pubkey = hex::encode(recipient_key.public_key());
        let wrapped = super::super::wrapped_store_key::WrappedStoreKey::signed(
            "remote-object-test-store",
            &recipient_pubkey,
            1,
            vec![7; 32],
            &owner_key,
        );
        let canonical = serde_json::to_vec(&wrapped).expect("serialize wrapped Store key");
        let wrap_hash = ObjectHash::digest(&canonical);
        let object = ExactObjectRef::new(
            ObjectSlot::logical(format!(
                "keys/{owner_pubkey}/{recipient_pubkey}/1/{wrap_hash}.json"
            ))
            .expect("valid wrapped-key slot"),
            canonical.len() as u64,
            ObjectHash::digest(&canonical),
        );
        let reference = super::super::wrapped_store_key::WrappedStoreKeyRef {
            owner_pubkey,
            recipient_pubkey,
            generation: 1,
            wrap_hash,
            object,
        };
        let candidate = test_commit_ref("membership-wrap-owner", 1);

        let record = RemoteObjectRecord::candidate_activated_membership_control_wrapped_store_key(
            reference,
            canonical.clone(),
            canonical,
            candidate.clone(),
        )
        .expect("close wrapped-key ownership");

        assert!(record.validate().is_ok());
        assert!(matches!(
            record,
            RemoteObjectRecord::RetainedAuthority(RetainedAuthorityRecord {
                identity: RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MembershipControlWrappedStoreKey { .. },
                    ..
                },
                state: RetainedAuthorityObjectState::Prepared { ownership },
                ..
            }) if ownership.pending == BTreeSet::from([candidate])
        ));
    }

    #[test]
    fn membership_resolution_is_candidate_activated_retained_authority() {
        let (reference, canonical) = test_membership_resolution();
        let candidate = test_commit_ref("membership-resolution-owner", 1);

        let record = test_membership_resolution_record(reference, canonical, candidate.clone())
            .expect("close membership-resolution ownership");
        let encoded = serde_json::to_vec(&record).expect("serialize retained resolution authority");
        let record: RemoteObjectRecord =
            serde_json::from_slice(&encoded).expect("deserialize retained resolution authority");

        assert!(record.validate().is_ok());
        assert!(matches!(
            record,
            RemoteObjectRecord::RetainedAuthority(RetainedAuthorityRecord {
                identity: RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::StoreMembershipResolution { .. },
                    ..
                },
                state: RetainedAuthorityObjectState::Prepared { ownership },
                ..
            }) if ownership.pending == BTreeSet::from([candidate])
        ));
    }

    #[test]
    fn membership_resolution_authority_rejects_a_different_semantic_reference() {
        let (reference, canonical) = test_membership_resolution();
        let candidate = test_commit_ref("membership-resolution-mismatch", 1);
        let mut record = test_membership_resolution_record(reference, canonical, candidate)
            .expect("close membership-resolution ownership");
        let RemoteObjectRecord::RetainedAuthority(inner) = &mut record else {
            panic!("membership resolution must use retained authority ownership")
        };
        let RetainedAuthorityObjectDomain::StoreMembershipResolution { reference } =
            &mut inner.identity.domain
        else {
            panic!("membership resolution must retain its exact domain")
        };
        reference.conflict_hash = ObjectHash::digest(b"another membership conflict");

        assert!(matches!(
            record.validate(),
            Err(RemoteObjectRecordError::StoredReferenceMismatch)
        ));
    }

    #[test]
    fn membership_resolution_authority_rejects_a_relocated_object() {
        let (mut reference, canonical) = test_membership_resolution();
        let stored_hash = reference.object.stored_hash();
        reference.object = ExactObjectRef::new(
            ObjectSlot::logical("store-v1/membership/resolutions/relocated.json".to_string())
                .expect("valid relocated resolution slot"),
            canonical.len() as u64,
            stored_hash,
        );
        let candidate = test_commit_ref("membership-resolution-relocation", 1);

        let error = test_membership_resolution_record(reference, canonical, candidate)
            .expect_err("relocated resolution must not enter retained authority");

        assert!(matches!(
            error,
            RemoteObjectRecordError::StoredReferenceMismatch
        ));
    }

    #[test]
    fn sole_losing_membership_resolution_becomes_exact_cleanable() {
        let (reference, _) = test_membership_resolution();
        let disposition = uploaded_retained_nonactivation_disposition(
            &RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
            CandidateOwnership {
                pending: BTreeSet::new(),
                activated: BTreeSet::new(),
                nonactivated: Vec::new(),
            },
        );

        assert!(matches!(
            disposition,
            UploadedRetainedNonactivation::Cleanup(_)
        ));
    }

    #[test]
    fn shared_membership_resolution_retains_its_remaining_candidate_owner() {
        let (reference, _) = test_membership_resolution();
        let remaining = test_commit_ref("shared-resolution-owner", 2);
        let disposition = uploaded_retained_nonactivation_disposition(
            &RetainedAuthorityObjectDomain::StoreMembershipResolution { reference },
            CandidateOwnership {
                pending: BTreeSet::from([remaining.clone()]),
                activated: BTreeSet::new(),
                nonactivated: Vec::new(),
            },
        );

        assert!(matches!(
            disposition,
            UploadedRetainedNonactivation::Retain(CandidateOwnership { pending, .. })
                if pending == BTreeSet::from([remaining])
        ));
    }

    #[test]
    fn deserialized_device_head_rejects_resolution_cleanup_state() {
        let (_, resolution_bytes) = test_membership_resolution();
        let resolution: super::super::membership::StoreMembershipConflictResolution =
            serde_json::from_slice(&resolution_bytes).expect("parse resolution fixture");
        let candidate = test_commit_ref("invalid-head-cleanup-state", 1);
        let head = super::super::store_commit::StoreDeviceHead {
            version: super::super::store_commit::STORE_PROTOCOL_VERSION,
            store_root_hash: resolution.store_root_hash,
            author_registration: resolution.replacement_acceptance.owner_registration.clone(),
            commit: candidate.clone(),
            history_summary: super::super::store_commit::ObjectHash::digest(&resolution_bytes),
            successor: super::super::store_commit::SuccessorLink {
                activation: super::super::store_commit::StreamActivation::grant_authorized(
                    resolution.store_root_hash,
                    resolution.replacement_acceptance.owner_registration,
                    resolution.replacement_grant,
                    resolution.replacement_membership,
                )
                .activation_id(),
                predecessor: None,
                next_slot: ObjectSlot::logical(
                    "store-v1/heads/invalid-cleanup-successor.json".to_string(),
                )
                .expect("valid successor slot"),
            },
            signature: "head signature is not checked by remote ownership".to_string(),
        };
        let bytes = head.to_bytes();
        let object = ExactObjectRef::new(
            ObjectSlot::logical("store-v1/heads/invalid-cleanup.json".to_string())
                .expect("valid head slot"),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        );
        let mut record = RemoteObjectRecord::candidate_activated_store_head(
            super::super::store_commit::StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object,
            },
            bytes.clone(),
            bytes,
            candidate,
        )
        .expect("prepare retained Store head");
        let RemoteObjectRecord::RetainedAuthority(retained) = &mut record else {
            panic!("Store head must use retained authority")
        };
        retained.state = RetainedAuthorityObjectState::CleanupPending {
            former_candidates: Vec::new(),
        };
        let encoded = serde_json::to_vec(&record).expect("serialize invalid retained state");
        let decoded: RemoteObjectRecord =
            serde_json::from_slice(&encoded).expect("deserialize invalid retained state");

        assert!(matches!(
            decoded.validate(),
            Err(RemoteObjectRecordError::DomainMismatch)
        ));
    }
}

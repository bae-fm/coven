use super::ownership::*;
use super::*;

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

    pub(super) fn candidate_activated_retained_authority(
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
        reference: crate::protocol::store_commit::StoreDeviceHeadRef,
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
        reference: crate::protocol::store_commit::StoreAckRef,
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

    pub(crate) fn candidate_activated_circle_acknowledgement(
        reference: CircleAckRef,
        canonical_semantic_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::CircleAcknowledgement { reference },
            ObjectHash::digest(&canonical_semantic_bytes),
            object,
            canonical_semantic_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_store_membership_resolution(
        reference: crate::protocol::membership::StoreMembershipConflictResolutionRef,
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
        reference: crate::protocol::membership::MembershipEntryRef,
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
        reference: crate::protocol::membership::MembershipHeadRef,
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
        reference: crate::protocol::wrapped_store_key::WrappedStoreKeyRef,
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
        reference: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
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
        reference: crate::protocol::store_commit::StoreDeviceExclusionOutcomeRef,
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
        reference: crate::protocol::reclaim::ReclaimEvidenceRef,
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
        reference: crate::protocol::reclaim::ReclaimAuthorizationRef,
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
        reference: crate::protocol::reclaim::ReclaimReceiptRef,
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
        stored: &crate::protocol::blob::locator::StoredBlobRef,
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
        image: &crate::protocol::store_commit::SnapshotImageRef,
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
        package: &crate::protocol::audience_package::AudiencePackage,
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
        stored: &crate::protocol::blob::locator::StoredBlobRef,
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

    pub(crate) fn candidate_owned_blob(
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        owner: StoreBatchCommitRef,
        uploaded_verified: bool,
    ) -> Result<Self, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let ownership = PendingCandidateOwnership {
            pending: BTreeSet::from([owner]),
            nonactivated: Vec::new(),
        };
        let state = if uploaded_verified {
            OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: ownership.pending,
                    activated: BTreeSet::new(),
                    nonactivated: ownership.nonactivated,
                },
            }
        } else {
            OwnedObjectState::Prepared { ownership }
        };
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            bytes: RemoteObjectBytes::blob(locator_bytes, stored.object().clone())?,
            state,
        });
        record.validate()?;
        Ok(record)
    }
}

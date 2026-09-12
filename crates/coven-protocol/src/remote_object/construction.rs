use super::ownership::*;
use super::*;

impl RemoteObjectRecord {
    pub fn prepared_owner_promotion_request_publication(
        publication: &crate::store_commit::RetainedOwnerPromotionRequestPublication,
        commit: &crate::store_commit::StoreBatchCommit,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        publication.validate_for(commit)?;
        let request = commit
            .owner_promotion_request()
            .ok_or(RemoteObjectRecordError::DomainMismatch)?;
        let bytes = publication.value.to_bytes();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::OwnerPromotionRequestPublication {
                promotion_id: request.promotion_id,
                activation: publication.value.body().clone(),
            },
            ObjectHash::digest(&bytes),
            publication.object.clone(),
            &bytes,
            &bytes,
            publication.value.commit.clone(),
        )
    }

    pub fn prepared_membership_head_acceptance(
        value: &crate::membership::MembershipHeadAcceptance,
        head: &crate::membership::AuthorHead,
        prepared: &crate::objects::PreparedExactObject,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let crate::membership::MembershipHeadActivation::StoreCommit {
            commit,
            acceptance_slot,
        } = &head.activation
        else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        if value.head.head_hash != head.head_hash()
            || value.head.coord != head.entry_coord()
            || prepared.reference().slot() != acceptance_slot
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        let bytes = value.to_bytes();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::MembershipHeadAcceptance {
                head: value.head.clone(),
                publication: value.publication()?.clone(),
            },
            ObjectHash::digest(&bytes),
            prepared.reference().clone(),
            &bytes,
            prepared.stored_bytes(),
            commit.clone(),
        )
    }

    fn candidate_exclusive_retained_authority(
        family: CandidateFamilyId,
        domain: CandidateExclusiveObjectDomain,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = domain.object().clone();
        let record = Self::CandidateExclusive(CandidateObjectRecord {
            identity: CandidateExclusiveTarget {
                family,
                domain,
                semantic_hash: ObjectHash::digest(canonical_signed_bytes),
                object,
            },
            payloads: RemoteObjectPayloads::SpooledInline,
            state: CandidateObjectState::Prepared {
                ownership: PendingCandidateOwnership {
                    pending: BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate_payload(canonical_signed_bytes)?;
        ClosedRemoteObject::with_spooled_payloads(record, canonical_signed_bytes, stored_bytes)
    }

    pub(super) fn candidate_activated_retained_authority(
        domain: RetainedAuthorityObjectDomain,
        semantic_hash: ObjectHash,
        object: ExactObjectRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let record = Self::RetainedAuthority(RetainedAuthorityRecord {
            identity: RetainedAuthorityObjectRef {
                domain,
                semantic_hash,
                object,
            },
            payloads: RemoteObjectPayloads::SpooledInline,
            state: RetainedAuthorityObjectState::Prepared {
                ownership: PendingCandidateOwnership {
                    pending: BTreeSet::from([owner]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate_payload(canonical_signed_bytes)?;
        ClosedRemoteObject::with_spooled_payloads(record, canonical_signed_bytes, stored_bytes)
    }

    pub fn candidate_commit(
        identity: StoreBatchCommitRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let record = Self::CandidateCommit(CandidateCommitRecord {
            identity,
            semantic_hash: ObjectHash::digest(canonical_signed_bytes),
            payloads: RemoteObjectPayloads::SpooledInline,
            state: CandidateCommitState::Prepared,
        });
        record.validate_payload(canonical_signed_bytes)?;
        ClosedRemoteObject::with_spooled_payloads(record, canonical_signed_bytes, stored_bytes)
    }

    pub(crate) fn candidate_activated_store_acknowledgement(
        reference: crate::store_commit::StoreAckRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::Acknowledgement { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_circle_acknowledgement(
        reference: CircleAckRef,
        canonical_semantic_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::CircleAcknowledgement { reference },
            ObjectHash::digest(canonical_semantic_bytes),
            object,
            canonical_semantic_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_activated_provider_access_grant(
        reference: crate::provider::StoreMemberProviderAccessGrantRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ProviderAccessGrant { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_activated_device_join_abandonment(
        reference: crate::store_commit::DeviceJoinAbandonmentRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceJoinAbandonment { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_activated_device_registration(
        reference: crate::store_commit::StoreDeviceRegistrationRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceRegistration { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_exclusive_merge_membership_entry(
        family: CandidateFamilyId,
        reference: crate::membership::MembershipEntryRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        Self::candidate_exclusive_retained_authority(
            family,
            CandidateExclusiveObjectDomain::MergeMembershipEntry { family, reference },
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_exclusive_merge_membership_head(
        family: CandidateFamilyId,
        reference: crate::membership::MembershipHeadRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        Self::candidate_exclusive_retained_authority(
            family,
            CandidateExclusiveObjectDomain::MergeMembershipHead { family, reference },
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub(crate) fn candidate_activated_device_exclusion_proposal(
        reference: crate::store_commit::StoreDeviceExclusionProposalRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        let semantic_hash = ObjectHash::digest(canonical_signed_bytes);
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
        reference: crate::store_commit::StoreDeviceExclusionOutcomeRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object().clone();
        let semantic_hash = ObjectHash::digest(canonical_signed_bytes);
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::DeviceExclusionOutcome { reference },
            semantic_hash,
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_activated_reclaim_evidence(
        reference: crate::reclaim::ReclaimEvidenceRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ReclaimEvidence { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn candidate_activated_reclaim_authorization(
        reference: crate::reclaim::ReclaimAuthorizationRef,
        canonical_signed_bytes: &[u8],
        stored_bytes: &[u8],
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let object = reference.object.clone();
        Self::candidate_activated_retained_authority(
            RetainedAuthorityObjectDomain::ReclaimAuthorization { reference },
            ObjectHash::digest(canonical_signed_bytes),
            object,
            canonical_signed_bytes,
            stored_bytes,
            owner,
        )
    }

    pub fn snapshot_activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            payloads: RemoteObjectPayloads::RowBlob { locator_bytes },
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        ClosedRemoteObject::carried(record)
    }

    pub fn snapshot_activated_image(
        image: &crate::store_commit::SnapshotImageRef,
        owner: SnapshotObjectOwner,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoreSnapshotImage {
                    reference: image.clone(),
                },
                semantic_hash: image.image_hash,
                object: image.object.clone(),
            },
            payloads: RemoteObjectPayloads::SpooledExternal,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        ClosedRemoteObject::carried(record)
    }

    /// The membership rollup bound to one exact snapshot metadata candidate.
    /// Its metadata slot determines the artifact slot; another snapshot uses
    /// another object even when its rollup bytes are identical.
    pub fn snapshot_activated_membership_rollup(
        rollup: &crate::store_commit::MembershipRollupRef,
        owner: SnapshotObjectOwner,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoreMembershipRollup {
                    reference: rollup.clone(),
                },
                semantic_hash: rollup.rollup_hash,
                object: rollup.object.clone(),
            },
            payloads: RemoteObjectPayloads::SpooledExternal,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        ClosedRemoteObject::carried(record)
    }

    pub fn activated_external_package(
        domain: SharedLiveSetObjectDomain,
        package: &crate::audience_package::AudiencePackage,
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
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
                object,
            },
            payloads: RemoteObjectPayloads::SpooledExternal,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        record.validate_payload(&canonical_semantic_bytes)?;
        let hash = ObjectHash::digest(&canonical_semantic_bytes);
        ClosedRemoteObject::with_payloads(
            record,
            BTreeMap::from([(hash, canonical_semantic_bytes)]),
        )
    }

    pub fn activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: StoreBatchCommitRef,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            payloads: RemoteObjectPayloads::RowBlob { locator_bytes },
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                    nonactivated: Vec::new(),
                },
            },
        });
        ClosedRemoteObject::carried(record)
    }

    pub fn candidate_owned_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: StoreBatchCommitRef,
        uploaded_verified: bool,
    ) -> Result<ClosedRemoteObject, RemoteObjectRecordError> {
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
            payloads: RemoteObjectPayloads::RowBlob { locator_bytes },
            state,
        });
        ClosedRemoteObject::carried(record)
    }
}

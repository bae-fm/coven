use super::identity::*;
use super::nonactivation::*;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateExclusiveTarget {
    pub family: CandidateFamilyId,
    pub domain: CandidateExclusiveObjectDomain,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CandidateExclusiveTarget {
    pub(super) fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        match &self.domain {
            CandidateExclusiveObjectDomain::CircleBootstrapImage { reference, .. }
                if bytes.is_empty() && self.semantic_hash == reference.image_hash =>
            {
                Ok(())
            }
            CandidateExclusiveObjectDomain::CircleBootstrapImage { .. } => {
                Err(RemoteObjectRecordError::StoredReferenceMismatch)
            }
            _ => validate_semantic_hash(self.semantic_hash, bytes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateExclusiveObjectDomain {
    MergeMembershipEntry {
        family: CandidateFamilyId,
        reference: crate::membership::MembershipEntryRef,
    },
    MergeMembershipHead {
        family: CandidateFamilyId,
        reference: crate::membership::MembershipHeadRef,
    },
    MergeMembershipWrappedStoreKey {
        family: CandidateFamilyId,
        reference: crate::wrapped_store_key::WrappedStoreKeyRef,
    },
    StorePackage {
        reference: crate::store_commit::StorePackageRef,
    },
    CirclePackage {
        reference: crate::store_commit::CirclePackageRef,
    },
    CircleAccessLeaf {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::store_commit::CircleAccessLeafObjectRef,
    },
    CircleAccessEnvelope {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::store_commit::CircleAccessEnvelopeObjectRef,
    },
    CircleEpochCloseIntent {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseIntentRef,
    },
    CircleEpochCloseOutcome {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseOutcomeRef,
    },
    CircleEpochCloseCancellation {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseCancellationRef,
    },
    CircleBootstrapImage {
        family: CandidateFamilyId,
        circle_id: CircleId,
        owner_pubkey: String,
        epoch_id: crate::circle::CircleEpochId,
        recipient_slot: String,
        reference: crate::store_commit::SnapshotImageRef,
    },
}

impl CandidateExclusiveObjectDomain {
    pub(super) fn family(&self) -> CandidateFamilyId {
        match self {
            Self::MergeMembershipEntry { family, .. }
            | Self::MergeMembershipHead { family, .. }
            | Self::MergeMembershipWrappedStoreKey { family, .. } => *family,
            Self::StorePackage { reference } => reference.candidate_family,
            Self::CirclePackage { reference } => reference.package.candidate_family,
            Self::CircleAccessLeaf { family, .. }
            | Self::CircleAccessEnvelope { family, .. }
            | Self::CircleEpochCloseIntent { family, .. }
            | Self::CircleEpochCloseOutcome { family, .. }
            | Self::CircleEpochCloseCancellation { family, .. }
            | Self::CircleBootstrapImage { family, .. } => *family,
        }
    }

    pub(super) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::MergeMembershipEntry { reference, .. } => &reference.object,
            Self::MergeMembershipHead { reference, .. } => &reference.object,
            Self::MergeMembershipWrappedStoreKey { reference, .. } => &reference.object,
            Self::StorePackage { reference } => &reference.object,
            Self::CirclePackage { reference } => &reference.package.object,
            Self::CircleAccessLeaf { reference, .. } => &reference.object,
            Self::CircleAccessEnvelope { reference, .. } => &reference.object,
            Self::CircleEpochCloseIntent { reference, .. } => &reference.object,
            Self::CircleEpochCloseOutcome { reference, .. } => &reference.object,
            Self::CircleEpochCloseCancellation { reference, .. } => &reference.object,
            Self::CircleBootstrapImage { reference, .. } => &reference.object,
        }
    }

    pub fn shared_destination(&self) -> Option<SharedLiveSetObjectDomain> {
        match self {
            Self::StorePackage { reference } => Some(SharedLiveSetObjectDomain::StorePackage {
                reference: reference.clone(),
            }),
            Self::CirclePackage { reference } => Some(SharedLiveSetObjectDomain::CirclePackage {
                reference: reference.clone(),
            }),
            Self::CircleBootstrapImage { reference, .. } => {
                Some(SharedLiveSetObjectDomain::CircleBootstrapImage {
                    reference: reference.clone(),
                })
            }
            Self::MergeMembershipEntry { .. }
            | Self::MergeMembershipHead { .. }
            | Self::MergeMembershipWrappedStoreKey { .. }
            | Self::CircleAccessLeaf { .. }
            | Self::CircleAccessEnvelope { .. }
            | Self::CircleEpochCloseIntent { .. }
            | Self::CircleEpochCloseOutcome { .. }
            | Self::CircleEpochCloseCancellation { .. } => None,
        }
    }

    pub fn retained_destination(&self) -> Option<RetainedAuthorityObjectDomain> {
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
                RetainedAuthorityObjectDomain::MergeMembershipWrappedStoreKey {
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
            Self::CircleEpochCloseIntent {
                family,
                circle_id,
                reference,
            } => Some(RetainedAuthorityObjectDomain::CircleEpochCloseIntent {
                family: *family,
                circle_id: *circle_id,
                reference: reference.clone(),
            }),
            Self::CircleEpochCloseOutcome {
                family,
                circle_id,
                reference,
            } => Some(RetainedAuthorityObjectDomain::CircleEpochCloseOutcome {
                family: *family,
                circle_id: *circle_id,
                reference: reference.clone(),
            }),
            Self::CircleEpochCloseCancellation {
                family,
                circle_id,
                reference,
            } => Some(
                RetainedAuthorityObjectDomain::CircleEpochCloseCancellation {
                    family: *family,
                    circle_id: *circle_id,
                    reference: reference.clone(),
                },
            ),
            Self::StorePackage { .. }
            | Self::CirclePackage { .. }
            | Self::CircleBootstrapImage { .. } => None,
        }
    }
}

impl SharedLiveSetObjectDomain {
    pub(super) fn package_object(&self) -> Result<&ExactObjectRef, RemoteObjectRecordError> {
        match self {
            Self::StoredBlob
            | Self::StoreSnapshotImage { .. }
            | Self::CircleBootstrapImage { .. } => Err(RemoteObjectRecordError::DomainMismatch),
            Self::StorePackage { reference } => Ok(&reference.object),
            Self::CirclePackage { reference } => Ok(&reference.package.object),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedLiveSetObjectRef {
    pub domain: SharedLiveSetObjectDomain,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl SharedLiveSetObjectRef {
    pub(super) fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
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
            SharedLiveSetObjectDomain::CircleBootstrapImage { reference }
                if bytes.is_empty()
                    && self.semantic_hash == reference.image_hash
                    && self.object == reference.object =>
            {
                Ok(())
            }
            SharedLiveSetObjectDomain::CircleBootstrapImage { .. } => {
                Err(RemoteObjectRecordError::StoredReferenceMismatch)
            }
            _ => validate_semantic_hash(self.semantic_hash, bytes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SharedLiveSetObjectDomain {
    StoredBlob,
    StoreSnapshotImage {
        reference: crate::store_commit::SnapshotImageRef,
    },
    CircleBootstrapImage {
        reference: crate::store_commit::SnapshotImageRef,
    },
    StorePackage {
        reference: crate::store_commit::StorePackageRef,
    },
    CirclePackage {
        reference: crate::store_commit::CirclePackageRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedAuthorityObjectRef {
    pub domain: RetainedAuthorityObjectDomain,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolInertObject {
    pub identity: RetainedAuthorityObjectRef,
    pub canonical_semantic_bytes: Vec<u8>,
    pub(crate) former_candidates: Vec<CandidateNonactivation>,
}

impl ProtocolInertObject {
    pub(super) fn new(
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

    pub fn object_id(&self) -> ObjectHash {
        remote_object_id(&self.identity.object)
    }

    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        validate_retained_authority_identity(&self.identity, &self.canonical_semantic_bytes)?;
        validate_nonactivations(&self.former_candidates)
    }

    pub fn candidate_nonactivation_proof(
        &self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<Option<&CandidateNonactivationProof>, RemoteObjectRecordError> {
        self.validate()?;
        find_nonactivation_proof(&self.former_candidates, candidate)
    }

    pub fn is_terminal_head_for(
        &self,
        candidate: &StoreBatchCommitRef,
        object: &ExactObjectRef,
    ) -> Result<bool, RemoteObjectRecordError> {
        self.validate()?;
        let head: crate::store_commit::StoreDeviceHead =
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

impl RetainedAuthorityObjectRef {
    pub(super) fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedAuthorityObjectDomain {
    Commit {
        reference: StoreBatchCommitRef,
    },
    DeviceHead {
        reference: crate::store_commit::StoreDeviceHeadRef,
    },
    Acknowledgement {
        reference: crate::store_commit::StoreAckRef,
    },
    CircleAcknowledgement {
        reference: CircleAckRef,
    },
    MergeMembershipWrappedStoreKey {
        reference: crate::wrapped_store_key::WrappedStoreKeyRef,
    },
    StoreMembershipResolution {
        reference: crate::membership::StoreMembershipConflictResolutionRef,
    },
    MergeMembershipEntry {
        reference: crate::membership::MembershipEntryRef,
    },
    MergeMembershipHead {
        reference: crate::membership::MembershipHeadRef,
    },
    DeviceExclusionProposal {
        reference: crate::store_commit::StoreDeviceExclusionProposalRef,
    },
    DeviceExclusionOutcome {
        reference: crate::store_commit::StoreDeviceExclusionOutcomeRef,
    },
    ReclaimEvidence {
        reference: crate::reclaim::ReclaimEvidenceRef,
    },
    ReclaimAuthorization {
        reference: crate::reclaim::ReclaimAuthorizationRef,
    },
    ReclaimReceipt {
        reference: crate::reclaim::ReclaimReceiptRef,
    },
    CircleAccessLeaf {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::store_commit::CircleAccessLeafObjectRef,
    },
    CircleAccessEnvelope {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::store_commit::CircleAccessEnvelopeObjectRef,
    },
    CircleEpochCloseIntent {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseIntentRef,
    },
    CircleEpochCloseOutcome {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseOutcomeRef,
    },
    CircleEpochCloseCancellation {
        family: CandidateFamilyId,
        circle_id: CircleId,
        reference: crate::circle_control::CircleEpochCloseCancellationRef,
    },
}

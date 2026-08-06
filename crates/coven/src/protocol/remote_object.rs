//! Closed local publication and ownership state for remote protocol objects.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle::CircleId;
use super::store_commit::{
    CandidateFamilyId, CircleAckRef, ObjectHash, StoreBatchCommitRef, StreamActivationId,
};
use crate::protocol::objects::ExactObjectRef;

mod construction;
use nonactivation::validate_nonactivations;
mod domains;
mod graph;
mod identity;
mod lifecycle;
mod nonactivation;
mod ownership;
mod reclaim;

pub(crate) use domains::{
    CandidateExclusiveObjectDomain, CandidateExclusiveTarget, ProtocolInertObject,
    RetainedAuthorityObjectDomain, RetainedAuthorityObjectRef, SharedLiveSetObjectDomain,
    SharedLiveSetObjectRef,
};
pub(crate) use graph::{CandidateObjectGraph, CandidateObjectMaterial};
pub(crate) use nonactivation::{
    CandidateNonactivation, CandidateNonactivationProof, VerifiedCandidateHead,
    VerifiedCandidateHeadNonactivation, VerifiedCandidateNonactivation,
    VerifiedDependencyRetractionAuthority,
};
pub(crate) use ownership::{
    CandidateOwnership, OwnedObjectState, PendingCandidateOwnership, RetainedReplayOwner,
    SharedObjectOwner, SharedObjectOwnership, SnapshotObjectOwner,
};

const REMOTE_OBJECT_ID_DOMAIN: &[u8] = b"coven.remote-object-id.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RemoteObjectRecord {
    CandidateCommit(CandidateCommitRecord),
    CandidateExclusive(CandidateObjectRecord),
    RetainedAuthority(RetainedAuthorityRecord),
    SharedLiveSet(SharedObjectRecord),
}

impl RemoteObjectRecord {}

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

pub(crate) use super::store_commit::StoreBatchCommitDeletionTarget;

#[derive(Debug, thiserror::Error)]
pub enum RemoteObjectRecordError {
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
mod tests;

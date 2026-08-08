//! Closed local publication and ownership state for remote protocol objects.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle::CircleId;
use super::store_commit::{
    CandidateFamilyId, CircleAckRef, ObjectHash, StoreBatchCommitRef, StreamActivationId,
};
use crate::objects::ExactObjectRef;

mod construction;
use nonactivation::validate_nonactivations;
mod domains;
mod graph;
mod identity;
mod lifecycle;
mod nonactivation;
mod ownership;
mod reclaim;

pub use domains::{
    CandidateExclusiveObjectDomain, CandidateExclusiveTarget, ProtocolInertObject,
    RetainedAuthorityObjectDomain, RetainedAuthorityObjectRef, SharedLiveSetObjectDomain,
    SharedLiveSetObjectRef,
};
pub use graph::{CandidateObjectGraph, CandidateObjectMaterial};
pub use nonactivation::{
    CandidateNonactivation, CandidateNonactivationProof, VerifiedCandidateHead,
    VerifiedCandidateHeadNonactivation, VerifiedCandidateNonactivation,
    VerifiedDependencyRetractionAuthority,
};
pub use ownership::{
    CandidateOwnership, OwnedObjectState, PendingCandidateOwnership, RetainedReplayOwner,
    SharedObjectOwner, SharedObjectOwnership, SnapshotObjectOwner,
};

const REMOTE_OBJECT_ID_DOMAIN: &[u8] = b"coven.remote-object-id.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteObjectRecord {
    CandidateCommit(CandidateCommitRecord),
    CandidateExclusive(CandidateObjectRecord),
    RetainedAuthority(RetainedAuthorityRecord),
    SharedLiveSet(SharedObjectRecord),
}

impl RemoteObjectRecord {}

pub fn remote_object_id(object: &ExactObjectRef) -> ObjectHash {
    let mut material = REMOTE_OBJECT_ID_DOMAIN.to_vec();
    material.extend(serde_json::to_vec(object).expect("ExactObjectRef serialization cannot fail"));
    ObjectHash::digest(&material)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateObjectRecord {
    pub identity: CandidateExclusiveTarget,
    pub payloads: RemoteObjectPayloads,
    pub state: CandidateObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCommitRecord {
    pub identity: StoreBatchCommitRef,
    /// The digest of the commit's canonical signed bytes, which is the name
    /// their payload file carries. A commit reference names the commit by its
    /// signed-body hash and its stored object, neither of which is the digest
    /// of the bytes as serialized, so the record carries it.
    pub semantic_hash: ObjectHash,
    pub payloads: RemoteObjectPayloads,
    pub state: CandidateCommitState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedAuthorityRecord {
    pub identity: RetainedAuthorityObjectRef,
    pub payloads: RemoteObjectPayloads,
    pub state: RetainedAuthorityObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedAuthorityObjectState {
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
    pub fn validate(&self) -> Result<(), RemoteObjectRecordError> {
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
pub struct SharedObjectRecord {
    pub identity: SharedLiveSetObjectRef,
    pub payloads: RemoteObjectPayloads,
    pub state: OwnedObjectState,
}

/// Where a remote object's payloads are, and what upload that implies.
///
/// A stored blob's row rides inside published snapshot and bootstrap images,
/// where a restoring device holds the row but none of the writing device's
/// payload spool, so it carries its locator in the row. Every other domain is
/// read only on the device that wrote it and names its payloads in the spool:
/// the plaintext under the identity's semantic hash, the ciphertext under the
/// exact object's stored hash. Neither hash is repeated here — the identity
/// already names both, and a second copy would be a second thing to keep in
/// agreement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteObjectPayloads {
    /// Plaintext and ciphertext both in the spool. This device uploads the
    /// ciphertext from there.
    SpooledInline,
    /// The ciphertext was created outside this record — a staged image, or a
    /// package this device observed rather than sealed — so this record never
    /// uploads it. The plaintext is in the spool, except for the image domains,
    /// which have no plaintext at all.
    SpooledExternal,
    /// The blob locator, in the row. The body is in the blob store and the
    /// device uploads it from its blob spool.
    RowBlob { locator_bytes: Vec<u8> },
}

impl RemoteObjectPayloads {
    /// The locator a stored blob's row carries, and nothing for the domains
    /// whose payloads are in the spool.
    pub fn carried_locator_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::RowBlob { locator_bytes } => Some(locator_bytes),
            Self::SpooledInline | Self::SpooledExternal => None,
        }
    }
}

/// One closed remote object and the payload bytes its row will name.
///
/// A record holds references into the payload spool, so a record on its own is
/// not yet something a row can name — the files have to be there first. This
/// carries both from the moment the record is closed to the transaction that
/// installs the files and writes the row, and the map is keyed by exactly the
/// hashes the record claims, so a claim whose bytes are missing cannot be
/// written down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedRemoteObject {
    record: RemoteObjectRecord,
    payloads: BTreeMap<ObjectHash, Vec<u8>>,
}

impl ClosedRemoteObject {
    /// A record whose payloads are named by other rows: a stored blob, whose
    /// body is in the blob store, or an image, whose bytes are staged by the
    /// flow that built it.
    pub(crate) fn carried(record: RemoteObjectRecord) -> Result<Self, RemoteObjectRecordError> {
        Self::with_payloads(record, BTreeMap::new())
    }

    /// Close a record with the plaintext and ciphertext its spool claims name.
    /// The exact object verifies the ciphertext here, so every constructor uses
    /// the same payload assembly and stored-byte check.
    fn with_spooled_payloads(
        record: RemoteObjectRecord,
        canonical_semantic_bytes: &[u8],
        stored_bytes: &[u8],
    ) -> Result<Self, RemoteObjectRecordError> {
        let mut payloads = BTreeMap::new();
        if let SemanticPayload::Spooled(hash) = record.semantic_payload() {
            payloads.insert(hash, canonical_semantic_bytes.to_vec());
        }
        if let Some(hash) = record.stored_payload() {
            record
                .object()
                .verify(stored_bytes)
                .map_err(|error| RemoteObjectRecordError::StoredBytes(error.to_string()))?;
            payloads.insert(hash, stored_bytes.to_vec());
        }
        Self::with_payloads(record, payloads)
    }

    /// A record and the bytes for exactly the payloads it claims.
    ///
    /// Used both when a record is first closed and when one is read back from
    /// its row alongside its spool files. The spool names files by the digest of
    /// their contents, so bytes found under a claimed hash are that payload; all
    /// this has to check is that the set matches.
    pub fn with_payloads(
        record: RemoteObjectRecord,
        payloads: BTreeMap<ObjectHash, Vec<u8>>,
    ) -> Result<Self, RemoteObjectRecordError> {
        if payloads.keys().copied().collect::<BTreeSet<_>>() != record.payload_claims() {
            return Err(RemoteObjectRecordError::PayloadPlacement);
        }
        Ok(Self { record, payloads })
    }

    pub fn record(&self) -> &RemoteObjectRecord {
        &self.record
    }

    pub fn into_record(self) -> RemoteObjectRecord {
        self.record
    }

    /// Advance the record this holds, keeping its payloads. A transition never
    /// changes what a record names — neither hash mutates and the domain changes
    /// re-wrap the same reference — so the payload set carries over unchanged,
    /// and is re-checked against the new record rather than assumed.
    pub fn map_record(
        self,
        transition: impl FnOnce(
            RemoteObjectRecord,
        ) -> Result<RemoteObjectRecord, RemoteObjectRecordError>,
    ) -> Result<Self, RemoteObjectRecordError> {
        Self::with_payloads(transition(self.record)?, self.payloads)
    }

    /// The payload files this record names, by the hash each is stored under.
    /// Named apart from the record's own [`RemoteObjectRecord::payloads`],
    /// which says *where* the payloads are rather than carrying them.
    pub fn payload_bytes(&self) -> &BTreeMap<ObjectHash, Vec<u8>> {
        &self.payloads
    }

    /// The record's plaintext: the locator a stored blob's row carries, or the
    /// spool file every other domain's identity names. `None` for the image
    /// domains, which name their payload by reference and have no body here.
    pub fn semantic_bytes(&self) -> Option<&[u8]> {
        match self.record.semantic_payload() {
            SemanticPayload::Carried(bytes) => Some(bytes),
            SemanticPayload::Spooled(hash) => self.payloads.get(&hash).map(Vec::as_slice),
            SemanticPayload::Absent => None,
        }
    }

    /// The ciphertext this record uploads, for the domains that seal one.
    pub fn stored_bytes(&self) -> Option<&[u8]> {
        self.record
            .stored_payload()
            .and_then(|hash| self.payloads.get(&hash).map(Vec::as_slice))
    }
}

impl std::ops::Deref for ClosedRemoteObject {
    type Target = RemoteObjectRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

/// Where one record's plaintext is: in the row, in the spool, or nowhere,
/// because the image domains name their payload by reference and have no
/// semantic body of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticPayload<'record> {
    Carried(&'record [u8]),
    Spooled(ObjectHash),
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateObjectState {
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
pub enum CandidateCommitState {
    Prepared,
    UploadedVerified,
    CleanupPending { proof: CandidateNonactivationProof },
    AbsentVerified { proof: CandidateNonactivationProof },
}

pub use super::store_commit::StoreBatchCommitDeletionTarget;

#[derive(Debug, thiserror::Error)]
pub enum RemoteObjectRecordError {
    #[error("prepared stored bytes do not match their exact reference: {0}")]
    StoredBytes(String),
    #[error("remote object payload placement contradicts its domain")]
    PayloadPlacement,
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

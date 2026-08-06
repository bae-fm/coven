//! Circle metadata, access records, controls, and creation objects.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::causal_grants::AuthorStreamId;
use super::circle::CircleEpochCloseId;
use super::circle::{generated_id_digest, AccessLeafId, CircleEpochId, CircleId};
use super::circle_roster::{
    CircleAuthorStreamKey, CircleGrantCreationAuthority, CircleMaterializedRoster,
    CircleRosterChain, CircleRosterEntry, CircleRosterError, CircleRosterHead, CircleRosterHeadRef,
    CircleRosterStateRef, MergeCircleRosterStateRef, ResolvedCircleRoster,
};
use super::membership::{MemberRole, MembershipGrantCreationAuthority, MembershipGrantId};
use super::membership::{MembershipHeadRef, StoreMembershipConflictResolutionRef};
use super::store_commit::{
    CommitFrontier, ObjectHash, OwnerRecoveryCursor, Signed, SignedBody, SnapshotImageRef,
    StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    SuccessorLink,
};
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::objects::ObjectSlot;
use coven_keys::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use coven_keys::keys::{self, UserKeypair};

const RECIPIENT_SLOT_DOMAIN: &[u8] = b"coven.circle-recipient-slot.v1\0";
const METADATA_DOMAIN: &[u8] = b"coven.circle-metadata.v1\0";
const METADATA_HEAD_DOMAIN: &[u8] = b"coven.circle-metadata-head.v1\0";
const ACCESS_DOMAIN: &[u8] = b"coven.circle-access-leaf.v1\0";
const CONTROL_DOMAIN: &[u8] = b"coven.circle-control.v1\0";
const CONTROL_HEAD_DOMAIN: &[u8] = b"coven.circle-control-head.v1\0";
const CLOSE_INTENT_DOMAIN: &[u8] = b"coven.circle-epoch-close-intent.v1\0";
const CLOSE_RESPONSE_DOMAIN: &[u8] = b"coven.circle-epoch-close-response.v1\0";
const CLOSE_EXCLUSION_DOMAIN: &[u8] = b"coven.circle-epoch-close-exclusion.v1\0";
const CLOSE_OUTCOME_DOMAIN: &[u8] = b"coven.circle-epoch-close-outcome.v1\0";
const CLOSE_CANCELLATION_DOMAIN: &[u8] = b"coven.circle-epoch-close-cancellation.v1\0";
const ENVELOPE_DOMAIN: &[u8] = b"coven.circle-access-envelope.v1\0";
const OWNER_GRANT_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-owner-grant-id-generation.v1\0";

mod access;
mod control;
mod drafts;
mod epoch_close;
mod epoch_transition;
mod metadata;
mod semantic_path;
mod transition;

pub(crate) use access::{
    merkle_root_and_proofs, verify_merkle_proof, CircleAccessDisposition, CircleAccessLeaf,
    CircleAccessLeafBody, MerkleStep,
};
pub(crate) use access::{CircleBootstrapCoverageRef, CircleBootstrapRef};
pub(crate) use control::{
    merge_frontier_head, AccessEnvelope, AccessEnvelopeBody, CircleControl, CircleControlBody,
    CircleControlHead, CircleControlState, CircleControlValue, DeletedCircle,
    MergeCircleControlHeadRef, MergeCircleControlOrder, MergeCircleOwnerAuthorityRef,
    ResolvedConflictBranch,
};
#[cfg(test)]
pub(crate) use drafts::CircleTransitionDraftPolicy;
pub(crate) use drafts::{
    CircleRosterDraftPolicy, CircleRosterPolicyObjects, CircleTransitionDraft,
    CircleTransitionPolicyObjects, PreparedAccessLeaf, PreparedCircleAccess, PreparedCircleControl,
    PreparedCircleTransition,
};
pub(crate) use epoch_close::{
    ActiveCircleEpochCore, CircleEpochClose, CircleEpochCloseCancellation,
    CircleEpochCloseExclusion, CircleEpochCloseExclusionRef, CircleEpochCloseIntent,
    CircleEpochCloseOutcome, CircleEpochCloseParticipant, CircleEpochCloseResponse,
    CircleEpochCloseResponseRef, CircleEpochCloseResponseSlotValue, CircleEpochCloseSettlement,
    CircleEpochCloseSlotValue, CircleEpochOrigin, CircleEpochSuccessor, MergeActiveCircleEpoch,
};
pub(crate) use epoch_close::{
    CircleEpochCloseCancellationRef, CircleEpochCloseIntentRef, CircleEpochCloseOutcomeRef,
};
pub(crate) use metadata::CircleMetadataHeadRef;
pub(crate) use metadata::{
    CircleMetadata, CircleMetadataBody, CircleMetadataCoord, CircleMetadataHead,
    CircleMetadataStateRef, MergeCircleMetadataStateRef,
};
pub(crate) use semantic_path::{
    circle_control_head_prefix, circle_epoch_close_intent_semantic_prefix,
    circle_epoch_close_outcome_semantic_prefix, circle_epoch_close_response_semantic_prefix,
    circle_metadata_head_prefix, circle_roster_head_prefix, circle_semantic_prefix, recipient_slot,
    recipient_slot_with_peer, verify_circle_semantic_prefix, CircleSemanticSlot,
};

/// Exact coordinate of one signed circle control entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlCoord {
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub control_hash: ObjectHash,
}

impl CircleControlCoord {
    pub fn control_hash(&self) -> ObjectHash {
        self.control_hash
    }

    pub fn validate(&self) -> Result<(), CircleControlCoordError> {
        if self.device_id.is_empty() || self.author_pubkey.is_empty() || self.seq == 0 {
            Err(CircleControlCoordError)
        } else {
            Ok(())
        }
    }

    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }

    /// A well-formed coordinate that names no real control, for API dispatch tests
    /// that only need a value to send through the command channel.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn placeholder(seed: u8) -> Self {
        let hash = ObjectHash::digest(&[seed]);
        Self {
            device_id: format!("device-{seed}"),
            stream_id: AuthorStreamId::from_digest(hash),
            author_pubkey: format!("pubkey-{seed}"),
            author_owner_grant: MembershipGrantId(hash),
            seq: 1,
            control_hash: hash,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("circle control coordinate has an empty device/author or zero sequence/generation")]
pub struct CircleControlCoordError;

/// The exact Store membership state whose identities require access dispositions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipStateRef {
    pub heads: Vec<MembershipHeadRef>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
    pub recovery: Vec<OwnerRecoveryCursor>,
    pub state_hash: ObjectHash,
}

impl StoreMembershipStateRef {
    pub fn from_parts(
        mut heads: Vec<MembershipHeadRef>,
        mut resolutions: Vec<StoreMembershipConflictResolutionRef>,
        recovery: Vec<OwnerRecoveryCursor>,
        membership_state_hash: ObjectHash,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        heads.sort();
        resolutions.sort();
        let recovery = super::store_commit::canonical_recovery_cursors(recovery)?;
        Ok(Self {
            heads,
            resolutions,
            state_hash: membership_state_ref_hash(membership_state_hash, &recovery),
            recovery,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn recovery(&self) -> &[OwnerRecoveryCursor] {
        &self.recovery
    }

    pub(crate) fn validate_shape(&self) -> Result<(), super::store_commit::StoreProtocolError> {
        super::store_commit::validate_recovery_cursors(self.recovery())?;
        if self.heads.windows(2).any(|pair| pair[0] >= pair[1])
            || self.resolutions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(super::store_commit::StoreProtocolError::Malformed(
                "Store membership state reference is not canonical".to_string(),
            ));
        }
        Ok(())
    }
}

fn membership_state_ref_hash(
    membership_state_hash: ObjectHash,
    recovery: &[OwnerRecoveryCursor],
) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(&(
            "coven.store-membership-state-ref.v1",
            membership_state_hash,
            recovery,
        ))
        .expect("Store membership state hash serialization cannot fail"),
    )
}

impl CircleTransitionDraft {}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CircleTransitionError {
    #[error("circle name cannot be empty")]
    EmptyName,
    #[error("circle creator is not a current Store writer")]
    AuthorNotStoreWriter,
    #[error("circle operation author is not a current Circle Owner")]
    AuthorNotCircleOwner,
    #[error("circle operation current state is invalid")]
    InvalidCurrentState,
    #[error("circle transition sequence overflow")]
    SequenceOverflow,
    #[error("circle recipient has an invalid Ed25519 public key: {0}")]
    InvalidRecipient(String),
    #[error("circle member is not a current Store member: {0}")]
    MemberNotInStore(String),
    #[error("circle roster: {0}")]
    Roster(#[from] CircleRosterError),
}

#[cfg(test)]
mod tests;

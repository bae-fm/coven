//! Verification and materialization of Store-activated Circle state.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle::{
    circle_semantic_prefix, recipient_slot_with_peer, verify_circle_semantic_prefix,
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleControl, CircleControlCoord,
    CircleId, CircleMetadata, CircleMetadataHeadRef, CircleRosterHeadRef, CircleSemanticSlot,
    MergeCircleOwnerAuthorityRef, PreparedAccessLeaf, PreparedCircleAccess, PreparedCircleControl,
    ResolvedCircleRoster, StoreMembershipStateRef,
};
use super::circle_ops::CircleOperationError;
use super::circle_roster::CircleMaterializedRoster;
use super::storage::{ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix, CandidateFamilyId,
    CircleAccessObjectRef, CircleActivationObjects, CircleControlRef, GrantStreamAnchor,
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreRootRef,
    StreamActivation, StreamActivationId,
};
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleReference {
    pub reference: CircleControlRef,
    pub circle_id: CircleId,
    pub control: PreparedCircleControl,
    pub local_access: Option<VerifiedCircleAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleAccess {
    pub envelope: AccessEnvelope,
    pub leaf: PreparedAccessLeaf,
    pub active: Option<VerifiedCircleActive>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleActive {
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedStreamActivations {
    activating_commit: StoreBatchCommitRef,
    activations: Vec<StreamActivation>,
}

impl VerifiedStreamActivations {
    pub(crate) fn none(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        if !commit.stream_activations().is_empty() {
            return Err(super::store_commit::StoreProtocolError::Malformed(
                "Store commit stream activations have not been verified".to_string(),
            ));
        }
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: Vec::new(),
        })
    }

    fn from_verified_circle_commit(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub(crate) fn from_verified_store_control(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        if commit.control().is_none() {
            return Err(super::store_commit::StoreProtocolError::Malformed(
                "verified Store membership activations carry another control".to_string(),
            ));
        }
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub(crate) fn as_slice(&self) -> &[StreamActivation] {
        &self.activations
    }

    pub(crate) fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedStreamActivationPrefix {
    by_activation: BTreeMap<StreamActivationId, (StreamActivation, StoreBatchCommitRef)>,
}

impl VerifiedStreamActivationPrefix {
    pub(crate) fn empty() -> Self {
        Self {
            by_activation: BTreeMap::new(),
        }
    }

    fn activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Option<&(StreamActivation, StoreBatchCommitRef)> {
        self.by_activation.get(&activation_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleActivations {
    circles: Vec<VerifiedCircleReference>,
    stream_activations: VerifiedStreamActivations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleActivations {
    activating_commit: StoreBatchCommitRef,
    circles: Vec<RetainedCircleReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleReference {
    reference: CircleControlRef,
    circle_id: CircleId,
    control: PreparedCircleControl,
    local_access: Option<RetainedCircleAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCircleAccess {
    access: PreparedCircleAccess,
    state: RetainedCircleAccessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RetainedCircleAccessState {
    Active {
        roster: CircleMaterializedRoster,
        metadata: CircleMetadata,
    },
    Inactive,
}

impl VerifiedCircleActivations {
    pub(crate) fn none(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::none(commit, commit_ref)?,
        })
    }

    pub(crate) fn membership_control(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        if !commit.circle_controls().is_empty() {
            return Err(super::store_commit::StoreProtocolError::Malformed(
                "Store membership control also carries Circle controls".to_string(),
            ));
        }
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::from_verified_store_control(
                commit, commit_ref,
            )?,
        })
    }

    pub(crate) fn circles(&self) -> &[VerifiedCircleReference] {
        &self.circles
    }

    pub(crate) fn stream_activations(&self) -> &VerifiedStreamActivations {
        &self.stream_activations
    }

    pub(crate) fn to_retained(&self) -> Result<Vec<u8>, CircleOperationError> {
        let retained = RetainedCircleActivations {
            activating_commit: self.stream_activations.activating_commit.clone(),
            circles: self
                .circles
                .iter()
                .map(RetainedCircleReference::from_verified)
                .collect(),
        };
        serde_json::to_vec(&retained).map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "serialize retained Circle activations: {error}"
            ))
        })
    }

    pub(crate) fn parse_retained(
        bytes: &[u8],
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        author: &StoreDeviceRegistration,
        recipient_pubkey: Option<&str>,
    ) -> Result<Self, CircleOperationError> {
        let retained: RetainedCircleActivations =
            serde_json::from_slice(bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse retained Circle activations: {error}"
                ))
            })?;
        let canonical = serde_json::to_vec(&retained).map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "serialize parsed retained Circle activations: {error}"
            ))
        })?;
        if canonical != bytes {
            return Err(CircleOperationError::InvalidState(
                "retained Circle activation bytes are not canonical".to_string(),
            ));
        }
        commit_ref
            .verify_commit(commit)
            .and_then(|()| commit.verify_at(commit.store_root_hash, &commit_ref.coord, author))
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if retained.activating_commit != *commit_ref
            || retained.circles.len() != commit.circle_controls().len()
        {
            return Err(CircleOperationError::InvalidState(
                "retained Circle activations differ from their exact Store commit".to_string(),
            ));
        }

        let circles = retained
            .circles
            .into_iter()
            .zip(commit.circle_controls())
            .map(|(retained, reference)| {
                retained.verify_and_open(commit, commit_ref, author, recipient_pubkey, reference)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            circles,
            stream_activations: VerifiedStreamActivations::from_verified_circle_commit(
                commit, commit_ref,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
        })
    }
}

impl RetainedCircleReference {
    fn from_verified(verified: &VerifiedCircleReference) -> Self {
        Self {
            reference: verified.reference.clone(),
            circle_id: verified.circle_id,
            control: verified.control.clone(),
            local_access: verified
                .local_access
                .as_ref()
                .map(RetainedCircleAccess::from_verified),
        }
    }

    fn verify_and_open(
        self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        author: &StoreDeviceRegistration,
        recipient_pubkey: Option<&str>,
        reference: &CircleControlRef,
    ) -> Result<VerifiedCircleReference, CircleOperationError> {
        if self.reference != *reference || self.circle_id != reference.circle_id() {
            return Err(CircleOperationError::InvalidState(
                "retained Circle reference differs from its exact Store commit".to_string(),
            ));
        }
        verify_control_context(reference, &self.control, commit_ref, commit, author)?;
        let local_access = self
            .local_access
            .map(|access| {
                access.verify_and_open(commit, reference, &self.control, recipient_pubkey)
            })
            .transpose()?;
        let verified = VerifiedCircleReference {
            reference: self.reference,
            circle_id: self.circle_id,
            control: self.control,
            local_access,
        };
        CircleCurrentState::from_verified(commit.candidate_family(), &verified).map_err(
            |error| {
                CircleOperationError::InvalidState(format!(
                    "retained Circle activation state failed verification: {error}"
                ))
            },
        )?;
        Ok(verified)
    }
}

impl RetainedCircleAccess {
    fn from_verified(verified: &VerifiedCircleAccess) -> Self {
        let state = match &verified.active {
            Some(active) => RetainedCircleAccessState::Active {
                roster: active.roster.clone(),
                metadata: active.metadata.clone(),
            },
            None => RetainedCircleAccessState::Inactive,
        };
        Self {
            access: PreparedCircleAccess {
                leaf: verified.leaf.clone(),
                envelope: verified.envelope.clone(),
            },
            state,
        }
    }

    fn verify_and_open(
        self,
        commit: &StoreBatchCommit,
        reference: &CircleControlRef,
        control: &PreparedCircleControl,
        recipient_pubkey: Option<&str>,
    ) -> Result<VerifiedCircleAccess, CircleOperationError> {
        if !self.access.leaf.verify_envelope(
            control,
            &self.access.envelope,
            commit.candidate_family(),
        ) {
            return Err(CircleOperationError::InvalidState(
                "retained Circle access leaf and envelope failed verification".to_string(),
            ));
        }
        if let Some(recipient_pubkey) = recipient_pubkey {
            if self.access.leaf.value.recipient_pubkey != recipient_pubkey {
                return Err(CircleOperationError::InvalidState(
                    "retained Circle access names another local recipient".to_string(),
                ));
            }
        }
        if !reference
            .objects()
            .access
            .iter()
            .any(|candidate| retained_access_matches(candidate, &self.access))
        {
            return Err(CircleOperationError::InvalidState(
                "retained Circle access differs from every exact commit reference".to_string(),
            ));
        }
        let active = match (self.access.leaf.value.disposition.clone(), self.state) {
            (
                CircleAccessDisposition::Active { .. },
                RetainedCircleAccessState::Active { roster, metadata },
            ) => Some(VerifiedCircleActive { roster, metadata }),
            (CircleAccessDisposition::Inactive, RetainedCircleAccessState::Inactive) => None,
            _ => {
                return Err(CircleOperationError::InvalidState(
                    "retained Circle access state differs from its signed disposition".to_string(),
                ));
            }
        };
        Ok(VerifiedCircleAccess {
            envelope: self.access.envelope,
            leaf: self.access.leaf,
            active,
        })
    }
}

fn retained_access_matches(
    reference: &CircleAccessObjectRef,
    access: &PreparedCircleAccess,
) -> bool {
    reference.envelope.owner_pubkey == access.envelope.owner_pubkey
        && reference.envelope.recipient_slot == access.envelope.recipient_slot
        && reference.envelope.control_hash == access.envelope.control_hash
        && reference.envelope.leaf_id == access.envelope.leaf_id
        && reference.envelope.leaf_hash == access.envelope.leaf_hash
        && reference.leaf.owner_pubkey == access.leaf.value.owner_pubkey
        && reference.leaf.epoch_id == access.leaf.value.epoch_id
        && reference.leaf.recipient_slot == access.leaf.value.recipient_slot
        && reference.leaf.leaf_id == access.leaf.value.leaf_id
        && reference.leaf.leaf_hash == access.leaf.leaf_hash
        && reference.leaf.object.stored_hash() == access.leaf.leaf_hash
        && u64::try_from(access.leaf.bytes.len())
            .is_ok_and(|size| reference.leaf.object.stored_size() == size)
}

#[derive(Debug, Clone)]
pub(crate) struct CircleAuthoringState {
    pub candidate_family: CandidateFamilyId,
    pub control: PreparedCircleControl,
    pub access: CircleAccessLeaf,
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleCurrentControl {
    control: PreparedCircleControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleInactiveAccess {
    NotGranted,
    Inactive {
        candidate_family: CandidateFamilyId,
        access: CircleAccessLeaf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) struct CircleActiveState {
    current: CircleCurrentControl,
    candidate_family: CandidateFamilyId,
    access: CircleAccessLeaf,
    roster: CircleMaterializedRoster,
    metadata: CircleMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleInactiveState {
    current: CircleCurrentControl,
    access: CircleInactiveAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleCurrentState {
    Active(Box<CircleActiveState>),
    Inactive(Box<CircleInactiveState>),
    ControlConflict { branches: Vec<CircleCurrentControl> },
}

impl CircleCurrentControl {
    fn from_verified(activation: &VerifiedCircleReference) -> Self {
        Self {
            control: activation.control.clone(),
        }
    }

    pub(crate) fn circle_id(&self) -> CircleId {
        self.control.value.circle_id
    }

    pub(crate) fn coordinate(&self) -> &CircleControlCoord {
        &self.control.coord
    }

    fn control_hash(&self) -> ObjectHash {
        self.control.coord.control_hash()
    }

    fn causally_covers(&self, prior: &Self) -> bool {
        self.control.value.causally_covers(&prior.control.value)
    }

    fn verify(&self) -> bool {
        self.control.verify()
    }
}

impl CircleCurrentState {
    pub(crate) fn from_verified(
        candidate_family: CandidateFamilyId,
        activation: &VerifiedCircleReference,
    ) -> Result<Self, String> {
        let current = CircleCurrentControl::from_verified(activation);
        let state = match &activation.local_access {
            None => Self::Inactive(Box::new(CircleInactiveState {
                current,
                access: CircleInactiveAccess::NotGranted,
            })),
            Some(VerifiedCircleAccess {
                leaf, active: None, ..
            }) => Self::Inactive(Box::new(CircleInactiveState {
                current,
                access: CircleInactiveAccess::Inactive {
                    candidate_family,
                    access: leaf.value.clone(),
                },
            })),
            Some(VerifiedCircleAccess {
                leaf,
                active: Some(active),
                ..
            }) => Self::Active(Box::new(CircleActiveState {
                current,
                candidate_family,
                access: leaf.value.clone(),
                roster: active.roster.clone(),
                metadata: active.metadata.clone(),
            })),
        };
        if state.verify() {
            Ok(state)
        } else {
            Err("verified Circle activation cannot form a valid current state".to_string())
        }
    }

    pub(crate) fn advance(self, next: Self) -> Result<Self, String> {
        if !self.verify() || !next.verify() {
            return Err("Circle current-state reduction received invalid state".to_string());
        }
        if self.circle_id() != next.circle_id() {
            return Err("Circle current-state reduction crossed Circle identities".to_string());
        }
        match self {
            Self::Active(active) => advance_resolved_control(active.current, next),
            Self::Inactive(inactive) => advance_resolved_control(inactive.current, next),
            Self::ControlConflict { mut branches } => {
                let next_current = next
                    .resolved_control()
                    .ok_or_else(|| "new Circle activation is already conflicted".to_string())?
                    .clone();
                branches.retain(|branch| !next_current.causally_covers(branch));
                if branches.is_empty() {
                    return Ok(next);
                }
                branches.push(next_current);
                canonicalize_control_branches(&mut branches)?;
                Ok(Self::ControlConflict { branches })
            }
        }
    }

    pub(crate) fn verify(&self) -> bool {
        match self {
            Self::Active(active) => {
                active.current.verify()
                    && active
                        .access
                        .verify_for_control(&active.current.control, active.candidate_family)
                    && matches!(
                        active.access.disposition,
                        CircleAccessDisposition::Active { .. }
                    )
                    && active.roster.verify()
                    && active.metadata.verify()
                    && active.metadata.circle_id == active.current.circle_id()
                    && active.metadata.epoch_id == active.current.control.value.epoch_id()
                    && active.metadata.key_fingerprint
                        == active.current.control.value.key_fingerprint()
                    && metadata_matches_control(&active.metadata, &active.current.control.value)
                    && roster_matches_control(&active.roster, &active.current.control.value)
            }
            Self::Inactive(inactive) => {
                inactive.current.verify()
                    && match &inactive.access {
                        CircleInactiveAccess::NotGranted => true,
                        CircleInactiveAccess::Inactive {
                            candidate_family,
                            access,
                        } => {
                            access.verify_for_control(&inactive.current.control, *candidate_family)
                                && matches!(access.disposition, CircleAccessDisposition::Inactive)
                        }
                    }
            }
            Self::ControlConflict { branches } => {
                branches.len() >= 2
                    && branches.iter().all(|branch| {
                        branch.verify() && branch.circle_id() == branches[0].circle_id()
                    })
                    && branches
                        .windows(2)
                        .all(|pair| pair[0].control_hash() < pair[1].control_hash())
            }
        }
    }

    pub(crate) fn circle_id(&self) -> CircleId {
        match self {
            Self::Active(active) => active.current.circle_id(),
            Self::Inactive(inactive) => inactive.current.circle_id(),
            Self::ControlConflict { branches } => branches[0].circle_id(),
        }
    }

    pub(crate) fn active(
        &self,
    ) -> Option<(
        &CircleCurrentControl,
        &CircleAccessLeaf,
        &CircleMaterializedRoster,
        &CircleMetadata,
    )> {
        match self {
            Self::Active(active) => Some((
                &active.current,
                &active.access,
                &active.roster,
                &active.metadata,
            )),
            Self::Inactive(_) | Self::ControlConflict { .. } => None,
        }
    }

    pub(crate) fn active_record_count(&self) -> usize {
        match self {
            Self::Active(_) => 1,
            Self::Inactive(_) => 0,
            Self::ControlConflict { branches } => branches.len(),
        }
    }

    pub(crate) fn authoring_state(&self) -> Option<CircleAuthoringState> {
        match self {
            Self::Active(active) => Some(CircleAuthoringState {
                candidate_family: active.candidate_family,
                control: active.current.control.clone(),
                access: active.access.clone(),
                roster: active.roster.clone(),
                metadata: active.metadata.clone(),
            }),
            Self::Inactive(_) | Self::ControlConflict { .. } => None,
        }
    }

    fn resolved_control(&self) -> Option<&CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&active.current),
            Self::Inactive(inactive) => Some(&inactive.current),
            Self::ControlConflict { .. } => None,
        }
    }
}

fn advance_resolved_control(
    current: CircleCurrentControl,
    next: CircleCurrentState,
) -> Result<CircleCurrentState, String> {
    let next_current = next
        .resolved_control()
        .ok_or_else(|| "new Circle activation is already conflicted".to_string())?;
    if next_current.causally_covers(&current) {
        Ok(next)
    } else {
        let mut branches = vec![current, next_current.clone()];
        canonicalize_control_branches(&mut branches)?;
        Ok(CircleCurrentState::ControlConflict { branches })
    }
}

fn canonicalize_control_branches(branches: &mut [CircleCurrentControl]) -> Result<(), String> {
    branches.sort_by_key(CircleCurrentControl::control_hash);
    if branches
        .windows(2)
        .any(|pair| pair[0].control_hash() == pair[1].control_hash())
    {
        return Err("Circle control conflict contains a duplicate branch".to_string());
    }
    Ok(())
}

fn roster_matches_control(roster: &CircleMaterializedRoster, control: &CircleControl) -> bool {
    control.roster_state_ref().state_hash == roster.state_hash()
}

fn metadata_matches_control(metadata: &CircleMetadata, control: &CircleControl) -> bool {
    let state = control.metadata_state_ref();
    state.selected == metadata.coord() && state.state_hash == metadata.metadata_hash()
}

struct VerifiedAccessPair {
    reference: CircleAccessObjectRef,
    envelope: AccessEnvelope,
    leaf_bytes: Vec<u8>,
}

async fn load_verified_access_pairs(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
) -> Result<Vec<VerifiedAccessPair>, CircleOperationError> {
    let family = commit.candidate_family();
    let mut verified = Vec::with_capacity(objects.access.len());
    for reference in &objects.access {
        if reference.leaf.owner_pubkey != reference.envelope.owner_pubkey
            || reference.leaf.recipient_slot != reference.envelope.recipient_slot
            || reference.leaf.leaf_id != reference.envelope.leaf_id
            || reference.leaf.leaf_hash != reference.envelope.leaf_hash
            || reference.leaf.leaf_hash != reference.leaf.object.stored_hash()
            || reference.envelope.control_hash != control.coord.control_hash()
        {
            return Err(CircleOperationError::InvalidState(
                "paired Circle access references differ".to_string(),
            ));
        }
        let envelope_prefix = circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &reference.envelope.owner_pubkey,
            &reference.envelope.recipient_slot,
            reference.envelope.control_hash,
        );
        let envelope_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &reference.envelope.object,
            &envelope_prefix,
        )
        .await?;
        let envelope: AccessEnvelope =
            serde_json::from_slice(&envelope_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access envelope: {error}"))
            })?;
        if envelope.candidate_family != family
            || envelope.circle_id != circle_id
            || envelope.owner_pubkey != reference.envelope.owner_pubkey
            || envelope.recipient_slot != reference.envelope.recipient_slot
            || envelope.control_hash != reference.envelope.control_hash
            || envelope.leaf_id != reference.envelope.leaf_id
            || envelope.leaf_hash != reference.envelope.leaf_hash
            || !envelope.verify(control, family)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access envelope failed verification".to_string(),
            ));
        }
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            circle_id,
            family,
            &reference.leaf.owner_pubkey,
            reference.leaf.epoch_id,
            &reference.leaf.recipient_slot,
            reference.leaf.leaf_id,
        );
        let leaf_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::recipient_sealed(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &reference.leaf.object,
            &leaf_prefix,
        )
        .await?;
        if ObjectHash::digest(&leaf_bytes) != reference.leaf.leaf_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle access leaf bytes differ from the paired leaf hash".to_string(),
            ));
        }
        verified.push(VerifiedAccessPair {
            reference: reference.clone(),
            envelope,
            leaf_bytes,
        });
    }
    Ok(verified)
}

fn verify_circle_owner_authority(
    author_pubkey: &str,
    control: &CircleControl,
    roster: &CircleMaterializedRoster,
) -> bool {
    verify_merge_circle_owner_authority(author_pubkey, &control.value.author_authority, roster)
}

fn verify_merge_circle_owner_authority(
    author_pubkey: &str,
    authority: &MergeCircleOwnerAuthorityRef,
    roster: &ResolvedCircleRoster,
) -> bool {
    match authority {
        MergeCircleOwnerAuthorityRef::Roster {
            grant_id,
            created_at,
            ..
        } => roster.authorizes_owner_grant(author_pubkey, grant_id, created_at),
        MergeCircleOwnerAuthorityRef::ConflictResolution {
            conflict_hash,
            resolution_hash,
        } => {
            let grant_id =
                super::circle_roster::derive_circle_resolution_grant(conflict_hash, author_pubkey);
            roster.authorizes_resolution_grant(
                author_pubkey,
                &grant_id,
                &super::circle_roster::CircleRosterConflictResolutionRef {
                    conflict_hash: *conflict_hash,
                    resolver_pubkey: author_pubkey.to_string(),
                    resolution_hash: *resolution_hash,
                },
            )
        }
    }
}

struct CircleStreamAuthority {
    activation_id: StreamActivationId,
    first_slot: crate::storage::cloud::ObjectSlot,
    registration: StoreDeviceRegistration,
    activated_here: bool,
}

#[derive(Clone, Copy)]
enum CircleHeadKind {
    Control,
    Roster,
    Metadata,
}

enum CircleHeadValue {
    Control(super::circle::CircleControlHead),
    Roster(super::circle::CircleRosterHead),
    Metadata(super::circle::CircleMetadataHead),
}

struct CircleHeadPosition<'a> {
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    author_pubkey: &'a str,
    device_id: &'a str,
    stream_id: super::causal_grants::AuthorStreamId,
    author_owner_grant: &'a super::membership::MembershipGrantId,
    seq: u64,
    successor: &'a super::store_commit::SuccessorLink,
}

impl CircleHeadValue {
    fn parse(kind: CircleHeadKind, bytes: &[u8]) -> Result<Self, CircleOperationError> {
        match kind {
            CircleHeadKind::Control => {
                serde_json::from_slice(bytes)
                    .map(Self::Control)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle control head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Roster => {
                serde_json::from_slice(bytes)
                    .map(Self::Roster)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle roster head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Metadata => {
                serde_json::from_slice(bytes)
                    .map(Self::Metadata)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle metadata head: {error}"
                        ))
                    })
            }
        }
    }

    fn position(&self) -> Result<CircleHeadPosition<'_>, CircleOperationError> {
        match self {
            Self::Control(head) => {
                let CircleControlCoord {
                    device_id,
                    stream_id,
                    author_pubkey,
                    author_owner_grant,
                    seq,
                    ..
                } = &head.control;
                Ok(CircleHeadPosition {
                    store_root_hash: head.store_root_hash,
                    circle_id: head.circle_id,
                    author_pubkey,
                    device_id,
                    stream_id: *stream_id,
                    author_owner_grant,
                    seq: *seq,
                    successor: &head.successor,
                })
            }
            Self::Roster(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
            Self::Metadata(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
        }
    }

    fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        match self {
            Self::Control(head) => head.verify(registration),
            Self::Roster(head) => head.verify_for_registration(registration),
            Self::Metadata(head) => head.verify_for_registration(registration),
        }
    }

    fn semantic_prefix(&self, object: ExactObjectRef) -> String {
        match self {
            Self::Control(head) => circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: head.circle_id,
                control: &head.control,
                head_hash: head.head_hash(),
            }),
            Self::Roster(head) => {
                let reference = CircleRosterHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
            Self::Metadata(head) => {
                let reference = CircleMetadataHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
        }
    }
}

async fn verify_circle_head_chain(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    kind: CircleHeadKind,
    current: CircleHeadValue,
    current_object: ExactObjectRef,
    authority: &CircleStreamAuthority,
) -> Result<(), CircleOperationError> {
    let mut current = current;
    let mut current_object = current_object;
    loop {
        let position = current.position()?;
        if !current.verify_for_registration(&authority.registration)
            || position.store_root_hash != authority.registration.store_root.store_root_hash
            || position.author_pubkey != authority.registration.author_pubkey
            || position.device_id != authority.registration.device_id.to_string()
            || position.successor.activation != authority.activation_id
        {
            return Err(CircleOperationError::InvalidState(
                "Circle head differs from its activated registration".to_string(),
            ));
        }
        if position.seq == 1 {
            if position.successor.predecessor.is_some()
                || current_object.slot() != &authority.first_slot
            {
                return Err(CircleOperationError::InvalidState(
                    "first Circle head differs from its activated slot".to_string(),
                ));
            }
            return Ok(());
        }
        let predecessor_object = position.successor.predecessor.clone().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "successor Circle head omits its exact predecessor".to_string(),
            )
        })?;
        let predecessor_prefix = predecessor_object
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle predecessor head has a non-canonical logical key".to_string(),
                )
            })?;
        let predecessor_bytes =
            read_exact_circle_object(storage, context, &predecessor_object, predecessor_prefix)
                .await?;
        let predecessor = CircleHeadValue::parse(kind, &predecessor_bytes)?;
        let predecessor_position = predecessor.position()?;
        if predecessor.semantic_prefix(predecessor_object.clone()) != predecessor_prefix
            || predecessor_position.store_root_hash != position.store_root_hash
            || predecessor_position.circle_id != position.circle_id
            || predecessor_position.author_pubkey != position.author_pubkey
            || predecessor_position.device_id != position.device_id
            || predecessor_position.stream_id != position.stream_id
            || predecessor_position.author_owner_grant != position.author_owner_grant
            || predecessor_position.seq.checked_add(1) != Some(position.seq)
            || predecessor_position.successor.next_slot != *current_object.slot()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle head does not occupy its predecessor-reserved successor slot".to_string(),
            ));
        }
        current = predecessor;
        current_object = predecessor_object;
    }
}

async fn verify_covered_control_heads(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    control: &CircleControl,
) -> Result<(), CircleOperationError> {
    let active_epoch = &control.value.active_epoch;
    let context = ProtocolObjectContext::store_encrypted(
        commit.store_root_hash,
        ProtocolObjectDomain::CircleControl,
    );
    for reference in &active_epoch.covered_control_heads {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id: control.circle_id,
            control: &reference.coord,
            head_hash: reference.head_hash,
        });
        let bytes = read_exact_circle_object(storage, &context, &reference.object, &prefix).await?;
        let head: super::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse covered Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_owner_grant,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            head.successor.activation,
            *stream_id,
            control.circle_id,
            author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
        )
        .await?;
        if authority.activated_here
            || head.control != reference.coord
            || head.head_hash() != reference.head_hash
        {
            return Err(CircleOperationError::InvalidState(
                "covered Circle control head differs from its exact reference".to_string(),
            ));
        }
        verify_circle_head_chain(
            storage,
            &context,
            CircleHeadKind::Control,
            CircleHeadValue::Control(head),
            reference.object.clone(),
            &authority,
        )
        .await?;
    }
    Ok(())
}

async fn resolve_circle_stream_authority(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    claimed_activation_id: StreamActivationId,
    stream_id: super::causal_grants::AuthorStreamId,
    circle_id: CircleId,
    grant_id: &super::membership::MembershipGrantId,
    expected_anchor: fn(CircleId, crate::storage::cloud::ObjectSlot) -> GrantStreamAnchor,
) -> Result<CircleStreamAuthority, CircleOperationError> {
    let current = commit
        .stream_activations()
        .iter()
        .find(|activation| activation.activation_id() == claimed_activation_id)
        .cloned();
    let (activation, activating_commit, activated_here) = if let Some(activation) = current {
        (activation, commit_ref.clone(), true)
    } else if let Some((activation, activating_commit)) =
        verified_prefix.activation(claimed_activation_id)
    {
        (activation.clone(), activating_commit.clone(), false)
    } else {
        let registered = db
            .registered_stream_activation(claimed_activation_id)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle author stream {stream_id} has no verified activation"
                ))
            })?;
        (
            registered.activation().clone(),
            registered.activating_commit().clone(),
            false,
        )
    };
    let StreamActivation::GrantAuthorized {
        store_root_hash,
        author_registration,
        grant_id: activation_grant,
        anchor,
    } = &activation
    else {
        return Err(CircleOperationError::InvalidState(
            "Circle author stream uses device authority".to_string(),
        ));
    };
    let expected = expected_anchor(circle_id, anchor.first_slot().clone());
    if *store_root_hash != root.store_root_hash
        || activation.author_stream_id() != stream_id
        || activation_grant != grant_id
        || anchor != &expected
    {
        return Err(CircleOperationError::InvalidState(
            "Circle author stream differs from its activation descriptor".to_string(),
        ));
    }
    if activated_here {
        if activating_commit != *commit_ref {
            return Err(CircleOperationError::InvalidState(
                "same-commit Circle activation names another Store commit".to_string(),
            ));
        }
    } else {
        let reached = super::store::pull::predecessor_commit_matching(
            storage,
            root,
            &commit.order,
            Box::new(|reference, predecessor| {
                reference == &activating_commit
                    && predecessor
                        .stream_activations()
                        .binary_search(&activation)
                        .is_ok()
            }),
        )
        .await
        .map_err(|error| match error {
            super::store::pull::RegistrationLoadError::Object(error) => {
                CircleOperationError::Object(error)
            }
            super::store::pull::RegistrationLoadError::Invalid(error) => {
                CircleOperationError::InvalidState(error)
            }
        })?
        .is_some();
        if !reached {
            return Err(CircleOperationError::InvalidState(
                "Circle author stream activation is outside the commit predecessor history"
                    .to_string(),
            ));
        }
    }
    let registration =
        super::store_objects::load_registration_ref(storage, root, author_registration)
            .await?
            .value;
    Ok(CircleStreamAuthority {
        activation_id: activation.activation_id(),
        first_slot: anchor.first_slot().clone(),
        registration,
        activated_here,
    })
}

pub(crate) async fn load_circle_activations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: &UserKeypair,
    founder_pubkey: &str,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    Box::pin(load_circle_activations_with_prefix(
        db,
        storage,
        root,
        commit_ref,
        commit,
        author,
        identity,
        founder_pubkey,
        &verified_prefix,
    ))
    .await
}

pub(crate) async fn load_circle_activations_with_prefix(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: &UserKeypair,
    founder_pubkey: &str,
    verified_prefix: &VerifiedStreamActivationPrefix,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if root.store_root_hash != commit.store_root_hash
        || commit
            .author_registration
            .verify_registration(author)
            .is_err()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle activation authority differs from its exact Store commit".to_string(),
        ));
    }
    let mut activations = Vec::with_capacity(commit.circle_controls().len());
    let mut consumed_stream_activations = BTreeSet::new();
    for reference in commit.circle_controls() {
        let objects = reference.objects();
        let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: reference.circle_id(),
            control: reference.control(),
        });
        let control_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &objects.control,
            &control_prefix,
        )
        .await?;
        if ObjectHash::digest(&control_bytes) != reference.control().control_hash() {
            return Err(CircleOperationError::InvalidState(
                "Circle control bytes differ from the signed control hash".to_string(),
            ));
        }
        let control_value: CircleControl =
            serde_json::from_slice(&control_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse Circle control: {error}"))
            })?;
        let declared_coord = control_value.coord();
        if !control_value.verify()
            || verify_circle_semantic_prefix(
                &control_prefix,
                CircleSemanticSlot::Control {
                    circle_id: control_value.circle_id,
                    control: &declared_coord,
                },
            )
            .is_err()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control failed exact verification".to_string(),
            ));
        }
        let control = PreparedCircleControl {
            coord: reference.control().clone(),
            bytes: control_bytes,
            value: control_value,
        };
        let circle_id = reference.circle_id;
        let control_coord = &reference.control;
        let head_hash = reference.head_hash;
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id,
            control: control_coord,
            head_hash,
        });
        let head_object = reference.head_object();
        let bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            head_object,
            &prefix,
        )
        .await?;
        let head: super::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse exact Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_pubkey,
            author_owner_grant,
            seq,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            head.successor.activation,
            *stream_id,
            circle_id,
            author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
        )
        .await?;
        verify_circle_head_chain(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            CircleHeadKind::Control,
            CircleHeadValue::Control(head.clone()),
            head_object.clone(),
            &authority,
        )
        .await?;
        if !head.verify(author)
            || !head.verify(&authority.registration)
            || authority.registration.author_pubkey != *author_pubkey
            || (authority.activated_here && *seq != 1)
            || head.successor.activation != authority.activation_id
            || (*seq == 1
                && (head.successor.predecessor.is_some()
                    || head_object.slot() != &authority.first_slot))
            || head.head_hash() != head_hash
            || head.entry != objects.control
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::ControlHead {
                    circle_id: head.circle_id,
                    control: &head.control,
                    head_hash: head.head_hash(),
                },
            )
            .is_err()
            || head.store_root_hash != commit.store_root_hash
            || head.circle_id != circle_id
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control head failed exact verification".to_string(),
            ));
        }
        if authority.activated_here {
            consumed_stream_activations.insert(authority.activation_id);
        }
        verify_covered_control_heads(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            &control.value,
        )
        .await?;
        verify_control_context(reference, &control, commit_ref, commit, author)?;
        consume_public_private_stream_activations(
            commit,
            author,
            reference.circle_id(),
            &control,
            objects,
            &mut consumed_stream_activations,
        )?;
        let verified_access =
            load_verified_access_pairs(storage, commit, reference.circle_id(), &control, objects)
                .await?;
        let checkpoint_members = Box::pin(verify_control_membership(
            storage,
            root,
            &control,
            founder_pubkey,
        ))
        .await?;
        let own_pubkey = keys::public_key_hex(identity);
        if !checkpoint_members
            .iter()
            .any(|(pubkey, _)| pubkey == &own_pubkey)
        {
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        }
        let owner_pubkey = &control.value.author_pubkey;
        let owner = (
            owner_pubkey.clone(),
            recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id()).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!(
                        "derive circle Owner recipient slot: {error}"
                    ))
                },
            )?,
        );
        let access = verified_access
            .iter()
            .find(|candidate| {
                candidate.reference.envelope.owner_pubkey == owner.0
                    && candidate.reference.envelope.recipient_slot == owner.1
                    && candidate.reference.envelope.control_hash
                        == reference.control().control_hash()
            })
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle activation lacks the recipient's exact access envelope".to_string(),
                )
            })?;
        let envelope = &access.envelope;
        let leaf_bytes = access.leaf_bytes.clone();
        let plaintext = keys::seal_box_decrypt(&leaf_bytes, &identity.to_x25519_secret_key())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
            })?;
        let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
        })?;
        let prepared_leaf = PreparedAccessLeaf {
            bytes: leaf_bytes,
            value: leaf,
            leaf_hash: envelope.leaf_hash,
        };
        let leaf = &prepared_leaf.value;
        if leaf.candidate_family != commit.candidate_family()
            || leaf.owner_pubkey != owner.0
            || leaf.recipient_pubkey != own_pubkey
            || leaf.recipient_slot != owner.1
            || leaf.store_membership != control.value.store_membership_state_ref()
            || leaf.epoch_id != access.reference.leaf.epoch_id
            || leaf.leaf_id != access.reference.leaf.leaf_id
            || !prepared_leaf.verify_envelope(&control, envelope, commit.candidate_family())
        {
            return Err(CircleOperationError::InvalidState(
                "circle access leaf failed context verification".to_string(),
            ));
        }
        let active = match &leaf.disposition {
            CircleAccessDisposition::Active { keyring, .. } => {
                let encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse circle access keyring: {error}"
                        ))
                    })?,
                );
                let authority_roster = load_circle_authority_roster(
                    db,
                    verified_prefix,
                    storage,
                    commit,
                    reference.circle_id(),
                    &control,
                    encryption.clone(),
                    objects,
                    root,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                if !verify_circle_owner_authority(
                    &control.value.author_pubkey,
                    &control.value,
                    &authority_roster,
                ) {
                    return Err(CircleOperationError::InvalidState(
                        "circle control author lacks its exact historical Owner grant".to_string(),
                    ));
                }
                let resolved = load_circle_roster_state(
                    db,
                    verified_prefix,
                    storage,
                    root,
                    commit_ref,
                    commit,
                    reference.circle_id(),
                    &control.value.value.active_epoch.roster,
                    encryption.clone(),
                    objects,
                    &mut consumed_stream_activations,
                )
                .await?;
                let resolved_members = resolved.members();
                if !resolved_members.contains_key(&leaf.recipient_pubkey) {
                    return Err(CircleOperationError::InvalidState(
                        "circle Active access recipient is absent from its resolved roster"
                            .to_string(),
                    ));
                }
                let roster_owners = resolved_members
                    .iter()
                    .filter_map(|(pubkey, role)| {
                        (*role == super::circle::CircleRole::Owner).then_some(pubkey.clone())
                    })
                    .collect::<Vec<_>>();
                if roster_owners != control.value.owners() {
                    return Err(CircleOperationError::InvalidState(
                        "circle control Owners differ from its roster".to_string(),
                    ));
                }
                let metadata_state = control.value.metadata_state_ref();
                let metadata = load_circle_metadata_state(
                    db,
                    verified_prefix,
                    storage,
                    commit,
                    reference.circle_id(),
                    &metadata_state,
                    encryption,
                    objects,
                    root,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                Some(VerifiedCircleActive {
                    roster: resolved,
                    metadata,
                })
            }
            CircleAccessDisposition::Inactive => None,
        };
        activations.push(VerifiedCircleReference {
            reference: reference.clone(),
            circle_id: reference.circle_id(),
            control,
            local_access: Some(VerifiedCircleAccess {
                envelope: envelope.clone(),
                leaf: prepared_leaf,
                active,
            }),
        });
    }
    let declared = commit
        .stream_activations()
        .iter()
        .map(StreamActivation::activation_id)
        .collect::<BTreeSet<_>>();
    if consumed_stream_activations != declared {
        return Err(CircleOperationError::InvalidState(
            "Store commit stream activations do not exactly introduce its first Circle heads"
                .to_string(),
        ));
    }
    let stream_activations =
        VerifiedStreamActivations::from_verified_circle_commit(commit, commit_ref)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    Ok(VerifiedCircleActivations {
        circles: activations,
        stream_activations,
    })
}

fn consume_public_private_stream_activations(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    consumed: &mut BTreeSet<StreamActivationId>,
) -> Result<(), CircleOperationError> {
    let roster = control.value.roster_state_ref();
    let metadata = control.value.metadata_state_ref();
    for activation in commit.stream_activations() {
        let StreamActivation::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        } = activation
        else {
            continue;
        };
        let valid = match anchor {
            GrantStreamAnchor::CircleRoster {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => roster.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.roster_heads.contains(head)
            }),
            GrantStreamAnchor::CircleMetadata {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => metadata.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.metadata_heads.contains(head)
            }),
            _ => continue,
        };
        if *store_root_hash != commit.store_root_hash
            || author_registration != &commit.author_registration
            || grant_id != &control.value.author_grant_id()
            || !valid
        {
            return Err(CircleOperationError::InvalidState(
                "private Circle stream activation differs from its signed public first-head reference"
                    .to_string(),
            ));
        }
        consumed.insert(activation.activation_id());
    }
    Ok(())
}

async fn load_circle_roster_state(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    state: &super::circle::MergeCircleRosterStateRef,
    encryption: EncryptionService,
    objects: &CircleActivationObjects,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<ResolvedCircleRoster, CircleOperationError> {
    let store_root_hash = commit.store_root_hash;
    if state.heads.is_empty()
        || !state
            .heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
    {
        return Err(CircleOperationError::InvalidState(
            "Circle roster heads are not one canonical head per stream".to_string(),
        ));
    }
    let context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRoster,
        encryption.clone(),
    );
    if !state.resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(CircleOperationError::InvalidState(
            "Circle roster resolutions are not canonical".to_string(),
        ));
    }
    let loaded_heads = load_exact_circle_roster_heads(
        db,
        verified_prefix,
        storage,
        root,
        commit_ref,
        commit,
        circle_id,
        &context,
        &state.heads,
        objects,
        consumed_stream_activations,
    )
    .await?;
    let activated_resolutions = loaded_heads
        .iter()
        .flat_map(|head| head.head().resolutions.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if activated_resolutions != state.resolutions {
        return Err(CircleOperationError::InvalidState(
            "Circle roster state resolution refs differ from its signed heads".to_string(),
        ));
    }
    let entries = load_circle_roster_entries_from_heads(
        storage,
        store_root_hash,
        circle_id,
        &context,
        &loaded_heads,
        objects,
    )
    .await?;
    let loaded_resolutions = load_circle_roster_resolutions(
        storage,
        store_root_hash,
        circle_id,
        &state.resolutions,
        &encryption,
        objects,
    )
    .await?;
    let chain = if loaded_resolutions.is_empty() {
        super::circle::CircleRosterChain::from_entries_with_heads(entries.clone(), loaded_heads)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
    } else {
        replay_circle_roster_resolutions(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            circle_id,
            &context,
            &entries,
            &loaded_heads,
            &loaded_resolutions,
            objects,
            consumed_stream_activations,
        )
        .await?
    };
    let expected_heads = state
        .heads
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<Vec<_>>();
    if chain.author_heads() != expected_heads {
        return Err(CircleOperationError::InvalidState(
            "Circle roster signed heads do not name its raw frontier".to_string(),
        ));
    }
    let resolved = chain
        .try_resolved()
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if resolved.state_hash != state.state_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle roster state hash differs from its effective assignments".to_string(),
        ));
    }
    Ok(resolved)
}

async fn load_circle_roster_entries_from_heads(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    heads: &[super::circle::ExactCircleRosterHead],
    objects: &CircleActivationObjects,
) -> Result<Vec<super::circle::CircleRosterEntry>, CircleOperationError> {
    let mut pending = heads
        .iter()
        .map(|head| head.head().entry_coord())
        .collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();
    while let Some(coord) = pending.pop_first() {
        if entries.contains_key(&coord) {
            continue;
        }
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
            circle_id,
            coord: &coord,
        });
        let object = objects.roster_entries.get(&coord).ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle activation omits exact roster entry {}",
                coord.entry_hash
            ))
        })?;
        let bytes = read_exact_circle_object(storage, context, object, &prefix).await?;
        if ObjectHash::digest(&bytes) != coord.entry_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle roster entry bytes differ from the signed coordinate".to_string(),
            ));
        }
        let entry: super::circle::CircleRosterEntry =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse Circle roster entry: {error}"))
            })?;
        let declared_coord = entry.coord();
        if !entry.verify()
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::RosterEntry {
                    circle_id: entry.circle_id,
                    coord: &declared_coord,
                },
            )
            .is_err()
            || entry.store_root_hash != store_root_hash
            || entry.circle_id != circle_id
            || declared_coord != coord
        {
            return Err(CircleOperationError::InvalidState(
                "Circle roster entry failed exact verification".to_string(),
            ));
        }
        pending.extend(entry.dependencies.iter().cloned());
        entries.insert(coord, entry);
    }
    Ok(entries.into_values().collect())
}

async fn load_circle_roster_resolutions(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    resolutions: &[super::circle::CircleRosterConflictResolutionRef],
    encryption: &EncryptionService,
    objects: &CircleActivationObjects,
) -> Result<Vec<super::circle::CircleRosterConflictResolution>, CircleOperationError> {
    let mut loaded_resolutions = Vec::with_capacity(resolutions.len());
    for reference in resolutions {
        let object = objects.roster_resolutions.get(reference).ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle activation omits exact roster resolution {}",
                reference.resolution_hash
            ))
        })?;
        let context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            encryption.clone(),
        );
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
            circle_id,
            resolution: reference,
        });
        let bytes = read_exact_circle_object(storage, &context, object, &prefix).await?;
        if ObjectHash::digest(&bytes) != reference.resolution_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolution bytes differ from the signed reference".to_string(),
            ));
        }
        let resolution = serde_json::from_slice(&bytes).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse Circle roster resolution: {error}"))
        })?;
        loaded_resolutions.push(resolution);
    }
    Ok(loaded_resolutions)
}

async fn load_exact_circle_roster_heads(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    references: &[CircleRosterHeadRef],
    objects: &CircleActivationObjects,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<Vec<super::circle::ExactCircleRosterHead>, CircleOperationError> {
    let store_root_hash = commit.store_root_hash;
    let mut loaded_heads = Vec::with_capacity(references.len());
    for reference in references {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
            circle_id,
            head: reference,
        });
        let object = objects
            .roster_heads
            .iter()
            .find(|stored| *stored == reference)
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact roster head {}",
                    reference.head_hash
                ))
            })?;
        let bytes = read_exact_circle_object(storage, context, &object.object, &prefix).await?;
        let head: super::circle::CircleRosterHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse Circle roster head: {error}"))
            })?;
        let declared_ref = CircleRosterHeadRef::from_stored_head(&head, object.object.clone());
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            head.successor.activation,
            head.stream_id,
            circle_id,
            &head.author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
        )
        .await?;
        verify_circle_head_chain(
            storage,
            context,
            CircleHeadKind::Roster,
            CircleHeadValue::Roster(head.clone()),
            object.object.clone(),
            &authority,
        )
        .await?;
        if !head.verify_for_registration(&authority.registration)
            || authority.registration.author_pubkey != head.author_pubkey
            || (authority.activated_here && head.seq != 1)
            || head.successor.activation != authority.activation_id
            || (head.seq == 1
                && (head.successor.predecessor.is_some()
                    || object.object.slot() != &authority.first_slot))
            || head.head_hash() != reference.head_hash
            || head.tip
                != *objects
                    .roster_entries
                    .get(&reference.coord)
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle activation omits roster head tip {}",
                            reference.coord.entry_hash
                        ))
                    })?
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::RosterHead {
                    circle_id: head.circle_id,
                    head: &declared_ref,
                },
            )
            .is_err()
            || head.store_root_hash != store_root_hash
            || head.circle_id != circle_id
            || &declared_ref != reference
        {
            return Err(CircleOperationError::InvalidState(
                "Circle roster head failed exact verification".to_string(),
            ));
        }
        if authority.activated_here {
            consumed_stream_activations.insert(authority.activation_id);
        }
        loaded_heads.push(
            super::circle::ExactCircleRosterHead::bind(head, reference.clone())
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
        );
    }
    Ok(loaded_heads)
}

async fn replay_circle_roster_resolutions(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    entries: &[super::circle::CircleRosterEntry],
    current_heads: &[super::circle::ExactCircleRosterHead],
    resolutions: &[super::circle::CircleRosterConflictResolution],
    objects: &CircleActivationObjects,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<super::circle::CircleRosterChain, CircleOperationError> {
    let store_root_hash = commit.store_root_hash;
    let known_resolution_refs = resolutions
        .iter()
        .map(|resolution| resolution.resolution_ref())
        .collect::<BTreeSet<_>>();
    let current_head_refs =
        super::circle::CircleRosterChain::validate_exact_heads(entries, current_heads)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let current_by_coord = entries
        .iter()
        .cloned()
        .map(|entry| (entry.coord(), entry))
        .collect::<BTreeMap<_, _>>();
    let activated_resolution_refs = current_heads
        .iter()
        .flat_map(|head| head.head().resolutions.iter().cloned())
        .collect::<BTreeSet<_>>();
    if let Some(reference) = activated_resolution_refs
        .difference(&known_resolution_refs)
        .next()
    {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle head references absent resolution {}",
            reference.resolution_hash
        )));
    }
    if known_resolution_refs != activated_resolution_refs {
        return Err(CircleOperationError::InvalidState(
            "Circle resolution objects differ from the exact signed head cut".to_string(),
        ));
    }
    let mut prepared = BTreeMap::new();
    for resolution in resolutions {
        let reference = resolution.resolution_ref();
        let conflict_heads = &resolution.conflicting_heads;
        if conflict_heads.is_empty()
            || !conflict_heads
                .windows(2)
                .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolution conflict heads are not canonical".to_string(),
            ));
        }
        let heads = load_exact_circle_roster_heads(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            circle_id,
            context,
            conflict_heads,
            objects,
            consumed_stream_activations,
        )
        .await?;
        let conflict_entries = load_circle_roster_entries_from_heads(
            storage,
            store_root_hash,
            circle_id,
            context,
            &heads,
            objects,
        )
        .await?;
        super::circle::CircleRosterChain::validate_exact_heads(&conflict_entries, &heads)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let dependencies = heads
            .iter()
            .flat_map(|head| head.head().resolutions.iter())
            .map(|reference| {
                known_resolution_refs
                    .contains(reference)
                    .then_some(reference.clone())
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle conflict head references absent resolution {}",
                            reference.resolution_hash
                        ))
                    })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if dependencies.contains(&reference) {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolution depends on itself".to_string(),
            ));
        }
        prepared.insert(
            reference,
            (resolution.clone(), heads, conflict_entries, dependencies),
        );
    }
    let mut resolved_by_ref = BTreeMap::<
        super::circle::CircleRosterConflictResolutionRef,
        super::circle::CircleRosterChain,
    >::new();
    let mut applied = BTreeSet::new();
    while !prepared.is_empty() {
        let next = super::causal_grants::canonical_ready_checkpoint(
            prepared
                .iter()
                .map(|(reference, (_, _, _, dependencies))| (reference, dependencies)),
            &applied,
        )
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle roster resolution checkpoints contain a causal cycle".to_string(),
            )
        })?;
        let (resolution, conflict_heads, conflict_entries, dependencies) =
            prepared.remove(&next).ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "ready Circle roster resolution checkpoint is absent".to_string(),
                )
            })?;
        let dependency_chains = dependencies
            .iter()
            .map(|dependency| {
                resolved_by_ref.get(dependency).ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "ready Circle resolution dependency is absent".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut prefix = BTreeMap::new();
        for chain in &dependency_chains {
            prefix.extend(
                chain
                    .entries()
                    .iter()
                    .cloned()
                    .map(|entry| (entry.coord(), entry)),
            );
        }
        prefix.extend(
            conflict_entries
                .into_iter()
                .map(|entry| (entry.coord(), entry)),
        );
        let mut conflict_chain = if dependency_chains.is_empty() {
            super::circle::CircleRosterChain::from_entries_with_heads(
                prefix.into_values().collect(),
                conflict_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        } else {
            let conflict_head_refs = conflict_heads
                .iter()
                .map(|head| head.reference().clone())
                .collect();
            super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
                &dependency_chains,
                prefix.into_values().collect(),
                conflict_head_refs,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        };
        conflict_chain
            .apply_resolutions(std::slice::from_ref(&resolution))
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        resolved_by_ref.insert(next.clone(), conflict_chain);
        applied.insert(next);
    }

    let mut heads_by_cut = BTreeMap::<Vec<_>, Vec<_>>::new();
    for head in &current_head_refs {
        let entry = current_by_coord.get(&head.coord).ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle roster head tip {} is absent",
                head.coord.entry_hash
            ))
        })?;
        heads_by_cut
            .entry(entry.resolution_dependencies.clone())
            .or_default()
            .push(head.clone());
    }
    let mut branch_chains = Vec::new();
    for (cut, heads) in heads_by_cut {
        let cut_set = cut.iter().cloned().collect::<BTreeSet<_>>();
        let branch_heads = heads
            .into_iter()
            .filter(|head| {
                let coord = head.coord.clone();
                !resolved_by_ref.values().any(|checkpoint| {
                    let checkpoint_cut = checkpoint
                        .resolution_refs()
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    cut_set.is_subset(&checkpoint_cut)
                        && cut_set != checkpoint_cut
                        && checkpoint.resolution_checkpoint_covers(&coord)
                })
            })
            .collect::<Vec<_>>();
        if branch_heads.is_empty() {
            continue;
        }
        let dependencies = cut
            .iter()
            .map(|reference| {
                resolved_by_ref.get(reference).ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle head references absent resolution {}",
                        reference.resolution_hash
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut branch_history = BTreeMap::new();
        for chain in &dependencies {
            branch_history.extend(
                chain
                    .entries()
                    .iter()
                    .cloned()
                    .map(|entry| (entry.coord(), entry)),
            );
        }
        let mut pending = branch_heads
            .iter()
            .map(|head| head.coord.clone())
            .collect::<BTreeSet<_>>();
        while let Some(coord) = pending.pop_first() {
            if branch_history.contains_key(&coord) {
                continue;
            }
            let entry = current_by_coord.get(&coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle suffix entry {} is absent",
                    coord.entry_hash
                ))
            })?;
            pending.extend(entry.dependencies.iter().cloned());
            branch_history.insert(coord, entry.clone());
        }
        let branch_exact_heads = current_heads
            .iter()
            .filter(|head| branch_heads.contains(head.reference()))
            .cloned()
            .collect::<Vec<_>>();
        let mut branch = if dependencies.is_empty() {
            super::circle::CircleRosterChain::from_entries_with_heads(
                branch_history.into_values().collect(),
                branch_exact_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        } else {
            super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
                &dependencies,
                branch_history.into_values().collect(),
                branch_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        };
        branch
            .checkpoint_current_resolved_state()
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        branch_chains.push(branch);
    }
    let branch_refs = resolved_by_ref
        .values()
        .chain(branch_chains.iter())
        .collect::<Vec<_>>();
    let mut history = current_by_coord;
    for chain in &branch_refs {
        history.extend(
            chain
                .entries()
                .iter()
                .cloned()
                .map(|entry| (entry.coord(), entry)),
        );
    }
    super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
        &branch_refs,
        history.into_values().collect(),
        current_head_refs,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
}

async fn load_circle_authority_roster(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    encryption: EncryptionService,
    objects: &CircleActivationObjects,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<CircleMaterializedRoster, CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let roster = match &control.value.value.author_authority {
        MergeCircleOwnerAuthorityRef::Roster { roster, .. } => roster,
        MergeCircleOwnerAuthorityRef::ConflictResolution { .. } => {
            &control.value.value.active_epoch.roster
        }
    };
    load_circle_roster_state(
        db,
        verified_prefix,
        storage,
        root,
        commit_ref,
        commit,
        circle_id,
        roster,
        encryption,
        objects,
        consumed_stream_activations,
    )
    .await
}

async fn load_metadata_author_roster(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    roster_ref: &super::circle::CircleRosterStateRef,
    encryption: EncryptionService,
    objects: &CircleActivationObjects,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<CircleMaterializedRoster, CircleOperationError> {
    load_circle_roster_state(
        db,
        verified_prefix,
        storage,
        root,
        commit_ref,
        commit,
        circle_id,
        roster_ref,
        encryption,
        objects,
        consumed_stream_activations,
    )
    .await
}

async fn load_circle_metadata_state(
    db: &Database,
    verified_prefix: &VerifiedStreamActivationPrefix,
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    state: &super::circle::CircleMetadataStateRef,
    encryption: EncryptionService,
    objects: &CircleActivationObjects,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
) -> Result<CircleMetadata, CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if root.store_root_hash != commit.store_root_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle metadata authority differs from its Store root".to_string(),
        ));
    }
    let store_root_hash = commit.store_root_hash;
    if state.heads.is_empty()
        || !state
            .heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
    {
        return Err(CircleOperationError::InvalidState(
            "Circle metadata heads are not one canonical head per stream".to_string(),
        ));
    }
    let mut pending = BTreeSet::new();
    for reference in &state.heads {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
            circle_id,
            head: reference,
        });
        let object = objects
            .metadata_heads
            .iter()
            .find(|stored| *stored == reference)
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact metadata head {}",
                    reference.head_hash
                ))
            })?;
        let tip_object = objects
            .metadata_entries
            .get(&reference.coord)
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits metadata head tip {}",
                    reference.coord.metadata_hash
                ))
            })?;
        let head_encryption = encryption
            .service_for_fingerprint(tip_object.key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle metadata head names an unavailable key fingerprint: {error}"
                ))
            })?;
        let context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            head_encryption,
        );
        let bytes = read_exact_circle_object(storage, &context, &object.object, &prefix).await?;
        let head: super::circle::CircleMetadataHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse exact Circle metadata head: {error}"
                ))
            })?;
        let declared_ref = CircleMetadataHeadRef::from_stored_head(&head, object.object.clone());
        let authority = resolve_circle_stream_authority(
            db,
            verified_prefix,
            storage,
            root,
            commit_ref,
            commit,
            head.successor.activation,
            head.stream_id,
            circle_id,
            &head.author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
                circle_id,
                first_slot,
            },
        )
        .await?;
        verify_circle_head_chain(
            storage,
            &context,
            CircleHeadKind::Metadata,
            CircleHeadValue::Metadata(head.clone()),
            object.object.clone(),
            &authority,
        )
        .await?;
        if !head.verify_for_registration(&authority.registration)
            || authority.registration.author_pubkey != head.author_pubkey
            || (authority.activated_here && head.seq != 1)
            || head.successor.activation != authority.activation_id
            || (head.seq == 1
                && (head.successor.predecessor.is_some()
                    || object.object.slot() != &authority.first_slot))
            || head.head_hash() != reference.head_hash
            || head.tip != tip_object.object
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::MetadataHead {
                    circle_id: head.circle_id,
                    head: &declared_ref,
                },
            )
            .is_err()
            || head.store_root_hash != store_root_hash
            || head.circle_id != circle_id
            || &declared_ref != reference
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata head failed exact verification".to_string(),
            ));
        }
        if authority.activated_here {
            consumed_stream_activations.insert(authority.activation_id);
        }
        pending.insert(head.coord());
    }
    let selected = state.selected.clone();
    let expected_heads = state
        .heads
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<Vec<_>>();

    let mut entries = BTreeMap::new();
    while let Some(coord) = pending.pop_first() {
        if entries.contains_key(&coord) {
            continue;
        }
        let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
            circle_id,
            coord: &coord,
        });
        let object = objects.metadata_entries.get(&coord).ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle activation omits exact metadata entry {}",
                coord.metadata_hash
            ))
        })?;
        let exact_encryption = encryption
            .service_for_fingerprint(object.key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle metadata names an unavailable key fingerprint: {error}"
                ))
            })?;
        let exact_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            exact_encryption,
        );
        let bytes =
            read_exact_circle_object(storage, &exact_context, &object.object, &prefix).await?;
        if ObjectHash::digest(&bytes) != coord.metadata_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata bytes differ from the signed coordinate".to_string(),
            ));
        }
        let entry: CircleMetadata = serde_json::from_slice(&bytes).map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "parse exact Circle metadata entry: {error}"
            ))
        })?;
        let declared_coord = entry.coord();
        if !entry.verify()
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::MetadataEntry {
                    circle_id: entry.circle_id,
                    coord: &declared_coord,
                },
            )
            .is_err()
            || entry.store_root_hash != store_root_hash
            || entry.circle_id != circle_id
            || declared_coord != coord
            || entry.key_fingerprint != object.key_fingerprint
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata entry failed exact verification".to_string(),
            ));
        }
        let author_roster = load_metadata_author_roster(
            db,
            verified_prefix,
            storage,
            commit,
            circle_id,
            &entry.author_roster,
            encryption.clone(),
            objects,
            root,
            commit_ref,
            consumed_stream_activations,
        )
        .await?;
        let author_is_owner = author_roster
            .authorizes_owner_grant_id(&entry.author_pubkey, &entry.author_owner_grant);
        if !author_is_owner {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata author lacks its exact grant in the named historical roster"
                    .to_string(),
            ));
        }
        pending.extend(entry.dependencies.iter().cloned());
        entries.insert(coord, entry);
    }
    verify_metadata_history(&entries, Some(&expected_heads))?;
    let selected_entry = entries.get(&selected).ok_or_else(|| {
        CircleOperationError::InvalidState(
            "selected Circle metadata coordinate is not in its covered history".to_string(),
        )
    })?;
    let canonical_selected = entries
        .values()
        .max_by_key(|entry| {
            (
                entry.metadata_stamp.as_str(),
                entry.author_pubkey.as_str(),
                entry.device_id.as_str(),
                entry.metadata_hash(),
            )
        })
        .expect("metadata history has a selected entry");
    if canonical_selected.coord() != selected || state.state_hash != selected_entry.metadata_hash()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle metadata selection or state hash is not canonical".to_string(),
        ));
    }
    Ok(selected_entry.clone())
}

fn verify_metadata_history(
    entries: &BTreeMap<super::circle::CircleMetadataCoord, CircleMetadata>,
    expected_heads: Option<&[super::circle::CircleMetadataCoord]>,
) -> Result<(), CircleOperationError> {
    let mut streams =
        BTreeMap::<super::circle::CircleAuthorStreamKey, BTreeMap<u64, &CircleMetadata>>::new();
    for (coord, entry) in entries {
        if entry
            .dependencies
            .iter()
            .any(|dependency| !entries.contains_key(dependency))
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata depends on an absent coordinate".to_string(),
            ));
        }
        if streams
            .entry(coord.stream_key())
            .or_default()
            .insert(coord.seq, entry)
            .is_some()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata stream has a conflicting sequence".to_string(),
            ));
        }
    }
    let mut actual_heads = Vec::new();
    for (stream, positions) in streams {
        let max = *positions
            .keys()
            .next_back()
            .expect("metadata stream is non-empty");
        let mut previous = None;
        for seq in 1..=max {
            let entry = positions.get(&seq).ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle metadata stream has a missing sequence".to_string(),
                )
            })?;
            if entry.previous_hash != previous {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata stream predecessor is invalid".to_string(),
                ));
            }
            if seq > 1 {
                let predecessor = positions[&(seq - 1)].coord();
                if !entry.dependencies.contains(&predecessor) {
                    return Err(CircleOperationError::InvalidState(
                        "Circle metadata entry lacks its exact own predecessor".to_string(),
                    ));
                }
            }
            previous = Some(entry.metadata_hash());
        }
        actual_heads.push(positions[&max].coord());
        debug_assert_eq!(
            actual_heads.last().map(|coord| coord.stream_key()),
            Some(stream)
        );
    }
    actual_heads.sort_by_key(|coord| coord.stream_key());
    if expected_heads.is_some_and(|expected| expected != actual_heads) {
        return Err(CircleOperationError::InvalidState(
            "Circle metadata heads do not name its exact frontier".to_string(),
        ));
    }
    Ok(())
}

async fn verify_control_membership(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    control: &PreparedCircleControl,
    founder_pubkey: &str,
) -> Result<Vec<(String, super::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.value.active_epoch.store_membership;
    let chain = Box::pin(super::membership_ops::load_anchored_chain_at_exact_heads(
        storage,
        root,
        founder_pubkey,
        &state.heads,
        &state.resolutions,
    ))
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if !chain.authorizes_write_authority(
        &control.value.value.membership_authority,
        &control.value.author_pubkey,
    ) {
        return Err(CircleOperationError::InvalidState(
            "Store membership does not authorize circle control author".to_string(),
        ));
    }
    let membership_state_hash = match chain.status() {
        super::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
        super::membership::MembershipStatus::Conflict(_) => {
            return Err(CircleOperationError::InvalidState(
                "Store membership state has an unresolved conflict".to_string(),
            ));
        }
    };
    let expected_state = StoreMembershipStateRef::from_parts(
        state.heads.clone(),
        state.resolutions.clone(),
        state.recovery.clone(),
        membership_state_hash,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if expected_state != control.value.store_membership_state_ref() {
        return Err(CircleOperationError::InvalidState(
            "circle control Store membership state reference is invalid".to_string(),
        ));
    }
    Ok(chain.current_members())
}

pub(crate) fn verify_control_context(
    reference: &CircleControlRef,
    control: &PreparedCircleControl,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<(), CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .and_then(|()| commit.verify_at(commit.store_root_hash, &commit_ref.coord, author))
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let device_matches = control.value.value.order.device_id == author.device_id.to_string();
    if !control.verify()
        || reference.circle_id() != control.value.circle_id
        || reference.control() != &control.coord
        || control.value.store_root_hash != commit.store_root_hash
        || control.value.author_pubkey != author.author_pubkey
        || !device_matches
    {
        return Err(CircleOperationError::InvalidState(
            "circle control context differs from its Store reference and commit".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn load_exact_slot_bytes(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    read_exact_circle_object(storage, context, object, semantic_prefix).await
}

async fn read_exact_circle_object(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    storage
        .read_protocol_object(context, object, semantic_prefix)
        .await
        .map_err(super::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}

#[cfg(test)]
mod authority_tests {
    use super::super::causal_grants::AuthorStreamId;
    use super::super::circle::{
        CircleControlValue, CircleRole, CircleRosterChain, CircleRosterConflict, CircleRosterEntry,
        CircleRosterHead, CircleRosterHeadRef, CircleRosterStatus, ExactCircleRosterHead,
        MergeCircleOwnerAuthorityRef,
    };
    use super::super::membership::MembershipGrantId;
    use super::super::test_helpers::user_keypair_from_seed;
    use super::*;

    fn exact_ref(label: &str) -> ExactObjectRef {
        let bytes = label.as_bytes();
        ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!(
                "store-v1/test-circle-objects/{label}.json"
            ))
            .unwrap(),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        )
    }

    fn roster_head(
        label: &str,
        entry: &CircleRosterEntry,
        signer: &UserKeypair,
    ) -> (CircleRosterHead, CircleRosterHeadRef) {
        let head = CircleRosterHead::signed(
            entry,
            exact_ref(&format!("{label}-tip")),
            super::super::store_commit::SuccessorLink {
                activation: super::super::store_commit::StreamActivationId::from_digest(
                    ObjectHash::digest(format!("{label}-activation").as_bytes()),
                ),
                predecessor: (entry.seq > 1).then(|| exact_ref(&format!("{label}-predecessor"))),
                next_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test-circle-successors/{label}.json"
                ))
                .unwrap(),
            },
            signer,
        );
        let reference =
            CircleRosterHeadRef::from_stored_head(&head, exact_ref(&format!("{label}-head")));
        (head, reference)
    }

    #[test]
    fn resolution_replay_uses_circle_conflict_closure_independently_of_current_suffix() {
        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let store_root_hash = ObjectHash::digest(b"Circle replay Store root");
        let founder_grant = MembershipGrantId(ObjectHash::digest(b"Circle replay founder grant"));
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &founder_grant);
        let first_stream = AuthorStreamId::from_bytes([71; 32]);
        let second_stream = AuthorStreamId::from_bytes([72; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            founder_grant,
            &first_owner,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .expect("founder roster")
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey,
                CircleRole::Owner,
                &first_owner,
            )
            .expect("add second owner");
        base.push(add_second);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .expect("two-owner roster")
            .signed_remove_member(
                "first-device",
                first_stream,
                keys::public_key_hex(&second_owner),
                &first_owner,
            )
            .expect("first branch");
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .expect("two-owner roster")
            .signed_remove_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                &second_owner,
            )
            .expect("second branch");
        let mut conflict_entries = base;
        conflict_entries.extend([remove_second.clone(), remove_first.clone()]);
        let (first_head, first_head_ref) =
            roster_head("first-conflict", &remove_second, &first_owner);
        let (second_head, second_head_ref) =
            roster_head("second-conflict", &remove_first, &second_owner);
        let conflict_heads = vec![first_head_ref, second_head_ref];
        let conflicted = CircleRosterChain::from_entries_with_heads(
            conflict_entries.clone(),
            vec![
                ExactCircleRosterHead::bind(first_head, conflict_heads[0].clone()).unwrap(),
                ExactCircleRosterHead::bind(second_head, conflict_heads[1].clone()).unwrap(),
            ],
        )
        .expect("cross-revocation conflict");
        let resolver_branch = match conflicted.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch
                        .active_grants()
                        .any(|(_, grant)| grant.member_pubkey == first_pubkey)
                })
                .expect("first owner's branch")
                .heads
                .clone(),
            _ => panic!("expected revocation cycle"),
        };
        let resolution = conflicted
            .signed_cycle_resolution(resolver_branch, &first_owner)
            .expect("resolution");
        let mut resumed = conflicted.clone();
        resumed
            .apply_resolutions(std::slice::from_ref(&resolution))
            .expect("apply resolution");
        let suffix = resumed
            .signed_set_member(
                "first-device",
                AuthorStreamId::from_bytes([73; 32]),
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &first_owner,
            )
            .expect("post-resolution suffix");
        let mut current_heads = conflict_heads.clone();
        current_heads.push(roster_head("suffix", &suffix, &first_owner).1);
        current_heads.sort_by_key(|head| head.coord.clone());
        let mut resumed_entries = resumed.entries().to_vec();
        resumed_entries.push(suffix.clone());
        resumed = resumed
            .replay_resolved_history_to_heads(resumed_entries, current_heads.clone())
            .expect("apply suffix");

        assert_eq!(
            resumed.author_heads(),
            current_heads
                .iter()
                .map(|head| head.coord.clone())
                .collect::<Vec<_>>()
        );
        assert!(resumed.resolved().members().contains_key(&first_pubkey));
    }

    #[test]
    fn resolution_replay_orders_circle_checkpoints_by_signed_head_references() {
        let first = user_keypair_from_seed([11; 32]);
        let second = user_keypair_from_seed([12; 32]);
        let third = user_keypair_from_seed([13; 32]);
        let fourth = user_keypair_from_seed([14; 32]);
        let pubkeys = [&first, &second, &third, &fourth]
            .into_iter()
            .map(keys::public_key_hex)
            .collect::<Vec<_>>();
        let store_root_hash = ObjectHash::digest(b"ordered Circle replay Store");
        let founder_grant = MembershipGrantId(ObjectHash::digest(b"ordered Circle founder"));
        let circle_id = CircleId::founder(store_root_hash, &pubkeys[0], &founder_grant);
        let founder_stream = AuthorStreamId::from_bytes([151; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            founder_stream,
            founder_grant,
            &first,
        );
        let mut history = vec![founder];
        for pubkey in pubkeys.iter().skip(1) {
            let add = CircleRosterChain::from_entries(history.clone())
                .expect("load roster")
                .signed_set_member(
                    "first-device",
                    founder_stream,
                    pubkey.clone(),
                    CircleRole::Owner,
                    &first,
                )
                .expect("add Owner");
            history.push(add);
        }
        let base = CircleRosterChain::from_entries(history.clone()).expect("four-Owner roster");
        let remove_second = base
            .signed_remove_member("first-device", founder_stream, pubkeys[1].clone(), &first)
            .expect("first conflict branch");
        let remove_first = base
            .signed_remove_member(
                "second-device",
                AuthorStreamId::from_bytes([153; 32]),
                pubkeys[0].clone(),
                &second,
            )
            .expect("second conflict branch");
        history.extend([remove_second.clone(), remove_first.clone()]);
        let first_bound_heads = [
            roster_head("ordered-first", &remove_second, &first),
            roster_head("ordered-second", &remove_first, &second),
        ];
        let first_heads = first_bound_heads
            .iter()
            .map(|(_, reference)| reference.clone())
            .collect::<Vec<_>>();
        let first_conflict = CircleRosterChain::from_entries_with_heads(
            history.clone(),
            first_bound_heads
                .into_iter()
                .map(|(head, reference)| ExactCircleRosterHead::bind(head, reference).unwrap())
                .collect(),
        )
        .expect("first conflict");
        let first_branch = match first_conflict.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == pubkeys[0] && record.role == CircleRole::Owner
                    })
                })
                .expect("first Owner branch")
                .heads
                .clone(),
            _ => panic!("expected first conflict"),
        };
        let first_resolution = first_conflict
            .signed_cycle_resolution(first_branch, &first)
            .expect("first resolution");
        let mut resumed = first_conflict;
        resumed
            .apply_resolutions(std::slice::from_ref(&first_resolution))
            .expect("apply first resolution");

        let remove_fourth = resumed
            .signed_remove_member(
                "third-device",
                AuthorStreamId::from_bytes([7; 32]),
                pubkeys[3].clone(),
                &third,
            )
            .expect("third Owner removes fourth");
        let remove_third = resumed
            .signed_remove_member(
                "fourth-device",
                AuthorStreamId::from_bytes([102; 32]),
                pubkeys[2].clone(),
                &fourth,
            )
            .expect("fourth Owner removes third");
        let refs = vec![first_resolution.resolution_ref()];
        assert_eq!(remove_fourth.resolution_dependencies, refs);
        assert_eq!(remove_third.resolution_dependencies, refs);
        let remove_fourth_ref = roster_head("ordered-third-alternate", &remove_fourth, &third).1;
        let remove_third_ref = roster_head("ordered-fourth", &remove_third, &fourth).1;
        let second_heads = vec![remove_fourth_ref, remove_third_ref];
        let mut entries = resumed.entries().to_vec();
        entries.extend([remove_fourth.clone(), remove_third.clone()]);
        let mut heads = first_heads.clone();
        heads.extend(second_heads.clone());
        let mut second_conflict = resumed
            .replay_resolved_history_to_heads(entries, heads)
            .expect("second conflict");
        let second_branch = match second_conflict.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == pubkeys[2] && record.role == CircleRole::Owner
                    })
                })
                .expect("third Owner branch")
                .heads
                .clone(),
            _ => panic!("expected second revocation cycle"),
        };
        let second_resolution = second_conflict
            .signed_cycle_resolution(second_branch, &third)
            .expect("second resolution");
        assert!(
            second_resolution.conflict_hash < first_resolution.conflict_hash,
            "fixture must put the causally later Circle resolution first by canonical key"
        );
        let second_entries = vec![remove_fourth, remove_third];
        second_conflict
            .apply_resolutions(std::slice::from_ref(&second_resolution))
            .expect("apply second resolution");
        let suffix = second_conflict
            .signed_set_member(
                "third-device",
                AuthorStreamId::from_bytes([250; 32]),
                keys::public_key_hex(&user_keypair_from_seed([15; 32])),
                CircleRole::Member,
                &third,
            )
            .expect("suffix");
        let mut final_entries = second_conflict.entries().to_vec();
        final_entries.push(suffix.clone());
        let mut current_heads = first_heads.clone();
        current_heads.extend(second_heads.clone());
        assert_eq!(
            suffix.resolution_dependencies,
            second_conflict.resolution_refs()
        );
        current_heads.push(roster_head("ordered-suffix", &suffix, &third).1);
        current_heads.sort_by_key(|head| head.coord.clone());
        second_conflict = second_conflict
            .replay_resolved_history_to_heads(final_entries, current_heads.clone())
            .expect("apply suffix");
        history.extend(second_entries);

        assert_eq!(
            second_conflict.author_heads(),
            current_heads
                .iter()
                .map(|head| head.coord.clone())
                .collect::<Vec<_>>()
        );
        let mut expected_resolutions = vec![
            first_resolution.resolution_ref(),
            second_resolution.resolution_ref(),
        ];
        expected_resolutions.sort();
        assert_eq!(second_conflict.resolution_refs(), expected_resolutions);
    }

    #[test]
    fn control_authority_uses_the_pre_transition_roster_for_self_demotion() {
        let author = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let author_pubkey = keys::public_key_hex(&author);
        let author_grant = MembershipGrantId(ObjectHash::digest(b"self-demotion grant"));
        let store_root_hash = ObjectHash::digest(b"self-demotion Store");
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &author_grant);
        let stream_id = AuthorStreamId::from_bytes([21; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "author-device",
            stream_id,
            author_grant.clone(),
            &author,
        );
        let author_created_at = founder.coord();
        let mut entries = vec![founder];
        let add_second_owner = CircleRosterChain::from_entries(entries.clone())
            .expect("load founder roster")
            .signed_set_member(
                "author-device",
                stream_id,
                keys::public_key_hex(&second_owner),
                CircleRole::Owner,
                &author,
            )
            .expect("add second Owner");
        entries.push(add_second_owner);
        let before = CircleRosterChain::from_entries(entries.clone())
            .expect("load pre-demotion roster")
            .resolved();
        let demotion = CircleRosterChain::from_entries(entries.clone())
            .expect("load pre-demotion roster")
            .signed_set_member(
                "author-device",
                stream_id,
                author_pubkey.clone(),
                CircleRole::Member,
                &author,
            )
            .expect("self-demote while another Owner remains");
        entries.push(demotion);
        let after = CircleRosterChain::from_entries(entries)
            .expect("load post-demotion roster")
            .resolved();
        let authority = MergeCircleOwnerAuthorityRef::Roster {
            roster: super::super::circle::MergeCircleRosterStateRef {
                heads: Vec::new(),
                resolutions: Vec::new(),
                state_hash: before.state_hash,
            },
            grant_id: author_grant,
            created_at: author_created_at,
        };

        assert!(verify_merge_circle_owner_authority(
            &author_pubkey,
            &authority,
            &before,
        ));
        assert!(!verify_merge_circle_owner_authority(
            &author_pubkey,
            &authority,
            &after,
        ));
    }

    #[tokio::test]
    async fn current_state_reducer_retains_each_concurrent_control_branch() {
        fn control_head_ref(
            label: &str,
            control: &CircleCurrentControl,
        ) -> super::super::circle::MergeCircleControlHeadRef {
            super::super::circle::MergeCircleControlHeadRef {
                coord: control.coordinate().clone(),
                head_hash: ObjectHash::digest(format!("{label}-head").as_bytes()),
                object: exact_ref(&format!("{label}-head")),
            }
        }

        fn branch(
            mut state: CircleCurrentState,
            owner: &UserKeypair,
            device_id: &str,
            stream_id: AuthorStreamId,
        ) -> CircleCurrentState {
            let CircleCurrentState::Active(active) = &mut state else {
                panic!("branch source must be active")
            };
            let current = &mut active.current;
            let predecessor = current.clone();
            let CircleControlValue {
                order,
                active_epoch,
                ..
            } = &mut current.control.value.value;
            order.device_id = device_id.to_string();
            order.stream_id = stream_id;
            order.seq = 1;
            order.previous_control_hash = None;
            order.dependencies = vec![predecessor.coordinate().clone()];
            active_epoch.covered_control_heads = vec![control_head_ref(device_id, &predecessor)];
            current.control.value.signature =
                keys::sign_hex(owner, &current.control.value.canonical_bytes()).1;
            current.control.coord = current.control.value.coord();
            current.control.bytes =
                serde_json::to_vec(&current.control.value).expect("serialize branch control");
            assert!(state.verify(), "branch current state must verify");
            state
        }

        fn successor(
            mut state: CircleCurrentState,
            owner: &UserKeypair,
            observed: &[(&str, &CircleCurrentState)],
        ) -> CircleCurrentState {
            let CircleCurrentState::Active(active) = &mut state else {
                panic!("successor source must be active")
            };
            let current = &mut active.current;
            let predecessor = current.clone();
            let predecessor_stream = predecessor.coordinate().stream_key();
            let CircleControlValue {
                order,
                active_epoch,
                ..
            } = &mut current.control.value.value;
            let mut frontier = active_epoch.covered_control_heads.clone();
            frontier.retain(|head| head.coord.stream_key() != predecessor_stream);
            frontier.push(control_head_ref("own-predecessor", &predecessor));
            for (label, observed) in observed {
                let observed = observed
                    .resolved_control()
                    .expect("observed control is resolved");
                let stream = observed.coordinate().stream_key();
                frontier.retain(|head| head.coord.stream_key() != stream);
                frontier.push(control_head_ref(label, observed));
            }
            frontier.sort_by_key(|head| head.coord.stream_key());
            order.seq = order.seq.checked_add(1).expect("control sequence fits u64");
            order.previous_control_hash = Some(predecessor.control_hash());
            order.dependencies = frontier
                .iter()
                .filter(|head| head.coord.stream_key() != predecessor_stream)
                .map(|head| head.coord.clone())
                .collect();
            active_epoch.covered_control_heads = frontier;
            current.control.value.signature =
                keys::sign_hex(owner, &current.control.value.canonical_bytes()).1;
            current.control.coord = current.control.value.coord();
            current.control.bytes =
                serde_json::to_vec(&current.control.value).expect("serialize successor control");
            assert!(state.verify(), "successor current state must verify");
            state
        }

        let db = super::super::test_helpers::open_test_db();
        let circle_id = db
            .call(|conn| {
                Ok(super::super::test_helpers::install_test_active_circle(
                    conn,
                    "current-control-conflict",
                )
                .0)
            })
            .await
            .expect("install founder current state");
        let founder = db
            .call(move |conn| {
                let payload = conn
                    .query_row(
                        "SELECT state FROM circle_current_state WHERE circle_id = ?1",
                        [circle_id.to_string()],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .map_err(crate::database::DbError::from)?;
                serde_json::from_slice::<CircleCurrentState>(&payload).map_err(|error| {
                    crate::database::DbError::Message(format!(
                        "parse test Circle current state: {error}"
                    ))
                })
            })
            .await
            .expect("load founder current state");
        let owner = super::super::test_helpers::test_circle_owner_keypair();
        let first = branch(
            founder.clone(),
            &owner,
            "first-successor-device",
            AuthorStreamId::from_bytes([41; 32]),
        );
        let second = branch(
            founder,
            &owner,
            "second-successor-device",
            AuthorStreamId::from_bytes([42; 32]),
        );
        let first_current = first
            .clone()
            .advance(first.clone())
            .expect_err("a control cannot advance itself");
        assert!(first_current.contains("duplicate branch"));

        let conflict = first
            .clone()
            .advance(second.clone())
            .expect("concurrent successors form a conflict");
        assert!(conflict.verify());
        assert_eq!(conflict.active_record_count(), 2);
        assert!(conflict.active().is_none());

        let first_descendant = successor(first.clone(), &owner, &[]);
        let advanced_conflict = conflict
            .clone()
            .advance(first_descendant)
            .expect("a branch descendant replaces its branch tip");
        assert!(advanced_conflict.verify());
        assert_eq!(advanced_conflict.active_record_count(), 2);

        let resolution = successor(first, &owner, &[("second-branch", &second)]);
        let resolved = conflict
            .advance(resolution)
            .expect("a control covering every branch resolves the conflict");
        assert!(resolved.verify());
        assert_eq!(resolved.active_record_count(), 1);
        assert!(resolved.active().is_some());
    }
}

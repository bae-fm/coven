use serde::{Deserialize, Serialize};

use super::verify_control_context;
use crate::sync::circle::{
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleControl, CircleControlCoord,
    CircleId, CircleMetadata, PreparedAccessLeaf, PreparedCircleAccess, PreparedCircleControl,
};
use crate::sync::circle_roster::CircleMaterializedRoster;
use crate::sync::store::circle_controls::CircleOperationError;
use crate::sync::store_commit::{
    CandidateFamilyId, CircleAccessObjectRef, CircleControlRef, ObjectHash, StoreBatchCommit,
    StoreBatchCommitRef, StoreDeviceRegistration, StreamActivation, StreamActivationId,
};

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
    ) -> Result<Self, crate::sync::store_commit::StoreProtocolError> {
        if !commit.stream_activations().is_empty() {
            return Err(crate::sync::store_commit::StoreProtocolError::Malformed(
                "Store commit stream activations have not been verified".to_string(),
            ));
        }
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: Vec::new(),
        })
    }

    pub(super) fn from_verified_circle_commit(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::sync::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        Ok(Self {
            activating_commit: activating_commit.clone(),
            activations: commit.stream_activations().to_vec(),
        })
    }

    pub(crate) fn from_verified_store_control(
        commit: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Self, crate::sync::store_commit::StoreProtocolError> {
        activating_commit.verify_commit(commit)?;
        if commit.control().is_none() {
            return Err(crate::sync::store_commit::StoreProtocolError::Malformed(
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

    pub(super) fn activation(
        &self,
        activation_id: StreamActivationId,
    ) -> Option<&(StreamActivation, StoreBatchCommitRef)> {
        self.by_activation.get(&activation_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedCircleActivations {
    pub(super) circles: Vec<VerifiedCircleReference>,
    pub(super) stream_activations: VerifiedStreamActivations,
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
    ) -> Result<Self, crate::sync::store_commit::StoreProtocolError> {
        Ok(Self {
            circles: Vec::new(),
            stream_activations: VerifiedStreamActivations::none(commit, commit_ref)?,
        })
    }

    pub(crate) fn membership_control(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<Self, crate::sync::store_commit::StoreProtocolError> {
        if !commit.circle_controls().is_empty() {
            return Err(crate::sync::store_commit::StoreProtocolError::Malformed(
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
        && reference.bootstrap
            == match &access.leaf.value.disposition {
                crate::sync::circle::CircleAccessDisposition::Active { bootstrap, .. } => {
                    bootstrap.as_ref().map(|bootstrap| bootstrap.image.clone())
                }
                crate::sync::circle::CircleAccessDisposition::Inactive => None,
            }
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
    pub(super) control: PreparedCircleControl,
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
pub(crate) struct CircleAccessibleState {
    pub(super) current: CircleCurrentControl,
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
    Active(Box<CircleAccessibleState>),
    Closing(Box<CircleAccessibleState>),
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

    pub(super) fn control_hash(&self) -> ObjectHash {
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
            }) => {
                let accessible = Box::new(CircleAccessibleState {
                    current,
                    candidate_family,
                    access: leaf.value.clone(),
                    roster: active.roster.clone(),
                    metadata: active.metadata.clone(),
                });
                match accessible.current.control.value.state() {
                    crate::sync::circle::CircleControlState::ActiveEpoch(_) => {
                        Self::Active(accessible)
                    }
                    crate::sync::circle::CircleControlState::EpochClose(_) => {
                        Self::Closing(accessible)
                    }
                }
            }
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
            Self::Closing(closing) => advance_resolved_control(closing.current, next),
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
                matches!(
                    active.current.control.value.state(),
                    crate::sync::circle::CircleControlState::ActiveEpoch(_)
                ) && verify_accessible_state(active)
            }
            Self::Closing(closing) => {
                matches!(
                    closing.current.control.value.state(),
                    crate::sync::circle::CircleControlState::EpochClose(_)
                ) && verify_accessible_state(closing)
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
            Self::Closing(closing) => closing.current.circle_id(),
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
            Self::Closing(_) | Self::Inactive(_) | Self::ControlConflict { .. } => None,
        }
    }

    pub(crate) fn active_record_count(&self) -> usize {
        match self {
            Self::Active(_) | Self::Closing(_) => 1,
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
            Self::Closing(_) | Self::Inactive(_) | Self::ControlConflict { .. } => None,
        }
    }

    pub(super) fn resolved_control(&self) -> Option<&CircleCurrentControl> {
        match self {
            Self::Active(active) => Some(&active.current),
            Self::Closing(closing) => Some(&closing.current),
            Self::Inactive(inactive) => Some(&inactive.current),
            Self::ControlConflict { .. } => None,
        }
    }
}

fn verify_accessible_state(state: &CircleAccessibleState) -> bool {
    state.current.verify()
        && state
            .access
            .verify_for_control(&state.current.control, state.candidate_family)
        && matches!(
            state.access.disposition,
            CircleAccessDisposition::Active { .. }
        )
        && state.roster.verify()
        && state.metadata.verify()
        && state.metadata.circle_id == state.current.circle_id()
        && state.metadata.epoch_id == state.current.control.value.epoch_id()
        && state.metadata.key_fingerprint == state.current.control.value.key_fingerprint()
        && metadata_matches_control(&state.metadata, &state.current.control.value)
        && roster_matches_control(&state.roster, &state.current.control.value)
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
use std::collections::BTreeMap;

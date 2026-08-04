use super::validation::{
    require_version, validate_commit_frontier, validate_store_device_state_ref,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceStateRef {
    frontier: CommitFrontier,
    recovery: Vec<OwnerRecoveryCursor>,
    state_hash: ObjectHash,
}

impl StoreDeviceStateRef {
    pub fn from_resolved(
        frontier: CommitFrontier,
        state: &ResolvedStoreDeviceState,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&frontier)?;
        validate_recovery_cursors(&state.recovery)?;
        Ok(Self {
            frontier,
            recovery: state.recovery.clone(),
            state_hash: state.state_hash,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn recovery(&self) -> &[OwnerRecoveryCursor] {
        &self.recovery
    }

    pub fn frontier(&self) -> &CommitFrontier {
        &self.frontier
    }

    pub(crate) fn with_frontier(
        &self,
        frontier: CommitFrontier,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&frontier)?;
        Ok(Self {
            frontier,
            recovery: self.recovery.clone(),
            state_hash: self.state_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceStatus {
    Active,
    Inactive {
        terminals: Vec<StoreDeviceExclusionRef>,
        accepted_cut: StoreHistoryCut,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRecord {
    pub registration: StoreDeviceRegistrationRef,
    pub proposals: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    pub status: StoreDeviceStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreDeviceExclusionProposalId(ObjectHash);

impl StoreDeviceExclusionProposalId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }
}

impl fmt::Display for StoreDeviceExclusionProposalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposalRef {
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    pub proposal_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionRef {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellationRef {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionOutcomeRef {
    Excluded(StoreDeviceExclusionRef),
    Cancelled(StoreDeviceExclusionCancellationRef),
}

#[derive(Debug)]
pub(crate) struct VerifiedDeviceExclusionProposal {
    pub(crate) reference: StoreDeviceExclusionProposalRef,
    pub(crate) object: crate::protocol::objects::VerifiedObject<StoreDeviceExclusionProposal>,
    pub(crate) target: StoreDeviceRegistration,
    pub(crate) owner: StoreDeviceRegistration,
}

#[derive(Debug)]
pub(crate) struct VerifiedDeviceExclusionOutcome {
    pub(crate) object: crate::protocol::objects::VerifiedObject<StoreDeviceExclusionOutcome>,
    pub(crate) owner: StoreDeviceRegistration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedStoreDeviceOperations {
    proposals: Vec<(
        RetainedStoreDeviceExclusionProposal,
        StoreDeviceExclusionProposal,
    )>,
    outcomes: Vec<VerifiedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VerifiedStoreDeviceExclusionOutcome {
    Excluded {
        source: RetainedStoreDeviceExclusionOutcome,
        accepted_cut: StoreHistoryCut,
    },
    Cancelled(RetainedStoreDeviceExclusionOutcome),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceRegistrationActivations {
    registrations: Vec<RetainedStoreDeviceRegistrationActivation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedStoreDeviceRegistrationActivation {
    canonical_registration: Vec<u8>,
    authority: StoreDeviceRegistrationActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceOperations {
    proposals: Vec<RetainedStoreDeviceExclusionProposal>,
    outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceExclusionProposal {
    reference: StoreDeviceExclusionProposalRef,
    canonical_proposal: Vec<u8>,
    canonical_target_registration: Vec<u8>,
    canonical_owner_registration: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedStoreDeviceExclusionOutcome {
    Excluded {
        reference: StoreDeviceExclusionRef,
        canonical_outcome: Vec<u8>,
        proposal: RetainedStoreDeviceExclusionProposal,
        canonical_owner_registration: Vec<u8>,
    },
    Cancelled {
        reference: StoreDeviceExclusionCancellationRef,
        canonical_outcome: Vec<u8>,
        proposal: RetainedStoreDeviceExclusionProposal,
        canonical_owner_registration: Vec<u8>,
    },
}

impl VerifiedStoreDeviceOperations {
    pub(crate) fn proposals(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (
            &StoreDeviceExclusionProposalRef,
            &StoreDeviceExclusionProposal,
        ),
    > {
        self.proposals
            .iter()
            .map(|(source, proposal)| (&source.reference, proposal))
    }

    pub(crate) fn exclusions(
        &self,
    ) -> impl Iterator<Item = (&StoreDeviceExclusionRef, &StoreHistoryCut)> {
        self.outcomes.iter().filter_map(|outcome| match outcome {
            VerifiedStoreDeviceExclusionOutcome::Excluded {
                source,
                accepted_cut,
            } => Some((source.exclusion_reference(), accepted_cut)),
            VerifiedStoreDeviceExclusionOutcome::Cancelled(_) => None,
        })
    }

    pub(crate) fn from_retained_sources(
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        proposals: Vec<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Result<Self, StoreProtocolError> {
        let proposal_refs = proposals
            .iter()
            .map(|source| source.reference.clone())
            .collect::<Vec<_>>();
        let outcome_refs = outcomes
            .iter()
            .map(RetainedStoreDeviceExclusionOutcome::wire_reference)
            .collect::<Vec<_>>();
        if proposal_refs.as_slice() != commit.device_exclusion_proposals()
            || outcome_refs.as_slice() != commit.device_exclusion_outcomes()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let retained = RetainedStoreDeviceOperations {
            proposals: proposals.clone(),
            outcomes: outcomes.clone(),
        };
        let proposals = proposals
            .into_iter()
            .map(|source| {
                let proposal = source.verify(root)?;
                if proposal.frozen_device_state != commit.device_state {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                Ok((source, proposal))
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        let outcomes = outcomes
            .into_iter()
            .map(|source| source.verify(root))
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        let verified = Self {
            proposals,
            outcomes,
        };
        if verified.to_retained() != retained {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(verified)
    }

    pub(crate) fn without_exclusions(
        commit: &StoreBatchCommit,
    ) -> Result<Self, StoreProtocolError> {
        if !commit.device_exclusion_proposals().is_empty()
            || !commit.device_exclusion_outcomes().is_empty()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(Self {
            proposals: Vec::new(),
            outcomes: Vec::new(),
        })
    }

    pub(crate) fn to_retained(&self) -> RetainedStoreDeviceOperations {
        RetainedStoreDeviceOperations {
            proposals: self
                .proposals
                .iter()
                .map(|(source, _)| source.clone())
                .collect(),
            outcomes: self
                .outcomes
                .iter()
                .map(VerifiedStoreDeviceExclusionOutcome::source)
                .cloned()
                .collect(),
        }
    }

    pub(crate) fn apply_to(
        &self,
        predecessor: ResolvedStoreDeviceState,
        predecessor_ref: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
        let mut state = predecessor;
        for (source, proposal) in &self.proposals {
            state = state.propose_exclusion(source.reference.clone(), proposal, predecessor_ref)?;
        }
        for outcome in &self.outcomes {
            state = match outcome {
                VerifiedStoreDeviceExclusionOutcome::Excluded {
                    source,
                    accepted_cut,
                } => state.exclude(source.exclusion_reference().clone(), accepted_cut.clone())?,
                VerifiedStoreDeviceExclusionOutcome::Cancelled(source) => {
                    state.cancel_exclusion(source.cancellation_reference().clone())?
                }
            };
        }
        Ok(state)
    }
}

impl RetainedStoreDeviceRegistrationActivations {
    pub(crate) fn from_verified(
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
    ) -> Result<Self, StoreProtocolError> {
        if registrations.len() != commit.device_registrations().len() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let retained = Self {
            registrations: registrations
                .iter()
                .map(|registration| RetainedStoreDeviceRegistrationActivation {
                    canonical_registration: registration.value().to_bytes(),
                    authority: registration.activation().clone(),
                })
                .collect(),
        };
        retained.verify_for(root, commit)?;
        Ok(retained)
    }

    pub(crate) fn verify_for(
        &self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
    ) -> Result<Vec<ActivatedStoreDeviceRegistration>, StoreProtocolError> {
        if self.registrations.len() != commit.device_registrations().len() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        commit
            .device_registrations()
            .iter()
            .zip(&self.registrations)
            .map(|(activated, retained)| retained.verify(root, activated))
            .collect()
    }
}

impl RetainedStoreDeviceRegistrationActivation {
    fn verify(
        &self,
        root: &StoreRootRef,
        activated: &ActivatedStoreDeviceRegistrationRef,
    ) -> Result<ActivatedStoreDeviceRegistration, StoreProtocolError> {
        let registration = verify_retained_registration(
            root,
            &activated.registration,
            &self.canonical_registration,
        )?;
        let registration = ReferencedStoreDeviceRegistration::verified(
            activated.registration.clone(),
            registration,
        )?;
        let registration =
            ActivatedStoreDeviceRegistration::verified(registration, self.authority.clone())?;
        registration.verify_reference(activated)?;
        Ok(registration)
    }
}

impl RetainedStoreDeviceOperations {
    pub(crate) fn from_sources(
        proposals: Vec<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Self {
        Self {
            proposals,
            outcomes,
        }
    }

    pub(crate) fn verify_for(
        &self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
    ) -> Result<VerifiedStoreDeviceOperations, StoreProtocolError> {
        VerifiedStoreDeviceOperations::from_retained_sources(
            root,
            commit,
            self.proposals.clone(),
            self.outcomes.clone(),
        )
    }
}

impl RetainedStoreDeviceExclusionProposal {
    pub(crate) fn from_exact(
        reference: StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let retained = Self {
            reference,
            canonical_proposal: proposal.to_bytes(),
            canonical_target_registration: target.to_bytes(),
            canonical_owner_registration: owner.to_bytes(),
        };
        let opened = retained.verify_with_registrations(&target.store_root)?;
        if opened.object.value != *proposal || opened.target != *target || opened.owner != *owner {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(retained)
    }

    pub(crate) fn from_verified(proposal: &VerifiedDeviceExclusionProposal) -> Self {
        Self {
            reference: proposal.reference.clone(),
            canonical_proposal: proposal.object.bytes.clone(),
            canonical_target_registration: proposal.target.to_bytes(),
            canonical_owner_registration: proposal.owner.to_bytes(),
        }
    }

    pub(crate) fn reference(&self) -> &StoreDeviceExclusionProposalRef {
        &self.reference
    }

    fn verify(
        &self,
        root: &StoreRootRef,
    ) -> Result<StoreDeviceExclusionProposal, StoreProtocolError> {
        self.verify_with_registrations(root)
            .map(|proposal| proposal.object.value)
    }

    fn verify_with_registrations(
        &self,
        root: &StoreRootRef,
    ) -> Result<VerifiedDeviceExclusionProposal, StoreProtocolError> {
        self.reference
            .object
            .verify(&self.canonical_proposal)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let unverified: StoreDeviceExclusionProposal =
            serde_json::from_slice(&self.canonical_proposal)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if unverified.to_bytes() != self.canonical_proposal {
            return Err(StoreProtocolError::Malformed(
                "retained Store device exclusion proposal is not canonically encoded".to_string(),
            ));
        }
        let target = verify_retained_registration(
            root,
            &unverified.target,
            &self.canonical_target_registration,
        )?;
        let owner = verify_retained_registration(
            root,
            &unverified.owner_registration,
            &self.canonical_owner_registration,
        )?;
        let proposal = StoreDeviceExclusionProposal::parse_at(
            &self.canonical_proposal,
            &self.reference,
            &target,
            &owner,
        )?;
        Ok(VerifiedDeviceExclusionProposal {
            reference: self.reference.clone(),
            object: crate::protocol::objects::VerifiedObject {
                value: proposal,
                bytes: self.canonical_proposal.clone(),
                semantic_hash: self.reference.proposal_hash,
                object: self.reference.object.clone(),
            },
            target,
            owner,
        })
    }
}

impl RetainedStoreDeviceExclusionOutcome {
    pub(crate) fn from_exact(
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: RetainedStoreDeviceExclusionProposal,
        outcome: &StoreDeviceExclusionOutcome,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        if reference.proposal() != outcome.proposal()
            || reference.outcome_hash() != outcome.outcome_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let canonical_outcome = outcome.to_bytes();
        reference
            .object()
            .verify(&canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        Ok(match (reference, outcome) {
            (
                StoreDeviceExclusionOutcomeRef::Excluded(reference),
                StoreDeviceExclusionOutcome::Excluded(_),
            ) => Self::Excluded {
                reference: reference.clone(),
                canonical_outcome,
                proposal,
                canonical_owner_registration: owner.to_bytes(),
            },
            (
                StoreDeviceExclusionOutcomeRef::Cancelled(reference),
                StoreDeviceExclusionOutcome::Cancelled(_),
            ) => Self::Cancelled {
                reference: reference.clone(),
                canonical_outcome,
                proposal,
                canonical_owner_registration: owner.to_bytes(),
            },
            _ => return Err(StoreProtocolError::DeviceStateMismatch),
        })
    }

    pub(crate) fn from_verified(
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: RetainedStoreDeviceExclusionProposal,
        outcome: &VerifiedDeviceExclusionOutcome,
    ) -> Result<Self, StoreProtocolError> {
        match (reference, &outcome.object.value) {
            (
                StoreDeviceExclusionOutcomeRef::Excluded(reference),
                StoreDeviceExclusionOutcome::Excluded(_),
            ) => Ok(Self::Excluded {
                reference: reference.clone(),
                canonical_outcome: outcome.object.bytes.clone(),
                proposal,
                canonical_owner_registration: outcome.owner.to_bytes(),
            }),
            (
                StoreDeviceExclusionOutcomeRef::Cancelled(reference),
                StoreDeviceExclusionOutcome::Cancelled(_),
            ) => Ok(Self::Cancelled {
                reference: reference.clone(),
                canonical_outcome: outcome.object.bytes.clone(),
                proposal,
                canonical_owner_registration: outcome.owner.to_bytes(),
            }),
            _ => Err(StoreProtocolError::DeviceStateMismatch),
        }
    }

    pub(crate) fn wire_reference(&self) -> StoreDeviceExclusionOutcomeRef {
        match self {
            Self::Excluded { reference, .. } => {
                StoreDeviceExclusionOutcomeRef::Excluded(reference.clone())
            }
            Self::Cancelled { reference, .. } => {
                StoreDeviceExclusionOutcomeRef::Cancelled(reference.clone())
            }
        }
    }

    fn exclusion_reference(&self) -> &StoreDeviceExclusionRef {
        match self {
            Self::Excluded { reference, .. } => reference,
            Self::Cancelled { .. } => unreachable!("verified exclusion changed variant"),
        }
    }

    fn cancellation_reference(&self) -> &StoreDeviceExclusionCancellationRef {
        match self {
            Self::Cancelled { reference, .. } => reference,
            Self::Excluded { .. } => unreachable!("verified cancellation changed variant"),
        }
    }

    fn verify(
        self,
        root: &StoreRootRef,
    ) -> Result<VerifiedStoreDeviceExclusionOutcome, StoreProtocolError> {
        let (reference, canonical_outcome, proposal_source, canonical_owner_registration) =
            match &self {
                Self::Excluded {
                    reference,
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                } => (
                    StoreDeviceExclusionOutcomeRef::Excluded(reference.clone()),
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                ),
                Self::Cancelled {
                    reference,
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                } => (
                    StoreDeviceExclusionOutcomeRef::Cancelled(reference.clone()),
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                ),
            };
        reference
            .object()
            .verify(canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let proposal = proposal_source.verify_with_registrations(root)?;
        let unverified: StoreDeviceExclusionOutcome = serde_json::from_slice(canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if unverified.to_bytes() != *canonical_outcome {
            return Err(StoreProtocolError::Malformed(
                "retained Store device exclusion outcome is not canonically encoded".to_string(),
            ));
        }
        let owner_reference = match &unverified {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => &exclusion.owner_registration,
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                &cancellation.owner_registration
            }
        };
        let owner =
            verify_retained_registration(root, owner_reference, canonical_owner_registration)?;
        let outcome = StoreDeviceExclusionOutcome::parse_at(
            canonical_outcome,
            &reference,
            &proposal.object.value,
            &proposal.target,
            &owner,
        )?;
        match (&self, outcome) {
            (Self::Excluded { .. }, StoreDeviceExclusionOutcome::Excluded(exclusion)) => {
                if exclusion.proof.frozen_device_state != proposal.object.value.frozen_device_state
                {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                Ok(VerifiedStoreDeviceExclusionOutcome::Excluded {
                    source: self,
                    accepted_cut: exclusion.proof.cutoff,
                })
            }
            (Self::Cancelled { .. }, StoreDeviceExclusionOutcome::Cancelled(_)) => {
                Ok(VerifiedStoreDeviceExclusionOutcome::Cancelled(self))
            }
            _ => Err(StoreProtocolError::DeviceStateMismatch),
        }
    }
}

fn verify_retained_registration(
    root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
    canonical_registration: &[u8],
) -> Result<StoreDeviceRegistration, StoreProtocolError> {
    reference
        .object
        .verify(canonical_registration)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    let registration =
        StoreDeviceRegistration::parse_at(canonical_registration, root, reference.device_id)?;
    if registration.to_bytes() != canonical_registration {
        return Err(StoreProtocolError::Malformed(
            "retained Store device registration is not canonically encoded".to_string(),
        ));
    }
    reference.verify_registration(&registration)?;
    Ok(registration)
}

impl VerifiedStoreDeviceExclusionOutcome {
    fn source(&self) -> &RetainedStoreDeviceExclusionOutcome {
        match self {
            Self::Excluded { source, .. } | Self::Cancelled(source) => source,
        }
    }
}

impl StoreDeviceExclusionOutcomeRef {
    pub fn proposal(&self) -> &StoreDeviceExclusionProposalRef {
        match self {
            Self::Excluded(reference) => &reference.proposal,
            Self::Cancelled(reference) => &reference.proposal,
        }
    }

    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Excluded(reference) => &reference.object,
            Self::Cancelled(reference) => &reference.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposal {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    pub frozen_device_state: StoreDeviceStateRef,
    pub outcome_slot: ObjectSlot,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionOutcome {
    Excluded(StoreDeviceExclusion),
    Cancelled(StoreDeviceExclusionCancellation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellation {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusion {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub target: StoreDeviceRegistrationRef,
    pub proof: StoreDeviceExclusionProof,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProof {
    pub frozen_device_state: StoreDeviceStateRef,
    pub remaining_device_acks: Vec<StoreAckRef>,
    pub cutoff: StoreHistoryCut,
}

impl StoreDeviceExclusionProposal {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        proposal_id: StoreDeviceExclusionProposalId,
        target: StoreDeviceRegistrationRef,
        target_registration: &StoreDeviceRegistration,
        frozen_device_state: StoreDeviceStateRef,
        outcome_slot: ObjectSlot,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        target.verify_registration(target_registration)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || owner.store_root.store_root_hash != store_root_hash
            || target_registration.store_root.store_root_hash != store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let expected_outcome = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id)
        );
        if outcome_slot.logical_key() != expected_outcome {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_outcome,
                actual: outcome_slot.logical_key().to_string(),
            });
        }
        validate_store_device_state_ref(&frozen_device_state)?;
        let mut proposal = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            proposal_id,
            target,
            frozen_device_state,
            outcome_slot,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &proposal.canonical_signed_bytes());
        proposal.signature = signature;
        Ok(proposal)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_PROPOSAL_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                self.proposal_id,
                &self.target,
                &self.frozen_device_state,
                &self.outcome_slot,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn proposal_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Store device exclusion proposal serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceExclusionProposalRef,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let proposal: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        require_version(proposal.version)?;
        expected.verify_proposal(&proposal)?;
        proposal.target.verify_registration(target)?;
        proposal.owner_registration.verify_registration(owner)?;
        validate_store_device_state_ref(&proposal.frozen_device_state)?;
        let expected_outcome = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                proposal.target.device_id,
                proposal.proposal_id,
            )
        );
        if proposal.outcome_slot.logical_key() != expected_outcome {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_outcome,
                actual: proposal.outcome_slot.logical_key().to_string(),
            });
        }
        if proposal.store_root_hash != owner.store_root.store_root_hash
            || proposal.store_root_hash != target.store_root.store_root_hash
            || !keys::verify_signature_hex(
                &owner.device_signing_pubkey,
                &proposal.signature,
                &proposal.canonical_signed_bytes(),
            )
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(proposal)
    }
}

impl StoreDeviceExclusionProposalRef {
    pub fn from_proposal(
        proposal: &StoreDeviceExclusionProposal,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        let reference = Self {
            proposal_id: proposal.proposal_id,
            target: proposal.target.clone(),
            proposal_hash: proposal.proposal_hash(),
            object,
        };
        reference.validate_path()?;
        Ok(reference)
    }

    pub(crate) fn validate_path(&self) -> Result<(), StoreProtocolError> {
        let expected = format!(
            "{}.json",
            device_exclusion_proposal_semantic_prefix(
                self.target.device_id,
                self.proposal_id,
                self.proposal_hash,
            )
        );
        if self.object.slot().logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.object.slot().logical_key().to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn verify_proposal(
        &self,
        proposal: &StoreDeviceExclusionProposal,
    ) -> Result<(), StoreProtocolError> {
        self.validate_path()?;
        if self.proposal_id != proposal.proposal_id
            || self.target != proposal.target
            || self.proposal_hash != proposal.proposal_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }
}

impl StoreDeviceExclusionCancellation {
    pub fn signed(
        proposal: StoreDeviceExclusionProposalRef,
        proposal_value: &StoreDeviceExclusionProposal,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || proposal.proposal_hash != proposal_value.proposal_hash()
            || proposal.target != proposal_value.target
            || proposal_value.store_root_hash != owner.store_root.store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut cancellation = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            proposal,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &cancellation.canonical_signed_bytes());
        cancellation.signature = signature;
        Ok(cancellation)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_CANCELLATION_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                &self.proposal,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }
}

impl StoreDeviceExclusion {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        proposal: StoreDeviceExclusionProposalRef,
        proposal_value: &StoreDeviceExclusionProposal,
        target: StoreDeviceRegistrationRef,
        target_registration: &StoreDeviceRegistration,
        proof: StoreDeviceExclusionProof,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        target.verify_registration(target_registration)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || proposal.target != target
            || proposal.proposal_hash != proposal_value.proposal_hash()
            || proposal.target != proposal_value.target
            || target_registration.store_root.store_root_hash != owner.store_root.store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        validate_device_exclusion_proof(&proof)?;
        let mut exclusion = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            proposal,
            target,
            proof,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &exclusion.canonical_signed_bytes());
        exclusion.signature = signature;
        Ok(exclusion)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                &self.proposal,
                &self.target,
                &self.proof,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }
}

impl StoreDeviceExclusionOutcome {
    pub fn outcome_hash(&self) -> ObjectHash {
        match self {
            Self::Excluded(exclusion) => exclusion.outcome_hash(),
            Self::Cancelled(cancellation) => cancellation.outcome_hash(),
        }
    }

    pub fn proposal(&self) -> &StoreDeviceExclusionProposalRef {
        match self {
            Self::Excluded(exclusion) => &exclusion.proposal,
            Self::Cancelled(cancellation) => &cancellation.proposal,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Store device exclusion outcome serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceExclusionOutcomeRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let outcome: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        if outcome.proposal().proposal_id != proposal.proposal_id
            || outcome.proposal().proposal_hash != proposal.proposal_hash()
            || outcome.proposal().target != proposal.target
            || expected.proposal() != outcome.proposal()
            || expected.object().slot() != &proposal.outcome_slot
            || expected.outcome_hash() != outcome.outcome_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        match &outcome {
            Self::Excluded(exclusion) => {
                require_version(exclusion.version)?;
                exclusion.target.verify_registration(target)?;
                exclusion.owner_registration.verify_registration(owner)?;
                validate_device_exclusion_proof(&exclusion.proof)?;
                if exclusion.store_root_hash != proposal.store_root_hash
                    || exclusion.store_root_hash != target.store_root.store_root_hash
                    || exclusion.target != proposal.target
                    || !keys::verify_signature_hex(
                        &owner.device_signing_pubkey,
                        &exclusion.signature,
                        &exclusion.canonical_signed_bytes(),
                    )
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
            }
            Self::Cancelled(cancellation) => {
                require_version(cancellation.version)?;
                cancellation.owner_registration.verify_registration(owner)?;
                if cancellation.store_root_hash != proposal.store_root_hash
                    || !keys::verify_signature_hex(
                        &owner.device_signing_pubkey,
                        &cancellation.signature,
                        &cancellation.canonical_signed_bytes(),
                    )
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
            }
        }
        Ok(outcome)
    }
}

impl StoreDeviceExclusionOutcomeRef {
    pub fn from_outcome(
        outcome: &StoreDeviceExclusionOutcome,
        proposal: &StoreDeviceExclusionProposal,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        if object.slot() != &proposal.outcome_slot
            || outcome.proposal().proposal_id != proposal.proposal_id
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(match outcome {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => {
                Self::Excluded(StoreDeviceExclusionRef {
                    proposal: exclusion.proposal.clone(),
                    outcome_hash: exclusion.outcome_hash(),
                    object,
                })
            }
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                Self::Cancelled(StoreDeviceExclusionCancellationRef {
                    proposal: cancellation.proposal.clone(),
                    outcome_hash: cancellation.outcome_hash(),
                    object,
                })
            }
        })
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        match self {
            Self::Excluded(reference) => reference.outcome_hash,
            Self::Cancelled(reference) => reference.outcome_hash,
        }
    }
}

fn validate_device_exclusion_proof(
    proof: &StoreDeviceExclusionProof,
) -> Result<(), StoreProtocolError> {
    if proof
        .remaining_device_acks
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    validate_store_device_state_ref(&proof.frozen_device_state)?;
    validate_store_history_cut(&proof.cutoff)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceProposalState {
    Pending {
        proposal: StoreDeviceExclusionProposalRef,
    },
    Cancelled {
        outcome: StoreDeviceExclusionCancellationRef,
    },
    Superseded {
        proposal: StoreDeviceExclusionProposalRef,
        terminals: Vec<StoreDeviceExclusionRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedStoreDeviceState {
    pub devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
    pub recovery: Vec<OwnerRecoveryCursor>,
    pub state_hash: ObjectHash,
}

impl ResolvedStoreDeviceState {
    pub(crate) fn validate_canonical(&self) -> Result<(), StoreProtocolError> {
        let canonical = Self::from_parts(self.devices.clone(), self.recovery.clone())?;
        if canonical != *self {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    pub fn founder(
        root: &StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
        founder_pubkey: &str,
        founder_grant: MembershipGrantId,
        founder_recovery: &GrantStreamAnchor,
    ) -> Result<Self, StoreProtocolError> {
        let cursor = OwnerRecoveryCursor {
            owner_grant: founder_grant.clone(),
            position: OwnerRecoveryPosition::BeforeFirst {
                activation: OwnerRecoveryActivationId::derive(
                    root,
                    founder_pubkey,
                    &founder_grant,
                    founder_recovery,
                )?,
            },
        };
        let devices = BTreeMap::from([(
            founder_registration.device_id,
            StoreDeviceRecord {
                registration: founder_registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        )]);
        Self::from_parts(devices, vec![cursor])
    }

    pub fn activate_registration(
        &self,
        registration: StoreDeviceRegistrationRef,
        recovery: Option<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        if self.devices.contains_key(&registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: registration.device_id.to_string(),
            });
        }
        let mut devices = self.devices.clone();
        devices.insert(
            registration.device_id,
            StoreDeviceRecord {
                registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        );
        let mut cursors = self.recovery.clone();
        if let Some(cursor) = recovery {
            if let Some(existing) = cursors
                .iter_mut()
                .find(|existing| existing.owner_grant == cursor.owner_grant)
            {
                *existing = cursor;
            } else {
                cursors.push(cursor);
            }
        }
        Self::from_parts(devices, cursors)
    }

    pub fn activate_owner_recovery(
        &self,
        owner_grant: MembershipGrantId,
        activation: OwnerRecoveryActivationId,
    ) -> Result<Self, StoreProtocolError> {
        if self
            .recovery
            .iter()
            .any(|cursor| cursor.owner_grant == owner_grant)
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        let mut recovery = self.recovery.clone();
        recovery.push(OwnerRecoveryCursor {
            owner_grant,
            position: OwnerRecoveryPosition::BeforeFirst { activation },
        });
        Self::from_parts(self.devices.clone(), recovery)
    }

    pub(crate) fn preactivate_recovery_author(
        mut self,
        commit: &StoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
    ) -> Result<(Self, Option<StoreDeviceRegistrationRef>), StoreProtocolError> {
        if commit.device_registrations().len() != registrations.len() {
            return Err(StoreProtocolError::Malformed(
                "verified registrations do not cover every activation".to_string(),
            ));
        }
        for (activated, registration) in commit.device_registrations().iter().zip(registrations) {
            registration.verify_reference(activated)?;
            if activated.registration == commit.author_registration {
                if let Some(cursor) = registration.recovery_cursor()? {
                    self =
                        self.activate_registration(activated.registration.clone(), Some(cursor))?;
                    return Ok((self, Some(activated.registration.clone())));
                }
            }
        }
        Ok((self, None))
    }

    pub(crate) fn apply_verified_lifecycle(
        mut self,
        commit: &StoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
        preactivated: Option<&StoreDeviceRegistrationRef>,
        owner_recovery: Option<(MembershipGrantId, OwnerRecoveryActivationId)>,
    ) -> Result<Self, StoreProtocolError> {
        if commit.device_registrations().len() != registrations.len() {
            return Err(StoreProtocolError::Malformed(
                "verified registrations do not cover every activation".to_string(),
            ));
        }
        for (activated, registration) in commit.device_registrations().iter().zip(registrations) {
            registration.verify_reference(activated)?;
            if preactivated != Some(&activated.registration) {
                self = self.activate_registration(
                    activated.registration.clone(),
                    registration.recovery_cursor()?,
                )?;
            }
        }
        if let Some((grant_id, activation)) = owner_recovery {
            self = self.activate_owner_recovery(grant_id, activation)?;
        }
        Ok(self)
    }

    pub fn propose_exclusion(
        &self,
        reference: StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
        predecessor_ref: &StoreDeviceStateRef,
    ) -> Result<Self, StoreProtocolError> {
        reference.verify_proposal(proposal)?;
        if &proposal.frozen_device_state != predecessor_ref
            || predecessor_ref.state_hash() != self.state_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&reference.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != reference.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || record.proposals.contains_key(&reference.proposal_id)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        record.proposals.insert(
            reference.proposal_id,
            StoreDeviceProposalState::Pending {
                proposal: reference,
            },
        );
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn cancel_exclusion(
        &self,
        cancellation: StoreDeviceExclusionCancellationRef,
    ) -> Result<Self, StoreProtocolError> {
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&cancellation.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        let state = record
            .proposals
            .get_mut(&cancellation.proposal.proposal_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if !matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == &cancellation.proposal)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        *state = StoreDeviceProposalState::Cancelled {
            outcome: cancellation,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn exclude(
        &self,
        exclusion: StoreDeviceExclusionRef,
        accepted_cut: StoreHistoryCut,
    ) -> Result<Self, StoreProtocolError> {
        validate_store_history_cut(&accepted_cut)?;
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&exclusion.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != exclusion.proposal.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || !matches!(
                record.proposals.get(&exclusion.proposal.proposal_id),
                Some(StoreDeviceProposalState::Pending { proposal }) if proposal == &exclusion.proposal
            )
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let terminals = vec![exclusion];
        supersede_pending_proposals(&mut record.proposals, &terminals);
        record.status = StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn merge(states: impl IntoIterator<Item = Self>) -> Result<Self, StoreProtocolError> {
        let mut devices = BTreeMap::new();
        let mut recovery = BTreeMap::<MembershipGrantId, OwnerRecoveryPosition>::new();
        for state in states {
            for (device_id, record) in state.devices {
                match devices.entry(device_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(record);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry.get().registration != record.registration {
                            return Err(StoreProtocolError::DeviceStateMismatch);
                        }
                        let merged_status =
                            merge_device_status(entry.get().status.clone(), record.status)?;
                        let mut merged_proposals = merge_device_proposals(
                            entry.get().proposals.clone(),
                            record.proposals,
                        )?;
                        if let StoreDeviceStatus::Inactive { terminals, .. } = &merged_status {
                            supersede_pending_proposals(&mut merged_proposals, terminals);
                        }
                        entry.get_mut().status = merged_status;
                        entry.get_mut().proposals = merged_proposals;
                    }
                }
            }
            for cursor in state.recovery {
                match recovery.entry(cursor.owner_grant) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(cursor.position);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        if entry.get() != &cursor.position {
                            return Err(StoreProtocolError::OwnerRecoveryMismatch);
                        }
                    }
                }
            }
        }
        Self::from_parts(
            devices,
            recovery
                .into_iter()
                .map(|(owner_grant, position)| OwnerRecoveryCursor {
                    owner_grant,
                    position,
                })
                .collect(),
        )
    }

    fn from_parts(
        devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
        mut recovery: Vec<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        recovery.sort();
        validate_recovery_cursors(&recovery)?;
        validate_store_device_records(&devices)?;
        let state_hash = ObjectHash::digest(&domain_json(
            b"coven.store-device-state.v1\0",
            &(&devices, &recovery),
        ));
        Ok(Self {
            devices,
            recovery,
            state_hash,
        })
    }
}

fn supersede_pending_proposals(
    proposals: &mut BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    terminals: &[StoreDeviceExclusionRef],
) {
    for state in proposals.values_mut() {
        if let StoreDeviceProposalState::Pending { proposal } = state {
            *state = StoreDeviceProposalState::Superseded {
                proposal: proposal.clone(),
                terminals: terminals.to_vec(),
            };
        }
    }
}

fn merge_device_proposals(
    mut left: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    right: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
) -> Result<BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>, StoreProtocolError>
{
    for (proposal_id, right_state) in right {
        match left.entry(proposal_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(right_state);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let merged = merge_device_proposal_state(entry.get().clone(), right_state)?;
                entry.insert(merged);
            }
        }
    }
    Ok(left)
}

fn merge_device_proposal_state(
    left: StoreDeviceProposalState,
    right: StoreDeviceProposalState,
) -> Result<StoreDeviceProposalState, StoreProtocolError> {
    let left_proposal = match &left {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    let right_proposal = match &right {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    if left_proposal != right_proposal {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    match (left, right) {
        (
            StoreDeviceProposalState::Pending { proposal },
            StoreDeviceProposalState::Pending { .. },
        ) => Ok(StoreDeviceProposalState::Pending { proposal }),
        (
            StoreDeviceProposalState::Cancelled { outcome },
            StoreDeviceProposalState::Cancelled { outcome: other },
        ) => {
            if outcome != other {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (StoreDeviceProposalState::Cancelled { outcome }, _)
        | (_, StoreDeviceProposalState::Cancelled { outcome }) => {
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals: left,
            },
            StoreDeviceProposalState::Superseded {
                terminals: right, ..
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals: merge_terminal_refs(left, right)?,
        }),
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
            _,
        )
        | (
            _,
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals,
        }),
    }
}

pub(super) fn merge_device_status(
    left: StoreDeviceStatus,
    right: StoreDeviceStatus,
) -> Result<StoreDeviceStatus, StoreProtocolError> {
    match (left, right) {
        (StoreDeviceStatus::Active, StoreDeviceStatus::Active) => Ok(StoreDeviceStatus::Active),
        (
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
            StoreDeviceStatus::Active,
        )
        | (
            StoreDeviceStatus::Active,
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        }),
        (
            StoreDeviceStatus::Inactive {
                terminals: left_terminals,
                accepted_cut: left_cut,
            },
            StoreDeviceStatus::Inactive {
                terminals: right_terminals,
                accepted_cut: right_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals: merge_terminal_refs(left_terminals, right_terminals)?,
            accepted_cut: intersect_terminal_history_cuts(left_cut, right_cut)?,
        }),
    }
}

fn merge_terminal_refs(
    left: Vec<StoreDeviceExclusionRef>,
    right: Vec<StoreDeviceExclusionRef>,
) -> Result<Vec<StoreDeviceExclusionRef>, StoreProtocolError> {
    let terminals = left
        .into_iter()
        .chain(right)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    validate_terminal_refs(&terminals)?;
    Ok(terminals)
}

pub(super) fn merge_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    {
        let StoreHistoryCut(mut left) = left;
        let StoreHistoryCut(right) = right;
        for (stream, reference) in right {
            match left.entry(stream) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if reference.coord.sequence() > current.coord.sequence() {
                        entry.insert(reference);
                    } else if reference.coord.sequence() == current.coord.sequence()
                        && reference != *current
                    {
                        return Err(StoreProtocolError::DeviceStateMismatch);
                    }
                }
            }
        }
        Ok(StoreHistoryCut(left))
    }
}

fn intersect_terminal_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    {
        let StoreHistoryCut(left) = left;
        let StoreHistoryCut(right) = right;
        let mut intersection = BTreeMap::new();
        for (stream, left_reference) in left {
            let Some(right_reference) = right.get(&stream) else {
                continue;
            };
            let left_sequence = left_reference.coord.sequence();
            let right_sequence = right_reference.coord.sequence();
            let reference = if left_sequence < right_sequence {
                left_reference
            } else if right_sequence < left_sequence {
                right_reference.clone()
            } else if left_reference == *right_reference {
                left_reference
            } else {
                return Err(StoreProtocolError::DeviceStateMismatch);
            };
            intersection.insert(stream, reference);
        }
        Ok(StoreHistoryCut(intersection))
    }
}

fn validate_store_device_records(
    devices: &BTreeMap<StoreDeviceId, StoreDeviceRecord>,
) -> Result<(), StoreProtocolError> {
    for (device_id, record) in devices {
        if record.registration.device_id != *device_id {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (proposal_id, state) in &record.proposals {
            let proposal = match state {
                StoreDeviceProposalState::Pending { proposal }
                | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
                StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
            };
            if proposal.proposal_id != *proposal_id {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if proposal.target != record.registration {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if let StoreDeviceProposalState::Superseded { terminals, .. } = state {
                validate_terminal_refs(terminals)?;
            }
        }
        if let StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        } = &record.status
        {
            validate_terminal_refs(terminals)?;
            validate_store_history_cut(accepted_cut)?;
            if record
                .proposals
                .values()
                .any(|state| matches!(state, StoreDeviceProposalState::Pending { .. }))
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
    }
    Ok(())
}

fn validate_terminal_refs(terminals: &[StoreDeviceExclusionRef]) -> Result<(), StoreProtocolError> {
    if terminals.is_empty() || terminals.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    Ok(())
}

pub(crate) fn canonical_recovery_cursors(
    mut recovery: Vec<OwnerRecoveryCursor>,
) -> Result<Vec<OwnerRecoveryCursor>, StoreProtocolError> {
    recovery.sort();
    validate_recovery_cursors(&recovery)?;
    Ok(recovery)
}

pub(crate) fn validate_recovery_cursors(
    recovery: &[OwnerRecoveryCursor],
) -> Result<(), StoreProtocolError> {
    if recovery.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::OwnerRecoveryMismatch);
    }
    Ok(())
}

impl OwnerRecoveryNodeRef {
    pub fn slot(&self) -> &ObjectSlot {
        self.object.slot()
    }
}

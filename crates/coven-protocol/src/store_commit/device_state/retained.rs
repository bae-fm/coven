use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedStoreDeviceOperations {
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
pub struct RetainedStoreDeviceRegistrationActivations {
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
pub struct RetainedStoreDeviceOperations {
    proposals: Vec<RetainedStoreDeviceExclusionProposal>,
    outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedStoreDeviceExclusionProposal {
    reference: StoreDeviceExclusionProposalRef,
    canonical_proposal: Vec<u8>,
    canonical_target_registration: Vec<u8>,
    canonical_owner_registration: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedStoreDeviceExclusionOutcome {
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
    pub fn proposals(
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

    pub fn exclusions(&self) -> impl Iterator<Item = (&StoreDeviceExclusionRef, &StoreHistoryCut)> {
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

    pub fn without_exclusions(commit: &StoreBatchCommit) -> Result<Self, StoreProtocolError> {
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

    pub fn to_retained(&self) -> RetainedStoreDeviceOperations {
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

    pub fn apply_to(
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
    pub fn from_verified(
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

    pub fn verify_for(
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
    pub fn from_sources(
        proposals: Vec<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Self {
        Self {
            proposals,
            outcomes,
        }
    }

    pub fn verify_for(
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
    pub fn from_exact(
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

    pub fn from_verified(proposal: &VerifiedDeviceExclusionProposal) -> Self {
        Self {
            reference: proposal.reference.clone(),
            canonical_proposal: proposal.object.bytes.clone(),
            canonical_target_registration: proposal.target.to_bytes(),
            canonical_owner_registration: proposal.owner.to_bytes(),
        }
    }

    pub fn reference(&self) -> &StoreDeviceExclusionProposalRef {
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
        self.reference.object.verify(&self.canonical_proposal)?;
        let unverified: StoreDeviceExclusionProposal =
            serde_json::from_slice(&self.canonical_proposal)?;
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
            object: crate::objects::VerifiedObject {
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
    pub fn from_exact(
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
        reference.object().verify(&canonical_outcome)?;
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

    pub fn from_verified(
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

    pub fn wire_reference(&self) -> StoreDeviceExclusionOutcomeRef {
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
        reference.object().verify(canonical_outcome)?;
        let proposal = proposal_source.verify_with_registrations(root)?;
        let unverified: StoreDeviceExclusionOutcome = serde_json::from_slice(canonical_outcome)?;
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
                    accepted_cut: exclusion.proof.cutoff.clone(),
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
    reference.object.verify(canonical_registration)?;
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

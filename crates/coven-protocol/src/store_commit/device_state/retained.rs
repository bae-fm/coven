use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedStoreDeviceOperations {
    proposal: Option<RetainedStoreDeviceExclusionProposal>,
    outcomes: Vec<VerifiedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VerifiedStoreDeviceExclusionOutcome {
    Excluded(RetainedStoreDeviceExclusionOutcome),
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
    /// A commit issues at most one exclusion proposal, through its control
    /// entry.
    proposal: Option<RetainedStoreDeviceExclusionProposal>,
    outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedStoreDeviceExclusionProposal {
    proposal: StoreDeviceExclusionProposal,
    canonical_target_registration: Vec<u8>,
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
    pub fn proposal(&self) -> Option<&StoreDeviceExclusionProposal> {
        self.proposal.as_ref().map(|source| &source.proposal)
    }

    pub fn exclusions(&self) -> impl Iterator<Item = &StoreDeviceExclusionRef> {
        self.outcomes.iter().filter_map(|outcome| match outcome {
            VerifiedStoreDeviceExclusionOutcome::Excluded(source) => {
                Some(source.exclusion_reference())
            }
            VerifiedStoreDeviceExclusionOutcome::Cancelled(_) => None,
        })
    }

    pub(crate) fn from_retained_sources(
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        control_entry: Option<&MembershipEntry>,
        proposal: Option<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Result<Self, StoreProtocolError> {
        let outcome_refs = outcomes
            .iter()
            .map(RetainedStoreDeviceExclusionOutcome::wire_reference)
            .collect::<Vec<_>>();
        if proposal.as_ref().map(|source| &source.proposal)
            != commit.proposed_device_exclusion(control_entry)
            || outcome_refs.as_slice() != commit.device_exclusion_outcomes()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let retained = RetainedStoreDeviceOperations {
            proposal: proposal.clone(),
            outcomes: outcomes.clone(),
        };
        if let Some(source) = &proposal {
            source.verify(root)?;
        }
        let outcomes = outcomes
            .into_iter()
            .map(|source| source.verify(root))
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        let verified = Self { proposal, outcomes };
        if verified.to_retained() != retained {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(verified)
    }

    pub fn without_exclusions(
        commit: &StoreBatchCommit,
        control_entry: Option<&MembershipEntry>,
    ) -> Result<Self, StoreProtocolError> {
        if commit.proposed_device_exclusion(control_entry).is_some()
            || !commit.device_exclusion_outcomes().is_empty()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(Self {
            proposal: None,
            outcomes: Vec::new(),
        })
    }

    pub fn to_retained(&self) -> RetainedStoreDeviceOperations {
        RetainedStoreDeviceOperations {
            proposal: self.proposal.clone(),
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
    ) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
        let mut state = predecessor;
        if let Some(source) = &self.proposal {
            state = state.propose_exclusion(source.proposal.clone())?;
        }
        for outcome in &self.outcomes {
            state = match outcome {
                VerifiedStoreDeviceExclusionOutcome::Excluded(source) => {
                    state.exclude(source.exclusion_reference().clone())?
                }
                VerifiedStoreDeviceExclusionOutcome::Cancelled(source) => {
                    state.cancel_exclusion(source.cancellation_reference().clone())?
                }
            };
        }
        Ok(state)
    }

    /// Return the state these operations continue to contribute after their
    /// publication is accepted. This does not establish their issuer authority
    /// or recheck transition preconditions at a later snapshot boundary.
    pub fn accepted_effect(&self) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
        let proposal = self
            .proposal
            .iter()
            .map(|source| StoreDeviceProposalState::Pending {
                proposal: source.proposal.clone(),
            });
        let outcomes = self.outcomes.iter().map(|outcome| match outcome {
            VerifiedStoreDeviceExclusionOutcome::Excluded(source) => {
                let exclusion = source.exclusion_reference();
                StoreDeviceProposalState::Superseded {
                    proposal: exclusion.proposal.clone(),
                    terminals: vec![exclusion.clone()],
                }
            }
            VerifiedStoreDeviceExclusionOutcome::Cancelled(source) => {
                StoreDeviceProposalState::Cancelled {
                    outcome: source.cancellation_reference().clone(),
                }
            }
        });
        ResolvedStoreDeviceState::merge(
            proposal
                .chain(outcomes)
                .map(ResolvedStoreDeviceState::exclusion_effect)
                .collect::<Result<Vec<_>, _>>()?,
        )
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
        proposal: Option<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Self {
        Self { proposal, outcomes }
    }

    pub fn verify_for(
        &self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        control_entry: Option<&MembershipEntry>,
    ) -> Result<VerifiedStoreDeviceOperations, StoreProtocolError> {
        VerifiedStoreDeviceOperations::from_retained_sources(
            root,
            commit,
            control_entry,
            self.proposal.clone(),
            self.outcomes.clone(),
        )
    }
}

impl RetainedStoreDeviceExclusionProposal {
    pub fn from_exact(
        proposal: StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let retained = Self {
            proposal,
            canonical_target_registration: target.to_bytes(),
        };
        let opened = retained.verify(&target.store_root)?;
        if opened != *target {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(retained)
    }

    /// Reopen the retained target registration and check the proposal's slot
    /// shape, returning the registration the proposal names.
    fn verify(&self, root: &StoreRootRef) -> Result<StoreDeviceRegistration, StoreProtocolError> {
        self.proposal.validate()?;
        verify_retained_registration(
            root,
            &self.proposal.target,
            &self.canonical_target_registration,
        )
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
        let target = proposal_source.verify(root)?;
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
            &proposal_source.proposal,
            &target,
            &owner,
        )?;
        match (&self, outcome) {
            (Self::Excluded { .. }, StoreDeviceExclusionOutcome::Excluded(_)) => {
                Ok(VerifiedStoreDeviceExclusionOutcome::Excluded(self))
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
            Self::Excluded(source) | Self::Cancelled(source) => source,
        }
    }
}

use super::*;

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

/// The wire body of a device-exclusion proposal. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposalBody {
    pub store_root_hash: ObjectHash,
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    pub frozen_device_state: StoreDeviceStateRef,
    pub outcome_slot: ObjectSlot,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for StoreDeviceExclusionProposalBody {
    const DOMAIN: &'static [u8] = DEVICE_EXCLUSION_PROPOSAL_DOMAIN;
}

pub(crate) type StoreDeviceExclusionProposal = Signed<StoreDeviceExclusionProposalBody>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionOutcome {
    Excluded(StoreDeviceExclusion),
    Cancelled(StoreDeviceExclusionCancellation),
}

/// The wire body of an owner's withdrawal of an exclusion proposal. Every field
/// here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellationBody {
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for StoreDeviceExclusionCancellationBody {
    const DOMAIN: &'static [u8] = DEVICE_EXCLUSION_CANCELLATION_DOMAIN;
}

pub(crate) type StoreDeviceExclusionCancellation = Signed<StoreDeviceExclusionCancellationBody>;

/// The wire body of a device's exclusion. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionBody {
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub target: StoreDeviceRegistrationRef,
    pub proof: StoreDeviceExclusionProof,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for StoreDeviceExclusionBody {
    const DOMAIN: &'static [u8] = DEVICE_EXCLUSION_DOMAIN;
}

pub(crate) type StoreDeviceExclusion = Signed<StoreDeviceExclusionBody>;

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
        Ok(Signed::sign(
            StoreDeviceExclusionProposalBody {
                store_root_hash,
                proposal_id,
                target,
                frozen_device_state,
                outcome_slot,
                owner_registration,
                owner_grant,
            },
            owner_device_signer,
        ))
    }

    pub fn proposal_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceExclusionProposalRef,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let proposal: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
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
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        proposal.verify_by(&owner.device_signing_pubkey)?;
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
        Ok(Signed::sign(
            StoreDeviceExclusionCancellationBody {
                store_root_hash: owner.store_root.store_root_hash,
                proposal,
                owner_registration,
                owner_grant,
            },
            owner_device_signer,
        ))
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        self.hash()
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
        Ok(Signed::sign(
            StoreDeviceExclusionBody {
                store_root_hash: owner.store_root.store_root_hash,
                proposal,
                target,
                proof,
                owner_registration,
                owner_grant,
            },
            owner_device_signer,
        ))
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        self.hash()
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
                exclusion.target.verify_registration(target)?;
                exclusion.owner_registration.verify_registration(owner)?;
                validate_device_exclusion_proof(&exclusion.proof)?;
                if exclusion.store_root_hash != proposal.store_root_hash
                    || exclusion.store_root_hash != target.store_root.store_root_hash
                    || exclusion.target != proposal.target
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                exclusion.verify_by(&owner.device_signing_pubkey)?;
            }
            Self::Cancelled(cancellation) => {
                cancellation.owner_registration.verify_registration(owner)?;
                if cancellation.store_root_hash != proposal.store_root_hash {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                cancellation.verify_by(&owner.device_signing_pubkey)?;
            }
        }
        Ok(outcome)
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

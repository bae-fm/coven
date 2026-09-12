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

/// A proposal to exclude one Store device. Carried by the Owner-signed
/// membership entry that issues it: the issuing device is that entry's
/// activating head author, the issuing Owner grant is the entry's author grant,
/// and the Store root is the activating commit's.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposal {
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    /// The exact slot any outcome of this proposal must occupy. The slot is
    /// allocated once, at proposal time, so competing outcomes race for one
    /// object and the first upload wins.
    pub outcome_slot: ObjectSlot,
}

impl StoreDeviceExclusionProposal {
    /// The outcome slot's logical key is fixed by the target and proposal id.
    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        let expected = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(self.target.device_id, self.proposal_id)
        );
        if self.outcome_slot.logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.outcome_slot.logical_key().to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionRef {
    pub proposal: StoreDeviceExclusionProposal,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellationRef {
    pub proposal: StoreDeviceExclusionProposal,
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
pub struct VerifiedDeviceExclusionOutcome {
    pub object: crate::objects::VerifiedObject<StoreDeviceExclusionOutcome>,
    pub owner: StoreDeviceRegistration,
}

impl StoreDeviceExclusionOutcomeRef {
    pub fn proposal(&self) -> &StoreDeviceExclusionProposal {
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
        if object.slot() != &proposal.outcome_slot || outcome.proposal() != proposal {
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
    pub proposal: StoreDeviceExclusionProposal,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for StoreDeviceExclusionCancellationBody {
    const DOMAIN: &'static [u8] = DEVICE_EXCLUSION_CANCELLATION_DOMAIN;
}

pub type StoreDeviceExclusionCancellation = Signed<StoreDeviceExclusionCancellationBody>;

/// The wire body of a device's exclusion. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionBody {
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposal,
    pub target: StoreDeviceRegistrationRef,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for StoreDeviceExclusionBody {
    const DOMAIN: &'static [u8] = DEVICE_EXCLUSION_DOMAIN;
}

pub type StoreDeviceExclusion = Signed<StoreDeviceExclusionBody>;

impl StoreDeviceExclusionCancellation {
    pub fn signed(
        proposal: StoreDeviceExclusionProposal,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        proposal.validate()?;
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
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
    pub fn signed(
        proposal: StoreDeviceExclusionProposal,
        target: StoreDeviceRegistrationRef,
        target_registration: &StoreDeviceRegistration,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        proposal.validate()?;
        owner_registration.verify_registration(owner)?;
        target.verify_registration(target_registration)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || proposal.target != target
            || target_registration.store_root.store_root_hash != owner.store_root.store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(Signed::sign(
            StoreDeviceExclusionBody {
                store_root_hash: owner.store_root.store_root_hash,
                proposal,
                target,
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

    pub fn proposal(&self) -> &StoreDeviceExclusionProposal {
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
        let outcome: Self = crate::objects::decode_protocol_object(bytes)?;
        if outcome.proposal() != proposal
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
                if exclusion.store_root_hash != owner.store_root.store_root_hash
                    || exclusion.store_root_hash != target.store_root.store_root_hash
                    || exclusion.target != proposal.target
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                exclusion.verify_by(&owner.device_signing_pubkey)?;
            }
            Self::Cancelled(cancellation) => {
                cancellation.owner_registration.verify_registration(owner)?;
                if cancellation.store_root_hash != owner.store_root.store_root_hash {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                cancellation.verify_by(&owner.device_signing_pubkey)?;
            }
        }
        Ok(outcome)
    }
}

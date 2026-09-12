//! Durable Store-device exclusion state: the exact outcome objects, prepared
//! candidates, and completion outcomes one exclusion operation persists,
//! validated against the slots and commits they bind.

use serde::{Deserialize, Serialize};

use crate::objects::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use crate::prepared_commit::PreparedStoreOperationCommit;
use crate::remote_object::{RemoteObjectRecord, RemoteObjectRecordError};
use crate::store_commit::{
    ObjectHash, StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposal,
};

/// One exclusion outcome held for upload: its exact reference, the outcome it
/// carries, and the object prepared under that reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStoreDeviceExclusionOutcome {
    pub reference: StoreDeviceExclusionOutcomeRef,
    pub value: StoreDeviceExclusionOutcome,
    pub prepared: PreparedExactObject,
}

impl DurableStoreDeviceExclusionOutcome {
    fn store_root_hash(&self) -> ObjectHash {
        match &self.value {
            StoreDeviceExclusionOutcome::Excluded(value) => value.store_root_hash,
            StoreDeviceExclusionOutcome::Cancelled(value) => value.store_root_hash,
        }
    }

    pub fn context(&self) -> ProtocolObjectContext {
        ProtocolObjectContext::signed_plaintext(
            self.store_root_hash(),
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        )
    }

    pub fn semantic_prefix(&self) -> Result<&str, StoreDeviceExclusionJournalError> {
        self.object()
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| {
                StoreDeviceExclusionJournalError::Invalid(
                    "exclusion exact object does not use its JSON semantic path".to_string(),
                )
            })
    }

    pub fn object(&self) -> &crate::objects::ExactObjectRef {
        self.reference.object()
    }

    pub fn semantic_bytes(&self) -> Vec<u8> {
        self.value.to_bytes()
    }

    pub(crate) fn remote_record(
        &self,
        candidate: &PreparedStoreOperationCommit,
    ) -> Result<crate::remote_object::ClosedRemoteObject, StoreDeviceExclusionJournalError> {
        RemoteObjectRecord::candidate_activated_device_exclusion_outcome(
            self.reference.clone(),
            &self.semantic_bytes(),
            self.prepared.stored_bytes(),
            candidate.reference.clone(),
        )
        .map_err(StoreDeviceExclusionJournalError::RemoteObject)
    }

    fn validate(&self) -> Result<(), StoreDeviceExclusionJournalError> {
        if self.prepared.reference() != self.object() {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "prepared exclusion object differs from its exact reference".to_string(),
            ));
        }
        if self.reference.proposal() != self.value.proposal()
            || self.reference.outcome_hash() != self.value.outcome_hash()
        {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion outcome differs from its exact reference".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionCompletion {
    ProposalActivated {
        proposal: StoreDeviceExclusionProposal,
        candidate: PreparedStoreOperationCommit,
    },
    OutcomeActivated {
        object: DurableStoreDeviceExclusionOutcome,
        candidate: PreparedStoreOperationCommit,
    },
    OutcomeSlotOccupied {
        intended: DurableStoreDeviceExclusionOutcome,
        winner: DurableStoreDeviceExclusionOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStoreDeviceExclusionOperation {
    /// A proposal has no exact object of its own: the Owner-signed membership
    /// entry its candidate publishes carries it.
    ProposalPrepared {
        proposal: StoreDeviceExclusionProposal,
        candidate: PreparedStoreOperationCommit,
    },
    OutcomePrepared {
        object: DurableStoreDeviceExclusionOutcome,
        candidate: PreparedStoreOperationCommit,
    },
    Completed(StoreDeviceExclusionCompletion),
}

impl DurableStoreDeviceExclusionOperation {
    pub fn prepared_proposal(
        proposal: StoreDeviceExclusionProposal,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<Self, StoreDeviceExclusionJournalError> {
        let operation = Self::ProposalPrepared {
            proposal,
            candidate,
        };
        operation.validate()?;
        Ok(operation)
    }

    pub fn prepared_outcome(
        object: DurableStoreDeviceExclusionOutcome,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<Self, StoreDeviceExclusionJournalError> {
        let operation = Self::OutcomePrepared { object, candidate };
        operation.validate()?;
        Ok(operation)
    }

    pub fn activated(&self) -> Result<Self, StoreDeviceExclusionJournalError> {
        self.validate()?;
        match self {
            Self::ProposalPrepared {
                proposal,
                candidate,
            } => Ok(Self::Completed(
                StoreDeviceExclusionCompletion::ProposalActivated {
                    proposal: proposal.clone(),
                    candidate: candidate.clone(),
                },
            )),
            Self::OutcomePrepared { object, candidate } => Ok(Self::Completed(
                StoreDeviceExclusionCompletion::OutcomeActivated {
                    object: object.clone(),
                    candidate: candidate.clone(),
                },
            )),
            Self::Completed(_) => Err(StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no pending activation candidate".into(),
            )),
        }
    }

    pub fn operation_id(&self) -> ObjectHash {
        match self {
            Self::ProposalPrepared { proposal, .. }
            | Self::Completed(StoreDeviceExclusionCompletion::ProposalActivated {
                proposal, ..
            }) => proposal_operation_id(proposal),
            Self::OutcomePrepared { object, .. }
            | Self::Completed(StoreDeviceExclusionCompletion::OutcomeActivated {
                object, ..
            }) => object.reference.outcome_hash(),
            Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended,
                ..
            }) => intended.reference.outcome_hash(),
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    pub fn allows_transition_to(&self, next: &Self) -> bool {
        let current_id = self.operation_id();
        let next_id = next.operation_id();
        if current_id != next_id {
            return false;
        }
        match (self, next) {
            (
                Self::ProposalPrepared {
                    proposal,
                    candidate,
                },
                Self::ProposalPrepared {
                    proposal: next_proposal,
                    candidate: next_candidate,
                },
            ) => {
                proposal == next_proposal
                    && candidate.reference == next_candidate.reference
                    && candidate.commit.to_bytes() == next_candidate.commit.to_bytes()
            }
            (
                Self::OutcomePrepared { object, candidate },
                Self::OutcomePrepared {
                    object: next_object,
                    candidate: next_candidate,
                },
            ) => {
                object == next_object
                    && candidate.reference == next_candidate.reference
                    && candidate.commit.to_bytes() == next_candidate.commit.to_bytes()
            }
            (
                Self::OutcomePrepared { .. },
                Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied { .. }),
            ) => true,
            (
                Self::ProposalPrepared {
                    proposal,
                    candidate,
                },
                Self::Completed(StoreDeviceExclusionCompletion::ProposalActivated {
                    proposal: next_proposal,
                    candidate: next_candidate,
                }),
            ) => {
                proposal == next_proposal
                    && candidate.has_same_durable_activation_as(next_candidate)
            }
            (
                Self::OutcomePrepared { object, candidate },
                Self::Completed(StoreDeviceExclusionCompletion::OutcomeActivated {
                    object: next_object,
                    candidate: next_candidate,
                }),
            ) => object == next_object && candidate.has_same_durable_activation_as(next_candidate),
            _ => false,
        }
    }

    /// The exact outcome object this operation holds, when it has one.
    pub fn outcome(&self) -> Option<&DurableStoreDeviceExclusionOutcome> {
        match self {
            Self::OutcomePrepared { object, .. }
            | Self::Completed(StoreDeviceExclusionCompletion::OutcomeActivated {
                object, ..
            })
            | Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended: object,
                ..
            }) => Some(object),
            Self::ProposalPrepared { .. }
            | Self::Completed(StoreDeviceExclusionCompletion::ProposalActivated { .. }) => None,
        }
    }

    pub fn candidate(&self) -> Option<&PreparedStoreOperationCommit> {
        match self {
            Self::ProposalPrepared { candidate, .. }
            | Self::OutcomePrepared { candidate, .. }
            | Self::Completed(StoreDeviceExclusionCompletion::ProposalActivated {
                candidate,
                ..
            })
            | Self::Completed(StoreDeviceExclusionCompletion::OutcomeActivated {
                candidate, ..
            }) => Some(candidate),
            Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied { .. }) => None,
        }
    }

    pub fn remote_objects(
        &self,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, StoreDeviceExclusionJournalError>
    {
        let candidate = self.candidate().ok_or_else(|| {
            StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no prepared activation candidate".to_string(),
            )
        })?;
        let authorities = match self.outcome() {
            Some(object) => vec![object.remote_record(candidate)?],
            None => Vec::new(),
        };
        candidate
            .retained_control_remote_objects(authorities)
            .map_err(StoreDeviceExclusionJournalError::Outbound)
    }

    pub fn authority_remote_object(
        &self,
    ) -> Result<crate::remote_object::ClosedRemoteObject, StoreDeviceExclusionJournalError> {
        let candidate = self.candidate().ok_or_else(|| {
            StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no authority owner candidate".to_string(),
            )
        })?;
        let object = self.outcome().ok_or_else(|| {
            StoreDeviceExclusionJournalError::Invalid(
                "proposal has no authority object".to_string(),
            )
        })?;
        object.remote_record(candidate)
    }

    pub fn validate(&self) -> Result<(), StoreDeviceExclusionJournalError> {
        if let Some(object) = self.outcome() {
            object.validate()?;
        }
        let Some(candidate) = self.candidate() else {
            if let Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended,
                winner,
            }) = self
            {
                winner.validate()?;
                if intended.object().slot() != winner.object().slot()
                    || intended.object() == winner.object()
                {
                    return Err(StoreDeviceExclusionJournalError::Invalid(
                        "occupied exclusion outcome slot lacks a distinct exact winner".to_string(),
                    ));
                }
            }
            return Ok(());
        };
        candidate.reference.verify_commit(&candidate.commit)?;
        let publication = candidate.prepared_membership_publication()?;
        let change_matches = match (self, &publication.entry.change) {
            (
                Self::ProposalPrepared { proposal, .. }
                | Self::Completed(StoreDeviceExclusionCompletion::ProposalActivated {
                    proposal, ..
                }),
                crate::membership::StoreAuthorityChange::DeviceExclusionProposal {
                    proposal: entry_proposal,
                },
            ) => {
                proposal == entry_proposal
                    && candidate.commit.device_exclusion_outcomes().is_empty()
            }
            (
                Self::OutcomePrepared { object, .. }
                | Self::Completed(StoreDeviceExclusionCompletion::OutcomeActivated {
                    object, ..
                }),
                crate::membership::StoreAuthorityChange::DeviceExclusionOutcome { outcome },
            ) => {
                &object.reference == outcome
                    && candidate.commit.device_exclusion_outcomes() == [object.reference.clone()]
            }
            _ => false,
        };
        if !change_matches || candidate.commit.acknowledgement().is_some() {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion journal candidate does not activate its one exact object".to_string(),
            ));
        }
        Ok(())
    }
}

/// A proposal's operation id is the digest of its canonical JSON: it has no
/// exact object whose hash could name it.
fn proposal_operation_id(proposal: &StoreDeviceExclusionProposal) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(proposal)
            .expect("Store device exclusion proposal serialization cannot fail"),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum StoreDeviceExclusionJournalError {
    #[error("invalid durable Store-device exclusion: {0}")]
    Invalid(String),
    #[error("Store-device exclusion protocol: {0}")]
    Protocol(#[from] crate::store_commit::StoreProtocolError),
    #[error("Store-device exclusion remote ownership: {0}")]
    RemoteObject(#[from] RemoteObjectRecordError),
    #[error("Store-device exclusion activation: {0}")]
    Outbound(#[from] crate::prepared_commit::PreparedCommitError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] crate::objects::StorageError),
}

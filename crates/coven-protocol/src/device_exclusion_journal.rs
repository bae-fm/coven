//! Durable Store-device exclusion state: the exact proposal/outcome objects,
//! prepared candidates, and completion outcomes one exclusion operation
//! persists, validated against the slots and commits they bind.

use serde::{Deserialize, Serialize};

use crate::objects::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use crate::prepared_commit::PreparedStoreOperationCommit;
use crate::remote_object::{RemoteObjectRecord, RemoteObjectRecordError};
use crate::store_commit::{
    ObjectHash, StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStoreDeviceExclusionObject {
    Proposal {
        reference: StoreDeviceExclusionProposalRef,
        value: StoreDeviceExclusionProposal,
        prepared: PreparedExactObject,
    },
    Outcome {
        reference: StoreDeviceExclusionOutcomeRef,
        value: StoreDeviceExclusionOutcome,
        prepared: PreparedExactObject,
    },
}

impl DurableStoreDeviceExclusionObject {
    fn store_root_hash(&self) -> ObjectHash {
        match self {
            Self::Proposal { value, .. } => value.store_root_hash,
            Self::Outcome { value, .. } => match value {
                StoreDeviceExclusionOutcome::Excluded(value) => value.store_root_hash,
                StoreDeviceExclusionOutcome::Cancelled(value) => value.store_root_hash,
            },
        }
    }

    pub fn context(&self) -> ProtocolObjectContext {
        let domain = match self {
            Self::Proposal { .. } => ProtocolObjectDomain::StoreDeviceExclusionProposal,
            Self::Outcome { .. } => ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        };
        ProtocolObjectContext::signed_plaintext(self.store_root_hash(), domain)
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

    pub fn operation_id(&self) -> ObjectHash {
        match self {
            Self::Proposal { reference, .. } => reference.proposal_hash,
            Self::Outcome { reference, .. } => reference.outcome_hash(),
        }
    }

    pub fn object(&self) -> &crate::objects::ExactObjectRef {
        match self {
            Self::Proposal { reference, .. } => &reference.object,
            Self::Outcome { reference, .. } => reference.object(),
        }
    }

    pub fn prepared(&self) -> &PreparedExactObject {
        match self {
            Self::Proposal { prepared, .. } | Self::Outcome { prepared, .. } => prepared,
        }
    }

    pub fn semantic_bytes(&self) -> Vec<u8> {
        match self {
            Self::Proposal { value, .. } => value.to_bytes(),
            Self::Outcome { value, .. } => value.to_bytes(),
        }
    }

    fn commit_names_exact_object(&self, candidate: &PreparedStoreOperationCommit) -> bool {
        match self {
            Self::Proposal { reference, .. } => {
                candidate.commit.device_exclusion_proposals() == [reference.clone()]
                    && candidate.commit.device_exclusion_outcomes().is_empty()
            }
            Self::Outcome { reference, .. } => {
                candidate.commit.device_exclusion_proposals().is_empty()
                    && candidate.commit.device_exclusion_outcomes() == [reference.clone()]
            }
        }
    }

    pub(crate) fn remote_record(
        &self,
        candidate: &PreparedStoreOperationCommit,
    ) -> Result<crate::remote_object::ClosedRemoteObject, StoreDeviceExclusionJournalError> {
        let bytes = self.semantic_bytes();
        let stored = self.prepared().stored_bytes();
        match self {
            Self::Proposal { reference, .. } => {
                RemoteObjectRecord::candidate_activated_device_exclusion_proposal(
                    reference.clone(),
                    &bytes,
                    stored,
                    candidate.reference.clone(),
                )
            }
            Self::Outcome { reference, .. } => {
                RemoteObjectRecord::candidate_activated_device_exclusion_outcome(
                    reference.clone(),
                    &bytes,
                    stored,
                    candidate.reference.clone(),
                )
            }
        }
        .map_err(StoreDeviceExclusionJournalError::RemoteObject)
    }

    fn validate(&self) -> Result<(), StoreDeviceExclusionJournalError> {
        if self.prepared().reference() != self.object() {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "prepared exclusion object differs from its exact reference".to_string(),
            ));
        }
        match self {
            Self::Proposal {
                reference, value, ..
            } => reference.verify_proposal(value)?,
            Self::Outcome {
                reference, value, ..
            } => {
                if reference.proposal() != value.proposal()
                    || reference.outcome_hash() != value.outcome_hash()
                {
                    return Err(StoreDeviceExclusionJournalError::Invalid(
                        "exclusion outcome differs from its exact reference".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionCompletion {
    Activated {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    },
    OutcomeSlotOccupied {
        intended: DurableStoreDeviceExclusionObject,
        winner: DurableStoreDeviceExclusionObject,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStoreDeviceExclusionOperation {
    CandidatePrepared {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    },
    Completed(StoreDeviceExclusionCompletion),
}

impl DurableStoreDeviceExclusionOperation {
    pub fn prepared(
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<Self, StoreDeviceExclusionJournalError> {
        let operation = Self::CandidatePrepared { object, candidate };
        operation.validate()?;
        Ok(operation)
    }

    pub fn activated(&self) -> Result<Self, StoreDeviceExclusionJournalError> {
        self.validate()?;
        let Self::CandidatePrepared { object, candidate } = self else {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no pending activation candidate".into(),
            ));
        };
        Ok(Self::Completed(StoreDeviceExclusionCompletion::Activated {
            object: object.clone(),
            candidate: candidate.clone(),
        }))
    }

    pub fn operation_id(&self) -> ObjectHash {
        self.object().operation_id()
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
                Self::CandidatePrepared { object, candidate },
                Self::CandidatePrepared {
                    object: next_object,
                    candidate: next_candidate,
                },
            ) => {
                object == next_object
                    && candidate.reference == next_candidate.reference
                    && candidate.commit.to_bytes() == next_candidate.commit.to_bytes()
            }
            (
                Self::CandidatePrepared { .. },
                Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied { .. }),
            ) => true,
            (
                Self::CandidatePrepared { object, candidate },
                Self::Completed(StoreDeviceExclusionCompletion::Activated {
                    object: next_object,
                    candidate: next_candidate,
                }),
            ) => object == next_object && candidate.has_same_durable_activation_as(next_candidate),
            _ => false,
        }
    }

    pub fn object(&self) -> &DurableStoreDeviceExclusionObject {
        match self {
            Self::CandidatePrepared { object, .. } => object,
            Self::Completed(StoreDeviceExclusionCompletion::Activated { object, .. }) => object,
            Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended,
                ..
            }) => intended,
        }
    }

    pub fn candidate(&self) -> Option<&PreparedStoreOperationCommit> {
        match self {
            Self::CandidatePrepared { candidate, .. } => Some(candidate),
            Self::Completed(StoreDeviceExclusionCompletion::Activated { candidate, .. }) => {
                Some(candidate)
            }
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
        let authority = self.object().remote_record(candidate)?;
        candidate
            .retained_control_remote_objects(vec![authority])
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
        self.object().remote_record(candidate)
    }

    pub fn validate(&self) -> Result<(), StoreDeviceExclusionJournalError> {
        self.object().validate()?;
        let Some(candidate) = self.candidate() else {
            if let Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended,
                winner,
            }) = self
            {
                winner.validate()?;
                if !matches!(intended, DurableStoreDeviceExclusionObject::Outcome { .. })
                    || !matches!(winner, DurableStoreDeviceExclusionObject::Outcome { .. })
                    || intended.object().slot() != winner.object().slot()
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
        let change_matches = match (self.object(), &publication.entry.change) {
            (
                DurableStoreDeviceExclusionObject::Proposal { reference, .. },
                crate::membership::StoreAuthorityChange::DeviceExclusionProposal { proposal },
            ) => reference == proposal,
            (
                DurableStoreDeviceExclusionObject::Outcome { reference, .. },
                crate::membership::StoreAuthorityChange::DeviceExclusionOutcome { outcome },
            ) => reference == outcome,
            _ => false,
        };
        if !change_matches
            || !self.object().commit_names_exact_object(candidate)
            || candidate.commit.acknowledgement().is_some()
        {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion journal candidate does not activate its one exact object".to_string(),
            ));
        }
        Ok(())
    }
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

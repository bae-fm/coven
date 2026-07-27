//! Durable publication of Store-device exclusion proposals and outcomes.

use serde::{Deserialize, Serialize};

use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::sync::membership::MembershipChain;

use super::database::StoreDatabase;
use super::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use super::{Store, StoreError};
use crate::sync::remote_object::{
    CandidateNonactivation, CandidateNonactivationProof, RemoteObjectRecord,
    RemoteObjectRecordError,
};
use crate::sync::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store_commit::{
    device_exclusion_outcome_semantic_prefix, device_exclusion_proposal_semantic_prefix,
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusion, StoreDeviceExclusionCancellation,
    StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProof,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalId, StoreDeviceExclusionProposalRef,
    StoreDeviceProposalState, StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DurableStoreDeviceExclusionObject {
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

    fn context(&self) -> ProtocolObjectContext {
        let domain = match self {
            Self::Proposal { .. } => ProtocolObjectDomain::StoreDeviceExclusionProposal,
            Self::Outcome { .. } => ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        };
        ProtocolObjectContext::signed_plaintext(self.store_root_hash(), domain)
    }

    fn semantic_prefix(&self) -> Result<&str, StoreDeviceExclusionJournalError> {
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

    pub(crate) fn operation_id(&self) -> ObjectHash {
        match self {
            Self::Proposal { reference, .. } => reference.proposal_hash,
            Self::Outcome { reference, .. } => reference.outcome_hash(),
        }
    }

    pub(crate) fn object(&self) -> &crate::sync::storage::ExactObjectRef {
        match self {
            Self::Proposal { reference, .. } => &reference.object,
            Self::Outcome { reference, .. } => reference.object(),
        }
    }

    pub(crate) fn prepared(&self) -> &PreparedExactObject {
        match self {
            Self::Proposal { prepared, .. } | Self::Outcome { prepared, .. } => prepared,
        }
    }

    pub(crate) fn semantic_bytes(&self) -> Vec<u8> {
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
    ) -> Result<RemoteObjectRecord, StoreDeviceExclusionJournalError> {
        let bytes = self.semantic_bytes();
        let stored = self.prepared().stored_bytes().to_vec();
        match self {
            Self::Proposal { reference, .. } => {
                RemoteObjectRecord::candidate_activated_device_exclusion_proposal(
                    reference.clone(),
                    bytes,
                    stored,
                    candidate.reference.clone(),
                )
            }
            Self::Outcome { reference, .. } => {
                RemoteObjectRecord::candidate_activated_device_exclusion_outcome(
                    reference.clone(),
                    bytes,
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
            } => reference
                .verify_proposal(value)
                .map_err(|error| StoreDeviceExclusionJournalError::Invalid(error.to_string()))?,
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
pub(crate) enum StoreDeviceExclusionCompletion {
    Activated {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    },
    OutcomeSlotOccupied {
        intended: DurableStoreDeviceExclusionObject,
        winner: DurableStoreDeviceExclusionObject,
    },
    CandidateNonactivated {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
        proof: CandidateNonactivationProof,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DurableStoreDeviceExclusionOperation {
    CandidatePrepared {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    },
    CandidateNonactivating {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
        proof: CandidateNonactivationProof,
    },
    ReplacingCandidate {
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
        losing: StoreDeviceExclusionCandidateLoss,
    },
    Completed(StoreDeviceExclusionCompletion),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreDeviceExclusionCandidateLoss {
    pub(crate) candidate: PreparedStoreOperationCommit,
    pub(crate) proof: CandidateNonactivationProof,
}

impl DurableStoreDeviceExclusionOperation {
    pub(crate) fn prepared(
        object: DurableStoreDeviceExclusionObject,
        candidate: PreparedStoreOperationCommit,
    ) -> Result<Self, StoreDeviceExclusionJournalError> {
        let operation = Self::CandidatePrepared { object, candidate };
        operation.validate()?;
        Ok(operation)
    }

    pub(crate) fn operation_id(&self) -> ObjectHash {
        self.object().operation_id()
    }

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    pub(crate) fn allows_transition_to(&self, next: &Self) -> bool {
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
            (Self::CandidatePrepared { .. }, Self::CandidateNonactivating { .. }) => true,
            (Self::CandidatePrepared { .. }, Self::ReplacingCandidate { .. }) => true,
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
            (
                Self::ReplacingCandidate {
                    object, candidate, ..
                },
                Self::CandidatePrepared {
                    object: next_object,
                    candidate: next_candidate,
                },
            ) => object == next_object && candidate.has_same_durable_activation_as(next_candidate),
            (
                Self::CandidateNonactivating {
                    object,
                    candidate,
                    proof,
                },
                Self::Completed(StoreDeviceExclusionCompletion::CandidateNonactivated {
                    object: next_object,
                    candidate: next_candidate,
                    proof: next_proof,
                }),
            ) => {
                object == next_object
                    && candidate.has_same_durable_activation_as(next_candidate)
                    && proof == next_proof
            }
            _ => false,
        }
    }

    pub(crate) fn object(&self) -> &DurableStoreDeviceExclusionObject {
        match self {
            Self::CandidatePrepared { object, .. }
            | Self::CandidateNonactivating { object, .. }
            | Self::ReplacingCandidate { object, .. } => object,
            Self::Completed(StoreDeviceExclusionCompletion::Activated { object, .. }) => object,
            Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied {
                intended,
                ..
            }) => intended,
            Self::Completed(StoreDeviceExclusionCompletion::CandidateNonactivated {
                object,
                ..
            }) => object,
        }
    }

    pub(crate) fn candidate(&self) -> Option<&PreparedStoreOperationCommit> {
        match self {
            Self::CandidatePrepared { candidate, .. }
            | Self::CandidateNonactivating { candidate, .. }
            | Self::ReplacingCandidate { candidate, .. } => Some(candidate),
            Self::Completed(StoreDeviceExclusionCompletion::Activated { candidate, .. }) => {
                Some(candidate)
            }
            Self::Completed(StoreDeviceExclusionCompletion::CandidateNonactivated {
                candidate,
                ..
            }) => Some(candidate),
            Self::Completed(StoreDeviceExclusionCompletion::OutcomeSlotOccupied { .. }) => None,
        }
    }

    pub(crate) fn remote_objects(
        &self,
    ) -> Result<Vec<RemoteObjectRecord>, StoreDeviceExclusionJournalError> {
        let candidate = self.candidate().ok_or_else(|| {
            StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no prepared activation candidate".to_string(),
            )
        })?;
        let authority = self.object().remote_record(candidate)?;
        candidate
            .retained_authority_remote_objects(vec![authority])
            .map_err(StoreDeviceExclusionJournalError::Outbound)
    }

    pub(crate) fn authority_remote_object(
        &self,
    ) -> Result<RemoteObjectRecord, StoreDeviceExclusionJournalError> {
        let candidate = self.candidate().ok_or_else(|| {
            StoreDeviceExclusionJournalError::Invalid(
                "Store-device exclusion has no authority owner candidate".to_string(),
            )
        })?;
        self.object().remote_record(candidate)
    }

    pub(crate) fn begin_nonactivation(
        &self,
        nonactivation: CandidateNonactivation,
    ) -> Result<(Self, CandidateNonactivation), StoreDeviceExclusionJournalError> {
        let Self::CandidatePrepared { object, candidate } = self else {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "only a prepared exclusion candidate can become nonactivating".to_string(),
            ));
        };
        if nonactivation
            .reference()
            .map_err(StoreDeviceExclusionJournalError::RemoteObject)?
            != candidate.reference
        {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion nonactivation names another candidate".to_string(),
            ));
        }
        let proof = nonactivation.proof().clone();
        let operation = Self::CandidateNonactivating {
            object: object.clone(),
            candidate: candidate.clone(),
            proof,
        };
        operation.validate()?;
        Ok((operation, nonactivation))
    }

    pub(crate) fn begin_replacement(
        &self,
        replacement: PreparedStoreOperationCommit,
        nonactivation: CandidateNonactivation,
    ) -> Result<(Self, CandidateNonactivation), StoreDeviceExclusionJournalError> {
        let Self::CandidatePrepared { object, candidate } = self else {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "only a prepared exclusion candidate can be replaced".to_string(),
            ));
        };
        if !matches!(object, DurableStoreDeviceExclusionObject::Outcome { .. }) {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "an exclusion proposal cannot move to another predecessor".to_string(),
            ));
        }
        if nonactivation
            .reference()
            .map_err(StoreDeviceExclusionJournalError::RemoteObject)?
            != candidate.reference
        {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "replacement exclusion nonactivation names another candidate".to_string(),
            ));
        }
        let proof = nonactivation.proof().clone();
        let operation = Self::ReplacingCandidate {
            object: object.clone(),
            candidate: replacement,
            losing: StoreDeviceExclusionCandidateLoss {
                candidate: candidate.clone(),
                proof,
            },
        };
        operation.validate()?;
        Ok((operation, nonactivation))
    }

    pub(crate) fn validate(&self) -> Result<(), StoreDeviceExclusionJournalError> {
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
        candidate
            .reference
            .verify_commit(&candidate.commit)
            .map_err(|error| StoreDeviceExclusionJournalError::Invalid(error.to_string()))?;
        if !self.object().commit_names_exact_object(candidate)
            || candidate.commit.acknowledgement().is_some()
        {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion journal candidate does not activate its one exact object".to_string(),
            ));
        }
        if let Self::CandidateNonactivating {
            candidate, proof, ..
        }
        | Self::Completed(StoreDeviceExclusionCompletion::CandidateNonactivated {
            candidate,
            proof,
            ..
        }) = self
        {
            CandidateNonactivation::validate_durable_shape(
                &candidate.reference,
                &candidate.commit,
                proof.clone(),
            )
            .map_err(StoreDeviceExclusionJournalError::RemoteObject)?;
        }
        if let Self::ReplacingCandidate {
            object,
            candidate,
            losing,
        } = self
        {
            if !matches!(object, DurableStoreDeviceExclusionObject::Outcome { .. })
                || candidate.reference == losing.candidate.reference
                || !object.commit_names_exact_object(candidate)
                || !object.commit_names_exact_object(&losing.candidate)
            {
                return Err(StoreDeviceExclusionJournalError::Invalid(
                    "replacement exclusion candidate changes its exact outcome".to_string(),
                ));
            }
            CandidateNonactivation::validate_durable_shape(
                &losing.candidate.reference,
                &losing.candidate.commit,
                losing.proof.clone(),
            )
            .map_err(StoreDeviceExclusionJournalError::RemoteObject)?;
        }
        Ok(())
    }

    pub(crate) async fn create_exact_object(
        &self,
        storage: &dyn SyncStorage,
    ) -> Result<(), StoreDeviceExclusionJournalError> {
        storage
            .create_protocol_object(self.object().prepared())
            .await
            .map_err(StoreDeviceExclusionJournalError::Storage)?;
        let context = self.object().context();
        let prefix = self.object().semantic_prefix()?;
        let opened = storage
            .read_protocol_object(&context, self.object().object(), prefix)
            .await
            .map_err(StoreDeviceExclusionJournalError::Storage)?;
        if opened != self.object().semantic_bytes() {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "exclusion exact readback differs from its signed bytes".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreDeviceExclusionResult {
    ProposalActivated {
        proposal: StoreDeviceExclusionProposalRef,
        commit: StoreBatchCommitRef,
    },
    OutcomeActivated {
        outcome: StoreDeviceExclusionOutcomeRef,
        commit: StoreBatchCommitRef,
    },
    OutcomeSlotOccupied {
        intended: StoreDeviceExclusionOutcomeRef,
        winner: StoreDeviceExclusionOutcomeRef,
    },
    CandidateNonactivated {
        object_hash: ObjectHash,
        candidate: StoreBatchCommitRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreDeviceExclusionOperationInfo {
    pub operation_id: ObjectHash,
    pub status: StoreDeviceExclusionOperationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreDeviceExclusionOperationStatus {
    Pending,
    Completed(StoreDeviceExclusionResult),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreDeviceExclusionError {
    #[error("Store-device exclusion operation {0} remains active")]
    OperationActive(ObjectHash),
    #[error("the local Store device has no active Owner authority")]
    OwnerAuthorityRequired,
    #[error("the target Store device is not active at the exact predecessor state")]
    TargetNotActive,
    #[error("the active Owner device cannot exclude its own registration")]
    CannotExcludeLocalDevice,
    #[error("Store-device exclusion database state: {0}")]
    Database(#[from] DbError),
    #[error("Store-device exclusion object: {0}")]
    Object(#[from] crate::sync::store_objects::StoreObjectError),
    #[error("Store-device exclusion protocol: {0}")]
    Protocol(#[from] StoreProtocolError),
    #[error("Store-device exclusion publication: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] crate::sync::storage::StorageError),
    #[error("Store-device exclusion journal: {0}")]
    Journal(String),
    #[error("Store-device exclusion state is invalid: {0}")]
    InvalidState(String),
}

impl Store {
    pub(crate) async fn propose_device_exclusion(
        &self,
        identity_signer: &UserKeypair,
        target: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        propose_device_exclusion(self.database(), &**self.storage(), identity_signer, target).await
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        identity_signer: &UserKeypair,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        cancel_device_exclusion(
            self.database(),
            &**self.storage(),
            identity_signer,
            proposal,
        )
        .await
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        identity_signer: &UserKeypair,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        finalize_device_exclusion(
            self.database(),
            &**self.storage(),
            identity_signer,
            proposal,
        )
        .await
    }

    pub(crate) async fn resume_device_exclusion(
        &self,
        identity_signer: &UserKeypair,
    ) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
        resume_device_exclusion(self.database(), &**self.storage(), identity_signer).await
    }

    pub(crate) async fn device_exclusion_operations(
        &self,
    ) -> Result<Vec<StoreDeviceExclusionOperationInfo>, StoreDeviceExclusionError> {
        get_device_exclusion_operations(self.database()).await
    }
}

pub(crate) async fn propose_device_exclusion(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    target: &crate::sync::store_commit::StoreDeviceRegistrationRef,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    let _lock = database.lock_device_exclusion().await;
    reject_active_operation(database).await?;
    let durable = Box::pin(prepare_proposal(database, storage, identity_signer, target)).await?;
    drive_device_exclusion(database, storage, identity_signer, Box::new(durable)).await
}

async fn prepare_proposal(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    target: &crate::sync::store_commit::StoreDeviceRegistrationRef,
) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
    let db = database.sqlite();
    let device_id = local_device_id(db).await?;
    let authorization = Box::new(
        Box::pin(super::device_join::load_current_device_join_authorization(
            database, storage,
        ))
        .await
        .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?,
    );
    let plan = Box::new(
        Box::pin(super::operations::prepare_plan(
            database,
            storage,
            &authorization,
            &device_id,
            identity_signer,
        ))
        .await?,
    );
    if plan.registration_ref() == target {
        return Err(StoreDeviceExclusionError::CannotExcludeLocalDevice);
    }
    let target_registration = database
        .activated_store_device_registration(target.clone())
        .await?;
    let state = Box::new(
        database
            .resolved_store_device_state(plan.device_state())
            .await?,
    );
    require_active_target(&state, target)?;
    let owner_grant = plan
        .owner_grant()
        .cloned()
        .ok_or(StoreDeviceExclusionError::OwnerAuthorityRequired)?;
    let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
        db.new_write_id().as_str().as_bytes(),
    ));
    let outcome_prefix = device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id);
    let outcome_context = ProtocolObjectContext::signed_plaintext(
        plan.root().store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionOutcome,
    );
    let outcome_slot = storage
        .allocate_protocol_slot(&outcome_context, &outcome_prefix, ".json")
        .await?;
    let proposal = StoreDeviceExclusionProposal::signed(
        plan.root().store_root_hash,
        proposal_id,
        target.clone(),
        &target_registration,
        plan.device_state().clone(),
        outcome_slot,
        plan.registration_ref().clone(),
        owner_grant,
        plan.registration(),
        plan.device_signer(),
    )?;
    let proposal_prefix = device_exclusion_proposal_semantic_prefix(
        target.device_id,
        proposal_id,
        proposal.proposal_hash(),
    );
    let proposal_context = ProtocolObjectContext::signed_plaintext(
        plan.root().store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionProposal,
    );
    let proposal_slot = storage
        .allocate_protocol_slot(&proposal_context, &proposal_prefix, ".json")
        .await?;
    let prepared = storage.prepare_protocol_object(
        &proposal_context,
        proposal_slot,
        &proposal_prefix,
        proposal.to_bytes(),
    )?;
    let reference =
        StoreDeviceExclusionProposalRef::from_proposal(&proposal, prepared.reference().clone())?;
    let retained = crate::sync::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
        reference.clone(),
        &proposal,
        &target_registration,
        plan.registration(),
    )?;
    let candidate = Box::pin(super::operations::prepare_candidate(
        database,
        storage,
        *plan,
        StoreOperationBatch::DeviceExclusionProposal(retained),
    ))
    .await?;
    let operation = DurableStoreDeviceExclusionOperation::prepared(
        DurableStoreDeviceExclusionObject::Proposal {
            reference,
            value: proposal,
            prepared,
        },
        candidate,
    )?;
    let durable = Box::pin(database.begin_outbound_store_device_exclusion(operation)).await?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged)
        .await;
    Ok(durable)
}

fn cancel_device_exclusion<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    identity_signer: &'a UserKeypair,
    proposal: &'a StoreDeviceExclusionProposalRef,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<StoreDeviceExclusionResult, StoreDeviceExclusionError>,
            > + Send
            + 'a,
    >,
> {
    publish_outcome(
        database,
        storage,
        identity_signer,
        proposal,
        OutcomeIntent::Cancel,
    )
}

pub(crate) fn finalize_device_exclusion<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    identity_signer: &'a UserKeypair,
    proposal: &'a StoreDeviceExclusionProposalRef,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<StoreDeviceExclusionResult, StoreDeviceExclusionError>,
            > + Send
            + 'a,
    >,
> {
    publish_outcome(
        database,
        storage,
        identity_signer,
        proposal,
        OutcomeIntent::Exclude,
    )
}

async fn resume_device_exclusion(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    let _lock = database.lock_device_exclusion().await;
    let Some(operation) = database.active_outbound_store_device_exclusion().await? else {
        return Ok(None);
    };
    drive_device_exclusion(database, storage, identity_signer, Box::new(operation))
        .await
        .map(Some)
}

async fn get_device_exclusion_operations(
    database: &StoreDatabase,
) -> Result<Vec<StoreDeviceExclusionOperationInfo>, StoreDeviceExclusionError> {
    database
        .outbound_store_device_exclusion_operations()
        .await?
        .into_iter()
        .map(|operation| {
            let operation_id = operation.operation_id();
            let status = if operation.is_completed() {
                StoreDeviceExclusionOperationStatus::Completed(completion_result(&operation)?)
            } else {
                StoreDeviceExclusionOperationStatus::Pending
            };
            Ok(StoreDeviceExclusionOperationInfo {
                operation_id,
                status,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum OutcomeIntent {
    Exclude,
    Cancel,
}

fn publish_outcome<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    identity_signer: &'a UserKeypair,
    proposal_ref: &'a StoreDeviceExclusionProposalRef,
    intent: OutcomeIntent,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<StoreDeviceExclusionResult, StoreDeviceExclusionError>,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let _lock = database.lock_device_exclusion().await;
        reject_active_operation(database).await?;
        let authorization = Box::pin(super::device_join::load_current_device_join_authorization(
            database, storage,
        ))
        .await
        .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
        let durable = Box::pin(prepare_outcome(
            database,
            storage,
            identity_signer,
            proposal_ref,
            intent,
            authorization,
        ))
        .await?;
        drive_device_exclusion(database, storage, identity_signer, Box::new(durable)).await
    })
}

async fn prepare_outcome(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    proposal_ref: &StoreDeviceExclusionProposalRef,
    intent: OutcomeIntent,
    authorization: MembershipChain,
) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
    let db = database.sqlite();
    let device_id = local_device_id(db).await?;
    let plan = Box::pin(super::operations::prepare_plan(
        database,
        storage,
        &authorization,
        &device_id,
        identity_signer,
    ))
    .await?;
    let owner_grant = plan
        .owner_grant()
        .cloned()
        .ok_or(StoreDeviceExclusionError::OwnerAuthorityRequired)?;
    let proposal = crate::sync::store_objects::load_device_exclusion_proposal_ref(
        storage,
        plan.root(),
        proposal_ref,
    )
    .await?;
    let state = database
        .resolved_store_device_state(plan.device_state())
        .await?;
    require_pending_proposal(&state, proposal_ref)?;
    let outcome = match intent {
        OutcomeIntent::Cancel => {
            StoreDeviceExclusionOutcome::Cancelled(StoreDeviceExclusionCancellation::signed(
                proposal_ref.clone(),
                &proposal.object.value,
                plan.registration_ref().clone(),
                owner_grant,
                plan.registration(),
                plan.device_signer(),
            )?)
        }
        OutcomeIntent::Exclude => {
            let proof = build_exclusion_proof(
                database,
                storage,
                plan.root(),
                proposal_ref,
                &proposal.object.value,
            )
            .await?;
            StoreDeviceExclusionOutcome::Excluded(StoreDeviceExclusion::signed(
                proposal_ref.clone(),
                &proposal.object.value,
                proposal_ref.target.clone(),
                &proposal.target,
                proof,
                plan.registration_ref().clone(),
                owner_grant,
                plan.registration(),
                plan.device_signer(),
            )?)
        }
    };
    let prefix = device_exclusion_outcome_semantic_prefix(
        proposal_ref.target.device_id,
        proposal_ref.proposal_id,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        plan.root().store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionOutcome,
    );
    let prepared = storage.prepare_protocol_object(
        &context,
        proposal.object.value.outcome_slot.clone(),
        &prefix,
        outcome.to_bytes(),
    )?;
    let reference = StoreDeviceExclusionOutcomeRef::from_outcome(
        &outcome,
        &proposal.object.value,
        prepared.reference().clone(),
    )?;
    let retained_proposal =
        crate::sync::store_commit::RetainedStoreDeviceExclusionProposal::from_verified(&proposal);
    let retained = crate::sync::store_commit::RetainedStoreDeviceExclusionOutcome::from_exact(
        &reference,
        retained_proposal,
        &outcome,
        plan.registration(),
    )?;
    let candidate = Box::pin(super::operations::prepare_candidate(
        database,
        storage,
        plan,
        StoreOperationBatch::DeviceExclusionOutcome(retained),
    ))
    .await?;
    let operation = DurableStoreDeviceExclusionOperation::prepared(
        DurableStoreDeviceExclusionObject::Outcome {
            reference,
            value: outcome,
            prepared,
        },
        candidate,
    )?;
    let durable = Box::pin(database.begin_outbound_store_device_exclusion(operation)).await?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged)
        .await;
    Ok(durable)
}

fn drive_device_exclusion<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    identity_signer: &'a UserKeypair,
    mut operation: Box<DurableStoreDeviceExclusionOperation>,
) -> impl std::future::Future<Output = Result<StoreDeviceExclusionResult, StoreDeviceExclusionError>>
       + Send
       + 'a {
    let future = async move {
        loop {
            if let Some(result) = Box::pin(resume_device_exclusion_candidate(
                database,
                storage,
                &mut operation,
            ))
            .await?
            {
                return Ok(result);
            }
            if let Some(result) = Box::pin(ensure_device_exclusion_authority_uploaded(
                database,
                storage,
                operation.as_ref(),
            ))
            .await?
            {
                return Ok(result);
            }
            let progress = Box::pin(publish_device_exclusion_candidate(
                database,
                storage,
                &mut operation,
            ))
            .await?;
            match progress {
                DeviceExclusionPublicationProgress::Completed(result) => return Ok(result),
                DeviceExclusionPublicationProgress::Continue => {}
                DeviceExclusionPublicationProgress::ReplacementRequired(proof) => {
                    Box::pin(replace_device_exclusion_candidate(
                        database,
                        storage,
                        identity_signer,
                        &mut operation,
                        proof,
                    ))
                    .await?;
                }
            }
        }
    };
    Box::pin(future)
}

enum DeviceExclusionPublicationProgress {
    Completed(StoreDeviceExclusionResult),
    Continue,
    ReplacementRequired(crate::sync::remote_object::VerifiedCandidateNonactivation),
}

async fn publish_device_exclusion_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
) -> Result<DeviceExclusionPublicationProgress, StoreDeviceExclusionError> {
    let candidate = operation.candidate().cloned().ok_or_else(|| {
        StoreDeviceExclusionError::InvalidState(
            "active exclusion operation has no activation candidate".to_string(),
        )
    })?;
    // Scoped to the publication alone: the arms below re-derive a plan, which
    // takes this same turn.
    let publication = Box::new({
        let _authorship = database.author_own_stream().await;
        let publish = super::operations::publish_prepared_store_operation(
            database,
            storage,
            Box::new(candidate),
        );
        Box::pin(publish).await?
    });
    match publication.as_ref() {
        StoreOperationPublicationOutcome::Activated(_) => {
            **operation = Box::pin(
                database.complete_outbound_store_device_exclusion_activation(
                    operation.as_ref().clone(),
                ),
            )
            .await?;
            completion_result(operation.as_ref()).map(DeviceExclusionPublicationProgress::Completed)
        }
        StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
            **operation = Box::pin(database.replace_outbound_store_device_exclusion_candidate(
                operation.as_ref().clone(),
                candidate.as_ref().clone(),
            ))
            .await?;
            Ok(DeviceExclusionPublicationProgress::Continue)
        }
        StoreOperationPublicationOutcome::NonactivatedCandidate {
            candidate,
            nonactivation,
        } => {
            if operation.candidate() != Some(candidate.as_ref()) {
                return Err(StoreDeviceExclusionError::InvalidState(
                    "publication conflict names another exclusion candidate".to_string(),
                ));
            }
            if matches!(
                operation.object(),
                DurableStoreDeviceExclusionObject::Outcome { .. }
            ) {
                return Ok(DeviceExclusionPublicationProgress::ReplacementRequired(
                    nonactivation.as_ref().clone(),
                ));
            } else {
                **operation = Box::pin(
                    database.begin_outbound_store_device_exclusion_nonactivation(
                        operation.as_ref().clone(),
                        nonactivation.as_ref().clone(),
                    ),
                )
                .await?;
            }
            Ok(DeviceExclusionPublicationProgress::Continue)
        }
        StoreOperationPublicationOutcome::Reprepared
        | StoreOperationPublicationOutcome::Nonactivated(_) => {
            Err(StoreDeviceExclusionError::InvalidState(
                "exclusion publication entered acknowledgement-only conflict state".to_string(),
            ))
        }
    }
}

async fn replace_device_exclusion_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
    nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
) -> Result<(), StoreDeviceExclusionError> {
    let replacement = Box::pin(prepare_replacement_candidate(
        database,
        storage,
        identity_signer,
        operation.object(),
    ))
    .await?;
    **operation = Box::pin(database.begin_outbound_store_device_exclusion_replacement(
        operation.as_ref().clone(),
        replacement,
        nonactivation,
    ))
    .await?;
    Ok(())
}

async fn ensure_device_exclusion_authority_uploaded(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation: &DurableStoreDeviceExclusionOperation,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    match Box::pin(operation.create_exact_object(storage)).await {
        Ok(()) => {}
        Err(StoreDeviceExclusionJournalError::Storage(
            crate::sync::storage::StorageError::SlotCollision(_),
        )) => {
            if let Some(completed) = Box::pin(resolve_exclusion_object_collision(
                database,
                storage,
                operation.clone(),
            ))
            .await?
            {
                return completion_result(&completed).map(Some);
            }
        }
        Err(error) => return Err(error.into()),
    }
    Box::pin(database.mark_store_device_exclusion_authority_uploaded(operation.clone())).await?;
    Ok(None)
}

async fn resume_device_exclusion_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    match operation.as_ref() {
        DurableStoreDeviceExclusionOperation::CandidateNonactivating { .. } => {
            for target in Box::pin(
                database.nonactivating_store_device_exclusion_cleanup_targets(
                    operation.as_ref().clone(),
                ),
            )
            .await?
            {
                crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
                database
                    .mark_candidate_cleanup_absent(target.object)
                    .await?;
            }
            **operation = Box::pin(
                database.complete_nonactivating_store_device_exclusion(operation.as_ref().clone()),
            )
            .await?;
            completion_result(operation.as_ref()).map(Some)
        }
        DurableStoreDeviceExclusionOperation::ReplacingCandidate { .. } => {
            for target in Box::pin(
                database.nonactivating_store_device_exclusion_cleanup_targets(
                    operation.as_ref().clone(),
                ),
            )
            .await?
            {
                crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
                database
                    .mark_candidate_cleanup_absent(target.object)
                    .await?;
            }
            **operation = Box::pin(
                database.complete_store_device_exclusion_replacement_cleanup(
                    operation.as_ref().clone(),
                ),
            )
            .await?;
            Ok(None)
        }
        DurableStoreDeviceExclusionOperation::CandidatePrepared { candidate, .. } => {
            let reference = candidate.reference.clone();
            let stream = reference.coord.stream_id.to_string();
            if database
                .exact_materialized_ref(&stream, reference.coord.sequence())
                .await?
                == Some(reference)
            {
                **operation = Box::pin(
                    database.complete_outbound_store_device_exclusion_activation(
                        operation.as_ref().clone(),
                    ),
                )
                .await?;
                completion_result(operation.as_ref()).map(Some)
            } else {
                Ok(None)
            }
        }
        DurableStoreDeviceExclusionOperation::Completed(_) => {
            completion_result(operation.as_ref()).map(Some)
        }
    }
}

async fn prepare_replacement_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    object: &DurableStoreDeviceExclusionObject,
) -> Result<PreparedStoreOperationCommit, StoreDeviceExclusionError> {
    let db = database.sqlite();
    let DurableStoreDeviceExclusionObject::Outcome {
        reference, value, ..
    } = object
    else {
        return Err(StoreDeviceExclusionError::InvalidState(
            "only an exclusion outcome can acquire a replacement candidate".to_string(),
        ));
    };
    let device_id = local_device_id(db).await?;
    let authorization =
        super::device_join::load_current_device_join_authorization(database, storage)
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
    let plan = super::operations::prepare_plan(
        database,
        storage,
        &authorization,
        &device_id,
        identity_signer,
    )
    .await?;
    let state = database
        .resolved_store_device_state(plan.device_state())
        .await?;
    require_pending_proposal(&state, reference.proposal())?;
    let proposal = crate::sync::store_objects::load_device_exclusion_proposal_ref(
        storage,
        plan.root(),
        reference.proposal(),
    )
    .await?;
    let retained = crate::sync::store_commit::RetainedStoreDeviceExclusionOutcome::from_exact(
        reference,
        crate::sync::store_commit::RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
        value,
        plan.registration(),
    )?;
    Box::pin(super::operations::prepare_candidate(
        database,
        storage,
        plan,
        StoreOperationBatch::DeviceExclusionOutcome(retained),
    ))
    .await
    .map_err(StoreDeviceExclusionError::from)
}

async fn resolve_exclusion_object_collision(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation: DurableStoreDeviceExclusionOperation,
) -> Result<Option<DurableStoreDeviceExclusionOperation>, StoreDeviceExclusionError> {
    let intended = operation.object();
    let (bytes, prepared) = storage
        .read_prepared_protocol_slot(
            &intended.context(),
            intended.object().slot(),
            intended.semantic_prefix()?,
        )
        .await?;
    if bytes == intended.semantic_bytes() {
        if prepared.reference() != intended.object() {
            return Err(StoreDeviceExclusionError::InvalidState(
                "identical exclusion bytes produced a different exact object reference".to_string(),
            ));
        }
        return Ok(None);
    }
    let DurableStoreDeviceExclusionObject::Outcome {
        reference: intended_ref,
        ..
    } = intended
    else {
        return Err(StoreDeviceExclusionError::InvalidState(
            "proposal hash slot contains different signed bytes".to_string(),
        ));
    };
    let root = database.local_store_root_ref().await?.ok_or_else(|| {
        StoreDeviceExclusionError::InvalidState("local Store root is absent".to_string())
    })?;
    let proposal = crate::sync::store_objects::load_device_exclusion_proposal_ref(
        storage,
        &root,
        intended_ref.proposal(),
    )
    .await?;
    let unverified: StoreDeviceExclusionOutcome =
        serde_json::from_slice(&bytes).map_err(|error| {
            StoreDeviceExclusionError::InvalidState(format!(
                "occupied exclusion outcome slot is malformed: {error}"
            ))
        })?;
    let winner_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
        &unverified,
        &proposal.object.value,
        prepared.reference().clone(),
    )?;
    let winner = crate::sync::store_objects::load_device_exclusion_outcome_ref(
        storage,
        &root,
        &winner_ref,
        &proposal,
    )
    .await?;
    if winner.object.value != unverified || winner.object.bytes != bytes {
        return Err(StoreDeviceExclusionError::InvalidState(
            "occupied exclusion outcome changed during exact verification".to_string(),
        ));
    }
    let completed = Box::pin(database.complete_outbound_store_device_exclusion_slot_loss(
        operation,
        DurableStoreDeviceExclusionObject::Outcome {
            reference: winner_ref,
            value: unverified,
            prepared,
        },
    ))
    .await?;
    Ok(Some(completed))
}

async fn build_exclusion_proof(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    proposal_ref: &StoreDeviceExclusionProposalRef,
    proposal: &StoreDeviceExclusionProposal,
) -> Result<StoreDeviceExclusionProof, StoreDeviceExclusionError> {
    let frozen = database
        .resolved_store_device_state(&proposal.frozen_device_state)
        .await?;
    let mut acknowledgements = Vec::new();
    let mut cutoff: Option<StoreHistoryCut> = None;
    for record in frozen.devices.values() {
        if record.registration == proposal.target
            || !matches!(record.status, StoreDeviceStatus::Active)
        {
            continue;
        }
        let reference = database
            .activated_store_ack(&record.registration)
            .await?
            .ok_or_else(|| {
                StoreDeviceExclusionError::InvalidState(format!(
                    "registration {} has not acknowledged exclusion proposal {}",
                    record.registration.device_id, proposal_ref.proposal_id
                ))
            })?;
        let registration = database
            .activated_store_device_registration(record.registration.clone())
            .await?;
        let acknowledgement = crate::sync::store_objects::load_store_ack_ref(
            storage,
            root,
            &reference,
            &registration,
        )
        .await?;
        let proposal_freezes = &acknowledgement.value.exclusions.proposal_freezes;
        let freeze = proposal_freezes
            .iter()
            .find(|freeze| freeze.proposal == *proposal_ref)
            .ok_or_else(|| {
                StoreDeviceExclusionError::InvalidState(format!(
                    "registration {} acknowledgement omits exclusion proposal {}",
                    record.registration.device_id, proposal_ref.proposal_id
                ))
            })?;
        cutoff = Some(match cutoff {
            Some(current) => current.join(freeze.target_cut.clone())?,
            None => freeze.target_cut.clone(),
        });
        acknowledgements.push(reference);
    }
    acknowledgements.sort();
    let cutoff = cutoff.ok_or_else(|| {
        StoreDeviceExclusionError::InvalidState(
            "Merge exclusion has no remaining active-device acknowledgement".to_string(),
        )
    })?;
    Ok(StoreDeviceExclusionProof {
        frozen_device_state: proposal.frozen_device_state.clone(),
        remaining_device_acks: acknowledgements,
        cutoff,
    })
}

fn completion_result(
    operation: &DurableStoreDeviceExclusionOperation,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    let DurableStoreDeviceExclusionOperation::Completed(completion) = operation else {
        return Err(StoreDeviceExclusionError::InvalidState(
            "Store-device exclusion operation is not complete".to_string(),
        ));
    };
    Ok(match completion {
        StoreDeviceExclusionCompletion::Activated { object, candidate } => match object {
            DurableStoreDeviceExclusionObject::Proposal { reference, .. } => {
                StoreDeviceExclusionResult::ProposalActivated {
                    proposal: reference.clone(),
                    commit: candidate.reference.clone(),
                }
            }
            DurableStoreDeviceExclusionObject::Outcome { reference, .. } => {
                StoreDeviceExclusionResult::OutcomeActivated {
                    outcome: reference.clone(),
                    commit: candidate.reference.clone(),
                }
            }
        },
        StoreDeviceExclusionCompletion::OutcomeSlotOccupied { intended, winner } => {
            let (
                DurableStoreDeviceExclusionObject::Outcome {
                    reference: intended,
                    ..
                },
                DurableStoreDeviceExclusionObject::Outcome {
                    reference: winner, ..
                },
            ) = (intended, winner)
            else {
                return Err(StoreDeviceExclusionError::InvalidState(
                    "outcome-slot completion contains a non-outcome object".to_string(),
                ));
            };
            StoreDeviceExclusionResult::OutcomeSlotOccupied {
                intended: intended.clone(),
                winner: winner.clone(),
            }
        }
        StoreDeviceExclusionCompletion::CandidateNonactivated {
            object, candidate, ..
        } => StoreDeviceExclusionResult::CandidateNonactivated {
            object_hash: object.operation_id(),
            candidate: candidate.reference.clone(),
        },
    })
}

async fn reject_active_operation(
    database: &StoreDatabase,
) -> Result<(), StoreDeviceExclusionError> {
    if let Some(operation) = database.active_outbound_store_device_exclusion().await? {
        return Err(StoreDeviceExclusionError::OperationActive(
            operation.operation_id(),
        ));
    }
    Ok(())
}

async fn local_device_id(db: &Database) -> Result<String, StoreDeviceExclusionError> {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or_else(|| {
            StoreDeviceExclusionError::InvalidState(
                "local Store device registration is absent".to_string(),
            )
        })
}

fn require_active_target(
    state: &crate::sync::store_commit::ResolvedStoreDeviceState,
    target: &crate::sync::store_commit::StoreDeviceRegistrationRef,
) -> Result<(), StoreDeviceExclusionError> {
    if !matches!(
        state.devices.get(&target.device_id),
        Some(record)
            if record.registration == *target && matches!(record.status, StoreDeviceStatus::Active)
    ) {
        return Err(StoreDeviceExclusionError::TargetNotActive);
    }
    Ok(())
}

fn require_pending_proposal(
    state: &crate::sync::store_commit::ResolvedStoreDeviceState,
    proposal: &StoreDeviceExclusionProposalRef,
) -> Result<(), StoreDeviceExclusionError> {
    require_active_target(state, &proposal.target)?;
    if !matches!(
        state.devices
            .get(&proposal.target.device_id)
            .and_then(|record| record.proposals.get(&proposal.proposal_id)),
        Some(StoreDeviceProposalState::Pending { proposal: current }) if current == proposal
    ) {
        return Err(StoreDeviceExclusionError::InvalidState(
            "exclusion proposal is not pending at the exact candidate predecessor".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreDeviceExclusionJournalError {
    #[error("invalid durable Store-device exclusion: {0}")]
    Invalid(String),
    #[error("Store-device exclusion remote ownership: {0}")]
    RemoteObject(#[from] RemoteObjectRecordError),
    #[error("Store-device exclusion activation: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] crate::sync::storage::StorageError),
}

impl From<StoreDeviceExclusionJournalError> for StoreDeviceExclusionError {
    fn from(error: StoreDeviceExclusionJournalError) -> Self {
        Self::Journal(error.to_string())
    }
}

#[cfg(test)]
mod tests;

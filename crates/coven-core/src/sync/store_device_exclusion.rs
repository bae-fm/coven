//! Durable publication of Store-device exclusion proposals and outcomes.

use serde::{Deserialize, Serialize};

use crate::database::{Database, DbError};
use crate::keys::UserKeypair;

use super::remote_object::{
    CandidateNonactivation, CandidateNonactivationProof, RemoteObjectRecord,
    RemoteObjectRecordError,
};
use super::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use super::store_commit::{
    device_exclusion_outcome_semantic_prefix, device_exclusion_proposal_semantic_prefix,
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusion, StoreDeviceExclusionCancellation,
    StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProof,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalId, StoreDeviceExclusionProposalRef,
    StoreDeviceProposalState, StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef,
};
use super::store_outbound::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
    StoreOutboundError,
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

    pub(crate) fn object(&self) -> &super::storage::ExactObjectRef {
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
        if self == next {
            return true;
        }
        if self.operation_id() != next.operation_id() {
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
                    && candidate.commit == next_candidate.commit
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
            ) => object == next_object && candidate == next_candidate,
            (
                Self::ReplacingCandidate {
                    object, candidate, ..
                },
                Self::CandidatePrepared {
                    object: next_object,
                    candidate: next_candidate,
                },
            ) => object == next_object && candidate == next_candidate,
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
            ) => object == next_object && candidate == next_candidate && proof == next_proof,
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
        proof: CandidateNonactivationProof,
    ) -> Result<(Self, CandidateNonactivation), StoreDeviceExclusionJournalError> {
        let Self::CandidatePrepared { object, candidate } = self else {
            return Err(StoreDeviceExclusionJournalError::Invalid(
                "only a prepared exclusion candidate can become nonactivating".to_string(),
            ));
        };
        let nonactivation = CandidateNonactivation::for_candidate(
            &candidate.reference,
            &candidate.commit,
            proof.clone(),
        )
        .map_err(StoreDeviceExclusionJournalError::RemoteObject)?;
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
        proof: CandidateNonactivationProof,
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
        let nonactivation = CandidateNonactivation::for_candidate(
            &candidate.reference,
            &candidate.commit,
            proof.clone(),
        )
        .map_err(StoreDeviceExclusionJournalError::RemoteObject)?;
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
            CandidateNonactivation::for_candidate(
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
            CandidateNonactivation::for_candidate(
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
    Object(#[from] super::store_objects::StoreObjectError),
    #[error("Store-device exclusion protocol: {0}")]
    Protocol(#[from] StoreProtocolError),
    #[error("Store-device exclusion publication: {0}")]
    Outbound(#[from] StoreOutboundError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] super::storage::StorageError),
    #[error("Store-device exclusion journal: {0}")]
    Journal(String),
    #[error("Store-device exclusion state is invalid: {0}")]
    InvalidState(String),
}

pub async fn propose_device_exclusion(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    target: &super::store_commit::StoreDeviceRegistrationRef,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    let _lock = db.lock_store_device_exclusion().await;
    reject_active_operation(db).await?;
    let durable = Box::pin(prepare_proposal(
        db,
        storage,
        coordination,
        identity_signer,
        target,
    ))
    .await?;
    drive_device_exclusion(
        db,
        storage,
        coordination,
        identity_signer,
        Box::new(durable),
    )
    .await
}

async fn prepare_proposal(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    target: &super::store_commit::StoreDeviceRegistrationRef,
) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
    let device_id = local_device_id(db).await?;
    let authorization = Box::new(
        super::device_join::load_current_device_join_authorization(db, storage, coordination)
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?,
    );
    let plan = Box::new(
        Box::pin(super::store_outbound::prepare_store_operation_commit(
            db,
            storage,
            coordination,
            &device_id,
            identity_signer,
            authorization.merge_chain(),
        ))
        .await?,
    );
    if plan.registration_ref() == target {
        return Err(StoreDeviceExclusionError::CannotExcludeLocalDevice);
    }
    let target_registration = db
        .activated_store_device_registration(target.clone())
        .await?;
    let state = Box::new(db.resolved_store_device_state(plan.device_state()).await?);
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
    let candidate = Box::pin(super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        *plan,
        StoreOperationBatch::DeviceExclusionProposal(reference.clone()),
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
    let durable = Box::pin(db.begin_outbound_store_device_exclusion(operation)).await?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged)
        .await;
    Ok(durable)
}

pub async fn cancel_device_exclusion(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    proposal: &StoreDeviceExclusionProposalRef,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    publish_outcome(
        db,
        storage,
        coordination,
        identity_signer,
        proposal,
        OutcomeIntent::Cancel,
    )
    .await
}

pub async fn finalize_device_exclusion(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    proposal: &StoreDeviceExclusionProposalRef,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    publish_outcome(
        db,
        storage,
        coordination,
        identity_signer,
        proposal,
        OutcomeIntent::Exclude,
    )
    .await
}

pub async fn resume_device_exclusion(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    let _lock = db.lock_store_device_exclusion().await;
    let Some(operation) = db.active_outbound_store_device_exclusion().await? else {
        return Ok(None);
    };
    drive_device_exclusion(
        db,
        storage,
        coordination,
        identity_signer,
        Box::new(operation),
    )
    .await
    .map(Some)
}

pub async fn get_device_exclusion_operations(
    db: &Database,
) -> Result<Vec<StoreDeviceExclusionOperationInfo>, StoreDeviceExclusionError> {
    db.outbound_store_device_exclusion_operations()
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

async fn publish_outcome(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    proposal_ref: &StoreDeviceExclusionProposalRef,
    intent: OutcomeIntent,
) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
    let _lock = db.lock_store_device_exclusion().await;
    reject_active_operation(db).await?;
    let durable = Box::pin(prepare_outcome(
        db,
        storage,
        coordination,
        identity_signer,
        proposal_ref,
        intent,
    ))
    .await?;
    drive_device_exclusion(
        db,
        storage,
        coordination,
        identity_signer,
        Box::new(durable),
    )
    .await
}

async fn prepare_outcome(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    proposal_ref: &StoreDeviceExclusionProposalRef,
    intent: OutcomeIntent,
) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
    let device_id = local_device_id(db).await?;
    let authorization =
        super::device_join::load_current_device_join_authorization(db, storage, coordination)
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
    let plan = super::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let owner_grant = plan
        .owner_grant()
        .cloned()
        .ok_or(StoreDeviceExclusionError::OwnerAuthorityRequired)?;
    let proposal = super::store_objects::load_device_exclusion_proposal_ref(
        storage,
        plan.root(),
        proposal_ref,
    )
    .await?;
    let state = db.resolved_store_device_state(plan.device_state()).await?;
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
                db,
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
    let candidate = super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        plan,
        StoreOperationBatch::DeviceExclusionOutcome(reference.clone()),
    )
    .await?;
    let operation = DurableStoreDeviceExclusionOperation::prepared(
        DurableStoreDeviceExclusionObject::Outcome {
            reference,
            value: outcome,
            prepared,
        },
        candidate,
    )?;
    let durable = db.begin_outbound_store_device_exclusion(operation).await?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged)
        .await;
    Ok(durable)
}

fn drive_device_exclusion<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    coordination: Option<&'a dyn super::storage::CoordinationStorage>,
    identity_signer: &'a UserKeypair,
    mut operation: Box<DurableStoreDeviceExclusionOperation>,
) -> impl std::future::Future<Output = Result<StoreDeviceExclusionResult, StoreDeviceExclusionError>>
       + Send
       + 'a {
    let future = async move {
        loop {
            if let Some(result) = Box::pin(resume_device_exclusion_candidate(
                db,
                storage,
                &mut operation,
            ))
            .await?
            {
                return Ok(result);
            }
            if let Some(result) = Box::pin(ensure_device_exclusion_authority_uploaded(
                db,
                storage,
                operation.as_ref(),
            ))
            .await?
            {
                return Ok(result);
            }
            match Box::pin(publish_device_exclusion_candidate(
                db,
                storage,
                coordination,
                &mut operation,
            ))
            .await?
            {
                DeviceExclusionPublicationProgress::Completed(result) => return Ok(result),
                DeviceExclusionPublicationProgress::Continue => {}
                DeviceExclusionPublicationProgress::ReplacementRequired(proof) => {
                    Box::pin(replace_device_exclusion_candidate(
                        db,
                        storage,
                        coordination,
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
    ReplacementRequired(CandidateNonactivationProof),
}

async fn publish_device_exclusion_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
) -> Result<DeviceExclusionPublicationProgress, StoreDeviceExclusionError> {
    let candidate = operation.candidate().cloned().ok_or_else(|| {
        StoreDeviceExclusionError::InvalidState(
            "active exclusion operation has no activation candidate".to_string(),
        )
    })?;
    let publish = super::store_outbound::publish_prepared_store_operation(
        db,
        storage,
        coordination,
        candidate,
    );
    let publication = Box::new(Box::pin(publish).await?);
    match publication.as_ref() {
        StoreOperationPublicationOutcome::Activated(_) => {
            **operation = Box::pin(
                db.complete_outbound_store_device_exclusion_activation(operation.as_ref().clone()),
            )
            .await?;
            completion_result(operation.as_ref()).map(DeviceExclusionPublicationProgress::Completed)
        }
        StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
            **operation = Box::pin(db.replace_outbound_store_device_exclusion_candidate(
                operation.as_ref().clone(),
                candidate.clone(),
            ))
            .await?;
            Ok(DeviceExclusionPublicationProgress::Continue)
        }
        StoreOperationPublicationOutcome::NonactivatedCandidate { candidate, proof } => {
            if operation.candidate() != Some(candidate) {
                return Err(StoreDeviceExclusionError::InvalidState(
                    "publication conflict names another exclusion candidate".to_string(),
                ));
            }
            if matches!(
                operation.object(),
                DurableStoreDeviceExclusionObject::Outcome { .. }
            ) {
                return Ok(DeviceExclusionPublicationProgress::ReplacementRequired(
                    proof.clone(),
                ));
            } else {
                **operation = Box::pin(db.begin_outbound_store_device_exclusion_nonactivation(
                    operation.as_ref().clone(),
                    proof.clone(),
                ))
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
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
    proof: CandidateNonactivationProof,
) -> Result<(), StoreDeviceExclusionError> {
    let replacement = Box::pin(prepare_replacement_candidate(
        db,
        storage,
        coordination,
        identity_signer,
        operation.object(),
    ))
    .await?;
    **operation = Box::pin(db.begin_outbound_store_device_exclusion_replacement(
        operation.as_ref().clone(),
        replacement,
        proof,
    ))
    .await?;
    Ok(())
}

async fn ensure_device_exclusion_authority_uploaded(
    db: &Database,
    storage: &dyn SyncStorage,
    operation: &DurableStoreDeviceExclusionOperation,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    match Box::pin(operation.create_exact_object(storage)).await {
        Ok(()) => {}
        Err(StoreDeviceExclusionJournalError::Storage(
            super::storage::StorageError::SlotCollision(_),
        )) => {
            if let Some(completed) = Box::pin(resolve_exclusion_object_collision(
                db,
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
    Box::pin(db.mark_store_device_exclusion_authority_uploaded(operation.clone())).await?;
    Ok(None)
}

async fn resume_device_exclusion_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    operation: &mut Box<DurableStoreDeviceExclusionOperation>,
) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
    match operation.as_ref() {
        DurableStoreDeviceExclusionOperation::CandidateNonactivating { .. } => {
            for target in Box::pin(
                db.nonactivating_store_device_exclusion_cleanup_targets(operation.as_ref().clone()),
            )
            .await?
            {
                super::store_objects::delete_exact_object(storage, &target.object).await?;
                db.mark_candidate_cleanup_absent(target.object).await?;
            }
            **operation = Box::pin(
                db.complete_nonactivating_store_device_exclusion(operation.as_ref().clone()),
            )
            .await?;
            completion_result(operation.as_ref()).map(Some)
        }
        DurableStoreDeviceExclusionOperation::ReplacingCandidate { .. } => {
            for target in Box::pin(
                db.nonactivating_store_device_exclusion_cleanup_targets(operation.as_ref().clone()),
            )
            .await?
            {
                super::store_objects::delete_exact_object(storage, &target.object).await?;
                db.mark_candidate_cleanup_absent(target.object).await?;
            }
            **operation = Box::pin(
                db.complete_store_device_exclusion_replacement_cleanup(operation.as_ref().clone()),
            )
            .await?;
            Ok(None)
        }
        DurableStoreDeviceExclusionOperation::CandidatePrepared { candidate, .. } => {
            let reference = candidate.reference.clone();
            let stream = match &reference.coord {
                super::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
                    stream_id.to_string()
                }
                super::store_commit::StoreCommitCoord::Serial { .. } => {
                    super::store_commit::SERIAL_STREAM_ID.to_string()
                }
            };
            if db
                .exact_materialized_ref(&stream, reference.coord.sequence())
                .await?
                == Some(reference)
            {
                **operation = Box::pin(db.complete_outbound_store_device_exclusion_activation(
                    operation.as_ref().clone(),
                ))
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
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity_signer: &UserKeypair,
    object: &DurableStoreDeviceExclusionObject,
) -> Result<PreparedStoreOperationCommit, StoreDeviceExclusionError> {
    let DurableStoreDeviceExclusionObject::Outcome { reference, .. } = object else {
        return Err(StoreDeviceExclusionError::InvalidState(
            "only an exclusion outcome can acquire a replacement candidate".to_string(),
        ));
    };
    let device_id = local_device_id(db).await?;
    let authorization =
        super::device_join::load_current_device_join_authorization(db, storage, coordination)
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
    let plan = super::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let state = db.resolved_store_device_state(plan.device_state()).await?;
    require_pending_proposal(&state, reference.proposal())?;
    super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        plan,
        StoreOperationBatch::DeviceExclusionOutcome(reference.clone()),
    )
    .await
    .map_err(StoreDeviceExclusionError::from)
}

async fn resolve_exclusion_object_collision(
    db: &Database,
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
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StoreDeviceExclusionError::InvalidState("local Store root is absent".to_string())
    })?;
    let proposal = super::store_objects::load_device_exclusion_proposal_ref(
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
    let winner = super::store_objects::load_device_exclusion_outcome_ref(
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
    let completed = db
        .complete_outbound_store_device_exclusion_slot_loss(
            operation,
            DurableStoreDeviceExclusionObject::Outcome {
                reference: winner_ref,
                value: unverified,
                prepared,
            },
        )
        .await?;
    Ok(Some(completed))
}

async fn build_exclusion_proof(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    proposal_ref: &StoreDeviceExclusionProposalRef,
    proposal: &StoreDeviceExclusionProposal,
) -> Result<StoreDeviceExclusionProof, StoreDeviceExclusionError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return Ok(StoreDeviceExclusionProof::Serial);
    }
    let frozen = db
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
        let reference = db
            .activated_store_ack(&record.registration)
            .await?
            .ok_or_else(|| {
                StoreDeviceExclusionError::InvalidState(format!(
                    "registration {} has not acknowledged exclusion proposal {}",
                    record.registration.device_id, proposal_ref.proposal_id
                ))
            })?;
        let registration = db
            .activated_store_device_registration(record.registration.clone())
            .await?;
        let acknowledgement =
            super::store_objects::load_store_ack_ref(storage, root, &reference, &registration)
                .await?;
        let super::store_commit::StoreAckExclusionState::MergeConcurrent { proposal_freezes } =
            &acknowledgement.value.exclusions
        else {
            return Err(StoreDeviceExclusionError::InvalidState(
                "Merge exclusion evidence contains a Serial acknowledgement".to_string(),
            ));
        };
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
    Ok(StoreDeviceExclusionProof::MergeConcurrent {
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

async fn reject_active_operation(db: &Database) -> Result<(), StoreDeviceExclusionError> {
    if let Some(operation) = db.active_outbound_store_device_exclusion().await? {
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
    state: &super::store_commit::ResolvedStoreDeviceState,
    target: &super::store_commit::StoreDeviceRegistrationRef,
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
    state: &super::store_commit::ResolvedStoreDeviceState,
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
    Outbound(#[from] StoreOutboundError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] super::storage::StorageError),
}

impl From<StoreDeviceExclusionJournalError> for StoreDeviceExclusionError {
    fn from(error: StoreDeviceExclusionJournalError) -> Self {
        Self::Journal(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::StoreAckExclusionState;
    use crate::sync::test_helpers::{install_active_device_fixture, open_test_db, TestStore};

    fn open(path: &Path, device_id: &str) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            device_id.to_string(),
            &crate::sync::test_helpers::test_migrations(),
        )
        .expect("open exclusion test database")
        .0
    }

    fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "device-exclusion-store",
            signer.clone(),
        )
        .expect("construct exclusion test storage")
    }

    #[tokio::test]
    async fn uploaded_proposal_resumes_after_restart_without_freezing_the_target() {
        let directory = tempfile::tempdir().expect("exclusion test directory");
        let path = directory.path().join("store.sqlite");
        let signer = UserKeypair::generate();
        let home = InMemoryCloudHome::new();
        let storage = Arc::new(storage(&home, &signer));
        let db = open(&path, "exclusion-host");
        Box::pin(create_exclusion_test_store(&db, storage.as_ref(), &signer)).await;
        let reference = Box::pin(stage_uploaded_proposal(&db, storage.as_ref(), &signer)).await;
        drop(db);

        let (reopened, base_sequence) = Box::pin(resume_proposal_and_publish_freeze_ack(
            &path,
            storage.clone(),
            signer.clone(),
            &reference,
        ))
        .await;
        Box::pin(race_cancellation_with_ack(
            reopened,
            storage,
            signer,
            reference,
            base_sequence,
        ))
        .await;
    }

    #[tokio::test]
    async fn remaining_device_freezes_and_acknowledges_before_owner_exclusion() {
        Box::pin(run_remaining_device_exclusion()).await;
    }

    async fn run_remaining_device_exclusion() {
        let signer = UserKeypair::generate();
        let owner_db = open_test_db();
        let store = Box::pin(TestStore::create(
            &owner_db,
            "device-exclusion-two-device-store",
            signer.clone(),
        ))
        .await
        .expect("create two-device exclusion Store");
        Box::pin(store.open_into(&owner_db))
            .await
            .expect("open two-device exclusion Store");
        let peer_db = open_test_db();
        Box::pin(install_active_device_fixture(
            &store,
            &owner_db,
            &peer_db,
            &signer,
            "2026-07-18T00:00:00Z",
        ))
        .await
        .expect("activate peer Store device");

        let local_device_id = local_device_id(&owner_db).await.expect("local device id");
        let target = owner_db
            .activated_store_device_registration_records()
            .await
            .expect("list active Store registrations")
            .into_iter()
            .map(|(reference, _)| reference)
            .find(|reference| reference.device_id.to_string() != local_device_id)
            .expect("peer Store registration");
        let proposal = match Box::pin(propose_device_exclusion(
            &owner_db,
            &store.storage,
            None,
            &signer,
            &target,
        ))
        .await
        .expect("propose peer exclusion")
        {
            StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => proposal,
            result => panic!("unexpected exclusion proposal result: {result:?}"),
        };
        let freezes = owner_db
            .store_device_exclusion_freezes()
            .await
            .expect("read owner exclusion freeze");
        assert_eq!(freezes.len(), 1);
        assert_eq!(freezes[0].proposal, proposal);
        assert_eq!(freezes[0].proposal.target, target);

        let frontier = super::super::store_commit::CommitFrontier::from_refs(
            owner_db.write_policy(),
            owner_db
                .materialized_frontier()
                .await
                .expect("read owner exclusion frontier"),
        )
        .expect("shape owner exclusion frontier");
        let acknowledgement = Box::pin(super::super::store_ack::stage_store_ack(
            &owner_db,
            &store.storage,
            frontier,
            "2026-07-18T00:01:00Z".to_string(),
            &signer,
        ))
        .await
        .expect("stage owner exclusion acknowledgement");
        let StoreAckExclusionState::MergeConcurrent { proposal_freezes } =
            acknowledgement.exclusions
        else {
            panic!("Merge acknowledgement changed policy")
        };
        assert_eq!(proposal_freezes, freezes);
        let membership = Box::pin(super::super::pull::load_cycle_membership(
            &store.storage,
            &owner_db,
        ))
        .await
        .expect("load owner exclusion membership");
        assert_eq!(
            Box::pin(super::super::store_ack::drain_outbound_store_acks(
                &owner_db,
                &store.storage,
                None,
                &signer,
                membership.chain.as_ref(),
            ))
            .await
            .expect("publish owner exclusion acknowledgement"),
            1
        );

        let result = Box::pin(finalize_device_exclusion(
            &owner_db,
            &store.storage,
            None,
            &signer,
            &proposal,
        ))
        .await
        .expect("finalize peer exclusion");
        assert!(matches!(
            result,
            StoreDeviceExclusionResult::OutcomeActivated {
                outcome: StoreDeviceExclusionOutcomeRef::Excluded(_),
                ..
            }
        ));
        assert!(owner_db
            .store_device_exclusion_freezes()
            .await
            .expect("read released owner exclusion freeze")
            .is_empty());
    }

    async fn create_exclusion_test_store(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
    ) {
        Box::pin(super::super::store_protocol_root::create_store(
            db,
            storage,
            "device-exclusion-store",
            signer,
        ))
        .await
        .expect("create exclusion test Store");
        super::super::store_registration::ensure_active_registration(db, storage, signer)
            .await
            .expect("activate exclusion test registration");
    }

    async fn stage_uploaded_proposal(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
    ) -> StoreDeviceExclusionProposalRef {
        let membership = super::super::pull::load_cycle_membership(storage, db)
            .await
            .expect("load exclusion test membership");
        let device_id = local_device_id(db).await.expect("local device id");
        let plan = super::super::store_outbound::prepare_store_operation_commit(
            db,
            storage,
            None,
            &device_id,
            signer,
            membership.chain.as_ref(),
        )
        .await
        .expect("prepare exclusion proposal predecessor");
        let target = plan.registration_ref().clone();
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            b"restart exclusion proposal",
        ));
        let outcome_prefix =
            device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id);
        let outcome_context = ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let outcome_slot = storage
            .allocate_protocol_slot(&outcome_context, &outcome_prefix, ".json")
            .await
            .expect("allocate exclusion outcome slot");
        let proposal = StoreDeviceExclusionProposal::signed(
            plan.root().store_root_hash,
            proposal_id,
            target.clone(),
            plan.registration(),
            plan.device_state().clone(),
            outcome_slot,
            target.clone(),
            plan.owner_grant().expect("founder Owner grant").clone(),
            plan.registration(),
            plan.device_signer(),
        )
        .expect("sign exclusion proposal");
        let prefix = device_exclusion_proposal_semantic_prefix(
            target.device_id,
            proposal_id,
            proposal.proposal_hash(),
        );
        let context = ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionProposal,
        );
        let slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .expect("allocate exclusion proposal slot");
        let prepared = storage
            .prepare_protocol_object(&context, slot, &prefix, proposal.to_bytes())
            .expect("prepare exclusion proposal object");
        let reference =
            StoreDeviceExclusionProposalRef::from_proposal(&proposal, prepared.reference().clone())
                .expect("reference exclusion proposal");
        let candidate = super::super::store_outbound::prepare_store_operation_candidate(
            db,
            storage,
            plan,
            StoreOperationBatch::DeviceExclusionProposal(reference.clone()),
        )
        .await
        .expect("prepare exclusion activation candidate");
        let operation = DurableStoreDeviceExclusionOperation::prepared(
            DurableStoreDeviceExclusionObject::Proposal {
                reference: reference.clone(),
                value: proposal,
                prepared,
            },
            candidate,
        )
        .expect("close exclusion journal");
        let durable = db
            .begin_outbound_store_device_exclusion(operation)
            .await
            .expect("persist exclusion journal");
        durable
            .create_exact_object(storage)
            .await
            .expect("upload exclusion proposal before simulated restart");
        db.mark_store_device_exclusion_authority_uploaded(durable)
            .await
            .expect("persist proposal upload");
        reference
    }

    async fn resume_proposal_and_publish_freeze_ack(
        path: &Path,
        storage: Arc<CloudSyncStorage>,
        signer: UserKeypair,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> (Database, u64) {
        let reopened = open(path, "exclusion-host");
        let result = Box::pin(resume_device_exclusion(
            &reopened,
            storage.as_ref(),
            None,
            &signer,
        ))
        .await
        .expect("resume exclusion proposal")
        .expect("pending exclusion operation");
        assert!(matches!(
            result,
            StoreDeviceExclusionResult::ProposalActivated { proposal, .. }
                if proposal == *reference
        ));
        assert!(reopened
            .active_outbound_store_device_exclusion()
            .await
            .expect("read exclusion journal")
            .is_none());
        let freezes = reopened
            .store_device_exclusion_freezes()
            .await
            .expect("read exclusion freezes");
        assert!(
            freezes.is_empty(),
            "the exclusion target must not freeze its own Store stream"
        );

        let frontier = super::super::store_commit::CommitFrontier::from_refs(
            reopened.write_policy(),
            reopened
                .materialized_frontier()
                .await
                .expect("read exclusion frontier"),
        )
        .expect("shape exclusion frontier");
        let acknowledgement = Box::pin(super::super::store_ack::stage_store_ack(
            &reopened,
            storage.as_ref(),
            frontier,
            "2026-07-18T00:00:00Z".to_string(),
            &signer,
        ))
        .await
        .expect("stage exclusion acknowledgement");
        let StoreAckExclusionState::MergeConcurrent { proposal_freezes } =
            acknowledgement.exclusions
        else {
            panic!("Merge acknowledgement changed policy")
        };
        assert!(proposal_freezes.is_empty());

        let membership = super::super::pull::load_cycle_membership(storage.as_ref(), &reopened)
            .await
            .expect("reload exclusion membership");
        assert_eq!(
            Box::pin(super::super::store_ack::drain_outbound_store_acks(
                &reopened,
                storage.as_ref(),
                None,
                &signer,
                membership.chain.as_ref(),
            ))
            .await
            .expect("publish exclusion acknowledgement"),
            1
        );
        let base_sequence = reopened
            .latest_local_store_position()
            .await
            .expect("read cancellation base")
            .expect("acknowledgement activation position")
            .coord
            .sequence();
        (reopened, base_sequence)
    }

    async fn race_cancellation_with_ack(
        reopened: Database,
        storage: Arc<CloudSyncStorage>,
        signer: UserKeypair,
        reference: StoreDeviceExclusionProposalRef,
        base_sequence: u64,
    ) {
        let (candidate_staged, resume_candidate) = reopened.arm_test_pause(
            crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
        );
        let cancel_db = reopened.clone();
        let cancel_storage = storage.clone();
        let cancel_signer = signer.clone();
        let cancel_reference = reference.clone();
        let cancellation_task = tokio::spawn(async move {
            cancel_device_exclusion(
                &cancel_db,
                cancel_storage.as_ref(),
                None,
                &cancel_signer,
                &cancel_reference,
            )
            .await
        });
        candidate_staged.notified().await;

        let frontier = super::super::store_commit::CommitFrontier::from_refs(
            reopened.write_policy(),
            reopened
                .materialized_frontier()
                .await
                .expect("read competing acknowledgement frontier"),
        )
        .expect("shape competing acknowledgement frontier");
        super::super::store_ack::stage_store_ack(
            &reopened,
            storage.as_ref(),
            frontier,
            "2026-07-18T00:01:00Z".to_string(),
            &signer,
        )
        .await
        .expect("stage competing acknowledgement");
        let membership = super::super::pull::load_cycle_membership(storage.as_ref(), &reopened)
            .await
            .expect("reload competing acknowledgement membership");
        assert_eq!(
            super::super::store_ack::drain_outbound_store_acks(
                &reopened,
                storage.as_ref(),
                None,
                &signer,
                membership.chain.as_ref(),
            )
            .await
            .expect("publish competing acknowledgement"),
            1
        );
        resume_candidate.notify_one();
        let cancellation = cancellation_task
            .await
            .expect("join cancellation publication")
            .expect("cancel exclusion proposal");
        assert!(matches!(
            &cancellation,
            StoreDeviceExclusionResult::OutcomeActivated {
                outcome: StoreDeviceExclusionOutcomeRef::Cancelled(_),
                commit,
            } if commit.coord.sequence() == base_sequence + 2
        ));
        assert!(reopened
            .store_device_exclusion_freezes()
            .await
            .expect("read released exclusion freezes")
            .is_empty());
        let operations = get_device_exclusion_operations(&reopened)
            .await
            .expect("list exclusion operations");
        assert_eq!(operations.len(), 2);
        assert!(operations.iter().all(|operation| matches!(
            operation.status,
            StoreDeviceExclusionOperationStatus::Completed(_)
        )));
    }
}

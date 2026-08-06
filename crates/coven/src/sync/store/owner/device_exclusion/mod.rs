//! Durable publication of Store-device exclusion proposals and outcomes.

mod history;
pub(super) use history::DeviceExclusionHistory;

use coven_protocol::device_exclusion_journal::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};

use super::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use super::verified_history::MergeHistoryVerifier;
use super::{AuthorizedWriterOperation, Store, StoreError};
use crate::database::DbError;
use crate::database::StoreDatabase;
use crate::storage::{SyncStorage, VerifiedObjectWrites};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    device_exclusion_outcome_semantic_prefix, device_exclusion_proposal_semantic_prefix,
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProof, StoreDeviceExclusionProposal, StoreDeviceExclusionProposalId,
    StoreDeviceExclusionProposalRef, StoreDeviceProposalState, StoreDeviceStatus, StoreHistoryCut,
    StoreProtocolError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreDeviceExclusionResult {
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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreDeviceExclusionOperationInfo {
    pub operation_id: ObjectHash,
    pub status: StoreDeviceExclusionOperationStatus,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreDeviceExclusionOperationStatus {
    Pending,
    Completed(StoreDeviceExclusionResult),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreDeviceExclusionError {
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
    Object(#[from] coven_protocol::objects::StoreObjectError),
    #[error("Store-device exclusion protocol: {0}")]
    Protocol(#[from] StoreProtocolError),
    #[error("Store-device exclusion publication: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("Store-device exclusion journal: {0}")]
    Journal(String),
    #[error("Store-device exclusion state is invalid: {0}")]
    InvalidState(String),
}

impl Store {
    pub(crate) async fn propose_device_exclusion_for_device(
        &self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<StoreDeviceExclusionProposalRef, StoreDeviceExclusionError> {
        let target = self
            .database
            .activated_store_device_registration_for_device(device_id)
            .await?
            .ok_or(StoreDeviceExclusionError::TargetNotActive)?;
        match self.propose_device_exclusion(target.reference()).await? {
            StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => Ok(proposal),
            other => Err(StoreDeviceExclusionError::InvalidState(format!(
                "proposal did not activate: {other:?}"
            ))),
        }
    }

    pub(crate) async fn cancel_device_exclusion_proposal(
        &self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<(), StoreDeviceExclusionError> {
        match self.cancel_device_exclusion(proposal).await? {
            StoreDeviceExclusionResult::OutcomeActivated { .. } => Ok(()),
            other => Err(StoreDeviceExclusionError::InvalidState(format!(
                "cancellation did not activate: {other:?}"
            ))),
        }
    }

    pub(crate) async fn finalize_device_exclusion_proposal(
        &self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<(), StoreDeviceExclusionError> {
        match self.finalize_device_exclusion(proposal).await? {
            StoreDeviceExclusionResult::OutcomeActivated { .. } => Ok(()),
            other => Err(StoreDeviceExclusionError::InvalidState(format!(
                "exclusion did not activate: {other:?}"
            ))),
        }
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
        authority.device_exclusion().propose(target).await
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
        authority
            .device_exclusion()
            .publish_outcome(proposal, OutcomeIntent::Cancel)
            .await
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let mut authority = self
            .authorize_writer()
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
        authority
            .device_exclusion()
            .publish_outcome(proposal, OutcomeIntent::Exclude)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn device_exclusion_operations_for_test(
        &self,
    ) -> Result<Vec<StoreDeviceExclusionOperationInfo>, StoreDeviceExclusionError> {
        self.database
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

    /// Stage and upload one exclusion proposal against this device's own
    /// registration, stopping before activation so a restart resumes it. The
    /// target is the local device — which [`AuthorizedDeviceExclusion::propose`]
    /// refuses — so the test enters the production pipeline one step below that
    /// gate, at [`AuthorizedDeviceExclusion::stage_proposal`], under a fixed
    /// proposal id.
    #[cfg(test)]
    pub(crate) async fn stage_uploaded_device_exclusion_proposal_for_test(
        &self,
    ) -> Result<StoreDeviceExclusionProposalRef, StoreDeviceExclusionError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| StoreDeviceExclusionError::InvalidState(error.to_string()))?;
        let plan = Box::new(writer.prepare_plan().await?);
        let target = plan.local_registration_reference_for_test();
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            b"restart exclusion proposal",
        ));
        let mut exclusion = writer.device_exclusion();
        let durable = exclusion.stage_proposal(plan, &target, proposal_id).await?;
        let DurableStoreDeviceExclusionObject::Proposal { reference, .. } = durable.object() else {
            return Err(StoreDeviceExclusionError::InvalidState(
                "staged exclusion operation is not a proposal".to_string(),
            ));
        };
        let reference = reference.clone();
        exclusion.create_exact_object(&durable).await?;
        self.database
            .mark_store_device_exclusion_authority_uploaded(durable)
            .await?;
        Ok(reference)
    }
}

pub(crate) struct AuthorizedDeviceExclusion<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: std::sync::Arc<dyn SyncStorage>,
}

impl<'operation, 'storage> AuthorizedDeviceExclusion<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: std::sync::Arc<dyn SyncStorage>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
        }
    }

    async fn create_exact_object(
        &self,
        operation: &DurableStoreDeviceExclusionOperation,
    ) -> Result<(), StoreDeviceExclusionJournalError> {
        let context = operation.object().context();
        let prefix = operation.object().semantic_prefix()?;
        self.storage
            .create_protocol_object(operation.object().prepared())
            .await
            .map_err(StoreDeviceExclusionJournalError::Storage)?;
        self.storage
            .verify_readback(
                &context,
                operation.object().object(),
                prefix,
                &operation.object().semantic_bytes(),
            )
            .await
            .map_err(StoreDeviceExclusionJournalError::Storage)
    }

    pub(super) async fn resume(
        &mut self,
    ) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let _lock = database.device_exclusion_permit().await;
        let Some(operation) = database.active_outbound_store_device_exclusion().await? else {
            return Ok(None);
        };
        self.drive(Box::new(operation)).await.map(Some)
    }

    async fn reject_active_operation(&self) -> Result<(), StoreDeviceExclusionError> {
        if let Some(operation) = self
            .database
            .active_outbound_store_device_exclusion()
            .await?
        {
            return Err(StoreDeviceExclusionError::OperationActive(
                operation.operation_id(),
            ));
        }
        Ok(())
    }

    async fn propose(
        &mut self,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let _lock = database.device_exclusion_permit().await;
        self.reject_active_operation().await?;
        let durable = self.prepare_proposal(target).await?;
        self.drive(Box::new(durable)).await
    }

    async fn prepare_proposal(
        &mut self,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let plan = Box::new(self.writer.prepare_plan().await?);
        if plan.is_local_registration(target) {
            return Err(StoreDeviceExclusionError::CannotExcludeLocalDevice);
        }
        let state = Box::new(
            database
                .resolved_store_device_state(plan.device_state())
                .await?,
        );
        require_active_target(&state, target)?;
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            database.new_store_write_id().as_str().as_bytes(),
        ));
        self.stage_proposal(plan, target, proposal_id).await
    }

    /// Sign one exclusion proposal against `target`, reserve its exact slots,
    /// and journal the candidate that activates it. The caller has already
    /// established that `target` is an excludable active device and chosen the
    /// proposal's identity.
    async fn stage_proposal(
        &mut self,
        plan: Box<crate::sync::store::operations::StoreOperationCommitPlan>,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        proposal_id: StoreDeviceExclusionProposalId,
    ) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let target_registration = database
            .activated_store_device_registration(target.clone())
            .await?;
        let owner_grant = plan
            .owner_grant()
            .cloned()
            .ok_or(StoreDeviceExclusionError::OwnerAuthorityRequired)?;
        let outcome_prefix =
            device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id);
        let outcome_context = ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let outcome_slot = self
            .storage
            .allocate_protocol_slot(&outcome_context, &outcome_prefix, ".json")
            .await?;
        let proposal = plan.sign_device_exclusion_proposal(
            proposal_id,
            target.clone(),
            target_registration.value(),
            outcome_slot,
            owner_grant,
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
        let proposal_slot = self
            .storage
            .allocate_protocol_slot(&proposal_context, &proposal_prefix, ".json")
            .await?;
        let prepared = self.storage.prepare_protocol_object(
            &proposal_context,
            proposal_slot,
            &proposal_prefix,
            proposal.to_bytes(),
        )?;
        let reference = StoreDeviceExclusionProposalRef::from_proposal(
            &proposal,
            prepared.reference().clone(),
        )?;
        let retained = plan.retain_device_exclusion_proposal(
            reference.clone(),
            &proposal,
            target_registration.value(),
        )?;
        let candidate = Box::pin(self.writer.prepare_candidate(
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
        #[cfg(test)]
        database
            .reach_test_point(
                crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
            )
            .await;
        Ok(durable)
    }

    async fn publish_outcome(
        &mut self,
        proposal_ref: &StoreDeviceExclusionProposalRef,
        intent: OutcomeIntent,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let _lock = database.device_exclusion_permit().await;
        self.reject_active_operation().await?;
        let durable = self.prepare_outcome(proposal_ref, intent).await?;
        self.drive(Box::new(durable)).await
    }

    async fn prepare_outcome(
        &mut self,
        proposal_ref: &StoreDeviceExclusionProposalRef,
        intent: OutcomeIntent,
    ) -> Result<DurableStoreDeviceExclusionOperation, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let plan = self.writer.prepare_plan().await?;
        let owner_grant = plan
            .owner_grant()
            .cloned()
            .ok_or(StoreDeviceExclusionError::OwnerAuthorityRequired)?;
        let proposal = self
            .writer
            .device_exclusion_history()
            .load_proposal(proposal_ref)
            .await?;
        let state = database
            .resolved_store_device_state(plan.device_state())
            .await?;
        require_pending_proposal(&state, proposal_ref)?;
        let outcome = match intent {
            OutcomeIntent::Cancel => {
                StoreDeviceExclusionOutcome::Cancelled(plan.sign_device_exclusion_cancellation(
                    proposal_ref.clone(),
                    &proposal.object.value,
                    owner_grant,
                )?)
            }
            OutcomeIntent::Exclude => {
                let proof = self
                    .build_exclusion_proof(proposal_ref, &proposal.object.value)
                    .await?;
                StoreDeviceExclusionOutcome::Excluded(plan.sign_device_exclusion(
                    proposal_ref.clone(),
                    &proposal.object.value,
                    proposal_ref.target.clone(),
                    &proposal.target,
                    proof,
                    owner_grant,
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
        let prepared = self.storage.prepare_protocol_object(
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
            coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_verified(
                &proposal,
            );
        let retained =
            plan.retain_device_exclusion_outcome(&reference, retained_proposal, &outcome)?;
        let candidate = Box::pin(
            self.writer
                .prepare_candidate(plan, StoreOperationBatch::DeviceExclusionOutcome(retained)),
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
        let durable = Box::pin(database.begin_outbound_store_device_exclusion(operation)).await?;
        #[cfg(test)]
        database
            .reach_test_point(
                crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
            )
            .await;
        Ok(durable)
    }

    async fn drive(
        &mut self,
        mut operation: Box<DurableStoreDeviceExclusionOperation>,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        loop {
            if let Some(result) = self.resume_candidate(&mut operation).await? {
                return Ok(result);
            }
            if let Some(result) = self.ensure_authority_uploaded(operation.as_ref()).await? {
                return Ok(result);
            }
            match self.publish_candidate(&mut operation).await? {
                DeviceExclusionPublicationProgress::Completed(result) => return Ok(result),
                DeviceExclusionPublicationProgress::Continue => {}
                DeviceExclusionPublicationProgress::ReplacementRequired(proof) => {
                    self.replace_candidate(&mut operation, proof).await?;
                }
            }
        }
    }

    async fn publish_candidate(
        &mut self,
        operation: &mut Box<DurableStoreDeviceExclusionOperation>,
    ) -> Result<DeviceExclusionPublicationProgress, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let candidate = operation.candidate().cloned().ok_or_else(|| {
            StoreDeviceExclusionError::InvalidState(
                "active exclusion operation has no activation candidate".to_string(),
            )
        })?;
        let publication = Box::new(
            self.writer
                .publish_prepared(Box::new(candidate), None, None)
                .await?,
        );
        match publication.as_ref() {
            StoreOperationPublicationOutcome::Activated(_) => {
                **operation = Box::pin(
                    database.complete_outbound_store_device_exclusion_activation(
                        operation.as_ref().clone(),
                    ),
                )
                .await?;
                completion_result(operation.as_ref())
                    .map(DeviceExclusionPublicationProgress::Completed)
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

    async fn replace_candidate(
        &mut self,
        operation: &mut Box<DurableStoreDeviceExclusionOperation>,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), StoreDeviceExclusionError> {
        let database = self.database.clone();
        let replacement = self
            .prepare_replacement_candidate(operation.object())
            .await?;
        **operation = Box::pin(database.begin_outbound_store_device_exclusion_replacement(
            operation.as_ref().clone(),
            replacement,
            nonactivation,
        ))
        .await?;
        Ok(())
    }

    async fn ensure_authority_uploaded(
        &mut self,
        operation: &DurableStoreDeviceExclusionOperation,
    ) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
        let database = self.database.clone();
        match Box::pin(self.create_exact_object(operation)).await {
            Ok(()) => {}
            Err(StoreDeviceExclusionJournalError::Storage(
                coven_protocol::objects::StorageError::SlotCollision(_),
            )) => {
                if let Some(completed) = self.resolve_object_collision(operation.clone()).await? {
                    return completion_result(&completed).map(Some);
                }
            }
            Err(error) => return Err(error.into()),
        }
        Box::pin(database.mark_store_device_exclusion_authority_uploaded(operation.clone()))
            .await?;
        Ok(None)
    }

    async fn resume_candidate(
        &mut self,
        operation: &mut Box<DurableStoreDeviceExclusionOperation>,
    ) -> Result<Option<StoreDeviceExclusionResult>, StoreDeviceExclusionError> {
        let database = self.database.clone();
        match operation.as_ref() {
            DurableStoreDeviceExclusionOperation::CandidateNonactivating { .. } => {
                let targets = Box::pin(
                    database.nonactivating_store_device_exclusion_cleanup_targets(
                        operation.as_ref().clone(),
                    ),
                )
                .await?;
                crate::sync::store::owner::delete_candidate_cleanup_targets::<
                    StoreDeviceExclusionError,
                >(self.storage.as_ref(), &database, targets)
                .await?;
                **operation = Box::pin(
                    database
                        .complete_nonactivating_store_device_exclusion(operation.as_ref().clone()),
                )
                .await?;
                completion_result(operation.as_ref()).map(Some)
            }
            DurableStoreDeviceExclusionOperation::ReplacingCandidate { .. } => {
                let targets = Box::pin(
                    database.nonactivating_store_device_exclusion_cleanup_targets(
                        operation.as_ref().clone(),
                    ),
                )
                .await?;
                crate::sync::store::owner::delete_candidate_cleanup_targets::<
                    StoreDeviceExclusionError,
                >(self.storage.as_ref(), &database, targets)
                .await?;
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
        &mut self,
        object: &DurableStoreDeviceExclusionObject,
    ) -> Result<PreparedStoreOperationCommit, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let DurableStoreDeviceExclusionObject::Outcome {
            reference, value, ..
        } = object
        else {
            return Err(StoreDeviceExclusionError::InvalidState(
                "only an exclusion outcome can acquire a replacement candidate".to_string(),
            ));
        };
        let plan = self.writer.prepare_plan().await?;
        let state = database
            .resolved_store_device_state(plan.device_state())
            .await?;
        require_pending_proposal(&state, reference.proposal())?;
        let proposal = self
            .writer
            .device_exclusion_history()
            .load_proposal(reference.proposal())
            .await?;
        let retained = plan.retain_device_exclusion_outcome(
            reference,
            coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_verified(
                &proposal,
            ),
            value,
        )?;
        Box::pin(
            self.writer
                .prepare_candidate(plan, StoreOperationBatch::DeviceExclusionOutcome(retained)),
        )
        .await
        .map_err(StoreDeviceExclusionError::from)
    }

    async fn resolve_object_collision(
        &mut self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<Option<DurableStoreDeviceExclusionOperation>, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let intended = operation.object();
        let (bytes, prepared) = self
            .storage
            .read_prepared_protocol_slot(
                &intended.context(),
                intended.object().slot(),
                intended.semantic_prefix()?,
            )
            .await?;
        if bytes == intended.semantic_bytes() {
            if prepared.reference() != intended.object() {
                return Err(StoreDeviceExclusionError::InvalidState(
                    "identical exclusion bytes produced a different exact object reference"
                        .to_string(),
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
        let proposal = self
            .writer
            .device_exclusion_history()
            .load_proposal(intended_ref.proposal())
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
        let winner = self
            .writer
            .device_exclusion_history()
            .load_outcome(&winner_ref, &proposal)
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
        &mut self,
        proposal_ref: &StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
    ) -> Result<StoreDeviceExclusionProof, StoreDeviceExclusionError> {
        let database = self.database.clone();
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
            let acknowledgement = self
                .writer
                .device_exclusion_history()
                .load_acknowledgement(&reference, registration.value())
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
}

#[derive(Clone, Copy)]
enum OutcomeIntent {
    Exclude,
    Cancel,
}

enum DeviceExclusionPublicationProgress {
    Completed(StoreDeviceExclusionResult),
    Continue,
    ReplacementRequired(coven_protocol::remote_object::VerifiedCandidateNonactivation),
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

fn require_active_target(
    state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
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
    state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
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

impl From<StoreDeviceExclusionJournalError> for StoreDeviceExclusionError {
    fn from(error: StoreDeviceExclusionJournalError) -> Self {
        Self::Journal(error.to_string())
    }
}

#[cfg(test)]
mod tests;

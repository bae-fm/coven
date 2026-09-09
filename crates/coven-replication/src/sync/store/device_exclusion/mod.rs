//! Durable publication of Store-device exclusion proposals and outcomes.

mod history;
pub(crate) use history::DeviceExclusionHistory;

use coven_protocol::device_exclusion_journal::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};

use super::{AuthorizedWriterOperation, StoreError};
use crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use coven_database::DbError;
use coven_database::StoreDatabase;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    device_exclusion_outcome_semantic_prefix, device_exclusion_proposal_semantic_prefix,
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusionOutcome, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposalId, StoreDeviceExclusionProposalRef, StoreDeviceProposalState,
    StoreDeviceStatus, StoreProtocolError,
};
use coven_storage::CloudSyncObjectStorage;

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
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreDeviceExclusionOperationInfo {
    pub operation_id: ObjectHash,
    pub status: StoreDeviceExclusionOperationStatus,
}

#[cfg(any(test, feature = "test-utils"))]
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
    Object(#[from] coven_protocol::objects::StoreObjectError),
    #[error("Store-device exclusion protocol: {0}")]
    Protocol(#[from] StoreProtocolError),
    #[error("Store-device exclusion JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Store-device exclusion publication: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store-device exclusion storage: {0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("Store-device exclusion authority: {0}")]
    Membership(#[from] crate::sync::store::membership::MembershipMutationError),
    #[error("Store-device exclusion journal: {0}")]
    Journal(#[from] StoreDeviceExclusionJournalError),
    #[error("Store-device exclusion state is invalid: {0}")]
    InvalidState(String),
}

/// Propose exclusion of the active registration a Store device id names.
pub(crate) async fn propose_for_device(
    database: &StoreDatabase,
    writer: &mut AuthorizedWriterOperation<'_>,
    device_id: coven_protocol::store_commit::StoreDeviceId,
) -> Result<StoreDeviceExclusionProposalRef, StoreDeviceExclusionError> {
    let target = database
        .activated_store_device_registration_for_device(device_id)
        .await?
        .ok_or(StoreDeviceExclusionError::TargetNotActive)?;
    match writer
        .device_exclusion()
        .propose(target.reference())
        .await?
    {
        StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => Ok(proposal),
        other => Err(StoreDeviceExclusionError::InvalidState(format!(
            "proposal did not activate: {other:?}"
        ))),
    }
}

pub(crate) async fn cancel_proposal(
    writer: &mut AuthorizedWriterOperation<'_>,
    proposal: &StoreDeviceExclusionProposalRef,
) -> Result<(), StoreDeviceExclusionError> {
    match writer.device_exclusion().cancel(proposal).await? {
        StoreDeviceExclusionResult::OutcomeActivated { .. } => Ok(()),
        other => Err(StoreDeviceExclusionError::InvalidState(format!(
            "cancellation did not activate: {other:?}"
        ))),
    }
}

pub(crate) async fn finalize_proposal(
    writer: &mut AuthorizedWriterOperation<'_>,
    proposal: &StoreDeviceExclusionProposalRef,
) -> Result<(), StoreDeviceExclusionError> {
    match writer.device_exclusion().exclude(proposal).await? {
        StoreDeviceExclusionResult::OutcomeActivated { .. } => Ok(()),
        other => Err(StoreDeviceExclusionError::InvalidState(format!(
            "exclusion did not activate: {other:?}"
        ))),
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn operations_for_test(
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

/// Stage and upload one exclusion proposal against this device's own
/// registration, stopping before activation so a restart resumes it. The
/// target is the local device — which [`AuthorizedDeviceExclusion::propose`]
/// refuses — so the test enters the production pipeline one step below that
/// gate, at [`AuthorizedDeviceExclusion::stage_proposal`], under a fixed
/// proposal id.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn stage_uploaded_proposal_for_test(
    database: &StoreDatabase,
    writer: &mut AuthorizedWriterOperation<'_>,
) -> Result<StoreDeviceExclusionProposalRef, StoreDeviceExclusionError> {
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
    database
        .mark_store_device_exclusion_authority_uploaded(durable)
        .await?;
    Ok(reference)
}

pub(crate) struct AuthorizedDeviceExclusion<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
}

impl<'operation, 'storage> AuthorizedDeviceExclusion<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
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
            .create_verified_protocol_object(
                &context,
                operation.object().prepared(),
                prefix,
                &operation.object().semantic_bytes(),
            )
            .await
            .map_err(StoreDeviceExclusionJournalError::Storage)
    }

    pub(crate) async fn resume(
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

    pub(crate) async fn propose(
        &mut self,
        target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let database = self.database.clone();
        let _lock = database.device_exclusion_permit().await;
        self.reject_active_operation().await?;
        let durable = self.prepare_proposal(target).await?;
        self.drive(Box::new(durable)).await
    }

    pub(crate) async fn cancel(
        &mut self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        self.publish_outcome(proposal, OutcomeIntent::Cancel).await
    }

    pub(crate) async fn exclude(
        &mut self,
        proposal: &StoreDeviceExclusionProposalRef,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        self.publish_outcome(proposal, OutcomeIntent::Exclude).await
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
        plan: Box<crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan>,
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
        let transition = self
            .writer
            .prepare_authority_change(
                plan.membership(),
                coven_protocol::membership::StoreAuthorityChange::DeviceExclusionProposal {
                    proposal: reference.clone(),
                },
            )
            .await?;
        let mut candidate = Box::pin(self.writer.prepare_candidate(
            &plan,
            StoreOperationBatch::DeviceExclusionProposal {
                proposal: retained,
                transition: transition.transition.clone(),
            },
        ))
        .await?;
        let publication = self
            .writer
            .finish_store_membership_transition(transition, candidate.reference.clone())
            .await?;
        candidate
            .attach_merge_membership_proof_with(&publication, None)
            .map_err(StoreError::from)?;
        let operation = DurableStoreDeviceExclusionOperation::prepared(
            DurableStoreDeviceExclusionObject::Proposal {
                reference,
                value: proposal,
                prepared,
            },
            candidate,
        )?;
        let durable = Box::pin(database.begin_outbound_store_device_exclusion(operation)).await?;
        drop(plan);
        #[cfg(any(test, feature = "test-utils"))]
        database
            .reach_test_point(
                coven_database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
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
                StoreDeviceExclusionOutcome::Excluded(plan.sign_device_exclusion(
                    proposal_ref.clone(),
                    &proposal.object.value,
                    proposal_ref.target.clone(),
                    &proposal.target,
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
        let transition = self
            .writer
            .prepare_authority_change(
                plan.membership(),
                coven_protocol::membership::StoreAuthorityChange::DeviceExclusionOutcome {
                    outcome: reference.clone(),
                },
            )
            .await?;
        let mut candidate = Box::pin(self.writer.prepare_candidate(
            &plan,
            StoreOperationBatch::DeviceExclusionOutcome {
                outcome: retained,
                transition: transition.transition.clone(),
            },
        ))
        .await?;
        let publication = self
            .writer
            .finish_store_membership_transition(transition, candidate.reference.clone())
            .await?;
        candidate
            .attach_merge_membership_proof_with(&publication, None)
            .map_err(StoreError::from)?;
        let operation = DurableStoreDeviceExclusionOperation::prepared(
            DurableStoreDeviceExclusionObject::Outcome {
                reference,
                value: outcome,
                prepared,
            },
            candidate,
        )?;
        let durable = Box::pin(database.begin_outbound_store_device_exclusion(operation)).await?;
        drop(plan);
        #[cfg(any(test, feature = "test-utils"))]
        database
            .reach_test_point(
                coven_database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
            )
            .await;
        Ok(durable)
    }

    async fn drive(
        &mut self,
        operation: Box<DurableStoreDeviceExclusionOperation>,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        if operation.is_completed() {
            return completion_result(&operation);
        }
        if let Some(result) = self.ensure_authority_uploaded(&operation).await? {
            return Ok(result);
        }
        self.publish_candidate(&operation).await
    }

    async fn publish_candidate(
        &mut self,
        operation: &DurableStoreDeviceExclusionOperation,
    ) -> Result<StoreDeviceExclusionResult, StoreDeviceExclusionError> {
        let candidate = operation.candidate().cloned().ok_or_else(|| {
            StoreDeviceExclusionError::InvalidState(
                "active exclusion operation has no activation candidate".to_string(),
            )
        })?;
        let publication = candidate
            .prepared_membership_publication()
            .map_err(StoreError::from)?;
        let transition = publication.transition();
        self.writer
            .publish_membership_authority(&transition, &[])
            .await?;
        let completion = coven_protocol::membership_mutation::StoreMembershipJournalCompletion::DeviceExclusion {
            operation: Box::new(operation.clone()),
            remote_objects: operation.remote_objects()?.into_iter().map(|object| object.record().clone()).collect(),
        };
        self.database
            .mark_remote_object_uploaded(
                completion
                    .remote_object(&publication.entry_ref.object)
                    .map_err(crate::sync::store::membership::MembershipMutationError::from)?,
            )
            .await?;
        self.writer
            .publish_membership_activation(
                &transition,
                &publication,
                Box::new(candidate),
                completion,
            )
            .await?;
        completion_result(&operation.activated()?)
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
        let unverified: StoreDeviceExclusionOutcome = serde_json::from_slice(&bytes)?;
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
}

#[derive(Clone, Copy)]
enum OutcomeIntent {
    Exclude,
    Cancel,
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

#[cfg(test)]
mod completion_tests;
#[cfg(test)]
mod recovery_authority_tests;
#[cfg(test)]
mod snapshot_authority_tests;
#[cfg(test)]
mod tests;

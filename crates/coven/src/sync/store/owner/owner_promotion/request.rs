use crate::protocol::store_commit::{
    OwnerPromotionFinalization, OwnerPromotionId, OwnerPromotionRequest,
    OwnerPromotionRequestActivation, StoreDeviceRegistrationRef,
};
use crate::sync::store::operations::{StoreOperationBatch, StoreOperationPublicationOutcome};

use super::authority::target_key;
use super::journal::{
    advance_owner_promotion_journal, OwnerPromotionJournal, OwnerPromotionJournalPredecessor,
    OwnerPromotionJournalState,
};
use super::OwnerPromotionError;

async fn resume_request_publication(
    operation: &mut super::AuthorizedOwnerPromotion<'_, '_>,
    journal: OwnerPromotionJournal,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    let (previous, state) = journal.into_predecessor()?;
    resume_request_publication_state(operation, previous, state).await
}

async fn resume_request_publication_state(
    operation: &mut super::AuthorizedOwnerPromotion<'_, '_>,
    mut previous: OwnerPromotionJournalPredecessor,
    mut state: OwnerPromotionJournalState,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    loop {
        match state {
            OwnerPromotionJournalState::Allocated => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion request allocation has not been prepared".to_string(),
                ));
            }
            OwnerPromotionJournalState::RequestPrepared { request, candidate } => {
                let head = candidate.head_ref();
                let outcome = operation
                    .writer
                    .publish_prepared(candidate, None, None)
                    .await?;
                let next_state = match outcome {
                    StoreOperationPublicationOutcome::Activated(commit) => {
                        let activation = OwnerPromotionRequestActivation { commit, head };
                        OwnerPromotionJournalState::AwaitingAcceptance {
                            request,
                            activation,
                        }
                    }
                    StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
                        OwnerPromotionJournalState::RequestPrepared { request, candidate }
                    }
                    StoreOperationPublicationOutcome::NonactivatedCandidate {
                        nonactivation,
                        ..
                    } => OwnerPromotionJournalState::Nonactivated {
                        request,
                        nonactivation: nonactivation.into_durable(),
                    },
                    StoreOperationPublicationOutcome::Nonactivated(_) => {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion request lost without exact nonactivation evidence"
                                .to_string(),
                        ));
                    }
                    StoreOperationPublicationOutcome::Reprepared => {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion request unexpectedly used acknowledgement reprepare"
                                .to_string(),
                        ));
                    }
                };
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: next_state,
                };
                let database = operation.database.clone();
                (previous, state) =
                    advance_owner_promotion_journal(&database, previous, next).await?;
            }
            OwnerPromotionJournalState::AwaitingAcceptance { request, .. }
            | OwnerPromotionJournalState::Nonactivated { request, .. } => return Ok(request),
            OwnerPromotionJournalState::AcceptanceReady { acceptance }
            | OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
            | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
            | OwnerPromotionJournalState::Finalized { acceptance, .. }
            | OwnerPromotionJournalState::Stale { acceptance, .. } => {
                return Ok(*acceptance.request);
            }
        }
    }
}

/// Publish a request that promotes one exact active member device to Owner.
impl super::AuthorizedOwnerPromotion<'_, '_> {
    pub(crate) async fn begin(
        &mut self,
        member_registration: StoreDeviceRegistrationRef,
    ) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
        let operation = self;
        let database = operation.database.clone();
        let db = &database;
        let (allocated, failed_attempt) = if let Some(existing) = database
            .load_owner_promotion_target(target_key(&member_registration)?)
            .await?
        {
            if matches!(&existing.state, OwnerPromotionJournalState::Allocated) {
                (Some(existing), None)
            } else if matches!(
                &existing.state,
                OwnerPromotionJournalState::Nonactivated { .. }
                    | OwnerPromotionJournalState::Stale { .. }
            ) {
                (None, Some(existing))
            } else {
                return resume_request_publication(operation, existing).await;
            }
        } else {
            (None, None)
        };
        let member = operation
            .writer
            .owner_promotion_history()
            .load_registration(&member_registration)
            .await
            .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let plan = operation.writer.prepare_plan().await?;
        let member_grant = operation.exact_member_grant(&member.value.author_pubkey)?;
        let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
            OwnerPromotionError::Protocol("promotion author is not an Owner".to_string())
        })?;
        let reusable = operation
            .membership
            .reusable_author_streams(&plan.registration().author_pubkey, &owner_grant);
        let author_stream = database
            .select_membership_author_stream(
                &plan.registration().author_pubkey,
                &owner_grant,
                reusable,
            )
            .await?;
        let (seq, previous_hash) = operation
            .membership
            .next_stream_position(
                &plan.registration().author_pubkey,
                &owner_grant,
                author_stream,
            )
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let finalization = OwnerPromotionFinalization {
            author_stream,
            seq,
            previous_hash,
        };
        let allocation = match allocated {
            Some(allocation) => allocation,
            None => {
                let allocation = OwnerPromotionJournal {
                    promotion_id: OwnerPromotionId::from_generated(
                        db.new_store_write_id().to_string(),
                    ),
                    target: member_registration.clone(),
                    state: OwnerPromotionJournalState::Allocated,
                };
                match failed_attempt {
                    Some(previous) => {
                        // The attempt being replaced may still owe deletions its
                        // own retry would have finished. Nothing else names its
                        // objects once this journal is gone, and its membership
                        // slots are the ones this attempt composes into.
                        if let OwnerPromotionJournalState::Stale { evidence, .. } = &previous.state
                        {
                            super::finalization::finish_stale_owner_promotion_cleanup(
                                &database,
                                operation.storage.as_ref(),
                                evidence,
                            )
                            .await?;
                        }
                        database
                            .replace_failed_owner_promotion_journal(previous, allocation)
                            .await?
                    }
                    None => {
                        database
                            .begin_owner_promotion_journal(
                                target_key(&allocation.target)?,
                                allocation,
                            )
                            .await?
                    }
                }
            }
        };
        let promotion_id = allocation.promotion_id;
        let request = plan.sign_owner_promotion_request(
            promotion_id,
            member_registration.clone(),
            member.value.author_pubkey,
            member_grant,
            finalization,
            &operation.identity,
        )?;
        let candidate = operation
            .writer
            .prepare_candidate(
                plan,
                StoreOperationBatch::OwnerPromotionRequest(request.clone()),
            )
            .await?;
        let prepared = OwnerPromotionJournal {
            promotion_id,
            target: member_registration,
            state: OwnerPromotionJournalState::RequestPrepared {
                request: request.clone(),
                candidate: Box::new(candidate),
            },
        };
        let (previous, _) = allocation.into_predecessor()?;
        let (previous, state) =
            advance_owner_promotion_journal(&database, previous, prepared).await?;
        resume_request_publication_state(operation, previous, state).await
    }
}

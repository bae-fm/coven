use crate::keys::UserKeypair;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::operations::{StoreOperationBatch, StoreOperationPublicationOutcome};
use crate::sync::store::Store;
use crate::sync::store_commit::{
    OwnerPromotionFinalization, OwnerPromotionId, OwnerPromotionRequest,
    OwnerPromotionRequestActivation, StoreDeviceRegistrationRef,
};

use super::authority::{exact_merge_member_grant, target_key};
use super::journal::{
    advance_owner_promotion_journal, OwnerPromotionJournal, OwnerPromotionJournalPredecessor,
    OwnerPromotionJournalState,
};
use super::OwnerPromotionError;

async fn resume_request_publication(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    journal: OwnerPromotionJournal,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    let (previous, state) = journal.into_predecessor()?;
    resume_request_publication_state(database, history_verifier, previous, state).await
}

async fn resume_request_publication_state(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
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
                // Scoped to the publication alone: the arms below re-derive a
                // plan, which takes this same turn.
                let outcome = {
                    let _authorship = database.author_own_stream().await;
                    crate::sync::store::operations::publish_prepared_with_history(
                        database,
                        history_verifier,
                        candidate,
                        None,
                        None,
                    )
                    .await?
                };
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
                (previous, state) =
                    advance_owner_promotion_journal(database, previous, next).await?;
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

impl Store {
    /// Publish a request that promotes one exact active member device to Owner.
    pub async fn begin_owner_promotion(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        member_registration: StoreDeviceRegistrationRef,
    ) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
        let mut authorization = self
            .authorize()
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let authority = authorization.operation_authority();
        let database = authority.database;
        let history_verifier = authority.history_verifier;
        let membership = &*authority.membership;
        let storage = history_verifier.storage();
        let db = database.sqlite();
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
                return resume_request_publication(database, history_verifier, existing).await;
            }
        } else {
            (None, None)
        };
        let root = history_verifier.root().clone();
        let member =
            crate::sync::store_objects::load_registration_ref(storage, &root, &member_registration)
                .await
                .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let plan = crate::sync::store::operations::prepare_plan_with_history(
            database,
            history_verifier,
            membership,
            device_id,
            identity,
        )
        .await?;
        let member_grant = exact_merge_member_grant(membership, &member.value.author_pubkey)?;
        let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
            OwnerPromotionError::Protocol("promotion author is not an Owner".to_string())
        })?;
        let reusable =
            membership.reusable_author_streams(&plan.registration().author_pubkey, &owner_grant);
        let author_stream = database
            .select_membership_author_stream(
                &plan.registration().author_pubkey,
                &owner_grant,
                reusable,
            )
            .await?;
        let (seq, previous_hash) = membership
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
                    promotion_id: OwnerPromotionId::from_generated(db.new_write_id().to_string()),
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
                                database, storage, evidence,
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
            identity,
        )?;
        let candidate = crate::sync::store::operations::prepare_candidate(
            database,
            storage,
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
            advance_owner_promotion_journal(database, previous, prepared).await?;
        resume_request_publication_state(database, history_verifier, previous, state).await
    }
}

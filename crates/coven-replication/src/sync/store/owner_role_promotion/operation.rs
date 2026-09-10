use crate::sync::store::commit_publication::operation::commit_plan::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationCommitPlan,
};
use coven_keys::encryption::EncryptionService;
use coven_protocol::circle_control::StoreMembershipStateRef;
use coven_protocol::membership::StoreMembershipRoleGrant;
use coven_protocol::membership_mutation::PreparedMembershipTransition;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors,
    OwnerPromotionFinalization as OwnerPromotionFinalizationPoint, OwnerPromotionId,
    OwnerPromotionRequest, OwnerPromotionStaleReason, StoreDeviceRegistrationRef, StreamActivation,
    StreamAnchorDomain,
};
use coven_protocol::wrapped_store_key::PreparedWrappedStoreKey;

use super::journal::target_key;
use super::journal::{
    OwnerPromotionJournal, OwnerPromotionJournalPredecessor, OwnerPromotionJournalState,
    OwnerPromotionStaleEvidence,
};
use super::OwnerPromotionError;

pub(crate) struct AuthorizedOwnerPromotion<'operation, 'storage> {
    writer: &'operation mut crate::sync::store::AuthorizedWriterOperation<'storage>,
    database: coven_database::StoreDatabase,
    storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage>,
    root: coven_protocol::store_commit::StoreRootRef,
}

impl<'operation, 'storage> AuthorizedOwnerPromotion<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut crate::sync::store::AuthorizedWriterOperation<'storage>,
        database: coven_database::StoreDatabase,
        storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage>,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
        }
    }

    async fn delete_candidate_objects(
        &self,
        targets: Vec<coven_database::CandidateCleanupObject>,
    ) -> Result<(), OwnerPromotionError> {
        crate::sync::store::authorization::delete_candidate_cleanup_targets(
            self.storage.as_ref(),
            targets,
        )
        .await
    }

    async fn finish_retired_cleanup(
        &self,
        promotion_id: OwnerPromotionId,
        _authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<(), OwnerPromotionError> {
        let journal = self
            .database
            .load_owner_promotion_journal(promotion_id)
            .await?
            .ok_or(OwnerPromotionError::NotFound(promotion_id))?;
        if matches!(&journal.state, OwnerPromotionJournalState::Stale { evidence, .. }
            if matches!(evidence.as_ref(), OwnerPromotionStaleEvidence::BeforePublication))
        {
            return Ok(());
        }
        let _upload = self.database.blob_upload_drain_permit().await;
        let _snapshot = self.database.snapshot_publication_permit().await;
        let (journal, targets) = self
            .database
            .owner_promotion_retirement_targets(journal)
            .await?;
        self.delete_candidate_objects(targets).await?;
        self.database
            .complete_owner_promotion_retirement(journal)
            .await?;
        Ok(())
    }

    async fn retire_candidate_with_lost_authority(
        &mut self,
        promotion_id: OwnerPromotionId,
        authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<(), OwnerPromotionError> {
        self.writer.refresh_membership_publication().await?;
        let journal = self
            .database
            .load_owner_promotion_journal(promotion_id)
            .await?
            .ok_or(OwnerPromotionError::NotFound(promotion_id))?;
        let candidate = match &journal.state {
            OwnerPromotionJournalState::RequestPrepared { candidate, .. }
            | OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } => candidate,
            _ => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion retirement preflight lost its prepared candidate".into(),
                ))
            }
        };
        let retirement = self
            .writer
            .owner_promotion_history()
            .candidate_grant_retirement(candidate)
            .await?;
        if let Some((membership, publication)) = retirement {
            let retired = self
                .database
                .retire_owner_promotion_candidate_authority(journal, membership, publication)
                .await?;
            self.finish_retired_cleanup(promotion_id, authorship)
                .await?;
            return match retired.state {
                OwnerPromotionJournalState::Nonactivated { .. } => {
                    Err(OwnerPromotionError::RequestNotActivated)
                }
                OwnerPromotionJournalState::Stale { reason, .. } => {
                    Err(OwnerPromotionError::Stale(Box::new(reason)))
                }
                _ => Err(OwnerPromotionError::Protocol(
                    "promotion retirement did not retain its terminal outcome".into(),
                )),
            };
        }
        Ok(())
    }

    fn exact_member_grant(
        membership: &coven_protocol::membership::MembershipChain,
        member_pubkey: &str,
    ) -> Result<coven_protocol::membership::MembershipGrantId, OwnerPromotionError> {
        let grants = membership.active_grant_ids(member_pubkey);
        let Some(grant) = grants.iter().next() else {
            return Err(OwnerPromotionError::Protocol(
                "promotion target has no active Member grant".to_string(),
            ));
        };
        if grants.len() != 1
            || membership.active_grant(grant).is_none_or(|record| {
                record.role != coven_protocol::membership::StoreMembershipRoleGrant::Member
            })
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion target does not have exactly one active Member grant".to_string(),
            ));
        }
        Ok(grant.clone())
    }

    async fn advance_journal(
        &self,
        previous: OwnerPromotionJournalPredecessor,
        next: OwnerPromotionJournal,
    ) -> Result<(OwnerPromotionJournalPredecessor, OwnerPromotionJournalState), OwnerPromotionError>
    {
        let transition = previous.transition_to(&next)?;
        let (successor, state) = next.into_predecessor()?;
        self.database
            .advance_owner_promotion_journal(transition)
            .await?;
        Ok((successor, state))
    }

    /// Accept an activated promotion request on the exact member device it names.
    pub(crate) async fn accept(
        &mut self,
        request: OwnerPromotionRequest,
    ) -> Result<OwnerPromotionAcceptance, OwnerPromotionError> {
        let store_db = self.database.clone();
        if let Some(existing) = store_db
            .load_owner_promotion_journal(request.promotion_id)
            .await?
        {
            if let OwnerPromotionJournalState::AcceptanceReady { acceptance }
            | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
            | OwnerPromotionJournalState::Finalized { acceptance, .. }
            | OwnerPromotionJournalState::Stale { acceptance, .. } = existing.state
            {
                if acceptance.request.as_ref() == &request {
                    return Ok(acceptance);
                }
            }
            return Err(OwnerPromotionError::Protocol(
                "promotion id is already bound to another journal state".to_string(),
            ));
        }
        if !self
            .writer
            .matches_local_author(&request.member_registration, &request.member_pubkey)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion request targets another local device".to_string(),
            ));
        }
        let verified_activation = self
            .writer
            .owner_promotion_history()
            .find_request_activation(&request)
            .await
            .map_err(OwnerPromotionError::from)?;
        let root = self.root.clone();
        let membership_stream = self.writer.grant_authorized_stream_id(
            &request.intended_owner_grant,
            StreamAnchorDomain::StoreMembership,
        );
        let membership_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let membership_prefix = membership_head_slot_prefix(
            &request.member_pubkey,
            &request.intended_owner_grant,
            membership_stream,
            1,
        );
        let membership_slot = self
            .storage
            .allocate_protocol_slot(&membership_context, &membership_prefix, ".json")
            .await?;
        let recovery_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let recovery_prefix = owner_recovery_semantic_prefix(
            &request.member_pubkey,
            request.intended_owner_grant.clone(),
            1,
        );
        let recovery_slot = self
            .storage
            .allocate_protocol_slot(&recovery_context, &recovery_prefix, ".json")
            .await?;
        let anchors = OwnerPromotionAnchors {
            membership: GrantStreamAnchor::StoreMembership {
                first_slot: membership_slot,
            },
            recovery: GrantStreamAnchor::OwnerRecovery {
                first_slot: recovery_slot,
            },
        };
        let acceptance = self
            .writer
            .sign_owner_promotion_acceptance(
                request.clone(),
                verified_activation.activation().clone(),
                anchors,
            )
            .map_err(OwnerPromotionError::from)?;
        self.writer
            .owner_promotion_history()
            .verify_acceptance_from_request(&acceptance, verified_activation)
            .await
            .map_err(OwnerPromotionError::from)?;
        let journal = OwnerPromotionJournal {
            promotion_id: request.promotion_id,
            target: request.member_registration.clone(),
            state: OwnerPromotionJournalState::AcceptanceReady {
                acceptance: acceptance.clone(),
            },
        };
        store_db
            .begin_owner_promotion_acceptance_journal(journal)
            .await?;
        Ok(acceptance)
    }

    async fn resume_request_publication_state(
        &mut self,
        mut previous: OwnerPromotionJournalPredecessor,
        mut state: OwnerPromotionJournalState,
        authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
        loop {
            match state {
                OwnerPromotionJournalState::Allocated => {
                    return Err(OwnerPromotionError::Protocol(
                        "promotion request allocation has not been prepared".to_string(),
                    ));
                }
                OwnerPromotionJournalState::RequestPrepared { request, candidate } => {
                    self.retire_candidate_with_lost_authority(previous.promotion_id, authorship)
                        .await?;
                    let accepted = self
                        .writer
                        .publish_prepared(candidate.clone(), None, None)
                        .await?;
                    let exact = accepted.exact_publication().ok_or_else(|| {
                        OwnerPromotionError::Protocol(
                            "promotion request publication was retired before its activation was retained".into(),
                        )
                    })?;
                    let value = self
                        .writer
                        .sign_owner_promotion_request_publication(&candidate.commit, exact)?;
                    let context = ProtocolObjectContext::signed_plaintext(
                        self.root.store_root_hash,
                        ProtocolObjectDomain::OwnerPromotionRequestPublication,
                    );
                    let prefix = coven_protocol::store_commit::owner_promotion_request_publication_semantic_prefix(request.promotion_id);
                    let prepared = self.storage.prepare_protocol_object(
                        &context,
                        request.publication_slot.clone(),
                        &prefix,
                        value.to_bytes(),
                    )?;
                    let next = OwnerPromotionJournal {
                        promotion_id: previous.promotion_id,
                        target: previous.target.clone(),
                        state: OwnerPromotionJournalState::RequestAccepted {
                            request,
                            candidate,
                            publication: coven_protocol::store_commit::RetainedOwnerPromotionRequestPublication {
                                value,
                                object: prepared.reference().clone(),
                            },
                        },
                    };
                    let transition = previous.transition_to(&next)?;
                    let successor = next.into_predecessor()?;
                    self.database
                        .advance_accepted_owner_promotion_request(transition, accepted)
                        .await?;
                    (previous, state) = successor;
                }
                OwnerPromotionJournalState::RequestAccepted {
                    request,
                    candidate,
                    publication,
                } => {
                    let context = ProtocolObjectContext::signed_plaintext(
                        self.root.store_root_hash,
                        ProtocolObjectDomain::OwnerPromotionRequestPublication,
                    );
                    let prefix = coven_protocol::store_commit::owner_promotion_request_publication_semantic_prefix(request.promotion_id);
                    let prepared = self.storage.prepare_protocol_object(
                        &context,
                        request.publication_slot.clone(),
                        &prefix,
                        publication.value.to_bytes(),
                    )?;
                    if prepared.reference() != &publication.object {
                        return Err(OwnerPromotionError::Protocol(
                            "request publication changed its exact object on restart".into(),
                        ));
                    }
                    self.storage.create_protocol_object(&prepared).await?;
                    let loaded = self
                        .writer
                        .owner_promotion_history()
                        .load_request_publication(&candidate.commit)
                        .await?;
                    if loaded != publication {
                        return Err(OwnerPromotionError::Protocol(
                            "request publication slot contains another accepted result".into(),
                        ));
                    }
                    let remote = coven_protocol::remote_object::RemoteObjectRecord::prepared_owner_promotion_request_publication(
                        &publication,
                        &candidate.commit,
                    ).map_err(crate::sync::store::StoreError::from)?;
                    self.database
                        .mark_remote_object_uploaded(remote.into_record())
                        .await?;
                    let next = OwnerPromotionJournal {
                        promotion_id: previous.promotion_id,
                        target: previous.target.clone(),
                        state: OwnerPromotionJournalState::AwaitingAcceptance {
                            request,
                            activation: (*publication.value).clone(),
                        },
                    };
                    (previous, state) = self.advance_journal(previous, next).await?;
                }
                OwnerPromotionJournalState::AwaitingAcceptance { request, .. } => {
                    return Ok(request)
                }
                OwnerPromotionJournalState::Nonactivated { .. } => {
                    self.finish_retired_cleanup(previous.promotion_id, authorship)
                        .await?;
                    return Err(OwnerPromotionError::RequestNotActivated);
                }
                OwnerPromotionJournalState::AcceptanceReady { acceptance }
                | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
                | OwnerPromotionJournalState::Finalized { acceptance, .. }
                | OwnerPromotionJournalState::Stale { acceptance, .. } => {
                    return Ok(acceptance.request.as_ref().clone());
                }
            }
        }
    }

    /// Publish a request that promotes one exact active member device to Owner.
    pub(crate) async fn begin(
        &mut self,
        member_registration: StoreDeviceRegistrationRef,
    ) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
        let operation = self;
        let database = operation.database.clone();
        let authorship = database.author_own_stream().await;
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
                operation
                    .finish_retired_cleanup(existing.promotion_id, &authorship)
                    .await?;
                (None, Some(existing))
            } else {
                let (previous, state) = existing.into_predecessor()?;
                return operation
                    .resume_request_publication_state(previous, state, &authorship)
                    .await;
            }
        } else {
            (None, None)
        };
        let member = operation
            .writer
            .owner_promotion_history()
            .load_registration(&member_registration)
            .await
            .map_err(OwnerPromotionError::from)?;
        operation.writer.refresh_membership_publication().await?;
        let plan = operation
            .writer
            .prepare_plan_with_authorship(authorship)
            .await?;
        let member_grant =
            Self::exact_member_grant(plan.membership(), &member.value.author_pubkey)?;
        let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
            OwnerPromotionError::Protocol("promotion author is not an Owner".to_string())
        })?;
        let author_pubkey = plan.author_pubkey();
        let reusable = plan
            .membership()
            .reusable_author_streams(&author_pubkey, &owner_grant);
        let author_stream = database
            .select_membership_author_stream(&author_pubkey, &owner_grant, reusable)
            .await?;
        let (seq, previous_hash) = plan
            .membership()
            .next_stream_position(&author_pubkey, &owner_grant, author_stream)
            .map_err(OwnerPromotionError::from)?;
        let finalization = OwnerPromotionFinalizationPoint {
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
        let publication_context = ProtocolObjectContext::signed_plaintext(
            operation.root.store_root_hash,
            ProtocolObjectDomain::OwnerPromotionRequestPublication,
        );
        let publication_prefix =
            coven_protocol::store_commit::owner_promotion_request_publication_semantic_prefix(
                promotion_id,
            );
        let publication_slot = operation
            .storage
            .allocate_protocol_slot(&publication_context, &publication_prefix, ".json")
            .await?;
        let request = plan.sign_owner_promotion_request(
            promotion_id,
            member_registration.clone(),
            member.value.author_pubkey.clone(),
            member_grant,
            finalization,
            publication_slot,
        )?;
        let candidate = operation
            .writer
            .prepare_candidate(
                &plan,
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
        let (previous, state) = operation.advance_journal(previous, prepared).await?;
        let authorship = plan.into_authorship();
        operation
            .resume_request_publication_state(previous, state, &authorship)
            .await
    }

    /// Activate the accepted promotion through Store membership and recovery state.
    pub(crate) async fn finalize(
        &mut self,
        encryption: &EncryptionService,
        acceptance: OwnerPromotionAcceptance,
    ) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        if !self
            .writer
            .is_local_registration(&acceptance.request.promoter_registration)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        let authorship = self.database.author_own_stream().await;
        self.resume(encryption, &acceptance, authorship).await
    }

    /// Compose membership authority and its activating Store candidate from one accepted plan.
    /// Advancing the journal to `MergeHeadPrepared` reserves that candidate's
    /// publication atomically, so another local operation cannot take its position.
    async fn prepare_merge_store_candidate(
        &mut self,
        journal: &OwnerPromotionJournalPredecessor,
        plan: &StoreOperationCommitPlan,
        acceptance: &OwnerPromotionAcceptance,
        wrapped_key: PreparedWrappedStoreKey,
        transition: PreparedMembershipTransition,
    ) -> Result<OwnerPromotionJournal, OwnerPromotionError> {
        let acceptance = acceptance.clone();
        let root = self.root.clone();
        let OwnerPromotionAnchors {
            membership: membership_anchor,
            recovery,
        } = &acceptance.anchors;
        let mut stream_activations = vec![
            StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                membership_anchor.clone(),
            ),
            StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                recovery.clone(),
            ),
        ];
        stream_activations.sort();
        let mut candidate = self
            .writer
            .prepare_candidate(
                plan,
                StoreOperationBatch::MergeMembershipActivation {
                    transition: transition.transition.clone(),
                    stream_activations,
                },
            )
            .await?;
        let publication = self
            .writer
            .finish_store_membership_transition(transition, candidate.reference.clone())
            .await
            .map_err(OwnerPromotionError::from)?;
        candidate
            .attach_merge_membership_proof(&publication)
            .map_err(crate::sync::store::StoreError::from)
            .map_err(OwnerPromotionError::from)?;
        Ok(OwnerPromotionJournal {
            promotion_id: journal.promotion_id,
            target: journal.target.clone(),
            state: OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance,
                wrapped_key,
                candidate: Box::new(candidate),
            },
        })
    }

    async fn prepare(
        &mut self,
        journal: OwnerPromotionJournalPredecessor,
        encryption: &EncryptionService,
        acceptance: &OwnerPromotionAcceptance,
        authorship: coven_database::OwnStreamAuthorship,
    ) -> Result<
        (
            OwnerPromotionJournalPredecessor,
            OwnerPromotionJournalState,
            coven_database::OwnStreamAuthorship,
        ),
        OwnerPromotionError,
    > {
        let acceptance = acceptance.clone();
        let operation = &mut *self;
        let author_stream = acceptance.request.finalization.author_stream;
        let seq = acceptance.request.finalization.seq;
        let database = operation.database.clone();
        let root = operation.root.clone();
        let db = &database;
        let promoter_pubkey = operation.writer.local_author_pubkey();
        if !operation
            .writer
            .is_local_registration(&acceptance.request.promoter_registration)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        operation.writer.refresh_membership_publication().await?;
        let plan = operation
            .writer
            .prepare_plan_with_authorship(authorship)
            .await?;
        let membership = plan.membership().clone();
        if let Some(winner) = membership.head_refs().iter().find(|head| {
            head.coord.author_pubkey == promoter_pubkey
                && head.coord.author_owner_grant == acceptance.request.promoter_owner_grant
                && head.coord.stream_id == author_stream
                && head.coord.seq >= seq
        }) {
            let reason = OwnerPromotionStaleReason::MergeFinalizationPointOccupied {
                winner: winner.clone(),
            };
            let next = OwnerPromotionJournal {
                promotion_id: journal.promotion_id,
                target: journal.target.clone(),
                state: OwnerPromotionJournalState::Stale {
                    acceptance,
                    reason: reason.clone(),
                    evidence: Box::new(OwnerPromotionStaleEvidence::BeforePublication),
                },
            };
            operation.advance_journal(journal, next).await?;
            return Err(OwnerPromotionError::Stale(Box::new(reason)));
        }
        let recipient = &acceptance.request.member_pubkey;
        let wrapped_key = operation
            .writer
            .prepare_member_wrapped_key(&membership, encryption, recipient)
            .await
            .map_err(OwnerPromotionError::from)?;
        let candidate = operation
            .writer
            .owner_promotion_history()
            .load_registration(&acceptance.request.member_registration)
            .await
            .map_err(OwnerPromotionError::from)?;
        let entry = operation
            .writer
            .sign_finalize_owner_promotion(
                &membership,
                &root,
                &candidate.value,
                acceptance.clone(),
                wrapped_key.reference.clone(),
                db.stamp(),
            )
            .map_err(OwnerPromotionError::from)?;
        let transition = operation
            .writer
            .prepare_membership_transition(&membership, entry)
            .await
            .map_err(OwnerPromotionError::from)?;
        let next = operation
            .prepare_merge_store_candidate(&journal, &plan, &acceptance, wrapped_key, transition)
            .await?;
        #[cfg(any(test, feature = "test-utils"))]
        operation
            .database
            .reach_test_point(coven_database::DatabaseTestPoint::OwnerPromotionCandidatePrepared)
            .await;
        let (previous, state) = operation.advance_journal(journal, next).await?;
        Ok((previous, state, plan.into_authorship()))
    }

    async fn activate_merge_head(
        &mut self,
        previous: &OwnerPromotionJournalPredecessor,
        acceptance: OwnerPromotionAcceptance,
        wrapped_key: PreparedWrappedStoreKey,
        candidate: Box<PreparedStoreOperationCommit>,
        authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        self.retire_candidate_with_lost_authority(previous.promotion_id, authorship)
            .await?;
        let operation = &mut *self;
        let publication = candidate.prepared_membership_publication()?;
        let candidate_ref = candidate.reference.clone();
        let candidate_commit = &candidate.commit;
        let remote_objects = candidate
            .merge_membership_activation_remote_objects(std::slice::from_ref(&wrapped_key))?;
        operation
            .writer
            .publish_membership_authority(&candidate, &remote_objects)
            .await
            .map_err(OwnerPromotionError::from)?;
        let predecessor = &candidate_commit.membership_state;
        let mut membership = operation
            .writer
            .owner_promotion_history()
            .load_membership(&predecessor.heads)
            .await
            .map_err(OwnerPromotionError::from)?;
        let resolved = membership.resolved();
        let exact_predecessor = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            candidate_commit.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(OwnerPromotionError::from)?;
        if exact_predecessor != candidate_commit.membership_state {
            return Err(OwnerPromotionError::Protocol(
                "Owner promotion candidate membership differs from its exact predecessor"
                    .to_string(),
            ));
        }
        membership
            .add_entry(publication.entry.clone())
            .and_then(|()| membership.activate_head_ref(publication.head_ref.clone()))
            .map_err(OwnerPromotionError::from)?;
        let resolved = membership.resolved();
        let coven_protocol::membership::StoreAuthorityChange::SetMember {
            user_pubkey,
            role:
                StoreMembershipRoleGrant::Owner {
                    recovery:
                        coven_protocol::membership::OwnerRecoveryAnchorRef::Promotion {
                            acceptance: promotion_acceptance,
                        },
                },
            grant_id,
            ..
        } = &publication.entry.change
        else {
            return Err(OwnerPromotionError::Protocol(
                "Merge Owner promotion entry does not add an Owner recovery stream".to_string(),
            ));
        };
        let mut recovery = candidate_commit.device_state.recovery().to_vec();
        if recovery
            .iter()
            .any(|cursor| &cursor.owner_grant == grant_id)
        {
            return Err(OwnerPromotionError::Protocol(
                "Merge Owner promotion recovery stream already exists".to_string(),
            ));
        }
        recovery.push(coven_protocol::store_commit::OwnerRecoveryCursor {
            owner_grant: grant_id.clone(),
            position: coven_protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                activation: coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
                    &operation.root,
                    user_pubkey,
                    grant_id,
                    promotion_acceptance.anchors.recovery(),
                )
                .map_err(OwnerPromotionError::from)?,
            },
        });
        let membership = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            recovery,
            resolved.state_hash,
        )
        .map_err(OwnerPromotionError::from)?;
        if membership
            .heads
            .binary_search(&publication.head_ref)
            .is_err()
        {
            return Err(OwnerPromotionError::Protocol(
                "activated promotion head is absent from current membership".to_string(),
            ));
        }
        let finalized = OwnerPromotionJournal {
            promotion_id: previous.promotion_id,
            target: previous.target.clone(),
            state: OwnerPromotionJournalState::Finalized {
                acceptance,
                membership: membership.clone(),
                candidate: candidate.clone(),
            },
        };
        let journal_transition = previous.transition_to(&finalized)?;
        let accepted = operation
            .writer
            .publish_membership_activation_with_authorship(
                candidate,
                coven_protocol::membership_mutation::StoreMembershipJournalCompletion::OwnerPromotion {
                    transition: journal_transition,
                    remote_objects: remote_objects
                        .into_iter()
                        .map(|remote| remote.into_record())
                        .collect(),
                },
                authorship,
            )
            .await
            .map_err(OwnerPromotionError::from)?;
        if accepted != candidate_ref {
            return Err(OwnerPromotionError::Protocol(
                "Merge promotion accepted another prepared candidate".to_string(),
            ));
        }
        Ok(membership)
    }

    async fn resume(
        &mut self,
        encryption: &EncryptionService,
        acceptance: &OwnerPromotionAcceptance,
        mut authorship: coven_database::OwnStreamAuthorship,
    ) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        let acceptance = acceptance.clone();
        let database = self.database.clone();
        let existing = database
            .load_owner_promotion_journal(acceptance.request.promotion_id)
            .await?;
        let journal = existing.ok_or(OwnerPromotionError::NotFound(
            acceptance.request.promotion_id,
        ))?;
        if journal.target != acceptance.request.member_registration {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal targets another registration".to_string(),
            ));
        }
        match &journal.state {
            OwnerPromotionJournalState::AwaitingAcceptance {
                request: persisted, ..
            } if persisted != acceptance.request.as_ref() => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion finalization differs from its persisted request".to_string(),
                ));
            }
            OwnerPromotionJournalState::AcceptanceReady {
                acceptance: persisted,
            }
            | OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance: persisted,
                ..
            }
            | OwnerPromotionJournalState::Finalized {
                acceptance: persisted,
                ..
            }
            | OwnerPromotionJournalState::Stale {
                acceptance: persisted,
                ..
            } if persisted != &acceptance => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion finalization differs from its persisted acceptance".to_string(),
                ));
            }
            _ => {}
        }
        // New finalization preparation consumes the request's live or retained
        // acceptance proof. A full prepared candidate already owns that verified
        // input; its accepted head can outlive the now-consumed request after a
        // peer compacts it. Resume that exact candidate through its publication
        // owner instead of requiring the retired request again.
        if matches!(
            journal.state,
            OwnerPromotionJournalState::AwaitingAcceptance { .. }
                | OwnerPromotionJournalState::AcceptanceReady { .. }
        ) {
            Box::pin(
                self.writer
                    .owner_promotion_history()
                    .verify_acceptance(&acceptance),
            )
            .await
            .map_err(OwnerPromotionError::from)?;
        }
        let (mut previous, mut state) = journal.into_predecessor()?;
        loop {
            match state {
                OwnerPromotionJournalState::AwaitingAcceptance { request, .. } => {
                    if request != *acceptance.request {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted request".to_string(),
                        ));
                    }
                    let next = OwnerPromotionJournal {
                        promotion_id: previous.promotion_id,
                        target: previous.target.clone(),
                        state: OwnerPromotionJournalState::AcceptanceReady {
                            acceptance: acceptance.clone(),
                        },
                    };
                    (previous, state) = self.advance_journal(previous, next).await?;
                }
                OwnerPromotionJournalState::AcceptanceReady {
                    acceptance: persisted,
                } => {
                    if persisted != acceptance {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted acceptance"
                                .to_string(),
                        ));
                    }
                    (previous, state, authorship) = self
                        .prepare(previous, encryption, &persisted, authorship)
                        .await?;
                }
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    wrapped_key,
                    candidate,
                } => {
                    return self
                        .activate_merge_head(
                            &previous,
                            acceptance,
                            wrapped_key,
                            candidate,
                            &authorship,
                        )
                        .await;
                }
                OwnerPromotionJournalState::Finalized { membership, .. } => {
                    return Ok(membership);
                }
                OwnerPromotionJournalState::Stale { reason, .. } => {
                    self.finish_retired_cleanup(previous.promotion_id, &authorship)
                        .await?;
                    return Err(OwnerPromotionError::Stale(Box::new(reason)));
                }
                OwnerPromotionJournalState::Allocated
                | OwnerPromotionJournalState::RequestPrepared { .. }
                | OwnerPromotionJournalState::RequestAccepted { .. }
                | OwnerPromotionJournalState::Nonactivated { .. } => {
                    return Err(OwnerPromotionError::RequestNotActivated)
                }
            }
        }
    }
}

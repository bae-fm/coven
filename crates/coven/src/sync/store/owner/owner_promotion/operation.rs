use crate::encryption::EncryptionService;
use crate::keys;
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::StoreMembershipRoleGrant;
use crate::protocol::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors,
    OwnerPromotionFinalization as OwnerPromotionFinalizationPoint, OwnerPromotionId,
    OwnerPromotionRequest, OwnerPromotionRequestActivation, OwnerPromotionStaleReason,
    StoreDeviceRegistrationRef, StreamActivation, StreamAnchorDomain,
};
use crate::protocol::wrapped_store_key::{PreparedWrappedStoreKey, WrappedStoreKey};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use crate::sync::store::owner::writer::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};

use super::authority::target_key;
use super::journal::{
    owner_promotion_published_objects, OwnerPromotionFinalizationReceipt, OwnerPromotionJournal,
    OwnerPromotionJournalPredecessor, OwnerPromotionJournalState, OwnerPromotionStaleEvidence,
};
use super::OwnerPromotionError;

pub(crate) struct AuthorizedOwnerPromotion<'operation, 'storage> {
    writer: &'operation mut super::super::AuthorizedWriterOperation<'storage>,
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    root: crate::protocol::store_commit::StoreRootRef,
    membership: crate::protocol::membership::MembershipChain,
    identity: crate::keys::UserKeypair,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
}

enum OwnerPromotionPreparation {
    Continue(OwnerPromotionJournal),
    Stale {
        journal: OwnerPromotionJournal,
        reason: OwnerPromotionStaleReason,
    },
}

enum MergeHeadPublication {
    Continue(OwnerPromotionJournal),
    DurablyComplete {
        membership: StoreMembershipStateRef,
    },
    /// The candidate lost its Store position. Its journal already rests on the
    /// stale state and its published objects are already deleted, because both
    /// belong to the step that read the winner.
    Ended {
        reason: OwnerPromotionStaleReason,
    },
}

struct PublishedOwnerPromotionMergeHead {
    acceptance: Box<OwnerPromotionAcceptance>,
    wrapped_key: Box<PreparedWrappedStoreKey>,
    transition: Box<PreparedMembershipTransition>,
    publication: Box<PreparedMembershipPublication>,
    candidate: Box<PreparedStoreOperationCommit>,
}

enum OwnerPromotionResumeOutcome {
    Complete(StoreMembershipStateRef),
    PublishMergeHead {
        previous: OwnerPromotionJournalPredecessor,
        pending: Box<PublishedOwnerPromotionMergeHead>,
    },
}

impl<'operation, 'storage> AuthorizedOwnerPromotion<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        writer: &'operation mut super::super::AuthorizedWriterOperation<'storage>,
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
        root: crate::protocol::store_commit::StoreRootRef,
        membership: crate::protocol::membership::MembershipChain,
        identity: crate::keys::UserKeypair,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            membership,
            identity,
            registration_ref,
            registration,
        }
    }

    async fn delete_candidate_objects(
        &self,
        targets: Vec<crate::database::CandidateCleanupObject>,
    ) -> Result<(), OwnerPromotionError> {
        for target in targets {
            self.storage
                .delete_protocol_object(&target.object)
                .await
                .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        Ok(())
    }

    async fn finish_stale_cleanup(
        &self,
        evidence: &OwnerPromotionStaleEvidence,
    ) -> Result<(), OwnerPromotionError> {
        let OwnerPromotionStaleEvidence::Candidate {
            receipt, published, ..
        } = evidence
        else {
            return Ok(());
        };
        let targets = self
            .database
            .owner_promotion_candidate_cleanup_targets(
                receipt.candidate.reference.clone(),
                published.clone(),
            )
            .await?;
        self.delete_candidate_objects(targets).await
    }

    fn exact_member_grant(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::protocol::membership::MembershipGrantId, OwnerPromotionError> {
        let grants = self.membership.active_grant_ids(member_pubkey);
        let Some(grant) = grants.iter().next() else {
            return Err(OwnerPromotionError::Protocol(
                "promotion target has no active Member grant".to_string(),
            ));
        };
        if grants.len() != 1
            || self.membership.active_grant(grant).is_none_or(|record| {
                record.role != crate::protocol::membership::StoreMembershipRoleGrant::Member
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
        let remote_objects = match &next.state {
            OwnerPromotionJournalState::MergeHeadPrepared {
                wrapped_key,
                transition,
                publication,
                candidate,
                ..
            } => candidate.merge_owner_promotion_remote_objects(
                transition,
                publication,
                wrapped_key,
            )?,
            _ => Vec::new(),
        };
        let transition = previous.transition_to(&next, remote_objects)?;
        let (successor, state) = next.into_predecessor()?;
        self.database
            .advance_owner_promotion_journal(transition)
            .await
            .map_err(|error| {
                OwnerPromotionError::Protocol(format!(
                    "advance exact Owner-promotion journal: {error}"
                ))
            })?;
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
            | OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
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
        let registration_ref = self.registration_ref.clone();
        let registration = self.registration.clone();
        if registration_ref != request.member_registration
            || registration.author_pubkey != request.member_pubkey
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
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let root = self.root.clone();
        let membership_stream = StreamActivation::grant_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
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
        let acceptance = OwnerPromotionAcceptance::signed(
            request.clone(),
            verified_activation.activation().clone(),
            anchors,
            &registration,
            &self.identity,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        self.writer
            .owner_promotion_history()
            .verify_acceptance_from_request(&acceptance, verified_activation)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
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
                    let outcome = self.writer.publish_prepared(candidate, None, None).await?;
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
                    (previous, state) = self.advance_journal(previous, next).await?;
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
                let (previous, state) = existing.into_predecessor()?;
                return operation
                    .resume_request_publication_state(previous, state)
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
                        // The attempt being replaced may still owe deletions its
                        // own retry would have finished. Nothing else names its
                        // objects once this journal is gone, and its membership
                        // slots are the ones this attempt composes into.
                        if let OwnerPromotionJournalState::Stale { evidence, .. } = &previous.state
                        {
                            operation.finish_stale_cleanup(evidence).await?;
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
        let (previous, state) = operation.advance_journal(previous, prepared).await?;
        operation
            .resume_request_publication_state(previous, state)
            .await
    }

    /// Activate the accepted promotion through Store membership and recovery state.
    pub(crate) async fn finalize(
        &mut self,
        encryption: &EncryptionService,
        acceptance: OwnerPromotionAcceptance,
    ) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        Box::pin(
            self.writer
                .owner_promotion_history()
                .verify_acceptance(&acceptance),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if self.registration_ref != acceptance.request.promoter_registration {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        loop {
            let resumed = self.resume(encryption, &acceptance).await?;
            match resumed {
                OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
                OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                    match self.activate_merge_head(&previous, pending).await? {
                        MergeHeadPublication::Continue(next) => {
                            self.advance_journal(previous, next).await?;
                        }
                        MergeHeadPublication::DurablyComplete { membership } => {
                            return Ok(membership);
                        }
                        MergeHeadPublication::Ended { reason } => {
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
            }
        }
    }

    /// Publish the promotion's membership authority and compose the Store candidate
    /// that activates it, journaled as `MergeHeadPrepared` for the publication that
    /// follows. The turn that claimed the stream position is released with the plan,
    /// so a writer can take that position before the publication runs; the candidate
    /// is then bound to a head slot it can never take, and the publication ends the
    /// attempt on the verified winner. Everything composed here — the membership
    /// entry and head above all — sits in create-once slots the promoter's next
    /// attempt composes into, which is why ending the attempt deletes them.
    async fn prepare_merge_store_candidate(
        &mut self,
        journal: &OwnerPromotionJournalPredecessor,
        acceptance: &OwnerPromotionAcceptance,
        wrapped_key: PreparedWrappedStoreKey,
        transition: Box<PreparedMembershipTransition>,
    ) -> Result<OwnerPromotionJournal, OwnerPromotionError> {
        let acceptance = acceptance.clone();
        let root = self.root.clone();
        self.writer
            .publish_membership_authority(&transition, std::slice::from_ref(&wrapped_key))
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let plan = self.writer.prepare_plan().await?;
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
            .finish_membership_transition(
                transition.as_ref().clone(),
                crate::protocol::membership::MembershipHeadActivation::StoreCommit {
                    commit: candidate.reference.clone(),
                },
            )
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        self.writer
            .attach_merge_membership_proof(&mut candidate, &publication, None)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        Ok(OwnerPromotionJournal {
            promotion_id: journal.promotion_id,
            target: journal.target.clone(),
            state: OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance,
                wrapped_key,
                transition,
                publication: Box::new(publication),
                candidate: Box::new(candidate),
            },
        })
    }

    async fn prepare(
        &mut self,
        journal: &OwnerPromotionJournalPredecessor,
        encryption: &EncryptionService,
        acceptance: &OwnerPromotionAcceptance,
    ) -> Result<OwnerPromotionPreparation, OwnerPromotionError> {
        let acceptance = acceptance.clone();
        let operation = &mut *self;
        let author_stream = acceptance.request.finalization.author_stream;
        let seq = acceptance.request.finalization.seq;
        let database = operation.database.clone();
        let root = operation.root.clone();
        let db = &database;
        let promoter = operation.registration.clone();
        if operation.registration_ref != acceptance.request.promoter_registration
            || promoter.author_pubkey != keys::public_key_hex(&operation.identity)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        let membership = operation.membership.clone();
        if let Some(winner) = membership.head_refs().iter().find(|head| {
            head.coord.author_pubkey == promoter.author_pubkey
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
            return Ok(OwnerPromotionPreparation::Stale {
                journal: next,
                reason,
            });
        }
        let identity = operation.identity.clone();
        let authorized = operation
            .writer
            .open_keyring_or_for_membership(&membership, encryption)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let recipient = &acceptance.request.member_pubkey;
        let recipient_key = keys::ed25519_hex_to_x25519_public_key(recipient)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let wrapped_key = WrappedStoreKey::seal_keyring(
            membership.store_id().ok_or_else(|| {
                OwnerPromotionError::Protocol("membership Store id is absent".to_string())
            })?,
            recipient,
            &recipient_key,
            &authorized,
            &identity,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let wrapped_key = operation
            .writer
            .prepare_wrapped_key(recipient, wrapped_key)
            .await?;
        let candidate = operation
            .writer
            .owner_promotion_history()
            .load_registration(&acceptance.request.member_registration)
            .await
            .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let identity = operation.identity.clone();
        let entry = membership
            .signed_finalize_owner_promotion_in_stream(
                &root,
                &promoter,
                &candidate.value,
                acceptance.clone(),
                &identity,
                wrapped_key.reference.clone(),
                db.stamp(),
            )
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let transition = operation
            .writer
            .prepare_membership_transition(&membership, entry)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let next = OwnerPromotionJournal {
            promotion_id: journal.promotion_id,
            target: journal.target.clone(),
            state: OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance,
                wrapped_key,
                transition: Box::new(transition),
            },
        };
        Ok(OwnerPromotionPreparation::Continue(next))
    }

    async fn activate_merge_head(
        &mut self,
        previous: &OwnerPromotionJournalPredecessor,
        published: Box<PublishedOwnerPromotionMergeHead>,
    ) -> Result<MergeHeadPublication, OwnerPromotionError> {
        let operation = &mut *self;
        let database = operation.database.clone();
        let PublishedOwnerPromotionMergeHead {
            acceptance,
            wrapped_key,
            transition,
            publication,
            candidate,
        } = *published;
        let candidate_ref = candidate.reference.clone();
        let candidate_commit = Box::new(candidate.commit.clone());
        let receipt_candidate = candidate.clone();
        operation
            .writer
            .publish_membership_authority(&transition, std::slice::from_ref(&wrapped_key))
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if candidate_commit.control()
            != Some(&crate::protocol::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                crate::protocol::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &candidate_ref
            )
        {
            return Err(OwnerPromotionError::Protocol(
                "activated Owner promotion differs from its exact membership transition"
                    .to_string(),
            ));
        }
        let predecessor = &candidate_commit.membership_state;
        let mut membership = operation
            .writer
            .owner_promotion_history()
            .load_membership(&predecessor.heads, &predecessor.resolutions)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(OwnerPromotionError::Protocol(
                "Owner promotion predecessor membership is conflicted".to_string(),
            ));
        };
        let exact_predecessor = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            candidate_commit.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if exact_predecessor != candidate_commit.membership_state {
            return Err(OwnerPromotionError::Protocol(
                "Owner promotion candidate membership differs from its exact predecessor"
                    .to_string(),
            ));
        }
        membership
            .add_entry(transition.entry.clone())
            .and_then(|()| membership.activate_head_ref(publication.head_ref.clone()))
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(OwnerPromotionError::Protocol(
                "finalized Owner promotion produced conflicted membership".to_string(),
            ));
        };
        let crate::protocol::membership::MembershipChange::SetMember {
            user_pubkey,
            role:
                StoreMembershipRoleGrant::Owner {
                    recovery:
                        crate::protocol::membership::OwnerRecoveryAnchorRef::Promotion {
                            acceptance: promotion_acceptance,
                        },
                },
            grant_id,
            ..
        } = &transition.entry.change
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
        recovery.push(crate::protocol::store_commit::OwnerRecoveryCursor {
            owner_grant: grant_id.clone(),
            position: crate::protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                activation: crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
                    &operation.root,
                    user_pubkey,
                    grant_id,
                    promotion_acceptance.anchors.recovery(),
                )
                .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?,
            },
        });
        let membership = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            recovery,
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
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
                acceptance: *acceptance.clone(),
                membership: membership.clone(),
                receipt: Box::new(OwnerPromotionFinalizationReceipt {
                    candidate: receipt_candidate.clone(),
                    publication: publication.clone(),
                }),
            },
        };
        let journal_transition = previous.transition_to(&finalized, Vec::new())?;
        let remote_objects = candidate.merge_owner_promotion_remote_objects(
            &transition,
            &publication,
            &wrapped_key,
        )?;
        for object in [&transition.entry_ref.object, &wrapped_key.reference.object] {
            let remote = remote_objects
                .iter()
                .find(|remote| remote.object() == object)
                .cloned()
                .ok_or_else(|| {
                    OwnerPromotionError::Protocol(
                        "Owner-promotion completion omits a published membership authority"
                            .to_string(),
                    )
                })?;
            database
                .mark_remote_object_uploaded(remote)
                .await
                .map_err(|error| {
                    OwnerPromotionError::Protocol(format!(
                        "record published Owner-promotion membership authority: {error}"
                    ))
                })?;
        }
        let outcome = operation
            .writer
            .publish_membership_activation(
                &transition,
                &publication,
                candidate,
                crate::sync::store::operations::StoreMembershipJournalCompletion::OwnerPromotion {
                    transition: journal_transition,
                    remote_objects,
                },
            )
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        match outcome {
            StoreOperationPublicationOutcome::Activated(commit) => {
                if commit != candidate_ref {
                    return Err(OwnerPromotionError::Protocol(
                        "Merge promotion activated another prepared candidate".to_string(),
                    ));
                }
                Ok(MergeHeadPublication::DurablyComplete { membership })
            }
            StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: OwnerPromotionJournalState::MergeHeadPrepared {
                        acceptance: *acceptance,
                        wrapped_key: *wrapped_key,
                        transition,
                        publication,
                        candidate,
                    },
                };
                Ok(MergeHeadPublication::Continue(next))
            }
            StoreOperationPublicationOutcome::NonactivatedCandidate { nonactivation, .. } => {
                let reason = OwnerPromotionStaleReason::MergeActivationRejected;
                let published = owner_promotion_published_objects(
                    &receipt_candidate,
                    &transition,
                    &publication,
                    &wrapped_key,
                )?;
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: OwnerPromotionJournalState::Stale {
                        acceptance: *acceptance,
                        reason: reason.clone(),
                        evidence: Box::new(OwnerPromotionStaleEvidence::Candidate {
                            nonactivation: (*nonactivation).clone().into_durable(),
                            receipt: Box::new(OwnerPromotionFinalizationReceipt {
                                candidate: receipt_candidate,
                                publication,
                            }),
                            published: published.clone(),
                        }),
                    },
                };
                let journal_transition = previous.transition_to(&next, Vec::new())?;
                let targets = database
                    .end_nonactivated_owner_promotion_candidate(
                        journal_transition,
                        candidate_ref,
                        published,
                        *nonactivation,
                    )
                    .await?;
                self.delete_candidate_objects(targets).await?;
                Ok(MergeHeadPublication::Ended { reason })
            }
            StoreOperationPublicationOutcome::Nonactivated(_) => {
                Err(OwnerPromotionError::Protocol(
                    "promotion finalization lost without exact nonactivation evidence".to_string(),
                ))
            }
            StoreOperationPublicationOutcome::Reprepared => Err(OwnerPromotionError::Protocol(
                "promotion finalization used acknowledgement reprepare".to_string(),
            )),
        }
    }

    async fn resume(
        &mut self,
        encryption: &EncryptionService,
        acceptance: &OwnerPromotionAcceptance,
    ) -> Result<OwnerPromotionResumeOutcome, OwnerPromotionError> {
        let acceptance = acceptance.clone();
        let database = self.database.clone();
        let existing = database
            .load_owner_promotion_journal(acceptance.request.promotion_id)
            .await?;
        if let Some(existing) = &existing {
            if let OwnerPromotionJournalState::Finalized { membership, .. } = &existing.state {
                return Ok(OwnerPromotionResumeOutcome::Complete(membership.clone()));
            }
        }
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
            | OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance: persisted,
                ..
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
                    let preparation = self.prepare(&previous, encryption, &persisted).await?;
                    match preparation {
                        OwnerPromotionPreparation::Continue(next) => {
                            (previous, state) = self.advance_journal(previous, next).await?;
                        }
                        OwnerPromotionPreparation::Stale {
                            journal: next,
                            reason,
                        } => {
                            self.advance_journal(previous, next).await?;
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
                OwnerPromotionJournalState::MergeMembershipPrepared {
                    acceptance: persisted,
                    wrapped_key,
                    transition,
                } => {
                    if persisted != acceptance {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted acceptance"
                                .to_string(),
                        ));
                    }
                    let next = self
                        .prepare_merge_store_candidate(
                            &previous,
                            &persisted,
                            wrapped_key,
                            transition,
                        )
                        .await?;
                    (previous, state) = self.advance_journal(previous, next).await?;
                }
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                    publication,
                    candidate,
                } => {
                    let pending = Box::new(PublishedOwnerPromotionMergeHead {
                        acceptance: Box::new(acceptance),
                        wrapped_key: Box::new(wrapped_key),
                        transition,
                        publication,
                        candidate,
                    });
                    return Ok(OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending });
                }
                OwnerPromotionJournalState::Finalized { membership, .. } => {
                    return Ok(OwnerPromotionResumeOutcome::Complete(membership));
                }
                OwnerPromotionJournalState::Stale {
                    reason, evidence, ..
                } => {
                    self.finish_stale_cleanup(&evidence).await?;
                    return Err(OwnerPromotionError::Stale(Box::new(reason)));
                }
                OwnerPromotionJournalState::Allocated
                | OwnerPromotionJournalState::RequestPrepared { .. }
                | OwnerPromotionJournalState::Nonactivated { .. } => {
                    return Err(OwnerPromotionError::RequestNotActivated)
                }
            }
        }
    }
}

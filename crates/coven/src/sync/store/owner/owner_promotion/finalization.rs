use crate::encryption::EncryptionService;
use crate::keys;
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::StoreMembershipRoleGrant;
use crate::protocol::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionStaleReason, StreamActivation,
};
use crate::protocol::wrapped_store_key::{PreparedWrappedStoreKey, WrappedStoreKey};
use crate::sync::store::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use crate::sync::store::owner::writer::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};

use super::journal::{
    owner_promotion_published_objects, OwnerPromotionFinalizationReceipt, OwnerPromotionJournal,
    OwnerPromotionJournalPredecessor, OwnerPromotionJournalState, OwnerPromotionStaleEvidence,
};
use super::OwnerPromotionError;

pub(super) struct OwnerPromotionFinalization<'finalization, 'operation, 'storage> {
    operation: &'finalization mut super::AuthorizedOwnerPromotion<'operation, 'storage>,
    encryption: &'finalization EncryptionService,
    acceptance: OwnerPromotionAcceptance,
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

impl<'finalization, 'operation, 'storage>
    OwnerPromotionFinalization<'finalization, 'operation, 'storage>
{
    pub(super) fn new(
        operation: &'finalization mut super::AuthorizedOwnerPromotion<'operation, 'storage>,
        encryption: &'finalization EncryptionService,
        acceptance: OwnerPromotionAcceptance,
    ) -> Self {
        Self {
            operation,
            encryption,
            acceptance,
        }
    }

    /// Activate the accepted promotion through Store membership and recovery state.
    pub(super) async fn run(&mut self) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        Box::pin(
            self.operation
                .writer
                .owner_promotion_history()
                .verify_acceptance(&self.acceptance),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if self.operation.registration_ref != self.acceptance.request.promoter_registration {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        loop {
            let resumed = self.resume().await?;
            match resumed {
                OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
                OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                    match self.activate_merge_head(&previous, pending).await? {
                        MergeHeadPublication::Continue(next) => {
                            self.operation.advance_journal(previous, next).await?;
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
        wrapped_key: PreparedWrappedStoreKey,
        transition: Box<PreparedMembershipTransition>,
    ) -> Result<OwnerPromotionJournal, OwnerPromotionError> {
        let acceptance = self.acceptance.clone();
        let root = self.operation.root.clone();
        self.operation
            .writer
            .publish_membership_authority(&transition, std::slice::from_ref(&wrapped_key))
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let plan = self.operation.writer.prepare_plan().await?;
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
            .operation
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
            .operation
            .writer
            .finish_membership_transition(
                transition.as_ref().clone(),
                crate::protocol::membership::MembershipHeadActivation::StoreCommit {
                    commit: candidate.reference.clone(),
                },
            )
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        candidate
            .attach_merge_membership_proof(
                self.operation.storage.as_ref(),
                &publication,
                None,
                &self.operation.identity,
            )
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
    ) -> Result<OwnerPromotionPreparation, OwnerPromotionError> {
        let acceptance = self.acceptance.clone();
        let encryption = self.encryption;
        let operation = &mut *self.operation;
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
                db.hlc().now().to_string(),
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
        let operation = &mut *self.operation;
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
                self.operation.delete_candidate_objects(targets).await?;
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

    async fn resume(&mut self) -> Result<OwnerPromotionResumeOutcome, OwnerPromotionError> {
        let acceptance = self.acceptance.clone();
        let database = self.operation.database.clone();
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
                    (previous, state) = self.operation.advance_journal(previous, next).await?;
                }
                OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                    if acceptance != self.acceptance {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted acceptance"
                                .to_string(),
                        ));
                    }
                    let preparation = self.prepare(&previous).await?;
                    match preparation {
                        OwnerPromotionPreparation::Continue(next) => {
                            (previous, state) =
                                self.operation.advance_journal(previous, next).await?;
                        }
                        OwnerPromotionPreparation::Stale {
                            journal: next,
                            reason,
                        } => {
                            self.operation.advance_journal(previous, next).await?;
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
                OwnerPromotionJournalState::MergeMembershipPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                } => {
                    if acceptance != self.acceptance {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted acceptance"
                                .to_string(),
                        ));
                    }
                    let next = self
                        .prepare_merge_store_candidate(&previous, wrapped_key, transition)
                        .await?;
                    (previous, state) = self.operation.advance_journal(previous, next).await?;
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
                    self.operation.finish_stale_cleanup(&evidence).await?;
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

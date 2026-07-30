use crate::database::StoreDatabase;
use crate::encryption::EncryptionService;
use crate::keys;
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::{MembershipChain, StoreMembershipRoleGrant};
use crate::protocol::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionStaleReason, StreamActivation,
};
use crate::protocol::wrapped_store_key::{PreparedWrappedStoreKey, WrappedStoreKey};
use crate::storage::SyncStorage;
use crate::sync::store::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use crate::sync::store::owner::writer::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};

use super::journal::{
    advance_owner_promotion_journal, owner_promotion_published_objects,
    OwnerPromotionFinalizationReceipt, OwnerPromotionJournal, OwnerPromotionJournalPredecessor,
    OwnerPromotionJournalState, OwnerPromotionStaleEvidence,
};
use super::OwnerPromotionError;

async fn prepare_promotion_wrap(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    store_id: &str,
    recipient: &str,
    encryption: &EncryptionService,
    membership: &MembershipChain,
) -> Result<PreparedWrappedStoreKey, OwnerPromotionError> {
    let identity = operation.identity().clone();
    let authorized = operation
        .keyring(membership)
        .open_or(encryption)
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let recipient_key = keys::ed25519_hex_to_x25519_public_key(recipient)
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let value =
        WrappedStoreKey::seal_keyring(store_id, recipient, &recipient_key, &authorized, &identity)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    operation
        .keyring(membership)
        .prepare(recipient, value)
        .await
        .map_err(OwnerPromotionError::from)
}

/// Activate an accepted promotion through Store membership and recovery state.
pub(crate) async fn finalize(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    encryption: &EncryptionService,
    acceptance: OwnerPromotionAcceptance,
) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
    let database = operation.database().clone();
    Box::pin(
        operation
            .history_verifier_mut()
            .verify_owner_promotion_acceptance_with_history(&acceptance),
    )
    .await
    .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    if operation.writer.registration_ref != acceptance.request.promoter_registration {
        return Err(OwnerPromotionError::Protocol(
            "promotion finalizer is not the request promoter".to_string(),
        ));
    }
    loop {
        let resumed =
            resume_owner_promotion_finalization(operation, encryption, acceptance.clone()).await?;
        match resumed {
            OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
            OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                match activate_owner_promotion_merge_head(operation, &previous, pending).await? {
                    MergeHeadPublication::Continue(next) => {
                        advance_owner_promotion_journal(&database, previous, next).await?;
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

enum OwnerPromotionPreparation {
    Continue(OwnerPromotionJournal),
    Stale {
        journal: OwnerPromotionJournal,
        reason: OwnerPromotionStaleReason,
    },
}

async fn prepare_owner_promotion_finalization(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    encryption: &EncryptionService,
    journal: &OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
) -> Result<OwnerPromotionPreparation, OwnerPromotionError> {
    let author_stream = acceptance.request.finalization.author_stream;
    let seq = acceptance.request.finalization.seq;
    prepare_merge_owner_promotion_finalization(
        operation,
        encryption,
        journal,
        acceptance,
        author_stream,
        seq,
    )
    .await
}

async fn prepare_merge_owner_promotion_finalization(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    encryption: &EncryptionService,
    journal: &OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    author_stream: crate::protocol::causal_grants::AuthorStreamId,
    seq: u64,
) -> Result<OwnerPromotionPreparation, OwnerPromotionError> {
    let database = operation.database().clone();
    let root = operation.store_root().clone();
    let db = &database;
    let (_, promoter, _) = operation.registration();
    let promoter = promoter.clone();
    if operation.writer.registration_ref != acceptance.request.promoter_registration
        || promoter.author_pubkey != keys::public_key_hex(operation.identity())
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion finalizer is not the request promoter".to_string(),
        ));
    }
    let membership = operation.membership().clone();
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
    let wrapped_key = prepare_promotion_wrap(
        operation,
        membership.store_id().ok_or_else(|| {
            OwnerPromotionError::Protocol("membership Store id is absent".to_string())
        })?,
        &acceptance.request.member_pubkey,
        encryption,
        &membership,
    )
    .await?;
    let candidate = operation
        .history_verifier_mut()
        .load_registration(&acceptance.request.member_registration)
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
    let identity = operation.identity().clone();
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

/// Publish the promotion's membership authority and compose the Store candidate
/// that activates it, journaled as `MergeHeadPrepared` for the publication that
/// follows. The turn that claimed the stream position is released with the plan,
/// so a writer can take that position before the publication runs; the candidate
/// is then bound to a head slot it can never take, and the publication ends the
/// attempt on the verified winner. Everything composed here — the membership
/// entry and head above all — sits in create-once slots the promoter's next
/// attempt composes into, which is why ending the attempt deletes them.
async fn prepare_merge_store_candidate(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    journal: &OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    wrapped_key: PreparedWrappedStoreKey,
    transition: Box<PreparedMembershipTransition>,
) -> Result<OwnerPromotionJournal, OwnerPromotionError> {
    let root = operation.store_root().clone();
    operation
        .publish_membership_authority(&transition, std::slice::from_ref(&wrapped_key))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let plan = operation.prepare_plan().await?;
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
    let mut candidate = operation
        .prepare_candidate(
            plan,
            StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(),
                stream_activations,
            },
        )
        .await?;
    let publication = operation
        .finish_membership_transition(
            transition.as_ref().clone(),
            crate::protocol::membership::MembershipHeadActivation::StoreCommit {
                commit: candidate.reference.clone(),
            },
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let identity = operation.identity().clone();
    candidate
        .attach_merge_membership_proof(operation.storage(), &publication, None, &identity)
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let next = OwnerPromotionJournal {
        promotion_id: journal.promotion_id,
        target: journal.target.clone(),
        state: OwnerPromotionJournalState::MergeHeadPrepared {
            acceptance,
            wrapped_key,
            transition,
            publication: Box::new(publication),
            candidate: Box::new(candidate),
        },
    };
    Ok(next)
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

async fn activate_owner_promotion_merge_head(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    previous: &OwnerPromotionJournalPredecessor,
    published: Box<PublishedOwnerPromotionMergeHead>,
) -> Result<MergeHeadPublication, OwnerPromotionError> {
    let database = operation.database().clone();
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
        .publish_membership_authority(&transition, std::slice::from_ref(&wrapped_key))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let membership = finalized_merge_membership_ref(
        operation.history_verifier_mut(),
        &candidate_ref,
        &candidate_commit,
        &transition,
        &publication,
    )
    .await?;
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
    let remote_objects =
        candidate.merge_owner_promotion_remote_objects(&transition, &publication, &wrapped_key)?;
    for object in [&transition.entry_ref.object, &wrapped_key.reference.object] {
        let remote = remote_objects
            .iter()
            .find(|remote| remote.object() == object)
            .cloned()
            .ok_or_else(|| {
                OwnerPromotionError::Protocol(
                    "Owner-promotion completion omits a published membership authority".to_string(),
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
            delete_promotion_candidate_objects(&database, operation.storage(), targets).await?;
            Ok(MergeHeadPublication::Ended { reason })
        }
        StoreOperationPublicationOutcome::Nonactivated(_) => Err(OwnerPromotionError::Protocol(
            "promotion finalization lost without exact nonactivation evidence".to_string(),
        )),
        StoreOperationPublicationOutcome::Reprepared => Err(OwnerPromotionError::Protocol(
            "promotion finalization used acknowledgement reprepare".to_string(),
        )),
    }
}

async fn delete_promotion_candidate_objects(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    targets: Vec<crate::database::CandidateCleanupObject>,
) -> Result<(), OwnerPromotionError> {
    for target in targets {
        storage
            .delete_protocol_object(&target.object)
            .await
            .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        database
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    Ok(())
}

/// Delete whatever a lost candidate still has in storage. The stale state names
/// the candidate and the objects it published, and each object's durable record
/// says whether it is still there, so running this again after an interrupted
/// deletion resumes it and running it on a finished one does nothing.
pub(super) async fn finish_stale_owner_promotion_cleanup(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    evidence: &OwnerPromotionStaleEvidence,
) -> Result<(), OwnerPromotionError> {
    let OwnerPromotionStaleEvidence::Candidate {
        receipt, published, ..
    } = evidence
    else {
        return Ok(());
    };
    let targets = database
        .owner_promotion_candidate_cleanup_targets(
            receipt.candidate.reference.clone(),
            published.clone(),
        )
        .await?;
    delete_promotion_candidate_objects(database, storage, targets).await
}

enum OwnerPromotionResumeOutcome {
    Complete(StoreMembershipStateRef),
    PublishMergeHead {
        previous: OwnerPromotionJournalPredecessor,
        pending: Box<PublishedOwnerPromotionMergeHead>,
    },
}

async fn resume_owner_promotion_finalization(
    operation: &mut crate::sync::store::owner::AuthorizedWriterOperation<'_>,
    encryption: &EncryptionService,
    acceptance: OwnerPromotionAcceptance,
) -> Result<OwnerPromotionResumeOutcome, OwnerPromotionError> {
    let database = operation.database().clone();
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
                (previous, state) =
                    advance_owner_promotion_journal(&database, previous, next).await?;
            }
            OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                let preparation = prepare_owner_promotion_finalization(
                    operation, encryption, &previous, acceptance,
                )
                .await?;
                match preparation {
                    OwnerPromotionPreparation::Continue(next) => {
                        (previous, state) =
                            advance_owner_promotion_journal(&database, previous, next).await?;
                    }
                    OwnerPromotionPreparation::Stale {
                        journal: next,
                        reason,
                    } => {
                        advance_owner_promotion_journal(&database, previous, next).await?;
                        return Err(OwnerPromotionError::Stale(Box::new(reason)));
                    }
                }
            }
            OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance,
                wrapped_key,
                transition,
            } => {
                let next = prepare_merge_store_candidate(
                    operation,
                    &previous,
                    acceptance,
                    wrapped_key,
                    transition,
                )
                .await?;
                (previous, state) =
                    advance_owner_promotion_journal(&database, previous, next).await?;
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
                finish_stale_owner_promotion_cleanup(&database, operation.storage(), &evidence)
                    .await?;
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

fn finalized_merge_membership_ref<'a>(
    history_verifier: &'a mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
    candidate_ref: &'a crate::protocol::store_commit::StoreBatchCommitRef,
    candidate: &'a crate::protocol::store_commit::StoreBatchCommit,
    transition: &'a PreparedMembershipTransition,
    publication: &'a PreparedMembershipPublication,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<StoreMembershipStateRef, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let root = history_verifier.root().clone();
        if candidate.control()
            != Some(&crate::protocol::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                crate::protocol::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == candidate_ref
            )
        {
            return Err(OwnerPromotionError::Protocol(
                "activated Owner promotion differs from its exact membership transition"
                    .to_string(),
            ));
        }
        let predecessor = &candidate.membership_state;
        let mut membership = history_verifier
            .load_membership_at_exact_heads(&predecessor.heads, &predecessor.resolutions)
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
            candidate.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if exact_predecessor != candidate.membership_state {
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
                        crate::protocol::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
                },
            grant_id,
            ..
        } = &transition.entry.change
        else {
            return Err(OwnerPromotionError::Protocol(
                "Merge Owner promotion entry does not add an Owner recovery stream".to_string(),
            ));
        };
        let mut recovery = candidate.device_state.recovery().to_vec();
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
                    &root,
                    user_pubkey,
                    grant_id,
                    acceptance.anchors.recovery(),
                )
                .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?,
            },
        });
        StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            recovery,
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))
    })
}

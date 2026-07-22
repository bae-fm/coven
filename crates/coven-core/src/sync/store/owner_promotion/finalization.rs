use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MembershipChain, StoreMembershipRoleGrant};
use crate::sync::storage::SyncStorage;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::membership::{PreparedMembershipPublication, PreparedMembershipTransition};
use crate::sync::store::operations::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use crate::sync::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionStaleReason,
    StoreDeviceRegistrationRef, StoreRootRef, StreamActivation,
};
use crate::sync::wrapped_store_key::PreparedWrappedStoreKey;

use super::authority::load_current_merge_membership;
use super::journal::{
    advance_owner_promotion_journal, OwnerPromotionFinalizationReceipt, OwnerPromotionJournal,
    OwnerPromotionJournalPredecessor, OwnerPromotionJournalState, OwnerPromotionStaleEvidence,
};
use super::OwnerPromotionError;

fn prepare_promotion_wrap<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    store_id: &'a str,
    recipient: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    membership: &'a MembershipChain,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<PreparedWrappedStoreKey, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let refs = membership
            .wrapped_key_authority_for(&keys::public_key_hex(identity))
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let authorized = Box::pin(
            crate::sync::store::membership::load_authorized_owner_keyring(
                storage,
                root.store_root_hash,
                identity,
                store_id,
                &refs,
                encryption,
            ),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let recipient_key = crate::sync::store::membership::ed25519_hex_to_x25519(recipient)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let value = crate::sync::store::membership::signed_wrapped_key(
            store_id,
            recipient,
            &recipient_key,
            &authorized,
            identity,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        Box::pin(crate::sync::wrapped_store_key::prepare_wrapped_store_key(
            storage,
            root.store_root_hash,
            recipient,
            value,
        ))
        .await
        .map_err(OwnerPromotionError::from)
    })
}

pub fn finalize_owner_promotion<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<StoreMembershipStateRef, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let root = db
            .local_store_root_ref()
            .await?
            .ok_or_else(|| OwnerPromotionError::Protocol("Store root is absent".to_string()))?;
        Box::pin(crate::sync::store::verify_owner_promotion_acceptance(
            storage,
            &root,
            &acceptance,
        ))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let promoter = load_owner_promotion_remote_promoter(
            storage,
            &root,
            &acceptance.request.promoter_registration,
        )
        .await?;
        loop {
            let resumed = resume_owner_promotion_finalization(
                db,
                storage,
                device_id,
                identity,
                encryption,
                root.clone(),
                acceptance.clone(),
            )
            .await?;
            match resumed {
                OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
                OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                    match activate_owner_promotion_merge_head(
                        db, storage, &root, &promoter, &previous, pending,
                    )
                    .await?
                    {
                        MergeHeadPublication::Continue(next) => {
                            advance_owner_promotion_journal(db, previous, next).await?;
                        }
                        MergeHeadPublication::DurablyComplete { membership } => {
                            return Ok(membership);
                        }
                        MergeHeadPublication::Stale {
                            journal: next,
                            reason,
                        } => {
                            advance_owner_promotion_journal(db, previous, next).await?;
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
            }
        }
    })
}

fn load_owner_promotion_promoter<'a>(
    db: &'a Database,
    device_id: &'a str,
    identity: &'a UserKeypair,
    root: &'a StoreRootRef,
    acceptance: &'a OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    crate::sync::store_commit::StoreDeviceRegistration,
                    OwnerPromotionError,
                >,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let (promoter_root, promoter_ref, promoter, _) = Box::pin(
            crate::sync::store::operations::load_local_store_authority(db, device_id, identity),
        )
        .await?;
        if promoter_root != *root
            || promoter_ref != acceptance.request.promoter_registration
            || promoter.author_pubkey != keys::public_key_hex(identity)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        Ok(promoter)
    })
}

enum OwnerPromotionPreparation {
    Continue(OwnerPromotionJournal),
    Stale {
        journal: OwnerPromotionJournal,
        reason: OwnerPromotionStaleReason,
    },
}

fn prepare_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionPreparation, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    let author_stream = acceptance.request.finalization.author_stream;
    let seq = acceptance.request.finalization.seq;
    prepare_merge_owner_promotion_finalization(
        db,
        storage,
        device_id,
        identity,
        encryption,
        root,
        journal,
        acceptance,
        author_stream,
        seq,
    )
}

fn prepare_merge_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    author_stream: crate::sync::causal_grants::AuthorStreamId,
    seq: u64,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionPreparation, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let promoter =
            load_owner_promotion_promoter(db, device_id, identity, root, &acceptance).await?;
        let membership = Box::pin(load_current_merge_membership(db, storage, root)).await?;
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
            storage,
            root,
            membership.store_id().ok_or_else(|| {
                OwnerPromotionError::Protocol("membership Store id is absent".to_string())
            })?,
            &acceptance.request.member_pubkey,
            identity,
            encryption,
            &membership,
        )
        .await?;
        let candidate = crate::sync::store_objects::load_registration_ref(
            storage,
            root,
            &acceptance.request.member_registration,
        )
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let entry = membership
            .signed_finalize_owner_promotion_in_stream(
                root,
                &promoter,
                &candidate.value,
                acceptance.clone(),
                identity,
                wrapped_key.reference.clone(),
                db.hlc().now().to_string(),
            )
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let transition = Box::pin(
            crate::sync::store::membership::prepare_membership_transition(
                storage,
                db,
                root.store_root_hash,
                &membership,
                entry,
                identity,
            ),
        )
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
    })
}

fn prepare_merge_store_candidate<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    wrapped_key: PreparedWrappedStoreKey,
    transition: Box<PreparedMembershipTransition>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionJournal, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        crate::sync::store::membership::publish_prepared_merge_membership_authority(
            storage,
            root.store_root_hash,
            &transition,
            std::slice::from_ref(&wrapped_key),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let membership = Box::pin(load_current_merge_membership(db, storage, root)).await?;
        let plan = Box::pin(crate::sync::store::operations::prepare_plan(
            db,
            storage,
            &membership,
            device_id,
            identity,
        ))
        .await?;
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
        let mut candidate = Box::pin(crate::sync::store::operations::prepare_candidate(
            db,
            storage,
            plan,
            StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(),
                stream_activations,
            },
        ))
        .await?;
        let publication = crate::sync::store::membership::finish_membership_transition(
            storage,
            db,
            root.store_root_hash,
            transition.as_ref().clone(),
            crate::sync::membership::MembershipHeadActivation::StoreCommit {
                commit: candidate.reference.clone(),
            },
            identity,
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        candidate
            .attach_merge_membership_proof(storage, &publication, None, identity)
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
    })
}

enum MergeHeadPublication {
    Continue(OwnerPromotionJournal),
    DurablyComplete {
        membership: StoreMembershipStateRef,
    },
    Stale {
        journal: OwnerPromotionJournal,
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

fn load_owner_promotion_remote_promoter<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    registration: &'a StoreDeviceRegistrationRef,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    crate::sync::store_commit::StoreDeviceRegistration,
                    OwnerPromotionError,
                >,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        Box::pin(crate::sync::store_objects::load_registration_ref(
            storage,
            root,
            registration,
        ))
        .await
        .map(|loaded| loaded.value)
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))
    })
}

fn activate_owner_promotion_merge_head<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    promoter: &'a crate::sync::store_commit::StoreDeviceRegistration,
    previous: &'a OwnerPromotionJournalPredecessor,
    published: Box<PublishedOwnerPromotionMergeHead>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<MergeHeadPublication, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
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
        crate::sync::store::membership::publish_prepared_merge_membership_authority(
            storage,
            root.store_root_hash,
            &transition,
            std::slice::from_ref(&wrapped_key),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let membership = finalized_merge_membership_ref(
            storage,
            root,
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
            db.mark_remote_object_uploaded(remote)
                .await
                .map_err(|error| {
                    OwnerPromotionError::Protocol(format!(
                        "record published Owner-promotion membership authority: {error}"
                    ))
                })?;
        }
        let outcome = Box::pin(
            crate::sync::store::membership::publish_prepared_merge_membership_activation(
                db,
                storage,
                root,
                promoter,
                &transition,
                &publication,
                candidate,
                crate::sync::store::operations::StoreMembershipJournalCompletion::OwnerPromotion {
                    transition: journal_transition,
                    remote_objects,
                },
            ),
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
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: OwnerPromotionJournalState::Stale {
                        acceptance: *acceptance,
                        reason: reason.clone(),
                        evidence: Box::new(OwnerPromotionStaleEvidence::Candidate {
                            nonactivation: nonactivation.into_durable(),
                            receipt: Box::new(OwnerPromotionFinalizationReceipt {
                                candidate: receipt_candidate,
                                publication,
                            }),
                        }),
                    },
                };
                Ok(MergeHeadPublication::Stale {
                    journal: next,
                    reason,
                })
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
    })
}

enum OwnerPromotionResumeOutcome {
    Complete(StoreMembershipStateRef),
    PublishMergeHead {
        previous: OwnerPromotionJournalPredecessor,
        pending: Box<PublishedOwnerPromotionMergeHead>,
    },
}

fn resume_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: StoreRootRef,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionResumeOutcome, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let existing = StoreDatabase::new(db)
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
                    (previous, state) = advance_owner_promotion_journal(db, previous, next).await?;
                }
                OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                    let preparation = prepare_owner_promotion_finalization(
                        db, storage, device_id, identity, encryption, &root, &previous, acceptance,
                    )
                    .await?;
                    match preparation {
                        OwnerPromotionPreparation::Continue(next) => {
                            (previous, state) =
                                advance_owner_promotion_journal(db, previous, next).await?;
                        }
                        OwnerPromotionPreparation::Stale {
                            journal: next,
                            reason,
                        } => {
                            advance_owner_promotion_journal(db, previous, next).await?;
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
                        db,
                        storage,
                        device_id,
                        identity,
                        &root,
                        &previous,
                        acceptance,
                        wrapped_key,
                        transition,
                    )
                    .await?;
                    (previous, state) = advance_owner_promotion_journal(db, previous, next).await?;
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
                OwnerPromotionJournalState::Stale { reason, .. } => {
                    return Err(OwnerPromotionError::Stale(Box::new(reason)))
                }
                OwnerPromotionJournalState::Allocated
                | OwnerPromotionJournalState::RequestPrepared { .. }
                | OwnerPromotionJournalState::Nonactivated { .. } => {
                    return Err(OwnerPromotionError::RequestNotActivated)
                }
            }
        }
    })
}

fn finalized_merge_membership_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    candidate_ref: &'a crate::sync::store_commit::StoreBatchCommitRef,
    candidate: &'a crate::sync::store_commit::StoreBatchCommit,
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
        if candidate.control()
            != Some(&crate::sync::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == candidate_ref
            )
        {
            return Err(OwnerPromotionError::Protocol(
                "activated Owner promotion differs from its exact membership transition"
                    .to_string(),
            ));
        }
        let predecessor = &candidate.membership_state;
        let root_value = crate::sync::store_objects::load_store_protocol_root(storage, root)
            .await
            .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?
            .value;
        let mut membership = crate::sync::store::membership::load_anchored_chain_at_exact_heads(
            storage,
            root,
            &root_value.descriptor.founder_pubkey,
            &predecessor.heads,
            &predecessor.resolutions,
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status()
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
        let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(OwnerPromotionError::Protocol(
                "finalized Owner promotion produced conflicted membership".to_string(),
            ));
        };
        let crate::sync::membership::MembershipChange::SetMember {
            user_pubkey,
            role:
                StoreMembershipRoleGrant::Owner {
                    recovery:
                        crate::sync::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
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
        recovery.push(crate::sync::store_commit::OwnerRecoveryCursor {
            owner_grant: grant_id.clone(),
            position: crate::sync::store_commit::OwnerRecoveryPosition::BeforeFirst {
                activation: crate::sync::store_commit::OwnerRecoveryActivationId::derive(
                    root,
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

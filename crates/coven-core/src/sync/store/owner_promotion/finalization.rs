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
use crate::sync::store::Store;
use crate::sync::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionStaleReason,
    StoreDeviceRegistrationRef, StoreRootRef, StreamActivation,
};
use crate::sync::wrapped_store_key::PreparedWrappedStoreKey;

use super::authority::load_current_merge_membership_with_history;
use super::journal::{
    advance_owner_promotion_journal, owner_promotion_published_objects,
    OwnerPromotionFinalizationReceipt, OwnerPromotionJournal, OwnerPromotionJournalPredecessor,
    OwnerPromotionJournalState, OwnerPromotionStaleEvidence,
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

impl Store {
    /// Activate an accepted promotion through Store membership and recovery state.
    pub async fn finalize_owner_promotion(
        &self,
        device_id: &str,
        identity: &UserKeypair,
        encryption: &EncryptionService,
        acceptance: OwnerPromotionAcceptance,
    ) -> Result<StoreMembershipStateRef, OwnerPromotionError> {
        let database = self.database();
        let storage = &**self.storage();
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or_else(|| OwnerPromotionError::Protocol("Store root is absent".to_string()))?;
        let mut history_verifier =
            crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root)
                .await
                .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        Box::pin(
            crate::sync::store::verify_owner_promotion_acceptance_with_history(
                &mut history_verifier,
                &acceptance,
            ),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let promoter = load_owner_promotion_remote_promoter(
            history_verifier.commit_verifier_ref(),
            &acceptance.request.promoter_registration,
        )
        .await?;
        loop {
            let resumed = resume_owner_promotion_finalization(
                &mut history_verifier,
                database,
                device_id,
                identity,
                encryption,
                acceptance.clone(),
            )
            .await?;
            match resumed {
                OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
                OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                    match activate_owner_promotion_merge_head(
                        &mut history_verifier,
                        database,
                        &promoter,
                        &previous,
                        pending,
                    )
                    .await?
                    {
                        MergeHeadPublication::Continue(next) => {
                            advance_owner_promotion_journal(database, previous, next).await?;
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
}

fn load_owner_promotion_promoter<'a>(
    database: &'a StoreDatabase,
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
        let (promoter_root, promoter_ref, promoter, _) =
            Box::pin(crate::sync::store::operations::load_local_store_authority(
                database, device_id, identity,
            ))
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
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &'a StoreDatabase,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
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
        history_verifier,
        database,
        device_id,
        identity,
        encryption,
        journal,
        acceptance,
        author_stream,
        seq,
    )
}

fn prepare_merge_owner_promotion_finalization<'a>(
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &'a StoreDatabase,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
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
        let storage = history_verifier.storage();
        let root = history_verifier.root().clone();
        let db = database.sqlite();
        let promoter =
            load_owner_promotion_promoter(database, device_id, identity, &root, &acceptance)
                .await?;
        let membership = Box::pin(load_current_merge_membership_with_history(
            history_verifier,
            database,
        ))
        .await?;
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
            &root,
            membership.store_id().ok_or_else(|| {
                OwnerPromotionError::Protocol("membership Store id is absent".to_string())
            })?,
            &acceptance.request.member_pubkey,
            identity,
            encryption,
            &membership,
        )
        .await?;
        let candidate = crate::sync::store_objects::load_registration_ref_with_root(
            storage,
            &root,
            history_verifier.verified_root(),
            &acceptance.request.member_registration,
        )
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let entry = membership
            .signed_finalize_owner_promotion_in_stream(
                &root,
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
                database,
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

/// Publish the promotion's membership authority and compose the Store candidate
/// that activates it, journaled as `MergeHeadPrepared` for the publication that
/// follows. The turn that claimed the stream position is released with the plan,
/// so a writer can take that position before the publication runs; the candidate
/// is then bound to a head slot it can never take, and the publication ends the
/// attempt on the verified winner. Everything composed here — the membership
/// entry and head above all — sits in create-once slots the promoter's next
/// attempt composes into, which is why ending the attempt deletes them.
fn prepare_merge_store_candidate<'a>(
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &'a StoreDatabase,
    device_id: &'a str,
    identity: &'a UserKeypair,
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
        let storage = history_verifier.storage();
        let root = history_verifier.root().clone();
        crate::sync::store::membership::publish_prepared_merge_membership_authority(
            storage,
            root.store_root_hash,
            &transition,
            std::slice::from_ref(&wrapped_key),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let membership = Box::pin(load_current_merge_membership_with_history(
            history_verifier,
            database,
        ))
        .await?;
        let plan = Box::pin(crate::sync::store::operations::prepare_plan_with_history(
            database,
            history_verifier,
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
            database,
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
            database,
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

async fn load_owner_promotion_remote_promoter(
    commit_verifier: &crate::sync::store::pull::StoreCommitVerifier<'_>,
    registration: &StoreDeviceRegistrationRef,
) -> Result<crate::sync::store_commit::StoreDeviceRegistration, OwnerPromotionError> {
    Box::pin(crate::sync::store_objects::load_registration_ref_with_root(
        commit_verifier.storage(),
        commit_verifier.root(),
        commit_verifier.verified_root(),
        registration,
    ))
    .await
    .map(|loaded| loaded.value)
    .map_err(|error| OwnerPromotionError::Storage(error.to_string()))
}

fn activate_owner_promotion_merge_head<'a>(
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &'a StoreDatabase,
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
        let storage = history_verifier.storage();
        let root = history_verifier.root().clone();
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
            history_verifier,
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
            database
                .mark_remote_object_uploaded(remote)
                .await
                .map_err(|error| {
                    OwnerPromotionError::Protocol(format!(
                        "record published Owner-promotion membership authority: {error}"
                    ))
                })?;
        }
        let outcome = Box::pin(
            crate::sync::store::membership::publish_prepared_merge_membership_activation_with_history(
                database,
                history_verifier,
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
                delete_promotion_candidate_objects(database, storage, targets).await?;
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
    })
}

async fn delete_promotion_candidate_objects(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    targets: Vec<crate::database::CandidateCleanupObject>,
) -> Result<(), OwnerPromotionError> {
    for target in targets {
        crate::sync::store_objects::delete_exact_object(storage, &target.object)
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

fn resume_owner_promotion_finalization<'a>(
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    database: &'a StoreDatabase,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionResumeOutcome, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let storage = history_verifier.storage();
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
                        advance_owner_promotion_journal(database, previous, next).await?;
                }
                OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                    let preparation = prepare_owner_promotion_finalization(
                        history_verifier,
                        database,
                        device_id,
                        identity,
                        encryption,
                        &previous,
                        acceptance,
                    )
                    .await?;
                    match preparation {
                        OwnerPromotionPreparation::Continue(next) => {
                            (previous, state) =
                                advance_owner_promotion_journal(database, previous, next).await?;
                        }
                        OwnerPromotionPreparation::Stale {
                            journal: next,
                            reason,
                        } => {
                            advance_owner_promotion_journal(database, previous, next).await?;
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
                        history_verifier,
                        database,
                        device_id,
                        identity,
                        &previous,
                        acceptance,
                        wrapped_key,
                        transition,
                    )
                    .await?;
                    (previous, state) =
                        advance_owner_promotion_journal(database, previous, next).await?;
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
                    finish_stale_owner_promotion_cleanup(database, storage, &evidence).await?;
                    return Err(OwnerPromotionError::Stale(Box::new(reason)));
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
    history_verifier: &'a mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
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
        let root = history_verifier.root().clone();
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
        let mut membership =
            crate::sync::store::membership::load_anchored_chain_at_exact_heads_with_history(
                history_verifier,
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

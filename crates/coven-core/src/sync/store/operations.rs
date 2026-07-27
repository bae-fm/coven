use super::abandonment::read_occupied_merge_head;
use super::database::{StoreDatabase, StoreDatabaseTransaction};
use super::device_join;
use super::membership as invite;
use super::owner_promotion;
use super::reclaim as store_reclaim;
use super::StoreError;
use super::*;
use crate::database::VerifiedMergeMaterialization;
use crate::sync::membership::MembershipChain;
use crate::sync::storage::{
    BlobWriteAuthority, ExactObjectRef, PreparedExactObject, ProtocolObjectContext,
    ProtocolObjectDomain, StorageError,
};
use crate::sync::store::circle_controls::activation::VerifiedCircleActivations;
use crate::sync::store_commit::{
    circle_package_semantic_prefix, commit_semantic_prefix, head_slot_prefix,
    package_semantic_prefix, ActivatedStoreDeviceRegistrationRef, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, ObjectHash, StoreBatchCommit, StoreBatchCommitDeletionTarget,
    StoreBatchCommitRef, StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder,
    StoreControl, StoreDeviceHead, StoreDeviceHeadRef, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreHistoryCut, StoreOperationMembershipAuthority,
    StoreProtocolError, StoreRootRef, SuccessorLink,
};
use crate::sync::store_objects::{run_blocking_object_verification, StoreObjectError};
use crate::sync::{
    audience_package, circle_control, membership, provider, remote_object, service, store_commit,
    wrapped_store_key,
};
use std::future::Future;
use std::pin::Pin;

mod announcement;
mod candidate;
mod local_authority;
mod plan;
mod prepared;
mod publication;
mod support;

#[cfg(test)]
mod tests;

pub(crate) use announcement::*;
pub(crate) use candidate::*;
pub(crate) use local_authority::*;
pub(crate) use plan::*;
pub(crate) use prepared::*;
pub(crate) use publication::*;
pub(crate) use support::*;

pub(crate) const STORE_ROOT_AUTHORITY: &str = "store_root_authority";

pub(crate) fn successor_store_sequence(current: u64) -> Result<u64, StoreError> {
    current
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted { current })
}

pub(crate) fn next_store_sequence(
    previous: Option<&StoreBatchCommitRef>,
) -> Result<u64, StoreError> {
    previous.map_or(Ok(1), |reference| {
        successor_store_sequence(reference.coord.sequence())
    })
}

pub(crate) async fn prepare_plan(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    candidate_membership: &crate::sync::membership::MembershipChain,
    device_id: &str,
    keypair: &UserKeypair,
) -> Result<StoreOperationCommitPlan, StoreError> {
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(database, device_id, keypair).await?;
    let base = database.local_commit_base().await?;
    let previous = base.predecessor;
    let dependencies = crate::sync::store_commit::CommitFrontier::from_refs(base.frontier)
        .map(|frontier| frontier.commits().clone())
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let seq = next_store_sequence(previous.as_ref())?;
    let coord = StoreCommitCoord {
        stream_id: crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: seq,
    };
    let order = crate::sync::store_commit::StoreCommitOrder {
        seq,
        predecessor: previous,
        dependencies,
    };
    let authorization = super::pull::load_retained_merge_outbound_authorization(
        database,
        storage,
        &root,
        &order,
        candidate_membership.head_refs(),
        &registration_ref,
    )
    .await
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let owner_grant = authorization
        .membership
        .active_owner_grant(&registration.author_pubkey);
    let predecessor = authorization
        .membership
        .write_grant_authority(&registration.author_pubkey)
        .ok_or_else(|| {
            StoreError::InvalidOutbound(format!(
                "Merge Store operation author {} has no active write grant",
                registration.author_pubkey
            ))
        })?;
    Ok(StoreOperationCommitPlan::new(
        StoreOperationPlanCommon::new(
            base.authorship,
            root,
            registration_ref,
            registration,
            device_signer,
            coord,
            order,
            authorization.membership_state,
            authorization.device_state_ref,
            crate::sync::store_commit::StoreOperationMembershipAuthority { predecessor },
            owner_grant,
        ),
        authorization.membership,
        authorization.device_state,
    ))
}

/// Compose a candidate and release this device's turn to author its own stream.
///
/// Consuming the plan is what releases the turn, and that is what a composer
/// which *stages* its candidate wants. Holding the turn past composition buys a
/// staged candidate nothing — its position can be taken between staging and
/// publication either way — while stalling every other writer on this device
/// until the staging is durable, and stalling one under a paused test point
/// deadlocks whoever waits behind it.
///
/// [`activate_store_operation_commit`] is the one caller that must keep the
/// turn, because it publishes against the position it just read; it uses
/// [`prepare_candidate_borrowed`] and holds the plan across the publication.
pub(crate) async fn prepare_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreError> {
    prepare_candidate_borrowed(database, storage, &plan, batch).await
}

/// Compose a candidate without consuming the plan, leaving the turn it holds in
/// the caller's hands. Only for a caller that publishes under that same turn.
pub(crate) async fn prepare_candidate_borrowed(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    plan: &StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreError> {
    let db = database.sqlite();
    let acknowledgement_evidence = match &batch {
        StoreOperationBatch::Acknowledgement {
            reference, value, ..
        } => Some((reference.clone(), value.clone())),
        _ => None,
    };
    let retained_registration_evidence = match &batch {
        StoreOperationBatch::Outcome {
            registration: Some(registration),
            ..
        } => vec![crate::sync::store_commit::RetainedVerifiedRegistration {
            reference: registration.reference.registration.clone(),
            value: registration.registration.clone(),
        }],
        _ => Vec::new(),
    };
    let retained_device_operations = match &batch {
        StoreOperationBatch::DeviceExclusionProposal(proposal) => Some(
            crate::sync::store_commit::RetainedStoreDeviceOperations::from_sources(
                vec![proposal.clone()],
                Vec::new(),
            ),
        ),
        StoreOperationBatch::DeviceExclusionOutcome(outcome) => Some(
            crate::sync::store_commit::RetainedStoreDeviceOperations::from_sources(
                Vec::new(),
                vec![outcome.clone()],
            ),
        ),
        _ => None,
    };
    let common =
        prepare_store_operation_candidate_common(db, storage, plan.common(), batch).await?;
    let acknowledgement = match acknowledgement_evidence {
        Some((reference, value)) => Some(
            crate::sync::store::pull::retain_activated_acknowledgement(
                storage,
                plan.root(),
                &common.reference,
                &common.commit,
                plan.registration(),
                reference,
                value,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        ),
        None => None,
    };
    let merge_history_evidence = super::pull::MergeHistorySuccessorEvidence {
        registrations: retained_registration_evidence,
        acknowledgement,
        membership_proof: None,
    };
    let registrations = common
        .registration_activation
        .as_ref()
        .map(|activation| {
            vec![(
                activation.registration.clone(),
                activation.authority.clone(),
            )]
        })
        .unwrap_or_default();
    let device_operations = match retained_device_operations {
        Some(retained) => retained
            .verify_for(plan.root(), &common.commit)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        None => crate::sync::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
            &common.commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
    };
    let state_after = Box::pin(super::pull::derive_local_post_device_state(
        storage,
        plan.root(),
        &common.commit,
        plan.predecessor_state().clone(),
        &registrations,
        device_operations,
    ))
    .await
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let head_context = ProtocolObjectContext::signed_plaintext(
        common.commit.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let device_id = plan.registration_ref().device_id.to_string();
    let successor = super::pull::prepare_merge_history_successor(
        database,
        plan.root(),
        &common.commit,
        &common.reference,
        plan.membership(),
        plan.registration(),
        None,
        state_after,
        merge_history_evidence,
    )
    .await
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let next_prefix = head_slot_prefix(&device_id, successor_store_sequence(common.commit.seq())?);
    let next_slot = storage
        .allocate_protocol_slot(&head_context, &next_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let head = StoreDeviceHead::signed(
        common.commit.store_root_hash,
        plan.registration_ref().clone(),
        common.reference.clone(),
        successor.summary.digest(),
        SuccessorLink {
            activation: plan
                .registration()
                .store_announcement_activation(plan.registration_ref())
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
                .activation_id(),
            predecessor: successor.predecessor_head.map(|reference| reference.object),
            next_slot,
        },
        plan.device_signer(),
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let head_prefix = head_slot_prefix(&device_id, common.commit.seq());
    let prepared_head = storage
        .prepare_protocol_object(
            &head_context,
            successor.head_slot,
            &head_prefix,
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    Ok(PreparedStoreOperationCommit {
        common,
        head,
        prepared_head,
        history_summary: successor.summary,
    })
}

pub(crate) async fn publish_prepared(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    candidate: Box<PreparedStoreOperationCommit>,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationPublicationOutcome, StoreError> {
    let root = required_store_root(database).await?;
    let retained_operation_objects = retained_store_operation_objects(&candidate.commit)?;
    let head = candidate.head.clone();
    let prepared_head = candidate.prepared_head.clone();
    let history_summary = candidate.history_summary.clone();
    let circle_activations = if candidate.commit.control().is_some() {
        super::pull::verify_merge_membership_control(
            storage,
            &root,
            &candidate.reference,
            &candidate.commit,
        )
        .await
        .map_err(StoreError::InvalidOutbound)?
    } else {
        VerifiedCircleActivations::none(&candidate.commit, &candidate.reference)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
    };
    publish(
        database,
        storage,
        root,
        PreparedStoreOperationActivation {
            candidate,
            retained_operation_objects,
        },
        head,
        prepared_head,
        history_summary,
        membership_objects,
        membership_completion,
        circle_activations,
    )
    .await
}

pub(crate) async fn upload_commit(
    storage: &dyn SyncStorage,
    candidate: &PreparedStoreOperationCommit,
) -> Result<(), StoreError> {
    let stream_id = candidate.reference.coord.stream_id;
    let context = ProtocolObjectContext::signed_plaintext(
        candidate.commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        candidate.commit.candidate_family(),
        &stream_id.to_string(),
        candidate.commit.seq(),
        candidate.commit.commit_hash(),
    );
    storage
        .create_protocol_object(&candidate.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let opened = storage
        .read_protocol_object(&context, &candidate.reference.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != candidate.commit.to_bytes() {
        return Err(StoreError::InvalidOutbound(
            "Store operation commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn publish<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    root: StoreRootRef,
    mut activation: PreparedStoreOperationActivation,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
    circle_activations: VerifiedCircleActivations,
) -> Pin<Box<dyn Future<Output = Result<StoreOperationPublicationOutcome, StoreError>> + Send + 'a>>
{
    Box::pin(async move {
        let db = database.sqlite();
        let reference = activation.candidate.reference.clone();
        let mut commit_verifier = super::pull::StoreCommitVerifier::new(storage, &root).await?;
        let verified_commit = commit_verifier
            .authenticate_bytes(&reference, &activation.candidate.commit.to_bytes())
            .await?;
        let commit = verified_commit.value().clone();
        upload_commit(storage, &activation.candidate).await?;
        let membership_heads = &commit.membership_state.heads;
        let authorization = Box::pin(super::pull::load_retained_merge_outbound_authorization(
            database,
            storage,
            &root,
            &commit.order,
            membership_heads,
            &commit.author_registration,
        ))
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let device_operations = Box::pin(super::pull::load_local_commit_device_operations(
            database,
            storage,
            &root,
            &commit,
            &authorization.membership,
            &authorization.device_state_ref,
            authorization.device_state,
        ))
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let has_tracked_remote_objects =
            !activation.retained_operation_objects.is_empty() || membership_completion.is_some();
        if has_tracked_remote_objects {
            database
                .mark_candidate_commit_uploaded(reference.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store candidate: {error}"))
                })?;
        }
        let head_context = ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = head_slot_prefix(
            &commit.author_registration.device_id.to_string(),
            commit.seq(),
        );
        match storage.create_protocol_object(&prepared_head).await {
            Ok(()) => {}
            Err(StorageError::SlotCollision(_)) => {
                return Box::pin(resolve_head_collision(
                    database,
                    storage,
                    activation.candidate,
                    &mut commit_verifier,
                    verified_commit,
                    reference,
                    head,
                    prepared_head,
                    head_prefix,
                ))
                .await;
            }
            Err(error) => return Err(StoreObjectError::from(error).into()),
        }
        let opened_head = storage
            .read_protocol_object(&head_context, prepared_head.reference(), &head_prefix)
            .await
            .map_err(StoreObjectError::from)?;
        if opened_head != head.to_bytes() {
            return Err(StoreError::InvalidOutbound(
                "Store operation head exact readback differs from its signed bytes".to_string(),
            ));
        }
        let activation_head = StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
        };
        let operation_object_ids = if has_tracked_remote_objects {
            database
                .mark_store_head_uploaded(activation_head.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store head: {error}"))
                })?;
            membership_completion.is_none().then(|| {
                std::iter::once(crate::sync::remote_object::remote_object_id(
                    &reference.object,
                ))
                .chain(
                    activation
                        .retained_operation_objects
                        .iter()
                        .map(crate::sync::remote_object::remote_object_id),
                )
                .chain(std::iter::once(
                    crate::sync::remote_object::remote_object_id(prepared_head.reference()),
                ))
                .collect::<Vec<_>>()
            })
        } else {
            None
        };
        if let Some(completion) = &membership_completion {
            let completion_ids = completion
                .object_refs()
                .iter()
                .map(crate::sync::remote_object::remote_object_id)
                .collect::<std::collections::BTreeSet<_>>();
            if completion_ids.is_empty()
                || !completion_ids.contains(&crate::sync::remote_object::remote_object_id(
                    &reference.object,
                ))
                || !completion_ids.contains(&crate::sync::remote_object::remote_object_id(
                    prepared_head.reference(),
                ))
            {
                return Err(StoreError::InvalidOutbound(
                    "membership journal completion does not cover its exact Store candidate"
                        .to_string(),
                ));
            }
        }
        let recorded_ref = reference.clone();
        let registrations = activation
            .candidate
            .registration_activation
            .take()
            .into_iter()
            .map(|activation| (activation.registration, activation.authority))
            .collect::<Vec<_>>();
        db.call(move |connection| {
            let tx = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            let store_transaction = StoreDatabaseTransaction::new(&tx);
            if let Some(object_ids) = operation_object_ids {
                store_transaction
                    .activate_store_operation_remote_objects(&recorded_ref, &object_ids)?;
            }
            if !registrations.is_empty() {
                crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
                    &tx,
                    &commit,
                    &registrations,
                )?;
            }
            let materialization = VerifiedMergeMaterialization::verify(
                &root,
                &verified_commit,
                &registrations,
                &device_operations,
                &circle_activations,
                &head,
                &activation_head.object,
                &history_summary,
                membership_objects.as_ref(),
                &[],
                None,
            )?;
            if let Some(completion) = membership_completion {
                completion
                    .complete_on(&tx, &recorded_ref)
                    .map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "complete exact membership journal: {error}"
                        ))
                    })?;
            }
            store_transaction
                .record_verified_merge_materialization(materialization)
                .map_err(|error| {
                    crate::database::DbError::Message(format!(
                        "record exact Merge materialization: {error}"
                    ))
                })?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await?;
        Ok(StoreOperationPublicationOutcome::Activated(reference))
    })
}

#[allow(clippy::too_many_arguments)]
async fn resolve_head_collision(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    mut candidate: Box<PreparedStoreOperationCommit>,
    commit_verifier: &mut super::pull::StoreCommitVerifier<'_>,
    commit: crate::sync::store_commit::VerifiedStoreBatchCommit,
    reference: StoreBatchCommitRef,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    head_prefix: String,
) -> Result<StoreOperationPublicationOutcome, StoreError> {
    let observation = read_occupied_merge_head(
        database,
        storage,
        commit_verifier,
        &head,
        &commit,
        prepared_head.reference().slot(),
        &head_prefix,
    )
    .await?;
    if observation.winner().commit == reference {
        let (winner, winner_prepared) = observation.into_head();
        if let Some(acknowledgement) = commit.acknowledgement().cloned() {
            database
                .adopt_acknowledgement_head(acknowledgement, winner, winner_prepared)
                .await?;
            return Ok(StoreOperationPublicationOutcome::Reprepared);
        }
        candidate.adopt_merge_head(winner, winner_prepared)?;
        return Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
            candidate,
        ));
    }
    let registration = database
        .activated_store_device_registration(commit.author_registration.clone())
        .await?;
    let nonactivation = observation
        .verified_nonactivation(
            StoreBatchCommitDeletionTarget {
                coord: reference.coord.clone(),
                object: reference.object.clone(),
                canonical_signed_bytes: commit.to_bytes(),
            },
            &registration,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let Some(acknowledgement) = commit.acknowledgement().cloned() else {
        return Ok(StoreOperationPublicationOutcome::NonactivatedCandidate {
            candidate,
            nonactivation: Box::new(nonactivation),
        });
    };
    database
        .begin_acknowledgement_nonactivation(acknowledgement.clone(), nonactivation)
        .await?;
    super::acknowledgements::finish_nonactivating_acknowledgement(
        database,
        storage,
        acknowledgement,
    )
    .await?;
    Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
}

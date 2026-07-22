use super::abandonment::read_occupied_merge_head;
use super::*;
use crate::database::VerifiedMergeMaterialization;
use crate::sync::circle_activation::VerifiedCircleActivations;
use crate::sync::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommit, StoreBatchCommitDeletionTarget,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceHeadRef,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::{
    finish_nonactivating_store_ack, PreparedStoreOperationActivation, PreparedStoreOperationCommit,
    StoreMembershipJournalCompletion, StoreOperationPublicationOutcome, StoreOutboundError,
};
use std::future::Future;
use std::pin::Pin;

pub(crate) async fn upload_commit(
    storage: &dyn SyncStorage,
    candidate: &PreparedStoreOperationCommit,
) -> Result<(), StoreOutboundError> {
    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = &candidate.reference.coord else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Merge commit upload received a Serial candidate".to_string(),
        ));
    };
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
        return Err(StoreOutboundError::InvalidOutbound(
            "Store operation commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn publish<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    root: StoreRootRef,
    mut activation: PreparedStoreOperationActivation,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
    circle_activations: VerifiedCircleActivations,
) -> Pin<
    Box<
        dyn Future<Output = Result<StoreOperationPublicationOutcome, StoreOutboundError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let commit = activation.candidate.commit.clone();
        let reference = activation.candidate.reference.clone();
        upload_commit(storage, &activation.candidate).await?;
        let membership_heads = match &commit.membership_state {
            crate::sync::circle_control::StoreMembershipStateRef::MergeConcurrent(state) => {
                &state.heads
            }
            crate::sync::circle_control::StoreMembershipStateRef::Serial(_) => {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Merge publication carries Serial membership authority".to_string(),
                ));
            }
        };
        let authorization = Box::pin(
            crate::sync::store_pull::load_retained_merge_outbound_authorization(
                db,
                storage,
                &root,
                &commit.order,
                membership_heads,
                &commit.author_registration,
            ),
        )
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let device_operations = Box::pin(
            crate::sync::store_pull::load_local_commit_device_operations_with_merge_membership(
                db,
                storage,
                &root,
                &commit,
                &authorization.membership,
                &authorization.device_state_ref,
                authorization.device_state,
            ),
        )
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let has_tracked_remote_objects =
            !activation.retained_operation_objects.is_empty() || membership_completion.is_some();
        if has_tracked_remote_objects {
            db.mark_candidate_commit_uploaded(reference.clone())
                .await
                .map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "record uploaded Store candidate: {error}"
                    ))
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
                    db,
                    storage,
                    activation.candidate,
                    commit,
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
            return Err(StoreOutboundError::InvalidOutbound(
                "Store operation head exact readback differs from its signed bytes".to_string(),
            ));
        }
        let activation_head = StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
        };
        let operation_object_ids = if has_tracked_remote_objects {
            db.mark_store_head_uploaded(activation_head.clone())
                .await
                .map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "record uploaded Store head: {error}"
                    ))
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
                return Err(StoreOutboundError::InvalidOutbound(
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
            if let Some(object_ids) = operation_object_ids {
                Database::activate_store_operation_remote_objects_on(
                    &tx,
                    &recorded_ref,
                    &object_ids,
                )?;
            }
            if !registrations.is_empty() {
                Database::record_activated_store_device_registrations_on(
                    &tx,
                    &commit,
                    &registrations,
                )?;
            }
            let materialization = VerifiedMergeMaterialization::verify(
                &root,
                &commit,
                &recorded_ref,
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
            Database::record_verified_merge_materialization_on(&tx, materialization).map_err(
                |error| {
                    crate::database::DbError::Message(format!(
                        "record exact Merge materialization: {error}"
                    ))
                },
            )?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await?;
        Ok(StoreOperationPublicationOutcome::Activated(reference))
    })
}

#[allow(clippy::too_many_arguments)]
async fn resolve_head_collision(
    db: &Database,
    storage: &dyn SyncStorage,
    mut candidate: Box<PreparedStoreOperationCommit>,
    commit: StoreBatchCommit,
    reference: StoreBatchCommitRef,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    head_prefix: String,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let observation = read_occupied_merge_head(
        db,
        storage,
        commit.store_root_hash,
        &head,
        &commit,
        prepared_head.reference().slot(),
        &head_prefix,
    )
    .await?;
    if observation.winner().commit == reference {
        let (winner, winner_prepared) = observation.into_head();
        if let Some(acknowledgement) = commit.acknowledgement().cloned() {
            db.adopt_outbound_store_ack_merge_head(acknowledgement, winner, winner_prepared)
                .await?;
            return Ok(StoreOperationPublicationOutcome::Reprepared);
        }
        candidate.adopt_merge_head(winner, winner_prepared)?;
        return Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
            candidate,
        ));
    }
    let registration = db
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
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let Some(acknowledgement) = commit.acknowledgement().cloned() else {
        return Ok(StoreOperationPublicationOutcome::NonactivatedCandidate {
            candidate,
            nonactivation: Box::new(nonactivation),
        });
    };
    db.begin_outbound_store_ack_nonactivation(acknowledgement.clone(), nonactivation)
        .await?;
    finish_nonactivating_store_ack(db, storage, acknowledgement).await?;
    Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
}

use super::abandonment::read_occupied_merge_head;
use super::*;
use crate::database::VerifiedMergeMaterialization;
use crate::sync::circle_activation::VerifiedCircleActivations;
use crate::sync::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommit, StoreBatchCommitDeletionTarget,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceHeadRef, SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::{
    PreparedStoreOperationActivation, PreparedStoreOperationCommit,
    StoreMembershipJournalCompletion, StoreOperationBatch, StoreOperationCommitPlan,
    StoreOperationPublicationOutcome, StoreOutboundError,
};
use std::future::Future;
use std::pin::Pin;

pub(crate) async fn prepare_plan(
    db: &Database,
    storage: &dyn SyncStorage,
    candidate_membership: &crate::sync::membership::MembershipChain,
    device_id: &str,
    keypair: &UserKeypair,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    let (root, registration_ref, registration, device_signer) =
        crate::sync::store_outbound::load_local_store_authority(db, device_id, keypair).await?;
    let previous = db.latest_local_store_position().await?;
    let dependencies =
        crate::sync::store_commit::CommitFrontier::from_refs(db.materialized_frontier().await?)
            .map(|frontier| frontier.commits().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let seq = crate::sync::store_outbound::next_store_sequence(previous.as_ref())?;
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
        db,
        storage,
        &root,
        &order,
        candidate_membership.head_refs(),
        &registration_ref,
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let owner_grant = authorization
        .membership
        .active_owner_grant(&registration.author_pubkey);
    let predecessor = authorization
        .membership
        .write_grant_authority(&registration.author_pubkey)
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(format!(
                "Merge Store operation author {} has no active write grant",
                registration.author_pubkey
            ))
        })?;
    Ok(StoreOperationCommitPlan::new(
        crate::sync::store_outbound::StoreOperationPlanCommon::new(
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

pub(crate) async fn prepare_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreOutboundError> {
    let acknowledgement_evidence = match &batch {
        StoreOperationBatch::Acknowledgement { reference, value } => {
            Some((reference.clone(), value.clone()))
        }
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
    let common = crate::sync::store_outbound::prepare_store_operation_candidate_common(
        db,
        storage,
        plan.common(),
        batch,
    )
    .await?;
    let acknowledgement = match acknowledgement_evidence {
        Some((reference, value)) => Some(
            crate::sync::store_pull::retain_activated_acknowledgement(
                storage,
                plan.root(),
                &common.reference,
                &common.commit,
                plan.registration(),
                reference,
                value,
            )
            .await
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
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
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        None => crate::sync::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
            &common.commit,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
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
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head_context = ProtocolObjectContext::signed_plaintext(
        common.commit.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let device_id = plan.registration_ref().device_id.to_string();
    let successor = super::pull::prepare_merge_history_successor(
        db,
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
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let next_prefix = head_slot_prefix(
        &device_id,
        crate::sync::store_outbound::successor_store_sequence(common.commit.seq())?,
    );
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
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
                .activation_id(),
            predecessor: successor.predecessor_head.map(|reference| reference.object),
            next_slot,
        },
        plan.device_signer(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
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
    db: &Database,
    storage: &dyn SyncStorage,
    candidate: Box<PreparedStoreOperationCommit>,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let root = crate::sync::store_outbound::required_store_root(db).await?;
    let retained_operation_objects =
        crate::sync::store_outbound::retained_store_operation_objects(&candidate.commit)?;
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
        .map_err(StoreOutboundError::InvalidOutbound)?
    } else {
        VerifiedCircleActivations::none(&candidate.commit, &candidate.reference)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
    };
    publish(
        db,
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
) -> Result<(), StoreOutboundError> {
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
        let membership_heads = &commit.membership_state.heads;
        let authorization = Box::pin(super::pull::load_retained_merge_outbound_authorization(
            db,
            storage,
            &root,
            &commit.order,
            membership_heads,
            &commit.author_registration,
        ))
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let device_operations = Box::pin(super::pull::load_local_commit_device_operations(
            db,
            storage,
            &root,
            &commit,
            &authorization.membership,
            &authorization.device_state_ref,
            authorization.device_state,
        ))
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
            StoreEngineDatabase::new(db)
                .adopt_acknowledgement_head(acknowledgement, winner, winner_prepared)
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
    StoreEngineDatabase::new(db)
        .begin_acknowledgement_nonactivation(acknowledgement.clone(), nonactivation)
        .await?;
    super::acknowledgements::finish_nonactivating_acknowledgement(db, storage, acknowledgement)
        .await?;
    Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
}

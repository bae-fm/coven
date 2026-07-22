use super::publication::activate_serial_commit_head;
use super::*;
use crate::sync::membership::SerialAuthorizationState;
use crate::sync::storage::{
    CoordinationStorage, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage, VersionedObject,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreBatchCommitRef,
    StoreCommitOrder, StoreSerialHead, StoreSerialPredecessor, SERIAL_STREAM_ID,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::{
    finish_nonactivating_store_ack, required_store_root, PreparedSerialStoreOperationCommit,
    PreparedStoreOperationActivation, SerialStoreOperationCommitPlan,
    StoreMembershipJournalCompletion, StoreOperationBatch, StoreOperationPublicationOutcome,
    StoreOutboundError,
};

pub(crate) async fn prepare_plan(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    keypair: &UserKeypair,
) -> Result<SerialStoreOperationCommitPlan, StoreOutboundError> {
    let snapshot = Box::pin(super::publication::current_serial_authorization_snapshot(
        db,
        storage,
        coordination,
    ))
    .await?;
    prepare_plan_from_snapshot(db, device_id, keypair, snapshot).await
}

pub(crate) async fn prepare_plan_from_snapshot(
    db: &Database,
    device_id: &str,
    keypair: &UserKeypair,
    snapshot: super::publication::SerialAuthorizationSnapshot,
) -> Result<SerialStoreOperationCommitPlan, StoreOutboundError> {
    if db.write_policy() != crate::WritePolicy::Serial {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial Store operation preparation requires Serial policy".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        crate::sync::store_outbound::load_local_store_authority(db, device_id, keypair).await?;
    let seq = crate::sync::store_outbound::next_store_sequence(snapshot.base.as_ref())?;
    let predecessor = snapshot.base.clone().map_or_else(
        || StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: registration_ref.clone(),
        },
        StoreSerialPredecessor::Commit,
    );
    let coord = crate::sync::store_commit::StoreCommitCoord::Serial { sequence: seq };
    let order = StoreCommitOrder::Serial {
        seq,
        predecessor: predecessor.clone(),
    };
    let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
    let membership_state = crate::sync::circle_control::StoreMembershipStateRef::serial(
        predecessor,
        resolved_devices.recovery,
        &snapshot.authorization,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let owner_grant = snapshot
        .authorization
        .membership
        .active_owner_grant(&registration.author_pubkey);
    Ok(SerialStoreOperationCommitPlan::new(
        crate::sync::store_outbound::StoreOperationPlanCommon::new(
            root,
            registration_ref,
            registration,
            device_signer,
            coord,
            order,
            membership_state,
            device_state,
            crate::sync::store_commit::StoreOperationMembershipAuthority::Serial,
            owner_grant,
        ),
        snapshot.base_head,
        snapshot.authorization,
    ))
}

#[cfg(test)]
pub(crate) async fn prepare_successor_plan_for_test(
    db: &Database,
    device_id: &str,
    signer: &UserKeypair,
    predecessor: &PreparedSerialStoreOperationCommit,
    base_head: VersionedObject,
) -> Result<SerialStoreOperationCommitPlan, StoreOutboundError> {
    if base_head.bytes != predecessor.head.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "test Serial successor receipt differs from its predecessor head".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        crate::sync::store_outbound::load_local_store_authority(db, device_id, signer).await?;
    let sequence = predecessor
        .reference
        .coord
        .sequence()
        .checked_add(1)
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "test Serial successor sequence overflow".to_string(),
            )
        })?;
    let position = StoreSerialPredecessor::Commit(predecessor.reference.clone());
    let membership_state = crate::sync::circle_control::StoreMembershipStateRef::serial(
        position.clone(),
        predecessor.commit.membership_state.recovery().to_vec(),
        &predecessor.authorization_after,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let device_state = crate::sync::store_commit::StoreDeviceStateRef::Serial {
        position: position.clone(),
        recovery: predecessor.commit.device_state.recovery().to_vec(),
        state_hash: predecessor.commit.device_state.state_hash(),
    };
    let owner_grant = predecessor
        .authorization_after
        .membership
        .active_owner_grant(&registration.author_pubkey);
    Ok(SerialStoreOperationCommitPlan::new(
        crate::sync::store_outbound::StoreOperationPlanCommon::new(
            root,
            registration_ref,
            registration,
            device_signer,
            crate::sync::store_commit::StoreCommitCoord::Serial { sequence },
            StoreCommitOrder::Serial {
                seq: sequence,
                predecessor: position,
            },
            membership_state,
            device_state,
            crate::sync::store_commit::StoreOperationMembershipAuthority::Serial,
            owner_grant,
        ),
        base_head,
        predecessor.authorization_after.clone(),
    ))
}

pub(crate) async fn prepare_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: SerialStoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedSerialStoreOperationCommit, StoreOutboundError> {
    let common = crate::sync::store_outbound::prepare_store_operation_candidate_common(
        db,
        storage,
        plan.common(),
        batch,
    )
    .await?;
    let authorization_after = plan
        .authorization()
        .authorize_and_apply(&common.reference, &common.commit, plan.registration())
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        common.commit.store_root_hash,
        crate::sync::store_commit::StoreSerialHeadState::Commit {
            author_registration: plan.registration_ref().clone(),
            commit: common.reference.clone(),
        },
        plan.device_signer(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok(PreparedSerialStoreOperationCommit {
        common,
        base_head: plan.base_head().clone(),
        head,
        authorization_after,
    })
}

pub(crate) async fn publish_prepared(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    candidate: Box<PreparedSerialStoreOperationCommit>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let root = required_store_root(db).await?;
    crate::sync::wrapped_store_key::validate_control_wrapped_keys(
        storage,
        &root,
        candidate.commit.control(),
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let retained_operation_objects =
        crate::sync::store_outbound::retained_store_operation_objects(&candidate.commit)?;
    let base_head = candidate.base_head.clone();
    let head = candidate.head.clone();
    let authorization_after = candidate.authorization_after.clone();
    let activation = PreparedStoreOperationActivation {
        candidate: Box::new(
            crate::sync::store_outbound::PreparedStoreOperationCommit::Serial(*candidate),
        ),
        retained_operation_objects,
    };
    match publish(
        db,
        storage,
        coordination,
        activation,
        base_head,
        head,
        authorization_after,
        membership_completion,
    )
    .await?
    {
        StoreOperationAttempt::Activated(reference) => {
            Ok(StoreOperationPublicationOutcome::Activated(reference))
        }
        StoreOperationAttempt::Conflict {
            activation,
            commit,
            reference,
            authorization_after,
            membership_completion,
        } => {
            resolve_conflict(
                db,
                storage,
                coordination,
                activation,
                commit,
                reference,
                authorization_after,
                membership_completion,
            )
            .await
        }
    }
}

pub(crate) enum StoreOperationAttempt {
    Activated(StoreBatchCommitRef),
    Conflict {
        activation: PreparedStoreOperationActivation,
        commit: Box<StoreBatchCommit>,
        reference: StoreBatchCommitRef,
        authorization_after: Box<SerialAuthorizationState>,
        membership_completion: Option<StoreMembershipJournalCompletion>,
    },
}

pub(crate) async fn publish(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    activation: PreparedStoreOperationActivation,
    base_head: VersionedObject,
    head: StoreSerialHead,
    authorization_after: SerialAuthorizationState,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationAttempt, StoreOutboundError> {
    let commit = activation.candidate.commit.clone();
    let prepared = activation.candidate.prepared.clone();
    let reference = activation.candidate.reference.clone();
    let head_activation = activate_serial_commit_head(
        db,
        storage,
        coordination,
        &base_head,
        &commit,
        &prepared,
        &reference,
        &head,
    )
    .await;
    let device_operations = match head_activation {
        Ok(device_operations) => device_operations,
        Err(error) => {
            if !matches!(&error, StoreOutboundError::SerialControlConflict { .. }) {
                return Err(error);
            }
            return Ok(StoreOperationAttempt::Conflict {
                activation,
                commit: Box::new(commit),
                reference,
                authorization_after: Box::new(authorization_after),
                membership_completion,
            });
        }
    };
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialStoreHeadActivated)
        .await;
    Box::pin(record_activated(
        db,
        activation,
        device_operations,
        Box::new(commit),
        reference.clone(),
        Box::new(authorization_after),
        membership_completion,
    ))
    .await?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialStoreMaterialized)
        .await;
    Ok(StoreOperationAttempt::Activated(reference))
}

async fn record_activated(
    db: &Database,
    activation: PreparedStoreOperationActivation,
    device_operations: crate::sync::store_commit::VerifiedStoreDeviceOperations,
    commit: Box<StoreBatchCommit>,
    reference: StoreBatchCommitRef,
    authorization_after: Box<SerialAuthorizationState>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<(), StoreOutboundError> {
    let has_tracked_remote_objects =
        !activation.retained_operation_objects.is_empty() || membership_completion.is_some();
    let operation_object_ids = (membership_completion.is_none()
        && !activation.retained_operation_objects.is_empty())
    .then(|| {
        std::iter::once(crate::sync::remote_object::remote_object_id(
            &reference.object,
        ))
        .chain(
            activation
                .retained_operation_objects
                .iter()
                .map(crate::sync::remote_object::remote_object_id),
        )
        .collect::<Vec<_>>()
    });
    if has_tracked_remote_objects {
        db.mark_candidate_commit_uploaded(reference.clone()).await?;
    }
    if let Some(completion) = &membership_completion {
        let completion_ids = completion
            .object_refs()
            .iter()
            .map(crate::sync::remote_object::remote_object_id)
            .collect::<std::collections::BTreeSet<_>>();
        if !completion_ids.contains(&crate::sync::remote_object::remote_object_id(
            &reference.object,
        )) || activation.retained_operation_objects.iter().any(|object| {
            !completion_ids.contains(&crate::sync::remote_object::remote_object_id(object))
        }) {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial membership completion does not cover its exact activated graph".to_string(),
            ));
        }
    }
    let recorded_ref = reference.clone();
    let registration_activation = activation.candidate.registration_activation.clone();
    let stream_activations =
        crate::sync::circle_activation::VerifiedStreamActivations::none(&commit, &recorded_ref)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.call(move |connection| {
        let tx = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        if let Some(object_ids) = operation_object_ids {
            Database::activate_store_operation_remote_objects_on(&tx, &recorded_ref, &object_ids)?;
        }
        if let Some(activation) = registration_activation {
            Database::record_activated_store_device_registrations_on(
                &tx,
                &commit,
                &[(activation.registration, activation.authority)],
            )?;
        }
        Database::record_materialized_serial_commit_with_device_operations_on(
            &tx,
            &commit,
            &recorded_ref,
            &authorization_after,
            &device_operations,
            &stream_activations,
        )?;
        if let Some(completion) = membership_completion {
            completion.complete_on(&tx, &recorded_ref)?;
        }
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await?;
    Ok(())
}

pub(crate) async fn resolve_conflict(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    mut activation: PreparedStoreOperationActivation,
    commit: Box<StoreBatchCommit>,
    reference: StoreBatchCommitRef,
    authorization_after: Box<SerialAuthorizationState>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let StoreCommitOrder::Serial { predecessor, .. } = &commit.order else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial acknowledgement activation carries Merge order".to_string(),
        ));
    };
    let root = required_store_root(db).await?;
    match super::pull::observe_serial_successors_after(storage, coordination, &root, predecessor)
        .await?
    {
        super::pull::SerialSuccessorObservation::Unchanged(observed) => {
            if let Some(acknowledgement) = commit.acknowledgement().cloned() {
                db.adopt_outbound_store_ack_serial_base_head(acknowledgement, observed)
                    .await?;
                return Ok(StoreOperationPublicationOutcome::Reprepared);
            }
            activation.candidate.adopt_serial_base_head(observed)?;
            Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
                activation.candidate,
            ))
        }
        super::pull::SerialSuccessorObservation::Advanced(suffix) => {
            if suffix.commits().first() == Some(&reference) {
                let device_operations = Box::pin(reload_uploaded_device_operations(
                    db, storage, &root, &commit, &reference,
                ))
                .await?;
                Box::pin(record_activated(
                    db,
                    activation,
                    device_operations,
                    commit,
                    reference.clone(),
                    authorization_after,
                    membership_completion,
                ))
                .await?;
                #[cfg(any(test, feature = "test-utils"))]
                db.reach_test_point(crate::database::DatabaseTestPoint::SerialStoreMaterialized)
                    .await;
                return Ok(StoreOperationPublicationOutcome::Activated(reference));
            }
            let author = db
                .activated_store_device_registration(commit.author_registration.clone())
                .await?;
            let nonactivation = suffix
                .verify_candidate_nonactivation(vec![(
                    StoreBatchCommitDeletionTarget {
                        coord: reference.coord.clone(),
                        object: reference.object.clone(),
                        canonical_signed_bytes: commit.to_bytes(),
                    },
                    author,
                )])
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let Some(acknowledgement) = commit.acknowledgement().cloned() else {
                return Ok(StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate: activation.candidate,
                    nonactivation: Box::new(nonactivation),
                });
            };
            db.begin_outbound_store_ack_nonactivation(acknowledgement.clone(), nonactivation)
                .await?;
            finish_nonactivating_store_ack(db, storage, acknowledgement).await?;
            Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
        }
    }
}

async fn reload_uploaded_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    reference: &StoreBatchCommitRef,
) -> Result<crate::sync::store_commit::VerifiedStoreDeviceOperations, StoreOutboundError> {
    let crate::sync::store_commit::StoreCommitCoord::Serial { .. } = &reference.coord else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial publication reload received a Merge commit".to_string(),
        ));
    };
    reference
        .verify_commit(commit)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        commit.seq(),
        commit.commit_hash(),
    );
    let opened = storage
        .read_protocol_object(&context, &reference.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != commit.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial Store operation exact readback differs from its signed bytes".to_string(),
        ));
    }
    crate::sync::store_pull::load_local_commit_device_operations(db, storage, root, commit)
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
}

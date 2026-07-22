use super::*;

#[derive(Clone, Copy)]
pub(crate) enum StoreOperationPublicationMode<'a> {
    MergeConcurrent,
    Serial {
        coordination: &'a dyn CoordinationStorage,
    },
}

impl<'a> StoreOperationPublicationMode<'a> {
    pub(crate) fn from_dependencies(
        policy: crate::WritePolicy,
        coordination: Option<&'a dyn CoordinationStorage>,
    ) -> Result<Self, StoreOutboundError> {
        match (policy, coordination) {
            (crate::WritePolicy::MergeConcurrent, None) => Ok(Self::MergeConcurrent),
            (crate::WritePolicy::Serial, Some(coordination)) => Ok(Self::Serial { coordination }),
            (crate::WritePolicy::MergeConcurrent, Some(_)) => {
                Err(StoreOutboundError::InvalidOutbound(
                    "Merge Store operation publication received Serial coordination".to_string(),
                ))
            }
            (crate::WritePolicy::Serial, None) => {
                Err(StoreOutboundError::MissingSerialCoordination)
            }
        }
    }
}

pub(crate) async fn publish_prepared_store_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    mode: StoreOperationPublicationMode<'_>,
    prepared: Box<PreparedStoreOperationCommit>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    Box::pin(publish_prepared_store_operation_with_membership_completion(
        db, storage, mode, prepared, None, None,
    ))
    .await
}

pub(crate) fn publish_prepared_store_membership_operation<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    prepared: Box<PreparedStoreOperationCommit>,
    membership_objects: crate::database::VerifiedMergeMembershipObjects,
    completion: StoreMembershipJournalCompletion,
) -> Pin<
    Box<
        dyn Future<Output = Result<StoreOperationPublicationOutcome, StoreOutboundError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(publish_prepared_store_operation_with_membership_completion(
        db,
        storage,
        StoreOperationPublicationMode::MergeConcurrent,
        prepared,
        Some(membership_objects),
        Some(completion),
    ))
}

pub(crate) async fn publish_prepared_serial_membership_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    prepared: Box<PreparedStoreOperationCommit>,
    completion: StoreMembershipJournalCompletion,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    Box::pin(publish_prepared_store_operation_with_membership_completion(
        db,
        storage,
        StoreOperationPublicationMode::Serial { coordination },
        prepared,
        None,
        Some(completion),
    ))
    .await
}

async fn publish_prepared_store_operation_with_membership_completion(
    db: &Database,
    storage: &dyn SyncStorage,
    mode: StoreOperationPublicationMode<'_>,
    prepared: Box<PreparedStoreOperationCommit>,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let root = required_store_root(db).await?;
    super::store_pull::validate_serial_control_wrapped_keys(
        storage,
        &root,
        prepared.commit.control(),
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let retained_operation_objects = retained_store_operation_objects(&prepared.commit)?;
    match (mode, *prepared) {
        (
            StoreOperationPublicationMode::Serial { coordination },
            PreparedStoreOperationCommit::Serial(candidate),
        ) => {
            if membership_objects.is_some() {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Serial membership publication received Merge membership objects".to_string(),
                ));
            }
            let base_head = candidate.base_head.clone();
            let head = candidate.head.clone();
            let authorization_after = candidate.authorization_after.clone();
            let activation = PreparedStoreOperationActivation {
                candidate: Box::new(PreparedStoreOperationCommit::Serial(candidate)),
                retained_operation_objects,
            };
            let attempt = Box::pin(crate::sync::store_engine::serial::operations::publish(
                db,
                storage,
                coordination,
                activation,
                base_head,
                head,
                authorization_after,
                membership_completion,
            ))
            .await?;
            match attempt {
                crate::sync::store_engine::serial::operations::StoreOperationAttempt::Activated(reference) => {
                    Ok(StoreOperationPublicationOutcome::Activated(reference))
                }
                crate::sync::store_engine::serial::operations::StoreOperationAttempt::Conflict {
                    activation,
                    commit,
                    reference,
                    authorization_after,
                    membership_completion,
                } => {
                    Box::pin(crate::sync::store_engine::serial::operations::resolve_conflict(
                        db,
                        storage,
                        coordination,
                        activation,
                        commit,
                        reference,
                        authorization_after,
                        membership_completion,
                    ))
                    .await
                }
            }
        }
        (
            StoreOperationPublicationMode::MergeConcurrent,
            PreparedStoreOperationCommit::MergeConcurrent(candidate),
        ) => {
            let head = candidate.head.clone();
            let prepared_head = candidate.prepared_head.clone();
            let history_summary = candidate.history_summary.clone();
            let activation = PreparedStoreOperationActivation {
                candidate: Box::new(PreparedStoreOperationCommit::MergeConcurrent(candidate)),
                retained_operation_objects,
            };
            let circle_activations = if matches!(
                activation.candidate.commit.control(),
                Some(StoreControl::MergeMembership { .. })
            ) {
                super::store_pull::verify_merge_membership_control(
                    storage,
                    &root,
                    &activation.candidate.reference,
                    &activation.candidate.commit,
                )
                .await
                .map_err(StoreOutboundError::InvalidOutbound)?
            } else {
                VerifiedCircleActivations::none(
                    &activation.candidate.commit,
                    &activation.candidate.reference,
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
            };
            Box::pin(crate::sync::store_engine::merge::operations::publish(
                db,
                storage,
                root,
                activation,
                head,
                prepared_head,
                history_summary,
                membership_objects,
                membership_completion,
                circle_activations,
            ))
            .await
        }
        (
            StoreOperationPublicationMode::MergeConcurrent,
            PreparedStoreOperationCommit::Serial(_),
        ) => Err(StoreOutboundError::InvalidOutbound(
            "Merge publication received a Serial Store candidate".to_string(),
        )),
        (
            StoreOperationPublicationMode::Serial { .. },
            PreparedStoreOperationCommit::MergeConcurrent(_),
        ) => Err(StoreOutboundError::InvalidOutbound(
            "Serial publication received a Merge Store candidate".to_string(),
        )),
    }
}

pub(crate) struct PreparedStoreOperationActivation {
    pub(crate) candidate: Box<PreparedStoreOperationCommit>,
    pub(crate) retained_operation_objects: Vec<ExactObjectRef>,
}

pub(crate) enum StoreMembershipJournalCompletion {
    Mutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
    RotationMutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
    OwnerPromotion {
        transition: super::owner_promotion::OwnerPromotionJournalTransition,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
}

impl StoreMembershipJournalCompletion {
    pub(crate) fn object_refs(&self) -> Vec<ExactObjectRef> {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .map(|remote| remote.object().clone())
            .collect()
    }

    pub(crate) fn remote_object(
        &self,
        object: &ExactObjectRef,
    ) -> Result<super::remote_object::RemoteObjectRecord, StoreOutboundError> {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .find(|remote| remote.object() == object)
            .cloned()
            .ok_or_else(|| {
                StoreOutboundError::InvalidOutbound(
                    "membership completion omits an exact activated object".to_string(),
                )
            })
    }

    pub(crate) fn complete_on(
        self,
        tx: &rusqlite::Transaction<'_>,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), crate::database::DbError> {
        match self {
            Self::Mutation {
                intent_hash,
                progress_bytes,
                remote_objects,
            } => Database::record_activated_membership_candidate_mutation_on(
                tx,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::WithoutRotation,
            ),
            Self::RotationMutation {
                intent_hash,
                progress_bytes,
                generation,
                remote_objects,
            } => Database::record_activated_membership_candidate_mutation_on(
                tx,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::Rotation { generation },
            ),
            Self::OwnerPromotion {
                transition,
                remote_objects,
            } => {
                let mut unique = std::collections::BTreeSet::new();
                let object_ids = remote_objects
                    .iter()
                    .map(|remote| remote.object_id())
                    .map(|object_id| {
                        if unique.insert(object_id) {
                            Ok(object_id)
                        } else {
                            Err(crate::database::DbError::Message(
                                "activated Owner-promotion graph repeats an exact object"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if object_ids.is_empty() {
                    return Err(crate::database::DbError::Message(
                        "activated Owner-promotion graph is empty".to_string(),
                    ));
                }
                Database::activate_store_operation_remote_objects_on(tx, candidate, &object_ids)?;
                let (journal_key, target_key, previous_value, next_value, remote_objects) =
                    transition.into_values();
                Database::advance_owner_promotion_journal_on(
                    tx,
                    journal_key,
                    target_key,
                    previous_value,
                    next_value,
                    remote_objects,
                )
            }
        }
    }
}

fn retained_store_operation_objects(
    commit: &StoreBatchCommit,
) -> Result<Vec<ExactObjectRef>, StoreOutboundError> {
    let objects = commit
        .acknowledgement()
        .map(|reference| reference.object.clone())
        .into_iter()
        .chain(
            commit
                .control()
                .into_iter()
                .flat_map(StoreControl::introduced_wrapped_keys)
                .map(|reference| reference.object.clone()),
        )
        .chain(
            commit
                .device_exclusion_proposals()
                .iter()
                .map(|reference| reference.object.clone()),
        )
        .chain(
            commit
                .device_exclusion_outcomes()
                .iter()
                .map(|reference| reference.object().clone()),
        )
        .chain(
            commit
                .reclaim_authorization()
                .into_iter()
                .flat_map(|reference| {
                    [reference.evidence.object.clone(), reference.object.clone()]
                }),
        )
        .chain(
            commit
                .reclaim_receipt()
                .map(|reference| reference.object.clone()),
        )
        .collect::<Vec<_>>();
    if objects
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != objects.len()
    {
        return Err(StoreOutboundError::InvalidOutbound(
            "Store operation publication repeats a retained authority object".to_string(),
        ));
    }
    Ok(objects)
}

pub(crate) async fn finish_nonactivating_store_ack(
    db: &Database,
    storage: &dyn SyncStorage,
    acknowledgement: super::store_commit::StoreAckRef,
) -> Result<(), StoreOutboundError> {
    let targets = db
        .nonactivating_outbound_store_ack_cleanup_targets(acknowledgement.clone())
        .await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    db.complete_nonactivating_outbound_store_ack(acknowledgement)
        .await?;
    Ok(())
}

pub(crate) async fn activate_store_operation_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    mode: StoreOperationPublicationMode<'_>,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<StoreBatchCommitRef, StoreOutboundError> {
    let prepared = prepare_store_operation_candidate(db, storage, plan, batch).await?;
    match publish_prepared_store_operation(db, storage, mode, Box::new(prepared)).await? {
        StoreOperationPublicationOutcome::Activated(reference) => Ok(reference),
        StoreOperationPublicationOutcome::Nonactivated(reference) => {
            Err(StoreOutboundError::InvalidOutbound(format!(
                "Store operation candidate {} did not activate",
                reference.commit_hash
            )))
        }
        StoreOperationPublicationOutcome::Reprepared => Err(StoreOutboundError::InvalidOutbound(
            "Store operation was reprepared during immediate activation".to_string(),
        )),
        StoreOperationPublicationOutcome::RepreparedCandidate(_)
        | StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
            Err(StoreOutboundError::InvalidOutbound(
                "unpersisted Store operation encountered an activation conflict".to_string(),
            ))
        }
    }
}

#[cfg(test)]
pub(crate) async fn activate_test_serial_control_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    signer: &UserKeypair,
    control: StoreControl,
    wraps: Vec<super::wrapped_store_key::PreparedWrappedStoreKey>,
) -> Result<PreparedStoreOperationCommit, StoreOutboundError> {
    let plan = prepare_store_operation_commit(
        db,
        storage,
        StoreOperationPreparation::Serial { coordination },
        device_id,
        signer,
    )
    .await?;
    let candidate =
        prepare_store_operation_candidate(db, storage, plan, StoreOperationBatch::Control(control))
            .await?;
    let remotes = candidate.membership_control_remote_objects(&wraps)?;
    let plan_bytes = serde_json::to_vec(&candidate).map_err(|error| {
        StoreOutboundError::InvalidOutbound(format!(
            "serialize test Serial control candidate: {error}"
        ))
    })?;
    let intent_hash = db
        .stage_membership_candidate_mutation(
            plan_bytes,
            b"test_serial_control".to_vec(),
            remotes,
            None,
        )
        .await?;
    let root = required_store_root(db).await?;
    super::membership_ops::publish_serial_membership_wraps(db, storage, &root, &candidate, &wraps)
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    match publish_prepared_store_operation(
        db,
        storage,
        StoreOperationPublicationMode::Serial { coordination },
        Box::new(candidate.clone()),
    )
    .await?
    {
        StoreOperationPublicationOutcome::Activated(reference)
            if reference == candidate.reference => {}
        outcome => {
            return Err(StoreOutboundError::InvalidOutbound(format!(
                "test Serial control candidate did not activate: {outcome:?}"
            )))
        }
    }
    db.complete_membership_mutation(intent_hash).await?;
    Ok(candidate)
}

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
    let publication = prepared.publication.clone();
    let activation = PreparedStoreOperationActivation {
        candidate: prepared,
        retained_operation_objects,
    };
    match (mode, publication) {
        (
            StoreOperationPublicationMode::Serial { coordination },
            StoreOperationPublication::Serial {
                base_head,
                head,
                authorization_after,
            },
        ) => {
            if membership_objects.is_some() {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Serial membership publication received Merge membership objects".to_string(),
                ));
            }
            let attempt = Box::pin(publish_prepared_serial_store_operation(
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
                SerialStoreOperationAttempt::Activated(reference) => {
                    Ok(StoreOperationPublicationOutcome::Activated(reference))
                }
                SerialStoreOperationAttempt::Conflict {
                    activation,
                    commit,
                    reference,
                    authorization_after,
                    membership_completion,
                } => {
                    Box::pin(resolve_serial_store_operation_conflict(
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
            StoreOperationPublication::MergeConcurrent {
                head,
                prepared: prepared_head,
                history_summary,
            },
        ) => {
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
            Box::pin(publish_prepared_merge_store_operation(
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
            StoreOperationPublication::Serial { .. },
        ) => Err(StoreOutboundError::InvalidOutbound(
            "Merge publication received a Serial Store candidate".to_string(),
        )),
        (
            StoreOperationPublicationMode::Serial { .. },
            StoreOperationPublication::MergeConcurrent { .. },
        ) => Err(StoreOutboundError::InvalidOutbound(
            "Serial publication received a Merge Store candidate".to_string(),
        )),
    }
}

pub(crate) async fn upload_prepared_merge_store_operation_commit(
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

async fn reload_uploaded_store_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    reference: &StoreBatchCommitRef,
) -> Result<super::store_commit::VerifiedStoreDeviceOperations, StoreOutboundError> {
    let StoreCommitCoord::Serial { .. } = &reference.coord else {
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
    super::store_pull::load_local_commit_device_operations(db, storage, root, commit)
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
}

struct PreparedStoreOperationActivation {
    candidate: Box<PreparedStoreOperationCommit>,
    retained_operation_objects: Vec<ExactObjectRef>,
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
    fn object_refs(&self) -> Vec<ExactObjectRef> {
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

    fn complete_on(
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

enum SerialStoreOperationAttempt {
    Activated(StoreBatchCommitRef),
    Conflict {
        activation: PreparedStoreOperationActivation,
        commit: Box<StoreBatchCommit>,
        reference: StoreBatchCommitRef,
        authorization_after: Box<SerialAuthorizationState>,
        membership_completion: Option<StoreMembershipJournalCompletion>,
    },
}

async fn publish_prepared_serial_store_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    activation: PreparedStoreOperationActivation,
    base_head: VersionedObject,
    head: StoreSerialHead,
    authorization_after: SerialAuthorizationState,
    membership_completion: Option<StoreMembershipJournalCompletion>,
) -> Result<SerialStoreOperationAttempt, StoreOutboundError> {
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
            return Ok(SerialStoreOperationAttempt::Conflict {
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
    Box::pin(record_activated_serial_store_operation(
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
    Ok(SerialStoreOperationAttempt::Activated(reference))
}

async fn record_activated_serial_store_operation(
    db: &Database,
    activation: PreparedStoreOperationActivation,
    device_operations: super::store_commit::VerifiedStoreDeviceOperations,
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
        std::iter::once(super::remote_object::remote_object_id(&reference.object))
            .chain(
                activation
                    .retained_operation_objects
                    .iter()
                    .map(super::remote_object::remote_object_id),
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
            .map(super::remote_object::remote_object_id)
            .collect::<std::collections::BTreeSet<_>>();
        if !completion_ids.contains(&super::remote_object::remote_object_id(&reference.object))
            || activation.retained_operation_objects.iter().any(|object| {
                !completion_ids.contains(&super::remote_object::remote_object_id(object))
            })
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial membership completion does not cover its exact activated graph".to_string(),
            ));
        }
    }
    let recorded_ref = reference.clone();
    let registration_activation = activation.candidate.registration_activation.clone();
    let stream_activations =
        super::circle_activation::VerifiedStreamActivations::none(&commit, &recorded_ref)
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

async fn resolve_serial_store_operation_conflict(
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
    match super::store_pull::observe_serial_successors_after(
        storage,
        coordination,
        &root,
        predecessor,
    )
    .await?
    {
        super::store_pull::SerialSuccessorObservation::Unchanged(observed) => {
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
        super::store_pull::SerialSuccessorObservation::Advanced(suffix) => {
            if suffix.commits().first() == Some(&reference) {
                let device_operations = Box::pin(reload_uploaded_store_device_operations(
                    db, storage, &root, &commit, &reference,
                ))
                .await?;
                Box::pin(record_activated_serial_store_operation(
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
            let nonactivation = super::remote_object::VerifiedCandidateNonactivation::serial(
                &suffix,
                vec![(
                    StoreBatchCommitDeletionTarget {
                        coord: reference.coord.clone(),
                        object: reference.object.clone(),
                        canonical_signed_bytes: commit.to_bytes(),
                    },
                    author,
                )],
            )
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

fn publish_prepared_merge_store_operation<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    root: StoreRootRef,
    activation: PreparedStoreOperationActivation,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    history_summary: super::store_commit::RetainedVerifiedMergeHistorySummary,
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
        upload_prepared_merge_store_operation_commit(storage, &activation.candidate).await?;
        let membership_heads = match &commit.membership_state {
            super::circle_control::StoreMembershipStateRef::MergeConcurrent(state) => &state.heads,
            super::circle_control::StoreMembershipStateRef::Serial(_) => {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Merge publication carries Serial membership authority".to_string(),
                ));
            }
        };
        let authorization = Box::pin(
            super::store_pull::load_retained_merge_outbound_authorization(
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
            super::store_pull::load_local_commit_device_operations_with_merge_membership(
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
                return Box::pin(resolve_merge_store_operation_head_collision(
                    db,
                    storage,
                    activation,
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
                std::iter::once(super::remote_object::remote_object_id(&reference.object))
                    .chain(
                        activation
                            .retained_operation_objects
                            .iter()
                            .map(super::remote_object::remote_object_id),
                    )
                    .chain(std::iter::once(super::remote_object::remote_object_id(
                        prepared_head.reference(),
                    )))
                    .collect::<Vec<_>>()
            })
        } else {
            None
        };
        if let Some(completion) = &membership_completion {
            let completion_ids = completion
                .object_refs()
                .iter()
                .map(super::remote_object::remote_object_id)
                .collect::<std::collections::BTreeSet<_>>();
            if completion_ids.is_empty()
                || !completion_ids
                    .contains(&super::remote_object::remote_object_id(&reference.object))
                || !completion_ids.contains(&super::remote_object::remote_object_id(
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

async fn resolve_merge_store_operation_head_collision(
    db: &Database,
    storage: &dyn SyncStorage,
    mut activation: PreparedStoreOperationActivation,
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
        activation
            .candidate
            .adopt_merge_head(winner, winner_prepared)?;
        return Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
            activation.candidate,
        ));
    }
    let registration = db
        .activated_store_device_registration(commit.author_registration.clone())
        .await?;
    let nonactivation = super::remote_object::VerifiedCandidateNonactivation::merge(
        &observation,
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
            candidate: activation.candidate,
            nonactivation: Box::new(nonactivation),
        });
    };
    db.begin_outbound_store_ack_nonactivation(acknowledgement.clone(), nonactivation)
        .await?;
    finish_nonactivating_store_ack(db, storage, acknowledgement).await?;
    Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
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

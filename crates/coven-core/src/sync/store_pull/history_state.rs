use super::*;

pub(super) async fn replay_merge_device_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: &StoreBatchCommitRef,
) -> Result<
    (
        ResolvedStoreDeviceState,
        VerifiedStoreDeviceOperations,
        StoreBatchCommit,
        Option<VerifiedCircleActivations>,
    ),
    StorePullError,
> {
    let history = verify_merge_history_refs(storage, root, [tip.clone()]).await?;
    let verified = history.commits.get(tip).ok_or_else(|| {
        StorePullError::Database(
            "author exclusion activation is absent from its verified history".to_string(),
        )
    })?;
    Ok((
        verified.predecessor_state.clone(),
        verified.operations.clone(),
        verified.commit.clone(),
        verified
            .membership_control
            .as_ref()
            .map(|control| control.activations.clone()),
    ))
}

pub(crate) struct VerifiedActivatedStoreAck {
    reference: super::store_commit::StoreAckRef,
    value: super::store_commit::StoreAck,
    chain: BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    activating_commit: StoreBatchCommitRef,
    activating_commit_value: StoreBatchCommit,
}

enum VerifiedStoreMembership {
    MergeConcurrent {
        membership: MembershipChain,
        checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
    },
    Serial(SerialAuthorizationState),
}

pub(crate) struct VerifiedStoreHistoryState {
    cut: StoreHistoryCut,
    membership_ref: StoreMembershipStateRef,
    membership: VerifiedStoreMembership,
    device_state_ref: StoreDeviceStateRef,
    device_state: ResolvedStoreDeviceState,
    active_registrations: BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
}

impl VerifiedStoreHistoryState {
    fn is_owner(&self, author: &str) -> bool {
        match &self.membership {
            VerifiedStoreMembership::MergeConcurrent { membership, .. } => {
                membership.is_owner_now(author)
            }
            VerifiedStoreMembership::Serial(authorization) => {
                authorization.membership.is_owner(author)
            }
        }
    }
}

pub(super) async fn load_active_history_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
) -> Result<
    BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
    StorePullError,
> {
    let mut active = BTreeMap::new();
    for (device_id, record) in &state.devices {
        if !matches!(record.status, StoreDeviceStatus::Active) {
            continue;
        }
        let registration = load_registration_ref(storage, root, &record.registration).await?;
        if registration.value.device_id != *device_id {
            return Err(StorePullError::Database(
                "resolved Store device state names another exact registration".to_string(),
            ));
        }
        active.insert(
            *device_id,
            (record.registration.clone(), registration.value),
        );
    }
    Ok(active)
}

pub(crate) fn verify_store_history_state<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    cut: &'a StoreHistoryCut,
    membership_ref: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, VerifiedStoreHistoryState> {
    Box::pin(verify_store_history_state_impl(
        storage,
        serial_coordination,
        root,
        cut,
        membership_ref,
    ))
}

async fn verify_store_history_state_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedStoreHistoryState, StorePullError> {
    match (cut, membership_ref) {
        (
            StoreHistoryCut::MergeConcurrent(frontier),
            StoreMembershipStateRef::MergeConcurrent(_),
        ) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge history verification received Serial coordination".to_string(),
                ));
            }
            let history = Box::pin(verify_merge_history_refs(
                storage,
                root,
                frontier.values().cloned().collect::<Vec<_>>(),
            ))
            .await?;
            let device_state = if frontier.is_empty() {
                history.genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    frontier
                        .values()
                        .map(|reference| {
                            history
                                .commits
                                .get(reference)
                                .map(|commit| commit.state_after.clone())
                                .ok_or_else(|| {
                                    StorePullError::Database(
                                        "Merge history frontier is absent from its verified graph"
                                            .to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?
            };
            let device_state_ref = StoreDeviceStateRef::merge_concurrent(
                CommitFrontier::MergeConcurrent(frontier.clone()),
                &device_state,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            let verified_membership_activations =
                verified_merge_membership_prefix(&history.commits, frontier.values().cloned())?;
            let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
                storage,
                root,
                membership_ref,
                &verified_membership_activations,
                None,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            verified_membership_activations
                .validate_complete_membership(&membership)
                .map_err(StorePullError::Database)?;
            verify_merge_membership_state_ref(membership_ref, &membership, &device_state)?;
            let active_registrations = Box::pin(load_active_history_registrations(
                storage,
                root,
                &device_state,
            ))
            .await?;
            let checkpoints = frontier
                .values()
                .map(|reference| {
                    history
                        .commits
                        .get(reference)
                        .map(|commit| commit.history.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge snapshot frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(VerifiedStoreHistoryState {
                cut: cut.clone(),
                membership_ref: membership_ref.clone(),
                membership: VerifiedStoreMembership::MergeConcurrent {
                    membership,
                    checkpoints,
                },
                device_state_ref,
                device_state,
                active_registrations,
            })
        }
        (StoreHistoryCut::Serial(position), StoreMembershipStateRef::Serial(_)) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial history verification requires coordination capability".to_string(),
                )
            })?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let (_, genesis_authorization, genesis_state) =
                Box::pin(load_authorized_serial_prefix(storage, root, None)).await?;
            let founder = load_founder_registration(storage, root).await?;
            let founder_ref = StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object.clone(),
            );
            let expected_genesis = super::store_commit::StoreSerialPredecessor::Genesis {
                root: root.clone(),
                founder_registration: founder_ref,
            };
            let accepted_prefix = match position {
                super::store_commit::StoreSerialPredecessor::Genesis { .. }
                    if position == &expected_genesis =>
                {
                    &accepted[..0]
                }
                super::store_commit::StoreSerialPredecessor::Genesis { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial history cut names another genesis authority".to_string(),
                    ));
                }
                super::store_commit::StoreSerialPredecessor::Commit(reference) => {
                    let index = accepted
                        .iter()
                        .position(|candidate| &candidate.commit_ref == reference)
                        .ok_or_else(|| {
                            StorePullError::Serial(
                                "Serial history cut is absent from the signed coordinated chain"
                                    .to_string(),
                            )
                        })?;
                    &accepted[..=index]
                }
            };
            let (authorization, device_state) = accepted_prefix.last().map_or_else(
                || (genesis_authorization, genesis_state),
                |accepted| {
                    (
                        accepted.authorization_after.clone(),
                        accepted.device_state_after.clone(),
                    )
                },
            );
            let expected_membership = StoreMembershipStateRef::serial(
                position.clone(),
                device_state.recovery.clone(),
                &authorization,
            )
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
            if &expected_membership != membership_ref {
                return Err(StorePullError::Serial(
                    "Serial history membership reference differs from its accepted state"
                        .to_string(),
                ));
            }
            let device_state_ref = StoreDeviceStateRef::serial(position.clone(), &device_state)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            let active_registrations =
                load_active_history_registrations(storage, root, &device_state).await?;
            Ok(VerifiedStoreHistoryState {
                cut: cut.clone(),
                membership_ref: expected_membership,
                membership: VerifiedStoreMembership::Serial(authorization),
                device_state_ref,
                device_state,
                active_registrations,
            })
        }
        _ => Err(StorePullError::Database(
            "Store history cut and membership state use different policies".to_string(),
        )),
    }
}

#[derive(Debug)]
pub(crate) struct VerifiedStoreSnapshotStability {
    authority: super::retained_replay::RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
    pub(crate) fn into_authority(self) -> super::retained_replay::RetainedReplaySnapshotAuthority {
        self.authority
    }
}

fn snapshot_history_cut(
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<StoreHistoryCut, StorePullError> {
    match (&snapshot.meta.coverage, &snapshot.meta.state.devices) {
        (
            CommitFrontier::MergeConcurrent(frontier),
            StoreDeviceStateRef::MergeConcurrent { .. },
        ) => Ok(StoreHistoryCut::MergeConcurrent(frontier.clone())),
        (CommitFrontier::Serial(_), StoreDeviceStateRef::Serial { position, .. }) => {
            Ok(StoreHistoryCut::Serial(position.clone()))
        }
        _ => Err(StorePullError::Database(
            "Store snapshot coverage and device state use different policies".to_string(),
        )),
    }
}

async fn accepted_history_cut_for_snapshot_stability(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot_state: &VerifiedStoreHistoryState,
) -> Result<StoreHistoryCut, StorePullError> {
    match &snapshot_state.cut {
        StoreHistoryCut::MergeConcurrent(snapshot_frontier) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge snapshot stability received Serial coordination".to_string(),
                ));
            }
            let mut accepted = snapshot_frontier.clone();
            for (registration_ref, registration) in snapshot_state.active_registrations.values() {
                let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    registration_ref,
                    super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
                let discovery =
                    discover_merge_stream(storage, root, registration_ref, registration, None)
                        .await?;
                let Some((_, _, latest, _)) = discovery.commits.last() else {
                    if accepted.contains_key(&stream_id) {
                        return Err(StorePullError::Database(
                            "accepted Merge snapshot history is absent from its author stream"
                                .to_string(),
                        ));
                    }
                    continue;
                };
                if let Some(snapshot_tip) = accepted.get(&stream_id) {
                    if latest.coord.sequence() < snapshot_tip.coord.sequence()
                        || (latest.coord.sequence() == snapshot_tip.coord.sequence()
                            && latest != snapshot_tip)
                    {
                        return Err(StorePullError::Database(
                            "current Merge author stream does not contain the snapshot cut"
                                .to_string(),
                        ));
                    }
                }
                accepted.insert(stream_id, latest.clone());
            }
            Ok(StoreHistoryCut::MergeConcurrent(accepted))
        }
        StoreHistoryCut::Serial(_) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial snapshot stability requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            Ok(StoreHistoryCut::Serial(match head.head.state {
                StoreSerialHeadState::Genesis {
                    root,
                    founder_registration,
                } => StoreSerialPredecessor::Genesis {
                    root,
                    founder_registration,
                },
                StoreSerialHeadState::Commit { commit, .. } => {
                    StoreSerialPredecessor::Commit(commit)
                }
            }))
        }
    }
}

fn activated_acknowledgements_through_cut<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    cut: &'a StoreHistoryCut,
) -> StorePullFuture<'a, Vec<VerifiedActivatedStoreAck>> {
    Box::pin(activated_acknowledgements_through_cut_impl(
        storage,
        serial_coordination,
        root,
        cut,
    ))
}

async fn activated_acknowledgements_through_cut_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
    match cut {
        StoreHistoryCut::MergeConcurrent(frontier) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge acknowledgement history received Serial coordination".to_string(),
                ));
            }
            let history = verify_merge_history_refs(
                storage,
                root,
                frontier.values().cloned().collect::<Vec<_>>(),
            )
            .await?;
            let mut acknowledgements = Vec::new();
            for (activating_commit, commit) in history.commits {
                let Some((reference, value)) = commit.acknowledgement else {
                    continue;
                };
                let chain = commit
                    .history
                    .summary
                    .acknowledgements
                    .get(&reference.registration.device_id)
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "verified acknowledgement history lacks its exact chain".to_string(),
                        )
                    })?
                    .chain
                    .clone();
                acknowledgements.push(VerifiedActivatedStoreAck {
                    reference,
                    value,
                    chain,
                    activating_commit,
                    activating_commit_value: commit.commit,
                });
            }
            Ok(acknowledgements)
        }
        StoreHistoryCut::Serial(position) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial acknowledgement history requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &head.head).await?;
            let prefix = match position {
                StoreSerialPredecessor::Genesis {
                    root: cut_root,
                    founder_registration,
                } => {
                    let founder = load_founder_registration(storage, root).await?;
                    let founder_ref = StoreDeviceRegistrationRef::from_registration(
                        &founder.value,
                        founder.object,
                    );
                    if cut_root != root || founder_registration != &founder_ref {
                        return Err(StorePullError::Serial(
                            "Serial acknowledgement cut names another genesis authority"
                                .to_string(),
                        ));
                    }
                    &accepted[..0]
                }
                StoreSerialPredecessor::Commit(reference) => {
                    let index = accepted
                        .iter()
                        .position(|candidate| &candidate.commit_ref == reference)
                        .ok_or_else(|| {
                            StorePullError::Serial(
                                "Serial acknowledgement cut is absent from the accepted chain"
                                    .to_string(),
                            )
                        })?;
                    &accepted[..=index]
                }
            };
            let mut acknowledgements = Vec::new();
            for accepted in prefix {
                let Some((reference, value)) = &accepted.acknowledgement else {
                    continue;
                };
                let chain = load_acknowledgement_proof_chain(
                    storage,
                    root,
                    reference.clone(),
                    value.clone(),
                    &accepted.author,
                )
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
                })?;
                acknowledgements.push(VerifiedActivatedStoreAck {
                    reference: reference.clone(),
                    value: value.clone(),
                    chain,
                    activating_commit: accepted.commit_ref.clone(),
                    activating_commit_value: accepted.commit.clone(),
                });
            }
            Ok(acknowledgements)
        }
    }
}

fn verify_store_snapshot_authority<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    snapshot: &'a crate::database::PublishedStoreSnapshot,
) -> StorePullFuture<'a, VerifiedStoreHistoryState> {
    Box::pin(verify_store_snapshot_authority_impl(
        storage,
        serial_coordination,
        root,
        snapshot,
    ))
}

async fn verify_store_snapshot_authority_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreHistoryState, StorePullError> {
    let snapshot_cut = snapshot_history_cut(snapshot)?;
    let snapshot_state = verify_store_history_state(
        storage,
        serial_coordination,
        root,
        &snapshot_cut,
        &snapshot.meta.state.membership,
    )
    .await?;
    if snapshot_state.membership_ref != snapshot.meta.state.membership
        || snapshot_state.device_state_ref != snapshot.meta.state.devices
    {
        return Err(StorePullError::Database(
            "Store snapshot state differs from its exact accepted history".to_string(),
        ));
    }
    let (_, snapshot_author) = snapshot_state
        .active_registrations
        .get(&snapshot.meta.author_registration.device_id)
        .filter(|(reference, _)| reference == &snapshot.meta.author_registration)
        .ok_or(StorePullError::SnapshotAuthorInactive)?;
    if !snapshot_state.is_owner(&snapshot_author.author_pubkey) {
        return Err(StorePullError::SnapshotAuthorNotOwner);
    }
    match (&snapshot_state.membership, &snapshot.meta.history_summary) {
        (
            VerifiedStoreMembership::MergeConcurrent {
                membership,
                checkpoints,
            },
            super::store_commit::StoreSnapshotHistorySummary::MergeConcurrent(summary),
        ) => {
            let canonical = compose_merge_snapshot_history_summary(
                root,
                &snapshot.meta.coverage,
                membership,
                &snapshot_state.device_state,
                &snapshot.meta.author_registration,
                snapshot_author,
                checkpoints.clone(),
            )?;
            if summary != &canonical {
                return Err(StorePullError::Database(
                    "Store snapshot history summary differs from its exact verified cut"
                        .to_string(),
                ));
            }
        }
        (
            VerifiedStoreMembership::Serial(_),
            super::store_commit::StoreSnapshotHistorySummary::Serial,
        ) => {}
        _ => {
            return Err(StorePullError::Database(
                "Store snapshot history summary uses another write policy".to_string(),
            ));
        }
    }
    Ok(snapshot_state)
}

pub(crate) async fn verify_store_snapshot_for_acknowledgement(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), StorePullError> {
    verify_store_snapshot_authority(storage, serial_coordination, root, snapshot)
        .await
        .map(|_| ())
}

pub(crate) fn verify_store_snapshot_stability<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    snapshot: &'a crate::database::PublishedStoreSnapshot,
) -> StorePullFuture<'a, VerifiedStoreSnapshotStability> {
    Box::pin(verify_store_snapshot_stability_impl(
        storage,
        serial_coordination,
        root,
        snapshot,
    ))
}

async fn verify_store_snapshot_stability_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let snapshot_state =
        verify_store_snapshot_authority(storage, serial_coordination, root, snapshot).await?;
    let snapshot_cut = snapshot_state.cut.clone();

    let accepted_cut = Box::pin(accepted_history_cut_for_snapshot_stability(
        storage,
        serial_coordination,
        root,
        &snapshot_state,
    ))
    .await?;
    let accepted_acknowledgements =
        activated_acknowledgements_through_cut(storage, serial_coordination, root, &accepted_cut)
            .await?;
    let mut acknowledgements = BTreeMap::new();
    for (device_id, (registration_ref, registration)) in &snapshot_state.active_registrations {
        let matching = accepted_acknowledgements
            .iter()
            .filter(|ack| {
                ack.value.registration == *registration_ref
                    && ack.value.snapshot.as_ref().is_some_and(|acknowledged| {
                        acknowledged.author_registration == snapshot.meta.author_registration
                            && acknowledged.snapshot == snapshot.reference
                    })
                    && ack.value.device_state == snapshot.meta.state.devices
                    && ack
                        .value
                        .store_cut
                        .frontier()
                        .covers(&snapshot.meta.coverage)
            })
            .max_by_key(|ack| (ack.reference.sequence, ack.activating_commit.clone()))
            .ok_or_else(|| StorePullError::SnapshotNotStable {
                member: registration.author_pubkey.clone(),
                device_id: device_id.to_string(),
            })?;
        acknowledgements.insert(
            *device_id,
            super::store_commit::RetainedVerifiedActivatedAck {
                chain: matching.chain.clone(),
                activating_commit: matching.activating_commit.clone(),
                activating_commit_value: matching.activating_commit_value.clone(),
            },
        );
    }
    let founder = load_founder_registration(storage, root).await?;
    let authority = super::retained_replay::RetainedReplaySnapshotAuthority {
        store_root: root.clone(),
        founder_registration: StoreDeviceRegistrationRef::from_registration(
            &founder.value,
            founder.object,
        ),
        snapshot: snapshot.reference.clone(),
        metadata: snapshot.meta.clone(),
        snapshot_cut,
        accepted_cut,
        device_state: snapshot_state.device_state,
        active_registrations: snapshot_state
            .active_registrations
            .into_iter()
            .map(|(device_id, (reference, value))| {
                (
                    device_id,
                    super::store_commit::RetainedVerifiedRegistration { reference, value },
                )
            })
            .collect(),
        acknowledgements,
    };
    authority
        .validate()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(VerifiedStoreSnapshotStability { authority })
}

async fn verify_merge_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<(), StorePullError> {
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        ..
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let history = verify_merge_history_refs(storage, root, [activation_commit.clone()]).await?;
    verify_merge_owner_promotion_acceptance_with_history(
        storage,
        root,
        acceptance,
        &history.commits,
    )
    .await
}

pub(super) async fn verify_merge_owner_promotion_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<(), StorePullError> {
    let request = &acceptance.request;
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        head: activation_head,
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    acceptance
        .verify(&candidate.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;

    let head_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_bytes = storage
        .read_protocol_object(&context, &activation_head.object, &head_prefix)
        .await?;
    activation_head.object.verify(&head_bytes)?;
    let head: StoreDeviceHead = serde_json::from_slice(&head_bytes).map_err(|error| {
        StorePullError::Database(format!("Owner-promotion activation head: {error}"))
    })?;
    let opened = super::store_objects::load_head_ref(
        storage,
        root.store_root_hash,
        activation_head,
        &promoter.value,
        activation_commit,
    )
    .await?;
    let (_, exact_head) = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &request.promoter_registration,
        &promoter.value,
        Some(activation_commit),
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if opened.value != head
        || head.head_hash() != activation_head.head_hash
        || head.commit != *activation_commit
        || exact_head.as_ref() != Some(activation_head)
    {
        return Err(StorePullError::Database(
            "Owner-promotion request is not activated by its exact Merge head".to_string(),
        ));
    }
    let verified = verified_commits.get(activation_commit).ok_or_else(|| {
        StorePullError::Database(
            "Owner-promotion request activation is absent from its verified history".to_string(),
        )
    })?;
    if verified.commit.owner_promotion_request() != Some(request)
        || verified.commit.membership_state != request.predecessor_membership
        || verified.commit.device_state != request.predecessor_devices
        || verified.commit.author_registration != request.promoter_registration
    {
        return Err(StorePullError::Database(
            "Owner-promotion request commit differs from its signed predecessor authority"
                .to_string(),
        ));
    }
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(&verified.commit),
    )?;
    let membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &request.predecessor_membership,
        &verified_membership_activations,
        None,
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    verify_merge_membership_state_ref(
        &request.predecessor_membership,
        &membership,
        &verified.predecessor_state,
    )?;
    if !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.promoter_registration,
    ) || !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.member_registration,
    ) {
        return Err(StorePullError::Database(
            "Owner-promotion request registrations are not active at its exact predecessor"
                .to_string(),
        ));
    }
    if membership
        .active_owner_grant(&promoter.value.author_pubkey)
        .as_ref()
        != Some(&request.promoter_owner_grant)
        || membership.active_grant_ids(&request.member_pubkey)
            != BTreeSet::from([request.member_grant.clone()])
        || membership
            .active_grant(&request.member_grant)
            .is_none_or(|record| {
                record.member_pubkey != request.member_pubkey
                    || record.role != super::membership::StoreMembershipRoleGrant::Member
            })
        || candidate.value.author_pubkey != request.member_pubkey
    {
        return Err(StorePullError::Database(
            "Owner-promotion request does not name the exact active Owner and Member grants"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) enum VerifiedOwnerPromotionAcceptance {
    MergeConcurrent,
    Serial(crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot),
}

pub(crate) async fn verify_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, StorePullError> {
    match &acceptance.activation {
        super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent { .. } => {
            verify_merge_owner_promotion_acceptance(storage, root, acceptance).await?;
            Ok(VerifiedOwnerPromotionAcceptance::MergeConcurrent)
        }
        super::store_commit::OwnerPromotionRequestActivation::Serial { .. } => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial Owner-promotion verification requires coordination".to_string(),
                )
            })?;
            let request = &acceptance.request;
            let promoter =
                load_registration_ref(storage, root, &request.promoter_registration).await?;
            let candidate =
                load_registration_ref(storage, root, &request.member_registration).await?;
            request
                .verify(root, &promoter.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            acceptance
                .verify(&candidate.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let mut matches = accepted
                .iter()
                .filter(|candidate| candidate.commit.owner_promotion_request() == Some(request));
            let Some(activated) = matches.next() else {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has no accepted Serial activation".to_string(),
                ));
            };
            if matches.next().is_some() {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has more than one Serial activation".to_string(),
                ));
            }
            let discovered = super::store_commit::OwnerPromotionRequestActivation::Serial {
                commit: activated.commit_ref.clone(),
            };
            if discovered != acceptance.activation {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion acceptance names another activation".to_string(),
                ));
            }
            let commit = &activated.commit;
            if commit.owner_promotion_request() != Some(request)
                || commit.membership_state != request.predecessor_membership
                || commit.device_state != request.predecessor_devices
                || commit.author_registration != request.promoter_registration
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request commit differs from its signed authority"
                        .to_string(),
                ));
            }
            if !device_state_has_active_registration(
                &activated.device_state_before,
                &request.promoter_registration,
            ) || !device_state_has_active_registration(
                &activated.device_state_before,
                &request.member_registration,
            ) {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion registrations are not active at its predecessor"
                        .to_string(),
                ));
            }
            if activated
                .authorization_before
                .membership
                .active_owner_grant(&promoter.value.author_pubkey)
                .as_ref()
                != Some(&request.promoter_owner_grant)
                || activated
                    .authorization_before
                    .membership
                    .active_grant_ids(&request.member_pubkey)
                    != BTreeSet::from([request.member_grant.clone()])
                || !activated
                    .authorization_before
                    .membership
                    .is_member_grant(&request.member_pubkey, &request.member_grant)
                || candidate.value.author_pubkey != request.member_pubkey
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request does not name the active Owner and Member"
                        .to_string(),
                ));
            }
            let authorization = accepted
                .last()
                .ok_or_else(|| {
                    StorePullError::Serial(
                        "Serial Owner-promotion activation has no accepted commit".to_string(),
                    )
                })?
                .authorization_after
                .clone();
            let base = match &verified_head.head.state {
                StoreSerialHeadState::Genesis { .. } => None,
                StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
            };
            Ok(VerifiedOwnerPromotionAcceptance::Serial(
                crate::sync::store_engine::serial::publication::SerialAuthorizationSnapshot {
                    base,
                    base_head: verified_head.object,
                    authorization,
                },
            ))
        }
    }
}

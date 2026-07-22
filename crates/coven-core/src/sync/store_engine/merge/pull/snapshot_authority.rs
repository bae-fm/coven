use super::*;

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
}

async fn verify_history_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedMergeSnapshotState, StorePullError> {
    let StoreMembershipStateRef::MergeConcurrent(_) = membership_ref else {
        return Err(StorePullError::Database(
            "Merge snapshot carries Serial membership state".to_string(),
        ));
    };
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
                                "Merge snapshot frontier is absent from its verified graph"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
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
    let active_registrations =
        load_active_history_registrations(storage, root, &device_state).await?;
    let checkpoints = frontier
        .values()
        .map(|reference| {
            history
                .commits
                .get(reference)
                .map(|commit| commit.history.clone())
                .ok_or_else(|| {
                    StorePullError::Database(
                        "Merge snapshot frontier is absent from its verified history".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(VerifiedMergeSnapshotState {
        common: VerifiedSnapshotState {
            device_state,
            active_registrations,
        },
        membership,
        checkpoints,
    })
}

async fn verify_authority(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
    let (CommitFrontier::MergeConcurrent(frontier), StoreDeviceStateRef::MergeConcurrent { .. }) =
        (&snapshot.meta.coverage, &snapshot.meta.state.devices)
    else {
        return Err(StorePullError::Database(
            "Merge snapshot coverage or device state uses Serial policy".to_string(),
        ));
    };
    let state =
        verify_history_state(storage, root, frontier, &snapshot.meta.state.membership).await?;
    let expected_device_state = StoreDeviceStateRef::merge_concurrent(
        snapshot.meta.coverage.clone(),
        &state.common.device_state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if expected_device_state != snapshot.meta.state.devices {
        return Err(StorePullError::Database(
            "Merge snapshot device state differs from its exact verified history".to_string(),
        ));
    }
    let (_, author) = state
        .common
        .active_registrations
        .get(&snapshot.meta.author_registration.device_id)
        .filter(|(reference, _)| reference == &snapshot.meta.author_registration)
        .ok_or(StorePullError::SnapshotAuthorInactive)?;
    if !state.membership.is_owner_now(&author.author_pubkey) {
        return Err(StorePullError::SnapshotAuthorNotOwner);
    }
    let super::store_commit::StoreSnapshotHistorySummary::MergeConcurrent(summary) =
        &snapshot.meta.history_summary
    else {
        return Err(StorePullError::Database(
            "Merge snapshot carries a Serial history summary".to_string(),
        ));
    };
    let canonical = compose_merge_snapshot_history_summary(
        root,
        &snapshot.meta.coverage,
        &state.membership,
        &state.common.device_state,
        &snapshot.meta.author_registration,
        author,
        state.checkpoints.clone(),
    )?;
    if summary != &canonical {
        return Err(StorePullError::Database(
            "Merge snapshot history summary differs from its exact verified cut".to_string(),
        ));
    }
    Ok((StoreHistoryCut::MergeConcurrent(frontier.clone()), state))
}

async fn accepted_cut(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot_frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    state: &VerifiedMergeSnapshotState,
) -> Result<StoreHistoryCut, StorePullError> {
    let mut accepted = snapshot_frontier.clone();
    for (registration_ref, registration) in state.common.active_registrations.values() {
        let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            registration_ref,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let discovery =
            discover_merge_stream(storage, root, registration_ref, registration, None).await?;
        let Some((_, _, latest, _)) = discovery.commits.last() else {
            if accepted.contains_key(&stream_id) {
                return Err(StorePullError::Database(
                    "accepted Merge snapshot history is absent from its author stream".to_string(),
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
                    "current Merge author stream does not contain the snapshot cut".to_string(),
                ));
            }
        }
        accepted.insert(stream_id, latest.clone());
    }
    Ok(StoreHistoryCut::MergeConcurrent(accepted))
}

async fn activated_acknowledgements(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
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

pub(in crate::sync::store_engine) async fn verify_snapshot_for_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), StorePullError> {
    verify_authority(storage, root, snapshot).await.map(|_| ())
}

pub(in crate::sync::store_engine) async fn verify_snapshot_stability(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let (snapshot_cut, state) = verify_authority(storage, root, snapshot).await?;
    let StoreHistoryCut::MergeConcurrent(snapshot_frontier) = &snapshot_cut else {
        return Err(StorePullError::Database(
            "Merge snapshot authority produced Serial history".to_string(),
        ));
    };
    let accepted_cut = accepted_cut(storage, root, snapshot_frontier, &state).await?;
    let StoreHistoryCut::MergeConcurrent(accepted_frontier) = &accepted_cut else {
        return Err(StorePullError::Database(
            "Merge snapshot acceptance produced Serial history".to_string(),
        ));
    };
    let acknowledgements = activated_acknowledgements(storage, root, accepted_frontier).await?;
    assemble_snapshot_stability(
        storage,
        root,
        snapshot,
        snapshot_cut,
        accepted_cut,
        state.common,
        acknowledgements,
    )
    .await
}

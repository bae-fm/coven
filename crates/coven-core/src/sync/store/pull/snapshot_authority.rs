use super::*;

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
}

pub(super) struct VerifiedMergeHistoryAuthority {
    device_state: ResolvedStoreDeviceState,
    pub(super) membership: MembershipChain,
}

pub(super) async fn verify_merge_history_authority(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedMergeHistoryAuthority, StorePullError> {
    history_verifier
        .verify_refs(frontier.values().cloned())
        .await?;
    let history = history_verifier.history();
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
    let verified_membership_activations =
        verified_merge_membership_prefix(&history.commits, frontier.values().cloned())?;
    let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
        history_verifier.commit_verifier_ref(),
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
    Ok(VerifiedMergeHistoryAuthority {
        device_state,
        membership,
    })
}

async fn verify_history_state(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedMergeSnapshotState, StorePullError> {
    let authority =
        verify_merge_history_authority(history_verifier, frontier, membership_ref).await?;
    let storage = history_verifier.storage();
    let root = history_verifier.root().clone();
    let active_registrations =
        load_active_history_registrations(storage, &root, &authority.device_state).await?;
    let checkpoints = frontier
        .values()
        .map(|reference| {
            history_verifier
                .history()
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
            device_state: authority.device_state,
            active_registrations,
        },
        membership: authority.membership,
        checkpoints,
    })
}

async fn verify_authority(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
    let frontier = &snapshot.meta.coverage.0;
    let state =
        verify_history_state(history_verifier, frontier, &snapshot.meta.state.membership).await?;
    let root = history_verifier.root();
    let expected_device_state = StoreDeviceStateRef::from_resolved(
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
    let summary = &snapshot.meta.history_summary;
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
    Ok((StoreHistoryCut(frontier.clone()), state))
}

async fn accepted_cut(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    snapshot_frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    state: &VerifiedMergeSnapshotState,
) -> Result<StoreHistoryCut, StorePullError> {
    let root = history_verifier.root().clone();
    let mut accepted = snapshot_frontier.clone();
    for (registration_ref, registration) in state.common.active_registrations.values() {
        let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            registration_ref,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let discovery =
            discover_merge_stream(history_verifier, registration_ref, registration, None).await?;
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
    Ok(StoreHistoryCut(accepted))
}

async fn activated_acknowledgements(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
    history_verifier
        .verify_refs(frontier.values().cloned())
        .await?;
    let mut acknowledgements = Vec::new();
    for (activating_commit, commit) in &history_verifier.history().commits {
        let Some((reference, value)) = commit.acknowledgement.as_ref() else {
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
            reference: reference.clone(),
            value: value.clone(),
            chain,
            activating_commit: activating_commit.clone(),
            activating_commit_value: commit.verified.value().clone(),
        });
    }
    Ok(acknowledgements)
}

#[cfg(test)]
pub(in crate::sync::store) async fn verify_snapshots_for_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshots: &[crate::database::PublishedStoreSnapshot],
) -> Result<(), StorePullError> {
    let mut history_verifier = MergeHistoryVerifier::new(storage, root).await?;
    verify_snapshots_for_acknowledgement_with_history(&mut history_verifier, snapshots).await
}

pub(in crate::sync::store) async fn verify_snapshots_for_acknowledgement_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    snapshots: &[crate::database::PublishedStoreSnapshot],
) -> Result<(), StorePullError> {
    for snapshot in snapshots {
        verify_authority(history_verifier, snapshot).await?;
    }
    Ok(())
}

pub(in crate::sync::store) async fn verify_snapshot_stability(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let mut history_verifier = MergeHistoryVerifier::new(storage, root).await?;
    verify_snapshot_stability_with_history(&mut history_verifier, snapshot).await
}

pub(in crate::sync::store) async fn verify_snapshot_stability_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let (snapshot_cut, state) = verify_authority(history_verifier, snapshot).await?;
    let snapshot_frontier = &snapshot_cut.0;
    let accepted_cut = accepted_cut(history_verifier, snapshot_frontier, &state).await?;
    let accepted_frontier = &accepted_cut.0;
    let acknowledgements = activated_acknowledgements(history_verifier, accepted_frontier).await?;
    let storage = history_verifier.storage();
    let root = history_verifier.root();
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

use super::*;

pub(crate) struct MergeConflictResolutionAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}

fn validate_retained_membership_floors(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
    membership: &MembershipChain,
) -> Result<(), StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        !retained_membership_floor_is_included(&checkpoint.summary.membership_floor, membership)
    }) {
        return Err(StorePullError::Database(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn retained_membership_floor_is_included(
    floor: &super::store_commit::MembershipCausalFloor,
    membership: &MembershipChain,
) -> bool {
    floor
        .effective_coordinates
        .iter()
        .all(|coordinate| membership.effectively_contains_coord(coordinate))
        && floor.resolutions.iter().all(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_ok()
        })
}

async fn retained_merge_device_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), StorePullError> {
    let state = if checkpoints.is_empty() {
        let founder = load_founder_registration_with_root(storage, root, root_value).await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        ResolvedStoreDeviceState::founder(
            root,
            founder_ref,
            &root_value.descriptor.founder_pubkey,
            root_value.descriptor.founder_grant.clone(),
            &root_value.descriptor.founder_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    } else {
        ResolvedStoreDeviceState::merge(
            checkpoints
                .iter()
                .map(|checkpoint| checkpoint.post_state.clone()),
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let reference = StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.clone()), &state)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok((reference, state))
}

pub(crate) async fn retained_merge_device_state_for_order(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), StorePullError> {
    let frontier = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .0;
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
    {
        return Err(StorePullError::Database(
            "Merge device-state authority is missing a retained predecessor checkpoint".to_string(),
        ));
    }
    let root_value = load_store_protocol_root(storage, root).await?.value;
    retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await
}

pub(crate) async fn load_merge_conflict_resolution_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    candidate_membership_heads: &[super::membership::MembershipHeadRef],
    author_registration: &StoreDeviceRegistrationRef,
    resolver_pubkey: &str,
) -> Result<MergeConflictResolutionAuthorization, StorePullError> {
    let frontier = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .0;
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
    {
        return Err(StorePullError::Database(
            "Merge conflict resolution is missing its retained predecessor authority".to_string(),
        ));
    }
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
    let membership = super::membership_ops::project_anchored_chain_to_verified_store_prefix(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        candidate_membership_heads,
        &prefix,
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    validate_retained_membership_floors(&checkpoints, &membership)?;
    prefix
        .validate_complete_membership(&membership)
        .map_err(StorePullError::Database)?;
    let (device_state_ref, device_state) =
        retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await?;
    if !device_state_has_active_registration(&device_state, author_registration) {
        return Err(StorePullError::Database(
            "Merge conflict-resolution author is inactive at its predecessor cut".to_string(),
        ));
    }
    verify_canonical_owner_registration(
        storage,
        root,
        &device_state,
        resolver_pubkey,
        author_registration,
    )
    .await?;
    Ok(MergeConflictResolutionAuthorization {
        membership,
        device_state_ref,
        device_state,
    })
}

pub(crate) async fn load_retained_merge_outbound_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    candidate_membership_heads: &[super::membership::MembershipHeadRef],
    author_registration: &StoreDeviceRegistrationRef,
) -> Result<MergeOutboundAuthorization, StorePullError> {
    let frontier = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .0;
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
    {
        return Err(StorePullError::Database(
            "Merge outbound authorization is missing retained predecessor authority".to_string(),
        ));
    }
    let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let membership = super::membership_ops::project_anchored_chain_to_verified_store_prefix(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        candidate_membership_heads,
        &prefix,
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    validate_retained_membership_floors(&checkpoints, &membership)?;
    prefix
        .validate_complete_membership(&membership)
        .map_err(StorePullError::Database)?;
    let (device_state_ref, device_state) =
        retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await?;
    if !device_state_has_active_registration(&device_state, author_registration) {
        return Err(StorePullError::Database(
            "Merge outbound author is inactive at its exact predecessor cut".to_string(),
        ));
    }
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::Database(
            "Merge outbound predecessor membership is conflicted".to_string(),
        ));
    };
    let membership_state = StoreMembershipStateRef::from_parts(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        device_state.recovery.clone(),
        resolved.state_hash,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(MergeOutboundAuthorization {
        membership,
        membership_state,
        device_state_ref,
        device_state,
    })
}

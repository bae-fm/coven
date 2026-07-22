use super::*;

pub(crate) fn membership_authorizes(
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> bool {
    if commit.operations().is_none() {
        return true;
    }
    let Some(chain) = membership else {
        return false;
    };
    commit
        .membership_authority
        .as_ref()
        .is_some_and(|authority| chain.authorizes_write_authority(authority, &author.author_pubkey))
}

pub(crate) async fn verify_merge_membership_control(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<VerifiedCircleActivations, String> {
    let history = verify_merge_history_refs(storage, root, commit_predecessor_references(commit))
        .await
        .map_err(|error| error.to_string())?;
    let states = history
        .commits
        .iter()
        .map(|(reference, verified)| (reference.clone(), verified.state_after.clone()))
        .collect::<BTreeMap<_, _>>();
    let predecessor_state = verified_merge_predecessor_state(&history.genesis, &states, commit)
        .map_err(|error| error.to_string())?;
    let verified_membership_activations =
        verified_merge_membership_prefix(&history.commits, commit_predecessor_references(commit))
            .map_err(|error| error.to_string())?;
    let pending_resolution = verify_merge_resolution_activation_acceptance_with_history(
        storage,
        root,
        commit,
        &history.genesis,
        &history.commits,
    )
    .await
    .map_err(|error| error.to_string())?;
    let predecessor_membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &commit.membership_state,
        &verified_membership_activations,
        pending_resolution.as_ref(),
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => error.to_string(),
        RegistrationLoadError::Invalid(error) => error,
    })?;
    verify_merge_membership_state_ref(
        &commit.membership_state,
        &predecessor_membership,
        &predecessor_state,
    )
    .map_err(|error| error.to_string())?;
    verify_merge_membership_control_with_history(
        storage,
        root,
        commit_ref,
        commit,
        &predecessor_membership,
        &predecessor_state,
        &history.commits,
        pending_resolution.as_ref(),
    )
    .await
    .map(|(activations, _)| activations)
}

pub(crate) async fn verify_merge_membership_control_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    predecessor_membership: &MembershipChain,
    predecessor_state: &ResolvedStoreDeviceState,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<
    (
        VerifiedCircleActivations,
        Option<VerifiedMergeConflictResolutionActivation>,
    ),
    String,
> {
    let Some(super::store_commit::StoreControl { transition }) = commit.control() else {
        return Err("Merge membership verifier received another Store control".to_string());
    };
    let state = &commit.membership_state;
    let commit_author =
        super::store_objects::load_registration_ref(storage, root, &commit.author_registration)
            .await
            .map_err(|error| error.to_string())?;
    if transition.body.author_registration != commit.author_registration
        || transition.body.entry.coord.author_pubkey != commit_author.value.author_pubkey
        || transition.body.resolutions != state.resolutions
        || transition.body.successor.predecessor
            != transition
                .body
                .predecessor
                .as_ref()
                .map(|reference| reference.object.clone())
    {
        return Err("Merge membership transition differs from its Store authority".to_string());
    }
    match &transition.body.predecessor {
        Some(predecessor) if state.heads.binary_search(predecessor).is_err() => {
            return Err(
                "Merge membership transition predecessor is absent from its signed state"
                    .to_string(),
            )
        }
        None if state
            .heads
            .iter()
            .any(|head| head.coord.stream_key() == transition.body.entry.coord.stream_key()) =>
        {
            return Err(
                "first Merge membership transition has an existing signed predecessor".to_string(),
            )
        }
        _ => {}
    }
    let opened_entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await
    .map_err(|error| error.to_string())?;
    if opened_entry.value.coord() != transition.body.entry.coord
        || opened_entry.value.dependencies != predecessor_membership.effective_frontier()
        || opened_entry.value.resolution_dependencies != transition.body.resolutions
    {
        return Err("Merge membership transition differs from its exact entry".to_string());
    }
    if let super::membership::MembershipChange::RemoveMember {
        user_pubkey,
        removes,
        retirement_device_state,
        ..
    } = &opened_entry.value.change
    {
        let removes_exact_member = removes == &predecessor_membership.active_grant_ids(user_pubkey);
        let retires_owner = removes.iter().any(|grant| {
            predecessor_membership
                .active_grant(grant)
                .is_some_and(|record| {
                    matches!(
                        record.role,
                        super::membership::StoreMembershipRoleGrant::Owner { .. }
                    )
                })
        });
        if !removes_exact_member
            || !retires_owner
            || retirement_device_state.as_ref() != Some(&commit.device_state)
            || !commit.stream_activations().is_empty()
        {
            return Err(
                "Merge Owner-removal control differs from its exact membership entry".to_string(),
            );
        }
        let mut successor_membership = predecessor_membership.clone();
        successor_membership
            .add_entry(opened_entry.value)
            .map_err(|error| error.to_string())?;
        return VerifiedCircleActivations::membership_control(commit, commit_ref)
            .map(|activations| (activations, None))
            .map_err(|error| error.to_string());
    }
    if let super::membership::MembershipChange::ResolutionActivation { resolution } =
        &opened_entry.value.change
    {
        let resolution = resolution.clone();
        let resolution_proof = pending_resolution
            .filter(|proof| proof.verifies(&resolution))
            .ok_or_else(|| {
                "Merge conflict resolution lacks its verified Store activation".to_string()
            })?
            .clone();
        let opened_resolution = super::store_objects::load_membership_resolution_ref(
            storage,
            root.store_root_hash,
            &resolution,
        )
        .await
        .map_err(|error| error.to_string())?;
        let acceptance = &opened_resolution.value.replacement_acceptance;
        let mut expected = vec![
            super::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                opened_resolution.value.replacement_grant.clone(),
                acceptance.membership.clone(),
            ),
            super::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                opened_resolution.value.replacement_grant.clone(),
                acceptance.recovery.clone(),
            ),
        ];
        expected.sort();
        if transition.body.predecessor.is_some()
            || transition
                .body
                .resolutions
                .binary_search(&resolution)
                .is_err()
            || commit.stream_activations() != expected
        {
            return Err(
                "Merge conflict-resolution control differs from its exact membership entry"
                    .to_string(),
            );
        }
        let mut successor_membership = predecessor_membership.clone();
        successor_membership
            .add_entry(opened_entry.value)
            .map_err(|error| error.to_string())?;
        return VerifiedCircleActivations::membership_control(commit, commit_ref)
            .map(|activations| (activations, Some(resolution_proof)))
            .map_err(|error| error.to_string());
    }
    let super::membership::MembershipChange::SetMember {
        user_pubkey,
        role:
            super::membership::StoreMembershipRoleGrant::Owner {
                recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
            },
        grant_id,
        membership: Some(membership_anchor),
        replaces,
        retirement_device_state,
        ..
    } = &opened_entry.value.change
    else {
        return Err("Merge membership control does not activate one Owner promotion".to_string());
    };
    if retirement_device_state.is_some()
        || user_pubkey != &acceptance.request.member_pubkey
        || grant_id != &acceptance.request.intended_owner_grant
        || replaces != &BTreeSet::from([acceptance.request.member_grant.clone()])
        || acceptance.request.promoter_registration != commit.author_registration
    {
        return Err(
            "Merge Owner-promotion control differs from its exact membership entry".to_string(),
        );
    }
    super::owner_promotion::verify_merge_owner_promotion_acceptance_with_history(
        storage,
        root,
        acceptance,
        verified_commits,
    )
    .await
    .map_err(|error| error.to_string())?;
    let request_activation = acceptance.activation.commit();
    let request_commit = verified_commits.get(request_activation).ok_or_else(|| {
        "Merge Owner-promotion request activation is absent from its verified history".to_string()
    })?;
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(&request_commit.commit),
    )
    .map_err(|error| error.to_string())?;
    let request_membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &acceptance.request.predecessor_membership,
        &verified_membership_activations,
        None,
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => error.to_string(),
        RegistrationLoadError::Invalid(error) => error,
    })?;
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| error.to_string())?;
    let predecessor_frontier = predecessor_cut.commits();
    let request_stream = request_activation.coord.stream_id;
    let activation_is_covered = predecessor_frontier
        .get(&request_stream)
        .is_some_and(|head| head.coord.sequence() >= request_activation.coord.sequence());
    let promoter_is_active = device_state_has_active_registration(
        predecessor_state,
        &acceptance.request.promoter_registration,
    );
    let candidate_is_active = device_state_has_active_registration(
        predecessor_state,
        &acceptance.request.member_registration,
    );
    let promoter_grant_is_active = predecessor_membership
        .active_owner_grant(&commit_author.value.author_pubkey)
        .as_ref()
        == Some(&acceptance.request.promoter_owner_grant);
    let candidate_grant_is_active = predecessor_membership
        .active_grant(&acceptance.request.member_grant)
        .is_some_and(|record| {
            record.member_pubkey == acceptance.request.member_pubkey
                && record.role == super::membership::StoreMembershipRoleGrant::Member
        });
    if !predecessor_membership.causally_includes(&request_membership)
        || !activation_is_covered
        || !promoter_is_active
        || !candidate_is_active
        || !promoter_grant_is_active
        || !candidate_grant_is_active
    {
        return Err(
            "Merge Owner-promotion transition does not include its accepted authority".to_string(),
        );
    }
    let super::store_commit::OwnerPromotionAnchors {
        membership,
        recovery,
    } = &acceptance.anchors;
    if membership != membership_anchor {
        return Err("Merge Owner-promotion entry carries another membership anchor".to_string());
    }
    let mut expected = vec![
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            membership.clone(),
        ),
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            recovery.clone(),
        ),
    ];
    expected.sort();
    if commit.stream_activations() != expected {
        return Err(
            "Merge Owner-promotion control carries different stream activations".to_string(),
        );
    }
    VerifiedCircleActivations::membership_control(commit, commit_ref)
        .map(|activations| (activations, None))
        .map_err(|error| error.to_string())
}

pub(crate) async fn verify_merge_membership_head_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &super::membership::MembershipHeadRef,
    head: &super::membership::AuthorHead,
    activation: &StoreBatchCommitRef,
) -> Result<bool, String> {
    let (commit, author) = Box::pin(load_commit_with_author(storage, root, activation))
        .await
        .map_err(|error| error.to_string())?;
    let transition = commit
        .control()
        .map(|control| &control.transition)
        .ok_or_else(|| {
            "membership head activation commit has no Merge membership transition".to_string()
        })?;
    if !transition.matches_head(head, reference)
        || transition.body.author_registration != commit.author_registration
    {
        return Err(
            "membership head differs from its exact activating Store transition".to_string(),
        );
    }
    let activation_observation = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &commit.author_registration,
        &author,
        Some(activation),
    )
    .await;
    match activation_observation {
        Ok((_, Some(_))) => {}
        Ok((_, None)) => return Ok(false),
        Err(super::store_outbound::StoreOutboundError::MergeAnnouncementOccupied { .. })
        | Err(super::store_outbound::StoreOutboundError::Object(
            super::store_objects::StoreObjectError::Storage(StorageError::NotFound(_)),
        )) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    }
    let (_, _, replayed, verified_control) =
        Box::pin(replay_merge_device_history(storage, root, activation))
            .await
            .map_err(|error| error.to_string())?;
    if replayed != commit {
        return Err("membership head activation replay changed its Store commit".to_string());
    }
    if verified_control.is_none() {
        return Err(
            "membership head activation replay did not verify its Merge membership control"
                .to_string(),
        );
    }
    Ok(true)
}

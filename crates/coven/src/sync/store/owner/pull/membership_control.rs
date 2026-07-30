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

pub(crate) async fn verify_merge_membership_control_with_history(
    commit_verifier: &mut StoreCommitVerifier<'_>,
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
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    let Some(super::store_commit::StoreControl { transition }) = commit.control() else {
        return Err("Merge membership verifier received another Store control".to_string());
    };
    let state = &commit.membership_state;
    let commit_author = commit_verifier
        .load_registration(&commit.author_registration)
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
    let opened_entry = crate::storage::load_membership_entry_ref(
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
        let opened_resolution = crate::storage::load_membership_resolution_ref(
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
        commit_verifier,
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
        commit_predecessor_references(request_commit.verified.value()),
    )
    .map_err(|error| error.to_string())?;
    let request_membership = commit_verifier
        .load_membership_at_verified_prefix(
            &acceptance.request.predecessor_membership.heads,
            &acceptance.request.predecessor_membership.resolutions,
            &verified_membership_activations,
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
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
    history_verifier: &mut MergeHistoryVerifier<'_>,
    reference: &super::membership::MembershipHeadRef,
    head: &super::membership::AuthorHead,
    activation: &StoreBatchCommitRef,
) -> Result<bool, String> {
    let verified = history_verifier
        .load_ref(activation)
        .await
        .map_err(|error| error.to_string())?;
    let commit = verified.value();
    let author = verified.author();
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
    let activation_observation = history_verifier
        .exact_next_announcement_slot(&commit.author_registration, author, Some(&verified))
        .await;
    match activation_observation {
        Ok((_, Some(_))) => {}
        Ok((_, None)) => return Ok(false),
        Err(StoreError::MergeAnnouncementOccupied { .. })
        | Err(StoreError::Object(crate::storage::StoreObjectError::Storage(
            StorageError::NotFound(_),
        ))) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    }
    history_verifier
        .verify_refs([activation.clone()])
        .await
        .map_err(|error| error.to_string())?;
    let verified_control = history_verifier
        .history()
        .commits
        .get(activation)
        .and_then(|commit| commit.membership_control.as_ref());
    if !verified_control
        .is_some_and(|control| control.verifies_head_activation(reference, head, activation))
    {
        return Err(
            "membership head activation differs from its verified Merge membership control"
                .to_string(),
        );
    }
    Ok(true)
}

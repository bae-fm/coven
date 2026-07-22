use super::*;

pub(in crate::sync::store_engine) async fn find_request_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<super::store_commit::OwnerPromotionRequestActivation, StorePullError> {
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let discovered = discover_merge_stream(
        storage,
        root,
        &request.promoter_registration,
        &promoter.value,
        None,
    )
    .await?;
    let mut matches =
        discovered
            .commits
            .into_iter()
            .filter_map(|(head_ref, _, commit_ref, commit)| {
                (commit.owner_promotion_request() == Some(request))
                    .then_some((commit_ref, head_ref))
            });
    let Some((commit, head)) = matches.next() else {
        return Err(StorePullError::Database(
            "Owner-promotion request has no accepted Merge activation".to_string(),
        ));
    };
    if matches.next().is_some() {
        return Err(StorePullError::Database(
            "Owner-promotion request has more than one Merge activation".to_string(),
        ));
    }
    Ok(super::store_commit::OwnerPromotionRequestActivation { commit, head })
}

pub(in crate::sync::store_engine) async fn verify_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<(), StorePullError> {
    let super::store_commit::OwnerPromotionRequestActivation {
        commit: activation_commit,
        ..
    } = &acceptance.activation;
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
    let super::store_commit::OwnerPromotionRequestActivation {
        commit: activation_commit,
        head: activation_head,
    } = &acceptance.activation;
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

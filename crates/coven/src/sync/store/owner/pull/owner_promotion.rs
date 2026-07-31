use super::*;

pub(crate) async fn verify_merge_owner_promotion_acceptance_with_history(
    root: &StoreRootRef,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<(), StorePullError> {
    let request = &acceptance.request;
    let super::store_commit::OwnerPromotionRequestActivation {
        commit: activation_commit,
        head: activation_head,
    } = &acceptance.activation;
    let promoter = commit_verifier
        .load_registration(&request.promoter_registration)
        .await?;
    let candidate = commit_verifier
        .load_registration(&request.member_registration)
        .await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    acceptance
        .verify(&candidate.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;

    let opened = commit_verifier
        .load_head(activation_head, &promoter.value, activation_commit)
        .await?;
    let verified = verified_commits.get(activation_commit).ok_or_else(|| {
        StorePullError::Database(
            "Owner-promotion request activation is absent from its verified history".to_string(),
        )
    })?;
    let (_, exact_head) = commit_verifier
        .exact_next_announcement_slot(
            &request.promoter_registration,
            &promoter.value,
            Some(&verified.verified),
        )
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if opened.value.head_hash() != activation_head.head_hash
        || opened.value.commit != *activation_commit
        || exact_head.as_ref() != Some(activation_head)
    {
        return Err(StorePullError::Database(
            "Owner-promotion request is not activated by its exact Merge head".to_string(),
        ));
    }
    let verified_commit = verified.verified.value();
    if verified_commit.owner_promotion_request() != Some(request)
        || verified_commit.membership_state != request.predecessor_membership
        || verified_commit.device_state != request.predecessor_devices
        || verified_commit.author_registration != request.promoter_registration
    {
        return Err(StorePullError::Database(
            "Owner-promotion request commit differs from its signed predecessor authority"
                .to_string(),
        ));
    }
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(verified_commit),
    )?;
    let membership = load_merge_predecessor_membership_with_verified_activations(
        commit_verifier,
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

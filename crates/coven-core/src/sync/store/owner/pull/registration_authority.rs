use super::*;

pub(crate) async fn load_merge_predecessor_membership_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    state: &StoreMembershipStateRef,
) -> Result<MembershipChain, RegistrationLoadError> {
    Box::pin(history_verifier.load_membership_at_exact_heads(&state.heads, &state.resolutions))
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) async fn load_merge_predecessor_membership_with_verified_activations(
    commit_verifier: &StoreCommitVerifier<'_>,
    state: &StoreMembershipStateRef,
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    Box::pin(commit_verifier.load_membership_at_verified_prefix(
        &state.heads,
        &state.resolutions,
        verified_activations,
        pending_resolution,
    ))
    .await
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) fn verify_merge_membership_state_ref(
    state: &StoreMembershipStateRef,
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::Database(
            "Store history membership state is conflicted".to_string(),
        ));
    };
    let expected = StoreMembershipStateRef::from_parts(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        device_state.recovery.clone(),
        resolved.state_hash,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != state {
        return Err(StorePullError::Database(
            "Store history membership reference differs from its exact resolved state".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_merge_owner(
    membership: &StoreMembershipStateRef,
    chain: &MembershipChain,
    owner_pubkey: &str,
    owner_grant: &super::membership::MembershipGrantId,
) -> bool {
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        return false;
    };
    StoreMembershipStateRef::from_parts(
        chain.head_refs().to_vec(),
        chain.resolution_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && chain.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

pub(crate) fn verify_merge_provider_administrator(
    chain: &MembershipChain,
    grant_id: &crate::sync::provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &crate::sync::provider::ProviderAdminGrantRecord,
) -> bool {
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        return false;
    };
    let state = resolved.provider_admin.combined_state();
    state.authorizes(grant_id, executor) && state.records().get(grant_id) == Some(expected)
}

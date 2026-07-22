use super::*;

pub(crate) async fn load_merge_predecessor_membership(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<MembershipChain, RegistrationLoadError> {
    load_merge_predecessor_membership_impl(storage, root, state, None, None).await
}

pub(crate) async fn load_merge_predecessor_membership_with_verified_activations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    load_merge_predecessor_membership_impl(
        storage,
        root,
        state,
        Some(verified_activations),
        pending_resolution,
    )
    .await
}

async fn load_merge_predecessor_membership_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
    verified_activations: Option<&VerifiedMergeMembershipPrefix>,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    let root_value = load_store_protocol_root(storage, root)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    let membership = match verified_activations {
        Some(verified_activations) => Box::pin(
            super::membership_ops::load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
                storage,
                root,
                &root_value,
                &root_value.descriptor.founder_pubkey,
                &state.heads,
                &state.resolutions,
                verified_activations,
                pending_resolution,
            ),
        )
        .await,
        None => Box::pin(
            super::membership_ops::load_anchored_chain_at_exact_heads_with_root(
                storage,
                root,
                &root_value,
                &root_value.descriptor.founder_pubkey,
                &state.heads,
                &state.resolutions,
            ),
        )
        .await,
    }
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    Ok(membership)
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

pub(in crate::sync::store_engine) async fn load_device_join_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<MembershipChain, StorePullError> {
    load_merge_predecessor_membership(storage, root, state)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })
}

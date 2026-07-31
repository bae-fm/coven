use super::*;

pub(crate) async fn load_local_commit_device_operations(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    verified_commit: &VerifiedStoreBatchCommit,
    membership: &MembershipChain,
    state_ref: &StoreDeviceStateRef,
    state: ResolvedStoreDeviceState,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    let root = commit_verifier.root();
    if verified_commit.store_root_hash() != root.store_root_hash {
        return Err(StorePullError::Database(
            "local device-operation commit belongs to another Store root".to_string(),
        ));
    }
    let commit = verified_commit.value();
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Database(error.to_string()));
    }
    if state_ref != &commit.device_state {
        return Err(StorePullError::Database(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    verify_merge_membership_state_ref(&commit.membership_state, membership, &state)?;
    let resolver = DeviceStateResolver::Database(database);
    Box::pin(load_commit_device_operations(
        Some(&resolver),
        commit_verifier,
        commit,
        &state,
        Some(membership),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })
}

pub(crate) async fn derive_local_post_device_state(
    commit_verifier: &StoreCommitVerifier<'_>,
    commit: &StoreBatchCommit,
    predecessor_state: ResolvedStoreDeviceState,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    device_operations: VerifiedStoreDeviceOperations,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let (authorized_predecessor, recovery_author) =
        predecessor_with_recovery_author(predecessor_state, commit, registrations)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery = commit_verifier
        .verify_owner_recovery_activation(commit)
        .await?;
    device_operations
        .apply_to(authorized_predecessor, &commit.device_state)
        .and_then(|state| {
            apply_verified_device_lifecycle(
                state,
                commit,
                registrations,
                recovery_author.as_ref(),
                owner_recovery,
            )
        })
        .map_err(|error| StorePullError::Database(error.to_string()))
}

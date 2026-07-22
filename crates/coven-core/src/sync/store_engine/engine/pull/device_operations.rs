use super::*;

pub(crate) async fn load_local_commit_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    membership: &MembershipChain,
    state_ref: &StoreDeviceStateRef,
    state: ResolvedStoreDeviceState,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
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
    let authority = RegistrationPredecessorAuthority(membership);
    let resolver = DeviceStateResolver::Database(db);
    Box::pin(load_commit_device_operations(
        Some(&resolver),
        storage,
        root,
        commit,
        &state,
        Some(&authority),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })
}

pub(crate) async fn derive_local_post_device_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    predecessor_state: ResolvedStoreDeviceState,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    device_operations: VerifiedStoreDeviceOperations,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let (authorized_predecessor, recovery_author) =
        predecessor_with_recovery_author(predecessor_state, commit, registrations)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery = Box::pin(verify_commit_owner_recovery_activation(
        storage, root, commit,
    ))
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

use super::*;

pub(super) async fn authorize_store(store: &Store) -> Result<AuthorizedStore<'_>, DeviceJoinError> {
    store
        .authorize()
        .await
        .map_err(|error| DeviceJoinError::Store(error.to_string()))
}

pub(super) fn exact_slot_storage(store: &Store) -> &dyn ExactSlotStorage {
    store.storage().exact_slot_storage()
}

pub(super) fn resolved_provider_admin(
    membership: &MembershipChain,
    grant_id: &ProviderAdminGrantId,
) -> Result<ProviderAdminGrantRecord, DeviceJoinError> {
    let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(DeviceJoinError::MembershipConflict);
    };
    let state = resolved.provider_admin.combined_state();
    state
        .records()
        .get(grant_id)
        .filter(|record| state.authorizes(grant_id, &record.administrator))
        .cloned()
        .ok_or(DeviceJoinError::ProviderAdministratorRequired)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn load_current_device_join_authorization(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
) -> Result<MembershipChain, DeviceJoinError> {
    let membership = crate::sync::store::pull::load_cycle_membership(storage, database)
        .await
        .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
    Ok(membership)
}

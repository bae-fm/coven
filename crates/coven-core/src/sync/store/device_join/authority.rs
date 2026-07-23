use super::journal::database_error;
use super::*;

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

pub async fn load_current_device_join_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<MembershipChain, DeviceJoinError> {
    let membership = crate::sync::store::pull::load_cycle_membership(storage, db)
        .await
        .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
    membership.chain.ok_or(DeviceJoinError::MembershipConflict)
}

pub(super) async fn load_local_store_root(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>, DeviceJoinError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    crate::sync::store_objects::load_store_protocol_root(storage, &root)
        .await
        .map_err(DeviceJoinError::from)
}

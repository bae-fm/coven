use super::*;
use crate::sync::circle_control::StoreMembershipStateRef;

pub(crate) fn verify_serial_provider_administrator(
    authorization: &SerialAuthorizationState,
    grant_id: &super::provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &super::provider::ProviderAdminGrantRecord,
) -> bool {
    authorization.provider_admin.authorizes(grant_id, executor)
        && authorization.provider_admin.records().get(grant_id) == Some(expected)
}

pub(crate) struct SerialRegistrationAuthority {
    pub(crate) position: super::store_commit::SerialStorePosition,
    pub(crate) authorization: SerialAuthorizationState,
    pub(crate) accepted_prefix: Vec<AuthorizedSerialCommit>,
}

pub(crate) async fn load_serial_registration_authority(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<SerialRegistrationAuthority, StorePullError> {
    let StoreMembershipStateRef::Serial(state_ref) = state else {
        return Err(StorePullError::Serial(
            "Serial device join carries Merge membership authority".to_string(),
        ));
    };
    let reference = match &state_ref.position {
        super::store_commit::SerialStorePosition::Genesis { .. } => None,
        super::store_commit::SerialStorePosition::Commit(reference) => Some(reference.clone()),
    };
    let (accepted_prefix, authorization, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, reference)).await?;
    let expected = StoreMembershipStateRef::serial(
        state_ref.position.clone(),
        state_ref.recovery.clone(),
        &authorization,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    if &expected != state {
        return Err(StorePullError::Serial(
            "Serial device join membership state differs from its exact authorization".to_string(),
        ));
    }
    Ok(SerialRegistrationAuthority {
        position: state_ref.position.clone(),
        authorization,
        accepted_prefix,
    })
}

pub(in crate::sync::store_engine) async fn load_device_join_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<
    (
        super::store_commit::SerialStorePosition,
        SerialAuthorizationState,
    ),
    StorePullError,
> {
    let authority = load_serial_registration_authority(storage, root, state).await?;
    Ok((authority.position, authority.authorization))
}

use super::*;
use crate::sync::circle_control::StoreMembershipStateRef;

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
    let StoreMembershipStateRef::Serial(state_ref) = state else {
        return Err(StorePullError::Serial(
            "Serial device join carries Merge membership authority".to_string(),
        ));
    };
    let reference = match &state_ref.position {
        super::store_commit::SerialStorePosition::Genesis { .. } => None,
        super::store_commit::SerialStorePosition::Commit(reference) => Some(reference.clone()),
    };
    let authorization = Box::pin(load_serial_authorization_at_position(
        storage, root, reference,
    ))
    .await?;
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
    Ok((state_ref.position.clone(), authorization))
}

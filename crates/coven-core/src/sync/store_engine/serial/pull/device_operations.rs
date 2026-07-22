use super::*;
use crate::sync::store_commit::StoreBatchCommit;

pub(crate) async fn load_local_commit_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Serial(error.to_string()));
    }
    if commit.policy() != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(
            "Serial device-operation validation received a Merge commit".to_string(),
        ));
    }
    let (state_ref, state) = db.store_device_state_for_order(&commit.order).await?;
    if state_ref != commit.device_state {
        return Err(StorePullError::Serial(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    let (position, authorization) =
        load_device_join_authorization(storage, root, &commit.membership_state).await?;
    let authority = RegistrationPredecessorAuthority::Serial {
        authorization: &authorization,
        position,
        history: SerialAuthorizationHistory::ExactPredecessor,
    };
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
        RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
    })
}

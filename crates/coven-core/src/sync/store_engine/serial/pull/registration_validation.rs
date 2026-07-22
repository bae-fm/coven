use super::*;
use crate::sync::store_commit::{StoreBatchCommit, StoreDeviceRegistrationActivation};

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_serial_commit_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    authorization: &SerialAuthorizationState,
    position: super::store_commit::SerialStorePosition,
    history: SerialAuthorizationHistory<'_>,
    accepted: &[AuthorizedSerialCommit],
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    let loaded =
        load_commit_join_evidence(storage, root, root_value, commit, activating_author).await?;
    let join_evidence =
        super::device_join_attempt::verify_commit_join_evidence(commit, loaded, accepted)
            .await
            .map_err(registration_attempt_error)?;
    let predecessor = RegistrationPredecessorAuthority::Serial {
        authorization,
        position,
        history,
    };
    load_commit_registrations_with_root(
        storage,
        root,
        root_value,
        commit,
        activating_author,
        Some(&predecessor),
        &join_evidence,
    )
    .await
}

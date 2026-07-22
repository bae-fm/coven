use super::*;

pub(super) async fn load_merge_commit_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor_membership: &MembershipChain,
    accepted: Option<super::device_join_attempt::MergeAcceptedJoinHistory<'_>>,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    let loaded =
        load_commit_join_evidence(storage, root, root_value, commit, activating_author).await?;
    let join_evidence =
        super::device_join_attempt::verify_commit_join_evidence(commit, loaded, accepted)
            .await
            .map_err(registration_attempt_error)?;
    let predecessor = RegistrationPredecessorAuthority(predecessor_membership);
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

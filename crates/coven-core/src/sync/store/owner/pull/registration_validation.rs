use super::*;

pub(super) async fn load_merge_commit_registrations(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor_membership: &MembershipChain,
    accepted: super::device_join_attempt::VerifiedMergePredecessorHistory<'_>,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    let loaded = load_commit_join_evidence(history_verifier, commit, activating_author).await?;
    let join_evidence =
        super::device_join_attempt::verify_commit_join_evidence(commit, loaded, accepted)
            .await
            .map_err(registration_attempt_error)?;
    load_commit_registrations(
        history_verifier,
        commit,
        activating_author,
        Some(predecessor_membership),
        &join_evidence,
        accepted,
    )
    .await
}

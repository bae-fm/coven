use super::*;

pub(crate) fn verify_device_join_activation_commit(
    commit: &StoreBatchCommit,
    attempt_id: super::store_commit::DeviceJoinAttemptId,
) -> Result<(), StorePullError> {
    let same_principal_attempt = [super::store_commit::DeviceJoinAttemptDecisionRef::Attempt(
        attempt_id,
    )];
    let attempt_shape = commit.device_join_attempt_decisions().is_empty()
        || commit.device_join_attempt_decisions() == same_principal_attempt;
    let activates_this_attempt = commit.device_registrations().iter().any(|registration| {
        matches!(
            &registration.authority,
            super::store_commit::StoreDeviceRegistrationActivationRef::Join { attempt_id: named }
                if *named == attempt_id
        )
    });
    if !activates_this_attempt
        || !attempt_shape
        || commit.device_registrations().len() != 1
        || !commit.provider_access_grants().is_empty()
        || !commit.circle_controls().is_empty()
        || !commit.circle_packages().is_empty()
        || commit.store_package().is_some()
        || commit.reclaim_authorization().is_some()
        || commit.reclaim_receipt().is_some()
        || commit.control().is_some()
    {
        return Err(StorePullError::InvalidState(
            "device join activation commit carries unrelated operations".to_string(),
        ));
    }
    Ok(())
}

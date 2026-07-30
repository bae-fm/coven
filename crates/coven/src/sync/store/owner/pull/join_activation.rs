use super::*;

pub(crate) fn verify_device_join_activation_commit(
    commit: &StoreBatchCommit,
    expected_outcome: &super::store_commit::DeviceJoinOutcomeRef,
) -> Result<(), StorePullError> {
    if commit.device_join_outcomes() != std::slice::from_ref(expected_outcome)
        || !commit.device_join_attempt_decisions().is_empty()
        || !commit.device_join_cleanup_receipts().is_empty()
        || commit.device_registrations().len() != 1
        || !commit.provider_access_grants().is_empty()
        || !commit.circle_controls().is_empty()
        || !commit.circle_packages().is_empty()
        || commit.store_package().is_some()
        || commit.reclaim_authorization().is_some()
        || commit.reclaim_receipt().is_some()
        || commit.control().is_some()
    {
        return Err(StorePullError::Database(
            "device join activation commit carries unrelated operations".to_string(),
        ));
    }
    Ok(())
}

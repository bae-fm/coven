use super::registration_authority::verify_merge_provider_administrator;
use super::*;
use crate::sync::store::pull::LoadedDeviceJoinCleanupActivation;

pub(in crate::sync::store) fn verify_device_join_cleanup_activation<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    activation: LoadedDeviceJoinCleanupActivation,
) -> StorePullFuture<'a, crate::sync::store::JoinerJoinTerminal> {
    Box::pin(async move {
        let membership = load_merge_predecessor_membership(
            storage,
            root,
            &activation.verified_commit.value().membership_state,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        if !membership.is_owner_now(&activation.verified_commit.author().author_pubkey) {
            return Err(StorePullError::Database(
                "device join cleanup activation author is not an active Merge Owner".to_string(),
            ));
        }
        let [loaded] = <[_; 1]>::try_from(activation.receipts).map_err(|_| {
            StorePullError::Database(
                "device join cleanup activation does not resolve to one verified receipt"
                    .to_string(),
            )
        })?;
        let attempt = super::device_join_attempt::verify_device_join_attempt_evidence(
            storage,
            root,
            loaded.attempt,
        )
        .await?;
        let expected = &attempt.value.provider_approval.request.offer.provider_admin;
        if !verify_merge_provider_administrator(
            &membership,
            &loaded.receipt.provider_admin_grant,
            &loaded.receipt.executor,
            expected,
        ) {
            return Err(StorePullError::Database(
                "device join cleanup executor is not the effective Merge provider administrator"
                    .to_string(),
            ));
        }
        Ok(loaded.receipt.joiner_terminal)
    })
}

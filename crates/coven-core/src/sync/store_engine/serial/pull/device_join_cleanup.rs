use super::registration_authority::verify_serial_provider_administrator;
use super::*;
use crate::sync::store_pull::LoadedDeviceJoinCleanupActivation;

pub(in crate::sync::store_engine) fn verify_device_join_cleanup_activation<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    activation: LoadedDeviceJoinCleanupActivation,
) -> StorePullFuture<'a, crate::sync::device_join::JoinerJoinTerminal> {
    Box::pin(async move {
        let (_, authorization) =
            load_device_join_authorization(storage, root, &activation.commit.membership_state)
                .await?;
        if !authorization
            .membership
            .is_owner(&activation.author.author_pubkey)
        {
            return Err(StorePullError::Serial(
                "device join cleanup activation author is not an active Serial Owner".to_string(),
            ));
        }
        let [loaded] = <[_; 1]>::try_from(activation.receipts).map_err(|_| {
            StorePullError::Serial(
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
        if !verify_serial_provider_administrator(
            &authorization,
            &loaded.receipt.provider_admin_grant,
            &loaded.receipt.executor,
            expected,
        ) {
            return Err(StorePullError::Serial(
                "device join cleanup executor is not the effective Serial provider administrator"
                    .to_string(),
            ));
        }
        Ok(loaded.receipt.joiner_terminal)
    })
}

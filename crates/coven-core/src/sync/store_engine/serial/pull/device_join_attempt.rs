use super::registration_authority::verify_serial_provider_administrator;
use super::*;
use crate::sync::store_commit::{DeviceJoinAttempt, StoreHistoryCut};
use crate::sync::store_objects::VerifiedObject;
use crate::sync::store_pull::LoadedDeviceJoinAttemptEvidence;

pub(in crate::sync::store_engine) fn verify_device_join_attempt_evidence<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    evidence: LoadedDeviceJoinAttemptEvidence,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let StoreHistoryCut::Serial(cut) = &evidence.attempt.value.bootstrap_cut else {
            return Err(StorePullError::Serial(
                "Serial device join attempt carries a Merge bootstrap cut".to_string(),
            ));
        };
        let (position, _) =
            load_device_join_authorization(storage, root, &evidence.attempt.value.membership)
                .await?;
        if &position != cut {
            return Err(StorePullError::Serial(
                "device join attempt cut differs from its exact Serial authorization position"
                    .to_string(),
            ));
        }
        let tip = match cut {
            StoreSerialPredecessor::Genesis { .. } => None,
            StoreSerialPredecessor::Commit(reference) => Some(reference.clone()),
        };
        let (accepted, _, _) = load_authorized_serial_prefix(storage, root, tip).await?;
        let access = &evidence.attempt.value.provider_approval.access_grant;
        let verified = accepted
            .iter()
            .find(|commit| commit.commit_ref == access.activation)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "provider-access activation is outside the verified Serial bootstrap history"
                        .to_string(),
                )
            })?;
        if verified.commit != evidence.provider_access_activation
            || verified.author != evidence.provider_administrator
            || !verify_serial_provider_administrator(
                &verified.authorization_before,
                &access.grant.administrator_grant,
                &verified.commit.author_registration,
                &evidence
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider_admin,
            )
        {
            return Err(StorePullError::Serial(
                "device join attempt lacks exact Serial provider-administrator authority"
                    .to_string(),
            ));
        }
        Ok(evidence.attempt)
    })
}

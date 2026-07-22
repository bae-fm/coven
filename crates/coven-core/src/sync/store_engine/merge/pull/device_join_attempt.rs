use super::registration_authority::verify_merge_provider_administrator;
use super::snapshot_authority::verify_merge_history_authority;
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
        let StoreHistoryCut::MergeConcurrent(frontier) = &evidence.attempt.value.bootstrap_cut
        else {
            return Err(StorePullError::Database(
                "Merge device join attempt carries a Serial bootstrap cut".to_string(),
            ));
        };
        if !matches!(
            evidence.attempt.value.membership,
            StoreMembershipStateRef::MergeConcurrent(_)
        ) {
            return Err(StorePullError::Database(
                "Merge device join attempt carries Serial membership authority".to_string(),
            ));
        }
        let authority = verify_merge_history_authority(
            storage,
            root,
            frontier,
            &evidence.attempt.value.membership,
        )
        .await?;
        let access = &evidence.attempt.value.provider_approval.access_grant;
        let verified = authority
            .history
            .commits
            .get(&access.activation)
            .ok_or_else(|| {
                StorePullError::Database(
                    "provider-access activation is outside the verified Merge bootstrap history"
                        .to_string(),
                )
            })?;
        if verified.commit != evidence.provider_access_activation
            || !verify_merge_provider_administrator(
                &verified.predecessor_membership,
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
            return Err(StorePullError::Database(
                "device join attempt lacks exact Merge provider-administrator authority"
                    .to_string(),
            ));
        }
        Ok(evidence.attempt)
    })
}

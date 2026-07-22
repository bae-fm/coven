use super::registration_authority::verify_merge_provider_administrator;
use super::snapshot_authority::verify_merge_history_authority;
use super::*;
use crate::sync::store_commit::DeviceJoinAttempt;
use crate::sync::store_objects::VerifiedObject;
use crate::sync::store_pull::{
    LoadedCommitJoinEvidence, LoadedDeviceJoinAttemptEvidence, VerifiedCommitJoinEvidence,
};

pub(super) struct MergeAcceptedJoinHistory<'a> {
    pub(super) commits: &'a BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    pub(super) frontier: &'a [StoreBatchCommitRef],
}

pub(in crate::sync::store_engine) fn verify_device_join_attempt_evidence<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    evidence: LoadedDeviceJoinAttemptEvidence,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let frontier = &evidence.attempt.value.bootstrap_cut.0;
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

pub(super) fn verify_commit_join_evidence<'a>(
    commit: &'a StoreBatchCommit,
    loaded: LoadedCommitJoinEvidence,
    accepted: Option<MergeAcceptedJoinHistory<'a>>,
) -> StorePullFuture<'a, VerifiedCommitJoinEvidence> {
    Box::pin(async move {
        if loaded.attempts.is_empty() {
            return Ok(VerifiedCommitJoinEvidence {
                commit: commit.clone(),
                attempts: BTreeMap::new(),
                cleanup_receipts: loaded.cleanup_receipts,
            });
        }
        let accepted = accepted.ok_or_else(|| {
            StorePullError::Database(
                "Merge commit join evidence has no verified accepted predecessor history"
                    .to_string(),
            )
        })?;
        let mut attempts = BTreeMap::new();
        for (reference, evidence) in loaded.attempts {
            let access = &evidence.attempt.value.provider_approval.access_grant;
            let mut pending = accepted.frontier.to_vec();
            let mut visited = BTreeSet::new();
            let mut verified_access = None;
            while let Some(candidate) = pending.pop() {
                if !visited.insert(candidate.clone()) {
                    continue;
                }
                let verified = accepted.commits.get(&candidate).ok_or_else(|| {
                    StorePullError::Database(
                        "accepted Merge predecessor graph is missing an exact commit".to_string(),
                    )
                })?;
                if candidate == access.activation {
                    verified_access = Some(verified);
                    break;
                }
                pending.extend(commit_predecessor_references(&verified.commit));
            }
            let verified = verified_access.ok_or_else(|| {
                StorePullError::Database(
                    "provider-access activation is outside the accepted Merge predecessor graph"
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
            attempts.insert(reference, evidence.attempt.value);
        }
        Ok(VerifiedCommitJoinEvidence {
            commit: commit.clone(),
            attempts,
            cleanup_receipts: loaded.cleanup_receipts,
        })
    })
}

use super::registration_authority::verify_merge_provider_administrator;
use super::snapshot_authority::verify_merge_history_authority;
use super::*;
use crate::sync::store::pull::{
    LoadedCommitJoinEvidence, LoadedDeviceJoinAttemptEvidence, VerifiedCommitJoinEvidence,
};
use crate::sync::store_commit::DeviceJoinAttempt;
use crate::sync::store_objects::VerifiedObject;

#[derive(Clone, Copy)]
pub(super) struct VerifiedMergePredecessorHistory<'a> {
    pub(super) commits: &'a BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    pub(super) frontier: &'a [StoreBatchCommitRef],
}

impl<'a> VerifiedMergePredecessorHistory<'a> {
    pub(super) fn find(
        &self,
        mut matches: impl FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool,
    ) -> Result<Option<&'a VerifiedMergeHistoryCommit>, StorePullError> {
        let mut pending = self.frontier.to_vec();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let verified = self.commits.get(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "verified Merge predecessor graph is missing an exact commit".to_string(),
                )
            })?;
            if matches(&reference, verified.verified.value()) {
                return Ok(Some(verified));
            }
            pending.extend(commit_predecessor_references(verified.verified.value()));
        }
        Ok(None)
    }
}

pub(in crate::sync::store) async fn verify_device_join_attempt_evidence_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    evidence: LoadedDeviceJoinAttemptEvidence,
) -> Result<VerifiedObject<DeviceJoinAttempt>, StorePullError> {
    let frontier = &evidence.attempt.value.bootstrap_cut.0;
    verify_merge_history_authority(
        history_verifier,
        frontier,
        &evidence.attempt.value.membership,
    )
    .await?;
    let access = &evidence.attempt.value.provider_approval.access_grant;
    let verified = history_verifier
        .history()
        .commits
        .get(&access.activation)
        .ok_or_else(|| {
            StorePullError::Database(
                "provider-access activation is outside the verified Merge bootstrap history"
                    .to_string(),
            )
        })?;
    if !verify_merge_provider_administrator(
        &verified.predecessor_membership,
        &access.grant.administrator_grant,
        &verified.verified.value().author_registration,
        &evidence
            .attempt
            .value
            .provider_approval
            .request
            .offer
            .provider_admin,
    ) {
        return Err(StorePullError::Database(
            "device join attempt lacks exact Merge provider-administrator authority".to_string(),
        ));
    }
    Ok(evidence.attempt)
}

pub(super) fn verify_commit_join_evidence<'a>(
    commit: &'a StoreBatchCommit,
    loaded: LoadedCommitJoinEvidence,
    accepted: VerifiedMergePredecessorHistory<'a>,
) -> StorePullFuture<'a, VerifiedCommitJoinEvidence> {
    Box::pin(async move {
        if loaded.attempts.is_empty() {
            return Ok(VerifiedCommitJoinEvidence {
                commit: commit.clone(),
                attempts: BTreeMap::new(),
                cleanup_receipts: loaded.cleanup_receipts,
            });
        }
        let mut attempts = BTreeMap::new();
        for (reference, evidence) in loaded.attempts {
            let access = &evidence.attempt.value.provider_approval.access_grant;
            let verified = accepted
                .find(|candidate, _| candidate == &access.activation)?
                .ok_or_else(|| {
                    StorePullError::Database(
                        "provider-access activation is outside the accepted Merge predecessor graph"
                            .to_string(),
                    )
                })?;
            if !verify_merge_provider_administrator(
                &verified.predecessor_membership,
                &access.grant.administrator_grant,
                &verified.verified.value().author_registration,
                &evidence
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider_admin,
            ) {
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

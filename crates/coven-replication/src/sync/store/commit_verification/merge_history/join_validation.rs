use super::registration::*;
use super::*;

pub(crate) struct LoadedCommitJoinCleanupReceipt {
    pub(crate) receipt: super::device_join::DeviceJoinCleanupReceiptObject,
    pub(crate) attempt: LoadedDeviceJoinAttemptEvidence,
}

pub(crate) struct CommitJoinCleanupReceiptEvidence {
    pub(crate) receipt: super::device_join::DeviceJoinCleanupReceiptObject,
    pub(crate) attempt: super::store_commit::DeviceJoinAttemptRef,
}

pub(crate) struct LoadedCommitJoinEvidence {
    pub(crate) attempts:
        BTreeMap<super::store_commit::DeviceJoinAttemptRef, LoadedDeviceJoinAttemptEvidence>,
    pub(crate) cleanup_receipts: Vec<CommitJoinCleanupReceiptEvidence>,
}

pub(crate) struct VerifiedCommitJoinEvidence {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) attempts: BTreeMap<super::store_commit::DeviceJoinAttemptRef, DeviceJoinAttempt>,
    pub(crate) cleanup_receipts: Vec<CommitJoinCleanupReceiptEvidence>,
}

pub(crate) fn validate_commit_join_attempts(
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join attempt activation has no exact predecessor membership authority"
                .to_string(),
        )
    })?;
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join attempt activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    let bootstrap_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    for reference in commit
        .device_join_attempt_decisions()
        .iter()
        .filter_map(|decision| match decision {
            DeviceJoinAttemptDecisionRef::Attempt(reference) => Some(reference),
            DeviceJoinAttemptDecisionRef::Abandoned(_) => None,
        })
    {
        let attempt = join_evidence.attempts.get(reference).ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join activation has no verified exact attempt".to_string(),
            )
        })?;
        if attempt.owner_registration != commit.author_registration
            || attempt.membership != commit.membership_state
            || attempt.bootstrap_cut != bootstrap_cut
            || !predecessor_verifies_owner(
                predecessor,
                &attempt.membership,
                &activating_author.author_pubkey,
                &attempt.owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device join attempt differs from its exact activating predecessor authority"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

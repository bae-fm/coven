use super::*;

pub(super) fn validate_device_registration_refs(
    registrations: &[ActivatedStoreDeviceRegistrationRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for activation in registrations {
        if !seen.insert(activation.registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: activation.registration.device_id.to_string(),
                revision: 1,
            });
        }
    }
    Ok(())
}

pub(super) fn validate_device_exclusion_refs(
    proposals: &[StoreDeviceExclusionProposalRef],
    outcomes: &[StoreDeviceExclusionOutcomeRef],
) -> Result<(), StoreProtocolError> {
    if proposals.windows(2).any(|pair| pair[0] >= pair[1])
        || outcomes.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    let mut ids = BTreeSet::new();
    for proposal in proposals {
        proposal.validate_path()?;
        if !ids.insert(proposal.proposal_id) {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    for outcome in outcomes {
        let proposal = outcome.proposal();
        proposal.validate_path()?;
        let expected = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                proposal.target.device_id,
                proposal.proposal_id,
            )
        );
        if outcome.object().slot().logical_key() != expected || !ids.insert(proposal.proposal_id) {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    Ok(())
}

pub(super) fn validate_commit_acknowledgement(
    acknowledgement: &Option<StoreAckRef>,
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    let Some(acknowledgement) = acknowledgement else {
        return Ok(());
    };
    let expected = format!(
        "{}.json",
        ack_slot_prefix(&author.device_id.to_string(), acknowledgement.sequence)
    );
    if acknowledgement.registration != *author
        || acknowledgement.sequence < 2
        || acknowledgement.object.slot().logical_key() != expected
    {
        return Err(StoreProtocolError::Malformed(
            "Store commit acknowledgement is not the author's exact non-initial acknowledgement"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_device_join_attempt_decision_refs(
    decisions: &[DeviceJoinAttemptDecisionRef],
) -> Result<(), StoreProtocolError> {
    if decisions
        .windows(2)
        .any(|pair| pair[0].attempt_id() >= pair[1].attempt_id())
    {
        return Err(StoreProtocolError::JoinAttemptMismatch);
    }
    Ok(())
}

pub(super) fn validate_device_join_outcome_refs(
    outcomes: &[DeviceJoinOutcomeRef],
) -> Result<(), StoreProtocolError> {
    let mut attempts = BTreeSet::new();
    if outcomes.windows(2).any(|pair| pair[0] >= pair[1])
        || outcomes
            .iter()
            .any(|outcome| !attempts.insert(outcome.attempt().attempt_id))
    {
        return Err(StoreProtocolError::JoinOutcomeMismatch);
    }
    Ok(())
}

pub(super) fn validate_device_join_cleanup_receipt_refs(
    receipts: &[crate::sync::device_join::DeviceJoinCleanupReceiptRef],
) -> Result<(), StoreProtocolError> {
    if receipts.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::JoinOutcomeMismatch);
    }
    Ok(())
}

pub(super) fn validate_provider_access_refs(
    grants: &[crate::sync::provider::StoreMemberProviderAccessGrantRef],
    withdrawals: &[crate::sync::provider::StoreMemberProviderAccessWithdrawalReceiptRef],
) -> Result<(), StoreProtocolError> {
    if grants.windows(2).any(|pair| pair[0] >= pair[1])
        || withdrawals.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(StoreProtocolError::ProviderAccessMismatch);
    }
    let granted = grants
        .iter()
        .map(|reference| &reference.grant_id)
        .collect::<BTreeSet<_>>();
    if withdrawals
        .iter()
        .any(|reference| granted.contains(&reference.grant_id))
    {
        return Err(StoreProtocolError::ProviderAccessMismatch);
    }
    Ok(())
}

pub(super) fn validate_device_retirement_refs(
    retirements: &[StoreDeviceSelfRetirementRef],
    candidate_family: CandidateFamilyId,
    author: &StoreDeviceRegistrationRef,
    order: &StoreCommitOrder,
) -> Result<(), StoreProtocolError> {
    if retirements.len() > 1 {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    let expected_cut = match order {
        StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => {
            let mut cut = dependencies.clone();
            if let Some(predecessor) = predecessor {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                };
                if cut
                    .insert(stream_id, predecessor.clone())
                    .is_some_and(|existing| existing != *predecessor)
                {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
            }
            StoreHistoryCut::MergeConcurrent(cut)
        }
        StoreCommitOrder::Serial { predecessor, .. } => {
            StoreHistoryCut::Serial(predecessor.clone())
        }
    };
    for retirement in retirements {
        if retirement.candidate_family != candidate_family
            || retirement.target != *author
            || retirement.retiring_cut != expected_cut
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    Ok(())
}

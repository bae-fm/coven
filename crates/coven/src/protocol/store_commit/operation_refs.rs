use super::*;

pub(super) fn validate_device_registration_refs(
    registrations: &[ActivatedStoreDeviceRegistrationRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for activation in registrations {
        if !seen.insert(activation.registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: activation.registration.device_id.to_string(),
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

pub(super) fn validate_commit_circle_acknowledgements(
    circle_acknowledgements: &[CircleAckRef],
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for acknowledgement in circle_acknowledgements {
        let expected = format!(
            "{}.json",
            circle_ack_slot_prefix(
                acknowledgement.circle_id,
                &author.device_id.to_string(),
                acknowledgement.sequence,
            )
        );
        if acknowledgement.registration != *author
            || acknowledgement.sequence == 0
            || acknowledgement.object.slot().logical_key() != expected
        {
            return Err(StoreProtocolError::Malformed(
                "Store commit Circle acknowledgement is not the author's exact acknowledgement"
                    .to_string(),
            ));
        }
        if !seen.insert(acknowledgement.circle_id) {
            return Err(StoreProtocolError::Malformed(
                "Store commit carries two Circle acknowledgements for one Circle".to_string(),
            ));
        }
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
    receipts: &[crate::sync::store::DeviceJoinCleanupReceiptRef],
) -> Result<(), StoreProtocolError> {
    if receipts.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::JoinOutcomeMismatch);
    }
    Ok(())
}

pub(super) fn validate_provider_access_refs(
    grants: &[crate::protocol::provider::StoreMemberProviderAccessGrantRef],
) -> Result<(), StoreProtocolError> {
    if grants.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::ProviderAccessMismatch);
    }
    Ok(())
}

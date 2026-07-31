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

pub(crate) async fn load_commit_join_evidence(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
) -> Result<LoadedCommitJoinEvidence, RegistrationLoadError> {
    let loaded_cleanup = history_verifier
        .load_commit_join_cleanup_receipts(commit, activating_author)
        .await?;
    let mut attempts = BTreeMap::new();
    let mut cleanup_receipts = Vec::with_capacity(loaded_cleanup.len());
    for loaded in loaded_cleanup {
        let attempt = loaded.receipt.cancellation.attempt().clone();
        attempts.entry(attempt.clone()).or_insert(loaded.attempt);
        cleanup_receipts.push(CommitJoinCleanupReceiptEvidence {
            receipt: loaded.receipt,
            attempt,
        });
    }
    let references = commit
        .device_join_attempt_decisions()
        .iter()
        .filter_map(|decision| match decision {
            DeviceJoinAttemptDecisionRef::Attempt(reference) => Some(reference),
            DeviceJoinAttemptDecisionRef::Abandoned(_) => None,
        })
        .chain(
            commit
                .device_join_outcomes()
                .iter()
                .map(|outcome| outcome.attempt()),
        )
        .cloned()
        .collect::<BTreeSet<_>>();
    for reference in references {
        if attempts.contains_key(&reference) {
            continue;
        }
        let (attempt, owner) = history_verifier
            .load_device_join_attempt_and_owner(&reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let evidence = history_verifier
            .validate_device_join_attempt_evidence(attempt, &owner.value)
            .await
            .map_err(registration_attempt_error)?;
        attempts.insert(reference, evidence);
    }
    Ok(LoadedCommitJoinEvidence {
        attempts,
        cleanup_receipts,
    })
}

pub(crate) fn validate_commit_join_cleanup_receipts(
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join cleanup activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join cleanup activation author is not an active Owner".to_string(),
        ));
    }
    for loaded in &join_evidence.cleanup_receipts {
        if !accepted
            .contains_join_outcome(&loaded.receipt.cancellation)
            .map_err(registration_attempt_error)?
        {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup receipt outcome is absent from its verified predecessor history"
                    .to_string(),
            ));
        }
        let attempt = join_evidence.attempts.get(&loaded.attempt).ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join cleanup receipt has no verified exact attempt".to_string(),
            )
        })?;
        let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
        if !predecessor_verifies_provider_administrator(
            predecessor,
            &loaded.receipt.provider_admin_grant,
            &loaded.receipt.executor,
            expected_administrator,
        ) {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup executor is not the exact effective provider administrator"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) async fn validate_commit_join_outcomes(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
    accepted: VerifiedMergePredecessorHistory<'_>,
) -> Result<
    BTreeMap<super::store_commit::DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>,
    RegistrationLoadError,
> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join outcome activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join outcome activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    let mut verified = BTreeMap::new();
    for outcome_ref in commit.device_join_outcomes() {
        if !accepted
            .contains_join_attempt(outcome_ref.attempt())
            .map_err(registration_attempt_error)?
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome names an attempt absent from its predecessor history"
                    .to_string(),
            ));
        }
        let attempt = join_evidence
            .attempts
            .get(outcome_ref.attempt())
            .ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device join outcome has no verified exact attempt".to_string(),
                )
            })?;
        if attempt.owner_registration != commit.author_registration
            || outcome_ref.slot() != &attempt.outcome_slot
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome differs from its exact Owner attempt".to_string(),
            ));
        }
        let outcome = history_verifier
            .load_device_join_outcome(outcome_ref, activating_author)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if outcome.owner_registration != attempt.owner_registration
            || outcome.owner_grant != attempt.owner_grant
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome signer differs from its attempt".to_string(),
            ));
        }
        let activation = commit.device_registrations().iter().find(|activation| {
            matches!(
                &activation.authority,
                StoreDeviceRegistrationActivationRef::Join { outcome, .. }
                    if outcome == outcome_ref
            )
        });
        if matches!(&outcome.body, DeviceJoinOutcomeBody::Activated { .. }) != activation.is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome and registration activation are not one closed operation"
                    .to_string(),
            ));
        }
        if verified
            .insert(
                outcome_ref.clone(),
                VerifiedCommitJoinOutcome {
                    attempt: attempt.clone(),
                    owner: activating_author.clone(),
                    outcome,
                },
            )
            .is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome is duplicated in one commit".to_string(),
            ));
        }
    }
    Ok(verified)
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

pub(crate) async fn registration_activation(
    history_verifier: &MergeHistoryVerifier<'_>,
    activated: &ActivatedStoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    activating_author: &StoreDeviceRegistration,
    predecessor: &MembershipChain,
    verified_join_outcomes: &BTreeMap<
        super::store_commit::DeviceJoinOutcomeRef,
        VerifiedCommitJoinOutcome,
    >,
) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "registration activation commit author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    match (&registration.origin, &activated.authority) {
        (
            StoreDeviceRegistrationOrigin::Join {
                attempt_id: origin_attempt,
                outcome_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Join {
                attempt_id,
                outcome,
            },
        ) if origin_attempt == attempt_id && outcome_slot == outcome.slot() => {
            let verified = verified_join_outcomes.get(outcome).ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "registration activation has no verified join outcome".to_string(),
                )
            })?;
            let attempt = &verified.attempt;
            let owner = &verified.owner;
            if attempt.expected_registration != *registration
                || attempt.registration_slot != *activated.registration.object.slot()
                || !predecessor_verifies_owner(
                    predecessor,
                    &attempt.membership,
                    &owner.author_pubkey,
                    &attempt.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "activated registration differs from its exact join attempt".to_string(),
                ));
            }
            let outcome_value = &verified.outcome;
            if outcome_value.owner_registration != attempt.owner_registration
                || outcome_value.owner_grant != attempt.owner_grant
            {
                return Err(RegistrationLoadError::Invalid(
                    "join outcome signer differs from its exact attempt authority".to_string(),
                ));
            }
            let DeviceJoinOutcomeBody::Activated { readiness } = &outcome_value.body else {
                return Err(RegistrationLoadError::Invalid(
                    "cancelled device join outcome cannot activate a registration".to_string(),
                ));
            };
            let initial_ack = history_verifier
                .load_store_ack(&readiness.initial_ack, registration)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            readiness
                .verify(
                    outcome.attempt(),
                    attempt,
                    registration,
                    &readiness.initial_ack,
                    &initial_ack,
                )
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            Ok(StoreDeviceRegistrationActivation::Join {
                attempt_id: *attempt_id,
                outcome: outcome.clone(),
            })
        }
        (
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id: origin_recovery,
                recovery_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
        ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
            let node_value = history_verifier
                .load_owner_recovery_node(node)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let mut reached_ref = node.clone();
            let mut reached = node_value.clone();
            while let Some(predecessor_ref) = reached.predecessor.clone() {
                let predecessor = history_verifier
                    .load_owner_recovery_node(&predecessor_ref)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                if predecessor.next_slot != *reached_ref.object.slot() {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery node does not occupy its exact predecessor successor slot"
                            .to_string(),
                    ));
                }
                if predecessor.recovery_id != node_value.recovery_id {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery predecessor belongs to another recovery operation".to_string(),
                    ));
                }
                reached_ref = predecessor_ref;
                reached = predecessor;
            }
            if node_value.recovery_id != *recovery_id
                || node_value.readiness.registration != activated.registration
                || node_value.next_slot == *node.object.slot()
                || registration.author_pubkey != node_value.owner_pubkey
                || !predecessor_verifies_owner(
                    predecessor,
                    &node_value.membership,
                    &node_value.owner_pubkey,
                    &node_value.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "recovery node differs from its exact registration".to_string(),
                ));
            }
            let initial_ack = history_verifier
                .load_store_ack(&node_value.readiness.initial_ack, registration)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            if initial_ack.sequence != 1
                || initial_ack.successor.predecessor.is_some()
                || initial_ack.registration != activated.registration
                || initial_ack.store_cut != node_value.readiness.bootstrap_cut
            {
                return Err(RegistrationLoadError::Invalid(
                    "recovery readiness differs from its initial acknowledgement".to_string(),
                ));
            }
            Ok(StoreDeviceRegistrationActivation::Recovery {
                recovery_id: *recovery_id,
                node: node.clone(),
            })
        }
        _ => Err(RegistrationLoadError::Invalid(format!(
            "Store registration {} origin differs from its activation authority",
            registration.device_id
        ))),
    }
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

pub(crate) async fn predecessor_commit_matching(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    order: &super::store_commit::StoreCommitOrder,
    matches: PredecessorCommitPredicate<'_>,
) -> Result<Option<VerifiedStoreBatchCommit>, RegistrationLoadError> {
    let mut matches = matches;
    let mut pending = order
        .predecessor
        .iter()
        .chain(order.dependencies.values())
        .cloned()
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.clone()) {
            continue;
        }
        let commit = history_verifier
            .load_ref(&reference)
            .await
            .map_err(registration_attempt_error)?;
        if matches(&commit) {
            return Ok(Some(commit));
        }
        pending.extend(commit.value().order.predecessor.iter().cloned());
        pending.extend(commit.value().order.dependencies.values().cloned());
    }
    Ok(None)
}

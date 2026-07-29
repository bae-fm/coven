use super::*;

pub(super) async fn validate_commit_join_abandonments(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join abandonment activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join abandonment activation author is not an active Owner".to_string(),
        ));
    }
    for reference in commit
        .device_join_attempt_decisions()
        .iter()
        .filter_map(|decision| match decision {
            DeviceJoinAttemptDecisionRef::Attempt(_) => None,
            DeviceJoinAttemptDecisionRef::Abandoned(reference) => Some(reference),
        })
    {
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let bytes = storage
            .read_protocol_object(
                &context,
                &reference.object,
                &super::store_commit::device_join_abandonment_semantic_prefix(reference.attempt_id),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let abandonment: super::device_join::DeviceJoinAbandonmentObject =
            serde_json::from_slice(&bytes)
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if abandonment.store_root_hash != root.store_root_hash
            || abandonment.owner_registration != commit.author_registration
            || abandonment.attempt_slot != *reference.object.slot()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join abandonment differs from its activating commit".to_string(),
            ));
        }
        reference
            .verify(&abandonment, activating_author)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    }
    Ok(())
}

async fn load_commit_device_join_attempt_evidence(
    history_verifier: &MergeHistoryVerifier<'_>,
    reference: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<LoadedDeviceJoinAttemptEvidence, RegistrationLoadError> {
    let verifier = history_verifier.commit_verifier_ref();
    let (attempt, owner) = verifier
        .load_device_join_attempt_and_owner(reference)
        .await
        .map_err(RegistrationLoadError::Object)?;
    verifier
        .validate_device_join_attempt_evidence(attempt, &owner.value)
        .await
        .map_err(registration_attempt_error)
}

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

pub(crate) fn load_commit_join_cleanup_receipts<'a>(
    history_verifier: &'a MergeHistoryVerifier<'_>,
    commit: &'a StoreBatchCommit,
    activating_author: &'a StoreDeviceRegistration,
) -> RegistrationLoadFuture<'a, Vec<LoadedCommitJoinCleanupReceipt>> {
    Box::pin(async move {
        let storage = history_verifier.storage();
        let root = history_verifier.root();
        let verifier = history_verifier.commit_verifier_ref();
        let root_value = verifier.verified_root();
        let mut receipts = Vec::with_capacity(commit.device_join_cleanup_receipts().len());
        for reference in commit.device_join_cleanup_receipts() {
            let context = ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::DeviceJoinCleanupReceipt,
            );
            let bytes = storage
                .read_protocol_object(
                    &context,
                    &reference.object,
                    &super::store_commit::device_join_cleanup_receipt_semantic_prefix(
                        reference.attempt_id,
                    ),
                )
                .await
                .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
            let receipt: super::device_join::DeviceJoinCleanupReceiptObject =
                serde_json::from_slice(&bytes)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            if receipt.executor != commit.author_registration
                || receipt.membership != commit.membership_state
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt differs from its activating predecessor"
                        .to_string(),
                ));
            }
            let attempt_ref = receipt.cancellation.attempt();
            let (attempt, owner) = verifier
                .load_device_join_attempt_and_owner(attempt_ref)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let attempt = verifier
                .validate_device_join_attempt_evidence(attempt, &owner.value)
                .await
                .map_err(registration_attempt_error)?;
            let expected_administrator = &attempt
                .attempt
                .value
                .provider_approval
                .request
                .offer
                .provider_admin;
            if activating_author.provider != expected_administrator.provider
                || attempt
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider
                    != root_value.descriptor.provider
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup executor differs from its exact provider authority"
                        .to_string(),
                ));
            }
            reference
                .verify(&receipt, activating_author)
                .and_then(|_| receipt.verify(&attempt.attempt.value, activating_author))
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            match &receipt.administrator_terminal {
                super::device_join::ProviderAdminJoinTerminal::Completed(_) => {}
                super::device_join::ProviderAdminJoinTerminal::Cancelled(closure) => {
                    let administrator = verifier
                        .load_registration(&closure.administrator_registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    closure
                        .verify(&administrator)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
                super::device_join::ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                    let executor = verifier
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
            }
            match &receipt.joiner_terminal {
                super::device_join::JoinerJoinTerminal::Ready(_) => {}
                super::device_join::JoinerJoinTerminal::Cancelled(closure) => closure
                    .verify()
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
                super::device_join::JoinerJoinTerminal::WriteRevoked(revocation) => {
                    let executor = verifier
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
            }
            receipts.push(LoadedCommitJoinCleanupReceipt { receipt, attempt });
        }
        Ok(receipts)
    })
}

pub(crate) async fn load_commit_join_evidence(
    history_verifier: &MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
) -> Result<LoadedCommitJoinEvidence, RegistrationLoadError> {
    let loaded_cleanup =
        load_commit_join_cleanup_receipts(history_verifier, commit, activating_author).await?;
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
        let evidence =
            load_commit_device_join_attempt_evidence(history_verifier, &reference).await?;
        attempts.insert(reference, evidence);
    }
    Ok(LoadedCommitJoinEvidence {
        attempts,
        cleanup_receipts,
    })
}

pub(super) fn validate_commit_join_cleanup_receipts(
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
    accepted: super::device_join_attempt::VerifiedMergePredecessorHistory<'_>,
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
        if !predecessor_contains_join_outcome(accepted, &loaded.receipt.cancellation)? {
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

pub(super) async fn validate_commit_join_outcomes(
    commit_verifier: &StoreCommitVerifier<'_>,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
    join_evidence: &VerifiedCommitJoinEvidence,
    accepted: super::device_join_attempt::VerifiedMergePredecessorHistory<'_>,
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
        if !predecessor_contains_join_attempt(accepted, outcome_ref.attempt())? {
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
        let outcome = commit_verifier
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

pub(super) fn validate_commit_join_attempts(
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
    commit_verifier: &StoreCommitVerifier<'_>,
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
            let initial_ack = commit_verifier
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
            let node_value = commit_verifier
                .load_owner_recovery_node(node)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let mut reached_ref = node.clone();
            let mut reached = node_value.clone();
            while let Some(predecessor_ref) = reached.predecessor.clone() {
                let predecessor = commit_verifier
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
            let initial_ack = commit_verifier
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

fn predecessor_contains_join_attempt(
    accepted: super::device_join_attempt::VerifiedMergePredecessorHistory<'_>,
    expected: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<bool, RegistrationLoadError> {
    accepted
        .find(|_, commit| {
                commit.device_join_attempt_decisions().iter().any(|decision| {
                    matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
                })
        })
        .map(|found| found.is_some())
        .map_err(registration_attempt_error)
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

pub(crate) async fn predecessor_commit_matching(
    commit_verifier: &mut StoreCommitVerifier<'_>,
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
        let commit = commit_verifier
            .load_ref(&reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        if matches(&commit) {
            return Ok(Some(commit));
        }
        pending.extend(commit.value().order.predecessor.iter().cloned());
        pending.extend(commit.value().order.dependencies.values().cloned());
    }
    Ok(None)
}

fn predecessor_contains_join_outcome(
    accepted: super::device_join_attempt::VerifiedMergePredecessorHistory<'_>,
    expected: &super::store_commit::DeviceJoinOutcomeRef,
) -> Result<bool, RegistrationLoadError> {
    accepted
        .find(|_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        })
        .map(|found| found.is_some())
        .map_err(registration_attempt_error)
}

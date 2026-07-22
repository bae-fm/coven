use super::*;

pub(super) async fn validate_commit_join_abandonments(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join abandonment activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<LoadedDeviceJoinAttemptEvidence, RegistrationLoadError> {
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let bytes = storage
        .read_protocol_object(
            &context,
            &reference.object,
            &super::store_commit::device_join_attempt_semantic_prefix(reference.attempt_id),
        )
        .await
        .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
    let unverified: DeviceJoinAttempt = serde_json::from_slice(&bytes)
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    let owner = load_registration_ref(storage, root, &unverified.owner_registration)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    load_device_join_attempt_evidence_ref(storage, root, reference, &owner)
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
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    commit: &'a StoreBatchCommit,
    activating_author: &'a StoreDeviceRegistration,
) -> RegistrationLoadFuture<'a, Vec<LoadedCommitJoinCleanupReceipt>> {
    Box::pin(async move {
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
                || !predecessor_contains_join_outcome(
                    storage,
                    root,
                    root_value,
                    &commit.order,
                    &receipt.cancellation,
                )
                .await?
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt differs from its activating predecessor"
                        .to_string(),
                ));
            }
            let attempt_ref = receipt.cancellation.attempt();
            let attempt_context = ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::DeviceJoinAttempt,
            );
            let attempt_bytes = storage
                .read_protocol_object(
                    &attempt_context,
                    &attempt_ref.object,
                    &super::store_commit::device_join_attempt_semantic_prefix(
                        attempt_ref.attempt_id,
                    ),
                )
                .await
                .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
            let unverified: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            let owner = load_registration_ref(storage, root, &unverified.owner_registration)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let attempt = load_device_join_attempt_evidence_ref(storage, root, attempt_ref, &owner)
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
                    let administrator =
                        load_registration_ref(storage, root, &closure.administrator_registration)
                            .await
                            .map_err(RegistrationLoadError::Object)?
                            .value;
                    closure
                        .verify(&administrator)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
                super::device_join::ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                    let executor = load_registration_ref(storage, root, &revocation.executor)
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
                    let executor = load_registration_ref(storage, root, &revocation.executor)
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
) -> Result<LoadedCommitJoinEvidence, RegistrationLoadError> {
    let loaded_cleanup =
        load_commit_join_cleanup_receipts(storage, root, root_value, commit, activating_author)
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
        let evidence = load_commit_device_join_attempt_evidence(storage, root, &reference).await?;
        attempts.insert(reference, evidence);
    }
    Ok(LoadedCommitJoinEvidence {
        attempts,
        cleanup_receipts,
    })
}

pub(super) fn validate_commit_join_cleanup_receipts(
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    join_evidence: &VerifiedCommitJoinEvidence,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join cleanup activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join cleanup activation author is not an active Owner".to_string(),
        ));
    }
    for loaded in &join_evidence.cleanup_receipts {
        let attempt = join_evidence.attempts.get(&loaded.attempt).ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join cleanup receipt has no verified exact attempt".to_string(),
            )
        })?;
        let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
        if !predecessor.verifies_provider_administrator(
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    join_evidence: &VerifiedCommitJoinEvidence,
) -> Result<
    BTreeMap<super::store_commit::DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>,
    RegistrationLoadError,
> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join outcome activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join outcome activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    let mut verified = BTreeMap::new();
    for outcome_ref in commit.device_join_outcomes() {
        if !Box::pin(predecessor_contains_join_attempt(
            storage,
            root,
            root_value,
            &commit.order,
            outcome_ref.attempt(),
        ))
        .await?
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
        let outcome = load_device_join_outcome_ref(storage, root, outcome_ref, activating_author)
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
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    join_evidence: &VerifiedCommitJoinEvidence,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join attempt activation has no exact predecessor membership authority"
                .to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
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
            || !predecessor.verifies_owner(
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
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activated: &ActivatedStoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    activating_author: &StoreDeviceRegistration,
    predecessor: &RegistrationPredecessorAuthority<'_>,
    verified_join_outcomes: &BTreeMap<
        super::store_commit::DeviceJoinOutcomeRef,
        VerifiedCommitJoinOutcome,
    >,
) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
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
                || !predecessor.verifies_owner(
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
            let initial_ack =
                load_store_ack_ref(storage, root, &readiness.initial_ack, registration)
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
            let node_value = load_owner_recovery_node_ref(storage, root, node)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let mut reached_ref = node.clone();
            let mut reached = node_value.clone();
            while let Some(predecessor_ref) = reached.predecessor.clone() {
                let predecessor = load_owner_recovery_node_ref(storage, root, &predecessor_ref)
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
                || !predecessor.verifies_owner(
                    &node_value.membership,
                    &node_value.owner_pubkey,
                    &node_value.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "recovery node differs from its exact registration".to_string(),
                ));
            }
            let initial_ack = load_store_ack_ref(
                storage,
                root,
                &node_value.readiness.initial_ack,
                registration,
            )
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

async fn predecessor_contains_join_attempt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(
        Box::pin(predecessor_commit_matching_at_root(
            storage,
            root,
            root_value,
            order,
            Box::new(|_, commit| {
                commit.device_join_attempt_decisions().iter().any(|decision| {
                    matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
                })
            }),
        ))
        .await?
        .is_some(),
    )
}

type PredecessorCommitPredicate<'a> =
    Box<dyn FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool + Send + 'a>;

pub(crate) async fn predecessor_commit_matching(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    matches: PredecessorCommitPredicate<'_>,
) -> Result<Option<(StoreBatchCommitRef, StoreBatchCommit)>, RegistrationLoadError> {
    let root_value = load_store_protocol_root(storage, root)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        &root_value,
        order,
        matches,
    ))
    .await
}

pub(super) async fn predecessor_commit_matching_at_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    mut matches: PredecessorCommitPredicate<'_>,
) -> Result<Option<(StoreBatchCommitRef, StoreBatchCommit)>, RegistrationLoadError> {
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
        let (commit, _) = load_commit_with_author_at_root(storage, root, root_value, &reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        if matches(&reference, &commit) {
            return Ok(Some((reference, commit)));
        }
        pending.extend(commit.order.predecessor);
        pending.extend(commit.order.dependencies.into_values());
    }
    Ok(None)
}

async fn predecessor_contains_join_outcome(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinOutcomeRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        order,
        Box::new(|_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        }),
    ))
    .await?
    .is_some())
}

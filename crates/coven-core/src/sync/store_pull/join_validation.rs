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

async fn load_commit_device_join_attempt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &super::store_commit::DeviceJoinAttemptRef,
    owner: &StoreDeviceRegistration,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<DeviceJoinAttempt, RegistrationLoadError> {
    let attempt = match accepted_predecessor {
        Some(accepted_predecessor) => load_verified_device_join_attempt_evidence_ref(
            storage,
            root,
            reference,
            owner,
            Some(accepted_predecessor),
        ),
        None => load_verified_device_join_attempt_ref(storage, root, reference, owner),
    }
    .await
    .map_err(registration_attempt_error)?;
    Ok(attempt.value)
}

pub(super) async fn validate_commit_join_cleanup_receipts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<Vec<super::device_join::DeviceJoinCleanupReceiptObject>, RegistrationLoadError> {
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
                "device join cleanup receipt differs from its activating predecessor".to_string(),
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
                &super::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let unverified: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        let owner = load_registration_ref(storage, root, &unverified.owner_registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            attempt_ref,
            &owner,
            accepted_predecessor,
        ))
        .await?;
        let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
        let protocol_root = load_store_protocol_root(storage, root)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if !predecessor.verifies_provider_administrator(
            &receipt.provider_admin_grant,
            &receipt.executor,
            expected_administrator,
        ) || activating_author.provider != expected_administrator.provider
            || attempt.provider_approval.request.offer.provider != protocol_root.descriptor.provider
        {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup executor is not the exact effective provider administrator"
                    .to_string(),
            ));
        }
        reference
            .verify(&receipt, activating_author)
            .and_then(|_| receipt.verify(&attempt, activating_author))
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
        receipts.push(receipt);
    }
    Ok(receipts)
}

pub(super) async fn validate_commit_join_outcomes(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
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
        let attempt_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_bytes = storage
            .read_protocol_object(
                &attempt_context,
                &outcome_ref.attempt().object,
                &super::store_commit::device_join_attempt_semantic_prefix(
                    outcome_ref.attempt().attempt_id,
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
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            outcome_ref.attempt(),
            &owner,
            accepted_predecessor,
        ))
        .await?;
        if owner != *activating_author
            || attempt.owner_registration != commit.author_registration
            || outcome_ref.slot() != &attempt.outcome_slot
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome differs from its exact Owner attempt".to_string(),
            ));
        }
        let outcome = load_device_join_outcome_ref(storage, root, outcome_ref, &owner)
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
                    attempt,
                    owner,
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

pub(super) async fn validate_commit_join_attempts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
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
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            reference,
            activating_author,
            accepted_predecessor,
        ))
        .await?;
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

pub(super) async fn registration_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activated: &ActivatedStoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    activating_author: &StoreDeviceRegistration,
    serial_recovery_activation: Option<&super::store_commit::SerialRecoveryActivation>,
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
                || !predecessor.verifies_owner_at_ancestor(
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
            if matches!(predecessor, RegistrationPredecessorAuthority::Serial { .. })
                && serial_recovery_activation.is_none_or(|body| &body.registration != activated)
            {
                return Err(RegistrationLoadError::Invalid(
                    "Serial recovery activation differs from its closed commit body".to_string(),
                ));
            }
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
    let mut pending = match order {
        super::store_commit::StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => predecessor
            .iter()
            .chain(dependencies.values())
            .cloned()
            .collect::<Vec<_>>(),
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: super::store_commit::StoreSerialPredecessor::Commit(predecessor),
            ..
        } => vec![predecessor.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: super::store_commit::StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    };
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
        match commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                pending.extend(predecessor);
                pending.extend(dependencies.into_values());
            }
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: super::store_commit::StoreSerialPredecessor::Commit(predecessor),
                ..
            } => pending.push(predecessor),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: super::store_commit::StoreSerialPredecessor::Genesis { .. },
                ..
            } => {}
        }
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

use super::*;

pub(super) enum DeviceStateResolver<'a> {
    Database(&'a Database),
    Loaded {
        genesis: &'a ResolvedStoreDeviceState,
        states: &'a BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    },
}

fn resolve_loaded_device_state(
    reference: &StoreDeviceStateRef,
    genesis: &ResolvedStoreDeviceState,
    states: &BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
    let state = match reference {
        StoreDeviceStateRef::MergeConcurrent { frontier, .. } => {
            let CommitFrontier::MergeConcurrent(frontier) = frontier else {
                return Err(RegistrationLoadError::Invalid(
                    "Merge device state contains a Serial frontier".to_string(),
                ));
            };
            if frontier.is_empty() {
                genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    frontier
                        .values()
                        .map(|commit| {
                            states.get(commit).cloned().ok_or_else(|| {
                                RegistrationLoadError::Invalid(
                                    "device state references an unloaded predecessor snapshot"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?
            }
        }
        StoreDeviceStateRef::Serial { position, .. } => match position {
            StoreSerialPredecessor::Genesis { .. } => genesis.clone(),
            StoreSerialPredecessor::Commit(commit) => {
                states.get(commit).cloned().ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "Serial device state references an unloaded predecessor snapshot"
                            .to_string(),
                    )
                })?
            }
        },
    };
    if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
        return Err(RegistrationLoadError::Invalid(
            "device state differs from its exact predecessor snapshots".to_string(),
        ));
    }
    Ok(state)
}

async fn resolve_device_state(
    resolver: &DeviceStateResolver<'_>,
    reference: &StoreDeviceStateRef,
) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
    match resolver {
        DeviceStateResolver::Database(db) => db
            .resolved_store_device_state(reference)
            .await
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string())),
        DeviceStateResolver::Loaded { genesis, states } => {
            resolve_loaded_device_state(reference, genesis, states)
        }
    }
}

async fn predecessor_acknowledgement_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::StoreAckRef,
    ack: &super::store_commit::StoreAck,
) -> Result<bool, RegistrationLoadError> {
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
            predecessor: StoreSerialPredecessor::Commit(predecessor),
            ..
        } => vec![predecessor.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    };
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.clone()) {
            continue;
        }
        let (commit, _) = load_commit_with_author(storage, root, &reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        if commit.acknowledgement() == Some(expected) {
            let predecessor_cut = commit
                .order
                .predecessor_cut()
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            return Ok(commit.author_registration == expected.registration
                && ack.registration == expected.registration
                && ack.store_cut == predecessor_cut
                && ack.device_state == commit.device_state);
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
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => pending.push(predecessor),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => {}
        }
    }
    Ok(false)
}

async fn verify_merge_device_exclusion_proof(
    resolver: &DeviceStateResolver<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    proposal: &super::store_objects::VerifiedDeviceExclusionProposal,
    remaining_device_acks: &[super::store_commit::StoreAckRef],
    cutoff: &StoreHistoryCut,
) -> Result<(), RegistrationLoadError> {
    let frozen = resolve_device_state(resolver, &proposal.object.value.frozen_device_state).await?;
    if !device_state_has_active_registration(&frozen, &proposal.object.value.target) {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion proposal frozen state does not contain its active target".to_string(),
        ));
    }
    let required = frozen
        .devices
        .values()
        .filter(|record| {
            record.registration != proposal.object.value.target
                && matches!(record.status, StoreDeviceStatus::Active)
        })
        .map(|record| (record.registration.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &proposal.object.value.target,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let mut certified = BTreeSet::new();
    let mut joined = BTreeMap::new();
    for reference in remaining_device_acks {
        let required_record = required.get(&reference.registration).ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device exclusion proof contains an acknowledgement from an ineligible registration"
                    .to_string(),
            )
        })?;
        if !certified.insert(reference.registration.clone()) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof repeats a remaining registration".to_string(),
            ));
        }
        let registration = load_registration_ref(storage, root, &required_record.registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let ack = load_store_ack_ref(storage, root, reference, &registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if !predecessor_acknowledgement_activation(storage, root, &commit.order, reference, &ack)
            .await?
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement is not activated in the outcome predecessor"
                    .to_string(),
            ));
        }
        let ack_state = resolve_device_state(resolver, &ack.device_state).await?;
        if !device_state_has_pending_proposal(&ack_state, &proposal.reference) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement does not observe the pending proposal"
                    .to_string(),
            ));
        }
        let freezes = match &ack.exclusions {
            super::store_commit::StoreAckExclusionState::MergeConcurrent { proposal_freezes } => {
                proposal_freezes
            }
            super::store_commit::StoreAckExclusionState::Serial => {
                return Err(RegistrationLoadError::Invalid(
                    "Merge device exclusion proof contains a Serial acknowledgement".to_string(),
                ))
            }
        };
        let freeze = freezes
            .iter()
            .find(|freeze| freeze.proposal == proposal.reference)
            .ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device exclusion proof acknowledgement omits the exact proposal freeze"
                        .to_string(),
                )
            })?;
        let StoreHistoryCut::MergeConcurrent(target_cut) = &freeze.target_cut else {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement carries a Serial target cut".to_string(),
            ));
        };
        if target_cut.len() > 1 || target_cut.keys().any(|stream| stream != &target_stream) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement includes a non-target stream".to_string(),
            ));
        }
        if !ack
            .store_cut
            .frontier()
            .covers(&freeze.target_cut.frontier())
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement target cut exceeds its Store cut"
                    .to_string(),
            ));
        }
        if let Some(reference) = target_cut.get(&target_stream) {
            match joined.entry(target_stream) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if reference.coord.sequence() > current.coord.sequence() {
                        entry.insert(reference.clone());
                    } else if reference.coord.sequence() == current.coord.sequence()
                        && reference != current
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof target cuts fork at one sequence".to_string(),
                        ));
                    }
                }
            }
        }
    }
    if certified != required.into_keys().collect()
        || cutoff != &StoreHistoryCut::MergeConcurrent(joined)
    {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion proof does not certify every remaining registration and exact cutoff"
                .to_string(),
        ));
    }
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    let StoreHistoryCut::MergeConcurrent(predecessor_frontier) = predecessor_cut else {
        return Err(RegistrationLoadError::Invalid(
            "Merge device exclusion outcome carries a Serial predecessor".to_string(),
        ));
    };
    let predecessor_target = predecessor_frontier
        .get(&target_stream)
        .map(|reference| BTreeMap::from([(target_stream, reference.clone())]));
    let target_predecessor_cut =
        StoreHistoryCut::MergeConcurrent(predecessor_target.unwrap_or_default());
    if !cutoff.frontier().covers(&target_predecessor_cut.frontier()) {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion outcome predecessor advances the target beyond its certified cutoff"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) async fn load_commit_device_operations(
    resolver: Option<&DeviceStateResolver<'_>>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    predecessor_state: &ResolvedStoreDeviceState,
    predecessor_authority: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<VerifiedStoreDeviceOperations, RegistrationLoadError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
    }
    let authority = predecessor_authority.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device exclusion activation has no exact predecessor membership authority".to_string(),
        )
    })?;
    let mut proposals = Vec::with_capacity(commit.device_exclusion_proposals().len());
    for reference in commit.device_exclusion_proposals() {
        let opened = load_device_exclusion_proposal_ref(storage, root, reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let proposal = &opened.object.value;
        if proposal.frozen_device_state != commit.device_state
            || !device_state_has_active_registration(predecessor_state, &proposal.target)
            || !device_state_has_active_registration(
                predecessor_state,
                &proposal.owner_registration,
            )
            || !authority.verifies_owner(
                &commit.membership_state,
                &opened.owner.author_pubkey,
                &proposal.owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proposal differs from its active predecessor authority"
                    .to_string(),
            ));
        }
        proposals.push(RetainedStoreDeviceExclusionProposal::from_verified(&opened));
    }
    let mut outcomes = Vec::with_capacity(commit.device_exclusion_outcomes().len());
    for reference in commit.device_exclusion_outcomes() {
        if !device_state_has_pending_proposal(predecessor_state, reference.proposal()) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion outcome does not resolve an exact pending proposal".to_string(),
            ));
        }
        let proposal = load_device_exclusion_proposal_ref(storage, root, reference.proposal())
            .await
            .map_err(RegistrationLoadError::Object)?;
        let outcome = load_device_exclusion_outcome_ref(storage, root, reference, &proposal)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let (owner_registration, owner_grant) = match &outcome.object.value {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => {
                (&exclusion.owner_registration, &exclusion.owner_grant)
            }
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                (&cancellation.owner_registration, &cancellation.owner_grant)
            }
        };
        if !device_state_has_active_registration(predecessor_state, owner_registration)
            || !authority.verifies_owner(
                &commit.membership_state,
                &outcome.owner.author_pubkey,
                owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion outcome signer is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        match (&outcome.object.value, reference) {
            (
                StoreDeviceExclusionOutcome::Cancelled(_),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Cancelled(_),
            ) => {}
            (
                StoreDeviceExclusionOutcome::Excluded(exclusion),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(_),
            ) => match &exclusion.proof {
                StoreDeviceExclusionProof::Serial
                    if commit.policy() == crate::WritePolicy::Serial => {}
                StoreDeviceExclusionProof::Serial => {
                    return Err(RegistrationLoadError::Invalid(
                        "Merge device exclusion outcome carries a Serial proof".to_string(),
                    ))
                }
                StoreDeviceExclusionProof::MergeConcurrent {
                    frozen_device_state,
                    remaining_device_acks,
                    cutoff,
                } if commit.policy() == crate::WritePolicy::MergeConcurrent => {
                    if frozen_device_state != &proposal.object.value.frozen_device_state {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof names another frozen device state".to_string(),
                        ));
                    }
                    let resolver = resolver.ok_or_else(|| {
                        RegistrationLoadError::Invalid(
                            "Merge device exclusion proof has no materialized state resolver"
                                .to_string(),
                        )
                    })?;
                    verify_merge_device_exclusion_proof(
                        resolver,
                        storage,
                        root,
                        commit,
                        &proposal,
                        remaining_device_acks,
                        cutoff,
                    )
                    .await?;
                }
                StoreDeviceExclusionProof::MergeConcurrent { .. } => {
                    return Err(RegistrationLoadError::Invalid(
                        "Serial device exclusion outcome carries a Merge proof".to_string(),
                    ))
                }
            },
            _ => {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome variant differs from its exact reference".to_string(),
                ))
            }
        }
        outcomes.push(
            RetainedStoreDeviceExclusionOutcome::from_verified(
                reference,
                RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
                &outcome,
            )
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
        );
    }
    RetainedStoreDeviceOperations::from_sources(proposals, outcomes)
        .verify_for(root, commit)
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) async fn load_local_commit_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Database(error.to_string()));
    }
    let (state_ref, state) = db.store_device_state_for_order(&commit.order).await?;
    if state_ref != commit.device_state {
        return Err(StorePullError::Database(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    let authorization =
        load_device_join_authorization(storage, root, &commit.membership_state).await?;
    let authority = match &authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    load_local_commit_device_operations_with_authority(db, storage, root, commit, state, &authority)
        .await
}

pub(crate) async fn load_local_commit_device_operations_with_merge_membership(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    membership: &MembershipChain,
    state_ref: &StoreDeviceStateRef,
    state: ResolvedStoreDeviceState,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Database(error.to_string()));
    }
    if commit.policy() != crate::WritePolicy::MergeConcurrent {
        return Err(StorePullError::Database(
            "retained Merge membership authority received a Serial commit".to_string(),
        ));
    }
    if state_ref != &commit.device_state {
        return Err(StorePullError::Database(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    verify_merge_membership_state_ref(&commit.membership_state, membership, &state)?;
    let authority = RegistrationPredecessorAuthority::MergeConcurrent(membership);
    load_local_commit_device_operations_with_authority(db, storage, root, commit, state, &authority)
        .await
}

async fn load_local_commit_device_operations_with_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    state: ResolvedStoreDeviceState,
    authority: &RegistrationPredecessorAuthority<'_>,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    let resolver = DeviceStateResolver::Database(db);
    Box::pin(load_commit_device_operations(
        Some(&resolver),
        storage,
        root,
        commit,
        &state,
        Some(authority),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })
}

pub(crate) async fn derive_local_merge_post_device_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    predecessor_state: ResolvedStoreDeviceState,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    device_operations: VerifiedStoreDeviceOperations,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let (authorized_predecessor, recovery_author) =
        predecessor_with_recovery_author(predecessor_state, commit, registrations)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery = Box::pin(verify_commit_owner_recovery_activation(
        storage, root, commit, None,
    ))
    .await?;
    device_operations
        .apply_to(authorized_predecessor, &commit.device_state)
        .and_then(|state| {
            apply_verified_device_lifecycle(
                state,
                commit,
                registrations,
                recovery_author.as_ref(),
                owner_recovery,
            )
        })
        .map_err(|error| StorePullError::Database(error.to_string()))
}

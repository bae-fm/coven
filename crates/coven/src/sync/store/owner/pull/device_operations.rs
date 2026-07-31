use super::*;

pub(crate) enum DeviceStateResolver<'a> {
    Database(&'a StoreDatabase),
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
    let state = {
        let frontier = &reference.frontier().0;
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
    };
    if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
        return Err(RegistrationLoadError::Invalid(
            "device state differs from its exact predecessor snapshots".to_string(),
        ));
    }
    Ok(state)
}

impl DeviceStateResolver<'_> {
    async fn resolve(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
        match self {
            DeviceStateResolver::Database(database) => database
                .resolved_store_device_state(reference)
                .await
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string())),
            DeviceStateResolver::Loaded { genesis, states } => {
                resolve_loaded_device_state(reference, genesis, states)
            }
        }
    }
}

pub(crate) async fn load_commit_device_operations(
    resolver: Option<&DeviceStateResolver<'_>>,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    commit: &StoreBatchCommit,
    predecessor_state: &ResolvedStoreDeviceState,
    predecessor_membership: Option<&MembershipChain>,
) -> Result<VerifiedStoreDeviceOperations, RegistrationLoadError> {
    let root = commit_verifier.root().clone();
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
    }
    let predecessor = predecessor_membership.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device exclusion activation has no exact predecessor membership authority".to_string(),
        )
    })?;
    let mut proposals = Vec::with_capacity(commit.device_exclusion_proposals().len());
    for reference in commit.device_exclusion_proposals() {
        let opened = commit_verifier
            .load_device_exclusion_proposal(reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let proposal = &opened.object.value;
        if proposal.frozen_device_state != commit.device_state
            || !device_state_has_active_registration(predecessor_state, &proposal.target)
            || !device_state_has_active_registration(
                predecessor_state,
                &proposal.owner_registration,
            )
            || !predecessor_verifies_owner(
                predecessor,
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
        let proposal = commit_verifier
            .load_device_exclusion_proposal(reference.proposal())
            .await
            .map_err(RegistrationLoadError::Object)?;
        let outcome = commit_verifier
            .load_device_exclusion_outcome(reference, &proposal)
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
            || !predecessor_verifies_owner(
                predecessor,
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
            ) => {
                let StoreDeviceExclusionProof {
                    frozen_device_state,
                    remaining_device_acks,
                    cutoff,
                } = &exclusion.proof;
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
                let frozen = resolver
                    .resolve(&proposal.object.value.frozen_device_state)
                    .await?;
                if !device_state_has_active_registration(&frozen, &proposal.object.value.target) {
                    return Err(RegistrationLoadError::Invalid(
                        "device exclusion proposal frozen state does not contain its active target"
                            .to_string(),
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
                let target_stream =
                    super::store_commit::StreamActivation::device_authorized_stream_id(
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
                    let registration = commit_verifier
                        .load_registration(&required_record.registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    let ack = commit_verifier
                        .load_store_ack(reference, &registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    if !commit_verifier
                        .predecessor_activates_acknowledgement(&commit.order, reference, &ack)
                        .await
                        .map_err(registration_attempt_error)?
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof acknowledgement is not activated in the outcome predecessor"
                                .to_string(),
                        ));
                    }
                    let ack_state = resolver.resolve(&ack.device_state).await?;
                    if !device_state_has_pending_proposal(&ack_state, &proposal.reference) {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof acknowledgement does not observe the pending proposal"
                                .to_string(),
                        ));
                    }
                    let freeze = ack
                        .exclusions
                        .proposal_freezes
                        .iter()
                        .find(|freeze| freeze.proposal == proposal.reference)
                        .ok_or_else(|| {
                            RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement omits the exact proposal freeze"
                                    .to_string(),
                            )
                        })?;
                    let target_cut = &freeze.target_cut.0;
                    if target_cut.len() > 1
                        || target_cut.keys().any(|stream| stream != &target_stream)
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof acknowledgement includes a non-target stream"
                                .to_string(),
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
                                        "device exclusion proof target cuts fork at one sequence"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
                if certified != required.into_keys().collect() || cutoff != &StoreHistoryCut(joined)
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
                let predecessor_target = predecessor_cut
                    .0
                    .get(&target_stream)
                    .map(|reference| BTreeMap::from([(target_stream, reference.clone())]));
                let target_predecessor_cut =
                    StoreHistoryCut(predecessor_target.unwrap_or_default());
                if !cutoff.frontier().covers(&target_predecessor_cut.frontier()) {
                    return Err(RegistrationLoadError::Invalid(
                        "device exclusion outcome predecessor advances the target beyond its certified cutoff"
                            .to_string(),
                    ));
                }
            }
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
        .verify_for(&root, commit)
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

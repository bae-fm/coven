use super::*;

pub(crate) struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: StoreBatchCommit,
    pub registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub activation: DeviceJoinBootstrapActivation,
}

pub(crate) enum DeviceJoinBootstrapActivation {
    MergeConcurrent {
        head: StoreDeviceHead,
        object: ExactObjectRef,
        history_summary: RetainedVerifiedMergeHistorySummary,
    },
    Serial,
}

pub(crate) struct DeviceJoinBootstrapPlan {
    pub founder_reference: StoreDeviceRegistrationRef,
    pub founder: StoreDeviceRegistration,
    pub founder_bytes: Vec<u8>,
    pub genesis: ResolvedStoreDeviceState,
    pub coverage: StoreHistoryCut,
    pub commits: Vec<DeviceJoinBootstrapCommit>,
}

pub(crate) fn history_cut_references(cut: &StoreHistoryCut) -> Vec<StoreBatchCommitRef> {
    match cut {
        StoreHistoryCut::MergeConcurrent(frontier) => frontier.values().cloned().collect(),
        StoreHistoryCut::Serial(StoreSerialPredecessor::Commit(reference)) => {
            vec![reference.clone()]
        }
        StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis { .. }) => Vec::new(),
    }
}

pub(crate) fn commit_predecessor_references(commit: &StoreBatchCommit) -> Vec<StoreBatchCommitRef> {
    match &commit.order {
        super::store_commit::StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => predecessor
            .iter()
            .chain(dependencies.values())
            .cloned()
            .collect(),
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Commit(reference),
            ..
        } => vec![reference.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    }
}

pub(crate) fn registration_recovery_cursor(
    origin: &StoreDeviceRegistrationOrigin,
    activation: &super::store_commit::StoreDeviceRegistrationActivation,
) -> Result<Option<super::store_commit::OwnerRecoveryCursor>, StoreProtocolError> {
    match (origin, activation) {
        (
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot,
                owner_grant,
            },
            StoreDeviceRegistrationActivation::Recovery {
                recovery_id: activated_recovery_id,
                node,
            },
        ) if recovery_id == activated_recovery_id
            && recovery_slot == node.object.slot()
            && owner_grant == &node.owner_grant =>
        {
            Ok(Some(OwnerRecoveryCursor {
                owner_grant: owner_grant.clone(),
                position: OwnerRecoveryPosition::At { node: node.clone() },
            }))
        }
        (
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                outcome_slot,
                ..
            },
            StoreDeviceRegistrationActivation::Join {
                attempt_id: activated_attempt_id,
                outcome,
            },
        ) if attempt_id == activated_attempt_id && outcome_slot == outcome.slot() => Ok(None),
        (
            StoreDeviceRegistrationOrigin::Founder { .. },
            StoreDeviceRegistrationActivation::Founder { .. },
        ) => Ok(None),
        _ => Err(StoreProtocolError::Malformed(
            "registration origin differs from its exact activation authority".to_string(),
        )),
    }
}

pub(crate) fn predecessor_with_recovery_author(
    mut predecessor: ResolvedStoreDeviceState,
    commit: &StoreBatchCommit,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
) -> Result<(ResolvedStoreDeviceState, Option<StoreDeviceRegistrationRef>), StoreProtocolError> {
    if commit.device_registrations().len() != registrations.len() {
        return Err(StoreProtocolError::Malformed(
            "verified registrations do not cover every activation".to_string(),
        ));
    }
    for (activated, (registration, authority)) in
        commit.device_registrations().iter().zip(registrations)
    {
        activated.registration.verify_registration(registration)?;
        if activated.registration == commit.author_registration {
            if let Some(cursor) = registration_recovery_cursor(&registration.origin, authority)? {
                predecessor = predecessor
                    .activate_registration(activated.registration.clone(), Some(cursor))?;
                return Ok((predecessor, Some(activated.registration.clone())));
            }
        }
    }
    Ok((predecessor, None))
}

pub(crate) async fn verify_commit_owner_recovery_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    serial_predecessor: Option<(&SerialAuthorizationState, &ResolvedStoreDeviceState)>,
) -> Result<
    Option<(
        super::membership::MembershipGrantId,
        super::store_commit::OwnerRecoveryActivationId,
    )>,
    StorePullError,
> {
    if let Some(super::store_commit::StoreControl::SerialMembership { entry }) = commit.control() {
        let super::membership::SerialMembershipChange::SetMember {
            user_pubkey,
            role:
                super::membership::StoreMembershipRoleGrant::Owner {
                    recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
                },
            grant_id,
            replaces,
            ..
        } = &entry.change
        else {
            return Ok(None);
        };
        let Some((authorization, devices)) = serial_predecessor else {
            return Err(StorePullError::Serial(
                "Serial Owner promotion has no verified predecessor authority".to_string(),
            ));
        };
        let request = &acceptance.request;
        let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
        let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
        request
            .verify(root, &promoter.value)
            .and_then(|()| acceptance.verify(&candidate.value))
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        let super::store_commit::OwnerPromotionRequestActivation::Serial {
            commit: request_commit_ref,
        } = &acceptance.activation
        else {
            return Err(StorePullError::Serial(
                "Serial Owner promotion carries Merge activation".to_string(),
            ));
        };
        if commit.order.predecessor() != Some(request_commit_ref)
            || user_pubkey != &request.member_pubkey
            || grant_id != &request.intended_owner_grant
            || replaces != &BTreeSet::from([request.member_grant.clone()])
            || authorization
                .membership
                .active_owner_grant(&promoter.value.author_pubkey)
                .as_ref()
                != Some(&request.promoter_owner_grant)
            || authorization
                .membership
                .active_grant_ids(&request.member_pubkey)
                != BTreeSet::from([request.member_grant.clone()])
            || !authorization
                .membership
                .is_member_grant(&request.member_pubkey, &request.member_grant)
            || !device_state_has_active_registration(devices, &request.promoter_registration)
            || !device_state_has_active_registration(devices, &request.member_registration)
        {
            return Err(StorePullError::Serial(
                "Serial Owner promotion differs from its exact predecessor authority".to_string(),
            ));
        }
        let request_commit = load_commit_ref(
            storage,
            root.store_root_hash,
            request_commit_ref,
            &promoter.value,
        )
        .await?;
        if request_commit.value.owner_promotion_request() != Some(request)
            || request_commit.value.membership_state != request.predecessor_membership
            || request_commit.value.device_state != request.predecessor_devices
            || request_commit.value.author_registration != request.promoter_registration
        {
            return Err(StorePullError::Serial(
                "Serial Owner-promotion request commit differs from its acceptance".to_string(),
            ));
        }
        return super::store_commit::OwnerRecoveryActivationId::derive(
            root,
            &request.member_pubkey,
            grant_id,
            acceptance.anchors.recovery(),
        )
        .map(|activation| Some((grant_id.clone(), activation)))
        .map_err(|error| StorePullError::Serial(error.to_string()));
    }

    let mut recoveries = commit.stream_activations().iter().filter_map(|activation| {
        let super::store_commit::StreamActivation::GrantAuthorized {
            author_registration,
            grant_id,
            anchor: anchor @ super::store_commit::GrantStreamAnchor::OwnerRecovery { .. },
            ..
        } = activation
        else {
            return None;
        };
        Some((author_registration, grant_id, anchor))
    });
    let Some((registration_ref, grant_id, anchor)) = recoveries.next() else {
        return Ok(None);
    };
    if recoveries.next().is_some() {
        return Err(StorePullError::Database(
            "Store commit activates more than one Owner recovery stream".to_string(),
        ));
    }
    let registration = load_registration_ref(storage, root, registration_ref).await?;
    super::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &registration.value.author_pubkey,
        grant_id,
        anchor,
    )
    .map(|activation| Some((grant_id.clone(), activation)))
    .map_err(|error| StorePullError::Database(error.to_string()))
}

pub(crate) fn apply_verified_device_lifecycle(
    mut state: ResolvedStoreDeviceState,
    commit: &StoreBatchCommit,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    preactivated: Option<&StoreDeviceRegistrationRef>,
    owner_recovery: Option<(
        super::membership::MembershipGrantId,
        super::store_commit::OwnerRecoveryActivationId,
    )>,
) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
    if commit.device_registrations().len() != registrations.len() {
        return Err(StoreProtocolError::Malformed(
            "verified registrations do not cover every activation".to_string(),
        ));
    }
    for (activated, (registration, authority)) in
        commit.device_registrations().iter().zip(registrations)
    {
        activated.registration.verify_registration(registration)?;
        if preactivated != Some(&activated.registration) {
            state = state.activate_registration(
                activated.registration.clone(),
                registration_recovery_cursor(&registration.origin, authority)?,
            )?;
        }
    }
    for retirement in commit.device_retirements() {
        state = state.self_retire(retirement.clone())?;
    }
    if let Some((grant_id, activation)) = owner_recovery {
        state = state.activate_owner_recovery(grant_id, activation)?;
    }
    Ok(state)
}

pub(crate) fn verified_merge_predecessor_state(
    genesis: &ResolvedStoreDeviceState,
    states: &BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    commit: &StoreBatchCommit,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let super::store_commit::StoreCommitOrder::MergeConcurrent {
        predecessor,
        dependencies,
        ..
    } = &commit.order
    else {
        return Err(StorePullError::Database(
            "Merge history contains a Serial commit order".to_string(),
        ));
    };
    let mut predecessor_refs = dependencies.values().collect::<Vec<_>>();
    predecessor_refs.extend(predecessor.iter());
    let predecessor_state = if predecessor_refs.is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            predecessor_refs
                .into_iter()
                .map(|dependency| {
                    states.get(dependency).cloned().ok_or_else(|| {
                        StorePullError::Database(
                            "Merge history has an unresolved predecessor state".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let mut frontier = dependencies.clone();
    if let Some(predecessor) = predecessor {
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
            return Err(StorePullError::Database(
                "Merge predecessor carries a Serial coordinate".to_string(),
            ));
        };
        if frontier
            .insert(stream_id, predecessor.clone())
            .is_some_and(|existing| existing != *predecessor)
        {
            return Err(StorePullError::Database(
                "Merge predecessor conflicts with its dependency cut".to_string(),
            ));
        }
    }
    let expected_state = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier),
        &predecessor_state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if commit.device_state != expected_state {
        return Err(StorePullError::Database(
            "Merge commit names another predecessor device state".to_string(),
        ));
    }
    Ok(predecessor_state)
}

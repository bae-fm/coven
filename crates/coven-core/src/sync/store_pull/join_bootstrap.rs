use super::*;

pub(crate) async fn prepare_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    verified_authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    if coverage.policy() != attempt_activation.coord.policy() {
        return Err(StorePullError::Database(
            "device join bootstrap cut and attempt activation use different policies".to_string(),
        ));
    }
    if matches!(coverage, StoreHistoryCut::Serial(_)) {
        return Box::pin(prepare_serial_device_join_bootstrap(
            storage,
            root,
            coverage,
            attempt_activation,
            verified_authorization,
        ))
        .await;
    }
    let DeviceJoinBootstrapAuthorization::MergeConcurrent {
        state: verified_state,
        chain: _,
    } = verified_authorization
    else {
        return Err(StorePullError::Database(
            "Merge device join bootstrap received Serial membership authority".to_string(),
        ));
    };
    let verified_root = load_store_protocol_root(storage, root).await?.value;
    let founder = Box::pin(load_founder_registration(storage, root)).await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_reference.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;

    let mut pending = history_cut_references(coverage);
    pending.push(attempt_activation.clone());
    let verified_history =
        verify_merge_history_refs(storage, root, pending.iter().cloned()).await?;
    let mut loaded =
        BTreeMap::<StoreBatchCommitRef, (StoreBatchCommit, StoreDeviceRegistration)>::new();
    while let Some(reference) = pending.pop() {
        if loaded.contains_key(&reference) {
            continue;
        }
        let (commit, author) = Box::pin(load_commit_with_author(storage, root, &reference)).await?;
        pending.extend(commit_predecessor_references(&commit));
        loaded.insert(reference, (commit, author));
    }
    let activation = loaded.get(attempt_activation).ok_or_else(|| {
        StorePullError::Database("device join attempt activation is absent from its graph".into())
    })?;
    if activation
        .0
        .order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        != *coverage
    {
        return Err(StorePullError::Database(
            "device join attempt activation predecessor differs from its signed bootstrap cut"
                .to_string(),
        ));
    }
    let verified_activation = verified_history
        .commits
        .get(attempt_activation)
        .ok_or_else(|| {
            StorePullError::Database(
                "device join attempt activation is absent from its verified Merge history"
                    .to_string(),
            )
        })?;
    if &verified_activation.commit.membership_state != verified_state {
        return Err(StorePullError::Database(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let mut states = BTreeMap::<StoreBatchCommitRef, ResolvedStoreDeviceState>::new();
    let mut ordered = Vec::with_capacity(loaded.len());
    while !loaded.is_empty() {
        let next = loaded.iter().find_map(|(reference, (commit, _))| {
            commit_predecessor_references(commit)
                .iter()
                .all(|dependency| states.contains_key(dependency))
                .then(|| reference.clone())
        });
        let Some(reference) = next else {
            return Err(StorePullError::Database(
                "device join bootstrap history is cyclic or has an unresolved predecessor"
                    .to_string(),
            ));
        };
        let (commit, author) = loaded
            .remove(&reference)
            .expect("selected bootstrap commit remains loaded");
        let predecessor_state = match &commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                verified_merge_predecessor_state(&genesis, &states, &commit)?
            }
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => genesis.clone(),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => states
                .get(predecessor)
                .expect("topological Serial predecessor state exists")
                .clone(),
        };
        let verified_commit = verified_history.commits.get(&reference).ok_or_else(|| {
            StorePullError::Database(
                "device join bootstrap commit is absent from its verified Merge history"
                    .to_string(),
            )
        })?;
        if verified_commit.commit != commit
            || verified_commit.predecessor_state != predecessor_state
        {
            return Err(StorePullError::Database(
                "device join bootstrap commit differs from its verified Merge history".to_string(),
            ));
        }
        let carries_lifecycle = !(commit.device_join_attempt_decisions().is_empty()
            && commit.device_join_outcomes().is_empty()
            && commit.device_join_cleanup_receipts().is_empty()
            && commit.device_registrations().is_empty()
            && commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
            && commit.reclaim_authorization().is_none()
            && commit.reclaim_receipt().is_none());
        let authority = RegistrationPredecessorAuthority::MergeConcurrent(
            &verified_commit.predecessor_membership,
        );
        let accepted_predecessor = VerifiedAcceptedPredecessor::MergeHistory {
            commits: &verified_history.commits,
            frontier: commit_predecessor_references(&commit),
        };
        let registrations = Box::pin(load_commit_registrations(
            storage,
            root,
            &commit,
            &author,
            carries_lifecycle.then_some(&authority),
            Some(&accepted_predecessor),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let (authorized_predecessor, recovery_author) =
            predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_predecessor,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Database(
                "device join bootstrap commit author is inactive at its predecessor".to_string(),
            ));
        }
        let resolver = DeviceStateResolver::Loaded {
            genesis: &genesis,
            states: &states,
        };
        let device_operations = load_commit_device_operations(
            Some(&resolver),
            storage,
            root,
            &commit,
            &authorized_predecessor,
            carries_lifecycle.then_some(&authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        if matches!(
            commit.control(),
            Some(super::store_commit::StoreControl::MergeMembership { .. })
        ) {
            verify_merge_membership_control(storage, root, &reference, &commit)
                .await
                .map_err(StorePullError::Database)?;
        }
        let owner_recovery =
            verify_commit_owner_recovery_activation(storage, root, &commit, None).await?;
        let activation = match &commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                let (_, head_ref) = super::store_outbound::exact_next_announcement_slot(
                    storage,
                    root,
                    &commit.author_registration,
                    &author,
                    Some(&reference),
                )
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                let head_ref = head_ref.ok_or_else(|| {
                    StorePullError::Database(
                        "Merge bootstrap commit has no exact accepted activation head".to_string(),
                    )
                })?;
                let head = super::store_objects::load_head_ref(
                    storage,
                    root.store_root_hash,
                    &head_ref,
                    &author,
                    &reference,
                )
                .await?;
                DeviceJoinBootstrapActivation::MergeConcurrent {
                    head: head.value,
                    object: head.object,
                    history_summary: verified_commit.history.summary.clone(),
                }
            }
            super::store_commit::StoreCommitOrder::Serial { .. } => {
                DeviceJoinBootstrapActivation::Serial
            }
        };
        let state = device_operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = apply_verified_device_lifecycle(
            state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        states.insert(reference.clone(), state);
        ordered.push(DeviceJoinBootstrapCommit {
            reference,
            commit,
            registrations,
            device_operations,
            activation,
        });
    }

    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        coverage: coverage.clone(),
        commits: ordered,
    })
}

async fn prepare_serial_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    verified_authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    let StoreHistoryCut::Serial(coverage_position) = coverage else {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received a Merge history cut".to_string(),
        ));
    };
    let DeviceJoinBootstrapAuthorization::Serial {
        state: verified_state,
        position: verified_position,
        authorization: verified_authorization,
    } = verified_authorization
    else {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received Merge membership authority".to_string(),
        ));
    };
    if verified_position != coverage_position {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap cut differs from its verified membership position"
                .to_string(),
        ));
    }

    let (authorized, _, _) = Box::pin(load_authorized_serial_prefix(
        storage,
        root,
        Some(attempt_activation.clone()),
    ))
    .await?;
    let activation = authorized.last().ok_or_else(|| {
        StorePullError::Serial(
            "device join attempt activation is absent from its Serial history".to_string(),
        )
    })?;
    if activation.commit_ref != *attempt_activation
        || activation
            .commit
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Serial(error.to_string()))?
            != *coverage
    {
        return Err(StorePullError::Serial(
            "device join attempt activation predecessor differs from its signed bootstrap cut"
                .to_string(),
        ));
    }
    if &activation.commit.membership_state != verified_state
        || &activation.authorization_before != verified_authorization
    {
        return Err(StorePullError::Serial(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let verified_root = load_store_protocol_root(storage, root).await?.value;
    let founder = load_founder_registration_with_root(storage, root, &verified_root).await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_reference.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let commits = authorized
        .into_iter()
        .map(|authorized| DeviceJoinBootstrapCommit {
            reference: authorized.commit_ref,
            commit: authorized.commit,
            registrations: authorized.registrations,
            device_operations: authorized.device_operations,
            activation: DeviceJoinBootstrapActivation::Serial,
        })
        .collect();
    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        coverage: coverage.clone(),
        commits,
    })
}

pub(crate) async fn materialize_device_join_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
    expected_outcome: &super::store_commit::DeviceJoinOutcomeRef,
    authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<(), StorePullError> {
    let (stream_id, sequence) = match reference.coord {
        StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } => (stream_id.to_string(), sequence),
        StoreCommitCoord::Serial { sequence } => (SERIAL_STREAM_ID.to_string(), sequence),
    };
    if let Some(materialized) = db.exact_materialized_ref(&stream_id, sequence).await? {
        if materialized == *reference {
            return Ok(());
        }
        return Err(StorePullError::Database(format!(
            "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
        )));
    }
    let (commit, author) = Box::pin(load_commit_with_author(storage, root, reference)).await?;
    if commit.device_join_outcomes() != std::slice::from_ref(expected_outcome)
        || !commit.device_join_attempt_decisions().is_empty()
        || !commit.device_join_cleanup_receipts().is_empty()
        || commit.device_registrations().len() != 1
        || !commit.provider_access_grants().is_empty()
        || !commit.provider_access_withdrawals().is_empty()
        || !commit.device_retirements().is_empty()
        || !commit.circle_controls().is_empty()
        || !commit.circle_packages().is_empty()
        || commit.store_package().is_some()
        || commit.reclaim_authorization().is_some()
        || commit.reclaim_receipt().is_some()
        || commit.control().is_some()
    {
        return Err(StorePullError::Database(
            "device join activation commit carries unrelated operations".to_string(),
        ));
    }
    let authority = match authorization {
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
    let accepted_predecessor = VerifiedAcceptedPredecessor::Exact;
    let registrations = Box::pin(load_commit_registrations(
        storage,
        root,
        &commit,
        &author,
        Some(&authority),
        Some(&accepted_predecessor),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    let author_authorized = match authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            membership_authorizes(Some(chain), &commit, &author)
        }
        DeviceJoinBootstrapAuthorization::Serial { authorization, .. } => {
            authorization.membership.can_write(&author.author_pubkey)
        }
    };
    if !author_authorized {
        return Err(StorePullError::Database(
            "device join activation author is not authorized by its exact predecessor membership"
                .to_string(),
        ));
    }
    enum Materialization {
        MergeConcurrent {
            head: StoreDeviceHead,
            object: ExactObjectRef,
            history_summary: RetainedVerifiedMergeHistorySummary,
        },
        Serial(SerialAuthorizationState),
    }
    let activation = match (&reference.coord, authorization) {
        (
            StoreCommitCoord::MergeConcurrent { .. },
            DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. },
        ) => {
            let (_, head_ref) = super::store_outbound::exact_next_announcement_slot(
                storage,
                root,
                &commit.author_registration,
                &author,
                Some(reference),
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            let head_ref = head_ref.ok_or_else(|| {
                StorePullError::Database(
                    "device join activation has no exact accepted activation head".to_string(),
                )
            })?;
            let head = super::store_objects::load_head_ref(
                storage,
                root.store_root_hash,
                &head_ref,
                &author,
                reference,
            )
            .await?;
            let (_, predecessor_state) = db.store_device_state_for_order(&commit.order).await?;
            let (authorized_predecessor, recovery_author) =
                predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                    .map_err(|error| StorePullError::Database(error.to_string()))?;
            let device_operations = VerifiedStoreDeviceOperations::without_exclusions(&commit)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let state_after = device_operations
                .apply_to(authorized_predecessor, &commit.device_state)
                .and_then(|state| {
                    apply_verified_device_lifecycle(
                        state,
                        &commit,
                        &registrations,
                        recovery_author.as_ref(),
                        None,
                    )
                })
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let history = prepare_merge_history_successor(
                db,
                root,
                &commit,
                reference,
                chain,
                &author,
                recovery_author.as_ref(),
                state_after.clone(),
                MergeHistorySuccessorEvidence {
                    registrations: commit
                        .device_registrations()
                        .iter()
                        .zip(&registrations)
                        .map(|(activation, (value, _))| RetainedVerifiedRegistration {
                            reference: activation.registration.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await?;
            history
                .summary
                .open(&commit, reference, &head.value, &head_ref, &state_after)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            Materialization::MergeConcurrent {
                head: head.value,
                object: head.object,
                history_summary: history.summary,
            }
        }
        (
            StoreCommitCoord::Serial { .. },
            DeviceJoinBootstrapAuthorization::Serial { authorization, .. },
        ) => Materialization::Serial(authorization.clone()),
        _ => {
            return Err(StorePullError::Database(
                "device join activation authority differs from commit policy".to_string(),
            ));
        }
    };
    let root = root.clone();
    let commit_ref = reference.clone();
    let expected_ref = reference.clone();
    db.call(move |connection| {
        let tx = connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        if let Some(materialized) =
            Database::materialized_commit_ref_on(&tx, &stream_id, sequence)?
        {
            if materialized != expected_ref {
                return Err(DbError::Message(format!(
                    "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                )));
            }
            tx.commit().map_err(DbError::from)?;
            return Ok(());
        }
        Database::record_activated_store_device_registrations_on(
            &tx,
            &commit,
            &registrations,
        )?;
        match activation {
            Materialization::MergeConcurrent {
                head,
                object,
                history_summary,
            } => {
                Database::record_materialized_merge_commit_on(
                    &tx,
                    &root,
                    &commit,
                    &commit_ref,
                    &registrations,
                    &head,
                    &object,
                    &history_summary,
                    &[],
                    None,
                )?;
            }
            Materialization::Serial(authorization) => {
                Database::record_materialized_serial_commit_on(
                    &tx,
                    &commit,
                    &commit_ref,
                    &authorization,
                )?;
            }
        }
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}

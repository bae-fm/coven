use super::*;
use crate::sync::store_objects::load_founder_registration;

pub(in crate::sync::store_engine) async fn prepare_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    membership_state: &StoreMembershipStateRef,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    if !matches!(coverage, StoreHistoryCut::MergeConcurrent(_))
        || !matches!(
            attempt_activation.coord,
            StoreCommitCoord::MergeConcurrent { .. }
        )
    {
        return Err(StorePullError::Database(
            "Merge device join bootstrap received Serial history".to_string(),
        ));
    }
    load_device_join_authorization(storage, root, membership_state).await?;
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
    if &verified_activation.commit.membership_state != membership_state {
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
        let predecessor_state = verified_merge_predecessor_state(&genesis, &states, &commit)?;
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
            activation: DeviceJoinBootstrapActivation::MergeConcurrent {
                head: head.value,
                object: head.object,
                history_summary: verified_commit.history.summary.clone(),
            },
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

pub(in crate::sync::store_engine) async fn materialize_device_join_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
    expected_outcome: &super::store_commit::DeviceJoinOutcomeRef,
    membership_state: &StoreMembershipStateRef,
) -> Result<(), StorePullError> {
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = reference.coord
    else {
        return Err(StorePullError::Database(
            "Merge device join materialization received a Serial commit".to_string(),
        ));
    };
    let stream_id = stream_id.to_string();
    if let Some(materialized) = db.exact_materialized_ref(&stream_id, sequence).await? {
        if materialized == *reference {
            return Ok(());
        }
        return Err(StorePullError::Database(format!(
            "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
        )));
    }
    let (commit, author) =
        load_device_join_activation_commit(storage, root, reference, expected_outcome).await?;
    let membership = load_device_join_authorization(storage, root, membership_state).await?;
    let authority = RegistrationPredecessorAuthority::MergeConcurrent(&membership);
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
    if !membership_authorizes(Some(&membership), &commit, &author) {
        return Err(StorePullError::Database(
            "device join activation author is not authorized by its exact predecessor membership"
                .to_string(),
        ));
    }
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
        &membership,
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
    let root = root.clone();
    let commit_ref = reference.clone();
    let expected_ref = reference.clone();
    db.call(move |connection| {
        let tx = connection.unchecked_transaction().map_err(DbError::from)?;
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
        Database::record_materialized_merge_commit_on(
            &tx,
            &root,
            &commit,
            &commit_ref,
            &registrations,
            &head.value,
            &head.object,
            &history.summary,
            &[],
            None,
        )?;
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}

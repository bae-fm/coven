use super::*;

#[cfg(test)]
pub(in crate::sync::store) fn prepare_device_join_bootstrap<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    coverage: &'a StoreHistoryCut,
    attempt_activation: &'a StoreBatchCommitRef,
    membership_state: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, DeviceJoinBootstrapPlan> {
    Box::pin(async move {
        let mut history_verifier = MergeHistoryVerifier::new(storage, root).await?;
        prepare_device_join_bootstrap_with_history(
            &mut history_verifier,
            coverage,
            attempt_activation,
            membership_state,
        )
        .await
    })
}

async fn prepare_device_join_bootstrap_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    membership_state: &StoreMembershipStateRef,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    load_merge_predecessor_membership_with_history(history_verifier, membership_state)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
    let founder = Box::pin(load_founder_registration_with_root(
        history_verifier.storage(),
        history_verifier.root(),
        history_verifier.verified_root(),
    ))
    .await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = history_verifier.history().genesis.clone();

    let mut pending = history_cut_references(coverage);
    pending.push(attempt_activation.clone());
    history_verifier.verify_refs(pending).await?;
    let activation = history_verifier
        .history()
        .commits
        .get(attempt_activation)
        .ok_or_else(|| {
            StorePullError::Database(
                "device join attempt activation is absent from its graph".into(),
            )
        })?;
    if activation
        .verified
        .value()
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
    if &activation.verified.value().membership_state != membership_state {
        return Err(StorePullError::Database(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let history = history_verifier.history();
    let mut emitted = BTreeSet::new();
    let mut ordered = Vec::with_capacity(history.commits.len());
    while emitted.len() != history.commits.len() {
        let next = history.commits.iter().find_map(|(reference, verified)| {
            (!emitted.contains(reference)
                && commit_predecessor_references(verified.verified.value())
                    .iter()
                    .all(|dependency| emitted.contains(dependency)))
            .then(|| reference.clone())
        });
        let Some(reference) = next else {
            return Err(StorePullError::Database(
                "verified device join bootstrap history has an unresolved predecessor".to_string(),
            ));
        };
        let verified = &history.commits[&reference];
        ordered.push(DeviceJoinBootstrapCommit {
            reference: reference.clone(),
            commit: verified.verified.clone(),
            registrations: verified.registrations.clone(),
            device_operations: verified.operations.clone(),
            activation: DeviceJoinBootstrapActivation {
                head: verified.activation_head.clone(),
                object: verified.activation_head_object.clone(),
                history_summary: verified.history.summary.clone(),
            },
        });
        emitted.insert(reference);
    }

    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        commits: ordered,
    })
}

pub(in crate::sync::store) fn verify_attempt_and_prepare_device_join_bootstrap<'a>(
    history_verifier: &'a mut MergeHistoryVerifier<'_>,
    attempt: &'a super::store_commit::DeviceJoinAttemptRef,
    attempt_owner: &'a StoreDeviceRegistration,
    attempt_activation: &'a StoreBatchCommitRef,
) -> StorePullFuture<
    'a,
    (
        super::store_objects::VerifiedObject<super::store_commit::DeviceJoinAttempt>,
        DeviceJoinBootstrapPlan,
    ),
> {
    Box::pin(async move {
        let storage = history_verifier.storage();
        let root = history_verifier.root().clone();
        let evidence = load_device_join_attempt_evidence_ref_with_root(
            storage,
            &root,
            history_verifier.verified_root_object(),
            attempt,
            attempt_owner,
        )
        .await?;
        let verified_attempt =
            super::device_join_attempt::verify_device_join_attempt_evidence_with_history(
                history_verifier,
                evidence,
            )
            .await?;
        let plan = prepare_device_join_bootstrap_with_history(
            history_verifier,
            &verified_attempt.value.bootstrap_cut,
            attempt_activation,
            &verified_attempt.value.membership,
        )
        .await?;
        Ok((verified_attempt, plan))
    })
}

pub(in crate::sync::store) fn materialize_device_join_activation<'a>(
    database: &'a StoreDatabase,
    history_verifier: &'a mut MergeHistoryVerifier<'_>,
    reference: &'a StoreBatchCommitRef,
    expected_outcome: &'a super::store_commit::DeviceJoinOutcomeRef,
    membership_state: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, ()> {
    Box::pin(async move {
        let storage = history_verifier.storage();
        let root = history_verifier.root().clone();
        let db = database.sqlite();
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = reference.coord;
        let stream_id = stream_id.to_string();
        if let Some(materialized) = database
            .exact_materialized_ref(&stream_id, sequence)
            .await?
        {
            if materialized == *reference {
                return Ok(());
            }
            return Err(StorePullError::Database(format!(
                "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
            )));
        }
        let verified_commit = history_verifier.load_ref(reference).await?;
        let commit = verified_commit.value().clone();
        let author = verified_commit.author().clone();
        verify_device_join_activation_commit(&commit, expected_outcome)?;
        if &commit.membership_state != membership_state {
            return Err(StorePullError::Database(
                "device join activation differs from its expected Merge membership state"
                    .to_string(),
            ));
        }
        let predecessor_cut = commit
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let frontier = predecessor_cut.0;
        let accepted_history = super::snapshot_authority::verify_merge_history_authority(
            history_verifier,
            &frontier,
            &commit.membership_state,
        )
        .await?;
        let membership = accepted_history.membership.clone();
        let accepted_frontier = commit_predecessor_references(&commit);
        let registrations = Box::pin(load_merge_commit_registrations(
            history_verifier,
            &commit,
            &author,
            &membership,
            super::device_join_attempt::VerifiedMergePredecessorHistory {
                commits: &history_verifier.history().commits,
                frontier: &accepted_frontier,
            },
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
        let (_, head_ref) = crate::sync::store::operations::exact_next_announcement_slot(
            storage,
            &root,
            &commit.author_registration,
            &author,
            history_verifier.commit_verifier(),
            Some(&verified_commit),
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
        let (_, predecessor_state) = database.store_device_state_for_order(&commit.order).await?;
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
            database,
            &root,
            &verified_commit,
            &membership,
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
        let commit_ref = reference.clone();
        let expected_ref = reference.clone();
        db.call(move |connection| {
            let tx = connection.unchecked_transaction().map_err(DbError::from)?;
            if let Some(materialized) =
                crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(&tx, &stream_id, sequence)?
            {
                if materialized != expected_ref {
                    return Err(DbError::Message(format!(
                        "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                    )));
                }
                tx.commit().map_err(DbError::from)?;
                return Ok(());
            }
            crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
                &tx,
                &commit,
                &registrations,
            )?;
            let circle_activations = VerifiedCircleActivations::none(&commit, &commit_ref)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let materialization = VerifiedMergeMaterialization::verify(
                &root,
                &verified_commit,
                &registrations,
                &device_operations,
                &circle_activations,
                &head.value,
                &head.object,
                &history.summary,
                None,
                &[],
                None,
            )?;
            crate::sync::store::database::StoreDatabaseTransaction::new(&tx)
                .record_verified_merge_materialization(materialization)?;
            tx.commit().map_err(DbError::from)
        })
        .await?;
        Ok(())
    })
}

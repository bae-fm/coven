use std::collections::BTreeSet;

use super::{MergeMaterializationTransaction, StoreDatabase};
use crate::{
    install_store_founder_state_on, required_store_root_authority_on, DbError,
    VerifiedMergeMaterialization,
};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, StoreDeviceHead, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};

impl StoreDatabase {
    pub async fn apply_received_merge_materialization(
        &self,
        materialization: crate::PreparedMergeMaterialization,
        retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<coven_protocol::membership::ApplyOutcome, DbError> {
        let root = materialization.root.clone();
        let blob_decls = self.blob_decls();
        let gates = self.gates();
        let synced_tables = self.synced_tables().to_vec();
        let activates_circle_epoch_cutoff = materialization
            .circle_activations
            .circles()
            .iter()
            .any(|activation| {
                activation
                    .control
                    .value
                    .active_epoch()
                    .is_some_and(|epoch| {
                        matches!(
                            &epoch.common.origin,
                            coven_protocol::circle::CircleEpochOrigin::Closed { .. }
                        )
                    })
            });
        let installs_circle_bootstrap = !materialization.circle_activations.bootstraps().is_empty();
        let local_exclusions = materialization
            .circle_activations
            .local_exclusions()
            .to_vec();
        #[cfg(any(test, feature = "test-utils"))]
        let materialization_failure = self.merge_materialization_failure_injection();
        let applied = self
            .with_retained_merge_materializations(move |conn, retained_cache| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let materialized_frontier =
                    coven_protocol::store_commit::CommitFrontier::from_refs(
                        Self::materialized_frontier_on(&tx, None)?,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let candidate_predecessors = materialization
                    .verified_commit
                    .value()
                    .order
                    .predecessor_cut()
                    .map_err(|error| DbError::Message(error.to_string()))?
                    .frontier();
                let requires_canonical_replay =
                    !candidate_predecessors.covers(&materialized_frontier);
                let merge_transaction = MergeMaterializationTransaction::new(&tx);
                let mut applied = merge_transaction.apply_prepared_merge_materialization(
                    &blob_decls,
                    &gates,
                    &synced_tables,
                    routing_key.as_ref(),
                    local_store_membership,
                    crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
                    None,
                    materialization,
                )?;
                if matches!(
                    applied.outcome,
                    coven_protocol::membership::ApplyOutcome::Applied(_)
                ) {
                    let mut transaction_cache = retained_cache.clone();
                    let retained = applied.retained.take().ok_or_else(|| {
                        DbError::Message(
                            "applied Merge materialization omitted its verified retained input"
                                .to_string(),
                        )
                    })?;
                    transaction_cache.insert_verified(retained)?;
                    #[cfg(any(test, feature = "test-utils"))]
                    if materialization_failure
                        .reach(crate::MergeMaterializationFailurePoint::SummaryMaterialization)?
                    {
                        return Err(DbError::Message(
                            "injected failure after Merge summary materialization".to_string(),
                        ));
                    }
                    for exclusion in &local_exclusions {
                        Self::record_circle_close_exclusion_on(&tx, exclusion)?;
                    }
                    let retracted = retractions
                        .iter()
                        .map(|retraction| {
                            retraction
                                .candidate_reference()
                                .map_err(|error| DbError::Message(error.to_string()))
                        })
                        .collect::<Result<BTreeSet<_>, _>>()?;
                    if !retractions.is_empty() {
                        applied.write_status_notifications = merge_transaction
                            .retract_verified_merge_materializations(
                                &root,
                                &mut transaction_cache,
                                retractions,
                            )?;
                        #[cfg(any(test, feature = "test-utils"))]
                        if materialization_failure
                            .reach(crate::MergeMaterializationFailurePoint::RetractionDeletion)?
                        {
                            return Err(DbError::Message(
                                "injected failure after Merge retraction deletion".to_string(),
                            ));
                        }
                    }
                    if requires_canonical_replay
                        || activates_circle_epoch_cutoff
                        || installs_circle_bootstrap
                        || !retracted.is_empty()
                    {
                        let replay = transaction_cache.replay_projection_on(
                            &tx,
                            &root,
                            &blob_decls,
                            &gates,
                            &synced_tables,
                            routing_key.as_ref(),
                            &retracted,
                            None,
                            true,
                            local_store_membership,
                        )?;
                        let mut host_changes =
                            rusqlite::session::Session::new(&tx).map_err(DbError::from)?;
                        for table in &synced_tables {
                            host_changes
                                .attach(Some(table.name()))
                                .map_err(DbError::from)?;
                        }
                        let mut tables = crate::projection_table_names(gates.has_scoped_graph());
                        tables.extend(synced_tables.iter().map(|table| table.name().to_string()));
                        tables.sort();
                        tables.dedup();
                        tx.pragma_update(None, "defer_foreign_keys", "ON")
                            .map_err(DbError::from)?;
                        for table in tables.iter().rev() {
                            tx.execute_batch(&format!("DELETE FROM {}", crate::quote_ident(table)))
                                .map_err(DbError::from)?;
                        }
                        for table in &tables {
                            crate::copy_table_with_conflicts(&replay, &tx, table, false)?;
                        }
                        let violations: bool = tx
                            .query_row(
                                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                                [],
                                |row| row.get(0),
                            )
                            .map_err(DbError::from)?;
                        if violations {
                            let violation: (String, Option<i64>, String, i64) = tx
                                .query_row(
                                    "SELECT * FROM pragma_foreign_key_check LIMIT 1",
                                    [],
                                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                                )
                                .map_err(DbError::from)?;
                            return Err(DbError::Message(format!(
                                "retained replay projection violates foreign keys: {violation:?}"
                            )));
                        }
                        let mut projection_changeset = Vec::new();
                        host_changes
                            .changeset_strm(&mut projection_changeset)
                            .map_err(DbError::from)?;
                        #[cfg(any(test, feature = "test-utils"))]
                        if materialization_failure
                            .reach(crate::MergeMaterializationFailurePoint::ProjectionReplacement)?
                        {
                            return Err(DbError::Message(
                                "injected failure after Merge projection replacement".to_string(),
                            ));
                        }
                        merge_transaction.replace_store_device_exclusion_freezes_from_replay()?;
                        let old_projection = crate::walk_old_changeset(&projection_changeset)
                            .map_err(DbError::Message)?;
                        let new_projection = crate::walk_changeset(&projection_changeset)
                            .map_err(DbError::Message)?;
                        for intent in crate::local_blob_cleanup_intents::intents_from_changes(
                            &blob_decls,
                            &old_projection,
                            &new_projection,
                        )
                        .map_err(|error| DbError::Message(error.to_string()))?
                        {
                            super::local_blob_cleanup::record_obsolete_copy_intents_on(
                                &tx,
                                &blob_decls,
                                &intent,
                            )?;
                        }
                        if let coven_protocol::membership::ApplyOutcome::Applied(rows) =
                            &mut applied.outcome
                        {
                            rows.extend(new_projection);
                        }
                    }
                    tx.commit().map_err(DbError::from)?;
                    *retained_cache = transaction_cache;
                }
                Ok(applied)
            })
            .await?;
        if let Some(max_applied) = applied.max_updated_at.as_ref() {
            self.hlc().advance_past(max_applied);
        }
        for (write_id, status) in applied.write_status_notifications {
            self.notify_write_status(write_id, status);
        }
        Ok(applied.outcome)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_published_store_operation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_summary: coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<(), DbError> {
        let reference = verified_commit.reference().clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let store_transaction = MergeMaterializationTransaction::new(&tx);
                if let Some(object_ids) = operation_object_ids {
                    store_transaction
                        .activate_store_operation_remote_objects(&reference, &object_ids)?;
                }
                if !registrations.is_empty() {
                    super::record_activated_store_device_registrations_on(
                        &tx,
                        verified_commit.value(),
                        &registrations,
                    )?;
                }
                let materialization = VerifiedMergeMaterialization::verify(
                    &root,
                    &verified_commit,
                    &registrations,
                    &device_operations,
                    &circle_activations,
                    &activation_head,
                    &activation_head_object,
                    &history_summary,
                    membership_objects.as_ref(),
                    &[],
                    None,
                )?;
                if let Some(completion) = membership_completion {
                    store_transaction
                        .complete_membership_journal(completion, &reference)
                        .map_err(|error| {
                            DbError::context("complete exact membership journal", error)
                        })?;
                }
                store_transaction
                    .record_verified_merge_materialization(materialization)
                    .map_err(|error| {
                        DbError::context("record exact Merge materialization", error)
                    })?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn materialize_device_join_activation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_summary: coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
    ) -> Result<(), DbError> {
        let expected_ref = verified_commit.reference().clone();
        let stream_id = expected_ref.coord.stream_id.to_string();
        let sequence = expected_ref.coord.sequence();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                if let Some(materialized) =
                    StoreDatabase::materialized_commit_ref_on(&tx, &stream_id, sequence)?
                {
                    if materialized != expected_ref {
                        return Err(DbError::Message(format!(
                            "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                        )));
                    }
                    tx.commit().map_err(DbError::from)?;
                    return Ok(());
                }
                super::record_activated_store_device_registrations_on(
                    &tx,
                    verified_commit.value(),
                    &registrations,
                )?;
                let circle_activations = VerifiedCircleActivations::none(
                    verified_commit.value(),
                    verified_commit.reference(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                let materialization = VerifiedMergeMaterialization::verify(
                    &root,
                    &verified_commit,
                    &registrations,
                    &device_operations,
                    &circle_activations,
                    &activation_head,
                    &activation_head_object,
                    &history_summary,
                    None,
                    &[],
                    None,
                )?;
                MergeMaterializationTransaction::new(&tx)
                    .record_verified_merge_materialization(materialization)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn install_device_join_bootstrap(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        self.connection.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let installed_root = required_store_root_authority_on(&tx)?;
            if installed_root != root || plan.founder.store_root != root {
                return Err(DbError::Message(
                    "device join bootstrap root differs from the installed exact root".to_string(),
                ));
            }
            install_store_founder_state_on(
                &tx,
                &root,
                &plan.founder_reference,
                &plan.founder,
                &plan.founder_bytes,
                &plan.genesis,
            )?;
            crate::set_protocol_state_on(
                &tx,
                coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
                &plan.founder.author_pubkey,
            )?;
            plan.membership.install_on(&tx)?;

            let frontier = crate::StoreDatabase::materialized_frontier_on(&tx, None)?;
            let mut represented = BTreeSet::new();
            for prepared in &plan.commits {
                let stream_id = prepared.reference.coord.stream_id.to_string();
                let sequence = prepared.reference.coord.sequence();
                if let Some(existing) =
                    crate::StoreDatabase::materialized_commit_ref_on(&tx, &stream_id, sequence)?
                {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{sequence}"
                        )));
                    }
                    represented.insert(prepared.reference.clone());
                    continue;
                }
                let encoded = serde_json::to_string(&prepared.reference).map_err(|error| {
                    DbError::context("serialize device join bootstrap commit ref", error)
                })?;
                let has_snapshot_state = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM store_device_state_snapshots
                         WHERE commit_ref = ?1)",
                        [&encoded],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(DbError::from)?;
                let covered = frontier
                    .get(&stream_id)
                    .is_some_and(|tip| sequence <= tip.coord.sequence());
                if has_snapshot_state && covered {
                    represented.insert(prepared.reference.clone());
                }
            }

            for prepared in &plan.commits {
                let commit = prepared.commit.value();
                if !represented.contains(&prepared.reference)
                    && (commit.store_package().is_some()
                        || !commit.circle_packages().is_empty())
                {
                    return Err(DbError::Message(format!(
                        "device join bootstrap cannot advance over unmaterialized row data at {}/{}",
                        prepared.reference.coord.stream_id,
                        prepared.reference.coord.sequence()
                    )));
                }
            }

            for prepared in plan.commits {
                if represented.contains(&prepared.reference) {
                    continue;
                }
                let stream_id = prepared.reference.coord.stream_id.to_string();
                if let Some(existing) = crate::StoreDatabase::materialized_commit_ref_on(
                    &tx,
                    &stream_id,
                    prepared.reference.coord.sequence(),
                )? {
                    if existing != prepared.reference {
                        return Err(DbError::Message(format!(
                            "device join bootstrap conflicts at {stream_id}/{}",
                            prepared.reference.coord.sequence()
                        )));
                    }
                    continue;
                }
                let commit = prepared.commit.value();
                super::record_activated_store_device_registrations_on(
                    &tx,
                    commit,
                    &prepared.registrations,
                )?;
                let circle_activations =
                    VerifiedCircleActivations::none(commit, &prepared.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?;
                let activation = &prepared.activation;
                let materialization = VerifiedMergeMaterialization::verify(
                    &root,
                    &prepared.commit,
                    &prepared.registrations,
                    &prepared.device_operations,
                    &circle_activations,
                    &activation.head,
                    &activation.object,
                    &activation.history_summary,
                    None,
                    &[],
                    None,
                )?;
                MergeMaterializationTransaction::new(&tx)
                    .record_verified_merge_materialization(materialization)?;
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub async fn complete_owner_recovery(
        &self,
        verified_commit: VerifiedStoreBatchCommit,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_summary: coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        registration: ActivatedStoreDeviceRegistration,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let root = required_store_root_authority_on(&tx)?;
                let registrations = vec![registration];
                let commit = verified_commit.value();
                super::record_activated_store_device_registrations_on(&tx, commit, &registrations)?;
                MergeMaterializationTransaction::new(&tx).record_materialized_merge_commit(
                    &root,
                    &verified_commit,
                    &registrations,
                    &activation_head,
                    &activation_head_object,
                    &history_summary,
                    &[],
                    None,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}

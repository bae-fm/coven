use std::collections::BTreeSet;

use super::{
    MergeMaterializationTransaction, StoreDatabase, StoreSession, StoreTransactionOutcome,
    VerifiedStoreTransaction,
};
use crate::{install_store_founder_state_on, DbError, VerifiedMergeMaterialization};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, StoreDeviceHead, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};

#[cfg(any(test, feature = "test-utils"))]
fn reach_materialization_failure(
    armed: &std::sync::Mutex<Option<crate::MergeMaterializationFailurePoint>>,
    point: crate::MergeMaterializationFailurePoint,
) -> Result<bool, DbError> {
    let mut armed = armed
        .lock()
        .map_err(|_| DbError::Message("Merge materialization failure lock poisoned".to_string()))?;
    if armed.as_ref() != Some(&point) {
        return Ok(false);
    }
    armed.take();
    Ok(true)
}

impl VerifiedStoreTransaction<'_, '_, '_> {
    fn apply_received_merge_materialization(
        &mut self,
        materialization: crate::PreparedMergeMaterialization,
        retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<super::merge_materialization_transaction::AppliedMergeMaterialization, DbError>
    {
        #[cfg(any(test, feature = "test-utils"))]
        let materialization_failure = self.merge_materialization_failure;
        let authority = &mut *self.authority;
        let blob_decls = self.blob_decls;
        let gates = self.gates;
        let synced_tables = self.synced_tables;
        let root = materialization.root.clone();
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
        let tx = self.store.transaction;
        let materialized_frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(tx, None)?,
        )
        .map_err(DbError::from)?;
        let candidate_predecessors = materialization
            .verified_commit
            .value()
            .order
            .predecessor_cut()
            .map_err(DbError::from)?
            .frontier();
        let requires_canonical_replay = !candidate_predecessors.covers(&materialized_frontier);
        let merge_transaction = MergeMaterializationTransaction::from_store(self.store);
        let mut applied = merge_transaction.apply_prepared_merge_materialization(
            authority,
            blob_decls,
            gates,
            synced_tables,
            routing_key.as_ref(),
            local_store_membership,
            crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
            None,
            materialization,
        )?;
        if matches!(applied.outcome, crate::MaterializationOutcome::Applied(_)) {
            let retained = applied.retained.take().ok_or_else(|| {
                DbError::Message(
                    "applied Merge materialization omitted its verified retained input".to_string(),
                )
            })?;
            authority.insert_verified(retained)?;
            #[cfg(any(test, feature = "test-utils"))]
            if reach_materialization_failure(
                materialization_failure,
                crate::MergeMaterializationFailurePoint::SummaryMaterialization,
            )? {
                return Err(DbError::Message(
                    "injected failure after Merge summary materialization".to_string(),
                ));
            }
            for exclusion in &local_exclusions {
                super::circle_operations::record_circle_close_exclusion_on(tx, exclusion)?;
            }
            let retracted = retractions
                .iter()
                .map(|retraction| retraction.candidate_reference().map_err(DbError::from))
                .collect::<Result<BTreeSet<_>, _>>()?;
            if !retractions.is_empty() {
                applied.write_status_notifications =
                    super::merge_materialization_transaction::retract_verified_merge_materializations(
                        &merge_transaction,
                        &root,
                        authority,
                        retractions,
                    )?;
                #[cfg(any(test, feature = "test-utils"))]
                if reach_materialization_failure(
                    materialization_failure,
                    crate::MergeMaterializationFailurePoint::RetractionDeletion,
                )? {
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
                let replay = authority.replay_projection_on(
                    crate::store::store_session::StoreTransaction::new(tx, self.store.store_dir),
                    blob_decls,
                    gates,
                    synced_tables,
                    routing_key.as_ref(),
                    &retracted,
                    None,
                    true,
                    local_store_membership,
                )?;
                let mut host_changes =
                    rusqlite::session::Session::new(tx).map_err(DbError::from)?;
                for table in synced_tables {
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
                crate::store::store_session::StoreTransaction::new(tx, self.store.store_dir)
                    .replace_tables_from_projection(&replay, &tables)?;
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
                if reach_materialization_failure(
                    materialization_failure,
                    crate::MergeMaterializationFailurePoint::ProjectionReplacement,
                )? {
                    return Err(DbError::Message(
                        "injected failure after Merge projection replacement".to_string(),
                    ));
                }
                merge_transaction.replace_store_device_exclusion_freezes_from_replay(&root)?;
                let old_projection =
                    crate::walk_old_changeset(&projection_changeset).map_err(DbError::Changeset)?;
                let new_projection =
                    crate::walk_changeset(&projection_changeset).map_err(DbError::Changeset)?;
                for intent in crate::local_blob_cleanup_intents::intents_from_changes(
                    blob_decls,
                    &old_projection,
                    &new_projection,
                )
                .map_err(DbError::from)?
                {
                    super::local_blob_cleanup::record_obsolete_copy_intents_on(
                        tx, blob_decls, &intent,
                    )?;
                }
                if let crate::MaterializationOutcome::Applied(rows) = &mut applied.outcome {
                    rows.extend(new_projection);
                }
            }
        }
        Ok(applied)
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_published_store_operation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<(), DbError> {
        let reference = verified_commit.reference().clone();
        let tx = self.store.transaction;
        let authority = &mut *self.authority;
        let store_transaction = MergeMaterializationTransaction::from_store(self.store);
        if let Some(object_ids) = operation_object_ids {
            store_transaction.activate_store_operation_remote_objects(&reference, &object_ids)?;
        }
        if !registrations.is_empty() {
            super::record_activated_store_device_registrations_on(
                tx,
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
            &history_evidence,
            membership_objects.as_ref(),
            &[],
            None,
        )?;
        if let Some(completion) = membership_completion {
            store_transaction
                .complete_membership_journal(completion, &reference)
                .map_err(|error| DbError::context("complete exact membership journal", error))?;
        }
        let retained = store_transaction
            .record_verified_merge_materialization(authority, materialization)
            .map_err(|error| DbError::context("record exact Merge materialization", error))?;
        authority.insert_verified(retained)?;
        Ok(())
    }

    fn materialize_device_join_activation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    ) -> Result<(), DbError> {
        let expected_ref = verified_commit.reference().clone();
        let stream_id = expected_ref.coord.stream_id.to_string();
        let sequence = expected_ref.coord.sequence();
        let tx = self.store.transaction;
        if let Some(materialized) =
            crate::store::materialized_commit_index::materialized_commit_ref_on(
                tx, &stream_id, sequence,
            )?
        {
            if materialized != expected_ref {
                return Err(DbError::Message(format!(
                    "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                )));
            }
            super::record_activated_store_device_registrations_on(
                tx,
                verified_commit.value(),
                &registrations,
            )?;
            return Ok(());
        }
        let authority = &mut *self.authority;
        super::record_activated_store_device_registrations_on(
            tx,
            verified_commit.value(),
            &registrations,
        )?;
        let circle_activations =
            VerifiedCircleActivations::none(verified_commit.value(), verified_commit.reference())
                .map_err(DbError::from)?;
        let materialization = VerifiedMergeMaterialization::verify(
            &root,
            &verified_commit,
            &registrations,
            &device_operations,
            &circle_activations,
            &activation_head,
            &activation_head_object,
            &history_evidence,
            None,
            &[],
            None,
        )?;
        let retained = MergeMaterializationTransaction::from_store(self.store)
            .record_verified_merge_materialization(authority, materialization)?;
        authority.insert_verified(retained)?;
        Ok(())
    }

    fn install_device_join_bootstrap(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        let tx = self.store.transaction;
        let authority = &mut *self.authority;
        let installed_root = authority.root().clone();
        if installed_root != root || plan.founder.store_root != root {
            return Err(DbError::Message(
                "device join bootstrap root differs from the installed exact root".to_string(),
            ));
        }
        install_store_founder_state_on(
            tx,
            &root,
            &plan.founder_reference,
            &plan.founder,
            &plan.founder_bytes,
            &plan.genesis,
        )?;
        crate::set_protocol_state_on(
            tx,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
            &plan.founder.author_pubkey,
        )?;
        plan.membership.install_on(tx)?;

        let frontier = crate::store::materialized_commit_index::materialized_frontier_on(tx, None)?;
        let mut represented = BTreeSet::new();
        for prepared in &plan.commits {
            let stream_id = prepared.reference.coord.stream_id.to_string();
            let sequence = prepared.reference.coord.sequence();
            if let Some(existing) =
                crate::store::materialized_commit_index::materialized_commit_ref_on(
                    tx, &stream_id, sequence,
                )?
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
                && (commit.store_package().is_some() || !commit.circle_packages().is_empty())
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
            if let Some(existing) =
                crate::store::materialized_commit_index::materialized_commit_ref_on(
                    tx,
                    &stream_id,
                    prepared.reference.coord.sequence(),
                )?
            {
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
                tx,
                commit,
                &prepared.registrations,
            )?;
            let circle_activations = VerifiedCircleActivations::none(commit, &prepared.reference)
                .map_err(DbError::from)?;
            let activation = &prepared.activation;
            let materialization = VerifiedMergeMaterialization::verify(
                &root,
                &prepared.commit,
                &prepared.registrations,
                &prepared.device_operations,
                &circle_activations,
                &activation.head,
                &activation.object,
                &activation.history_evidence,
                None,
                &[],
                None,
            )?;
            let retained = MergeMaterializationTransaction::from_store(self.store)
                .record_verified_merge_materialization(authority, materialization)?;
            authority.insert_verified(retained)?;
        }
        Ok(())
    }

    fn complete_owner_recovery(
        &mut self,
        verified_commit: VerifiedStoreBatchCommit,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
    ) -> Result<(), DbError> {
        let tx = self.store.transaction;
        let authority = &mut *self.authority;
        let root = authority.root().clone();
        let registrations = vec![registration];
        let commit = verified_commit.value();
        super::record_activated_store_device_registrations_on(tx, commit, &registrations)?;
        let retained = MergeMaterializationTransaction::from_store(self.store)
            .record_materialized_merge_commit(
                authority,
                &root,
                &verified_commit,
                &registrations,
                &activation_head,
                &activation_head_object,
                &history_evidence,
                &[],
                None,
            )?;
        authority.insert_verified(retained)?;
        super::owner_recovery_publication::complete_owner_recovery_publication_on(
            tx,
            &verified_commit,
            &activation_head,
            &activation_head_object,
        )?;
        #[cfg(any(test, feature = "test-utils"))]
        if reach_materialization_failure(
            self.merge_materialization_failure,
            crate::MergeMaterializationFailurePoint::SummaryMaterialization,
        )? {
            return Err(DbError::Message(
                "injected failure after Merge summary materialization".to_string(),
            ));
        }
        Ok(())
    }
}

impl StoreSession<'_> {
    fn apply_received_merge_materialization(
        &mut self,
        materialization: crate::PreparedMergeMaterialization,
        retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<super::merge_materialization_transaction::AppliedMergeMaterialization, DbError>
    {
        let applied = self.verified_store_transaction(move |transaction| {
            let applied = transaction.apply_received_merge_materialization(
                materialization,
                retractions,
                local_store_membership,
                routing_key,
                receiver_wall_ms,
            )?;
            if matches!(applied.outcome, crate::MaterializationOutcome::Applied(_)) {
                Ok(StoreTransactionOutcome::Commit(applied))
            } else {
                Ok(StoreTransactionOutcome::Rollback(applied))
            }
        })?;
        if let Some(max_applied) = applied.max_updated_at.as_ref() {
            self.hlc.advance_past(max_applied);
        }
        Ok(applied)
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_published_store_operation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                activation_head,
                activation_head_object,
                history_evidence,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }

    fn materialize_device_join_activation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.materialize_device_join_activation(
                root,
                verified_commit,
                registrations,
                device_operations,
                activation_head,
                activation_head_object,
                history_evidence,
            )?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }

    fn install_device_join_bootstrap(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.install_device_join_bootstrap(root, plan)?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }

    fn complete_owner_recovery(
        &mut self,
        verified_commit: VerifiedStoreBatchCommit,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.complete_owner_recovery(
                verified_commit,
                activation_head,
                activation_head_object,
                history_evidence,
                registration,
            )?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }
}

impl StoreDatabase {
    pub async fn apply_received_merge_materialization(
        &self,
        materialization: crate::PreparedMergeMaterialization,
        retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<crate::MaterializationOutcome, DbError> {
        let applied = self
            .call_store(move |session| {
                session.apply_received_merge_materialization(
                    materialization,
                    retractions,
                    local_store_membership,
                    routing_key,
                    receiver_wall_ms,
                )
            })
            .await?;
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
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                activation_head,
                activation_head_object,
                history_evidence,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )
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
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.materialize_device_join_activation(
                root,
                verified_commit,
                registrations,
                device_operations,
                activation_head,
                activation_head_object,
                history_evidence,
            )
        })
        .await
    }

    pub async fn install_device_join_bootstrap(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.install_device_join_bootstrap(root, plan))
            .await
    }

    pub async fn complete_owner_recovery(
        &self,
        verified_commit: VerifiedStoreBatchCommit,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_owner_recovery(
                verified_commit,
                activation_head,
                activation_head_object,
                history_evidence,
                registration,
            )
        })
        .await
    }
}

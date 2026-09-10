use std::collections::BTreeSet;

use super::{
    MergeMaterializationTransaction, StoreDatabase, StoreSession, StoreTransactionOutcome,
    VerifiedStoreTransaction,
};
use crate::{
    install_store_founder_state_on, AcceptedStorePublicationInterval, DbError,
    VerifiedMergeMaterialization,
};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
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

/// The bootstrap commits this database already materializes, which is to say
/// the ones a previous run of this same installation landed before it stopped.
///
/// A plan carries only the history past the installed snapshot's coverage: the
/// device that built it knew which snapshot this device installs and walked
/// forward from that snapshot's tips. So a plan commit at or under a coverage
/// tip is never the ordinary case — it is a plan built against a different
/// history, a fork at that coordinate, or a snapshot that ran past the
/// bootstrap cut. None can be installed over, so the join fails here instead of
/// writing rows against an image that disagrees with them.
fn device_join_bootstrap_represented_on(
    tx: &rusqlite::Transaction<'_>,
    commits: &[crate::DeviceJoinBootstrapCommit],
) -> Result<BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
    let mut represented = BTreeSet::new();
    let coverage = crate::store::materialized_commit_index::snapshot_coverage_on(tx)?;
    for prepared in commits {
        let stream_id = prepared.reference.coord.stream_id.to_string();
        let sequence = prepared.reference.coord.sequence();
        if let Some(existing) = crate::store::materialized_commit_index::materialized_commit_ref_on(
            tx, &stream_id, sequence,
        )? {
            if existing != prepared.reference {
                return Err(DbError::Message(format!(
                    "device join bootstrap conflicts at {stream_id}/{sequence}"
                )));
            }
            represented.insert(prepared.reference.clone());
            continue;
        }
        if coverage
            .get(&stream_id)
            .is_some_and(|tip| sequence <= tip.coord.sequence())
        {
            return Err(DbError::Message(format!(
                "device join bootstrap history at {stream_id}/{sequence} is not the history the \
                 installed snapshot covers"
            )));
        }
    }
    Ok(represented)
}

impl VerifiedStoreTransaction<'_, '_, '_, '_> {
    fn retain_received_merge_materialization(
        &mut self,
        materialization: &crate::PreparedMergeMaterialization,
        receiver_wall_ms: u64,
    ) -> Result<(), DbError> {
        super::clock_floor::observe_circle_metadata(
            &mut self.clock_floor,
            materialization.circle_activations.circles(),
            crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
        )?;
        if !materialization.packages.is_empty()
            && materialization.package_application
                != Some(crate::RetainedPackageApplication::Received { receiver_wall_ms })
        {
            return Err(DbError::Message(
                "received Merge packages carry another application timestamp".to_string(),
            ));
        }
        let merge_transaction = MergeMaterializationTransaction::from_store(self.store);
        merge_transaction
            .record_prepared_materialization_authority(materialization)
            .map_err(|error| DbError::context("received authority records", error))?;
        let retained = merge_transaction
            .retain_prepared_merge_materialization(self.authority, materialization)
            .map_err(|error| {
                DbError::context(
                    format!(
                        "received retained materialization {:?}",
                        materialization.verified_commit.reference()
                    ),
                    error,
                )
            })?;
        self.authority
            .insert_verified(retained)
            .map_err(|error| DbError::context("received authority cache", error))?;
        #[cfg(any(test, feature = "test-utils"))]
        if reach_materialization_failure(
            self.merge_materialization_failure,
            crate::MergeMaterializationFailurePoint::SummaryMaterialization,
        )? {
            return Err(DbError::Message(
                "injected failure after Merge summary materialization".to_string(),
            ));
        }
        for exclusion in materialization.circle_activations.local_exclusions() {
            super::circle_operations::record_circle_close_exclusion_on(
                self.store.transaction,
                exclusion,
            )?;
        }
        Ok(())
    }

    fn replay_received_merge_materializations(
        &mut self,
        candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<super::merge_materialization_transaction::AppliedMergeMaterialization, DbError>
    {
        let replayed = self.authority.replay_projection_watching_on(
            self.store,
            self.blob_decls,
            self.gates,
            self.synced_tables,
            routing_key,
            &BTreeSet::new(),
            crate::ReplayJournal::Owed,
            local_store_membership,
            candidate,
        )?;
        let watched = replayed.watched_outcome().ok_or_else(|| {
            DbError::Message("incoming Merge materialization was not replayed".to_string())
        })?;
        if let super::WatchedReplayOutcome::Held(reason) = watched {
            return Ok(
                super::merge_materialization_transaction::AppliedMergeMaterialization {
                    outcome: crate::MaterializationOutcome::Held(reason),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                },
            );
        }
        let rows = replayed.install_on(self)?;
        let max_updated_at = replayed.max_updated_at();
        if max_updated_at > self.clock_floor {
            self.clock_floor = max_updated_at.clone();
        }
        Ok(
            super::merge_materialization_transaction::AppliedMergeMaterialization {
                outcome: crate::MaterializationOutcome::Applied(rows),
                max_updated_at,
                write_status_notifications: Vec::new(),
            },
        )
    }

    pub(super) fn apply_received_store_publication_interval(
        &mut self,
        materializations: Vec<crate::PreparedMergeMaterialization>,
        accepted: AcceptedStorePublicationInterval,
        replay: coven_protocol::store_commit::VerifiedStorePublicationInterval,
        snapshots: Vec<coven_protocol::store_commit::RetainedReplaySnapshotAuthority>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        schema_version: u32,
        sync_routing_hash: coven_protocol::store_commit::ObjectHash,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
        installed_checkpoint: Option<coven_protocol::store_commit::AcceptedStoreSnapshotRef>,
    ) -> Result<
        (
            super::merge_materialization_transaction::AppliedMergeMaterialization,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        if replay.current() != accepted.interval().current() {
            return Err(DbError::Message(
                "Store replay and observed publication name different accepted tips".to_string(),
            ));
        }
        // Acceptance and materialization become durable together, including the
        // first observation in a newly opened founder database.
        super::observed_store_publication::observe_store_publication_interval_on(
            self.store, &accepted,
        )?;
        let baseline = super::retained_replay::load_replay_baseline_metadata_on(
            super::StoreRecords::new(self.store.transaction, self.store.store_dir),
        )?
        .ok_or_else(|| DbError::Message("Store replay has no installed baseline".to_string()))?;
        let anchor = match &baseline.authority {
            crate::RetainedReplayAuthority::Genesis(authority) => {
                coven_protocol::store_commit::StoreCurrentPublicationRecordBody::genesis(
                    authority.store_root.store_root_hash,
                )
            }
            crate::RetainedReplayAuthority::InstalledSnapshot(authority) => {
                (*authority.metadata.publication_predecessor).clone()
            }
        };
        if replay.previous() != &anchor {
            return Err(DbError::Message(
                "Store replay starts from another installed baseline".to_string(),
            ));
        }
        let mut pending = std::collections::BTreeMap::new();
        for prepared in materializations {
            let reference = prepared.verified_commit.reference().clone();
            if pending.insert(reference, prepared).is_some() {
                return Err(DbError::Message(
                    "duplicate Store publication materialization".to_string(),
                ));
            }
        }
        let mut snapshots = snapshots.into_iter();
        let mut latest_snapshot = None;
        let mut rebased_snapshot = installed_checkpoint;
        let mut rows = Vec::new();
        let mut max_updated_at = None;
        let mut installed = Vec::new();
        let mut complete = true;
        // Snapshot boundaries close replay intervals. Within each interval,
        // retain every accepted input before the canonical scheduler runs, so
        // a concurrent dependency can be supplied by another accepted commit.
        for entries in replay.entries().chunk_by(|left, right| {
            matches!(
                left.entry().payload,
                coven_protocol::store_commit::StorePublicationPayload::Commit(_)
            ) && matches!(
                right.entry().payload,
                coven_protocol::store_commit::StorePublicationPayload::Commit(_)
            )
        }) {
            let entry = &entries[0];
            match &entry.entry().payload {
                coven_protocol::store_commit::StorePublicationPayload::Snapshot(reference) => {
                    let authority = snapshots.next().ok_or_else(|| {
                        DbError::Message(
                            "Store snapshot publication has no verified baseline authority"
                                .to_string(),
                        )
                    })?;
                    if &authority.snapshot != reference
                        || authority.store_root.store_root_hash != entry.reference().store_root_hash
                        || authority.metadata.publication_predecessor.state_hash()
                            != entry.entry().previous_state_hash
                    {
                        return Err(DbError::Message(
                            "Store snapshot authority differs from its accepted publication"
                                .to_string(),
                        ));
                    }
                    if let Some(prepared) = self
                        .prepare_snapshot_replay_baseline_advance(
                            &authority.store_root,
                            &authority,
                            routing_encryption,
                        )
                        .map_err(|error| {
                            DbError::context("received snapshot baseline advance", error)
                        })?
                    {
                        let changes_publication_base = prepared.changes_publication_base;
                        self.store.advance_snapshot_replay_baseline(
                            self.authority, &authority.store_root, schema_version,
                            sync_routing_hash, authority.clone(), prepared, self.blob_decls, self.synced_tables,
                        ).map_err(|error| DbError::context("received snapshot baseline installation", error))?.ok_or_else(|| DbError::Message(
                            "prepared Store snapshot no longer advances the replay baseline".to_string(),
                        ))?;
                        self.authority.forget_superseded_replay_baseline();
                        if changes_publication_base {
                            rebased_snapshot =
                                Some(coven_protocol::store_commit::AcceptedStoreSnapshotRef {
                                    snapshot: authority.snapshot.clone(),
                                    publication: entry.reference().clone(),
                                });
                        }
                    }
                    latest_snapshot = Some(entry.reference());
                }
                coven_protocol::store_commit::StorePublicationPayload::Commit(_) => {
                    let references = entries
                        .iter()
                        .map(|entry| match &entry.entry().payload {
                            coven_protocol::store_commit::StorePublicationPayload::Commit(
                                reference,
                            ) => reference,
                            coven_protocol::store_commit::StorePublicationPayload::Snapshot(_) => {
                                unreachable!("commit segments stop before every snapshot")
                            }
                        })
                        .collect::<Vec<_>>();
                    let mut replay = None;
                    for reference in &references {
                        if let Some(prepared) = pending.remove(*reference) {
                            self.retain_received_merge_materialization(&prepared, receiver_wall_ms)
                                .map_err(|error| {
                                    DbError::context("received commit authority retention", error)
                                })?;
                            installed.push((*reference).clone());
                            replay = Some(*reference);
                        }
                    }
                    if let Some(reference) = replay {
                        let applied = self
                            .replay_received_merge_materializations(
                                reference,
                                local_store_membership,
                                routing_key.as_ref(),
                            )
                            .map_err(|error| {
                                DbError::context("received commit projection", error)
                            })?;
                        match applied.outcome {
                            crate::MaterializationOutcome::Applied(changes) => rows.extend(changes),
                            held @ crate::MaterializationOutcome::Held(_) => {
                                return Ok((super::merge_materialization_transaction::AppliedMergeMaterialization {
                                    outcome: held, max_updated_at: None, write_status_notifications: Vec::new(),
                                }, Vec::new()));
                            }
                        }
                        if applied.max_updated_at > max_updated_at {
                            max_updated_at = applied.max_updated_at;
                        }
                    }
                    for reference in references {
                        let stream_id = reference.coord.stream_id.to_string();
                        if crate::store::materialized_commit_index::materialized_commit_ref_on(
                            self.store.transaction,
                            &stream_id,
                            reference.coord.sequence(),
                        )? != Some(reference.clone())
                        {
                            complete = false;
                        }
                    }
                    if !complete {
                        // A snapshot closes the whole preceding interval. Keep
                        // independent progress here, but do not fold past a held
                        // commit or apply work on the far side of that boundary.
                        break;
                    }
                }
            }
        }
        if !pending.is_empty() || (complete && snapshots.next().is_some()) {
            return Err(DbError::Message(
                "prepared Store materialization is outside its accepted interval".to_string(),
            ));
        }
        if let Some(snapshot) = rebased_snapshot {
            rows.extend(
                self.rebase_unpublished_store_writes(
                    &snapshot,
                    routing_key.as_ref(),
                    local_store_membership,
                )
                .map_err(|error| DbError::context("received snapshot unpublished rebase", error))?,
            );
        }
        if let Some(publication) = latest_snapshot {
            super::observed_store_publication::retire_store_publication_prefix_before_snapshot_on(
                self.store,
                self.authority,
                publication,
            )
            .map_err(|error| DbError::context("received snapshot publication retirement", error))?;
        }
        Ok((
            super::merge_materialization_transaction::AppliedMergeMaterialization {
                outcome: crate::MaterializationOutcome::Applied(rows),
                max_updated_at,
                write_status_notifications: Vec::new(),
            },
            installed,
        ))
    }

    pub(super) fn install_replay_projection(
        &self,
        replay: &super::ReplayProjection,
    ) -> Result<Vec<coven_foundation::changeset::RowChange>, DbError> {
        let tx = self.store.transaction;
        let mut host_changes = rusqlite::session::Session::new(tx).map_err(DbError::from)?;
        for table in self.synced_tables {
            host_changes
                .attach(Some(table.name()))
                .map_err(DbError::from)?;
        }
        let mut tables = crate::projection_table_names(self.gates.has_scoped_graph());
        tables.extend(
            self.synced_tables
                .iter()
                .map(|table| table.name().to_string()),
        );
        tables.sort();
        tables.dedup();
        let suspended_cleanup =
            replay.suspend_blob_cleanup_for_restoration_on(tx, self.blob_decls)?;
        let old_exact_bindings = super::local_blob_cleanup::exact_blob_bindings_on(tx)?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        crate::store::store_session::StoreTransaction::new(tx, self.store.store_dir)
            .replace_tables_from_projection(replay, &tables)?;
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
            self.merge_materialization_failure,
            crate::MergeMaterializationFailurePoint::ProjectionReplacement,
        )? {
            return Err(DbError::Message(
                "injected failure after Merge projection replacement".to_string(),
            ));
        }
        let old_projection =
            crate::walk_old_changeset(&projection_changeset).map_err(DbError::Changeset)?;
        let new_projection =
            crate::walk_changeset(&projection_changeset).map_err(DbError::Changeset)?;
        for intent in crate::local_blob_cleanup_intents::intents_from_changes(
            self.blob_decls,
            &old_projection,
            &new_projection,
        )
        .map_err(DbError::from)?
        {
            super::local_blob_cleanup::record_obsolete_copy_intents_from_bindings_on(
                tx,
                self.blob_decls,
                &intent,
                &old_exact_bindings,
            )?;
        }
        super::local_blob_cleanup::reevaluate_suspended_blob_cleanup_on(
            tx,
            self.blob_decls,
            &suspended_cleanup,
        )?;
        crate::Database::cancel_transitions_for_deleted_roots_on(
            tx,
            &super::merge_materialization_transaction::deleted_rows(&new_projection),
        )?;
        Ok(new_projection)
    }

    fn complete_published_store_operation(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        acceptance: &crate::AcceptedStoreCommitEvidence,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<(), DbError> {
        let reference = verified_commit.reference();
        let store_transaction = MergeMaterializationTransaction::from_store(self.store);
        if let Some(object_ids) = operation_object_ids {
            store_transaction.activate_store_operation_remote_objects(reference, &object_ids)?;
        }
        if matches!(
            super::active_store_publication::load_active_store_publication_on(
                self.store.transaction
            )?
            .as_ref()
            .map(|active| active.owner()),
            Some(crate::ActiveStorePublicationOwner::DeviceJoin(_))
        ) {
            super::device_join_publication::complete_owner_device_join_publication_on(
                self.store.transaction,
                reference,
                membership_completion.as_ref(),
            )?;
        }
        if let Some(completion) = membership_completion {
            store_transaction
                .complete_membership_journal(completion, acceptance, verified_commit)
                .map_err(|error| DbError::context("complete exact membership journal", error))?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_published_store_operation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<Option<crate::OwnedVerifiedMergeMaterialization>, DbError> {
        let publication = publication.resolve_installed_on(self.store, &verified_commit)?;
        let materialize = publication.requires_materialization();
        if materialize {
            super::clock_floor::observe_circle_metadata(
                &mut self.clock_floor,
                circle_activations.circles(),
                crate::IncomingTimestampPolicy::LocallyAuthored,
            )?;
        }
        let tx = self.store.transaction;
        let acceptance = publication.install_on(self.store, &verified_commit)?;
        let authority = &mut *self.authority;
        let store_transaction = MergeMaterializationTransaction::from_store(self.store);
        if materialize && !registrations.is_empty() {
            super::record_activated_store_device_registrations_on(
                tx,
                verified_commit.value(),
                &registrations,
            )?;
        }
        let retained = if materialize {
            let materialization = VerifiedMergeMaterialization::verify(
                &root,
                &verified_commit,
                &registrations,
                &device_operations,
                &circle_activations,
                &acceptance,
                &history_evidence,
                membership_objects.as_ref(),
                &[],
                None,
            )?;
            let retained = store_transaction
                .record_verified_merge_materialization(authority, materialization)
                .map_err(|error| DbError::context("record exact Merge materialization", error))?;
            authority.insert_verified(retained.clone())?;
            Some(retained)
        } else {
            None
        };
        self.complete_published_store_operation(
            &verified_commit,
            &acceptance,
            operation_object_ids,
            membership_completion,
        )?;
        Ok(retained)
    }

    fn install_device_join_bootstrap(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        resolved: crate::ResolvedDeviceJoinBootstrap,
    ) -> Result<(), DbError> {
        let crate::ResolvedDeviceJoinBootstrap {
            plan,
            snapshot_circles,
            mut row_data,
            local_store_membership,
            routing_key,
            receiver_wall_ms,
        } = resolved;
        super::clock_floor::observe_circle_metadata(
            &mut self.clock_floor,
            row_data
                .values()
                .flat_map(|data| data.circle_activations.circles()),
            crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
        )?;
        let tx = self.store.transaction;
        let blob_decls = self.blob_decls;
        let gates = self.gates;
        let synced_tables = self.synced_tables;
        let authority = &mut *self.authority;
        let installed_root = authority.root().clone();
        if installed_root != root || plan.founder.store_root != root {
            return Err(DbError::Message(
                "device join bootstrap root differs from the installed exact root".to_string(),
            ));
        }
        if !snapshot_circles.access.is_empty() || !snapshot_circles.bases.is_empty() {
            let snapshot_floor = self.store.restore_device_join_snapshot_circles(
                &root,
                &snapshot_circles,
                synced_tables,
                blob_decls,
                receiver_wall_ms,
            )?;
            if snapshot_floor > self.clock_floor {
                self.clock_floor = snapshot_floor;
            }
            authority.forget_superseded_replay_baseline();
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
        let publication_previous =
            super::observed_store_publication::load_store_current_publication_on(tx)?;
        if &**publication_previous.record() != plan.publication.interval().previous() {
            return Err(DbError::Message(
                "device join publication history starts from another snapshot boundary".to_string(),
            ));
        }
        // Replay verifies every retained commit against its accepted
        // publication. Install the verified interval first so those lookups
        // see the same authority as the plan; the surrounding transaction
        // still exposes the publication and materialized rows atomically.
        super::observed_store_publication::install_store_publication_interval_on(
            tx,
            &publication_previous,
            &plan.publication,
        )?;
        if let Some(snapshot) = plan.publication.interval().current().latest_snapshot() {
            super::observed_store_publication::retire_store_publication_prefix_before_snapshot_on(
                self.store,
                authority,
                &snapshot.publication,
            )?;
        }

        let represented = device_join_bootstrap_represented_on(tx, &plan.commits)?;

        // Row data has to be present before anything advances over it. A commit
        // that names a Store package but resolved none would otherwise leave the
        // joining device with an advanced position and no rows.
        for prepared in &plan.commits {
            if represented.contains(&prepared.reference) {
                continue;
            }
            let commit = prepared.commit.value();
            let resolved = row_data.get(&prepared.reference);
            let carries_store_package = resolved.is_some_and(|data| {
                data.packages.iter().any(|prepared| {
                    matches!(
                        prepared.package.audience(),
                        coven_protocol::audience_package::PackageAudience::Store
                    )
                })
            });
            if resolved.is_none() || (commit.store_package().is_some() && !carries_store_package) {
                return Err(DbError::Message(format!(
                    "device join bootstrap cannot advance over unmaterialized row data at {}/{}",
                    prepared.reference.coord.stream_id,
                    prepared.reference.coord.sequence()
                )));
            }
        }

        let mut retained_any = false;
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
            let data = row_data.remove(&prepared.reference).ok_or_else(|| {
                DbError::Message(format!(
                    "device join bootstrap has no resolved row data at {stream_id}/{}",
                    prepared.reference.coord.sequence()
                ))
            })?;
            let publication = plan
                .publication
                .accepted_commit(&prepared.commit)
                .map_err(|error| DbError::context("device join accepted publication", error))?;
            let materialization = crate::PreparedMergeMaterialization {
                root: root.clone(),
                verified_commit: prepared.commit,
                acceptance: publication.into(),
                history_evidence: prepared.history_evidence,
                membership_objects: data.membership_objects,
                membership_remote_objects: data.membership_remote_objects,
                registrations: prepared.registrations,
                package_application: (!data.packages.is_empty())
                    .then_some(crate::RetainedPackageApplication::Received { receiver_wall_ms }),
                packages: data.packages,
                device_operations: prepared.device_operations,
                circle_activations: data.circle_activations,
            };
            let merge_transaction = MergeMaterializationTransaction::from_store(self.store);
            merge_transaction.record_prepared_materialization_authority(&materialization)?;
            let retained = merge_transaction
                .retain_prepared_merge_materialization(authority, &materialization)?;
            authority.insert_verified(retained)?;
            retained_any = true;
        }
        if !row_data.is_empty() {
            return Err(DbError::Message(
                "device join bootstrap resolved row data outside its exact history".to_string(),
            ));
        }
        if !retained_any {
            return Ok(());
        }
        let replayed = authority.replay_projection_result_on(
            crate::store::store_session::StoreTransaction::new(tx, self.store.store_dir),
            blob_decls,
            gates,
            synced_tables,
            routing_key.as_ref(),
            None,
            crate::ReplayJournal::Owed,
            local_store_membership,
        )?;
        replayed.install_on(self)?;
        let max_updated_at = replayed.max_updated_at();
        if max_updated_at > self.clock_floor {
            self.clock_floor = max_updated_at.clone();
        }
        Ok(())
    }

    fn complete_owner_recovery(
        &mut self,
        verified_commit: VerifiedStoreBatchCommit,
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
        acceptance_result: coven_protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        let root = self.authority.root().clone();
        let publication = publication.resolve_installed_on(self.store, &verified_commit)?;
        let proof = history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(|| DbError::Message("Owner recovery has no membership proof".into()))?;
        let membership_objects = crate::VerifiedMergeMembershipObjects::verify(
            verified_commit.value(),
            verified_commit.reference(),
            &proof.entry_value,
            &proof.head_value,
            proof.head.clone(),
        )?;
        let coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(result) =
            &acceptance_result
        else {
            return Err(DbError::Message(
                "Owner recovery result has another remote object domain".into(),
            ));
        };
        if !matches!(&result.identity.domain, coven_protocol::remote_object::RetainedAuthorityObjectDomain::MembershipHeadAcceptance { head, .. } if head == &proof.head)
        {
            return Err(DbError::Message(
                "Owner recovery result names another authority head".into(),
            ));
        }
        let mut object_ids = membership_objects.object_ids().collect::<Vec<_>>();
        object_ids.push(acceptance_result.object_id());
        MergeMaterializationTransaction::from_store(self.store)
            .activate_store_operation_remote_objects(verified_commit.reference(), &object_ids)?;
        let accepted = match &publication {
            crate::StoreCommitPublicationOutcome::Accepted { interval, .. } => {
                interval.accepted_commit(&verified_commit)?
            }
            crate::StoreCommitPublicationOutcome::Installed(_) => {
                let [activation] = verified_commit.device_registrations() else {
                    return Err(DbError::Message(
                        "installed Owner recovery must carry exactly one registration activation"
                            .to_string(),
                    ));
                };
                if !matches!(
                    activation.authority,
                    coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery { .. }
                ) || verified_commit.seq() != 1
                    || verified_commit.author_registration != activation.registration
                {
                    return Err(DbError::Message(
                        "installed Owner recovery commit differs from its recovery registration"
                            .to_string(),
                    ));
                }
                registration.verify_reference(activation)?;
                let records =
                    super::StoreRecords::new(self.store.transaction, self.store.store_dir);
                let installed = records.activated_registration(&root, registration.reference())?;
                let authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation =
                    serde_json::from_str(
                        &records.activated_registration_authority(registration.reference())?,
                    )
                    .map_err(|error| {
                        DbError::context("installed Owner recovery registration authority", error)
                    })?;
                if installed != *registration.value()
                    || authority != *registration.activation()
                    || records.local_activated_registration_ref()?.as_ref()
                        != Some(registration.reference())
                {
                    return Err(DbError::Message(
                        "installed Owner recovery differs from the local activated registration"
                            .to_string(),
                    ));
                }
                let evidence = publication.install_on(self.store, &verified_commit)?;
                let completed = super::owner_recovery_publication::complete_matching_owner_recovery_publication_on(
                    self.store,
                    &verified_commit,
                    &evidence,
                )?;
                if !completed && records.owner_recovery_publication_row()?.is_some() {
                    return Err(DbError::Message(
                        "installed Owner recovery differs from the pending publication journal"
                            .to_string(),
                    ));
                }
                return Ok(());
            }
        };
        let reference = verified_commit.reference();
        let device_operations =
            VerifiedStoreDeviceOperations::without_exclusions(verified_commit.value())?;
        let circle_activations =
            VerifiedCircleActivations::membership_control(verified_commit.value(), reference)?;
        let retained = self
            .materialize_published_store_operation(
                root,
                verified_commit,
                vec![registration],
                device_operations,
                circle_activations,
                publication,
                history_evidence,
                Some(membership_objects),
                None,
                None,
            )?
            .ok_or_else(|| {
                DbError::Message("new Owner recovery publication was already installed".to_string())
            })?;
        super::owner_recovery_publication::complete_owner_recovery_publication_on(
            self.store,
            retained.verified_commit(),
            &accepted,
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
    fn apply_received_store_publication_interval(
        &mut self,
        materializations: Vec<crate::PreparedMergeMaterialization>,
        accepted: AcceptedStorePublicationInterval,
        replay: coven_protocol::store_commit::VerifiedStorePublicationInterval,
        snapshots: Vec<coven_protocol::store_commit::RetainedReplaySnapshotAuthority>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<
        (
            super::merge_materialization_transaction::AppliedMergeMaterialization,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        let schema_version = self.schema_version;
        let sync_routing_hash = self.sync_routing_hash;
        let applied = self.verified_store_transaction(move |transaction| {
            let applied = transaction.apply_received_store_publication_interval(
                materializations,
                accepted,
                replay,
                snapshots,
                local_store_membership,
                schema_version,
                sync_routing_hash,
                routing_encryption,
                routing_key,
                receiver_wall_ms,
                None,
            )?;
            if matches!(applied.0.outcome, crate::MaterializationOutcome::Applied(_)) {
                Ok(StoreTransactionOutcome::Commit(applied))
            } else {
                Ok(StoreTransactionOutcome::Rollback(applied))
            }
        })?;
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
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<Option<crate::OwnedVerifiedMergeMaterialization>, DbError> {
        self.verified_store_transaction(move |transaction| {
            let retained = transaction.materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                publication,
                history_evidence,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )?;
            Ok(StoreTransactionOutcome::Commit(retained))
        })
    }

    fn unrepresented_device_join_bootstrap_commits(
        &mut self,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<
        (
            crate::DeviceJoinBootstrapPlan,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        self.verified_store_transaction(move |transaction| {
            let represented =
                device_join_bootstrap_represented_on(transaction.store.transaction, &plan.commits)?;
            let unrepresented = plan
                .commits
                .iter()
                .map(|prepared| prepared.reference.clone())
                .filter(|reference| !represented.contains(reference))
                .collect::<Vec<_>>();
            Ok(StoreTransactionOutcome::Rollback((plan, unrepresented)))
        })
    }

    fn install_device_join_bootstrap(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        resolved: crate::ResolvedDeviceJoinBootstrap,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.install_device_join_bootstrap(root, resolved)?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }

    fn complete_owner_recovery(
        &mut self,
        verified_commit: VerifiedStoreBatchCommit,
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
        acceptance_result: coven_protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        self.verified_store_transaction(move |transaction| {
            transaction.complete_owner_recovery(
                verified_commit,
                publication,
                history_evidence,
                registration,
                acceptance_result,
            )?;
            Ok(StoreTransactionOutcome::Commit(()))
        })
    }
}

impl StoreDatabase {
    pub async fn apply_received_store_publication_interval(
        &self,
        materializations: Vec<crate::PreparedMergeMaterialization>,
        accepted: AcceptedStorePublicationInterval,
        replay: coven_protocol::store_commit::VerifiedStorePublicationInterval,
        snapshots: Vec<crate::VerifiedStoreSnapshotAuthority>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<
        (
            crate::MaterializationOutcome,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        let snapshots = snapshots
            .into_iter()
            .map(crate::VerifiedStoreSnapshotAuthority::into_authority)
            .collect();
        let (applied, installed) = self
            .call_store(move |session| {
                session.apply_received_store_publication_interval(
                    materializations,
                    accepted,
                    replay,
                    snapshots,
                    local_store_membership,
                    routing_encryption.as_ref(),
                    routing_key,
                    receiver_wall_ms,
                )
            })
            .await?;
        for (write_id, status) in applied.write_status_notifications {
            self.notify_write_status(write_id, status);
        }
        Ok((applied.outcome, installed))
    }

    /// Complete an operation that an accepted pull already installed. Its
    /// historical materialization inputs are no longer required after retirement.
    pub async fn complete_installed_store_operation(
        &self,
        verified_commit: VerifiedStoreBatchCommit,
        acceptance: crate::AcceptedStoreCommitEvidence,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<crate::AcceptedStoreCommitEvidence, DbError> {
        self.call_store(move |session| {
            session.verified_store_transaction(move |transaction| {
                let publication = crate::StoreCommitPublicationOutcome::Installed(acceptance)
                    .resolve_installed_on(transaction.store, &verified_commit)?;
                let acceptance = publication.install_on(transaction.store, &verified_commit)?;
                transaction.complete_published_store_operation(
                    &verified_commit,
                    &acceptance,
                    operation_object_ids,
                    membership_completion,
                )?;
                Ok(StoreTransactionOutcome::Commit(acceptance))
            })
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_published_store_operation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        membership_objects: Option<crate::VerifiedMergeMembershipObjects>,
        operation_object_ids: Option<Vec<coven_protocol::store_commit::ObjectHash>>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<Option<crate::OwnedVerifiedMergeMaterialization>, DbError> {
        self.call_store(move |session| {
            session.materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                publication,
                history_evidence,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )
        })
        .await
    }

    /// The plan commits whose rows this database does not already materialize.
    /// The joining device resolves row data for exactly these before installing.
    pub async fn unrepresented_device_join_bootstrap_commits(
        &self,
        plan: crate::DeviceJoinBootstrapPlan,
    ) -> Result<
        (
            crate::DeviceJoinBootstrapPlan,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        self.call_store(move |session| session.unrepresented_device_join_bootstrap_commits(plan))
            .await
    }

    pub async fn install_device_join_bootstrap(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        resolved: crate::ResolvedDeviceJoinBootstrap,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.install_device_join_bootstrap(root, resolved))
            .await
    }

    pub async fn complete_owner_recovery(
        &self,
        verified_commit: VerifiedStoreBatchCommit,
        publication: crate::StoreCommitPublicationOutcome,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        registration: ActivatedStoreDeviceRegistration,
        acceptance_result: coven_protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_owner_recovery(
                verified_commit,
                publication,
                history_evidence,
                registration,
                acceptance_result,
            )
        })
        .await
    }
}

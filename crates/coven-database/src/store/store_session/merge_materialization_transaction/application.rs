use super::*;
use crate::query_mapped_rows;

/// The rows a set of applied changes removed, as `(table, row id)`.
///
/// A delete carries the row's old values, which is what makes the identity
/// readable after the row itself is gone.
pub(super) fn deleted_rows_inner(
    changes: &[RowChange],
) -> std::collections::HashSet<(String, String)> {
    changes
        .iter()
        .filter(|change| matches!(change.op, coven_foundation::changeset::ChangeOp::Delete))
        .filter_map(|change| change.pk().map(|id| (change.table.clone(), id.to_string())))
        .collect()
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn activate_store_operation_remote_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
        let mut unique = std::collections::BTreeSet::new();
        for object_id in object_ids {
            if !unique.insert(*object_id) {
                return Err(DbError::Message(
                    "Store operation names a duplicate remote object".to_string(),
                ));
            }
            let remote = load_remote_object_on(conn, *object_id).map_err(|error| {
                DbError::context(
                    format!("load Store operation remote object {object_id} for activation"),
                    error,
                )
            })?;
            let kind = match &remote {
                RemoteObjectRecord::CandidateCommit(_) => "candidate commit",
                RemoteObjectRecord::CandidateExclusive(_) => "candidate-exclusive object",
                RemoteObjectRecord::RetainedAuthority(_) => "retained authority",
                RemoteObjectRecord::SharedLiveSet(_) => "shared live-set object",
            };
            let remote = remote.into_activated(commit_ref).map_err(|error| {
                DbError::context(
                    format!("activate Store operation {kind} {object_id}"),
                    error,
                )
            })?;
            update_remote_object_on(conn, *object_id, &remote)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_merge_subset(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        source: &ValidatedChangeset<Vec<u8>>,
        bytes: Vec<u8>,
        package_audience: Option<&coven_protocol::circle::Audience>,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<coven_protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let applied_changeset = source
            .validate_subset(bytes.clone())
            .map_err(DbError::from)?;
        let actual_changes = crate::walk_changeset(&bytes).map_err(DbError::Changeset)?;
        if let Some(receiver_wall_ms) = timestamp_policy.received_wall_ms() {
            advance_max_updated_at(
                changeset_max,
                &actual_changes,
                source.schema(),
                receiver_wall_ms,
            );
        }
        returned_changes.extend(
            actual_changes
                .iter()
                .filter(|change| !crate::is_routing_table(&change.table))
                .cloned(),
        );
        let apply = self.apply_changeset(applied_changeset, timestamp_policy)?;
        if !apply.constraint_conflict_tables.is_empty() {
            return Ok(MergeSubsetOutcome::ConstraintConflict(
                apply.constraint_conflict_tables,
            ));
        }
        if let Some(package_audience) = package_audience {
            crate::align_inbound_scoped_root_audiences(
                self.store.transaction,
                &bytes,
                package_audience,
                gates,
                routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped audience application requires a row-routing key".to_string(),
                    )
                })?,
            )
            .map_err(DbError::from)?;
        }
        let winning_rows = self.current_winning_rows(source.schema(), &bytes)?;
        let old_changes = crate::walk_old_changeset(&bytes).map_err(DbError::Changeset)?;
        let cleanup = local_blob_cleanup_intents(blob_decls, &old_changes, &actual_changes)
            .map_err(DbError::from)?;
        for intent in cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        // A root another device deleted is deleted here too, and this device's
        // queue for it is this device's to unwind — the peer that removed the
        // row knows nothing about what is still queued on this one.
        //
        // Uploads are only queued against Local blob references, and a Local
        // root belongs to this device, so a peer normally has none to delete.
        // Keep the cancellation at the merge boundary because that conclusion
        // depends on rules owned outside this transaction.
        crate::Database::cancel_transitions_for_deleted_roots_on(
            self.store.transaction,
            &deleted_rows_inner(&actual_changes),
        )?;
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_merge_package(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        package: &AudiencePackage,
        changeset: &ValidatedChangeset<Vec<u8>>,
        store_audience_transitions: &crate::StoreAudienceTransitions,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<coven_protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
        schema: &TableSchema,
        private_rows: &ReplayRows,
        commit: &StoreBatchCommitRef,
        own_publication: bool,
        adopted_private_rows: &mut BTreeSet<(String, String)>,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let conn = self.store.transaction;
        let mut winning_rows = Vec::new();
        match package.audience() {
            PackageAudience::Store if gates.has_scoped_graph() => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped Store package application requires a row-routing key".to_string(),
                    )
                })?;
                let inbound = crate::normalize_inbound_store_changeset(
                    conn,
                    package.changeset(),
                    gates,
                    routing_key,
                )
                .map_err(DbError::from)?;
                if let Err(outcome) = self
                    .apply_merge_subset(
                        blob_decls,
                        gates,
                        Some(routing_key),
                        changeset,
                        inbound.mirror,
                        None,
                        timestamp_policy,
                        changeset_max,
                        returned_changes,
                    )?
                    .append_winning_rows(&mut winning_rows)
                {
                    return Ok(outcome);
                }
                let rows =
                    crate::filter_inbound_store_rows(conn, &inbound.rows, gates, routing_key)
                        .map_err(DbError::from)?;
                if let Some(hold) = self.adopt_equivalent_private_rows(
                    gates,
                    blob_decls,
                    schema,
                    private_rows,
                    &rows,
                    package,
                    commit,
                    own_publication,
                    adopted_private_rows,
                )? {
                    return Ok(MergeSubsetOutcome::Held(hold));
                }
                if let Err(outcome) = self
                    .apply_merge_subset(
                        blob_decls,
                        gates,
                        Some(routing_key),
                        changeset,
                        rows,
                        Some(&coven_protocol::circle::Audience::Store),
                        timestamp_policy,
                        changeset_max,
                        returned_changes,
                    )?
                    .append_winning_rows(&mut winning_rows)
                {
                    return Ok(outcome);
                }
            }
            PackageAudience::Store => {
                if let Some(hold) = self.adopt_equivalent_private_rows(
                    gates,
                    blob_decls,
                    schema,
                    private_rows,
                    package.changeset(),
                    package,
                    commit,
                    own_publication,
                    adopted_private_rows,
                )? {
                    return Ok(MergeSubsetOutcome::Held(hold));
                }
                return self.apply_merge_subset(
                    blob_decls,
                    gates,
                    None,
                    changeset,
                    package.changeset().to_vec(),
                    None,
                    timestamp_policy,
                    changeset_max,
                    returned_changes,
                );
            }
            PackageAudience::Circle { circle_id, .. } => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "Circle package application requires a row-routing key".to_string(),
                    )
                })?;
                let rows = crate::filter_inbound_circle_changeset(
                    conn,
                    package.changeset(),
                    *circle_id,
                    store_audience_transitions,
                    gates,
                    routing_key,
                )
                .map_err(DbError::from)?;
                if let Some(hold) = self.adopt_equivalent_private_rows(
                    gates,
                    blob_decls,
                    schema,
                    private_rows,
                    &rows,
                    package,
                    commit,
                    own_publication,
                    adopted_private_rows,
                )? {
                    return Ok(MergeSubsetOutcome::Held(hold));
                }
                return self.apply_merge_subset(
                    blob_decls,
                    gates,
                    Some(routing_key),
                    changeset,
                    rows,
                    Some(&coven_protocol::circle::Audience::Circle(*circle_id)),
                    timestamp_policy,
                    changeset_max,
                    returned_changes,
                );
            }
        }
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    pub(super) fn apply_prepared_merge_materialization_inner(
        &self,
        registrations_lookup: &mut dyn VerifiedStoreLookup,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        local_store_membership: LocalStoreMembership,
        timestamp_policy: IncomingTimestampPolicy,
        baseline_circle_cuts: Option<
            &BTreeMap<
                coven_protocol::circle::CircleId,
                coven_protocol::store_commit::CommitFrontier,
            >,
        >,
        materialization: PreparedMergeMaterialization,
        local_effect: Option<crate::MergeReplayWriteEffect>,
        schema: std::sync::Arc<TableSchema>,
        private_rows: &mut ReplayRows,
    ) -> Result<AppliedMergeMaterialization, DbError> {
        let conn = self.store.transaction;
        self.record_prepared_materialization_authority(&materialization)?;
        let commit_ref = materialization.verified_commit.reference();
        let own_publication = local_effect.is_some();
        let mut next_private_rows = private_rows.clone();
        let mut adopted_private_rows = BTreeSet::new();
        let mut inactive_circles = materialization
            .circle_activations
            .circles()
            .iter()
            .filter_map(|activation| {
                activation
                    .local_access
                    .as_ref()
                    .filter(|access| access.active.is_none())
                    .filter(|_| {
                        baseline_circle_cuts
                            .and_then(|cuts| cuts.get(&activation.circle_id))
                            .is_none_or(|cut| !cut.covers_commit(commit_ref))
                    })
                    .map(|_| activation.circle_id)
            })
            .collect::<BTreeSet<_>>();
        let mut changeset_max = None;
        let mut returned_changes = Vec::new();
        // A Circle whose winning control chain is now Deleted prunes its rows,
        // routes, and blob bindings like an inactive recipient. Recording the
        // verified activation above already removed its live access cache while
        // retaining the authority spine.
        for activation in materialization.circle_activations.circles() {
            if self.circle_current_state_is_deleted(activation.circle_id)? {
                inactive_circles.insert(activation.circle_id);
            }
        }
        let store_audience_transitions = materialization
            .packages
            .iter()
            .find(|prepared| matches!(prepared.package.audience(), PackageAudience::Store))
            .map(|prepared| crate::store_audience_transitions(prepared.package.changeset()))
            .transpose()
            .map_err(DbError::from)?
            .unwrap_or_default();
        for prepared in &materialization.packages {
            let package = &prepared.package;
            let changeset = &prepared.changeset;
            let winning_rows = match self.apply_merge_package(
                blob_decls,
                gates,
                routing_key,
                package,
                changeset,
                &store_audience_transitions,
                timestamp_policy,
                &mut changeset_max,
                &mut returned_changes,
                &schema,
                &next_private_rows,
                commit_ref,
                own_publication,
                &mut adopted_private_rows,
            )? {
                MergeSubsetOutcome::Applied(rows) => rows,
                MergeSubsetOutcome::ConstraintConflict(tables) => {
                    return Ok(AppliedMergeMaterialization {
                        outcome: crate::MaterializationOutcome::Held(
                            crate::MaterializationHold::ConstraintConflict(tables),
                        ),
                        max_updated_at: None,
                        write_status_notifications: Vec::new(),
                    });
                }
                MergeSubsetOutcome::Held(hold) => {
                    return Ok(AppliedMergeMaterialization {
                        outcome: crate::MaterializationOutcome::Held(hold),
                        max_updated_at: None,
                        write_status_notifications: Vec::new(),
                    });
                }
            };
            self.install_winning_blob_bindings(
                gates,
                synced_tables,
                package,
                &BlobActivation {
                    coord: commit_ref.coord.clone(),
                },
                &winning_rows,
            )?;
            self.record_accepted_rows(gates, &mut next_private_rows, &winning_rows, commit_ref)?;
        }
        let private_row_keys = next_private_rows
            .private
            .keys()
            .filter(|key| !adopted_private_rows.contains(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        match crate::gate::validate_accepted_foreign_key_closure(conn, gates, &private_row_keys) {
            Ok(()) => {}
            Err(crate::GateError::UnsharedForeignKeyParent(_)) => {
                return Ok(AppliedMergeMaterialization {
                    outcome: crate::MaterializationOutcome::Held(
                        crate::MaterializationHold::ForeignKeyDependency,
                    ),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                });
            }
            Err(error) => return Err(DbError::from(error)),
        }
        if let Some(hold) = self.validate_shared_rows_do_not_borrow_private_state(
            gates,
            &next_private_rows,
            &adopted_private_rows,
            commit_ref,
        )? {
            return Ok(AppliedMergeMaterialization {
                outcome: crate::MaterializationOutcome::Held(hold),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
            });
        }
        if let Some(effect) = local_effect {
            if let Some(hold) = self.apply_local_replay_effect(
                effect,
                schema.clone(),
                gates,
                commit_ref,
                &mut next_private_rows,
            )? {
                return Ok(AppliedMergeMaterialization {
                    outcome: crate::MaterializationOutcome::Held(hold),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                });
            }
        }
        if gates.has_scoped_graph() && !local_store_membership.retains_circle_rows() {
            let circles = query_mapped_rows(
                conn,
                "SELECT DISTINCT circle_id
                     FROM _coven_audience
                     WHERE circle_id IS NOT NULL
                     ORDER BY circle_id",
                [],
                |row| row.get::<_, String>(0),
            )?;
            for encoded in circles {
                inactive_circles.insert(encoded.parse().map_err(|error| {
                    DbError::context(
                        format!("parse materialized Circle audience {encoded}"),
                        error,
                    )
                })?);
            }
            crate::store::circle_operations::remove_local_circle_access_on(conn)?;
        }
        let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
        for table in synced_tables {
            removal_session
                .attach(Some(table.name()))
                .map_err(DbError::from)?;
        }
        crate::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
            .map_err(DbError::from)?;
        crate::validate_scoped_foreign_key_audiences(conn, gates).map_err(DbError::from)?;
        let mut removal_changeset = Vec::new();
        removal_session
            .changeset_strm(&mut removal_changeset)
            .map_err(DbError::from)?;
        drop(removal_session);
        let removed = crate::walk_old_changeset(&removal_changeset).map_err(DbError::Changeset)?;
        let removal_changes =
            crate::walk_changeset(&removal_changeset).map_err(DbError::Changeset)?;
        let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &removal_changes)
            .map_err(DbError::from)?;
        // A root pruned because its circle went inactive is as gone as one a
        // peer deleted, and owes the same unwind.
        crate::Database::cancel_transitions_for_deleted_roots_on(
            conn,
            &deleted_rows_inner(&removal_changes),
        )?;
        returned_changes.extend(removal_changes);
        for intent in removal_cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        if self.has_foreign_key_violations()? {
            return Ok(AppliedMergeMaterialization {
                outcome: crate::MaterializationOutcome::Held(
                    crate::MaterializationHold::ForeignKeyDependency,
                ),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
            });
        }
        if let Some(hold) = self.validate_private_rows_unchanged(
            gates,
            &schema,
            &next_private_rows,
            &adopted_private_rows,
            commit_ref,
        )? {
            return Ok(AppliedMergeMaterialization {
                outcome: crate::MaterializationOutcome::Held(hold),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
            });
        }
        Self::record_adopted_rows(&mut next_private_rows, &adopted_private_rows, commit_ref);
        self.retain_prepared_merge_materialization(registrations_lookup, &materialization)?;
        *private_rows = next_private_rows;
        Ok(AppliedMergeMaterialization {
            outcome: crate::MaterializationOutcome::Applied(returned_changes),
            max_updated_at: changeset_max,
            write_status_notifications: Vec::new(),
        })
    }
}

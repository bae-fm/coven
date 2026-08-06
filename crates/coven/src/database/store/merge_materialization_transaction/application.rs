use super::*;
use crate::database::query_mapped_rows;

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn activate_store_operation_remote_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
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
        gates: &crate::database::Gates,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        source: &ValidatedChangeset<Vec<u8>>,
        bytes: Vec<u8>,
        package_audience: Option<&crate::protocol::circle::Audience>,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<crate::protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
        package_reported_fk_violation: &mut bool,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let applied_changeset = source
            .validate_subset(bytes.clone())
            .map_err(|error| DbError::Message(error.to_string()))?;
        let actual_changes = crate::database::walk_changeset(&bytes).map_err(DbError::Message)?;
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
                .filter(|change| !crate::database::is_routing_table(&change.table))
                .cloned(),
        );
        let apply = self.apply_changeset(applied_changeset, timestamp_policy)?;
        if !apply.constraint_conflict_tables.is_empty() {
            return Ok(MergeSubsetOutcome::ConstraintConflict(
                apply.constraint_conflict_tables,
            ));
        }
        *package_reported_fk_violation |= apply.had_fk_violations;
        if let Some(package_audience) = package_audience {
            crate::database::align_inbound_scoped_root_audiences(
                self.transaction,
                &bytes,
                package_audience,
                gates,
                routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped audience application requires a row-routing key".to_string(),
                    )
                })?,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let winning_rows = self.current_winning_rows(source.schema(), &bytes)?;
        let old_changes = crate::database::walk_old_changeset(&bytes).map_err(DbError::Message)?;
        let cleanup = local_blob_cleanup_intents(blob_decls, &old_changes, &actual_changes)
            .map_err(|error| DbError::Message(error.to_string()))?;
        for intent in cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_merge_package(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        package: &AudiencePackage,
        changeset: &ValidatedChangeset<Vec<u8>>,
        store_audience_transitions: &crate::database::StoreAudienceTransitions,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<crate::protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
        package_reported_fk_violation: &mut bool,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let conn = self.transaction;
        let mut winning_rows = Vec::new();
        match package.audience() {
            PackageAudience::Store if gates.has_scoped_graph() => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped Store package application requires a row-routing key".to_string(),
                    )
                })?;
                let inbound = crate::database::normalize_inbound_store_changeset(
                    conn,
                    package.changeset(),
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if let Err(tables) = self
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
                        package_reported_fk_violation,
                    )?
                    .extend_winning_rows(&mut winning_rows)
                {
                    return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
                }
                let rows = crate::database::filter_inbound_store_rows(
                    conn,
                    &inbound.rows,
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if let Err(tables) = self
                    .apply_merge_subset(
                        blob_decls,
                        gates,
                        Some(routing_key),
                        changeset,
                        rows,
                        Some(&crate::protocol::circle::Audience::Store),
                        timestamp_policy,
                        changeset_max,
                        returned_changes,
                        package_reported_fk_violation,
                    )?
                    .extend_winning_rows(&mut winning_rows)
                {
                    return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
                }
            }
            PackageAudience::Store => {
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
                    package_reported_fk_violation,
                );
            }
            PackageAudience::Circle { circle_id, .. } => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "Circle package application requires a row-routing key".to_string(),
                    )
                })?;
                let rows = crate::database::filter_inbound_circle_changeset(
                    conn,
                    package.changeset(),
                    *circle_id,
                    store_audience_transitions,
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                return self.apply_merge_subset(
                    blob_decls,
                    gates,
                    Some(routing_key),
                    changeset,
                    rows,
                    Some(&crate::protocol::circle::Audience::Circle(*circle_id)),
                    timestamp_policy,
                    changeset_max,
                    returned_changes,
                    package_reported_fk_violation,
                );
            }
        }
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    pub(crate) fn apply_prepared_merge_materialization(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        synced_tables: &[SyncedTable],
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        local_store_membership: LocalStoreMembership,
        timestamp_policy: IncomingTimestampPolicy,
        baseline_circle_cuts: Option<
            &BTreeMap<
                crate::protocol::circle::CircleId,
                crate::protocol::store_commit::CommitFrontier,
            >,
        >,
        materialization: PreparedMergeMaterialization,
    ) -> Result<AppliedMergeMaterialization, DbError> {
        let conn = self.transaction;
        let PreparedMergeMaterialization {
            root,
            verified_commit,
            activation_head,
            activation_head_object,
            history_summary,
            membership_objects,
            membership_remote_objects,
            registrations,
            packages,
            device_operations,
            circle_activations,
            package_application,
        } = materialization;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let mut inactive_circles = circle_activations
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
        let mut package_reported_fk_violation = false;
        crate::database::store::record_activated_store_device_registrations_on(
            conn,
            commit,
            &registrations,
        )?;
        for bootstrap in circle_activations.bootstraps() {
            crate::database::install_circle_bootstrap_remote_objects_on(
                conn, commit_ref, bootstrap,
            )?;
        }
        self.record_verified_circle_activations(&verified_commit, circle_activations.circles())?;
        // A Circle whose winning control chain is now Deleted prunes its rows,
        // routes, and blob bindings like an inactive recipient. Recording the
        // verified activation above already removed its live access cache while
        // retaining the authority spine.
        for activation in circle_activations.circles() {
            if self.circle_current_state_is_deleted(activation.circle_id)? {
                inactive_circles.insert(activation.circle_id);
            }
        }
        let retained_packages = packages
            .iter()
            .map(|prepared| prepared.package.clone())
            .collect::<Vec<_>>();
        let store_audience_transitions = packages
            .iter()
            .find(|prepared| matches!(prepared.package.audience(), PackageAudience::Store))
            .map(|prepared| {
                crate::database::store_audience_transitions(prepared.package.changeset())
            })
            .transpose()
            .map_err(|error| DbError::Message(error.to_string()))?
            .unwrap_or_default();
        for prepared in packages {
            let PreparedMergeMaterializationPackage { package, changeset } = prepared;
            let winning_rows = match self.apply_merge_package(
                blob_decls,
                gates,
                routing_key,
                &package,
                &changeset,
                &store_audience_transitions,
                timestamp_policy,
                &mut changeset_max,
                &mut returned_changes,
                &mut package_reported_fk_violation,
            )? {
                MergeSubsetOutcome::Applied(rows) => rows,
                MergeSubsetOutcome::ConstraintConflict(tables) => {
                    return Ok(AppliedMergeMaterialization {
                        outcome: ApplyOutcome::Held(HeldStorePositionReason::ConstraintConflict(
                            tables,
                        )),
                        max_updated_at: None,
                        write_status_notifications: Vec::new(),
                        retained: None,
                    });
                }
            };
            let retained = crate::database::RetainedAudiencePackage::verify(
                commit,
                commit_ref,
                package.clone(),
            )?;
            Database::install_pulled_package_activation_on(
                conn,
                commit_ref,
                retained.domain(),
                retained.object(),
                retained.package(),
            )?;
            Database::install_pulled_blob_activations_on(conn, &package, commit_ref)?;
            self.install_winning_blob_bindings(
                gates,
                synced_tables,
                &package,
                &BlobActivation {
                    coord: commit_ref.coord.clone(),
                },
                &winning_rows,
            )?;
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
            crate::database::StoreDatabase::remove_local_circle_access_on(conn)?;
        }
        let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
        for table in synced_tables {
            removal_session
                .attach(Some(table.name()))
                .map_err(DbError::from)?;
        }
        crate::database::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
            .map_err(|error| DbError::Message(error.to_string()))?;
        crate::database::validate_scoped_foreign_key_audiences(conn, gates)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let mut removal_changeset = Vec::new();
        removal_session
            .changeset_strm(&mut removal_changeset)
            .map_err(DbError::from)?;
        drop(removal_session);
        let removed =
            crate::database::walk_old_changeset(&removal_changeset).map_err(DbError::Message)?;
        let removal_changes =
            crate::database::walk_changeset(&removal_changeset).map_err(DbError::Message)?;
        let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &removal_changes)
            .map_err(|error| DbError::Message(error.to_string()))?;
        returned_changes.extend(removal_changes);
        for intent in removal_cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        if package_reported_fk_violation {
            let violations: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if violations {
                return Ok(AppliedMergeMaterialization {
                    outcome: ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                    retained: None,
                });
            }
        }
        let verified = VerifiedMergeMaterialization::verify(
            &root,
            &verified_commit,
            &registrations,
            &device_operations,
            &circle_activations,
            &activation_head,
            &activation_head_object,
            &history_summary,
            membership_objects.as_ref(),
            &retained_packages,
            package_application,
        )?;
        Database::install_pulled_merge_membership_activations_on(
            conn,
            commit_ref,
            &membership_remote_objects,
        )?;
        let retained = self.record_verified_merge_materialization(verified)?;
        Ok(AppliedMergeMaterialization {
            outcome: ApplyOutcome::Applied(returned_changes),
            max_updated_at: changeset_max,
            write_status_notifications: Vec::new(),
            retained: Some(retained),
        })
    }
}

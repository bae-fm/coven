use super::*;

impl MergeMaterializationTransaction<'_, '_> {
    pub(super) fn apply_unaccepted_replay_effect_inner(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        effect: crate::MergeReplayWriteEffect,
        schema: std::sync::Arc<TableSchema>,
        gates: &crate::Gates,
        replay_rows: &mut ReplayRows,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        if let Some(hold) = self.validate_unaccepted_circle_context(authority, root, &effect)? {
            return Ok(Some(hold));
        }
        let public_rows = replay_effect_public_rows(self.store.transaction, &effect)?;
        let local_rows = replay_effect_local_rows(&effect)?;
        let changed_rows = replay_effect_rows(&effect)?;
        if let Some((table, row_id)) =
            self.local_write_would_change_shared_row(gates, &public_rows, &local_rows)?
        {
            return self.local_shared_conflict(replay_rows, &effect.write_id, table, row_id);
        }
        let partitions = effect
            .partitions
            .store
            .into_iter()
            .chain(effect.partitions.circles)
            .chain(effect.partitions.local);
        if let Some(tables) =
            self.apply_replay_partitions(&effect.write_id, partitions, schema.clone())?
        {
            return Ok(Some(crate::MaterializationHold::ConstraintConflict(tables)));
        }
        if self.has_foreign_key_violations()? {
            return Ok(Some(crate::MaterializationHold::ForeignKeyDependency));
        }
        if let Some((table, row_id)) = self.update_replay_rows_after_unaccepted_effect(
            gates,
            &schema,
            replay_rows,
            &changed_rows,
            &local_rows,
        )? {
            return self.local_shared_conflict(replay_rows, &effect.write_id, table, row_id);
        }
        Ok(None)
    }

    fn validate_unaccepted_circle_context(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        effect: &crate::MergeReplayWriteEffect,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        for partition in &effect.partitions.circles {
            let coven_protocol::circle::Audience::Circle(circle_id) = partition.audience else {
                return Err(DbError::Message(format!(
                    "local replay write {} has a non-Circle partition in its Circle set",
                    effect.write_id
                )));
            };
            if partition.control.is_none() {
                return Err(DbError::Message(format!(
                    "local replay write {} Circle partition has no captured control",
                    effect.write_id
                )));
            }
            let Some(state) = self.replay_circle_current_state(circle_id)? else {
                return Ok(Some(
                    crate::MaterializationHold::InvalidLocalCircleContext { circle_id },
                ));
            };
            let current = match &state {
                coven_protocol::circle_activation::CircleCurrentState::Active(_) => {
                    state
                        .authoring_state()
                        .expect("active Circle carries authoring state")
                        .control
                }
                coven_protocol::circle_activation::CircleCurrentState::Closing(_) => {
                    state
                        .closing_authoring_state()
                        .expect("closing Circle carries closing authoring state")
                        .control
                }
                coven_protocol::circle_activation::CircleCurrentState::Inactive(_)
                | coven_protocol::circle_activation::CircleCurrentState::Deleted(_)
                | coven_protocol::circle_activation::CircleCurrentState::ControlConflict {
                    ..
                } => {
                    return Ok(Some(
                        crate::MaterializationHold::InvalidLocalCircleContext { circle_id },
                    ));
                }
            };
            if !StoreDatabase::verified_circle_control_covers_on(
                crate::store::store_session::StoreRecords::new(
                    self.store.transaction,
                    self.store.store_dir,
                ),
                authority,
                root,
                circle_id,
                &current,
                partition
                    .control
                    .as_ref()
                    .expect("captured Circle control checked above")
                    .coordinate(),
            )? {
                return Ok(Some(
                    crate::MaterializationHold::InvalidLocalCircleContext { circle_id },
                ));
            }
        }
        Ok(None)
    }

    fn update_replay_rows_after_unaccepted_effect(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        replay_rows: &mut ReplayRows,
        changed_rows: &[(String, String, coven_foundation::changeset::ChangeOp)],
        local_rows: &[(String, String, coven_foundation::changeset::ChangeOp)],
    ) -> Result<Option<(String, String)>, DbError> {
        let shared_after = gates.shared_rows(self.store.transaction)?;
        for (table, row_id, op) in local_rows {
            if !matches!(op, coven_foundation::changeset::ChangeOp::Delete)
                && shared_after.contains(table, row_id)?
            {
                return Ok(Some((table.clone(), row_id.clone())));
            }
        }
        for (table, row_id, op) in changed_rows {
            let key = (table.clone(), row_id.clone());
            if matches!(op, coven_foundation::changeset::ChangeOp::Delete) {
                replay_rows.private.remove(&key);
                replay_rows.adopted_by.remove(&key);
            } else {
                self.record_private_row(schema, replay_rows, table, row_id)?;
            }
        }
        Ok(None)
    }

    fn local_shared_conflict(
        &self,
        replay_rows: &ReplayRows,
        write_id: &WriteId,
        table: String,
        row_id: String,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let commit = replay_rows
            .adopted_by
            .get(&(table.clone(), row_id.clone()))
            .ok_or_else(|| {
                DbError::Message(format!(
                    "local replay write {write_id} would change shared row {table}/{row_id} without an accepted adoption"
                ))
            })?;
        Ok(Some(crate::MaterializationHold::PrivateSharedConflict {
            table,
            row_id,
            commit: commit.clone(),
        }))
    }

    pub(super) fn apply_local_replay_effect(
        &self,
        effect: crate::MergeReplayWriteEffect,
        schema: std::sync::Arc<TableSchema>,
        gates: &crate::Gates,
        commit: &StoreBatchCommitRef,
        replay_rows: &mut ReplayRows,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let public_rows = replay_effect_public_rows(self.store.transaction, &effect)?;
        let local_rows = replay_effect_local_rows(&effect)?;
        if let Some((table, row_id)) =
            self.local_write_would_change_shared_row(gates, &public_rows, &local_rows)?
        {
            return Ok(Some(crate::MaterializationHold::PrivateSharedConflict {
                table,
                row_id,
                commit: commit.clone(),
            }));
        }
        if let Some(tables) =
            self.apply_replay_partitions(&effect.write_id, effect.partitions.local, schema.clone())?
        {
            return Ok(Some(crate::MaterializationHold::ConstraintConflict(tables)));
        }
        if let Some((table, row_id)) = self.update_private_rows_after_effect(
            gates,
            &schema,
            replay_rows,
            &public_rows,
            &local_rows,
            commit,
        )? {
            return Ok(Some(crate::MaterializationHold::PrivateSharedConflict {
                table,
                row_id,
                commit: commit.clone(),
            }));
        }
        Ok(None)
    }

    fn local_write_would_change_shared_row(
        &self,
        gates: &crate::Gates,
        public_rows: &BTreeSet<(String, String)>,
        local_rows: &[(String, String, coven_foundation::changeset::ChangeOp)],
    ) -> Result<Option<(String, String)>, DbError> {
        let shared_before = gates.shared_rows(self.store.transaction)?;
        for (table, row_id, _) in local_rows {
            let key = (table.clone(), row_id.clone());
            if !public_rows.contains(&key) && shared_before.contains(table, row_id)? {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }

    fn update_private_rows_after_effect(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        replay_rows: &mut ReplayRows,
        public_rows: &BTreeSet<(String, String)>,
        local_rows: &[(String, String, coven_foundation::changeset::ChangeOp)],
        commit: &StoreBatchCommitRef,
    ) -> Result<Option<(String, String)>, DbError> {
        for key in public_rows {
            replay_rows.private.remove(key);
            replay_rows.adopted_by.insert(key.clone(), commit.clone());
        }
        let shared_after = gates.shared_rows(self.store.transaction)?;
        for (table, row_id, op) in local_rows {
            let key = (table.clone(), row_id.clone());
            if matches!(op, coven_foundation::changeset::ChangeOp::Delete) {
                replay_rows.private.remove(&key);
                replay_rows.adopted_by.remove(&key);
            } else {
                if shared_after.contains(table, row_id)? {
                    return Ok(Some(key));
                }
                self.record_private_row(schema, replay_rows, table, row_id)?;
            }
        }
        Ok(None)
    }

    fn apply_replay_partitions(
        &self,
        write_id: &WriteId,
        partitions: impl IntoIterator<Item = crate::AudiencePartition>,
        schema: std::sync::Arc<TableSchema>,
    ) -> Result<Option<Vec<String>>, DbError> {
        self.store
            .transaction
            .pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::context(format!("local replay write {write_id} changeset"), error)
                })?;
            let applied =
                self.apply_changeset(changeset, IncomingTimestampPolicy::LocallyAuthored)?;
            if !applied.constraint_conflict_tables.is_empty() {
                return Ok(Some(applied.constraint_conflict_tables));
            }
        }
        Ok(None)
    }

    pub(super) fn has_foreign_key_violations(&self) -> Result<bool, DbError> {
        self.store
            .transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }
}

fn replay_effect_public_rows(
    connection: &rusqlite::Connection,
    effect: &crate::MergeReplayWriteEffect,
) -> Result<BTreeSet<(String, String)>, DbError> {
    let changes = effect
        .partitions
        .store
        .iter()
        .chain(effect.partitions.circles.iter())
        .map(|partition| crate::walk_changeset(&partition.changeset))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let routes = changes
        .iter()
        .filter(|change| change.table == "_coven_row_routes")
        .filter_map(|change| {
            Some((
                change.pk()?.to_string(),
                (change.col(1)?.to_string(), change.col(2)?.to_string()),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let mut represented = changes
        .iter()
        .filter(|change| !crate::is_routing_table(&change.table))
        .filter_map(|change| Some((change.table.clone(), change.pk()?.to_string())))
        .collect::<BTreeSet<_>>();
    for routing_id in changes
        .iter()
        .filter(|change| change.table == "_coven_audience")
        .filter_map(|change| change.pk())
    {
        if let Some(row) = routes.get(routing_id) {
            represented.insert(row.clone());
            continue;
        }
        let rows = crate::query_mapped_rows(
            connection,
            "SELECT table_name, row_id FROM _coven_row_routes WHERE routing_id = ?1",
            [routing_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        let [row] = rows.as_slice() else {
            return Err(DbError::Message(format!(
                "local replay public audience row {routing_id} has no exact row route"
            )));
        };
        represented.insert(row.clone());
    }
    Ok(represented)
}

fn replay_effect_local_rows(
    effect: &crate::MergeReplayWriteEffect,
) -> Result<Vec<(String, String, coven_foundation::changeset::ChangeOp)>, DbError> {
    replay_partition_rows(effect.partitions.local.iter())
}

fn replay_effect_rows(
    effect: &crate::MergeReplayWriteEffect,
) -> Result<Vec<(String, String, coven_foundation::changeset::ChangeOp)>, DbError> {
    replay_partition_rows(
        effect
            .partitions
            .store
            .iter()
            .chain(effect.partitions.circles.iter())
            .chain(effect.partitions.local.iter()),
    )
}

fn replay_partition_rows<'a>(
    partitions: impl Iterator<Item = &'a crate::AudiencePartition>,
) -> Result<Vec<(String, String, coven_foundation::changeset::ChangeOp)>, DbError> {
    let rows = partitions
        .map(|partition| crate::walk_changeset(&partition.changeset))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter_map(|change| {
            if crate::is_routing_table(&change.table) {
                return None;
            }
            let row_id = change.pk()?.to_string();
            Some((change.table, row_id, change.op))
        })
        .collect::<Vec<_>>();
    Ok(rows)
}

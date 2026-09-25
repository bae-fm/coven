use super::*;

impl MergeMaterializationTransaction<'_, '_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_unaccepted_replay_effect_inner(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        effect: crate::MergeReplayWriteEffect,
        schema_history: &crate::changeset_migration::ApplicationSchemaHistory,
        schema: std::sync::Arc<TableSchema>,
        gates: &crate::Gates,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        replay_rows: &mut ReplayRows,
    ) -> Result<(), DbError> {
        let mut effect = self.migrate_replay_effect(effect, schema_history)?;
        self.validate_unaccepted_circle_context(authority, root, &effect)?;
        let public_rows =
            replay_effect_public_rows(self.store.transaction, gates, &effect, routing_key)?;
        let joined = match self.local_rows_meeting_shared_rows(
            gates,
            &schema,
            &public_rows,
            &replay_effect_local_rows(&effect)?,
        )? {
            SharedRowMeeting::Joined(joined) => joined,
            SharedRowMeeting::Conflict((table, row_id)) => {
                return Err(Self::local_shared_conflict(&effect.write_id, table, row_id));
            }
        };
        self.drop_joined_private_changes(&mut effect, &joined)?;
        let local_rows = replay_effect_local_rows(&effect)?;
        let changed_rows = replay_effect_rows(&effect)?;
        let partitions = effect
            .partitions
            .store
            .into_iter()
            .chain(effect.partitions.circles)
            .chain(effect.partitions.local);
        self.apply_replay_partitions(&effect.write_id, partitions, schema.clone())?;
        self.validate_recorded_foreign_keys(&effect.write_id, &schema)?;
        if let Some((table, row_id)) = self.update_replay_rows_after_unaccepted_effect(
            gates,
            &schema,
            replay_rows,
            &changed_rows,
            &local_rows,
        )? {
            return Err(Self::local_shared_conflict(&effect.write_id, table, row_id));
        }
        Ok(())
    }

    pub(super) fn validate_unaccepted_circle_context(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        effect: &crate::MergeReplayWriteEffect,
    ) -> Result<(), DbError> {
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
                return invalid_circle_context(effect, partition, circle_id);
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
                    return invalid_circle_context(effect, partition, circle_id);
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
                return invalid_circle_context(effect, partition, circle_id);
            }
        }
        Ok(())
    }

    fn update_replay_rows_after_unaccepted_effect(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        replay_rows: &mut ReplayRows,
        changed_rows: &BTreeSet<(String, String)>,
        local_rows: &BTreeSet<(String, String)>,
    ) -> Result<Option<(String, String)>, DbError> {
        let shared_after = gates.shared_rows(self.store.transaction)?;
        for (table, row_id) in local_rows {
            if shared_after.contains(table, row_id)? {
                return Ok(Some((table.clone(), row_id.clone())));
            }
        }
        for (table, row_id) in changed_rows {
            self.record_replayed_row(schema, replay_rows, table, row_id)?;
        }
        Ok(None)
    }

    pub(super) fn local_shared_conflict(
        write_id: &WriteId,
        table: String,
        row_id: String,
    ) -> DbError {
        coven_protocol::write::WriteRebaseConflict {
            write_id: write_id.clone(),
            affected_rows: vec![coven_protocol::write::AffectedRow {
                table,
                primary_key: row_id,
            }],
            reason: coven_protocol::write::WriteRebaseConflictReason::PrivateShared,
        }
        .into()
    }

    pub(super) fn apply_local_replay_effect(
        &self,
        effect: crate::MergeReplayWriteEffect,
        schema_history: &crate::changeset_migration::ApplicationSchemaHistory,
        schema: std::sync::Arc<TableSchema>,
        gates: &crate::Gates,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        commit: &StoreBatchCommitRef,
        replay_rows: &mut ReplayRows,
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let mut effect = self.migrate_replay_effect(effect, schema_history)?;
        let public_rows =
            replay_effect_public_rows(self.store.transaction, gates, &effect, routing_key)?;
        let joined = match self.local_rows_meeting_shared_rows(
            gates,
            &schema,
            &public_rows,
            &replay_effect_local_rows(&effect)?,
        )? {
            SharedRowMeeting::Joined(joined) => joined,
            SharedRowMeeting::Conflict((table, row_id)) => {
                return Ok(Some(crate::MaterializationHold::PrivateSharedConflict {
                    table,
                    row_id,
                    commit: commit.clone(),
                }));
            }
        };
        self.drop_joined_private_changes(&mut effect, &joined)?;
        let local_rows = replay_effect_local_rows(&effect)?;
        self.apply_replay_partitions(&effect.write_id, effect.partitions.local, schema.clone())?;
        if let Some((table, row_id)) = self.update_private_rows_after_effect(
            gates,
            &schema,
            replay_rows,
            &public_rows,
            &local_rows,
        )? {
            return Ok(Some(crate::MaterializationHold::PrivateSharedConflict {
                table,
                row_id,
                commit: commit.clone(),
            }));
        }
        Ok(None)
    }

    /// The private rows of a replayed write that are already shared here.
    ///
    /// Rows that [join](super::private_shared::joins_shared_row) their shared
    /// copy are returned so the write's private change to them is dropped: the
    /// accepted shared row stands, as it does when a pull meets the private
    /// row. Any other such row is a conflict.
    fn local_rows_meeting_shared_rows(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        public_rows: &BTreeSet<(String, String)>,
        local_rows: &BTreeSet<(String, String)>,
    ) -> Result<SharedRowMeeting, DbError> {
        let shared_before = gates.shared_rows(self.store.transaction)?;
        let mut joined = BTreeSet::new();
        for (table, row_id) in local_rows {
            let key = (table.clone(), row_id.clone());
            if public_rows.contains(&key) || !shared_before.contains(table, row_id)? {
                continue;
            }
            if !super::private_shared::joins_shared_row(gates, schema, table) {
                return Ok(SharedRowMeeting::Conflict(key));
            }
            joined.insert(key);
        }
        Ok(SharedRowMeeting::Joined(joined))
    }

    /// Remove the changes to `joined` rows from the write's private partition.
    fn drop_joined_private_changes(
        &self,
        effect: &mut crate::MergeReplayWriteEffect,
        joined: &BTreeSet<(String, String)>,
    ) -> Result<(), DbError> {
        if joined.is_empty() {
            return Ok(());
        }
        if let Some(partition) = &mut effect.partitions.local {
            let group = crate::gate::Changegroup::new().map_err(DbError::from)?;
            // SAFETY: the iterator passed to `add_change` is the one
            // `for_each_change` is positioned on, and the connection handle
            // outlives the changegroup.
            unsafe {
                group
                    .set_schema(self.store.transaction.handle())
                    .map_err(DbError::from)?;
                crate::gate::for_each_change(&partition.changeset, |iter, change| {
                    let joined_row = change.pk().is_some_and(|row_id| {
                        joined.contains(&(change.table.clone(), row_id.to_string()))
                    });
                    if joined_row {
                        Ok(())
                    } else {
                        group.add_change(iter)
                    }
                })
                .map_err(DbError::from)?;
            }
            partition.changeset = group.output().map_err(DbError::from)?;
        }
        if effect
            .partitions
            .local
            .as_ref()
            .is_some_and(|partition| partition.changeset.is_empty())
        {
            effect.partitions.local = None;
        }
        Ok(())
    }

    fn update_private_rows_after_effect(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
        replay_rows: &mut ReplayRows,
        public_rows: &BTreeSet<(String, String)>,
        local_rows: &BTreeSet<(String, String)>,
    ) -> Result<Option<(String, String)>, DbError> {
        for key in public_rows {
            replay_rows.private.remove(key);
        }
        let shared_after = gates.shared_rows(self.store.transaction)?;
        for (table, row_id) in local_rows {
            if shared_after.contains(table, row_id)? {
                return Ok(Some((table.clone(), row_id.clone())));
            }
            self.record_replayed_row(schema, replay_rows, table, row_id)?;
        }
        Ok(None)
    }

    fn migrate_replay_effect(
        &self,
        mut effect: crate::MergeReplayWriteEffect,
        schema_history: &crate::changeset_migration::ApplicationSchemaHistory,
    ) -> Result<crate::MergeReplayWriteEffect, DbError> {
        for partition in effect
            .partitions
            .store
            .iter_mut()
            .chain(effect.partitions.circles.iter_mut())
            .chain(effect.partitions.local.iter_mut())
        {
            partition.changeset = schema_history.migrate(
                self.store.transaction,
                effect.schema_version,
                &partition.changeset,
            )?;
        }
        Ok(effect)
    }

    fn apply_replay_partitions(
        &self,
        write_id: &WriteId,
        partitions: impl IntoIterator<Item = crate::AudiencePartition>,
        schema: std::sync::Arc<TableSchema>,
    ) -> Result<(), DbError> {
        self.store
            .transaction
            .pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::context(format!("local replay write {write_id} changeset"), error)
                })?;
            self.apply_recorded_changeset(changeset, write_id)?;
        }
        Ok(())
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

/// How a replayed write's private rows meet rows already shared here.
enum SharedRowMeeting {
    /// Every such row joins its shared copy; these are the rows.
    Joined(BTreeSet<(String, String)>),
    /// This row cannot join the shared row it meets.
    Conflict((String, String)),
}

fn invalid_circle_context(
    effect: &crate::MergeReplayWriteEffect,
    partition: &crate::AudiencePartition,
    circle_id: coven_protocol::circle::CircleId,
) -> Result<(), DbError> {
    let affected_rows = replay_partition_rows(std::iter::once(partition))?
        .into_iter()
        .map(|(table, primary_key)| coven_protocol::write::AffectedRow { table, primary_key })
        .collect();
    Err(coven_protocol::write::WriteRebaseConflict {
        write_id: effect.write_id.clone(),
        affected_rows,
        reason: coven_protocol::write::WriteRebaseConflictReason::InvalidCircleContext {
            circle_id,
        },
    }
    .into())
}

pub(super) fn replay_effect_local_rows(
    effect: &crate::MergeReplayWriteEffect,
) -> Result<BTreeSet<(String, String)>, DbError> {
    replay_partition_rows(effect.partitions.local.iter())
}

fn replay_effect_rows(
    effect: &crate::MergeReplayWriteEffect,
) -> Result<BTreeSet<(String, String)>, DbError> {
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
) -> Result<BTreeSet<(String, String)>, DbError> {
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
            Some((change.table, row_id))
        })
        .collect();
    Ok(rows)
}

#[cfg(test)]
#[path = "replay_effect_tests.rs"]
mod tests;

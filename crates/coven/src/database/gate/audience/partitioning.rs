use super::routing::*;
use super::*;

/// Every audience move a captured write performs, with the component of rows each
/// one drags and the stamp it moves them at. Reads only, so a caller can decide
/// what a move obliges it to change before the write is partitioned for real —
/// partitioning writes the routing rows the move implies, and doing that twice
/// would leave the second pass with nothing to publish.
pub(crate) fn audience_moves(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<Vec<AudienceMove>, GateError> {
    let mut moves = Vec::new();
    unsafe {
        for_each_change(changeset, |_iter, row| {
            if !gates.tables.contains_key(&row.table) {
                return Ok(());
            }
            let Some((source, destination)) = row_audience_move(conn, gates, &row)? else {
                return Ok(());
            };
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            let seed = (row.table.clone(), row_id.to_string());
            let stamp = live_row_stamp(conn, &seed.0, &seed.1)?;
            let component = scoped_materialization_rows(conn, gates, seed)?;
            moves.push(AudienceMove {
                source,
                destination,
                rows: component.into_iter().collect(),
                stamp,
            });
            Ok(())
        })?;
    }
    Ok(moves)
}

pub(crate) fn partition_outbound(
    conn: &Connection,
    changeset: &[u8],
    routing: &RoutingChanges,
    gates: &Gates,
) -> Result<PartitionedAudienceWrite, GateError> {
    unsafe {
        let mut groups = AudiencePartitionGroups::new(conn, gates);
        let audience_moves = audience_moves(conn, changeset, gates)?;
        let mut ancestor_inserts = HashSet::new();
        let mut ancestor_deletes = HashSet::new();
        let captured_deletes = collect_deletes(changeset)?;
        let mut non_local_deletes = HashSet::new();
        let deleted_audiences = captured_deleted_audiences(conn, &captured_deletes, gates)?;
        let store_changeset = gate_store_outbound(conn, changeset, gates)?;
        let mut store_rows = HashSet::new();
        for_each_change(&store_changeset, |iter, row| {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            store_rows.insert((row.table.clone(), row_id.to_string()));
            groups.group(Audience::Store)?.group.add_change(iter)?;
            Ok(())
        })?;
        for_each_change(changeset, |iter, row| {
            if !gates.tables.contains_key(&row.table) {
                return Ok(());
            }
            if row_audience_move(conn, gates, &row)?.is_some() {
                return Ok(());
            }
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            let row_key = (row.table.clone(), row_id.to_string());
            let audience = if !gates.table_is_scoped(&row.table) {
                if store_rows.contains(&row_key) {
                    Audience::Store
                } else {
                    Audience::Local
                }
            } else if row.op == ffi::SQLITE_DELETE {
                routing
                    .deleted_rows
                    .get(&row_key)
                    .or_else(|| deleted_audiences.get(&row_key))
                    .cloned()
                    .map(Ok)
                    .unwrap_or_else(|| change_audience(conn, gates, &row))?
            } else {
                change_audience(conn, gates, &row)?
            };
            if audience != Audience::Local {
                if row.op == ffi::SQLITE_INSERT {
                    let component = scoped_materialization_rows(conn, gates, row_key.clone())?;
                    ancestor_inserts.extend(required_store_ancestors(conn, gates, &component)?);
                } else if row.op == ffi::SQLITE_DELETE {
                    non_local_deletes.insert(row_key);
                }
            }
            if gates.table_is_scoped(&row.table) || audience == Audience::Local {
                let partition = groups.group(audience)?;
                partition.group.add_change(iter)?;
            }
            Ok(())
        })?;
        for (table, id) in required_store_ancestors_for_deleted_rows(
            conn,
            gates,
            &captured_deletes,
            &non_local_deletes,
        )? {
            if !gates.row_kept(conn, &table, &id)? {
                ancestor_deletes.insert((table, id));
            }
        }
        for audience_move in &audience_moves {
            let component = audience_move.rows.iter().cloned().collect::<HashSet<_>>();
            let ancestors = required_store_ancestors(conn, gates, &component)?;
            if audience_move.source == Audience::Local
                && audience_move.destination != Audience::Local
            {
                ancestor_inserts.extend(ancestors.iter().cloned());
            } else if audience_move.source != Audience::Local
                && audience_move.destination == Audience::Local
            {
                for (table, id) in ancestors {
                    if !gates.row_kept(conn, &table, &id)? {
                        ancestor_deletes.insert((table, id));
                    }
                }
            }
            if audience_move.destination != Audience::Local {
                groups.add_materialization(
                    &component,
                    FullStateDirection::Inserts,
                    audience_move.destination.clone(),
                )?;
            }
        }
        if !ancestor_inserts.is_empty() {
            groups.add_materialization(
                &ancestor_inserts,
                FullStateDirection::Inserts,
                Audience::Store,
            )?;
        }
        if !ancestor_deletes.is_empty() {
            groups.add_materialization(
                &ancestor_deletes,
                FullStateDirection::Deletes,
                Audience::Store,
            )?;
        }
        for_each_change(&routing.store_mirror, |iter, row| {
            if row.table != "_coven_audience" {
                return Err(GateError::Sql(
                    format!("unexpected Store mirror changeset table {}", row.table),
                    rusqlite::Error::InvalidQuery,
                ));
            }
            groups.group(Audience::Store)?.group.add_change(iter)?;
            Ok(())
        })?;
        for (audience, routes) in &routing.private_routes {
            for_each_change(routes, |iter, row| {
                if row.table != "_coven_row_routes" || row.op != ffi::SQLITE_INSERT {
                    return Err(GateError::InvalidInboundAudiencePackage(
                        "generated private routes must be complete INSERT images".to_string(),
                    ));
                }
                groups.group(audience.clone())?.group.add_change(iter)?;
                Ok(())
            })?;
        }
        let partitions = groups.finish()?;
        Ok(PartitionedAudienceWrite {
            partitions,
            moves: audience_moves,
        })
    }
}

pub(crate) fn validate_scoped_foreign_key_audiences(
    conn: &Connection,
    gates: &Gates,
) -> Result<(), GateError> {
    let mut tables = gates
        .tables
        .keys()
        .filter(|table| gates.table_is_scoped(table))
        .cloned()
        .collect::<Vec<_>>();
    tables.sort();
    for table in tables {
        let row_ids = query_mapped_rows(
            conn,
            &format!("SELECT id FROM {}", quote_ident(&table)),
            [],
            |row| row.get::<_, String>(0),
        )?;
        for row_id in row_ids {
            let row_audience = live_row_audience(conn, gates, &table, &row_id)?;
            for (fk_column, parent_table, parent_column) in foreign_keys(conn, &table)? {
                if !gates.is_synced_table(&parent_table) {
                    continue;
                }
                let parent_id = match fk_parent_row(
                    conn,
                    &table,
                    &row_id,
                    &fk_column,
                    &parent_table,
                    &parent_column,
                )? {
                    FkParentRow::Found(parent_id) => parent_id,
                    FkParentRow::NullForeignKey => continue,
                    FkParentRow::RowAbsent => {
                        return Err(GateError::MissingAudienceRow {
                            table: table.clone(),
                            row_id: row_id.clone(),
                        })
                    }
                    FkParentRow::ParentAbsent => {
                        return Err(GateError::MissingAudienceParent {
                            table: table.clone(),
                            row_id: Some(row_id.clone()),
                            parent: parent_table.clone(),
                        })
                    }
                };
                let parent_audience = live_row_audience(conn, gates, &parent_table, &parent_id)?;
                if parent_audience != Audience::Store && parent_audience != row_audience {
                    return Err(GateError::InvalidAudience {
                        table: table.clone(),
                        value: row_audience.column_value(),
                        reason: format!(
                            "relationship through {fk_column} references {parent_table}.{parent_id} in {parent_audience:?}"
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

pub(super) struct AudiencePartitionGroups<'connection> {
    connection: &'connection Connection,
    gates: &'connection Gates,
    groups: BTreeMap<Audience, PartitionGroup>,
}

impl<'connection> AudiencePartitionGroups<'connection> {
    fn new(connection: &'connection Connection, gates: &'connection Gates) -> Self {
        Self {
            connection,
            gates,
            groups: BTreeMap::new(),
        }
    }

    unsafe fn add_materialization(
        &mut self,
        component: &HashSet<(String, String)>,
        direction: FullStateDirection,
        audience: Audience,
    ) -> Result<(), GateError> {
        let materialization = full_state_diff(self.connection, self.gates, direction)?;
        let partition = self.group(audience)?;
        for_each_change(&materialization, |iter, row| {
            if row
                .pk()
                .is_some_and(|id| component.contains(&(row.table.clone(), id.to_string())))
            {
                partition.group.add_change(iter)?;
            }
            Ok(())
        })
    }

    unsafe fn group(&mut self, audience: Audience) -> Result<&mut PartitionGroup, GateError> {
        let control = match audience {
            Audience::Circle(circle_id) => Some(active_circle_control(self.connection, circle_id)?),
            Audience::Store | Audience::Local => None,
        };
        match self.groups.entry(audience) {
            std::collections::btree_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let group = Changegroup::new()?;
                group.set_schema(self.connection.handle())?;
                Ok(entry.insert(PartitionGroup { control, group }))
            }
        }
    }

    fn finish(self) -> Result<Vec<AudiencePartition>, GateError> {
        self.groups
            .into_iter()
            .map(|(audience, group)| {
                Ok(AudiencePartition {
                    audience,
                    control: group.control,
                    changeset: group.group.output()?,
                })
            })
            .collect()
    }
}

pub(super) fn row_audience_move(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Option<(Audience, Audience)>, GateError> {
    if row.op != ffi::SQLITE_UPDATE {
        return Ok(None);
    }
    let (source, destination) = match gates.tables.get(&row.table) {
        Some(TableGate::ScopedRoot { audience_col }) => {
            let Some(destination_value) = row.new_value(audience_col.index) else {
                return Ok(None);
            };
            let source_value = row.old_value(audience_col.index).unwrap_or(None);
            let parse = |value: Option<&str>| {
                Audience::from_column(value).map_err(|error| GateError::InvalidAudience {
                    table: row.table.clone(),
                    value: value.map(str::to_string),
                    reason: error.to_string(),
                })
            };
            (parse(source_value)?, parse(destination_value)?)
        }
        Some(TableGate::Root { gate_col }) => {
            let Some(destination_value) = row.new_value(gate_col.index) else {
                return Ok(None);
            };
            let source_value = row.old_value(gate_col.index).flatten();
            let audience = |value: Option<&str>| {
                if value.is_some_and(truthy) {
                    Audience::Store
                } else {
                    Audience::Local
                }
            };
            (audience(source_value), audience(destination_value))
        }
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let Some(destination_key) = row.new_value(fk_col.index) else {
                return Ok(None);
            };
            let row_id = row.pk().map(str::to_string);
            let source_key = row.old_value(fk_col.index).flatten().ok_or_else(|| {
                GateError::MissingAudienceParent {
                    table: row.table.clone(),
                    row_id: row_id.clone(),
                    parent: parent.clone(),
                }
            })?;
            let destination_key =
                destination_key.ok_or_else(|| GateError::MissingAudienceParent {
                    table: row.table.clone(),
                    row_id: row_id.clone(),
                    parent: parent.clone(),
                })?;
            let resolve = |parent_key: &str| {
                let parent_id =
                    row_id_for_column_value(conn, parent, &parent_col.name, parent_key)?
                        .ok_or_else(|| GateError::MissingAudienceParent {
                            table: row.table.clone(),
                            row_id: row_id.clone(),
                            parent: parent.clone(),
                        })?;
                live_row_audience(conn, gates, parent, &parent_id)
            };
            (resolve(source_key)?, resolve(destination_key)?)
        }
        None | Some(TableGate::RemoteRoot) | Some(TableGate::Parent { .. }) => return Ok(None),
    };
    Ok((source != destination).then_some((source, destination)))
}

pub(super) fn change_audience(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Audience, GateError> {
    match gates.tables.get(&row.table) {
        Some(TableGate::ScopedRoot { audience_col }) => {
            let value = match row.op {
                op if op == ffi::SQLITE_INSERT => row
                    .new
                    .get(audience_col.index)
                    .and_then(|value| value.as_deref()),
                op if op == ffi::SQLITE_DELETE => row
                    .old
                    .get(audience_col.index)
                    .and_then(|value| value.as_deref()),
                _ => {
                    if let Some(changed) = row.new_value(audience_col.index) {
                        changed
                    } else {
                        return row
                            .pk()
                            .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))
                            .and_then(|id| live_row_audience(conn, gates, &row.table, id));
                    }
                }
            };
            Audience::from_column(value).map_err(|error| GateError::InvalidAudience {
                table: row.table.clone(),
                value: value.map(str::to_string),
                reason: error.to_string(),
            })
        }
        Some(TableGate::Root { gate_col }) => {
            let value = match row.op {
                op if op == ffi::SQLITE_INSERT => row
                    .new
                    .get(gate_col.index)
                    .and_then(|value| value.as_deref()),
                _ => {
                    let row_id = row
                        .pk()
                        .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
                    return live_row_audience(conn, gates, &row.table, row_id);
                }
            };
            Ok(if value.is_some_and(truthy) {
                Audience::Store
            } else {
                Audience::Local
            })
        }
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let parent_key = if let Some(value) = row.fk_value(fk_col.index) {
                value.to_string()
            } else {
                let id = row
                    .pk()
                    .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
                query_column_text(conn, &row.table, &fk_col.name, id)?.ok_or_else(|| {
                    GateError::MissingAudienceParent {
                        table: row.table.clone(),
                        row_id: Some(id.to_string()),
                        parent: fk_col.name.clone(),
                    }
                })?
            };
            let parent_id = row_id_for_column_value(conn, parent, &parent_col.name, &parent_key)?
                .ok_or_else(|| GateError::MissingAudienceParent {
                table: row.table.clone(),
                row_id: row.pk().map(str::to_string),
                parent: parent.clone(),
            })?;
            live_row_audience(conn, gates, parent, &parent_id)
        }
        Some(TableGate::RemoteRoot) | Some(TableGate::Parent { .. }) => Ok(Audience::Store),
        None => {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            Err(GateError::MissingAudienceRow {
                table: row.table.clone(),
                row_id: row_id.to_string(),
            })
        }
    }
}

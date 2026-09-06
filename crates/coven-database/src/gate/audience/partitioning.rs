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
        let shared = gates.shared_rows(conn)?;
        let mut groups = AudiencePartitionGroups::new(conn, gates);
        let audience_moves = audience_moves(conn, changeset, gates)?;
        let mut local_retained_rows = HashSet::new();
        let mut ancestor_deletes = HashSet::new();
        let mut ancestor_inserts = HashSet::new();
        for audience_move in &audience_moves {
            let component = audience_move.rows.iter().cloned().collect::<HashSet<_>>();
            let ancestors = required_store_ancestors(conn, gates, &component)?;
            if audience_move.source == Audience::Local
                && audience_move.destination != Audience::Local
            {
                ancestor_inserts.extend(ancestors);
            } else if audience_move.source != Audience::Local
                && audience_move.destination == Audience::Local
            {
                local_retained_rows.extend(component);
                for (table, id) in ancestors {
                    if !shared.contains(&table, &id)? {
                        ancestor_deletes.insert((table, id));
                    }
                }
            }
        }
        let captured_deletes = collect_deletes(changeset)?;
        let mut non_local_deletes = HashSet::new();
        let deleted_audiences =
            captured_deleted_audiences(conn, &captured_deletes, gates, &shared)?;
        let store_changeset = gate_store_outbound(conn, changeset, gates)?;
        let mut store_rows = HashSet::new();
        for_each_change(&store_changeset, |_iter, row| {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            store_rows.insert((row.table.clone(), row_id.to_string()));
            Ok(())
        })?;
        let mut change_audiences = HashMap::new();
        for_each_change(changeset, |_iter, row| {
            if !gates.tables.contains_key(&row.table) {
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
                    non_local_deletes.insert(row_key.clone());
                }
            }
            change_audiences.insert(row_key, audience);
            Ok(())
        })?;
        for (table, id) in required_store_ancestors_for_deleted_rows(
            conn,
            gates,
            &captured_deletes,
            &non_local_deletes,
        )? {
            if !shared.contains(&table, &id)? {
                ancestor_deletes.insert((table, id));
            }
        }
        local_retained_rows.extend(ancestor_deletes.iter().cloned());
        loop {
            let prior_len = local_retained_rows.len();
            let ancestors = required_store_ancestors(conn, gates, &local_retained_rows)?;
            for ancestor in ancestors {
                if !shared.contains(&ancestor.0, &ancestor.1)? {
                    local_retained_rows.insert(ancestor);
                }
            }
            let retained_ancestors = local_retained_rows
                .iter()
                .filter(|(table, _)| {
                    matches!(gates.tables.get(table), Some(TableGate::Parent { .. }))
                })
                .cloned()
                .collect::<Vec<_>>();
            for ancestor in retained_ancestors {
                for row in scoped_materialization_rows(conn, gates, ancestor)? {
                    if !shared.contains(&row.0, &row.1)? {
                        local_retained_rows.insert(row);
                    }
                }
            }
            if local_retained_rows.len() == prior_len {
                break;
            }
        }
        let destination_materialized_rows = audience_moves
            .iter()
            .filter(|audience_move| audience_move.destination != Audience::Local)
            .flat_map(|audience_move| audience_move.rows.iter().cloned())
            .collect::<HashSet<_>>();
        for_each_change(&store_changeset, |iter, row| {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            if !destination_materialized_rows.contains(&(row.table.clone(), row_id.to_string())) {
                groups.group(Audience::Store)?.group.add_change(iter)?;
            }
            Ok(())
        })?;
        for_each_change(changeset, |iter, row| {
            if !gates.tables.contains_key(&row.table) {
                return Ok(());
            }
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            let row_key = (row.table.clone(), row_id.to_string());
            if row_audience_move(conn, gates, &row)?.is_some()
                || destination_materialized_rows.contains(&row_key)
                || local_retained_rows.contains(&row_key)
            {
                return Ok(());
            }
            let audience =
                change_audiences
                    .get(&row_key)
                    .ok_or_else(|| GateError::MissingAudienceRow {
                        table: row.table.clone(),
                        row_id: row_id.to_string(),
                    })?;
            if gates.table_is_scoped(&row.table) || *audience == Audience::Local {
                let partition = groups.group(audience.clone())?;
                partition.group.add_change(iter)?;
            }
            Ok(())
        })?;
        for audience_move in &audience_moves {
            let component = audience_move.rows.iter().cloned().collect::<HashSet<_>>();
            if audience_move.destination != Audience::Local {
                groups.add_materialization(
                    &component,
                    FullStateDirection::Inserts,
                    audience_move.destination.clone(),
                )?;
            }
        }
        if !local_retained_rows.is_empty() {
            groups.add_materialization(
                &local_retained_rows,
                FullStateDirection::Inserts,
                Audience::Local,
            )?;
            let local_routes = local_retained_rows
                .iter()
                .filter(|(table, _)| gates.table_is_scoped(table))
                .map(|(table, row_id)| {
                    query_row_optional(
                        conn,
                        "SELECT routing_id, table_name, row_id, _updated_at
                         FROM _coven_row_routes
                         WHERE table_name = ?1 AND row_id = ?2",
                        (table, row_id),
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )?
                    .ok_or_else(|| GateError::MissingAudienceRow {
                        table: table.clone(),
                        row_id: row_id.clone(),
                    })
                })
                .collect::<Result<Vec<_>, GateError>>()?;
            if !local_routes.is_empty() {
                let routes = private_route_insert_changeset(&local_routes)?;
                for_each_change(&routes, |iter, _row| {
                    groups.group(Audience::Local)?.group.add_change(iter)?;
                    Ok(())
                })?;
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
            let materialization =
                pre_write_full_state_diff(conn, gates, changeset, FullStateDirection::Deletes)?;
            groups.add_materialization_changeset(
                &materialization,
                &ancestor_deletes,
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
        validate_store_partition_foreign_key_closure(conn, gates, &shared, &partitions)?;
        Ok(PartitionedAudienceWrite {
            partitions,
            moves: audience_moves,
        })
    }
}

/// Refuse a captured write whose Store rows carry a foreign key the shared set
/// does not resolve.
///
/// Every device materializes the Store by replaying its published commits into
/// an empty database, so a shared row's foreign keys have to name rows that are
/// themselves shared. [`SharedRows`] makes that true by construction, and this
/// says so out loud at the one place it still can: the host's own write
/// transaction, before the partition becomes a package, a commit, and an object
/// in the cloud. Past that point the wrong state is durable and no device can
/// apply it — the replay holds on the missing parent, makes no progress, and
/// fails, cycle after cycle, with nothing on any device able to supply the row.
///
/// A parent counts as resolved if this same write carries it or the shared set
/// already holds it. The second half leans on the emission being complete rather
/// than on publication order: `reemit_subtrees` emits a flipped root's whole
/// connected shared component, so a shared parent was published no later than the
/// row that names it.
///
/// Only the Store partition is checked here. A Circle partition's parents are a
/// question about audiences rather than about sharing, answered by
/// [`validate_scoped_foreign_key_audiences`] before the write is captured at all.
///
/// # Safety
/// `changeset` iteration reads raw session bytes; each partition's changeset came
/// from a changegroup built on this connection's schema.
unsafe fn validate_store_partition_foreign_key_closure(
    conn: &Connection,
    gates: &Gates,
    shared: &SharedRows<'_>,
    partitions: &[AudiencePartition],
) -> Result<(), GateError> {
    let Some(store) = partitions
        .iter()
        .find(|partition| partition.audience == Audience::Store)
    else {
        return Ok(());
    };

    // A row this write itself carries resolves a reference to it, so collect the
    // partition's own rows before testing any of them. Only a gated row can be a
    // gated row's parent, which is also why an ungated table needs no entry.
    let mut carried: HashSet<(String, String)> = HashSet::new();
    for_each_change(&store.changeset, |_iter, row| {
        if row.op == ffi::SQLITE_DELETE || !gates.tables.contains_key(&row.table) {
            return Ok(());
        }
        if let Some(id) = row.pk() {
            carried.insert((row.table.clone(), id.to_string()));
        }
        Ok(())
    })?;

    // The foreign keys each table holds into a gated table, read once per table
    // rather than once per row: a release publishes hundreds of rows over a
    // handful of tables.
    let mut gated_parents: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    for_each_change(&store.changeset, |_iter, row| {
        // A DELETE carries no reference of its own. The mirror obligation — never
        // retracting a row something shared still names — is the retract path's
        // shared-set filter, not a property of these bytes.
        if row.op == ffi::SQLITE_DELETE {
            return Ok(());
        }
        let edges = match gated_parents.entry(row.table.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let edges = foreign_keys(conn, &row.table)?
                    .into_iter()
                    .filter(|(_, parent, _)| gates.tables.contains_key(parent))
                    .collect::<Vec<_>>();
                entry.insert(edges)
            }
        };
        if edges.is_empty() {
            return Ok(());
        }
        let Some(row_id) = row.pk().map(str::to_string) else {
            return Err(GateError::MissingChangesetPrimaryKey(row.table.clone()));
        };
        for (fk_column, parent, parent_column) in edges.clone() {
            let parent_id = match fk_parent_row(
                conn,
                &row.table,
                &row_id,
                &fk_column,
                &parent,
                &parent_column,
            )? {
                FkParentRow::Found(parent_id) => parent_id,
                FkParentRow::NullForeignKey | FkParentRow::RowAbsent => continue,
                FkParentRow::ParentAbsent => {
                    return Err(GateError::UnsharedForeignKeyParent(Box::new(
                        UnsharedForeignKeyParent {
                            table: row.table.clone(),
                            row_id,
                            column: fk_column,
                            parent,
                            parent_id: None,
                        },
                    )))
                }
            };
            if carried.contains(&(parent.clone(), parent_id.clone()))
                || shared.contains(&parent, &parent_id)?
            {
                continue;
            }
            return Err(GateError::UnsharedForeignKeyParent(Box::new(
                UnsharedForeignKeyParent {
                    table: row.table.clone(),
                    row_id,
                    column: fk_column,
                    parent,
                    parent_id: Some(parent_id),
                },
            )));
        }
        Ok(())
    })
}

pub(crate) fn validate_scoped_foreign_key_audiences(
    conn: &Connection,
    gates: &Gates,
) -> Result<(), GateError> {
    for table in gates.scoped_table_names() {
        for row_id in all_row_ids(conn, &table)? {
            let row_audience = live_row_audience(conn, gates, &table, &row_id)?;
            compatible_parent_rows(conn, gates, &table, &row_id, &row_audience)?;
        }
    }
    Ok(())
}

/// Refuse an accepted row whose foreign key can only be satisfied by private
/// state. The private-row set carries provenance across the temporary state in
/// which an accepted parent deletion can leave its accepted child dangling;
/// deriving sharing from that state would misclassify the child as private.
pub(crate) fn validate_accepted_foreign_key_closure(
    conn: &Connection,
    gates: &Gates,
    private_rows: &std::collections::BTreeSet<(String, String)>,
) -> Result<(), GateError> {
    for table in gates.sorted_synced_table_names() {
        for row_id in all_row_ids(conn, &table)? {
            if private_rows.contains(&(table.clone(), row_id.clone())) {
                continue;
            }
            for (fk_column, parent, parent_column) in foreign_keys(conn, &table)? {
                if !gates.is_synced_table(&parent) {
                    continue;
                }
                let parent_id = match fk_parent_row(
                    conn,
                    &table,
                    &row_id,
                    &fk_column,
                    &parent,
                    &parent_column,
                )? {
                    FkParentRow::Found(parent_id) => parent_id,
                    FkParentRow::NullForeignKey => continue,
                    FkParentRow::RowAbsent | FkParentRow::ParentAbsent => {
                        return Err(GateError::UnsharedForeignKeyParent(Box::new(
                            UnsharedForeignKeyParent {
                                table,
                                row_id,
                                column: fk_column,
                                parent,
                                parent_id: None,
                            },
                        )))
                    }
                };
                if private_rows.contains(&(parent.clone(), parent_id.clone())) {
                    return Err(GateError::UnsharedForeignKeyParent(Box::new(
                        UnsharedForeignKeyParent {
                            table,
                            row_id,
                            column: fk_column,
                            parent,
                            parent_id: Some(parent_id),
                        },
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(crate) struct AudiencePartitionGroups<'connection> {
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
        self.add_materialization_changeset(&materialization, component, audience)
    }

    unsafe fn add_materialization_changeset(
        &mut self,
        materialization: &[u8],
        component: &HashSet<(String, String)>,
        audience: Audience,
    ) -> Result<(), GateError> {
        let partition = self.group(audience)?;
        for_each_change(materialization, |iter, row| {
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

pub(crate) fn row_audience_move(
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
                Audience::from_column(value).map_err(|source| GateError::InvalidAudienceEncoding {
                    table: row.table.clone(),
                    value: value.map(str::to_string),
                    source,
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

/// The audience this change puts its row in: what the change itself records,
/// and otherwise what the live row already says. A change records the column
/// that decides the audience only when it wrote it — an INSERT's new image, a
/// DELETE's old image, an UPDATE's value only if that update changed it — so
/// every other case is the live resolution, which walks the same gate model
/// against the db.
pub(crate) fn change_audience(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Audience, GateError> {
    let live_audience = |table: &str, id: &str| live_row_audience(conn, gates, table, id);
    let live_row = || {
        let id = row
            .pk()
            .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
        live_audience(&row.table, id)
    };
    match gates.tables.get(&row.table) {
        Some(TableGate::ScopedRoot { audience_col }) => {
            match recorded_column(row, audience_col.index) {
                Some(value) => Audience::from_column(value).map_err(|source| {
                    GateError::InvalidAudienceEncoding {
                        table: row.table.clone(),
                        value: value.map(str::to_string),
                        source,
                    }
                }),
                None => live_row(),
            }
        }
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => match row.fk_value(fk_col.index) {
            // The change repointed (or first set) the foreign key, so the parent
            // it now names decides the audience, not the one the live row holds.
            Some(parent_key) => {
                let parent_id =
                    row_id_for_column_value(conn, parent, &parent_col.name, parent_key)?
                        .ok_or_else(|| GateError::MissingAudienceParent {
                            table: row.table.clone(),
                            row_id: row.pk().map(str::to_string),
                            parent: parent.clone(),
                        })?;
                live_audience(parent, &parent_id)
            }
            None => live_row(),
        },
        // Only a scoped table reaches this: an unscoped row's audience follows
        // from its gate, which the live resolution reads directly.
        _ => live_row(),
    }
}

/// The value this change records for the column at `index`, following op
/// semantics: an INSERT's new image, a DELETE's old image, and for an UPDATE the
/// new value only when the update changed that column. `None` when the change
/// does not record it, so the caller reads the live row instead.
fn recorded_column(row: &ChangeRow, index: usize) -> Option<Option<&str>> {
    match row.op {
        op if op == ffi::SQLITE_INSERT => row.new.get(index).map(|value| value.as_deref()),
        op if op == ffi::SQLITE_DELETE => row.old.get(index).map(|value| value.as_deref()),
        _ => row.new_value(index),
    }
}

use super::inbound::winning_store_audience;
use super::partitioning::*;
use super::*;

pub(crate) fn captured_deleted_audiences(
    conn: &Connection,
    deleted: &HashMap<(String, String), ChangeRow>,
    gates: &Gates,
    shared: &SharedRows<'_>,
) -> Result<BTreeMap<(String, String), Audience>, GateError> {
    let mut audiences = BTreeMap::new();
    let mut resolution =
        DeletedAudiences::new(conn, gates, shared, deleted, UnresolvedAudience::Rejected);
    for key in deleted
        .keys()
        .filter(|(table, _)| gates.tables.contains_key(table))
    {
        let audience = resolution.audience(key)?;
        audiences.insert(key.clone(), audience);
    }
    Ok(audiences)
}

pub(crate) fn capture_routing_changes(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
    key: &RowRoutingKey,
) -> Result<RoutingChanges, GateError> {
    let mut session =
        rusqlite::session::Session::new(conn).map_err(|source| GateError::Session {
            operation: "create routing journal".to_string(),
            source,
        })?;
    session
        .attach(Some("_coven_audience"))
        .map_err(|source| GateError::Session {
            operation: "attach Store audience mirror".to_string(),
            source,
        })?;

    let transitions = routing_transitions(conn, changeset, gates)?;
    // The mirror is where a deleted row's audience is recorded, and the writes
    // below remove it, so every deletion is answered first.
    let deleted_rows = deleted_row_audiences(conn, changeset, gates, key, &transitions)?;
    for ((table, row_id), transition) in transitions {
        let routing_id = row_routing_id(key, &table, &row_id).to_string();
        // A deletion and a move to Local both leave the row with no public
        // audience, so both retract the mirror.
        let mirrored = match transition {
            RoutingTransition::Delete => None,
            RoutingTransition::Set { audience, stamp } => match audience {
                Audience::Local => None,
                Audience::Store => Some((None, stamp)),
                Audience::Circle(circle_id) => Some((Some(circle_id.to_string()), stamp)),
            },
        };
        let Some((circle_id, stamp)) = mirrored else {
            conn.execute(
                "DELETE FROM _coven_audience WHERE routing_id = ?1",
                [&routing_id],
            )
            .map_err(|source| GateError::Sql("delete Store audience mirror".to_string(), source))?;
            continue;
        };
        conn.execute(
            "INSERT INTO _coven_audience (routing_id, circle_id, _updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(routing_id) DO UPDATE SET
                 circle_id = excluded.circle_id,
                 _updated_at = excluded._updated_at",
            (&routing_id, circle_id, &stamp),
        )
        .map_err(|source| GateError::Sql("persist Store audience mirror".to_string(), source))?;
    }

    let mut out = Vec::new();
    session
        .changeset_strm(&mut out)
        .map_err(|source| GateError::Session {
            operation: "extract routing journal".to_string(),
            source,
        })?;
    Ok(RoutingChanges {
        store_mirror: out,
        deleted_rows,
    })
}

/// Where each row this write deletes was before the write, read from the public
/// mirror it is about to lose. A row with no mirror never had a public audience,
/// which is only true of a Local row — resolved on the state before this write,
/// because the same write can move the row's scoped root to Local while deleting
/// the row, and the live value then describes the move rather than the row's
/// standing audience. Anything else is a mirror that went missing, which no
/// later pass can reconstruct.
fn deleted_row_audiences(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
    key: &RowRoutingKey,
    transitions: &BTreeMap<(String, String), RoutingTransition>,
) -> Result<BTreeMap<(String, String), Audience>, GateError> {
    let mut audiences = BTreeMap::new();
    let mut unmirrored = Vec::new();
    for ((table, row_id), transition) in transitions {
        if !matches!(transition, RoutingTransition::Delete) {
            continue;
        }
        let routing_id = row_routing_id(key, table, row_id).to_string();
        match winning_store_audience(conn, &routing_id)? {
            Some(audience) => {
                audiences.insert((table.clone(), row_id.clone()), audience);
            }
            None => unmirrored.push((table.clone(), row_id.clone())),
        }
    }
    if unmirrored.is_empty() {
        return Ok(audiences);
    }
    with_pre_write_synced_projection(conn, gates, changeset, |before| {
        for (table, row_id) in &unmirrored {
            let audience = live_row_audience(before, gates, table, row_id)?;
            if audience != Audience::Local {
                return Err(GateError::UnmirroredDeletedRow {
                    table: table.clone(),
                    row_id: row_id.clone(),
                    audience,
                });
            }
            audiences.insert((table.clone(), row_id.clone()), Audience::Local);
        }
        Ok(())
    })?;
    Ok(audiences)
}

pub(crate) enum RoutingTransition {
    Set { audience: Audience, stamp: String },
    Delete,
}

pub(crate) fn routing_transitions(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<BTreeMap<(String, String), RoutingTransition>, GateError> {
    let mut transitions = BTreeMap::new();
    unsafe {
        for_each_change(changeset, |_iter, row| {
            if !gates.table_is_scoped(&row.table) {
                return Ok(());
            }
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            if row.op == ffi::SQLITE_DELETE {
                transitions.insert(
                    (row.table.clone(), row_id.to_string()),
                    RoutingTransition::Delete,
                );
                return Ok(());
            }
            if row.op == ffi::SQLITE_INSERT {
                let audience = live_row_audience(conn, gates, &row.table, row_id)?;
                let stamp = live_row_stamp(conn, &row.table, row_id)?;
                transitions.insert(
                    (row.table.clone(), row_id.to_string()),
                    RoutingTransition::Set { audience, stamp },
                );
                return Ok(());
            }
            let Some((_source, destination)) = row_audience_move(conn, gates, &row)? else {
                return Ok(());
            };
            let stamp = live_row_stamp(conn, &row.table, row_id)?;
            let component =
                scoped_materialization_rows(conn, gates, (row.table.clone(), row_id.to_string()))?;
            for (table, id) in component {
                if gates.table_is_scoped(&table) {
                    transitions.insert(
                        (table, id),
                        RoutingTransition::Set {
                            audience: destination.clone(),
                            stamp: stamp.clone(),
                        },
                    );
                }
            }
            Ok(())
        })?;
    }
    Ok(transitions)
}

pub(crate) fn scoped_materialization_rows(
    conn: &Connection,
    gates: &Gates,
    seed: (String, String),
) -> Result<HashSet<(String, String)>, GateError> {
    let mut rows = HashSet::new();
    let mut pending = vec![seed];
    while let Some((table, row_id)) = pending.pop() {
        if !rows.insert((table.clone(), row_id.clone())) {
            continue;
        }
        for (child_table, gate) in &gates.tables {
            let TableGate::Child {
                fk_col,
                parent,
                parent_col,
            } = gate
            else {
                continue;
            };
            if parent != &table {
                continue;
            }
            let parent_key = query_column_text(conn, &table, &parent_col.name, &row_id)?
                .ok_or_else(|| GateError::MissingAudienceRow {
                    table: table.clone(),
                    row_id: row_id.clone(),
                })?;
            for child_id in rows_referencing(conn, child_table, &fk_col.name, &parent_key)? {
                pending.push((child_table.clone(), child_id));
            }
        }
    }
    Ok(rows)
}

pub(crate) fn required_store_ancestors(
    conn: &Connection,
    gates: &Gates,
    seeds: &HashSet<(String, String)>,
) -> Result<HashSet<(String, String)>, GateError> {
    let mut ancestors = HashSet::new();
    let mut visited = HashSet::new();
    let mut pending = seeds.iter().cloned().collect::<Vec<_>>();
    while let Some((table, row_id)) = pending.pop() {
        if !visited.insert((table.clone(), row_id.clone())) {
            continue;
        }
        if matches!(gates.tables.get(&table), Some(TableGate::Parent { .. })) {
            ancestors.insert((table.clone(), row_id.clone()));
        }
        for (fk_column, parent, parent_column) in foreign_keys(conn, &table)? {
            if !gates.tables.contains_key(&parent) {
                continue;
            }
            match fk_parent_row(conn, &table, &row_id, &fk_column, &parent, &parent_column)? {
                FkParentRow::Found(parent_id) => pending.push((parent, parent_id)),
                FkParentRow::RowAbsent | FkParentRow::NullForeignKey => continue,
                FkParentRow::ParentAbsent => {
                    return Err(GateError::MissingAudienceParent {
                        table: table.clone(),
                        row_id: Some(row_id.clone()),
                        parent,
                    });
                }
            }
        }
    }
    Ok(ancestors)
}

pub(crate) fn required_store_ancestors_for_deleted_rows(
    conn: &Connection,
    gates: &Gates,
    deleted: &HashMap<(String, String), ChangeRow>,
    seeds: &HashSet<(String, String)>,
) -> Result<HashSet<(String, String)>, GateError> {
    let mut live_seeds = HashSet::new();
    let mut visited = HashSet::new();
    let mut pending = seeds.iter().cloned().collect::<Vec<_>>();
    while let Some(key) = pending.pop() {
        if !visited.insert(key.clone()) {
            continue;
        }
        let row = deleted
            .get(&key)
            .ok_or_else(|| GateError::MissingAudienceRow {
                table: key.0.clone(),
                row_id: key.1.clone(),
            })?;
        for (fk_column, parent, parent_column) in foreign_keys(conn, &key.0)? {
            if !gates.tables.contains_key(&parent) {
                continue;
            }
            let fk_col = fk_column_ref(conn, &key.0, &fk_column)?;
            let Some(parent_key) = row.old.get(fk_col.index).and_then(|value| value.as_deref())
            else {
                continue;
            };
            let parent_col = fk_column_ref(conn, &parent, &parent_column)?;
            match deleted_or_live_parent(conn, deleted, &parent, &parent_col, parent_key)? {
                Some(DeletedParent::Deleted(deleted_parent)) => pending.push(deleted_parent),
                Some(DeletedParent::Live(parent_id)) => {
                    live_seeds.insert((parent, parent_id));
                }
                None => {
                    return Err(GateError::MissingAudienceParent {
                        table: key.0.clone(),
                        row_id: Some(key.1.clone()),
                        parent,
                    });
                }
            }
        }
    }
    required_store_ancestors(conn, gates, &live_seeds)
}

pub(crate) fn live_row_stamp(
    conn: &Connection,
    table: &str,
    row_id: &str,
) -> Result<String, GateError> {
    query_column_text(conn, table, "_updated_at", row_id)?.ok_or_else(|| {
        GateError::MissingAudienceRow {
            table: table.to_string(),
            row_id: row_id.to_string(),
        }
    })
}

/// Every synced foreign-key parent of the live row `(table, row_id)`, checked
/// against `audience`: a parent in Store is compatible with any audience, and a
/// parent in any other audience must be in that same one — otherwise the row
/// reaches across an audience boundary and the relationship cannot travel with
/// it. Returns the parents it accepted, so a caller walking the closure of a
/// row's relationships continues through them.
pub(crate) fn compatible_parent_rows(
    conn: &Connection,
    gates: &Gates,
    table: &str,
    row_id: &str,
    audience: &Audience,
) -> Result<Vec<(String, String)>, GateError> {
    let mut parents = Vec::new();
    for (fk_column, parent_table, parent_column) in foreign_keys(conn, table)? {
        if !gates.is_synced_table(&parent_table) {
            continue;
        }
        let parent_id = match fk_parent_row(
            conn,
            table,
            row_id,
            &fk_column,
            &parent_table,
            &parent_column,
        )? {
            FkParentRow::Found(parent_id) => parent_id,
            FkParentRow::NullForeignKey => continue,
            FkParentRow::RowAbsent => {
                return Err(GateError::MissingAudienceRow {
                    table: table.to_string(),
                    row_id: row_id.to_string(),
                });
            }
            FkParentRow::ParentAbsent => {
                return Err(GateError::MissingAudienceParent {
                    table: table.to_string(),
                    row_id: Some(row_id.to_string()),
                    parent: parent_table,
                });
            }
        };
        let parent_audience = live_row_audience(conn, gates, &parent_table, &parent_id)?;
        if parent_audience != Audience::Store && &parent_audience != audience {
            return Err(GateError::InvalidAudience {
                table: table.to_string(),
                value: audience.column_value(),
                reason: format!(
                    "relationship through {fk_column} references {parent_table}.{parent_id} in {parent_audience:?}"
                ),
            });
        }
        parents.push((parent_table, parent_id));
    }
    Ok(parents)
}

pub(crate) fn live_row_audience(
    conn: &Connection,
    gates: &Gates,
    table: &str,
    id: &str,
) -> Result<Audience, GateError> {
    if !gates.table_is_scoped(table) {
        if !gates.tables.contains_key(table) {
            return Ok(Audience::Store);
        }
        return gates
            .root_kept_of(conn, table, id)?
            .map(|kept| {
                if kept {
                    Audience::Store
                } else {
                    Audience::Local
                }
            })
            .ok_or_else(|| GateError::MissingAudienceRow {
                table: table.to_string(),
                row_id: id.to_string(),
            });
    }
    match gates.tables.get(table) {
        Some(TableGate::ScopedRoot { audience_col }) => {
            let value =
                query_column_present(conn, table, &audience_col.name, id)?.ok_or_else(|| {
                    GateError::MissingAudienceRow {
                        table: table.to_string(),
                        row_id: id.to_string(),
                    }
                })?;
            Audience::from_column(value.as_deref()).map_err(|source| {
                GateError::InvalidAudienceEncoding {
                    table: table.to_string(),
                    value,
                    source,
                }
            })
        }
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let FkParentRow::Found(parent_id) =
                fk_parent_row(conn, table, id, &fk_col.name, parent, &parent_col.name)?
            else {
                return Err(GateError::MissingAudienceParent {
                    table: table.to_string(),
                    row_id: Some(id.to_string()),
                    parent: parent.clone(),
                });
            };
            live_row_audience(conn, gates, parent, &parent_id)
        }
        None
        | Some(TableGate::Root { .. })
        | Some(TableGate::RemoteRoot)
        | Some(TableGate::Parent { .. }) => Err(GateError::MissingAudienceRow {
            table: table.to_string(),
            row_id: id.to_string(),
        }),
    }
}

pub(crate) fn active_circle_control(
    conn: &Connection,
    circle_id: CircleId,
) -> Result<CirclePartitionControl, GateError> {
    let state = query_row_optional(
        conn,
        "SELECT state FROM circle_current_state WHERE circle_id = ?1",
        [circle_id.to_string()],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    let Some(state) = state else {
        return Err(GateError::CircleAuthority {
            circle_id,
            active_records: 0,
        });
    };
    let state: CircleCurrentState =
        serde_json::from_slice(&state).map_err(|error| GateError::InvalidCircleControl {
            circle_id,
            source: CircleControlFailure::ParseCurrentState(error),
        })?;
    if !state.verify() || state.circle_id() != circle_id {
        return Err(GateError::InvalidCircleControl {
            circle_id,
            source: CircleControlFailure::Verification,
        });
    }
    if state.is_deleted() {
        return Err(GateError::CircleDeleted { circle_id });
    }
    let Some((current, _access, _roster, _metadata)) = state.active() else {
        return Err(GateError::CircleAuthority {
            circle_id,
            active_records: state.active_record_count(),
        });
    };
    let stored_control = serde_json::to_string(current.coordinate()).map_err(|error| {
        GateError::InvalidCircleControl {
            circle_id,
            source: CircleControlFailure::SerializeCoordinate(error),
        }
    })?;
    let parsed = CirclePartitionControl::from_stored_json(stored_control).map_err(|source| {
        GateError::InvalidCircleControl {
            circle_id,
            source: source.into(),
        }
    })?;
    Ok(parsed)
}

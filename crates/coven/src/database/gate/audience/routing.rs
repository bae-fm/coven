use super::partitioning::*;
use super::*;

pub(super) fn captured_deleted_audiences(
    conn: &Connection,
    deleted: &HashMap<(String, String), ChangeRow>,
    gates: &Gates,
) -> Result<BTreeMap<(String, String), Audience>, GateError> {
    let mut audiences = BTreeMap::new();
    let mut resolution = DeletedAudiences::default();
    for key in deleted
        .keys()
        .filter(|(table, _)| gates.tables.contains_key(table))
    {
        let audience = deleted_row_audience(
            conn,
            gates,
            deleted,
            key,
            &mut resolution,
            UnresolvedAudience::Rejected,
        )?;
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
    let mut deleted_rows = BTreeMap::new();
    let mut private_route_rows = BTreeMap::<Audience, Vec<(String, String, String, String)>>::new();
    for ((table, row_id), transition) in transitions {
        let routing_id = row_routing_id(key, &table, &row_id).to_string();
        let (audience, stamp) = match transition {
            RoutingTransition::Set { audience, stamp } => (audience, Some(stamp)),
            RoutingTransition::Delete => {
                let audience = stored_route_audience(conn, &routing_id, &table, &row_id)?;
                deleted_rows.insert((table.clone(), row_id.clone()), audience.clone());
                (audience, None)
            }
        };
        let Some(stamp) = stamp else {
            conn.execute(
                "DELETE FROM _coven_audience WHERE routing_id = ?1",
                [&routing_id],
            )
            .map_err(|source| GateError::Sql("delete Store audience mirror".to_string(), source))?;
            conn.execute(
                "DELETE FROM _coven_row_routes WHERE routing_id = ?1",
                [&routing_id],
            )
            .map_err(|source| GateError::Sql("delete private row route".to_string(), source))?;
            continue;
        };
        conn.execute(
            "INSERT INTO _coven_row_routes
             (routing_id, table_name, row_id, _updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(table_name, row_id) DO UPDATE SET
                 routing_id = excluded.routing_id,
                 _updated_at = excluded._updated_at",
            (&routing_id, &table, &row_id, &stamp),
        )
        .map_err(|source| GateError::Sql("persist private row route".to_string(), source))?;
        if audience != Audience::Local {
            private_route_rows
                .entry(audience.clone())
                .or_default()
                .push((
                    routing_id.clone(),
                    table.clone(),
                    row_id.clone(),
                    stamp.clone(),
                ));
        }
        match audience {
            Audience::Local => {
                conn.execute(
                    "DELETE FROM _coven_audience WHERE routing_id = ?1",
                    [&routing_id],
                )
                .map_err(|source| {
                    GateError::Sql("remove Local row from Store mirror".to_string(), source)
                })?;
            }
            Audience::Store | Audience::Circle(_) => {
                let circle_id = match audience {
                    Audience::Circle(circle_id) => Some(circle_id.to_string()),
                    Audience::Store => None,
                    Audience::Local => unreachable!(),
                };
                conn.execute(
                    "INSERT INTO _coven_audience (routing_id, circle_id, _updated_at)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(routing_id) DO UPDATE SET
                         circle_id = excluded.circle_id,
                         _updated_at = excluded._updated_at",
                    (&routing_id, circle_id, &stamp),
                )
                .map_err(|source| {
                    GateError::Sql("persist Store audience mirror".to_string(), source)
                })?;
            }
        }
    }

    let mut out = Vec::new();
    session
        .changeset_strm(&mut out)
        .map_err(|source| GateError::Session {
            operation: "extract routing journal".to_string(),
            source,
        })?;
    let private_routes = private_route_rows
        .into_iter()
        .map(|(audience, rows)| Ok((audience, private_route_insert_changeset(&rows)?)))
        .collect::<Result<BTreeMap<_, _>, GateError>>()?;
    Ok(RoutingChanges {
        store_mirror: out,
        private_routes,
        deleted_rows,
    })
}

pub(super) fn private_route_insert_changeset(
    rows: &[(String, String, String, String)],
) -> Result<Vec<u8>, GateError> {
    let conn = Connection::open_in_memory()
        .map_err(|source| GateError::Sql("open private route image".to_string(), source))?;
    crate::database::apply_coven_routing_schema(&conn)
        .map_err(|source| GateError::Sql("create private route image".to_string(), source))?;
    let mut session =
        rusqlite::session::Session::new(&conn).map_err(|source| GateError::Session {
            operation: "create private route image".to_string(),
            source,
        })?;
    session
        .attach(Some("_coven_row_routes"))
        .map_err(|source| GateError::Session {
            operation: "attach private route image".to_string(),
            source,
        })?;
    for (routing_id, table, row_id, stamp) in rows {
        conn.execute(
            "INSERT INTO _coven_row_routes
             (routing_id, table_name, row_id, _updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            (routing_id, table, row_id, stamp),
        )
        .map_err(|source| GateError::Sql("insert private route image".to_string(), source))?;
    }
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .map_err(|source| GateError::Session {
            operation: "extract private route image".to_string(),
            source,
        })?;
    Ok(changeset)
}

pub(super) enum RoutingTransition {
    Set { audience: Audience, stamp: String },
    Delete,
}

pub(super) fn routing_transitions(
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

pub(super) fn scoped_materialization_rows(
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

pub(super) fn required_store_ancestors(
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
                    })
                }
            }
        }
    }
    Ok(ancestors)
}

pub(super) fn required_store_ancestors_for_deleted_rows(
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
                    })
                }
            }
        }
    }
    required_store_ancestors(conn, gates, &live_seeds)
}

pub(super) fn live_row_stamp(
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

pub(super) fn stored_route_audience(
    conn: &Connection,
    routing_id: &str,
    table: &str,
    row_id: &str,
) -> Result<Audience, GateError> {
    let mirror = query_row_optional(
        conn,
        "SELECT audience.routing_id IS NOT NULL, audience.circle_id
         FROM _coven_row_routes AS route
         LEFT JOIN _coven_audience AS audience
           ON audience.routing_id = route.routing_id
         WHERE route.routing_id = ?1
           AND route.table_name = ?2
           AND route.row_id = ?3",
        (routing_id, table, row_id),
        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, Option<String>>(1)?)),
    )?
    .ok_or_else(|| GateError::MissingAudienceRow {
        table: table.to_string(),
        row_id: row_id.to_string(),
    })?;
    if !mirror.0 {
        return Ok(Audience::Local);
    }
    Audience::from_column(mirror.1.as_deref()).map_err(|error| GateError::InvalidAudience {
        table: table.to_string(),
        value: mirror.1,
        reason: error.to_string(),
    })
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
            Audience::from_column(value.as_deref()).map_err(|error| GateError::InvalidAudience {
                table: table.to_string(),
                value,
                reason: error.to_string(),
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
            reason: format!("parse current state: {error}"),
        })?;
    if !state.verify() || state.circle_id() != circle_id {
        return Err(GateError::InvalidCircleControl {
            circle_id,
            reason: "current state failed verification".to_string(),
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
            reason: format!("serialize current control coordinate: {error}"),
        }
    })?;
    let parsed = CirclePartitionControl::from_stored_json(stored_control)
        .map_err(|reason| GateError::InvalidCircleControl { circle_id, reason })?;
    Ok(parsed)
}

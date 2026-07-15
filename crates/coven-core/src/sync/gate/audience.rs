//! Audience resolution and atomic partitioning of one captured host changeset.

use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::ffi;
use rusqlite::Connection;

use super::ffi::{for_each_change, ChangeRow, Changegroup};
use super::model::{rows_referencing, GateColumn, Gates, TableGate};
use super::outbound::{full_state_diff, FullStateDirection};
use super::{query_row_optional, row_value_to_string, GateError};
use crate::sync::circle::{row_routing_id, Audience, CircleControlCoord, CircleId, RowRoutingKey};
use crate::sync::session::quote_ident;
use crate::WritePolicy;

pub(crate) fn is_routing_table(table: &str) -> bool {
    matches!(table, "_coven_audience" | "_coven_row_routes")
}

pub(crate) struct AudiencePartition {
    pub(crate) audience: Audience,
    pub(crate) control_coord_json: Option<String>,
    pub(crate) changeset: Vec<u8>,
}

pub(crate) struct RoutingChanges {
    changeset: Vec<u8>,
    deleted_rows: BTreeMap<(String, String), Audience>,
    deleted_routes: BTreeMap<String, Audience>,
}

impl RoutingChanges {
    pub(crate) fn empty() -> Self {
        Self {
            changeset: Vec::new(),
            deleted_rows: BTreeMap::new(),
            deleted_routes: BTreeMap::new(),
        }
    }
}

struct PartitionGroup {
    control_coord_json: Option<String>,
    group: Changegroup,
}

pub(crate) fn partition_outbound(
    conn: &Connection,
    changeset: &[u8],
    routing: &RoutingChanges,
    gates: &Gates,
    write_policy: WritePolicy,
) -> Result<Vec<AudiencePartition>, GateError> {
    unsafe { partition_outbound_raw(conn, changeset, routing, gates, write_policy) }
}

unsafe fn partition_outbound_raw(
    conn: &Connection,
    changeset: &[u8],
    routing: &RoutingChanges,
    gates: &Gates,
    write_policy: WritePolicy,
) -> Result<Vec<AudiencePartition>, GateError> {
    let mut groups = BTreeMap::<Audience, PartitionGroup>::new();
    let mut moves = Vec::new();
    let serial_deleted_rows = if write_policy == WritePolicy::Serial {
        captured_deleted_audiences(conn, changeset, gates)?
    } else {
        BTreeMap::new()
    };
    for_each_change(changeset, |iter, row| {
        if let Some((source, destination)) = scoped_root_move(gates, &row)? {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            moves.push((source, destination, (row.table.clone(), row_id.to_string())));
            return Ok(());
        }
        let audience = if row.op == ffi::SQLITE_DELETE {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            routing
                .deleted_rows
                .get(&(row.table.clone(), row_id.to_string()))
                .or_else(|| serial_deleted_rows.get(&(row.table.clone(), row_id.to_string())))
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| change_audience(conn, gates, &row))?
        } else {
            change_audience(conn, gates, &row)?
        };
        let partition = partition_group(conn, &mut groups, audience, write_policy)?;
        partition.group.add_change(iter)?;
        Ok(())
    })?;
    for (source, destination, seed) in moves {
        let component = scoped_materialization_rows(conn, gates, seed)?;
        add_materialization(
            conn,
            gates,
            &component,
            FullStateDirection::Deletes,
            partition_group(conn, &mut groups, source, write_policy)?,
        )?;
        add_materialization(
            conn,
            gates,
            &component,
            FullStateDirection::Inserts,
            partition_group(conn, &mut groups, destination, write_policy)?,
        )?;
    }
    for_each_change(&routing.changeset, |iter, row| {
        let audience = match row.table.as_str() {
            "_coven_audience" => Audience::Store,
            "_coven_row_routes" => routing_change_audience(conn, gates, routing, &row)?,
            _ => {
                return Err(GateError::Sql(
                    format!("unexpected routing changeset table {}", row.table),
                    rusqlite::Error::InvalidQuery,
                ));
            }
        };
        partition_group(conn, &mut groups, audience, write_policy)?
            .group
            .add_change(iter)?;
        Ok(())
    })?;
    groups
        .into_iter()
        .filter(|(audience, _)| *audience != Audience::Local)
        .map(|(audience, group)| {
            Ok(AudiencePartition {
                audience,
                control_coord_json: group.control_coord_json,
                changeset: group.group.output()?,
            })
        })
        .collect()
}

unsafe fn captured_deleted_audiences(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<BTreeMap<(String, String), Audience>, GateError> {
    let mut deleted = HashMap::new();
    for_each_change(changeset, |_iter, row| {
        if row.op == ffi::SQLITE_DELETE && table_is_scoped(gates, &row.table) {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            deleted.insert((row.table.clone(), row_id.to_string()), row);
        }
        Ok(())
    })?;
    let mut audiences = BTreeMap::new();
    for key in deleted.keys() {
        let audience = resolve_deleted_audience(conn, gates, &deleted, key, &mut HashSet::new())?;
        audiences.insert(key.clone(), audience);
    }
    Ok(audiences)
}

fn resolve_deleted_audience(
    conn: &Connection,
    gates: &Gates,
    deleted: &HashMap<(String, String), ChangeRow>,
    key: &(String, String),
    seen: &mut HashSet<(String, String)>,
) -> Result<Audience, GateError> {
    if !seen.insert(key.clone()) {
        return Err(GateError::FkCycle(vec![key.0.clone()]));
    }
    let row = deleted
        .get(key)
        .ok_or_else(|| GateError::MissingAudienceRow {
            table: key.0.clone(),
            row_id: key.1.clone(),
        })?;
    let resolved = match gates.tables.get(&key.0) {
        Some(TableGate::ScopedRoot { audience_col }) => {
            let audience = row.old_value(audience_col.index).unwrap_or(None);
            Audience::from_column(audience).map_err(|error| GateError::InvalidAudience {
                table: key.0.clone(),
                value: audience.map(str::to_string),
                reason: error.to_string(),
            })
        }
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let parent_key = row.old_value(fk_col.index).flatten().ok_or_else(|| {
                GateError::MissingAudienceParent {
                    table: key.0.clone(),
                    row_id: Some(key.1.clone()),
                    parent: parent.clone(),
                }
            })?;
            if let Some(parent_row) = deleted.iter().find_map(|(candidate, row)| {
                (candidate.0 == *parent
                    && row.old_value(parent_col.index).flatten() == Some(parent_key))
                .then_some(candidate)
            }) {
                resolve_deleted_audience(conn, gates, deleted, parent_row, seen)
            } else {
                let parent_id =
                    row_id_for_column_value(conn, parent, &parent_col.name, parent_key)?
                        .ok_or_else(|| GateError::MissingAudienceParent {
                            table: key.0.clone(),
                            row_id: Some(key.1.clone()),
                            parent: parent.clone(),
                        })?;
                live_row_audience(conn, gates, parent, &parent_id)
            }
        }
        _ => Err(GateError::MissingAudienceRow {
            table: key.0.clone(),
            row_id: key.1.clone(),
        }),
    };
    seen.remove(key);
    resolved
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
    for table in ["_coven_audience", "_coven_row_routes"] {
        session
            .attach(Some(table))
            .map_err(|source| GateError::Session {
                operation: format!("attach routing table {table}"),
                source,
            })?;
    }

    let transitions = routing_transitions(conn, changeset, gates)?;
    let mut deleted_rows = BTreeMap::new();
    let mut deleted_routes = BTreeMap::new();
    for ((table, row_id), transition) in transitions {
        let routing_id = row_routing_id(key, &table, &row_id).to_string();
        let (audience, stamp) = match transition {
            RoutingTransition::Set { audience, stamp } => (audience, Some(stamp)),
            RoutingTransition::Delete => {
                let audience = stored_route_audience(conn, &routing_id, &table, &row_id)?;
                deleted_rows.insert((table.clone(), row_id.clone()), audience.clone());
                deleted_routes.insert(routing_id.clone(), audience.clone());
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
    Ok(RoutingChanges {
        changeset: out,
        deleted_rows,
        deleted_routes,
    })
}

enum RoutingTransition {
    Set { audience: Audience, stamp: String },
    Delete,
}

fn routing_transitions(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<BTreeMap<(String, String), RoutingTransition>, GateError> {
    let mut transitions = BTreeMap::new();
    unsafe {
        for_each_change(changeset, |_iter, row| {
            if !table_is_scoped(gates, &row.table) {
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
            let Some((_source, destination)) = scoped_root_move(gates, &row)? else {
                return Ok(());
            };
            let stamp = live_row_stamp(conn, &row.table, row_id)?;
            let component =
                scoped_materialization_rows(conn, gates, (row.table.clone(), row_id.to_string()))?;
            for (table, id) in component {
                if table_is_scoped(gates, &table) {
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

fn scoped_materialization_rows(
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
            let parent_key = query_text_value(conn, &table, &parent_col.name, "id", &row_id)?
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

fn table_is_scoped(gates: &Gates, table: &str) -> bool {
    let mut current = table;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(current.to_string()) {
            return false;
        }
        match gates.tables.get(current) {
            Some(TableGate::ScopedRoot { .. }) => return true,
            Some(TableGate::Child { parent, .. }) => current = parent,
            _ => return false,
        }
    }
}

fn live_row_stamp(conn: &Connection, table: &str, row_id: &str) -> Result<String, GateError> {
    query_text_value(conn, table, "_updated_at", "id", row_id)?.ok_or_else(|| {
        GateError::MissingAudienceRow {
            table: table.to_string(),
            row_id: row_id.to_string(),
        }
    })
}

fn routing_change_audience(
    conn: &Connection,
    gates: &Gates,
    routing: &RoutingChanges,
    row: &ChangeRow,
) -> Result<Audience, GateError> {
    let routing_id = row
        .pk()
        .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
    if row.op == ffi::SQLITE_DELETE {
        return routing
            .deleted_routes
            .get(routing_id)
            .cloned()
            .ok_or_else(|| GateError::MissingAudienceRow {
                table: row.table.clone(),
                row_id: routing_id.to_string(),
            });
    }
    let route = query_row_optional(
        conn,
        "SELECT table_name, row_id FROM _coven_row_routes WHERE routing_id = ?1",
        [routing_id],
        |record| Ok((record.get::<_, String>(0)?, record.get::<_, String>(1)?)),
    )?
    .ok_or_else(|| GateError::MissingAudienceRow {
        table: row.table.clone(),
        row_id: routing_id.to_string(),
    })?;
    live_row_audience(conn, gates, &route.0, &route.1)
}

fn stored_route_audience(
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

unsafe fn partition_group<'a>(
    conn: &Connection,
    groups: &'a mut BTreeMap<Audience, PartitionGroup>,
    audience: Audience,
    write_policy: WritePolicy,
) -> Result<&'a mut PartitionGroup, GateError> {
    let control_coord_json = match audience {
        Audience::Circle(circle_id) => Some(active_circle_control(conn, circle_id, write_policy)?),
        Audience::Store | Audience::Local => None,
    };
    match groups.entry(audience) {
        std::collections::btree_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::btree_map::Entry::Vacant(entry) => {
            let group = Changegroup::new()?;
            group.set_schema(conn.handle())?;
            Ok(entry.insert(PartitionGroup {
                control_coord_json,
                group,
            }))
        }
    }
}

fn scoped_root_move(
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Option<(Audience, Audience)>, GateError> {
    if row.op != ffi::SQLITE_UPDATE {
        return Ok(None);
    }
    let Some(TableGate::ScopedRoot { audience_col }) = gates.tables.get(&row.table) else {
        return Ok(None);
    };
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
    let source = parse(source_value)?;
    let destination = parse(destination_value)?;
    Ok((source != destination).then_some((source, destination)))
}

unsafe fn add_materialization(
    conn: &Connection,
    gates: &Gates,
    component: &HashSet<(String, String)>,
    direction: FullStateDirection,
    partition: &mut PartitionGroup,
) -> Result<(), GateError> {
    let materialization = full_state_diff(conn, gates, direction)?;
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

fn change_audience(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Audience, GateError> {
    match gates.tables.get(&row.table) {
        None | Some(TableGate::RemoteRoot) => Ok(Audience::Store),
        Some(TableGate::Root { gate_col }) => {
            let kept = match row.effective_truth(gate_col.index) {
                Some(kept) => kept,
                None => row
                    .pk()
                    .map(|id| gates.row_kept(conn, &row.table, id))
                    .transpose()?
                    .unwrap_or(false),
            };
            Ok(if kept {
                Audience::Store
            } else {
                Audience::Local
            })
        }
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
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let parent_key = change_parent_key(conn, row, fk_col)?;
            let parent_id = row_id_for_column_value(conn, parent, &parent_col.name, &parent_key)?
                .ok_or_else(|| GateError::MissingAudienceParent {
                table: row.table.clone(),
                row_id: row.pk().map(str::to_string),
                parent: parent.clone(),
            })?;
            live_row_audience(conn, gates, parent, &parent_id)
        }
        Some(TableGate::Parent { .. }) => {
            let id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            Ok(if gates.row_kept(conn, &row.table, id)? {
                Audience::Store
            } else {
                Audience::Local
            })
        }
    }
}

fn change_parent_key(
    conn: &Connection,
    row: &ChangeRow,
    fk_col: &GateColumn,
) -> Result<String, GateError> {
    if let Some(value) = row.fk_value(fk_col.index) {
        return Ok(value.to_string());
    }
    let id = row
        .pk()
        .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
    query_text_value(conn, &row.table, &fk_col.name, "id", id)?.ok_or_else(|| {
        GateError::MissingAudienceParent {
            table: row.table.clone(),
            row_id: Some(id.to_string()),
            parent: fk_col.name.clone(),
        }
    })
}

fn live_row_audience(
    conn: &Connection,
    gates: &Gates,
    table: &str,
    id: &str,
) -> Result<Audience, GateError> {
    match gates.tables.get(table) {
        None | Some(TableGate::RemoteRoot) => Ok(Audience::Store),
        Some(TableGate::Root { .. }) | Some(TableGate::Parent { .. }) => {
            Ok(if gates.row_kept(conn, table, id)? {
                Audience::Store
            } else {
                Audience::Local
            })
        }
        Some(TableGate::ScopedRoot { audience_col }) => {
            let sql = format!(
                "SELECT {} FROM {} WHERE id = ?1",
                quote_ident(&audience_col.name),
                quote_ident(table)
            );
            let value =
                query_row_optional(conn, &sql, [id], |row| row.get::<_, Option<String>>(0))?
                    .ok_or_else(|| GateError::MissingAudienceRow {
                        table: table.to_string(),
                        row_id: id.to_string(),
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
            let parent_key =
                query_text_value(conn, table, &fk_col.name, "id", id)?.ok_or_else(|| {
                    GateError::MissingAudienceParent {
                        table: table.to_string(),
                        row_id: Some(id.to_string()),
                        parent: parent.clone(),
                    }
                })?;
            let parent_id = row_id_for_column_value(conn, parent, &parent_col.name, &parent_key)?
                .ok_or_else(|| GateError::MissingAudienceParent {
                table: table.to_string(),
                row_id: Some(id.to_string()),
                parent: parent.clone(),
            })?;
            live_row_audience(conn, gates, parent, &parent_id)
        }
    }
}

fn query_text_value(
    conn: &Connection,
    table: &str,
    selected: &str,
    key_column: &str,
    key: &str,
) -> Result<Option<String>, GateError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        quote_ident(selected),
        quote_ident(table),
        quote_ident(key_column)
    );
    query_row_optional(conn, &sql, [key], |row| row_value_to_string(row, 0))
        .map(|value| value.flatten())
}

fn row_id_for_column_value(
    conn: &Connection,
    table: &str,
    column: &str,
    value: &str,
) -> Result<Option<String>, GateError> {
    query_text_value(conn, table, "id", column, value)
}

fn active_circle_control(
    conn: &Connection,
    circle_id: CircleId,
    write_policy: WritePolicy,
) -> Result<String, GateError> {
    let mut statement = conn
        .prepare(
            "SELECT access.control_coord
             FROM circle_access_cache AS access
             JOIN circle_control_activations AS activation
               ON activation.circle_id = access.circle_id
              AND activation.control_coord = access.control_coord
             WHERE access.circle_id = ?1 AND access.disposition = 'active'
             ORDER BY access.control_coord, access.owner_pubkey
             LIMIT 2",
        )
        .map_err(|error| GateError::Sql("prepare active circle authority".to_string(), error))?;
    let controls = statement
        .query_map([circle_id.to_string()], |row| row.get::<_, String>(0))
        .map_err(|error| GateError::Sql("query active circle authority".to_string(), error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| GateError::Sql("read active circle authority".to_string(), error))?;
    let [control] = controls.as_slice() else {
        return Err(GateError::CircleAuthority {
            circle_id,
            active_records: controls.len(),
        });
    };
    let parsed: CircleControlCoord =
        serde_json::from_str(control).map_err(|error| GateError::InvalidCircleControl {
            circle_id,
            reason: error.to_string(),
        })?;
    parsed
        .validate()
        .map_err(|error| GateError::InvalidCircleControl {
            circle_id,
            reason: error.to_string(),
        })?;
    let control_policy = match parsed {
        CircleControlCoord::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
        CircleControlCoord::Serial { .. } => WritePolicy::Serial,
    };
    if control_policy != write_policy {
        return Err(GateError::CircleControlPolicy {
            circle_id,
            expected: write_policy,
            actual: control_policy,
        });
    }
    Ok(control.clone())
}

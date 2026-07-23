//! Audience resolution and atomic partitioning of one captured host changeset.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::ffi;
use rusqlite::Connection;

use super::ffi::{collect_deletes, for_each_change, ChangeRow, Changegroup};
use super::model::{foreign_keys, rows_referencing, truthy, GateColumn, Gates, TableGate};
use super::outbound::{
    full_state_diff, gate_store_outbound, query_column_text, row_id_for_column_value,
    FullStateDirection,
};
use super::{query_mapped_rows, query_row_optional, GateError};
use crate::sync::circle::{row_routing_id, Audience, CircleControlCoord, CircleId, RowRoutingKey};
use crate::sync::session::quote_ident;
use crate::sync::store::CircleCurrentState;

pub(crate) fn is_routing_table(table: &str) -> bool {
    matches!(table, "_coven_audience" | "_coven_row_routes")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudiencePartition {
    pub(crate) audience: Audience,
    pub(crate) control: Option<CirclePartitionControl>,
    pub(crate) changeset: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CirclePartitionControl {
    coordinate: CircleControlCoord,
    stored_json: String,
}

impl CirclePartitionControl {
    pub(crate) fn from_stored_json(stored_json: String) -> Result<Self, String> {
        let coordinate: CircleControlCoord =
            serde_json::from_str(&stored_json).map_err(|error| error.to_string())?;
        coordinate.validate().map_err(|error| error.to_string())?;
        Ok(Self {
            coordinate,
            stored_json,
        })
    }

    pub(crate) fn coordinate(&self) -> &CircleControlCoord {
        &self.coordinate
    }

    pub(crate) fn stored_json(&self) -> &str {
        &self.stored_json
    }
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
    control: Option<CirclePartitionControl>,
    group: Changegroup,
}

pub(crate) fn partition_outbound(
    conn: &Connection,
    changeset: &[u8],
    routing: &RoutingChanges,
    gates: &Gates,
) -> Result<Vec<AudiencePartition>, GateError> {
    unsafe { partition_outbound_raw(conn, changeset, routing, gates) }
}

pub(crate) fn filter_inbound_circle_changeset(
    conn: &Connection,
    changeset: &[u8],
    circle_id: CircleId,
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    unsafe { filter_inbound_circle_changeset_raw(conn, changeset, circle_id, gates) }
}

unsafe fn filter_inbound_circle_changeset_raw(
    conn: &Connection,
    changeset: &[u8],
    circle_id: CircleId,
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    let mut package_routes = HashMap::<(String, String), String>::new();
    for_each_change(changeset, |_iter, row| {
        if row.table == "_coven_audience" {
            return Err(GateError::InvalidInboundAudiencePackage(
                "Circle package contains the Store audience mirror".to_string(),
            ));
        }
        if row.table != "_coven_row_routes" {
            if !gates.table_is_scoped(&row.table) {
                return Err(GateError::InvalidInboundAudiencePackage(format!(
                    "Circle package contains unscoped table {}",
                    row.table
                )));
            }
            return Ok(());
        }
        let routing_id = row
            .pk()
            .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
        let table = row
            .new_value(1)
            .or_else(|| row.old_value(1))
            .flatten()
            .ok_or_else(|| {
                GateError::InvalidInboundAudiencePackage(
                    "private route has no table name".to_string(),
                )
            })?;
        let row_id = row
            .new_value(2)
            .or_else(|| row.old_value(2))
            .flatten()
            .ok_or_else(|| {
                GateError::InvalidInboundAudiencePackage("private route has no row id".to_string())
            })?;
        if !gates.table_is_scoped(table) {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route names unscoped table {table}"
            )));
        }
        if package_routes
            .insert(
                (table.to_string(), row_id.to_string()),
                routing_id.to_string(),
            )
            .is_some()
        {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "duplicate private route for {table}.{row_id}"
            )));
        }
        Ok(())
    })?;

    let group = Changegroup::new()?;
    group.set_schema(conn.handle())?;
    let expected_circle = circle_id.to_string();
    for_each_change(changeset, |iter, row| {
        let routing_id = if row.table == "_coven_row_routes" {
            row.pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?
                .to_string()
        } else {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            if let Some(routing_id) = package_routes.get(&(row.table.clone(), row_id.to_string())) {
                routing_id.clone()
            } else {
                query_row_optional(
                    conn,
                    "SELECT routing_id FROM _coven_row_routes
                     WHERE table_name = ?1 AND row_id = ?2",
                    (&row.table, row_id),
                    |record| record.get::<_, String>(0),
                )?
                .ok_or_else(|| GateError::MissingAudienceRow {
                    table: row.table.clone(),
                    row_id: row_id.to_string(),
                })?
            }
        };
        let winning_circle = query_row_optional(
            conn,
            "SELECT circle_id FROM _coven_audience WHERE routing_id = ?1",
            [&routing_id],
            |record| record.get::<_, Option<String>>(0),
        )?
        .flatten();
        if winning_circle.as_deref() == Some(expected_circle.as_str()) {
            group.add_change(iter)?;
        }
        Ok(())
    })?;
    group.output()
}

pub(crate) fn prune_ineligible_scoped_rows(
    conn: &Connection,
    gates: &Gates,
    inactive_circles: &BTreeSet<CircleId>,
) -> Result<(), GateError> {
    if !gates.has_scoped_graph() {
        return Ok(());
    }
    let mut removed = HashSet::<(String, String)>::new();
    for (table, gate) in &gates.tables {
        let TableGate::ScopedRoot { audience_col } = gate else {
            continue;
        };
        let sql = format!(
            "SELECT {id}, {audience} FROM {table}",
            id = quote_ident("id"),
            audience = quote_ident(&audience_col.name),
            table = quote_ident(table),
        );
        let roots = query_mapped_rows(conn, &sql, [], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        for (row_id, local_audience) in roots {
            let parsed_audience =
                Audience::from_column(local_audience.as_deref()).map_err(|error| {
                    GateError::InvalidAudience {
                        table: table.clone(),
                        value: local_audience.clone(),
                        reason: error.to_string(),
                    }
                })?;
            if parsed_audience == Audience::Local {
                continue;
            }
            let mirror = query_row_optional(
                conn,
                "SELECT audience.circle_id
                 FROM _coven_row_routes AS route
                 JOIN _coven_audience AS audience
                   ON audience.routing_id = route.routing_id
                 WHERE route.table_name = ?1 AND route.row_id = ?2",
                (table, &row_id),
                |row| row.get::<_, Option<String>>(0),
            )?;
            let mirror_audience = match mirror.as_ref() {
                None => None,
                Some(value) => Some(Audience::from_column(value.as_deref()).map_err(|error| {
                    GateError::InvalidAudience {
                        table: "_coven_audience".to_string(),
                        value: value.clone(),
                        reason: error.to_string(),
                    }
                })?),
            };
            let mirror_matches = mirror_audience.as_ref() == Some(&parsed_audience);
            let inactive = matches!(
                parsed_audience,
                Audience::Circle(circle) if inactive_circles.contains(&circle)
            );
            if !mirror_matches || inactive {
                removed.extend(gates.subtree_rows(conn, table, &row_id)?);
            }
        }
    }
    delete_scoped_rows(conn, gates, &removed, true)
}

fn delete_scoped_rows(
    conn: &Connection,
    gates: &Gates,
    removed: &HashSet<(String, String)>,
    has_routing_tables: bool,
) -> Result<(), GateError> {
    let mut order = gates.gated_tables_parent_first(conn)?;
    order.reverse();
    for table in order {
        let mut row_ids = removed
            .iter()
            .filter(|(removed_table, _)| removed_table == &table)
            .map(|(_, row_id)| row_id)
            .collect::<Vec<_>>();
        row_ids.sort();
        for row_id in row_ids {
            conn.execute(
                "DELETE FROM row_blob_locators WHERE table_name = ?1 AND row_id = ?2",
                (&table, row_id),
            )
            .map_err(|error| {
                GateError::Sql(
                    format!("delete ineligible row blob bindings {table}.{row_id}"),
                    error,
                )
            })?;
            conn.execute(
                &format!(
                    "DELETE FROM {} WHERE {} = ?1",
                    quote_ident(&table),
                    quote_ident("id")
                ),
                [row_id],
            )
            .map_err(|error| {
                GateError::Sql(
                    format!("delete ineligible scoped row {table}.{row_id}"),
                    error,
                )
            })?;
            if has_routing_tables {
                conn.execute(
                    "DELETE FROM _coven_row_routes WHERE table_name = ?1 AND row_id = ?2",
                    (&table, row_id),
                )
                .map_err(|error| {
                    GateError::Sql(
                        format!("delete ineligible private route {table}.{row_id}"),
                        error,
                    )
                })?;
            }
        }
        conn.execute(
            &format!(
                "DELETE FROM row_blob_locators AS locator
                 WHERE locator.table_name = ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM {table} AS host WHERE host.{id} = locator.row_id
                   )",
                table = quote_ident(&table),
                id = quote_ident("id"),
            ),
            [&table],
        )
        .map_err(|error| {
            GateError::Sql(
                format!("delete orphaned row blob bindings for {table}"),
                error,
            )
        })?;
        if has_routing_tables {
            conn.execute(
                &format!(
                    "DELETE FROM _coven_row_routes AS route
                 WHERE route.table_name = ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM {table} AS host WHERE host.{id} = route.row_id
                   )",
                    table = quote_ident(&table),
                    id = quote_ident("id"),
                ),
                [&table],
            )
            .map_err(|error| {
                GateError::Sql(format!("delete orphaned private routes for {table}"), error)
            })?;
        }
    }
    Ok(())
}

unsafe fn partition_outbound_raw(
    conn: &Connection,
    changeset: &[u8],
    routing: &RoutingChanges,
    gates: &Gates,
) -> Result<Vec<AudiencePartition>, GateError> {
    let mut groups = BTreeMap::<Audience, PartitionGroup>::new();
    let mut moves = Vec::new();
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
        partition_group(conn, &mut groups, Audience::Store)?
            .group
            .add_change(iter)?;
        Ok(())
    })?;
    for_each_change(changeset, |iter, row| {
        if !gates.tables.contains_key(&row.table) {
            return Ok(());
        }
        if gates.table_is_scoped(&row.table) {
            validate_outgoing_synced_fk_audiences(conn, gates, &row)?;
        }
        if let Some((source, destination)) = row_audience_move(conn, gates, &row)? {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            moves.push((source, destination, (row.table.clone(), row_id.to_string())));
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
            let partition = partition_group(conn, &mut groups, audience)?;
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
    for (source, destination, seed) in moves {
        let component = scoped_materialization_rows(conn, gates, seed)?;
        let ancestors = required_store_ancestors(conn, gates, &component)?;
        if source == Audience::Local && destination != Audience::Local {
            ancestor_inserts.extend(ancestors.iter().cloned());
        } else if source != Audience::Local && destination == Audience::Local {
            for (table, id) in ancestors {
                if !gates.row_kept(conn, &table, &id)? {
                    ancestor_deletes.insert((table, id));
                }
            }
        }
        add_materialization(
            conn,
            gates,
            &component,
            FullStateDirection::Deletes,
            partition_group(conn, &mut groups, source)?,
        )?;
        add_materialization(
            conn,
            gates,
            &component,
            FullStateDirection::Inserts,
            partition_group(conn, &mut groups, destination)?,
        )?;
    }
    if !ancestor_inserts.is_empty() {
        add_materialization(
            conn,
            gates,
            &ancestor_inserts,
            FullStateDirection::Inserts,
            partition_group(conn, &mut groups, Audience::Store)?,
        )?;
    }
    if !ancestor_deletes.is_empty() {
        add_materialization(
            conn,
            gates,
            &ancestor_deletes,
            FullStateDirection::Deletes,
            partition_group(conn, &mut groups, Audience::Store)?,
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
        partition_group(conn, &mut groups, audience)?
            .group
            .add_change(iter)?;
        Ok(())
    })?;
    groups
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

fn validate_outgoing_synced_fk_audiences(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<(), GateError> {
    if row.op == ffi::SQLITE_DELETE || !gates.table_is_scoped(&row.table) {
        return Ok(());
    }
    let row_id = row
        .pk()
        .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
    let row_audience = live_row_audience(conn, gates, &row.table, row_id)?;
    for (fk_column, parent_table, parent_column) in foreign_keys(conn, &row.table)? {
        if !gates.is_synced_table(&parent_table) {
            continue;
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1",
            quote_ident(&fk_column),
            quote_ident(&row.table),
        );
        let parent_key = query_row_optional(conn, &sql, [row_id], |record| {
            record.get::<_, Option<String>>(0)
        })?
        .ok_or_else(|| GateError::MissingAudienceRow {
            table: row.table.clone(),
            row_id: row_id.to_string(),
        })?;
        let Some(parent_key) = parent_key else {
            continue;
        };
        let parent_id = row_id_for_column_value(conn, &parent_table, &parent_column, &parent_key)?
            .ok_or_else(|| GateError::MissingAudienceParent {
                table: row.table.clone(),
                row_id: Some(row_id.to_string()),
                parent: parent_table.clone(),
            })?;
        let parent_audience = live_row_audience(conn, gates, &parent_table, &parent_id)?;
        if parent_audience != Audience::Store && parent_audience != row_audience {
            return Err(GateError::InvalidAudience {
                table: row.table.clone(),
                value: row_audience.column_value(),
                reason: format!(
                    "relationship through {fk_column} references {parent_table}.{parent_id} in {parent_audience:?}"
                ),
            });
        }
    }
    Ok(())
}

fn captured_deleted_audiences(
    conn: &Connection,
    deleted: &HashMap<(String, String), ChangeRow>,
    gates: &Gates,
) -> Result<BTreeMap<(String, String), Audience>, GateError> {
    let mut audiences = BTreeMap::new();
    for key in deleted
        .keys()
        .filter(|(table, _)| gates.tables.contains_key(table))
    {
        let audience = resolve_deleted_audience(conn, gates, deleted, key, &mut HashSet::new())?;
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
        Some(TableGate::Root { gate_col }) => {
            let kept = row.old_value(gate_col.index).flatten().is_some_and(truthy);
            Ok(if kept {
                Audience::Store
            } else {
                Audience::Local
            })
        }
        Some(TableGate::RemoteRoot) | Some(TableGate::Parent { .. }) => Ok(Audience::Store),
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

fn required_store_ancestors(
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
            let Some(parent_key) = query_column_text(conn, &table, &fk_column, &row_id)? else {
                continue;
            };
            let parent_id = row_id_for_column_value(conn, &parent, &parent_column, &parent_key)?
                .ok_or_else(|| GateError::MissingAudienceParent {
                    table: table.clone(),
                    row_id: Some(row_id.clone()),
                    parent: parent.clone(),
                })?;
            pending.push((parent, parent_id));
        }
    }
    Ok(ancestors)
}

fn required_store_ancestors_for_deleted_rows(
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
        let columns = super::gate_table_columns(conn, &key.0)?;
        for (fk_column, parent, parent_column) in foreign_keys(conn, &key.0)? {
            if !gates.tables.contains_key(&parent) {
                continue;
            }
            let fk_index = columns
                .iter()
                .position(|column| column == &fk_column)
                .ok_or_else(|| GateError::MissingFkColumn(key.0.clone(), fk_column.clone()))?;
            let Some(parent_key) = row.old.get(fk_index).and_then(|value| value.as_deref()) else {
                continue;
            };
            let parent_columns = super::gate_table_columns(conn, &parent)?;
            let parent_index = parent_columns
                .iter()
                .position(|column| column == &parent_column)
                .ok_or_else(|| GateError::MissingFkColumn(parent.clone(), parent_column.clone()))?;
            if let Some(deleted_parent) = deleted.iter().find_map(|(candidate, parent_row)| {
                (candidate.0 == parent
                    && parent_row
                        .old
                        .get(parent_index)
                        .and_then(|value| value.as_deref())
                        == Some(parent_key))
                .then(|| candidate.clone())
            }) {
                pending.push(deleted_parent);
                continue;
            }
            let parent_id = row_id_for_column_value(conn, &parent, &parent_column, parent_key)?
                .ok_or_else(|| GateError::MissingAudienceParent {
                    table: key.0.clone(),
                    row_id: Some(key.1.clone()),
                    parent: parent.clone(),
                })?;
            live_seeds.insert((parent, parent_id));
        }
    }
    required_store_ancestors(conn, gates, &live_seeds)
}

fn live_row_stamp(conn: &Connection, table: &str, row_id: &str) -> Result<String, GateError> {
    query_column_text(conn, table, "_updated_at", row_id)?.ok_or_else(|| {
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
) -> Result<&'a mut PartitionGroup, GateError> {
    let control = match audience {
        Audience::Circle(circle_id) => Some(active_circle_control(conn, circle_id)?),
        Audience::Store | Audience::Local => None,
    };
    match groups.entry(audience) {
        std::collections::btree_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::btree_map::Entry::Vacant(entry) => {
            let group = Changegroup::new()?;
            group.set_schema(conn.handle())?;
            Ok(entry.insert(PartitionGroup { control, group }))
        }
    }
}

fn row_audience_move(
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
            let parent_key = change_parent_key(conn, row, fk_col)?;
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
    query_column_text(conn, &row.table, &fk_col.name, id)?.ok_or_else(|| {
        GateError::MissingAudienceParent {
            table: row.table.clone(),
            row_id: Some(id.to_string()),
            parent: fk_col.name.clone(),
        }
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
                query_column_text(conn, table, &fk_col.name, id)?.ok_or_else(|| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::session::{RowIdentity, SyncedTable};
    use rusqlite::session::Session;

    fn row_blob_locator_schema(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE row_blob_locators (
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 column_name TEXT NOT NULL,
                 row_stamp TEXT NOT NULL,
                 audience_authority TEXT NOT NULL CHECK (json_valid(audience_authority)),
                 remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
                 PRIMARY KEY (table_name, row_id, column_name, row_stamp)
             ) STRICT;",
        )
        .expect("create row blob locator schema");
    }

    fn routing_schema(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE _coven_audience (
                 routing_id TEXT PRIMARY KEY,
                 circle_id TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE _coven_row_routes (
                 routing_id TEXT PRIMARY KEY,
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 UNIQUE(table_name, row_id)
             ) STRICT;",
        )
        .expect("create inbound audience test schema");
        row_blob_locator_schema(conn);
    }

    fn note_gates(conn: &Connection) -> Gates {
        Gates::from_tables(
            conn,
            &[SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience")],
        )
        .expect("build scoped gates")
    }

    #[test]
    fn inbound_circle_filter_keeps_only_rows_owned_by_its_winning_mirror() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        let first = CircleId::from_bytes([1; 16]);
        let second = CircleId::from_bytes([2; 16]);
        source
            .execute(
                "INSERT INTO notes VALUES (?1, ?2, 'first', '1')",
                ("first", first.to_string()),
            )
            .expect("insert first note");
        source
            .execute(
                "INSERT INTO notes VALUES (?1, ?2, 'second', '1')",
                ("second", second.to_string()),
            )
            .expect("insert second note");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES ('route-first', 'notes', 'first', '1')",
                [],
            )
            .expect("insert first route");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES ('route-second', 'notes', 'second', '1')",
                [],
            )
            .expect("insert second route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract source changeset");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES ('route-first', ?1, '2')",
                [first.to_string()],
            )
            .expect("install first mirror");
        target
            .execute(
                "INSERT INTO _coven_audience VALUES ('route-second', ?1, '2')",
                [second.to_string()],
            )
            .expect("install second mirror");

        let filtered =
            filter_inbound_circle_changeset(&target, &changeset, first, &note_gates(&target))
                .expect("filter first Circle package");
        let rows = crate::changeset::walk(&filtered).expect("walk filtered changeset");
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.table == "notes" && row.pk() == Some("first")));
        assert!(rows
            .iter()
            .any(|row| { row.table == "_coven_row_routes" && row.pk() == Some("route-first") }));
    }

    #[test]
    fn inbound_circle_filter_rejects_a_store_mirror_change() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let mut session = Session::new(&source).expect("create source session");
        session
            .attach(Some("_coven_audience"))
            .expect("attach audience mirror");
        source
            .execute(
                "INSERT INTO _coven_audience VALUES ('route', NULL, '1')",
                [],
            )
            .expect("insert mirror");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract mirror changeset");
        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);

        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            CircleId::from_bytes([1; 16]),
            &note_gates(&target),
        )
        .expect_err("Circle package must not carry the Store mirror");
        assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
    }

    #[test]
    fn inbound_circle_filter_rejects_a_route_for_an_unscoped_table() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let mut session = Session::new(&source).expect("create source session");
        session
            .attach(Some("_coven_row_routes"))
            .expect("attach private routes");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES ('route', 'unknown', 'row', '1')",
                [],
            )
            .expect("insert undeclared route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract route changeset");
        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let circle = CircleId::from_bytes([1; 16]);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
                [circle.to_string()],
            )
            .expect("install winning mirror");

        let error =
            filter_inbound_circle_changeset(&target, &changeset, circle, &note_gates(&target))
                .expect_err("Circle package route must name a scoped table");
        assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
    }

    #[test]
    fn audience_prune_removes_stale_scoped_subtrees_and_keeps_local_rows() {
        let conn = Connection::open_in_memory().expect("open target");
        routing_schema(&conn);
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id),
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO notes VALUES ('local', 'local', 'local', '1');
             INSERT INTO comments VALUES ('local-child', 'local', 'local', '1');
             INSERT INTO _coven_row_routes VALUES ('local-route', 'notes', 'local', '1');
             INSERT INTO _coven_row_routes VALUES ('local-child-route', 'comments', 'local-child', '1');
             INSERT INTO _coven_row_routes VALUES ('orphan-route', 'notes', 'absent', '1');",
        )
        .expect("install scoped rows");
        conn.execute(
            "INSERT INTO notes VALUES ('stale', ?1, 'stale', '1')",
            [CircleId::from_bytes([1; 16]).to_string()],
        )
        .expect("install stale root");
        conn.execute_batch(
            "INSERT INTO comments VALUES ('stale-child', 'stale', 'stale', '1');
             INSERT INTO _coven_row_routes VALUES ('stale-route', 'notes', 'stale', '1');
             INSERT INTO _coven_row_routes VALUES ('stale-child-route', 'comments', 'stale-child', '1');",
        )
        .expect("install stale subtree");
        let inactive = CircleId::from_bytes([2; 16]);
        conn.execute(
            "INSERT INTO notes VALUES ('inactive', ?1, 'inactive', '1')",
            [inactive.to_string()],
        )
        .expect("install inactive root");
        conn.execute(
            "INSERT INTO _coven_row_routes VALUES ('inactive-route', 'notes', 'inactive', '1')",
            [],
        )
        .expect("install inactive route");
        conn.execute(
            "INSERT INTO _coven_audience VALUES ('inactive-route', ?1, '1')",
            [inactive.to_string()],
        )
        .expect("install matching inactive mirror");
        let tables = vec![
            SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
            SyncedTable::new("comments", RowIdentity::IndependentUuid),
        ];
        let gates = Gates::from_tables(&conn, &tables).expect("build scoped gates");

        prune_ineligible_scoped_rows(&conn, &gates, &BTreeSet::from([inactive]))
            .expect("prune stale scoped rows");

        let notes: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
            .expect("count notes");
        let comments: i64 = conn
            .query_row("SELECT COUNT(*) FROM comments", [], |row| row.get(0))
            .expect("count comments");
        let routes: i64 = conn
            .query_row("SELECT COUNT(*) FROM _coven_row_routes", [], |row| {
                row.get(0)
            })
            .expect("count routes");
        assert_eq!((notes, comments, routes), (1, 1, 2));
    }
}

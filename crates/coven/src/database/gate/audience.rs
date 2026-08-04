//! Audience resolution and atomic partitioning of one captured host changeset.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use rusqlite::ffi;
use rusqlite::Connection;

use super::ffi::{collect_deletes, for_each_change, ChangeRow, Changegroup};
use super::model::{foreign_keys, rows_referencing, truthy, Gates, TableGate};
use super::outbound::{
    full_state_diff, gate_store_outbound, query_column_text, row_id_for_column_value,
    FullStateDirection,
};
use super::{query_mapped_rows, query_row_optional, GateError};
use crate::database::quote_ident;
use crate::protocol::circle::{
    row_routing_id, Audience, CircleControlCoord, CircleId, RowRoutingKey,
};
use crate::protocol::circle_activation::CircleCurrentState;

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
pub(crate) struct AudienceMove {
    pub(crate) source: Audience,
    pub(crate) destination: Audience,
    pub(crate) rows: BTreeSet<(String, String)>,
    /// The moved row's `_updated_at` after the change that moved it — the version
    /// at which its whole component now lives in `destination`, and the stamp the
    /// routing transitions for that component carry.
    pub(crate) stamp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionedAudienceWrite {
    pub(crate) partitions: Vec<AudiencePartition>,
    pub(crate) moves: Vec<AudienceMove>,
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
    store_mirror: Vec<u8>,
    private_routes: BTreeMap<Audience, Vec<u8>>,
    deleted_rows: BTreeMap<(String, String), Audience>,
}

#[derive(Default)]
pub(crate) struct StoreAudienceTransitions {
    by_routing_id: HashMap<String, (Audience, String)>,
}

#[derive(Debug)]
pub(crate) struct InboundStoreChangesets {
    pub(crate) mirror: Vec<u8>,
    pub(crate) rows: Vec<u8>,
}

impl RoutingChanges {
    pub(crate) fn empty() -> Self {
        Self {
            store_mirror: Vec::new(),
            private_routes: BTreeMap::new(),
            deleted_rows: BTreeMap::new(),
        }
    }
}

struct PartitionGroup {
    control: Option<CirclePartitionControl>,
    group: Changegroup,
}

pub(crate) fn filter_inbound_circle_changeset(
    conn: &Connection,
    changeset: &[u8],
    circle_id: CircleId,
    store_transitions: &StoreAudienceTransitions,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<Vec<u8>, GateError> {
    unsafe {
        let package_audience = Audience::Circle(circle_id);
        let (normalized, _) = normalize_inbound_private_routes_raw(
            conn,
            changeset,
            &package_audience,
            store_transitions,
            gates,
            routing_key,
        )?;
        filter_inbound_audience_rows_raw(conn, &normalized, &package_audience, gates, routing_key)
    }
}

pub(crate) fn filter_inbound_store_rows(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<Vec<u8>, GateError> {
    unsafe {
        filter_inbound_audience_rows_raw(conn, changeset, &Audience::Store, gates, routing_key)
    }
}

unsafe fn filter_inbound_audience_rows_raw(
    conn: &Connection,
    changeset: &[u8],
    package_audience: &Audience,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<Vec<u8>, GateError> {
    let allow_unscoped = package_audience == &Audience::Store;
    for_each_change(changeset, |_iter, row| {
        if row.table == "_coven_audience" {
            return Err(GateError::InvalidInboundAudiencePackage(
                "audience row package contains the Store audience mirror".to_string(),
            ));
        }
        if row.table != "_coven_row_routes" && !gates.table_is_scoped(&row.table) && !allow_unscoped
        {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "Circle package contains unscoped table {}",
                row.table
            )));
        }
        if row.op != ffi::SQLITE_DELETE {
            if let Some(TableGate::ScopedRoot { audience_col }) = gates.tables.get(&row.table) {
                if let Some(value) = row.new_value(audience_col.index) {
                    let row_audience = Audience::from_column(value).map_err(|error| {
                        GateError::InvalidInboundAudiencePackage(format!(
                            "scoped row {} has an invalid audience: {error}",
                            row.table
                        ))
                    })?;
                    if &row_audience != package_audience {
                        return Err(GateError::InvalidInboundAudiencePackage(format!(
                            "scoped row {} is packaged for a different audience than its row value",
                            row.table
                        )));
                    }
                }
            }
        }
        Ok(())
    })?;

    let group = Changegroup::new()?;
    group.set_schema(conn.handle())?;
    for_each_change(changeset, |iter, row| {
        if row.table != "_coven_row_routes" && !gates.table_is_scoped(&row.table) {
            group.add_change(iter)?;
            return Ok(());
        }
        let routing_id = if row.table == "_coven_row_routes" {
            row.pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?
                .to_string()
        } else {
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            row_routing_id(routing_key, &row.table, row_id).to_string()
        };
        let winning_audience = winning_store_audience(conn, &routing_id)?;
        if winning_audience.as_ref() == Some(package_audience) {
            group.add_change(iter)?;
        }
        Ok(())
    })?;
    group.output()
}

pub(crate) fn align_inbound_scoped_root_audiences(
    conn: &Connection,
    changeset: &[u8],
    package_audience: &Audience,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<(), GateError> {
    unsafe {
        for_each_change(changeset, |_iter, row| {
            if row.op == ffi::SQLITE_DELETE {
                return Ok(());
            }
            let Some(TableGate::ScopedRoot { audience_col }) = gates.tables.get(&row.table) else {
                return Ok(());
            };
            let row_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            let routing_id = row_routing_id(routing_key, &row.table, row_id).to_string();
            let winning_audience = winning_store_audience(conn, &routing_id)?;
            if winning_audience.as_ref() != Some(package_audience) {
                return Err(GateError::InvalidInboundAudiencePackage(format!(
                    "eligible {}.{row_id} package no longer matches its winning Store audience",
                    row.table
                )));
            }
            let updated = conn
                .execute(
                    &format!(
                        "UPDATE {} SET {} = ?1 WHERE id = ?2",
                        quote_ident(&row.table),
                        quote_ident(&audience_col.name),
                    ),
                    rusqlite::params![package_audience.column_value(), row_id],
                )
                .map_err(|source| {
                    GateError::Sql(
                        format!("align inbound audience for {}.{row_id}", row.table),
                        source,
                    )
                })?;
            if updated != 1 {
                return Err(GateError::InvalidInboundAudiencePackage(format!(
                    "eligible {}.{row_id} did not materialize exactly one scoped root",
                    row.table
                )));
            }
            Ok(())
        })
    }
}

fn winning_store_audience(
    conn: &Connection,
    routing_id: &str,
) -> Result<Option<Audience>, GateError> {
    query_row_optional(
        conn,
        "SELECT circle_id FROM _coven_audience WHERE routing_id = ?1",
        [routing_id],
        |record| record.get::<_, Option<String>>(0),
    )?
    .map(|circle_id| {
        Audience::from_column(circle_id.as_deref()).map_err(|error| {
            GateError::InvalidInboundAudiencePackage(format!(
                "winning Store audience for {routing_id} is invalid: {error}"
            ))
        })
    })
    .transpose()
}

pub(crate) fn normalize_inbound_store_changeset(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<InboundStoreChangesets, GateError> {
    let store_transitions = store_audience_transitions(changeset)?;
    let normalized = unsafe {
        normalize_inbound_private_routes_raw(
            conn,
            changeset,
            &Audience::Store,
            &store_transitions,
            gates,
            routing_key,
        )
        .map(|(normalized, _)| normalized)?
    };
    unsafe {
        let mirror = Changegroup::new()?;
        mirror.set_schema(conn.handle())?;
        let rows = Changegroup::new()?;
        rows.set_schema(conn.handle())?;
        for_each_change(&normalized, |iter, row| {
            if row.table == "_coven_audience" {
                mirror.add_change(iter)
            } else {
                rows.add_change(iter)
            }
        })?;
        Ok(InboundStoreChangesets {
            mirror: mirror.output()?,
            rows: rows.output()?,
        })
    }
}

type PackageRoutes = HashMap<(String, String), (String, String)>;

unsafe fn normalize_inbound_private_routes_raw(
    conn: &Connection,
    changeset: &[u8],
    package_audience: &Audience,
    store_transitions: &StoreAudienceTransitions,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<(Vec<u8>, PackageRoutes), GateError> {
    let package_routes = validate_inbound_private_routes_raw(
        conn,
        changeset,
        package_audience,
        store_transitions,
        gates,
        routing_key,
    )?;
    let group = Changegroup::new()?;
    group.set_schema(conn.handle())?;
    for_each_change(changeset, |iter, row| {
        if row.table != "_coven_row_routes" {
            group.add_change(iter)?;
        }
        Ok(())
    })?;
    let mut rows = package_routes
        .iter()
        .map(|((table, row_id), (routing_id, stamp))| {
            (
                routing_id.clone(),
                table.clone(),
                row_id.clone(),
                stamp.clone(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    let canonical_routes = private_route_insert_changeset(&rows)?;
    for_each_change(&canonical_routes, |iter, _row| group.add_change(iter))?;
    Ok((group.output()?, package_routes))
}

unsafe fn validate_inbound_private_routes_raw(
    conn: &Connection,
    changeset: &[u8],
    package_audience: &Audience,
    store_transitions: &StoreAudienceTransitions,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<PackageRoutes, GateError> {
    let mut package_routes = PackageRoutes::new();
    let mut package_row_inserts = HashSet::<(String, String)>::new();
    for_each_change(changeset, |_iter, row| {
        if row.table == "_coven_audience" {
            return Ok(());
        }
        if row.table != "_coven_row_routes" {
            if gates.table_is_scoped(&row.table) && row.op == ffi::SQLITE_INSERT {
                let row_id = row
                    .pk()
                    .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
                let columns = super::gate_table_columns(conn, &row.table)?;
                let stamp_index = columns
                    .iter()
                    .position(|column| column == "_updated_at")
                    .ok_or_else(|| {
                        GateError::MissingFkColumn(row.table.clone(), "_updated_at".to_string())
                    })?;
                row.new_value(stamp_index).flatten().ok_or_else(|| {
                    GateError::InvalidInboundAudiencePackage(format!(
                        "complete row INSERT {}.{row_id} has no _updated_at",
                        row.table
                    ))
                })?;
                package_row_inserts.insert((row.table.clone(), row_id.to_string()));
            }
            return Ok(());
        }
        if row.op != ffi::SQLITE_INSERT {
            return Err(GateError::InvalidInboundAudiencePackage(
                "private routes must be complete INSERT images".to_string(),
            ));
        }
        let routing_id = row.new_value(0).flatten().ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage(
                "private route INSERT has no routing id".to_string(),
            )
        })?;
        let table = row.new_value(1).flatten().ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage("private route has no table name".to_string())
        })?;
        let row_id = row.new_value(2).flatten().ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage("private route has no row id".to_string())
        })?;
        let stamp = row.new_value(3).flatten().ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage("private route has no _updated_at".to_string())
        })?;
        if !gates.table_is_scoped(table) {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route names unscoped table {table}"
            )));
        }
        let identity = gates.row_identity(table).ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage(format!(
                "private route names undeclared table {table}"
            ))
        })?;
        identity.validate(table, row_id).map_err(|error| {
            GateError::InvalidInboundAudiencePackage(format!(
                "private route row identity is invalid: {error}"
            ))
        })?;
        let expected_routing_id = row_routing_id(routing_key, table, row_id).to_string();
        if routing_id != expected_routing_id {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route id does not authenticate {table}.{row_id}"
            )));
        }
        if package_routes
            .insert(
                (table.to_string(), row_id.to_string()),
                (routing_id.to_string(), stamp.to_string()),
            )
            .is_some()
        {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "duplicate private route for {table}.{row_id}"
            )));
        }
        Ok(())
    })?;
    for (row, (routing_id, route_stamp)) in &package_routes {
        if !package_row_inserts.contains(row) {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route for {}.{} has no complete row INSERT",
                row.0, row.1
            )));
        }
        let (transition_audience, audience_stamp) = store_transitions
            .by_routing_id
            .get(routing_id)
            .ok_or_else(|| {
                GateError::InvalidInboundAudiencePackage(format!(
                    "private route for {}.{} has no Store audience transition",
                    row.0, row.1
                ))
            })?;
        if transition_audience != package_audience {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route for {}.{} is packaged for a different audience than its Store transition",
                row.0, row.1
            )));
        }
        if route_stamp != audience_stamp {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "private route for {}.{} has a different _updated_at than its Store audience transition",
                row.0, row.1
            )));
        }
    }
    for row in &package_row_inserts {
        if package_routes.contains_key(row) {
            continue;
        }
        let existing = query_row_optional(
            conn,
            "SELECT routing_id FROM _coven_row_routes
             WHERE table_name = ?1 AND row_id = ?2",
            (&row.0, &row.1),
            |record| record.get::<_, String>(0),
        )?
        .ok_or_else(|| {
            GateError::InvalidInboundAudiencePackage(format!(
                "scoped row INSERT {}.{} has no private route",
                row.0, row.1
            ))
        })?;
        let expected = row_routing_id(routing_key, &row.0, &row.1).to_string();
        if existing != expected {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "stored private route does not authenticate {}.{}",
                row.0, row.1
            )));
        }
    }
    Ok(package_routes)
}

pub(crate) fn store_audience_transitions(
    changeset: &[u8],
) -> Result<StoreAudienceTransitions, GateError> {
    let mut transitions = StoreAudienceTransitions::default();
    unsafe {
        for_each_change(changeset, |_iter, row| {
            if row.table != "_coven_audience"
                || (row.op != ffi::SQLITE_INSERT && row.op != ffi::SQLITE_UPDATE)
            {
                return Ok(());
            }
            let routing_id = row
                .pk()
                .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
            let circle_id = row.new_value(1).ok_or_else(|| {
                GateError::InvalidInboundAudiencePackage(format!(
                    "Store audience transition {routing_id} has no audience"
                ))
            })?;
            let audience = Audience::from_column(circle_id).map_err(|error| {
                GateError::InvalidInboundAudiencePackage(format!(
                    "Store audience transition {routing_id} has an invalid audience: {error}"
                ))
            })?;
            if audience == Audience::Local {
                return Err(GateError::InvalidInboundAudiencePackage(format!(
                    "Store audience transition {routing_id} has a Local audience"
                )));
            }
            let stamp = row.new_value(2).flatten().ok_or_else(|| {
                GateError::InvalidInboundAudiencePackage(format!(
                    "Store audience transition {routing_id} has no _updated_at"
                ))
            })?;
            if transitions
                .by_routing_id
                .insert(routing_id.to_string(), (audience, stamp.to_string()))
                .is_some()
            {
                return Err(GateError::InvalidInboundAudiencePackage(format!(
                    "Store package contains duplicate audience transitions for {routing_id}"
                )));
            }
            Ok(())
        })?;
    }
    Ok(transitions)
}

pub(crate) fn retain_snapshot_audience_rows(
    conn: &Connection,
    gates: &Gates,
    audience: &Audience,
) -> Result<(), GateError> {
    let Audience::Circle(circle_id) = audience else {
        return Err(GateError::InvalidMaterializedRouting(
            "audience row projection requires a Circle".to_string(),
        ));
    };
    let retained = circle_snapshot_retained_rows(conn, gates, *circle_id)?;
    let mut tables = gates
        .synced_table_names()
        .map(str::to_string)
        .collect::<Vec<_>>();
    tables.sort();
    conn.execute_batch(
        "CREATE TEMP TABLE snapshot_retained_rows (
             table_name TEXT NOT NULL,
             row_id TEXT NOT NULL,
             PRIMARY KEY (table_name, row_id)
         ) STRICT;",
    )
    .map_err(|error| GateError::Sql("create snapshot retained rows".to_string(), error))?;
    for (table, row_id) in &retained {
        conn.execute(
            "INSERT INTO snapshot_retained_rows (table_name, row_id) VALUES (?1, ?2)",
            (table, row_id),
        )
        .map_err(|error| GateError::Sql("retain snapshot row".to_string(), error))?;
    }
    tables.reverse();
    for table in tables {
        conn.execute(
            &format!(
                "DELETE FROM {}
                 WHERE NOT EXISTS (
                     SELECT 1 FROM snapshot_retained_rows AS retained
                     WHERE retained.table_name = ?1
                       AND retained.row_id = {}.id
                 )",
                quote_ident(&table),
                quote_ident(&table),
            ),
            [&table],
        )
        .map_err(|error| GateError::Sql(format!("scope {table} to Circle snapshot rows"), error))?;
    }
    conn.execute_batch(
        "DELETE FROM _coven_row_routes
         WHERE NOT EXISTS (
             SELECT 1 FROM snapshot_retained_rows AS retained
             WHERE retained.table_name = _coven_row_routes.table_name
               AND retained.row_id = _coven_row_routes.row_id
         );
         DELETE FROM _coven_audience
         WHERE NOT EXISTS (
             SELECT 1 FROM _coven_row_routes AS route
             WHERE route.routing_id = _coven_audience.routing_id
         );
         DROP TABLE snapshot_retained_rows;",
    )
    .map_err(|error| GateError::Sql("scope Circle snapshot routing".to_string(), error))?;
    Ok(())
}

fn circle_snapshot_retained_rows(
    conn: &Connection,
    gates: &Gates,
    circle_id: CircleId,
) -> Result<BTreeSet<(String, String)>, GateError> {
    let audience = Audience::Circle(circle_id);
    let mut tables = gates
        .synced_table_names()
        .map(str::to_string)
        .collect::<Vec<_>>();
    tables.sort();
    let mut retained = BTreeSet::<(String, String)>::new();
    let mut pending = VecDeque::new();
    for table in &tables {
        let row_ids = query_mapped_rows(
            conn,
            &format!("SELECT id FROM {}", quote_ident(table)),
            [],
            |row| row.get::<_, String>(0),
        )?;
        for row_id in row_ids {
            if live_row_audience(conn, gates, table, &row_id)? == audience {
                pending.push_back((table.clone(), row_id));
            }
        }
    }

    while let Some((table, row_id)) = pending.pop_front() {
        if !retained.insert((table.clone(), row_id.clone())) {
            continue;
        }
        for (child_column, parent_table, parent_column) in foreign_keys(conn, &table)? {
            if !gates.is_synced_table(&parent_table) {
                continue;
            }
            let parent_key = query_row_optional(
                conn,
                &format!(
                    "SELECT {} FROM {} WHERE id = ?1",
                    quote_ident(&child_column),
                    quote_ident(&table),
                ),
                [&row_id],
                |row| row.get::<_, Option<String>>(0),
            )?
            .ok_or_else(|| GateError::MissingAudienceRow {
                table: table.clone(),
                row_id: row_id.clone(),
            })?;
            let Some(parent_key) = parent_key else {
                continue;
            };
            let parent_id =
                row_id_for_column_value(conn, &parent_table, &parent_column, &parent_key)?
                    .ok_or_else(|| GateError::MissingAudienceParent {
                        table: table.clone(),
                        row_id: Some(row_id.clone()),
                        parent: parent_table.clone(),
                    })?;
            let parent_audience = live_row_audience(conn, gates, &parent_table, &parent_id)?;
            if parent_audience != Audience::Store && parent_audience != audience {
                return Err(GateError::InvalidAudience {
                    table: table.clone(),
                    value: audience.column_value(),
                    reason: format!(
                        "Circle snapshot relationship through {child_column} references \
                         {parent_table}.{parent_id} in {parent_audience:?}"
                    ),
                });
            }
            pending.push_back((parent_table, parent_id));
        }
    }
    Ok(retained)
}

pub(crate) fn validate_snapshot_routing_state(
    conn: &Connection,
    gates: &Gates,
    routing_key: &RowRoutingKey,
    snapshot_audience: &Audience,
) -> Result<(), GateError> {
    if matches!(snapshot_audience, Audience::Local) {
        return Err(GateError::InvalidMaterializedRouting(
            "Local rows cannot enter a snapshot".to_string(),
        ));
    }
    if let Audience::Circle(circle_id) = snapshot_audience {
        let expected = circle_snapshot_retained_rows(conn, gates, *circle_id)?;
        for table in gates.synced_table_names() {
            let row_ids = query_mapped_rows(
                conn,
                &format!("SELECT id FROM {}", quote_ident(table)),
                [],
                |row| row.get::<_, String>(0),
            )?;
            for row_id in row_ids {
                if expected.contains(&(table.to_string(), row_id.clone())) {
                    continue;
                }
                return Err(GateError::InvalidMaterializedRouting(format!(
                    "Circle {circle_id} snapshot contains row {table}.{row_id} \
                     outside its exact audience closure"
                )));
            }
        }
    }
    if !gates.has_scoped_graph() {
        return Ok(());
    }

    let mut scoped_tables = gates
        .tables
        .keys()
        .filter(|table| gates.table_is_scoped(table))
        .cloned()
        .collect::<Vec<_>>();
    scoped_tables.sort();
    let mut materialized_rows = HashSet::new();
    let mut materialized_routing_ids = HashSet::new();
    let mut audience_mirrors = HashMap::new();
    let mirror_rows = query_mapped_rows(
        conn,
        "SELECT routing_id, circle_id, _updated_at
         FROM _coven_audience
         ORDER BY routing_id",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    for (routing_id, circle_id, stamp) in mirror_rows {
        routing_id
            .parse::<crate::protocol::circle::RowRoutingId>()
            .map_err(|error| {
                GateError::InvalidMaterializedRouting(format!(
                    "Store audience mirror has invalid routing id {routing_id:?}: {error}"
                ))
            })?;
        let audience = Audience::from_column(circle_id.as_deref()).map_err(|error| {
            GateError::InvalidMaterializedRouting(format!(
                "Store audience mirror has invalid audience for {routing_id}: {error}"
            ))
        })?;
        if audience == Audience::Local {
            return Err(GateError::InvalidMaterializedRouting(format!(
                "Store audience mirror has invalid audience for {routing_id}: Local"
            )));
        }
        audience_mirrors.insert(routing_id, (audience, stamp));
    }

    for table in scoped_tables {
        let identity = gates.row_identity(&table).ok_or_else(|| {
            GateError::InvalidMaterializedRouting(format!(
                "scoped table {table} has no declared row identity"
            ))
        })?;
        let sql = format!(
            "SELECT {id} FROM {table} ORDER BY {id}",
            id = quote_ident("id"),
            table = quote_ident(&table),
        );
        let rows = query_mapped_rows(conn, &sql, [], |row| row.get::<_, String>(0))?;
        for row_id in rows {
            identity.validate(&table, &row_id).map_err(|error| {
                GateError::InvalidMaterializedRouting(format!(
                    "row identity {table}.{row_id} is invalid: {error}"
                ))
            })?;
            let (routing_id, route_stamp) = query_row_optional(
                conn,
                "SELECT routing_id, _updated_at
                 FROM _coven_row_routes
                 WHERE table_name = ?1 AND row_id = ?2",
                (&table, &row_id),
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .ok_or_else(|| {
                GateError::InvalidMaterializedRouting(format!(
                    "scoped row {table}.{row_id} has no private route"
                ))
            })?;
            let expected_routing_id = row_routing_id(routing_key, &table, &row_id).to_string();
            if routing_id != expected_routing_id {
                return Err(GateError::InvalidMaterializedRouting(format!(
                    "private route id does not authenticate {table}.{row_id}"
                )));
            }
            let mirror = audience_mirrors.get(&routing_id);
            let audience = live_row_audience(conn, gates, &table, &row_id)?;
            match (snapshot_audience, &audience) {
                (_, Audience::Local) => {
                    return Err(GateError::InvalidMaterializedRouting(format!(
                        "Store snapshot contains Local row {table}.{row_id}"
                    )));
                }
                (Audience::Store, Audience::Circle(_)) => {
                    return Err(GateError::InvalidMaterializedRouting(format!(
                        "Store snapshot contains Circle row {table}.{row_id}"
                    )));
                }
                (Audience::Circle(expected), Audience::Circle(actual)) if expected != actual => {
                    return Err(GateError::InvalidMaterializedRouting(format!(
                        "Circle {expected} snapshot contains row {table}.{row_id} for Circle {actual}"
                    )));
                }
                _ => {
                    let (mirrored, mirror_stamp) = mirror.ok_or_else(|| {
                        GateError::InvalidMaterializedRouting(format!(
                            "shared row {table}.{row_id} has no Store audience mirror"
                        ))
                    })?;
                    if mirrored != &audience {
                        return Err(GateError::InvalidMaterializedRouting(format!(
                            "Store audience mirror for {table}.{row_id} differs from its row"
                        )));
                    }
                    if mirror_stamp != &route_stamp {
                        return Err(GateError::InvalidMaterializedRouting(format!(
                            "Store audience mirror for {table}.{row_id} has a different _updated_at than its private route"
                        )));
                    }
                }
            }
            materialized_routing_ids.insert(routing_id);
            materialized_rows.insert((table.clone(), row_id));
        }
    }

    let private_routes = query_mapped_rows(
        conn,
        "SELECT table_name, row_id
         FROM _coven_row_routes
         ORDER BY table_name, row_id",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    for (table, row_id) in private_routes {
        if !materialized_rows.contains(&(table.clone(), row_id.clone())) {
            return Err(GateError::InvalidMaterializedRouting(format!(
                "private route {table}.{row_id} has no materialized scoped row"
            )));
        }
    }
    for (routing_id, (audience, _)) in audience_mirrors {
        let must_be_materialized =
            audience == Audience::Store || matches!(snapshot_audience, Audience::Circle(_));
        if must_be_materialized && !materialized_routing_ids.contains(&routing_id) {
            return Err(GateError::InvalidMaterializedRouting(format!(
                "Store audience mirror has no materialized row for {routing_id}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn prune_private_routes_without_rows(
    conn: &Connection,
    gates: &Gates,
) -> Result<(), GateError> {
    let routes = query_mapped_rows(
        conn,
        "SELECT routing_id, table_name, row_id
         FROM _coven_row_routes
         ORDER BY table_name, row_id",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    for (routing_id, table, row_id) in routes {
        if !gates.table_is_scoped(&table) {
            return Err(GateError::InvalidMaterializedRouting(format!(
                "private route names unscoped table {table}"
            )));
        }
        let sql = format!(
            "SELECT 1 FROM {} WHERE {} = ?1",
            quote_ident(&table),
            quote_ident("id")
        );
        if query_row_optional(conn, &sql, [&row_id], |_| Ok(()))?.is_some() {
            continue;
        }
        conn.execute(
            "DELETE FROM _coven_row_routes WHERE routing_id = ?1",
            [&routing_id],
        )
        .map_err(|error| {
            GateError::Sql(
                format!("scope private route {table}.{row_id} to snapshot rows"),
                error,
            )
        })?;
    }
    Ok(())
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
                let sql = format!(
                    "SELECT {} FROM {} WHERE id = ?1",
                    quote_ident(&fk_column),
                    quote_ident(&table),
                );
                let parent_key = query_row_optional(conn, &sql, [&row_id], |record| {
                    record.get::<_, Option<String>>(0)
                })?
                .ok_or_else(|| GateError::MissingAudienceRow {
                    table: table.clone(),
                    row_id: row_id.clone(),
                })?;
                let Some(parent_key) = parent_key else {
                    continue;
                };
                let parent_id =
                    row_id_for_column_value(conn, &parent_table, &parent_column, &parent_key)?
                        .ok_or_else(|| GateError::MissingAudienceParent {
                            table: table.clone(),
                            row_id: Some(row_id.clone()),
                            parent: parent_table.clone(),
                        })?;
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

fn private_route_insert_changeset(
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

struct AudiencePartitionGroups<'connection> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::synced_schema::{RowIdentity, SyncedTable};
    use rusqlite::session::{ConflictAction, Session};

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
             ) STRICT;
             CREATE TABLE row_blob_locators (
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 column_name TEXT NOT NULL,
                 row_stamp TEXT NOT NULL,
                 audience_authority TEXT NOT NULL CHECK (json_valid(audience_authority)),
                 remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
                 PRIMARY KEY (table_name, row_id, column_name, row_stamp)
             ) STRICT;",
        )
        .expect("create inbound audience test schema");
    }

    fn note_gates(conn: &Connection) -> Gates {
        Gates::from_tables(
            conn,
            &[SyncedTable::new("notes", RowIdentity::SharedKey).scoped_by("audience")],
        )
        .expect("build scoped gates")
    }

    fn routing_key() -> RowRoutingKey {
        crate::protocol::circle::derive_row_routing_key(
            &crate::encryption::EncryptionService::from_key([7; 32]),
            crate::protocol::store_commit::ObjectHash::digest(b"audience test"),
        )
        .expect("derive test row-routing key")
    }

    fn store_transitions(
        transitions: impl IntoIterator<Item = (String, Audience, String)>,
    ) -> StoreAudienceTransitions {
        StoreAudienceTransitions {
            by_routing_id: transitions
                .into_iter()
                .map(|(routing_id, audience, stamp)| (routing_id, (audience, stamp)))
                .collect(),
        }
    }

    /// Capturing a write's routing is the write that installs its audience mirror,
    /// and it publishes that mirror by snapshotting what it just wrote — so it runs
    /// exactly once per write. A second pass over the same write re-upserts the rows
    /// it already wrote, sees no change, and hands back an empty mirror: the moved
    /// rows would reach their destination audience with nothing telling the devices
    /// there that they moved. Whatever a write has to decide between capture and
    /// partition (an audience move's blob row stamps, today) is read separately and
    /// this runs after it.
    #[test]
    fn capturing_a_write_s_routing_publishes_its_mirror_once() {
        let conn = Connection::open_in_memory().expect("open connection");
        routing_schema(&conn);
        let gates = note_gates(&conn);
        let key = routing_key();
        let circle = CircleId::from_bytes([3; 16]);
        conn.execute("INSERT INTO notes VALUES ('moved', NULL, 'body', '1')", [])
            .expect("insert the note in the Store audience");
        let mut session = Session::new(&conn).expect("create session");
        session.attach(Some("notes")).expect("attach notes");
        conn.execute(
            "UPDATE notes SET audience = ?1, _updated_at = '2' WHERE id = 'moved'",
            [circle.to_string()],
        )
        .expect("move the note into the Circle");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract the move changeset");
        drop(session);

        let first = capture_routing_changes(&conn, &changeset, &gates, &key)
            .expect("capture the move's routing");
        let mirror =
            crate::database::walk_changeset(&first.store_mirror).expect("walk the Store mirror");
        assert_eq!(
            mirror.len(),
            1,
            "the move publishes the moved row's audience mirror: {mirror:?}",
        );

        let again = capture_routing_changes(&conn, &changeset, &gates, &key)
            .expect("capture the same move's routing again");
        assert!(
            crate::database::walk_changeset(&again.store_mirror)
                .expect("walk the repeated Store mirror")
                .is_empty(),
            "a second capture has nothing left to publish",
        );
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
        let key = routing_key();
        let first_route = row_routing_id(&key, "notes", "first").to_string();
        let second_route = row_routing_id(&key, "notes", "second").to_string();
        source
            .execute(
                "INSERT INTO notes VALUES (?1, ?2, 'first', '1')",
                ("first", first.to_string()),
            )
            .expect("insert first note");
        source
            .execute(
                "INSERT INTO notes VALUES (?1, ?2, 'second', '1')",
                ("second", first.to_string()),
            )
            .expect("insert second note");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'first', '1')",
                [&first_route],
            )
            .expect("insert first route");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'second', '1')",
                [&second_route],
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
                "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
                (&first_route, first.to_string()),
            )
            .expect("install first mirror");
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
                (&second_route, second.to_string()),
            )
            .expect("install second mirror");

        let transitions = store_transitions([
            (
                first_route.clone(),
                Audience::Circle(first),
                "1".to_string(),
            ),
            (
                second_route.clone(),
                Audience::Circle(first),
                "1".to_string(),
            ),
        ]);
        let filtered = filter_inbound_circle_changeset(
            &target,
            &changeset,
            first,
            &transitions,
            &note_gates(&target),
            &key,
        )
        .expect("filter first Circle package");
        let rows = crate::database::walk_changeset(&filtered).expect("walk filtered changeset");
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.table == "notes" && row.pk() == Some("first")));
        assert!(rows.iter().any(|row| {
            row.table == "_coven_row_routes" && row.pk() == Some(first_route.as_str())
        }));
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
            &StoreAudienceTransitions::default(),
            &note_gates(&target),
            &routing_key(),
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

        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            circle,
            &StoreAudienceTransitions::default(),
            &note_gates(&target),
            &routing_key(),
        )
        .expect_err("Circle package route must name a scoped table");
        assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
    }

    #[test]
    fn inbound_circle_filter_rejects_a_private_route_update() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        source
            .execute_batch(
                "CREATE TABLE tasks (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("create second scoped table");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES ('route', 'notes', 'row', '1')",
                [],
            )
            .expect("seed private route");
        let mut session = Session::new(&source).expect("create source session");
        session
            .attach(Some("_coven_row_routes"))
            .expect("attach private routes");
        source
            .execute(
                "UPDATE _coven_row_routes
                 SET table_name = 'tasks', row_id = 'row2', _updated_at = '2'
                 WHERE routing_id = 'route'",
                [],
            )
            .expect("update private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract route update");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute_batch(
                "CREATE TABLE tasks (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("create second scoped table");
        let circle = CircleId::from_bytes([1; 16]);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
                [circle.to_string()],
            )
            .expect("install winning mirror");
        let gates = Gates::from_tables(
            &target,
            &[
                SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
                SyncedTable::new("tasks", RowIdentity::IndependentUuid).scoped_by("audience"),
            ],
        )
        .expect("build scoped gates");

        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            circle,
            &StoreAudienceTransitions::default(),
            &gates,
            &routing_key(),
        )
        .expect_err("private routes must be complete INSERT images");
        assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
    }

    #[test]
    fn inbound_circle_filter_rejects_a_private_route_delete() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES ('route', 'notes', 'row', '1')",
                [],
            )
            .expect("seed private route");
        let mut session = Session::new(&source).expect("create source session");
        session
            .attach(Some("_coven_row_routes"))
            .expect("attach private routes");
        source
            .execute(
                "DELETE FROM _coven_row_routes WHERE routing_id = 'route'",
                [],
            )
            .expect("delete private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract route delete");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let circle = CircleId::from_bytes([1; 16]);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
                [circle.to_string()],
            )
            .expect("install winning mirror");

        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            circle,
            &StoreAudienceTransitions::default(),
            &note_gates(&target),
            &routing_key(),
        )
        .expect_err("private routes must be complete INSERT images");
        assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
    }

    #[test]
    fn inbound_circle_filter_rejects_a_duplicate_private_route() {
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        // Two authenticated INSERT images for the same (table, row_id). A session
        // cannot capture both — the UNIQUE(table_name, row_id) constraint refuses
        // the second — so concatenate two single-route changesets to forge the
        // duplicate a malicious package could carry on the wire.
        let mut changeset = private_route_insert_changeset(&[(
            routing_id.clone(),
            "notes".to_string(),
            "row".to_string(),
            "1".to_string(),
        )])
        .expect("build first route image");
        changeset.extend(
            private_route_insert_changeset(&[(
                routing_id.clone(),
                "notes".to_string(),
                "row".to_string(),
                "1".to_string(),
            )])
            .expect("build second route image"),
        );

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let circle = CircleId::from_bytes([1; 16]);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
                (&routing_id, circle.to_string()),
            )
            .expect("install winning mirror");
        let transitions = store_transitions([(
            routing_id.clone(),
            Audience::Circle(circle),
            "1".to_string(),
        )]);

        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            circle,
            &transitions,
            &note_gates(&target),
            &key,
        )
        .expect_err("a package must not carry two routes for one row");
        assert!(
            error
                .to_string()
                .contains("duplicate private route for notes.row"),
            "{error}"
        );
    }

    #[test]
    fn inbound_private_route_must_authenticate_its_table_and_row() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
            .expect("insert scoped row");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
                ["0".repeat(64)],
            )
            .expect("insert forged private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract forged route package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let error = normalize_inbound_store_changeset(
            &target,
            &changeset,
            &note_gates(&target),
            &routing_key(),
        )
        .expect_err("forged private route id must be rejected");
        assert!(error
            .to_string()
            .contains("does not authenticate notes.row"));
    }

    #[test]
    fn inbound_private_route_must_accompany_its_complete_row_insert() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let mut session = Session::new(&source).expect("create source session");
        session
            .attach(Some("_coven_row_routes"))
            .expect("attach private routes");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
                [&routing_id],
            )
            .expect("insert orphan private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract orphan route package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let error =
            normalize_inbound_store_changeset(&target, &changeset, &note_gates(&target), &key)
                .expect_err("orphan private route must be rejected");
        assert!(error.to_string().contains("has no complete row INSERT"));
    }

    #[test]
    fn inbound_private_route_uses_its_audience_transition_stamp() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let circle = CircleId::from_bytes([1; 16]);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute(
                "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
                [circle.to_string()],
            )
            .expect("insert scoped row with an older content stamp");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '2')",
                [&routing_id],
            )
            .expect("insert private route with the audience transition stamp");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract Circle package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
                (&routing_id, circle.to_string()),
            )
            .expect("install winning audience transition");

        let filtered = filter_inbound_circle_changeset(
            &target,
            &changeset,
            circle,
            &store_transitions([(routing_id, Audience::Circle(circle), "2".to_string())]),
            &note_gates(&target),
            &key,
        )
        .expect("route stamp follows the audience transition, not row content");
        assert_eq!(
            crate::database::walk_changeset(&filtered)
                .expect("walk filtered Circle package")
                .len(),
            2
        );
    }

    #[test]
    fn inbound_circle_filter_omits_an_authenticated_route_after_a_newer_move() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let old_circle = CircleId::from_bytes([1; 16]);
        let new_circle = CircleId::from_bytes([2; 16]);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute(
                "INSERT INTO notes VALUES ('row', ?1, 'old move', '1')",
                [old_circle.to_string()],
            )
            .expect("insert old destination row");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
                [&routing_id],
            )
            .expect("insert old destination route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract old Circle package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
                (&routing_id, new_circle.to_string()),
            )
            .expect("install newer winning move");
        let filtered = filter_inbound_circle_changeset(
            &target,
            &changeset,
            old_circle,
            &store_transitions([(routing_id, Audience::Circle(old_circle), "1".to_string())]),
            &note_gates(&target),
            &key,
        )
        .expect("authenticate the old package before omitting it");

        assert!(crate::database::walk_changeset(&filtered)
            .expect("walk omitted package")
            .is_empty());
    }

    #[test]
    fn inbound_store_filter_omits_a_stale_edit_after_a_circle_move() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        source
            .execute("INSERT INTO notes VALUES ('row', NULL, 'base', '1')", [])
            .expect("insert source Store row");
        let mut session = Session::new(&source).expect("create source session");
        session.attach(Some("notes")).expect("attach source row");
        source
            .execute(
                "UPDATE notes SET body = 'stale edit', _updated_at = '2' WHERE id = 'row'",
                [],
            )
            .expect("edit source Store row");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract Store edit");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '3')",
                (&routing_id, CircleId::from_bytes([1; 16]).to_string()),
            )
            .expect("install winning Circle move");
        let filtered = filter_inbound_store_rows(&target, &changeset, &note_gates(&target), &key)
            .expect("filter stale Store edit");

        assert!(crate::database::walk_changeset(&filtered)
            .expect("walk omitted Store edit")
            .is_empty());
    }

    #[test]
    fn inbound_private_route_must_match_its_store_transition_audience() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let package_circle = CircleId::from_bytes([1; 16]);
        let transition_circle = CircleId::from_bytes([2; 16]);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute(
                "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
                [package_circle.to_string()],
            )
            .expect("insert packaged Circle row");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
                [&routing_id],
            )
            .expect("insert packaged private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract Circle package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
                (&routing_id, package_circle.to_string()),
            )
            .expect("install package Circle as the current winner");
        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            package_circle,
            &store_transitions([(
                routing_id,
                Audience::Circle(transition_circle),
                "1".to_string(),
            )]),
            &note_gates(&target),
            &key,
        )
        .expect_err("a package must match its own Store transition audience");

        assert!(error
            .to_string()
            .contains("packaged for a different audience"));
    }

    #[test]
    fn inbound_scoped_row_must_match_its_package_audience() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let package_circle = CircleId::from_bytes([1; 16]);
        let row_circle = CircleId::from_bytes([2; 16]);
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute(
                "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
                [row_circle.to_string()],
            )
            .expect("insert row for a different Circle");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
                [&routing_id],
            )
            .expect("insert private route");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract malformed Circle package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        target
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
                (&routing_id, package_circle.to_string()),
            )
            .expect("install package Circle as the current winner");
        let error = filter_inbound_circle_changeset(
            &target,
            &changeset,
            package_circle,
            &store_transitions([(
                routing_id,
                Audience::Circle(package_circle),
                "1".to_string(),
            )]),
            &note_gates(&target),
            &key,
        )
        .expect_err("a scoped row value must match its package audience");

        assert!(error
            .to_string()
            .contains("different audience than its row value"));
    }

    #[test]
    fn inbound_private_route_is_rebuilt_as_canonical_text() {
        let source = Connection::open_in_memory().expect("open source");
        source
            .execute_batch(
                "CREATE TABLE notes (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE _coven_row_routes (
                     routing_id PRIMARY KEY,
                     table_name,
                     row_id,
                     _updated_at,
                     UNIQUE(table_name, row_id)
                 );
                 CREATE TABLE _coven_audience (
                     routing_id TEXT PRIMARY KEY,
                     circle_id TEXT,
                     _updated_at TEXT NOT NULL
                 );",
            )
            .expect("create source schema with untyped private routes");
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        let mut session = Session::new(&source).expect("create source session");
        for table in ["notes", "_coven_audience", "_coven_row_routes"] {
            session.attach(Some(table)).expect("attach source table");
        }
        source
            .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
            .expect("insert scoped row");
        source
            .execute(
                "INSERT INTO _coven_row_routes VALUES (?1, ?2, ?3, ?4)",
                (
                    routing_id.as_bytes().to_vec(),
                    b"notes".to_vec(),
                    b"row".to_vec(),
                    b"1".to_vec(),
                ),
            )
            .expect("insert byte-valued private route");
        source
            .execute(
                "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
                [&routing_id],
            )
            .expect("insert Store audience transition");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract byte-valued route package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let normalized =
            normalize_inbound_store_changeset(&target, &changeset, &note_gates(&target), &key)
                .expect("normalize authenticated private route");
        for part in [normalized.mirror, normalized.rows] {
            target
                .apply_strm(
                    &mut &part[..],
                    None::<fn(&str) -> bool>,
                    |_conflict, _item| ConflictAction::SQLITE_CHANGESET_ABORT,
                )
                .expect("apply normalized package");
        }
        let types = target
            .query_row(
                "SELECT typeof(routing_id), typeof(table_name), typeof(row_id), typeof(_updated_at)
                 FROM _coven_row_routes",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .expect("read normalized private route types");
        assert_eq!(
            types,
            (
                "text".to_string(),
                "text".to_string(),
                "text".to_string(),
                "text".to_string(),
            )
        );
    }

    #[test]
    fn inbound_scoped_row_insert_must_have_a_private_route() {
        let source = Connection::open_in_memory().expect("open source");
        routing_schema(&source);
        let mut session = Session::new(&source).expect("create source session");
        session.attach(Some("notes")).expect("attach scoped table");
        source
            .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
            .expect("insert unbound scoped row");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract unbound row package");

        let target = Connection::open_in_memory().expect("open target");
        routing_schema(&target);
        let error = normalize_inbound_store_changeset(
            &target,
            &changeset,
            &note_gates(&target),
            &routing_key(),
        )
        .expect_err("unbound scoped row must be rejected");
        assert!(error.to_string().contains("has no private route"));
    }

    #[test]
    fn store_snapshot_routing_stamp_is_independent_from_content_stamp() {
        let conn = Connection::open_in_memory().expect("open snapshot");
        routing_schema(&conn);
        let key = routing_key();
        let routing_id = row_routing_id(&key, "notes", "row").to_string();
        conn.execute("INSERT INTO notes VALUES ('row', NULL, 'edited', '2')", [])
            .expect("insert content-edited Store row");
        conn.execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            [&routing_id],
        )
        .expect("insert private route at the audience-transition stamp");
        conn.execute(
            "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
            [&routing_id],
        )
        .expect("insert Store mirror at the audience-transition stamp");

        validate_snapshot_routing_state(&conn, &note_gates(&conn), &key, &Audience::Store)
            .expect("content-only edits must not invalidate unchanged routing");
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
            SyncedTable::new("comments", RowIdentity::IndependentUuid)
                .inherits_audience_through("note_id"),
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

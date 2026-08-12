use super::routing::*;
use super::*;

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

pub(crate) fn winning_store_audience(
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
        Audience::from_column(circle_id.as_deref()).map_err(|source| {
            GateError::InvalidInboundAudienceEncoding {
                context: format!("winning Store audience for {routing_id} is invalid"),
                source,
            }
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

pub(crate) type PackageRoutes = HashMap<(String, String), (String, String)>;

pub fn store_audience_transitions(changeset: &[u8]) -> Result<StoreAudienceTransitions, GateError> {
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
            let audience = Audience::from_column(circle_id).map_err(|source| {
                GateError::InvalidInboundAudienceEncoding {
                    context: format!(
                        "Store audience transition {routing_id} has an invalid audience"
                    ),
                    source,
                }
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

pub(crate) unsafe fn filter_inbound_audience_rows_raw(
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
                    let row_audience = Audience::from_column(value).map_err(|source| {
                        GateError::InvalidInboundAudienceEncoding {
                            context: format!("scoped row {} has an invalid audience", row.table),
                            source,
                        }
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

pub(crate) unsafe fn normalize_inbound_private_routes_raw(
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

pub(crate) unsafe fn validate_inbound_private_routes_raw(
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
                let columns = crate::gate::gate_table_columns(conn, &row.table)?;
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
        identity.validate(table, row_id).map_err(|source| {
            GateError::InvalidInboundRowIdentity {
                context: "private route row identity is invalid".to_string(),
                source,
            }
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

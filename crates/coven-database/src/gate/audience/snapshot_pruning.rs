use super::routing::*;
use super::*;

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
    let mut tables = gates.sorted_synced_table_names();
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

pub(crate) fn circle_snapshot_retained_rows(
    conn: &Connection,
    gates: &Gates,
    circle_id: CircleId,
) -> Result<BTreeSet<(String, String)>, GateError> {
    let audience = Audience::Circle(circle_id);
    let mut retained = BTreeSet::<(String, String)>::new();
    let mut pending = VecDeque::new();
    for table in gates.sorted_synced_table_names() {
        for row_id in all_row_ids(conn, &table)? {
            if live_row_audience(conn, gates, &table, &row_id)? == audience {
                pending.push_back((table.clone(), row_id));
            }
        }
    }

    while let Some((table, row_id)) = pending.pop_front() {
        if !retained.insert((table.clone(), row_id.clone())) {
            continue;
        }
        // A compatible parent is part of the Circle's snapshot too: the retained
        // set is the closure of the Circle's rows over their relationships.
        pending.extend(compatible_parent_rows(
            conn, gates, &table, &row_id, &audience,
        )?);
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
        for table in gates.sorted_synced_table_names() {
            for row_id in all_row_ids(conn, &table)? {
                if expected.contains(&(table.clone(), row_id.clone())) {
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
            .parse::<coven_protocol::circle::RowRoutingId>()
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

    for table in gates.scoped_table_names() {
        let identity = gates.row_identity(&table).ok_or_else(|| {
            GateError::InvalidMaterializedRouting(format!(
                "scoped table {table} has no declared row identity"
            ))
        })?;
        for row_id in all_row_ids(conn, &table)? {
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

pub(crate) fn delete_scoped_rows(
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

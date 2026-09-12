use super::*;

pub(crate) fn filter_inbound_circle_changeset(
    conn: &Connection,
    changeset: &[u8],
    circle_id: CircleId,
    store_transitions: &StoreAudienceTransitions,
    held_rows: &BTreeSet<(String, String)>,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<Vec<u8>, GateError> {
    unsafe {
        let package_audience = Audience::Circle(circle_id);
        validate_inbound_scoped_inserts_raw(
            conn,
            changeset,
            &package_audience,
            InboundRoutingSource::Published(store_transitions),
            held_rows,
            gates,
            routing_key,
        )?;
        filter_inbound_audience_rows_raw(conn, changeset, &package_audience, gates, routing_key)
    }
}

enum InboundRoutingSource<'a> {
    Published(&'a StoreAudienceTransitions),
    InstalledSnapshot,
}

pub(crate) fn filter_snapshot_circle_changeset(
    conn: &Connection,
    changeset: &[u8],
    circle_id: CircleId,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<Vec<u8>, GateError> {
    unsafe {
        let audience = Audience::Circle(circle_id);
        // An installed snapshot is applied onto the state it is the whole of, so
        // no replay holds a row whose identity has yet to be re-materialized.
        validate_inbound_scoped_inserts_raw(
            conn,
            changeset,
            &audience,
            InboundRoutingSource::InstalledSnapshot,
            &BTreeSet::new(),
            gates,
            routing_key,
        )?;
        filter_inbound_audience_rows_raw(conn, changeset, &audience, gates, routing_key)
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
    held_rows: &BTreeSet<(String, String)>,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<InboundStoreChangesets, GateError> {
    let store_transitions = store_audience_transitions(changeset)?;
    unsafe {
        validate_inbound_scoped_inserts_raw(
            conn,
            changeset,
            &Audience::Store,
            InboundRoutingSource::Published(&store_transitions),
            held_rows,
            gates,
            routing_key,
        )?;
        let mirror = Changegroup::new()?;
        mirror.set_schema(conn.handle())?;
        let rows = Changegroup::new()?;
        rows.set_schema(conn.handle())?;
        for_each_change(changeset, |iter, row| {
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
        if !gates.table_is_scoped(&row.table) && !allow_unscoped {
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
        if !gates.table_is_scoped(&row.table) {
            group.add_change(iter)?;
            return Ok(());
        }
        let row_id = row
            .pk()
            .ok_or_else(|| GateError::MissingChangesetPrimaryKey(row.table.clone()))?;
        let routing_id = row_routing_id(routing_key, &row.table, row_id).to_string();
        let winning_audience = winning_store_audience(conn, &routing_id)?;
        if winning_audience.as_ref() == Some(package_audience) {
            group.add_change(iter)?;
        }
        Ok(())
    })?;
    group.output()
}

/// Refuse an inbound package whose scoped row INSERTs do not stand for rows
/// this device can place. An INSERT never establishes its own identity: either
/// the Store package it travels with carries the audience transition that
/// places the row, or the row already has an identity here — a Store audience
/// mirror, the live row itself, or a private row a replay is holding and has
/// yet to re-materialize.
///
/// # Safety
/// `changeset` iteration reads raw session bytes.
unsafe fn validate_inbound_scoped_inserts_raw(
    conn: &Connection,
    changeset: &[u8],
    package_audience: &Audience,
    routing_source: InboundRoutingSource<'_>,
    held_rows: &BTreeSet<(String, String)>,
    gates: &Gates,
    routing_key: &RowRoutingKey,
) -> Result<(), GateError> {
    for_each_change(changeset, |_iter, row| {
        if row.table == "_coven_audience" {
            return Ok(());
        }
        if !gates.is_synced_table(&row.table) {
            return Err(GateError::InvalidInboundAudiencePackage(format!(
                "package names undeclared table {}",
                row.table
            )));
        }
        if !gates.table_is_scoped(&row.table) || row.op != ffi::SQLITE_INSERT {
            return Ok(());
        }
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
        let routing_id = row_routing_id(routing_key, &row.table, row_id).to_string();
        if let InboundRoutingSource::Published(store_transitions) = &routing_source {
            if let Some((transition_audience, _)) = store_transitions.by_routing_id.get(&routing_id)
            {
                if transition_audience != package_audience {
                    return Err(GateError::InvalidInboundAudiencePackage(format!(
                        "scoped row INSERT {}.{row_id} is packaged for a different audience than \
                         its Store transition",
                        row.table
                    )));
                }
                return Ok(());
            }
        }
        if winning_store_audience(conn, &routing_id)?.is_some()
            || live_row_present(conn, &row.table, row_id)?
            || held_rows.contains(&(row.table.clone(), row_id.to_string()))
        {
            return Ok(());
        }
        Err(GateError::InvalidInboundAudiencePackage(format!(
            "scoped row INSERT {}.{row_id} has no Store audience transition and no prior identity",
            row.table
        )))
    })
}

fn live_row_present(conn: &Connection, table: &str, row_id: &str) -> Result<bool, GateError> {
    let sql = format!(
        "SELECT 1 FROM {} WHERE {} = ?1",
        quote_ident(table),
        quote_ident("id"),
    );
    Ok(query_row_optional(conn, &sql, [row_id], |_| Ok(()))?.is_some())
}

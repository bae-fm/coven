//! Carry the counted projection through the same schema identities as app rows.
use crate::{
    migration_names::MigrationMatch, sqlite::DatabaseConnection, write_encoding::decoded,
    write_schema::WriteSchema, DbError,
};
use coven_format::{
    loss::{Loss, LossValues},
    merge_fields,
};
use coven_merge::{ColumnValue, RowId};
use rusqlite::params;

fn column(
    names: &MigrationMatch,
    schema: &WriteSchema,
    table: &str,
    name: String,
    mut value: ColumnValue<coven_format::value::Value>,
) -> Option<(String, ColumnValue<coven_format::value::Value>)> {
    names.tables.get(table)?;
    let name = names.columns.get(&(table.to_owned(), name))?.clone();
    value.parents = names.carried_parents(schema, table, value.parents);
    Some((name, value))
}

/// Carry the kept records through the run's identities. `removed` holds the
/// removal losses of the kept rows the counted projection took out under the
/// schema the run started with (`published::removal_losses`): a table the run
/// drops keeps those losses frozen, as it keeps its rows' own (§18.7).
/// Returns the losses this freezes: the migration over the counted prefix,
/// which the migration write's own freezes replace once they are put.
pub(crate) fn carry(
    db: &DatabaseConnection,
    names: &MigrationMatch,
    schema: &WriteSchema,
    removed: &[Loss],
) -> Result<Vec<Loss>, DbError> {
    let mut frozen = Vec::new();
    if !crate::published::exists(db)? {
        return Ok(frozen);
    }
    // The kept rows and losses are put again under their new names, and
    // name their writes again; a dropped table's row names none.
    crate::forget_entries::unnamed_by_published(db, None)?;
    db.batch("DELETE FROM _coven_published_writes")?;
    // Staging makes table-name swaps independent of update order.
    db.batch("CREATE TEMP TABLE _coven_carried_rows AS SELECT * FROM _coven_published_rows; DELETE FROM _coven_published_rows")?;
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    db.for_each("SELECT record FROM temp._coven_carried_rows", [], |r| {
        let merged = merge_fields::decode_merge_row(&r.get::<_, Vec<u8>>(0)?, &oracle)
            .map_err(crate::write_metadata::MetadataFailure::stored)?;
        let old = merged.state.row().clone();
        let Some(table) = names.tables.get(&old.table) else {
            // Its table goes, and the app's row with it, but this device's
            // queue still reached it: the counted state keeps only its kept
            // losses, never the ones the app's row leaves frozen (§18.7).
            db.internal_execute(
                "INSERT INTO _coven_published_dropped(table_name,key,audience) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                params![old.table, old.key, &old.audience],
            )?;
            return Ok::<_, DbError>(());
        };
        let row = RowId { table: table.clone(), ..old.clone() };
        let state = merged
            .state
            .map_schema(row.clone(), |name, value| column(names, schema, &old.table, name, value));
        crate::published::put_row(db, state)
    })?;
    db.batch("DROP TABLE temp._coven_carried_rows; CREATE TEMP TABLE _coven_carried_creations AS SELECT * FROM _coven_published_creations; CREATE INDEX temp._coven_carried_creations_table ON _coven_carried_creations(table_name); DELETE FROM _coven_published_creations")?;
    for (old, new) in &names.tables {
        db.internal_execute("INSERT INTO _coven_published_creations SELECT ?2,key,audience,write_id,creator FROM temp._coven_carried_creations WHERE table_name=?1",params![old,new])?;
    }
    db.batch("DROP TABLE temp._coven_carried_creations; CREATE TEMP TABLE _coven_carried_losses AS SELECT record FROM _coven_published_losses; DELETE FROM _coven_published_losses")?;
    db.for_each("SELECT record FROM temp._coven_carried_losses", [], |r| {
        let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
        let was_frozen = loss.frozen;
        let loss = carry_loss(names, schema, loss)?;
        if loss.frozen && !was_frozen {
            frozen.push(loss.clone());
        }
        crate::published::put_loss(db, &loss)
    })?;
    db.batch("DROP TABLE temp._coven_carried_losses")?;
    for loss in removed.iter().filter(|loss| !names.tables.contains_key(&loss.row.table)) {
        let loss = loss.clone().freeze();
        crate::published::put_loss(db, &loss)?;
        frozen.push(loss);
    }
    Ok(frozen)
}

/// A kept loss under the run's names. The kept losses are frozen ones and
/// the cells concurrent writes displaced (`published::capture_rows`): a frozen
/// loss keeps its old names, and a cell loss whose table or column goes is
/// frozen under them, as the live losses are (§18.7).
fn carry_loss(
    names: &MigrationMatch,
    schema: &WriteSchema,
    mut loss: Loss,
) -> Result<Loss, DbError> {
    if loss.frozen {
        return Ok(loss);
    }
    let old = loss.row.table.clone();
    let LossValues::Cell { column: name, mut cell } = loss.values else {
        return Err(DbError::Invariant { what: "a kept active loss is a displaced cell" });
    };
    if !names.columns.contains_key(&(old.clone(), name.clone())) {
        loss.values = LossValues::Cell { column: name, cell };
        return Ok(loss.freeze());
    }
    loss.row.table = names.tables[&old].clone();
    let (name, value) = column(names, schema, &old, name, cell.value).expect("surviving column");
    cell.value = value;
    loss.values = LossValues::Cell { column: name, cell };
    Ok(loss)
}

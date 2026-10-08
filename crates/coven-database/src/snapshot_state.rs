//! Replace one audience's records inside the loading transaction.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::merge_store::StoredRow;
use crate::snapshot_error::invalid;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    merge_fields,
    snapshot_rows::{MergeRow, SyncedRow},
};
use coven_merge::{Audience, RowId, RowState, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn begin(
    database: &DatabaseConnection,
    audience: &Audience,
) -> Result<BTreeSet<RowId>, DbError> {
    let audience = audience_text(audience);
    let touched = database
        .query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1",
            [&audience],
            crate::row_queries::read_identity,
        )?
        .into_iter()
        .collect();
    for table in ["_coven_references", "_coven_cells", "_coven_claims"] {
        database.internal_execute(
            &format!(
                "DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE audience=?1)"
            ),
            [&audience],
        )?;
    }
    for table in [
        "_coven_rows",
        "_coven_lost",
        "_coven_fingerprint_leaves",
        "_coven_fingerprint_sums",
    ] {
        database.internal_execute(
            &format!("DELETE FROM {table} WHERE audience=?1"),
            [&audience],
        )?;
    }

    Ok(touched)
}

pub(crate) fn create_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE _coven_snapshot_values(audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,columns BLOB NOT NULL,matched INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(audience,table_name,key)) WITHOUT ROWID;
        CREATE TEMP TABLE _coven_snapshot_writes(device BLOB NOT NULL,number BLOB NOT NULL,timestamp BLOB NOT NULL,had_read BLOB NOT NULL,PRIMARY KEY(device,number)) WITHOUT ROWID;
        CREATE TEMP TABLE _coven_snapshot_columns(table_name TEXT NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(table_name,column_name)) WITHOUT ROWID;")?;
    Ok(())
}

pub(crate) fn synced(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    mut row: SyncedRow,
    additions: bool,
) -> Result<(), DbError> {
    let table = schema.row_table(&row.row)?;
    let values = row
        .columns
        .iter()
        .map(|(name, value)| (name.clone(), value.value.clone()))
        .collect();
    for (name, value) in &row.columns {
        schema.row_column(table, name, value)?;
    }
    let evaluated = crate::removal_sql::evaluate_values(database, table, &values)?;
    if values
        .iter()
        .any(|(name, value)| evaluated.get(name) != Some(value))
        || (!additions && values.len() != evaluated.len())
        || crate::write_rows::row_key(table, &evaluated)? != row.row.key
    {
        return Err(invalid("synced values disagree with their key or schema"));
    }
    for (name, value) in evaluated {
        row.columns.entry(name).or_insert(coven_merge::ColumnValue {
            value,
            parents: Default::default(),
        });
    }
    database.internal_execute(
        "INSERT INTO temp._coven_snapshot_values(table_name,key,columns,audience) VALUES(?1,?2,?3,?4)",
        params![
            row.row.table,
            row.row.key,
            encoded(merge_fields::encode_columns(&row.columns))?,
            audience_text(&row.row.audience)
        ],
    )?;
    Ok(())
}

pub(crate) fn merged(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    merged: MergeRow,
) -> Result<RowId, DbError> {
    let state = &merged.state;
    let row = state.row();
    let table = schema.row_columns(
        row,
        state.present(),
        state.cells().iter().map(|(name, cell)| (name, &cell.value)),
    )?;
    if state.generations().is_empty() {
        return Err(invalid("merge row has no generations"));
    }
    for name in state.cells().keys() {
        let declared: bool=database.query_row("SELECT EXISTS(SELECT 1 FROM temp._coven_snapshot_columns WHERE table_name=?1 AND column_name=?2)",params![row.table,name],|r| r.get(0))?;
        if !declared {
            return Err(invalid("merge cell has no column record"));
        }
    }
    let synced = database
        .query(
            "SELECT columns FROM temp._coven_snapshot_values WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
            |r| decoded(merge_fields::decode_columns(&r.get::<_, Vec<u8>>(0)?)),
        )?
        .into_iter()
        .next();
    if let Some(synced) = synced {
        if !state.present() {
            return Err(invalid("deleted merge row also has a synced row"));
        }
        if state
            .cells()
            .iter()
            .any(|(name, cell)| synced.get(name) != Some(&cell.value))
        {
            return Err(invalid("synced and merge values disagree"));
        }
        let raw = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
            .collect();
        let evaluated = crate::removal_sql::evaluate_values(database, table, &raw)?;
        if synced
            .iter()
            .any(|(name, value)| evaluated.get(name) != Some(&value.value))
        {
            return Err(invalid("synced values disagree with merge defaults"));
        }
        database.internal_execute(
            "UPDATE temp._coven_snapshot_values SET matched=1 WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
        )?;
    } else if state.present() {
        let columns = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.clone()))
            .collect();
        database.internal_execute("INSERT INTO temp._coven_snapshot_values(table_name,key,columns,audience,matched) VALUES(?1,?2,?3,?4,2)",params![row.table,row.key,encoded(merge_fields::encode_columns(&columns))?,audience_text(&row.audience)])?;
    }
    let row = row.clone();
    let update = RowUpdate {
        state: merged.state,
        lost_changes: Vec::new(),
    };
    crate::write_commit::persist(
        database,
        &BTreeMap::from([(row.clone(), update)]),
        |row| {
            Ok(StoredRow {
                state: RowState::new(row.clone()),
                ordinal: None,
                loss: None,
                lost_ids: BTreeMap::new(),
            })
        },
        |write| ordinal(database, write),
    )?;
    Ok(row)
}

/// Validate live losses against the merged row before storing the common record.
pub(crate) fn loss(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    loss: coven_format::loss::Loss,
    metadata: &impl coven_merge::WriteOracle,
    cached: &mut Option<RowState<coven_format::value::Value>>,
) -> Result<(), DbError> {
    use coven_format::loss::{LossCause, LossValues};
    if !loss.retired {
        // Loss records are ordered by row; keep only this row's immutable state.
        if cached.as_ref().is_none_or(|state| state.row() != &loss.row) {
            let audiences = BTreeSet::from([loss.row.audience.clone()]);
            let store =
                crate::merge_store::MergeStore::from_snapshot(database, &schema.schema, &audiences);
            *cached = Some(store.row_without_losses(&loss.row)?.state);
        }
        let state = cached.as_ref().expect("loaded the loss's row");
        match (&loss.values, &loss.cause) {
            (LossValues::Cell { column, cell }, LossCause::Write(write)) => {
                schema.row_column(schema.row_table(&loss.row)?, column, &cell.value)?;
                let declared: bool = database.query_row("SELECT EXISTS(SELECT 1 FROM temp._coven_snapshot_columns WHERE table_name=?1 AND column_name=?2)",params![loss.row.table,column],|r| r.get(0))?;
                if !declared {
                    return Err(invalid("lost cell has no column record"));
                }
                state
                    .validate_loss(
                        &coven_merge::LostKey {
                            column: column.clone(),
                            write: cell.write,
                        },
                        &coven_merge::LostValue {
                            incarnation: loss.generation,
                            value: cell.value.clone(),
                            replaced_by: *write,
                        },
                        metadata,
                    )
                    .map_err(|error| {
                        crate::SnapshotError::Format(coven_format::Error::Merge(error))
                    })?;
            }
            (LossValues::Row(cells), LossCause::Rules(_))
                if state.present()
                    && state.generation() == loss.generation
                    && state.cells() == cells => {}
            _ => return Err(invalid("active loss disagrees with merge state")),
        }
    }
    crate::loss_record::put(database, &loss, None)
}

pub(crate) fn ordinal(database: &DatabaseConnection, id: WriteId) -> Result<i64, DbError> {
    database.query_row(
        &format!("SELECT id FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2"),
        params![
            id.device.0.to_be_bytes().as_slice(),
            id.number.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )
}

pub(crate) fn finish(database: &DatabaseConnection) -> Result<(), DbError> {
    let unmatched: bool = database.query_row(
        "SELECT EXISTS(SELECT 1 FROM temp._coven_snapshot_values v WHERE matched=0
         OR (matched=2) != EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=v.table_name AND l.key=v.key AND l.audience=v.audience AND l.retired=0 AND l.replacement_kind='rules'))",
        [],
        |r| r.get(0),
    )?;
    if unmatched {
        return Err(invalid("synced row has no merge record"));
    }
    Ok(())
}

pub(crate) fn values(
    database: &DatabaseConnection,
    row: &RowId,
) -> Result<crate::write_rows::AppValues, DbError> {
    database.query_row(
        "SELECT columns FROM temp._coven_snapshot_values WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, audience_text(&row.audience)],
        |r| {
            Ok(
                decoded(merge_fields::decode_columns(&r.get::<_, Vec<u8>>(0)?))?
                    .into_iter()
                    .map(|(name, value)| (name, value.value))
                    .collect(),
            )
        },
    )
}

pub(crate) fn drop_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("DROP TABLE temp._coven_snapshot_values; DROP TABLE temp._coven_snapshot_writes; DROP TABLE temp._coven_snapshot_columns")
}

#[cfg(test)]
#[path = "snapshot_state_tests.rs"]
mod tests;

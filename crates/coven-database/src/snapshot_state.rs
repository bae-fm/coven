//! Replace one audience's records inside the loading transaction.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::snapshot_error::invalid;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::audience_text;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_merge::{Audience, RowState, WriteId};
use rusqlite::params;
use std::collections::BTreeSet;

/// Delete the audience's merge records before its snapshot's are written,
/// keeping in `_coven_install_before` which of its rows there were and
/// which the app showed: at an odd generation, with no rules loss; and in
/// `_coven_install_lost` the keys whose lost values it deletes, whose
/// protection the install brings up to date once its rows are in.
pub(crate) fn begin(database: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    let audience = audience_text(audience);
    database.internal_execute(
        "INSERT INTO temp._coven_install_before(table_name,key,audience,shown)
         SELECT r.table_name,r.key,r.audience,
           instr('13579BDF',substr(hex(max(r.generation)),16,1))>0
           AND NOT EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=r.table_name AND l.key=r.key AND l.audience=r.audience AND l.frozen=0 AND l.replacement_kind='rules')
         FROM _coven_rows r WHERE r.audience=?1 GROUP BY r.table_name,r.key,r.audience",
        [&audience],
    )?;
    database.internal_execute(
        "INSERT INTO temp._coven_install_lost(table_name,key)
         SELECT DISTINCT table_name,key FROM _coven_lost WHERE audience=?1 ON CONFLICT DO NOTHING",
        [&audience],
    )?;
    for table in ["_coven_references", "_coven_cells", "_coven_claims"] {
        database.internal_execute(
            &format!(
                "DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE audience=?1)"
            ),
            [&audience],
        )?;
    }
    for table in ["_coven_rows", "_coven_lost"] {
        database.internal_execute(
            &format!("DELETE FROM {table} WHERE audience=?1"),
            [&audience],
        )?;
    }
    Ok(())
}

pub(crate) fn create_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE _coven_snapshot_columns(table_name TEXT NOT NULL,column_name TEXT NOT NULL,PRIMARY KEY(table_name,column_name)) WITHOUT ROWID;")?;
    crate::snapshot_metadata::SnapshotMetadata::create_table(database)
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
    if !loss.frozen {
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
                schema.row_column(
                    schema.row_table(&loss.row)?,
                    &loss.row.audience,
                    column,
                    &cell.value,
                )?;
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
    crate::loss_record::put(database, &loss, None)?;
    crate::file_protection::retain_loss(database, schema, &loss)
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

pub(crate) fn drop_tables(database: &DatabaseConnection) -> Result<(), DbError> {
    database
        .batch("DROP TABLE temp._coven_snapshot_writes; DROP TABLE temp._coven_snapshot_columns")
}

#[cfg(test)]
#[path = "snapshot_state_tests.rs"]
mod tests;

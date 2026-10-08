//! Audience snapshots are streamed from one pinned reader transaction.

use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, decoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use coven_format::snapshot::{SnapshotEncoder, SnapshotHeader, SnapshotRecord};
use coven_format::snapshot_rows::{AppliedWrite, MergeRow, SyncedColumn, SyncedRow};
use coven_format::store_log::SnapshotId;
use coven_format::value::{EntryId, EntryPositions};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, ColumnValue, WriteId};

const COLUMNS: &str = "SELECT c.table_name,c.column_name FROM _coven_columns c
    WHERE EXISTS (SELECT 1 FROM _coven_cells v JOIN _coven_rows r ON r.id=v.row_id
                  WHERE v.column_id=c.id AND r.audience=?1)
       OR EXISTS (SELECT 1 FROM _coven_lost l WHERE l.column_id=c.id AND l.audience=?1)";

/// A snapshot failed to read, encode or reach its plaintext consumer.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotWriteError<E> {
    /// Reading the committed database state failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// A snapshot record could not be represented by the format.
    #[error(transparent)]
    Format(#[from] coven_format::Error),
    /// The consumer refused a plaintext frame.
    #[error("snapshot output: {0}")]
    Output(E),
}

pub(crate) fn write<E>(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    id: SnapshotId,
    begin: impl FnOnce(&SnapshotHeader) -> Result<(), E>,
    mut emit: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<(), SnapshotWriteError<E>> {
    let audience = audience_text(&id.audience);
    let selected = id.audience.clone();
    let mut counts = [0; 5];
    visit_rows(database, schema, &id.audience, |row| {
        counts[3] += 1;
        if row.state.present() && row.loss.is_none() {
            counts[0] += 1;
        }
        Ok::<_, SnapshotWriteError<E>>(())
    })?;
    let writes = crate::download::positions(database)?;
    database.for_each(
        "SELECT substr(timestamp,9,8),number FROM _coven_writes",
        [],
        |r| {
            let id = WriteId {
                device: DeviceId(counter(r.get(0).map_err(DbError::from)?)),
                number: counter(r.get(1).map_err(DbError::from)?),
            };
            if writes.covers(id) {
                counts[1] += 1;
            }
            Ok::<_, SnapshotWriteError<E>>(())
        },
    )?;
    counts[2] = database.query_row(
        &format!("SELECT count(*) FROM ({COLUMNS})"),
        [&audience],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )?;
    counts[4] = database.query_row(
        "SELECT count(*) FROM _coven_lost WHERE audience=?1",
        [&audience],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )?;
    let header = SnapshotHeader {
        id,
        schema_version: database.schema_version()?,
        writes: writes.clone(),
        store_log: EntryPositions(database.query(
            "SELECT device,max(number) FROM _coven_store_log GROUP BY device ORDER BY device",
            [],
            |r| {
                Ok(EntryId {
                    device: DeviceId(counter(r.get(0)?)),
                    number: counter(r.get(1)?),
                })
            },
        )?),
        counts,
    };
    let (mut encoder, header_frame) = SnapshotEncoder::start(header)?;
    begin(encoder.header()).map_err(SnapshotWriteError::Output)?;
    emit(header_frame).map_err(SnapshotWriteError::Output)?;
    let mut record = |record| emit(encoder.record(record)?).map_err(SnapshotWriteError::Output);
    visit_rows(database, schema, &selected, |stored| {
        if stored.state.present() && stored.loss.is_none() {
            let id = stored.state.row();
            let values =
                crate::write_rows::read_values(database, schema.table(&id.table), &id.key)?
                    .ok_or(DbError::DamagedDatabase)?;
            let columns = values
                .into_iter()
                .map(|(name, value)| {
                    let written = stored
                        .state
                        .cells()
                        .get(&name)
                        .map(|cell| cell.value.clone())
                        .unwrap_or(ColumnValue {
                            value,
                            parents: Default::default(),
                        });
                    (name, written)
                })
                .collect();
            record(SnapshotRecord::Synced(SyncedRow {
                row: id.clone(),
                columns,
            }))?;
        }
        Ok::<_, SnapshotWriteError<E>>(())
    })?;
    database.for_each(
        "SELECT timestamp,number,had_read FROM _coven_writes ORDER BY substr(timestamp,9,8),number",
        [],
        |r| {
            let write = read_write(r).map_err(DbError::from)?;
            if writes.covers(write.id) {
                record(SnapshotRecord::Write(write))?;
            }
            Ok::<_, SnapshotWriteError<E>>(())
        },
    )?;
    database.for_each(
        &format!("{COLUMNS} ORDER BY c.table_name,c.column_name"),
        [&audience],
        |r| {
            let column = (|| {
                Ok::<_, rusqlite::Error>(SyncedColumn {
                    table: r.get(0)?,
                    column: r.get(1)?,
                })
            })()
            .map_err(DbError::from)?;
            record(SnapshotRecord::Column(column))
        },
    )?;
    visit_rows(database, schema, &selected, |stored| {
        record(SnapshotRecord::Merge(MergeRow {
            state: stored.state,
        }))
    })?;
    crate::loss_record::visit(database, &selected, |loss| {
        record(SnapshotRecord::Loss(loss))
    })?;
    emit(encoder.finish()?).map_err(SnapshotWriteError::Output)
}

fn visit_rows<E: From<DbError>>(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
    mut visit: impl FnMut(crate::merge_store::StoredRow) -> Result<(), E>,
) -> Result<(), E> {
    database.for_each(
        "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1 ORDER BY table_name,key",
        [audience_text(audience)],
        |r| {
            let row = crate::row_queries::read_identity(r).map_err(DbError::from)?;
            // Each row's causal metadata and values die before reading the next.
            let store = MergeStore::from_schema(database, &schema.schema);
            visit(store.row_without_losses(&row)?)
        },
    )
}

fn read_write(r: &rusqlite::Row<'_>) -> rusqlite::Result<AppliedWrite> {
    let timestamp = decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(0)?))?;
    Ok(AppliedWrite {
        id: WriteId {
            device: timestamp.device(),
            number: counter(r.get(1)?),
        },
        timestamp,
        had_read: decoded(merge_fields::decode_write_positions(
            &r.get::<_, Vec<u8>>(2)?,
        ))?,
    })
}

#[cfg(test)]
#[path = "snapshot_write_tests.rs"]
pub(crate) mod tests;

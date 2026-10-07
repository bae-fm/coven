//! Audience snapshots are streamed from one pinned reader transaction.

use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, decoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use coven_format::snapshot::{SnapshotEncoder, SnapshotHeader, SnapshotRecord};
use coven_format::snapshot_rows::{
    AppliedWrite, LostWrite, LostWriteRow, MergeRow, SyncedColumn, SyncedRow,
};
use coven_format::store_log::SnapshotId;
use coven_format::value::{EntryId, EntryPositions};
use coven_format::write::RowChange;
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, ColumnValue, WriteId};

const COLUMNS: &str = "SELECT c.table_name,c.column_name FROM coven_columns c
    WHERE EXISTS (SELECT 1 FROM coven_cells v JOIN coven_rows r ON r.id=v.row_id
                  WHERE v.column_id=c.id AND r.audience=?1)
       OR EXISTS (SELECT 1 FROM coven_lost l WHERE l.column_id=c.id AND l.audience=?1)";

/// A snapshot failed to read, encode or reach its plaintext consumer.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotWriteError<E> {
    /// Applied parts extend beyond the positions this snapshot could record.
    #[error("snapshot positions do not cover applied writes {writes:?}")]
    IncompletePositions {
        /// Writes whose effects would be omitted from the snapshot's positions.
        writes: Vec<WriteId>,
    },
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
    mut emit: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<(), SnapshotWriteError<E>> {
    let audience = audience_text(&id.audience);
    let selected = id.audience.clone();
    let mut counts = [0; 6];
    visit_rows(database, schema, &id.audience, |row| {
        counts[3] += 1;
        if row.state.present() && row.loss.is_none() {
            counts[0] += 1;
        }
        Ok::<_, SnapshotWriteError<E>>(())
    })?;
    let writes = crate::snapshot_coverage::frontier(database, &selected)?;
    // A prefix cannot describe isolated waiting writes replayed beyond it.
    // Refuse before emitting anything until downloads fill those gaps.
    let pending: Vec<_> = database.query(
        "SELECT device,max(number) FROM coven_snapshot_parts WHERE audience=?1 GROUP BY device ORDER BY device",
        [&audience],
        |r| Ok(WriteId { device: DeviceId(counter(r.get(0)?)), number: counter(r.get(1)?) }),
    )?.into_iter().filter(|write| !writes.covers(*write)).collect();
    if !pending.is_empty() {
        return Err(SnapshotWriteError::IncompletePositions { writes: pending });
    }
    database.for_each(
        "SELECT substr(timestamp,9,8),number FROM coven_writes",
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
        "SELECT count(*) FROM coven_excluded_writes WHERE audience=?1",
        [&audience],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )?;
    counts[5] = crate::snapshot_loss::count(database, &selected)?;
    let header = SnapshotHeader {
        id,
        schema_version: database.schema_version()?,
        writes: writes.clone(),
        store_log: EntryPositions(database.query(
            "SELECT device,max(number) FROM coven_store_log GROUP BY device ORDER BY device",
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
        "SELECT timestamp,number,had_read FROM coven_writes ORDER BY substr(timestamp,9,8),number",
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
        let removed = match stored.loss {
            Some(loss) => database.query_row(
                "SELECT replaced_by FROM coven_lost WHERE id=?1",
                [loss],
                |r| decoded(merge_fields::decode_rules(&r.get::<_, Vec<u8>>(0)?)),
            )?,
            None => Default::default(),
        };
        record(SnapshotRecord::Merge(MergeRow {
            state: stored.state,
            removed,
        }))
    })?;
    database.for_each("SELECT id,header,cause FROM coven_excluded_writes WHERE audience=?1 ORDER BY device,number", [&audience], |r| {
        let (ordinal, frame, cause) = (|| Ok::<_, rusqlite::Error>((
            r.get::<_, i64>(0)?,
            decoded(WriteHeaderFrame::decode(&r.get::<_, Vec<u8>>(1)?))?,
            decoded(merge_fields::decode_lost_write_cause(&r.get::<_, Vec<u8>>(2)?))?,
        )))().map_err(DbError::from)?;
        let part = &frame.parts[0];
        record(SnapshotRecord::LostWrite(LostWrite { header: frame.header, audience: part.audience.clone(), row_count: part.record_count, cause }))?;
        database.for_each("SELECT record FROM coven_excluded_rows WHERE write_id=?1 ORDER BY table_name,key", [ordinal], |r| {
            let change = (|| decoded(RowChange::decode(&r.get::<_, Vec<u8>>(0)?)))().map_err(DbError::from)?;
            record(SnapshotRecord::LostWriteRow(LostWriteRow { change }))
        })
    })?;
    crate::snapshot_loss::visit(database, &selected, |loss| {
        record(SnapshotRecord::RetainedLoss(loss))
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
        "SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE audience=?1 ORDER BY table_name,key",
        [audience_text(audience)],
        |r| {
            let row = crate::row_queries::read_identity(r).map_err(DbError::from)?;
            // Each row's causal metadata and values die before reading the next.
            let store = MergeStore::from_schema(database, &schema.schema);
            visit(store.row(&row)?)
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

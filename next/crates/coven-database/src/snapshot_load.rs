//! Install streamed plaintext with row replacement and local replay in one commit.

use crate::merge_store::MergeStore;
use crate::removal_view::DatabaseRemovalView;
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_format::snapshot_rows::LostWrite;
use coven_format::snapshot_stream::SnapshotChunkDecoder;
use coven_format::store_log::SnapshotId;
use coven_format::write_stream::{PartHeader, WriteHeaderFrame};
use rusqlite::params;
use std::collections::BTreeMap;
use std::io::Read;

pub(crate) fn load(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    expected: SnapshotId,
    mut input: impl Read,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    database.transaction(|database| {
        let local_version = database.schema_version()?;
        let visible = AppView::after(before, schema);
        let old_store = MergeStore::from_schema(before, &schema.schema);
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        let empty = BTreeMap::new();
        let old = DatabaseRemovalView::new(
            before, &old_store, schema, &visible, &empty, &deleted, None,
        )?;
        let mut touched = crate::snapshot_state::begin(database, &expected.audience)?;
        let metadata = SnapshotMetadata::new(database);
        let mut decoder = SnapshotChunkDecoder::new(expected.audience.clone());
        let mut checked_header = false;
        let mut excluded = None;
        let mut chunk = [0; coven_format::chunks::CHUNK_SIZE];
        loop {
            let length = match input.read(&mut chunk) {
                Ok(length) => length,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(SnapshotError::Read(error).into()),
            };
            if length == 0 {
                break;
            }
            let mut bytes = &chunk[..length];
            loop {
                let record = decoder.next_record(&mut bytes, &metadata);
                metadata.check()?;
                let record = record.map_err(SnapshotError::Format)?;
                if !checked_header {
                    if let Some(header) = decoder.header() {
                        check_header(database, &expected, header)?;
                        checked_header = true;
                    }
                }
                let Some(record) = record else { break };
                match record {
                    SnapshotRecord::Synced(row) => crate::snapshot_state::synced(
                        database,
                        schema,
                        row,
                        decoder.header().expect("record follows header").schema_version < local_version,
                    )?,
                    SnapshotRecord::Write(write) => metadata.put(&write)?,
                    SnapshotRecord::Column(column) => {
                        crate::write_commit::column(database, &column.table, &column.column)?;
                        database.internal_execute(
                            "INSERT INTO temp.coven_snapshot_columns(table_name,column_name) VALUES(?1,?2)",
                            params![column.table, column.column],
                        )?;
                    }
                    SnapshotRecord::Merge(row) => {
                        touched.insert(crate::snapshot_state::merged(database, schema, row)?);
                    }
                    SnapshotRecord::LostWrite(write) => {
                        metadata.check_header(&write.header)?;
                        if let Some(previous) = excluded.take() {
                            finish_excluded(database, previous)?;
                        }
                        database.internal_execute(
                            "INSERT INTO coven_excluded_writes(audience,device,number,header,cause)
                             VALUES(?1,?2,?3,x'',?4)",
                            params![
                                audience_text(&write.audience),
                                write.header.position.device.0.to_be_bytes().as_slice(),
                                write.header.position.number.to_be_bytes().as_slice(),
                                encoded(coven_format::merge_fields::encode_lost_write_cause(&write.cause))?,
                            ],
                        )?;
                        excluded = Some(PendingExcluded { write, bytes: 0 });
                    }
                    SnapshotRecord::LostWriteRow(row) => {
                        let pending = excluded.as_mut()
                            .ok_or_else(|| invalid("excluded row has no header"))?;
                        let bytes = encoded(row.change.encode())?;
                        pending.bytes = pending.bytes.checked_add(bytes.len() as u64)
                            .ok_or_else(|| invalid("excluded write length overflow"))?;
                        database.internal_execute(
                            "INSERT INTO coven_excluded_rows(write_id,table_name,key,record)
                             VALUES((SELECT id FROM coven_excluded_writes
                                     WHERE audience=?1 AND device=?2 AND number=?3),?4,?5,?6)",
                            params![
                                audience_text(&pending.write.audience),
                                pending.write.header.position.device.0.to_be_bytes().as_slice(),
                                pending.write.header.position.number.to_be_bytes().as_slice(),
                                row.change.row.table,
                                row.change.row.key,
                                bytes,
                            ],
                        )?;
                        crate::download::exclude_row(
                            database, &pending.write.header, pending.write.cause, &row.change,
                        )?;
                    }
                    SnapshotRecord::RetainedLoss(loss) => crate::snapshot_state::retained(database, loss)?,
                }
            }
        }
        decoder.finish().map_err(SnapshotError::Format)?;
        let header = decoder.header().ok_or_else(|| invalid("snapshot has no header"))?;
        check_header(database, &expected, header)?;
        if let Some(pending) = excluded {
            finish_excluded(database, pending)?;
        }
        metadata.validate(&header.writes)?;
        crate::snapshot_state::finish(database)?;
        crate::snapshot_coverage::loaded(database, &expected.audience, &header.writes)?;
        let current = MergeStore::from_snapshot(database, &schema.schema, &expected.audience);
        let mut affected = crate::write_apply::WriteApply::new(database, schema, &current, &visible, &visible, &deleted)
            .replace(&old, touched)?;
        crate::snapshot_state::drop_tables(database)?;
        // Queue records and their original numbers stay in place. Only this
        // audience's parts are replayed; other audiences kept their histories.
        database.for_each(
            "SELECT record FROM coven_uploads ORDER BY device,number",
            [],
            |r| {
                let mut record = decoded(coven_format::write_stream::decode_plaintext(
                    &r.get::<_, Vec<u8>>(0)?,
                ))?;
                record.parts.retain(|part| part.audience == expected.audience);
                let write = record.header.position;
                // A queue entry without this audience's part is still a causal
                // predecessor of later waiting writes. Count that no-op only
                // when this audience already covers its own prior reads.
                let replayed = !crate::snapshot_coverage::covers(database, &expected.audience, write)?
                    && (!record.parts.is_empty()
                        || crate::snapshot_coverage::missing_past(database, &expected.audience, &record.header)?.is_empty());
                affected.extend(crate::download::apply_opened(database, schema, record.into(), &deleted)?);
                if replayed {
                    crate::snapshot_coverage::replayed(database, &expected.audience, write)?;
                }
                Ok::<_, DbError>(())
            },
        )?;
        // Intermediate replay states may not name a file restored by a later
        // waiting write. Retention follows the final state of this transaction.
        files.retain_rows(affected, &deleted)?;
        files.before_commit()?;
        Ok(())
    })
}

fn check_header(
    database: &DatabaseConnection,
    expected: &SnapshotId,
    header: &SnapshotHeader,
) -> Result<(), DbError> {
    if header.id != *expected {
        return Err(invalid(
            "snapshot identity differs from the requested object",
        ));
    }
    let local = database.schema_version()?;
    let minimum: u32 = database.query_row(
        "SELECT minimum FROM coven_snapshot_schema WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    if header.schema_version > local || header.schema_version < minimum {
        return Err(SnapshotError::Schema {
            snapshot: header.schema_version,
            database: local,
        }
        .into());
    }
    let mut missing = Vec::new();
    for entry in &header.store_log.0 {
        let number: Option<Vec<u8>> = database.query_row(
            "SELECT max(number) FROM coven_store_log WHERE device=?1",
            [entry.device.0.to_be_bytes().as_slice()],
            |r| r.get(0),
        )?;
        if number.map_or(0, crate::write_encoding::counter) < entry.number {
            missing.push(*entry);
        }
    }
    if !missing.is_empty() {
        return Err(SnapshotError::StoreLog { missing }.into());
    }
    Ok(())
}

struct PendingExcluded {
    write: LostWrite,
    bytes: u64,
}

fn finish_excluded(database: &DatabaseConnection, pending: PendingExcluded) -> Result<(), DbError> {
    let header = WriteHeaderFrame {
        header: pending.write.header,
        parts: vec![PartHeader {
            audience: pending.write.audience.clone(),
            record_count: pending.write.row_count,
            plaintext_length: pending.bytes,
        }],
    };
    database.internal_execute(
        "UPDATE coven_excluded_writes SET header=?1 WHERE audience=?2 AND device=?3 AND number=?4",
        params![
            header.encode().map_err(SnapshotError::Format)?,
            audience_text(&pending.write.audience),
            header.header.position.device.0.to_be_bytes().as_slice(),
            header.header.position.number.to_be_bytes().as_slice()
        ],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_load_tests.rs"]
mod tests;

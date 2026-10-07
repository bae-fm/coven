//! Queue parts resume as their audience's causal history becomes available.

use crate::snapshot_coverage::{covers, missing_past, replayed};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, decoded};
use crate::write_rows::AppKey;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::write::{WriteDisposition, WriteRecord};
use coven_foundation::id_source::CircleId;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn retain_queue(database: &DatabaseConnection) -> Result<(), DbError> {
    // Upload completion can remove the queue entry before its missing reads
    // arrive. Keep the plaintext until the reloaded history has consumed it.
    database.internal_execute(
        "INSERT INTO coven_snapshot_waiting(device,number,record)
         SELECT device,number,record FROM coven_uploads WHERE true
         ON CONFLICT(device,number) DO UPDATE SET record=excluded.record",
        [],
    )?;
    Ok(())
}

pub(crate) fn apply(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeSet<CircleId>,
) -> Result<BTreeSet<AppKey>, DbError> {
    let audiences = database.query(
        "SELECT audience FROM coven_snapshot_coverage ORDER BY audience",
        [],
        |r| audience(&r.get::<_, String>(0)?),
    )?;
    if audiences.is_empty() {
        return Ok(BTreeSet::new());
    }
    let boundaries = crate::write_boundary::WriteBoundary::load(database)?;
    let mut affected = BTreeSet::new();
    database.for_each(
        "SELECT record FROM coven_snapshot_waiting ORDER BY device,number",
        [],
        |r| {
            let mut record = read_record(r)?;
            let mut ready = BTreeSet::new();
            for audience in &audiences {
                if covers(database, audience, record.header.position)? {
                    continue;
                }
                let excluded = record.parts.iter().any(|part| {
                    part.audience == *audience
                        && (record.header.disposition != WriteDisposition::Apply
                            || boundaries
                                .iter()
                                .any(|b| b.excludes(&record.header, part).is_some()))
                });
                if excluded || missing_past(database, audience, &record.header)?.is_empty() {
                    ready.insert(audience.clone());
                }
            }
            // An audience with no part still consumes this write as a causal
            // predecessor. Covered parts keep their already committed effects.
            record.parts.retain(|part| ready.contains(&part.audience));
            let write = record.header.position;
            if !ready.is_empty() {
                affected.extend(record.parts.iter().flat_map(|part| {
                    part.rows
                        .iter()
                        .map(|row| (row.row.table.clone(), row.row.key.clone()))
                }));
                affected.extend(crate::download::apply_opened(
                    database,
                    schema,
                    record.clone().into(),
                    deleted,
                )?);
                for audience in ready {
                    if !covers(database, &audience, write)? {
                        replayed(database, &audience, write)?;
                    }
                }
            }
            let mut all_covered = true;
            for audience in &audiences {
                all_covered &= covers(database, audience, write)?;
            }
            if all_covered {
                crate::snapshot_coverage::committed(database, &record)?;
            }
            if crate::download::positions(database)?.covers(write) {
                database.internal_execute(
                    "DELETE FROM coven_snapshot_waiting WHERE device=?1 AND number=?2",
                    (
                        write.device.0.to_be_bytes().as_slice(),
                        write.number.to_be_bytes().as_slice(),
                    ),
                )?;
            }
            Ok::<_, DbError>(())
        },
    )?;
    Ok(affected)
}

pub(crate) fn files(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeSet<CircleId>,
) -> Result<BTreeMap<AppKey, BTreeSet<Vec<u8>>>, DbError> {
    let mut files = BTreeMap::<_, BTreeSet<_>>::new();
    let boundaries = crate::write_boundary::WriteBoundary::load(database)?;
    database.for_each("SELECT record FROM coven_snapshot_waiting", [], |r| {
        let record = read_record(r)?;
        if record.header.disposition != WriteDisposition::Apply {
            return Ok::<_, DbError>(());
        }
        for part in &record.parts {
            if covers(database, &part.audience, record.header.position)?
                || matches!(part.audience, coven_merge::Audience::Circle(id) if deleted.contains(&id))
                || boundaries.iter().any(|b| b.excludes(&record.header, part).is_some())
            {
                continue;
            }
            for row in &part.rows {
                let Some(file) = &schema.declaration(&row.row.table).files else { continue };
                let columns = match &row.change.operation {
                    coven_merge::Operation::Insert(columns) | coven_merge::Operation::Update(columns) => columns,
                    coven_merge::Operation::Delete => continue,
                };
                if !columns.contains_key(&file.hash) { continue; }
                let values = columns.iter().map(|(name, value)| (name.clone(), value.value.clone())).collect();
                if let Some(identity) = crate::file_row::identity(file, &values)? {
                    files.entry((row.row.table.clone(), row.row.key.clone())).or_default().insert(identity);
                }
            }
        }
        Ok(())
    })?;
    Ok(files)
}

fn read_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<WriteRecord> {
    decoded(coven_format::write_stream::decode_plaintext(
        &row.get::<_, Vec<u8>>(0)?,
    ))
}

#[cfg(test)]
#[path = "snapshot_replay_tests.rs"]
mod tests;

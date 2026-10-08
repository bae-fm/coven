//! Apply supplied gap streams and waiting writes in causal order inside a reload.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::snapshot_coverage::SnapshotCoverage;
use crate::snapshot_error::{invalid, SnapshotError};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, decoded};
use crate::write_rows::AppKey;
use crate::write_schema::WriteSchema;
use crate::{DbError, DownloadedWriteStream, WriteWait};
use coven_format::{merge_fields, snapshot_rows::AppliedWrite};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{WriteId, WritePast};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::time::SystemTime;

enum Source<R> {
    Download(DownloadedWriteStream<R>),
    Queue(i64),
    Deleted(AppliedWrite),
}

pub(crate) fn apply<R: Read>(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    now: SystemTime,
    downloads: Vec<DownloadedWriteStream<R>>,
    absent: Vec<WriteId>,
    mut coverage: SnapshotCoverage,
    deleted: &BTreeSet<CircleId>,
) -> Result<BTreeSet<AppKey>, DbError> {
    let mut sources = BTreeMap::new();
    let mut supplied = BTreeSet::new();
    for download in downloads {
        download.header.encode().map_err(SnapshotError::Format)?;
        for (part, input) in download.header.parts.iter().zip(&download.parts) {
            if matches!(input, crate::DownloadedPartStream::Opened(_)) {
                coverage.opened(&part.audience);
            }
        }
        let header = &download.header.header;
        if !supplied.insert(header.position) {
            return Err(invalid("reload repeats a supplied write"));
        }
        coverage.include(header.position);
        for past in &header.had_read.0 {
            coverage.include(*past);
        }
        sources.insert(
            (header.timestamp, header.position, false),
            Source::Download(download),
        );
    }
    let mut queued = BTreeSet::new();
    database.for_each(
        "SELECT u.rowid,u.device,u.number,w.timestamp,w.had_read FROM _coven_uploads u
         LEFT JOIN _coven_writes w ON substr(w.timestamp,9,8)=u.device AND w.number=u.number
         ORDER BY u.device,u.number",
        [],
        |r| {
            let rowid: i64 = r.get(0)?;
            let write = AppliedWrite {
                id: WriteId {
                    device: DeviceId(counter(r.get(1)?)),
                    number: counter(r.get(2)?),
                },
                timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(3)?))?,
                had_read: decoded(merge_fields::decode_write_positions(
                    &r.get::<_, Vec<u8>>(4)?,
                ))?,
            };
            coverage.include(write.id);
            for past in write.had_read.0 {
                coverage.include(past);
            }
            queued.insert(write.id);
            sources.insert((write.timestamp, write.id, true), Source::Queue(rowid));
            Ok::<_, DbError>(())
        },
    )?;
    for id in absent {
        if supplied.contains(&id) || queued.contains(&id) {
            return Err(invalid("absent write also has a supplied stream"));
        }
        if !coverage.covered_by_snapshot(id) {
            return Err(invalid("absent write is not covered by a loaded snapshot"));
        }
        let write = database.query_row(&format!("SELECT timestamp,had_read FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2"),
            (id.device.0.to_be_bytes().as_slice(), id.number.to_be_bytes().as_slice()), |r| Ok(AppliedWrite {
                id, timestamp: decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(0)?))?,
                had_read: decoded(merge_fields::decode_write_positions(&r.get::<_, Vec<u8>>(1)?))?,
            }))?;
        sources.insert((write.timestamp, id, false), Source::Deleted(write));
    }
    coverage.start(database)?;
    let mut affected = BTreeSet::new();
    for source in sources.into_values() {
        let supplied = matches!(source, Source::Download(_));
        let write = match source {
            Source::Download(stream) => stream.read()?,
            Source::Deleted(write) => {
                let positions = crate::download::positions(database)?;
                if !positions.covers(write.id) {
                    let past = write.had_read.causal_past(write.id);
                    let missing: Vec<_> = past
                        .frontier()
                        .copied()
                        .filter(|id| !positions.covers(*id))
                        .collect();
                    if !missing.is_empty() {
                        return Err(SnapshotError::MissingWrites { missing }.into());
                    }
                    database.internal_execute("INSERT INTO _coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number", (write.id.device.0.to_be_bytes().as_slice(), write.id.number.to_be_bytes().as_slice()))?;
                }
                continue;
            }
            Source::Queue(rowid) => database
                .query_row(
                    "SELECT record FROM _coven_uploads WHERE rowid=?1",
                    [rowid],
                    |r| {
                        decoded(coven_format::write_stream::decode_plaintext(
                            &r.get::<_, Vec<u8>>(0)?,
                        ))
                    },
                )?
                .into(),
        };
        if (supplied && queued.contains(&write.header.position))
            || crate::download::positions(database)?.covers(write.header.position)
        {
            // Validate supplied copies, but replay the queue's current plaintext
            // (which an app migration may have converted) from the queue itself.
            crate::write_commit::retain_metadata(
                database,
                &AppliedWrite {
                    id: write.header.position,
                    timestamp: write.header.timestamp,
                    had_read: write.header.had_read,
                },
            )?;
            continue;
        }
        if let Some(wait) = crate::download::prerequisite(database, now, &write.header)? {
            return Err(match wait {
                WriteWait::Writes(missing) => SnapshotError::MissingWrites { missing },
                wait => SnapshotError::WriteWaiting(wait),
            }
            .into());
        }
        let write = coverage.uncovered(write)?;
        affected.extend(crate::download::apply_opened(
            database, schema, write, deleted,
        )?);
    }
    coverage.finish(database)?;
    Ok(affected)
}

#[cfg(test)]
#[path = "snapshot_writes_tests.rs"]
mod tests;

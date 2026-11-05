//! Audience snapshots stream the counted prefix from one pinned transaction.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::write_encoding::{counter, decoded};
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    admin::SnapshotId,
    merge_fields,
    snapshot::{section, SnapshotEncoder, SnapshotHeader, SnapshotRecord, SECTIONS},
    snapshot_rows::{AppliedWrite, SyncedColumn},
};
use coven_merge::{Audience, WriteId};
use std::collections::BTreeSet;

/// A snapshot failed to read, encode or reach its plaintext consumer.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotWriteError<E> {
    /// Reading committed data failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// A record could not be encoded.
    #[error(transparent)]
    Format(#[from] coven_format::Error),
    /// The consumer refused a frame.
    #[error("snapshot output: {0}")]
    Output(E),
}

impl<E: coven_foundation::task::CarriesBug> coven_foundation::task::CarriesBug
    for SnapshotWriteError<E>
{
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Database(error) => error.bug(),
            Self::Format(_) => None,
            Self::Output(error) => error.bug(),
        }
    }
}

/// Returns the snapshot's breaking version, read in the transaction that
/// reads its schema version (§16.3).
pub(crate) fn write<E>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    id: SnapshotId,
    begin: impl FnOnce(&SnapshotHeader) -> Result<(), E>,
    mut emit: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<u32, SnapshotWriteError<E>> {
    let covers = crate::sequence::applied(db)?;
    let audience = id.audience.clone();
    let removed = crate::published::removals(db, &schema.schema, &schema.rules)?;
    let mut counts = [0; SECTIONS];
    let mut columns = BTreeSet::new();
    crate::snapshot_view::rows(db, schema, &removed, &audience, |row, synced| {
        counts[section::MERGE] += 1;
        counts[section::SYNCED] += u64::from(synced.is_some());
        columns.extend(
            row.state.cells().keys().map(|column| (row.state.row().table.clone(), column.clone())),
        );
        Ok::<_, DbError>(())
    })?;
    crate::snapshot_view::losses(db, &removed, &audience, |loss| {
        counts[section::LOSS] += 1;
        if let coven_format::loss::LossValues::Cell { column, .. } = &loss.values {
            columns.insert((loss.row.table.clone(), column.clone()));
        }
        Ok::<_, DbError>(())
    })?;
    counts[section::WRITE] = db.query_row(
        "SELECT count(*) FROM _coven_writes WHERE slot<=?1",
        [covers.to_be_bytes()],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )?;
    counts[section::COLUMN] = columns.len() as u64;
    crate::write_sources::visit_evidence(db, &audience, covers, |_| {
        counts[section::AUTHORED] += 1;
        Ok::<_, DbError>(())
    })?;
    crate::snapshot_view::creations(db, &audience, |_| {
        counts[section::CREATION] += 1;
        Ok::<_, DbError>(())
    })?;
    let evidence = crate::authorship::evidence(
        db,
        schema,
        &removed,
        &audience,
        crate::authorship::Author {
            device: id.device,
            member: crate::store_state_tables::device_member(db, id.device)?,
        },
    )?;
    counts[section::EVIDENCE] = evidence.len() as u64;
    if audience == Audience::Store {
        for (at, kind) in [
            (section::ADMIN, "admin"),
            (section::KEY_COPY, "key"),
            (section::ANNOUNCEMENT, "announcement"),
        ] {
            counts[at] = db.query_row(
                "SELECT count(*) FROM _coven_slot_records WHERE kind=?1 AND slot<=?2",
                rusqlite::params![kind, covers.to_be_bytes()],
                |r| r.get::<_, i64>(0).map(|n| n as u64),
            )?;
        }
    }
    let claims = crate::sequence::claims(db)?;
    counts[section::CLAIM] = claims.len() as u64;
    let breaking = crate::raise_capture::breaking_version(db)?;
    let (mut encoder, header) = SnapshotEncoder::start(SnapshotHeader {
        id,
        schema_version: db.schema_version()?,
        covers,
        counts,
    })?;
    begin(encoder.header()).map_err(SnapshotWriteError::Output)?;
    emit(header).map_err(SnapshotWriteError::Output)?;
    let mut record = |record| emit(encoder.record(record)?).map_err(SnapshotWriteError::Output);
    crate::snapshot_view::rows(db, schema, &removed, &audience, |_, synced| {
        if let Some(row) = synced {
            record(SnapshotRecord::Synced(row))?;
        }
        Ok::<_, SnapshotWriteError<E>>(())
    })?;
    db.for_each(&format!("SELECT timestamp,number,read_through,slot FROM _coven_writes WHERE slot<=?1 ORDER BY {WRITE_DEVICE_SQL},number"),[covers.to_be_bytes()],|r|record(SnapshotRecord::Write(read_write(r).map_err(DbError::from)?)))?;
    for (table, column) in columns {
        record(SnapshotRecord::Column(SyncedColumn { table, column }))?;
    }
    crate::snapshot_view::rows(db, schema, &removed, &audience, |row, _| {
        record(SnapshotRecord::Merge(row))
    })?;
    crate::snapshot_view::losses(db, &removed, &audience, |loss| {
        record(SnapshotRecord::Loss(loss))
    })?;
    crate::write_sources::visit_evidence(db, &audience, covers, |authored| {
        record(SnapshotRecord::Authored(authored))
    })?;
    crate::snapshot_view::creations(db, &audience, |creation| {
        record(SnapshotRecord::Creation(creation))
    })?;
    for evidence in evidence {
        record(SnapshotRecord::Evidence(evidence))?;
    }
    if audience == Audience::Store {
        for kind in ["admin", "key", "announcement"] {
            db.for_each("SELECT slot,record FROM _coven_slot_records WHERE kind=?1 AND slot<=?2 ORDER BY slot",rusqlite::params![kind,covers.to_be_bytes()],|r| {
                let original=coven_format::floor::OriginalSlot {slot:coven_merge::Slot(counter(r.get(0).map_err(DbError::from)?).map_err(DbError::from)?),object:r.get(1).map_err(DbError::from)?};
                record(match kind {"admin"=>SnapshotRecord::Admin(original),"key"=>SnapshotRecord::KeyCopy(original),_=>SnapshotRecord::Announcement(original)})
            })?;
        }
    }
    for claim in claims {
        record(SnapshotRecord::Claim(claim))?;
    }
    emit(encoder.finish()?).map_err(SnapshotWriteError::Output)?;
    Ok(breaking)
}

pub(crate) fn read_write(r: &rusqlite::Row<'_>) -> rusqlite::Result<AppliedWrite> {
    let timestamp = decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(0)?))?;
    Ok(AppliedWrite {
        id: WriteId { device: timestamp.device(), number: counter(r.get(1)?)? },
        timestamp,
        read_through: counter(r.get(2)?)?,
        slot: coven_merge::Slot(counter(r.get(3)?)?),
    })
}

#[cfg(test)]
#[path = "snapshot_write_tests.rs"]
pub(crate) mod tests;

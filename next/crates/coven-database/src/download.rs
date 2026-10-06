//! Authenticated, decoded writes at the sync/database boundary.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, encoded};
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    merge_fields,
    snapshot_rows::LostWriteCause,
    value::WritePositions,
    write::{WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, ColumnValue, Operation, Timestamp, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// A write whose signature and opened parts sync has already authenticated.
#[derive(Clone, Debug)]
pub struct DownloadedWrite {
    /// The authenticated write header.
    pub header: WriteHeader,
    /// Every part, including ones the receiving device could not decrypt.
    pub parts: Vec<DownloadedPart>,
}

/// Opening a part and skipping it both advance the write's applied position.
#[derive(Clone, Debug)]
pub enum DownloadedPart {
    /// Authenticated plaintext changes for an audience.
    Opened(WritePart),
    /// The device does not have this audience's key.
    Skipped(Audience),
}

impl From<WriteRecord> for DownloadedWrite {
    fn from(record: WriteRecord) -> Self {
        Self {
            header: record.header,
            parts: record
                .parts
                .into_iter()
                .map(DownloadedPart::Opened)
                .collect(),
        }
    }
}

/// Applying a previously applied write never runs it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// All opened parts and all merge metadata committed together.
    Applied,
    /// This position had already been consumed, including skipped or lost parts.
    AlreadyApplied,
    /// Nothing applied; sync retains the write until this prerequisite changes.
    Waiting(WriteWait),
}

/// A prerequisite for applying a downloaded write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteWait {
    /// These positions must have applied, including the author's preceding write.
    Writes(Vec<WriteId>),
    /// The app must support this schema version.
    SchemaVersion(u32),
    /// The timestamp is more than five minutes ahead of the receiving clock.
    Clock(Timestamp),
}

pub(crate) fn positions(database: &DatabaseConnection) -> Result<WritePositions, DbError> {
    Ok(WritePositions(database.query(
        "SELECT device,number FROM coven_positions ORDER BY device",
        [],
        |r| {
            Ok(WriteId {
                device: DeviceId(counter(r.get(0)?)),
                number: counter(r.get(1)?),
            })
        },
    )?))
}

pub(crate) fn deleted_circles(
    database: &DatabaseConnection,
) -> Result<BTreeSet<CircleId>, DbError> {
    Ok(database
        .query("SELECT circle FROM coven_deleted_circles", [], |r| {
            match crate::write_encoding::audience(&r.get::<_, String>(0)?)? {
                Audience::Circle(circle) => Ok(circle),
                Audience::Store => Err(rusqlite::Error::InvalidQuery),
            }
        })?
        .into_iter()
        .collect())
}

pub(crate) fn apply(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    now: SystemTime,
    download: DownloadedWrite,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<ApplyOutcome, DbError> {
    database.transaction(|database| {
        let positions = positions(database)?;
        let header = &download.header;
        if positions.covers(header.position) {
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        let mut missing: Vec<_> = header
            .had_read
            .0
            .iter()
            .copied()
            .filter(|id| !positions.covers(*id))
            .collect();
        if header.position.number > 1 {
            let prior = WriteId {
                number: header.position.number - 1,
                ..header.position
            };
            if !positions.covers(prior) {
                missing.push(prior);
            }
        }
        if !missing.is_empty() {
            return Ok(ApplyOutcome::Waiting(WriteWait::Writes(missing)));
        }
        let milliseconds = match now.duration_since(UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
            Err(_) => 0,
        };
        if u128::from(header.timestamp.milliseconds()) > milliseconds + 300_000 {
            return Ok(ApplyOutcome::Waiting(WriteWait::Clock(header.timestamp)));
        }
        if header.schema_version > database.schema_version()? {
            return Ok(ApplyOutcome::Waiting(WriteWait::SchemaVersion(
                header.schema_version,
            )));
        }
        let result = apply_opened(database, schema, download, files)?;
        files.before_commit()?;
        Ok(result)
    })
}

fn apply_opened(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<ApplyOutcome, DbError> {
    let mut record = WriteRecord {
        header: download.header,
        parts: download
            .parts
            .into_iter()
            .filter_map(|part| match part {
                DownloadedPart::Opened(part) => Some(part),
                DownloadedPart::Skipped(_) => None,
            })
            .collect(),
    };
    let boundaries = crate::write_boundary::WriteBoundary::load(database)?;
    let deleted = deleted_circles(database)?;
    let visible = AppView::after(database, schema);
    let store = MergeStore::new(database, &visible);
    let mut kept = Vec::new();
    for part in record.parts {
        let cause = match record.header.disposition {
            WriteDisposition::Migration => continue,
            WriteDisposition::Lost(version) => Some(LostWriteCause::SchemaChange(version)),
            WriteDisposition::Apply => boundaries
                .iter()
                .find_map(|boundary| boundary.excludes(&record.header, &part)),
        };
        if let Some(cause) = cause {
            exclude(database, &record.header, &part, cause)?;
        } else {
            kept.push(part);
        }
    }
    record.parts = kept;
    let affected =
        crate::write_apply::WriteApply::new(database, schema, &store, &visible, &visible, &deleted)
            .apply(Some(&record), BTreeSet::new())?;
    files.retain_rows(affected, &deleted)?;
    Ok(ApplyOutcome::Applied)
}

fn exclude(
    database: &DatabaseConnection,
    header: &WriteHeader,
    part: &WritePart,
    cause: LostWriteCause,
) -> Result<(), DbError> {
    let cause = encoded(merge_fields::encode_lost_write_cause(&cause))?;
    let setter = encoded(merge_fields::encode_write_id(&header.position))?;
    for change in &part.rows {
        let values = match &change.change.operation {
            Operation::Insert(values) | Operation::Update(values) => values.clone(),
            Operation::Delete => change
                .old
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ColumnValue {
                            value: value.clone(),
                            parents: BTreeMap::new(),
                        },
                    )
                })
                .collect(),
        };
        let setters = values
            .keys()
            .map(|name| (name.clone(), header.position))
            .collect();
        let values = encoded(merge_fields::encode_columns(&values))?;
        let setters = encoded(merge_fields::encode_setters(&setters))?;
        let audience = audience_text(&part.audience);
        database.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES(?1,?2,?3,?4,NULL,?5,?6,'excluded',?7)", params![change.row.table,change.row.key,audience,change.change.generation.to_be_bytes().as_slice(),values,setters,cause])?;
        crate::fingerprint::excluded(
            database,
            &change.row,
            &setter,
            &[
                &change.change.generation.to_be_bytes(),
                &values,
                &setters,
                &cause,
            ],
        )?;
    }
    Ok(())
}

/// Agreement state read atomically for sync to post.
pub struct SyncState {
    /// Fingerprints may be compared only at the same app schema version.
    pub schema_version: u32,
    /// Applied positions, including skipped and excluded writes.
    pub positions: WritePositions,
    /// Only audiences for which the caller supplied a key.
    pub fingerprints: Vec<(Audience, coven_crypto::Fingerprint)>,
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;

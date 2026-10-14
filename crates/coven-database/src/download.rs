//! Authenticated, decoded writes at the sync/database boundary.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::counter;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    snapshot_rows::LostWriteCause,
    value::WritePositions,
    write::{WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, ColumnValue, Operation, WriteId, WritePast};
use std::collections::{BTreeMap, BTreeSet};

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
    /// Store-log entries in the write's authoring view have not applied.
    StoreLog(Vec<crate::EntryId>),
    /// These positions must have applied, including the author's preceding write.
    Writes(Vec<WriteId>),
    /// These original row changes are unavailable even if their positions passed.
    Sources(Vec<coven_format::write_sources::ChangeId>),
    /// The app must support this schema version.
    SchemaVersion(u32),
    /// The local breaking migration awaits publication and snapshot reload.
    SchemaPublication(u32),
    /// The selected boundary has not committed its replacement data.
    Reload(Audience),
}

pub(crate) fn positions(database: &DatabaseConnection) -> Result<WritePositions, DbError> {
    Ok(WritePositions(database.query(
        "SELECT device,number FROM _coven_positions ORDER BY device",
        [],
        |r| {
            Ok(WriteId {
                device: DeviceId(counter(r.get(0)?)),
                number: counter(r.get(1)?),
            })
        },
    )?))
}

pub(crate) fn apply<P: crate::PendingValue>(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
    stored_at: std::time::SystemTime,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<ApplyOutcome, DbError> {
    source_wait(database.transaction(|database| {
        for part in &download.parts {
            if let DownloadedPart::Opened(part) = part {
                for dismissal in &part.dismissals {
                    dismissal.validate_past(&download.header)?;
                }
            }
        }
        let positions = positions(database)?;
        let header = &download.header;
        if positions.covers(header.position) {
            crate::write_commit::retain_metadata(
                database,
                &coven_format::snapshot_rows::AppliedWrite {
                    id: header.position,
                    timestamp: header.timestamp,
                    had_read: header.had_read.clone(),
                },
            )?;
            retain_sources(database, &download)?;
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        if let Some(wait) = prerequisite(database, header)? {
            return Ok(ApplyOutcome::Waiting(wait));
        }
        for part in &download.parts {
            if let DownloadedPart::Opened(part) = part {
                if crate::entry_effects::reloading(database, &part.audience)? {
                    return Ok(ApplyOutcome::Waiting(WriteWait::Reload(
                        part.audience.clone(),
                    )));
                }
            }
        }
        crate::pending::clear_log(database, crate::LogObject::Write(header.position))?;
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        crate::write_landing::land::<P>(
            database,
            schema,
            header,
            stored_at,
            &opened_audiences(&download),
            true,
        )?;
        crate::retained_writes::download(database, &download)?;
        let affected = apply_opened(database, schema, download, &deleted)?;
        files.retain_rows(affected, &deleted)?;
        files.before_commit()?;
        Ok(ApplyOutcome::Applied)
    }))
}

/// The audiences whose parts this device opened.
fn opened_audiences(download: &DownloadedWrite) -> Vec<Audience> {
    download
        .parts
        .iter()
        .filter_map(|part| match part {
            DownloadedPart::Opened(part) => Some(part.audience.clone()),
            DownloadedPart::Skipped(_) => None,
        })
        .collect()
}

pub(crate) fn source_wait(result: Result<ApplyOutcome, DbError>) -> Result<ApplyOutcome, DbError> {
    match result {
        Err(DbError::MissingWriteSources(sources)) => {
            Ok(ApplyOutcome::Waiting(WriteWait::Sources(sources)))
        }
        other => other,
    }
}

pub(crate) fn prerequisite(
    database: &DatabaseConnection,
    header: &WriteHeader,
) -> Result<Option<WriteWait>, DbError> {
    let positions = positions(database)?;
    let entries = crate::store_log::positions(database)?;
    let missing_entries: Vec<_> = header
        .store_log_read
        .0
        .iter()
        .copied()
        .filter(|id| !entries.covers(*id))
        .collect();
    if !missing_entries.is_empty() {
        return Ok(Some(WriteWait::StoreLog(missing_entries)));
    }
    let past = header.had_read.causal_past(header.position);
    let missing: Vec<_> = past
        .frontier()
        .copied()
        .filter(|id| !positions.covers(*id))
        .collect();
    if !missing.is_empty() {
        return Ok(Some(WriteWait::Writes(missing)));
    }
    crate::download_checks::past(database, header)?;
    if header.schema_version > database.schema_version()? {
        return Ok(Some(WriteWait::SchemaVersion(header.schema_version)));
    }
    let publication: Option<u32> = database.query_row(
        "SELECT (SELECT publication FROM _coven_snapshot_schema WHERE EXISTS(SELECT 1 FROM _coven_members))",
        [], |r| r.get(0),
    )?;
    if let Some(publication) = publication.filter(|version| *version > 0) {
        let applied = crate::write_boundary::WriteBoundary::load(database)?
            .into_iter()
            .filter_map(|b| match b.boundary {
                crate::Boundary::SchemaChange(version)
                    if version.snapshot.audience == Audience::Store =>
                {
                    Some(version.number)
                }
                _ => None,
            })
            .max()
            .unwrap_or(0);
        if publication > applied {
            return Ok(Some(WriteWait::SchemaPublication(publication)));
        }
    }
    Ok(None)
}

pub(crate) fn apply_opened(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
    deleted: &BTreeMap<CircleId, crate::EntryId>,
) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
    let record = accepted(database, schema, download)?;
    apply_interpreted(database, schema, &record, deleted)
}

/// Merge a locally derived interpretation whose original evidence is retained.
pub(crate) fn apply_interpreted(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    record: &WriteRecord,
    deleted: &BTreeMap<CircleId, crate::EntryId>,
) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
    let visible = AppView::after(database, schema);
    let store = MergeStore::new(database, &visible);
    crate::write_apply::WriteApply::new(database, schema, &store, &visible, &visible, deleted)
        .apply(Some(record), BTreeSet::new())
}

/// Authenticated covered writes may supply missing originals without repeating
/// any app effect. This also checks originals supplied alongside snapshot gaps.
pub(crate) fn retain_sources(
    database: &DatabaseConnection,
    download: &DownloadedWrite,
) -> Result<(), DbError> {
    retain_source_rows(database, download)?;
    for part in &download.parts {
        if let DownloadedPart::Opened(part) = part {
            crate::write_sources::complete(database, &download.header, &part.audience)?;
        }
    }
    Ok(())
}

/// A streamed fragment cannot establish absence of other original rows.
pub(crate) fn retain_source_rows(
    database: &DatabaseConnection,
    download: &DownloadedWrite,
) -> Result<(), DbError> {
    for part in &download.parts {
        if let DownloadedPart::Opened(part) = part {
            for row in &part.rows {
                crate::write_sources::validate(database, &download.header, row)?;
                crate::write_sources::retain(database, &download.header, row)?;
            }
        }
    }
    Ok(())
}

fn source_cause(
    boundaries: &[crate::write_boundary::WriteBoundary],
    header: &WriteHeader,
    part: &WritePart,
) -> Option<LostWriteCause> {
    match header.disposition {
        WriteDisposition::Migration => None,
        WriteDisposition::Lost(version) => Some(LostWriteCause::SchemaChange(version)),
        WriteDisposition::Apply => boundaries
            .iter()
            .find_map(|boundary| boundary.excludes(header, part)),
    }
}

pub(crate) fn accepted(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
) -> Result<WriteRecord, DbError> {
    retain_sources(database, &download)?;
    interpret(database, schema, download)
}

pub(crate) fn interpret(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
) -> Result<WriteRecord, DbError> {
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
    // A write outside its own retirement window is consumed without effects
    // or losses; later readers never rescue it (§10).
    if crate::write_landing::late(database, record.header.position)? {
        record.parts.clear();
        return Ok(record);
    }
    let boundaries = crate::write_boundary::WriteBoundary::load(database)?;
    let mut kept = Vec::new();
    for mut part in record.parts {
        if record.header.disposition == WriteDisposition::Migration {
            continue;
        }
        let originals: Vec<_> = part.rows.iter().map(|row| row.row.clone()).collect();
        let mut cause = source_cause(&boundaries, &record.header, &part);
        if matches!(cause, Some(LostWriteCause::SchemaChange(_)))
            && record.header.disposition == WriteDisposition::Apply
        {
            let version = boundaries
                .iter()
                .filter_map(|boundary| match boundary.excludes(&record.header, &part) {
                    Some(LostWriteCause::SchemaChange(version)) => Some(version),
                    _ => None,
                })
                .max()
                .expect("schema exclusion has a boundary");
            if !boundaries.iter().any(|b| {
                matches!(
                    b.excludes(&record.header, &part),
                    Some(LostWriteCause::Reset(_))
                )
            }) && crate::migration_receive::available(
                &schema.conversions,
                record.header.schema_version,
                version,
            ) {
                crate::migration_receive::convert(
                    &schema.conversions,
                    record.header.schema_version,
                    version,
                    &mut part,
                )?;
                cause = None;
            }
        }
        for row in originals {
            crate::write_sources::admit(database, &record.header, &row, cause)?;
        }
        if let Some(cause) = cause {
            exclude(database, schema, &record.header, &part, cause)?;
            if !part.dismissals.is_empty() {
                kept.push(WritePart {
                    rows: Vec::new(),
                    ..part
                });
            }
        } else {
            crate::download_checks::part(schema, &part)?;
            kept.push(part);
        }
    }
    record.parts = kept;
    Ok(record)
}

fn exclude(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    part: &WritePart,
    cause: LostWriteCause,
) -> Result<(), DbError> {
    for change in &part.rows {
        exclude_row(database, schema, header, cause, change)?;
    }
    Ok(())
}

fn exclude_row(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    reason: LostWriteCause,
    change: &coven_format::write::RowChange,
) -> Result<(), DbError> {
    let mut values = match &change.change.operation {
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
    for value in values.values_mut() {
        value.parents.clear();
    }
    let loss = coven_format::loss::Loss {
        row: change.row.clone(),
        generation: change.change.generation,
        retired: true,
        values: coven_format::loss::LossValues::Row(
            values
                .into_iter()
                .map(|(name, value)| {
                    (
                        name,
                        coven_merge::Cell {
                            write: header.position,
                            value,
                        },
                    )
                })
                .collect(),
        ),
        cause: coven_format::loss::LossCause::Excluded {
            write: header.position,
            cause: reason,
        },
    };
    crate::loss_record::put(database, &loss, None)?;
    crate::file_protection::retain_loss(database, schema, &loss)
}

/// Agreement state read atomically for sync to post.
pub struct SyncState {
    /// This install's device identity.
    pub device: DeviceId,
    /// Applied store-log positions from the same committed state.
    pub store_log: coven_format::value::EntryPositions,
    /// A queued local write prevents publishing the current fingerprints.
    pub uploads_pending: bool,
    /// Selected audience data has not yet been realized by a reload.
    pub reloading: bool,
    /// The final version of the last breaking migration batch; uploads await its raise.
    pub breaking_version: u32,
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

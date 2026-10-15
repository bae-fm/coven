//! Authenticated, decoded writes at the sync/database boundary.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::counter;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    value::WritePositions,
    write::{WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, WriteId, WritePast};
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
    /// The local breaking migration awaits publication and snapshot reload,
    /// and an opened part's audience has no capture to judge it under.
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

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn apply<P: crate::PendingValue>(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
    published: crate::Published,
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
        if let Some(wait) = prerequisite(database, header, &opened_audiences(&download))? {
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
            published,
            &opened_audiences(&download),
            true,
        )?;
        crate::retained_writes::download(database, &download)?;
        let affected = apply_opened(database, schema, download, &deleted)?;
        files.retain_rows(affected)?;
        files.before_commit()?;
        Ok(ApplyOutcome::Applied)
    }))
}

/// The audiences whose parts this device opened.
#[cfg(any(test, feature = "test-utils"))]
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
    opened: &[Audience],
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
            .filter_map(|b| match b.rule {
                crate::write_boundary::BoundaryRule::Raise(version)
                    if b.audience == Audience::Store =>
                {
                    Some(version)
                }
                _ => None,
            })
            .max()
            .unwrap_or(0);
        // Before its raise is kept, a migrated device judges a received
        // write under its captures; a part in an audience without one waits.
        if publication > applied {
            let captured: BTreeSet<_> = crate::raise_capture::provisional(database)?
                .into_iter()
                .map(|(audience, _, _)| audience)
                .collect();
            if !opened.iter().all(|audience| captured.contains(audience)) {
                return Ok(Some(WriteWait::SchemaPublication(publication)));
            }
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
    let record = crate::interpretation::accepted(database, schema, download)?;
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
            for record in part.records() {
                crate::write_sources::validate(database, &download.header, record)?;
                crate::write_sources::retain(database, &download.header, record)?;
            }
        }
    }
    Ok(())
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
    /// The newest own write an unpublished raise capture includes: it and
    /// every earlier own write upload before that raise publishes (§17.1).
    pub captured_through: Option<u64>,
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

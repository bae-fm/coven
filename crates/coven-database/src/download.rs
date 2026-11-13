//! Authenticated, decoded writes at the sync/database boundary.
use crate::sqlite::DatabaseConnection;
use crate::DbError;
use coven_format::write::WriteHeader;
#[cfg(test)]
use coven_format::write::{WritePart, WriteRecord};
use coven_merge::Audience;

/// A write whose signature and opened parts sync has already authenticated.
#[derive(Clone, Debug)]
#[cfg(test)]
pub(crate) struct DownloadedWrite {
    /// The authenticated write header.
    pub header: WriteHeader,
    /// Every part, including ones the receiving device could not decrypt.
    pub parts: Vec<DownloadedPart>,
}

/// Readable and unreadable parts belong to the same atomic slot.
#[derive(Clone, Debug)]
#[cfg(test)]
pub(crate) enum DownloadedPart {
    /// Authenticated plaintext changes for an audience.
    Opened(WritePart),
    /// The device does not have this audience's key.
    Skipped(Audience),
}

#[cfg(test)]
impl From<WriteRecord> for DownloadedWrite {
    fn from(record: WriteRecord) -> Self {
        Self {
            header: record.header,
            parts: record.parts.into_iter().map(DownloadedPart::Opened).collect(),
        }
    }
}

/// Applying a previously applied write never runs it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// All opened parts and all merge metadata committed together.
    Applied,
    /// This slot had already been consumed, including skipped or lost parts.
    AlreadyApplied,
    /// Nothing applied; sync retains the write until this prerequisite changes.
    Waiting(WriteWait),
}

/// A prerequisite for applying a downloaded write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteWait {
    /// These original row changes are unavailable even if their slots were consumed.
    Sources(Vec<coven_format::write_sources::ChangeId>),
    /// The app must support this schema version.
    SchemaVersion(u32),
    /// The selected boundary has not committed its replacement data.
    Reload(Audience),
}

pub(crate) fn source_wait(result: Result<ApplyOutcome, DbError>) -> Result<ApplyOutcome, DbError> {
    match result {
        Err(DbError::WriteWaiting(wait)) => Ok(ApplyOutcome::Waiting(wait)),
        Err(DbError::MissingWriteSources(sources)) => {
            Ok(ApplyOutcome::Waiting(WriteWait::Sources(sources)))
        }
        other => other,
    }
}

/// What the write `header` waits for before it applies. Its prefix needs no check: its slot holds its
/// `read_through` to the slot's `after`, which the caller has applied.
pub(crate) fn prerequisite(
    database: &DatabaseConnection,
    header: &WriteHeader,
) -> Result<Option<WriteWait>, DbError> {
    if header.schema_version > database.schema_version()? {
        return Ok(Some(WriteWait::SchemaVersion(header.schema_version)));
    }
    Ok(None)
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;

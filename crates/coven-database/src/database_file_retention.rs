//! File references used to prove that storage deletion cannot strand a row.

use super::Database;
use crate::file_protection::value as retain_value;
use crate::{
    DbError, DownloadedPartStream, DownloadedWriteStream, EntryId, FileEvidence, UnprovenFiles,
};
use coven_format::{value::Value, write::RowChange};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::{ColumnValue, Operation};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

/// Local references read in one database transaction.
pub struct FileRetention {
    /// Fixed paths named by rows, losses, reversible inputs, waiting writes or
    /// this device's upload queue.
    pub references: BTreeSet<(DeviceId, FileId)>,
}

impl Database {
    /// Every fixed path local data protects, in one read transaction.
    pub async fn retained_files(&self) -> Result<FileRetention, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                db.read_transaction(|| {
                    let references = db.query("SELECT device,file FROM _coven_row_file_refs UNION SELECT device,file FROM _coven_input_file_refs UNION SELECT device,file FROM _coven_queued_file_refs UNION SELECT device,file FROM _coven_file_uploads", [], |r| Ok((DeviceId(u64::from_be_bytes(r.get(0)?)),FileId(uuid::Uuid::from_bytes(r.get(1)?)))))?.into_iter().collect();
                    Ok(FileRetention { references })
                })
            })
        })
        .await
    }

    /// Stream declared uploaded references from checked plaintext. The caller
    /// must authenticate the complete object before using this result. It
    /// names the audience and key of each part it skipped: a skipped part, or
    /// a schema version this version cannot interpret, proves no file absent.
    pub async fn write_file_references<R: std::io::Read + Send + 'static>(
        &self,
        write: DownloadedWriteStream<R>,
        skipped: BTreeSet<(coven_merge::Audience, coven_foundation::id_source::KeyId)>,
    ) -> Result<FileEvidence, DbError> {
        self.call(move |inner| {
            let mut references = BTreeSet::new();
            write.header.encode()?;
            let version = write.header.header.schema_version;
            if !inner.access.write_schema.versions.contains(&version) {
                return Ok(FileEvidence::Unproven(UnprovenFiles::Schema(version)));
            }
            if write.header.parts.len() != write.parts.len() {
                return Err(crate::SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            let opened = write
                .parts
                .iter()
                .filter(|part| matches!(part, DownloadedPartStream::Opened(_)))
                .count();
            if (opened == write.parts.len()) != skipped.is_empty() {
                return Err(crate::SnapshotError::Inconsistent(
                    "skipped parts disagree with the parts named skipped",
                )
                .into());
            }
            if !skipped.is_empty() {
                return Ok(FileEvidence::Unproven(UnprovenFiles::Parts(skipped)));
            }
            for (header, part) in write.header.parts.into_iter().zip(write.parts) {
                let DownloadedPartStream::Opened(input) = part else {
                    unreachable!("every part opened");
                };
                crate::download_stream::read_part(input, header, &write.header.header, |frame| {
                    if let coven_format::dismissal::WriteFrame::Change(row) = frame {
                        retain_change(&inner.access.write_schema, &row, &mut references)?;
                    }
                    Ok::<_, DbError>(())
                })?;
            }
            Ok(FileEvidence::Complete(references))
        })
        .await
    }

    /// The schema versions whose writes and snapshots this version interprets.
    pub async fn write_schema_versions(&self) -> Result<RangeInclusive<u32>, DbError> {
        self.call(move |inner| Ok(inner.access.write_schema.versions.clone()))
            .await
    }

    /// The candidates of these uploaders, each with whether local data still
    /// protects it (§16.5). Other uploaders' candidates are not read: their
    /// files are deleted by another device until ownership changes.
    pub async fn file_candidates(
        &self,
        uploaders: BTreeSet<DeviceId>,
    ) -> Result<Vec<FileCandidate>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                db.read_transaction(|| {
                    let mut candidates = Vec::new();
                    for device in uploaders {
                        candidates.extend(db.query(
                            "SELECT file,EXISTS(SELECT 1 FROM _coven_file_protection p WHERE p.device=c.device AND p.file=c.file) FROM _coven_file_candidates c WHERE c.device=?1 ORDER BY c.file",
                            [device.0.to_be_bytes()],
                            |r| {
                                Ok(FileCandidate {
                                    device,
                                    file: FileId(uuid::Uuid::from_bytes(r.get(0)?)),
                                    protected: r.get(1)?,
                                })
                            },
                        )?);
                    }
                    Ok(candidates)
                })
            })
        })
        .await
    }

    /// Settle candidates in one transaction. A protected one goes only while
    /// local data still protects it, so releasing that protection adds it
    /// again; a deleted one goes after storage confirmed its deletion.
    pub async fn settle_file_candidates(
        &self,
        protected: Vec<(DeviceId, FileId)>,
        deleted: Vec<(DeviceId, FileId)>,
    ) -> Result<(), DbError> {
        if protected.is_empty() && deleted.is_empty() {
            return Ok(());
        }
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (device, file) in protected {
                        db.internal_execute(
                            "DELETE FROM _coven_file_candidates WHERE device=?1 AND file=?2 AND EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=?1 AND file=?2)",
                            params![device.0.to_be_bytes(), file.0.as_bytes()],
                        )?;
                    }
                    for (device, file) in deleted {
                        db.internal_execute(
                            "DELETE FROM _coven_file_candidates WHERE device=?1 AND file=?2",
                            params![device.0.to_be_bytes(), file.0.as_bytes()],
                        )?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// Paths named by a retained object this device confirmed deleted: its
    /// protection ended with the object (§16.5).
    pub async fn add_file_candidates(
        &self,
        files: BTreeSet<(DeviceId, FileId)>,
    ) -> Result<(), DbError> {
        if files.is_empty() {
            return Ok(());
        }
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (device, file) in files {
                        insert_candidate(db, device, file)?;
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// The kept removal whose cut-off this device scanned `device`'s file
    /// folder after, if it did.
    pub async fn file_scan(&self, device: DeviceId) -> Result<Option<EntryId>, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                Ok(db
                    .query(
                        "SELECT entry_device,entry_number FROM _coven_file_scans WHERE device=?1",
                        [device.0.to_be_bytes()],
                        |r| crate::store_log_tables::entry_id(r, 0),
                    )?
                    .pop())
            })
        })
        .await
    }

    /// Record a complete listing of a removed uploader's file folder, taken
    /// after `removal` cut its access off, with every path it found as a
    /// candidate. Returns false, recording nothing, once `removal` is no longer
    /// kept: that listing cannot stand for the last one (§16.5).
    pub async fn complete_file_scan(
        &self,
        device: DeviceId,
        removal: EntryId,
        files: BTreeSet<FileId>,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let kept: bool = db.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_store_log WHERE device=?1 AND number=?2 AND outcome='kept')",
                        params![removal.device.0.to_be_bytes(), removal.number.to_be_bytes()],
                        |r| r.get(0),
                    )?;
                    if !kept {
                        return Ok(false);
                    }
                    for file in files {
                        insert_candidate(db, device, file)?;
                    }
                    db.internal_execute(
                        "INSERT INTO _coven_file_scans(device,entry_device,entry_number) VALUES(?1,?2,?3) ON CONFLICT(device) DO UPDATE SET entry_device=excluded.entry_device,entry_number=excluded.entry_number",
                        params![
                            device.0.to_be_bytes(),
                            removal.device.0.to_be_bytes(),
                            removal.number.to_be_bytes()
                        ],
                    )?;
                    Ok(true)
                })
            })
        })
        .await
    }

    /// Forget `device`'s completed scan: its access was restored, so the
    /// next cut-off needs a listing of its own (§16.5). Candidates it found
    /// stay.
    pub async fn forget_file_scan(&self, device: DeviceId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "DELETE FROM _coven_file_scans WHERE device=?1",
                        [device.0.to_be_bytes()],
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Whether this device holds confirmation that every listed access was
    /// taken away: each provider account revoked with no request outstanding,
    /// and each S3 key deleted and read gone or confirmed by the person (§13).
    /// Nothing listed confirms nothing.
    pub async fn access_taken_away(
        &self,
        accounts: BTreeSet<String>,
        keys: BTreeSet<String>,
    ) -> Result<bool, DbError> {
        if accounts.is_empty() && keys.is_empty() {
            return Ok(false);
        }
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                db.read_transaction(|| {
                    for account in &accounts {
                        let revoked = db.query(
                            "SELECT 1 FROM _coven_provider_access WHERE account=?1 AND intended=0 AND granted=0 AND operation IS NULL",
                            [account],
                            |_| Ok(()),
                        )?;
                        if revoked.is_empty() {
                            return Ok(false);
                        }
                    }
                    for key in &keys {
                        let deleted = db.query(
                            "SELECT 1 FROM _coven_access_keys WHERE access_key_id=?1 AND operation IS NULL",
                            [key],
                            |_| Ok(()),
                        )?;
                        if deleted.is_empty() {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })
            })
        })
        .await
    }
}

/// A fixed file path awaiting a storage-deletion decision (§16.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileCandidate {
    /// The uploader named by its path.
    pub device: DeviceId,
    /// The file's fixed id.
    pub file: FileId,
    /// A local row, loss, reversible input, queued write or upload still names it.
    pub protected: bool,
}

fn insert_candidate(
    db: &crate::sqlite::DatabaseConnection,
    device: DeviceId,
    file: FileId,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_file_candidates(device,file) VALUES(?1,?2) ON CONFLICT DO NOTHING",
        params![device.0.to_be_bytes(), file.0.as_bytes()],
    )?;
    Ok(())
}

/// In the replay transaction: a scan whose removal is no longer kept no
/// longer stands for the last listing; the next cut-off scans again.
pub(crate) fn forget_dropped_scans(db: &crate::sqlite::DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_file_scans WHERE NOT EXISTS(
            SELECT 1 FROM _coven_store_log l WHERE l.device=entry_device
            AND l.number=entry_number AND l.outcome='kept')",
        [],
    )?;
    Ok(())
}

/// Uploaded files named by the rows and losses of the audience snapshot just
/// read into this transaction. A location the schema cannot accept is the
/// snapshot's own failure.
pub(crate) fn snapshot_references(
    db: &crate::sqlite::DatabaseConnection,
    audience: &coven_merge::Audience,
    schema: &crate::write_schema::WriteSchema,
) -> Result<BTreeSet<(DeviceId, FileId)>, DbError> {
    let mut references = BTreeSet::new();
    db.for_each(
        "SELECT table_name,columns FROM temp._coven_snapshot_values WHERE audience=?1",
        [crate::write_encoding::audience_text(audience)],
        |row| {
            let columns = coven_format::merge_fields::decode_columns(&row.get::<_, Vec<u8>>(1)?)?;
            retain_columns(schema, &row.get::<_, String>(0)?, &columns, &mut references)
        },
    )
    .and_then(|()| {
        crate::loss_record::visit(db, audience, |loss| {
            crate::file_protection::loss(schema, &loss, &mut references)
        })
    })
    .map_err(|error| match error {
        DbError::DamagedDatabase => {
            crate::snapshot_error::invalid("snapshot file location is invalid")
        }
        error => error,
    })?;
    Ok(references)
}

fn retain_change(
    schema: &crate::write_schema::WriteSchema,
    row: &RowChange,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if let Operation::Insert(columns) | Operation::Update(columns) = &row.change.operation {
        retain_columns(schema, &row.row.table, columns, references)?;
    }
    if let Some(column) = location_column(schema, &row.row.table)? {
        if let Some(value) = row.old.get(column) {
            retain_value(value, references)?;
        }
    }
    Ok(())
}

fn retain_columns(
    schema: &crate::write_schema::WriteSchema,
    table: &str,
    columns: &BTreeMap<String, ColumnValue<Value>>,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if let Some(column) = location_column(schema, table)? {
        if let Some(value) = columns.get(column) {
            retain_value(&value.value, references)?;
        }
    }
    Ok(())
}

fn location_column<'a>(
    schema: &'a crate::write_schema::WriteSchema,
    table: &str,
) -> Result<Option<&'a str>, DbError> {
    let declaration = schema
        .declarations
        .iter()
        .find(|d| d.name == table)
        .ok_or(DbError::DamagedDatabase)?;
    Ok(declaration
        .files
        .as_ref()
        .map(|file| file.location.as_str()))
}

#[cfg(test)]
#[path = "database_file_retention_tests.rs"]
mod tests;

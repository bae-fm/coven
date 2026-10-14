//! File references used to prove that storage deletion cannot strand a row.

use super::Database;
use crate::file_protection::value as retain_value;
use crate::{DbError, DownloadedPartStream, DownloadedWriteStream, FileUpload};
use coven_format::{value::Value, write::RowChange};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::{ColumnValue, Operation};
use std::collections::{BTreeMap, BTreeSet};

/// Local references and publication attempts read in one database transaction.
pub struct FileRetention {
    /// Uploaded paths named by rows, losses, reversible inputs or waiting writes.
    pub references: BTreeSet<(DeviceId, FileId)>,
    /// Includes unfinished publication, whose opaque fixed identity sync owns.
    pub uploads: Vec<FileUpload>,
}

impl Database {
    /// Retire an unused publication and its chunk hashes after storage confirms
    /// deletion. Repeating deletion after a lost reply is safe.
    pub async fn retire_unused_file(&self, id: i64) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let invalid: bool = db.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads WHERE id=?1 AND (unused=0 OR stored=0))",
                        [id], |r| r.get(0),
                    )?;
                    if invalid { return Err(DbError::DamagedDatabase); }
                    db.internal_execute("DELETE FROM _coven_file_uploads WHERE id=?1", [id])?;
                    Ok::<_, DbError>(())
                })
            })
        })
        .await
    }

    /// Read local protection after listing storage files. A file already listed
    /// must either remain in this queue or have a committed row/write reference.
    pub async fn retained_files(&self) -> Result<FileRetention, DbError> {
        self.call(move |inner| {
            let reader = inner.access.readers.acquire_reader();
            reader.with_reader(|db| {
                db.read_transaction(|| {
                    let references = db.query("SELECT device,file FROM _coven_row_file_refs UNION SELECT device,file FROM _coven_input_file_refs UNION SELECT device,file FROM _coven_queued_file_refs", [], |r| Ok((DeviceId(u64::from_be_bytes(r.get(0)?)),FileId(uuid::Uuid::from_bytes(r.get(1)?)))))?.into_iter().collect();
                    Ok(FileRetention {
                        references,
                        uploads: crate::file_queue::read(db)?,
                    })
                })
            })
        })
        .await
    }

    /// Stream declared uploaded references from checked plaintext. The caller
    /// must authenticate the complete object before using this result. A skipped
    /// part or unsupported schema returns `None`: neither can prove file absence.
    pub async fn write_file_references<R: std::io::Read + Send + 'static>(
        &self,
        write: DownloadedWriteStream<R>,
    ) -> Result<Option<BTreeSet<(DeviceId, FileId)>>, DbError> {
        self.call(move |inner| {
            let mut references = BTreeSet::new();
            write.header.encode()?;
            if !inner
                .access
                .write_schema
                .versions
                .contains(&write.header.header.schema_version)
            {
                return Ok(None);
            }
            if write.header.parts.len() != write.parts.len() {
                return Err(crate::SnapshotError::Inconsistent(
                    "write stream count differs from its header",
                )
                .into());
            }
            for (header, part) in write.header.parts.into_iter().zip(write.parts) {
                let DownloadedPartStream::Opened(input) = part else {
                    return Ok(None);
                };
                crate::download_stream::read_part(input, header, &write.header.header, |frame| {
                    if let coven_format::dismissal::WriteFrame::Change(row) = frame {
                        retain_change(&inner.access.write_schema, &row, &mut references)?;
                    }
                    Ok::<_, DbError>(())
                })?;
            }
            Ok(Some(references))
        })
        .await
    }
}

pub(super) fn snapshot_references(
    db: &crate::sqlite::DatabaseConnection,
    audience: &coven_merge::Audience,
    schema: &crate::write_schema::WriteSchema,
) -> Result<BTreeSet<(DeviceId, FileId)>, DbError> {
    let mut references = BTreeSet::new();
    db.for_each(
        "SELECT table_name,columns FROM temp._coven_snapshot_values",
        [],
        |row| {
            let columns = coven_format::merge_fields::decode_columns(&row.get::<_, Vec<u8>>(1)?)?;
            retain_columns(schema, &row.get::<_, String>(0)?, &columns, &mut references)
        },
    )?;
    crate::loss_record::visit(db, audience, |loss| {
        crate::file_protection::loss(schema, &loss, &mut references)
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

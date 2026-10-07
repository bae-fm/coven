//! File references used to prove that storage deletion cannot strand a row.

use super::{finish_blocking, Database};
use crate::{DbError, DownloadedPart, DownloadedWriteStream, FileUpload};
use coven_format::{file_reference::UploadedFileReference, value::Value, write::RowChange};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::{ColumnValue, Operation};
use std::collections::{BTreeMap, BTreeSet};

/// Local references and publication attempts read in one database transaction.
pub struct FileRetention {
    /// Uploaded objects still named by visible rows or waiting writes.
    pub references: BTreeSet<(DeviceId, FileId)>,
    /// Includes unfinished publication, whose opaque fixed identity sync owns.
    pub uploads: Vec<FileUpload>,
}

impl Database {
    /// Retire an unused publication and its chunk hashes after storage confirms
    /// deletion. Repeating deletion after a lost reply is safe.
    pub async fn retire_unused_file(&self, id: i64) -> Result<(), DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
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
            .await,
        )
    }

    /// Read local protection after listing storage files. A file already listed
    /// must either remain in this queue or have a committed row/write reference.
    pub async fn retained_files(&self) -> Result<FileRetention, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.with_reader(|db| {
                    db.read_transaction(|| {
                        let schema = &inner.write_schema;
                        let mut references = BTreeSet::new();
                        for table in &schema.declarations {
                            let Some(file) = &table.files else {
                                continue;
                            };
                            db.for_each(
                                &format!(
                                    "SELECT {} FROM main.{} WHERE {} IS NOT NULL",
                                    crate::sql::identifier(&file.location),
                                    crate::sql::identifier(&table.name),
                                    crate::sql::identifier(&file.location)
                                ),
                                [],
                                |row| {
                                    retain_value(
                                        &crate::write_encoding::value(row.get_ref(0)?)?,
                                        &mut references,
                                    )
                                },
                            )?;
                        }
                        crate::upload::each_plaintext(db, |_, parts| {
                            for (header, chunks) in parts {
                                let mut decoder =
                                    coven_format::write_stream::PartDecoder::new(header)?;
                                for chunk in chunks {
                                    for frame in decoder.chunk(&chunk?)? {
                                        if let coven_format::dismissal::WriteFrame::Change(row) =
                                            frame
                                        {
                                            retain_change(schema, &row, &mut references)?;
                                        }
                                    }
                                }
                                decoder.finish()?;
                            }
                            Ok(())
                        })?;
                        Ok(FileRetention {
                            references,
                            uploads: crate::file_queue::read(db)?,
                        })
                    })
                })
            })
            .await,
        )
    }

    /// Find declared uploaded references in one authenticated write. A skipped
    /// part returns `None`: sync cannot prove file absence without that audience.
    pub async fn write_file_references<R: std::io::Read + Send + 'static>(
        &self,
        write: DownloadedWriteStream<R>,
    ) -> Result<Option<BTreeSet<(DeviceId, FileId)>>, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let mut references = BTreeSet::new();
                for part in write.read()?.parts {
                    match part {
                        DownloadedPart::Skipped(_) => return Ok(None),
                        DownloadedPart::Opened(part) => {
                            for row in part.rows {
                                retain_change(&inner.write_schema, &row, &mut references)?;
                            }
                        }
                    }
                }
                Ok(Some(references))
            })
            .await,
        )
    }
}

pub(super) fn snapshot_references(
    db: &crate::sqlite::DatabaseConnection,
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
    Ok(())
}

fn retain_columns(
    schema: &crate::write_schema::WriteSchema,
    table: &str,
    columns: &BTreeMap<String, ColumnValue<Value>>,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    let declaration = schema
        .declarations
        .iter()
        .find(|d| d.name == table)
        .ok_or(DbError::DamagedDatabase)?;
    if let Some(file) = &declaration.files {
        if let Some(value) = columns.get(&file.location) {
            retain_value(&value.value, references)?;
        }
    }
    Ok(())
}

fn retain_value(
    value: &Value,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    if let crate::file_location::StoredLocation::Uploaded(text) =
        crate::file_location::StoredLocation::decode(value)?
    {
        let file = UploadedFileReference::decode(text.as_str())?;
        references.insert((file.device, file.id));
    }
    Ok(())
}

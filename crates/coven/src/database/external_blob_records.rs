use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use super::{with_coven_sql_authority, DbError};
use crate::protocol::blob::{RowBlobAuthority, RowBlobRef};

/// An external user-owned file a blob id resolves to, read back from a
/// `local_blob_refs` row. The blob's plaintext lives at `path` (an absolute file
/// Coven references but does not own); `size` is its registered plaintext length,
/// combined with the row's signed content hash to validate the exact file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBlob {
    /// Absolute path to the external file Coven reads but does not own.
    pub path: std::path::PathBuf,
    /// The file's plaintext length at registration. A read fails loud if the
    /// file's current length differs.
    pub size: u64,
}

pub(super) struct ExternalBlobRecords<'connection> {
    connection: &'connection Connection,
}

impl<'connection> ExternalBlobRecords<'connection> {
    pub(super) fn new(connection: &'connection Connection) -> Self {
        Self { connection }
    }

    pub(super) fn register(&self, reference: &RowBlobRef, path: &Path) -> Result<(), DbError> {
        if reference.authority() != &RowBlobAuthority::Local || reference.stored().is_some() {
            return Err(DbError::Message(
                "external file requires an exact Local row blob reference".to_string(),
            ));
        }
        let path = path.to_str().ok_or_else(|| {
            DbError::Message(format!("external blob path is not UTF-8: {path:?}"))
        })?;
        let size = i64::try_from(reference.plaintext_size()).map_err(|_| {
            DbError::Message("external blob plaintext size exceeds SQLite INTEGER".to_string())
        })?;
        with_coven_sql_authority(|| {
            self.connection
                .execute(
                    "INSERT INTO local_blob_refs
                     (table_name, row_id, column_name, row_stamp, namespace, blob_id,
                      path, plaintext_size, plaintext_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(table_name, row_id, column_name, row_stamp) DO UPDATE SET
                       namespace = excluded.namespace,
                       blob_id = excluded.blob_id,
                       path = excluded.path,
                       plaintext_size = excluded.plaintext_size,
                       plaintext_hash = excluded.plaintext_hash",
                    rusqlite::params![
                        reference.table(),
                        reference.row_id(),
                        reference.column(),
                        reference.row_stamp(),
                        &reference.blob().namespace,
                        &reference.blob().id,
                        path,
                        size,
                        reference.plaintext_hash().to_string(),
                    ],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
    }

    pub(super) fn clear(&self, reference: &RowBlobRef) -> Result<(), DbError> {
        with_coven_sql_authority(|| {
            self.connection
                .execute(
                    "DELETE FROM local_blob_refs WHERE table_name = ?1 AND row_id = ?2
                     AND column_name = ?3 AND row_stamp = ?4",
                    rusqlite::params![
                        reference.table(),
                        reference.row_id(),
                        reference.column(),
                        reference.row_stamp(),
                    ],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
    }

    pub(super) fn load(&self, reference: &RowBlobRef) -> Result<Option<ExternalBlob>, DbError> {
        let row = self
            .connection
            .query_row(
                "SELECT path, plaintext_size, plaintext_hash, namespace, blob_id
                 FROM local_blob_refs
                 WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                   AND row_stamp = ?4",
                rusqlite::params![
                    reference.table(),
                    reference.row_id(),
                    reference.column(),
                    reference.row_stamp()
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((path, size, hash, stored_namespace, stored_blob_id)) = row else {
            return Ok(None);
        };
        let size = u64::try_from(size).map_err(|_| {
            DbError::Message(format!(
                "external blob {} has negative size",
                reference.blob().id
            ))
        })?;
        let hash: crate::protocol::store_commit::ObjectHash = hash.parse().map_err(|error| {
            DbError::context(format!("external blob {} hash", reference.blob().id), error)
        })?;
        if size != reference.plaintext_size()
            || hash != reference.plaintext_hash()
            || stored_namespace != reference.blob().namespace
            || stored_blob_id != reference.blob().id
        {
            return Err(DbError::Message(format!(
                "external blob row {}/{}/{} differs from its row reference",
                reference.table(),
                reference.row_id(),
                reference.column()
            )));
        }
        Ok(Some(ExternalBlob {
            path: std::path::PathBuf::from(path),
            size,
        }))
    }
}

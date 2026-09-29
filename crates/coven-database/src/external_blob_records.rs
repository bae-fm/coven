use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use super::{with_coven_sql_authority, DbError};
use coven_protocol::blob::{RowBlobAuthority, RowBlobRef};

/// A user-provided blob's registered file, read back from its `local_blob_refs`
/// row. Coven reads the file at `path` but does not own it.
///
/// `prepare_external_blob` hashes the file, and registration writes that
/// SHA-256 into the row's hash column. Registration requires the row to be
/// Local, so at first the hash is a record on this device only. It reaches
/// other devices, inside the signed commit that publishes it, only if the row
/// is later published to a Store or Circle audience; a Local row's changes stay
/// in the Local partition, which is never published.
///
/// Whole reads and `materialize_row_blob` hash the file again and fail if its
/// size or hash differs from the row. A stream checks only the length when it
/// opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBlob {
    pub path: std::path::PathBuf,
    /// The file's length at registration, equal to the row's size column
    /// (`load` fails otherwise).
    pub size: u64,
}

pub(crate) struct ExternalBlobRecords<'connection> {
    connection: &'connection Connection,
}

impl<'connection> ExternalBlobRecords<'connection> {
    pub(crate) fn new(connection: &'connection Connection) -> Self {
        Self { connection }
    }

    pub(crate) fn register(&self, reference: &RowBlobRef, path: &Path) -> Result<(), DbError> {
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

    pub(crate) fn clear(&self, reference: &RowBlobRef) -> Result<(), DbError> {
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

    pub(crate) fn load(&self, reference: &RowBlobRef) -> Result<Option<ExternalBlob>, DbError> {
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
        let hash: coven_protocol::store_commit::ObjectHash = hash.parse().map_err(|error| {
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

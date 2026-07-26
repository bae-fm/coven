use crate::database::cloud_outbox_records::outbox_identity;
use crate::database::cloud_outbox_records::row_to_outbox_entry;
use crate::database::cloud_outbox_records::OutboxIdentity;

use super::*;

impl Database {
    // ---- Row-bound blob outbox ----

    pub fn enqueue_upload_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        row: &RowBlobRef,
        source_path: &Path,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        if row.authority() != &RowBlobAuthority::Local || row.stored().is_some() {
            return Err(DbError::Message(
                "cloud upload requires an exact Local row blob reference".to_string(),
            ));
        }
        let source_path = source_path.to_str().ok_or_else(|| {
            DbError::Message(format!(
                "blob source path for {}/{}/{} is not UTF-8: {source_path:?}",
                row.table(),
                row.row_id(),
                row.column()
            ))
        })?;
        let encoded = serde_json::to_string(row)
            .map_err(|error| DbError::Message(format!("serialize row blob ref: {error}")))?;
        let pending = serde_json::to_string(&OutboxUploadState::Pending).map_err(|error| {
            DbError::Message(format!("serialize pending blob upload state: {error}"))
        })?;
        conn.execute(
            "INSERT INTO cloud_outbox
             (operation, table_name, row_id, column_name, row_stamp, root_table, root_id,
              row_ref, upload_state, source_path, retain_pinned, created_at)
             VALUES ('upload', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(operation, table_name, row_id, column_name, row_stamp) DO UPDATE SET
               root_table = excluded.root_table,
               root_id = excluded.root_id,
               source_path = excluded.source_path,
               retain_pinned = excluded.retain_pinned,
               attempt_count = 0,
               last_error = NULL,
               last_attempt_at = NULL
             WHERE cloud_outbox.row_ref = excluded.row_ref
               AND cloud_outbox.root_table = excluded.root_table
               AND cloud_outbox.root_id = excluded.root_id",
            rusqlite::params![
                row.table(),
                row.row_id(),
                row.column(),
                row.row_stamp(),
                root_table,
                root_id,
                encoded,
                pending,
                source_path,
                retain_pinned,
                created_at,
            ],
        )
        .map_err(DbError::from)
        .and_then(|changed| {
            if changed == 1 {
                Ok(())
            } else {
                Err(DbError::Message(format!(
                    "upload outbox identity {}/{}/{}/{} carries different row facts",
                    row.table(),
                    row.row_id(),
                    row.column(),
                    row.row_stamp()
                )))
            }
        })
    }

    pub async fn mark_cloud_upload_prepared(
        &self,
        entry: &OutboxEntry,
        authority: crate::sync::audience_package::PackageAudience,
        stored: StoredBlobRef,
        spool_path: PathBuf,
    ) -> Result<(), DbError> {
        let OutboxOperation::Upload { row, state, .. } = &entry.operation else {
            return Err(DbError::Message(
                "only an upload outbox entry can own a prepared blob".to_string(),
            ));
        };
        if state != &OutboxUploadState::Pending {
            return Err(DbError::Message(
                "blob upload is already prepared".to_string(),
            ));
        }
        let locator = stored.locator();
        if locator.namespace() != row.blob().namespace
            || locator.blob_id() != row.blob().id
            || locator.plaintext_size() != row.plaintext_size()
            || locator.plaintext_hash() != row.plaintext_hash()
            || locator
                .scope()
                .is_some_and(|scope| scope != &row.blob().scope)
        {
            return Err(DbError::Message(
                "prepared blob differs from its exact Local row version".to_string(),
            ));
        }
        if locator.audience() != authority.remote_audience() {
            return Err(DbError::Message(
                "prepared blob audience differs from its package authority".to_string(),
            ));
        }
        let prepared = OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        };
        let prepared_json = serde_json::to_string(&prepared).map_err(|error| {
            DbError::Message(format!("serialize prepared blob upload: {error}"))
        })?;
        let pending_json = serde_json::to_string(&OutboxUploadState::Pending)
            .map_err(|error| DbError::Message(format!("serialize pending blob upload: {error}")))?;
        let id = entry.id;
        let table = row.table().to_string();
        let row_id = row.row_id().to_string();
        let column = row.column().to_string();
        let row_stamp = row.row_stamp().to_string();
        self.call(move |conn| {
            let updated = conn
                .execute(
                    "UPDATE cloud_outbox SET upload_state = ?1
                     WHERE id = ?2 AND operation = 'upload' AND table_name = ?3
                       AND row_id = ?4 AND column_name = ?5 AND row_stamp = ?6
                       AND upload_state = ?7",
                    rusqlite::params![
                        prepared_json,
                        id,
                        table,
                        row_id,
                        column,
                        row_stamp,
                        pending_json,
                    ],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "upload outbox entry changed before prepared-object handoff".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub async fn mark_cloud_upload_created(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        let OutboxOperation::Upload { row, state, .. } = &entry.operation else {
            return Err(DbError::Message(
                "only a prepared upload outbox entry can record cloud creation".to_string(),
            ));
        };
        let OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        } = state
        else {
            return Err(DbError::Message(
                "cloud creation requires a prepared upload object".to_string(),
            ));
        };
        let created_json = serde_json::to_string(&OutboxUploadState::Created {
            authority: authority.clone(),
            stored: stored.clone(),
            spool_path: spool_path.clone(),
        })
        .map_err(|error| DbError::Message(format!("serialize created blob upload: {error}")))?;
        let prepared_json = serde_json::to_string(state).map_err(|error| {
            DbError::Message(format!("serialize prepared blob upload identity: {error}"))
        })?;
        let id = entry.id;
        let table = row.table().to_string();
        let row_id = row.row_id().to_string();
        let column = row.column().to_string();
        let row_stamp = row.row_stamp().to_string();
        self.call(move |conn| {
            let updated = conn
                .execute(
                    "UPDATE cloud_outbox SET upload_state = ?1
                     WHERE id = ?2 AND operation = 'upload' AND table_name = ?3
                       AND row_id = ?4 AND column_name = ?5 AND row_stamp = ?6
                       AND upload_state = ?7",
                    rusqlite::params![
                        created_json,
                        id,
                        table,
                        row_id,
                        column,
                        row_stamp,
                        prepared_json,
                    ],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "upload outbox entry changed before cloud-created handoff".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub fn enqueue_delete_on(
        conn: &Connection,
        stored: &StoredBlobRef,
        created_at: &str,
    ) -> Result<(), DbError> {
        let encoded = serde_json::to_string(stored)
            .map_err(|error| DbError::Message(format!("serialize stored blob ref: {error}")))?;
        conn.execute(
            "INSERT INTO cloud_outbox (operation, stored_ref, created_at)
             VALUES ('delete', ?1, ?2)
             ON CONFLICT(stored_ref) DO UPDATE SET
               created_at = excluded.created_at,
               attempt_count = 0,
               last_error = NULL,
               last_attempt_at = NULL",
            (encoded, created_at),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Every upload the durable queue is still holding, oldest first.
    ///
    /// This is the read side of the same rows the drain works: an upload
    /// appears here the moment `make_remote` enqueues it, before any transfer
    /// is attempted, and stays until its publication activates or its
    /// cancellation clears it — across restarts, because the queue is a table
    /// in the store database rather than anything the process holds.
    pub async fn queued_uploads(&self) -> Result<Vec<QueuedUpload>, DbError> {
        self.queued_upload_rows(None).await
    }

    /// The queued uploads belonging to one gated root.
    ///
    /// Filtered in SQL rather than by the caller, so asking about one root
    /// neither loads nor decodes the row references of every other queued
    /// upload in the store.
    pub async fn queued_uploads_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<QueuedUpload>, DbError> {
        self.queued_upload_rows(Some((root_table.to_string(), root_id.to_string())))
            .await
    }

    async fn queued_upload_rows(
        &self,
        root: Option<(String, String)>,
    ) -> Result<Vec<QueuedUpload>, DbError> {
        self.call(move |conn| {
            const COLUMNS: &str = "SELECT row_ref, root_table, root_id, retain_pinned,
                            attempt_count, last_error, created_at, last_attempt_at
                     FROM cloud_outbox WHERE operation = 'upload'";
            let (sql, params): (String, Vec<String>) = match root {
                Some((root_table, root_id)) => (
                    format!("{COLUMNS} AND root_table = ?1 AND root_id = ?2 ORDER BY id"),
                    vec![root_table, root_id],
                ),
                None => (format!("{COLUMNS} ORDER BY id"), Vec::new()),
            };
            let mut statement = conn.prepare(&sql).map_err(DbError::from)?;
            let queued = statement
                .query_map(rusqlite::params_from_iter(params), row_to_queued_upload)
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(queued)
        })
        .await
    }

    /// Every cloud tombstone the durable queue is still holding, oldest first.
    pub async fn queued_deletes(&self) -> Result<Vec<QueuedDelete>, DbError> {
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT stored_ref, attempt_count, last_error, created_at, last_attempt_at
                     FROM cloud_outbox WHERE operation = 'delete' ORDER BY id",
                )
                .map_err(DbError::from)?;
            let queued = statement
                .query_map([], row_to_queued_delete)
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(queued)
        })
        .await
    }

    pub async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("upload").await
    }

    pub async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("delete").await
    }

    async fn pending_outbox(&self, operation: &'static str) -> Result<Vec<OutboxEntry>, DbError> {
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT id, operation, row_ref, stored_ref, source_path, retain_pinned,
                            upload_state, attempt_count, last_attempt_at, root_table, root_id
                     FROM cloud_outbox WHERE operation = ?1 ORDER BY id",
                )
                .map_err(DbError::from)?;
            let entries = statement
                .query_map([operation], row_to_outbox_entry)
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            Ok(entries)
        })
        .await
    }

    pub async fn remove_cloud_outbox_entry(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        let entry = entry.clone();
        self.call(move |conn| Self::remove_cloud_outbox_entry_on(conn, &entry))
            .await
    }

    pub(crate) fn remove_cloud_outbox_entry_on(
        conn: &Connection,
        entry: &OutboxEntry,
    ) -> Result<(), DbError> {
        let identity = outbox_identity(&entry.operation)?;
        let removed = match identity {
            OutboxIdentity::Upload {
                table,
                row_id,
                column,
                row_stamp,
            } => conn.execute(
                "DELETE FROM cloud_outbox WHERE id = ?1 AND operation = 'upload'
                 AND table_name = ?2 AND row_id = ?3 AND column_name = ?4 AND row_stamp = ?5",
                rusqlite::params![entry.id, table, row_id, column, row_stamp],
            ),
            OutboxIdentity::Stored { operation, stored } => conn.execute(
                "DELETE FROM cloud_outbox WHERE id = ?1 AND operation = ?2 AND stored_ref = ?3",
                rusqlite::params![entry.id, operation, stored],
            ),
        }
        .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(
                "cloud outbox entry changed before exact dequeue".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn finish_cancelled_upload_on(
        conn: &Connection,
        entry: &OutboxEntry,
    ) -> Result<bool, DbError> {
        let OutboxOperation::Upload {
            root_table,
            root_id,
            ..
        } = &entry.operation
        else {
            return Err(DbError::Message(
                "make_remote cleanup requires an upload entry".to_string(),
            ));
        };
        if !matches!(
            Self::make_remote_intent_state(conn, root_table, root_id)?,
            Some(MakeRemoteIntentState::Cancelling)
        ) {
            return Err(DbError::Message(format!(
                "make_remote cleanup for {root_table:?}/{root_id:?} lost cancellation ownership"
            )));
        }
        Self::remove_cloud_outbox_entry_on(conn, entry)?;
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM cloud_outbox
                 WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2",
                (root_table, root_id),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if remaining != 0 {
            return Ok(false);
        }
        let removed = conn
            .execute(
                "DELETE FROM blob_make_remote_intents
                 WHERE root_table = ?1 AND root_id = ?2 AND state = 'cancelling'",
                (root_table, root_id),
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "make_remote cancellation {root_table:?}/{root_id:?} changed before completion"
            )));
        }
        Ok(true)
    }

    pub async fn record_cloud_outbox_failure(
        &self,
        entry: &OutboxEntry,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let id = entry.id;
        let identity = outbox_identity(&entry.operation)?;
        let error = error.to_string();
        let attempted_at = attempted_at.to_string();
        self.call(move |conn| {
            let updated = match identity {
                OutboxIdentity::Upload {
                    table,
                    row_id,
                    column,
                    row_stamp,
                } => conn.execute(
                    "UPDATE cloud_outbox SET attempt_count = attempt_count + 1,
                     last_error = ?1, last_attempt_at = ?2
                     WHERE id = ?3 AND operation = 'upload' AND table_name = ?4
                       AND row_id = ?5 AND column_name = ?6 AND row_stamp = ?7",
                    rusqlite::params![error, attempted_at, id, table, row_id, column, row_stamp],
                ),
                OutboxIdentity::Stored { operation, stored } => conn.execute(
                    "UPDATE cloud_outbox SET attempt_count = attempt_count + 1,
                     last_error = ?1, last_attempt_at = ?2
                     WHERE id = ?3 AND operation = ?4 AND stored_ref = ?5",
                    rusqlite::params![error, attempted_at, id, operation, stored],
                ),
            }
            .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(
                    "cloud outbox entry changed before failure recording".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub async fn reset_cloud_outbox_backoff(&self) -> Result<(), DbError> {
        self.call(|conn| {
            conn.execute(
                "UPDATE cloud_outbox SET last_attempt_at = NULL WHERE attempt_count > 0",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }
}

fn row_to_queued_upload(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedUpload> {
    let invalid = |index: usize, message: String| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            message.into(),
        )
    };
    let encoded: String = row.get(0)?;
    let reference: RowBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(0, error.to_string()))?;
    let attempt_count: i64 = row.get(4)?;
    Ok(QueuedUpload {
        namespace: reference.blob().namespace.clone(),
        blob_id: reference.blob().id.clone(),
        table_name: reference.table().to_string(),
        row_id: reference.row_id().to_string(),
        root_table: row.get(1)?,
        root_id: row.get(2)?,
        retain_pinned: row.get(3)?,
        attempt_count: u64::try_from(attempt_count)
            .map_err(|error| invalid(4, error.to_string()))?,
        last_error: row.get(5)?,
        created_at: row.get(6)?,
        last_attempt_at: row.get(7)?,
    })
}

fn row_to_queued_delete(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueuedDelete> {
    let invalid = |index: usize, message: String| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            message.into(),
        )
    };
    let encoded: String = row.get(0)?;
    let stored: StoredBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(0, error.to_string()))?;
    let attempt_count: i64 = row.get(1)?;
    Ok(QueuedDelete {
        namespace: stored.locator().namespace().to_string(),
        blob_id: stored.locator().blob_id().to_string(),
        attempt_count: u64::try_from(attempt_count)
            .map_err(|error| invalid(1, error.to_string()))?,
        last_error: row.get(2)?,
        created_at: row.get(3)?,
        last_attempt_at: row.get(4)?,
    })
}

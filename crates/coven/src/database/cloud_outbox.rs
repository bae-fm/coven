use crate::database::cloud_outbox_records::outbox_identity;
use crate::database::cloud_outbox_records::row_to_outbox_entry;
use crate::database::cloud_outbox_records::OutboxIdentity;

use super::*;

impl Database {
    // ---- Row-bound blob outbox ----

    /// Move one upload entry's `upload_state` from `from` to `to`, keyed by the
    /// entry id and the five columns that identify which row version it uploads.
    ///
    /// The `from` value is part of the WHERE clause, so this is a
    /// compare-and-swap: an entry whose state has already moved on is left
    /// alone and `context` names the handoff that lost the race, rather than
    /// one writer overwriting another's transition.
    async fn swap_upload_state(
        &self,
        id: i64,
        row: &crate::blob::RowBlobRef,
        from: String,
        to: String,
        context: &'static str,
    ) -> Result<(), DbError> {
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
                    rusqlite::params![to, id, table, row_id, column, row_stamp, from],
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(format!(
                    "upload outbox entry changed before {context}"
                )));
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn mark_cloud_upload_prepared(
        &self,
        entry: &OutboxEntry,
        authority: crate::protocol::audience_package::PackageAudience,
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
        if !crate::blob::locator_describes_row(
            locator,
            row.blob(),
            row.plaintext_size(),
            row.plaintext_hash(),
        ) {
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
        self.swap_upload_state(
            entry.id,
            row,
            pending_json,
            prepared_json,
            "prepared-object handoff",
        )
        .await
    }

    pub(crate) async fn mark_cloud_upload_created(
        &self,
        entry: &OutboxEntry,
    ) -> Result<(), DbError> {
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
        self.swap_upload_state(
            entry.id,
            row,
            prepared_json,
            created_json,
            "cloud-created handoff",
        )
        .await
    }

    pub(crate) async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("upload").await
    }

    pub(crate) async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
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

    pub(crate) async fn record_cloud_outbox_failure(
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

    pub(crate) async fn reset_cloud_outbox_backoff(&self) -> Result<(), DbError> {
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

use super::*;

#[derive(Clone)]
pub(crate) struct PublishedBlobDropIntent {
    pub(crate) seq: u64,
    pub(crate) drop: crate::sync::cycle::DeferredLocalBlobDrop,
}

impl StoreDatabase {
    #[doc(hidden)]
    pub(crate) async fn queued_uploads(&self) -> Result<Vec<crate::db::QueuedUpload>, DbError> {
        self.queued_upload_rows(None).await
    }

    #[doc(hidden)]
    pub(crate) async fn queued_uploads_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<crate::db::QueuedUpload>, DbError> {
        self.queued_upload_rows(Some((root_table.to_string(), root_id.to_string())))
            .await
    }

    async fn queued_upload_rows(
        &self,
        root: Option<(String, String)>,
    ) -> Result<Vec<crate::db::QueuedUpload>, DbError> {
        self.connection
            .call(move |connection| {
                const COLUMNS: &str = "SELECT row_ref, root_table, root_id, retain_pinned,
                            attempt_count, last_error, created_at, last_attempt_at
                     FROM cloud_outbox WHERE operation = 'upload'";
                let (sql, parameters): (String, Vec<String>) = match root {
                    Some((root_table, root_id)) => (
                        format!("{COLUMNS} AND root_table = ?1 AND root_id = ?2 ORDER BY id"),
                        vec![root_table, root_id],
                    ),
                    None => (format!("{COLUMNS} ORDER BY id"), Vec::new()),
                };
                let mut statement = connection.prepare(&sql).map_err(DbError::from)?;
                let uploads = statement
                    .query_map(rusqlite::params_from_iter(parameters), row_to_queued_upload)
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(uploads)
            })
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn queued_deletes(&self) -> Result<Vec<crate::db::QueuedDelete>, DbError> {
        self.connection
            .call(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT stored_ref, attempt_count, last_error, created_at, last_attempt_at
                         FROM cloud_outbox WHERE operation = 'delete' ORDER BY id",
                    )
                    .map_err(DbError::from)?;
                let deletes = statement
                    .query_map([], row_to_queued_delete)
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(deletes)
            })
            .await
    }

    pub(crate) async fn pending_blob_deletes(
        &self,
    ) -> Result<Vec<crate::db::OutboxEntry>, DbError> {
        self.connection
            .call(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation, row_ref, stored_ref, source_path, retain_pinned,
                                upload_state, attempt_count, last_attempt_at, root_table, root_id
                         FROM cloud_outbox WHERE operation = 'delete' ORDER BY id",
                    )
                    .map_err(DbError::from)?;
                let entries = statement
                    .query_map([], crate::database::row_to_outbox_entry)
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(entries)
            })
            .await
    }

    pub(crate) async fn remove_blob_delete(
        &self,
        entry: &crate::db::OutboxEntry,
    ) -> Result<(), DbError> {
        let crate::db::OutboxOperation::Delete { stored } = &entry.operation else {
            return Err(DbError::Message(
                "blob delete dequeue requires a delete outbox entry".to_string(),
            ));
        };
        let id = entry.id;
        let stored = serde_json::to_string(stored)
            .map_err(|error| DbError::Message(format!("serialize stored blob ref: {error}")))?;
        self.connection
            .call(move |connection| {
                let removed = connection
                    .execute(
                        "DELETE FROM cloud_outbox
                         WHERE id = ?1 AND operation = 'delete' AND stored_ref = ?2",
                        rusqlite::params![id, stored],
                    )
                    .map_err(DbError::from)?;
                if removed != 1 {
                    return Err(DbError::Message(
                        "blob delete outbox entry changed before exact dequeue".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }

    pub(crate) async fn record_blob_delete_failure(
        &self,
        entry: &crate::db::OutboxEntry,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let crate::db::OutboxOperation::Delete { stored } = &entry.operation else {
            return Err(DbError::Message(
                "blob delete failure requires a delete outbox entry".to_string(),
            ));
        };
        let id = entry.id;
        let stored = serde_json::to_string(stored)
            .map_err(|error| DbError::Message(format!("serialize stored blob ref: {error}")))?;
        let error = error.to_string();
        let attempted_at = attempted_at.to_string();
        self.connection
            .call(move |connection| {
                let updated = connection
                    .execute(
                        "UPDATE cloud_outbox SET attempt_count = attempt_count + 1,
                         last_error = ?1, last_attempt_at = ?2
                         WHERE id = ?3 AND operation = 'delete' AND stored_ref = ?4",
                        rusqlite::params![error, attempted_at, id, stored],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "blob delete outbox entry changed before failure recording".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }

    pub(crate) async fn published_blob_drop_intents(
        &self,
        max_seq: u64,
    ) -> Result<Vec<PublishedBlobDropIntent>, DbError> {
        self.connection
            .call(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition
                         FROM published_blob_drop_intents
                         WHERE seq <= ?1
                           AND NOT EXISTS (
                               SELECT 1 FROM store_write_blob_leases lease
                               WHERE lease.namespace = published_blob_drop_intents.namespace
                                 AND lease.blob_id = published_blob_drop_intents.blob_id
                           )
                         ORDER BY seq, namespace, blob_id, locator_hash",
                    )
                    .map_err(DbError::from)?;
                let intents = statement
                    .query_map([max_seq as i64], |row| {
                        let size: Option<i64> = row.get(3)?;
                        let size = size.ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Integer,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "published blob drop intent is missing size",
                                )),
                            )
                        })?;
                        if size < 0 {
                            return Err(rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Integer,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!(
                                        "published blob drop intent has negative size {size}"
                                    ),
                                )),
                            ));
                        }
                        let plaintext_hash =
                            row.get::<_, String>(4)?.parse().map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    4,
                                    rusqlite::types::Type::Text,
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!(
                                            "invalid published blob plaintext hash: {error}"
                                        ),
                                    )),
                                )
                            })?;
                        let locator_hash =
                            row.get::<_, String>(5)?.parse().map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    5,
                                    rusqlite::types::Type::Text,
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!(
                                            "invalid published blob locator hash: {error}"
                                        ),
                                    )),
                                )
                            })?;
                        let disposition_raw: String = row.get(6)?;
                        let disposition =
                            crate::sync::cycle::DeferredLocalBlobDisposition::from_db(
                                &disposition_raw,
                            )
                            .map_err(|message| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    6,
                                    rusqlite::types::Type::Text,
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        message,
                                    )),
                                )
                            })?;
                        Ok(PublishedBlobDropIntent {
                            seq: row.get::<_, i64>(0)? as u64,
                            drop: crate::sync::cycle::DeferredLocalBlobDrop {
                                namespace: row.get(1)?,
                                id: row.get(2)?,
                                size: size as u64,
                                plaintext_hash,
                                locator_hash,
                                disposition,
                            },
                        })
                    })
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(intents)
            })
            .await
    }

    pub(crate) async fn clear_published_blob_drop_intent(
        &self,
        intent: &PublishedBlobDropIntent,
    ) -> Result<(), DbError> {
        let seq = intent.seq;
        let namespace = intent.drop.namespace.clone();
        let id = intent.drop.id.clone();
        let locator_hash = intent.drop.locator_hash.to_string();
        self.connection
            .call(move |connection| {
                connection
                    .execute(
                        "DELETE FROM published_blob_drop_intents
                         WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3 AND locator_hash = ?4",
                        rusqlite::params![seq as i64, namespace, id, locator_hash],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn pending_blob_uploads(
        &self,
    ) -> Result<Vec<crate::db::OutboxEntry>, DbError> {
        self.connection
            .call(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT id, operation, row_ref, stored_ref, source_path, retain_pinned,
                                upload_state, attempt_count, last_attempt_at, root_table, root_id
                         FROM cloud_outbox WHERE operation = 'upload' ORDER BY id",
                    )
                    .map_err(DbError::from)?;
                let entries = statement
                    .query_map([], crate::database::row_to_outbox_entry)
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                Ok(entries)
            })
            .await
    }

    pub(crate) async fn mark_blob_upload_prepared(
        &self,
        entry: &crate::db::OutboxEntry,
        authority: crate::protocol::audience_package::PackageAudience,
        stored: crate::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    ) -> Result<(), DbError> {
        let crate::db::OutboxOperation::Upload { row, state, .. } = &entry.operation else {
            return Err(DbError::Message(
                "only an upload outbox entry can own a prepared blob".to_string(),
            ));
        };
        if state != &crate::db::OutboxUploadState::Pending {
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
        let prepared = crate::db::OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        };
        let prepared_json = serde_json::to_string(&prepared).map_err(|error| {
            DbError::Message(format!("serialize prepared blob upload: {error}"))
        })?;
        let pending_json = serde_json::to_string(&crate::db::OutboxUploadState::Pending)
            .map_err(|error| DbError::Message(format!("serialize pending blob upload: {error}")))?;
        self.swap_blob_upload_state(
            entry.id,
            row,
            pending_json,
            prepared_json,
            "prepared-object handoff",
        )
        .await
    }

    pub(crate) async fn mark_blob_upload_created(
        &self,
        entry: &crate::db::OutboxEntry,
    ) -> Result<(), DbError> {
        let crate::db::OutboxOperation::Upload { row, state, .. } = &entry.operation else {
            return Err(DbError::Message(
                "only a prepared upload outbox entry can record cloud creation".to_string(),
            ));
        };
        let crate::db::OutboxUploadState::Prepared {
            authority,
            stored,
            spool_path,
        } = state
        else {
            return Err(DbError::Message(
                "cloud creation requires a prepared upload object".to_string(),
            ));
        };
        let created_json = serde_json::to_string(&crate::db::OutboxUploadState::Created {
            authority: authority.clone(),
            stored: stored.clone(),
            spool_path: spool_path.clone(),
        })
        .map_err(|error| DbError::Message(format!("serialize created blob upload: {error}")))?;
        let prepared_json = serde_json::to_string(state).map_err(|error| {
            DbError::Message(format!("serialize prepared blob upload identity: {error}"))
        })?;
        self.swap_blob_upload_state(
            entry.id,
            row,
            prepared_json,
            created_json,
            "cloud-created handoff",
        )
        .await
    }

    pub(crate) async fn record_blob_upload_failure(
        &self,
        entry: &crate::db::OutboxEntry,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let id = entry.id;
        let identity = crate::database::outbox_identity(&entry.operation)?;
        let error = error.to_string();
        let attempted_at = attempted_at.to_string();
        self.connection
            .call(move |connection| {
                let updated = match identity {
                    crate::database::OutboxIdentity::Upload {
                        table,
                        row_id,
                        column,
                        row_stamp,
                    } => connection.execute(
                        "UPDATE cloud_outbox SET attempt_count = attempt_count + 1,
                         last_error = ?1, last_attempt_at = ?2
                         WHERE id = ?3 AND operation = 'upload' AND table_name = ?4
                           AND row_id = ?5 AND column_name = ?6 AND row_stamp = ?7",
                        rusqlite::params![
                            error,
                            attempted_at,
                            id,
                            table,
                            row_id,
                            column,
                            row_stamp
                        ],
                    ),
                    crate::database::OutboxIdentity::Stored { operation, stored } => connection
                        .execute(
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

    async fn swap_blob_upload_state(
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
        self.connection
            .call(move |connection| {
                let updated = connection
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

    pub(crate) async fn make_remote_intent_state(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<crate::database::MakeRemoteIntentState>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.connection
            .call(move |connection| {
                Database::make_remote_intent_state(connection, &root_table, &root_id)
            })
            .await
    }

    pub(crate) async fn make_remote_progress(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<crate::MakeRemoteProgress>, DbError> {
        Ok(self
            .make_remote_intent_state(root_table, root_id)
            .await?
            .map(|state| match state {
                crate::database::MakeRemoteIntentState::Uploading => {
                    crate::MakeRemoteProgress::Uploading
                }
                crate::database::MakeRemoteIntentState::Cancelling => {
                    crate::MakeRemoteProgress::Cancelling
                }
                crate::database::MakeRemoteIntentState::Publishing(_) => {
                    crate::MakeRemoteProgress::Publishing
                }
            }))
    }

    pub(crate) async fn finish_cancelled_blob_upload(
        &self,
        entry: &crate::db::OutboxEntry,
    ) -> Result<bool, DbError> {
        let entry = entry.clone();
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                let finished = crate::database::CloudOutboxRecords::new(&transaction)
                    .finish_cancelled_upload(&entry)?;
                transaction.commit().map_err(DbError::from)?;
                Ok(finished)
            })
            .await
    }
}

fn row_to_queued_upload(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::db::QueuedUpload> {
    let invalid = |index: usize, message: String| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            message.into(),
        )
    };
    let encoded: String = row.get(0)?;
    let reference: crate::blob::RowBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(0, error.to_string()))?;
    let attempt_count: i64 = row.get(4)?;
    Ok(crate::db::QueuedUpload {
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

fn row_to_queued_delete(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::db::QueuedDelete> {
    let invalid = |index: usize, message: String| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            message.into(),
        )
    };
    let encoded: String = row.get(0)?;
    let stored: crate::blob::locator::StoredBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(0, error.to_string()))?;
    let attempt_count: i64 = row.get(1)?;
    Ok(crate::db::QueuedDelete {
        namespace: stored.locator().namespace().to_string(),
        blob_id: stored.locator().blob_id().to_string(),
        attempt_count: u64::try_from(attempt_count)
            .map_err(|error| invalid(1, error.to_string()))?,
        last_error: row.get(2)?,
        created_at: row.get(3)?,
        last_attempt_at: row.get(4)?,
    })
}

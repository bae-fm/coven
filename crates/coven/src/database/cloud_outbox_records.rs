use super::*;

pub(crate) struct CloudOutboxRecords<'connection> {
    connection: &'connection Connection,
}

impl<'connection> CloudOutboxRecords<'connection> {
    pub(crate) fn new(connection: &'connection Connection) -> Self {
        Self { connection }
    }
}

pub(crate) enum OutboxIdentity {
    Upload {
        table: String,
        row_id: String,
        column: String,
        row_stamp: String,
    },
    Stored {
        operation: &'static str,
        stored: String,
    },
}

impl CloudOutboxRecords<'_> {
    fn upload_entry_for_identity(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<Option<OutboxEntry>, DbError> {
        self.connection
            .query_row(
                "SELECT id, operation, row_ref, stored_ref, source_path, retain_pinned,
                        upload_state, attempt_count, last_attempt_at, root_table, root_id
                 FROM cloud_outbox
                 WHERE operation = 'upload' AND table_name = ?1 AND row_id = ?2
                   AND column_name = ?3 AND row_stamp = ?4",
                rusqlite::params![table, row_id, column, row_stamp],
                row_to_outbox_entry,
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(crate) fn consume_created_upload_handoff(
        &self,
        package: &AudiencePackage,
        binding: &RowBlobLocatorBinding,
    ) -> Result<bool, DbError> {
        let Some(entry) = self.upload_entry_for_identity(
            binding.table(),
            binding.row_id(),
            binding.column(),
            binding.row_stamp(),
        )?
        else {
            return Ok(false);
        };
        let OutboxOperation::Upload {
            row,
            state: OutboxUploadState::Created {
                authority, stored, ..
            },
            ..
        } = &entry.operation
        else {
            return Err(DbError::Message(format!(
                "activated blob binding {}/{}/{} at {} has an upload that is not Created",
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp()
            )));
        };
        if row.table() != binding.table()
            || row.row_id() != binding.row_id()
            || row.column() != binding.column()
            || row.row_stamp() != binding.row_stamp()
            || authority != package.audience()
            || stored != binding.blob()
        {
            return Err(DbError::Message(format!(
                "activated blob binding {}/{}/{} at {} differs from its Created upload handoff",
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp()
            )));
        }
        self.remove_entry(&entry)?;
        Ok(true)
    }

    pub(crate) fn created_upload_handoff(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<Option<StoreWriteRemoteBlob>, DbError> {
        let Some(entry) = self.upload_entry_for_identity(table, row_id, column, row_stamp)? else {
            return Ok(None);
        };
        let OutboxOperation::Upload { row, state, .. } = entry.operation else {
            return Err(DbError::Message(
                "upload identity query returned a non-upload operation".to_string(),
            ));
        };
        if row.table() != table
            || row.row_id() != row_id
            || row.column() != column
            || row.row_stamp() != row_stamp
        {
            return Err(DbError::Message(format!(
                "upload outbox row facts differ from identity {table}/{row_id}/{column} at {row_stamp}"
            )));
        }
        match state {
            OutboxUploadState::Created {
                authority, stored, ..
            } => Ok(Some(StoreWriteRemoteBlob { authority, stored })),
            OutboxUploadState::Pending | OutboxUploadState::Prepared { .. } => Ok(None),
        }
    }

    pub(crate) fn upload_entries_for_rows(
        &self,
        rows: &[RowBlobRef],
    ) -> Result<Vec<OutboxEntry>, DbError> {
        rows.iter()
            .filter_map(|row| {
                match self.upload_entry_for_identity(
                    row.table(),
                    row.row_id(),
                    row.column(),
                    row.row_stamp(),
                ) {
                    Ok(Some(entry)) => Some(Ok(entry)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
    }

    pub(crate) fn upload_entries_for_root(
        &self,
        gates: &Gates,
        tables: &[SyncedTable],
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<OutboxEntry>, DbError> {
        let rows = Database::row_blob_refs_for_root_on(
            self.connection,
            gates,
            tables,
            root_table,
            root_id,
        )?;
        self.upload_entries_for_rows(&rows)
    }

    pub(crate) fn enqueue_upload(
        &self,
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
        self.connection
            .execute(
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

    pub(crate) fn enqueue_delete(
        &self,
        stored: &StoredBlobRef,
        created_at: &str,
    ) -> Result<(), DbError> {
        let encoded = serde_json::to_string(stored)
            .map_err(|error| DbError::Message(format!("serialize stored blob ref: {error}")))?;
        crate::database::with_coven_sql_authority(|| {
            self.connection
                .execute(
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
        })
    }

    pub(crate) fn remove_entry(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        let identity = outbox_identity(&entry.operation)?;
        let removed = match identity {
            OutboxIdentity::Upload {
                table,
                row_id,
                column,
                row_stamp,
            } => self.connection.execute(
                "DELETE FROM cloud_outbox WHERE id = ?1 AND operation = 'upload'
                 AND table_name = ?2 AND row_id = ?3 AND column_name = ?4 AND row_stamp = ?5",
                rusqlite::params![entry.id, table, row_id, column, row_stamp],
            ),
            OutboxIdentity::Stored { operation, stored } => self.connection.execute(
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

    pub(crate) fn finish_cancelled_upload(&self, entry: &OutboxEntry) -> Result<bool, DbError> {
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
            Database::make_remote_intent_state(self.connection, root_table, root_id)?,
            Some(MakeRemoteIntentState::Cancelling)
        ) {
            return Err(DbError::Message(format!(
                "make_remote cleanup for {root_table:?}/{root_id:?} lost cancellation ownership"
            )));
        }
        self.remove_entry(entry)?;
        let remaining: i64 = self
            .connection
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
        let removed = self
            .connection
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
}

pub(crate) fn outbox_identity(operation: &OutboxOperation) -> Result<OutboxIdentity, DbError> {
    match operation {
        OutboxOperation::Upload { row, .. } => Ok(OutboxIdentity::Upload {
            table: row.table().to_string(),
            row_id: row.row_id().to_string(),
            column: row.column().to_string(),
            row_stamp: row.row_stamp().to_string(),
        }),
        OutboxOperation::Delete { stored } => Ok(OutboxIdentity::Stored {
            operation: "delete",
            stored: serde_json::to_string(stored).map_err(|error| {
                DbError::Message(format!("serialize stored blob outbox identity: {error}"))
            })?,
        }),
    }
}

pub(crate) fn row_to_outbox_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    fn invalid(index: usize, message: String) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message,
            )),
        )
    }

    let tag: String = row.get(1)?;
    let operation = match tag.as_str() {
        "upload" => {
            let encoded: String = row.get(2)?;
            let reference: RowBlobRef =
                serde_json::from_str(&encoded).map_err(|error| invalid(2, error.to_string()))?;
            let source_path: String = row.get(4)?;
            let state_json: String = row.get(6)?;
            let state: OutboxUploadState =
                serde_json::from_str(&state_json).map_err(|error| invalid(6, error.to_string()))?;
            if let OutboxUploadState::Prepared {
                authority, stored, ..
            }
            | OutboxUploadState::Created {
                authority, stored, ..
            } = &state
            {
                let locator = stored.locator();
                if !crate::blob::locator_describes_row(
                    locator,
                    reference.blob(),
                    reference.plaintext_size(),
                    reference.plaintext_hash(),
                ) {
                    return Err(invalid(
                        6,
                        "prepared upload differs from its exact row version".to_string(),
                    ));
                }
                if locator.audience() != authority.remote_audience() {
                    return Err(invalid(
                        6,
                        "upload package authority differs from its stored locator".to_string(),
                    ));
                }
            }
            OutboxOperation::Upload {
                root_table: row.get(9)?,
                root_id: row.get(10)?,
                row: reference,
                source_path: PathBuf::from(source_path),
                retain_pinned: row.get(5)?,
                state,
            }
        }
        "delete" => {
            let encoded: String = row.get(3)?;
            let stored: StoredBlobRef =
                serde_json::from_str(&encoded).map_err(|error| invalid(3, error.to_string()))?;
            OutboxOperation::Delete { stored }
        }
        _ => {
            return Err(invalid(
                1,
                format!("invalid cloud outbox operation {tag:?}"),
            ))
        }
    };
    Ok(OutboxEntry {
        id: row.get(0)?,
        attempt_count: row.get(7)?,
        last_attempt_at: row.get(8)?,
        operation,
    })
}

use super::*;

pub struct CloudOutboxRecords<'connection> {
    connection: &'connection Connection,
}

impl<'connection> CloudOutboxRecords<'connection> {
    pub(crate) fn new(connection: &'connection Connection) -> Self {
        Self { connection }
    }

    fn upload_entry_for_identity(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<Option<OutboxEntry>, DbError> {
        self.connection
            .query_row(
                &format!(
                    "SELECT {OUTBOX_ENTRY_COLUMNS} FROM cloud_outbox
                     WHERE table_name = ?1 AND row_id = ?2
                       AND column_name = ?3 AND row_stamp = ?4"
                ),
                rusqlite::params![table, row_id, column, row_stamp],
                row_to_outbox_entry,
            )
            .optional()
            .map_err(DbError::from)
    }

    pub fn consume_created_upload_handoff(
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
        let OutboxUpload { row, state, .. } = &entry.upload;
        let OutboxUploadState::Created {
            authority, stored, ..
        } = state
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

    pub fn created_upload_handoff(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<Option<StoreWriteRemoteBlob>, DbError> {
        let Some(entry) = self.upload_entry_for_identity(table, row_id, column, row_stamp)? else {
            return Ok(None);
        };
        let OutboxUpload { row, state, .. } = entry.upload;
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

    pub fn upload_entries_for_rows(
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

    /// How many upload journals this root holds, counted on the queue rather
    /// than through its current rows. A journal whose row version is gone is
    /// invisible to [`upload_entries_for_root`] but still owes the cloud an
    /// object, so the transition finalizer compares both.
    pub(crate) fn upload_entry_count_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<usize, DbError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM cloud_outbox
                 WHERE root_table = ?1 AND root_id = ?2",
                (root_table, root_id),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        usize::try_from(count).map_err(|_| {
            DbError::Message(format!(
                "queued upload count {count} for {root_table:?}/{root_id:?} is out of range"
            ))
        })
    }

    pub fn upload_entries_for_root(
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

    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_upload(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
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
            .map_err(|error| DbError::context("serialize row blob ref", error))?;
        let pending = serde_json::to_string(&OutboxUploadState::Pending)
            .map_err(|error| DbError::context("serialize pending blob upload state", error))?;
        self.connection
            .execute(
                "INSERT INTO cloud_outbox
                 (table_name, row_id, column_name, row_stamp, root_table, root_id,
                  root_label, row_ref, upload_state, source_path, retain_pinned, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(table_name, row_id, column_name, row_stamp) DO UPDATE SET
                   root_table = excluded.root_table,
                   root_id = excluded.root_id,
                   root_label = excluded.root_label,
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
                    root_label,
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

    pub fn remove_entry(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        let identity = outbox_identity(&entry.upload);
        let removed = self
            .connection
            .execute(
                "DELETE FROM cloud_outbox WHERE id = ?1
                 AND table_name = ?2 AND row_id = ?3 AND column_name = ?4 AND row_stamp = ?5",
                rusqlite::params![
                    entry.id,
                    identity.table,
                    identity.row_id,
                    identity.column,
                    identity.row_stamp
                ],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(
                "cloud outbox entry changed before exact dequeue".to_string(),
            ));
        }
        Ok(())
    }

    pub fn finish_cancelled_upload(&self, entry: &OutboxEntry) -> Result<bool, DbError> {
        let OutboxUpload {
            root_table,
            root_id,
            ..
        } = &entry.upload;
        if !matches!(
            Database::make_remote_intent_state(self.connection, root_table, root_id)?,
            Some(MakeRemoteIntentState::Cancelling)
        ) {
            return Err(DbError::Message(format!(
                "make_remote cleanup for {root_table:?}/{root_id:?} lost cancellation ownership"
            )));
        }
        self.remove_entry(entry)?;
        if self.upload_entry_count_for_root(root_table, root_id)? != 0 {
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

/// The exact row version one queue row uploads, which is what every update to
/// that row compares against before it changes anything.
pub struct OutboxIdentity {
    pub table: String,
    pub row_id: String,
    pub column: String,
    pub row_stamp: String,
}

pub fn outbox_identity(upload: &OutboxUpload) -> OutboxIdentity {
    OutboxIdentity {
        table: upload.row.table().to_string(),
        row_id: upload.row.row_id().to_string(),
        column: upload.row.column().to_string(),
        row_stamp: upload.row.row_stamp().to_string(),
    }
}

/// The queue row's columns in the order [`row_to_outbox_entry`] reads them.
pub(crate) const OUTBOX_ENTRY_COLUMNS: &str =
    "id, row_ref, source_path, retain_pinned, upload_state,
     attempt_count, last_attempt_at, root_table, root_id";

pub fn row_to_outbox_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    fn invalid(
        index: usize,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(source),
        )
    }

    let encoded: String = row.get(1)?;
    let reference: RowBlobRef =
        serde_json::from_str(&encoded).map_err(|error| invalid(1, error))?;
    let source_path: String = row.get(2)?;
    let state_json: String = row.get(4)?;
    let state: OutboxUploadState =
        serde_json::from_str(&state_json).map_err(|error| invalid(4, error))?;
    if let OutboxUploadState::Prepared {
        authority, stored, ..
    }
    | OutboxUploadState::Created {
        authority, stored, ..
    } = &state
    {
        let locator = stored.locator();
        if !coven_protocol::blob::locator_describes_row(
            locator,
            reference.blob(),
            reference.plaintext_size(),
            reference.plaintext_hash(),
        ) {
            return Err(invalid(
                4,
                std::io::Error::other("prepared upload differs from its exact row version"),
            ));
        }
        if locator.audience() != authority.remote_audience() {
            return Err(invalid(
                4,
                std::io::Error::other("upload package authority differs from its stored locator"),
            ));
        }
    }
    Ok(OutboxEntry {
        id: row.get(0)?,
        attempt_count: row.get(5)?,
        last_attempt_at: row.get(6)?,
        upload: OutboxUpload {
            root_table: row.get(7)?,
            root_id: row.get(8)?,
            row: reference,
            source_path: PathBuf::from(source_path),
            retain_pinned: row.get(3)?,
            state,
        },
    })
}

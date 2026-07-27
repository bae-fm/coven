use super::*;

pub(super) enum OutboxIdentity {
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

pub(super) fn upload_entry_for_identity_on(
    conn: &Connection,
    table: &str,
    row_id: &str,
    column: &str,
    row_stamp: &str,
) -> Result<Option<OutboxEntry>, DbError> {
    conn.query_row(
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

pub(crate) fn consume_created_upload_handoff_on(
    conn: &Connection,
    package: &AudiencePackage,
    binding: &RowBlobLocatorBinding,
) -> Result<bool, DbError> {
    let Some(entry) = upload_entry_for_identity_on(
        conn,
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
    Database::remove_cloud_outbox_entry_on(conn, &entry)?;
    Ok(true)
}

pub(super) fn created_upload_handoff_on(
    conn: &Connection,
    table: &str,
    row_id: &str,
    column: &str,
    row_stamp: &str,
) -> Result<Option<StoreWriteRemoteBlob>, DbError> {
    let Some(entry) = upload_entry_for_identity_on(conn, table, row_id, column, row_stamp)? else {
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

pub(super) fn outbox_identity(operation: &OutboxOperation) -> Result<OutboxIdentity, DbError> {
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

pub(super) fn row_to_outbox_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
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

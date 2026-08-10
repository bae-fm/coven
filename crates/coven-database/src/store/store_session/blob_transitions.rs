use super::*;

use crate::{CloudOutboxRecords, MakeRemoteIntentState};
use crate::{OutboxEntry, OutboxOperation, OutboxUploadState};
use coven_protocol::blob::RowBlobRef;

pub enum PostUpload {
    Waiting,
    Cancelled,
    MadeRemote { root_table: String, root_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobTransitionRoot {
    Gated,
    RemoteRoot,
    NotGated,
}

#[derive(Clone)]
pub struct MaterializedLocalBlob {
    pub remote: RowBlobRef,
    pub stored: coven_protocol::blob::locator::StoredBlobRef,
    pub destination: Option<std::path::PathBuf>,
}

impl StoreSession<'_> {
    fn gated_root_gate_column(&self, root_table: &str) -> Result<&str, DbError> {
        self.synced_tables
            .iter()
            .find(|table| table.name() == root_table)
            .and_then(|table| table.gate_column())
            .ok_or_else(|| {
                DbError::Message(format!(
                    "blob locality transition root {root_table:?} has no boolean gate column"
                ))
            })
    }

    fn gated_root_locality(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<bool>, DbError> {
        let gate_column = self.gated_root_gate_column(root_table)?;
        crate::query_truth(self.conn, root_table, gate_column, root_id)
            .map_err(|error| DbError::Message(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
        created_at: &str,
        uploads: &[(RowBlobRef, std::path::PathBuf)],
    ) -> Result<Option<bool>, DbError> {
        let gate_column = self.gated_root_gate_column(root_table)?;
        let transaction = self.conn.unchecked_transaction()?;
        let locality = crate::query_truth(&transaction, root_table, gate_column, root_id)
            .map_err(|error| DbError::Message(error.to_string()))?;
        if locality == Some(false) {
            let current = Database::row_blob_refs_for_root_on(
                &transaction,
                self.gates,
                self.synced_tables,
                root_table,
                root_id,
            )?;
            if current.len() != uploads.len()
                || current
                    .iter()
                    .zip(uploads)
                    .any(|(current, (verified, _))| current != verified)
            {
                return Err(DbError::Message(format!(
                    "blob rows below {root_table:?}/{root_id:?} changed while make_remote verified their sources"
                )));
            }
            Database::insert_make_remote_intent_on(&transaction, root_table, root_id, pin)?;
            let cloud_outbox = CloudOutboxRecords::new(&transaction);
            for (reference, source_path) in uploads {
                cloud_outbox.enqueue_upload(
                    root_table,
                    root_id,
                    reference,
                    source_path,
                    pin,
                    created_at,
                )?;
            }
            transaction.commit().map_err(DbError::from)?;
        }
        Ok(locality)
    }

    fn finalize_created_blob_upload(
        &mut self,
        entry: OutboxEntry,
        root_table: String,
        root_id: String,
        row: RowBlobRef,
        stamp: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<PostUpload, DbError> {
        let connection = self.conn;
        let resolved_root = self
            .gates
            .resolve_root_of(connection, row.table(), row.row_id())
            .map_err(|error| DbError::Message(error.to_string()))?
            .ok_or_else(|| {
                DbError::Message(format!(
                    "upload row {:?}/{:?} has no gated transition root",
                    row.table(),
                    row.row_id()
                ))
            })?;
        if resolved_root != (root_table.clone(), root_id.clone()) {
            return Err(DbError::Message(format!(
                "upload row {:?}/{:?} moved from make_remote root {:?}/{:?} to {:?}/{:?}",
                row.table(),
                row.row_id(),
                root_table,
                root_id,
                resolved_root.0,
                resolved_root.1
            )));
        }
        match Database::make_remote_intent_state(connection, &root_table, &root_id)? {
            Some(MakeRemoteIntentState::Uploading) => {}
            Some(MakeRemoteIntentState::Publishing(_)) => return Ok(PostUpload::Waiting),
            Some(MakeRemoteIntentState::Cancelling) => return Ok(PostUpload::Cancelled),
            None => {
                return Err(DbError::Message(format!(
                    "upload for {root_table:?}/{root_id:?} has no make_remote intent"
                )));
            }
        }

        let rows = Database::row_blob_refs_for_root_on(
            connection,
            self.gates,
            self.synced_tables,
            &root_table,
            &root_id,
        )?;
        let entries = CloudOutboxRecords::new(connection).upload_entries_for_root(
            self.gates,
            self.synced_tables,
            &root_table,
            &root_id,
        )?;
        if rows.len() != entries.len() {
            return Err(DbError::Message(format!(
                "make_remote root {root_table:?}/{root_id:?} has {} blob rows but {} exact upload journals",
                rows.len(),
                entries.len()
            )));
        }
        if !entries.iter().all(|candidate| {
            matches!(
                candidate.operation,
                OutboxOperation::Upload {
                    state: OutboxUploadState::Created { .. },
                    ..
                }
            )
        }) {
            return Ok(PostUpload::Waiting);
        }
        if !entries.iter().any(|candidate| candidate == &entry) {
            return Err(DbError::Message(
                "Created upload changed before make_remote finalization".to_string(),
            ));
        }

        let gate_column = self
            .synced_tables
            .iter()
            .find(|table| table.name() == root_table)
            .and_then(|table| table.gate_column())
            .ok_or_else(|| {
                DbError::Message(format!(
                    "make_remote root {root_table:?} has no boolean gate column"
                ))
            })?;
        super::host_write_capture::CapturedStoreWriteTransaction::begin_prepared_blob_transition(
            connection,
            self.store_dir,
            self.synced_tables,
            self.gates,
            self.blob_decls,
            routing_encryption,
            self.verified_store_authority,
            write_id.clone(),
        )?
        .execute_make_remote(
            root_table.clone(),
            root_id.clone(),
            gate_column.to_string(),
            stamp.to_string(),
            rows,
            write_id,
        )?;
        Ok(PostUpload::MadeRemote {
            root_table,
            root_id,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_make_local(
        &mut self,
        root_table: &str,
        root_id: &str,
        stamp: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        materialized: &[MaterializedLocalBlob],
        write_id: coven_protocol::write::WriteId,
    ) -> Result<(), DbError> {
        let gate_column = self.gated_root_gate_column(root_table)?.to_string();
        super::host_write_capture::CapturedStoreWriteTransaction::begin_prepared_blob_transition(
            self.conn,
            self.store_dir,
            self.synced_tables,
            self.gates,
            self.blob_decls,
            routing_encryption,
            self.verified_store_authority,
            write_id,
        )?
        .execute_make_local(
            root_table.to_string(),
            root_id.to_string(),
            gate_column,
            stamp.to_string(),
            materialized.to_vec(),
        )
        .map(|receipt| receipt.value)
    }

    fn cancel_make_remote(&self, root_table: &str, root_id: &str) -> Result<(), DbError> {
        self.gated_root_gate_column(root_table)?;
        let transaction = self.conn.unchecked_transaction()?;
        match Database::make_remote_intent_state(&transaction, root_table, root_id)? {
            Some(MakeRemoteIntentState::Uploading) => {
                let updated = transaction
                    .execute(
                        "UPDATE blob_make_remote_intents SET state = 'cancelling'
                         WHERE root_table = ?1 AND root_id = ?2 AND state = 'uploading'",
                        (root_table, root_id),
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "make_remote intent {root_table:?}/{root_id:?} cannot enter cancellation"
                    )));
                }
                transaction
                    .execute(
                        "UPDATE cloud_outbox
                         SET attempt_count = 0, last_error = NULL, last_attempt_at = NULL
                         WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2",
                        (root_table, root_id),
                    )
                    .map_err(DbError::from)?;
            }
            Some(MakeRemoteIntentState::Cancelling) => {}
            Some(MakeRemoteIntentState::Publishing(write_id)) => {
                return Err(DbError::Message(format!(
                    "make_remote for {root_table:?}/{root_id:?} is already publishing as {write_id}"
                )));
            }
            None => {
                return Err(DbError::Message(format!(
                    "make_remote for {root_table:?}/{root_id:?} does not exist"
                )));
            }
        }
        transaction.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn gated_root_locality(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<bool>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.call_store(move |session| session.gated_root_locality(&root_table, &root_id))
            .await
    }

    pub async fn begin_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
        created_at: String,
        uploads: Vec<(RowBlobRef, std::path::PathBuf)>,
    ) -> Result<Option<bool>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.call_store(move |session| {
            session.begin_make_remote(&root_table, &root_id, pin, &created_at, &uploads)
        })
        .await
    }

    pub async fn cancel_make_remote(&self, root_table: &str, root_id: &str) -> Result<(), DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.call_store(move |session| session.cancel_make_remote(&root_table, &root_id))
            .await
    }

    /// Complete a Created upload journal entry without exposing a Remote row that lacks
    /// exact object authority. Every blob-bearing row below the same gated root must
    /// still match its queued row version and have reached Created. The final transaction
    /// then flips the gate, clears external-file ownership, records the pending Store
    /// write, and binds the transition intent to that write together. The intent and
    /// Created handoffs remain until that Store write activates, so a crash cannot make
    /// the upload drain mistake a published object for an orphan.
    pub async fn finalize_created_blob_upload(
        &self,
        entry: &OutboxEntry,
        stamp: String,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    ) -> Result<PostUpload, DbError> {
        let OutboxOperation::Upload {
            root_table,
            root_id,
            row,
            state,
            ..
        } = &entry.operation
        else {
            return Err(DbError::Message(
                "make_remote finalizer received a non-upload outbox entry".to_string(),
            ));
        };
        if !matches!(state, OutboxUploadState::Created { .. }) {
            return Err(DbError::Message(
                "make_remote finalizer requires a Created exact upload".to_string(),
            ));
        }

        let entry = entry.clone();
        let root_table = root_table.clone();
        let root_id = root_id.clone();
        let row = row.clone();
        let write_id = self.new_store_write_id();
        self.call_store(move |session| {
            session.finalize_created_blob_upload(
                entry,
                root_table,
                root_id,
                row,
                &stamp,
                routing_encryption.as_ref(),
                write_id,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        stamp: String,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        materialized: Vec<MaterializedLocalBlob>,
    ) -> Result<(), DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let write_id = self.new_store_write_id();
        self.call_store(move |session| {
            session.commit_make_local(
                &root_table,
                &root_id,
                &stamp,
                routing_encryption.as_ref(),
                &materialized,
                write_id,
            )
        })
        .await
    }
}

/// Set a gated root's locality and stamp the row inside the caller's prepared
/// blob-transition transaction.
pub(super) fn write_gate(
    transaction: &rusqlite::Transaction<'_>,
    root_table: &str,
    gate_column: &str,
    remote: bool,
    stamp: &str,
    root_id: &str,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        &format!(
            "UPDATE {} SET {} = ?1, _updated_at = ?2 WHERE id = ?3",
            crate::quote_ident(root_table),
            crate::quote_ident(gate_column),
        ),
        (remote as i64, stamp, root_id),
    )?;
    Ok(())
}

pub(super) fn same_row_blob_version(left: &RowBlobRef, right: &RowBlobRef) -> bool {
    left.table() == right.table()
        && left.row_id() == right.row_id()
        && left.row_stamp() == right.row_stamp()
        && left.column() == right.column()
        && left.blob() == right.blob()
        && left.plaintext_size() == right.plaintext_size()
        && left.plaintext_hash() == right.plaintext_hash()
}

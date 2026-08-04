use super::*;

use crate::blob::transition::PostUpload;
use crate::database::{CloudOutboxRecords, ExternalBlobRecords, MakeRemoteIntentState};
use crate::database::{OutboxEntry, OutboxOperation, OutboxUploadState};
use crate::protocol::blob::{Provenance, RowBlobRef};

#[derive(Clone)]
pub(crate) struct MaterializedLocalBlob {
    pub(crate) remote: RowBlobRef,
    pub(crate) stored: crate::protocol::blob::locator::StoredBlobRef,
    pub(crate) destination: Option<std::path::PathBuf>,
}

impl StoreDatabase {
    pub(crate) async fn gated_root_locality(
        &self,
        root_table: &str,
        gate_column: &str,
        root_id: &str,
    ) -> Result<Option<bool>, DbError> {
        let root_table = root_table.to_string();
        let gate_column = gate_column.to_string();
        let root_id = root_id.to_string();
        self.connection
            .call(move |connection| {
                crate::database::query_truth(connection, &root_table, &gate_column, &root_id)
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .await
    }

    pub(crate) async fn begin_make_remote(
        &self,
        root_table: &str,
        gate_column: &str,
        root_id: &str,
        pin: bool,
        created_at: String,
        uploads: Vec<(RowBlobRef, std::path::PathBuf)>,
    ) -> Result<Option<bool>, DbError> {
        let root_table = root_table.to_string();
        let gate_column = gate_column.to_string();
        let root_id = root_id.to_string();
        let gates = self.gates();
        let tables = self.synced_tables().to_vec();
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction()?;
                let locality = crate::database::query_truth(
                    &transaction,
                    &root_table,
                    &gate_column,
                    &root_id,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if locality == Some(false) {
                    let current = Database::row_blob_refs_for_root_on(
                        &transaction,
                        &gates,
                        &tables,
                        &root_table,
                        &root_id,
                    )?;
                    if current.len() != uploads.len()
                        || current
                            .iter()
                            .zip(&uploads)
                            .any(|(current, (verified, _))| current != verified)
                    {
                        return Err(DbError::Message(format!(
                            "blob rows below {root_table:?}/{root_id:?} changed while make_remote verified their sources"
                        )));
                    }
                    Database::insert_make_remote_intent_on(
                        &transaction,
                        &root_table,
                        &root_id,
                        pin,
                    )?;
                    let cloud_outbox = CloudOutboxRecords::new(&transaction);
                    for (reference, source_path) in &uploads {
                        cloud_outbox.enqueue_upload(
                            &root_table,
                            &root_id,
                            reference,
                            source_path,
                            pin,
                            &created_at,
                        )?;
                    }
                    transaction.commit().map_err(DbError::from)?;
                }
                Ok(locality)
            })
            .await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction()?;
                match Database::make_remote_intent_state(
                    &transaction,
                    &root_table,
                    &root_id,
                )? {
                    Some(MakeRemoteIntentState::Uploading) => {
                        let updated = transaction
                            .execute(
                                "UPDATE blob_make_remote_intents SET state = 'cancelling'
                                 WHERE root_table = ?1 AND root_id = ?2 AND state = 'uploading'",
                                (&root_table, &root_id),
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
                                (&root_table, &root_id),
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
            })
            .await
    }

    /// Complete a Created upload journal entry without exposing a Remote row that lacks
    /// exact object authority. Every blob-bearing row below the same gated root must
    /// still match its queued row version and have reached Created. The final transaction
    /// then flips the gate, clears external-file ownership, records the pending Store
    /// write, and binds the transition intent to that write together. The intent and
    /// Created handoffs remain until that Store write activates, so a crash cannot make
    /// the upload drain mistake a published object for an orphan.
    pub(crate) async fn finalize_created_blob_upload(
        &self,
        entry: &OutboxEntry,
        stamp: String,
        routing_encryption: Option<crate::encryption::EncryptionService>,
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
        let tables = self.synced_tables().to_vec();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let write_id = self.new_store_write_id();
        self.connection
            .call(move |connection| {
                let resolved_root = gates
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
                    Some(MakeRemoteIntentState::Publishing(_)) => {
                        return Ok(PostUpload::Waiting)
                    }
                    Some(MakeRemoteIntentState::Cancelling) => {
                        return Ok(PostUpload::Cancelled);
                    }
                    None => {
                        return Err(DbError::Message(format!(
                            "upload for {root_table:?}/{root_id:?} has no make_remote intent"
                        )));
                    }
                }

                let rows = Database::row_blob_refs_for_root_on(
                    connection,
                    &gates,
                    &tables,
                    &root_table,
                    &root_id,
                )?;
                let entries = CloudOutboxRecords::new(connection).upload_entries_for_root(
                    &gates,
                    &tables,
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

                let gate_column = tables
                    .iter()
                    .find(|table| table.name() == root_table)
                    .and_then(|table| table.gate_column())
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "make_remote root {root_table:?} has no boolean gate column"
                        ))
                    })?;
                let publication_write_id = write_id.clone();
                super::host_write_capture::CapturedStoreWriteTransaction::begin_prepared_blob_transition(
                    connection,
                    &tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    write_id,
                )?
                .execute(|transaction| {
                        write_gate(
                            transaction,
                            &root_table,
                            gate_column,
                            true,
                            &stamp,
                            &root_id,
                        )
                        .map_err(DbError::from)?;
                        let external_blobs = ExternalBlobRecords::new(transaction);
                        for reference in &rows {
                            if reference.blob().provenance == Provenance::UserProvided {
                                external_blobs.clear(reference)?;
                            }
                        }
                        Database::mark_make_remote_publishing_on(
                            transaction,
                            &root_table,
                            &root_id,
                            &publication_write_id,
                        )
                    })?;
                Ok(PostUpload::MadeRemote {
                    root_table,
                    root_id,
                })
            })
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        gate_column: &str,
        stamp: String,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        materialized: Vec<MaterializedLocalBlob>,
    ) -> Result<(), DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let gate_column = gate_column.to_string();
        let tables = self.synced_tables().to_vec();
        let write_id = self.new_store_write_id();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        self.connection
            .call(move |connection| {
                super::host_write_capture::CapturedStoreWriteTransaction::begin_prepared_blob_transition(
                    connection,
                    &tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    write_id,
                )?
                .execute(|transaction| {
                        let remote = Database::row_blob_refs_for_root_on(
                            transaction,
                            &gates,
                            &tables,
                            &root_table,
                            &root_id,
                        )?;
                        if remote.len() != materialized.len()
                            || remote.iter().zip(&materialized).any(|(current, local)| {
                                !same_row_blob_version(current, &local.remote)
                                    || current.authority() != local.remote.authority()
                                    || current.stored() != Some(&local.stored)
                            })
                        {
                            return Err(DbError::Message(format!(
                                "make_local root {root_table:?}/{root_id:?} changed while its blobs were materialized"
                            )));
                        }
                        write_gate(
                            transaction,
                            &root_table,
                            &gate_column,
                            false,
                            &stamp,
                            &root_id,
                        )
                        .map_err(DbError::from)?;

                        for local in &materialized {
                            let reference = &local.remote;
                            if reference.table() == root_table && reference.row_id() == root_id {
                                continue;
                            }
                            let sql = format!(
                                "UPDATE {} SET _updated_at = ?1 WHERE id = ?2 AND _updated_at = ?3",
                                crate::database::quote_ident(reference.table())
                            );
                            let updated = transaction
                                .execute(
                                    &sql,
                                    rusqlite::params![
                                        &stamp,
                                        reference.row_id(),
                                        reference.row_stamp()
                                    ],
                                )
                                .map_err(DbError::from)?;
                            if updated != 1 {
                                return Err(DbError::Message(format!(
                                    "make_local row {:?}/{:?} changed before restamping",
                                    reference.table(),
                                    reference.row_id()
                                )));
                            }
                        }
                        let local_rows = Database::row_blob_refs_for_root_on(
                            transaction,
                            &gates,
                            &tables,
                            &root_table,
                            &root_id,
                        )?;
                        if local_rows.len() != materialized.len() {
                            return Err(DbError::Message(format!(
                                "make_local root {root_table:?}/{root_id:?} changed while its blobs were materialized"
                            )));
                        }
                        let cloud_outbox = CloudOutboxRecords::new(transaction);
                        let external_blobs = ExternalBlobRecords::new(transaction);
                        for (local, materialized) in local_rows.iter().zip(&materialized) {
                            if local.table() != materialized.remote.table()
                                || local.row_id() != materialized.remote.row_id()
                                || local.column() != materialized.remote.column()
                                || local.row_stamp() != stamp
                                || local.blob() != materialized.remote.blob()
                                || local.plaintext_size()
                                    != materialized.remote.plaintext_size()
                                || local.plaintext_hash()
                                    != materialized.remote.plaintext_hash()
                                || local.authority() != &crate::protocol::blob::RowBlobAuthority::Local
                                || local.stored().is_some()
                            {
                                return Err(DbError::Message(format!(
                                    "make_local row {:?}/{:?}/{:?} changed while its blob was materialized",
                                    materialized.remote.table(),
                                    materialized.remote.row_id(),
                                    materialized.remote.column()
                                )));
                            }
                            if let Some(path) = &materialized.destination {
                                external_blobs.register(local, path)?;
                            }
                            cloud_outbox.enqueue_delete(&materialized.stored, &stamp)?;
                        }
                        Ok(())
                    })
                .map(|receipt| receipt.value)
            })
            .await
    }
}

/// Set a gated root's locality and stamp the row inside the caller's prepared
/// blob-transition transaction.
fn write_gate(
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
            crate::database::quote_ident(root_table),
            crate::database::quote_ident(gate_column),
        ),
        (remote as i64, stamp, root_id),
    )?;
    Ok(())
}

fn same_row_blob_version(left: &RowBlobRef, right: &RowBlobRef) -> bool {
    left.table() == right.table()
        && left.row_id() == right.row_id()
        && left.row_stamp() == right.row_stamp()
        && left.column() == right.column()
        && left.blob() == right.blob()
        && left.plaintext_size() == right.plaintext_size()
        && left.plaintext_hash() == right.plaintext_hash()
}

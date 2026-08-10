use std::collections::BTreeMap;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension};

use crate::BlobDecls;
use crate::PublicationBlob;
use crate::WriteId;
use crate::{
    audience_moves, capture_routing_changes, partition_outbound,
    validate_scoped_foreign_key_audiences, AudienceMove, AudiencePartition, Gates, RoutingChanges,
};
use crate::{capture_changeset, *};

use coven_protocol::blob::Provenance;

use coven_protocol::synced_schema::SyncedTable;

use coven_keys::encryption::EncryptionService;
use coven_protocol::write::WriteReceipt;

use super::*;

/// Rolls back the staged audience blob files after a failed capture,
/// folding any cleanup failure into the operation error it returns.
pub type StagedAudienceBlobRollback = Box<dyn FnOnce(DbError) -> DbError + Send>;

fn rollback_staged_audience_blobs(
    rollback: Option<StagedAudienceBlobRollback>,
    error: DbError,
) -> DbError {
    match rollback {
        Some(rollback) => rollback(error),
        None => error,
    }
}

/// Staging audience-move blobs into the spool needs the connected staging
/// owner replication composes. The capture transaction names only this port;
/// the returned rollback closure is consumed on failure.
pub trait AudienceBlobMoveStaging: Send + Sync {
    fn stage_audience_move_blobs_on(
        &self,
        transaction: &mut HostWriteBlobTransaction<'_, '_>,
        facts: &mut StoreWriteBlobFacts,
        moves: &[AudienceMove],
        partitions: &[AudiencePartition],
    ) -> Result<StagedAudienceBlobRollback, DbError>;
}

enum AudienceBlobMoveMaterialization<'a> {
    Host(&'a dyn AudienceBlobMoveStaging),
    PreparedTransition,
}

pub(crate) struct CapturedStoreWriteTransaction<'connection, 'operation> {
    transaction: rusqlite::Transaction<'connection>,
    /// Where this store's payload files are. Staging an audience blob move
    /// resolves the Circle authority behind it, whose records name payloads.
    store_dir: &'operation coven_foundation::store_dir::StoreDir,
    changes_before: u64,
    synced_tables: &'operation [SyncedTable],
    gates: &'operation Gates,
    blob_decls: &'operation BlobDecls,
    routing: StoreWriteRouting<'operation>,
    blob_materialization: Option<AudienceBlobMoveMaterialization<'operation>>,
    verified_authority: &'operation mut super::verified_store_authority::VerifiedStoreAuthority,
    write_id: WriteId,
}

pub struct HostWriteBlobTransaction<'transaction, 'connection> {
    store: crate::store::store_session::StoreTransaction<'transaction, 'connection>,
    verified_authority: &'transaction mut super::verified_store_authority::VerifiedStoreAuthority,
}

impl StoreSession<'_> {
    fn prepare_store_write(&self) -> Result<Option<PreparedStoreWrite>, DbError> {
        let stored = self
            .conn
            .query_row(
                "SELECT write_id, base, blob_facts FROM store_writes
                 WHERE status = '\"pending\"'
                   AND ordinal = (
                       SELECT MIN(ordinal) FROM store_writes
                       WHERE status != '\"local_only\"'
                         AND json_extract(status, '$.published') IS NULL
                         AND json_extract(status, '$.resolved') IS NULL
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM store_writes WHERE prepared IS NOT NULL
                   )
                 ORDER BY ordinal LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((write_id, base, blob_facts)) = stored else {
            return Ok(None);
        };
        let partitions = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .store_write_partitions(&write_id)?;
        Ok(Some(PreparedStoreWrite {
            write_id: WriteId::from_generated(write_id),
            partitions,
            base: serde_json::from_str(&base)
                .map_err(|error| DbError::context("pending write base", error))?,
            blob_facts: serde_json::from_str(&blob_facts)
                .map_err(|error| DbError::context("pending write blob facts", error))?,
        }))
    }
}

impl<'transaction, 'connection> HostWriteBlobTransaction<'transaction, 'connection> {
    fn new(
        store: crate::store::store_session::StoreTransaction<'transaction, 'connection>,
        verified_authority: &'transaction mut super::verified_store_authority::VerifiedStoreAuthority,
    ) -> Self {
        Self {
            store,
            verified_authority,
        }
    }

    pub fn local_activated_registration(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<coven_protocol::store_commit::ReferencedStoreDeviceRegistration, DbError> {
        let reference =
            local_activated_registration_ref_on(self.store.transaction)?.ok_or_else(|| {
                DbError::Message(
                    "audience blob move has no activated local Store registration".to_string(),
                )
            })?;
        let registration =
            self.store
                .activated_registration(self.verified_authority, root, &reference)?;
        coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            reference,
            registration,
        )
        .map_err(|error| DbError::Message(error.to_string()))
    }

    pub fn circle_publication_context(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::CircleEpochAccess, DbError> {
        super::circle_publication_context_on(self.store.transaction, circle_id, expected_control)
    }

    pub fn circle_blob_opening_protection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        self.store.circle_blob_opening_protection(
            self.verified_authority,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }

    pub fn external_local_path(
        &self,
        fact: &StoreWriteBlobFact,
    ) -> Result<Option<PathBuf>, DbError> {
        let stored = self
            .store
            .transaction
            .query_row(
                "SELECT path, plaintext_size, plaintext_hash
                 FROM local_blob_refs
                 WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                   AND namespace = ?4 AND blob_id = ?5
                 ORDER BY row_stamp DESC LIMIT 1",
                rusqlite::params![
                    fact.table,
                    fact.row_id,
                    fact.column,
                    fact.blob.namespace,
                    fact.blob.id,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((path, size, hash)) = stored else {
            return Ok(None);
        };
        let size = u64::try_from(size).map_err(|_| {
            DbError::Message("registered external blob has a negative size".to_string())
        })?;
        if size != fact.plaintext_size || hash != fact.plaintext_hash.to_string() {
            return Err(DbError::Message(
                "registered external blob identity differs from the moved row".to_string(),
            ));
        }
        Ok(Some(PathBuf::from(path)))
    }
}

impl StoreDatabase {
    fn drain_host_change_journal_on(
        session: &mut rusqlite::session::Session<'_>,
    ) -> Result<Vec<u8>, DbError> {
        capture_changeset(session)
    }

    /// Everything the attached journal has recorded so far. A session accumulates,
    /// so draining it again after more changes returns the whole write, merged —
    /// which is what lets a write react to what it captured and be captured again.
    fn drain_host_change_journal(
        session: &mut rusqlite::session::Session<'_>,
        synced_tables: &[SyncedTable],
    ) -> Result<Vec<u8>, DbError> {
        let captured = Self::drain_host_change_journal_on(session)?;
        crate::changeset_identity::validate_changeset_row_identities(&captured, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(captured)
    }

    pub fn invert_changeset(changeset: &[u8]) -> Result<Vec<u8>, DbError> {
        if changeset.is_empty() {
            return Ok(Vec::new());
        }
        let mut inverse = Vec::new();
        rusqlite::session::invert_strm(&mut &changeset[..], &mut inverse).map_err(DbError::from)?;
        Ok(inverse)
    }

    fn capture_store_write_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        changeset: &[u8],
        blob_decls: &BlobDecls,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let changes = crate::walk_changeset(changeset)
            .map_err(|error| DbError::Message(format!("read Store write blobs: {error}")))?;
        let mut facts = BTreeMap::new();
        for change in changes {
            let Some(publication) = blob_decls
                .publication_blob_from_change(tx, &change)
                .map_err(|error| DbError::context("capture Store write blob", error))?
            else {
                continue;
            };
            let fact = Self::capture_store_write_blob_fact_on(tx, publication)?;
            let key = fact.identity_key();
            if let Some(prior) = facts.insert(key.clone(), fact.clone()) {
                if prior != fact {
                    return Err(DbError::Message(format!(
                        "Store write gives row {}/{}/{} at {} conflicting blob facts",
                        key.0, key.1, key.2, key.3
                    )));
                }
            }
        }
        Ok(StoreWriteBlobFacts {
            blobs: facts.into_values().collect(),
        })
    }

    fn capture_store_write_blob_fact_on(
        tx: &rusqlite::Transaction<'_>,
        publication: PublicationBlob,
    ) -> Result<StoreWriteBlobFact, DbError> {
        let plaintext_hash = publication.plaintext_hash.parse().map_err(|error| {
            DbError::context(
                format!(
                    "capture Store write blob {}/{} plaintext hash",
                    publication.blob.namespace, publication.blob.id
                ),
                error,
            )
        })?;
        let external_path = if publication.blob.provenance == Provenance::UserProvided {
            tx.query_row(
                "SELECT path FROM local_blob_refs
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                       AND row_stamp = ?4 AND namespace = ?5 AND blob_id = ?6",
                rusqlite::params![
                    publication.table,
                    publication.row_id,
                    publication.column,
                    publication.row_stamp,
                    publication.blob.namespace,
                    publication.blob.id,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(PathBuf::from)
        } else {
            None
        };
        let previous = previous_row_blob_for_write_on(
            tx,
            &publication.table,
            &publication.row_id,
            &publication.row_stamp,
            &publication.column,
            &publication.blob,
            publication.plaintext_size,
            plaintext_hash,
        )?;
        Ok(StoreWriteBlobFact {
            table: publication.table,
            row_id: publication.row_id,
            row_stamp: publication.row_stamp,
            column: publication.column,
            blob: publication.blob,
            plaintext_size: publication.plaintext_size,
            plaintext_hash,
            external_path,
            previous,
            audience_move: None,
        })
    }

    fn capture_audience_move_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        moves: &[AudienceMove],
        blob_decls: &BlobDecls,
        captured: StoreWriteBlobFacts,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let mut facts = captured
            .blobs
            .into_iter()
            .map(|fact| (fact.identity_key(), fact))
            .collect::<BTreeMap<_, _>>();
        for audience_move in moves {
            for (table, row_id) in &audience_move.rows {
                let Some(publication) = blob_decls
                    .publication_blob_for_row(tx, table, row_id)
                    .map_err(|error| {
                        DbError::context(
                            format!("capture audience-move blob {table}/{row_id}"),
                            error,
                        )
                    })?
                else {
                    continue;
                };
                let fact = Self::capture_store_write_blob_fact_on(tx, publication)?;
                let key = fact.identity_key();
                if let Some(prior) = facts.get(&key) {
                    if prior != &fact {
                        return Err(DbError::Message(format!(
                            "audience move gives row {}/{}/{} at {} conflicting blob facts",
                            key.0, key.1, key.2, key.3
                        )));
                    }
                } else {
                    facts.insert(key, fact);
                }
            }
        }
        Ok(StoreWriteBlobFacts {
            blobs: facts.into_values().collect(),
        })
    }

    /// Give every blob-bearing row an audience move drags along the move's stamp.
    ///
    /// The move re-seals those blobs under the destination audience, which mints a
    /// new locator for content the row still binds at its old `_updated_at` — and a
    /// row stamp binds one exact locator, so publishing would refuse the second
    /// binding at that stamp. The stamp is the row's version, the re-seal is a new
    /// version of it, and the move already stamps the component's routing at this
    /// value: carrying the same stamp onto the rows makes the binding a fresh one by
    /// construction. Rows the caller already stamped past the move are left alone —
    /// their bindings are new stamps already, and lowering them would reorder the
    /// caller's own writes.
    fn advance_moved_blob_row_stamps_on(
        tx: &rusqlite::Transaction<'_>,
        moves: &[AudienceMove],
        blob_decls: &BlobDecls,
    ) -> Result<bool, DbError> {
        let mut advanced = false;
        for audience_move in moves {
            for (table, row_id) in &audience_move.rows {
                let carries_blob = blob_decls
                    .publication_blob_for_row(tx, table, row_id)
                    .map_err(|error| {
                        DbError::context(
                            format!("read audience-move blob row {table}/{row_id}"),
                            error,
                        )
                    })?
                    .is_some();
                if !carries_blob {
                    continue;
                }
                let sql = format!(
                    "UPDATE {} SET {} = ?1 WHERE {} = ?2 AND {} < ?1",
                    crate::quote_ident(table),
                    crate::quote_ident("_updated_at"),
                    crate::quote_ident("id"),
                    crate::quote_ident("_updated_at"),
                );
                let updated = tx
                    .execute(&sql, rusqlite::params![audience_move.stamp, row_id])
                    .map_err(DbError::from)?;
                advanced |= updated > 0;
            }
        }
        Ok(advanced)
    }

    fn store_write_routing<'a>(
        has_scoped_graph: bool,
        routing_encryption: Option<&'a EncryptionService>,
    ) -> Result<StoreWriteRouting<'a>, DbError> {
        if !has_scoped_graph {
            return Ok(StoreWriteRouting::Unscoped);
        }
        routing_encryption
            .map(StoreWriteRouting::MergeScoped)
            .ok_or_else(|| {
                DbError::Message(
                    "scoped write requires the Store generation-1 routing key".to_string(),
                )
            })
    }

    pub fn validate_store_write_routing(
        &self,
        routing_encryption: Option<&EncryptionService>,
    ) -> Result<(), DbError> {
        Self::store_write_routing(self.has_scoped_graph(), routing_encryption).map(drop)
    }

    pub async fn prepare_store_write(&self) -> Result<Option<PreparedStoreWrite>, DbError> {
        self.call_store(|session| session.prepare_store_write())
            .await
    }
}

pub(crate) fn capture_partition_blob_facts_on(
    tx: &rusqlite::Transaction<'_>,
    partitions: &[AudiencePartition],
    blob_decls: &BlobDecls,
) -> Result<StoreWriteBlobFacts, DbError> {
    let mut facts = BTreeMap::new();
    for partition in partitions {
        if partition.audience == coven_protocol::circle::Audience::Local {
            continue;
        }
        for fact in
            StoreDatabase::capture_store_write_blob_facts_on(tx, &partition.changeset, blob_decls)?
                .blobs
        {
            let key = fact.identity_key();
            if let Some(prior) = facts.insert(key.clone(), fact.clone()) {
                if prior != fact {
                    return Err(DbError::Message(format!(
                        "audience partitions give row {}/{}/{} at {} conflicting blob facts",
                        key.0, key.1, key.2, key.3
                    )));
                }
            }
        }
    }
    Ok(StoreWriteBlobFacts {
        blobs: facts.into_values().collect(),
    })
}

impl<'connection, 'operation> CapturedStoreWriteTransaction<'connection, 'operation> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn begin_host(
        connection: &'connection Connection,
        store_dir: &'operation coven_foundation::store_dir::StoreDir,
        synced_tables: &'operation [SyncedTable],
        gates: &'operation Gates,
        blob_decls: &'operation BlobDecls,
        routing_encryption: Option<&'operation EncryptionService>,
        blob_staging: Option<&'operation dyn AudienceBlobMoveStaging>,
        verified_authority: &'operation mut super::verified_store_authority::VerifiedStoreAuthority,
        write_id: WriteId,
    ) -> Result<Self, DbError> {
        Self::begin(
            connection,
            store_dir,
            synced_tables,
            gates,
            blob_decls,
            routing_encryption,
            blob_staging.map(AudienceBlobMoveMaterialization::Host),
            verified_authority,
            write_id,
        )
    }

    pub(crate) fn begin_prepared_blob_transition(
        connection: &'connection Connection,
        store_dir: &'operation coven_foundation::store_dir::StoreDir,
        synced_tables: &'operation [SyncedTable],
        gates: &'operation Gates,
        blob_decls: &'operation BlobDecls,
        routing_encryption: Option<&'operation EncryptionService>,
        verified_authority: &'operation mut super::verified_store_authority::VerifiedStoreAuthority,
        write_id: WriteId,
    ) -> Result<Self, DbError> {
        Self::begin(
            connection,
            store_dir,
            synced_tables,
            gates,
            blob_decls,
            routing_encryption,
            Some(AudienceBlobMoveMaterialization::PreparedTransition),
            verified_authority,
            write_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn begin(
        connection: &'connection Connection,
        store_dir: &'operation coven_foundation::store_dir::StoreDir,
        synced_tables: &'operation [SyncedTable],
        gates: &'operation Gates,
        blob_decls: &'operation BlobDecls,
        routing_encryption: Option<&'operation EncryptionService>,
        blob_materialization: Option<AudienceBlobMoveMaterialization<'operation>>,
        verified_authority: &'operation mut super::verified_store_authority::VerifiedStoreAuthority,
        write_id: WriteId,
    ) -> Result<Self, DbError> {
        let routing =
            StoreDatabase::store_write_routing(gates.has_scoped_graph(), routing_encryption)?;
        let changes_before = connection.total_changes();
        let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
        Ok(Self {
            transaction,
            store_dir,
            changes_before,
            synced_tables,
            gates,
            blob_decls,
            routing,
            blob_materialization,
            verified_authority,
            write_id,
        })
    }

    pub(crate) fn execute_host<R, E>(
        self,
        mut staged: super::host_write_operation::StagedBlobBatch,
        deleted: Vec<coven_protocol::blob::BlobRef>,
        sql: super::host_write_operation::HostSql<R, E>,
        stamper: coven_protocol::hlc::UpdatedAtStamper,
    ) -> Result<WriteReceipt<R>, super::host_write_operation::HostWriteError<E>> {
        use super::host_sql_transaction::HostSqlAuthorization;
        use super::host_write_operation::HostWriteError;

        let blob_decls = self.blob_decls;
        let store_dir = self.store_dir;
        let synced_tables = self.synced_tables;
        let gates = self.gates;
        let result = self.execute(|transaction| -> Result<R, HostWriteError<E>> {
            let cleanup_intents = deleted
                .iter()
                .map(|blob| {
                    blob_decls
                        .row_for_blob_in_namespace(transaction, &blob.namespace, &blob.id)
                        .map_err(|error| HostWriteError::Blob(error.to_string()))
                        .map(|row| match row {
                            Some((table, row_id)) => {
                                crate::local_blob_cleanup_intents::LocalBlobCleanupIntent::for_row(
                                    &blob.namespace,
                                    &blob.id,
                                    table,
                                    row_id,
                                )
                            }
                            None => {
                                crate::local_blob_cleanup_intents::LocalBlobCleanupIntent::local(
                                    &blob.namespace,
                                    &blob.id,
                                )
                            }
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            staged.publish(|namespace, id| {
                match blob_decls.row_for_blob_in_namespace(transaction, namespace, id) {
                    Ok(Some(_)) => {
                        return Err(HostWriteError::BlobAlreadyReferenced {
                            namespace: namespace.to_string(),
                            id: id.to_string(),
                        });
                    }
                    Ok(None) => {}
                    Err(error) => return Err(HostWriteError::Blob(error.to_string())),
                }
                let leased = transaction
                    .query_row(
                        "SELECT EXISTS(\
                             SELECT 1 FROM store_write_blob_leases \
                             WHERE namespace = ?1 AND blob_id = ?2\
                         )",
                        (namespace, id),
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(DbError::from)?;
                if leased {
                    return Err(HostWriteError::BlobOwnedByPendingWrite {
                        namespace: namespace.to_string(),
                        id: id.to_string(),
                    });
                }
                Ok(())
            })?;

            let host_sql = HostSqlAuthorization::begin(transaction)?;
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                host_sql.run(|| {
                    sql(super::SqlContext::new(
                        transaction,
                        stamper,
                        synced_tables,
                        gates,
                    ))
                })
            })) {
                Ok(Ok(value)) => {
                    for (blob, intent) in deleted.iter().zip(&cleanup_intents) {
                        let _ = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
                        if blob_decls
                            .blob_id_is_referenced(transaction, &blob.namespace, &blob.id)
                            .map_err(|error| DbError::Message(error.to_string()))?
                        {
                            return Err(HostWriteError::BlobStillReferenced {
                                namespace: blob.namespace.clone(),
                                id: blob.id.clone(),
                            });
                        }
                        super::local_blob_cleanup::record_obsolete_copy_intents_on(
                            transaction,
                            blob_decls,
                            intent,
                        )?;
                    }
                    Ok(value)
                }
                Ok(Err(error)) => Err(HostWriteError::Host(error)),
                Err(_) => Err(HostWriteError::WriteClosurePanicked),
            }
        });

        match result {
            Ok(receipt) => {
                staged.commit();
                Ok(receipt)
            }
            Err(error) => Err(staged.rollback(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_make_remote(
        self,
        root_table: String,
        root_id: String,
        gate_column: String,
        stamp: String,
        rows: Vec<coven_protocol::blob::RowBlobRef>,
        publication_write_id: WriteId,
    ) -> Result<WriteReceipt<()>, DbError> {
        self.execute(|transaction| {
            super::blob_transitions::write_gate(
                transaction,
                &root_table,
                &gate_column,
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
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_make_local(
        self,
        root_table: String,
        root_id: String,
        gate_column: String,
        stamp: String,
        materialized: Vec<super::blob_transitions::MaterializedLocalBlob>,
    ) -> Result<WriteReceipt<()>, DbError> {
        let gates = self.gates;
        let synced_tables = self.synced_tables;
        self.execute(|transaction| {
            let remote = Database::row_blob_refs_for_root_on(
                transaction,
                gates,
                synced_tables,
                &root_table,
                &root_id,
            )?;
            if remote.len() != materialized.len()
                || remote.iter().zip(&materialized).any(|(current, local)| {
                    !super::blob_transitions::same_row_blob_version(current, &local.remote)
                        || current.authority() != local.remote.authority()
                        || current.stored() != Some(&local.stored)
                })
            {
                return Err(DbError::Message(format!(
                    "make_local root {root_table:?}/{root_id:?} changed while its blobs were materialized"
                )));
            }
            super::blob_transitions::write_gate(
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
                    crate::quote_ident(reference.table())
                );
                let updated = transaction
                    .execute(
                        &sql,
                        rusqlite::params![stamp, reference.row_id(), reference.row_stamp()],
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
                gates,
                synced_tables,
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
                    || local.plaintext_size() != materialized.remote.plaintext_size()
                    || local.plaintext_hash() != materialized.remote.plaintext_hash()
                    || local.authority() != &coven_protocol::blob::RowBlobAuthority::Local
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
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn execute_test<R>(
        self,
        operation: impl FnOnce(crate::DatabaseTestTransaction<'_, '_>) -> Result<R, DbError>,
    ) -> Result<WriteReceipt<R>, DbError> {
        self.execute(|transaction| operation(crate::DatabaseTestTransaction::new(transaction)))
    }

    fn execute<R, E>(
        self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<WriteReceipt<R>, E>
    where
        E: From<DbError>,
    {
        let Self {
            transaction: tx,
            store_dir,
            changes_before,
            synced_tables,
            gates,
            blob_decls,
            routing,
            blob_materialization,
            verified_authority,
            write_id,
        } = self;
        (|| {
            let mut journal = rusqlite::session::Session::new(&tx)
                .map_err(|error| DbError::context("failed to create capture session", error))
                .map_err(E::from)?;
            for table in synced_tables {
                journal
                    .attach(Some(table.name()))
                    .map_err(|error| {
                        DbError::context(
                            format!("failed to attach synced table {} to session", table.name()),
                            error,
                        )
                    })
                    .map_err(E::from)?;
            }
            if gates.has_scoped_graph() {
                for table in ["_coven_audience", "_coven_row_routes"] {
                    journal
                        .attach(Some(table))
                        .map_err(DbError::from)
                        .map_err(E::from)?;
                }
            }
            let value = f(&tx)?;
            let mut captured =
                StoreDatabase::drain_host_change_journal(&mut journal, synced_tables)
                    .map_err(E::from)?;
            validate_scoped_foreign_key_audiences(&tx, gates)
                .map_err(|error| DbError::Message(error.to_string()))
                .map_err(E::from)?;
            // A move whose blobs get re-sealed owes those rows new stamps, and the
            // rows have to reach the destination audience carrying them — so the
            // moves are read first, the stamps land as more changes to this same
            // transaction, and the journal that now holds all of it is what gets
            // partitioned. Only the re-sealing materialization owes them: a blob
            // locality transition moves its own bindings across the two phases it
            // already runs, and restamping under it would rewrite rows it is
            // mid-flight over.
            if matches!(
                blob_materialization,
                Some(AudienceBlobMoveMaterialization::Host(_))
            ) {
                let moves = audience_moves(&tx, &captured, gates)
                    .map_err(|error| DbError::Message(error.to_string()))
                    .map_err(E::from)?;
                if StoreDatabase::advance_moved_blob_row_stamps_on(&tx, &moves, blob_decls)
                    .map_err(E::from)?
                {
                    captured =
                        StoreDatabase::drain_host_change_journal(&mut journal, synced_tables)
                            .map_err(E::from)?;
                }
            }
            let partitioned = match routing {
                StoreWriteRouting::MergeScoped(encryption) => {
                    let store_root_hash =
                        crate::store::store_session::StoreTransaction::new(&tx, store_dir)
                            .required_root_authority(verified_authority)
                            .map_err(E::from)?
                            .store_root_hash;
                    let key =
                        coven_protocol::circle::derive_row_routing_key(encryption, store_root_hash)
                            .map_err(|error| {
                                E::from(DbError::context("derive row routing key", error))
                            })?;
                    let routing_changeset = capture_routing_changes(&tx, &captured, gates, &key)
                        .map_err(|error| {
                            E::from(DbError::context("capture scoped routing changes", error))
                        })?;
                    partition_outbound(&tx, &captured, &routing_changeset, gates).map_err(
                        |error| {
                            E::from(DbError::context("partition scoped host transaction", error))
                        },
                    )?
                }
                StoreWriteRouting::Unscoped => {
                    partition_outbound(&tx, &captured, &RoutingChanges::empty(), gates).map_err(
                        |error| {
                            E::from(DbError::context("partition gated host transaction", error))
                        },
                    )?
                }
            };
            let mut blob_facts =
                capture_partition_blob_facts_on(&tx, &partitioned.partitions, blob_decls)
                    .map_err(E::from)?;
            blob_facts = StoreDatabase::capture_audience_move_blob_facts_on(
                &tx,
                &partitioned.moves,
                blob_decls,
                blob_facts,
            )
            .map_err(E::from)?;
            let moved_blob_exists = blob_facts.blobs.iter().any(|fact| {
                partitioned.moves.iter().any(|audience_move| {
                    audience_move
                        .rows
                        .contains(&(fact.table.clone(), fact.row_id.clone()))
                })
            });
            let staged_files = match (moved_blob_exists, &blob_materialization) {
                (false, _) => None,
                (true, Some(AudienceBlobMoveMaterialization::Host(staging))) => {
                    let mut blob_transaction = HostWriteBlobTransaction::new(
                        crate::store::store_session::StoreTransaction::new(&tx, store_dir),
                        verified_authority,
                    );
                    Some(
                        staging
                            .stage_audience_move_blobs_on(
                                &mut blob_transaction,
                                &mut blob_facts,
                                &partitioned.moves,
                                &partitioned.partitions,
                            )
                            .map_err(E::from)?,
                    )
                }
                (true, Some(AudienceBlobMoveMaterialization::PreparedTransition)) => {
                    record_prepared_transition_local_blob_moves(
                        &mut blob_facts,
                        &partitioned.moves,
                    )
                    .map_err(E::from)?;
                    None
                }
                (true, None) => {
                    return Err(E::from(DbError::Message(
                        "BlobMoveRequiresMaterialization: audience move staging is unavailable"
                            .to_string(),
                    )));
                }
            };
            let changeset_hash = match (|| -> Result<ObjectHash, DbError> {
                let mut changeset_writer =
                    crate::store::store_session::StoreTransaction::new(&tx, store_dir)
                        .payload_writer();
                journal.changeset_strm(&mut changeset_writer)?;
                Ok(changeset_writer.commit()?.0)
            })() {
                Ok(hash) => hash,
                Err(error) => {
                    return Err(E::from(rollback_staged_audience_blobs(staged_files, error)));
                }
            };
            drop(journal);
            let committed = (|| {
                let rows_changed = tx.total_changes().saturating_sub(changes_before);
                let local_stream_id =
                    crate::store::store_session::StoreTransaction::new(&tx, store_dir)
                        .local_merge_stream_id(verified_authority)?;
                let base = StoreWriteBase {
                    dependencies:
                        crate::store::materialized_commit_index::materialized_frontier_on(
                            &tx,
                            local_stream_id.as_deref(),
                        )?,
                };
                let status = crate::store::store_session::StoreTransaction::new(&tx, store_dir)
                    .insert_store_write(
                        &write_id,
                        &partitioned.partitions,
                        changeset_hash,
                        &base,
                        &blob_facts,
                        rows_changed,
                    )?;
                tx.commit().map_err(DbError::from)?;
                Ok::<_, DbError>(status)
            })();
            let status = match committed {
                Ok(status) => status,
                Err(error) => {
                    return Err(E::from(rollback_staged_audience_blobs(staged_files, error)));
                }
            };
            Ok(WriteReceipt {
                value,
                write_id,
                status,
            })
        })()
    }
}

pub(crate) fn record_prepared_transition_local_blob_moves(
    facts: &mut StoreWriteBlobFacts,
    moves: &[AudienceMove],
) -> Result<(), DbError> {
    let moved_rows = audience_moves_by_row(moves)?;
    for fact in &mut facts.blobs {
        let Some(audience_move) = moved_rows.get(&(fact.table.clone(), fact.row_id.clone())) else {
            continue;
        };
        if audience_move.destination == coven_protocol::circle::Audience::Local {
            fact.audience_move = Some(StoreWriteBlobMoveDestination::Local);
        }
    }
    Ok(())
}

pub fn audience_moves_by_row(
    moves: &[AudienceMove],
) -> Result<BTreeMap<(String, String), &AudienceMove>, DbError> {
    let mut moved_rows = BTreeMap::new();
    for audience_move in moves {
        for row in &audience_move.rows {
            if let Some(prior) = moved_rows.insert(row.clone(), audience_move) {
                if prior.source != audience_move.source
                    || prior.destination != audience_move.destination
                {
                    return Err(DbError::Message(format!(
                        "row {}/{} belongs to conflicting audience moves",
                        row.0, row.1
                    )));
                }
            }
        }
    }
    Ok(moved_rows)
}

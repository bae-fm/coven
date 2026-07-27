use std::collections::BTreeMap;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension};
use tracing::warn;

use crate::blob::decl::BlobDecls;
use crate::blob::decl::PublicationBlob;
use crate::database::{
    attach_session, authorize_host_sql, capture_changeset, local_merge_stream_id_on, *,
};
use crate::encryption::EncryptionService;
use crate::sync::gate::{self, Gates};
use crate::{AffectedRow, Provenance, SyncedTable, WriteId, WriteReceipt, WriteStatus};

use super::*;

enum AudienceBlobMoveMaterialization<'a> {
    Host(&'a super::audience_blob_staging::HostWriteBlobStaging),
    PreparedTransition,
}

impl StoreDatabase {
    fn start_host_change_journal_on<'c>(
        conn: &'c Connection,
        synced_tables: &[SyncedTable],
    ) -> Result<rusqlite::session::Session<'c>, DbError> {
        attach_session(conn, synced_tables)
    }

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
        crate::sync::session::validate_changeset_row_identities(&captured, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(captured)
    }

    pub(crate) fn invert_changeset(changeset: &[u8]) -> Result<Vec<u8>, DbError> {
        if changeset.is_empty() {
            return Ok(Vec::new());
        }
        let mut inverse = Vec::new();
        rusqlite::session::invert_strm(&mut &changeset[..], &mut inverse).map_err(DbError::from)?;
        Ok(inverse)
    }

    pub(super) fn run_host_sql_on<R, E>(
        conn: &Connection,
        f: impl FnOnce() -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        conn.authorizer(Some(authorize_host_sql))
            .map_err(DbError::from)
            .map_err(E::from)?;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        if let Err(error) = conn.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        ) {
            panic!("failed to remove host SQL gate-baseline guard: {error}");
        }
        match outcome {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn capture_store_write_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        changeset: &[u8],
        blob_decls: &BlobDecls,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let changes = crate::changeset::walk(changeset)
            .map_err(|error| DbError::Message(format!("read Store write blobs: {error}")))?;
        let mut facts = BTreeMap::new();
        for change in changes {
            let Some(publication) = blob_decls
                .publication_blob_from_change(tx, &change)
                .map_err(|error| DbError::Message(format!("capture Store write blob: {error}")))?
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
            DbError::Message(format!(
                "capture Store write blob {}/{} plaintext hash: {error}",
                publication.blob.namespace, publication.blob.id
            ))
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
        moves: &[gate::AudienceMove],
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
                        DbError::Message(format!(
                            "capture audience-move blob {table}/{row_id}: {error}"
                        ))
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
        moves: &[gate::AudienceMove],
        blob_decls: &BlobDecls,
    ) -> Result<bool, DbError> {
        let mut advanced = false;
        for audience_move in moves {
            for (table, row_id) in &audience_move.rows {
                let carries_blob = blob_decls
                    .publication_blob_for_row(tx, table, row_id)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "read audience-move blob row {table}/{row_id}: {error}"
                        ))
                    })?
                    .is_some();
                if !carries_blob {
                    continue;
                }
                let sql = format!(
                    "UPDATE {} SET {} = ?1 WHERE {} = ?2 AND {} < ?1",
                    crate::sync::session::quote_ident(table),
                    crate::sync::session::quote_ident("_updated_at"),
                    crate::sync::session::quote_ident("id"),
                    crate::sync::session::quote_ident("_updated_at"),
                );
                let updated = tx
                    .execute(&sql, rusqlite::params![audience_move.stamp, row_id])
                    .map_err(DbError::from)?;
                advanced |= updated > 0;
            }
        }
        Ok(advanced)
    }

    pub(crate) fn capture_partition_blob_facts_on(
        tx: &rusqlite::Transaction<'_>,
        partitions: &[gate::AudiencePartition],
        blob_decls: &BlobDecls,
    ) -> Result<StoreWriteBlobFacts, DbError> {
        let mut facts = BTreeMap::new();
        for partition in partitions {
            if partition.audience == crate::sync::circle::Audience::Local {
                continue;
            }
            for fact in
                Self::capture_store_write_blob_facts_on(tx, &partition.changeset, blob_decls)?.blobs
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

    pub(crate) fn partition_captured_write_on(
        tx: &rusqlite::Transaction<'_>,
        captured: &[u8],
        gates: &Gates,
        routing: StoreWriteRouting<'_>,
    ) -> Result<gate::PartitionedAudienceWrite, DbError> {
        match routing {
            StoreWriteRouting::MergeScoped(encryption) => {
                let store_root_hash = required_store_root_authority_on(tx)?.store_root_hash;
                let key = crate::sync::circle::derive_row_routing_key(encryption, store_root_hash)
                    .map_err(|error| {
                        DbError::Message(format!("derive row routing key: {error}"))
                    })?;
                let routing_changeset = gate::capture_routing_changes(tx, captured, gates, &key)
                    .map_err(|error| {
                        DbError::Message(format!("capture scoped routing changes: {error}"))
                    })?;
                gate::partition_outbound(tx, captured, &routing_changeset, gates).map_err(|error| {
                    DbError::Message(format!("partition scoped host transaction: {error}"))
                })
            }
            StoreWriteRouting::Unscoped => {
                gate::partition_outbound(tx, captured, &gate::RoutingChanges::empty(), gates)
                    .map_err(|error| {
                        DbError::Message(format!("partition gated host transaction: {error}"))
                    })
            }
        }
    }

    fn store_write_routing<'a>(
        gates: &Gates,
        routing_encryption: Option<&'a EncryptionService>,
    ) -> Result<StoreWriteRouting<'a>, DbError> {
        if !gates.has_scoped_graph() {
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

    pub(crate) fn validate_store_write_routing(
        gates: &Gates,
        routing_encryption: Option<&EncryptionService>,
    ) -> Result<(), DbError> {
        Self::store_write_routing(gates, routing_encryption).map(drop)
    }

    pub(crate) fn insert_store_write_on(
        tx: &rusqlite::Transaction<'_>,
        write_id: &WriteId,
        partitions: &[gate::AudiencePartition],
        inverse_changeset: &[u8],
        base: &StoreWriteBase,
        blob_facts: &StoreWriteBlobFacts,
        rows_changed: u64,
    ) -> Result<WriteStatus, DbError> {
        let remote_partitions = partitions
            .iter()
            .filter(|partition| partition.audience != crate::sync::circle::Audience::Local)
            .collect::<Vec<_>>();
        let affected_rows = if remote_partitions.is_empty() {
            // A tripwire, not a routine event. An empty capture from a transaction
            // that also CHANGED NO ROWS is a pure read left on the write path —
            // warn so it gets moved to the journal-free read path
            // (`CovenHandle::sql_read`). An empty capture
            // from a transaction that DID change rows is a device-local-table
            // write (those tables aren't in the session) — a supported, routine
            // pattern that stays on `sql()` silently. The one case this misses:
            // a conditional write to a synced table that no-op'd this cycle (an
            // idempotent INSERT OR IGNORE re-run) also changed no rows and warns;
            // legitimate but rare, tolerated.
            if partitions.is_empty() && rows_changed == 0 {
                warn!("journaled sql transaction changed nothing; pure reads belong on sql_read");
                // Debug builds name the offender: the backtrace runs through the
                // host's monomorphized closure, whose symbol carries the call
                // site's module path. Captured only when the warn fires.
                #[cfg(debug_assertions)]
                warn!(
                    "zero-change sql transaction backtrace:\n{}",
                    std::backtrace::Backtrace::force_capture()
                );
            }
            Vec::new()
        } else {
            let mut affected = Vec::new();
            for partition in &remote_partitions {
                affected.extend(
                    crate::changeset::walk(&partition.changeset)
                        .map_err(|error| {
                            DbError::Message(format!("read affected write rows: {error}"))
                        })?
                        .into_iter()
                        .filter(|row| !gate::is_routing_table(&row.table))
                        .map(|row| {
                            let primary_key = row.pk().map(str::to_owned).ok_or_else(|| {
                                DbError::Message(format!(
                                    "shared write row in {:?} has no primary key",
                                    row.table
                                ))
                            })?;
                            Ok(AffectedRow {
                                table: row.table,
                                primary_key,
                            })
                        })
                        .collect::<Result<Vec<_>, DbError>>()?,
                );
            }
            affected.sort();
            affected.dedup();
            affected
        };
        let status = if remote_partitions.is_empty() {
            WriteStatus::LocalOnly
        } else {
            WriteStatus::Pending
        };
        let base = serde_json::to_string(base)
            .map_err(|error| DbError::Message(format!("serialize pending Store base: {error}")))?;
        let status_json = serde_json::to_string(&status)
            .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
        let affected_rows = serde_json::to_string(&affected_rows)
            .map_err(|error| DbError::Message(format!("serialize affected rows: {error}")))?;
        let blob_facts_json = serde_json::to_string(blob_facts).map_err(|error| {
            DbError::Message(format!("serialize Store write blob facts: {error}"))
        })?;
        let store_changeset = remote_partitions
            .iter()
            .find(|partition| partition.audience == crate::sync::circle::Audience::Store)
            .map(|partition| partition.changeset.as_slice())
            .unwrap_or_default();
        tx.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                write_id.as_str(),
                status_json,
                affected_rows,
                store_changeset,
                inverse_changeset,
                base,
                blob_facts_json,
            ],
        )
        .map_err(DbError::from)?;
        for partition in partitions {
            let audience = match partition.audience {
                crate::sync::circle::Audience::Store => "store".to_string(),
                crate::sync::circle::Audience::Local => "local".to_string(),
                crate::sync::circle::Audience::Circle(circle_id) => circle_id.to_string(),
            };
            let control = partition
                .control
                .as_ref()
                .map(gate::CirclePartitionControl::stored_json);
            tx.execute(
                "INSERT INTO store_write_partitions
                 (write_id, audience, control_coord, changeset)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![write_id.as_str(), audience, control, partition.changeset,],
            )
            .map_err(DbError::from)?;
        }
        if status == WriteStatus::Pending {
            for fact in &blob_facts.blobs {
                if fact.blob.provenance != Provenance::HostProvided {
                    continue;
                }
                tx.execute(
                    "INSERT OR IGNORE INTO store_write_blob_leases
                     (write_id, namespace, blob_id) VALUES (?1, ?2, ?3)",
                    (write_id.as_str(), &fact.blob.namespace, &fact.blob.id),
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(status)
    }

    pub fn run_store_write_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        gates: &Gates,
        blob_decls: &BlobDecls,
        routing_encryption: Option<&EncryptionService>,
        blob_staging: Option<&super::audience_blob_staging::HostWriteBlobStaging>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<WriteReceipt<R>, E>
    where
        E: From<DbError>,
    {
        Self::run_store_write_transaction_with_blob_materialization_on(
            conn,
            synced_tables,
            gates,
            blob_decls,
            routing_encryption,
            blob_staging.map(AudienceBlobMoveMaterialization::Host),
            write_id,
            f,
        )
    }

    fn run_store_write_transaction_with_blob_materialization_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        gates: &Gates,
        blob_decls: &BlobDecls,
        routing_encryption: Option<&EncryptionService>,
        blob_materialization: Option<AudienceBlobMoveMaterialization<'_>>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<WriteReceipt<R>, E>
    where
        E: From<DbError>,
    {
        (|| {
            let routing = Self::store_write_routing(gates, routing_encryption).map_err(E::from)?;
            let changes_before = conn.total_changes();
            let tx = conn
                .unchecked_transaction()
                .map_err(DbError::from)
                .map_err(E::from)?;
            let mut journal =
                Self::start_host_change_journal_on(&tx, synced_tables).map_err(E::from)?;
            let value = Self::run_host_sql_on(&tx, || f(&tx))?;
            let mut captured =
                Self::drain_host_change_journal(&mut journal, synced_tables).map_err(E::from)?;
            gate::validate_scoped_foreign_key_audiences(&tx, gates)
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
                let moves = gate::audience_moves(&tx, &captured, gates)
                    .map_err(|error| DbError::Message(error.to_string()))
                    .map_err(E::from)?;
                if Self::advance_moved_blob_row_stamps_on(&tx, &moves, blob_decls)
                    .map_err(E::from)?
                {
                    captured = Self::drain_host_change_journal(&mut journal, synced_tables)
                        .map_err(E::from)?;
                }
            }
            drop(journal);
            let partitioned = Self::partition_captured_write_on(&tx, &captured, gates, routing)
                .map_err(E::from)?;
            let mut blob_facts =
                Self::capture_partition_blob_facts_on(&tx, &partitioned.partitions, blob_decls)
                    .map_err(E::from)?;
            blob_facts = Self::capture_audience_move_blob_facts_on(
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
                (true, Some(AudienceBlobMoveMaterialization::Host(staging))) => Some(
                    Self::stage_audience_move_blobs_on(
                        &tx,
                        &mut blob_facts,
                        &partitioned.moves,
                        &partitioned.partitions,
                        staging,
                    )
                    .map_err(E::from)?,
                ),
                (true, Some(AudienceBlobMoveMaterialization::PreparedTransition)) => {
                    Self::record_prepared_transition_local_blob_moves(
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
            let committed = (|| {
                let rows_changed = conn.total_changes().saturating_sub(changes_before);
                let local_stream_id = local_merge_stream_id_on(&tx)?;
                let inverse_changeset = Self::invert_changeset(&captured)?;
                let base = StoreWriteBase {
                    dependencies: Self::materialized_frontier_on(&tx, local_stream_id.as_deref())?,
                };
                let status = Self::insert_store_write_on(
                    &tx,
                    &write_id,
                    &partitioned.partitions,
                    &inverse_changeset,
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
                    let error = match (&blob_materialization, staged_files) {
                        (Some(AudienceBlobMoveMaterialization::Host(staging)), Some(files)) => {
                            Self::rollback_staged_audience_blobs(staging, files, error)
                        }
                        (_, None) => error,
                        (_, Some(_)) => unreachable!("staged files require host staging"),
                    };
                    return Err(E::from(error));
                }
            };
            Ok(WriteReceipt {
                value,
                write_id,
                status,
            })
        })()
    }

    pub fn run_internal_store_write_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        routing_encryption: Option<&EncryptionService>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        Self::run_coven_store_write_transaction_on(
            conn,
            synced_tables,
            routing_encryption,
            None,
            write_id,
            f,
        )
    }

    pub(crate) fn run_prepared_blob_transition_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        routing_encryption: Option<&EncryptionService>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        Self::run_coven_store_write_transaction_on(
            conn,
            synced_tables,
            routing_encryption,
            Some(AudienceBlobMoveMaterialization::PreparedTransition),
            write_id,
            f,
        )
    }

    fn run_coven_store_write_transaction_on<R, E>(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        routing_encryption: Option<&EncryptionService>,
        blob_materialization: Option<AudienceBlobMoveMaterialization<'_>>,
        write_id: WriteId,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        let gates = Gates::from_tables(conn, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))
            .map_err(E::from)?;
        let blob_decls = BlobDecls::from_tables(conn, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))
            .map_err(E::from)?;
        Self::run_store_write_transaction_with_blob_materialization_on(
            conn,
            synced_tables,
            &gates,
            &blob_decls,
            routing_encryption,
            blob_materialization,
            write_id,
            f,
        )
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn prepare_store_write(&self) -> Result<Option<PreparedStoreWrite>, DbError> {
        self.database
            .call(move |conn| {
                let stored = conn
                .query_row(
                "SELECT write_id, changeset, inverse_changeset, base, blob_facts FROM store_writes
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
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
                .optional()
                .map_err(DbError::from)?;
                let Some((write_id, changeset, inverse_changeset, base, blob_facts)) = stored
                else {
                    return Ok(None);
                };
                let partitions = Self::store_write_partitions_on(conn, &write_id, &changeset)?;
                Ok(Some(PreparedStoreWrite {
                    write_id: WriteId::from_generated(write_id),
                    changeset,
                    partitions,
                    inverse_changeset,
                    base: serde_json::from_str(&base).map_err(|error| {
                        DbError::Message(format!("pending write base: {error}"))
                    })?,
                    blob_facts: serde_json::from_str(&blob_facts).map_err(|error| {
                        DbError::Message(format!("pending write blob facts: {error}"))
                    })?,
                }))
            })
            .await
    }

    pub(crate) fn store_write_partitions_on(
        conn: &Connection,
        write_id: &str,
        stored_store_changeset: &[u8],
    ) -> Result<PreparedStoreWritePartitions, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions
                 WHERE write_id = ?1
                 ORDER BY CASE audience WHEN 'store' THEN 0 WHEN 'local' THEN 2 ELSE 1 END,
                          audience, control_coord",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([write_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut store = None;
        let mut circles = Vec::new();
        let mut local = None;
        for row in rows {
            let (audience, control, changeset) = row.map_err(DbError::from)?;
            if audience == "store" {
                if control.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} Store partition carries a Circle control"
                    )));
                }
                if store.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} carries more than one Store partition"
                    )));
                }
                store = Some(gate::AudiencePartition {
                    audience: crate::sync::circle::Audience::Store,
                    control: None,
                    changeset,
                });
                continue;
            }
            if audience == "local" {
                if control.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} Local partition carries a Circle control"
                    )));
                }
                if local.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} carries more than one Local partition"
                    )));
                }
                local = Some(gate::AudiencePartition {
                    audience: crate::sync::circle::Audience::Local,
                    control: None,
                    changeset,
                });
                continue;
            }
            let circle_id = audience
                .parse::<crate::sync::circle::CircleId>()
                .map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} has invalid audience {audience:?}: {error}"
                    ))
                })?;
            let control_json = control.ok_or_else(|| {
                DbError::Message(format!(
                    "pending write {write_id} Circle {circle_id} has no control coordinate"
                ))
            })?;
            let control =
                gate::CirclePartitionControl::from_stored_json(control_json).map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} Circle {circle_id} control coordinate: {error}"
                    ))
                })?;
            circles.push(gate::AudiencePartition {
                audience: crate::sync::circle::Audience::Circle(circle_id),
                control: Some(control),
                changeset,
            });
        }
        drop(statement);
        match &store {
            Some(partition) if partition.changeset != stored_store_changeset => {
                return Err(DbError::Message(format!(
                    "pending write {write_id} Store partition differs from store_writes.changeset"
                )));
            }
            None if !stored_store_changeset.is_empty() => {
                return Err(DbError::Message(format!(
                    "pending write {write_id} has a store_writes.changeset without a Store partition"
                )));
            }
            Some(_) | None => {}
        }
        Ok(PreparedStoreWritePartitions {
            store,
            circles,
            local,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_sql_authorizer_is_removed_after_success_error_and_panic() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "ATTACH ':memory:' AS coven_gate_empty; \
             CREATE TABLE coven_gate_empty.baseline (id TEXT PRIMARY KEY) STRICT; \
             INSERT INTO coven_gate_empty.baseline VALUES ('guarded');",
        )
        .expect("attach guarded schema");
        let assert_guard_removed = || {
            let id: String = conn
                .query_row("SELECT id FROM coven_gate_empty.baseline", [], |row| {
                    row.get(0)
                })
                .expect("internal SQL can address the baseline after host SQL");
            assert_eq!(id, "guarded");
        };

        StoreDatabase::run_host_sql_on(&conn, || Ok::<_, DbError>(()))
            .expect("successful host SQL");
        assert_guard_removed();

        let error =
            StoreDatabase::run_host_sql_on(&conn, || Err::<(), _>(DbError::Message("host".into())));
        assert!(error.is_err());
        assert_guard_removed();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            StoreDatabase::run_host_sql_on(&conn, || -> Result<(), DbError> {
                panic!("host panic")
            })
            .expect("panicking host SQL closure never returns");
        }));
        assert!(panic.is_err());
        assert_guard_removed();
    }
}

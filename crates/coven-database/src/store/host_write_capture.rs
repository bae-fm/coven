use std::collections::BTreeMap;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension};
use tracing::warn;

use crate::BlobDecls;
use crate::PublicationBlob;
use crate::{attach_session, capture_changeset, *};
use crate::{
    audience_moves, capture_routing_changes, is_routing_table, partition_outbound,
    validate_scoped_foreign_key_audiences, AudienceMove, AudiencePartition, CirclePartitionControl,
    Gates, RoutingChanges,
};
use crate::{WriteId, WriteStatus};

use coven_protocol::write::AffectedRow;

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
    transaction: &'transaction rusqlite::Transaction<'connection>,
    store_dir: &'transaction coven_foundation::store_dir::StoreDir,
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
        let partitions = StoreDatabase::store_write_partitions_on(
            crate::payload_spool::StoreRecords::new(self.conn, self.store_dir),
            &write_id,
        )?;
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
        transaction: &'transaction rusqlite::Transaction<'connection>,
        store_dir: &'transaction coven_foundation::store_dir::StoreDir,
        verified_authority: &'transaction mut super::verified_store_authority::VerifiedStoreAuthority,
    ) -> Self {
        Self {
            transaction,
            store_dir,
            verified_authority,
        }
    }

    pub fn local_activated_registration(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<coven_protocol::store_commit::ReferencedStoreDeviceRegistration, DbError> {
        let reference =
            local_activated_registration_ref_on(self.transaction)?.ok_or_else(|| {
                DbError::Message(
                    "audience blob move has no activated local Store registration".to_string(),
                )
            })?;
        let registration = self.verified_authority.activated_registration_on(
            crate::payload_spool::StoreRecords::new(self.transaction, self.store_dir),
            root,
            &reference,
        )?;
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
        super::circle_publication_context_on(self.transaction, circle_id, expected_control)
    }

    pub fn circle_blob_opening_protection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        super::circle_blob_opening_protection_on(
            crate::payload_spool::StoreRecords::new(self.transaction, self.store_dir),
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

    pub fn capture_partition_blob_facts_on(
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

    pub fn validate_store_write_routing(
        &self,
        routing_encryption: Option<&EncryptionService>,
    ) -> Result<(), DbError> {
        Self::store_write_routing(&self.gates, routing_encryption).map(drop)
    }

    pub(crate) fn insert_store_write_on(
        records: crate::payload_spool::StoreRecordTransaction<'_, '_>,
        write_id: &WriteId,
        partitions: &[AudiencePartition],
        changeset_hash: ObjectHash,
        base: &StoreWriteBase,
        blob_facts: &StoreWriteBlobFacts,
        rows_changed: u64,
    ) -> Result<WriteStatus, DbError> {
        let tx = records.transaction;
        let remote_partitions = partitions
            .iter()
            .filter(|partition| partition.audience != coven_protocol::circle::Audience::Local)
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
                    crate::walk_changeset(&partition.changeset)
                        .map_err(|error| {
                            DbError::Message(format!("read affected write rows: {error}"))
                        })?
                        .into_iter()
                        .filter(|row| !is_routing_table(&row.table))
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
            .map_err(|error| DbError::context("serialize pending Store base", error))?;
        let status_json = serde_json::to_string(&status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        let affected_rows = serde_json::to_string(&affected_rows)
            .map_err(|error| DbError::context("serialize affected rows", error))?;
        let blob_facts_json = serde_json::to_string(blob_facts)
            .map_err(|error| DbError::context("serialize Store write blob facts", error))?;
        tx.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset_hash, base, blob_facts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                write_id.as_str(),
                status_json,
                affected_rows,
                changeset_hash.to_string(),
                base,
                blob_facts_json,
            ],
        )
        .map_err(DbError::from)?;
        let mut payloads = std::collections::BTreeSet::from([changeset_hash]);
        for partition in partitions {
            let audience = match partition.audience {
                coven_protocol::circle::Audience::Store => "store".to_string(),
                coven_protocol::circle::Audience::Local => "local".to_string(),
                coven_protocol::circle::Audience::Circle(circle_id) => circle_id.to_string(),
            };
            let control = partition
                .control
                .as_ref()
                .map(CirclePartitionControl::stored_json);
            let partition_hash = records.install_payload(&partition.changeset)?;
            payloads.insert(partition_hash);
            tx.execute(
                "INSERT INTO store_write_partitions
                 (write_id, audience, control_coord, changeset_hash)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    write_id.as_str(),
                    audience,
                    control,
                    partition_hash.to_string()
                ],
            )
            .map_err(DbError::from)?;
        }
        crate::payload_spool::set_payload_owner_claims_on(
            tx,
            &crate::payload_spool::store_write_owner_key(write_id),
            &payloads,
        )?;
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

    pub async fn prepare_store_write(&self) -> Result<Option<PreparedStoreWrite>, DbError> {
        self.connection
            .call_store(|session| session.prepare_store_write())
            .await
    }

    pub(crate) fn store_write_partitions_on(
        records: crate::payload_spool::StoreRecords<'_>,
        write_id: &str,
    ) -> Result<PreparedStoreWritePartitions, DbError> {
        let conn = records.conn;
        let mut statement = conn
            .prepare(
                "SELECT audience, control_coord, changeset_hash
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
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut store = None;
        let mut circles = Vec::new();
        let mut local = None;
        for row in rows {
            let (audience, control, changeset_hash) = row.map_err(DbError::from)?;
            let changeset = records.payload(changeset_hash.parse()?)?;
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
                store = Some(AudiencePartition {
                    audience: coven_protocol::circle::Audience::Store,
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
                local = Some(AudiencePartition {
                    audience: coven_protocol::circle::Audience::Local,
                    control: None,
                    changeset,
                });
                continue;
            }
            let circle_id = audience
                .parse::<coven_protocol::circle::CircleId>()
                .map_err(|error| {
                    DbError::context(
                        format!("pending write {write_id} has invalid audience {audience:?}"),
                        error,
                    )
                })?;
            let control_json = control.ok_or_else(|| {
                DbError::Message(format!(
                    "pending write {write_id} Circle {circle_id} has no control coordinate"
                ))
            })?;
            let control =
                CirclePartitionControl::from_stored_json(control_json).map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} Circle {circle_id} control coordinate: {error}"
                    ))
                })?;
            circles.push(AudiencePartition {
                audience: coven_protocol::circle::Audience::Circle(circle_id),
                control: Some(control),
                changeset,
            });
        }
        drop(statement);
        Ok(PreparedStoreWritePartitions {
            store,
            circles,
            local,
        })
    }
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
        let routing = StoreDatabase::store_write_routing(gates, routing_encryption)?;
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

    pub(crate) fn execute<R, E>(
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
            let mut journal = attach_session(&tx, synced_tables).map_err(E::from)?;
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
                    let store_root_hash = verified_authority
                        .required_root_authority_on(crate::payload_spool::StoreRecords::new(
                            &tx, store_dir,
                        ))
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
            let mut blob_facts = StoreDatabase::capture_partition_blob_facts_on(
                &tx,
                &partitioned.partitions,
                blob_decls,
            )
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
                    let mut blob_transaction =
                        HostWriteBlobTransaction::new(&tx, store_dir, verified_authority);
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
                    crate::payload_spool::PayloadSpoolWriter::create(store_dir)?;
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
                let local_stream_id = verified_authority.local_merge_stream_id_on(
                    crate::payload_spool::StoreRecords::new(&tx, store_dir),
                )?;
                let base = StoreWriteBase {
                    dependencies: StoreDatabase::materialized_frontier_on(
                        &tx,
                        local_stream_id.as_deref(),
                    )?,
                };
                let status = StoreDatabase::insert_store_write_on(
                    crate::payload_spool::StoreRecordTransaction::new(&tx, store_dir),
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

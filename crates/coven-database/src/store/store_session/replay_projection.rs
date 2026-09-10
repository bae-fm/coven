use super::replay_sql::ReplaySql;
use super::*;

mod rebase;
mod relationships;

/// A replay-owned SQLite image. Callers can apply or inspect the projection,
/// but cannot obtain the connection that implements it.
pub(crate) struct ReplayProjection {
    connection: rusqlite::Connection,
    store_dir: coven_foundation::store_dir::StoreDir,
    baseline: RetainedReplayBaseline,
}

pub(crate) struct ReplayProjectionResult {
    projection: ReplayProjection,
    watched: Option<WatchedReplayOutcome>,
    applied_order: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
    max_updated_at: Option<coven_protocol::hlc::Timestamp>,
    unaccepted_journal: Vec<crate::MergeReplayWrite>,
}

#[derive(Clone)]
pub(crate) enum WatchedReplayOutcome {
    Applied,
    Held(crate::MaterializationHold),
}

impl ReplayProjectionResult {
    pub(super) fn new(
        projection: ReplayProjection,
        watched: Option<WatchedReplayOutcome>,
        applied_order: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        max_updated_at: Option<coven_protocol::hlc::Timestamp>,
    ) -> Self {
        Self {
            projection,
            watched,
            applied_order,
            max_updated_at,
            unaccepted_journal: Vec::new(),
        }
    }

    pub(super) fn with_unaccepted_journal(mut self, journal: Vec<crate::MergeReplayWrite>) -> Self {
        self.unaccepted_journal = journal;
        self
    }

    pub(super) fn take_unaccepted_journal(&mut self) -> Vec<crate::MergeReplayWrite> {
        std::mem::take(&mut self.unaccepted_journal)
    }

    pub(super) fn restore_unaccepted_write(
        &self,
        live: &mut VerifiedStoreTransaction<'_, '_, '_, '_>,
        effect: crate::MergeReplayWriteEffect,
    ) -> Result<(), DbError> {
        let projection = &self.projection;
        let schema = projection.table_schema(live.synced_tables, live.gates)?;
        let mut private_rows = projection.private_rows(live.gates, &schema)?;
        let mut authority =
            VerifiedStoreAuthority::for_replay_baseline(projection.baseline.clone());
        ReplaySql::begin(&projection.connection)?.run(|| {
            projection.apply_write_effect(
                &mut authority,
                live.authority.root(),
                effect,
                schema,
                live.gates,
                &mut private_rows,
            )
        })?;
        Ok(())
    }

    pub(super) fn rebase_write(
        &self,
        transaction: &mut VerifiedStoreTransaction<'_, '_, '_, '_>,
        effect: crate::MergeReplayWriteEffect,
        base: &coven_protocol::store_commit::StorePublicationBase,
    ) -> Result<(), DbError> {
        self.projection.rebase_write(transaction, effect, base)
    }

    pub(super) fn watched_outcome(&self) -> Option<WatchedReplayOutcome> {
        self.watched.clone()
    }

    pub(super) fn max_updated_at(&self) -> Option<coven_protocol::hlc::Timestamp> {
        self.max_updated_at.clone()
    }

    pub(super) fn applied_order(
        &self,
    ) -> impl Iterator<Item = &coven_protocol::store_commit::StoreBatchCommitRef> {
        self.applied_order.iter()
    }

    pub(super) fn materialized_frontier(
        &self,
    ) -> Result<coven_protocol::store_commit::CommitFrontier, DbError> {
        self.projection.materialized_frontier()
    }

    pub(super) fn install_on(
        &self,
        transaction: &VerifiedStoreTransaction<'_, '_, '_, '_>,
    ) -> Result<Vec<coven_foundation::changeset::RowChange>, DbError> {
        transaction.install_replay_projection(&self.projection)
    }

    pub(super) fn capture_replay_baseline(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        cut: &coven_protocol::store_commit::CommitFrontier,
        snapshot_hash: crate::ObjectHash,
    ) -> Result<Vec<u8>, DbError> {
        self.projection
            .capture_replay_baseline(root, cut, snapshot_hash)
    }

    pub(super) fn capture_snapshot(
        &self,
        image: crate::SnapshotDatabaseImage,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<crate::CreatedSnapshot, crate::SnapshotImageError> {
        self.projection
            .capture_snapshot(image, root, tables, routing_encryption, audience)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn row_count(&self, table: &str) -> Result<i64, DbError> {
        self.projection.row_count(table)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn document_count(&self, id: &str) -> Result<i64, DbError> {
        self.projection.document_count(id)
    }
}

impl ReplayProjection {
    pub(super) fn replace_store_publication_state(
        &self,
        source: &rusqlite::Connection,
    ) -> Result<(), DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        replace_tables_from_connection_on(
            source,
            &transaction,
            &[
                "store_publication_current".to_string(),
                "store_publication_entries".to_string(),
            ],
        )?;
        transaction.commit().map_err(DbError::from)
    }

    pub(super) fn publication_blobs(
        &self,
        blob_decls: &crate::BlobDecls,
    ) -> Result<Vec<crate::PublicationBlob>, DbError> {
        blob_decls
            .publication_blobs_in_db(&self.connection)
            .map_err(DbError::from)
    }

    pub(super) fn from_image(
        image: &[u8],
        store_dir: coven_foundation::store_dir::StoreDir,
        baseline: &RetainedReplayBaseline,
        accepted: std::collections::BTreeMap<
            coven_protocol::store_commit::StoreBatchCommitRef,
            std::sync::Arc<coven_protocol::store_commit::ResolvedStoreDeviceState>,
        >,
    ) -> Result<Self, DbError> {
        let mut connection = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(&mut connection, image)
            .map_err(|error| DbError::context("open retained replay database image", error))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
        let image_states =
            crate::store::store_device_state::load_covered_store_device_snapshots_on(
                &connection,
                &baseline.exact_cut,
            )?;
        let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
        for (reference, state) in accepted {
            match image_states.get(&reference) {
                Some(existing) if existing != &state => {
                    return Err(DbError::Message(format!(
                        "replay image device state disagrees with accepted history at {reference:?}"
                    )));
                }
                Some(_) => {}
                None => {
                    crate::store::store_device_state::record_store_device_snapshot_on(
                        &transaction,
                        &reference,
                        &state,
                    )?;
                }
            }
        }
        transaction.commit().map_err(DbError::from)?;
        Ok(Self {
            connection,
            store_dir,
            baseline: baseline.clone(),
        })
    }

    pub(super) fn table_schema(
        &self,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        gates: &crate::Gates,
    ) -> Result<std::sync::Arc<TableSchema>, DbError> {
        Ok(std::sync::Arc::new(TableSchema::for_apply(
            &self.connection,
            synced_tables,
            gates,
        )?))
    }

    pub(super) fn install_circle_bootstrap(
        &self,
        image_bytes: &[u8],
        coverage: &coven_protocol::circle::CircleBootstrapCoverageRef,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<(), DbError> {
        let mut source = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(&mut source, image_bytes)
            .map_err(|error| DbError::context("open retained Circle bootstrap image", error))?;
        crate::store::verify_circle_bootstrap_connection(
            &source,
            &coverage.bootstrap,
            coverage.circle_id,
            synced_tables,
            routing_key,
        )
        .map_err(|error| {
            DbError::context(
                format!("verify retained Circle {} bootstrap", coverage.circle_id),
                error,
            )
        })?;
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        crate::store::install_circle_bootstrap_connection_on(
            &transaction,
            &source,
            synced_tables,
            &coverage.activation_commit,
            coverage.circle_id,
            &coverage.bootstrap,
        )?;
        transaction.commit().map_err(DbError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_materialization(
        &self,
        authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        blob_decls: &crate::BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        timestamp_policy: IncomingTimestampPolicy,
        circle_bootstrap_cuts: &std::collections::BTreeMap<
            coven_protocol::circle::CircleId,
            coven_protocol::store_commit::CommitFrontier,
        >,
        materialization: crate::PreparedMergeMaterialization,
        local_effect: Option<crate::MergeReplayWriteEffect>,
        schema: std::sync::Arc<TableSchema>,
        private_rows: &mut super::merge_materialization_transaction::ReplayRows,
    ) -> Result<super::merge_materialization_transaction::AppliedMergeMaterialization, DbError>
    {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let mut next_private_rows = private_rows.clone();
        let applied = MergeMaterializationTransaction::from_store(
            crate::store::store_session::StoreTransaction::new(&transaction, &self.store_dir),
        )
        .apply_prepared_merge_materialization(
            authority,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            local_store_membership,
            timestamp_policy,
            Some(circle_bootstrap_cuts),
            materialization,
            local_effect,
            schema,
            &mut next_private_rows,
        )?;
        match &applied.outcome {
            crate::MaterializationOutcome::Applied(_) => {
                transaction.commit().map_err(DbError::from)?;
                *private_rows = next_private_rows;
            }
            crate::MaterializationOutcome::Held(_) => {
                transaction.rollback().map_err(DbError::from)?;
            }
        }
        Ok(applied)
    }

    pub(super) fn apply_write_effect(
        &self,
        authority: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        effect: crate::MergeReplayWriteEffect,
        schema: std::sync::Arc<TableSchema>,
        gates: &crate::Gates,
        private_rows: &mut super::merge_materialization_transaction::ReplayRows,
    ) -> Result<(), DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let mut next_private_rows = private_rows.clone();
        MergeMaterializationTransaction::from_store(
            crate::store::store_session::StoreTransaction::new(&transaction, &self.store_dir),
        )
        .apply_unaccepted_replay_effect(
            authority,
            root,
            effect,
            schema,
            gates,
            &mut next_private_rows,
        )?;
        transaction.commit().map_err(DbError::from)?;
        *private_rows = next_private_rows;
        Ok(())
    }

    pub(super) fn private_rows(
        &self,
        gates: &crate::Gates,
        schema: &TableSchema,
    ) -> Result<super::merge_materialization_transaction::ReplayRows, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let private_rows = MergeMaterializationTransaction::from_store(
            crate::store::store_session::StoreTransaction::new(&transaction, &self.store_dir),
        )
        .capture_replay_rows(gates, schema)?;
        transaction.rollback().map_err(DbError::from)?;
        Ok(private_rows)
    }

    pub(super) fn materialized_frontier(
        &self,
    ) -> Result<coven_protocol::store_commit::CommitFrontier, DbError> {
        coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                &self.connection,
                None,
            )?,
        )
        .map_err(DbError::from)
    }

    /// Serialize this projection as a retained-replay baseline image at `cut`.
    ///
    /// The projection already stands at `cut` — the caller checks that against
    /// its frontier before asking. What is left is to restate that position the
    /// way an installed snapshot states it, so the bytes validate as the shape
    /// a joining device captures rather than as a second, nearly identical
    /// shape: coverage rows naming the cut, no materialized commits, and the
    /// retained inputs pruned to the closure the cut needs.
    ///
    /// Unlike [`capture_snapshot`](Self::capture_snapshot) this does not project
    /// the image for an audience. A published snapshot is stripped down to what
    /// its recipients may read; a baseline is this device's own rewind point and
    /// keeps the protocol state, root authority, and registrations that a replay
    /// starts from — the very rows the published projection drops.
    pub(super) fn capture_replay_baseline(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        cut: &coven_protocol::store_commit::CommitFrontier,
        snapshot_hash: crate::ObjectHash,
    ) -> Result<Vec<u8>, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        transaction
            .pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        // Same order the published projection uses: the position rows are the
        // foreign-key children, so they go before the retained rows they name.
        transaction
            .execute("DELETE FROM materialized_commits", [])
            .map_err(DbError::from)?;
        let records = super::StoreTransaction::new(&transaction, &self.store_dir);
        let mut authority =
            super::VerifiedStoreAuthority::for_replay_baseline(self.baseline.clone());
        records.retain_snapshot_replay_inputs(&mut authority, root, cut)?;
        let records = super::StoreTransaction::new(&transaction, &self.store_dir);
        records.retain_snapshot_device_states(&mut authority, root, cut.clone().into_refs())?;
        transaction
            .execute("DELETE FROM snapshot_coverage", [])
            .map_err(DbError::from)?;
        for (stream_id, reference) in cut.clone().into_refs() {
            let encoded = serde_json::to_string(&reference)
                .map_err(|error| DbError::context("serialize replay baseline coverage", error))?;
            transaction
                .execute(
                    "INSERT INTO snapshot_coverage
                     (device_id, seq, commit_ref, snapshot_hash) VALUES (?1, ?2, ?3, ?4)",
                    (
                        &stream_id,
                        crate::Database::sequence_to_sqlite(
                            &stream_id,
                            reference.coord.sequence(),
                        )?,
                        encoded,
                        snapshot_hash.to_string(),
                    ),
                )
                .map_err(DbError::from)?;
        }
        transaction.commit().map_err(DbError::from)?;
        crate::connection_io::serialize_database_image(&self.connection)
    }

    pub(super) fn capture_snapshot(
        &self,
        image: crate::SnapshotDatabaseImage,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<crate::CreatedSnapshot, crate::SnapshotImageError> {
        image.capture_on(
            &self.connection,
            &self.store_dir,
            VerifiedStoreAuthority::for_replay_baseline(self.baseline.clone()),
            root,
            tables,
            routing_encryption,
            audience,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn row_count(&self, table: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", crate::quote_ident(table)),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn document_count(&self, id: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }
}

pub(super) fn replace_tables_from_projection_on(
    source: &ReplayProjection,
    target: &rusqlite::Transaction<'_>,
    tables: &[String],
) -> Result<(), DbError> {
    replace_tables_from_connection_on(&source.connection, target, tables)
}

fn replace_tables_from_connection_on(
    source: &rusqlite::Connection,
    target: &rusqlite::Transaction<'_>,
    tables: &[String],
) -> Result<(), DbError> {
    let mut tables = tables
        .iter()
        .map(|table| ProjectionTableRows::load(source, target, table))
        .collect::<Result<Vec<_>, _>>()?;
    relationships::with_local_relationships(target, &mut tables, |tables| {
        super::replay_sql::ReplaySql::begin(target)?.run(|| {
            for table in tables {
                table.delete_changed(target)?;
            }
            for table in tables {
                table.insert_changed(target)?;
            }
            for table in tables {
                table.validate_exact(target)?;
            }
            Ok(())
        })
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ProjectionKeyValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<&rusqlite::types::Value> for ProjectionKeyValue {
    fn from(value: &rusqlite::types::Value) -> Self {
        match value {
            rusqlite::types::Value::Null => Self::Null,
            rusqlite::types::Value::Integer(value) => Self::Integer(*value),
            rusqlite::types::Value::Real(value) => Self::Real(value.to_bits()),
            rusqlite::types::Value::Text(value) => Self::Text(value.clone()),
            rusqlite::types::Value::Blob(value) => Self::Blob(value.clone()),
        }
    }
}

struct ProjectionTableRows {
    table: String,
    columns: Vec<String>,
    primary_key: Vec<usize>,
    writable_columns: Vec<usize>,
    source: std::collections::BTreeMap<Vec<ProjectionKeyValue>, Vec<rusqlite::types::Value>>,
    target: std::collections::BTreeMap<Vec<ProjectionKeyValue>, Vec<rusqlite::types::Value>>,
}

impl ProjectionTableRows {
    fn load(
        source: &rusqlite::Connection,
        target: &rusqlite::Connection,
        table: &str,
    ) -> Result<Self, DbError> {
        let (columns, primary_key, writable_columns) = projection_table_columns(source, table)?;
        let (target_columns, target_primary_key, target_writable_columns) =
            projection_table_columns(target, table)?;
        if columns != target_columns
            || primary_key != target_primary_key
            || writable_columns != target_writable_columns
        {
            return Err(DbError::Message(format!(
                "retained replay projection table {table:?} differs from the live schema"
            )));
        }
        if primary_key.is_empty() {
            return Err(DbError::Message(format!(
                "retained replay projection table {table:?} has no primary key"
            )));
        }
        Ok(Self {
            table: table.to_string(),
            source: projection_table_rows(source, table, &columns, &primary_key)?,
            target: projection_table_rows(target, table, &columns, &primary_key)?,
            columns,
            primary_key,
            writable_columns,
        })
    }

    fn delete_changed(&self, target: &rusqlite::Connection) -> Result<(), DbError> {
        let predicate = self
            .primary_key
            .iter()
            .enumerate()
            .map(|(parameter, index)| {
                format!(
                    "{} IS ?{}",
                    crate::quote_ident(&self.columns[*index]),
                    parameter + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "DELETE FROM {} WHERE {predicate}",
            crate::quote_ident(&self.table)
        );
        let mut statement = target.prepare(&sql).map_err(DbError::from)?;
        for (key, row) in &self.target {
            if self.source.get(key) == Some(row) {
                continue;
            }
            let values = self.primary_key.iter().map(|index| &row[*index]);
            statement
                .execute(rusqlite::params_from_iter(values))
                .map_err(DbError::from)?;
        }
        Ok(())
    }

    fn insert_changed(&self, target: &rusqlite::Connection) -> Result<(), DbError> {
        let quoted_columns = self
            .writable_columns
            .iter()
            .map(|index| crate::quote_ident(&self.columns[*index]))
            .collect::<Vec<_>>();
        let placeholders = (1..=self.writable_columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            crate::quote_ident(&self.table),
            quoted_columns.join(", ")
        );
        let mut statement = target.prepare(&sql)?;
        for (key, row) in &self.source {
            if self.target.get(key) != Some(row) {
                statement.execute(rusqlite::params_from_iter(
                    self.writable_columns.iter().map(|index| &row[*index]),
                ))?;
            }
        }
        Ok(())
    }

    fn validate_exact(&self, target: &rusqlite::Connection) -> Result<(), DbError> {
        let installed =
            projection_table_rows(target, &self.table, &self.columns, &self.primary_key)?;
        if installed != self.source {
            return Err(DbError::Message(format!(
                "installed retained replay projection table {:?} differs from its source",
                self.table
            )));
        }
        Ok(())
    }
}

// Column names, primary-key positions in key order, and writable positions.
type ProjectionColumns = (Vec<String>, Vec<usize>, Vec<usize>);

fn projection_table_columns(
    connection: &rusqlite::Connection,
    table: &str,
) -> Result<ProjectionColumns, DbError> {
    let pragma = format!("PRAGMA table_xinfo({})", crate::quote_ident(table));
    let columns = crate::query_mapped_rows(connection, &pragma, [], |row| {
        Ok((
            row.get::<_, String>(1)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
        ))
    })?;
    if columns.is_empty() {
        return Err(DbError::Message(format!(
            "retained replay projection table {table:?} is absent"
        )));
    }
    let names = columns
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect::<Vec<_>>();
    let writable_columns = columns
        .iter()
        .enumerate()
        .filter_map(|(index, (_, _, hidden))| (*hidden == 0).then_some(index))
        .collect();
    let mut primary_key_columns = columns
        .into_iter()
        .filter(|(_, order, _)| *order > 0)
        .collect::<Vec<_>>();
    primary_key_columns.sort_by_key(|(_, order, _)| *order);
    let primary_key = primary_key_columns
        .into_iter()
        .map(|(name, _, _)| {
            names
                .iter()
                .position(|column| column == &name)
                .ok_or_else(|| {
                    DbError::Message(format!(
                    "retained replay projection table {table:?} lost primary-key column {name:?}"
                ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((names, primary_key, writable_columns))
}

fn projection_table_rows(
    connection: &rusqlite::Connection,
    table: &str,
    columns: &[String],
    primary_key: &[usize],
) -> Result<std::collections::BTreeMap<Vec<ProjectionKeyValue>, Vec<rusqlite::types::Value>>, DbError>
{
    let select = format!(
        "SELECT {} FROM {}",
        columns
            .iter()
            .map(|column| crate::quote_ident(column))
            .collect::<Vec<_>>()
            .join(", "),
        crate::quote_ident(table)
    );
    let rows = crate::query_mapped_rows(connection, &select, [], |row| {
        (0..columns.len())
            .map(|index| row.get::<_, rusqlite::types::Value>(index))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;
    let mut indexed = std::collections::BTreeMap::new();
    for row in rows {
        let key = primary_key
            .iter()
            .map(|index| ProjectionKeyValue::from(&row[*index]))
            .collect::<Vec<_>>();
        if indexed.insert(key, row).is_some() {
            return Err(DbError::Message(format!(
                "retained replay projection table {table:?} has a duplicate primary key"
            )));
        }
    }
    Ok(indexed)
}

#[cfg(test)]
#[path = "replay_projection_tests.rs"]
mod tests;

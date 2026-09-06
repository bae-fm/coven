use super::*;

/// A replay-owned SQLite image. Callers can apply or inspect the projection,
/// but cannot obtain the connection that implements it.
pub(crate) struct ReplayProjection {
    connection: rusqlite::Connection,
    store_dir: coven_foundation::store_dir::StoreDir,
}

pub(crate) struct ReplayProjectionResult {
    projection: ReplayProjection,
    watched: Option<WatchedReplayOutcome>,
    applied_order: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
    max_updated_at: Option<coven_protocol::hlc::Timestamp>,
}

#[derive(Clone)]
pub(crate) enum WatchedReplayOutcome {
    Applied {
        max_updated_at: Option<coven_protocol::hlc::Timestamp>,
    },
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
        }
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
        transaction: &VerifiedStoreTransaction<'_, '_, '_>,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<coven_foundation::changeset::RowChange>, DbError> {
        transaction.install_replay_projection(root, &self.projection)
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
        cut: &coven_protocol::store_commit::CommitFrontier,
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
                cut,
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
    ) -> Result<Option<crate::MaterializationHold>, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let mut next_private_rows = private_rows.clone();
        let outcome = MergeMaterializationTransaction::from_store(
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
        match outcome {
            None => {
                transaction.commit().map_err(DbError::from)?;
                *private_rows = next_private_rows;
                Ok(None)
            }
            Some(hold) => {
                transaction.rollback().map_err(DbError::from)?;
                Ok(Some(hold))
            }
        }
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
        let mut authority = super::VerifiedStoreAuthority::default();
        records.retain_snapshot_replay_inputs(&mut authority, root)?;
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
    target
        .pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(DbError::from)?;
    let tables = tables
        .iter()
        .map(|table| ProjectionTableRows::load(&source.connection, target, table))
        .collect::<Result<Vec<_>, _>>()?;
    let order = projection_parent_first_order(&source.connection, &tables)?;
    for index in order.iter().rev() {
        let table = &tables[*index];
        table.delete_absent(target)?;
    }
    for index in &order {
        let table = &tables[*index];
        table.install_changed(target)?;
    }
    for table in &tables {
        table.validate_exact(target)?;
    }
    Ok(())
}

fn projection_parent_first_order(
    source: &rusqlite::Connection,
    tables: &[ProjectionTableRows],
) -> Result<Vec<usize>, DbError> {
    let indices = tables
        .iter()
        .enumerate()
        .map(|(index, table)| (table.table.as_str(), index))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut parents = tables
        .iter()
        .map(|table| {
            crate::foreign_key_edges(source, &table.table)
                .map(|edges| {
                    edges
                        .into_iter()
                        .filter_map(|edge| indices.get(edge.parent_table.as_str()).copied())
                        .collect::<std::collections::BTreeSet<_>>()
                })
                .map_err(|error| {
                    DbError::Message(format!(
                        "read projection foreign keys for {}: {error}",
                        table.table
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut remaining = (0..tables.len()).collect::<std::collections::BTreeSet<_>>();
    let mut order = Vec::with_capacity(tables.len());
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .copied()
            .filter(|index| {
                parents[*index]
                    .iter()
                    .all(|parent| parent == index || !remaining.contains(parent))
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            order.extend(remaining.iter().copied());
            break;
        }
        for index in ready {
            remaining.remove(&index);
            order.push(index);
            for dependencies in &mut parents {
                dependencies.remove(&index);
            }
        }
    }
    Ok(order)
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
    source: std::collections::BTreeMap<Vec<ProjectionKeyValue>, Vec<rusqlite::types::Value>>,
    target: std::collections::BTreeMap<Vec<ProjectionKeyValue>, Vec<rusqlite::types::Value>>,
}

impl ProjectionTableRows {
    fn load(
        source: &rusqlite::Connection,
        target: &rusqlite::Connection,
        table: &str,
    ) -> Result<Self, DbError> {
        let (columns, primary_key) = projection_table_columns(source, table)?;
        let (target_columns, target_primary_key) = projection_table_columns(target, table)?;
        if columns != target_columns || primary_key != target_primary_key {
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
        })
    }

    fn delete_absent(&self, target: &rusqlite::Connection) -> Result<(), DbError> {
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
            if self.source.contains_key(key) {
                continue;
            }
            let values = self.primary_key.iter().map(|index| &row[*index]);
            statement
                .execute(rusqlite::params_from_iter(values))
                .map_err(DbError::from)?;
        }
        Ok(())
    }

    fn install_changed(&self, target: &rusqlite::Connection) -> Result<(), DbError> {
        let target_rows =
            projection_table_rows(target, &self.table, &self.columns, &self.primary_key)?;
        let quoted_columns = self
            .columns
            .iter()
            .map(|column| crate::quote_ident(column))
            .collect::<Vec<_>>();
        let placeholders = (1..=self.columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            crate::quote_ident(&self.table),
            quoted_columns.join(", ")
        );
        let mut insert = target.prepare(&insert_sql).map_err(DbError::from)?;
        let non_key = (0..self.columns.len())
            .filter(|index| !self.primary_key.contains(index))
            .collect::<Vec<_>>();
        let assignments = non_key
            .iter()
            .enumerate()
            .map(|(parameter, index)| format!("{} = ?{}", quoted_columns[*index], parameter + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = self
            .primary_key
            .iter()
            .enumerate()
            .map(|(parameter, index)| {
                format!(
                    "{} IS ?{}",
                    quoted_columns[*index],
                    non_key.len() + parameter + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let update_sql = (!non_key.is_empty()).then(|| {
            format!(
                "UPDATE {} SET {assignments} WHERE {predicate}",
                crate::quote_ident(&self.table)
            )
        });
        let mut update = update_sql
            .as_deref()
            .map(|sql| target.prepare(sql).map_err(DbError::from))
            .transpose()?;
        let mut pending = self
            .source
            .iter()
            .filter(|(key, source_row)| {
                target_rows
                    .get(*key)
                    .is_some_and(|target_row| target_row != *source_row)
            })
            .collect::<Vec<_>>();
        while !pending.is_empty() {
            let mut deferred = Vec::new();
            let mut first_conflict = None;
            let mut progressed = false;
            for (key, source_row) in pending {
                let values = non_key
                    .iter()
                    .chain(self.primary_key.iter())
                    .map(|index| &source_row[*index])
                    .collect::<Vec<_>>();
                match execute_projection_update(
                    target,
                    update
                        .as_mut()
                        .expect("a changed row has non-primary-key columns"),
                    &values,
                )? {
                    Ok(1) => progressed = true,
                    Ok(updated) => {
                        return Err(DbError::Message(format!(
                            "retained replay projection updated {updated} rows for {:?} key {key:?}",
                            self.table
                        )));
                    }
                    Err(error) if is_unique_constraint(&error) => {
                        if first_conflict.is_none() {
                            first_conflict = Some(error);
                        }
                        deferred.push((key, source_row));
                    }
                    Err(error) => return Err(DbError::from(error)),
                }
            }
            if !progressed {
                return Err(clone_sqlite_error(
                    first_conflict
                        .as_ref()
                        .expect("deferred update carries its constraint error"),
                ));
            }
            pending = deferred;
        }
        let installed =
            projection_table_rows(target, &self.table, &self.columns, &self.primary_key)?;
        for (key, source_row) in &self.source {
            if !installed.contains_key(key) {
                insert
                    .execute(rusqlite::params_from_iter(source_row))
                    .map_err(DbError::from)?;
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

fn execute_projection_update(
    connection: &rusqlite::Connection,
    statement: &mut rusqlite::Statement<'_>,
    values: &[&rusqlite::types::Value],
) -> Result<Result<usize, rusqlite::Error>, DbError> {
    const SAVEPOINT: &str = "coven_replay_projection_update";
    connection
        .execute_batch(&format!("SAVEPOINT {SAVEPOINT}"))
        .map_err(DbError::from)?;
    match statement.execute(rusqlite::params_from_iter(values.iter().copied())) {
        Ok(updated) => {
            connection
                .execute_batch(&format!("RELEASE {SAVEPOINT}"))
                .map_err(DbError::from)?;
            Ok(Ok(updated))
        }
        Err(error) => {
            connection
                .execute_batch(&format!("ROLLBACK TO {SAVEPOINT}; RELEASE {SAVEPOINT}"))
                .map_err(|rollback| {
                    DbError::context(
                        format!("roll back failed projection update after {error}"),
                        rollback,
                    )
                })?;
            Ok(Err(error))
        }
    }
}

fn is_unique_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
    )
}

fn clone_sqlite_error(error: &rusqlite::Error) -> DbError {
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            DbError::from(rusqlite::Error::SqliteFailure(*code, message.clone()))
        }
        _ => DbError::Message(error.to_string()),
    }
}

fn projection_table_columns(
    connection: &rusqlite::Connection,
    table: &str,
) -> Result<(Vec<String>, Vec<usize>), DbError> {
    let pragma = format!("PRAGMA table_info({})", crate::quote_ident(table));
    let columns = crate::query_mapped_rows(connection, &pragma, [], |row| {
        Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
    })?;
    if columns.is_empty() {
        return Err(DbError::Message(format!(
            "retained replay projection table {table:?} is absent"
        )));
    }
    let names = columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let mut primary_key_columns = columns
        .into_iter()
        .filter(|(_, order)| *order > 0)
        .collect::<Vec<_>>();
    primary_key_columns.sort_by_key(|(_, order)| *order);
    let primary_key = primary_key_columns
        .into_iter()
        .map(|(name, _)| {
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
    Ok((names, primary_key))
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

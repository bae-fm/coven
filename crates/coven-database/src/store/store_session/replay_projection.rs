use super::*;

/// A replay-owned SQLite image. Callers can apply or inspect the projection,
/// but cannot obtain the connection that implements it.
pub(crate) struct ReplayProjection {
    connection: rusqlite::Connection,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl ReplayProjection {
    pub(super) fn from_image(
        image: &[u8],
        store_dir: coven_foundation::store_dir::StoreDir,
    ) -> Result<Self, DbError> {
        let mut connection = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(&mut connection, image)
            .map_err(|error| DbError::context("open retained replay database image", error))?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
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
    ) -> Result<crate::MaterializationOutcome, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
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
        )?;
        match applied.outcome {
            outcome @ crate::MaterializationOutcome::Applied(_) => {
                transaction.commit().map_err(DbError::from)?;
                Ok(outcome)
            }
            outcome @ crate::MaterializationOutcome::Held(_) => {
                transaction.rollback().map_err(DbError::from)?;
                Ok(outcome)
            }
        }
    }

    pub(super) fn apply_write_overlay(
        &self,
        overlay: crate::MergeReplayWriteOverlay,
        schema: std::sync::Arc<TableSchema>,
    ) -> Result<(), DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        transaction
            .pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        let partitions = overlay
            .partitions
            .store
            .into_iter()
            .chain(overlay.partitions.circles)
            .chain(overlay.partitions.local);
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::context(
                        format!("local replay write {} changeset", overlay.write_id),
                        error,
                    )
                })?;
            let applied = MergeMaterializationTransaction::from_store(
                crate::store::store_session::StoreTransaction::new(&transaction, &self.store_dir),
            )
            .apply_changeset(changeset, IncomingTimestampPolicy::LocallyAuthored)?;
            if applied.had_fk_violations || !applied.constraint_conflict_tables.is_empty() {
                return Err(DbError::Message(format!(
                    "local replay write {} conflicts with accepted history",
                    overlay.write_id
                )));
            }
        }
        let violations: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Err(DbError::Message(format!(
                "local replay write {} violates foreign keys",
                overlay.write_id
            )));
        }
        transaction.commit().map_err(DbError::from)
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
    for table in tables {
        crate::copy_table_with_conflicts(&source.connection, target, table, false)?;
    }
    Ok(())
}

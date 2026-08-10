use super::*;

/// A replay-owned SQLite image. Callers can apply or inspect the projection,
/// but cannot obtain the connection that implements it.
pub(crate) struct ReplayProjection {
    connection: rusqlite::Connection,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl ReplayProjection {
    pub(super) fn new(
        connection: rusqlite::Connection,
        store_dir: coven_foundation::store_dir::StoreDir,
    ) -> Self {
        Self {
            connection,
            store_dir,
        }
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
        let source = crate::open_database_image(image_bytes)
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
    ) -> Result<coven_protocol::membership::ApplyOutcome, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let applied = MergeMaterializationTransaction::new(&transaction, &self.store_dir)
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
            outcome @ coven_protocol::membership::ApplyOutcome::Applied(_) => {
                transaction.commit().map_err(DbError::from)?;
                Ok(outcome)
            }
            outcome @ coven_protocol::membership::ApplyOutcome::Held(_) => {
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
            let applied = MergeMaterializationTransaction::new(&transaction, &self.store_dir)
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
        .map_err(|error| DbError::Message(error.to_string()))
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

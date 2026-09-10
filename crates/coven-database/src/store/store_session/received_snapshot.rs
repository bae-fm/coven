use super::*;

impl StoreSession<'_> {
    pub(crate) fn prepare_received_snapshot_circles(
        &mut self,
        selection: &crate::StagedCircleRestore,
        receiver_wall_ms: u64,
    ) -> Result<(), DbError> {
        let root = self.required_root_authority()?;
        let transaction = self.conn.unchecked_transaction()?;
        StoreTransaction::new(&transaction, self.store_dir).restore_device_join_snapshot_circles(
            &root,
            selection,
            self.synced_tables,
            self.blob_decls,
            receiver_wall_ms,
        )?;
        transaction.commit()?;
        self.verified_store_authority
            .forget_superseded_replay_baseline();
        Ok(())
    }
}

/// A closed, migrated snapshot image and the directory owning its payloads.
/// Installation opens the image only within the receiving database operation.
pub struct PreparedStoreSnapshot {
    image: crate::SnapshotDatabaseImage,
    directory: SnapshotPreparationDirectory,
}

impl PreparedStoreSnapshot {
    pub(crate) fn seal(core: crate::DatabaseCore) -> Result<Self, DbError> {
        let (bytes, directory) = core.serialize_and_close_snapshot()?;
        let image =
            crate::SnapshotDatabaseImage::create(directory.path.join("prepared.sqlite"), &bytes)
                .map_err(DbError::from);
        match image {
            Ok(image) => Ok(Self { image, directory }),
            Err(error) => directory.finish(Err(error)),
        }
    }

    pub(crate) fn install_on(
        self,
        receiver: &mut crate::store::StoreSession<'_>,
        expected: crate::StorePublicationBoundary,
        materializations: Vec<crate::PreparedMergeMaterialization>,
        accepted: crate::AcceptedStorePublicationInterval,
        mut snapshots: Vec<coven_protocol::store_commit::RetainedReplaySnapshotAuthority>,
        membership: coven_protocol::membership::LocalStoreMembership,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<
        (
            crate::store::AppliedMergeMaterialization,
            Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ),
        DbError,
    > {
        let Self { image, directory } = self;
        let outcome = (|| {
            let bytes = image.read_and_discard().map_err(DbError::from)?;
            let mut source = rusqlite::Connection::open_in_memory()?;
            crate::connection_io::deserialize_database_image_into(&mut source, &bytes)?;
            let source_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(&directory.path);
            let source_version: u32 =
                source.pragma_query_value(None, "user_version", |row| row.get(0))?;
            let source_routing = crate::database_open::load_coven_metadata(&source)?;
            if source_version != receiver.schema_version
                || source_routing.hash() != receiver.sync_routing_hash
            {
                return Err(DbError::Message(
                    "prepared checkpoint differs from the receiver schema".into(),
                ));
            }
            let root = receiver.required_root_authority()?;
            let source_records = StoreRecords::new(&source, &source_dir);
            let mut source_authority = VerifiedStoreAuthority::default();
            if source_authority.required_root_authority_on(source_records)? != root {
                return Err(DbError::Message(
                    "prepared checkpoint belongs to another Store".into(),
                ));
            }
            let baseline =
                retained_replay::load_replay_baseline_on(source_records)?.ok_or_else(|| {
                    DbError::Message("prepared checkpoint has no replay baseline".into())
                })?;
            let crate::RetainedReplayAuthority::InstalledSnapshot(checkpoint) = &baseline.authority
            else {
                return Err(DbError::Message(
                    "prepared checkpoint has genesis authority".into(),
                ));
            };
            let snapshot = accepted
                .interval()
                .current()
                .latest_snapshot()
                .cloned()
                .ok_or_else(|| {
                    DbError::Message("received interval has no accepted checkpoint".into())
                })?;
            if checkpoint.snapshot != snapshot.snapshot {
                return Err(DbError::Message(
                    "prepared image is not the received current checkpoint".into(),
                ));
            }
            snapshots.push(checkpoint.clone());
            let inputs = source_authority
                .retained_replay_inputs_on(source_records, &root)
                .map_err(|error| DbError::context("checkpoint source retained inputs", error))?;
            let source_floor = crate::connection_io::parse_seed(
                crate::get_protocol_state_on(&source, coven_protocol::hlc::HIGHWATER_STATE_KEY)?,
                "received checkpoint clock floor",
            )?;
            let row_floor = crate::connection_io::parse_seed(
                crate::connection_io::scan_max_updated_at(
                    &source,
                    receiver.synced_tables,
                    receiver_wall_ms.saturating_add(coven_protocol::hlc::MAX_FUTURE_SKEW_MS),
                )?,
                "received checkpoint row clock",
            )?;
            let schema_version = receiver.schema_version;
            let routing_hash = receiver.sync_routing_hash;
            receiver.verified_store_transaction(|transaction| {
                if observed_store_publication::load_store_current_publication_on(
                    transaction.store.transaction,
                )? != expected
                {
                    return Err(DbError::StorePublicationChanged);
                }
                let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
                    materialized_commit_index::materialized_frontier_on(
                        transaction.store.transaction,
                        None,
                    )?,
                )?;
                let (local_image, folded) = transaction
                    .capture_replay_baseline_at_cut(
                        &root,
                        &frontier,
                        &frontier,
                        snapshot.snapshot.snapshot_hash,
                        routing_encryption,
                    )
                    .map_err(|error| {
                        DbError::context("checkpoint receiver replay capture", error)
                    })?;
                let covered_suffix = StoreDatabase::covered_replay_suffix_on(
                    StoreRecords::new(transaction.store.transaction, transaction.store.store_dir),
                    &baseline,
                    &folded,
                )?;
                let image = source_records
                    .received_snapshot_image_with_local_rows(
                        &local_image,
                        transaction.gates,
                        &covered_suffix,
                    )
                    .map_err(|error| DbError::context("checkpoint Local rows", error))?;
                transaction
                    .store
                    .import_snapshot_device_states(source_records)
                    .map_err(|error| DbError::context("checkpoint device states", error))?;
                let installed_baseline = transaction
                    .store
                    .replace_received_snapshot_baseline(
                        source_records,
                        &baseline,
                        image,
                        &folded,
                        &inputs,
                        transaction.blob_decls,
                    )
                    .map_err(|error| DbError::context("checkpoint baseline replacement", error))?;
                observed_store_publication::install_store_checkpoint_publication_on(
                    transaction.store.transaction,
                    &expected,
                    &accepted,
                    checkpoint,
                )
                .map_err(|error| DbError::context("checkpoint publication boundary", error))?;
                transaction
                    .store
                    .import_received_snapshot_inputs(source_records, &inputs, &installed_baseline)
                    .map_err(|error| {
                        DbError::context("checkpoint retained inputs import", error)
                    })?;
                transaction
                    .store
                    .import_received_snapshot_blob_inventory(source_records)
                    .map_err(|error| DbError::context("checkpoint blob inventory", error))?;
                // The replacement was validated against the received image. Its
                // registration rows reach the live projection below, so reopening
                // it against the previous projection would mix those two states.
                transaction
                    .authority
                    .replace_installed_replay_baseline(installed_baseline)?;
                transaction.clock_floor = source_floor.clone().max(row_floor.clone());
                if let Some(floor) = &transaction.clock_floor {
                    transaction.clock.advance_past(floor);
                }
                let replay = accepted.interval().clone();
                let applied = transaction
                    .apply_received_store_publication_interval(
                        materializations,
                        accepted,
                        replay,
                        snapshots,
                        membership,
                        schema_version,
                        routing_hash,
                        routing_encryption,
                        routing_key,
                        receiver_wall_ms,
                        Some(snapshot),
                    )
                    .map_err(|error| DbError::context("checkpoint received interval", error))?;
                if matches!(applied.0.outcome, crate::MaterializationOutcome::Applied(_)) {
                    transaction.store.retain_snapshot_device_states(
                        &mut *transaction.authority,
                        &root,
                        checkpoint.metadata.coverage.clone().into_refs(),
                    )?;
                    let installed_records = StoreRecords::new(
                        transaction.store.transaction,
                        transaction.store.store_dir,
                    );
                    let installed = retained_replay::load_replay_baseline_on(installed_records)?
                        .ok_or_else(|| {
                            DbError::Message(
                                "received checkpoint installation lost its baseline".into(),
                            )
                        })?;
                    if installed.authority != baseline.authority {
                        return Err(DbError::Message(
                            "received checkpoint installation changed its accepted authority"
                                .into(),
                        ));
                    }
                    installed_records.declared_store_device_state(
                        &checkpoint.metadata.history_summary.post_state,
                    )?;
                    installed_records.store_device_state_for_history_cut(
                        &coven_protocol::store_commit::StoreHistoryCut(
                            coven_protocol::store_commit::CommitFrontier::from_refs(
                                installed_records.materialized_frontier()?,
                            )?
                            .0,
                        ),
                    )?;
                    Ok(StoreTransactionOutcome::Commit(applied))
                } else {
                    Ok(StoreTransactionOutcome::Rollback(applied))
                }
            })
        })();
        directory.finish(outcome)
    }

    /// Release a preparation whose verified interval cannot be installed.
    #[cfg(test)]
    pub(crate) fn discard(self) -> Result<(), DbError> {
        self.finish(Ok(()))
    }

    #[cfg(test)]
    fn finish<T>(self, outcome: Result<T, DbError>) -> Result<T, DbError> {
        let outcome = match self.image.finish_operation(outcome) {
            Ok(value) => Ok(value),
            Err(crate::SnapshotImageOperationError::Operation(error)) => Err(error),
            Err(crate::SnapshotImageOperationError::Cleanup { path, cleanup }) => {
                Err(crate::SnapshotImageError::Cleanup { path, cleanup }.into())
            }
            Err(crate::SnapshotImageOperationError::CleanupAfterFailure {
                path,
                cleanup,
                cause,
            }) => Err(crate::SnapshotImageError::CleanupAfterFailure {
                path,
                cleanup,
                cause: Box::new(crate::SnapshotImageError::from(cause)),
            }
            .into()),
        };
        self.directory.finish(outcome)
    }
}

/// Created exclusively for one preparation, including its file-backed payloads.
/// The connection is declared before this guard so it closes before cleanup.
pub(crate) struct SnapshotPreparationDirectory {
    path: std::path::PathBuf,
    armed: bool,
}

impl SnapshotPreparationDirectory {
    pub(crate) fn after_close<T>(
        self,
        outcome: Result<T, DbError>,
        closed: Result<(), DbError>,
    ) -> Result<(T, Self), DbError> {
        let outcome = match (outcome, closed) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(operation), Err(close)) => Err(crate::SnapshotImageError::CleanupAfterFailure {
                path: self.path.clone(),
                cleanup: close.to_string(),
                cause: Box::new(crate::SnapshotImageError::from(operation)),
            }
            .into()),
        };
        match outcome {
            Ok(value) => Ok((value, self)),
            Err(error) => self.finish(Err(error)),
        }
    }

    pub(crate) fn create(path: std::path::PathBuf) -> Result<Self, DbError> {
        std::fs::create_dir(&path)
            .map_err(|error| DbError::context("create snapshot preparation directory", error))?;
        Ok(Self { path, armed: true })
    }

    pub(crate) fn finish<T>(mut self, outcome: Result<T, DbError>) -> Result<T, DbError> {
        let cleanup = std::fs::remove_dir_all(&self.path);
        self.armed = false;
        match (outcome, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup)) => Err(crate::SnapshotImageError::Cleanup {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
            }
            .into()),
            (Err(operation), Err(cleanup)) => Err(crate::SnapshotImageError::CleanupAfterFailure {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
                cause: Box::new(crate::SnapshotImageError::from(operation)),
            }
            .into()),
        }
    }
}

impl Drop for SnapshotPreparationDirectory {
    fn drop(&mut self) {
        if self.armed {
            if let Err(error) = std::fs::remove_dir_all(&self.path) {
                tracing::warn!(path = %self.path.display(), %error, "could not remove abandoned snapshot preparation");
            }
        }
    }
}

#[cfg(test)]
#[path = "received_snapshot_tests.rs"]
mod tests;

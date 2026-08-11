use super::*;

impl StoreSync {
    #[cfg(test)]
    pub(super) fn loop_uses_connected_storage(&self) -> bool {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, storage, .. } => sync.uses_storage_for_test(storage),
            _ => false,
        }
    }

    #[cfg(test)]
    pub(super) fn stopped_loop_count(&self) -> u64 {
        self.stopped_loops.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn stop_loop(&self) -> Result<(), SyncError> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => sync.stop().map_err(SyncError::Loop),
            _ => Err(SyncError::LoopNotRunning),
        }
    }

    #[cfg(test)]
    pub(crate) async fn create_test_store(
        &self,
        store_id: &str,
        signer: coven_keys::keys::UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<std::sync::Arc<coven_replication::sync::test_helpers::TestStore>, String> {
        coven_replication::sync::test_helpers::TestStore::create_with_database(
            self.database.clone(),
            self.store_dir.clone(),
            store_id,
            signer,
            home,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_test_store(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
    ) -> Result<bool, String> {
        store
            .publish_pending_store_database(&self.database, &self.store_dir)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn pull_test_store(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            coven_replication::sync::store::StorePullResult,
        ),
        coven_replication::sync::cycle::SyncCycleFailure,
    > {
        let device = store
            .open_into_store_database(&self.database, self.store_dir.clone())
            .await
            .map_err(coven_replication::sync::cycle::SyncCycleFailure::from)?;
        let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let mut authorization = device.authorize_writer().await.map_err(|error| {
            coven_replication::sync::cycle::SyncCycleFailure::operation(
                "authorize Store writer",
                error,
            )
        })?;
        let result = authorization.pull(Some(&routing_encryption)).await?;
        let sequences = result
            .frontier
            .iter()
            .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
            .collect();
        Ok((sequences, result))
    }

    #[cfg(test)]
    pub(crate) async fn prepare_test_join_snapshot(
        &self,
        store: &coven_replication::sync::test_helpers::TestStore,
        owner: &coven_keys::keys::UserKeypair,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), String> {
        let owner_device = store
            .bind_store_device(&self.database, self.store_dir.clone(), owner)
            .await?;
        let snapshot = self
            .database
            .capture_snapshot_image_for_test(store.root().clone(), snapshot_path, None)
            .await
            .map_err(|error| error.to_string())?;
        let coverage =
            coven_protocol::store_commit::CommitFrontier(std::collections::BTreeMap::new());
        owner_device
            .publish_snapshot(snapshot, coverage.clone())
            .await?;
        owner_device.publish_acknowledgement(coverage).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), coven_database::DbError> {
        self.database
            .latest_materialized_commit_coordinate_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) fn arm_pull_after_remote_commit_for_test(
        &self,
        device_id: String,
        sequence: u64,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.database
            .arm_test_pause(coven_database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id,
                seq: sequence,
            })
    }

    #[cfg(test)]
    pub(crate) fn loop_uses_connected_storage_for_test(&self) -> bool {
        self.loop_uses_connected_storage()
    }

    #[cfg(test)]
    pub(crate) fn stopped_loop_count_for_test(&self) -> u64 {
        self.stopped_loop_count()
    }

    #[cfg(test)]
    pub(crate) fn stop_loop_for_test(&self) -> Result<(), SyncError> {
        self.stop_loop()
    }

    #[cfg(test)]
    pub(crate) fn connected_store_id_for_test(&self) -> Option<String> {
        Some(self.connected()?.config().store_id.clone())
    }

    #[cfg(test)]
    pub(crate) fn connected_uses_store_dir_for_test(
        &self,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> bool {
        self.connected()
            .is_some_and(|sync| sync.uses_store_dir_for_test(store_dir))
    }

    #[cfg(test)]
    pub(crate) fn connected_blob_path_scheme_for_test(&self) -> Option<BlobPathScheme> {
        Some(self.connected()?.blob_path_scheme())
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: EncryptionService,
    ) -> Result<(), SyncError> {
        self.connected()
            .ok_or(SyncError::LoopNotRunning)?
            .adopt_key_rotation_for_test(encryption)
            .map(|_| ())
            .map_err(SyncError::from)
    }

    #[cfg(test)]
    pub(crate) fn encryption_generation_for_test(&self) -> Option<u64> {
        self.connected()?.encryption_generation_for_test()
    }

    #[cfg(test)]
    pub(crate) fn open_sealed_blob_for_test(
        &self,
        bytes: &[u8],
        context: &[u8],
    ) -> Result<(coven_keys::encryption::KeyFingerprint, Vec<u8>), StorageError> {
        self.connected()
            .ok_or_else(|| StorageError::Storage("sync connection is not installed".to_string()))?
            .open_sealed_blob_for_test(bytes, context)
            .map_err(StorageError::Storage)
    }

    #[cfg(test)]
    pub(crate) fn has_remote_storage_for_test(&self) -> bool {
        self.has_cloud()
    }
}

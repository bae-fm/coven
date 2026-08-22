use super::*;

/// Store operations backed by one owned database.
///
/// This capability retains the database as a whole. It never copies out the
/// connection, Store directory, schema configuration, clock, gates, blob
/// declarations, or coordination state that database owns.
#[derive(Clone)]
pub struct StoreDatabase {
    database: Database,
}

impl StoreDatabase {
    #[doc(hidden)]
    pub fn from_database(database: Database) -> Self {
        Self { database }
    }

    #[doc(hidden)]
    pub fn subscribe_committed_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<std::sync::Arc<crate::CommittedChanges>> {
        self.database.subscribe_committed_changes()
    }

    pub(super) async fn call_store<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(&mut StoreSession<'session>) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.database.call_store(operation).await
    }

    pub(super) async fn call_database<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(
                &mut crate::database_session::DatabaseSession<'session>,
            ) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.database.call_database(operation).await
    }

    pub async fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_store(move |session| session.read(read)).await
    }

    pub async fn read_tracked<F, R, E>(
        &self,
        read: F,
    ) -> Result<(Result<R, E>, crate::QueryDependencies), DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_store(move |session| session.read_tracked(read))
            .await
    }

    pub fn schema_version(&self) -> u32 {
        self.database.store_schema_version()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn assert_owns_payload_directory_for_test(
        &self,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) {
        self.database
            .assert_owns_payload_directory_for_test(store_dir);
    }

    pub fn sync_routing_hash(&self) -> coven_protocol::store_commit::ObjectHash {
        self.database.store_sync_routing_hash()
    }

    pub fn has_synced_tables(&self) -> bool {
        self.database.store_has_synced_tables()
    }

    pub fn blob_transition_root(&self, table_name: &str) -> crate::BlobTransitionRoot {
        self.database.store_blob_transition_root(table_name)
    }

    pub fn transfer_limits(&self) -> coven_protocol::blob::TransferLimits {
        self.database.store_transfer_limits()
    }

    /// Replace the transfer limits for every later upload-drain pass and pin
    /// call. A pass already running keeps the limit it admitted under.
    pub fn set_transfer_limits(&self, limits: coven_protocol::blob::TransferLimits) {
        self.database.set_store_transfer_limits(limits)
    }

    pub fn blob_tombstone_grace(&self) -> chrono::Duration {
        self.database.store_blob_tombstone_grace()
    }

    pub fn has_scoped_graph(&self) -> bool {
        self.database.store_has_scoped_graph()
    }

    pub fn stamp(&self) -> String {
        self.database.store_stamp()
    }

    pub async fn persist_hlc_high_water(&self) -> Result<(), DbError> {
        self.set_protocol_state(
            coven_protocol::hlc::HIGHWATER_STATE_KEY,
            &self.database.store_hlc_high_water(),
        )
        .await
    }

    pub fn blob_ref_from_change(
        &self,
        change: &coven_foundation::changeset::RowChange,
    ) -> Result<Option<coven_protocol::blob::BlobRef>, crate::BlobDeclError> {
        self.database.store_blob_ref_from_change(change)
    }

    pub fn validate_local_blob_cleanup_changes(
        &self,
        old_changes: &[coven_foundation::changeset::RowChange],
        new_changes: &[coven_foundation::changeset::RowChange],
    ) -> Result<(), crate::BlobDeclError> {
        self.database
            .validate_store_local_blob_cleanup_changes(old_changes, new_changes)
    }

    pub fn receive_wall_ms(&self) -> u64 {
        self.database.store_receive_wall_ms()
    }

    pub fn new_store_write_id(&self) -> coven_protocol::write::WriteId {
        coven_protocol::write::WriteId::from_generated(self.database.new_store_id())
    }

    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call_store(move |session| session.protocol_state(&key))
            .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let key = key.to_string();
        let value = value.to_string();
        self.call_store(move |session| session.set_protocol_state(&key, &value))
            .await
    }

    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = cache_budget_state_key(namespace);
        match self.get_protocol_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|error| {
                DbError::context(
                    format!("cache budget for {namespace:?} in protocol_state is not a byte count"),
                    error,
                )
            }),
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, &max_bytes.to_string()).await
    }

    pub async fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.call_store(move |session| session.write_status(&write_id))
            .await
    }

    pub fn notify_write_status(
        &self,
        write_id: coven_protocol::write::WriteId,
        status: coven_protocol::write::WriteStatus,
    ) {
        self.database.notify_store_write_status(write_id, status);
    }

    pub(super) fn subscribe_store_write_status(
        &self,
        write_id: coven_protocol::write::WriteId,
        current: coven_protocol::write::WriteStatus,
    ) -> tokio::sync::watch::Receiver<coven_protocol::write::WriteStatus> {
        self.database
            .subscribe_store_write_status(write_id, current)
    }

    pub async fn membership_load_permit(&self) -> MembershipLoadPermit {
        self.database.membership_load_permit().await
    }

    pub async fn membership_mutation_permit(&self) -> MembershipMutationPermit {
        self.database.membership_mutation_permit().await
    }

    pub async fn store_creation_permit(&self) -> StoreCreationPermit {
        self.database.store_creation_permit().await
    }

    pub async fn device_exclusion_permit(&self) -> DeviceExclusionPermit {
        self.database.device_exclusion_permit().await
    }

    /// Wait for this device's turn to author its own next Store commit.
    ///
    /// Every path that reads the local position to compose a commit, and every
    /// path that publishes a device head, takes this and holds it across the
    /// pair. Never taken twice in one call chain: a composer holds it until its
    /// candidate is either activated or durably persisted, and a publisher of an
    /// already-persisted candidate takes it for that publication alone.
    pub async fn author_own_stream(&self) -> OwnStreamAuthorship {
        self.database.author_own_store_stream().await
    }

    /// Wait for this drain's exclusive turn over the blob upload queue.
    ///
    /// Taken before the queue is read and held for the whole pass, so the
    /// entries a drain admits are entries no other drain is already running.
    /// Never taken twice in one call chain: an upload attempt performs no
    /// second drain.
    pub async fn blob_upload_drain_permit(&self) -> BlobUploadDrainPermit {
        self.database.blob_upload_drain_permit().await
    }

    pub async fn snapshot_publication_permit(&self) -> SnapshotPublicationPermit {
        self.database.snapshot_publication_permit().await
    }

    pub(super) async fn local_blob_cleanup_permit(&self) -> LocalBlobCleanupPermit {
        self.database.local_blob_cleanup_permit().await
    }

    pub(super) async fn apply_local_blob_cleanup_intent(
        &self,
        intent: &crate::local_blob_cleanup_intents::LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        self.database.apply_local_blob_cleanup_intent(intent).await
    }

    pub(super) async fn stage_host_write_blobs<E>(
        &self,
        blobs: Vec<super::NewBlob>,
    ) -> Result<super::StagedBlobBatch, crate::HostWriteError<E>> {
        self.database.stage_host_write_blobs(blobs).await
    }

    pub async fn begin_store_creation_attempt(
        &self,
        initialized: coven_protocol::store_creation::StoreCreationAttempt,
    ) -> Result<coven_protocol::store_creation::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized)
            .map_err(|error| DbError::context("serialize Store creation attempt", error))?;
        self.call_store(move |session| session.begin_store_creation_attempt(&value))
            .await
    }

    pub async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<coven_protocol::store_creation::StoreCreationAttempt>, DbError> {
        self.call_store(|session| session.load_store_creation_attempt())
            .await
    }

    pub async fn advance_store_creation_attempt(
        &self,
        previous: coven_protocol::store_creation::StoreCreationAttempt,
        next: coven_protocol::store_creation::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous)
            .map_err(|error| DbError::context("serialize Store creation predecessor", error))?;
        let next = serde_json::to_string(&next)
            .map_err(|error| DbError::context("serialize Store creation successor", error))?;
        self.call_store(move |session| session.advance_store_creation_attempt(&previous, &next))
            .await
    }

    pub(super) async fn sync_store_parent_dir(
        &self,
        path: &std::path::Path,
    ) -> Result<(), coven_foundation::atomic_file::FileError> {
        self.database.sync_store_parent_dir(path).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new(database: &Database) -> Self {
        Self::from_database(database.clone())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn arm_test_pause(
        &self,
        point: crate::DatabaseTestPoint,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.database.arm_test_pause(point)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn set_invalid_cache_budget_for_test(
        &self,
        namespace: &str,
        value: &str,
    ) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, value).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn reach_test_point(&self, point: crate::DatabaseTestPoint) {
        self.database.reach_store_test_point(point).await;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn required_store_root_hash(
        &self,
    ) -> Result<coven_protocol::store_commit::ObjectHash, DbError> {
        self.call_store(|session| Ok(session.required_root_authority()?.store_root_hash))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn scoped_snapshot_counts_for_test(&self) -> Result<(i64, i64, i64), DbError> {
        self.call_store(|session| session.scoped_snapshot_counts())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), DbError> {
        self.call_store(|session| session.migrated_scoped_snapshot_facts())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn generation_zero_replay_baseline_for_test(
        &self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.call_store(|session| session.generation_zero_replay_baseline())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn replace_generation_zero_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.replace_generation_zero_replay_authority(&authority_bytes)
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_bootstrap_coverage_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        self.call_store(move |session| session.circle_bootstrap_coverage_ref(circle_id))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        self.call_store(|session| session.circle_bootstrap_replay_inputs())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_control_activation_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.call_store(move |session| session.circle_control_activation_count(circle_id))
            .await
    }
}

impl coven_foundation::id_provider::IdProvider for StoreDatabase {
    fn new_id(&self) -> String {
        self.database.new_store_id()
    }
}

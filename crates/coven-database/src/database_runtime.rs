use super::*;

/// Bind a database to the store directory that owns its payload files.
///
/// Production databases live under their store directory. In-memory databases
/// have no parent path, so the database opening boundary creates their
/// process-local directory once and passes that dependency into the core.
fn store_dir_of(path: &Path) -> coven_foundation::store_dir::StoreDir {
    if path == Path::new(":memory:") {
        return coven_foundation::store_dir::StoreDir::new_ephemeral(
            std::env::temp_dir().join(format!("coven-in-memory-store-{}", uuid::Uuid::new_v4())),
        );
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    coven_foundation::store_dir::StoreDir::new(parent)
}

impl Database {
    pub(crate) async fn call_database<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(
                &mut crate::database_session::DatabaseSession<'session>,
            ) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.connection.call_database(operation).await
    }

    pub(crate) async fn call_store<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(&mut crate::store::StoreSession<'session>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.connection.call_store(operation).await
    }

    pub(crate) fn store_schema_version(&self) -> u32 {
        self.state.schema_version
    }

    pub(crate) fn store_sync_routing_hash(&self) -> ObjectHash {
        self.state.sync_routing_hash
    }

    pub(crate) fn store_synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    pub(crate) fn store_transfer_limits(&self) -> coven_protocol::blob::TransferLimits {
        self.state.transfer_limits
    }

    pub(crate) fn store_blob_tombstone_grace(&self) -> chrono::Duration {
        self.state.blob_tombstone_grace
    }

    pub(crate) fn store_has_scoped_graph(&self) -> bool {
        self.state.gates.has_scoped_graph()
    }

    pub(crate) fn store_stamp(&self) -> String {
        self.state.hlc.now().to_string()
    }

    pub(crate) fn store_hlc_high_water(&self) -> String {
        self.state.hlc.high_water().to_string()
    }

    pub(crate) fn store_blob_ref_from_change(
        &self,
        change: &coven_foundation::changeset::RowChange,
    ) -> Result<Option<coven_protocol::blob::BlobRef>, BlobDeclError> {
        self.state.blob_decls.ref_from_change(change)
    }

    pub(crate) fn validate_store_local_blob_cleanup_changes(
        &self,
        old_changes: &[coven_foundation::changeset::RowChange],
        new_changes: &[coven_foundation::changeset::RowChange],
    ) -> Result<(), BlobDeclError> {
        crate::local_blob_cleanup_intents::intents_from_changes(
            self.state.blob_decls.as_ref(),
            old_changes,
            new_changes,
        )
        .map(|_| ())
    }

    pub(crate) fn store_receive_wall_ms(&self) -> u64 {
        self.state.hlc.wall_now_ms()
    }

    pub(crate) fn new_store_id(&self) -> String {
        self.state.ids.new_id()
    }

    pub(crate) fn notify_store_write_status(&self, write_id: WriteId, status: WriteStatus) {
        let senders = self
            .state
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        if let Some(sender) = senders.get(&write_id) {
            sender.send_replace(status);
        }
    }

    pub(crate) fn subscribe_store_write_status(
        &self,
        write_id: WriteId,
        current: WriteStatus,
    ) -> tokio::sync::watch::Receiver<WriteStatus> {
        let mut senders = self
            .state
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        let sender = senders
            .entry(write_id)
            .or_insert_with(|| tokio::sync::watch::channel(current.clone()).0);
        sender.send_replace(current);
        sender.subscribe()
    }

    pub(crate) async fn membership_load_permit(&self) -> crate::store::MembershipLoadPermit {
        self.state.store_runtime.membership_load_permit().await
    }

    pub(crate) async fn membership_mutation_permit(
        &self,
    ) -> crate::store::MembershipMutationPermit {
        self.state.store_runtime.membership_mutation_permit().await
    }

    pub(crate) async fn store_creation_permit(&self) -> crate::store::StoreCreationPermit {
        self.state.store_runtime.store_creation_permit().await
    }

    pub(crate) async fn device_exclusion_permit(&self) -> crate::store::DeviceExclusionPermit {
        self.state.store_runtime.device_exclusion_permit().await
    }

    pub(crate) async fn author_own_store_stream(&self) -> crate::store::OwnStreamAuthorship {
        self.state.store_runtime.author_own_stream().await
    }

    pub(crate) async fn snapshot_publication_permit(
        &self,
    ) -> crate::store::SnapshotPublicationPermit {
        self.state.store_runtime.snapshot_publication_permit().await
    }

    pub(crate) async fn local_blob_cleanup_permit(&self) -> crate::store::LocalBlobCleanupPermit {
        self.state.store_runtime.local_blob_cleanup_permit().await
    }

    pub(crate) async fn apply_local_blob_cleanup_intent(
        &self,
        intent: &crate::local_blob_cleanup_intents::LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        intent.apply(&self.state.store_dir).await
    }

    pub(crate) async fn stage_host_write_blobs<E>(
        &self,
        blobs: Vec<crate::store::NewBlob>,
    ) -> Result<crate::store::StagedBlobBatch, crate::HostWriteError<E>> {
        crate::store::StagedBlobBatch::stage(&self.state.store_dir, blobs).await
    }

    pub(crate) async fn sync_store_parent_dir(&self, path: &Path) -> Result<(), String> {
        self.state.store_dir.sync_parent_dir(path).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn store_merge_materialization_failure_injection(
        &self,
    ) -> MergeMaterializationFailureInjection {
        StoreDatabaseTestAccess {
            pause_points: self.state.test_pause_points.clone(),
            merge_materialization_failure: self.state.merge_materialization_failure.clone(),
        }
        .merge_materialization_failure_injection()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn reach_store_test_point(&self, point: DatabaseTestPoint) {
        self.state.test_pause_points.reach(point).await;
    }

    /// Open and own the connection at `path`.
    ///
    /// Runs the host migration ladder and validates its final sync-routing
    /// contract in one transaction. A fresh database creates Coven metadata in
    /// that transaction; an initialized database commits only when the final
    /// contract exactly matches its pinned bytes. Then seeds the register clock
    /// from on-disk rows. The `_updated_at` stamper remains inside the database
    /// boundary and is used by every synced-row write.
    pub fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc = Hlc::try_new(device_id, clock).map_err(|e| DbError::context("device_id", e))?;
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            Arc::new(hlc),
            migrations,
            CovenMetadataOpen::Detect,
        )
    }

    pub fn open_initialized_store(
        path: &Path,
        install: &VerifiedSnapshotBootstrapInstall,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc = Hlc::try_new(device_id, clock).map_err(|e| DbError::context("device_id", e))?;
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            Arc::new(hlc),
            migrations,
            CovenMetadataOpen::VerifiedSnapshot(install),
        )
    }

    fn open_with_hlc_and_coven_metadata(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen<'_>,
    ) -> Result<Database, OpenError> {
        let store_dir = store_dir_of(path);
        Self::open_with_hlc_and_coven_metadata_in_store_dir(
            path,
            store_dir,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            migrations,
            metadata_open,
        )
    }

    fn open_with_hlc_and_coven_metadata_in_store_dir(
        path: &Path,
        store_dir: coven_foundation::store_dir::StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen<'_>,
    ) -> Result<Database, OpenError> {
        let (core, state) = DatabaseCore::open(
            path,
            store_dir,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            migrations,
            metadata_open,
        )?;

        let database = Database {
            connection: DatabaseConnection::start(core, "coven-db")?,
            state,
        };

        Ok(database)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_in_store_dir_for_test(
        path: &Path,
        store_dir: coven_foundation::store_dir::StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc = Hlc::try_new(device_id, clock).map_err(|e| DbError::context("device_id", e))?;
        Self::open_with_hlc_in_store_dir_for_test(
            path,
            store_dir,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            Arc::new(hlc),
            migrations,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_with_hlc_in_store_dir_for_test(
        path: &Path,
        store_dir: coven_foundation::store_dir::StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        Self::open_with_hlc_and_coven_metadata_in_store_dir(
            path,
            store_dir,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            migrations,
            CovenMetadataOpen::Detect,
        )
    }

    /// Open the store at `path` read-only for a same-store secondary reader
    /// (e.g. a separate process reading while another holds the writer open).
    ///
    /// Distinct from [`Database::open`] in three ways, all so the reader never
    /// mutates shared state a concurrent writer owns: the connection is
    /// `SQLITE_OPEN_READONLY`; no migration ladder or bookkeeping DDL runs (it
    /// opens against the schema the writer left, and refuses one newer than this
    /// binary knows — the writer's `SchemaTooNew` policy); and it returns no
    /// stamper, because a reader mints no `_updated_at`. Reads are safe across
    /// processes because the writer opens the db in WAL mode, so a reader observes
    /// committed rows while the writer commits more.
    ///
    /// The caller takes no store open-lock for a read-only open: the exclusive
    /// advisory lock guards against a second *writer*, and a read-only connection
    /// cannot write, so multiple readers and one writer coexist under WAL.
    pub fn open_read_only(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc = Hlc::try_new(device_id, clock).map_err(|e| DbError::context("device_id", e))?;
        let store_dir = store_dir_of(path);
        let (core, state) = DatabaseCore::open_read_only(
            path,
            store_dir,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            Arc::new(hlc),
            migrations,
        )?;
        Ok(Database {
            connection: DatabaseConnection::start(core, "coven-db-ro")?,
            state,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn arm_test_pause(
        &self,
        point: DatabaseTestPoint,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        self.state.test_pause_points.arm(point)
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn observe_test_points(&self) -> tokio::sync::mpsc::UnboundedReceiver<DatabaseTestPoint> {
        self.state.test_pause_points.observe()
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn fail_next_merge_materialization_at(&self, point: MergeMaterializationFailurePoint) {
        *self
            .state
            .merge_materialization_failure
            .lock()
            .expect("Merge materialization failure lock poisoned") = Some(point);
    }

    /// Open with a caller-supplied register clock instead of a fresh
    /// system-wall-clock one. Lets a test inject an [`Hlc`] over a controlled
    /// wall clock to exercise the skew/restart-seeding guarantees, sharing the
    /// production open path (migration, seed, session) so the test drives the
    /// real unit.
    ///
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        Self::open_with_hlc_and_coven_metadata(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            migrations,
            CovenMetadataOpen::Detect,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn schema_version(&self) -> u32 {
        self.state.schema_version
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn sync_routing_hash(&self) -> ObjectHash {
        self.state.sync_routing_hash
    }

    /// The receiver's current wall-clock millis, read from this database's
    /// register clock. The pull reads it once and passes it down to bound an
    /// incoming `_updated_at`'s physical component (a grossly-future stamp must not
    /// win last-writer-wins or ratchet the clock).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn receive_wall_ms(&self) -> u64 {
        self.state.hlc.wall_now_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relative_database_uses_the_working_directory_as_its_store_directory() {
        assert_eq!(
            store_dir_of(Path::new("store.sqlite")).as_ref(),
            Path::new(".")
        );
    }
}

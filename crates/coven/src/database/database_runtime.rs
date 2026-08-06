use super::*;

impl Database {
    /// Open and own the connection at `path`.
    ///
    /// Runs the host migration ladder and validates its final sync-routing
    /// contract in one transaction. A fresh database creates Coven metadata in
    /// that transaction; an initialized database commits only when the final
    /// contract exactly matches its pinned bytes. Then seeds the register clock
    /// from on-disk rows. The `_updated_at` stamper remains inside the database
    /// boundary and is used by every synced-row write.
    pub(crate) fn open(
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

    pub(crate) fn open_initialized_store(
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
        let (core, state) = DatabaseCore::open(
            path,
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
    pub(crate) fn open_read_only(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc = Hlc::try_new(device_id, clock).map_err(|e| DbError::context("device_id", e))?;
        let (core, state) = DatabaseCore::open_read_only(
            path,
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

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn arm_test_pause(
        &self,
        point: DatabaseTestPoint,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        self.state.test_pause_points.arm(point)
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn observe_test_points(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<DatabaseTestPoint> {
        self.state.test_pause_points.observe()
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn fail_next_merge_materialization_at(
        &self,
        point: MergeMaterializationFailurePoint,
    ) {
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
    #[cfg(test)]
    pub(crate) fn open_with_hlc(
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

    #[cfg(test)]
    pub(crate) fn synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    #[cfg(test)]
    pub(crate) fn schema_version(&self) -> u32 {
        self.state.schema_version
    }

    #[cfg(test)]
    pub(crate) fn sync_routing_hash(&self) -> ObjectHash {
        self.state.sync_routing_hash
    }

    /// The receiver's current wall-clock millis, read from this database's
    /// register clock. The pull reads it once and passes it down to bound an
    /// incoming `_updated_at`'s physical component (a grossly-future stamp must not
    /// win last-writer-wins or ratchet the clock).
    #[cfg(test)]
    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.state.hlc.wall_now_ms()
    }

    /// Run `f` against the connection and await the result.
    ///
    /// This is how tests run raw bookkeeping and gating operations that need a
    /// `&Connection`. Host writes and coven transitions that mutate synced rows
    /// go through [`StoreDatabase`] so they land in the pending-write journal.
    ///
    /// Hands `f` to the connection thread and awaits its reply, so the SQL runs
    /// off the async executor.
    #[cfg(test)]
    pub(super) async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.connection.call(f).await
    }
}

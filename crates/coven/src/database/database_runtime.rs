use super::*;

impl Database {
    pub(crate) fn store_connection(&self) -> crate::database::StoreDatabaseConnection {
        crate::database::StoreDatabaseConnection::new(self.connection.clone())
    }

    pub(crate) fn id_provider_ref(&self) -> crate::id_provider::IdRef {
        self.state.ids.clone()
    }

    pub(crate) fn write_status_senders(
        &self,
    ) -> Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>> {
        self.state.write_statuses.clone()
    }

    #[cfg(test)]
    pub(crate) fn store_test_access(&self) -> StoreDatabaseTestAccess {
        StoreDatabaseTestAccess {
            pause_points: self.state.test_pause_points.clone(),
            merge_materialization_failure: self.state.merge_materialization_failure.clone(),
        }
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn new_write_id(&self) -> WriteId {
        WriteId::from_generated(self.state.ids.new_id())
    }

    pub(crate) fn store_runtime(&self) -> crate::database::StoreDatabaseRuntime {
        self.state.store_runtime.clone()
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

    /// Open and own the connection at `path`.
    ///
    /// Runs the host migration ladder and validates its final sync-routing
    /// contract in one transaction. A fresh database creates Coven metadata in
    /// that transaction; an initialized database commits only when the final
    /// contract exactly matches its pinned bytes. Then seeds the register clock
    /// from on-disk rows. Returns the handle plus the non-optional `_updated_at`
    /// stamper the host binds into every synced-row write.
    ///
    pub(crate) fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
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
        transfer_limits: crate::blob::TransferLimits,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
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
        transfer_limits: crate::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
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

    fn open_with_hlc_and_coven_metadata(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
        metadata_open: CovenMetadataOpen<'_>,
    ) -> Result<(Database, UpdatedAtStamper), OpenError> {
        let (core, state, stamper) = DatabaseCore::open(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            migrations,
            metadata_open,
        )?;

        let database = {
            let (jobs_tx, jobs_rx) = tokio::sync::mpsc::unbounded_channel::<DbJob>();
            let join = std::thread::Builder::new()
                .name("coven-db".to_string())
                .spawn(move || run_connection_thread(core, jobs_rx))
                .map_err(|e| DbError::Message(format!("spawn database connection thread: {e}")))?;
            Database {
                connection: DatabaseConnection {
                    thread: Arc::new(ConnectionThread {
                        jobs: jobs_tx,
                        join: Some(join),
                    }),
                },
                state,
            }
        };

        Ok((database, stamper))
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
        transfer_limits: crate::blob::TransferLimits,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<Database, OpenError> {
        let hlc =
            Hlc::try_new(device_id).map_err(|e| DbError::Message(format!("device_id {e}")))?;
        let (core, state) = DatabaseCore::open_read_only(
            path,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            Arc::new(hlc),
            migrations,
        )?;
        let (jobs_tx, jobs_rx) = tokio::sync::mpsc::unbounded_channel::<DbJob>();
        let join = std::thread::Builder::new()
            .name("coven-db-ro".to_string())
            .spawn(move || run_connection_thread(core, jobs_rx))
            .map_err(|e| DbError::Message(format!("spawn database connection thread: {e}")))?;
        Ok(Database {
            connection: DatabaseConnection {
                thread: Arc::new(ConnectionThread {
                    jobs: jobs_tx,
                    join: Some(join),
                }),
            },
            state,
        })
    }

    /// The host's declared synced-table set, the single owner of which tables
    /// participate in changeset sync. Each journaled write's capture session
    /// attaches exactly these, the register-clock seed scanned these, and the
    /// gate/apply operate over these — so the sync layer reads the set from here
    /// instead of carrying a separately-passed copy that could silently diverge.
    pub(crate) fn synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    /// The host's blob-tombstone convergence window. The tombstone GC ages each
    /// tombstone's `deleted_at` against this to decide when a deleted blob may be
    /// erased. Fixed for this handle's life.
    pub(crate) fn blob_tombstone_grace(&self) -> chrono::Duration {
        self.state.blob_tombstone_grace
    }

    /// How many blob transfers each transfer loop may run at once. Read by the
    /// upload drain ([`crate::blob::upload::drain_uploads`]) and the pin loop
    /// ([`crate::blob::cache::pin`]). Fixed for this handle's life.
    pub(crate) fn transfer_limits(&self) -> crate::blob::TransferLimits {
        self.state.transfer_limits
    }

    /// The gate model resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    #[doc(hidden)]
    pub(crate) fn gates(&self) -> Arc<Gates> {
        self.state.gates.clone()
    }

    /// Blob declarations resolved from the final synced table set and live schema at
    /// open. Fixed for this handle's life.
    #[doc(hidden)]
    pub(crate) fn blob_decls(&self) -> Arc<BlobDecls> {
        self.state.blob_decls.clone()
    }

    /// The applied synced-schema version — `PRAGMA user_version` after the
    /// migration ladder ran at open. This is the single source of the wire
    /// `schema_version`: every outgoing changeset is stamped with it, the pull
    /// gates compare incoming changesets and the min-floor against it, and the
    /// snapshot meta carries it. A device cannot stamp a version it has not
    /// migrated to. Cached because migrations run only at open, so the value is
    /// fixed for the handle's life.
    pub(crate) fn schema_version(&self) -> u32 {
        self.state.schema_version
    }

    /// Hash of the declarations and live schema shape that decide row routing
    /// and confidentiality for this Store.
    pub(crate) fn sync_routing_hash(&self) -> ObjectHash {
        self.state.sync_routing_hash
    }

    /// The shared register clock. coven's sync layer records pulled rows as its
    /// floor and stamps envelopes off it; it is the same `Arc<Hlc>` the stamper wraps.
    pub(crate) fn hlc(&self) -> Arc<Hlc> {
        self.state.hlc.clone()
    }

    #[cfg(test)]
    pub(crate) fn stamper(&self) -> UpdatedAtStamper {
        UpdatedAtStamper::new(self.state.hlc.clone())
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
    /// This is how coven runs bookkeeping, gating reads, raw test writes, and
    /// apply — anything that needs `&Connection`. Public host writes and coven
    /// transitions that mutate synced rows wrap their write in a
    /// [`Self::run_internal_store_write_transaction_on`] transaction (still through
    /// `call`) so it lands in the pending-changeset journal.
    ///
    /// Hands `f` to the connection thread and awaits its reply, so the SQL runs
    /// off the async executor.
    #[cfg(test)]
    pub(crate) async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.connection.call(f).await
    }
}

impl DatabaseConnection {
    pub(crate) async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| f(core.connection()))
            .await
    }

    /// Send `f` to the connection thread, run it against the owned core there, and
    /// await its result. A panic in `f` is caught on the connection thread (so it
    /// cannot unwind the thread and take the connection with it) and resumed on
    /// this task, matching the pre-thread behavior where the closure panicked
    /// directly on the caller.
    ///
    /// Cancellation: once dispatched, `f` runs to completion on the connection
    /// thread regardless of whether the caller is still awaiting. If the caller is
    /// cancelled between the thread committing and this reply resolving, it never
    /// observes the result even though the effect landed — the same "the operation
    /// may have committed" contract any network call carries. This is deliberate:
    /// the durable database state is the source of truth, and a caller must treat
    /// a cancelled call as possibly-committed. Follow-ups that matter beyond that
    /// durable state — observer notifications, publish triggers — are not driven
    /// off this return value; the sync cycle re-derives them from durable state.
    async fn on_connection_thread<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut DatabaseCore) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let job = DbJob::Run(Box::new(move |core| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(core)));
            // The caller may have been cancelled and dropped `reply_rx`; a failed
            // send is that normal outcome, not an error.
            let _ = reply_tx.send(outcome);
        }));
        if self.thread.jobs.send(job).is_err() {
            panic!("database connection thread stopped before a call completed");
        }
        match reply_rx.await {
            Ok(Ok(value)) => value,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => {
                panic!("database connection thread dropped a call's reply without responding")
            }
        }
    }
}

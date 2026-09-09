use super::*;
use crate::database_session::DatabaseSession;
use std::collections::HashMap;
use tracing::error;

mod snapshot_preparation;
pub use crate::store::PreparedStoreSnapshot;
use crate::store::SnapshotPreparationDirectory;

/// Database state used both by connection-thread SQL and caller-task
/// coordination. One instance is created at open and shared by the connection
/// handle and worker; neither side derives a second aggregate from it.
struct DatabaseContext {
    store_dir: coven_foundation::store_dir::StoreDir,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    migrations: Arc<[Migration]>,
    coven_migration_policy: CovenMigrationPolicy,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    /// Read by every upload-drain pass and pin call, so a host can change
    /// them while the store is open and the next pass runs under the new
    /// limits.
    transfer_limits: std::sync::Mutex<coven_protocol::blob::TransferLimits>,
    store_runtime: crate::store::StoreDatabaseRuntime,
    ids: coven_foundation::id_provider::IdRef,
    write_statuses: std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>,
    committed_changes:
        Option<tokio::sync::broadcast::Sender<Arc<crate::live_query::CommittedChanges>>>,
    #[cfg(any(test, feature = "test-utils"))]
    test_pause_points: TestPausePoints<DatabaseTestPoint>,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure: std::sync::Mutex<Option<MergeMaterializationFailurePoint>>,
}

/// The owned SQLite connection and its connection-lifetime verified state.
/// Caller-task services live in the one shared context created beside it.
pub(crate) struct DatabaseCore {
    conn: Connection,
    verified_store_authority: crate::store::VerifiedStoreAuthority,
    context: Arc<DatabaseContext>,
    snapshot_preparation: Option<SnapshotPreparationDirectory>,
}

impl DatabaseCore {
    pub(crate) fn new(
        store_dir: coven_foundation::store_dir::StoreDir,
        conn: Connection,
        hlc: Arc<Hlc>,
        synced_tables: Arc<Vec<SyncedTable>>,
        migrations: Arc<[Migration]>,
        coven_migration_policy: CovenMigrationPolicy,
        schema_version: u32,
        sync_routing_hash: ObjectHash,
        gates: Arc<Gates>,
        blob_decls: Arc<BlobDecls>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        capture_committed_changes: bool,
    ) -> Self {
        let committed_changes =
            capture_committed_changes.then(|| tokio::sync::broadcast::channel(256).0);
        Self {
            conn,
            verified_store_authority: Default::default(),
            snapshot_preparation: None,
            context: Arc::new(DatabaseContext {
                store_dir,
                hlc,
                synced_tables,
                migrations,
                coven_migration_policy,
                schema_version,
                sync_routing_hash,
                gates,
                blob_decls,
                blob_tombstone_grace,
                transfer_limits: std::sync::Mutex::new(transfer_limits),
                store_runtime: crate::store::StoreDatabaseRuntime::new(),
                ids: Arc::new(coven_foundation::id_provider::UuidProvider),
                write_statuses: std::sync::Mutex::new(HashMap::new()),
                committed_changes,
                #[cfg(any(test, feature = "test-utils"))]
                test_pause_points: TestPausePoints::default(),
                #[cfg(any(test, feature = "test-utils"))]
                merge_materialization_failure: std::sync::Mutex::new(None),
            }),
        }
    }

    fn begin_change_capture(&self) -> Result<crate::live_query::ChangeCapture, DbError> {
        let schema_version = self
            .conn
            .pragma_query_value(None, "schema_version", |row| row.get(0))
            .map_err(DbError::from)?;
        // SAFETY: the connection worker owns `self.conn` for longer than the
        // returned capture, and processes no other job until it is consumed.
        let database = unsafe { self.conn.handle() };
        crate::live_query::ChangeCapture::begin(database, schema_version)
    }

    pub(super) fn seed_clock(&self) -> Result<(), DbError> {
        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("seed database clock");
        timings.mark("read and seed the clock floor", || {
            let persisted = get_protocol_state_on(&self.conn, HIGHWATER_STATE_KEY)?;
            seed_from(
                &self.context.hlc,
                persisted,
                "HLC high-water mark in protocol_state",
            )?;
            let seed_bound_ms = self
                .context
                .hlc
                .wall_now_ms()
                .saturating_add(MAX_FUTURE_SKEW_MS);
            let on_disk =
                scan_max_updated_at(&self.conn, &self.context.synced_tables, seed_bound_ms)?;
            seed_from(&self.context.hlc, on_disk, "`_updated_at` in synced tables")
        })?;
        timings.report();
        Ok(())
    }

    fn finish_change_capture(
        &self,
        mut capture: crate::live_query::ChangeCapture,
    ) -> Result<crate::live_query::CommittedChanges, DbError> {
        let bytes = capture.take_changeset()?;
        let current_schema_version: i64 = self
            .conn
            .pragma_query_value(None, "schema_version", |row| row.get(0))
            .map_err(DbError::from)?;
        let mut committed = crate::live_query::decode_changeset(&self.conn, &bytes)?;
        if current_schema_version != capture.schema_version() {
            committed.mark_schema_changed();
        }
        Ok(committed)
    }
}

fn capture_committed_changes<R>(
    core: &mut DatabaseCore,
    operation: impl FnOnce(&mut DatabaseCore) -> Result<R, DbError>,
) -> Result<R, DbError> {
    let Some(sender) = core.context.committed_changes.clone() else {
        return operation(core);
    };
    if sender.receiver_count() == 0 {
        return operation(core);
    }
    let capture = core.begin_change_capture()?;
    let outcome = operation(core);
    let captured = core.finish_change_capture(capture);
    match (outcome, captured) {
        (Ok(value), Ok(changes)) => {
            publish_committed_changes(&sender, changes);
            Ok(value)
        }
        (Err(operation), Ok(changes)) => {
            publish_committed_changes(&sender, changes);
            Err(operation)
        }
        (Ok(_), Err(capture)) => {
            publish_committed_changes(&sender, crate::live_query::CommittedChanges::unknown());
            Err(capture)
        }
        (Err(operation), Err(capture)) => {
            publish_committed_changes(&sender, crate::live_query::CommittedChanges::unknown());
            Err(DbError::ChangeCaptureFailed {
                operation: Box::new(operation),
                capture: Box::new(capture),
            })
        }
    }
}

fn publish_committed_changes(
    sender: &tokio::sync::broadcast::Sender<Arc<crate::live_query::CommittedChanges>>,
    changes: crate::live_query::CommittedChanges,
) {
    if !changes.is_empty() {
        let _ = sender.send(Arc::new(changes));
    }
}

/// A cloneable handle to the thread that owns one SQLite connection. Every
/// clone sends work through the same channel, so database access is serialized
/// in send order.
#[derive(Clone)]
pub(crate) struct DatabaseConnection {
    thread: Arc<ConnectionThread>,
    context: Arc<DatabaseContext>,
}

impl DatabaseConnection {
    /// Build the channel and start the worker that owns `core` until the final
    /// handle drops.
    pub(crate) fn start(core: DatabaseCore, thread_name: &str) -> Result<Self, DbError> {
        let context = core.context.clone();
        let (jobs, receiver) = tokio::sync::mpsc::unbounded_channel();
        let worker = ConnectionWorker { core, receiver };
        let join = std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || worker.run())
            .map_err(|error| DbError::context("spawn database connection thread", error))?;
        Ok(Self {
            thread: Arc::new(ConnectionThread {
                jobs,
                join: Some(join),
            }),
            context,
        })
    }

    pub(crate) fn call_database<F, R>(
        &self,
        operation: F,
    ) -> impl std::future::Future<Output = Result<R, DbError>> + Send + '_
    where
        F: for<'session> FnOnce(&mut DatabaseSession<'session>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| {
            capture_committed_changes(core, |core| {
                let mut session = DatabaseSession::new(
                    &core.conn,
                    #[cfg(any(test, feature = "test-utils"))]
                    &core.context.store_dir,
                );
                operation(&mut session)
            })
        })
    }

    /// Run one Store operation against the connection-owned row, payload, and
    /// verified-authority state, then discharge every payload deletion the
    /// operation committed before another Store operation can run.
    pub(crate) fn call_store<F, R>(
        &self,
        operation: F,
    ) -> impl std::future::Future<Output = Result<R, DbError>> + Send + '_
    where
        F: for<'session> FnOnce(&mut crate::store::StoreSession<'session>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| {
            capture_committed_changes(core, |core| {
                let outcome = {
                    let mut session = store_session(core);
                    operation(&mut session)
                };
                let cleanup = crate::payload_store::pay_owed_payload_deletions_on(
                    &core.conn,
                    &core.context.store_dir,
                );
                match (outcome, cleanup) {
                    (Ok(value), Ok(())) => Ok(value),
                    (Err(operation), Ok(())) => Err(operation),
                    (Ok(_), Err(cleanup)) => Err(cleanup),
                    (Err(operation), Err(cleanup)) => Err(DbError::PayloadCleanupFailed {
                        operation: Box::new(operation),
                        cleanup: Box::new(cleanup),
                    }),
                }
            })
        })
    }

    pub(crate) fn read_store<F, R, E>(
        &self,
        read: F,
    ) -> impl std::future::Future<Output = Result<Result<R, E>, DbError>> + Send + '_
    where
        F: for<'connection> FnOnce(crate::store::SqlReadContext<'connection>) -> Result<R, E>
            + Send
            + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.on_connection_thread(move |core| store_session(core).read(read))
    }

    pub(crate) fn store_schema_version(&self) -> u32 {
        self.context.schema_version
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn store_synced_tables(&self) -> Vec<SyncedTable> {
        self.context.synced_tables.as_ref().clone()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn replace_with_database_image_for_test(
        &self,
        image: Vec<u8>,
    ) -> Result<(), DbError> {
        self.on_connection_thread(move |core| {
            let mut replacement = Connection::open_in_memory().map_err(DbError::from)?;
            crate::connection_io::deserialize_database_image_into(&mut replacement, &image)?;
            crate::connection_io::configure_connection_durability(
                &replacement,
                crate::connection_io::ConnectionDurability::Disabled,
            )?;
            replacement
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(DbError::from)?;
            let routing = crate::database_open::load_coven_metadata(&replacement)?;
            if routing.hash() != core.context.sync_routing_hash {
                return Err(DbError::Message(format!(
                    "replaced test database has sync-routing hash {}, expected {}",
                    routing.hash(),
                    core.context.sync_routing_hash,
                )));
            }
            crate::validate_coven_schema_for_reader(
                &replacement,
                core.context.gates.has_scoped_graph(),
            )
            .map_err(|error| {
                DbError::Message(format!("validate replaced test database: {error}"))
            })?;
            let schema_version: u32 = replacement
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .map_err(DbError::from)?;
            if schema_version != core.context.schema_version {
                return Err(DbError::Message(
                    "replaced test database has a different host schema version".to_string(),
                ));
            }
            // This importer preserves existing Database handles and their
            // resolved column indexes and attached gate schema. Schema changes
            // must go through database opening, which rebuilds those owners.
            for table in core.context.synced_tables.iter() {
                let current =
                    crate::schema_introspection::create_table_sql(&core.conn, table.name())
                        .map_err(|error| {
                            DbError::context(
                                "read current host table",
                                crate::GateError::from(error),
                            )
                        })?;
                let incoming =
                    crate::schema_introspection::create_table_sql(&replacement, table.name())
                        .map_err(|error| {
                            DbError::context(
                                "read replacement host table",
                                crate::GateError::from(error),
                            )
                        })?;
                if crate::schema_introspection::normalize_schema_sql(&current)
                    .map_err(DbError::from)?
                    != crate::schema_introspection::normalize_schema_sql(&incoming)
                        .map_err(DbError::from)?
                {
                    return Err(DbError::Message(format!(
                        "replaced test database changes host table {}",
                        table.name(),
                    )));
                }
            }

            let persisted = crate::get_protocol_state_on(&replacement, HIGHWATER_STATE_KEY)?;
            let seed_bound_ms = core
                .context
                .hlc
                .wall_now_ms()
                .saturating_add(MAX_FUTURE_SKEW_MS);
            let on_disk = crate::connection_io::scan_max_updated_at(
                &replacement,
                &core.context.synced_tables,
                seed_bound_ms,
            )?;
            let persisted = crate::connection_io::parse_seed(
                persisted,
                "HLC high-water mark in replaced test database",
            )?;
            let on_disk = crate::connection_io::parse_seed(
                on_disk,
                "`_updated_at` in replaced test database",
            )?;

            // Copy into the owned connection: swapping in the memory image
            // would detach a file-backed fixture from its persistent database.
            // SQLite rolls back an unfinished backup when this handle drops.
            {
                let backup = rusqlite::backup::Backup::new(&replacement, &mut core.conn)
                    .map_err(DbError::from)?;
                let outcome = backup.step(-1).map_err(DbError::from)?;
                if !matches!(outcome, rusqlite::backup::StepResult::Done) {
                    return Err(DbError::Message(format!(
                        "replace test database image did not finish: {outcome:?}",
                    )));
                }
            }
            for seed in [persisted, on_disk].into_iter().flatten() {
                core.context.hlc.seed(&seed);
            }
            core.verified_store_authority = crate::store::VerifiedStoreAuthority::default();
            Ok(())
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn database_image_for_test(&self) -> Result<Vec<u8>, DbError> {
        self.on_connection_thread(|core| crate::connection_io::serialize_database_image(&core.conn))
            .await
    }

    pub(crate) fn store_sync_routing_hash(&self) -> ObjectHash {
        self.context.sync_routing_hash
    }

    pub(crate) fn store_has_synced_tables(&self) -> bool {
        !self.context.synced_tables.is_empty()
    }

    pub(crate) fn store_blob_transition_root(&self, table_name: &str) -> BlobTransitionRoot {
        let Some(table) = self
            .context
            .synced_tables
            .iter()
            .find(|table| table.name() == table_name)
        else {
            return BlobTransitionRoot::NotGated;
        };
        if table.is_remote_root() {
            BlobTransitionRoot::RemoteRoot
        } else if table.gate_column().is_some() {
            BlobTransitionRoot::Gated
        } else {
            BlobTransitionRoot::NotGated
        }
    }

    pub(crate) fn store_transfer_limits(&self) -> coven_protocol::blob::TransferLimits {
        *self
            .context
            .transfer_limits
            .lock()
            .expect("transfer limits mutex poisoned")
    }

    pub(crate) fn set_store_transfer_limits(&self, limits: coven_protocol::blob::TransferLimits) {
        *self
            .context
            .transfer_limits
            .lock()
            .expect("transfer limits mutex poisoned") = limits;
    }

    pub(crate) fn store_blob_tombstone_grace(&self) -> chrono::Duration {
        self.context.blob_tombstone_grace
    }

    pub(crate) fn store_has_scoped_graph(&self) -> bool {
        self.context.gates.has_scoped_graph()
    }

    pub(crate) fn store_stamp(&self) -> String {
        self.context.hlc.now().to_string()
    }

    pub(crate) fn store_hlc_high_water(&self) -> String {
        self.context.hlc.high_water().to_string()
    }

    pub(crate) fn store_blob_ref_from_change(
        &self,
        change: &coven_foundation::changeset::RowChange,
    ) -> Result<Option<coven_protocol::blob::BlobRef>, BlobDeclError> {
        self.context.blob_decls.ref_from_change(change)
    }

    pub(crate) fn validate_store_local_blob_cleanup_changes(
        &self,
        old_changes: &[coven_foundation::changeset::RowChange],
        new_changes: &[coven_foundation::changeset::RowChange],
    ) -> Result<(), BlobDeclError> {
        crate::local_blob_cleanup_intents::intents_from_changes(
            self.context.blob_decls.as_ref(),
            old_changes,
            new_changes,
        )
        .map(|_| ())
    }

    pub(crate) fn store_receive_wall_ms(&self) -> u64 {
        self.context.hlc.wall_now_ms()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn assert_owns_payload_directory_for_test(
        &self,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) {
        let owned = self
            .context
            .store_dir
            .canonicalize()
            .expect("canonicalize database payload directory");
        let supplied = store_dir
            .canonicalize()
            .expect("canonicalize supplied payload directory");
        assert_eq!(
            owned, supplied,
            "payload directory does not belong to this database",
        );
    }

    pub(crate) fn new_store_id(&self) -> String {
        self.context.ids.new_id()
    }

    pub(crate) fn notify_store_write_status(&self, write_id: WriteId, status: WriteStatus) {
        let senders = self
            .context
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
            .context
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        let sender = senders
            .entry(write_id)
            .or_insert_with(|| tokio::sync::watch::channel(current.clone()).0);
        sender.send_replace(current);
        sender.subscribe()
    }

    pub(crate) fn subscribe_committed_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<Arc<crate::live_query::CommittedChanges>> {
        self.context
            .committed_changes
            .as_ref()
            .expect("only a writer exposes committed changes")
            .subscribe()
    }

    pub(crate) async fn membership_load_permit(&self) -> crate::store::MembershipLoadPermit {
        self.context.store_runtime.membership_load_permit().await
    }

    pub(crate) async fn membership_mutation_permit(
        &self,
    ) -> crate::store::MembershipMutationPermit {
        self.context
            .store_runtime
            .membership_mutation_permit()
            .await
    }

    pub(crate) async fn store_creation_permit(&self) -> crate::store::StoreCreationPermit {
        self.context.store_runtime.store_creation_permit().await
    }

    pub(crate) async fn device_exclusion_permit(&self) -> crate::store::DeviceExclusionPermit {
        self.context.store_runtime.device_exclusion_permit().await
    }

    pub(crate) async fn author_own_store_stream(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.context.store_runtime.author_own_stream().await
    }

    pub(crate) async fn blob_upload_drain_permit(&self) -> crate::store::BlobUploadDrainPermit {
        self.context.store_runtime.blob_upload_drain_permit().await
    }

    pub(crate) async fn snapshot_publication_permit(
        &self,
    ) -> crate::store::SnapshotPublicationPermit {
        self.context
            .store_runtime
            .snapshot_publication_permit()
            .await
    }

    pub(crate) async fn local_blob_cleanup_permit(&self) -> crate::store::LocalBlobCleanupPermit {
        self.context.store_runtime.local_blob_cleanup_permit().await
    }

    pub(crate) async fn apply_local_blob_cleanup_intent(
        &self,
        intent: &crate::local_blob_cleanup_intents::LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        intent.apply(&self.context.store_dir).await
    }

    pub(crate) async fn stage_host_write_blobs<E>(
        &self,
        blobs: Vec<crate::store::NewBlob>,
    ) -> Result<crate::store::StagedBlobBatch, crate::HostWriteError<E>> {
        crate::store::StagedBlobBatch::stage(&self.context.store_dir, blobs).await
    }

    pub(crate) async fn sync_store_parent_dir(
        &self,
        path: &Path,
    ) -> Result<(), coven_foundation::atomic_file::FileError> {
        self.context.store_dir.sync_parent_dir(path).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn reach_store_test_point(&self, point: DatabaseTestPoint) {
        self.context.test_pause_points.reach(point).await;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn arm_test_pause(
        &self,
        point: DatabaseTestPoint,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        self.context.test_pause_points.arm(point)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn observe_test_points(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<DatabaseTestPoint> {
        self.context.test_pause_points.observe()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn fail_next_merge_materialization_at(
        &self,
        point: MergeMaterializationFailurePoint,
    ) {
        *self
            .context
            .merge_materialization_failure
            .lock()
            .expect("Merge materialization failure lock poisoned") = Some(point);
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
    // Put the operation in the existing job allocation before constructing the
    // returned future. Keeping this frame separate prevents large captures from
    // occupying every forwarding future and its caller's polling stack.
    #[inline(never)]
    fn on_connection_thread<F, R>(&self, f: F) -> impl std::future::Future<Output = R> + Send + '_
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
        async move {
            // Creating or dropping an unpolled call must not submit work.
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
}

fn store_session(core: &mut DatabaseCore) -> crate::store::StoreSession<'_> {
    crate::store::StoreSession::new(
        &core.conn,
        &core.context.store_dir,
        &mut core.verified_store_authority,
        &core.context.gates,
        &core.context.synced_tables,
        core.context.schema_version,
        core.context.sync_routing_hash,
        &core.context.hlc,
        &core.context.blob_decls,
        #[cfg(any(test, feature = "test-utils"))]
        &core.context.merge_materialization_failure,
    )
}

/// A unit of work for the connection thread: a caller's closure to run against
/// the owned core, or the sentinel the final [`DatabaseConnection`] clone sends
/// as it drops to stop the thread.
enum DbJob {
    Run(Box<dyn FnOnce(&mut DatabaseCore) + Send>),
    SealSnapshot(tokio::sync::oneshot::Sender<Result<PreparedStoreSnapshot, DbError>>),
    DiscardSnapshot(tokio::sync::oneshot::Sender<Result<(), DbError>>),
    Stop,
}

/// The channel and join handle shared by every [`DatabaseConnection`] clone.
/// Its final owner queues `Stop` and releases the worker thread.
struct ConnectionThread {
    jobs: tokio::sync::mpsc::UnboundedSender<DbJob>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ConnectionThread {
    fn drop(&mut self) {
        // Reached only when the last `DatabaseConnection` clone drops — no other
        // clone can still be sending. Queue `Stop` behind whatever jobs are
        // already in flight so the worker drains them and closes the connection
        // on its owning thread.
        let handle = match self.join.take() {
            Some(handle) => handle,
            None => return,
        };
        let _ = self.jobs.send(DbJob::Stop);
        if tokio::runtime::Handle::try_current().is_ok() {
            // Joining inside a runtime task would stall that executor worker
            // while queued database work finishes. Detaching preserves the
            // queue: the worker drains it, drops the core, and exits.
            drop(handle);
        } else if handle.join().is_err() {
            // Outside a runtime there is no executor worker to stall, so close
            // deterministically and surface a worker fault.
            error!("database connection thread panicked");
        }
    }
}

/// Owns the SQLite core and the matching receive half for their complete
/// lifetime. Dropping this value closes the connection on its owning thread.
struct ConnectionWorker {
    core: DatabaseCore,
    receiver: tokio::sync::mpsc::UnboundedReceiver<DbJob>,
}

impl ConnectionWorker {
    fn run(mut self) {
        while let Some(job) = self.receiver.blocking_recv() {
            match job {
                DbJob::Run(f) => f(&mut self.core),
                DbJob::SealSnapshot(reply) => {
                    let sealed = PreparedStoreSnapshot::seal(self.core);
                    // Cancellation drops the closed image and its payload directory.
                    let _ = reply.send(sealed);
                    return;
                }
                DbJob::DiscardSnapshot(reply) => {
                    let discarded = self.core.discard_snapshot();
                    let _ = reply.send(discarded);
                    return;
                }
                DbJob::Stop => break,
            }
        }
    }
}

#[cfg(test)]
#[path = "database_connection_tests.rs"]
mod tests;

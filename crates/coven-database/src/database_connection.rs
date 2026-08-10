use super::*;
use crate::database_session::DatabaseSession;
use std::collections::HashMap;
use tracing::error;

/// Database state used both by connection-thread SQL and caller-task
/// coordination. One instance is created at open and shared by the connection
/// handle and worker; neither side derives a second aggregate from it.
struct DatabaseContext {
    store_dir: coven_foundation::store_dir::StoreDir,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: coven_protocol::blob::TransferLimits,
    store_runtime: crate::store::StoreDatabaseRuntime,
    ids: coven_foundation::id_provider::IdRef,
    write_statuses: std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>,
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
}

impl DatabaseCore {
    pub(crate) fn new(
        store_dir: coven_foundation::store_dir::StoreDir,
        conn: Connection,
        hlc: Arc<Hlc>,
        synced_tables: Arc<Vec<SyncedTable>>,
        schema_version: u32,
        sync_routing_hash: ObjectHash,
        gates: Arc<Gates>,
        blob_decls: Arc<BlobDecls>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
    ) -> Self {
        Self {
            conn,
            verified_store_authority: Default::default(),
            context: Arc::new(DatabaseContext {
                store_dir,
                hlc,
                synced_tables,
                schema_version,
                sync_routing_hash,
                gates,
                blob_decls,
                blob_tombstone_grace,
                transfer_limits,
                store_runtime: crate::store::StoreDatabaseRuntime::new(),
                ids: Arc::new(coven_foundation::id_provider::UuidProvider),
                write_statuses: std::sync::Mutex::new(HashMap::new()),
                #[cfg(any(test, feature = "test-utils"))]
                test_pause_points: TestPausePoints::default(),
                #[cfg(any(test, feature = "test-utils"))]
                merge_materialization_failure: std::sync::Mutex::new(None),
            }),
        }
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

    pub(crate) async fn call_database<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(&mut DatabaseSession<'session>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| {
            let mut session = DatabaseSession::new(
                &core.conn,
                #[cfg(any(test, feature = "test-utils"))]
                &core.context.store_dir,
            );
            operation(&mut session)
        })
        .await
    }

    /// Run one Store operation against the connection-owned row, payload, and
    /// verified-authority state, then discharge every payload deletion the
    /// operation committed before another Store operation can run.
    pub(crate) async fn call_store<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'session> FnOnce(&mut crate::store::StoreSession<'session>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.on_connection_thread(move |core| {
            let outcome = {
                let mut session = crate::store::StoreSession::new(
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
                );
                operation(&mut session)
            };
            let cleanup = crate::payload_spool::pay_owed_payload_deletions_on(
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
        .await
    }

    pub(crate) fn store_schema_version(&self) -> u32 {
        self.context.schema_version
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
        self.context.transfer_limits
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

    pub(crate) async fn author_own_store_stream(&self) -> crate::store::OwnStreamAuthorship {
        self.context.store_runtime.author_own_stream().await
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

    pub(crate) async fn sync_store_parent_dir(&self, path: &Path) -> Result<(), String> {
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

/// A unit of work for the connection thread: a caller's closure to run against
/// the owned core, or the sentinel the final [`DatabaseConnection`] clone sends
/// as it drops to stop the thread.
enum DbJob {
    Run(Box<dyn FnOnce(&mut DatabaseCore) + Send>),
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
        let _ = self.jobs.send(DbJob::Stop);
        let handle = match self.join.take() {
            Some(handle) => handle,
            None => return,
        };
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
                DbJob::Stop => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;

    /// A SQL closure that blocks for a while must not stall other tasks on the
    /// same runtime, because jobs run on the dedicated connection thread rather
    /// than the async executor.
    #[tokio::test]
    async fn slow_db_call_does_not_block_the_executor() {
        use std::time::{Duration, Instant};

        let db = Database::open(
            Path::new(":memory:"),
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "liveness".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &[],
        )
        .expect("open database");

        let slow_db = db.clone();
        let slow = tokio::spawn(async move {
            slow_db
                .call_database(|session| session.select_one_after_delay(Duration::from_millis(500)))
                .await
        });

        let start = Instant::now();
        tokio::task::yield_now().await;
        let stalled = start.elapsed();

        assert!(
            stalled < Duration::from_millis(250),
            "unrelated task stalled {stalled:?} behind the slow DB call — the executor was blocked",
        );

        let value = slow
            .await
            .expect("slow DB task joins")
            .expect("slow DB call succeeds");
        assert_eq!(value, 1, "the slow DB call still returns its result");
    }

    /// Dropping the last handle from inside a runtime task must not block that
    /// task on the connection thread's queue, and a job already dispatched must
    /// still run to completion.
    #[tokio::test]
    async fn dropping_last_handle_in_async_context_does_not_stall_but_job_still_lands() {
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("db.sqlite");
        let marker = dir.path().join("marker");

        let db = Database::open(
            &db_path,
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "drop-async".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &[],
        )
        .expect("open");

        let job_db = db.clone();
        let job_marker = marker.clone();
        let task = tokio::spawn(async move {
            let _ = job_db
                .call_database(move |_session| {
                    std::thread::sleep(Duration::from_millis(300));
                    std::fs::write(&job_marker, b"landed")
                        .map_err(|e| DbError::Message(e.to_string()))
                })
                .await;
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        let drop_start = Instant::now();
        drop(db);
        let drop_elapsed = drop_start.elapsed();
        assert!(
            drop_elapsed < Duration::from_millis(200),
            "dropping the last handle stalled {drop_elapsed:?} — it joined the connection thread \
             instead of detaching",
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while !marker.exists() {
            assert!(
                Instant::now() < deadline,
                "the dispatched job's effect never landed after the last handle dropped",
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

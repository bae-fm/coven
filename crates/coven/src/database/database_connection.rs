use super::*;
use tracing::error;

/// A cloneable handle to the thread that owns one SQLite connection. Every
/// clone sends work through the same channel, so database access is serialized
/// in send order.
#[derive(Clone)]
pub(crate) struct DatabaseConnection {
    thread: Arc<ConnectionThread>,
}

impl DatabaseConnection {
    /// Build the channel and start the worker that owns `core` until the final
    /// handle drops.
    pub(super) fn start(core: DatabaseCore, thread_name: &str) -> Result<Self, DbError> {
        let (jobs, receiver) = tokio::sync::mpsc::unbounded_channel();
        let worker = ConnectionWorker { core, receiver };
        let join = std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || worker.run())
            .map_err(|error| {
                DbError::Message(format!("spawn database connection thread: {error}"))
            })?;
        Ok(Self {
            thread: Arc::new(ConnectionThread {
                jobs,
                join: Some(join),
            }),
        })
    }

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
    use crate::blob::BLOB_TOMBSTONE_GRACE;

    /// A SQL closure that blocks for a while must not stall other tasks on the
    /// same runtime, because jobs run on the dedicated connection thread rather
    /// than the async executor.
    #[tokio::test]
    async fn slow_db_call_does_not_block_the_executor() {
        use std::time::{Duration, Instant};

        let (db, _stamper) = Database::open(
            Path::new(":memory:"),
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "liveness".to_string(),
            &[],
        )
        .expect("open database");

        let slow_db = db.clone();
        let slow = tokio::spawn(async move {
            slow_db
                .call(|conn| {
                    std::thread::sleep(Duration::from_millis(500));
                    conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))
                        .map_err(DbError::from)
                })
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

        let (db, _stamper) = Database::open(
            &db_path,
            Vec::new(),
            BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "drop-async".to_string(),
            &[],
        )
        .expect("open");

        let job_db = db.clone();
        let job_marker = marker.clone();
        let task = tokio::spawn(async move {
            let _ = job_db
                .call(move |_conn| {
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

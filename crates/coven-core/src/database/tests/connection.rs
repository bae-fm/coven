use super::super::*;
use crate::blob::BLOB_TOMBSTONE_GRACE;

/// A SQL closure that blocks for a while must not stall other tasks on the
/// same runtime, because jobs run on the dedicated connection thread rather
/// than the async executor. On a current-thread runtime the scheduler has to
/// poll the spawned DB call before it can resume us; if that call ran its
/// blocking closure inline on the executor thread, this single `yield_now`
/// would not return until the closure finished. With the closure on its own
/// thread we resume immediately, long before it completes.
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
/// still run to completion. The drop detaches the thread in async context, so
/// it returns at once; the detached thread finishes the queued job (its effect
/// is durable) and exits on its own.
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

    // Dispatch a slow job to the connection thread, then release the clone
    // tied to it so the job is in flight with no handle awaiting it: spawn the
    // call, let it dispatch, then abort the task so its `Database` clone drops
    // while the job still runs. The job writes a marker file when it finishes.
    let job_db = db.clone();
    let job_marker = marker.clone();
    let task = tokio::spawn(async move {
        let _ = job_db
            .call(move |_conn| {
                std::thread::sleep(Duration::from_millis(300));
                std::fs::write(&job_marker, b"landed").map_err(|e| DbError::Message(e.to_string()))
            })
            .await;
    });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;

    // `db` is now the last clone; dropping it inside this runtime task must
    // detach (not join) so it returns without waiting out the queued job.
    let drop_start = Instant::now();
    drop(db);
    let drop_elapsed = drop_start.elapsed();
    assert!(
        drop_elapsed < Duration::from_millis(200),
        "dropping the last handle stalled {drop_elapsed:?} — it joined the connection thread \
         instead of detaching",
    );

    // The detached thread still runs the already-dispatched job to completion.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "the dispatched job's effect never landed after the last handle dropped",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

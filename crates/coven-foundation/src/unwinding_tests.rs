use super::*;

#[tokio::test]
async fn a_finished_join_returns_the_work_s_value() {
    let joined = tokio::task::spawn_blocking(|| 2 + 2).await;
    assert_eq!(blocking("add", joined), Ok(4));
}

#[test]
fn a_closure_sent_after_its_runtime_shut_down_did_not_run() {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let handle = runtime.handle().clone();
    drop(runtime);
    let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let running = ran.clone();
    let sent =
        handle.spawn_blocking(move || running.store(true, std::sync::atomic::Ordering::SeqCst));
    let joined = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(sent);
    assert_eq!(
        blocking("read the settings", joined),
        Err(Shutdown { operation: "read the settings", outcome: ShutdownOutcome::NotRun })
    );
    assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn a_dropped_task_may_have_run_in_part() {
    let closing = tokio::spawn(std::future::pending::<()>());
    closing.abort();
    let shutdown = task("close the store", closing.await).unwrap_err();
    assert_eq!(shutdown.outcome, ShutdownOutcome::Unknown);
    assert_eq!(
        shutdown.to_string(),
        "close the store may have run in part: its runtime is shutting down"
    );
}

#[tokio::test]
async fn a_panic_unwinds_into_the_caller() {
    let joined = tokio::task::spawn_blocking(|| -> u8 { std::panic::panic_any(7_u64) }).await;
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| blocking("count", joined)))
            .unwrap_err();
    assert_eq!(*panic.downcast::<u64>().unwrap(), 7);
}

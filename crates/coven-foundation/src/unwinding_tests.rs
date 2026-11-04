use super::*;

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
async fn a_panic_unwinds_into_the_owner() {
    let joined = tokio::spawn(async { std::panic::panic_any(7_u64) }).await;
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task::<()>("count", joined)))
            .unwrap_err();
    assert_eq!(*panic.downcast::<u64>().unwrap(), 7);
}

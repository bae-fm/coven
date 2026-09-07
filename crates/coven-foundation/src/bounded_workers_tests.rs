use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn bounded_admission_discards_cancelled_work_before_execution() {
    let pool =
        BoundedWorkers::start(vec![()], NonZeroUsize::new(1).unwrap(), "bounded-test").unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let started = Arc::new(tokio::sync::Notify::new());
    let first_pool = pool.clone();
    let first_started = started.clone();
    let first = tokio::spawn(async move {
        first_pool
            .call(move |()| {
                first_started.notify_one();
                wait.recv().unwrap();
            })
            .await
    });
    started.notified().await;
    let ran = Arc::new(AtomicBool::new(false));
    let queued_ran = ran.clone();
    let mut cancelled = Box::pin(pool.call(move |()| {
        queued_ran.store(true, Ordering::SeqCst);
    }));
    assert!(futures_util::poll!(&mut cancelled).is_pending());
    assert_eq!(pool.inner.sender.as_ref().unwrap().capacity(), 0);
    let mut waiting = Box::pin(pool.call(|()| 7));
    assert!(futures_util::poll!(&mut waiting).is_pending());
    assert_eq!(pool.inner.sender.as_ref().unwrap().capacity(), 0);
    drop(cancelled);
    release.send(()).unwrap();
    first.await.unwrap();
    assert_eq!(waiting.await, 7);
    assert!(!ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn panic_and_error_leave_the_worker_available() {
    let pool =
        BoundedWorkers::start(vec![()], NonZeroUsize::new(1).unwrap(), "panic-test").unwrap();
    let panicking = pool.clone();
    assert!(
        tokio::spawn(async move { panicking.call(|()| panic!("caller panic")).await })
            .await
            .unwrap_err()
            .is_panic()
    );
    assert_eq!(
        pool.call(|()| Err::<(), _>("read error")).await,
        Err("read error")
    );
    assert_eq!(pool.call(|()| 42).await, 42);
}

#[tokio::test]
async fn cancelled_work_can_drop_the_last_pool_clone_on_its_worker() {
    let pool =
        BoundedWorkers::start(vec![()], NonZeroUsize::new(1).unwrap(), "self-drop-test").unwrap();
    let closure_pool = pool.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let closure_started = started.clone();
    let (release, wait) = std::sync::mpsc::channel();
    let (finished, completion) = oneshot::channel();
    let caller = tokio::spawn(async move {
        pool.call(move |()| {
            closure_started.notify_one();
            wait.recv().unwrap();
            drop(closure_pool);
            finished.send(()).unwrap();
        })
        .await
    });
    started.notified().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), completion)
        .await
        .expect("dropping the last clone on an owned worker must not join itself")
        .unwrap();
}

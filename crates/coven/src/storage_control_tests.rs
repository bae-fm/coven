use super::*;

#[tokio::test]
async fn repeated_start_preserves_the_connector_waiting_in_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.connector.refuse.store(true, Ordering::SeqCst);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Failed { .. })).await;
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn start_after_stop_during_backoff_owns_a_replacement_connector() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.connector.refuse.store(true, Ordering::SeqCst);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Failed { .. })).await;
    f.handle.stop_sync();
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stopping_a_failing_connector_does_not_wait_for_its_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    f.connector.refuse.store(true, Ordering::SeqCst);
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), f.handle.close())
        .await
        .expect("stopping must wake a connector even while its request is in flight")
        .unwrap();
}

use super::*;

#[tokio::test]
async fn repeated_start_preserves_the_connector_waiting_in_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline { .. })
    })
    .await;
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
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline { .. })
    })
    .await;
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
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), f.handle.close())
        .await
        .expect("stopping must wake a connector even while its request is in flight")
        .unwrap();
}

async fn stopped_fixture() -> Fixture {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f
}

fn failed(status: &SyncStatus) -> bool {
    matches!(
        status,
        SyncStatus::Failed { .. } | SyncStatus::Offline { .. }
    )
}

#[tokio::test]
async fn connection_backoff_keeps_its_deadline_and_exponent_across_reopening() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    for failures in 1..=4 {
        let wait = f
            .handle
            .test_connection_retry_wait()
            .await
            .expect("persisted connection wait");
        assert_eq!(wait.failures, failures);
        assert_eq!(wait.wait, Duration::from_secs(1 << (failures - 1)));
        if failures < 4 {
            *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
            let mut shown = f.handle.subscribe_sync_status();
            shown.borrow_and_update();
            f.advance(wait.wait);
            shown.changed().await.unwrap();
            status(&f.handle, failed).await;
        }
    }
    let persisted = f.handle.test_connection_retry_wait().await.unwrap();
    f.advance(Duration::from_secs(4));
    f.handle.close().await.unwrap();
    let reopened = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    assert_eq!(reopened.test_connection_retry_wait().await, Some(persisted));
    let attempts = *f.connector.attempts.borrow();
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    reopened.start_sync().await.unwrap();
    f.advance(Duration::from_secs(3));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        *f.connector.attempts.borrow(),
        attempts,
        "four seconds remain after reopening"
    );
    f.advance(Duration::from_secs(1));
    status(&reopened, failed).await;
    let next = reopened.test_connection_retry_wait().await.unwrap();
    assert_eq!(next.failures, 5);
    assert_eq!(next.wait, Duration::from_secs(16));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_connector_failure_is_offline() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source(
        std::io::Error::new(std::io::ErrorKind::NotConnected, "offline"),
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    assert!(
        matches!(&*f.handle.subscribe_sync_status().borrow(), SyncStatus::Offline { error }
        if std::error::Error::source(error.as_ref()).unwrap().is::<std::io::Error>())
    );
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn connector_failures_wait_for_new_credentials_without_timers() {
    for failure in [StorageFailure::Authentication, StorageFailure::Protocol] {
        let f = stopped_fixture().await;
        *f.connector.failure.lock().unwrap() = Some(failure.into());
        f.handle.start_sync().await.unwrap();
        status(&f.handle, failed).await;
        assert!(f.handle.test_connection_retry_wait().await.is_none());
        let attempts = *f.connector.attempts.borrow();
        f.advance(Duration::from_secs(600));
        f.handle.sync_now();
        f.handle.start_sync().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *f.connector.attempts.borrow(),
            attempts,
            "{failure:?} must not retry by time or repeated start"
        );
        f.handle
            .update_credentials("replacement".into(), SecretText::new("secret".into()))
            .await
            .unwrap();
        status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_connected_pass_waits_for_new_credentials_after_authentication_failure() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::floor(f.directory.id()),
                StorageFailure::Authentication,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    assert!(f.handle.test_connection_retry_wait().await.is_none());
    let requests = f.storage.request_count();
    f.advance(Duration::from_secs(600));
    f.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.storage.request_count(), requests);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stopping_an_inflight_failure_preserves_its_delay_for_the_next_start() {
    let f = stopped_fixture().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync();
    resume.send(()).unwrap();
    f.handle.start_sync().await.unwrap();
    assert_eq!(
        f.handle.test_connection_retry_wait().await.unwrap().wait,
        Duration::from_secs(1)
    );
    f.advance(Duration::from_secs(1));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn credentials_changed_during_a_failed_pass_are_evidence_for_its_retry() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let floor = ObjectPath::floor(f.directory.id());
    f.storage
        .set_faults(Faults {
            read_failures: [(floor.clone(), StorageFailure::Authentication)].into(),
            ..Faults::none()
        })
        .await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(floor, entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    f.storage.set_faults(Faults::none()).await;
    resume.send(()).unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

use super::*;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    Storage,
};
use std::time::{Duration, UNIX_EPOCH};

struct Fixture {
    app: TestCoven,
    handle: CovenHandle,
    storage: Arc<MemoryStorage>,
    connector: Arc<TrackingConnector>,
    clock: Arc<FixedClock>,
    directory: StoreDir,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = TestCoven::new();
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(
            MemoryStorage::new(
                StorageConfig::S3 {
                    bucket: "sync".into(),
                    region: "test".into(),
                    prefix: "store".into(),
                    endpoint: None,
                },
                clock.clone(),
            )
            .unwrap()
            .with_transfer_limits(1024 * 1024, 64 * 1024)
            .unwrap(),
        );
        let directory = app
            .create_store(
                &StoreLayout::new(root.path().into()),
                "Loop",
                Arc::new(UuidIds),
            )
            .await
            .unwrap();
        let connector = Arc::new(TrackingConnector {
            storage: storage.clone(),
            clients: std::sync::Mutex::new(Vec::new()),
            credentials: std::sync::Mutex::new(Vec::new()),
            refuse: AtomicBool::new(false),
            held: tokio::sync::Mutex::new(None),
        });
        let handle = builder(
            &app,
            StoreLayout::new(root.path().into()),
            clock.clone(),
            connector.clone(),
        )
        .open(directory.id())
        .await
        .unwrap();
        handle.initialize_identity().unwrap();
        Self {
            app,
            handle,
            storage,
            connector,
            clock,
            directory,
            _root: root,
        }
    }

    async fn setup(&self) -> Result<ConnectedStorage, StorageSetupError> {
        self.handle
            .setup_s3_storage(
                self.storage.config(),
                "Test device",
                "member-key".into(),
                SecretText::new("secret".into()),
            )
            .await
    }

    fn advance(&self, duration: Duration) {
        self.clock.set(self.clock.now() + duration);
    }
}

struct TrackingConnector {
    storage: Arc<MemoryStorage>,
    clients: std::sync::Mutex<Vec<std::sync::Weak<dyn Storage>>>,
    credentials: std::sync::Mutex<Vec<StorageCredentials>>,
    refuse: AtomicBool,
    held: tokio::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[async_trait::async_trait]
impl StorageConnector for TrackingConnector {
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: coven_storage::StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<dyn Storage>, StorageError> {
        if let Some((entered, resume)) = self.held.lock().await.take() {
            entered.send(()).unwrap();
            resume.await.unwrap();
        }
        if self.refuse.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Injected(StorageFailure::Authentication));
        }
        self.credentials.lock().unwrap().push(credentials.clone());
        let client = self.storage.connect(config, credentials, device).await?;
        self.clients.lock().unwrap().push(Arc::downgrade(&client));
        Ok(client)
    }
}

#[tokio::test]
async fn stopping_releases_the_provider_and_start_rebuilds_it_once() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 1);
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    let mut code = coven_sync::read_restore_code(&f.handle.restore_code().await.unwrap()).unwrap();
    let mut data = RestoreStorage::decode(code.storage.as_bytes()).unwrap();
    data.credentials = StorageCredentials::S3(S3Credentials {
        access_key_id: "replacement".into(),
        secret_access_key: SecretText::new("replacement secret".into()),
    });
    code.storage = data.encode().unwrap();
    f.handle
        .update_credentials(&code.to_text().unwrap())
        .await
        .unwrap();
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    let credentials = f.connector.credentials.lock().unwrap()[1].clone();
    let StorageCredentials::S3(keys) = credentials else {
        panic!("S3 credentials")
    };
    assert_eq!(keys.access_key_id, "replacement");
    assert_eq!(keys.secret_access_key.as_str(), "replacement secret");
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_start_preserves_stopped_storage_and_can_be_retried() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.connector.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(f.handle.start_sync().await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Authentication));
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    f.app.fail_next_keychain_operation();
    assert!(matches!(
        f.handle.start_sync().await,
        Err(SyncError::SecureStorage(_))
    ));
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_queued_during_cancelled_start_releases_the_new_client() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    let mut subscription = f.handle.subscribe_sync_status();
    subscription.borrow_and_update();
    {
        let start = f.handle.start_sync();
        tokio::pin!(start);
        tokio::select! {
            result = &mut start => panic!("start completed while provider construction was held: {result:?}"),
            _ = entering => {}
        }
        f.handle.stop_sync();
    }
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            subscription.changed().await.unwrap();
            if matches!(&*subscription.borrow_and_update(), SyncStatus::Stopped) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    assert!(f
        .connector
        .clients
        .lock()
        .unwrap()
        .iter()
        .all(|client| client.upgrade().is_none()));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn unconfigured_and_forgotten_storage_stay_disconnected_on_start() {
    let f = Fixture::new().await;
    f.handle.stop_sync();
    f.handle.start_sync().await.unwrap();
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert!(f.connector.clients.lock().unwrap().is_empty());
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.disconnect_storage().await.unwrap();
    f.handle.start_sync().await.unwrap();
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    assert!(matches!(
        f.handle.restore_code().await,
        Err(SyncError::NoStorage)
    ));
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    handle.start_sync().await.unwrap();
    assert!(matches!(
        &*handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn disconnect_waits_for_the_active_pass_and_custody_failure_keeps_sync_running() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.app.fail_next_keychain_operation();
    assert!(matches!(
        f.handle.disconnect_storage().await,
        Err(SyncError::SecureStorage(_))
    ));
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Synced(_)
    ));
    let code = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(ObjectPrefix::device_logs(), entered, resumed)
        .await;
    f.handle.sync_now();
    entering.await.unwrap();
    let disconnect = f.handle.disconnect_storage();
    tokio::pin!(disconnect);
    tokio::select! {
        result = &mut disconnect => panic!("disconnect completed during a pass: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    assert_eq!(f.handle.restore_code().await.unwrap(), code);
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_some());
    resume.send(()).unwrap();
    disconnect.await.unwrap();
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    assert!(matches!(
        f.handle.restore_code().await,
        Err(SyncError::NoStorage)
    ));
    f.handle.close().await.unwrap();
}

fn builder(
    app: &TestCoven,
    layout: StoreLayout,
    clock: Arc<FixedClock>,
    storage: Arc<dyn StorageConnector>,
) -> CovenBuilder {
    app.builder(layout)
        .clock(clock)
        .storage_connector(storage)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
        )])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
}

async fn status(handle: &CovenHandle, expected: impl Fn(&SyncStatus) -> bool) {
    let mut status = handle.subscribe_sync_status();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if expected(&status.borrow_and_update()) {
                return;
            }
            status.changed().await.expect("open loop status");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out at {:?}", &*status.borrow()));
}

#[tokio::test]
async fn setup_starts_sync_and_committed_writes_wake_it_without_a_tick() {
    let f = Fixture::new().await;
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    let connected = f.setup().await.unwrap();
    assert_eq!(connected.key_state, StoreKeyState::Available);
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.advance(Duration::from_secs(1));
    let written = f.clock.now();
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('one','written')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    status(
        &f.handle,
        |s| matches!(s, SyncStatus::Synced(report) if report.finished_at >= written),
    )
    .await;
    assert_eq!(
        f.storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .len(),
        1
    );
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_start_idle_tick_offline_recovery_and_close_use_the_injected_clock() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('held','offline')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let before = f.storage.request_count();
    f.advance(Duration::from_secs(90));
    f.handle.sync_now();
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(f.storage.request_count(), before);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.storage
        .set_faults(Faults {
            fail_next: usize::MAX,
            ..Faults::none()
        })
        .await;
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Failed { .. })).await;
    f.storage.set_faults(Faults::none()).await;
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.close().await.unwrap();
    let closed = f.storage.request_count();
    f.advance(Duration::from_secs(90));
    assert!(f.handle.start_sync().await.is_err());
    assert_eq!(f.storage.request_count(), closed);
    assert!(matches!(
        f.handle.store_key_state(),
        Err(KeyError::StoreClosed)
    ));
}

#[tokio::test]
async fn setup_network_failure_preserves_keys_and_connection_then_retry_succeeds() {
    let f = Fixture::new().await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert_eq!(
        f.setup().await.unwrap_err().failure(),
        StorageSetupFailure::Network
    );
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.app.fail_next_keychain_operation();
    assert!(f.handle.disconnect_storage().await.is_err());
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.handle.disconnect_storage().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Disconnected)).await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.close().await.unwrap();
    let reopened = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.storage.clone(),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    assert!(matches!(
        &*reopened.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    reopened.start_sync().await.unwrap();
    status(&reopened, |s| matches!(s, SyncStatus::Synced(_))).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn probing_and_unlocking_preserve_the_requested_connection_lifetime() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let objects = f.storage.list(&ObjectPrefix::all()).await.unwrap();
    f.handle.probe_storage(&f.storage.config()).await.unwrap();
    assert_eq!(objects, f.storage.list(&ObjectPrefix::all()).await.unwrap());
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.app.fail_next_keychain_operation();
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(
        f.handle.store_key_state().unwrap(),
        StoreKeyState::Available
    );
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.handle.forget_store_keys().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(f.handle.start_sync().await.is_err());
    let opened = f.handle.unlock_store_key().await.unwrap();
    assert_eq!(opened.key_state, StoreKeyState::Available);
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_is_offline_until_reached_and_stop_and_close_finish_the_active_pass() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.storage.set_online(false);
    let mut requests = f.storage.subscribe_requests();
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    requests.wait_for(|n| *n > before).await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Offline)).await;
    f.storage.set_online(true);
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    for closing in [false, true] {
        let (listed, listing) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        f.storage
            .hold_next_listing(ObjectPrefix::device_logs(), listed, resumed)
            .await;
        f.handle.sync_now();
        listing.await.unwrap();
        assert!(matches!(
            &*f.handle.subscribe_sync_status().borrow(),
            SyncStatus::Syncing
        ));
        if closing {
            let close = f.handle.close();
            tokio::pin!(close);
            tokio::select! {
                result = &mut close => panic!("close ended during the pass: {result:?}"),
                _ = tokio::task::yield_now() => {},
            }
            resume.send(()).unwrap();
            close.await.unwrap();
        } else {
            f.handle.stop_sync();
            tokio::task::yield_now().await;
            assert!(matches!(
                &*f.handle.subscribe_sync_status().borrow(),
                SyncStatus::Syncing
            ));
            resume.send(()).unwrap();
            status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
            f.handle.start_sync().await.unwrap();
            status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
        }
    }
    let before = f.storage.request_count();
    f.advance(Duration::from_secs(60));
    assert!(f.handle.start_sync().await.is_err());
    assert_eq!(f.storage.request_count(), before);
}

struct Locations(Vec<Arc<MemoryStorage>>);
#[async_trait::async_trait]
impl StorageConnector for Locations {
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: coven_storage::StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<dyn Storage>, StorageError> {
        let storage = self
            .0
            .iter()
            .find(|storage| storage.config() == config)
            .ok_or(StorageError::InvalidConfiguration("unknown test location"))?;
        storage.connect(config, credentials, device).await
    }
}

#[tokio::test]
async fn moving_storage_retries_a_partial_copy_before_committing_the_new_location() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('kept','copied')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.advance(Duration::from_secs(1));
    f.handle.sync_now();
    status(
        &f.handle,
        |s| matches!(s, SyncStatus::Synced(r) if r.finished_at>=f.clock.now()),
    )
    .await;
    f.handle.close().await.unwrap();
    let own_objects = f.storage.list(&ObjectPrefix::all()).await.unwrap();
    // Both creations can publish after observing an empty location. Moving the
    // losing store must not copy the other store's authenticated origin or keys.
    let foreign = Fixture::new().await;
    foreign.setup().await.unwrap();
    status(&foreign.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    foreign.handle.close().await.unwrap();
    let foreign_objects = foreign.storage.list(&ObjectPrefix::all()).await.unwrap();
    for object in &foreign_objects {
        let bytes = foreign.storage.read(&object.path).await.unwrap();
        if object.path.is_replaceable() {
            f.storage.replace(&object.path, &bytes).await.unwrap();
        } else {
            f.storage.create(&object.path, &bytes).await.unwrap();
        }
    }
    let mut config = f.storage.config();
    let StorageConfig::S3 { prefix, .. } = &mut config else {
        unreachable!()
    };
    *prefix = "destination".into();
    let destination = Arc::new(
        MemoryStorage::new(config.clone(), f.clock.clone())
            .unwrap()
            .with_transfer_limits(16, 65536)
            .unwrap(),
    );
    let locations = Arc::new(Locations(vec![f.storage.clone(), destination.clone()]));
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        locations,
    )
    .open(f.directory.id())
    .await
    .unwrap();
    let before = handle.restore_code().await.unwrap();
    destination
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(handle
        .setup_s3_storage(
            config.clone(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into())
        )
        .await
        .is_err());
    assert_eq!(handle.restore_code().await.unwrap(), before);
    assert!(matches!(
        &*handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    handle
        .setup_s3_storage(
            config.clone(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.stop_sync();
    status(&handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let new = destination.list(&ObjectPrefix::all()).await.unwrap();
    assert!(foreign_objects
        .iter()
        .all(|foreign| !new.iter().any(|object| object.path == foreign.path)));
    for object in own_objects
        .iter()
        .filter(|object| !object.path.is_replaceable())
    {
        assert!(new.iter().any(|copied| copied.path == object.path));
        assert_eq!(
            f.storage.read(&object.path).await.unwrap(),
            destination.read(&object.path).await.unwrap()
        );
    }
    handle.close().await.unwrap();
}

#[tokio::test]
async fn a_first_pass_that_reaches_storage_then_loses_network_is_failed() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(ObjectPrefix::device_logs(), listed, resumed)
        .await;
    f.handle.start_sync().await.unwrap();
    listing.await.unwrap();
    f.storage.set_online(false);
    resume.send(()).unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline)
    })
    .await;
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Failed { .. }
    ));
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn forgetting_keys_finishes_the_active_pass_before_removing_custody() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(ObjectPrefix::device_logs(), listed, resumed)
        .await;
    f.handle.sync_now();
    listing.await.unwrap();
    let forget = f.handle.forget_store_keys();
    tokio::pin!(forget);
    tokio::select! {
        result = &mut forget => panic!("custody was removed during an active sync: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    resume.send(()).unwrap();
    forget.await.unwrap();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn repeated_setup_and_sync_requests_do_not_deadlock_credential_refresh() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced(_))).await;
    // Each round has its own generous limit: a deadlock never finishes, while
    // a slow filesystem (Windows CI) only takes longer.
    for round in 0..30 {
        tokio::time::timeout(Duration::from_secs(60), async {
            let setup = f.setup();
            let request = async {
                tokio::task::yield_now().await;
                f.handle.sync_now();
            };
            let (setup, ()) = tokio::join!(setup, request);
            setup.unwrap();
        })
        .await
        .unwrap_or_else(|_| {
            panic!("round {round}: setup and sync share a consistent credential lock order")
        });
    }
    f.handle.close().await.unwrap();
}

struct RefusePersist {
    keys: std::sync::Mutex<Option<StoreKeyring>>,
    refuse: AtomicBool,
}

#[tokio::test]
async fn moving_storage_waits_for_file_publication_before_copying_history() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let f = Fixture::new().await;
        f.handle.close().await.unwrap();
        let mut config = f.storage.config();
        let StorageConfig::S3 { prefix, .. } = &mut config else { unreachable!() };
        *prefix = "destination".into();
        let destination = Arc::new(MemoryStorage::new(config.clone(), f.clock.clone()).unwrap()
            .with_transfer_limits(1024 * 1024, 65536).unwrap());
        let handle = builder(&f.app, StoreLayout::new(f._root.path().into()), f.clock.clone(), Arc::new(Locations(vec![f.storage.clone(), destination.clone()])))
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new("files", Provenance::AppProvided, Uploads::WhenAttached, CacheFill::CacheLazy))])
            .migrations(vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)"),
                Migration::sql(2, "files", "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
            .open(f.directory.id()).await.unwrap();
        handle.setup_s3_storage(f.storage.config(), "Test device", "member-key".into(), SecretText::new("secret".into())).await.unwrap();
        status(&handle, |s| matches!(s, SyncStatus::Synced(_))).await;
        handle.stop_sync();
        status(&handle, |s| matches!(s, SyncStatus::Stopped)).await;
        handle.unlock_store_key().await.unwrap();
        let (started, uploading) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        f.storage.hold_next_creation(ObjectPrefix::files(), started, resumed).await;
        handle.write_with_files(|batch| {
            batch.put_file("files", "one", b"preserved file".to_vec());
            Ok(())
        }, |sql| { sql.execute("INSERT INTO files(id) VALUES('one')", [])?; Ok(()) }).await.unwrap();
        uploading.await.unwrap();
        let setup = handle.setup_s3_storage(config, "Test device", "member-key".into(), SecretText::new("secret".into()));
        tokio::pin!(setup);
        tokio::select! {
            result = &mut setup => panic!("setup changed location while a file was still publishing: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        resume.send(()).unwrap();
        setup.await.unwrap();
        let file = handle.file_ref("files", "one").await.unwrap();
        assert_eq!(file.location(), FileLocation::Uploaded);
        handle.evict_file(&file).await.unwrap();
        assert_eq!(handle.read_file(&file).await.unwrap(), b"preserved file");
        assert_eq!(destination.list(&ObjectPrefix::files()).await.unwrap().len(), 1);
        handle.stop_sync();
        status(&handle, |s| matches!(s, SyncStatus::Stopped)).await;
        handle.unlock_store_key().await.unwrap();
        destination.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
        handle.write_with_files(|batch| {
            batch.put_file("files", "partial", vec![91; 1024 * 1024 + 7]);
            Ok(())
        }, |sql| { sql.execute("INSERT INTO files(id) VALUES('partial')", [])?; Ok(()) }).await.unwrap();
        let mut uploads = handle.subscribe_uploads();
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.iter().any(|file| file.last_failure.is_some()) { break; }
        }
        handle.set_uploads_paused(true);
        handle.setup_s3_storage(f.storage.config(), "Test device", "member-key".into(), SecretText::new("secret".into())).await.unwrap();
        handle.set_uploads_paused(false);
        match handle.retry_uploads_now().await.unwrap() {
            DrainOutcome::Drained { failures, .. } => assert!(failures.is_empty(), "a recorded upload must resume at the new location"),
            DrainOutcome::QueueEmpty => {},
            _ => panic!("explicit retry did not drain the relocated file"),
        }
        let file = handle.file_ref("files", "partial").await.unwrap();
        assert_eq!(file.location(), FileLocation::Uploaded);
        handle.evict_file(&file).await.unwrap();
        assert_eq!(handle.read_file(&file).await.unwrap(), vec![91; 1024 * 1024 + 7]);
        handle.close().await.unwrap();
    }).await.expect("storage relocation completed");
}
impl StoreKeyCustody for RefusePersist {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        Ok(self.keys.lock().unwrap().clone())
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        if self.refuse.swap(false, Ordering::SeqCst) {
            return Err(KeyError::PassphraseAuthentication);
        }
        *self.keys.lock().unwrap() = Some(keys.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.keys.lock().unwrap().take();
        Ok(())
    }
}

#[tokio::test]
async fn custody_failure_rolls_back_credentials_and_the_reserved_creation_requires_the_same_access()
{
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.storage.clone(),
    )
    .key_custody(KeyCustody::Custom(Arc::new(RefusePersist {
        keys: std::sync::Mutex::new(None),
        refuse: AtomicBool::new(true),
    })))
    .open(f.directory.id())
    .await
    .unwrap();
    let setup = |key: &str| {
        handle.setup_s3_storage(
            f.storage.config(),
            "Test device",
            key.into(),
            SecretText::new("secret".into()),
        )
    };
    assert!(setup("original").await.is_err());
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(handle.restore_code().await.is_err());
    assert!(matches!(
        &*handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    let path = f.storage.list(&ObjectPrefix::store_logs()).await.unwrap()[0]
        .path
        .clone();
    let original = f.storage.read(&path).await.unwrap();
    assert!(
        setup("changed").await.is_err(),
        "a reserved creation cannot silently record another access key"
    );
    setup("original").await.unwrap();
    assert_eq!(f.storage.read(&path).await.unwrap(), original);
    handle.close().await.unwrap();
}

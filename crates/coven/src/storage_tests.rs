use super::*;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    Storage, StorageConnection, StorageSettings,
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, UNIX_EPOCH},
};

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
            MemoryStorage::builder()
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 64 * 1024)
                .build()
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
            providers: std::sync::Mutex::new(Vec::new()),
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
    clients: std::sync::Mutex<Vec<std::sync::Weak<StorageConnection>>>,
    credentials: std::sync::Mutex<Vec<StorageCredentials>>,
    providers: std::sync::Mutex<Vec<MemoryStorage>>,
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
        _device: DeviceId,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        if let Some((entered, resume)) = self.held.lock().await.take() {
            entered.send(()).unwrap();
            resume.await.unwrap();
        }
        if self.refuse.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Failure(StorageFailure::Authentication));
        }
        self.credentials.lock().unwrap().push(credentials.clone());
        let provider = self.storage.connect_device(config, credentials).await?;
        let client = provider.shared_connection();
        self.providers.lock().unwrap().push(provider);
        self.clients.lock().unwrap().push(Arc::downgrade(&client));
        Ok(client)
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
    ) -> Result<coven_storage::S3Detection, StorageError> {
        self.storage.detect_s3(config, credentials, store).await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
    ) -> Result<(), StorageError> {
        self.storage
            .check_new_s3_store(config, credentials, store)
            .await
    }
}

#[tokio::test]
async fn stopping_releases_the_provider_and_start_rebuilds_it_once() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 1);
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    let code = f.handle.restore_code().await.unwrap();
    f.handle
        .update_credentials(
            "replacement".into(),
            SecretText::new("replacement secret".into()),
        )
        .await
        .unwrap();
    assert_eq!(f.handle.restore_code().await.unwrap(), code);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_queued_during_cancelled_start_releases_the_new_client() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.disconnect_storage().await.unwrap();
    f.handle.start_sync().await.unwrap();
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    assert!(f.handle.restore_code().await.is_ok());
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.app.fail_next_keychain_operation();
    assert!(matches!(
        f.handle.disconnect_storage().await,
        Err(SyncError::SecureStorage(_))
    ));
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Synced { .. }
    ));
    let code = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(
            ObjectPrefix::device_logs(f.directory.id()),
            entered,
            resumed,
        )
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
    assert!(f.handle.restore_code().await.is_ok());
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
        |s| matches!(s, SyncStatus::Synced { finished_at } if *finished_at >= written),
    )
    .await;
    assert_eq!(
        f.storage
            .list(&ObjectPrefix::device_logs(f.directory.id()))
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
async fn unlocking_preserves_the_requested_connection_lifetime() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_is_offline_until_reached_and_stop_and_close_finish_the_active_pass() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.storage.set_online(false);
    let mut requests = f.storage.subscribe_requests();
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    requests.wait_for(|n| *n > before).await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Offline { .. })).await;
    {
        let subscription = f.handle.subscribe_sync_status();
        let status = subscription.borrow();
        let SyncStatus::Offline { error } = &*status else {
            panic!("offline storage")
        };
        assert_eq!(error.failure(), StorageFailure::Network);
        assert!(std::error::Error::source(error.as_ref())
            .unwrap()
            .is::<std::io::Error>());
    }
    f.storage.set_online(true);
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    for closing in [false, true] {
        let (listed, listing) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        f.storage
            .hold_next_listing(ObjectPrefix::device_logs(f.directory.id()), listed, resumed)
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
            status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
    ) -> Result<Arc<StorageConnection>, StorageError> {
        let storage = self
            .0
            .iter()
            .find(|storage| storage.config() == config)
            .ok_or(StorageFailure::InvalidConfiguration.with_source("unknown test location"))?;
        storage.connect(config, credentials, device).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
    ) -> Result<coven_storage::S3Detection, StorageError> {
        self.0[0].detect_s3(config, credentials, store).await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
    ) -> Result<(), StorageError> {
        self.0[0]
            .check_new_s3_store(config, credentials, store)
            .await
    }
}

#[tokio::test]
async fn two_stores_share_one_location_without_reading_each_others_history() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let layout = StoreLayout::new(f._root.path().into());
    let directory = f
        .app
        .create_store(&layout, "Garden", Arc::new(UuidIds))
        .await
        .unwrap();
    let other = builder(&f.app, layout, f.clock.clone(), f.connector.clone())
        .open(directory.id())
        .await
        .unwrap();
    other.initialize_identity().unwrap();
    other
        .setup_s3_storage(
            f.storage.config(),
            "Garden device",
            "garden-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    status(&other, |s| matches!(s, SyncStatus::Synced { .. })).await;
    for (handle, body) in [(&f.handle, "Household"), (&other, "Garden")] {
        handle
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES('same', ?1)", [body])?;
                Ok(())
            })
            .await
            .unwrap();
    }
    f.advance(Duration::from_secs(1));
    for handle in [&f.handle, &other] {
        handle.sync_now();
        status(
            handle,
            |s| matches!(s, SyncStatus::Synced { finished_at } if *finished_at >= f.clock.now()),
        )
        .await;
    }
    for (handle, expected) in [(&f.handle, "Household"), (&other, "Garden")] {
        let body = handle
            .read(|sql| {
                Ok(
                    sql.query_row("SELECT body FROM notes WHERE id='same'", [], |row| {
                        row.get::<_, String>(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(body, expected);
        handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_first_pass_that_reaches_storage_then_loses_network_is_failed() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(ObjectPrefix::device_logs(f.directory.id()), listed, resumed)
        .await;
    f.handle.start_sync().await.unwrap();
    listing.await.unwrap();
    f.storage.set_online(false);
    resume.send(()).unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline { .. })
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_listing(ObjectPrefix::device_logs(f.directory.id()), listed, resumed)
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
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
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
async fn setup_refuses_another_location_after_disconnect_and_reopen() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let mut config = f.storage.config();
    let StorageConfig::S3 { prefix, .. } = &mut config else {
        unreachable!()
    };
    *prefix = "destination".into();
    let destination = Arc::new(
        MemoryStorage::builder()
            .location(config.clone())
            .build()
            .unwrap(),
    );
    let before = f.handle.restore_code().await.unwrap();
    let error = f
        .handle
        .setup_s3_storage(
            config.clone(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "storage location change is unsupported");
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle.disconnect_storage().await.unwrap();
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        Arc::new(Locations(vec![f.storage.clone(), destination.clone()])),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    let error = handle
        .setup_s3_storage(
            config,
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "storage location change is unsupported");
    assert_eq!(destination.request_count(), 0);
    assert_eq!(
        StorageSettings::new(f.directory.clone()).read().unwrap(),
        Some(f.storage.config())
    );
    handle
        .setup_s3_storage(
            f.storage.config(),
            "Ignored",
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.close().await.unwrap();
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
    fn close(&self) {
        self.keys.lock().unwrap().take();
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
    let path = f
        .storage
        .list(&ObjectPrefix::store_logs(f.directory.id()))
        .await
        .unwrap()[0]
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

#[tokio::test]
async fn setup_rejects_overwriting_providers_before_committing() {
    let f = Fixture::new().await;
    f.storage
        .set_faults(Faults {
            overwrite_create: true,
            ..Faults::none()
        })
        .await;
    assert!(
        matches!(f.setup().await.unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(matches!(
        f.handle.restore_code().await,
        Err(SyncError::NoStorage)
    ));
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert!(f
        .storage
        .list(&ObjectPrefix::all(f.directory.id()))
        .await
        .unwrap()
        .is_empty());
    assert!(f.handle.get_members().await.unwrap().is_empty());
    assert_eq!(
        StorageSettings::new(f.directory.clone()).read().unwrap(),
        None
    );
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .setup_s3_storage(
            f.storage.config(),
            "Another name",
            "another-key".into(),
            SecretText::new("another secret".into()),
        )
        .await
        .unwrap();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_checks_the_provider_and_preserves_the_previous_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let before = f.handle.restore_code().await.unwrap();
    let settings = StorageSettings::new(f.directory.clone());
    let location = settings.read().unwrap();
    let objects = f
        .storage
        .list(&ObjectPrefix::all(f.directory.id()))
        .await
        .unwrap();
    f.storage
        .set_faults(Faults {
            overwrite_create: true,
            ..Faults::none()
        })
        .await;
    let error = f
        .handle
        .setup_s3_storage(
            f.storage.config(),
            "Ignored name",
            "replacement".into(),
            SecretText::new("new secret".into()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_eq!(settings.read().unwrap(), location);
    assert_eq!(
        f.handle.store_key_state().unwrap(),
        StoreKeyState::Available
    );
    assert_eq!(
        f.storage
            .list(&ObjectPrefix::all(f.directory.id()))
            .await
            .unwrap(),
        objects
    );
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.storage.set_faults(Faults::none()).await;
    f.handle.disconnect_storage().await.unwrap();
    f.storage
        .set_faults(Faults {
            overwrite_create: true,
            ..Faults::none()
        })
        .await;
    assert!(
        matches!(f.setup().await.unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    assert_eq!(
        f.storage
            .list(&ObjectPrefix::all(f.directory.id()))
            .await
            .unwrap(),
        objects
    );
    f.storage.set_faults(Faults::none()).await;
    f.setup().await.unwrap();
    f.handle.close().await.unwrap();
}

#[path = "storage_credentials_tests.rs"]
mod credential_changes;

#[path = "storage_s3_tests.rs"]
mod s3_setup;

use super::*;
use crate::tests::wait_for_sent;
use coven_foundation::progress::RecordingClock;
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
    /// The handle's clock: `clock`'s time, recording the steps of a setup
    /// whose watch a test hands it.
    recorder: Arc<RecordingClock<StorageSetupStep>>,
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
            .create_closed_store(&StoreLayout::new(root.path().into()), "Loop", Arc::new(UuidIds))
            .await
            .unwrap();
        let connector = Arc::new(TrackingConnector {
            storage: storage.clone(),
            clients: std::sync::Mutex::new(Vec::new()),
            credentials: std::sync::Mutex::new(Vec::new()),
            providers: std::sync::Mutex::new(Vec::new()),
            failure: std::sync::Mutex::new(None),
            attempts: tokio::sync::watch::channel(0).0,
            held: tokio::sync::Mutex::new(None),
        });
        let recorder = Arc::new(RecordingClock::new(clock.clone()));
        let handle = builder(
            &app,
            StoreLayout::new(root.path().into()),
            recorder.clone(),
            connector.clone(),
        )
        .open(directory.id(), |_| ())
        .await
        .unwrap();

        Self { app, handle, storage, connector, clock, recorder, directory, _root: root }
    }

    async fn setup(&self) -> Result<StorageSetup, StorageSetupError> {
        self.handle
            .setup_s3_storage(
                self.storage.config(),
                "Test device",
                "member-key".into(),
                SecretText::new("secret".into()),
                |_| {},
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
    failure: std::sync::Mutex<Option<StorageError>>,
    attempts: tokio::sync::watch::Sender<usize>,
    held: tokio::sync::Mutex<
        Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>,
    >,
}

#[async_trait::async_trait]
impl StorageConnector for TrackingConnector {
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.attempts.send_modify(|attempts| *attempts += 1);
        if let Some((entered, resume)) = self.held.lock().await.take() {
            entered.send(()).unwrap();
            resume.await.unwrap();
        }
        if let Some(error) = self.failure.lock().unwrap().take() {
            return Err(error);
        }
        self.credentials.lock().unwrap().push(auth.credentials());
        let provider = self
            .storage
            .connect_device(config, auth)
            .await?
            .with_device(device)
            .with_publication_gate(publications)
            .with_cooldown(cooldown)
            .with_transfers(transfers);
        let provider = match setup {
            Some(waits) => provider.during_setup(waits),
            None => provider,
        };
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
        waits: &coven_storage::SetupWaits,
    ) -> Result<coven_storage::S3Detection, StorageError> {
        self.storage.detect_s3(config, credentials, store, waits).await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_s3_store(config, credentials, store, waits).await
    }
}

#[tokio::test]
async fn stopping_releases_the_provider_and_start_rebuilds_it_once() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 1);
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    let code = f.handle.restore_code().await.unwrap();
    f.handle
        .update_credentials("replacement".into(), SecretText::new("replacement secret".into()))
        .await
        .unwrap();
    assert_eq!(f.handle.restore_code().await.unwrap(), code);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    let credentials = f.connector.credentials.lock().unwrap()[1].clone();
    let StorageCredentials::S3(keys) = credentials else { panic!("S3 credentials") };
    assert_eq!(keys.access_key_id, "replacement");
    assert_eq!(keys.secret_access_key.as_str(), "replacement secret");
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_first_connection_shows_in_status_and_retries_after_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { failure: StatusFailure::Storage(failure), .. } | SyncStatus::Offline { failure, .. }
        if *failure == StorageFailure::Network)
    })
    .await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    // The loop stays started: the pass after backoff connects.
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.app.fail_next_keychain_operation();
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Failed { .. })).await;
    // A custody failure clears by itself, as a locked keychain does once
    // unlocked: the pass after backoff reads the credentials again (§19.1).
    assert!(f.handle.test_connection_retry_wait().await.is_some());
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_queued_while_the_first_pass_connects_releases_the_new_client() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    // Starting writes `Syncing` before it returns, so the `Stopped` after it
    // comes from the queued stop.
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync().await.unwrap();
    resume.send(()).unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    assert!(f.connector.clients.lock().unwrap().iter().all(|client| client.upgrade().is_none()));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn unconfigured_and_forgotten_storage_stay_disconnected_on_start() {
    let f = Fixture::new().await;
    f.handle.stop_sync().await.unwrap();
    f.handle.start_sync().await.unwrap();
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert!(f.connector.clients.lock().unwrap().is_empty());
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.disconnect_storage().await.unwrap();
    f.handle.start_sync().await.unwrap();
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    assert!(f.handle.restore_code().await.is_ok());
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id(), |_| ())
    .await
    .unwrap();
    handle.start_sync().await.unwrap();
    assert!(matches!(&handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn disconnect_waits_for_the_active_pass_and_custody_failure_keeps_sync_running() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.app.fail_next_keychain_operation();
    assert!(matches!(f.handle.disconnect_storage().await, Err(SyncError::SecureStorage(_))));
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Synced { .. }));
    let code = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed).await;
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
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    assert!(f.handle.restore_code().await.is_ok());
    f.handle.close().await.unwrap();
}

/// An invite ends when its lifetime does with storage disconnected too: the
/// handle writes its expiry, which the app's live query hears, and so does
/// opening for one that ended while the store was closed (§13.2). The
/// ended invite's own work waits for a connected pass.
#[tokio::test]
async fn an_invite_expires_with_storage_disconnected_and_while_closed() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let invite = || async {
        crate::tests::invite(
            &f.handle,
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::S3AccessKey {
                access_key_id: "carol-key".into(),
                secret_access_key: SecretText::new("carol-secret".into()),
            },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Hour },
        )
        .await
    };
    let mut invites = f.handle.test_invites();
    assert!(invites.next().await.is_empty());
    // Made after the handle opened, so its expiry joins the handle's wakes.
    let first = invite().await;
    assert_eq!(invites.next().await, std::slice::from_ref(&first));
    f.handle.disconnect_storage().await.unwrap();
    f.clock.set(first.expires_at - Duration::from_secs(1));
    let early = tokio::time::timeout(Duration::from_millis(200), invites.next()).await;
    assert!(early.is_err(), "the invite ended before its lifetime: {early:?}");
    f.clock.set(first.expires_at);
    assert!(invites.next().await.is_empty());
    f.setup().await.unwrap();
    let second = invite().await;
    assert_eq!(invites.next().await, std::slice::from_ref(&second));
    drop(invites);
    f.handle.close().await.unwrap();
    f.clock.set(second.expires_at);
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id(), |_| ())
    .await
    .unwrap();
    assert!(handle.read(Invite::list).await.unwrap().is_empty());
    handle.close().await.unwrap();
}

fn builder(
    app: &TestCoven,
    layout: StoreLayout,
    clock: ClockRef,
    storage: Arc<dyn StorageConnector>,
) -> CovenBuilder {
    app.builder(layout)
        .clock(clock)
        .storage_connector(storage)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
        )])
}

async fn status(handle: &CovenHandle, expected: impl Fn(&SyncStatus) -> bool) {
    handle.test_sync_status().wait_for(expected).await;
}

/// Run an S3 setup of `f`'s location on `handle`, whose clock is `recorder`,
/// and return every step it reported, as `(step, done, total)`.
async fn recorded_setup(
    f: &Fixture,
    handle: &CovenHandle,
    recorder: &RecordingClock<StorageSetupStep>,
) -> Vec<(StorageSetupStep, u64, u64)> {
    let (progress, watched) = tokio::sync::watch::channel(None);
    recorder.watch(watched);
    handle
        .setup_s3_reporting(
            f.storage.config(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
            progress,
        )
        .await
        .unwrap();
    recorder.seen().into_iter().map(|seen| (seen.step, seen.done, seen.total)).collect()
}

/// Ana's first setup of the Household store, in a bucket whose member keys
/// reach all of it, reports each step it runs, in order, each begun and then
/// done. Her reconnect after the store is disconnected and reopened checks no
/// bucket and creates nothing: it detects, connects, and finds nothing new to
/// load and no access to record.
#[tokio::test]
async fn setup_reports_each_step_it_runs() {
    use StorageSetupStep::*;
    let f = Fixture::new().await;
    assert_eq!(
        recorded_setup(&f, &f.handle, &f.recorder).await,
        [
            (CheckingBucket, 0, 1),
            (CheckingBucket, 1, 1),
            (DetectingSettings, 0, 1),
            (DetectingSettings, 1, 1),
            (Connecting, 0, 1),
            (Connecting, 1, 1),
            (CreatingStore, 0, 1),
            (CreatingStore, 1, 1),
        ]
    );
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.disconnect_storage().await.unwrap();
    f.handle.close().await.unwrap();

    let recorder = Arc::new(RecordingClock::new(f.clock.clone()));
    let layout = StoreLayout::new(f._root.path().into());
    let reopened = builder(&f.app, layout, recorder.clone(), f.connector.clone())
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(
        recorded_setup(&f, &reopened, &recorder).await,
        [
            (DetectingSettings, 0, 1),
            (DetectingSettings, 1, 1),
            (Connecting, 0, 1),
            (Connecting, 1, 1),
        ]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn setup_starts_sync_and_committed_writes_wake_it_without_a_tick() {
    let f = Fixture::new().await;
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    let connected = f.setup().await.unwrap();
    assert_eq!(connected.connection.key_state, StoreKeyState::Available);
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('one','written')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    wait_for_sent(&f.handle).await;
    assert!(f.handle.test_queued_writes().await.unwrap().is_empty());
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_start_idle_tick_offline_recovery_and_close_use_the_injected_clock() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
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
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(f.storage.request_count(), before);
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage.set_faults(Faults { fail_next: usize::MAX, ..Faults::none() }).await;
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Offline { .. })).await;
    f.storage.set_faults(Faults::none()).await;
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
    let closed = f.storage.request_count();
    f.advance(Duration::from_secs(90));
    assert!(f.handle.start_sync().await.is_err());
    assert_eq!(f.storage.request_count(), closed);
    assert!(matches!(f.handle.store_key_state(), Err(KeyError::StoreClosed)));
}

#[tokio::test]
async fn unlocking_preserves_the_requested_connection_lifetime() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.app.fail_next_keychain_operation();
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Stopped));
    f.handle.forget_store_keys().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(f.handle.start_sync().await.is_err());
    let opened = f.handle.unlock_store_key().await.unwrap();
    assert_eq!(opened.key_state, StoreKeyState::Available);
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Stopped));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnecting_without_network_is_offline_and_stop_and_close_finish_the_active_pass() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.storage.set_online(false);
    let mut requests = f.storage.subscribe_requests();
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    requests.wait_for(|n| *n > before).await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Offline { .. })).await;
    {
        let status = f.handle.test_sync_status().latest().await;
        let SyncStatus::Offline { failure, .. } = status else { panic!("offline storage") };
        assert_eq!(failure, StorageFailure::Network);
    }
    f.storage.set_online(true);
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    for closing in [false, true] {
        let (listed, listing) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), listed, resumed).await;
        f.handle.sync_now();
        listing.await.unwrap();
        assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Syncing));
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
            f.handle.stop_sync().await.unwrap();
            tokio::task::yield_now().await;
            assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Syncing));
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
        auth: coven_storage::ProviderAuth,
        device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        let storage = self
            .0
            .iter()
            .find(|storage| storage.config() == config)
            .ok_or(StorageFailure::InvalidConfiguration.with_source("unknown test location"))?;
        storage.connect(config, auth, device, publications, cooldown, transfers, setup).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<coven_storage::S3Detection, StorageError> {
        self.0[0].detect_s3(config, credentials, store, waits).await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.0[0].check_new_s3_store(config, credentials, store, waits).await
    }
}

/// Reconnecting preserves both the failed attempt's ordinary delay and
/// the provider's independently persisted cooldown.
#[tokio::test]
async fn reconnecting_preserves_backoff_and_the_providers_cooldown() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage.set_online(false);
    f.handle.sync_now();
    status(&f.handle, |s| matches!(s, SyncStatus::Offline { .. })).await;
    f.storage.set_online(true);
    // The failed connection backs off: no pass runs until its timer ends.
    let before = f.storage.request_count();
    f.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.storage.request_count(), before);
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.handle.start_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.storage.request_count(), before);
    f.advance(Duration::from_secs(1));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(600),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, |s| matches!(s, SyncStatus::Failed { .. })).await;
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        f.storage.request_count(),
        before,
        "a reconnected pass ran inside the provider's cooldown"
    );
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Syncing));
    f.advance(Duration::from_secs(600));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn two_stores_share_one_location_without_reading_each_others_history() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let layout = StoreLayout::new(f._root.path().into());
    let directory = f.app.create_closed_store(&layout, "Garden", Arc::new(UuidIds)).await.unwrap();
    let other = builder(&f.app, layout, f.clock.clone(), f.connector.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    other
        .setup_s3_storage(
            f.storage.config(),
            "Garden device",
            "garden-key".into(),
            SecretText::new("secret".into()),
            |_| {},
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
    for handle in [&f.handle, &other] {
        wait_for_sent(handle).await;
    }
    for (handle, expected) in [(&f.handle, "Household"), (&other, "Garden")] {
        let body = handle
            .read(|sql| {
                Ok(sql.query_row("SELECT body FROM notes WHERE id='same'", [], |row| {
                    row.get::<_, String>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(body, expected);
        handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn forgetting_keys_finishes_the_active_pass_before_removing_custody() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), listed, resumed).await;
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
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let mut config = f.storage.config();
    let StorageConfig::S3 { prefix, .. } = &mut config else { unreachable!() };
    *prefix = crate::S3Prefix::parse("destination").unwrap();
    let destination = Arc::new(MemoryStorage::builder().location(config.clone()).build().unwrap());
    let before = f.handle.restore_code().await.unwrap();
    let error = f
        .handle
        .setup_s3_storage(
            config.clone(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
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
    .open(f.directory.id(), |_| ())
    .await
    .unwrap();
    let error = handle
        .setup_s3_storage(
            config,
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "storage location change is unsupported");
    assert_eq!(destination.request_count(), 0);
    assert_eq!(StorageSettings::new(f.directory.clone()).read().unwrap(), Some(f.storage.config()));
    handle
        .setup_s3_storage(
            f.storage.config(),
            "Ignored",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
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
async fn setup_rejects_overwriting_providers_before_committing() {
    let f = Fixture::new().await;
    f.storage.set_faults(Faults { overwrite_create: true, ..Faults::none() }).await;
    assert!(
        matches!(f.setup().await.unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(matches!(f.handle.restore_code().await, Err(SyncError::NoStorage)));
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap().is_empty());
    assert!(f.handle.members().await.unwrap().is_empty());
    assert_eq!(StorageSettings::new(f.directory.clone()).read().unwrap(), None);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .setup_s3_storage(
            f.storage.config(),
            "Another name",
            "another-key".into(),
            SecretText::new("another secret".into()),
            |_| {},
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
    f.handle.stop_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let before = f.handle.restore_code().await.unwrap();
    let settings = StorageSettings::new(f.directory.clone());
    let location = settings.read().unwrap();
    let objects = f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap();
    f.storage.set_faults(Faults { overwrite_create: true, ..Faults::none() }).await;
    let error = f
        .handle
        .setup_s3_storage(
            f.storage.config(),
            "Ignored name",
            "replacement".into(),
            SecretText::new("new secret".into()),
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_eq!(settings.read().unwrap(), location);
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert_eq!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap(), objects);
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Stopped));
    f.storage.set_faults(Faults::none()).await;
    f.handle.disconnect_storage().await.unwrap();
    f.storage.set_faults(Faults { overwrite_create: true, ..Faults::none() }).await;
    assert!(
        matches!(f.setup().await.unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::CreateOnce, source } if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert!(matches!(&f.handle.test_sync_status().latest().await, SyncStatus::Disconnected));
    assert_eq!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap(), objects);
    f.storage.set_faults(Faults::none()).await;
    f.setup().await.unwrap();
    f.handle.close().await.unwrap();
}

#[path = "storage_credentials_tests.rs"]
mod credential_changes;

#[path = "storage_s3_tests.rs"]
mod s3_setup;

#[path = "storage_control_tests.rs"]
mod control;

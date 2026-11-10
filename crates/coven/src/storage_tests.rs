use super::*;
use crate::tests::{last_pass, synced, wait_for_sent};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::progress::RecordingClock;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    Storage, StorageConnection,
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, UNIX_EPOCH},
};

struct Fixture {
    app: Coven,
    handle: CovenHandle,
    storage: Arc<MemoryStorage>,
    connector: Arc<TrackingConnector>,
    clock: Arc<FixedClock>,
    /// The handle's clock: `clock`'s time, recording the steps of a setup
    /// whose watch a test hands it.
    recorder: Arc<RecordingClock<StorageSetupStep>>,
    directory: StoreDir,
    /// The app's bug sink, which keeps every bug it hears of.
    bugs: Arc<crate::task::HeardBugs>,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let bugs = crate::task::HeardBugs::shared();
        let app = Coven::in_memory_reporting_to(root.path().into(), bugs.clone());
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 64 * 1024)
                .build()
                .unwrap(),
        );
        let directory = app.create_closed_store("Loop", Arc::new(UuidIds)).await.unwrap();
        let connector = Arc::new(TrackingConnector {
            storage: storage.clone(),
            clients: std::sync::Mutex::new(Vec::new()),
            credentials: std::sync::Mutex::new(Vec::new()),
            providers: std::sync::Mutex::new(Vec::new()),
            failure: std::sync::Mutex::new(None),
            panics: AtomicBool::new(false),
            attempts: tokio::sync::watch::channel(0).0,
            counted: crate::tests::Clients::new(),
            held: tokio::sync::Mutex::new(None),
        });
        let recorder = Arc::new(RecordingClock::new(clock.clone()));
        let handle = builder(&app, recorder.clone(), connector.clone())
            .open(directory.id(), |_| ())
            .await
            .unwrap();

        Self { app, handle, storage, connector, clock, recorder, directory, bugs, _root: root }
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

    /// Close the handle, delete this device's storage credentials as the
    /// person deleting the keychain entry does, and open the store again: its
    /// location stays recorded, with no storage set up on this device.
    async fn lose_credentials(&mut self) {
        self.handle.close().await.unwrap();
        self.app.lose_storage_credentials(self.directory.id()).unwrap();
        self.handle = builder(&self.app, self.recorder.clone(), self.connector.clone())
            .open(self.directory.id(), |_| ())
            .await
            .unwrap();
    }
}

struct TrackingConnector {
    storage: Arc<MemoryStorage>,
    clients: std::sync::Mutex<Vec<std::sync::Weak<StorageConnection>>>,
    credentials: std::sync::Mutex<Vec<StorageCredentials>>,
    providers: std::sync::Mutex<Vec<MemoryStorage>>,
    failure: std::sync::Mutex<Option<StorageError>>,
    /// Panic in the next attempt, as a bug in connecting would.
    panics: AtomicBool,
    attempts: tokio::sync::watch::Sender<usize>,
    /// The same clients, counted as they are made and dropped.
    counted: crate::tests::Clients,
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
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.attempts.send_modify(|attempts| *attempts += 1);
        if let Some((entered, resume)) = self.held.lock().await.take() {
            entered.send(()).unwrap();
            resume.await.unwrap();
        }
        assert!(!self.panics.swap(false, Ordering::SeqCst), "connecting panicked");
        if let Some(error) = self.failure.lock().unwrap().take() {
            return Err(error);
        }
        self.credentials.lock().unwrap().push(auth.credentials());
        let provider = self
            .storage
            .connect_device(config, auth)
            .await?
            .with_device(device)
            .with_cooldown(cooldown)
            .with_transfers(transfers);
        let client = provider.with_publication_gate(self.counted.gate(publications.clone()));
        let provider = provider.with_publication_gate(publications);
        let client = client.shared_connection();
        self.providers.lock().unwrap().push(provider);
        self.clients.lock().unwrap().push(Arc::downgrade(&client));
        Ok(client)
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        self.storage.detect_s3(config, credentials, store, runs, waits).await
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_store(config, auth, store, waits).await
    }
}

/// Stop sync, which returns once the provider's client is released (E5).
async fn stop(f: &Fixture) {
    f.connector.counted.stop(&f.handle).await;
}

#[tokio::test]
async fn stopping_releases_the_provider_and_start_rebuilds_it_once() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 1);
    stop(&f).await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    let code = f.handle.restore_code().await.unwrap();
    f.handle
        .update_credentials("replacement".into(), SecretText::new("replacement secret".into()))
        .await
        .unwrap();
    assert_eq!(f.handle.restore_code().await.unwrap(), code);
    f.advance(Duration::from_secs(1));
    f.handle.start_sync().await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
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
    synced(&f.handle).await;
    stop(&f).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, |last| {
        matches!(last, Some(LastPass::Failed { failure: StatusFailure::Storage(failure), .. } | LastPass::Offline { failure, .. })
        if *failure == StorageFailure::Network)
    })
    .await;
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_none());
    // The loop stays started: the pass after backoff connects.
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
    stop(&f).await;
    f.app.fail_next_keychain_operation();
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, |last| matches!(last, Some(LastPass::Failed { .. }))).await;
    // A custody failure clears by itself, as a locked keychain does once
    // unlocked: the pass after backoff reads the credentials again (§19.1).
    assert!(f.handle.test_connection_retry_wait().await.is_some());
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stop_queued_while_the_first_pass_connects_releases_the_new_client() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    let stopping = crate::tests::stopping(&f.handle);
    resume.send(()).unwrap();
    // The stop waits for the attempt, then releases the client it made.
    stopping.await.unwrap().unwrap();
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    assert!(f.connector.clients.lock().unwrap().iter().all(|client| client.upgrade().is_none()));
    f.advance(Duration::from_secs(1));
    f.handle.start_sync().await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    f.handle.close().await.unwrap();
}

/// The first pass's connection panics while the person's retry waits for
/// it: the app's bug sink hears the bug, the last pass shows it, and the
/// retry is answered with it as `WorkerFailed`, which the app doesn't report
/// again (E15).
#[tokio::test]
async fn a_panicking_connection_attempt_reaches_the_apps_bug_sink() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    let answer = f.handle.retry_now().await.unwrap();
    f.connector.panics.store(true, Ordering::SeqCst);
    resume.send(()).unwrap();
    let answer = tokio::time::timeout(HANG_GUARD, answer).await.unwrap();
    let coven_sync::RetryOutcome::Failed(coven_sync::PassFailure::Failed {
        error: coven_sync::SyncFailure::Other(error),
    }) = &answer
    else {
        panic!("the retry heard no failure: {answer:?}");
    };
    let [bug] = <[_; 1]>::try_from(f.bugs.take()).expect("the sink heard the one bug");
    assert_eq!(
        (bug.operation, bug.message.as_str()),
        ("connect to storage", "connecting panicked")
    );
    assert_eq!(
        bug.location.as_ref().map(|at| at.file.as_str()),
        Some(file!()),
        "the bug names where the connector panicked"
    );
    assert!(
        matches!(error.downcast_ref::<SyncError>(), Some(SyncError::WorkerFailed(heard)) if *heard == bug),
        "{error:?}"
    );
    last_pass(&f.handle, |last| {
        matches!(last, Some(LastPass::Failed { error, .. }) if error.contains("connecting panicked"))
    })
    .await;
    f.handle.close().await.unwrap();
}

/// The first pass's connection fails, and the write saving its retry wait
/// panics: nobody waits for the attempt, so the bug goes to the app's bug
/// sink, once, and the attempt stops as on any failed write of its own
/// (E15).
#[tokio::test]
async fn a_bug_saving_a_failed_connections_wait_reaches_the_sink() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.test_panic_in_next_write("the connection's wait broke").await;
    resume.send(()).unwrap();
    let bug = tokio::time::timeout(HANG_GUARD, f.bugs.next()).await.expect("the sink hears it");
    assert_eq!(bug.message, "the connection's wait broke");
    f.handle.close().await.unwrap();
    assert!(f.bugs.take().is_empty(), "the sink heard the bug once");
}

#[tokio::test]
async fn unconfigured_storage_and_lost_credentials_stay_disconnected_on_start() {
    let mut f = Fixture::new().await;
    f.handle.stop_sync().await.unwrap();
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.handle.test_sync_status().latest().await.state, SyncState::NotSynced);
    assert!(f.connector.clients.lock().unwrap().is_empty());
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.lose_credentials().await;
    f.handle.start_sync().await.unwrap();
    assert_eq!(
        f.handle.test_sync_status().latest().await.state,
        SyncState::SignedOut { provider: CloudProvider::S3, cause: None }
    );
    {
        let clients = f.connector.clients.lock().unwrap();
        assert_eq!(clients.len(), 1, "starting without credentials connects nothing");
        assert!(clients[0].upgrade().is_none());
    }
    assert!(f.handle.restore_code().await.is_ok());
    f.handle.close().await.unwrap();
}

/// An invite ends when its lifetime does with sync stopped too: the handle
/// writes its expiry, which the app's live query hears, and so does opening
/// for one that ended while the store was closed (§13.2). The ended invite's
/// own work waits for a connected pass.
#[tokio::test]
async fn an_invite_expires_with_sync_stopped_and_while_closed() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let invite = || async {
        crate::tests::invite(
            &f.handle,
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::S3AccessKey {
                access_key_id: "carol-key".into(),
                secret_access_key: SecretText::new("carol-secret".into()),
            },
            crate::InviteLifetime::Hour,
        )
        .await
    };
    let mut invites = f.handle.test_invites();
    assert!(invites.next().await.is_empty());
    // Made after the handle opened, so its expiry joins the handle's wakes.
    let first = invite().await;
    assert_eq!(invites.next().await, std::slice::from_ref(&first));
    f.handle.stop_sync().await.unwrap();
    f.clock.set(first.expires_at - Duration::from_secs(1));
    f.handle.test_parked().await;
    let open = f.handle.test_invites().next().await;
    assert_eq!(open, std::slice::from_ref(&first), "the invite ended before its lifetime");
    f.clock.set(first.expires_at);
    assert!(invites.next().await.is_empty());
    f.setup().await.unwrap();
    let second = invite().await;
    assert_eq!(invites.next().await, std::slice::from_ref(&second));
    drop(invites);
    f.handle.close().await.unwrap();
    f.clock.set(second.expires_at);
    let handle = builder(&f.app, f.clock.clone(), f.connector.clone())
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    assert!(handle.read(Invite::list).await.unwrap().is_empty());
    handle.close().await.unwrap();
}

fn builder(app: &Coven, clock: ClockRef, storage: Arc<dyn StorageConnector>) -> CovenBuilder {
    app.builder()
        .clock(clock)
        .storage_connector(storage)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
        )])
}

/// Wait for a pass that ended synced at `at` or later, by the store's
/// clock: where the last pass may already be synced, the test moves the
/// clock first, and the last synced time shows the pass after.
async fn synced_since(handle: &CovenHandle, at: std::time::SystemTime) {
    handle.test_sync_status().wait_for(|status| status.last_synced >= Some(at)).await;
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
/// done. Her reconnect after this device lost its credentials and reopened
/// checks no bucket and creates nothing: it detects, connects, and finds
/// nothing new to load and no access to record.
#[tokio::test]
async fn setup_reports_each_step_it_runs() {
    use StorageSetupStep::*;
    let f = Fixture::new().await;
    assert_eq!(
        recorded_setup(&f, &f.handle, &f.recorder).await,
        [
            (CheckingLocation, 0, 1),
            (CheckingLocation, 1, 1),
            (DetectingSettings, 0, 1),
            (DetectingSettings, 1, 1),
            (Connecting, 0, 1),
            (Connecting, 1, 1),
            (CreatingStore, 0, 1),
            (CreatingStore, 1, 1),
        ]
    );
    synced(&f.handle).await;
    f.handle.close().await.unwrap();
    f.app.lose_storage_credentials(f.directory.id()).unwrap();

    let recorder = Arc::new(RecordingClock::new(f.clock.clone()));
    let reopened = builder(&f.app, recorder.clone(), f.connector.clone())
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
    assert_eq!(f.handle.test_sync_status().latest().await.state, SyncState::NotSynced);
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    let connected = f.setup().await.unwrap();
    assert_eq!(connected.connection.key_state, StoreKeyState::Available);
    synced(&f.handle).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('one','written')", [])?;
            Ok::<_, CovenError>(())
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
    synced(&f.handle).await;
    stop(&f).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('held','offline')", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    let before = f.storage.request_count();
    f.advance(Duration::from_secs(90));
    f.handle.sync_now();
    stop(&f).await;
    assert_eq!(f.storage.request_count(), before);
    // The clock moved since the last pass, so the next one's end moves the
    // last synced time.
    f.handle.start_sync().await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    f.storage.set_faults(Faults { fail_next: usize::MAX, ..Faults::none() }).await;
    f.advance(Duration::from_secs(30));
    last_pass(&f.handle, |last| matches!(last, Some(LastPass::Offline { .. }))).await;
    f.storage.set_faults(Faults::none()).await;
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
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
    synced(&f.handle).await;
    stop(&f).await;
    f.app.fail_next_keychain_operation();
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    // Neither forgetting records a failure: the last pass still ended synced.
    assert_eq!(f.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    f.handle.forget_store_keys().await.unwrap();
    assert_eq!(f.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(f.handle.start_sync().await.is_err());
    let opened = f.handle.unlock_store_key().await.unwrap();
    assert_eq!(opened.key_state, StoreKeyState::Available);
    assert_eq!(f.handle.test_sync_status().latest().await.state, SyncState::Synced);
    f.advance(Duration::from_secs(1));
    f.handle.start_sync().await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnecting_without_network_is_offline_and_stop_and_close_finish_the_active_pass() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    f.storage.set_online(false);
    let mut requests = f.storage.subscribe_requests();
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    requests.wait_for(|n| *n > before).await.unwrap();
    let last = last_pass(&f.handle, |last| matches!(last, Some(LastPass::Offline { .. }))).await;
    let Some(LastPass::Offline { failure, .. }) = last else { unreachable!() };
    assert_eq!(failure, StorageFailure::Network);
    f.storage.set_online(true);
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
    for closing in [false, true] {
        let (listed, listing) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        // Held as it claims the slot for this write: the pass has moved,
        // so the state shows it syncing.
        f.storage.hold_next_creation(ObjectPrefix::all(f.directory.id()), listed, resumed).await;
        let id = format!("held while closing: {closing}");
        f.handle
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,'held')", [id])?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        listing.await.unwrap();
        let mut status = f.handle.test_sync_status();
        status.wait_for(|status| status.state == SyncState::Syncing).await;
        // Closing and stopping first write that sync isn't running; then
        // they wait for the pass.
        let mut effect = f.handle.test_retry_effect();
        if closing {
            let close = f.handle.close();
            tokio::pin!(close);
            tokio::select! {
                biased;
                result = &mut close => panic!("close ended during the pass: {result:?}"),
                _ = effect.wait_for(|effect| *effect == RetryEffect::NotSyncing) => {},
            }
            resume.send(()).unwrap();
            close.await.unwrap();
        } else {
            let stopping = crate::tests::stopping(&f.handle);
            effect.wait_for(|effect| *effect == RetryEffect::NotSyncing).await;
            assert_eq!(f.handle.sync_status().await.unwrap().state, SyncState::Syncing);
            resume.send(()).unwrap();
            stopping.await.unwrap().unwrap();
            f.connector.counted.released().await;
            f.advance(Duration::from_secs(1));
            f.handle.start_sync().await.unwrap();
            synced_since(&f.handle, f.clock.now()).await;
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
    ) -> Result<Arc<StorageConnection>, StorageError> {
        let storage = self
            .0
            .iter()
            .find(|storage| storage.config() == config)
            .ok_or(StorageFailure::InvalidConfiguration.with_source("unknown test location"))?;
        storage.connect(config, auth, device, publications, cooldown, transfers).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        self.0[0].detect_s3(config, credentials, store, runs, waits).await
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.0[0].check_new_store(config, auth, store, waits).await
    }
}

/// Reconnecting preserves both the failed attempt's ordinary delay and
/// the provider's independently persisted cooldown.
#[tokio::test]
async fn reconnecting_preserves_backoff_and_the_providers_cooldown() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.storage.set_online(false);
    f.handle.sync_now();
    last_pass(&f.handle, |last| matches!(last, Some(LastPass::Offline { .. }))).await;
    f.storage.set_online(true);
    // The failed connection backs off: no pass runs until its timer ends.
    let before = f.storage.request_count();
    f.handle.sync_now();
    f.handle.test_parked().await;
    assert_eq!(f.storage.request_count(), before);
    stop(&f).await;
    f.handle.start_sync().await.unwrap();
    f.handle.test_parked().await;
    assert_eq!(f.storage.request_count(), before);
    f.advance(Duration::from_secs(1));
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(600),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, |last| matches!(last, Some(LastPass::Failed { .. }))).await;
    let until = f.clock.now() + Duration::from_secs(600);
    stop(&f).await;
    let before = f.storage.request_count();
    f.handle.start_sync().await.unwrap();
    f.handle.test_parked().await;
    assert_eq!(
        f.storage.request_count(),
        before,
        "a reconnected pass ran inside the provider's cooldown"
    );
    // Sync is running, held by the cooldown, which the pass waits on: it
    // didn't get everything in or out, so it isn't synced (§19.1).
    f.handle
        .test_retry_effect()
        .wait_for(|effect| *effect == RetryEffect::CoolingDown { until })
        .await;
    assert_eq!(
        f.handle.test_sync_status().latest().await.state,
        SyncState::WaitingToSync(SyncWait::CoolingDown { until })
    );
    f.advance(Duration::from_secs(600));
    synced(&f.handle).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn forgetting_keys_finishes_the_active_pass_before_removing_custody() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), listed, resumed).await;
    f.handle.sync_now();
    listing.await.unwrap();
    let paused = f.handle.test_pause_asked();
    let forget = f.handle.forget_store_keys();
    tokio::pin!(forget);
    // Forgetting asks the loop to pause, which it takes only after its pass.
    tokio::select! {
        result = &mut forget => panic!("custody was removed during an active sync: {result:?}"),
        () = paused => {}
    }
    resume.send(()).unwrap();
    forget.await.unwrap();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    // The held pass ended synced, releasing recorded no failure, and sync
    // isn't running.
    assert_eq!(f.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    assert_eq!(f.handle.test_retry_effect().latest().await, RetryEffect::NotSyncing);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn repeated_setup_and_sync_requests_do_not_deadlock_credential_refresh() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    // Each round has its own generous limit: a deadlock never finishes, while
    // a slow filesystem (Windows CI) only takes longer.
    for round in 0..30 {
        tokio::time::timeout(HANG_GUARD, async {
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
async fn setup_refuses_another_location_after_lost_credentials_and_reopen() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let mut config = f.storage.config();
    let StorageConfig::S3 { bucket, .. } = &mut config else { unreachable!() };
    *bucket = "destination".into();
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
    f.handle.close().await.unwrap();
    f.app.lose_storage_credentials(f.directory.id()).unwrap();
    let handle = builder(
        &f.app,
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
    assert_eq!(coven_storage::recorded_config(&f.directory).unwrap(), Some(f.storage.config()));
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

impl Custody<StoreKeyring> for RefusePersist {
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
        matches!(f.setup().await.unwrap_err(), StorageSetupError::Check(StorageCheckError::ProviderCheck { check: StorageCheck::CreateOnce, source }) if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(matches!(f.handle.restore_code().await, Err(SyncError::NoStorage)));
    assert_eq!(f.handle.test_sync_status().latest().await.state, SyncState::NotSynced);
    assert!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap().is_empty());
    assert!(f.handle.members().await.unwrap().is_empty());
    assert_eq!(coven_storage::recorded_config(&f.directory).unwrap(), None);
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
    let mut f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let before = f.handle.restore_code().await.unwrap();
    let location = coven_storage::recorded_config(&f.directory).unwrap();
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
        matches!(error, StorageSetupError::Check(StorageCheckError::ProviderCheck { check: StorageCheck::CreateOnce, source }) if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_eq!(coven_storage::recorded_config(&f.directory).unwrap(), location);
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert_eq!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap(), objects);
    // The refused setup records no failure, and sync stays stopped.
    assert_eq!(f.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    assert_eq!(f.handle.test_sync_status().latest().await.state, SyncState::Synced);
    assert_eq!(f.handle.test_retry_effect().latest().await, RetryEffect::NotSyncing);
    f.storage.set_faults(Faults::none()).await;
    f.lose_credentials().await;
    f.storage.set_faults(Faults { overwrite_create: true, ..Faults::none() }).await;
    assert!(
        matches!(f.setup().await.unwrap_err(), StorageSetupError::Check(StorageCheckError::ProviderCheck { check: StorageCheck::CreateOnce, source }) if source.failure() == StorageFailure::Protocol)
    );
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_eq!(
        f.handle.test_sync_status().latest().await.state,
        SyncState::SignedOut { provider: CloudProvider::S3, cause: None }
    );
    assert_eq!(f.storage.list(&ObjectPrefix::all(f.directory.id())).await.unwrap(), objects);
    f.storage.set_faults(Faults::none()).await;
    f.setup().await.unwrap();
    f.handle.close().await.unwrap();
}

/// Ana's account holds Household on iCloud, and Household was shared with
/// her work account too. Setting up again signed in with it is refused:
/// no member would reach storage through the account that holds the store,
/// so nobody could share it or take sharing back. Setup commits nothing, and
/// her access still names the account that holds the store (§10.1, §20.9).
#[tokio::test]
async fn setup_with_an_account_other_than_the_one_holding_the_store_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(CloudProvider::CloudKit)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let StorageConfig::CloudKit { container, .. } = storage.config() else { unreachable!() };
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    let handle =
        builder(&app, clock.clone(), storage.clone()).open(directory.id(), |_| ()).await.unwrap();
    handle.setup_cloudkit_storage(&container, "Ana's Mac", |_| {}).await.unwrap();
    let owner = handle.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().access;
    let coven_storage::AccessGrant::Granted { invitation } =
        storage.grant_access("ana@work.example").await.unwrap()
    else {
        panic!("an account grant")
    };
    let work = Arc::new(MemoryStorage::for_recipient(&storage, "ana@work.example").unwrap());
    work.join(directory.id(), &invitation).await.unwrap();
    let code = handle.restore_code().await.unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app, clock, work).open(directory.id(), |_| ()).await.unwrap();
    let result = handle.setup_cloudkit_storage(&container, "Ana's Mac", |_| {}).await;
    assert!(matches!(result, Err(StorageSetupError::StoreOwner)), "{result:?}");
    assert_eq!(handle.restore_code().await.unwrap(), code);
    let members = handle.members().await.unwrap();
    assert_eq!(members.into_iter().find(|m| m.is_self).unwrap().access, owner);
    handle.close().await.unwrap();
}

#[path = "storage_credentials_tests.rs"]
mod credential_changes;

#[path = "storage_s3_tests.rs"]
mod s3_setup;

#[path = "storage_control_tests.rs"]
mod control;

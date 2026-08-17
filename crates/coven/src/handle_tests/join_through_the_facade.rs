//! A whole device join run through coven's own surface.
//!
//! The transport tests drive the join through the engine types a host never
//! names. These drive the same join the way a host that depends on the `coven`
//! crate alone must: the owner's side entirely through [`crate::CovenHandle`],
//! the joining side through the one scanned pairing offer, and nothing in the
//! flow reaching past those.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use coven_domain::joining::test_runtime::on_a_deep_stack;
use coven_replication::sync::test_helpers::*;

fn timing() -> crate::DeviceJoinTransportTiming {
    crate::DeviceJoinTransportTiming {
        poll: Duration::from_millis(2),
        deadline: Duration::from_secs(60),
    }
}

/// An owner whose store is reachable only through its handle, over a cloud home
/// a joining device can also be pointed at.
struct FacadeFixture {
    handle: crate::CovenHandle,
    home: Arc<dyn crate::ExactCloudHome>,
    layout: crate::StoreLayout,
    tables: Vec<crate::SyncedTable>,
    _store_tmp: tempfile::TempDir,
    _joiner_tmp: tempfile::TempDir,
    _snapshot_tmp: Option<tempfile::TempDir>,
}

impl FacadeFixture {
    async fn build(store_id: &str) -> Self {
        coven_keys::keys::test_keyring::install();
        let owner = coven_keys::keys::UserKeypair::generate();
        // The same key `TestStore::create` seals this store's objects with, so
        // the store key the sealed admission wraps opens the snapshot the joining device
        // bootstraps from.
        let encryption = crate::EncryptionService::from_key([42; 32]);
        let keyring = crate::MasterKeyring::from(encryption.clone());
        let store_tmp = tempfile::tempdir().expect("store directory");
        let store_dir = crate::StoreDir::new_ephemeral(store_tmp.path());
        let tables = test_synced_tables();

        let handle = crate::Coven::builder(
            store_dir.clone(),
            crate::Config::with_defaults(
                store_id.to_string(),
                "owner-device".to_string(),
                "Facade Join Store".to_string(),
            ),
        )
        .synced_tables(tables.clone())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(keyring))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the owner's store");

        // The store's protocol root, founder registration, and the snapshot a
        // joining device bootstraps from are all fixture setup — the flow under
        // test starts once they exist.
        let home = test_cloud_home();
        let store = handle
            .create_test_store(store_id, owner.clone(), home.clone())
            .await
            .expect("create the owner's Store");
        let snapshot_tmp = tempfile::tempdir().expect("snapshot directory");
        let snapshot_path = snapshot_tmp.path().to_path_buf();
        handle
            .prepare_test_join_snapshot(&store, &owner, snapshot_path)
            .await
            .expect("create the join snapshot");

        handle
            .connect_sync_with_test_home(
                home.clone(),
                coven_storage::CloudCipher::Encrypted(encryption),
            )
            .await
            .expect("connect the owner's store to its home");

        let joiner_tmp = tempfile::tempdir().expect("joining device directory");
        Self {
            handle,
            home,
            layout: crate::StoreLayout::new(joiner_tmp.path()),
            tables,
            _store_tmp: store_tmp,
            _joiner_tmp: joiner_tmp,
            _snapshot_tmp: Some(snapshot_tmp),
        }
    }
}

async fn wait_for_initial_sync(handle: &crate::CovenHandle) {
    let mut sync = handle.subscribe_sync_status();
    handle.sync_now();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let current = sync.borrow_and_update().clone();
            match current {
                crate::SyncLoopStatus::Synchronized(_) => break,
                crate::SyncLoopStatus::Failed { error } => {
                    panic!("initial live S3 sync failed: {error}")
                }
                _ => sync.changed().await.expect("sync status remains open"),
            }
        }
    })
    .await
    .expect("initial live S3 sync publishes the joining snapshot");
}

#[derive(Clone)]
struct PairingTimeline {
    started: Instant,
    events: Arc<Mutex<Vec<PairingTimelineEvent>>>,
}

struct PairingTimelineEvent {
    elapsed: Duration,
    device: &'static str,
    progress: String,
}

impl PairingTimeline {
    fn start() -> Self {
        Self {
            started: Instant::now(),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record(&self, device: &'static str, progress: impl std::fmt::Debug) {
        let elapsed = self.started.elapsed();
        let progress = format!("{progress:?}");
        eprintln!("pairing +{elapsed:.3?} {device}: {progress}");
        self.events
            .lock()
            .expect("lock pairing timeline")
            .push(PairingTimelineEvent {
                elapsed,
                device,
                progress,
            });
    }

    fn recorded_at(&self, device: &'static str, progress: &str) -> Duration {
        self.events
            .lock()
            .expect("lock pairing timeline")
            .iter()
            .find(|event| event.device == device && event.progress.starts_with(progress))
            .unwrap_or_else(|| panic!("{device} never reported {progress}"))
            .elapsed
    }
}

fn live_timing() -> crate::DeviceJoinTransportTiming {
    crate::DeviceJoinTransportTiming {
        poll: Duration::from_secs(1),
        deadline: Duration::from_secs(180),
    }
}

/// Two independent device stores complete one-scan pairing over the configured
/// S3-compatible provider. Run with `--ignored --nocapture`; every user-visible
/// transition is timestamped so a provider regression cannot hide behind the
/// in-memory transport's zero-latency reads.
#[test]
#[ignore]
fn two_devices_pair_end_to_end_over_live_s3_within_the_product_bound() {
    on_a_deep_stack(run_two_devices_pair_end_to_end_over_live_s3_within_the_product_bound);
}

async fn run_two_devices_pair_end_to_end_over_live_s3_within_the_product_bound() {
    coven_keys::keys::test_keyring::install();
    let factory =
        coven_storage::cloud::CloudHomeFactory::new(coven_storage::oauth::OAuthClients::empty());
    let live = crate::test_support::RealS3TestHome::open(
        &factory,
        "two-device-pairing",
        crate::HomeStorage::Opaque,
    )
    .await;
    live.reset().await;
    let store_id = "live-s3-two-device-pairing";
    let store_tmp = tempfile::tempdir().expect("store directory");
    let store_dir = crate::StoreDir::new_ephemeral(store_tmp.path());
    let tables = test_synced_tables();
    let mut config = crate::Config::with_defaults(
        store_id.to_string(),
        "owner-device".to_string(),
        "Facade Join Store".to_string(),
    );
    config.cloud_home = live.config();
    let handle = crate::Coven::builder(store_dir, config)
        .synced_tables(tables.clone())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::generate()))
        .identity_custody(crate::IdentityCustody::InMemory(
            crate::UserKeypair::generate(),
        ))
        .open()
        .expect("open the owner's store");
    handle
        .setup_cloud_home_with_test_home(live.config(), live.home(), Some(live.credentials()))
        .await
        .expect("create the owner's Store on live S3");
    wait_for_initial_sync(&handle).await;
    let joiner_tmp = tempfile::tempdir().expect("joining device directory");
    let fixture = FacadeFixture {
        handle,
        home: live.home(),
        layout: crate::StoreLayout::new(joiner_tmp.path()),
        tables,
        _store_tmp: store_tmp,
        _joiner_tmp: joiner_tmp,
        _snapshot_tmp: None,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pairing listener");
    let endpoint = listener.local_addr().expect("pairing endpoint");
    let pairing_key = crate::UserKeypair::generate();
    let offer = crate::DevicePairingOffer::new(
        &pairing_key,
        vec![endpoint],
        "Facade Join Store".to_string(),
        crate::CloudProvider::S3,
        1_900_000_000,
    )
    .expect("pairing offer");
    let pairing_journal = tempfile::tempdir().expect("pairing journal directory");
    let host = crate::DevicePairingHost::start(
        listener,
        offer.clone(),
        pairing_key,
        pairing_journal.path().join("pairing.json"),
        Arc::new(crate::SystemClock),
    )
    .await
    .expect("start pairing host");
    let pairing =
        crate::PreparedDevicePairing::open_or_create(&offer.encode(), None, &fixture.layout)
            .expect("prepare joining identity from the scanned code");
    let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);
    let timeline = PairingTimeline::start();
    let joiner_timeline = timeline.clone();
    let joining = crate::join_with_device_pairing(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        live.config().exact_upload_verification,
        crate::KeyCustody::Keyring,
        crate::IdentityCustody::Keyring,
        crate::OAuthClients::empty(),
        None,
        None,
        Arc::new(crate::SystemClock),
        live_timing(),
        Arc::new(move |progress| joiner_timeline.record("joining device", progress)),
        &cancel,
    );
    let owner_timeline = timeline.clone();
    let owner_progress = move |progress| owner_timeline.record("existing device", progress);
    let admitting = async {
        let request = host
            .wait_for_request()
            .await
            .expect("receive signed request");
        fixture
            .handle
            .approve_device_pairing(
                &host,
                &request,
                crate::MemberRole::Member,
                crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                None,
                &owner_progress,
                live_timing(),
                tokio::sync::watch::channel(false).1,
            )
            .await
    };
    let (joined, admitted) = tokio::join!(Box::pin(joining), Box::pin(admitting));
    let elapsed = timeline.started.elapsed();
    let joined = joined.expect("joining device completes over live S3");
    let admitted = admitted.expect("existing device completes over live S3");

    assert!(matches!(
        joined,
        crate::DeviceJoinTransportOutcome::Joined(_)
    ));
    assert!(matches!(
        admitted,
        crate::DeviceJoinDriveOutcome::Activated(_)
    ));
    let snapshot_download = timeline.recorded_at("joining device", "DownloadingSnapshot");
    let snapshot_install = timeline.recorded_at("joining device", "InstallingSnapshot");
    let waiting_for_joiner = timeline.recorded_at("existing device", "WaitingForJoiningDevice");
    assert!(snapshot_download <= snapshot_install);
    assert!(waiting_for_joiner <= elapsed);
    assert!(
        elapsed <= Duration::from_secs(30),
        "live S3 device pairing took {elapsed:.3?}, beyond the 30-second product bound"
    );
    assert!(fixture
        .layout
        .store_dir("live-s3-two-device-pairing")
        .config_path()
        .exists());
    fixture.handle.stop_sync();
    live.reset().await;
}

/// The existing device displays one code; the joining device scans it, submits
/// its signed identity over that session, and receives its sealed invitation
/// without a second scan or copied response code.
#[test]
fn a_facade_only_host_runs_a_whole_join_from_one_scanned_pairing_code() {
    on_a_deep_stack(run_a_facade_only_host_runs_a_whole_join_from_one_scanned_pairing_code);
}

async fn run_a_facade_only_host_runs_a_whole_join_from_one_scanned_pairing_code() {
    let fixture = FacadeFixture::build("facade-one-scan-pairing").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pairing listener");
    let endpoint = listener.local_addr().expect("pairing endpoint");
    let pairing_key = crate::UserKeypair::generate();
    let offer = crate::DevicePairingOffer::new(
        &pairing_key,
        vec![endpoint],
        "Facade Join Store".to_string(),
        crate::CloudProvider::S3,
        1_900_000_000,
    )
    .expect("pairing offer");
    let pairing_journal = tempfile::tempdir().expect("pairing journal directory");
    let host = crate::DevicePairingHost::start(
        listener,
        offer.clone(),
        pairing_key,
        pairing_journal.path().join("pairing.json"),
        Arc::new(crate::SystemClock),
    )
    .await
    .expect("start pairing host");
    let pairing =
        crate::PreparedDevicePairing::open_or_create(&offer.encode(), None, &fixture.layout)
            .expect("prepare joining identity from the scanned code");
    let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);

    let joining = coven_domain::joining::join_with_device_pairing_over_test_home(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        timing(),
        Arc::new(|_| {}),
        &cancel,
    );
    let admitting = async {
        let request = host
            .wait_for_request()
            .await
            .expect("receive signed request");
        fixture
            .handle
            .approve_device_pairing(
                &host,
                &request,
                crate::MemberRole::Member,
                crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                None,
                &|_| {},
                timing(),
                tokio::sync::watch::channel(false).1,
            )
            .await
    };
    let (joined, admitted) = tokio::join!(Box::pin(joining), Box::pin(admitting));
    let joined = joined.expect("joining device completes from one scan");
    let admitted = admitted.expect("existing device completes pairing");

    assert!(matches!(
        joined,
        crate::DeviceJoinTransportOutcome::Joined(_)
    ));
    assert!(matches!(
        admitted,
        crate::DeviceJoinDriveOutcome::Activated(_)
    ));
    assert!(fixture
        .layout
        .store_dir("facade-one-scan-pairing")
        .config_path()
        .exists());
}

/// Cancelling approval after the invitation exists unwinds the Store attempt,
/// tells the joining device to discard its partial Store, and closes the local
/// pairing session without requiring either process to be killed.
#[test]
fn owner_cancellation_reaches_a_joiner_through_the_facade() {
    on_a_deep_stack(run_owner_cancellation_reaches_a_joiner_through_the_facade);
}

async fn run_owner_cancellation_reaches_a_joiner_through_the_facade() {
    let fixture = FacadeFixture::build("facade-cancelled-pairing").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pairing listener");
    let endpoint = listener.local_addr().expect("pairing endpoint");
    let pairing_key = crate::UserKeypair::generate();
    let offer = crate::DevicePairingOffer::new(
        &pairing_key,
        vec![endpoint],
        "Facade Join Store".to_string(),
        crate::CloudProvider::S3,
        1_900_000_000,
    )
    .expect("pairing offer");
    let pairing_journal = tempfile::tempdir().expect("pairing journal directory");
    let host = crate::DevicePairingHost::start(
        listener,
        offer.clone(),
        pairing_key,
        pairing_journal.path().join("pairing.json"),
        Arc::new(crate::SystemClock),
    )
    .await
    .expect("start pairing host");
    let pairing =
        crate::PreparedDevicePairing::open_or_create(&offer.encode(), None, &fixture.layout)
            .expect("prepare joining identity from the scanned code");
    let (_join_cancel_tx, join_cancel) = tokio::sync::watch::channel(false);
    let (_approval_cancel_tx, approval_cancel) = tokio::sync::watch::channel(true);

    let joining = coven_domain::joining::join_with_device_pairing_over_test_home(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        timing(),
        Arc::new(|_| {}),
        &join_cancel,
    );
    let admitting = async {
        let request = host
            .wait_for_request()
            .await
            .expect("receive signed request");
        fixture
            .handle
            .approve_device_pairing(
                &host,
                &request,
                crate::MemberRole::Member,
                crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                None,
                &|_| {},
                timing(),
                approval_cancel,
            )
            .await
    };
    let (joined, admitted) = tokio::join!(Box::pin(joining), Box::pin(admitting));

    assert!(matches!(
        admitted,
        Err(crate::ApproveDevicePairingError::Cancelled)
    ));
    assert!(matches!(
        joined.expect("joining device completes cancellation"),
        crate::DeviceJoinTransportOutcome::Abandoned(_)
            | crate::DeviceJoinTransportOutcome::Cancelled(_)
    ));
    assert!(!fixture
        .layout
        .store_dir("facade-cancelled-pairing")
        .config_path()
        .exists());
}

/// A persisted invitation remains cancellable even when the approval future
/// that created it no longer exists. The facade owns the durable Store attempt,
/// so cancellation cannot depend on an in-memory approval task surviving.
#[test]
fn facade_cancellation_unwinds_a_persisted_invitation_without_an_approval_future() {
    on_a_deep_stack(
        run_facade_cancellation_unwinds_a_persisted_invitation_without_an_approval_future,
    );
}

async fn run_facade_cancellation_unwinds_a_persisted_invitation_without_an_approval_future() {
    let fixture = FacadeFixture::build("facade-persisted-cancellation").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pairing listener");
    let endpoint = listener.local_addr().expect("pairing endpoint");
    let pairing_key = crate::UserKeypair::generate();
    let offer = crate::DevicePairingOffer::new(
        &pairing_key,
        vec![endpoint],
        "Facade Join Store".to_string(),
        crate::CloudProvider::S3,
        1_900_000_000,
    )
    .expect("pairing offer");
    let pairing_journal = tempfile::tempdir().expect("pairing journal directory");
    let host = crate::DevicePairingHost::start(
        listener,
        offer.clone(),
        pairing_key,
        pairing_journal.path().join("pairing.json"),
        Arc::new(crate::SystemClock),
    )
    .await
    .expect("start pairing host");
    let pairing =
        crate::PreparedDevicePairing::open_or_create(&offer.encode(), None, &fixture.layout)
            .expect("prepare joining identity from the scanned code");
    let (_join_cancel_tx, join_cancel) = tokio::sync::watch::channel(false);
    let joining = coven_domain::joining::join_with_device_pairing_over_test_home(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        timing(),
        Arc::new(|_| {}),
        &join_cancel,
    );
    tokio::pin!(joining);
    let request = tokio::select! {
        request = host.wait_for_request() => request.expect("receive signed request"),
        outcome = &mut joining => panic!("joining finished before approval: {outcome:?}"),
    };
    {
        let approving = fixture.handle.approve_device_pairing(
            &host,
            &request,
            crate::MemberRole::Member,
            crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
            None,
            &|_| {},
            timing(),
            tokio::sync::watch::channel(false).1,
        );
        tokio::pin!(approving);
        loop {
            tokio::select! {
                outcome = &mut approving => panic!("approval finished before cancellation: {outcome:?}"),
                outcome = &mut joining => panic!("joining finished before cancellation: {outcome:?}"),
                () = tokio::time::sleep(Duration::from_millis(2)) => {
                    if host.invitation(&request).expect("read pairing journal").is_some() {
                        break;
                    }
                }
            }
        }
    }

    fixture
        .handle
        .cancel_device_pairing(&host, timing())
        .await
        .expect("cancel persisted invitation");

    assert!(matches!(
        joining.await.expect("joining device receives cancellation"),
        crate::DeviceJoinTransportOutcome::Abandoned(_)
            | crate::DeviceJoinTransportOutcome::Cancelled(_)
    ));
    assert!(!fixture
        .layout
        .store_dir("facade-persisted-cancellation")
        .config_path()
        .exists());
}

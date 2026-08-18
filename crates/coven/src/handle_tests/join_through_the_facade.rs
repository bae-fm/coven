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
    memory_home: Option<Arc<crate::InMemoryCloudHome>>,
    layout: crate::StoreLayout,
    tables: Vec<crate::SyncedTable>,
    encryption: crate::EncryptionService,
    _store_tmp: tempfile::TempDir,
    _joiner_tmp: tempfile::TempDir,
    _snapshot_tmp: Option<tempfile::TempDir>,
}

impl FacadeFixture {
    async fn build(store_id: &str) -> Self {
        Self::build_with_eager_images(store_id, 0).await
    }

    async fn build_with_eager_images(store_id: &str, image_count: usize) -> Self {
        coven_keys::keys::test_keyring::install();
        let owner = coven_keys::keys::UserKeypair::generate();
        // The same key `TestStore::create` seals this store's objects with, so
        // the store key the sealed admission wraps opens the snapshot the joining device
        // bootstraps from.
        let encryption = crate::EncryptionService::from_key([42; 32]);
        let keyring = crate::MasterKeyring::from(encryption.clone());
        let store_tmp = tempfile::tempdir().expect("store directory");
        let store_dir = crate::StoreDir::new_ephemeral(store_tmp.path());
        let tables = if image_count == 0 {
            test_synced_tables()
        } else {
            test_synced_tables_with_blob(photo_decl())
        };

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
        let snapshot_tmp = if image_count > 0 {
            handle
                .connect_sync_with_test_home(
                    home.clone(),
                    coven_storage::CloudCipher::Encrypted(encryption.clone()),
                )
                .await
                .expect("connect fixture storage for eager image publication");
            seed_eager_library_images(&handle, image_count).await;
            handle
                .make_remote("notes", "image-root", false)
                .await
                .expect("queue fixture eager images for cloud storage");
            wait_for_initial_sync(&handle).await;
            None
        } else {
            let snapshot_tmp = tempfile::tempdir().expect("snapshot directory");
            let snapshot_path = snapshot_tmp.path().to_path_buf();
            handle
                .prepare_test_join_snapshot(&store, &owner, snapshot_path)
                .await
                .expect("create the join snapshot");
            handle
                .connect_sync_with_test_home(
                    home.clone(),
                    coven_storage::CloudCipher::Encrypted(encryption.clone()),
                )
                .await
                .expect("connect the owner's store to its home");
            Some(snapshot_tmp)
        };

        let joiner_tmp = tempfile::tempdir().expect("joining device directory");
        Self {
            handle,
            home: home.clone(),
            memory_home: Some(home),
            layout: crate::StoreLayout::new(joiner_tmp.path()),
            tables,
            encryption,
            _store_tmp: store_tmp,
            _joiner_tmp: joiner_tmp,
            _snapshot_tmp: snapshot_tmp,
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

async fn seed_eager_library_images(handle: &crate::CovenHandle, image_count: usize) {
    const IMAGE_BYTES: usize = 64 * 1024;

    if image_count == 0 {
        return;
    }

    let images = (0..image_count)
        .map(|index| {
            let id = format!("image{index:04}");
            let bytes = vec![u8::try_from(index).expect("image index fits u8"); IMAGE_BYTES];
            let hash = coven_protocol::blob::content_hash(&bytes);
            (id, bytes, hash)
        })
        .collect::<Vec<_>>();
    let blobs = images
        .iter()
        .map(|(id, bytes, _)| (id.clone(), bytes.clone()))
        .collect::<Vec<_>>();

    handle
        .write_with_blobs(
            move |batch| {
                for (id, bytes) in blobs {
                    batch.put_blob("photos", id, bytes);
                }
                Ok(())
            },
            move |sql| {
                let stamp = sql.stamp().to_string();
                sql.execute(
                    "INSERT INTO notes
                     (id, title, shared, _updated_at, created_at)
                     VALUES (?1, 'Library images', 0, ?2, '2026-01-01')",
                    rusqlite::params!["image-root", stamp],
                )?;
                for (id, bytes, hash) in images {
                    sql.execute(
                        "INSERT INTO note_photos
                         (id, note_id, kind, size, hash, _updated_at, created_at)
                         VALUES (?1, 'image-root', 'cover', ?2, ?3, ?4, '2026-01-01')",
                        rusqlite::params![id, bytes.len() as i64, hash, stamp],
                    )?;
                }
                Ok(())
            },
        )
        .await
        .expect("seed eager library images");
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

    fn recorded_times(&self, device: &'static str, progress: &str) -> Vec<Duration> {
        self.events
            .lock()
            .expect("lock pairing timeline")
            .iter()
            .filter(|event| event.device == device && event.progress.starts_with(progress))
            .map(|event| event.elapsed)
            .collect()
    }

    fn assert_progress_cadence(&self, device: &'static str, progress: &str) {
        let reports = self.recorded_times(device, progress);
        assert!(
            reports.len() >= 2,
            "{progress} must report its initial and final totals"
        );
        for pair in reports.windows(2).take(reports.len() - 2) {
            assert!(
                pair[1].saturating_sub(pair[0]) >= Duration::from_millis(250),
                "non-terminal {progress} arrived faster than the 300 ms cadence: {reports:?}",
            );
        }
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
    let tables = test_synced_tables_with_blob(photo_decl());
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
    seed_eager_library_images(&handle, 24).await;
    handle
        .setup_cloud_home_with_test_home(live.config(), live.home(), Some(live.credentials()))
        .await
        .expect("create the owner's Store on live S3");
    handle
        .make_remote("notes", "image-root", false)
        .await
        .expect("queue live eager images for cloud storage");
    wait_for_initial_sync(&handle).await;
    let joiner_tmp = tempfile::tempdir().expect("joining device directory");
    let fixture = FacadeFixture {
        handle,
        home: live.home(),
        memory_home: None,
        layout: crate::StoreLayout::new(joiner_tmp.path()),
        tables,
        encryption: crate::EncryptionService::from_key([42; 32]),
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
        crate::TransferLimits {
            uploads: std::num::NonZeroUsize::new(3).expect("transfer limit is nonzero"),
            downloads: std::num::NonZeroUsize::new(3).expect("transfer limit is nonzero"),
        },
        crate::KeyCustody::Keyring,
        crate::IdentityCustody::Keyring,
        crate::OAuthClients::empty(),
        None,
        None,
        Arc::new(crate::SystemClock),
        Arc::new(move |progress| joiner_timeline.record("joining device", progress)),
        &cancel,
    );
    let owner_timeline = timeline.clone();
    let owner_progress = move |progress| owner_timeline.record("existing device", progress);
    let admission_started = timeline.started;
    let admitting = async {
        let request = host
            .wait_for_request()
            .await
            .expect("receive signed request");
        let outcome = fixture
            .handle
            .approve_device_pairing(
                &host,
                &request,
                crate::MemberRole::Member,
                crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                None,
                &owner_progress,
                tokio::sync::watch::channel(false).1,
            )
            .await
            .expect("existing device completes over live S3");
        (outcome, admission_started.elapsed())
    };
    let (joined, (admitted, admission_elapsed)) =
        tokio::join!(Box::pin(joining), Box::pin(admitting));
    let elapsed = timeline.started.elapsed();
    let joined = joined.expect("joining device completes over live S3");
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
    let registration = timeline.recorded_at("existing device", "RegisteringDevice");
    timeline.assert_progress_cadence("joining device", "DownloadingSnapshot");
    assert!(snapshot_download <= snapshot_install);
    assert!(registration <= admission_elapsed);
    assert!(
        admission_elapsed <= Duration::from_secs(10),
        "live S3 device enrollment took {admission_elapsed:.3?}, beyond the ten-second product bound"
    );
    assert!(
        elapsed <= Duration::from_secs(10),
        "live S3 device pairing took {elapsed:.3?}, beyond the ten-second product bound"
    );
    assert!(fixture
        .layout
        .store_dir("live-s3-two-device-pairing")
        .config_path()
        .exists());
    fixture.handle.stop_sync();
    live.reset().await;
}

/// Enrollment survives a snapshot transfer failure as one explicit pending
/// installation. A relaunched app reconstructs that state from coven's journal
/// and retries the same invitation without another pairing scan.
#[test]
fn a_failed_snapshot_installation_restarts_from_the_durable_pairing() {
    on_a_deep_stack(run_a_failed_snapshot_installation_restarts_from_the_durable_pairing);
}

async fn run_a_failed_snapshot_installation_restarts_from_the_durable_pairing() {
    let fixture = FacadeFixture::build("facade-snapshot-restart").await;
    let memory_home = fixture
        .memory_home
        .as_ref()
        .expect("in-memory facade fixture")
        .clone();
    memory_home.fail_next_exact_stream_reads(1);
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
    let attempt_timing = crate::DeviceJoinTransportTiming {
        poll: Duration::from_millis(2),
        deadline: Duration::from_secs(2),
    };

    let first_join = coven_domain::joining::join_with_device_pairing_over_test_home(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        attempt_timing,
        Arc::new(|_| {}),
        &cancel,
    );
    tokio::pin!(first_join);
    let request = tokio::select! {
        request = host.wait_for_request() => request.expect("receive signed request"),
        outcome = &mut first_join => panic!("joining finished before approval: {outcome:?}"),
    };
    let first_approval = fixture.handle.approve_device_pairing(
        &host,
        &request,
        crate::MemberRole::Member,
        crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
        None,
        &|_| {},
        tokio::sync::watch::channel(false).1,
    );
    let (first_join, first_approval) = tokio::join!(Box::pin(first_join), Box::pin(first_approval));
    assert!(matches!(
        first_join,
        Err(crate::BootstrapError::Snapshot(_))
    ));
    assert!(matches!(
        first_approval.expect("owner records the durable activation"),
        crate::DeviceJoinDriveOutcome::Activated(_)
    ));
    assert!(!fixture
        .layout
        .store_dir("facade-snapshot-restart")
        .config_path()
        .exists());

    let mut pending = crate::PreparedDevicePairing::pending(&fixture.layout)
        .expect("reload pending enrollments after restart");
    assert_eq!(pending.len(), 1);
    let resumed_pairing = pending.pop().expect("one pending enrollment");
    let resumed_join = coven_domain::joining::join_with_device_pairing_over_test_home(
        &resumed_pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        timing(),
        Arc::new(|_| {}),
        &cancel,
    );
    let resumed_approval = fixture.handle.approve_device_pairing(
        &host,
        &request,
        crate::MemberRole::Member,
        crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
        None,
        &|_| {},
        tokio::sync::watch::channel(false).1,
    );
    let (resumed_join, resumed_approval) =
        tokio::join!(Box::pin(resumed_join), Box::pin(resumed_approval));

    assert!(matches!(
        resumed_join.expect("resume snapshot installation"),
        crate::DeviceJoinTransportOutcome::Joined(_)
    ));
    assert!(matches!(
        resumed_approval.expect("resume device approval"),
        crate::DeviceJoinDriveOutcome::Activated(_)
    ));
    assert!(crate::PreparedDevicePairing::pending(&fixture.layout)
        .expect("enumerate completed enrollments")
        .is_empty());
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

mod post_open_eager_cache;

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
        .cancel_device_pairing(&host)
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

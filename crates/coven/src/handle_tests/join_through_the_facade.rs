//! A whole device join run through coven's own surface.
//!
//! The transport tests drive the join through the engine types a host never
//! names. These drive the same join the way a host that depends on the `coven`
//! crate alone must: the owner's side entirely through [`crate::CovenHandle`],
//! the joining side through the one scanned pairing offer, and nothing in the
//! flow reaching past those.

use std::sync::Arc;
use std::time::Duration;

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
    home: Arc<crate::InMemoryCloudHome>,
    layout: crate::StoreLayout,
    tables: Vec<crate::SyncedTable>,
    _store_tmp: tempfile::TempDir,
    _joiner_tmp: tempfile::TempDir,
    _snapshot_tmp: tempfile::TempDir,
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
            _snapshot_tmp: snapshot_tmp,
        }
    }
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
        |_status| {},
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
                timing(),
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

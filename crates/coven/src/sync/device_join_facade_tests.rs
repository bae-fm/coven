//! A whole device join run through coven's own surface.
//!
//! The transport tests drive the join through the engine types a host never
//! names. These drive the same join the way a host that depends on the `coven`
//! crate alone must: the owner's side entirely through [`crate::CovenHandle`],
//! the joining side through the one scanned-invite call, and nothing in the
//! flow reaching past those.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::sync::test_helpers::*;

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
        crate::keys::test_keyring::install();
        let owner = crate::keys::UserKeypair::generate();
        // The same key `TestStore::create` seals this store's objects with, so
        // the store key the invite wraps opens the snapshot the joining device
        // bootstraps from.
        let encryption = crate::EncryptionService::from_key([42; 32]);
        let keyring = crate::MasterKeyring::from(encryption.clone());
        let store_tmp = tempfile::tempdir().expect("store directory");
        let store_dir = crate::StoreDir::new(store_tmp.path());
        let tables = test_synced_tables();

        let handle = crate::Coven::builder(crate::Config::with_defaults(
            store_id.to_string(),
            "owner-device".to_string(),
            store_dir.clone(),
            "Facade Join Store".to_string(),
        ))
        .synced_tables(tables.clone())
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(keyring))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the owner's store");

        // The store's protocol root, founder registration, and the snapshot a
        // joining device bootstraps from are all fixture setup — the flow under
        // test starts once they exist.
        let store = TestStore::create(handle.db(), store_id, owner.clone())
            .await
            .expect("create the owner's Store");
        let membership = store
            .open_into(handle.db())
            .await
            .expect("load the owner's membership");
        let snapshot_tmp = tempfile::tempdir().expect("snapshot directory");
        let snapshot_path = snapshot_tmp.path().to_path_buf();
        let snapshot_tables = tables.clone();
        let snapshot = handle
            .db()
            .call(move |connection| {
                crate::sync::store::create_snapshot(
                    connection,
                    &snapshot_path,
                    &snapshot_tables,
                    None,
                )
                .map_err(|error| crate::DbError::Message(error.to_string()))
            })
            .await
            .expect("create the join snapshot");
        let coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
        publish_snapshot_fixture(
            &store.storage,
            &store.root,
            snapshot,
            coverage.clone(),
            &owner,
            &membership,
            handle.db(),
        )
        .await
        .expect("publish the join snapshot");
        publish_store_ack_fixture(handle.db(), &store.storage, coverage, &owner)
            .await
            .expect("publish the snapshot acknowledgement");

        handle
            .connect_sync_with_test_home(
                store.home.clone(),
                crate::CloudCipher::Encrypted(encryption),
            )
            .await
            .expect("connect the owner's store to its home");

        let joiner_tmp = tempfile::tempdir().expect("joining device directory");
        Self {
            handle,
            home: store.home.clone(),
            layout: crate::StoreLayout::new(joiner_tmp.path()),
            tables,
            _store_tmp: store_tmp,
            _joiner_tmp: joiner_tmp,
            _snapshot_tmp: snapshot_tmp,
        }
    }
}

/// A host holding nothing but `coven` can admit a device end to end: the owner
/// mints one payload from the joining device's request, and the two sides run
/// to a saved member config without either naming an engine type.
#[test]
fn a_facade_only_host_runs_a_whole_device_join() {
    // These flows compose more `poll` frames than a test thread's stack holds
    // in an unoptimized build, and the join is not `Send`, so it carries its
    // own runtime on a deeper thread.
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build the test runtime")
                .block_on(run_a_facade_only_host_runs_a_whole_device_join());
        })
        .expect("spawn the test thread")
        .join()
        // Carry the body's own panic across the thread boundary rather than
        // reporting an opaque join failure in its place.
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}

async fn run_a_facade_only_host_runs_a_whole_device_join() {
    let fixture = FacadeFixture::build("facade-device-join").await;

    // The joining device generates its request first: the offer is signed for
    // that device's key, so the owner cannot mint the payload without it.
    let join_request = crate::generate_join_request(None).expect("generate the join request");

    let invite = fixture
        .handle
        .begin_device_invite(&join_request, crate::MemberRole::Member)
        .await
        .expect("mint the scannable invite");
    let scanned = invite.to_bytes();
    assert!(
        !invite.invite_code.is_empty(),
        "the payload carries the invite code the joining device needs for its provider credentials",
    );

    let cancel = tokio::sync::watch::channel(false).1;
    let (joined, drove) = tokio::join!(
        Box::pin(crate::join_with_scanned_invite_over_test_home(
            &scanned,
            &join_request,
            fixture.layout.clone(),
            fixture.tables.clone(),
            test_migrations(),
            Arc::new(crate::SystemClock),
            fixture.home.clone(),
            timing(),
            |_status| {},
            &cancel,
        )),
        Box::pin(fixture.handle.drive_device_join(
            &invite,
            crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
            None,
            timing(),
        )),
    );

    let config = match joined.expect("the joining device completes") {
        crate::DeviceJoinTransportOutcome::Joined(config) => config,
        crate::DeviceJoinTransportOutcome::Abandoned(_) => {
            panic!("the attempt was abandoned, not completed")
        }
    };
    let activation = match drove.expect("the owner's handle drives the admission") {
        crate::DeviceJoinDriveOutcome::Activated(activation) => activation,
        crate::DeviceJoinDriveOutcome::Abandoned(_) => {
            panic!("the attempt was abandoned, not activated")
        }
    };

    assert!(config.store_dir.config_path().exists());
    assert_eq!(config.store_id, "facade-device-join");
    assert_eq!(
        activation.outcome.attempt().attempt_id,
        invite.bundle.offer.attempt_id,
    );

    // The payload survives the round trip a QR makes it take.
    let decoded = crate::DeviceJoinInvite::from_bytes(&scanned).expect("decode the scanned invite");
    assert_eq!(decoded.invite_code, invite.invite_code);
    assert!(crate::DeviceJoinInvite::from_bytes(b"{}").is_err());
}

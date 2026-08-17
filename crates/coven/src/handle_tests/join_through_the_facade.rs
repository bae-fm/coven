//! A whole device join run through coven's own surface.
//!
//! The transport tests drive the join through the engine types a host never
//! names. These drive the same join the way a host that depends on the `coven`
//! crate alone must: the owner's side entirely through [`crate::CovenHandle`],
//! the joining side through the one scanned-invite call, and nothing in the
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
        // the store key the invite wraps opens the snapshot the joining device
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

    /// The joining device's side, over this fixture's home.
    async fn join_over_test_home(
        &self,
        scanned: &[u8],
        join_request: &str,
        timing: crate::DeviceJoinTransportTiming,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<crate::DeviceJoinTransportOutcome, crate::BootstrapError> {
        coven_domain::joining::join_with_scanned_invite_over_test_home(
            scanned,
            join_request,
            self.layout.clone(),
            self.tables.clone(),
            test_migrations(),
            Arc::new(crate::SystemClock),
            self.home.clone(),
            timing,
            |_status| {},
            cancel,
        )
        .await
    }

    async fn close_over_test_home(
        &self,
        scanned: &[u8],
        join_request: &str,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<(), crate::BootstrapError> {
        coven_domain::joining::close_scanned_invite_join_over_test_home(
            scanned,
            join_request,
            self.layout.clone(),
            self.tables.clone(),
            test_migrations(),
            Arc::new(crate::SystemClock),
            self.home.clone(),
            timing,
        )
        .await
    }

    /// Whether the attempt's slot for `kind` holds nothing.
    async fn slot_is_empty(
        &self,
        invite: &crate::DeviceJoinInvite,
        kind: crate::DeviceJoinTransportKind,
    ) -> bool {
        let slot = invite
            .bundle
            .transport
            .slots
            .get(&kind)
            .expect("every kind has a slot");
        crate::ExactSlotStorage::read_at(self.home.as_ref(), slot)
            .await
            .is_err()
    }
}

/// A deadline short enough that a side with no counterpart running gives up
/// promptly. It bounds only the wait for an artifact, never the work between.
fn one_shot() -> crate::DeviceJoinTransportTiming {
    crate::DeviceJoinTransportTiming {
        poll: Duration::from_millis(2),
        deadline: Duration::from_millis(300),
    }
}

/// A host can cancel an invited join through its handle, and the joining device
/// closes its own side through the same facade — leaving the attempt's slots
/// gone and nothing pending on either side.
#[test]
fn a_facade_only_host_cancels_an_invited_join() {
    on_a_deep_stack(run_a_facade_only_host_cancels_an_invited_join);
}

async fn run_a_facade_only_host_cancels_an_invited_join() {
    let fixture = FacadeFixture::build("facade-device-cancel").await;
    let join_request = crate::generate_join_request(None).expect("generate the join request");
    let invite = fixture
        .handle
        .begin_device_invite(&join_request, crate::MemberRole::Member)
        .await
        .expect("mint the scannable invite");
    let scanned = invite.to_bytes();
    let cancel = tokio::sync::watch::channel(false).1;

    // Advance far enough that the owner has an activated attempt to cancel:
    // each side runs alone until it has nothing left to answer.
    for _ in 0..2 {
        let _ = Box::pin(fixture.join_over_test_home(&scanned, &join_request, one_shot(), &cancel))
            .await;
        let _ = Box::pin(fixture.handle.drive_device_join(
            &invite,
            crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
            None,
            one_shot(),
        ))
        .await;
    }

    let closing = fixture.close_over_test_home(&scanned, &join_request, timing());
    // A connected handle runs its sync loop, which publishes Store operations
    // of its own. A commit that loses that race is refused *before it
    // persists*, so the unwind resumes from its journal and the caller retries
    // the whole operation — the contract every Store write here keeps. The
    // assertion is that it converges, not that it wins the first race.
    let cancelling = async {
        let mut attempts = Vec::new();
        for _ in 0..5 {
            match fixture.handle.cancel_device_invite(&invite, timing()).await {
                Ok(activation) => return Ok(activation),
                Err(error) => attempts.push(error.to_string()),
            }
        }
        Err(attempts)
    };
    let (cancelled, closed) = tokio::join!(Box::pin(cancelling), Box::pin(closing));
    let cancelled = match cancelled {
        Ok(activation) => activation,
        Err(attempts) => panic!(
            "the owner's handle never carried the cancellation to its cleanup: {attempts:#?}; \
             the joining device's side ended {closed:?}"
        ),
    };
    closed.expect("the joining device closes its side");

    assert_eq!(
        cancelled.receipt.attempt_id, invite.bundle.offer.attempt_id,
        "the cleanup settled this attempt",
    );
    for kind in crate::DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_is_empty(&invite, kind).await,
            "{kind:?} slot outlived the cancelled attempt",
        );
    }
}

/// A host can abandon an invited join before the joining device has done
/// anything with it, and the joining device's next run reads the abandonment in
/// place of the artifact it was waiting for.
#[test]
fn a_facade_only_host_abandons_an_invited_join() {
    on_a_deep_stack(run_a_facade_only_host_abandons_an_invited_join);
}

async fn run_a_facade_only_host_abandons_an_invited_join() {
    let fixture = FacadeFixture::build("facade-device-abandon").await;
    let join_request = crate::generate_join_request(None).expect("generate the join request");
    let invite = fixture
        .handle
        .begin_device_invite(&join_request, crate::MemberRole::Member)
        .await
        .expect("mint the scannable invite");
    let scanned = invite.to_bytes();
    let cancel = tokio::sync::watch::channel(false).1;

    // The joining device publishes its access request and waits.
    let waited =
        Box::pin(fixture.join_over_test_home(&scanned, &join_request, one_shot(), &cancel)).await;
    assert!(
        waited.is_err(),
        "the joining device waits for an admitting side that is not running",
    );

    let abandonment = fixture
        .handle
        .abandon_device_invite(&invite)
        .await
        .expect("the owner abandons the attempt through its handle");

    match Box::pin(fixture.join_over_test_home(&scanned, &join_request, timing(), &cancel))
        .await
        .expect("the joining device accepts the abandonment")
    {
        crate::DeviceJoinTransportOutcome::Abandoned(observed) => {
            assert_eq!(observed, abandonment)
        }
        crate::DeviceJoinTransportOutcome::Joined(_) => {
            panic!("an abandoned attempt must not produce a member config")
        }
    }
    for kind in crate::DeviceJoinTransportKind::ALL {
        assert!(
            fixture.slot_is_empty(&invite, kind).await,
            "{kind:?} slot outlived the abandoned attempt",
        );
    }
}

/// A host holding nothing but `coven` can admit a device end to end: the owner
/// mints one payload from the joining device's request, and the two sides run
/// to a saved member config without either naming an engine type.
#[test]
fn a_facade_only_host_runs_a_whole_device_join() {
    on_a_deep_stack(run_a_facade_only_host_runs_a_whole_device_join);
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
        !String::from_utf8_lossy(&scanned).contains("\"invite_code\""),
        "the device-invite wire must not nest the superseded plaintext credential code",
    );
    let preview = invite
        .inspect(&join_request)
        .expect("the intended joining identity opens its invitation");
    assert_eq!(preview.store_id, "facade-device-join");
    let other_request = crate::generate_join_request(None).expect("generate another request");
    assert!(matches!(
        invite.inspect(&other_request),
        Err(crate::DeviceInviteError::RecipientMismatch)
    ));
    let decoded = crate::DeviceJoinInvite::from_bytes(&scanned).expect("decode the scanned invite");
    assert_eq!(
        decoded
            .inspect(&join_request)
            .expect("the round-tripped invitation opens")
            .store_id,
        "facade-device-join",
    );
    assert!(crate::DeviceJoinInvite::from_bytes(b"{}").is_err());

    let cancel = tokio::sync::watch::channel(false).1;
    let (joined, drove) = tokio::join!(
        Box::pin(
            coven_domain::joining::join_with_scanned_invite_over_test_home(
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
            )
        ),
        Box::pin(fixture.handle.drive_device_join(
            &invite,
            crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
            None,
            timing(),
        )),
    );

    // Report both sides: a failure on one is usually explained by the other,
    // and expecting them one at a time throws that half away.
    let (joined, drove) = match (joined, drove) {
        (Ok(joined), Ok(drove)) => (joined, drove),
        (joined, drove) => panic!(
            "the join did not complete: the joining device ended {joined:?}, \
             the admitting side ended {drove:?}"
        ),
    };
    let config = match joined {
        crate::DeviceJoinTransportOutcome::Joined(config) => config,
        crate::DeviceJoinTransportOutcome::Abandoned(_) => {
            panic!("the attempt was abandoned, not completed")
        }
    };
    let activation = match drove {
        crate::DeviceJoinDriveOutcome::Activated(activation) => activation,
        crate::DeviceJoinDriveOutcome::Abandoned(_) => {
            panic!("the attempt was abandoned, not activated")
        }
    };

    assert!(fixture
        .layout
        .store_dir("facade-device-join")
        .config_path()
        .exists());
    assert_eq!(config.store_id, "facade-device-join");
    assert_eq!(
        activation.outcome.attempt().attempt_id,
        invite.bundle.offer.attempt_id,
    );
}

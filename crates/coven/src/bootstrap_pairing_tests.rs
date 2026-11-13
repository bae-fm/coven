//! The new device's side of a pairing, against the showing device's
//! (§13.2): the whole pairing through `join_with_pairing`, how it ends
//! before and after the handover, and what it keeps (§13.4).

use super::*;
use coven_format::codes::PairingCode;
use coven_format::{handover::Handover, invite_end::InviteEnd, objects::JoinRequest as AskRequest};
use coven_foundation::locks::lock_unpoisoned;
use coven_storage::test_utils::Faults;

type Reported = Arc<std::sync::Mutex<Vec<BootstrapStatus>>>;

/// A status callback that records every report, and tells the returned watch
/// once the call waits for approval.
fn recording() -> (impl Fn(BootstrapStatus), Reported, watch::Receiver<bool>) {
    let reported = Reported::default();
    let (waiting, waits) = watch::channel(false);
    let record = reported.clone();
    let status = move |status: BootstrapStatus| {
        if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
            waiting.send_replace(true);
        }
        lock_unpoisoned(&record).push(status);
    };
    (status, reported, waits)
}

/// Ana's showing device with a pairing, and a new installation to pair.
struct Scene {
    owner: Owner,
    install: Installation,
    pairing: Pairing,
    control: JoinControl,
}

impl Scene {
    async fn new(provider: CloudProvider) -> Self {
        let owner = Owner::new(provider, false).await;
        let pairing = owner.handle.start_pairing().await.unwrap();
        Self { owner, install: Installation::new(), pairing, control: JoinControl::new() }
    }

    fn store(&self) -> StoreId {
        self.owner.directory.id()
    }

    fn code(&self) -> PairingCode {
        coven_sync::read_pairing_code(&self.pairing.shared_code).unwrap()
    }

    /// The new device's call, with the code Ana shows as a QR code or as text
    /// to share; on S3 a shared code needs the key entered.
    async fn join(
        &self,
        qr: bool,
        status: impl Fn(BootstrapStatus),
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        let (text, key) = if qr {
            (&self.pairing.qr_code, None)
        } else {
            (&self.pairing.shared_code, self.owner.s3_credentials())
        };
        join_with_pairing(
            self.install.authenticated_builder(&self.owner, self.owner.storage.clone()).await,
            text,
            key,
            "Ana’s laptop",
            status,
            &self.control,
        )
        .await
    }

    /// Once the new device has sent its request and waits for the next
    /// check, Ana's pass reads it and the screen offers it.
    async fn offered(&self, waits: &mut watch::Receiver<bool>) -> PairingRequest {
        waits.wait_for(|waiting| *waiting).await.unwrap();
        self.control.parked_from_now().await;
        self.owner.pass_now().await;
        match self.owner.handle.pairing(self.pairing.id).await.unwrap() {
            Some(PairingStatus::Waiting { request: Some(request) }) => request,
            status => panic!("a checked request was expected: {status:?}"),
        }
    }

    /// Ana approves, her pass puts the handover in place, and the new device
    /// is asked to check at once. The ask lasts as long as the guard.
    async fn approve(&self, request: &PairingRequest) -> coven_sync::SyncOftenGuard {
        self.owner.handle.approve_pairing(request).await.unwrap();
        self.owner.pass_now().await;
        self.control.check_often(Duration::ZERO)
    }

    /// The object at the request's path.
    async fn stored(&self) -> Vec<u8> {
        self.owner.storage.read(&self.code().request_path()).await.unwrap()
    }

    async fn status(&self) -> Option<PairingStatus> {
        self.owner.handle.pairing(self.pairing.id).await.unwrap()
    }

    async fn close(self) {
        self.owner.close().await;
    }
}

/// The request at the pairing's path, opened with the code's secret.
fn request_in(code: &PairingCode, bytes: &[u8]) -> AskRequest {
    let envelope = coven_format::sealed_single::SingleChunkObject::decode(bytes).unwrap();
    let coven_format::sealed_single::SingleChunkObject::JoinRequest { chunk, .. } = &envelope;
    let plain = code
        .secret
        .join_request_key()
        .open_object_chunk(
            code.request_path().as_str(),
            &envelope.prefix().encode().unwrap(),
            0,
            0,
            chunk,
        )
        .unwrap();
    let coven_format::Object::JoinRequest(request) = coven_format::Object::decode(&plain).unwrap()
    else {
        panic!("a join request")
    };
    request
}

/// A call that is refused before it touches storage; one that goes on to wait
/// for approval is cut off.
async fn within(
    call: impl std::future::Future<Output = Result<Option<CovenHandle>, BootstrapError>>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    tokio::time::timeout(Duration::from_secs(30), call)
        .await
        .expect("the pairing was refused before it waited")
}

/// The call on an installation that cannot pair.
async fn refusal(scene: &Scene) -> Result<Option<CovenHandle>, BootstrapError> {
    within(scene.join(true, |_| {})).await
}

fn is_marker(bytes: &[u8]) -> bool {
    InviteEnd::decode(bytes).is_ok()
}

/// What a pairing reports once its handover opened: loading, registering and
/// committing, with the waiting shown once before them.
fn assert_reported(reported: &Reported) {
    let reported = lock_unpoisoned(reported).clone();
    let waiting = reported
        .iter()
        .filter(|status| matches!(status, BootstrapStatus::WaitingForApproval { .. }))
        .count();
    assert_eq!(waiting, 1, "{reported:?}");
    let first = reported
        .iter()
        .position(|status| matches!(status, BootstrapStatus::WaitingForApproval { .. }))
        .unwrap();
    // The asking reported the opening and the connecting, and a phase never
    // comes back.
    let restoring: Vec<_> = reported[first + 1..].iter().collect();
    let [loading @ .., BootstrapStatus::Registering, BootstrapStatus::Committing] = &restoring[..]
    else {
        panic!("{reported:?} doesn't register, then commit, after the wait");
    };
    assert!(
        !loading.is_empty()
            && loading.iter().all(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
}

/// Ana pairs a new laptop: the laptop sends its request, Ana compares the
/// number and approves, the laptop restores from the handover and registers
/// its device with its own entry, and Ana's pairing ends as added with only
/// the marker left at the path, on Dropbox, on S3 with the QR code (no key
/// asked) and on S3 with the code to share (the key entered).
#[tokio::test]
async fn a_pairing_restores_the_new_device_and_ends_added() {
    for (provider, qr) in
        [(CloudProvider::Dropbox, true), (CloudProvider::S3, true), (CloudProvider::S3, false)]
    {
        let scene = Scene::new(provider).await;
        let mut live = coven_database::test_utils::Followed::new(
            scene.owner.handle.subscribe_pairing(scene.pairing.id),
        );
        let (status, reported, mut waits) = recording();
        let joining = scene.join(qr, status);
        let responding = async {
            let request = scene.offered(&mut waits).await;
            // The number Ana compares is the one the laptop shows.
            let shown = lock_unpoisoned(&reported)
                .iter()
                .find_map(|status| match status {
                    BootstrapStatus::WaitingForApproval { confirmation } => Some(*confirmation),
                    _ => None,
                })
                .unwrap();
            assert_eq!(request.confirmation, shown);
            let guard = scene.approve(&request).await;
            (request, guard)
        };
        let (result, (request, _guard)) = join_with_response(joining, responding).await;
        let handle = result.unwrap().unwrap();
        let device = handle.device_id();
        assert_eq!(request.device_name, "Ana’s laptop");
        assert_reported(&reported);
        // The laptop's own add-device entry registered it with its name; Ana's
        // next pass applies it and ends the pairing as added.
        scene.owner.pass_now().await;
        let added =
            PairingStatus::Ended(PairingEnd::Added { device, name: "Ana’s laptop".into() });
        assert_eq!(scene.status().await, Some(added.clone()), "{provider:?} qr {qr}");
        live.wait_for(|status| status.as_ref() == Some(&added)).await;
        scene.owner.pass_now().await;
        assert!(is_marker(&scene.stored().await), "{provider:?} qr {qr}");
        let members = scene.owner.handle.members().await.unwrap();
        let ana = members.iter().find(|member| member.id == scene.owner.member.member()).unwrap();
        assert!(
            ana.devices.iter().any(|d| d.id == device
                && d.name == "Ana’s laptop"
                && d.state == DeviceState::Active),
            "{provider:?} qr {qr}: {:?}",
            ana.devices
        );
        // The laptop is a restored device of the store: its code, and on S3
        // its key, are saved beside the store.
        assert_eq!(
            handle.restore_code().await.unwrap(),
            scene.owner.code.to_text().unwrap().as_str(),
            "{provider:?} qr {qr}"
        );
        handle.close().await.unwrap();
        scene.close().await;
    }
}

/// A key that S3 needs and the code does not carry is asked for, and the one
/// the QR code carries takes no second: either way before anything changes.
#[tokio::test]
async fn an_s3_pairing_needs_one_key_and_changes_nothing_without_it() {
    let scene = Scene::new(CloudProvider::S3).await;
    let missing = within(join_with_pairing(
        scene.install.authenticated_builder(&scene.owner, scene.owner.storage.clone()).await,
        &scene.pairing.shared_code,
        None,
        "Ana’s laptop",
        |_| {},
        &scene.control,
    ))
    .await;
    assert!(
        matches!(&missing, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::InvalidConfiguration),
        "{:?}",
        missing.as_ref().err()
    );
    let second = within(join_with_pairing(
        scene.install.authenticated_builder(&scene.owner, scene.owner.storage.clone()).await,
        &scene.pairing.qr_code,
        scene.owner.s3_credentials(),
        "Ana’s laptop",
        |_| {},
        &scene.control,
    ))
    .await;
    assert!(
        matches!(&second, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::InvalidConfiguration),
        "{:?}",
        second.as_ref().err()
    );
    scene.install.absent(scene.store()).await;
    assert!(scene.owner.storage.read(&scene.code().request_path()).await.is_err());
    scene.close().await;
}

/// Ana declines the request: the laptop's next check finds the marker, and
/// the call ends with nothing left on the laptop.
#[tokio::test]
async fn a_declined_pairing_ends_the_call_with_no_installation_left() {
    let scene = Scene::new(CloudProvider::S3).await;
    let (status, reported, mut waits) = recording();
    let joining = scene.join(true, status);
    let responding = async {
        let request = scene.offered(&mut waits).await;
        scene.owner.handle.decline_pairing(&request).await.unwrap();
        scene.owner.pass_now().await;
        scene.control.check_often(Duration::ZERO)
    };
    let (result, _guard) = join_with_response(joining, responding).await;
    assert!(matches!(result, Ok(None)), "{:?}", result.err());
    assert_eq!(scene.status().await, Some(PairingStatus::Ended(PairingEnd::Declined)));
    assert!(is_marker(&scene.stored().await));
    scene.install.absent(scene.store()).await;
    assert!(!scene.install.root.path().join("stores").join(scene.store().to_string()).exists());
    assert!(!lock_unpoisoned(&reported)
        .iter()
        .any(|status| matches!(status, BootstrapStatus::Registering)));
    scene.close().await;
}

/// Ana closes the screen before she approves, and again after she approved
/// but before the laptop read the handover: the marker replaces the request
/// or the handover, and the laptop's call ends with nothing left.
#[tokio::test]
async fn closing_the_screen_before_the_laptop_reads_the_handover_ends_the_call() {
    for approved in [false, true] {
        let scene = Scene::new(CloudProvider::S3).await;
        let (status, _, mut waits) = recording();
        let joining = scene.join(true, status);
        let responding = async {
            let request = scene.offered(&mut waits).await;
            if approved {
                scene.owner.handle.approve_pairing(&request).await.unwrap();
                scene.owner.pass_now().await;
                // The handover is in the request's place, unread.
                assert!(Handover::decode(&scene.stored().await).is_ok());
            }
            scene.owner.handle.end_pairing(scene.pairing.id).await.unwrap();
            scene.owner.pass_now().await;
            assert!(is_marker(&scene.stored().await), "approved {approved}");
            scene.control.check_often(Duration::ZERO)
        };
        let (result, _guard) = join_with_response(joining, responding).await;
        assert!(matches!(result, Ok(None)), "approved {approved}: {:?}", result.err());
        scene.install.absent(scene.store()).await;
        scene.close().await;
    }
}

/// The laptop cancels after its request was sent: its installation goes, and
/// Ana's screen still offers the request. Approving it writes a handover
/// nobody opens, and closing the screen replaces it with the marker.
#[tokio::test]
async fn cancelling_after_the_request_was_sent_leaves_the_pairing_to_close() {
    let scene = Scene::new(CloudProvider::S3).await;
    let (status, _, mut waits) = recording();
    let joining = scene.join(true, status);
    let responding = async {
        let request = scene.offered(&mut waits).await;
        scene.control.cancel();
        request
    };
    let (result, request) = join_with_response(joining, responding).await;
    assert!(matches!(result, Err(BootstrapError::Cancelled)), "{:?}", result.err());
    scene.install.absent(scene.store()).await;
    assert!(!scene.install.root.path().join("stores").join(scene.store().to_string()).exists());
    // Ana's screen offers the request still, and approving it hands over to
    // no one.
    scene.owner.pass_now().await;
    assert_eq!(
        scene.status().await,
        Some(PairingStatus::Waiting { request: Some(request.clone()) })
    );
    scene.owner.handle.approve_pairing(&request).await.unwrap();
    scene.owner.pass_now().await;
    assert!(Handover::decode(&scene.stored().await).is_ok());
    scene.owner.handle.end_pairing(scene.pairing.id).await.unwrap();
    scene.owner.pass_now().await;
    assert!(is_marker(&scene.stored().await));
    scene.close().await;
}

/// Someone who saw the code replaces the request with a handover of their own
/// making, sealed to the laptop's key but holding another pair: the laptop
/// opens it and its restore fails when it checks the pair, before it records
/// anything.
#[tokio::test]
async fn a_forged_handover_fails_the_restore_with_nothing_recorded() {
    let scene = Scene::new(CloudProvider::S3).await;
    let (status, _, mut waits) = recording();
    let joining = scene.join(true, status);
    let responding = async {
        scene.offered(&mut waits).await;
        let code = scene.code();
        let path = code.request_path();
        let asking = request_in(&code, &scene.stored().await);
        let forged = Handover::seal(
            code.member,
            &coven_crypto::MemberKeys::generate().unwrap(),
            &asking.keys.sealing,
            &path,
        )
        .unwrap();
        scene.owner.storage.delete(&path).await.unwrap();
        scene.owner.storage.create(&path, &forged.encode()).await.unwrap();
        scene.control.check_often(Duration::ZERO)
    };
    let (result, _guard) = join_with_response(joining, responding).await;
    assert!(result.is_err(), "a forged pair restored");
    let error = result.err().unwrap();
    assert!(
        !matches!(error, BootstrapError::Cancelled | BootstrapError::RestoreRegistered),
        "{error:?}"
    );
    scene.install.absent(scene.store()).await;
    assert!(!scene.install.root.path().join("stores").join(scene.store().to_string()).exists());
    scene.close().await;
}

/// Ana's keys are replaced after she approved: the handover holds a pair
/// history has since replaced, and the laptop's restore fails as an old
/// restore code does.
#[tokio::test]
async fn keys_replaced_after_approval_fail_the_restore() {
    let scene = Scene::new(CloudProvider::S3).await;
    let (status, _, mut waits) = recording();
    let joining = scene.join(true, status);
    let responding = async {
        let request = scene.offered(&mut waits).await;
        scene.owner.handle.approve_pairing(&request).await.unwrap();
        scene.owner.pass_now().await;
        scene.owner.handle.replace_member_keys().await.unwrap();
        scene.owner.sync().await;
        scene.control.check_often(Duration::ZERO)
    };
    let (result, _guard) = join_with_response(joining, responding).await;
    assert!(
        matches!(&result, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { member }))
            if *member == scene.owner.member.member()),
        "{:?}",
        result.err()
    );
    scene.install.absent(scene.store()).await;
    scene.close().await;
}

/// A pairing on an installation that holds a join's saved code or a restore's
/// record is refused before anything changes, as a restore is on a join's.
#[tokio::test]
async fn a_pairing_refuses_a_joins_or_a_restores_installation() {
    // A join waiting for approval.
    let scene = Scene::new(CloudProvider::S3).await;
    let invite = scene.owner.invite().await;
    waiting_join(&scene.owner, &scene.install, &invite).await;
    let refused = refusal(&scene).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(
            StoreLockError::BootstrapPending(store)
        ))) if *store == scene.store()),
        "{:?}",
        refused.err()
    );
    assert_eq!(saved_code(&scene.install, scene.store()).unwrap().as_str(), invite.shown_code());
    assert!(scene.owner.storage.read(&scene.code().request_path()).await.is_err());
    scene.close().await;
    // A restore recorded and stopped.
    let scene = Scene::new(CloudProvider::S3).await;
    let (_, cancel) = watch::channel(false);
    scene.owner.storage.set_faults(Faults { refuse_after_storing: 1, ..Faults::none() }).await;
    let stopped = restore_from_code(
        scene.install.authenticated_builder(&scene.owner, scene.owner.storage.clone()).await,
        &scene.owner.code.to_text().unwrap(),
        scene.owner.s3_credentials(),
        "Ana’s laptop",
        |_| {},
        &cancel,
    )
    .await;
    assert!(stopped.is_err());
    scene.owner.storage.set_faults(Faults::none()).await;
    let refused = refusal(&scene).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(
            StoreLockError::BootstrapPending(store)
        ))) if *store == scene.store()),
        "{:?}",
        refused.err()
    );
    assert!(scene.owner.storage.read(&scene.code().request_path()).await.is_err());
    scene.close().await;
}

/// While the laptop waits, nothing of the pair it asks with is on its disk
/// or in its keychain: no file holds a member pair's private bytes, and the
/// store has no saved code, record, custody or credentials.
#[tokio::test]
async fn the_requests_pair_is_nowhere_on_the_new_device_while_it_waits() {
    let scene = Scene::new(CloudProvider::S3).await;
    let (status, _, mut waits) = recording();
    let joining = scene.join(true, status);
    let responding = async {
        let request = scene.offered(&mut waits).await;
        let asking = request_in(&scene.code(), &scene.stored().await);
        assert_eq!(request.device_name, asking.device_name);
        let id = scene.store();
        let entries = StoreKeychain::new(scene.install.keychain.clone(), id);
        assert!(entries.invite_code().unwrap().is_none());
        assert!(entries.restore_record().unwrap().is_none());
        assert!(entries.storage_credentials().unwrap().is_none());
        assert!(PersonEntries::new(scene.install.keychain.clone(), id)
            .restore_code()
            .unwrap()
            .is_none());
        // A pair written anywhere would carry the magic of the encoding
        // custody and codes use for it; none decodes to the request's pair.
        let files = files_under(scene.install.root.path());
        for (path, bytes) in &files {
            let Some(bytes) = bytes else { continue };
            for at in bytes
                .windows(MEMBER_KEYS_MAGIC.len())
                .enumerate()
                .filter_map(|(at, window)| (window == MEMBER_KEYS_MAGIC).then_some(at))
            {
                let found = coven_crypto::MemberKeys::from_secret_bytes(&bytes[at..]);
                assert!(
                    found.map_or(true, |keys| keys.public_keys() != asking.keys),
                    "{path:?} holds the request's pair"
                );
            }
        }
        assert!(!files.is_empty());
        scene.control.cancel();
    };
    let (result, ()) = join_with_response(joining, responding).await;
    assert!(matches!(result, Err(BootstrapError::Cancelled)));
    scene.close().await;
}

const MEMBER_KEYS_MAGIC: &[u8] = b"CVMK\x01";

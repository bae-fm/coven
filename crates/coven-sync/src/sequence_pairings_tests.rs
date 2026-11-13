use super::*;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_crypto::{ConfirmationNumber, MemberKeys};
use coven_database::{DeviceStop, RetrySubject};
use coven_format::{
    admin::StoreChange, invite_end::InviteEnd, objects::JoinRequest, objects::Joiner as Tag, Object,
};
use coven_foundation::files::DeviceReset;
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_storage::test_utils::{Faults, MemoryRequest, MemoryStorage};
use coven_storage::{Storage, StorageFailure};
use std::time::Duration;

impl SequenceSync {
    /// Start a pairing as the app's write step does, on a location that holds
    /// no S3 key.
    async fn start_pairing(&self) -> Result<crate::Pairing, SyncError> {
        let location = self.storage.config().clone();
        coven_storage::record_config(&self.directory, &location)?;
        let steps = self.operation_steps();
        self.database.write_as(move |sql| steps.start_pairing(&sql, None)).await
    }

    async fn approve_request(&self, pairing: InviteId, shown: &[u8]) -> Result<(), SyncError> {
        let (steps, shown) = (self.operation_steps(), shown_request(pairing, shown));
        self.database.write_as(move |sql| steps.approve_pairing(&sql, &shown)).await
    }

    async fn decline_request(&self, pairing: InviteId, shown: &[u8]) -> Result<(), SyncError> {
        let (steps, shown) = (self.operation_steps(), shown_request(pairing, shown));
        self.database.write_as(move |sql| steps.decline_pairing(&sql, &shown)).await
    }

    async fn end_pairing(&self, pairing: InviteId) -> Result<(), SyncError> {
        let steps = self.operation_steps();
        self.database.write_as(move |sql| steps.end_pairing(&sql, pairing.into())).await
    }
}

/// The request the app was shown, as the pairing's status presents it.
fn shown_request(pairing: InviteId, record: &[u8]) -> PairingRequest {
    let Object::JoinRequest(read) = Object::decode(record).unwrap() else { panic!("a request") };
    PairingRequest {
        pairing: pairing.into(),
        device_name: read.device_name,
        confirmation: ConfirmationNumber::of(pairing, &read.keys),
        record: record.to_vec(),
    }
}

/// A pairing Ana's device made, and what the new device needs to ask.
struct Pairing {
    id: InviteId,
    code: PairingCode,
    path: ObjectPath,
}

async fn started(ana: &Device) -> Pairing {
    let made = ana.sync.start_pairing().await.unwrap();
    let code = read_pairing_code(&made.shared_code).unwrap();
    assert_eq!(code.pairing, InviteId::from(made.id));
    let path = code.request_path();
    Pairing { id: code.pairing, code, path }
}

/// The new device's request, answering `pairing` with fresh `keys` as `device`.
fn request(pairing: &Pairing, keys: &MemberKeys, device: DeviceId, joiner: Tag) -> JoinRequest {
    JoinRequest {
        invite: pairing.code.pairing,
        member: pairing.code.member,
        keys: keys.public_keys(),
        device,
        device_name: "Ana's laptop".into(),
        joiner,
    }
}

async fn send(group: &Group, pairing: &Pairing, request: &JoinRequest, keys: &MemberKeys) {
    let bytes =
        crate::sealed_join_request(&pairing.code.secret, &pairing.path, request, keys).unwrap();
    group.storage.create(&pairing.path, &bytes).await.unwrap();
}

/// Whether the ended pairing's marker, and nothing else, is at its path.
async fn marker_at(storage: &MemoryStorage, path: &ObjectPath) -> bool {
    InviteEnd::decode(&storage.read(path).await.unwrap()).is_ok()
}

async fn rows(device: &Device) -> Vec<(OperationRecord, PairingWork)> {
    device
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.kind == operation_kind::PAIRING)
        .map(|record| {
            let work = PairingWork::read(&record).unwrap();
            (record, work)
        })
        .collect()
}

async fn work_of(device: &Device, pairing: &Pairing) -> Option<PairingWork> {
    rows(device).await.into_iter().map(|(_, work)| work).find(|work| work.id == pairing.id)
}

async fn state_of(device: &Device, pairing: &Pairing) -> Option<PairingState> {
    work_of(device, pairing).await.map(|work| work.state)
}

/// How a test builds the request it sends to a pairing.
type Asking = Box<dyn Fn(&Pairing) -> (JoinRequest, MemberKeys)>;

/// A fresh request that passes every check, sent by a new device.
async fn asked(group: &Group, ana: &Device, pairing: &Pairing) -> (JoinRequest, MemberKeys) {
    let keys = MemberKeys::generate().unwrap();
    let request = request(pairing, &keys, DeviceId(70), Tag::OwnDevice);
    send(group, pairing, &request, &keys).await;
    ana.sync.pass().await.unwrap();
    (request, keys)
}

fn plaintext(request: &JoinRequest) -> Vec<u8> {
    Object::JoinRequest(request.clone()).encode().unwrap()
}

/// Where a pairing is when a test acts on it.
#[derive(Clone, Copy, Debug)]
enum At {
    Nothing,
    Offered,
    Approved,
    HandedOver,
}

const ALL: [At; 4] = [At::Nothing, At::Offered, At::Approved, At::HandedOver];

async fn reach(
    group: &Group,
    ana: &Device,
    at: At,
) -> (Pairing, Option<(JoinRequest, MemberKeys)>) {
    let pairing = started(ana).await;
    if matches!(at, At::Nothing) {
        return (pairing, None);
    }
    let (request, keys) = asked(group, ana, &pairing).await;
    if matches!(at, At::Approved | At::HandedOver) {
        ana.sync.approve_request(pairing.id, &plaintext(&request)).await.unwrap();
    }
    if matches!(at, At::HandedOver) {
        ana.sync.pass().await.unwrap();
    }
    (pairing, Some((request, keys)))
}

async fn owed_removals(device: &Device) -> Vec<String> {
    device.database().due_object_removals(u64::MAX).await.unwrap()
}

/// A request that passes the pairing's checks waits for the app with its
/// exact bytes, and nothing is shared, claimed or recorded for the pairing.
#[tokio::test]
async fn a_checked_request_is_offered_with_its_exact_bytes() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let pairing = started(&ana).await;
    assert!(matches!(
        state_of(&ana, &pairing).await,
        Some(PairingState::Waiting { request: None })
    ));
    ana.sync.pass().await.unwrap();
    assert!(
        matches!(state_of(&ana, &pairing).await, Some(PairingState::Waiting { request: None })),
        "a pass finds no request"
    );
    let (request, keys) = asked(&group, &ana, &pairing).await;
    let Some(PairingState::Waiting { request: Some(offered) }) = state_of(&ana, &pairing).await
    else {
        panic!("the request is offered");
    };
    assert_eq!(offered, plaintext(&request));
    // The number the new device shows is computed from the same keys.
    let Object::JoinRequest(read) = Object::decode(&offered).unwrap() else { panic!("a request") };
    assert_eq!(
        ConfirmationNumber::of(pairing.id, &read.keys),
        ConfirmationNumber::of(pairing.id, &keys.public_keys())
    );
    ana.database().close().await.unwrap();
}

/// A request that fails a check ends the pairing in the pass that read it,
/// with the marker in place, owed its removal and no pending record.
#[tokio::test]
async fn a_request_that_fails_a_check_ends_the_pairing_with_the_marker_placed() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let other = MemberId(uuid::Uuid::from_u128(99));
    let bound = ana.identity.keys().clone();
    let fresh = || MemberKeys::generate().unwrap();
    let cases: Vec<(&str, Asking)> = vec![
        (
            "a new person",
            Box::new(move |p| {
                let keys = fresh();
                (request(p, &keys, DeviceId(70), Tag::NewPerson { name: "Eve".into() }), keys)
            }),
        ),
        (
            "a recovery",
            Box::new(move |p| {
                let keys = fresh();
                (request(p, &keys, DeviceId(70), Tag::Recovery), keys)
            }),
        ),
        (
            "another member",
            Box::new(move |p| {
                let keys = fresh();
                let mut asking = request(p, &keys, DeviceId(70), Tag::OwnDevice);
                asking.member = other;
                (asking, keys)
            }),
        ),
        (
            "keys history knows",
            Box::new(move |p| {
                let keys = bound.clone();
                (request(p, &keys, DeviceId(70), Tag::OwnDevice), keys)
            }),
        ),
        (
            "a registered device id",
            Box::new({
                let registered = ana.sync.device;
                move |p| {
                    let keys = fresh();
                    (request(p, &keys, registered, Tag::OwnDevice), keys)
                }
            }),
        ),
    ];
    for (what, make) in cases {
        let pairing = started(&ana).await;
        let (asking, keys) = make(&pairing);
        send(&group, &pairing, &asking, &keys).await;
        ana.sync.pass().await.unwrap();
        assert!(
            matches!(
                state_of(&ana, &pairing).await,
                Some(PairingState::Ended { how: PairingEnd::Refused })
            ),
            "{what}: {:?}",
            state_of(&ana, &pairing).await
        );
        assert!(marker_at(&group.storage, &pairing.path).await, "{what}: the marker is in place");
        assert!(owed_removals(&ana).await.contains(&pairing.path.as_str().to_owned()), "{what}");
        for (record, _) in rows(&ana).await {
            let pending = ana.database().operation_pending::<PendingRecord>(record.id).await;
            assert!(pending.unwrap().is_none(), "{what}: no pending record");
        }
    }
    ana.database().close().await.unwrap();
}

/// Approving seals this member's current pair to the offered request's key in
/// the write; the pass puts exactly those bytes in the request's place.
#[tokio::test]
async fn approval_replaces_the_request_with_exactly_the_journaled_handover() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let pairing = started(&ana).await;
    let (asking, keys) = asked(&group, &ana, &pairing).await;
    ana.sync.approve_request(pairing.id, &plaintext(&asking)).await.unwrap();
    let Some(PairingState::Approving { handover, .. }) = state_of(&ana, &pairing).await else {
        panic!("approving");
    };
    // The write fixed the bytes; the request is still there until a pass.
    assert!(!marker_at(&group.storage, &pairing.path).await);
    assert_ne!(group.storage.read(&pairing.path).await.unwrap(), handover);
    ana.sync.pass().await.unwrap();
    assert!(matches!(state_of(&ana, &pairing).await, Some(PairingState::HandedOver { .. })));
    let stored = group.storage.read(&pairing.path).await.unwrap();
    assert_eq!(stored, handover);
    // It opens with the request's key, at its path, and holds this member's pair.
    let sealed = Handover::decode(&stored).unwrap();
    let (member, pair) = sealed.open(&pairing.path, &keys).unwrap();
    assert_eq!(member, pairing.code.member);
    assert_eq!(pair.public_keys(), ana.identity.keys().public_keys());
    // And with nothing else.
    assert!(sealed.open(&pairing.path, &MemberKeys::generate().unwrap()).is_err());
    let elsewhere = ObjectPath::join_request(group.store, DeviceId(9), pairing.id);
    assert!(sealed.open(&elsewhere, &keys).is_err());
    // A second pass changes nothing.
    ana.sync.pass().await.unwrap();
    assert_eq!(group.storage.read(&pairing.path).await.unwrap(), handover);
    ana.database().close().await.unwrap();
}

/// Approving takes the request the app was shown: a different request, or
/// none, is refused and seals nothing.
#[tokio::test]
async fn approval_of_a_request_the_pairing_does_not_hold_is_refused() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let pairing = started(&ana).await;
    let shown = {
        let keys = MemberKeys::generate().unwrap();
        plaintext(&request(&pairing, &keys, DeviceId(71), Tag::OwnDevice))
    };
    assert!(matches!(
        ana.sync.approve_request(pairing.id, &shown).await,
        Err(SyncError::InvitationChanged)
    ));
    let (asking, _) = asked(&group, &ana, &pairing).await;
    assert!(matches!(
        ana.sync.approve_request(pairing.id, &shown).await,
        Err(SyncError::InvitationChanged)
    ));
    assert!(matches!(
        state_of(&ana, &pairing).await,
        Some(PairingState::Waiting { request: Some(_) })
    ));
    ana.sync.approve_request(pairing.id, &plaintext(&asking)).await.unwrap();
    // Once approved, a decline of the same request fails, and so does another approval.
    assert!(matches!(
        ana.sync.decline_request(pairing.id, &plaintext(&asking)).await,
        Err(SyncError::InvitationChanged)
    ));
    assert!(matches!(
        ana.sync.approve_request(pairing.id, &plaintext(&asking)).await,
        Err(SyncError::InvitationChanged)
    ));
    // An expired pairing refuses both.
    let late = started(&ana).await;
    let (waiting, _) = asked(&group, &ana, &late).await;
    group.clock.advance(LIFETIME);
    assert!(matches!(
        ana.sync.approve_request(late.id, &plaintext(&waiting)).await,
        Err(SyncError::InvitationChanged)
    ));
    assert!(matches!(
        ana.sync.decline_request(late.id, &plaintext(&waiting)).await,
        Err(SyncError::InvitationChanged)
    ));
    ana.database().close().await.unwrap();
}

/// A request created between the pass's delete and its create is deleted
/// again, and the handover still ends up in the request's place.
#[tokio::test]
async fn a_request_created_between_the_delete_and_the_create_is_deleted_again() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let pairing = started(&ana).await;
    let (asking, _) = asked(&group, &ana, &pairing).await;
    ana.sync.approve_request(pairing.id, &plaintext(&asking)).await.unwrap();
    let Some(PairingState::Approving { handover, .. }) = state_of(&ana, &pairing).await else {
        panic!("approving");
    };
    let (started, holding) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    ana.storage
        .hold_next_request_of(MemoryRequest::Create(pairing.path.clone()), started, resume)
        .await;
    let (pass, ()) = tokio::join!(ana.sync.pass(), async {
        // A pass that never reaches the create fails the test instead of
        // leaving it waiting.
        tokio::time::timeout(Duration::from_secs(30), holding)
            .await
            .expect("the pass reached the handover's create")
            .unwrap();
        // The request is deleted and the handover not yet created: whoever
        // saw the code writes a request of their own.
        let keys = MemberKeys::generate().unwrap();
        let other = request(&pairing, &keys, DeviceId(72), Tag::OwnDevice);
        send(&group, &pairing, &other, &keys).await;
        release.send(()).unwrap();
    });
    pass.unwrap();
    assert_eq!(group.storage.read(&pairing.path).await.unwrap(), handover);
    ana.database().close().await.unwrap();
}

/// The new device's add-device entry, applied here, ends the pairing as
/// added and the marker replaces the handover.
#[tokio::test]
async fn the_new_devices_registration_ends_the_pairing_as_added() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let (temp, _) = group.directory();
    let laptop = group.open(temp, ana.identity.clone()).await;
    let laptop_id = laptop.sync.device;
    let pairing = started(&ana).await;
    let keys = MemberKeys::generate().unwrap();
    let asking = request(&pairing, &keys, laptop_id, Tag::OwnDevice);
    send(&group, &pairing, &asking, &keys).await;
    ana.sync.pass().await.unwrap();
    ana.sync.approve_request(pairing.id, &plaintext(&asking)).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(matches!(state_of(&ana, &pairing).await, Some(PairingState::HandedOver { .. })));
    // Until history shows the device registered, the handover stays.
    ana.sync.pass().await.unwrap();
    assert!(matches!(state_of(&ana, &pairing).await, Some(PairingState::HandedOver { .. })));
    assert!(Handover::decode(&group.storage.read(&pairing.path).await.unwrap()).is_ok());
    laptop
        .sync
        .administer(StoreChange::AddDevice { name: "Ana's laptop".into(), replaces: None })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let Some(PairingState::Ended { how }) = state_of(&ana, &pairing).await else {
        panic!("ended: {:?}", state_of(&ana, &pairing).await)
    };
    assert_eq!(how, PairingEnd::Added { device: laptop_id, name: "Ana's laptop".into() });
    assert!(marker_at(&group.storage, &pairing.path).await);
    ana.database().close().await.unwrap();
    laptop.database().close().await.unwrap();
}

/// Declining, expiry and the app ending the pairing, at every stage, leave
/// only the marker at the path; once the app has closed the screen no row is
/// left.
#[tokio::test]
async fn every_ending_leaves_only_the_marker_and_then_no_row() {
    for at in ALL {
        for how in ["expiry", "end", "decline"] {
            if how == "decline" && !matches!(at, At::Offered) {
                continue;
            }
            let group = Group::shared();
            let ana = group.device(1).await;
            group.create(&ana).await;
            let (pairing, asked) = reach(&group, &ana, at).await;
            let label = format!("{how} at {at:?}");
            match how {
                "decline" => {
                    let (request, _) = asked.as_ref().expect("an offered request");
                    ana.sync.decline_request(pairing.id, &plaintext(request)).await.unwrap();
                }
                "expiry" => {
                    group.clock.advance(LIFETIME);
                    let waits = ana.sync.waits.clone();
                    let state = ana.sync.state.clone();
                    let next = crate::sequence_sync::sequence_invites::expiry::expire(
                        ana.database(),
                        ana.sync.clock.now(),
                        &waits,
                        &state,
                    )
                    .await
                    .unwrap();
                    assert_eq!(next, None, "{label}");
                    assert!(
                        matches!(
                            state_of(&ana, &pairing).await,
                            Some(PairingState::Ending { how: PairingEnd::Expired })
                        ),
                        "{label}: {:?}",
                        state_of(&ana, &pairing).await
                    );
                }
                _ => {
                    ana.sync.end_pairing(pairing.id).await.unwrap();
                    assert!(
                        matches!(state_of(&ana, &pairing).await, Some(PairingState::Forgetting)),
                        "{label}"
                    );
                }
            }
            ana.sync.pass().await.unwrap();
            assert!(marker_at(&group.storage, &pairing.path).await, "{label}: marker");
            assert!(owed_removals(&ana).await.contains(&pairing.path.as_str().to_owned()));
            if how == "end" {
                assert!(state_of(&ana, &pairing).await.is_none(), "{label}: forgotten");
            } else {
                assert!(
                    matches!(state_of(&ana, &pairing).await, Some(PairingState::Ended { .. })),
                    "{label}: {:?}",
                    state_of(&ana, &pairing).await
                );
                ana.sync.end_pairing(pairing.id).await.unwrap();
                assert!(state_of(&ana, &pairing).await.is_none(), "{label}: forgotten");
                assert!(marker_at(&group.storage, &pairing.path).await, "{label}");
            }
            // Ending it again changes nothing.
            ana.sync.end_pairing(pairing.id).await.unwrap();
            ana.database().close().await.unwrap();
        }
    }
}

/// Opening the store forgets every pairing it finds, and still places the
/// marker where its request or handover is.
#[tokio::test]
async fn opening_the_store_forgets_every_pairing_and_still_places_the_marker() {
    for at in ALL {
        let group = Group::shared();
        let ana = group.device(1).await;
        group.create(&ana).await;
        let (pairing, _) = reach(&group, &ana, at).await;
        // A pairing that ended already: its request failed a check.
        let ended = started(&ana).await;
        let keys = MemberKeys::generate().unwrap();
        send(&group, &ended, &request(&ended, &keys, DeviceId(73), Tag::Recovery), &keys).await;
        ana.sync.pass().await.unwrap();
        assert!(matches!(state_of(&ana, &ended).await, Some(PairingState::Ended { .. })));
        pairings_opened(ana.database()).await.unwrap();
        assert!(state_of(&ana, &ended).await.is_none(), "an ended pairing is forgotten at once");
        assert!(
            matches!(state_of(&ana, &pairing).await, Some(PairingState::Forgetting)),
            "{at:?}: {:?}",
            state_of(&ana, &pairing).await
        );
        ana.sync.pass().await.unwrap();
        assert!(state_of(&ana, &pairing).await.is_none(), "{at:?}");
        assert!(marker_at(&group.storage, &pairing.path).await, "{at:?}");
        ana.database().close().await.unwrap();
    }
}

/// A stop for good finishes the pairing rows in its own transaction and
/// claims nothing; the admin's sweep of the gone device's requests takes the
/// handover too.
#[tokio::test]
async fn a_stop_for_good_finishes_the_rows_and_claims_nothing() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let (offered, _) = reach(&group, &ana, At::Offered).await;
    let (handed, _) = reach(&group, &ana, At::HandedOver).await;
    assert!(Handover::decode(&group.storage.read(&handed.path).await.unwrap()).is_ok());
    ana.database().end_for_good(DeviceStop::Removed).await.unwrap();
    assert!(rows(&ana).await.is_empty());
    assert!(ana.database().current_claim().await.unwrap().is_none());
    // The request and the handover are still at their paths: the gone
    // device's marker sweep takes them.
    assert!(group.storage.read(&offered.path).await.is_ok());
    ana.sync.end_requests(ana.sync.device).await.unwrap();
    for path in [&offered.path, &handed.path] {
        let gone = group.storage.read(path).await;
        assert!(
            matches!(gone, Err(ref error) if error.failure() == StorageFailure::NotFound),
            "the sweep takes {path:?}"
        );
    }
    ana.database().close().await.unwrap();
}

/// A reset carries an open pairing into the new database ended, so the new
/// installation puts the marker where the request is, under the old
/// device's id.
#[tokio::test]
async fn a_reset_carries_the_ending_under_the_old_device_id() {
    for at in ALL {
        let group = Group::shared();
        let ana = group.device(1).await;
        group.create(&ana).await;
        let (pairing, _) = reach(&group, &ana, at).await;
        let new = group.newcomer(ana.identity.clone(), ana.storage.for_device()).await;
        new.sync.unlock().await.unwrap();
        new.sync.register_device("Replacement".into(), Some(ana.sync.device)).await.unwrap();
        let records = ana.database().operations().await.unwrap();
        new.sync
            .record_device_reset(
                DeviceReset { old_device: ana.sync.device, new_device: new.sync.device },
                &records,
            )
            .await
            .unwrap();
        let carried = work_of(&new, &pairing).await.expect("carried");
        assert_eq!(carried.device, ana.sync.device, "{at:?}");
        assert!(matches!(carried.state, PairingState::Forgetting), "{at:?}");
        new.sync.pass().await.unwrap();
        assert!(work_of(&new, &pairing).await.is_none(), "{at:?}");
        // The marker went in the place of the request or handover. The
        // replaced device's gone marker sweep follows in the same pass and
        // takes it with everything else under that device's requests.
        match group.storage.read(&pairing.path).await {
            Ok(found) => assert!(InviteEnd::decode(&found).is_ok(), "{at:?}: only the marker"),
            Err(error) => assert_eq!(error.failure(), StorageFailure::NotFound, "{at:?}"),
        }
        let gone = ObjectPath::device_gone(group.store, ana.sync.device);
        assert!(group.storage.read(&gone).await.is_ok(), "{at:?}: the gone marker");
        ana.database().close().await.unwrap();
        new.database().close().await.unwrap();
    }
}

/// A storage failure is the pairing's pending record, with no wait armed;
/// the next pass that succeeds ends it.
#[tokio::test]
async fn a_storage_failure_is_a_pending_record_that_the_next_success_ends() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let pairing = started(&ana).await;
    ana.storage
        .set_faults(Faults {
            read_failures: [(pairing.path.clone(), StorageFailure::Network)].into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    let (record, _) = rows(&ana).await.pop().unwrap();
    let pending = ana.database().operation_pending::<PendingRecord>(record.id).await.unwrap();
    let pending = pending.expect("the failure is recorded");
    assert!(pending.reason.offline(), "{:?}", pending.reason);
    assert_eq!(
        pending.subject,
        PendingSubject::Operation { id: record.id, kind: OperationKind::Pairing }
    );
    assert!(ana.database().retry_wait(RetrySubject::Operation(record.id)).await.unwrap().is_none());
    // The next pass looks again, with no wait, and its success ends the record.
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.pass().await.unwrap();
    assert!(ana.database().operation_pending::<PendingRecord>(record.id).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

/// A pairing claims no slot and records no provider access, from its start to
/// its end.
#[tokio::test]
async fn a_pairing_claims_nothing_and_records_no_provider_access() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let before = other_work(&ana).await;
    let (pairing, asked) = reach(&group, &ana, At::HandedOver).await;
    assert!(asked.is_some());
    for _ in 0..2 {
        assert!(ana.database().current_claim().await.unwrap().is_none());
        assert_eq!(other_work(&ana).await, before);
        ana.sync.end_pairing(pairing.id).await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    ana.database().close().await.unwrap();
}

/// The journal rows that are no pairing's: the claims of slots and the
/// provider access a pairing would leave.
async fn other_work(device: &Device) -> Vec<(String, u32)> {
    device
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.kind != operation_kind::PAIRING)
        .map(|record| (record.kind, record.last_step))
        .collect()
}

/// The expiry write ends a waiting pairing and reports the next pairing's
/// expiry as the time to wake.
#[tokio::test]
async fn the_expiry_write_covers_pairings_and_wakes_at_the_earliest() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let first = started(&ana).await;
    group.clock.advance(Duration::from_secs(60));
    let second = started(&ana).await;
    let now = ana.sync.clock.now();
    let next = crate::sequence_sync::sequence_invites::expiry::expire(
        ana.database(),
        now,
        &ana.sync.waits,
        &ana.sync.state,
    )
    .await
    .unwrap();
    let expires =
        |pairing: &Pairing| std::time::UNIX_EPOCH + Duration::from_secs(pairing.code.expires_at);
    assert_eq!(next, Some(expires(&first)), "the wake is the earliest pairing's expiry");
    group.clock.set(expires(&first));
    let next = crate::sequence_sync::sequence_invites::expiry::expire(
        ana.database(),
        expires(&first),
        &ana.sync.waits,
        &ana.sync.state,
    )
    .await
    .unwrap();
    assert!(matches!(
        state_of(&ana, &first).await,
        Some(PairingState::Ending { how: PairingEnd::Expired })
    ));
    assert!(matches!(state_of(&ana, &second).await, Some(PairingState::Waiting { .. })));
    assert_eq!(next, Some(expires(&second)));
    ana.database().close().await.unwrap();
}

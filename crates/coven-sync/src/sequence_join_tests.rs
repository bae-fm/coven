use crate::sequence_sync::tests::*;
use crate::{floor_object::FloorObject, JoinOutcome};
use coven_format::{admin::StoreChange, sealed_slot::SlotKind, MemberAccess};
use coven_foundation::clock::FixedClock;
use coven_merge::Audience;
use coven_storage::{test_utils::MemoryStorage, CloudProvider, ObjectPath, Storage};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn clocks(provider: SystemTime, deleting: SystemTime) -> Group {
    let mut group = Group::shared();
    group.clock.set(deleting);
    group.storage = MemoryStorage::builder()
        .provider(CloudProvider::Dropbox)
        .clock(Arc::new(FixedClock::new(provider)))
        .build()
        .unwrap();
    group
}

async fn waiting(group: &Group) -> (Device, Joiner) {
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invitation = invite(&ana).await;
    let carol = join(group, &invitation.code, "Carol's phone").await;
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Waiting
    );
    (ana, carol)
}

async fn floor(group: &Group, ana: &Device) -> FloorObject {
    FloorObject::read(
        ana.database(),
        &group.storage,
        &ObjectPath::floor(group.store),
        None,
        &mut group.ring.clone(),
        &ana.identity,
    )
    .await
    .unwrap()
    .unwrap()
}

async fn retained_prefix(group: &Group, ana: &Device) {
    while ana.database().applied_slot().await.unwrap() < 12 {
        ana.sync
            .administer(StoreChange::RenameStore {
                name: format!(
                    "Household {}",
                    ana.database().applied_slot().await.unwrap() + 1
                ),
            })
            .await
            .unwrap();
    }
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 4.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    // Every slot is past grace, so the pass also snapshots slot 13, the
    // announcement, before its floor write names that newer snapshot.
    assert_eq!(floor(group, ana).await.header.first.0, 14);
}

async fn approved_with_retention(
    provider: SystemTime,
    deleting: SystemTime,
    deleted_copy: bool,
    compare: bool,
) {
    let group = clocks(provider, deleting);
    let (ana, carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    retained_prefix(&group, &ana).await;
    let request = ana.sync.invites().await.unwrap().remove(0).request.unwrap();
    ana.sync.approve_join_request(&request).await.unwrap();
    let copies: Vec<_> = ana
        .sync
        .session
        .lock()
        .await
        .history
        .copies
        .iter()
        .filter_map(|copy| match &copy.prefix.kind {
            SlotKind::KeyCopy(route) if route.routing.held_by(&carol.device.identity) => {
                Some(copy.path.slot_number().unwrap().0)
            }
            _ => None,
        })
        .collect();
    assert!(!copies.is_empty());
    if deleted_copy {
        ana.sync.snapshot(Audience::Store).await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    let current = floor(&group, &ana).await;
    for copy in &copies {
        assert_eq!(*copy < current.header.first.0, deleted_copy);
        assert_eq!(
            group
                .storage
                .status(&ObjectPath::slot(group.store, (*copy).try_into().unwrap()))
                .await
                .unwrap()
                .is_none(),
            deleted_copy
        );
        assert_eq!(
            current
                .copies
                .iter()
                .any(|original| original.slot.0 == *copy),
            deleted_copy
        );
    }
    assert!(!carol
        .request
        .pending(carol.device.sync.storage.as_ref())
        .await
        .unwrap());
    let outcome = carol
        .device
        .sync
        .join_outcome(&carol.request)
        .await
        .unwrap();
    assert_eq!(outcome, JoinOutcome::Admitted);
    if compare {
        let input = serde_json::json!({"kind":"joinScan", "first":current.header.first.0,
            "head":ana.database().applied_slot().await.unwrap(), "captured_at":current.header.at,
            "copies":copies, "scanned":scanned, "binding":true, "pending":false});
        assert_eq!(
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            serde_json::json!(format!("{outcome:?}"))
        );
    }
    carol
        .device
        .sync
        .register_device("Carol's phone".into(), None)
        .await
        .unwrap();
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn provider_time_in_1970_does_not_decline_an_approved_joiner() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
    for deleted_copy in [false, true] {
        approved_with_retention(UNIX_EPOCH, now, deleted_copy, false).await;
    }
}

#[tokio::test]
async fn a_fast_deleting_clock_does_not_decline_an_approved_joiner() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
    let fast = now + crate::sequence_sync::sequence_retention::GRACE + Duration::from_secs(1);
    for deleted_copy in [false, true] {
        approved_with_retention(now, fast, deleted_copy, false).await;
    }
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn join_scan_retention_matches_lean() {
    let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
    for deleted_copy in [false, true] {
        approved_with_retention(UNIX_EPOCH, now, deleted_copy, true).await;
    }
    declined_with_retention(true).await;
}

async fn declined_with_retention(compare: bool) {
    let group = clocks(UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(1_790_000_000));
    let (ana, carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    retained_prefix(&group, &ana).await;
    let request = ana.sync.invites().await.unwrap().remove(0).request.unwrap();
    ana.sync.decline_join_request(&request).await.unwrap();
    // Read the deleted request before the next pass takes back provider access.
    let outcome = carol
        .device
        .sync
        .join_outcome(&carol.request)
        .await
        .unwrap();
    assert_eq!(outcome, JoinOutcome::Declined);
    if compare {
        let current = floor(&group, &ana).await;
        let input = serde_json::json!({"kind":"joinScan", "first":current.header.first.0,
            "head":ana.database().applied_slot().await.unwrap(), "captured_at":current.header.at,
            "copies":[], "scanned":scanned, "binding":false, "pending":false});
        assert_eq!(
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            serde_json::json!(format!("{outcome:?}"))
        );
    }
    ana.sync.pass().await.unwrap();
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_real_decline_still_reports_declined_after_retention() {
    declined_with_retention(false).await;
}

#[tokio::test]
async fn retention_between_the_miss_and_floor_read_cannot_end_an_approved_join() {
    let group = clocks(UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(1_790_000_000));
    let (ana, carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    let (missed, miss) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    carol
        .device
        .storage
        .hold_next_read(
            ObjectPath::slot(group.store, scanned.try_into().unwrap()),
            missed,
            held,
        )
        .await;
    let polling = async {
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap()
    };
    let retention = async {
        miss.await.unwrap();
        retained_prefix(&group, &ana).await;
        let request = ana.sync.invites().await.unwrap().remove(0).request.unwrap();
        ana.sync.approve_join_request(&request).await.unwrap();
        let (observed, floor_read) = tokio::sync::oneshot::channel();
        let (resume_floor, held_floor) = tokio::sync::oneshot::channel();
        carol
            .device
            .storage
            .hold_next_read(ObjectPath::floor(group.store), observed, held_floor)
            .await;
        resume.send(()).unwrap();
        floor_read.await.unwrap();
        // The first floor lacks the copy. Before that reply reaches the
        // joiner, a second floor captures it and deletes its sequence slot.
        ana.sync.snapshot(Audience::Store).await.unwrap();
        ana.sync.pass().await.unwrap();
        resume_floor.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(polling, retention);
    assert_eq!(outcome, JoinOutcome::Admitted);
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_scan_does_not_advance_past_the_keys_it_could_not_keep() {
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    ana.sync.pass().await.unwrap();
    let request = ana.sync.invites().await.unwrap().remove(0).request.unwrap();
    ana.sync.approve_join_request(&request).await.unwrap();
    let binding = ana.database().applied_slot().await.unwrap();
    carol
        .device
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: [(
                ObjectPath::slot(group.store, binding.try_into().unwrap()),
                coven_storage::StorageFailure::Network,
            )]
            .into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    assert!(
        matches!(carol.device.sync.join_outcome(&carol.request).await,
        Err(crate::SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network)
    );
    assert_eq!(carol.device.sync.session.lock().await.scanned, scanned);
    carol
        .device
        .storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_history_read_does_not_lose_the_scans_tentative_keys() {
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    ana.sync.pass().await.unwrap();
    let request = ana.sync.invites().await.unwrap().remove(0).request.unwrap();
    ana.sync.approve_join_request(&request).await.unwrap();
    let scanned = carol.device.sync.session.lock().await.scanned;
    let path = ObjectPath::floor(group.store);
    let (observed, floor_read) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    carol
        .device
        .storage
        .hold_next_read(path.clone(), observed, held)
        .await;
    let polling = carol.device.sync.join_outcome(&carol.request);
    let fail_history = async {
        floor_read.await.unwrap();
        // Bootstrap gets its floor response and finds the copy. The next
        // floor read fails before authenticated history retains those keys.
        carol
            .device
            .storage
            .set_faults(coven_storage::test_utils::Faults {
                read_failures: [(path, coven_storage::StorageFailure::Network)].into(),
                ..coven_storage::test_utils::Faults::none()
            })
            .await;
        resume.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(polling, fail_history);
    assert!(matches!(outcome, Err(crate::SyncError::Storage(error))
        if error.failure() == coven_storage::StorageFailure::Network));
    assert!(carol.device.sync.keys.read().unwrap().is_none());
    assert_eq!(carol.device.sync.session.lock().await.scanned, scanned);
    carol
        .device
        .storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// Asking to check now ends the join's own wait at once, keeping its failure
/// count, but not a provider cooldown, which the wait reports instead
/// (§13.2, §6.7).
#[tokio::test]
async fn check_now_ends_the_joins_wait_but_not_a_provider_cooldown() {
    use crate::NextCheck;
    use coven_database::RetrySubject;
    use coven_foundation::clock::Clock;
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let wait = || async {
        carol
            .device
            .database()
            .retry_wait(RetrySubject::Join)
            .await
            .unwrap()
            .unwrap()
    };
    // Polls that found the request waiting back the join off to 300 seconds.
    while sync.join_poll_waiting().await.is_ok() && wait().await.wait < Duration::from_secs(300) {}
    let failures = wait().await.failures;
    let now = group.clock.now();
    let (asking, mut asked) = tokio::sync::watch::channel(0u64);
    let reported = std::sync::Mutex::new(Vec::new());
    let mut due = Box::pin(sync.join_poll_due(&mut asked, |next| {
        reported.lock().unwrap().push(next);
    }));
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut due)
        .await
        .is_err());
    assert_eq!(
        *reported.lock().unwrap(),
        [NextCheck::At(now + Duration::from_secs(300))]
    );
    asking.send_modify(|asked| *asked += 1);
    tokio::time::timeout(Duration::from_secs(5), due)
        .await
        .unwrap()
        .unwrap();
    let ended = wait().await;
    assert_eq!((ended.failures, ended.wait), (failures, Duration::ZERO));

    // The next wait is 300 seconds again; a 600-second cooldown outlasts it.
    sync.join_poll_waiting().await.unwrap();
    coven_storage::Cooldown::record(sync.waits.as_ref(), Duration::from_secs(600))
        .await
        .unwrap();
    reported.lock().unwrap().clear();
    let mut due = Box::pin(sync.join_poll_due(&mut asked, |next| {
        reported.lock().unwrap().push(next);
    }));
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut due)
        .await
        .is_err());
    let cooling = NextCheck::CoolingDown(now + Duration::from_secs(600));
    assert_eq!(*reported.lock().unwrap(), [cooling]);
    asking.send_modify(|asked| *asked += 1);
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut due)
        .await
        .is_err());
    // The join's own wait ended; the cooldown still holds the check.
    assert_eq!(wait().await.wait, Duration::ZERO);
    assert_eq!(*reported.lock().unwrap(), [cooling, cooling]);
    group.clock.set(now + Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(5), due)
        .await
        .unwrap()
        .unwrap();
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// Only a sent invite's join has a confirmation number: Ana compares it
/// before approving. In person, she watched the phone that scanned.
#[tokio::test]
async fn only_a_sent_invites_join_has_a_confirmation_number() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let sent = invite(&ana).await;
    let in_person = ana
        .sync
        .create_invite(
            crate::InviteTarget::NewMember {
                role: coven_format::admin::MemberRole::Member,
                name: "Dan".into(),
            },
            crate::InviteAccess::ProviderAccount {
                email: "dan@example.org".into(),
            },
            crate::InviteDelivery::InPerson,
        )
        .await
        .unwrap();
    let carol = join(&group, &sent.code, "Carol's phone").await;
    let (_temp, directory) = group.directory();
    let dan = crate::JoiningIdentity::prepare(
        &directory,
        &crate::read_invite_code(&in_person.code).unwrap(),
        "Dan's phone",
    )
    .unwrap();
    assert_eq!(
        carol.request.confirmation(),
        Some(coven_crypto::ConfirmationNumber::of(
            sent.id,
            &carol.request.identity().keys().public_keys()
        ))
    );
    assert_eq!(dan.confirmation(), None);
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

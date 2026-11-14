use crate::sequence_sync::tests::*;
use crate::{floor_object::FloorObject, ApprovalWaits, JoinOutcome, JoinPoll};
use coven_format::admin::StoreChange;
use coven_foundation::clock::FixedClock;
use coven_foundation::clock::HANG_GUARD;
use coven_merge::Audience;
use coven_storage::{test_utils::MemoryStorage, CloudProvider, ObjectPath, Storage};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// The approval waits of `sync`'s installation.
fn approval(sync: &crate::SequenceSync) -> ApprovalWaits {
    ApprovalWaits::new(sync.database.clone(), sync.waits.clone(), sync.clock.clone())
}

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

async fn waiting(group: &Group) -> (Device, JoiningDevice) {
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invitation = invite(&ana).await;
    let mut carol = join(group, invitation.shown_code(), "Carol's phone").await;
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
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
                name: format!("Household {}", ana.database().applied_slot().await.unwrap() + 1),
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
    assert_eq!(floor(group, ana).await.header.first.get(), 14);
}

async fn approved_with_retention(
    provider: SystemTime,
    deleting: SystemTime,
    deleted_copy: bool,
    compare: bool,
) {
    let group = clocks(provider, deleting);
    let (ana, mut carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    retained_prefix(&group, &ana).await;
    let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
    if !deleted_copy {
        // The pass that approves also retains every slot past grace; by the
        // provider's time none is, so Carol's copies stay in the sequence.
        group.clock.set(provider);
    }
    ana.sync.approve_join_request(&request).await.unwrap();
    group.clock.set(deleting);
    let copies: Vec<_> = ana
        .sync
        .session
        .lock()
        .await
        .history
        .copies
        .iter()
        .filter(|copy| {
            copy.route().audience == Audience::Store && copy.route().held_by(&carol.device.identity)
        })
        .map(|copy| copy.path.slot_number().unwrap().get())
        .collect();
    assert!(!copies.is_empty());
    if deleted_copy {
        ana.sync.snapshot(Audience::Store).await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    let current = floor(&group, &ana).await;
    for copy in &copies {
        assert_eq!(*copy < current.header.first.get(), deleted_copy);
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
            current.copies.iter().any(|original| original.slot.get() == *copy),
            deleted_copy
        );
    }
    assert!(!carol.request.pending(carol.device.sync.storage.as_ref()).await.unwrap());
    let outcome = carol.device.sync.join_outcome(&mut carol.request).await.unwrap();
    assert_eq!(outcome, JoinOutcome::Admitted);
    if compare {
        let input = serde_json::json!({"kind":"joinScan", "first":current.header.first.get(),
            "head":ana.database().applied_slot().await.unwrap(), "captured_at":current.header.at,
            "copies":copies, "scanned":scanned.get(), "pending":false,
            "reach":true});
        assert_eq!(
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            serde_json::json!(format!("{outcome:?}"))
        );
    }
    // The admission registered Carol's phone; it registers nothing itself.
    carol.device.sync.pass().await.unwrap();
    let state = carol.device.database().store_state().await.unwrap();
    assert!(state.devices.contains_key(&carol.device.sync.device));
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
    let (ana, mut carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    retained_prefix(&group, &ana).await;
    let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
    ana.sync.decline_join_request(&request).await.unwrap();
    // Read the deleted request before the next pass takes back provider access.
    let outcome = carol.device.sync.join_outcome(&mut carol.request).await.unwrap();
    assert_eq!(outcome, JoinOutcome::Declined);
    if compare {
        let current = floor(&group, &ana).await;
        let input = serde_json::json!({"kind":"joinScan", "first":current.header.first.get(),
            "head":ana.database().applied_slot().await.unwrap(), "captured_at":current.header.at,
            "copies":[], "scanned":scanned.get(), "pending":false,
            "reach":true});
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

/// A waiting join's next check meets `failure` reading the path `at` names:
/// it could not reach the shared location, so it ends as declined, as the
/// model's `outcome` with no reach (`JoinScan.lost_access_ends`).
async fn losing_access(
    group: &Group,
    at: impl FnOnce(&Group, coven_merge::Slot, &JoiningDevice) -> ObjectPath,
    failure: coven_storage::StorageFailure,
    compare: bool,
) {
    let (ana, mut carol) = waiting(group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    let current = floor(group, &ana).await;
    group
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: [(at(group, scanned, &carol), failure)].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let outcome = carol.device.sync.join_outcome(&mut carol.request).await.unwrap();
    assert_eq!(outcome, JoinOutcome::Declined);
    if compare {
        let input = serde_json::json!({"kind":"joinScan", "first":current.header.first.get(),
            "head":ana.database().applied_slot().await.unwrap(), "captured_at":current.header.at,
            "copies":[], "scanned":scanned.get(), "pending":true, "reach":false});
        assert_eq!(
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            serde_json::json!(format!("{outcome:?}"))
        );
    }
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_check_that_cannot_read_the_request_marker_or_slots_declines() {
    use coven_storage::StorageFailure::{ContainerNotFound, PermissionDenied};
    let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
    losing_access(
        &clocks(UNIX_EPOCH, now),
        |_, _, carol| carol.request.path().clone(),
        PermissionDenied,
        false,
    )
    .await;
    let group = clocks(UNIX_EPOCH, now);
    losing_access(
        &group,
        |group, _, carol| ObjectPath::device_gone(group.store, carol.request.approver()),
        ContainerNotFound,
        false,
    )
    .await;
    let group = clocks(UNIX_EPOCH, now);
    losing_access(
        &group,
        |group, scanned, _| ObjectPath::slot(group.store, scanned.into()),
        PermissionDenied,
        false,
    )
    .await;
}

#[tokio::test]
async fn a_rejected_oauth_sign_in_is_no_lost_access() {
    use coven_storage::StorageFailure::Authentication;
    // An OAuth sign-in the provider rejects is an error to sign in again for.
    let group = clocks(UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(1_790_000_000));
    let (ana, mut carol) = waiting(&group).await;
    group
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: [(carol.request.path().clone(), Authentication)].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(carol.device.sync.join_outcome(&mut carol.request).await.is_err());
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn join_scan_lost_access_matches_lean() {
    losing_access(
        &clocks(UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(1_790_000_000)),
        |_, _, carol| carol.request.path().clone(),
        coven_storage::StorageFailure::PermissionDenied,
        true,
    )
    .await;
}

#[tokio::test]
async fn a_real_decline_still_reports_declined_after_retention() {
    declined_with_retention(false).await;
}

#[tokio::test]
async fn retention_between_the_miss_and_floor_read_cannot_end_an_approved_join() {
    let group = clocks(UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(1_790_000_000));
    let (ana, mut carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    let (missed, miss) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    carol
        .device
        .storage
        .hold_next_read(ObjectPath::slot(group.store, scanned.into()), missed, held)
        .await;
    let polling = async { carol.device.sync.join_outcome(&mut carol.request).await.unwrap() };
    let retention = async {
        miss.await.unwrap();
        retained_prefix(&group, &ana).await;
        let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
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

/// Approval can land just after the read that missed it. Once the request is
/// gone, the joining device reads forward again before concluding it was
/// declined or expired (§13.2).
#[tokio::test]
async fn a_joiner_whose_request_disappears_reads_forward_before_concluding() {
    let group = clocks(UNIX_EPOCH, UNIX_EPOCH);
    let (ana, mut carol) = waiting(&group).await;
    let scanned = carol.device.sync.session.lock().await.scanned;
    let (missed, miss) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    carol
        .device
        .storage
        .hold_next_read(ObjectPath::slot(group.store, scanned.into()), missed, held)
        .await;
    let polling = async { carol.device.sync.join_outcome(&mut carol.request).await.unwrap() };
    let approval = async {
        miss.await.unwrap();
        ana.sync.pass().await.unwrap();
        let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
        ana.sync.approve_join_request(&request).await.unwrap();
        resume.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(polling, approval);
    assert_eq!(outcome, JoinOutcome::Admitted);
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_scan_does_not_advance_past_the_keys_it_could_not_keep() {
    let group = Group::shared();
    let (ana, mut carol) = waiting(&group).await;
    ana.sync.pass().await.unwrap();
    let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
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
    assert!(matches!(carol.device.sync.join_outcome(&mut carol.request).await,
        Err(crate::SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network));
    assert_eq!(carol.device.sync.session.lock().await.scanned, scanned);
    carol.device.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// The store a join published loads in its first pass. A failed read there
/// keeps the keys the join's scan opened: the next pass loads with them,
/// scanning for nothing (§13.2).
#[tokio::test]
async fn a_failed_first_load_keeps_the_scans_keys() {
    let group = Group::shared();
    let (ana, mut carol) = waiting(&group).await;
    ana.sync.pass().await.unwrap();
    let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
    ana.sync.approve_join_request(&request).await.unwrap();
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    assert!(carol.device.sync.keys.read().unwrap().is_some());
    carol
        .device
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: [(
                ObjectPath::floor(group.store),
                coven_storage::StorageFailure::Network,
            )]
            .into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(matches!(carol.device.sync.pass().await, Err(crate::SyncError::Storage(error))
        if error.failure() == coven_storage::StorageFailure::Network));
    assert!(carol.device.sync.keys.read().unwrap().is_some());
    carol.device.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    carol.device.sync.pass().await.unwrap();
    let state = carol.device.database().store_state().await.unwrap();
    assert!(state.devices.contains_key(&carol.device.sync.device));
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// While a joiner's request waits, reading the routing of later slots for its
/// copy applies none of them and gives it no key or authority (§13.2, format.md
/// D13).
#[tokio::test]
async fn a_routing_scan_records_no_applied_slot() {
    let group = Group::shared();
    let (ana, mut carol) = waiting(&group).await;
    let before = carol.device.sync.session.lock().await.scanned;
    for name in ["Family", "Home"] {
        ana.sync.administer(StoreChange::RenameStore { name: name.into() }).await.unwrap();
    }
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Waiting
    );
    let scanned = carol.device.sync.session.lock().await.scanned;
    assert!(scanned > before, "the scan read the slots written while the request waited");
    assert_eq!(carol.device.database().applied_slot().await.unwrap(), 0);
    assert!(carol.device.sync.keys.read().unwrap().is_none());
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

type Due<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), crate::SyncError>> + 'a>>;

/// The poll parks, with everything the test did before seen and no check
/// due: it has not finished.
async fn stays_waiting(due: &mut Due<'_>, parks: &crate::Parks) {
    tokio::select! {
        biased;
        ended = due.as_mut() => panic!("the poll came due: {ended:?}"),
        () = parks.parked_from_now() => (),
    }
}

/// A join that polls found waiting until its backoff is 300 seconds: the
/// time its last check ended, and the failure count it carries.
async fn backed_off(group: &Group, carol: &JoiningDevice) -> (std::time::SystemTime, u32) {
    use coven_database::RetrySubject;
    use coven_foundation::clock::Clock;
    let sync = &carol.device.sync;
    let wait =
        || async { carol.device.database().retry_wait(RetrySubject::Join).await.unwrap().unwrap() };
    while approval(sync).waiting().await.is_ok() && wait().await.wait < Duration::from_secs(300) {}
    (group.clock.now(), wait().await.failures)
}

async fn close_both(ana: &Device, carol: &JoiningDevice) {
    for device in [ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// While a guard is held, a waiting join checks within its interval of the
/// end of its last check, however long the backoff; the check keeps the
/// failure count, so the doubling continues (§13.2).
#[tokio::test]
async fn a_held_interval_brings_a_backed_off_joins_check_forward() {
    use coven_database::RetrySubject;
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, failures) = backed_off(&group, &carol).await;
    let often = crate::SyncOften::new();
    let _guard = often.hold(Duration::from_secs(5));
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    let mut due: Due<'_> = Box::pin(poll.due());
    group.clock.set(last + Duration::from_secs(4));
    stays_waiting(&mut due, &parks).await;
    group.clock.set(last + Duration::from_secs(5));
    tokio::time::timeout(HANG_GUARD, due).await.unwrap().unwrap();
    let ended = carol.device.database().retry_wait(RetrySubject::Join).await.unwrap();
    assert!(ended.is_none_or(|ended| (ended.failures, ended.wait) == (failures, Duration::ZERO)));
    // The check found the request waiting: the doubling goes on from there.
    approval(sync).waiting().await.unwrap();
    let next = carol.device.database().retry_wait(RetrySubject::Join).await.unwrap().unwrap();
    assert_eq!((next.failures, next.wait), (failures + 1, Duration::from_secs(300)));
    close_both(&ana, &carol).await;
}

/// Dropping the last guard returns the check to the backoff's deadline,
/// with the failure count as it was (§13.2).
#[tokio::test]
async fn dropping_the_guard_returns_the_check_to_the_backoff() {
    use coven_database::RetrySubject;
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, failures) = backed_off(&group, &carol).await;
    let often = crate::SyncOften::new();
    let guard = often.hold(Duration::from_secs(5));
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    let mut due: Due<'_> = Box::pin(poll.due());
    stays_waiting(&mut due, &parks).await;
    drop(guard);
    // The wait takes the drop in before the clock moves.
    stays_waiting(&mut due, &parks).await;
    group.clock.set(last + Duration::from_secs(299));
    stays_waiting(&mut due, &parks).await;
    let kept = carol.device.database().retry_wait(RetrySubject::Join).await.unwrap().unwrap();
    assert_eq!(kept.failures, failures);
    group.clock.set(last + Duration::from_secs(300));
    tokio::time::timeout(HANG_GUARD, due).await.unwrap().unwrap();
    close_both(&ana, &carol).await;
}

/// A guard taken after its interval has passed since the last check checks
/// at once (§13.2).
#[tokio::test]
async fn a_guard_taken_after_its_interval_checks_at_once() {
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, _) = backed_off(&group, &carol).await;
    group.clock.set(last + Duration::from_secs(20));
    let often = crate::SyncOften::new();
    let _guard = often.hold(Duration::from_secs(5));
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    tokio::time::timeout(HANG_GUARD, poll.due()).await.unwrap().unwrap();
    close_both(&ana, &carol).await;
}

/// Taking a guard while the join waits shortens that wait (§13.2).
#[tokio::test]
async fn a_guard_taken_during_the_wait_shortens_it() {
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, _) = backed_off(&group, &carol).await;
    let often = crate::SyncOften::new();
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    let mut due: Due<'_> = Box::pin(poll.due());
    group.clock.set(last + Duration::from_secs(6));
    stays_waiting(&mut due, &parks).await;
    let _guard = often.hold(Duration::from_secs(5));
    tokio::time::timeout(HANG_GUARD, due).await.unwrap().unwrap();
    close_both(&ana, &carol).await;
}

/// With several guards held, the shortest interval decides (§13.2).
#[tokio::test]
async fn the_shortest_of_several_guards_decides_the_joins_check() {
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, _) = backed_off(&group, &carol).await;
    let often = crate::SyncOften::new();
    let _long = often.hold(Duration::from_secs(30));
    let _short = often.hold(Duration::from_secs(5));
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    let mut due: Due<'_> = Box::pin(poll.due());
    group.clock.set(last + Duration::from_secs(4));
    stays_waiting(&mut due, &parks).await;
    group.clock.set(last + Duration::from_secs(5));
    tokio::time::timeout(HANG_GUARD, due).await.unwrap().unwrap();
    close_both(&ana, &carol).await;
}

/// A provider cooldown still holds the check a guard would make, and the
/// join's own wait it ended stays ended (§13.2, §6.7).
#[tokio::test]
async fn a_provider_cooldown_holds_the_check_a_guard_would_make() {
    use coven_database::RetrySubject;
    let group = Group::shared();
    let (ana, carol) = waiting(&group).await;
    let sync = &carol.device.sync;
    let (last, _) = backed_off(&group, &carol).await;
    coven_storage::Cooldown::record(sync.waits.as_ref(), Duration::from_secs(600)).await.unwrap();
    let often = crate::SyncOften::new();
    let _guard = often.hold(Duration::from_secs(5));
    let parks = crate::Parks::new();
    let mut poll = JoinPoll::new(approval(sync), often.subscribe(), parks.clone());
    let mut due: Due<'_> = Box::pin(poll.due());
    group.clock.set(last + Duration::from_secs(5));
    stays_waiting(&mut due, &parks).await;
    group.clock.set(last + Duration::from_secs(599));
    stays_waiting(&mut due, &parks).await;
    let wait = carol.device.database().retry_wait(RetrySubject::Join).await.unwrap();
    assert!(wait.is_none_or(|wait| wait.wait.is_zero()));
    group.clock.set(last + Duration::from_secs(600));
    tokio::time::timeout(HANG_GUARD, due).await.unwrap().unwrap();
    close_both(&ana, &carol).await;
}

/// Carol's phone shows the number Ana's phone shows with her request: Ana
/// compares the two before approving (§13.2).
#[tokio::test]
async fn a_joins_confirmation_number_is_the_one_the_inviting_device_shows() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = listed(&ana).await[0].shown_request().cloned().expect("Carol's request");
    assert_eq!(carol.request.confirmation(), request.confirmation);
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// A joiner admitted soon after a snapshot starts from it, with no slot
/// deleted and no grace passed: the floor names the snapshot at once and
/// carries the joiner's copy, so the joiner reads only the slots after the
/// snapshot's coverage (§16.4).
#[tokio::test]
async fn an_admitted_joiner_starts_from_a_fresh_snapshot_without_waiting_for_grace() {
    let group = Group::shared();
    let (ana, mut carol) = waiting(&group).await;
    for (id, title) in [("a", "one"), ("b", "two"), ("c", "three")] {
        insert(ana.database(), id, title).await;
        ana.sync.pass().await.unwrap();
    }
    let request = listed(&ana).await.remove(0).shown_request().cloned().unwrap();
    ana.sync.approve_join_request(&request).await.unwrap();
    let announced = ana.sync.snapshot(Audience::Store).await.unwrap();
    let covers = ana.sync.session.lock().await.history.announcements[&announced].covers;
    insert(ana.database(), "d", "after the snapshot").await;
    ana.sync.pass().await.unwrap();
    let current = floor(&group, &ana).await;
    assert_eq!(current.header.first.get(), 1, "nothing is deleted yet");
    assert_eq!(current.header.snapshots, [announced]);
    let before = carol.device.storage.request_trace().len();
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    // The store the join publishes loads in its first pass.
    carol.device.sync.pass().await.unwrap();
    assert_eq!(rows(carol.device.database()).await, rows(ana.database()).await);
    let read: Vec<_> = carol.device.storage.request_trace()[before..]
        .iter()
        .filter_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Read(path, _) => path.slot_number(),
            _ => None,
        })
        .collect();
    assert!(
        read.iter().all(|slot| slot.get() > covers),
        "read {read:?} at or below coverage {covers}"
    );
    for device in [&ana, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// A store a join published on copies whose admission its history then
/// refused: the keys open every slot, and no slot binds the pair the device
/// holds. Read to the end of the sequence, the store stops for good with
/// `NotAdmitted`, as a removal stops a device, and every pass after finds it
/// again; syncing on, it would send nothing and show nothing wrong (§13.2,
/// §14.1).
#[tokio::test]
async fn a_store_whose_history_binds_its_pair_nowhere_stops_not_admitted() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let carol = group.device(3).await;
    for _ in 0..2 {
        assert!(matches!(
            carol.sync.pass().await,
            Err(crate::SyncError::Removed(coven_database::Removal::NeverAdmitted))
        ));
        assert!(matches!(
            &*carol.sync.state.subscribe().borrow(),
            crate::SequenceStatus::Stopped(crate::SyncFailure::Removed(
                coven_database::Removal::NeverAdmitted
            ))
        ));
    }
    for device in [&ana, &carol] {
        device.database().close().await.unwrap();
    }
}

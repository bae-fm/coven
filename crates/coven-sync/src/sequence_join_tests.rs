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
    assert_eq!(floor(group, ana).await.header.first.0, 13);
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
    let request = ana.sync.join_requests().await.unwrap().remove(0);
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
    let request = ana.sync.join_requests().await.unwrap().remove(0);
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
        let request = ana.sync.join_requests().await.unwrap().remove(0);
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
    let request = ana.sync.join_requests().await.unwrap().remove(0);
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
    let request = ana.sync.join_requests().await.unwrap().remove(0);
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

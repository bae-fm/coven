use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::tests::*;
use coven_storage::{
    test_utils::{Faults, MemoryRequest},
    ObjectPath, Storage,
};

#[tokio::test]
async fn independent_sealings_of_the_same_floor_differ_and_both_authenticate() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    let session = ana.sync.session.lock().await;
    let mut sealed = Vec::new();
    for _ in 0..2 {
        let proposal = ana.sync.propose_floor(&session, &group.ring).await.unwrap().unwrap();
        let super::Retention::Capture(capture) = &proposal else {
            panic!("expected a floor proposal");
        };
        let record = ana
            .database()
            .start_operation(coven_database::NewOperation {
                kind: "sequence-retention".into(),
                data: serde_json::to_vec(&proposal).unwrap(),
                started_by: "coven".into(),
                pauses_writes: false,
            })
            .await
            .unwrap();
        let artifact = ana.sync.capture_floor(capture, &group.ring, &ana.identity).await.unwrap();
        sealed.push(
            ana.sync
                .artifact_file(&capture.name)
                .unwrap()
                .open_reader()
                .unwrap()
                .read_at(0, artifact.size as usize)
                .unwrap(),
        );
        ana.sync.cleanup_floor(&artifact).await.unwrap();
        ana.database().finish_operation(record).await.unwrap();
    }
    assert_ne!(sealed[0], sealed[1]);
    let path = ObjectPath::floor(group.store);
    let mut previous: Option<crate::floor_object::FloorObject> = None;
    let mut ring = group.ring.clone();
    for bytes in sealed {
        match &previous {
            Some(floor) => group.storage.replace(floor.object(), &bytes).await.unwrap(),
            None => {
                group.storage.create(&path, &bytes).await.unwrap();
            }
        }
        let floor = crate::floor_object::FloorObject::read(
            ana.database(),
            &group.storage,
            &path,
            None,
            &mut ring,
            &ana.identity,
        )
        .await
        .unwrap()
        .unwrap();
        floor.authenticate(&session.history).unwrap();
        if let Some(previous) = &previous {
            assert_eq!(floor.header, previous.header);
            assert_eq!(floor.announcements, previous.announcements);
            assert_eq!(floor.copies, previous.copies);
        }
        previous = Some(floor);
    }
    drop(session);
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn retention_does_not_wait_for_an_absent_device_and_returning_loads_the_floor() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "published", "kept by snapshot").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    assert!(group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_some());
    insert(&ben.sync.database, "offline", "original queue").await;
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    assert_eq!(rows(&ben.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_lost_floor_reply_deletes_nothing_until_the_floor_is_observed() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "published", "retained").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    // Retention alone, so the lost reply is the floor's: a whole pass would
    // first take a snapshot of the slots grace has now passed.
    let mut session = ana.sync.session.lock().await;
    ana.sync.retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity).await.unwrap();
    drop(session);
    assert!(group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_some());
    let path = ObjectPath::slot(group.store, covered.try_into().unwrap());
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|record| record.kind == "sequence-retention"));
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(5),
    );
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}

/// Ana's floor write lands but its reply is lost. Before her retention
/// resumes, Ben announces a newer snapshot and writes a floor naming it, past
/// the slots Ana has applied. Ana reads forward to that floor's `at` before
/// checking it, accepts it, and its `first` authorizes her deletions (§16.4,
/// §16.6).
#[tokio::test]
async fn a_lost_floor_reply_accepts_a_newer_floor_written_past_the_applied_slots() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "published", "retained").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    // Retention alone, so the lost reply is the floor's.
    let mut session = ana.sync.session.lock().await;
    ana.sync.retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity).await.unwrap();
    drop(session);
    ana.storage.set_faults(Faults::none()).await;
    let path = ObjectPath::slot(group.store, covered.try_into().unwrap());
    assert!(group.storage.status(&path).await.unwrap().is_some());
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    assert!(
        crate::sequence_sync::sequence_reload::tests::stored_floor(&ben).await.header.at > applied
    );
    // Retention alone again, once its retry is due, as in a pass whose read
    // forward came before Ben's floor.
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(5),
    );
    let mut session = ana.sync.session.lock().await;
    ana.sync.retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity).await.unwrap();
    drop(session);
    let pending = ana.sync.database.pending::<crate::PendingRecord>().await.unwrap();
    assert!(pending.is_empty(), "{pending:?}");
    assert!(group.storage.status(&path).await.unwrap().is_none());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's floor write lands but its reply is lost. Before her retention
/// resumes, Ben writes past her slots, snapshots them and, once that snapshot
/// has passed grace, writes a floor whose first kept slot has passed the slot
/// after Ana's, deleting the slots between. Ana's confirm needs none of them:
/// it starts from that floor's store snapshot, checks the floor against the
/// history there, and deletes below its first kept slot (§16.6,
/// `RetentionConfirm.landed_confirms`, `FloorCheck.check_from_floor`).
#[tokio::test]
async fn a_lost_floor_reply_confirms_a_floor_moved_past_the_applied_slots() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "published", "retained").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let grace = crate::sequence_sync::sequence_retention::GRACE;
    let second = std::time::Duration::from_secs(1);
    group.clock.set(std::time::UNIX_EPOCH + grace + second);
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    // Retention alone, so the lost reply is the floor's.
    let mut session = ana.sync.session.lock().await;
    ana.sync.retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity).await.unwrap();
    drop(session);
    ana.storage.set_faults(Faults::none()).await;
    let own = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    let applied = ana.sync.database.applied_slot().await.unwrap();
    let path = ObjectPath::slot(group.store, covered.try_into().unwrap());
    assert!(group.storage.status(&path).await.unwrap().is_some());

    ben.sync.pass().await.unwrap();
    for id in ["after", "her floor"] {
        insert(&ben.sync.database, id, "written by ben").await;
        ben.sync.pass().await.unwrap();
    }
    ben.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + grace * 2 + second * 3);
    ben.sync.pass().await.unwrap();
    let moved = crate::sequence_sync::sequence_reload::tests::stored_floor(&ben).await;
    assert_ne!(moved.object(), own.object());
    assert!(moved.header.first.get() > applied + 1, "{:?} after {applied}", moved.header);
    let next = ObjectPath::slot(group.store, (applied + 1).try_into().unwrap());
    assert!(group.storage.status(&next).await.unwrap().is_none(), "Ben deleted the slots between");

    // Retention alone again, as in a pass whose read forward came before
    // Ben's floor.
    let mut session = ana.sync.session.lock().await;
    ana.sync.retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity).await.unwrap();
    drop(session);
    let pending = ana.sync.database.pending::<crate::PendingRecord>().await.unwrap();
    assert!(pending.is_empty(), "{pending:?}");
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    for slot in 1..moved.header.first.get() {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        assert!(group.storage.status(&path).await.unwrap().is_none(), "slot {slot} kept");
    }
    assert!(ana.sync.database.applied_slot().await.unwrap() >= moved.header.first.get() - 1);
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    assert_eq!(rows(&ana.sync.database).await.len(), 3);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The floor a quiet pass reads back unchanged was authenticated when it was
/// first read; checking it again decodes its copies and announcements.
#[tokio::test]
async fn a_quiet_pass_under_an_unchanged_floor_decodes_nothing() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "kept", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_some());
    crate::sequence_sync::tests::assert_idle_work(&ana).await;
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn superseded_snapshots_wait_for_replacement_grace_and_floor_release() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let first = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let old = ana.sync.session.lock().await.history.announcements[&first].snapshot.clone();
    let path = crate::snapshot_object::path(group.store, &old).unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(
        group.storage.status(&path).await.unwrap().is_none(),
        "superseded snapshot survives floor release"
    );
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2);
    ana.sync.database.close().await.unwrap();
}

// §16.6 and E6: failed superseded-body deletion remains journaled and retryable.
#[tokio::test]
async fn a_failed_snapshot_deletion_resumes_its_recorded_body_after_reopening() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let first = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let old = ana.sync.session.lock().await.history.announcements[&first].snapshot.clone();
    let path = crate::snapshot_object::path(group.store, &old).unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage
        .set_faults(Faults {
            delete_failures: [(path.clone(), coven_storage::StorageFailure::PermissionDenied)]
                .into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let records = ana.database().operations().await.unwrap();
    let operation = records
        .iter()
        .find(|record| record.kind == "sequence-retention")
        .expect("failed snapshot deletion must retain its journal")
        .id;
    let pending =
        ana.database().operation_pending::<crate::PendingRecord>(operation).await.unwrap().unwrap();
    assert!(matches!(
        pending.reason,
        crate::PendingReason::Storage {
            failure: coven_storage::StorageFailure::PermissionDenied,
            ..
        }
    ));
    assert!(ana.storage.status(&path).await.unwrap().is_some());
    let ana = group.reopen(ana).await;
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(ana.storage.status(&path).await.unwrap().is_none());
    assert!(ana.database().snapshot_absent(old).await.unwrap());
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    assert!(ana
        .database()
        .operation_pending::<crate::PendingRecord>(operation)
        .await
        .unwrap()
        .is_none());
    ana.database().close().await.unwrap();
}

/// OneDrive can refuse every writer of one floor race while the floor stays
/// unchanged. The refusal is the pass's ordinary conflict outcome: it rereads
/// the floor, deletes nothing, and a later pass publishes the floor again.
#[tokio::test]
async fn a_refused_floor_replacement_with_the_floor_unchanged_advances_on_a_later_pass() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let floor_path = ObjectPath::floor(group.store);
    let grace = |extra| {
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(extra)
    };
    insert(&ana.sync.database, "first", "under the first floor").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(grace(1));
    ana.sync.pass().await.unwrap();
    let first_floor = group.storage.status(&floor_path).await.unwrap().unwrap();

    insert(&ana.sync.database, "second", "under the second floor").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    let covered = ObjectPath::slot(group.store, covered.try_into().unwrap());
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(grace(2) + crate::sequence_sync::sequence_retention::GRACE);
    ana.storage.set_faults(Faults { refuse_replacements: 1, ..Faults::none() }).await;
    let traced = ana.storage.request_trace().len();
    let status = ana.sync.pass().await.unwrap();
    assert!(matches!(status, crate::SequenceStatus::Synced { .. }));
    let trace = ana.storage.request_trace().split_off(traced);
    let refused = trace
        .iter()
        .position(|request| *request == MemoryRequest::Other("finish_upload"))
        .expect("the pass attempts the floor replacement");
    assert!(
        trace[refused..]
            .iter()
            .any(|request| *request == MemoryRequest::Read(floor_path.clone(), None)),
        "the refusal rereads the floor"
    );
    assert_eq!(group.storage.status(&floor_path).await.unwrap().unwrap(), first_floor);
    assert_eq!(
        ana.sync.session.lock().await.floor.as_ref().map(|floor| floor.object().clone()),
        Some(first_floor.clone())
    );
    assert!(group.storage.status(&covered).await.unwrap().is_some());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());

    // The refused pass also published a growth snapshot; the next floor
    // names it once it passes grace.
    group.clock.set(
        grace(3)
            + crate::sequence_sync::sequence_retention::GRACE
            + crate::sequence_sync::sequence_retention::GRACE,
    );
    ana.sync.pass().await.unwrap();
    let second_floor = group.storage.status(&floor_path).await.unwrap().unwrap();
    assert_ne!(second_floor, first_floor);
    assert!(group.storage.status(&covered).await.unwrap().is_none());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
}

/// Each floor write deletes only the slots between the previous floor's
/// `first` and its own. The slots below the previous `first` were that
/// floor's deletions; sending them again would grow with every floor.
#[tokio::test]
async fn a_floor_advance_deletes_only_the_slots_the_previous_floor_kept() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut firsts = vec![1];
    for (index, id) in ["first", "second"].into_iter().enumerate() {
        insert(ana.database(), id, "retained in the snapshot").await;
        ana.sync.pass().await.unwrap();
        let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        let first = ana.sync.session.lock().await.history.announcements[&announced].covers + 1;
        group.clock.set(
            std::time::UNIX_EPOCH
                + (super::GRACE + std::time::Duration::from_secs(1)) * (index as u32 + 1),
        );
        let traced = ana.storage.request_trace().len();
        ana.sync.pass().await.unwrap();
        let deleted: Vec<_> = ana
            .storage
            .request_trace()
            .split_off(traced)
            .into_iter()
            .filter_map(|request| match request {
                MemoryRequest::Delete(path) => path.slot_number(),
                _ => None,
            })
            .map(|slot| slot.get())
            .collect();
        let previous = *firsts.last().unwrap();
        assert_eq!(deleted, (previous..first).collect::<Vec<_>>());
        firsts.push(first);
    }
    assert!(firsts[2] > firsts[1] && firsts[1] > 1);
    ana.database().close().await.unwrap();
}

/// Storage refuses to delete slot 1. The floor that moved `first` past it
/// keeps that deletion journaled and reports it, and the next floor doesn't
/// wait for it: it moves `first` on and deletes the slots it adds. Once
/// storage allows it, retrying the kept deletion removes slot 1 (§16.6).
#[tokio::test]
async fn a_failed_slot_deletion_stays_journaled_and_the_next_floor_does_not_wait() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let refused = ObjectPath::slot(group.store, 1.try_into().unwrap());
    ana.storage
        .set_faults(Faults {
            delete_failures: [(refused.clone(), coven_storage::StorageFailure::PermissionDenied)]
                .into(),
            ..Faults::none()
        })
        .await;
    let mut firsts = Vec::new();
    for (index, id) in ["first", "second"].into_iter().enumerate() {
        insert(ana.database(), id, "retained in the snapshot").await;
        ana.sync.pass().await.unwrap();
        ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        group.clock.set(
            std::time::UNIX_EPOCH
                + (super::GRACE + std::time::Duration::from_secs(1)) * (index as u32 + 1),
        );
        ana.sync.pass().await.unwrap();
        firsts.push(stored_floor(&ana).await.header.first.get());
    }
    assert!(firsts[1] > firsts[0] && firsts[0] > 1);
    let retention: Vec<_> = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.kind == "sequence-retention")
        .collect();
    let [kept] = retention.as_slice() else {
        panic!("only the failed deletion stays journaled: {retention:?}");
    };
    let pending =
        ana.database().operation_pending::<crate::PendingRecord>(kept.id).await.unwrap().unwrap();
    assert!(
        matches!(
            pending.reason,
            crate::PendingReason::Storage {
                failure: coven_storage::StorageFailure::PermissionDenied,
                ..
            }
        ),
        "{pending:?}"
    );
    assert!(group.storage.status(&refused).await.unwrap().is_some());
    for slot in firsts[0]..firsts[1] {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        assert!(group.storage.status(&path).await.unwrap().is_none(), "slot {slot}");
    }
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(kept.id).await.unwrap();
    for slot in 1..firsts[1] {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        assert!(group.storage.status(&path).await.unwrap().is_none(), "slot {slot}");
    }
    assert!(ana.database().operation(kept.id).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

async fn idle_circle_coverage(compare: bool) {
    use coven_merge::Audience;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    let store = ana.sync.snapshot(Audience::Store).await.unwrap();
    let mut coverage =
        vec![Some(ana.sync.session.lock().await.history.announcements[&store].covers), None];
    for covered in [false, true] {
        if covered {
            let slot = ana.sync.snapshot(Audience::Circle(circle)).await.unwrap();
            coverage[1] = Some(ana.sync.session.lock().await.history.announcements[&slot].covers);
        }
        group.clock.set(
            std::time::UNIX_EPOCH
                + super::GRACE * if covered { 2 } else { 1 }
                + std::time::Duration::from_secs(if covered { 2 } else { 1 }),
        );
        let session = ana.sync.session.lock().await;
        let proposal = ana.sync.propose_floor(&session, &group.ring).await.unwrap();
        let first = proposal.map(|proposal| match proposal {
            super::Retention::Capture(capture) => capture.first.get(),
            _ => panic!("expected floor"),
        });
        assert_eq!(first.is_some(), covered, "an idle introduced circle still needs coverage");
        if compare {
            let input = serde_json::json!({"kind":"floor","coverage":coverage});
            assert_eq!(
                serde_json::json!(first),
                crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input)
            );
        }
    }
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn idle_circle_requires_floor_coverage() {
    idle_circle_coverage(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_audience_coverage_matches_lean() {
    idle_circle_coverage(true).await;
}

async fn daily_snapshots(compare: bool) {
    use coven_merge::Audience;
    use serde_json::json;
    use std::time::{Duration, UNIX_EPOCH};
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut published = Vec::new();
    let day_seconds = 24 * 60 * 60;
    for day in 0..14usize {
        let announced_at = day as u64 * day_seconds;
        let now = announced_at + 1;
        group.clock.set(UNIX_EPOCH + Duration::from_secs(announced_at));
        let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
        group.clock.set(UNIX_EPOCH + Duration::from_secs(now));
        let mut session = ana.sync.session.lock().await;
        let announcement = session.history.announcements[&slot].clone();
        published.push((slot, announcement.covers, announcement.snapshot));
        let current = session.floor.as_ref().map_or(1, |floor| floor.header.first.get());
        let proposal = ana.sync.propose_floor(&session, &group.ring).await.unwrap();
        let Some(super::Retention::Capture(capture)) = proposal else {
            panic!("day {day}: a new snapshot must be named at once");
        };
        // The floor names today's snapshot; `first` follows the one announced
        // seven days ago, the newest past grace.
        assert_eq!(capture.announcements, vec![slot], "day {day}");
        let expected = if day >= 7 { published[day - 7].1 + 1 } else { 1 };
        assert_eq!(capture.first.get(), expected, "day {day}");
        if compare {
            let mut snapshots = Vec::new();
            for (slot, covers, _) in published.iter().rev() {
                let object = ana
                    .sync
                    .immutable_metadata(&ObjectPath::slot(
                        group.store,
                        slot.get().try_into().unwrap(),
                    ))
                    .await
                    .unwrap();
                let latest = object.stored_at.elapsed_since_epoch() + object.time_resolution;
                snapshots.push(
                    json!({"slot":slot.get(),"covers":covers,"announced_at":latest.as_nanos(),
                    "usable":session.history.usable(&session.history.announcements[slot])}),
                );
            }
            let input = json!({"kind":"retention","snapshots":snapshots,"first":current,
                "grace":super::GRACE.as_nanos(),"time":Duration::from_secs(now).as_nanos()});
            assert_eq!(
                crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
                json!({"named":{"slot":slot.get(),"covers":announcement.covers},"first":capture.first.get()})
            );
        }
        let mut ring = group.ring.clone();
        ana.sync.retain_sequence(&mut session, &mut ring, &ana.identity).await.unwrap();
        ana.sync.retain_snapshots(&mut session, &mut ring, &ana.identity).await.unwrap();
        if day >= 7 {
            let covered = published[day - 7].1;
            assert!(group
                .storage
                .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
                .await
                .unwrap()
                .is_none());
        }
        for (index, (_, _, snapshot)) in published.iter().enumerate() {
            let body = crate::snapshot_object::path(group.store, snapshot).unwrap();
            assert_eq!(
                group.storage.status(&body).await.unwrap().is_some(),
                index + 7 >= day,
                "day {day}, snapshot {index}: keep bodies until a replacement passes grace"
            );
        }
    }
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn daily_snapshots_are_named_at_once_and_advance_first_after_grace() {
    daily_snapshots(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_daily_retention_matches_lean() {
    daily_snapshots(true).await;
}

/// Ana's phone snapshots the store, and the phone's next pass writes a floor
/// naming it. Ben's phone then snapshots the store, and grace passes before
/// anyone writes a floor again. Ana's laptop removes her phone, which is now
/// gone for good, so the laptop deletes the phone's snapshots for it (§16.6).
/// Ben's snapshot has passed grace and covers more, yet the phone's stays
/// while the floor names it. Once the laptop's next floor names Ben's, the
/// phone's goes.
#[tokio::test]
async fn a_gone_devices_snapshot_the_floor_names_stays_until_the_floor_moves_on() {
    use coven_merge::Audience;
    let group = Group::new();
    let phone = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&phone).await;
    group.admit(&phone, &ben).await;
    let laptop = group.laptop(&phone).await;
    let announced = phone.sync.snapshot(Audience::Store).await.unwrap();
    phone.sync.pass().await.unwrap();
    let floor = crate::floor_object::FloorObject::read(
        phone.database(),
        &group.storage,
        &ObjectPath::floor(group.store),
        None,
        &mut group.ring.clone(),
        &phone.identity,
    )
    .await
    .unwrap()
    .expect("the phone's pass writes a floor");
    assert_eq!(floor.header.snapshots, [announced]);
    let id = phone.sync.session.lock().await.history.announcements[&announced].snapshot.clone();
    let body = crate::snapshot_object::path(group.store, &id).unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    laptop
        .sync
        .administer(coven_format::admin::StoreChange::RemoveDevice { device: phone.sync.device })
        .await
        .unwrap();
    // Snapshot deletion alone, so the floor still names the phone's: a whole
    // pass would first write a floor naming Ben's.
    let mut session = laptop.sync.session.lock().await;
    assert!(session.history.gone_for_good(phone.sync.device).is_some());
    laptop
        .sync
        .retain_snapshots(&mut session, &mut group.ring.clone(), &laptop.identity)
        .await
        .unwrap();
    drop(session);
    assert!(group.storage.status(&body).await.unwrap().is_some(), "the floor names it");
    laptop.sync.pass().await.unwrap();
    assert!(
        group.storage.status(&body).await.unwrap().is_none(),
        "a floor naming Ben's snapshot releases the gone phone's"
    );
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// Ana's phone uploaded a store snapshot it never announced, as a raise it
/// was preparing when it was removed leaves one: no announcement can name it
/// now, and the phone will never run the preparation's undo. Ana's laptop
/// removes the phone. Ben's member is neither the phone's nor the store
/// owner, so his phone leaves the phone's snapshots alone. The laptop is
/// responsible for the phone's files, so it lists the phone's snapshot
/// folders once and deletes the body; the phone's announced snapshot, which
/// the floor names, stays (§16.6).
#[tokio::test]
async fn a_gone_devices_unannounced_snapshot_is_deleted_by_the_device_responsible_for_it() {
    use coven_merge::Audience;
    let group = Group::new();
    let phone = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&phone).await;
    group.admit(&phone, &ben).await;
    let laptop = group.laptop(&phone).await;
    let announced = phone.sync.snapshot(Audience::Store).await.unwrap();
    phone.sync.pass().await.unwrap();
    let id = phone.sync.session.lock().await.history.announcements[&announced].snapshot.clone();
    let named = crate::snapshot_object::path(group.store, &id).unwrap();
    let abandoned = ObjectPath::snapshot(
        group.store,
        Audience::Store,
        phone.sync.device,
        std::num::NonZeroU64::new(id.number + 1).unwrap(),
    );
    group.storage.plant(&abandoned, b"a preparation's body").await.unwrap();
    laptop
        .sync
        .administer(coven_format::admin::StoreChange::RemoveDevice { device: phone.sync.device })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert!(group.storage.status(&abandoned).await.unwrap().is_some(), "not Ben's to delete");
    laptop.sync.pass().await.unwrap();
    assert!(group.storage.status(&abandoned).await.unwrap().is_none());
    assert!(group.storage.status(&named).await.unwrap().is_some(), "the floor names it");
    let folder = coven_storage::ObjectPrefix::device_snapshots(
        group.store,
        Audience::Store,
        phone.sync.device,
    );
    let listings = |device: &Device| {
        device
            .storage
            .request_trace()
            .iter()
            .filter(|request| matches!(request, MemoryRequest::List(listed) if *listed == folder))
            .count()
    };
    assert_eq!(listings(&ben), 0);
    assert_eq!(listings(&laptop), 1);
    laptop.sync.pass().await.unwrap();
    assert_eq!(listings(&laptop), 1, "the laptop lists the gone phone's folders once");
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// A claim's session left at a slot the sequence has filled can never
/// publish, whoever began it, and nothing would ever end it. The pass that
/// writes a floor ends every session at a slot it has applied; one at a slot
/// past them stays (§16.6, §17.10).
#[tokio::test]
async fn a_floor_write_ends_the_sessions_left_at_applied_slots() {
    use coven_merge::Audience;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let through = ana.sync.session.lock().await.through;
    let slot = |number| ObjectPath::slot(group.store, std::num::NonZeroU64::new(number).unwrap());
    let other = group.storage.with_device(coven_foundation::id_source::DeviceId(99));
    // A claim racing Ana's next one, at a slot the sequence fills next.
    other.started_upload(&slot(through + 1)).await.unwrap();
    other.started_upload(&slot(through + 5)).await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.session.lock().await.through > through);
    assert!(
        group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_some(),
        "the pass writes a floor"
    );
    let left: Vec<_> =
        group.storage.unfinished_uploads().await.into_iter().map(|(_, path)| path).collect();
    assert_eq!(left, [slot(through + 5)]);
    ana.database().close().await.unwrap();
}

/// Another device began writing the floor and stopped before it finished.
/// Once Ana's floor is published, that session observed an older floor and
/// can never publish, so the pass that wrote Ana's floor ends it (§17.10).
#[tokio::test]
async fn a_floor_write_ends_the_floor_sessions_it_superseded() {
    use coven_merge::Audience;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let other = group.storage.with_device(coven_foundation::id_source::DeviceId(99));
    other.started_upload(&ObjectPath::floor(group.store)).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(10));
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&ObjectPath::floor(group.store)).await.unwrap().is_some());
    assert!(group.storage.unfinished_uploads().await.is_empty());
    ana.database().close().await.unwrap();
}

/// §16.6: a floor write journaled while Ana's older snapshot was the newest
/// lands only after her newer one has passed grace. The floor it publishes
/// names the older body, so the pass that lands it keeps that body: a new
/// device starts from every body the stored floor names.
#[tokio::test]
async fn a_floor_landing_after_a_newer_snapshot_passed_grace_keeps_the_bodies_it_names() {
    use coven_merge::Audience;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let floor = ObjectPath::floor(group.store);
    insert(ana.database(), "older", "in the snapshot the floor names").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.storage.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
    ana.sync.pass().await.unwrap();
    ana.storage.set_faults(Faults::none()).await;
    assert!(group.storage.status(&floor).await.unwrap().is_none(), "the floor write waits");

    insert(ana.database(), "newer", "in the snapshot that replaces it").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    assert!(group.storage.status(&floor).await.unwrap().is_none(), "the floor write waits");
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();

    let named = stored_floor(&ana).await.header.snapshots;
    assert!(!named.is_empty());
    let history = ana.sync.session.lock().await.history.clone();
    for slot in named {
        let path =
            crate::snapshot_object::path(group.store, &history.announcements[&slot].snapshot)
                .unwrap();
        assert!(group.storage.status(&path).await.unwrap().is_some(), "the floor names {path:?}");
    }
    ana.database().close().await.unwrap();
}

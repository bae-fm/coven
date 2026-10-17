use crate::sequence_sync::tests::*;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

#[tokio::test]
async fn independent_sealings_of_the_same_floor_differ_and_both_authenticate() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    group
        .clock
        .set(std::time::UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    let session = ana.sync.session.lock().await;
    let mut sealed = Vec::new();
    for _ in 0..2 {
        let proposal = ana
            .sync
            .propose_floor(&session, &group.ring, &ana.identity)
            .await
            .unwrap()
            .unwrap();
        let super::Retention::Floor { bytes, .. } = proposal else {
            panic!("expected a floor proposal");
        };
        sealed.push(bytes);
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
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
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
    assert!(group
        .storage
        .status(&ObjectPath::floor(group.store))
        .await
        .unwrap()
        .is_some());
    insert(&ben.sync.database, "offline", "original queue").await;
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
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
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::floor(group.store))
        .await
        .unwrap()
        .is_some());
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

#[tokio::test]
async fn superseded_snapshots_wait_for_replacement_grace_and_floor_release() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let first = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    let old = ana.sync.session.lock().await.history.announcements[&first]
        .snapshot
        .clone();
    let path = crate::snapshot_object::path(group.store, &old).unwrap();
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
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

// §15 and E6: failed superseded-body deletion remains journaled and retryable.
#[tokio::test]
async fn a_failed_snapshot_deletion_resumes_its_recorded_body_after_reopening() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let first = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    let old = ana.sync.session.lock().await.history.announcements[&first]
        .snapshot
        .clone();
    let path = crate::snapshot_object::path(group.store, &old).unwrap();
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage
        .set_faults(Faults {
            delete_failures: [(
                path.clone(),
                coven_storage::StorageFailure::PermissionDenied,
            )]
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
    let pending = ana
        .database()
        .operation_pending::<crate::PendingRecord>(operation)
        .await
        .unwrap()
        .unwrap();
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

use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

#[tokio::test]
async fn a_lost_create_reply_settles_exact_bytes_without_a_second_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "one").await;
    ana.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(matches!(ana.sync.pass().await, Err(SyncError::Storage(_))));
    assert!(ana.sync.database.current_claim().await.unwrap().is_some());
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), 3);
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 4.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn administrative_contention_reuses_the_claim_number() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let counter = ana.sync.database.claim_counter().await.unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let first = ana.sync.administer(StoreChange::SetAccess {
        member: ana.identity.member(),
        access: access("ana-new"),
    });
    let second = async {
        observed.await.unwrap();
        ben.sync
            .administer(StoreChange::SetAccess {
                member: ben.identity.member(),
                access: access("ben-new"),
            })
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(first, second);
    result.unwrap();
    assert_eq!(
        ana.sync.database.claim_counter().await.unwrap(),
        counter + 1
    );
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_stale_create_below_the_floor_is_removed_and_reclaimed_above_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "ana", "waiting").await;
    let stale = ana.sync.database.applied_slot().await.unwrap() + 1;
    let path = ObjectPath::slot(group.store, stale.try_into().unwrap());
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let retention = async {
        observed.await.unwrap();
        insert(&ben.sync.database, "ben", "first claimant").await;
        ben.sync.pass().await.unwrap();
        ben.sync
            .snapshot(coven_merge::Audience::Store)
            .await
            .unwrap();
        group.clock.set(
            std::time::UNIX_EPOCH
                + crate::sequence_sync::sequence_retention::GRACE
                + std::time::Duration::from_secs(1),
        );
        ben.sync.pass().await.unwrap();
        assert!(group.storage.status(&path).await.unwrap().is_none());
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), retention);
    result.unwrap();
    assert!(
        group.storage.status(&path).await.unwrap().is_none(),
        "a settled stale attempt must not leave recreated bytes below the floor"
    );
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn contending_writes_slots_apply_the_winner_and_claim_the_next_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let contested = ana.sync.database.applied_slot().await.unwrap() + 1;
    let counter = ana.sync.database.claim_counter().await.unwrap();
    insert(&ana.sync.database, "ana", "second").await;
    insert(&ben.sync.database, "ben", "first").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let ben_first = async {
        observed.await.unwrap();
        ben.sync.pass().await.unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), ben_first);
    result.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        contested + 1
    );
    assert_eq!(
        ana.sync.database.claim_counter().await.unwrap(),
        counter + 1,
        "contention keeps the claim number"
    );
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    ben.sync.pass().await.unwrap();
    let rows_ben = rows(&ben.sync.database).await;
    assert_eq!(rows(&ana.sync.database).await, rows_ben);
    assert_eq!(
        rows_ben,
        [
            ("ana".into(), "second".into()),
            ("ben".into(), "first".into())
        ]
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn equal_claim_numbers_from_two_copies_at_one_slot_are_not_contention() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let (ana, copy) = group.copy(ana).await;
    insert(&ana.sync.database, "a", "original").await;
    insert(&copy.sync.database, "b", "copy").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let copy_first = async {
        observed.await.unwrap();
        copy.sync.pass().await.unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(ana.sync.pass(), copy_first);
    assert!(
        matches!(result, Err(SyncError::DeviceIdInUse(_))),
        "{result:?}"
    );
    assert!(matches!(
        &*ana.sync.state.subscribe().borrow(),
        SyncStatus::Stopped(SyncFailure::DeviceIdInUse(_))
    ));
    for device in [&ana, &copy] {
        device.sync.database.close().await.unwrap();
    }
}

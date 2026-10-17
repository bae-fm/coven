use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::{ObjectPath, Storage};

#[tokio::test]
async fn a_restored_device_opens_its_copy_loads_the_sequence_and_registers() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "before restore").await;
    ana.sync.pass().await.unwrap();
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    assert!(laptop.sync.keys.read().unwrap().is_none());
    assert!(matches!(
        laptop.sync.pass().await.unwrap(),
        SequenceStatus::Synced { through: 3 }
    ));
    assert_eq!(
        rows(&laptop.sync.database).await,
        rows(&ana.sync.database).await
    );
    laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await
        .unwrap();
    insert(&laptop.sync.database, "b", "from the laptop").await;
    laptop.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&laptop.sync.database).await
    );
    assert!(laptop.sync.keys.read().unwrap().is_some());
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_restored_device_opens_the_floor_copies_when_old_slots_are_deleted() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "covered by the snapshot").await;
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
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(
        rows(&laptop.sync.database).await,
        [("a".into(), "covered by the snapshot".into())]
    );
    laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        laptop.sync.database.applied_slot().await.unwrap()
    );
    for device in [&ana, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_old_restore_code_cannot_register_after_its_keys_were_replaced() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let replacement = coven_format::test_utils::keys_from_seed(9).public_keys();
    ben.sync
        .administer(StoreChange::ReplaceMemberKeys {
            member: ana.identity.member(),
            expected: ana.identity.keys().public_keys(),
            keys: replacement,
        })
        .await
        .unwrap();
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    let count = laptop.storage.request_count();
    let result = laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await;
    assert!(
        matches!(result, Err(SyncError::MemberKeysReplaced { member }) if member == ana.identity.member()),
        "{result:?}"
    );
    assert!(laptop
        .sync
        .database
        .current_claim()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        laptop.storage.request_count() - count,
        2,
        "only the next-slot read and the floor read"
    );
    for device in [&ana, &ben, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_newcomer_without_a_copy_reports_the_key_it_lacks() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let stranger = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(77)),
        coven_format::test_utils::keys_from_seed(5),
    );
    let device = group.newcomer(stranger, group.storage.for_device()).await;
    let key = group.ring.store_key_ids().next().unwrap();
    assert!(
        matches!(device.sync.pass().await, Err(SyncError::KeyUnavailable(missing)) if missing == key)
    );
    assert_eq!(device.sync.database.applied_slot().await.unwrap(), 0);
    for device in [&ana, &device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Custody that lost its store keys after this installation applied history
/// opens them again from this member's copies, then keeps them (§11).
#[tokio::test]
async fn an_installation_whose_custody_lost_its_keys_unlocks_from_its_copies() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "kept").await;
    ana.sync.pass().await.unwrap();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    ana.sync.database.close().await.unwrap();
    let identity = ana.identity.clone();
    let locked = group
        .open_with(ana._temp, identity, None, group.storage.for_device())
        .await;
    assert!(locked.sync.keys.read().unwrap().is_none());
    locked.sync.unlock().await.unwrap();
    assert!(locked.sync.keys.read().unwrap().is_some());
    assert_eq!(locked.sync.database.applied_slot().await.unwrap(), applied);
    insert(&locked.sync.database, "b", "after unlocking").await;
    locked.sync.pass().await.unwrap();
    assert_eq!(
        locked.sync.database.applied_slot().await.unwrap(),
        applied + 1
    );
    locked.sync.database.close().await.unwrap();
}

use crate::sequence_sync::tests::*;
use crate::*;

/// E9 and §12.2: replacement supplies every store key and only held circle keys.
#[tokio::test]
async fn an_admin_outside_the_targets_circle_can_replace_its_pair() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ben.sync.create_circle("Ben's".into()).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync
            .keys
            .read()
            .unwrap()
            .unwrap()
            .circle_key_ids(circle)
            .count(),
        0
    );
    let keys = coven_format::test_utils::keys_from_seed(12).public_keys();
    ana.sync
        .replace_member_keys_for(ben.identity.member(), keys)
        .await
        .unwrap();
    let state = ana.database().store_state().await.unwrap();
    assert_eq!(state.members[&ben.identity.member()].keys, keys);
    assert!(state.reads_circle(circle, &ben.identity.member()));
    assert!(!state.reads_circle(circle, &ana.identity.member()));
    assert!(ana.database().operations().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_self_replacement_sends_queued_writes_first_and_retires_other_holders_of_the_old_pair() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let laptop = group
        .newcomer(ana.identity.clone(), group.storage.for_device())
        .await;
    laptop.sync.pass().await.unwrap();
    laptop
        .sync
        .register_device("Ana's laptop".into(), None)
        .await
        .unwrap();
    insert(&ana.sync.database, "queued", "under the old pair").await;
    let old = ana.identity.keys().public_keys();
    let installed = ana.sync.replace_member_keys().await.unwrap();
    assert_ne!(installed.keys().public_keys(), old);
    assert!(
        installed.holds(&old),
        "the retired pair stays held for copies naming it"
    );
    assert!(
        ana.sync.database.operations().await.unwrap().is_empty(),
        "the pause is released"
    );
    assert_eq!(
        ana.sync.database.store_state().await.unwrap().members[&ana.identity.member()].keys,
        installed.keys().public_keys()
    );
    insert(&ana.sync.database, "after", "under the new pair").await;
    // The old pair's copies expose the store key: this pass rotates it,
    // and the write waits for the replacement key (§11).
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(record.reason, PendingReason::KeyUnavailable { .. })));
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ben.sync.database).await,
        rows(&ana.sync.database).await
    );
    assert_eq!(rows(&ben.sync.database).await.len(), 2);
    let result = laptop.sync.pass().await;
    assert!(
        matches!(result, Err(SyncError::MemberKeysReplaced { member }) if member == ana.identity.member()),
        "{result:?}"
    );
    for device in [&ana, &ben, &laptop] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_admin_publishes_a_prepared_pair_with_copies_for_its_member() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let replacement = coven_crypto::MemberIdentity::new(
        ben.identity.member(),
        coven_format::test_utils::keys_from_seed(12),
    );
    ana.sync
        .replace_member_keys_for(ben.identity.member(), replacement.keys().public_keys())
        .await
        .unwrap();
    assert!(matches!(
        ana.sync
            .replace_member_keys_for(ana.identity.member(), replacement.keys().public_keys())
            .await,
        Err(SyncError::Rejected(OperationRejection::NotAllowed))
    ));
    let phone = group
        .newcomer(replacement.clone(), group.storage.for_device())
        .await;
    phone.sync.pass().await.unwrap();
    phone
        .sync
        .register_device("Ben's new phone".into(), None)
        .await
        .unwrap();
    assert!(matches!(
        ben.sync.pass().await,
        Err(SyncError::MemberKeysReplaced { .. })
    ));
    for device in [&ana, &ben, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

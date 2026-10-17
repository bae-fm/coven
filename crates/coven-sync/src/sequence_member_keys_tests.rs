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
    let (codes, _) = codes(
        &ana,
        coven_crypto::custody::Keychain::in_memory("self-replacement").unwrap(),
    );
    codes.replace_member_keys().await.unwrap();
    let installed = ana.sync.member.read().unwrap().unwrap();
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

use coven_crypto::custody::Keychain;
use coven_crypto::SecretBytes;

// §18.1 and E9: identity installation is unfinished until code custody commits.
#[tokio::test]
async fn a_failed_code_refresh_keeps_the_replacement_paused_and_resumes_after_reopening() {
    let group = Group::new();
    let mut ana = group.device(1).await;
    group.create(&ana).await;
    let keychain = Keychain::in_memory("replacement-code").unwrap();
    let (owner, store) = codes(&ana, keychain.clone());
    let old = owner.restore_code().await.unwrap();
    store
        .set_synced_restore_code(&SecretBytes::new(b"old code".to_vec()))
        .unwrap();
    keychain.fail_operations_after(&[0]);
    let result = owner.replace_member_keys().await;
    assert!(
        matches!(result, Err(SyncError::SecureStorage(_))),
        "{result:?}"
    );
    let current = ana.sync.member.read().unwrap().unwrap();
    assert_ne!(
        current.keys().public_keys(),
        ana.identity.keys().public_keys()
    );
    let operation = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "member-keys")
        .expect("refresh remains in the self-replacement journal")
        .id;
    assert!(matches!(
        ana.sync
            .database
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('held','until the code commits')",
                    [],
                )?;
                Ok(())
            })
            .await,
        Err(coven_database::DbError::MemberKeysChanging)
    ));
    assert_eq!(
        store.synced_restore_code().unwrap().unwrap().as_bytes(),
        b"old code"
    );
    owner.close().await;
    ana.identity = current;
    let ana = group.reopen(ana).await;
    let (owner, store) = codes(&ana, keychain);
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(120));
    // Custody failures on app operations wait for the app's retry (E6).
    ana.sync.retry_pending_operation(operation).await.unwrap();
    run_background(&ana, owner.clone()).await;
    let code = coven_format::codes::RestoreCode::from_bytes(
        store.synced_restore_code().unwrap().unwrap().as_bytes(),
    )
    .unwrap();
    assert_eq!(
        code.member_keys.public_keys(),
        ana.identity.keys().public_keys()
    );
    assert_ne!(owner.restore_code().await.unwrap(), old);
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    insert(&ana.sync.database, "after", "code committed").await;
    owner.close().await;
    ana.sync.database.close().await.unwrap();
}

// §18.1: completing a binding in a background pass also commits its restore code.
#[tokio::test]
async fn a_background_replacement_refreshes_the_code_before_releasing_writes() {
    let group = Group::new();
    let mut ana = group.device(1).await;
    group.create(&ana).await;
    let keychain = Keychain::in_memory("background-replacement-code").unwrap();
    let (owner, store) = codes(&ana, keychain.clone());
    store
        .set_synced_restore_code(&SecretBytes::new(b"old code".to_vec()))
        .unwrap();
    let next = ana.database().applied_slot().await.unwrap() + 1;
    ana.storage
        .set_faults(coven_storage::test_utils::Faults {
            create_failures: [(
                coven_storage::ObjectPath::slot(group.store, next.try_into().unwrap()),
                (coven_storage::StorageFailure::Network, 500),
            )]
            .into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(matches!(
        owner.replace_member_keys().await,
        Err(SyncError::Storage(_))
    ));
    ana.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    owner.close().await;
    ana.identity = ana.sync.member.read().unwrap().unwrap();
    let ana = group.reopen(ana).await;
    let (owner, store) = codes(&ana, keychain);
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(120));
    run_background(&ana, owner.clone()).await;
    let code = coven_format::codes::RestoreCode::from_bytes(
        store.synced_restore_code().unwrap().unwrap().as_bytes(),
    )
    .unwrap();
    assert_eq!(
        code.member_keys.public_keys(),
        ana.sync
            .member
            .read()
            .unwrap()
            .unwrap()
            .keys()
            .public_keys()
    );
    assert!(ana.database().operations().await.unwrap().is_empty());
    insert(ana.database(), "after", "code committed in background").await;
    owner.close().await;
    ana.database().close().await.unwrap();
}

async fn run_background(ana: &Device, owner: RestoreCodes) {
    let mut status = ana.sync.state.subscribe_sync();
    let control = SyncLoop::new(ana.sync.clone(), owner);
    control.start().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if matches!(*status.borrow_and_update(), SyncStatus::Synced { .. }) {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    control.close().await.unwrap();
}

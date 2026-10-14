//! An admin desktop replacing a member's S3 key (§13).
use super::*;
use crate::store_log_sync::tests::{access_keys, device, key, member, store, Device};
use coven_format::store_log::MemberRole;
use coven_storage::{test_utils::MemoryKeyAdmin, Storage};

async fn begin(d: &mut Device, command: crate::operations::Command) -> OperationId {
    match d.sync.begin_operation_call(command).await.unwrap() {
        Begun::Operation(id) => id,
        _ => panic!("operation expected"),
    }
}

async fn finish(d: &mut Device, id: OperationId) -> Result<Output, SyncError> {
    loop {
        match d.step(id).await? {
            Progress::Finished(value) | Progress::Reply(Ok(value)) => return Ok(value),
            Progress::Reply(Err(error)) => return Err(error),
            Progress::Advanced => (),
            Progress::Waiting(reason) => panic!("unexpected wait: {reason:?}"),
        }
    }
}

async fn replace(
    d: &mut Device,
    target: &MemberId,
    admin: &MemoryKeyAdmin,
) -> Result<AccessKeyReplacement, SyncError> {
    let id = begin(
        d,
        crate::operations::Command::ReplaceAccessKey(*target, Arc::new(admin.clone())),
    )
    .await;
    match finish(d, id).await? {
        Output::AccessKeyReplaced(value) => Ok(value),
        _ => panic!("replacement result"),
    }
}

/// Ana creates the store; Ben joins as a member with his device.
async fn household() -> (
    Arc<coven_storage::test_utils::MemoryStorage>,
    Device,
    Device,
) {
    let storage = Arc::new(
        coven_storage::test_utils::MemoryStorage::builder()
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    b.sync().await;
    b.sync.load_new_device("Ben", None).await.unwrap();
    a.sync().await;
    (storage, a, b)
}

async fn minted_user(a: &Device, target: &MemberId) -> String {
    let state = a.log().await.replay.state;
    match &state.members[target].access {
        MemberAccess::S3AccessKey {
            principal: S3Principal::IamUser { user, .. },
            ..
        } => user.clone(),
        other => panic!("minted access: {other:?}"),
    }
}

#[tokio::test]
async fn an_admin_makes_delivers_and_records_a_members_new_key_then_deletes_the_old() {
    let (storage, mut a, mut b) = household().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let first = replace(&mut a, &ben, &admin).await.unwrap();
    assert_eq!(first.key.credentials.access_key_id, "minted-1");
    // Ben's previous key was made in the console: only the console deletes it.
    let OldKeyDeletion::Console { .. } = first.old_key else {
        panic!("console key: {:?}", first.old_key)
    };
    let state = a.log().await.replay.state;
    assert_eq!(
        state.members[&ben].access,
        MemberAccess::S3AccessKey {
            access_key_id: "minted-1".into(),
            principal: first.key.principal.clone(),
        }
    );
    // The replaced key is pending from the recording transaction.
    assert!(access_keys(&a.db)
        .await
        .iter()
        .any(|key| key.access_key_id == "fixture-access-key" && key.member.as_ref() == Some(&ben)));
    // Ben's devices find the delivery, signed by the admin who recorded it.
    b.sync().await;
    let delivered = b.sync.delivered_access_key(None).await.unwrap().unwrap();
    assert_eq!(delivered.access_key_id, "minted-1");
    assert_eq!(
        delivered.secret_access_key.as_str(),
        first.key.credentials.secret_access_key.as_str()
    );
    assert!(a.sync.delivered_access_key(None).await.unwrap().is_none());
    // A device already holding the recorded key reads nothing.
    assert!(b
        .sync
        .delivered_access_key(Some("minted-1"))
        .await
        .unwrap()
        .is_none());
    // A second replacement deletes the coven-made key and reads it gone.
    let second = replace(&mut a, &ben, &admin).await.unwrap();
    assert!(matches!(second.old_key, OldKeyDeletion::Deleted));
    assert_eq!(admin.users(), [minted_user(&a, &ben).await]);
    assert!(!access_keys(&a.db)
        .await
        .iter()
        .any(|key| key.access_key_id == "minted-1"));
    let listed = storage
        .list(&ObjectPrefix::access_keys(store(1), &ben))
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    b.sync().await;
    assert_eq!(
        b.sync
            .delivered_access_key(Some("minted-1"))
            .await
            .unwrap()
            .unwrap()
            .access_key_id,
        "minted-2"
    );
}

#[tokio::test]
async fn a_member_cannot_replace_another_members_key() {
    let (_storage, mut a, mut b) = household().await;
    let admin = MemoryKeyAdmin::default();
    assert!(matches!(
        b.sync
            .begin_operation_call(crate::operations::Command::ReplaceAccessKey(
                a.member.member(),
                Arc::new(admin.clone()),
            ))
            .await,
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    assert!(admin.calls().is_empty());
    // A member may replace their own key with admin access.
    let own = b.member.member();
    let replaced = replace(&mut b, &own, &admin).await.unwrap();
    assert_eq!(replaced.key.credentials.access_key_id, "minted-1");
    a.sync().await;
    assert_eq!(
        a.log().await.replay.state.members[&own].access,
        MemberAccess::S3AccessKey {
            access_key_id: "minted-1".into(),
            principal: replaced.key.principal.clone(),
        }
    );
}

#[tokio::test]
async fn a_restart_needs_admin_access_again_and_a_failed_deletion_keeps_the_record() {
    let (_storage, mut a, b) = household().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let first = replace(&mut a, &ben, &admin).await.unwrap();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    a.restart(a.storage.clone()).await;
    assert!(matches!(
        a.step(id).await.unwrap(),
        Progress::Waiting(PendingReason::AdminAccessRequired)
    ));
    a.sync.hold_admin_access(id, Arc::new(admin.clone()));
    admin.fail_next_delete();
    let Output::AccessKeyReplaced(second) = finish(&mut a, id).await.unwrap() else {
        panic!("replacement result")
    };
    let OldKeyDeletion::Failed { operation, .. } = second.old_key else {
        panic!("failed deletion: {:?}", second.old_key)
    };
    let held = first.key.credentials.access_key_id;
    assert!(access_keys(&a.db)
        .await
        .iter()
        .any(|key| key.access_key_id == held));
    a.sync.delete_access_key(operation, &admin).await.unwrap();
    assert!(!access_keys(&a.db)
        .await
        .iter()
        .any(|key| key.access_key_id == held));
}

#[tokio::test]
async fn a_replacement_lost_to_removal_is_reported_and_its_key_stays_pending() {
    let (storage, mut a, b) = household().await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.add(&c.member, MemberRole::Admin).await;
    c.sync().await;
    c.sync.load_new_device("Carol", None).await.unwrap();
    a.sync().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    // Make, check and deliver the key, and reserve the entry.
    while a.operation(id).await.last_step < 3 {
        assert!(matches!(a.step(id).await.unwrap(), Progress::Advanced));
    }
    c.sync
        .make_and_upload_entry(StoreChange::RemoveMember { member: ben })
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut a, id).await,
        Err(SyncError::Rejected(_))
    ));
    assert!(a.db.operation(id).await.unwrap().is_none());
    let pending = access_keys(&a.db).await;
    for key in ["minted-1", "fixture-access-key"] {
        assert!(
            pending.iter().any(|pending| pending.access_key_id == key),
            "{key} pending"
        );
    }
}

/// The scope a replacement's journal fixed for its key.
async fn scope(d: &Device, id: OperationId) -> MintScope {
    match crate::operation_data::Data::read(&d.operation(id).await).unwrap() {
        crate::operation_data::Data::ReplaceAccessKey(work) => work.mint.scope,
        _ => panic!("replacement"),
    }
}

async fn discard(d: &mut Device, id: OperationId) -> Result<Begun, SyncError> {
    let row = d.operation(id).await;
    d.sync
        .record_operation_condition(&row, PendingReason::AdminAccessRequired)
        .await
        .unwrap();
    d.sync
        .begin_operation_call(crate::operations::Command::Discard(id))
        .await
}

#[tokio::test]
async fn discarding_a_replacement_leaves_no_key_unrecorded() {
    let (_storage, mut a, b) = household().await;
    let ben = b.member.member();
    // A made key becomes pending deletion as the row goes.
    let admin = MemoryKeyAdmin::default();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    assert!(matches!(a.step(id).await.unwrap(), Progress::Advanced));
    assert!(matches!(discard(&mut a, id).await, Ok(Begun::Value(_))));
    assert!(a.db.operation(id).await.unwrap().is_none());
    assert!(access_keys(&a.db)
        .await
        .iter()
        .any(|key| key.access_key_id == "minted-1" && key.member.as_ref() == Some(&ben)));
    // Without a key yet, what the attempt may have made is rolled back with
    // the admin access held.
    let admin = MemoryKeyAdmin::default();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    admin.mint(&scope(&a, id).await).await.unwrap();
    assert!(matches!(discard(&mut a, id).await, Ok(Begun::Value(_))));
    assert!(admin.users().is_empty());
    assert!(a.db.operation(id).await.unwrap().is_none());
    // Without admin access, the row waits for it with only the rollback left.
    let admin = MemoryKeyAdmin::default();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    admin.mint(&scope(&a, id).await).await.unwrap();
    a.restart(a.storage.clone()).await;
    assert!(matches!(
        discard(&mut a, id).await,
        Err(SyncError::Pending(pending)) if pending.reason == PendingReason::AdminAccessRequired
    ));
    assert!(matches!(
        a.step(id).await.unwrap(),
        Progress::Waiting(PendingReason::AdminAccessRequired)
    ));
    a.sync.hold_admin_access(id, Arc::new(admin.clone()));
    assert!(matches!(a.step(id).await.unwrap(), Progress::Finished(_)));
    assert!(admin.users().is_empty());
    assert_eq!(admin.calls(), ["mint", "roll_back"]);
    assert!(a.db.operation(id).await.unwrap().is_none());
}

/// The replacement's delivery path once its fixed bytes are sealed.
async fn sealed_delivery(
    d: &mut Device,
    id: OperationId,
    member: &MemberId,
) -> (ObjectPath, Vec<u8>) {
    while d.operation(id).await.last_step < 1 {
        assert!(matches!(d.step(id).await.unwrap(), Progress::Advanced));
    }
    assert!(matches!(d.step(id).await.unwrap(), Progress::Advanced));
    match crate::operation_data::Data::read(&d.operation(id).await).unwrap() {
        crate::operation_data::Data::ReplaceAccessKey(work) => (
            ObjectPath::access_key(store(1), member, work.attempt),
            work.delivery.expect("sealed delivery"),
        ),
        _ => panic!("replacement"),
    }
}

#[tokio::test]
async fn an_occupied_delivery_counts_only_with_its_own_complete_bytes() {
    // A lost reply stored the fixed bytes: the retry counts them.
    let (storage, mut a, b) = household().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    let (path, bytes) = sealed_delivery(&mut a, id, &ben).await;
    storage.create(&path, &bytes).await.unwrap();
    assert!(matches!(a.step(id).await.unwrap(), Progress::Advanced));
    assert_eq!(a.operation(id).await.last_step, 2);
    assert!(finish(&mut a, id).await.is_ok());
    // Different bytes at this device's attempt path: a failed comparison read
    // stays pending; a completed one stops this copy before the entry.
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    let (path, _) = sealed_delivery(&mut a, id, &ben).await;
    storage.create(&path, b"another copy").await.unwrap();
    let mut faults = coven_storage::test_utils::Faults::none();
    faults
        .read_failures
        .insert(path.clone(), coven_storage::StorageFailure::Network);
    storage.set_faults(faults).await;
    assert!(
        matches!(a.step(id).await, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network)
    );
    assert_eq!(a.send.stopped(), None);
    assert_eq!(a.operation(id).await.last_step, 1);
    storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    assert!(matches!(
        a.step(id).await,
        Err(SyncError::DeviceReset(
            coven_foundation::files::DeviceResetReason::SlotMismatch
        ))
    ));
    assert_eq!(a.operation(id).await.last_step, 1);
    assert_eq!(storage.read(&path).await.unwrap(), b"another copy");
}

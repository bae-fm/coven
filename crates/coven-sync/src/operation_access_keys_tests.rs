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
async fn discarding_a_replacement_leaves_no_key_it_holds_unrecorded() {
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
    // A failed mint call rolls back what its names created.
    let admin = MemoryKeyAdmin::default();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    admin.fail_next_mint();
    assert!(a.step(id).await.is_err());
    assert!(admin.users().is_empty());
    assert_eq!(admin.calls(), ["mint", "roll_back"]);
    // With no key the row goes without admin access. A key a lost call made,
    // under names no journal holds, waits for §13's reclaim.
    let lost = MintScope::new(
        store(1),
        &ben.to_string(),
        a.device().await,
        uuid::Uuid::from_u128(9),
    );
    admin.mint(&lost).await.unwrap();
    a.restart(a.storage.clone()).await;
    assert!(matches!(discard(&mut a, id).await, Ok(Begun::Value(_))));
    assert!(a.db.operation(id).await.unwrap().is_none());
    assert_eq!(admin.users(), [lost.key_name]);
}

/// T259 (KeyListing `clone_mint_spares_recorded_key`): a backup of Ana's
/// desktop, taken after the replacement is journaled and before its key is
/// made, runs as a second copy and mints too. Each mint call names its
/// principal afresh, so the copy takes nothing from the original, which
/// checks, delivers and records its key.
#[tokio::test]
async fn a_second_copys_mint_keeps_the_key_the_original_records() {
    use crate::store_log_sync::store_log_identity::tests::{backup, restored};
    let (storage, mut a, b) = household().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    let copy = backup(&mut a, &storage, (vec![], vec![])).await;
    let mut c = restored(&a, copy, &storage, (vec![], vec![])).await;
    for d in [&mut a, &mut c] {
        d.sync.hold_admin_access(id, Arc::new(admin.clone()));
        assert!(matches!(d.step(id).await.unwrap(), Progress::Advanced));
    }
    assert_eq!(admin.users().len(), 2);
    let Output::AccessKeyReplaced(replaced) = finish(&mut a, id).await.unwrap() else {
        panic!("replacement result")
    };
    let made = replaced.key;
    assert!(admin
        .key_exists(&made.credentials.access_key_id, &made.principal)
        .await
        .unwrap());
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

/// The deletion operation a database holds for an access key id.
async fn deletion_for(d: &Device, access_key_id: &str) -> Option<OperationId> {
    for record in d.db.operations().await.unwrap() {
        if let crate::operation_data::Data::DeleteAccessKey(key) =
            crate::operation_data::Data::read(&record).unwrap()
        {
            if key.access_key_id == access_key_id {
                return Some(record.id);
            }
        }
    }
    None
}

/// T267 (KeyListing `reset_spares_recorded_key`): Ana's desktop makes Ben's
/// new key, and a second live copy of its database, holding the same
/// journal, resets. Its replacement keeps no made key, so when the original
/// records the key nothing holds it pending and it stays live.
#[tokio::test]
async fn a_reset_copy_leaves_the_key_the_original_records() {
    let (storage, mut a, b) = household().await;
    let admin = MemoryKeyAdmin::default();
    let ben = b.member.member();
    let id = begin(
        &mut a,
        crate::operations::Command::ReplaceAccessKey(ben, Arc::new(admin.clone())),
    )
    .await;
    while a.operation(id).await.last_step < 1 {
        assert!(matches!(a.step(id).await.unwrap(), Progress::Advanced));
    }
    // The copy holding this journal finds the original's delivery and resets.
    let owed = crate::owed_access_key_deletions(&a.db.operations().await.unwrap()).unwrap();
    assert!(owed.is_empty());
    // The original records the key before it reads the replacement (§10).
    let Output::AccessKeyReplaced(replaced) = finish(&mut a, id).await.unwrap() else {
        panic!("replacement result")
    };
    let made = replaced.key.credentials.access_key_id;
    // The copy's replacement registers a fresh device for Ana and loads the store.
    let mut c = device(storage.clone(), 3, member(1), store(1)).await;
    c.sync().await;
    c.sync
        .load_new_device(
            "Ana's desktop",
            Some(coven_foundation::id_source::DeviceId(1)),
        )
        .await
        .unwrap();
    c.db.record_access_key_deletions::<PendingRecord>(None, owed)
        .await
        .unwrap();
    c.sync().await;
    for d in [&a, &c] {
        assert!(deletion_for(d, &made).await.is_none());
    }
    assert_eq!(admin.users().len(), 1);
}

/// A reset keeps each deletion its discarded journal owed, which replay never
/// cancels (§13), and no key it made.
#[test]
fn a_reset_carries_each_owed_deletion_and_no_made_key() {
    let ben = member(2).member();
    let key = |id: &str| coven_storage::S3MemberKey {
        credentials: coven_storage::S3Credentials {
            access_key_id: id.into(),
            secret_access_key: coven_crypto::SecretText::new("secret".into()),
        },
        principal: S3Principal::Console,
    };
    let record = |data: crate::operation_data::Data| {
        let operation = data.new_operation("test").unwrap();
        coven_database::OperationRecord {
            id: OperationId(7),
            kind: operation.kind,
            last_step: 1,
            data: operation.data,
            started_by: operation.started_by,
        }
    };
    let mut mint = crate::operation_data::MintWork::new(coven_storage::MintScope::new(
        store(1),
        &ben.to_string(),
        coven_foundation::id_source::DeviceId(1),
        uuid::Uuid::from_u128(5),
    ));
    mint.minted = Some(key("made"));
    let owed = key("owed");
    let records = [
        record(crate::operation_data::Data::ReplaceAccessKey(
            crate::operation_data::ReplaceKeyWork {
                member: ben,
                attempt: uuid::Uuid::from_u128(5),
                mint,
                delivery: None,
                entry: None,
            },
        )),
        record(crate::operation_data::Data::DeleteAccessKey(
            crate::AccessKeyToDelete {
                access_key_id: owed.credentials.access_key_id,
                principal: owed.principal,
                member: Some(ben),
            },
        )),
    ];
    let carried = crate::owed_access_key_deletions(&records).unwrap();
    assert_eq!(carried.len(), 1);
    assert_eq!(carried[0].key.access_key_id, "owed");
    assert_eq!(carried[0].key.member, Some(ben));
}

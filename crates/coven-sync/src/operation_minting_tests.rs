//! Invites whose S3 key coven makes with admin access, and verified deletion.
use super::*;
use coven_format::codes::InviteCode;
use coven_storage::test_utils::MemoryKeyAdmin;

fn minted_invite(admin: &MemoryKeyAdmin) -> Command {
    Command::Invite(
        InviteTarget::NewMember {
            role: MemberRole::Member,
        },
        InviteAccess::S3AdminAccess(Arc::new(admin.clone())),
    )
}

fn invite_key(invite: &Invite) -> coven_storage::S3Credentials {
    let code = InviteCode::from_text(&invite.code).unwrap();
    let coven_storage::InviteStorage::S3 { credentials, .. } =
        coven_storage::InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!("S3 invite")
    };
    credentials
}

#[tokio::test]
async fn an_invite_carries_a_key_made_and_checked_with_admin_access() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let admin = MemoryKeyAdmin::default();
    let id = begin(&mut a, minted_invite(&admin)).await;
    let Output::Invite(invite) = finish(&mut a, id).await else {
        panic!("invite")
    };
    assert_eq!(invite_key(&invite).access_key_id, "minted-1");
    assert_eq!(admin.calls(), ["mint", "probe"]);
    assert!(
        a.sync.admin_access(id).is_none(),
        "admin access is released"
    );
    // Cancelling lists the made key with its principal until verified deletion.
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, id).await;
    let keys = access_keys(&a.db).await;
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].access_key_id, "minted-1");
    assert!(matches!(
        &keys[0].principal,
        coven_format::S3Principal::IamUser { user, .. } if user.starts_with("coven-")
    ));
    let deletion = pending_id(
        &a.db
            .pending::<PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .find(|record| matches!(record.reason, PendingReason::DeleteAccessKey(_)))
            .unwrap(),
    );
    admin.keep_next_deleted_key();
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::DeleteKey(deletion, Arc::new(admin.clone())))
            .await,
        Err(SyncError::AccessKeyNotDeleted(
            DeleteAccessKeyError::StillPresent
        ))
    ));
    assert_eq!(access_keys(&a.db).await.len(), 1);
    a.sync
        .begin_operation_call(Command::DeleteKey(deletion, Arc::new(admin.clone())))
        .await
        .unwrap();
    assert!(access_keys(&a.db).await.is_empty());
    assert!(admin.users().is_empty());
    assert_eq!(
        admin.calls(),
        [
            "mint",
            "probe",
            "revoke",
            "key_exists",
            "revoke",
            "key_exists"
        ]
    );
}

#[tokio::test]
async fn failed_minting_rolls_back_and_a_restart_needs_admin_access_again() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let admin = MemoryKeyAdmin::default();
    admin.fail_next_mint();
    let id = begin(&mut a, minted_invite(&admin)).await;
    assert!(matches!(step(&mut a, id).await, Err(SyncError::Storage(_))));
    assert_eq!(admin.calls(), ["mint", "roll_back"]);
    assert!(admin.users().is_empty());
    // A failed check also rolls back and forgets the key.
    a.sync.hold_admin_access(id, Arc::new(admin.clone()));
    assert!(matches!(step(&mut a, id).await, Ok(Progress::Advanced)));
    admin.fail_next_probe();
    assert!(matches!(step(&mut a, id).await, Err(SyncError::Storage(_))));
    assert!(admin.users().is_empty());
    // After a restart the invite waits for admin access, then finishes.
    a.restart(storage.clone()).await;
    assert!(matches!(
        step(&mut a, id).await,
        Ok(Progress::Waiting(PendingReason::AdminAccessRequired))
    ));
    a.sync.hold_admin_access(id, Arc::new(admin.clone()));
    let Output::Invite(invite) = finish(&mut a, id).await else {
        panic!("invite")
    };
    assert_eq!(invite_key(&invite).access_key_id, "minted-3");
    assert_eq!(admin.users().len(), 1);
}

#[tokio::test]
async fn cancelling_before_a_key_exists_rolls_back_with_admin_access() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let admin = MemoryKeyAdmin::default();
    let id = begin(&mut a, minted_invite(&admin)).await;
    let record = a.db.operation(id).await.unwrap().unwrap();
    let Data::Invite(work) = Data::read(&record).unwrap() else {
        panic!("invite")
    };
    a.restart(storage.clone()).await;
    begin(&mut a, Command::Cancel(work.id)).await;
    assert!(matches!(
        step(&mut a, id).await,
        Ok(Progress::Waiting(PendingReason::AdminAccessRequired))
    ));
    a.sync.hold_admin_access(id, Arc::new(admin.clone()));
    finish(&mut a, id).await;
    assert_eq!(admin.calls(), ["roll_back"]);
    assert!(access_keys(&a.db).await.is_empty());
    assert!(a.db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn deleting_with_admin_access_reports_each_failure_and_keeps_the_notice() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let admin = MemoryKeyAdmin::default();
    let id = begin(&mut a, minted_invite(&admin)).await;
    let Output::Invite(invite) = finish(&mut a, id).await else {
        panic!("invite")
    };
    begin(&mut a, Command::Cancel(invite.id)).await;
    finish(&mut a, id).await;
    let deletion = pending_id(&a.db.pending::<PendingRecord>().await.unwrap()[0]);
    admin.fail_next_delete();
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::DeleteKey(deletion, Arc::new(admin.clone())))
            .await,
        Err(SyncError::AccessKeyNotDeleted(
            DeleteAccessKeyError::DeleteFailed(_)
        ))
    ));
    assert_eq!(access_keys(&a.db).await.len(), 1);
    // Another operation's id is not a deletion notice.
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::DeleteKey(
                OperationId(i64::MAX),
                Arc::new(admin.clone())
            ))
            .await,
        Err(SyncError::NotPending(_))
    ));
    a.sync
        .begin_operation_call(Command::DeleteKey(deletion, Arc::new(admin.clone())))
        .await
        .unwrap();
    assert!(access_keys(&a.db).await.is_empty());
    assert!(admin.users().is_empty());
}

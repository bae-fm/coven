use super::*;
use crate::snapshot_data::{ReloadScope, SnapshotJob, SnapshotTask, SnapshotTrigger};
use crate::{
    operation_data::Data,
    operations::{Begun, Command, Output, Progress},
    *,
};
use coven_storage::MemberRemoval;

async fn pending(operations: &Operations, db: &coven_database::Database) -> Vec<PendingRecord> {
    operations.get_members().await.unwrap();
    db.pending().await.unwrap()
}
fn pending_id(record: &PendingRecord) -> OperationId {
    let PendingSubject::Operation { id, .. } = record.subject else {
        panic!("{record:?}")
    };
    id
}
fn pending_kind(record: &PendingRecord) -> OperationKind {
    let PendingSubject::Operation { kind, .. } = record.subject else {
        panic!("{record:?}")
    };
    kind
}

fn file_owner(d: &Device) -> Files {
    Files::new(
        coven_database::FileDatabase::new(d.db.clone()),
        d.directory.clone(),
        d.sync.storage.clone(),
        d.clock.clone(),
        d.sync.ids.clone(),
        crate::TransferLimits::default(),
    )
}

pub(crate) fn operation_owner(sync: StoreLogSync, files: Files) -> Operations {
    let writes = match sync.storage.clone() {
        Some(storage) => crate::DeviceLogSync::new(
            storage,
            sync.database.clone(),
            sync.store_keys.clone(),
            sync.member_keys.clone(),
        ),
        None => crate::DeviceLogSync::disconnected(
            sync.database.clone(),
            sync.store_keys.clone(),
            sync.member_keys.clone(),
        ),
    };
    let clock = sync.clock.clone();
    Operations::new(sync, files, writes, clock)
}

fn google() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::GoogleDrive)
            .build()
            .unwrap(),
    )
}
async fn accounts(storage: Arc<MemoryStorage>) -> [Device; 3] {
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: store(1),
            name: "Household".into(),
            admin: public(&a.member),
            access: coven_format::MemberAccess::ProviderAccount("owner@example.com".into()),
            key: key(1),
            key_hash: a.sync.test_key(coven_merge::Audience::Store, key(1)),
            device_name: "owner".into(),
        })
        .await
        .unwrap();
    for (d, email, role) in [
        (&mut b, "ben@example.com", MemberRole::Admin),
        (&mut c, "cat@example.com", MemberRole::Member),
    ] {
        storage.grant_access(email).await.unwrap();
        a.sync
            .make_and_upload_entry(StoreChange::AddMember {
                keys: public(&d.member),
                role,
                access: coven_format::MemberAccess::ProviderAccount(email.into()),
            })
            .await
            .unwrap();
        d.sync.storage = Some(Arc::new(
            MemoryStorage::for_recipient(&storage, email).unwrap(),
        ));
        d.sync().await;
        let device = d.device().await;
        d.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device,
                name: email.into(),
                replaces: None,
            })
            .await
            .unwrap();
    }
    a.sync().await;
    b.sync().await;
    c.sync().await;
    [a, b, c]
}
async fn begin(d: &mut Device, command: Command) -> OperationId {
    match d.sync.begin_operation_call(command).await.unwrap() {
        Begun::Operation(id) => id,
        _ => panic!("operation expected"),
    }
}
async fn step(d: &mut Device, id: OperationId) -> Result<Progress, SyncError> {
    d.step(id).await
}
async fn finish(d: &mut Device, id: OperationId) -> Output {
    loop {
        match step(d, id).await.unwrap() {
            Progress::Finished(value) | Progress::Reply(Ok(value)) => return value,
            Progress::Reply(Err(error)) => panic!("operation call failed: {error}"),
            Progress::Advanced => (),
            Progress::Waiting(_) => panic!("unexpected wait"),
        }
    }
}

#[tokio::test]
async fn owner_removal_preserves_keys_and_revokes_the_account() {
    let storage = google();
    let [mut a, mut b, mut c] = accounts(storage.clone()).await;
    let circle = begin(&mut a, Command::CreateCircle("Household".into())).await;
    let Output::CircleId(circle) = finish(&mut a, circle).await else {
        panic!()
    };
    for who in [&b.member, &c.member] {
        let id = begin(&mut a, Command::AddCircleMember(circle, who.member_id())).await;
        finish(&mut a, id).await;
    }
    b.sync().await;
    c.sync().await;
    let old = a.log().await.replay.state;
    let id = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    assert!(matches!(
        finish(&mut a, id).await,
        Output::Removal(MemberRemoval::Revoked)
    ));
    c.sync().await;
    let state = a.log().await.replay.state;
    assert_eq!(state.store.as_ref().unwrap().key, old.store.unwrap().key);
    assert_eq!(state.circles[&circle].key, old.circles[&circle].key);
    assert!(state.members[&b.member.member_id()].removed);
    assert!(state.devices[&b.device().await].removed);
    let ring = c.custody.read().unwrap().unwrap();
    assert!(ring.store_key(state.store.as_ref().unwrap().key).is_ok());
    assert!(ring.circle_key(circle, state.circles[&circle].key).is_ok());
    let log = a.log().await;
    let removal = &log.entries.last().unwrap().entry.change;
    assert!(keys::introduced(removal).is_none());
    assert!(
        matches!(b.sync.sync_store_log().await, Err(SyncFailure::Storage(error)) if error.failure() == StorageFailure::PermissionDenied)
    );
    assert!(a.db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn outside_admin_removes_a_member_without_learning_the_circle_key() {
    let storage = google();
    let [mut a, mut b, mut c] = accounts(storage).await;
    let id = begin(&mut b, Command::CreateCircle("Gifts".into())).await;
    let Output::CircleId(gifts) = finish(&mut b, id).await else {
        panic!()
    };
    let id = begin(
        &mut b,
        Command::AddCircleMember(gifts, c.member.member_id()),
    )
    .await;
    finish(&mut b, id).await;
    a.sync().await;
    c.sync().await;
    let id = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    finish(&mut a, id).await;
    c.sync().await;
    let key = a.log().await.replay.state.circles[&gifts].key;
    assert!(matches!(
        a.custody.read().unwrap().unwrap().circle_key(gifts, key),
        Err(coven_crypto::MaterialError::UnknownCircleKey { .. })
    ));
    assert!(c
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(gifts, key)
        .is_ok());
}

#[tokio::test]
async fn another_admin_cannot_share_and_owner_applies_their_revocation() {
    let storage = google();
    let [mut a, mut b, c] = accounts(storage.clone()).await;
    assert!(matches!(
        b.sync
            .begin_operation_call(Command::Invite(
                MemberRole::Member,
                InviteAccess::ProviderAccount {
                    email: "new@example.com".into()
                }
            ))
            .await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::NotStoreOwner
    ));
    assert!(b.db.operations().await.unwrap().is_empty());
    let id = begin(&mut b, Command::RemoveMember(c.member.member_id())).await;
    assert!(matches!(
        finish(&mut b, id).await,
        Output::Removal(MemberRemoval::PendingOwner)
    ));
    let recipient = MemoryStorage::for_recipient(&storage, "cat@example.com").unwrap();
    assert!(recipient
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .is_ok());
    a.sync().await;
    let records = a.db.operations().await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(
        finish(&mut a, records[0].id).await,
        Output::Removal(MemberRemoval::Revoked)
    ));
    assert!(
        matches!(recipient.list(&ObjectPrefix::store_logs(store(1))).await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
}

#[tokio::test]
async fn restart_after_each_step_uses_the_committed_entry_and_keys() {
    'crashes: for crash_step in 0.. {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        a.create(key(1)).await;
        a.add(&member(2), MemberRole::Member).await;
        let id = begin(&mut a, Command::RemoveMember(member(2).member_id())).await;
        for _ in 0..crash_step {
            match step(&mut a, id).await.unwrap() {
                Progress::Finished(Output::Removal(MemberRemoval::DeleteAccessKey { .. })) => {
                    break 'crashes
                }
                Progress::Advanced => (),
                _ => panic!("unexpected removal progress"),
            }
        }
        let before = a.db.local_store_log().await.unwrap().upload;
        let expected = before.as_ref().map(|upload| a.reseal(upload));
        let record = a.db.operations().await.unwrap().remove(0);
        if let Some(upload) = &before {
            for key in &upload.sealing.keys {
                let stored = storage.read(&ObjectPath::parse(&key.path).unwrap()).await;
                if record.last_step >= 2 {
                    assert_eq!(stored.unwrap(), key.bytes);
                } else {
                    assert!(
                        matches!(stored, Err(error) if error.failure() == StorageFailure::NotFound)
                    );
                }
            }
            let stored = storage
                .read(&object::path(store(1), upload.entry.position))
                .await;
            if record.last_step >= 3 {
                assert_eq!(stored.unwrap(), *expected.as_ref().unwrap());
            } else {
                assert!(
                    matches!(stored, Err(error) if error.failure() == StorageFailure::NotFound)
                );
            }
        }
        a.restart(storage.clone()).await;
        let resumed = a.db.operations().await.unwrap().remove(0);
        assert_eq!(record.last_step, resumed.last_step);
        assert_eq!(record.data, resumed.data);
        assert!(matches!(
            finish(&mut a, id).await,
            Output::Removal(MemberRemoval::DeleteAccessKey { .. })
        ));
        if let Some(before) = before {
            assert_eq!(
                storage
                    .read(&object::path(store(1), before.entry.position))
                    .await
                    .unwrap(),
                expected.unwrap()
            );
            for key in before.sealing.keys {
                assert_eq!(
                    storage
                        .read(&ObjectPath::parse(&key.path).unwrap())
                        .await
                        .unwrap(),
                    key.bytes
                );
            }
        }
        assert_eq!(a.log().await.entries.len(), 3);
        assert_eq!(
            a.db.access_keys_to_delete().await.unwrap(),
            [AccessKeyToDelete {
                access_key_id: "fixture-access-key".into(),
                member: Some(member(2).member_id())
            }]
        );
        a.db.confirm_access_key_deleted("fixture-access-key".into())
            .await
            .unwrap();
        assert!(a.db.access_keys_to_delete().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancelled_app_future_keeps_running_and_permanent_failure_retries_or_discards() {
    let storage = storage();
    let mut d = device(storage.clone(), 1, member(1), store(1)).await;
    d.create(key(1)).await;
    d.add(&member(2), MemberRole::Member).await;
    d.sync.storage = None;
    let files = file_owner(&d);
    let operations = { operation_owner(d.sync, files) };
    let mut waiting = Box::pin(operations.create_circle("offline"));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    operations.get_members().await.unwrap();
    assert_eq!(d.db.operations().await.unwrap().len(), 1);
    drop(waiting);
    operations.set_storage(Some(storage.clone())).await.unwrap();
    assert_eq!(operations.circles().await.unwrap()[0].name, "offline");
    // Invalid names fail during format validation, after their intent is durable.
    assert!(matches!(
        operations.create_circle("").await,
        Err(SyncError::Failed {
            failure: LocalFailure::Database,
            ..
        })
    ));
    let conditions = pending(&operations, &d.db).await;
    assert_eq!(conditions.len(), 1);
    let id = pending_id(&conditions[0]);
    assert!(matches!(
        operations.retry_pending_operation(id).await,
        Err(SyncError::Failed {
            failure: LocalFailure::Database,
            ..
        })
    ));
    operations.discard_pending_operation(id).await.unwrap();
    assert!(pending(&operations, &d.db).await.is_empty());
    operations.close().await.unwrap();
}

#[path = "operation_invites_tests.rs"]
mod invites;

#[tokio::test]
async fn independent_removals_keep_their_original_attempts() {
    let storage = google();
    let [mut a, mut b, c] = accounts(storage.clone()).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let a_op = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    assert!(matches!(
        step(&mut a, a_op).await.unwrap(),
        Progress::Advanced
    ));
    let attempted = a.db.local_store_log().await.unwrap().upload.unwrap();
    let b_op = begin(&mut b, Command::RemoveMember(c.member.member_id())).await;
    finish(&mut b, b_op).await;
    let concurrent = b.log().await.entries.last().unwrap().entry.position;
    assert!(matches!(
        finish(&mut a, a_op).await,
        Output::Removal(MemberRemoval::Revoked)
    ));
    let log = a.log().await;
    for position in [attempted.entry.position, concurrent] {
        assert_eq!(
            log.replay.entries[&position],
            coven_database::EntryOutcome::Kept
        );
    }
    let removal = &log.entries.last().unwrap().entry;
    assert_eq!(removal, &attempted.entry);
    assert!(!removal.had_read.0.contains(&concurrent));
    assert!(log.replay.state.members[&b.member.member_id()].removed);
    assert!(log.replay.state.members[&c.member.member_id()].removed);
    assert_eq!(log.replay.state.store.unwrap().key, key(1));
    assert!(attempted.sealing.keys.is_empty());
    assert!(keys::introduced(&removal.change).is_none());
}

#[tokio::test]
async fn circle_membership_rotates_and_shares_history() {
    let storage = google();
    let [mut a, mut b, mut c] = accounts(storage).await;
    let create = begin(&mut a, Command::CreateCircle("Circle".into())).await;
    let Output::CircleId(circle) = finish(&mut a, create).await else {
        panic!()
    };
    let add = begin(
        &mut a,
        Command::AddCircleMember(circle, b.member.member_id()),
    )
    .await;
    finish(&mut a, add).await;
    b.sync().await;
    let old_key = a.log().await.replay.state.circles[&circle].key;
    let remove = begin(
        &mut a,
        Command::RemoveCircleMember(circle, b.member.member_id()),
    )
    .await;
    finish(&mut a, remove).await;
    assert_eq!(a.log().await.replay.state.circles[&circle].key, old_key);
    a.sync.test_rotate(Audience::Circle(circle), key(20)).await;
    b.sync().await;
    let new_key = a.log().await.replay.state.circles[&circle].key;
    assert_ne!(old_key, new_key);
    assert!(matches!(
        b.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle, new_key),
        Err(coven_crypto::MaterialError::UnknownCircleKey { .. })
    ));
    let add = begin(
        &mut a,
        Command::AddCircleMember(circle, c.member.member_id()),
    )
    .await;
    finish(&mut a, add).await;
    c.sync().await;
    let ring = c.custody.read().unwrap().unwrap();
    assert!(ring.circle_key(circle, old_key).is_ok());
    assert!(ring.circle_key(circle, new_key).is_ok());
    assert!(matches!(
        b.sync
            .begin_operation_call(Command::RenameCircle(circle, "Forbidden".into()))
            .await,
        Err(SyncError::CircleNotMember(_))
    ));
}

#[path = "operation_steps_tests.rs"]
mod circle_rows;

#[tokio::test]
async fn permanent_storage_failure_preserves_fixed_bytes_for_retry_and_discard() {
    for discard in [false, true] {
        let storage = google();
        let [mut a, _b, _c] = accounts(storage.clone()).await;
        let id = begin(&mut a, Command::CreateCircle("Retained".into())).await;
        let held = a.custody.read().unwrap().unwrap().to_secret_bytes();
        step(&mut a, id).await.unwrap();
        assert_eq!(
            a.custody
                .read()
                .unwrap()
                .unwrap()
                .to_secret_bytes()
                .as_bytes(),
            held.as_bytes()
        );
        let fixed = a.db.local_store_log().await.unwrap().upload.unwrap();
        let StoreChange::CreateCircle {
            circle,
            key,
            key_hash,
            ..
        } = &fixed.entry.change
        else {
            panic!("circle creation")
        };
        let copy = &fixed.sealing.keys[0];
        let material = coven_format::sealed_key::SealedKey::decode(&copy.bytes)
            .unwrap()
            .open_circle_key(&a.member, &copy.path)
            .unwrap();
        assert_eq!(
            (material.circle(), material.id(), material.hash()),
            (*circle, *key, *key_hash)
        );
        let expected = a.reseal(&fixed);
        a.sync.storage = Some(Arc::new(
            MemoryStorage::for_recipient(&storage, "uninvited@example.com").unwrap(),
        ));
        let files = file_owner(&a);
        let operations = { operation_owner(a.sync, files) };
        let conditions = pending(&operations, &a.db).await;
        assert_eq!(conditions.len(), 1);
        assert_eq!(pending_id(&conditions[0]), id);
        assert_eq!(pending_kind(&conditions[0]), OperationKind::CreateCircle);
        assert!(matches!(
            conditions[0].reason,
            PendingReason::Storage {
                failure: StorageFailure::PermissionDenied,
                ..
            }
        ));
        assert!(matches!(
            storage.read(&object::path(store(1), fixed.entry.position)).await,
            Err(error) if error.failure() == StorageFailure::NotFound
        ));
        operations.set_storage(Some(storage.clone())).await.unwrap();
        operations.sync_store_log().await.unwrap();
        assert_eq!(pending(&operations, &a.db).await.len(), 1);
        assert!(a.db.local_store_log().await.unwrap().upload.is_some());
        if discard {
            operations.discard_pending_operation(id).await.unwrap();
        } else {
            operations.retry_pending_operation(id).await.unwrap();
        }
        assert!(a.db.operations().await.unwrap().is_empty());
        assert_eq!(
            storage
                .read(&object::path(store(1), fixed.entry.position))
                .await
                .unwrap(),
            expected
        );
        assert!(a.db.local_store_log().await.unwrap().upload.is_none());
        operations.close().await.unwrap();
    }
}

#[tokio::test]
async fn retained_provider_grants_block_both_requested_and_remote_revocations() {
    for remote in [false, true] {
        let storage = google();
        let [mut a, mut b, c] = accounts(storage.clone()).await;
        let shares = vec![coven_storage::RetainedAccess {
            provider_id: "parent-permission".into(),
            reason: coven_storage::RetainedAccessReason::Inherited,
        }];
        storage
            .set_retained_access("cat@example.com", shares.clone())
            .await;
        if remote {
            let id = begin(&mut b, Command::RemoveMember(c.member.member_id())).await;
            finish(&mut b, id).await;
            a.sync().await;
        }
        let files = file_owner(&a);
        let operations = { operation_owner(a.sync, files) };
        if !remote {
            assert_eq!(
                operations
                    .remove_member(&c.member.member_id())
                    .await
                    .unwrap(),
                MemberRemoval::AccessRemains {
                    shares: shares.clone()
                }
            );
        }
        let conditions = pending(&operations, &a.db).await;
        assert_eq!(conditions.len(), 1);
        assert_eq!(
            pending_kind(&conditions[0]),
            if remote {
                OperationKind::RevokeAccess
            } else {
                OperationKind::RemoveMember
            }
        );
        let id = pending_id(&conditions[0]);
        assert!(
            matches!(operations.retry_pending_operation(id).await, Err(SyncError::AccessRemains(found)) if found == shares)
        );
        assert!(MemoryStorage::for_recipient(&storage, "cat@example.com")
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await
            .is_ok());
        storage
            .set_retained_access("cat@example.com", Vec::new())
            .await;
        operations.retry_pending_operation(id).await.unwrap();
        assert!(pending(&operations, &a.db).await.is_empty());
        assert!(
            matches!(MemoryStorage::for_recipient(&storage, "cat@example.com")
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
        );
        operations.close().await.unwrap();
    }
}

#[tokio::test]
async fn removing_a_member_cuts_off_every_device_of_their_account() {
    let storage = google();
    let [mut a, b, _c] = accounts(storage.clone()).await;
    let mut phone = device(storage.clone(), 4, b.member.clone(), store(1)).await;
    phone.sync.storage = Some(Arc::new(
        MemoryStorage::for_recipient(&storage, "ben@example.com").unwrap(),
    ));
    phone.sync().await;
    let phone_id = phone.device().await;
    phone
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: phone_id,
            name: "Second phone".into(),
            replaces: None,
        })
        .await
        .unwrap();
    a.sync().await;
    let operation = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    finish(&mut a, operation).await;
    let state = a.log().await.replay.state;
    assert!(state.devices[&phone_id].removed);
    assert!(state.devices[&b.device().await].removed);
    assert!(
        matches!(phone.sync.sync_store_log().await, Err(SyncFailure::Storage(error)) if error.failure() == StorageFailure::PermissionDenied)
    );
    assert!(phone
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(state.store.unwrap().key)
        .is_ok());
}

#[tokio::test]
async fn plain_device_removal_preserves_member_keys_and_returns_provider_sign_out() {
    let storage = google();
    let [mut a, mut b, mut c] = accounts(storage.clone()).await;
    let b_id = b.device().await;
    assert!(matches!(
        c.sync
            .begin_operation_call(Command::RemoveDevice(b_id))
            .await,
        Err(SyncError::PermissionDenied)
    ));
    let previous_key = a.log().await.replay.state.store.unwrap().key;
    assert!(matches!(
        a.sync
            .begin_operation_call(Command::RemoveDevice(b_id))
            .await
            .unwrap(),
        Begun::Value(Output::SignOut(
            coven_storage::ProviderSignOut::RemoveAppAccess {
                provider: coven_storage::CloudProvider::GoogleDrive
            }
        ))
    ));
    let state = a.log().await.replay.state;
    assert_eq!(state.store.unwrap().key, previous_key);
    assert!(!state.members[&b.member.member_id()].removed);
    assert!(state.devices[&b_id].removed);
    assert!(a.db.operations().await.unwrap().is_empty());
    assert!(matches!(
        b.sync.sync_store_log().await,
        Err(SyncFailure::Removed)
    ));
    assert!(MemoryStorage::for_recipient(&storage, "ben@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    let c_id = c.device().await;
    assert!(matches!(
        c.sync
            .begin_operation_call(Command::RemoveDevice(c_id))
            .await
            .unwrap(),
        Begun::Value(Output::SignOut(_))
    ));
}

#[tokio::test]
async fn role_changes_preserve_an_admin_and_owner_accounts_cannot_be_removed() {
    let [a, b, c] = accounts(google()).await;
    let owner = a.member.member_id();
    let admin = b.member.member_id();
    let files = file_owner(&a);
    let a = { operation_owner(a.sync, files) };
    let files = file_owner(&b);
    let b = { operation_owner(b.sync, files) };
    let files = file_owner(&c);
    let c = { operation_owner(c.sync, files) };
    assert!(matches!(
        b.remove_member(&owner).await,
        Err(SyncError::StoreOwner)
    ));
    assert!(matches!(
        c.set_member_role(&admin, MemberRole::Member).await,
        Err(SyncError::PermissionDenied)
    ));
    b.set_member_role(&owner, MemberRole::Member).await.unwrap();
    assert!(matches!(
        b.set_member_role(&admin, MemberRole::Member).await,
        Err(SyncError::LastAdmin)
    ));
    a.sync_store_log().await.unwrap();
    assert!(a
        .get_members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == owner && m.role == MemberRole::Member));
    let circle = a.create_circle("Members' circle").await.unwrap();
    assert!(matches!(
        a.add_circle_member(circle, &member(99).member_id()).await,
        Err(SyncError::NotStoreMember(id)) if id == member(99).member_id()
    ));
    assert!(matches!(
        a.circle_members(super::circle(99)).await,
        Err(SyncError::CircleDeleted(id)) if id == super::circle(99)
    ));
    a.close().await.unwrap();
    b.close().await.unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
async fn resumed_removal_uses_the_targets_current_storage_account() {
    let storage = google();
    let [mut a, b, _c] = accounts(storage.clone()).await;
    let mut owner_phone = device(storage.clone(), 4, a.member.clone(), store(1)).await;
    owner_phone.sync().await;
    let device_id = owner_phone.device().await;
    owner_phone
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: device_id,
            name: "Owner phone".into(),
            replaces: None,
        })
        .await
        .unwrap();
    a.sync().await;
    let waiting = begin(&mut a, Command::RemoveMember(b.member.member_id())).await;
    let remove = begin(
        &mut owner_phone,
        Command::RemoveMember(b.member.member_id()),
    )
    .await;
    finish(&mut owner_phone, remove).await;
    storage.grant_access("ben-new@example.com").await.unwrap();
    owner_phone
        .sync
        .make_and_upload_entry(StoreChange::AddMember {
            keys: public(&b.member),
            role: MemberRole::Member,
            access: coven_format::MemberAccess::ProviderAccount("ben-new@example.com".into()),
        })
        .await
        .unwrap();
    finish(&mut a, waiting).await;
    assert!(
        matches!(MemoryStorage::for_recipient(&storage, "ben-new@example.com")
            .unwrap()
            .list(&ObjectPrefix::all(store(1)))
            .await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
}

#[tokio::test]
async fn store_reset_requires_admin_and_circle_reset_requires_membership() {
    let [a, b, c] = accounts(google()).await;
    let files = file_owner(&a);
    let a = { operation_owner(a.sync, files) };
    let files = file_owner(&b);
    let b = { operation_owner(b.sync, files) };
    let files = file_owner(&c);
    let c = { operation_owner(c.sync, files) };
    assert!(matches!(
        c.reset_store().await,
        Err(SyncError::PermissionDenied)
    ));
    let circle = c.create_circle("Gifts").await.unwrap();
    a.sync_store_log().await.unwrap();
    assert!(matches!(
        a.reset_circle(circle).await,
        Err(SyncError::CircleNotMember(id)) if id == circle
    ));
    c.reset_circle(circle).await.unwrap();
    a.sync_store_log().await.unwrap();
    a.reset_store().await.unwrap();
    b.sync_store_log().await.unwrap();
    c.sync_store_log().await.unwrap();
    for operations in [a, b, c] {
        operations.close().await.unwrap();
    }
}

#[path = "operation_pending_tests.rs"]
mod pending_records;

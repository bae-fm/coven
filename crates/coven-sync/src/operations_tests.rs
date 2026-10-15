use super::*;
use crate::snapshot_data::{ReloadScope, SnapshotJob, SnapshotTask, SnapshotTrigger};
use crate::{
    operation_data::Data,
    operations::{Begun, Command, Output, Progress},
    *,
};

async fn pending(operations: &Operations, db: &coven_database::Database) -> Vec<PendingRecord> {
    operations.barrier().await.unwrap();
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

#[path = "operation_calls_tests.rs"]
mod calls;

fn file_owner(d: &Device) -> Files {
    Files::new(
        coven_database::FileDatabase::new(d.db.clone()),
        d.directory.clone(),
        d.sync.storage.clone(),
        d.send.clone(),
        d.waits.clone(),
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
            sync.clock.clone(),
            sync.send.clone(),
        ),
        None => crate::DeviceLogSync::disconnected(
            sync.database.clone(),
            sync.store_keys.clone(),
            sync.member_keys.clone(),
            sync.clock.clone(),
            sync.send.clone(),
        ),
    };
    let clock = sync.clock.clone();
    Operations::new(sync, files, writes, clock)
}

pub(crate) fn google() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::GoogleDrive)
            .build()
            .unwrap(),
    )
}
pub(crate) async fn accounts(storage: Arc<MemoryStorage>) -> [Device; 3] {
    accounts_with(storage, Vec::new, Vec::new).await
}
/// Ana (owner, admin), Ben (admin) and Carol (member), each on one device
/// whose database declares `tables` and runs `migrations`.
async fn accounts_with(
    storage: Arc<MemoryStorage>,
    tables: impl Fn() -> Vec<coven_database::SyncedTable>,
    migrations: impl Fn() -> Vec<coven_database::Migration>,
) -> [Device; 3] {
    let open = |n: u8| {
        Device::new(
            storage.clone(),
            n.into(),
            member(n),
            store(1),
            tables(),
            migrations(),
            Arc::new(coven_foundation::id_source::UuidIds),
        )
    };
    let mut a = open(1).await;
    let mut b = open(2).await;
    let mut c = open(3).await;
    a.sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: store(1),
            name: a.directory.settings().unwrap().name,
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
                member: d.member.member(),
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
        let record = d.db.operation(id).await.unwrap().unwrap();
        if matches!(Data::read(&record).unwrap(), Data::Invite(_)) {
            drive_access(d).await;
        }
        match step(d, id).await.unwrap() {
            Progress::Finished(value) | Progress::Reply(Ok(value)) => return value,
            Progress::Reply(Err(error)) => panic!("operation call failed: {error}"),
            Progress::Advanced => (),
            Progress::Waiting(_) => panic!("unexpected wait"),
        }
    }
}

pub(crate) async fn publish(d: &mut Device, command: Command) {
    assert!(matches!(
        d.sync.begin_operation_call(command).await.unwrap(),
        Begun::Value(Output::Unit)
    ));
}
async fn queue_removal(d: &mut Device, command: Command) -> coven_database::StoreLogUpload {
    let local = d.db.local_store_log().await.unwrap();
    let number = local
        .log
        .entries
        .iter()
        .filter(|e| e.entry.position.device == local.device)
        .map(|e| e.entry.position.number)
        .max()
        .unwrap()
        + 1;
    let previous = d.sync.storage.clone();
    d.interrupt_entry(number).await;
    let result = d.sync.begin_operation_call(command).await;
    d.sync.storage = previous;
    assert!(
        matches!(result,Err(SyncError::Storage(ref error)) if error.failure() == StorageFailure::Network)
    );
    d.db.local_store_log().await.unwrap().upload.unwrap()
}
pub(crate) async fn drive_access(d: &mut Device) {
    for record in d.db.operations().await.unwrap() {
        if matches!(Data::read(&record).unwrap(), Data::ProviderAccess { .. }) {
            loop {
                match step(d, record.id).await.unwrap() {
                    Progress::Advanced => (),
                    Progress::Finished(Output::Unit) => break,
                    Progress::Waiting(reason) => {
                        d.sync
                            .record_operation_condition(&record, reason)
                            .await
                            .unwrap();
                        break;
                    }
                    _ => panic!("unexpected provider progress"),
                }
            }
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
        let id = begin(&mut a, Command::AddCircleMember(circle, who.member())).await;
        finish(&mut a, id).await;
    }
    b.sync().await;
    c.sync().await;
    let old_store = a.newest(Audience::Store).await;
    let old_circle = a.newest(Audience::Circle(circle)).await;
    publish(&mut a, Command::RemoveMember(b.member.member())).await;
    drive_access(&mut a).await;
    c.sync().await;
    let state = a.log().await.replay.state;
    // A removal introduces no key; rotation is the only replacement (§11).
    assert_eq!(a.newest(Audience::Store).await, old_store);
    assert_eq!(a.newest(Audience::Circle(circle)).await, old_circle);
    assert!(state.members[&b.member.member()].removed);
    assert!(state.devices[&b.device().await].removed());
    let ring = c.custody.read().unwrap().unwrap();
    assert!(ring.store_key(old_store).is_ok());
    assert!(ring.circle_key(circle, old_circle).is_ok());
    let log = a.log().await;
    let removal = &log.entries.last().unwrap().entry.change;
    assert!(keys::introduced(removal).is_none());
    assert!(
        matches!(b.sync.sync_store_log().await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::PermissionDenied)
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
    let id = begin(&mut b, Command::AddCircleMember(gifts, c.member.member())).await;
    finish(&mut b, id).await;
    a.sync().await;
    c.sync().await;
    publish(&mut a, Command::RemoveMember(b.member.member())).await;
    c.sync().await;
    let key = a.newest(Audience::Circle(gifts)).await;
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
                InviteTarget::NewMember { role: MemberRole::Member },
                InviteAccess::ProviderAccount {
                    email: "new@example.com".into()
                }
            ))
            .await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::NotStoreOwner
    ));
    assert!(b.db.operations().await.unwrap().is_empty());
    publish(&mut b, Command::RemoveMember(c.member.member())).await;
    drive_access(&mut b).await;
    assert!(b
        .db
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|p| matches!(p.reason, PendingReason::PendingOwner { .. })));
    let recipient = MemoryStorage::for_recipient(&storage, "cat@example.com").unwrap();
    assert!(recipient
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .is_ok());
    a.sync().await;
    let records = a.db.operations().await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(finish(&mut a, records[0].id).await, Output::Unit));
    assert!(
        matches!(recipient.list(&ObjectPrefix::store_logs(store(1))).await, Err(error) if error.failure() == StorageFailure::PermissionDenied)
    );
}

#[tokio::test]
async fn interrupted_plain_removal_keeps_its_entry_and_schedules_access_on_replay() {
    for lost_reply in [false, true] {
        let storage = google();
        let [mut a, b, _] = accounts(storage.clone()).await;
        let local = a.db.local_store_log().await.unwrap();
        let number = local
            .log
            .entries
            .iter()
            .filter(|e| e.entry.position.device == local.device)
            .map(|e| e.entry.position.number)
            .max()
            .unwrap()
            + 1;
        a.sync.storage = Some(key_distribution::failing_creation(
            storage.clone(),
            ObjectPath::store_log(store(1), local.device, number.try_into().unwrap()),
            lost_reply,
        ));
        assert!(matches!(
            a.sync
                .begin_operation_call(Command::RemoveMember(b.member.member()))
                .await,
            Err(SyncError::Storage(_))
        ));
        let fixed = a.db.local_store_log().await.unwrap().upload.unwrap();
        let bytes = a.reseal(&fixed).await;
        assert!(a.db.operations().await.unwrap().is_empty());
        a.restart(storage.clone()).await;
        a.sync().await;
        assert_eq!(
            a.log().await.replay.entries[&fixed.entry.position],
            coven_database::EntryOutcome::Kept
        );
        assert_eq!(
            storage
                .read(&object::path(store(1), fixed.entry.position))
                .await
                .unwrap(),
            bytes
        );
        assert!(a.db.local_store_log().await.unwrap().upload.is_none());
        drive_access(&mut a).await;
        assert_eq!(
            MemoryStorage::for_recipient(&storage, "ben@example.com")
                .unwrap()
                .list(&ObjectPrefix::all(store(1)))
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::PermissionDenied
        );
    }
}

#[path = "operation_invites_tests.rs"]
mod invites;

#[path = "join_admission_tests.rs"]
mod join_admission;

#[path = "member_keys_replacement_tests.rs"]
mod member_keys_replacement;

#[path = "member_keys_settlement_tests.rs"]
mod member_keys_settlement;

#[tokio::test]
async fn independent_removals_keep_their_original_attempts() {
    let storage = google();
    let [mut a, mut b, c] = accounts(storage.clone()).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let attempted = queue_removal(&mut a, Command::RemoveMember(b.member.member())).await;
    publish(&mut b, Command::RemoveMember(c.member.member())).await;
    let concurrent = b.log().await.entries.last().unwrap().entry.position;
    a.sync().await;
    drive_access(&mut a).await;
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
    assert!(log.replay.state.members[&b.member.member()].removed);
    assert!(log.replay.state.members[&c.member.member()].removed);
    assert_eq!(
        crate::key_introductions::newest(&log, &Audience::Store),
        Some(key(1))
    );
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
    let add = begin(&mut a, Command::AddCircleMember(circle, b.member.member())).await;
    finish(&mut a, add).await;
    b.sync().await;
    let old_key = a.newest(Audience::Circle(circle)).await;
    publish(
        &mut a,
        Command::RemoveCircleMember(circle, b.member.member()),
    )
    .await;
    assert_eq!(a.newest(Audience::Circle(circle)).await, old_key);
    a.sync.test_rotate(Audience::Circle(circle), key(20)).await;
    b.sync().await;
    let new_key = a.newest(Audience::Circle(circle)).await;
    assert_ne!(old_key, new_key);
    assert!(matches!(
        b.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle, new_key),
        Err(coven_crypto::MaterialError::UnknownCircleKey { .. })
    ));
    let add = begin(&mut a, Command::AddCircleMember(circle, c.member.member())).await;
    finish(&mut a, add).await;
    c.sync().await;
    let ring = c.custody.read().unwrap().unwrap();
    assert!(ring.circle_key(circle, old_key).is_ok());
    assert!(ring.circle_key(circle, new_key).is_ok());
    assert!(matches!(
        b.sync
            .begin_operation_call(Command::RenameCircle(circle, "Forbidden".into()))
            .await,
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
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
            .open_circle_key(&a.member, copy.path(store(1)).as_str())
            .unwrap();
        assert_eq!(
            (material.circle(), material.id(), material.hash()),
            (*circle, *key, *key_hash)
        );
        let expected = a.reseal(&fixed).await;
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
            publish(&mut b, Command::RemoveMember(c.member.member())).await;
            a.sync().await;
        }
        let files = file_owner(&a);
        let operations = { operation_owner(a.sync, files) };
        if !remote {
            operations.remove_member(&c.member.member()).await.unwrap();
        }
        let conditions = pending(&operations, &a.db).await;
        assert_eq!(conditions.len(), 1);
        // Retained grants are provider access work, whichever device removed.
        assert_eq!(pending_kind(&conditions[0]), OperationKind::RevokeAccess);
        let id = pending_id(&conditions[0]);
        assert!(
            matches!(operations.retry_pending_operation(id).await, Err(SyncError::Pending(found)) if matches!(&found.reason, PendingReason::AccessRemains { shares: found, .. } if *found == shares))
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
    publish(&mut a, Command::RemoveMember(b.member.member())).await;
    drive_access(&mut a).await;
    let state = a.log().await.replay.state;
    let store_key = a.newest(Audience::Store).await;
    assert!(state.devices[&phone_id].removed());
    assert!(state.devices[&b.device().await].removed());
    assert!(
        matches!(phone.sync.sync_store_log().await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::PermissionDenied)
    );
    assert!(phone
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(store_key)
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
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    let previous_key = a.newest(Audience::Store).await;
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
    assert_eq!(a.newest(Audience::Store).await, previous_key);
    assert!(!state.members[&b.member.member()].removed);
    assert!(state.devices[&b_id].removed());
    assert!(a.db.operations().await.unwrap().is_empty());
    assert!(matches!(
        b.sync.sync_store_log().await,
        Err(SyncError::Removed)
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
    let owner = a.member.member();
    let admin = b.member.member();
    let a_db = a.db.clone();
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
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    b.set_member_role(&owner, MemberRole::Member).await.unwrap();
    assert!(matches!(
        b.set_member_role(&admin, MemberRole::Member).await,
        Err(SyncError::Rejected(coven_database::DropReason::NoAdminLeft))
    ));
    a.sync_store_log().await.unwrap();
    assert!(a_db
        .members()
        .await
        .unwrap()
        .iter()
        .any(|m| m.id == owner && m.role == MemberRole::Member));
    let circle = a.create_circle("Members' circle").await.unwrap();
    assert!(matches!(
        a.add_circle_member(circle, &member(99).member()).await,
        Err(SyncError::Rejected(coven_database::DropReason::TargetGone(coven_database::EntryTarget::Member(id)))) if id == member(99).member()
    ));
    // An absent circle has no members; listing it is not a change to reject.
    assert!(a_db
        .circle_members(super::circle(99))
        .await
        .unwrap()
        .is_empty());
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
    queue_removal(&mut a, Command::RemoveMember(b.member.member())).await;
    publish(&mut owner_phone, Command::RemoveMember(b.member.member())).await;
    storage.grant_access("ben-new@example.com").await.unwrap();
    owner_phone
        .sync
        .make_and_upload_entry(StoreChange::AddMember {
            member: b.member.member(),
            keys: crate::effects::tests::addition_keys(&owner_phone.log().await, &b.member),
            role: MemberRole::Member,
            access: coven_format::MemberAccess::ProviderAccount("ben-new@example.com".into()),
        })
        .await
        .unwrap();
    publish(&mut a, Command::RemoveMember(b.member.member())).await;
    drive_access(&mut a).await;
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
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    let circle = c.create_circle("Gifts").await.unwrap();
    a.sync_store_log().await.unwrap();
    assert!(matches!(
        a.reset_circle(circle).await,
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
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

#[path = "operation_minting_tests.rs"]
mod minting;

#[tokio::test]
async fn removal_returns_unit_before_provider_work_and_has_no_operation_row() {
    let storage = google();
    let [mut ana, ben, _] = accounts(storage.clone()).await;
    let result = ana
        .sync
        .begin_operation_call(Command::RemoveMember(ben.member.member()))
        .await
        .unwrap();
    assert!(matches!(result, Begun::Value(Output::Unit)));
    assert!(ana.log().await.replay.state.members[&ben.member.member()].removed);
    assert!(ana
        .db
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|r| r.kind != "remove-member"));
    assert!(MemoryStorage::for_recipient(&storage, "ben@example.com")
        .unwrap()
        .list(&ObjectPrefix::all(store(1)))
        .await
        .is_ok());
    assert!(ana.db.pending::<PendingRecord>().await.unwrap().iter().any(|p|
        matches!(&p.reason, PendingReason::ProviderPending { action: ProviderAccessAction::Revoke, account } if account == "ben@example.com")));
}

#[path = "operation_access_tests.rs"]
mod access;

#[tokio::test]
async fn a_worker_stopped_on_its_journal_fails_later_calls_with_that_database_cause() {
    let storage = google();
    let [a, _b, _c] = accounts(storage).await;
    let files = file_owner(&a);
    let db = a.db.clone();
    let operations = operation_owner(a.sync, files);
    operations.barrier().await.unwrap();
    // The journal becomes unreadable under the running worker.
    db.close().await.unwrap();
    for _ in 0..2 {
        let Err(SyncError::Failed { failure, source }) = operations.barrier().await else {
            panic!("a stopped worker's calls fail with its cause")
        };
        assert_eq!(failure, crate::LocalFailure::Database);
        assert!(matches!(
            source.downcast_ref::<SyncError>(),
            Some(SyncError::Database(coven_database::DbError::StoreClosed))
        ));
    }
    operations.close().await.unwrap();
}

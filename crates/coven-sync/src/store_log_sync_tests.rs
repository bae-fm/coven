use super::*;
use coven_crypto::{custody::InMemoryCustody, MemberKeys, StoreKey};
use coven_database::{CovenMigrationPolicy, DatabaseBuilder};
use coven_format::store_log::{CircleKeyId, MemberPublicKeys, MemberRole};
use coven_foundation::{
    clock::FixedClock,
    files::{StoreDir, StoreLayout},
    id_source::{CircleId, DeviceId, IdSource, SequentialIds, StoreId},
};
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    StorageConfig,
};
use std::time::Duration;
use uuid::Uuid;

struct Device {
    storage: Arc<MemoryStorage>,
    sync: StoreLogSync,
    db: Database,
    directory: StoreDir,
    custody: Arc<InMemoryCustody<StoreKeyring>>,
    member: MemberKeys,
    clock: Arc<FixedClock>,
    _temporary: tempfile::TempDir,
}

fn key(n: u128) -> KeyId {
    KeyId(Uuid::from_u128(n))
}
fn circle(n: u128) -> CircleId {
    CircleId(Uuid::from_u128(n))
}
fn store(n: u128) -> StoreId {
    StoreId(Uuid::from_u128(n))
}
fn member(n: u8) -> MemberKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([n; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
fn public(member: &MemberKeys) -> MemberPublicKeys {
    MemberPublicKeys {
        signing: member.member_id(),
        sealing: member.sealing_public_key(),
    }
}
fn storage() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::new(
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "store".into(),
            },
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )
        .unwrap(),
    )
}
async fn open(directory: StoreDir, clock: Arc<FixedClock>) -> Database {
    DatabaseBuilder::new(directory)
        .synced_tables(vec![])
        .migrations(vec![])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .clock(clock)
        .open()
        .await
        .unwrap()
}
async fn device(storage: Arc<MemoryStorage>, n: u64, member: MemberKeys, store: StoreId) -> Device {
    let temporary = tempfile::tempdir().unwrap();
    let ids = SequentialIds::new();
    for _ in 1..n {
        ids.new_device_id();
    }
    let directory = StoreLayout::new(temporary.path().into())
        .create_store_dir(store, "Store", &ids)
        .unwrap();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let db = open(directory.clone(), clock.clone()).await;
    let custody = Arc::new(InMemoryCustody::new(StoreKeyring::new(
        StoreKey::generate(key(999)).unwrap(),
    )));
    custody.forget().unwrap();
    let sync = StoreLogSync::new(
        storage.clone(),
        db.clone(),
        custody.clone(),
        Arc::new(InMemoryCustody::new(member.clone())),
        clock.clone(),
        Arc::new(coven_foundation::id_source::UuidIds),
        directory.clone(),
    );
    Device {
        storage,
        sync,
        db,
        directory,
        custody,
        member,
        clock,
        _temporary: temporary,
    }
}

impl Device {
    fn writes(&self) -> crate::DeviceLogSync {
        crate::DeviceLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.custody.clone(),
            Arc::new(InMemoryCustody::new(self.member.clone())),
        )
    }

    async fn create(&mut self, key: KeyId) -> EntryId {
        self.sync
            .make_and_upload_entry(StoreChange::CreateStore {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                },
                store: self.directory.id(),
                name: "Store".into(),
                admin: public(&self.member),
                key,
                device_name: "first".into(),
            })
            .await
            .unwrap()
    }
    async fn add(&mut self, other: &MemberKeys, role: MemberRole) -> EntryId {
        self.sync
            .make_and_upload_entry(StoreChange::AddMember {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                },
                keys: public(other),
                role,
            })
            .await
            .unwrap()
    }
    async fn sync(&mut self) -> SyncResults {
        self.sync.sync_store_log().await.unwrap()
    }
    async fn log(&self) -> StoreLog {
        self.db.local_store_log().await.unwrap().log
    }
    async fn restart(&mut self, storage: Arc<MemoryStorage>) {
        self.db.close().await.unwrap();
        self.db = open(self.directory.clone(), self.clock.clone()).await;
        self.sync = StoreLogSync::new(
            storage,
            self.db.clone(),
            self.custody.clone(),
            Arc::new(InMemoryCustody::new(self.member.clone())),
            self.clock.clone(),
            self.sync.ids.clone(),
            self.directory.clone(),
        );
    }
    async fn device(&self) -> DeviceId {
        self.db.local_store_log().await.unwrap().device
    }
}

#[tokio::test]
async fn three_devices_publish_concurrent_changes_and_converge() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Admin).await;
    a.add(&c.member, MemberRole::Member).await;
    b.sync().await;
    c.sync().await;
    for (device, id) in [(&mut a, 10), (&mut b, 11), (&mut c, 12)] {
        device
            .sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle: circle(id),
                name: id.to_string(),
                key: key(id),
            })
            .await
            .unwrap();
    }
    for device in [&mut a, &mut b, &mut c] {
        assert!(device.sync().await.damaged_objects.is_empty());
    }
    assert_eq!(a.log().await, b.log().await);
    assert_eq!(b.log().await, c.log().await);
    assert_eq!(a.log().await.replay.state.circles.len(), 3);
}

#[tokio::test]
async fn missing_causal_entry_waits_in_storage_then_applies() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    let first = a.create(key(1)).await;
    b.sync().await;
    let second = a
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Circle".into(),
            key: key(2),
        })
        .await
        .unwrap();
    b.sync().await;
    let third = b
        .sync
        .make_and_upload_entry(StoreChange::RenameCircle {
            circle: circle(1),
            name: "Renamed".into(),
        })
        .await
        .unwrap();
    let missing_path = object::path(second);
    let bytes = storage.read(&missing_path).await.unwrap();
    storage.delete(&missing_path).await.unwrap();
    let mut c = device(storage.clone(), 3, member(1), store(1)).await;
    c.sync().await;
    assert_eq!(
        c.log()
            .await
            .entries
            .iter()
            .map(|e| e.entry.position)
            .collect::<Vec<_>>(),
        vec![first]
    );
    assert!(c.db.local_store_log().await.unwrap().upload.is_none());
    storage.create(&missing_path, &bytes).await.unwrap();
    c.sync().await;
    assert!(c.log().await.replay.entries.contains_key(&third));
    assert_eq!(c.log().await, b.log().await);
}

#[tokio::test]
async fn fixed_bytes_survive_restart_and_lost_create_reply() {
    for lost_reply in [false, true] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        a.create(key(1)).await;
        storage
            .set_faults(Faults {
                fail_next: usize::from(!lost_reply),
                lose_completion_reply: lost_reply,
                ..Faults::none()
            })
            .await;
        let result = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(20),
                name: "Other".into(),
            })
            .await;
        assert!(matches!(result, Err(SyncError::Storage(_))));
        let fixed = a.db.local_store_log().await.unwrap().upload.unwrap();
        assert_eq!(fixed.entry.position.number, 2);
        if lost_reply {
            assert_eq!(
                storage
                    .read(&object::path(fixed.entry.position))
                    .await
                    .unwrap(),
                fixed.sealed.bytes
            );
        }
        a.restart(storage.clone()).await;
        a.sync().await;
        assert_eq!(
            storage
                .read(&object::path(fixed.entry.position))
                .await
                .unwrap(),
            fixed.sealed.bytes
        );
        assert!(a.db.local_store_log().await.unwrap().upload.is_none());
        assert_eq!(a.log().await.entries.len(), 2);
    }
}

#[tokio::test]
async fn removal_rotates_keys_for_remaining_members_and_stops_removed_devices() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Member).await;
    a.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Shared".into(),
            key: key(2),
        })
        .await
        .unwrap();
    for who in [&b.member, &c.member] {
        a.sync
            .make_and_upload_entry(StoreChange::AddCircleMember {
                circle: circle(1),
                member: who.member_id(),
            })
            .await
            .unwrap();
    }
    b.sync().await;
    c.sync().await;
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member_id(),
            key: key(3),
            circle_keys: vec![CircleKeyId {
                circle: circle(1),
                key: key(4),
            }],
        })
        .await
        .unwrap();
    c.sync().await;
    assert!(matches!(
        b.sync.sync_store_log().await,
        Err(SyncFailure::Removed)
    ));
    for device in [&a, &c] {
        let ring = device.custody.unlock().unwrap().unwrap();
        assert!(ring.store_key(key(1)).is_ok());
        assert!(ring.store_key(key(3)).is_ok());
        assert!(ring.circle_key(circle(1), key(4)).is_ok());
    }
    for (audience, key) in [
        (Audience::Store, key(3)),
        (Audience::Circle(circle(1)), key(4)),
    ] {
        assert_eq!(
            storage
                .read(&keys::path(&audience, key, &b.member.member_id()))
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::NotFound
        );
    }
    assert_eq!(a.log().await, c.log().await);
    let mut joined = device(storage.clone(), 4, member(4), store(1)).await;
    a.add(&joined.member, MemberRole::Member).await;
    joined.sync().await;
    let ring = joined.custody.unlock().unwrap().unwrap();
    assert!(ring.store_key(key(1)).is_ok());
    assert!(ring.store_key(key(3)).is_ok());
    assert_eq!(a.log().await, joined.log().await);
}

#[tokio::test]
async fn own_device_removal_and_setup_race_stop_sync() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(2)).await;
    a.create(key(1)).await;
    b.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    b.create(key(2)).await;
    assert!(matches!(
        b.sync.sync_store_log().await,
        Err(SyncFailure::LocationTaken)
    ));
    assert!(a.sync().await.damaged_objects.is_empty());
    let own = a.device().await;
    assert!(matches!(
        a.sync
            .make_and_upload_entry(StoreChange::RemoveDevice { device: own })
            .await,
        Err(SyncError::Stopped(SyncFailure::Removed))
    ));
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
    assert!(matches!(
        a.sync.sync_store_log().await,
        Err(SyncFailure::Removed)
    ));
}

#[tokio::test]
async fn missing_or_damaged_sealed_keys_wait_without_advancing_the_entry() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let path = ObjectPath::store_key(key(1), &a.member.member_id());
    let original = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    assert!(b.sync().await.damaged_objects.is_empty());
    assert!(b.log().await.entries.is_empty());
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    storage.create(&path, &damaged).await.unwrap();
    let report = b.sync().await;
    assert_eq!(report.damaged_objects[0].path, path.as_str());
    assert!(matches!(
        report.damaged_objects[0].failure,
        ObjectCheckFailure::Decryption(_)
    ));
    assert!(b.log().await.entries.is_empty());
    storage.delete(&path).await.unwrap();
    let wrong = coven_crypto::seal_store_key(
        &StoreKey::generate(key(2)).unwrap(),
        &a.member.sealing_public_key(),
        path.as_str(),
    )
    .unwrap();
    storage.create(&path, &wrong).await.unwrap();
    assert!(matches!(
        b.sync().await.damaged_objects[0].failure,
        ObjectCheckFailure::Parse(_)
    ));
    storage.delete(&path).await.unwrap();
    storage.create(&path, &original).await.unwrap();
    assert!(b.sync().await.damaged_objects.is_empty());
    assert_eq!(a.log().await, b.log().await);
}

#[tokio::test]
async fn dropped_concurrent_role_change_is_reported_to_its_author() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Admin).await;
    b.sync().await;
    c.sync().await;
    let lost = a
        .sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: b.member.member_id(),
            role: MemberRole::Admin,
        })
        .await
        .unwrap();
    let winner = c
        .sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: b.member.member_id(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let report = a.sync().await;
    assert_eq!(
        report.dropped_entries,
        vec![DroppedEntry {
            entry: lost,
            change: crate::StoreLogChange::SetMemberRole {
                member: b.member.member_id(),
                role: MemberRole::Admin
            },
            reason: coven_database::DropReason::BeatenBy(winner)
        }]
    );
    assert!(b.sync().await.dropped_entries.is_empty());
    assert!(c.sync().await.dropped_entries.is_empty());
    assert_eq!(a.log().await, b.log().await);
    assert_eq!(b.log().await, c.log().await);
}

#[tokio::test]
async fn future_entry_waits_and_a_restored_device_starts_at_one() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(302));
    let future = a
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(50),
            name: "future".into(),
        })
        .await
        .unwrap();
    assert!(b.sync().await.damaged_objects.is_empty());
    assert!(!b.log().await.replay.entries.contains_key(&future));
    b.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    b.sync().await;
    assert!(b.log().await.replay.entries.contains_key(&future));
    let id = b.device().await;
    let restored = b
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: id,
            name: "restored".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        restored,
        EntryId {
            device: id,
            number: 1
        }
    );
}

#[tokio::test]
async fn key_objects_are_fixed_and_published_before_the_entry() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    let change = StoreChange::CreateStore {
        access: coven_format::MemberAccess::S3AccessKey {
            access_key_id: "fixture-access-key".into(),
        },
        store: store(1),
        name: "Store".into(),
        admin: public(&a.member),
        key: key(1),
        device_name: "First".into(),
    };
    assert!(matches!(
        a.sync.make_and_upload_entry(change).await,
        Err(SyncError::Storage(_))
    ));
    let pending = a.db.local_store_log().await.unwrap().upload.unwrap();
    assert!(storage
        .list(&ObjectPrefix::store_logs())
        .await
        .unwrap()
        .is_empty());
    assert!(a.custody.unlock().unwrap().is_none());
    let sealed_key = &pending.sealed.keys[0];
    assert_eq!(
        storage
            .read(&ObjectPath::parse(&sealed_key.path).unwrap())
            .await
            .unwrap(),
        sealed_key.bytes
    );
    a.restart(storage.clone()).await;
    a.sync().await;
    assert_eq!(
        storage
            .read(&object::path(pending.entry.position))
            .await
            .unwrap(),
        pending.sealed.bytes
    );
    assert_eq!(
        storage
            .read(&ObjectPath::parse(&sealed_key.path).unwrap())
            .await
            .unwrap(),
        sealed_key.bytes
    );
    assert!(a
        .custody
        .unlock()
        .unwrap()
        .unwrap()
        .store_key(key(1))
        .is_ok());
}

#[tokio::test]
async fn outside_admin_does_not_keep_circle_key_and_noop_keeps_named_store_key() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Member).await;
    b.sync().await;
    b.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Private".into(),
            key: key(2),
        })
        .await
        .unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle: circle(1),
            member: c.member.member_id(),
        })
        .await
        .unwrap();
    a.sync().await;
    c.sync().await;
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member_id(),
            key: key(3),
            circle_keys: vec![CircleKeyId {
                circle: circle(1),
                key: key(4),
            }],
        })
        .await
        .unwrap();
    c.sync().await;
    assert!(a
        .custody
        .unlock()
        .unwrap()
        .unwrap()
        .circle_key(circle(1), key(4))
        .is_err());
    assert!(c
        .custody
        .unlock()
        .unwrap()
        .unwrap()
        .circle_key(circle(1), key(4))
        .is_ok());
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member_id(),
            key: key(5),
            circle_keys: vec![],
        })
        .await
        .unwrap();
    c.sync().await;
    for device in [&a, &c] {
        assert_eq!(device.log().await.replay.state.store.unwrap().key, key(3));
        assert!(device
            .custody
            .unlock()
            .unwrap()
            .unwrap()
            .store_key(key(5))
            .is_ok());
    }
}

use crate::replay::tests as history;
use coven_format::store_log::MemberRole::{Admin, Member};

fn examples() -> Vec<(history::History, usize)> {
    let mut cases = Vec::new();
    let mut h = history::household(Member, Member).prefix(2);
    h.push(1, 1, &[0, 1], history::device(2));
    h.push(0, 3, &[0, 1], history::role(1, Admin));
    cases.push((h, 2));
    let mut h = history::household(Admin, Member).prefix(4);
    h.push(0, 0, &[0, 1, 2, 3], history::add(3, Member));
    h.push(1, 1, &[0, 1, 2, 3], history::role(2, Admin));
    cases.push((h, 4));
    let mut h = history::household(Admin, Member).prefix(3);
    h.push(1, 4, &[0, 1, 2], history::device(4));
    h.push(0, 0, &[0, 1, 2], history::remove(1, &[]));
    cases.push((h, 3));
    let mut h = history::household(Member, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], history::role(1, Admin));
    h.push(2, 2, &[0, 1, 2, 3, 4], history::role(1, Member));
    cases.push((h, 5));
    let mut h = history::household(Admin, Member).prefix(3);
    h.push(0, 0, &[0, 1, 2], history::remove(1, &[]));
    h.push(1, 1, &[0, 1, 2], history::remove(0, &[]));
    cases.push((h, 3));
    let mut h = history::household(Admin, Member).prefix(3);
    h.all(0, 0, history::add(3, Member));
    h.push(0, 0, &[0, 1, 2, 3], history::add(2, Member));
    h.push(1, 1, &[0, 1, 2, 3], history::remove(3, &[]));
    cases.push((h, 4));
    let mut h = history::household(Admin, Member).prefix(3);
    h.push(0, 0, &[0, 1, 2], history::add(3, Member));
    h.push(1, 1, &[0, 1, 2], history::add(3, Member));
    cases.push((h, 3));
    let mut h = history::household(Admin, Admin);
    h.push(0, 0, &[0, 1, 2, 3, 4], history::remove(1, &[]));
    h.push(1, 1, &[0, 1, 2, 3, 4], history::remove(2, &[]));
    h.push(2, 2, &[0, 1, 2, 3, 4], history::remove(0, &[]));
    cases.push((h, 5));
    let mut h = history::losing_removal();
    h.push(1, 1, &[0, 1, 2, 3], history::remove(0, &[]));
    h.push(0, 0, &[0, 1, 2, 3], history::remove(1, &[]));
    h.push(1, 4, &[0, 1, 2, 3], history::device(5));
    cases.push((h, 4));
    let mut h = history::losing_removal();
    h.push(0, 0, &[0, 1, 2, 3], history::add(2, Admin));
    h.push(1, 1, &[0, 1, 2, 3], history::role(1, Member));
    h.push(1, 4, &[0, 1, 2, 3], history::remove(0, &[]));
    cases.push((h, 4));
    let mut h = history::gifts();
    let past: Vec<_> = (0..h.entries.len()).collect();
    let prefix = past.len();
    h.push(0, 0, &past, history::leave(0, 1));
    h.push(0, 3, &past, history::remove(0, &[0]));
    cases.push((h, prefix));
    let mut h = history::gifts();
    let past: Vec<_> = (0..h.entries.len()).collect();
    h.push(0, 0, &past, history::leave(0, 1));
    h.push(1, 1, &past, history::delete(0));
    cases.push((h, past.len()));
    cases
}

fn permutations(values: &mut [usize], start: usize, out: &mut Vec<Vec<usize>>) {
    if start == values.len() {
        out.push(values.to_vec());
        return;
    }
    for index in start..values.len() {
        values.swap(start, index);
        permutations(values, start + 1, out);
        values.swap(start, index);
    }
}

#[tokio::test]
async fn section_nine_examples_converge_for_every_concurrent_upload_order() {
    for (case, (history, prefix)) in examples().into_iter().enumerate() {
        let expected = crate::replay(&history.entries);
        let mut orders = Vec::new();
        permutations(
            &mut (prefix..history.entries.len()).collect::<Vec<_>>(),
            0,
            &mut orders,
        );
        for order in orders {
            let storage = storage();
            // These observers hold a fixture's shared key and are not members
            // targeted by the history, so §10's stopping rule cannot end replay.
            let mut left = device(storage.clone(), 98, member(20), store(1)).await;
            let mut right = device(storage.clone(), 99, member(21), store(1)).await;
            let key = StoreKey::generate(key(1)).unwrap();
            for observer in [&left, &right] {
                observer
                    .custody
                    .persist(&StoreKeyring::new(key.clone()))
                    .unwrap();
            }
            // Older store keys remain valid decryption keys. These transport
            // fixtures use one held key; authoring/key-rotation tests use the owner.
            let objects: Vec<_> = history
                .entries
                .iter()
                .map(|entry| {
                    let author = (1..=4)
                        .map(member)
                        .find(|m| m.member_id() == entry.author)
                        .unwrap();
                    (
                        object::path(entry.position),
                        object::seal(entry, &key, &author).unwrap(),
                    )
                })
                .collect();
            for (path, bytes) in &objects[..prefix] {
                storage.create(path, bytes).await.unwrap();
            }
            left.sync().await;
            right.sync().await;
            for index in &order {
                let (path, bytes) = &objects[*index];
                storage.create(path, bytes).await.unwrap();
                let report = left.sync().await;
                assert!(
                    report.damaged_objects.is_empty(),
                    "case {case}, {order:?}: {report:?}"
                );
            }
            right.sync().await;
            let left = left.log().await;
            let right = right.log().await;
            assert_eq!(left.replay, expected, "case {case}, {order:?}");
            assert_eq!(left, right, "case {case}, {order:?}");
        }
    }
}

#[tokio::test]
async fn only_newer_envelopes_require_an_update() {
    for sealed_key in [false, true] {
        for version in [0, 2] {
            let storage = storage();
            let mut a = device(storage.clone(), 1, member(1), store(1)).await;
            let first = a.create(key(1)).await;
            let path = if sealed_key {
                ObjectPath::store_key(key(1), &a.member.member_id())
            } else {
                object::path(first)
            };
            let mut bytes = storage.read(&path).await.unwrap();
            bytes[2] = version;
            storage.delete(&path).await.unwrap();
            storage.create(&path, &bytes).await.unwrap();
            let mut b = device(storage.clone(), 2, member(1), store(1)).await;
            let result = b.sync.sync_store_log().await;
            if version == 2 {
                assert!(matches!(result, Err(SyncFailure::UpdateRequired)));
            } else {
                assert_eq!(result.unwrap().damaged_objects[0].path, path.as_str());
            }
        }
    }
}

#[tokio::test]
async fn changing_custody_cannot_change_a_devices_author() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Admin).await;
    a.sync.member_keys = Arc::new(InMemoryCustody::new(member(2)));
    let result = a
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(90),
            name: "different author".into(),
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[path = "store_log_object_tests.rs"]
mod objects;

#[path = "store_log_keys_tests.rs"]
mod key_distribution;

#[path = "operations_tests.rs"]
mod operations;

#[path = "snapshots_tests.rs"]
mod snapshots;

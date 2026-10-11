use super::*;
use coven_crypto::{custody::InMemoryCustody, MemberKeys, StoreKey};
use coven_database::{DatabaseBuilder, EntryOutcome, Migration, SyncedTable};
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
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

#[cfg(test)]
pub(crate) struct Device {
    pub(crate) storage: Arc<MemoryStorage>,
    pub(crate) sync: StoreLogSync,
    pub(crate) writes: crate::DeviceLogSync,
    pub(crate) db: Database,
    pub(crate) directory: StoreDir,
    pub(crate) custody: Arc<KeySession<StoreKeyring>>,
    pub(crate) identity: Arc<KeySession<MemberKeys>>,
    pub(crate) member: MemberKeys,
    pub(crate) clock: Arc<FixedClock>,
    pub(crate) ids: coven_foundation::id_source::IdSourceRef,
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
    Arc::new(MemoryStorage::builder().build().unwrap())
}
async fn device(storage: Arc<MemoryStorage>, n: u64, member: MemberKeys, store: StoreId) -> Device {
    Device::new(
        storage,
        n,
        member,
        store,
        vec![],
        vec![],
        Arc::new(coven_foundation::id_source::UuidIds),
    )
    .await
}

impl Device {
    pub(crate) async fn new(
        storage: Arc<MemoryStorage>,
        n: u64,
        member: MemberKeys,
        store: StoreId,
        tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
        ids: coven_foundation::id_source::IdSourceRef,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let directory_ids = SequentialIds::new();
        for _ in 1..n {
            directory_ids.new_device_id();
        }
        let directory = StoreLayout::new(temporary.path().into())
            .create_store_dir(store, "Store", &directory_ids)
            .unwrap();
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
        let db = DatabaseBuilder::new(directory.clone())
            .synced_tables(tables)
            .migrations(migrations)
            .clock(clock.clone())
            .open()
            .await
            .unwrap();
        let custody = Arc::new(KeySession::store(Arc::new(InMemoryCustody::empty())).unwrap());
        let identity =
            Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(member.clone()))).unwrap());
        let writes = crate::DeviceLogSync::new(
            storage.clone(),
            db.clone(),
            custody.clone(),
            identity.clone(),
        );
        let sync = StoreLogSync::new(
            storage.clone(),
            db.clone(),
            custody.clone(),
            identity.clone(),
            clock.clone(),
            ids.clone(),
            directory.clone(),
        );
        Self {
            storage,
            sync,
            writes,
            db,
            directory,
            custody,
            identity,
            member,
            clock,
            ids,
            _temporary: temporary,
        }
    }

    pub(crate) async fn reopen(
        &mut self,
        storage: Arc<MemoryStorage>,
        tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
    ) {
        self.db.close().await.unwrap();
        self.db = DatabaseBuilder::new(self.directory.clone())
            .synced_tables(tables)
            .migrations(migrations)
            .clock(self.clock.clone())
            .open()
            .await
            .unwrap();
        self.storage = storage;
        self.writes = self.writes();
        self.sync = StoreLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.custody.clone(),
            self.identity.clone(),
            self.clock.clone(),
            self.ids.clone(),
            self.directory.clone(),
        );
    }

    pub(crate) async fn operation(
        &self,
        id: coven_database::OperationId,
    ) -> coven_database::OperationRecord {
        self.db
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.id == id)
            .expect("pending operation")
    }

    pub(crate) async fn step(
        &mut self,
        id: coven_database::OperationId,
    ) -> Result<crate::operations::Progress, SyncError> {
        let row = self.operation(id).await;
        let data = crate::operation_data::Data::read(&row)?;
        self.sync.operation_step(&row, data).await
    }

    fn reseal(&self, upload: &coven_database::StoreLogUpload) -> Vec<u8> {
        object::seal_upload(upload, self.custody.read().unwrap().as_ref(), &self.member).unwrap()
    }

    fn writes(&self) -> crate::DeviceLogSync {
        crate::DeviceLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.custody.clone(),
            self.identity.clone(),
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
    async fn sync(&mut self) {
        self.sync.sync_store_log().await.unwrap();
    }
    async fn log(&self) -> StoreLog {
        self.db.local_store_log().await.unwrap().log
    }
    async fn restart(&mut self, storage: Arc<MemoryStorage>) {
        self.reopen(storage, vec![], vec![]).await;
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
        device.sync().await;
    }
    assert_eq!(a.log().await, b.log().await);
    assert_eq!(b.log().await, c.log().await);
    assert_eq!(a.log().await.replay.state.circles.len(), 3);
}

#[tokio::test]
async fn far_future_entry_waits_for_its_missing_causal_entry_then_applies() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    let first = a.create(key(1)).await;
    b.sync().await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
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
        let expected = a.reseal(&fixed);
        assert_eq!(fixed.format, coven_format::FormatVersion::V1);
        assert_eq!(&expected[..3], &[33, 0, 1]);
        assert_eq!(fixed.entry.position.number, 2);
        if lost_reply {
            assert_eq!(
                storage
                    .read(&object::path(fixed.entry.position))
                    .await
                    .unwrap(),
                expected
            );
        }
        a.restart(storage.clone()).await;
        let reopened = a.db.local_store_log().await.unwrap().upload.unwrap();
        assert_eq!(reopened, fixed);
        assert_eq!(a.reseal(&reopened), expected);
        a.sync().await;
        assert_eq!(
            storage
                .read(&object::path(fixed.entry.position))
                .await
                .unwrap(),
            expected
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
        let ring = device.custody.read().unwrap().unwrap();
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
    let ring = joined.custody.read().unwrap().unwrap();
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
    a.sync().await;
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
async fn far_future_entry_waits_for_missing_or_damaged_sealed_keys() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    a.create(key(1)).await;
    let path = ObjectPath::store_key(key(1), &a.member.member_id());
    let original = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    assert!(b.log().await.entries.is_empty());
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    storage.create(&path, &damaged).await.unwrap();
    let damages = b.sync.step().await.unwrap();
    assert_eq!(damages[0].path, path.as_str());
    assert!(matches!(
        damages[0].failure,
        Refusal::Decryption { cause: Some(_) }
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
        b.sync.step().await.unwrap()[0].failure,
        Refusal::WrongIdentity { cause: None }
    ));
    storage.delete(&path).await.unwrap();
    storage.create(&path, &original).await.unwrap();
    b.sync().await;
    assert_eq!(a.log().await, b.log().await);
}

#[tokio::test]
async fn far_future_entry_applies_and_a_restored_device_stamps_after_it() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    let future = a
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(50),
            name: "future".into(),
        })
        .await
        .unwrap();
    b.sync().await;
    assert!(b.log().await.replay.entries.contains_key(&future));
    b.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    b.restart(storage.clone()).await;
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
    let log = b.log().await;
    let timestamp = |id| {
        log.entries
            .iter()
            .find(|e| e.entry.position == id)
            .unwrap()
            .entry
            .timestamp
    };
    assert!(timestamp(restored) > timestamp(future));
    a.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    a.sync().await;
    assert_eq!(a.log().await, log);
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
    assert_eq!(pending.format, coven_format::FormatVersion::V1);
    assert!(storage
        .list(&ObjectPrefix::store_logs())
        .await
        .unwrap()
        .is_empty());
    assert!(a.custody.read().unwrap().is_none());
    let expected = a.reseal(&pending);
    assert!(matches!(
        object::seal_upload(&pending, None, &member(2)),
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    let sealed_key = &pending.sealing.keys[0];
    assert_eq!(
        storage
            .read(&ObjectPath::parse(&sealed_key.path).unwrap())
            .await
            .unwrap(),
        sealed_key.bytes
    );
    a.restart(storage.clone()).await;
    let reopened = a.db.local_store_log().await.unwrap().upload.unwrap();
    assert_eq!(reopened, pending);
    assert_eq!(a.reseal(&reopened), expected);
    a.sync().await;
    assert_eq!(
        storage
            .read(&object::path(pending.entry.position))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        storage
            .read(&ObjectPath::parse(&sealed_key.path).unwrap())
            .await
            .unwrap(),
        sealed_key.bytes
    );
    assert!(a.custody.read().unwrap().unwrap().store_key(key(1)).is_ok());
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
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle(1), key(4))
        .is_err());
    assert!(c
        .custody
        .read()
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
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(5))
            .is_ok());
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
            let original = storage.read(&path).await.unwrap();
            let mut bytes = original.clone();
            bytes[2] = version;
            storage.delete(&path).await.unwrap();
            storage.create(&path, &bytes).await.unwrap();
            let mut b = device(storage.clone(), 2, member(1), store(1)).await;
            let result = b.sync.sync_store_log().await;
            if version == 2 {
                assert!(matches!(result, Err(SyncFailure::UpdateRequired)));
            } else {
                let damage = result.unwrap();
                if sealed_key {
                    assert_eq!(damage[0].path, path.as_str());
                } else {
                    assert!(damage.is_empty());
                    assert_eq!(
                        b.db.stuck_logs().await.unwrap()[0].record.object,
                        coven_format::stuck::LogObject::Entry(first)
                    );
                }
            }
            assert!(b.log().await.entries.is_empty());
            assert!(b.db.operations().await.unwrap().is_empty());
            assert!(storage
                .list(&ObjectPrefix::snapshots())
                .await
                .unwrap()
                .is_empty());
            assert_eq!(storage.read(&path).await.unwrap(), bytes);
            storage.delete(&path).await.unwrap();
            storage.create(&path, &original).await.unwrap();
            b.sync().await;
            if !sealed_key && version == 0 {
                assert!(b.log().await.entries.is_empty());
            } else {
                assert_eq!(b.log().await, a.log().await);
            }
            assert!(b.db.operations().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn changing_custody_cannot_change_a_devices_author() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Admin).await;
    a.sync.member_keys =
        Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(member(2)))).unwrap());
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
pub(crate) mod operations;

#[path = "snapshots_tests.rs"]
mod snapshots;

#[tokio::test]
async fn a_waiting_entry_reseals_with_its_recorded_key_after_rotation() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Member).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(a
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(20),
            name: "waiting".into(),
        })
        .await
        .is_err());
    let pending = a.db.local_store_log().await.unwrap().upload.unwrap();
    let path = object::path(pending.entry.position);
    let first = storage.read(&path).await.unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member(2).member_id(),
            key: key(2),
            circle_keys: vec![],
        })
        .await
        .unwrap();
    // Apply the downloaded rotation while the local entry is still queued.
    let rotation = b.log().await.entries.last().unwrap().entry.clone();
    let (entry, replay) = crate::replay_entry(&a.log().await, rotation);
    a.db.apply_store_log(entry, replay).await.unwrap();
    a.custody
        .persist(&b.custody.read().unwrap().unwrap())
        .unwrap();
    a.restart(storage.clone()).await;
    assert_eq!(a.log().await.replay.state.store.unwrap().key, key(2));
    assert_eq!(a.reseal(&pending), first);
    // Remove the fixture's first copy so the retry must actually reproduce it.
    storage.delete(&path).await.unwrap();
    a.sync().await;
    assert_eq!(storage.read(&path).await.unwrap(), first);
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
}

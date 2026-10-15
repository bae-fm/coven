use super::*;
use crate::retry_waits::tests::connected;
use coven_crypto::{custody::InMemoryCustody, MemberIdentity, MemberKeys, StoreKey};
use coven_database::{DatabaseBuilder, EntryOutcome, Migration, SyncedTable};
use coven_format::sealed_single::SingleChunkObject;
use coven_format::store_log::{MemberPublicKeys, MemberRole};
use coven_foundation::{
    clock::FixedClock,
    files::{StoreDir, StoreLayout},
    id_source::{CircleId, DeviceId, IdSource, SequentialIds, StoreId},
};
use coven_storage::ObjectPrefix;
use coven_storage::{
    test_utils::{Faults, MemoryRequest, MemoryStorage},
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
    pub(crate) identity: Arc<KeySession<MemberIdentity>>,
    pub(crate) member: MemberIdentity,
    pub(crate) clock: Arc<FixedClock>,
    pub(crate) send: Arc<crate::SendGate>,
    /// The retry waits `sync` and `storage` share, as the handle composes them.
    pub(crate) waits: Arc<crate::RetryWaits>,
    pub(crate) ids: coven_foundation::id_source::IdSourceRef,
    pub(crate) _temporary: tempfile::TempDir,
}

pub(crate) fn key(n: u128) -> KeyId {
    KeyId(Uuid::from_u128(n))
}
pub(crate) fn circle(n: u128) -> CircleId {
    CircleId(Uuid::from_u128(n))
}
pub(crate) fn store(n: u128) -> StoreId {
    StoreId(Uuid::from_u128(n))
}
/// Member `n`: a stable id and a private pair, both from seed `n` but
/// independent of each other (§9).
pub(crate) fn member(n: u8) -> MemberIdentity {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([n; 64]);
    MemberIdentity::new(
        coven_foundation::id_source::MemberId(Uuid::from_bytes([n; 16])),
        MemberKeys::from_secret_bytes(&bytes).unwrap(),
    )
}
/// S3 keys this device lists as pending deletion, by key id (§13).
pub(crate) async fn access_keys(db: &coven_database::Database) -> Vec<crate::AccessKeyToDelete> {
    let mut keys: Vec<_> = db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|record| match record.reason {
            crate::PendingReason::DeleteAccessKey(key) => Some(key),
            _ => None,
        })
        .collect();
    keys.sort_by(|a, b| a.access_key_id.cmp(&b.access_key_id));
    keys
}
/// The pending deletion of a console-made key recorded for a member.
pub(crate) fn console_key(id: &str, member: Option<&MemberIdentity>) -> crate::AccessKeyToDelete {
    crate::AccessKeyToDelete {
        access_key_id: id.into(),
        principal: coven_format::S3Principal::Console,
        member: member.map(MemberIdentity::member),
    }
}
pub(crate) fn public(member: &MemberIdentity) -> MemberPublicKeys {
    member.keys().public_keys()
}
pub(crate) fn storage() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::builder().build().unwrap())
}
pub(crate) async fn find_copy(
    storage: &dyn Storage,
    audience: Audience,
    key: KeyId,
    member: &coven_foundation::id_source::MemberId,
) -> Option<keys::ObservedCopy> {
    for object in storage.list(&ObjectPrefix::keys(store(1))).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        // Tests also plant malformed copies; inspection skips them.
        let Ok(sealed) = SealedKey::decode(&bytes) else {
            continue;
        };
        if sealed.prefix().audience == audience
            && sealed.prefix().key == key
            && &sealed.prefix().member == member
        {
            return Some(keys::ObservedCopy {
                path: object.path,
                prefix: sealed.prefix().clone(),
                bytes,
            });
        }
    }
    None
}

async fn copy_path(
    storage: &dyn Storage,
    audience: Audience,
    key: KeyId,
    member: &coven_foundation::id_source::MemberId,
) -> ObjectPath {
    find_copy(storage, audience, key, member)
        .await
        .expect("stored copy")
        .path
}

pub(crate) async fn device(
    storage: Arc<MemoryStorage>,
    n: u64,
    member: MemberIdentity,
    store: StoreId,
) -> Device {
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

impl StoreLogSync {
    pub(crate) fn test_track_received_entries(
        &mut self,
        counter: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.received_entry_decodes = Some(counter);
    }

    pub(crate) async fn test_rotate(&mut self, audience: Audience, key: KeyId) -> EntryId {
        let key_hash = self.test_key(audience.clone(), key);
        self.make_and_upload_entry(StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        })
        .await
        .unwrap()
    }

    pub(crate) fn test_key(&self, audience: Audience, id: KeyId) -> [u8; 32] {
        let mut ring = self.store_keys.read().unwrap();
        let hash = match audience {
            Audience::Store => {
                if !keys::holds(ring.as_ref(), &audience, id) {
                    let key = StoreKey::generate(id).unwrap();
                    match &mut ring {
                        Some(ring) => ring.insert_store_key(key).unwrap(),
                        None => ring = Some(StoreKeyring::new(key)),
                    }
                }
                ring.as_ref().unwrap().store_key(id).unwrap().hash()
            }
            Audience::Circle(circle) => {
                let ring = ring.as_mut().unwrap();
                if !ring.circle_key_ids(circle).any(|key| key == id) {
                    ring.insert_circle_key(coven_crypto::CircleKey::generate(circle, id).unwrap())
                        .unwrap();
                }
                ring.circle_key(circle, id).unwrap().hash()
            }
        };
        self.store_keys.persist(ring.as_ref().unwrap()).unwrap();
        hash
    }
}

impl Device {
    pub(crate) async fn interrupt_entry(&mut self, number: u64) {
        self.fail_entry(number, false).await;
    }

    /// Store this device's next attempt at entry `number` but lose its reply.
    pub(crate) async fn lose_entry_reply(&mut self, number: u64) {
        self.fail_entry(number, true).await;
    }

    async fn fail_entry(&mut self, number: u64, lost_reply: bool) {
        let device = self.db.local_store_log().await.unwrap().device;
        self.sync.storage = Some(key_distribution::failing_creation(
            self.storage.clone(),
            object::path(self.directory.id(), EntryId { device, number }),
            lost_reply,
        ));
    }

    /// Let the armed retry delays elapse, as before a later automatic
    /// attempt (§16.5). Only that long passes, so a catch-up stays fresh.
    pub(crate) fn elapse_retry_delays(&self) {
        crate::retry_waits::tests::elapse_retry_delays(&self.waits, &self.clock);
    }

    /// Undo [`Self::interrupt_entry`], reconnecting to the shared storage.
    pub(crate) fn restore_storage(&mut self) {
        self.sync.storage = Some(self.storage.clone());
    }

    pub(crate) async fn new(
        storage: Arc<MemoryStorage>,
        n: u64,
        member: MemberIdentity,
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
        let send = Arc::new(crate::SendGate::new(clock.clone()));
        let (waits, storage) = connected(&storage, &db, &clock, &send).await;
        let writes = crate::DeviceLogSync::new(
            storage.clone(),
            db.clone(),
            custody.clone(),
            identity.clone(),
            clock.clone(),
            send.clone(),
        );
        let sync = StoreLogSync::new(
            storage.clone(),
            db.clone(),
            custody.clone(),
            identity.clone(),
            clock.clone(),
            send.clone(),
            ids.clone(),
            directory.clone(),
            waits.clone(),
            crate::RestoreCodeCustody::Unkept,
        );
        Self {
            waits,
            storage,
            sync,
            writes,
            db,
            directory,
            custody,
            identity,
            member,
            clock,
            send,
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
        // A test may already have closed it, e.g. to copy the closed files.
        match self.db.close().await {
            Ok(()) | Err(coven_database::DbError::StoreClosed) => {}
            Err(error) => panic!("closing before reopening failed: {error}"),
        }
        self.db = DatabaseBuilder::new(self.directory.clone())
            .synced_tables(tables)
            .migrations(migrations)
            .clock(self.clock.clone())
            .open()
            .await
            .unwrap();
        self.send = Arc::new(crate::SendGate::new(self.clock.clone()));
        let waits;
        (waits, self.storage) = connected(&storage, &self.db, &self.clock, &self.send).await;
        self.writes = self.writes();
        self.sync = StoreLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.custody.clone(),
            self.identity.clone(),
            self.clock.clone(),
            self.send.clone(),
            self.ids.clone(),
            self.directory.clone(),
            waits.clone(),
            crate::RestoreCodeCustody::Unkept,
        );
        self.waits = waits;
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
        if !self.send.can_send() {
            self.sync.scan_store_log_once(true).await?;
        }
        // Each step is its own drive: request checks list afresh.
        self.sync.forget_join_requests();
        let row = self.operation(id).await;
        let data = crate::operation_data::Data::read(&row)?;
        self.sync.operation_step(&row, data).await
    }

    /// The bytes every attempt of a queued entry sends, signed with the key
    /// its recorded past binds (§10).
    pub(crate) async fn reseal(&self, upload: &coven_database::StoreLogUpload) -> Vec<u8> {
        let log = self.db.local_store_log().await.unwrap().log;
        object::seal_upload(
            self.directory.id(),
            &log,
            upload,
            self.custody.read().unwrap().as_ref(),
            &self.member,
        )
        .unwrap()
    }

    pub(crate) fn writes(&self) -> crate::DeviceLogSync {
        crate::DeviceLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.custody.clone(),
            self.identity.clone(),
            self.clock.clone(),
            self.send.clone(),
        )
    }

    pub(crate) async fn create(&mut self, key: KeyId) -> EntryId {
        self.sync
            .make_and_upload_entry(StoreChange::CreateStore {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                    principal: coven_format::S3Principal::Console,
                },
                store: self.directory.id(),
                name: "Store".into(),
                admin: public(&self.member),
                key,
                key_hash: self.sync.test_key(coven_merge::Audience::Store, key),
                device_name: "first".into(),
            })
            .await
            .unwrap()
    }
    pub(crate) async fn add(&mut self, other: &MemberIdentity, role: MemberRole) -> EntryId {
        self.sync
            .make_and_upload_entry(StoreChange::AddMember {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                    principal: coven_format::S3Principal::Console,
                },
                member: other.member(),
                keys: public(other),
                role,
            })
            .await
            .unwrap()
    }
    pub(crate) async fn sync(&mut self) {
        self.sync.sync_store_log().await.unwrap();
    }
    pub(crate) async fn catch_up(&self) {
        self.sync.scan_store_log_once(true).await.unwrap();
    }
    /// Enter a pass and complete its write catch-up, as SP step 6 does, so
    /// operations driven while the returned scope lives can prepare raise
    /// snapshots (§15).
    pub(crate) async fn caught_up_pass(&mut self) -> crate::pass_reads::ReadScope {
        let pass = self.sync.begin_pass(&mut self.writes);
        self.sync.catch_up_writes(&mut self.writes).await.unwrap();
        pass
    }
    /// Publish a requested snapshot after this device's own fresh catch-up (§15).
    pub(crate) async fn write_snapshot(
        &mut self,
        audience: Audience,
    ) -> Result<coven_format::store_log::SnapshotId, SyncError> {
        self.sync.write_snapshot(&mut self.writes, audience).await
    }
    /// SP step 8 of two passes: the first requests growth snapshots, which
    /// wait for the next pass's catch-up, sampled fresh, to prepare (§15).
    pub(crate) async fn write_snapshots(&mut self) -> Result<bool, SyncError> {
        self.sync.write_snapshots().await?;
        let _pass = self.caught_up_pass().await;
        self.sync.write_snapshots().await
    }
    pub(crate) async fn log(&self) -> StoreLog {
        self.db.local_store_log().await.unwrap().log
    }
    /// The newest acceptable introduction of an audience in the applied log.
    pub(crate) async fn newest(&self, audience: Audience) -> KeyId {
        crate::key_introductions::newest(&self.log().await, &audience).expect("introduced audience")
    }
    pub(crate) async fn restart(&mut self, storage: Arc<MemoryStorage>) {
        self.reopen(storage, vec![], vec![]).await;
    }
    pub(crate) async fn device(&self) -> DeviceId {
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
    b.sync.load_new_device("Ben", None).await.unwrap();
    c.sync.load_new_device("Carol", None).await.unwrap();
    for device in [&mut a, &mut b, &mut c] {
        device.sync().await;
    }
    for (device, id) in [(&mut a, 10), (&mut b, 11), (&mut c, 12)] {
        device
            .sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle: circle(id),
                name: id.to_string(),
                key: key(id),
                key_hash: device
                    .sync
                    .test_key(coven_merge::Audience::Circle(circle(id)), key(id)),
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
    a.catch_up().await;
    let second = a
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Circle".into(),
            key: key(2),
            key_hash: a
                .sync
                .test_key(coven_merge::Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    b.sync().await;
    b.sync.load_new_device("second", None).await.unwrap();
    let third = b
        .sync
        .make_and_upload_entry(StoreChange::RenameCircle {
            circle: circle(1),
            name: "Renamed".into(),
        })
        .await
        .unwrap();
    let missing_path = object::path(store(1), second);
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
        a.sync.storage = Some(key_distribution::failing_creation(
            storage.clone(),
            ObjectPath::store_log(store(1), DeviceId(1), 2.try_into().unwrap()),
            lost_reply,
        ));
        let result = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(20),
                name: "Other".into(),
                replaces: None,
            })
            .await;
        assert!(matches!(result, Err(SyncError::Storage(_))));
        let fixed = a.db.local_store_log().await.unwrap().upload.unwrap();
        let expected = a.reseal(&fixed).await;
        assert_eq!(fixed.format, coven_format::FormatVersion::V1);
        assert_eq!(&expected[..3], &[33, 0, 1]);
        assert_eq!(fixed.entry.position.number, 2);
        if lost_reply {
            assert_eq!(
                storage
                    .read(&object::path(store(1), fixed.entry.position))
                    .await
                    .unwrap(),
                expected
            );
        }
        a.restart(storage.clone()).await;
        let reopened = a.db.local_store_log().await.unwrap().upload.unwrap();
        assert_eq!(reopened, fixed);
        assert_eq!(a.reseal(&reopened).await, expected);
        a.sync().await;
        assert_eq!(
            storage
                .read(&object::path(store(1), fixed.entry.position))
                .await
                .unwrap(),
            expected
        );
        assert!(a.db.local_store_log().await.unwrap().upload.is_none());
        assert_eq!(a.log().await.entries.len(), 2);
    }
}

#[tokio::test]
async fn removal_then_rotations_share_keys_only_with_remaining_members() {
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
            key_hash: a
                .sync
                .test_key(coven_merge::Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    for who in [&b.member, &c.member] {
        a.sync
            .make_and_upload_entry(StoreChange::AddCircleMember {
                circle: circle(1),
                member: who.member(),
            })
            .await
            .unwrap();
    }
    b.sync().await;
    c.sync().await;
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member(),
        })
        .await
        .unwrap();
    assert_eq!(a.newest(Audience::Store).await, key(1));
    assert_eq!(a.newest(Audience::Circle(circle(1))).await, key(2));
    a.sync.test_rotate(Audience::Store, key(3)).await;
    a.sync
        .test_rotate(Audience::Circle(circle(1)), key(4))
        .await;
    c.sync().await;
    assert!(matches!(
        b.sync.sync_store_log().await,
        Err(SyncError::Removed)
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
        assert!(
            find_copy(storage.as_ref(), audience, key, &b.member.member())
                .await
                .is_none()
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
async fn own_device_removal_stops_sync() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let own = a.device().await;
    assert!(matches!(
        a.sync
            .make_and_upload_entry(StoreChange::RemoveDevice { device: own })
            .await,
        Err(SyncError::Removed)
    ));
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
    assert!(matches!(
        a.sync.sync_store_log().await,
        Err(SyncError::Removed)
    ));
}

#[tokio::test]
async fn far_future_entry_applies_and_a_restored_device_stamps_after_it() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    b.sync().await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    a.catch_up().await;
    let future = a
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(50),
            name: "future".into(),
            replaces: None,
        })
        .await
        .unwrap();
    b.sync().await;
    assert!(b.log().await.replay.entries.contains_key(&future));
    b.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    b.restart(storage.clone()).await;
    b.catch_up().await;
    let id = b.device().await;
    let restored = b
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: id,
            name: "restored".into(),
            replaces: None,
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
            principal: coven_format::S3Principal::Console,
        },
        store: store(1),
        name: "Store".into(),
        admin: public(&a.member),
        key: key(1),
        key_hash: a.sync.test_key(coven_merge::Audience::Store, key(1)),
        device_name: "First".into(),
    };
    assert!(matches!(
        a.sync.make_and_upload_entry(change).await,
        Err(SyncError::Storage(_))
    ));
    a.custody.forget().unwrap();
    let pending = a.db.local_store_log().await.unwrap().upload.unwrap();
    assert_eq!(pending.format, coven_format::FormatVersion::V1);
    assert!(storage
        .list(&ObjectPrefix::store_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    assert!(a.custody.read().unwrap().is_none());
    let expected = a.reseal(&pending).await;
    assert!(matches!(
        object::seal_upload(store(1), &StoreLog::default(), &pending, None, &member(2)),
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    let sealed_key = &pending.sealing.keys[0];
    assert_eq!(
        storage.read(&sealed_key.path(store(1))).await.unwrap(),
        sealed_key.bytes
    );
    a.restart(storage.clone()).await;
    let reopened = a.db.local_store_log().await.unwrap().upload.unwrap();
    assert_eq!(reopened, pending);
    assert_eq!(a.reseal(&reopened).await, expected);
    a.sync().await;
    assert_eq!(
        storage
            .read(&object::path(store(1), pending.entry.position))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        storage.read(&sealed_key.path(store(1))).await.unwrap(),
        sealed_key.bytes
    );
    assert!(a.custody.read().unwrap().unwrap().store_key(key(1)).is_ok());
}

#[tokio::test]
async fn circle_member_rotates_after_outside_admin_removal_and_repeated_removal_keeps_keys() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.create(key(1)).await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Member).await;
    b.sync().await;
    b.sync.load_new_device("Ben", None).await.unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Private".into(),
            key: key(2),
            key_hash: b
                .sync
                .test_key(coven_merge::Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle: circle(1),
            member: c.member.member(),
        })
        .await
        .unwrap();
    a.sync().await;
    c.sync().await;
    c.sync.load_new_device("Carol", None).await.unwrap();
    a.sync().await;
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member(),
        })
        .await
        .unwrap();
    c.sync().await;
    a.sync.test_rotate(Audience::Store, key(3)).await;
    c.sync().await;
    c.sync
        .test_rotate(Audience::Circle(circle(1)), key(4))
        .await;
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
            member: b.member.member(),
        })
        .await
        .unwrap();
    c.sync().await;
    for device in [&a, &c] {
        assert_eq!(device.newest(Audience::Store).await, key(3));
        assert!(device
            .custody
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(5))
            .is_err());
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
                copy_path(
                    storage.as_ref(),
                    Audience::Store,
                    key(1),
                    &a.member.member(),
                )
                .await
            } else {
                object::path(store(1), first)
            };
            let original = storage.read(&path).await.unwrap();
            let mut bytes = original.clone();
            bytes[2] = version;
            storage.delete(&path).await.unwrap();
            storage.create(&path, &bytes).await.unwrap();
            let mut b = device(storage.clone(), 2, member(1), store(1)).await;
            let result = b.sync.sync_store_log().await;
            let entry = |reason: &crate::PendingReason| crate::PendingRecord {
                subject: crate::PendingSubject::Entry(first),
                reason: reason.clone(),
                reported_by: DeviceId(2),
            };
            let pending = b.db.pending::<crate::PendingRecord>().await.unwrap();
            match (sealed_key, version) {
                // A copy whose prefix cannot be read names no key copy subject.
                (true, 2) => assert!(matches!(result, Err(SyncError::UpdateRequired(_)))),
                (true, _) => {
                    result.unwrap();
                    // It leaves the copy reading incomplete; the creation waits
                    // for its key.
                    let reading = keys::read_copies(
                        &b.db,
                        &crate::pass_reads::PassReads::default(),
                        std::num::NonZeroUsize::MIN,
                        storage.as_ref(),
                        store(1),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(&reading.unreadable[..], [(damaged, _)] if *damaged == path));
                    assert!(
                        pending.contains(&entry(&crate::PendingReason::KeyUnavailable {
                            audience: Audience::Store,
                            key: key(1),
                        }))
                    );
                }
                // The entry itself waits for an update; the scan goes on.
                (false, 2) => {
                    result.unwrap();
                    assert_eq!(
                        pending,
                        [entry(&crate::PendingReason::UpdateRequired(
                            crate::RequiredUpdate::CovenFormat { version: 2 }
                        ))]
                    );
                }
                (false, _) => {
                    result.unwrap();
                    assert_eq!(pending.len(), 1);
                    assert_eq!(pending[0].subject, crate::PendingSubject::Entry(first));
                    assert!(matches!(
                        pending[0].reason,
                        crate::PendingReason::Refused(_)
                    ));
                }
            }
            assert!(b.log().await.entries.is_empty());
            assert!(b.db.operations().await.unwrap().is_empty());
            assert!(storage
                .list(&ObjectPrefix::snapshots(store(1)))
                .await
                .unwrap()
                .is_empty());
            assert_eq!(storage.read(&path).await.unwrap(), bytes);
            assert_ne!(original, bytes);
            // The retained immutable bytes decide the next attempt as well;
            // the object is not downloaded again to look for another version.
            let before = storage.request_trace().len();
            let again = b.sync.sync_store_log().await;
            if sealed_key && version == 2 {
                assert!(matches!(again, Err(SyncError::UpdateRequired(_))));
            } else {
                again.unwrap();
                assert_eq!(
                    b.db.pending::<crate::PendingRecord>().await.unwrap(),
                    pending
                );
            }
            assert!(!storage.request_trace()[before..]
                .iter()
                .any(|request| matches!(request, MemoryRequest::Read(read, _) if read == &path)));
            assert!(b.log().await.entries.is_empty());
            assert!(b.db.operations().await.unwrap().is_empty());
        }
    }
}

#[path = "store_log_object_tests.rs"]
mod objects;

#[path = "store_log_keys_tests.rs"]
pub(crate) mod key_distribution;

#[path = "member_credentials_tests.rs"]
mod member_credentials;

#[path = "operations_tests.rs"]
pub(crate) mod operations;

#[path = "snapshots_tests.rs"]
mod snapshots;

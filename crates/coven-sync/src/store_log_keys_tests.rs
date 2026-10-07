use super::*;
use coven_storage::{
    AccessGrant, ByteRange, MemberAccess, MemberRemoval, StorageError, StoredObject, UploadSession,
};
use std::sync::Mutex;
use tokio::sync::Barrier;

enum CopyAttempt {
    InterruptOnce,
    LoseReplyOnce,
    Race(Barrier),
}

struct CopyStorage {
    storage: Arc<MemoryStorage>,
    path: ObjectPath,
    action: CopyAttempt,
    attempts: Mutex<Vec<Vec<u8>>>,
}

#[async_trait::async_trait]
impl Storage for CopyStorage {
    fn config(&self) -> StorageConfig {
        self.storage.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.storage.single_request_limit()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if path == &self.path {
            let first = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(bytes.to_vec());
                attempts.len() == 1
            };
            match &self.action {
                CopyAttempt::InterruptOnce if first => {
                    return Err(StorageError::Injected(StorageFailure::Network))
                }
                CopyAttempt::LoseReplyOnce if first => {
                    self.storage.create(path, bytes).await?;
                    return Err(StorageError::Injected(StorageFailure::Network));
                }
                CopyAttempt::Race(barrier) => {
                    barrier.wait().await;
                }
                _ => (),
            }
        }
        self.storage.create(path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.storage.replace(path, bytes).await
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.storage.read(path).await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.storage.read_range(path, range).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.storage.list(prefix).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.storage.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.storage.grant_access(account).await
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        self.storage.revoke_access(member).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.storage.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.resume_upload(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.storage.upload_part(session, bytes).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.finish_upload(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.storage.abort_upload(session).await
    }
}

async fn household(storage: &Arc<MemoryStorage>) -> [Device; 4] {
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let dan = device(storage.clone(), 3, member(3), store(1)).await;
    let mut erin = device(storage.clone(), 4, member(4), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Admin).await;
    ana.add(&dan.member, MemberRole::Member).await;
    ana.add(&erin.member, MemberRole::Member).await;
    ben.sync().await;
    erin.sync().await;
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    [ana, ben, dan, erin]
}

async fn remove(device: &mut Device, member: &MemberKeys, key: KeyId) -> EntryId {
    device
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member.member_id(),
            key,
            circle_keys: vec![],
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn dropped_store_removal_key_lets_the_excluded_member_read_later_entries() {
    let storage = storage();
    let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
    let winner = remove(&mut ana, &dan.member, key(2)).await;
    let dropped = remove(&mut ben, &erin.member, key(3)).await;
    let excluded = ObjectPath::store_key(key(3), &dan.member.member_id());
    storage.delete(&excluded).await.unwrap();
    let later = ben
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(20),
            name: "Ben's phone".into(),
        })
        .await
        .unwrap();
    let bytes = storage.read(&object::path(later)).await.unwrap();
    assert!(matches!(SingleChunkObject::decode(&bytes).unwrap(),
        SingleChunkObject::StoreLog { key: id, .. } if id == key(3)));
    let recipient = ObjectPath::store_key(key(3), &erin.member.member_id());
    assert_eq!(
        storage.read(&recipient).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    ana.sync().await;
    let copy = storage
        .read(&recipient)
        .await
        .expect("Erin receives the dropped removal's key");
    assert_eq!(
        erin.member
            .open_store_key(recipient.as_str(), &copy)
            .unwrap()
            .id(),
        key(3)
    );
    erin.sync().await;
    let report = ben.sync().await;
    assert_eq!(report.dropped_entries[0].entry, dropped);
    assert_eq!(
        report.dropped_entries[0].reason,
        coven_database::DropReason::BeatenBy(winner)
    );
    assert_eq!(ana.log().await, erin.log().await);
    assert_eq!(ana.log().await, ben.log().await);
    assert!(erin
        .log()
        .await
        .replay
        .state
        .devices
        .contains_key(&DeviceId(20)));
    assert!(erin
        .custody
        .unlock()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .is_ok());
    assert!(matches!(
        storage.read(&excluded).await,
        Err(StorageError::NotFound)
    ));
}

#[tokio::test]
async fn a_missing_or_damaged_holder_copy_waits_and_a_later_copy_enables_sharing() {
    for damaged in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, dan, erin] = household(&storage).await;
        remove(&mut ana, &dan.member, key(2)).await;
        let dropped = remove(&mut ben, &erin.member, key(3)).await;
        let holder = ObjectPath::store_key(key(3), &ana.member.member_id());
        let recipient = ObjectPath::store_key(key(3), &erin.member.member_id());
        let original = storage.read(&holder).await.unwrap();
        storage.delete(&holder).await.unwrap();
        if damaged {
            storage.create(&holder, b"damaged").await.unwrap();
        }
        let report = ana.sync().await;
        assert_eq!(report.damaged_objects.len(), usize::from(damaged));
        if damaged {
            assert_eq!(report.damaged_objects[0].path, holder.as_str());
        }
        assert!(matches!(
            ana.log().await.replay.entries[&dropped],
            EntryOutcome::Dropped(_)
        ));
        assert!(matches!(
            storage.read(&recipient).await,
            Err(StorageError::NotFound)
        ));
        storage.delete(&holder).await.unwrap();
        storage.create(&holder, &original).await.unwrap();
        assert!(ana.sync().await.damaged_objects.is_empty());
        let copy = storage.read(&recipient).await.unwrap();
        assert_eq!(
            erin.member
                .open_store_key(recipient.as_str(), &copy)
                .unwrap()
                .id(),
            key(3)
        );
    }
}

#[tokio::test]
async fn occupied_recipient_path_is_preserved_and_damage_is_reported_by_its_recipient() {
    let storage = storage();
    let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
    remove(&mut ana, &dan.member, key(2)).await;
    remove(&mut ben, &erin.member, key(3)).await;
    let path = ObjectPath::store_key(key(3), &erin.member.member_id());
    storage.create(&path, b"damaged").await.unwrap();
    assert!(ana.sync().await.damaged_objects.is_empty());
    assert_eq!(storage.read(&path).await.unwrap(), b"damaged");
    let report = erin.sync().await;
    assert_eq!(report.damaged_objects.len(), 1);
    assert_eq!(report.damaged_objects[0].path, path.as_str());
}

#[tokio::test]
async fn two_holders_race_to_store_the_same_key_for_one_member() {
    let storage = storage();
    let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
    remove(&mut ana, &dan.member, key(2)).await;
    remove(&mut ben, &erin.member, key(3)).await;
    let path = ObjectPath::store_key(key(3), &erin.member.member_id());
    let racing = Arc::new(CopyStorage {
        storage: storage.clone(),
        path: path.clone(),
        action: CopyAttempt::Race(Barrier::new(2)),
        attempts: Mutex::new(Vec::new()),
    });
    ana.sync.storage = Some(racing.clone());
    ben.sync.storage = Some(racing.clone());
    let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(ana.sync.sync_store_log(), ben.sync.sync_store_log())
    })
    .await
    .expect("both holders must attempt the missing copy");
    assert!(a.unwrap().damaged_objects.is_empty());
    assert!(b.unwrap().damaged_objects.is_empty());
    let stored = storage.read(&path).await.unwrap();
    {
        let attempts = racing.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1], "each writer independently seals");
        assert!(
            attempts.contains(&stored),
            "storage keeps one original copy"
        );
        let keys: Vec<_> = attempts
            .iter()
            .map(|bytes| erin.member.open_store_key(path.as_str(), bytes).unwrap())
            .collect();
        let encrypted = keys[0]
            .derive()
            .seal_object_chunk("probe", b"", 0, 0, b"same key")
            .unwrap();
        assert_eq!(
            keys[1]
                .derive()
                .open_object_chunk("probe", b"", 0, 0, &encrypted)
                .unwrap(),
            b"same key"
        );
    }
    erin.sync().await;
    assert!(erin
        .custody
        .unlock()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .is_ok());
    assert_eq!(ana.log().await, erin.log().await);
}

#[tokio::test]
async fn interrupted_copy_reuses_fixed_bytes_after_restart_and_lost_reply_counts_as_stored() {
    for lost_reply in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
        remove(&mut ana, &dan.member, key(2)).await;
        let dropped = remove(&mut ben, &erin.member, key(3)).await;
        let path = ObjectPath::store_key(key(3), &erin.member.member_id());
        let fault = Arc::new(CopyStorage {
            storage: storage.clone(),
            path: path.clone(),
            action: if lost_reply {
                CopyAttempt::LoseReplyOnce
            } else {
                CopyAttempt::InterruptOnce
            },
            attempts: Mutex::new(Vec::new()),
        });
        ana.sync.storage = Some(fault.clone());
        assert!(matches!(ana.sync.sync_store_log().await.unwrap_err(),
            SyncFailure::Storage(error) if error.failure() == StorageFailure::Network));
        assert!(matches!(
            ana.log().await.replay.entries[&dropped],
            EntryOutcome::Dropped(_)
        ));
        let fixed = fault.attempts.lock().unwrap()[0].clone();
        ana.restart(storage.clone()).await;
        assert_eq!(
            ana.db
                .prepare_key_upload(
                    path.as_str().into(),
                    || -> Result<Vec<u8>, coven_database::DbError> {
                        panic!("the first attempted bytes must already be durable")
                    }
                )
                .await
                .unwrap(),
            fixed
        );
        ana.sync.storage = Some(fault.clone());
        // Both public entry points resume copies left by a previously failed call.
        if lost_reply {
            ana.sync
                .make_and_upload_entry(StoreChange::AddDevice {
                    device: DeviceId(20),
                    name: "Ana's phone".into(),
                })
                .await
                .unwrap();
        } else {
            ana.sync().await;
        }
        assert_eq!(storage.read(&path).await.unwrap(), fixed);
        let attempts = fault.attempts.lock().unwrap().clone();
        assert_eq!(attempts, vec![fixed; if lost_reply { 1 } else { 2 }]);
        erin.sync().await;
        assert!(erin
            .custody
            .unlock()
            .unwrap()
            .unwrap()
            .store_key(key(3))
            .is_ok());
    }
}

#[tokio::test]
async fn dropped_circle_removals_share_only_with_the_latest_circle_members() {
    for remove_from_store in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
        let outsider = member(5);
        ana.add(&outsider, MemberRole::Member).await;
        ana.sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle: circle(1),
                name: "Shared".into(),
                key: key(10),
            })
            .await
            .unwrap();
        for who in [&ben.member, &dan.member, &erin.member] {
            ana.sync
                .make_and_upload_entry(StoreChange::AddCircleMember {
                    circle: circle(1),
                    member: who.member_id(),
                })
                .await
                .unwrap();
        }
        ben.sync().await;
        erin.sync().await;
        ana.sync
            .make_and_upload_entry(StoreChange::RemoveCircleMember {
                circle: circle(1),
                member: dan.member.member_id(),
                key: key(11),
            })
            .await
            .unwrap();
        let removal = if remove_from_store {
            StoreChange::RemoveMember {
                member: erin.member.member_id(),
                key: key(13),
                circle_keys: vec![CircleKeyId {
                    circle: circle(1),
                    key: key(12),
                }],
            }
        } else {
            StoreChange::RemoveCircleMember {
                circle: circle(1),
                member: erin.member.member_id(),
                key: key(12),
            }
        };
        let dropped = ben.sync.make_and_upload_entry(removal).await.unwrap();
        let excluded = ObjectPath::circle_key(circle(1), key(12), &dan.member.member_id());
        storage.delete(&excluded).await.unwrap();
        let secret = ben
            .custody
            .unlock()
            .unwrap()
            .unwrap()
            .circle_key(circle(1), key(12))
            .unwrap()
            .clone();
        let encrypted = secret
            .derive()
            .seal_object_chunk("circle-data", b"header", 0, 0, b"before the drop")
            .unwrap();
        let missing = ObjectPath::circle_key(circle(1), key(12), &erin.member.member_id());
        assert_eq!(
            storage.read(&missing).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
        ana.sync().await;
        erin.sync().await;
        let ring = erin.custody.unlock().unwrap().unwrap();
        let received = ring
            .circle_key(circle(1), key(12))
            .expect("Erin gets the dropped circle key");
        assert_eq!(
            received
                .derive()
                .open_object_chunk("circle-data", b"header", 0, 0, &encrypted)
                .unwrap(),
            b"before the drop"
        );
        assert!(matches!(
            erin.log().await.replay.entries[&dropped],
            EntryOutcome::Dropped(_)
        ));
        assert_eq!(
            erin.log().await.replay.state.circles[&circle(1)].key,
            key(11)
        );
        assert!(matches!(
            storage.read(&excluded).await,
            Err(StorageError::NotFound)
        ));
        assert_eq!(
            storage
                .read(&ObjectPath::circle_key(
                    circle(1),
                    key(12),
                    &outsider.member_id()
                ))
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::NotFound
        );
        ana.sync
            .make_and_upload_entry(StoreChange::DeleteCircle { circle: circle(1) })
            .await
            .unwrap();
        storage.delete(&missing).await.unwrap();
        ana.sync().await;
        assert!(
            matches!(storage.read(&missing).await, Err(StorageError::NotFound)),
            "deleted circles have no audience for redistribution"
        );
    }
}

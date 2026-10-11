use super::*;
use coven_storage::{
    AccessGrant, ByteRange, MemberAccess, MemberRemoval, StorageError, StoredObject, UploadSession,
};
use std::sync::Mutex;
use tokio::sync::Barrier;

#[tokio::test]
async fn introductions_refuse_material_that_does_not_match_the_committed_hash() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut wrong_hash = ana.sync.test_key(Audience::Store, key(1));
    wrong_hash[0] ^= 1;
    let result = ana
        .sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: store(1),
            name: "Home".into(),
            admin: public(&ana.member),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner".into(),
            },
            device_name: "Ana".into(),
            key: key(1),
            key_hash: wrong_hash,
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncError::Key(
            coven_crypto::MaterialError::StoreKeyConflict(_)
        ))
    ));
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    assert!(ana.log().await.entries.is_empty());
    ana.create(key(1)).await;
    let mut wrong_hash = ana.sync.test_key(Audience::Circle(circle(1)), key(2));
    wrong_hash[31] ^= 1;
    let result = ana
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Home".into(),
            key: key(2),
            key_hash: wrong_hash,
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncError::Key(
            coven_crypto::MaterialError::CircleKeyConflict { .. }
        ))
    ));
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Home".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    for audience in [Audience::Store, Audience::Circle(circle(1))] {
        let before = ana.log().await;
        let mut key_hash = ana.sync.test_key(audience.clone(), key(3));
        key_hash[15] ^= 1;
        let result = ana
            .sync
            .make_and_upload_entry(StoreChange::RotateKey {
                audience,
                key: key(3),
                key_hash,
            })
            .await;
        assert!(matches!(result, Err(SyncError::Key(_))));
        assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
        assert_eq!(ana.log().await, before);
    }
}

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
                    return Err(StorageError::Failure(StorageFailure::Network))
                }
                CopyAttempt::LoseReplyOnce if first => {
                    self.storage.create(path, bytes).await?;
                    return Err(StorageError::Failure(StorageFailure::Network));
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
    ben.sync.load_new_device("Ben").await.unwrap();
    ana.sync().await;
    erin.sync().await;
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    [ana, ben, dan, erin]
}

#[tokio::test]
async fn a_missing_or_damaged_holder_copy_waits_and_a_later_copy_enables_sharing() {
    for damaged in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, _dan, erin] = household(&storage).await;
        let rotation = ben.sync.test_rotate(Audience::Store, key(3)).await;
        let holder = ObjectPath::store_key(store(1), key(3), &ana.member.member_id());
        let recipient = ObjectPath::store_key(store(1), key(3), &erin.member.member_id());
        storage.delete(&recipient).await.unwrap();
        let original = storage.read(&holder).await.unwrap();
        storage.delete(&holder).await.unwrap();
        if damaged {
            storage.create(&holder, b"damaged").await.unwrap();
        }
        let damages = ana.sync.step().await.unwrap();
        assert_eq!(damages.len(), usize::from(damaged));
        if damaged {
            assert_eq!(damages[0].path, holder.as_str());
        }
        assert_eq!(
            ana.log().await.replay.entries[&rotation],
            EntryOutcome::Kept
        );
        assert!(matches!(
            storage.read(&recipient).await,
            Err(error) if error.failure() == StorageFailure::NotFound
        ));
        storage.delete(&holder).await.unwrap();
        storage.create(&holder, &original).await.unwrap();
        ana.sync().await;
        let copy = storage.read(&recipient).await.unwrap();
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&copy)
                .unwrap()
                .open_store_key(&erin.member, recipient.as_str())
                .unwrap()
                .id(),
            key(3)
        );
    }
}

#[tokio::test]
async fn occupied_recipient_path_is_preserved_and_damage_is_reported_by_its_recipient() {
    let storage = storage();
    let [mut ana, mut ben, _dan, erin] = household(&storage).await;
    ben.sync.test_rotate(Audience::Store, key(3)).await;
    let path = ObjectPath::store_key(store(1), key(3), &erin.member.member_id());
    storage.delete(&path).await.unwrap();
    storage.create(&path, b"damaged").await.unwrap();
    ana.sync().await;
    assert_eq!(storage.read(&path).await.unwrap(), b"damaged");
    let damages = erin.sync.step().await.unwrap();
    assert_eq!(damages.len(), 1);
    assert_eq!(damages[0].path, path.as_str());
}

#[tokio::test]
async fn two_holders_race_to_store_the_same_key_for_one_member() {
    let storage = storage();
    let [mut ana, mut ben, _dan, mut erin] = household(&storage).await;
    ben.sync.test_rotate(Audience::Store, key(3)).await;
    let path = ObjectPath::store_key(store(1), key(3), &erin.member.member_id());
    ana.sync().await;
    storage.delete(&path).await.unwrap();
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
    assert!(a.unwrap().is_empty());
    assert!(b.unwrap().is_empty());
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
            .map(|bytes| {
                coven_format::sealed_key::SealedKey::decode(bytes)
                    .unwrap()
                    .open_store_key(&erin.member, path.as_str())
                    .unwrap()
            })
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
        .read()
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
        let [mut ana, mut ben, _dan, mut erin] = household(&storage).await;
        let rotation = ben.sync.test_rotate(Audience::Store, key(3)).await;
        let path = ObjectPath::store_key(store(1), key(3), &erin.member.member_id());
        ana.sync().await;
        storage.delete(&path).await.unwrap();
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
        assert_eq!(
            ana.log().await.replay.entries[&rotation],
            EntryOutcome::Kept
        );
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
                    replaces: None,
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
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(3))
            .is_ok());
    }
}

#[tokio::test]
async fn circle_rotations_share_only_with_current_circle_members() {
    let storage = storage();
    let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Shared".into(),
            key: key(10),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(10)),
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
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: circle(1),
            member: dan.member.member_id(),
        })
        .await
        .unwrap();
    ben.sync().await;
    let rotation = ben
        .sync
        .test_rotate(Audience::Circle(circle(1)), key(11))
        .await;
    let missing = ObjectPath::circle_key(store(1), circle(1), key(11), &erin.member.member_id());
    storage.delete(&missing).await.unwrap();
    ana.sync().await;
    erin.sync().await;
    let ring = erin.custody.read().unwrap().unwrap();
    let received = ring.circle_key(circle(1), key(11)).unwrap();
    assert_eq!(
        received.hash(),
        ben.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle(1), key(11))
            .unwrap()
            .hash()
    );
    assert_eq!(
        erin.log().await.replay.entries[&rotation],
        EntryOutcome::Kept
    );
    assert!(
        matches!(storage.read(&ObjectPath::circle_key(store(1), circle(1), key(11), &dan.member.member_id())).await,
        Err(error) if error.failure() == StorageFailure::NotFound)
    );
    ana.sync
        .make_and_upload_entry(StoreChange::DeleteCircle { circle: circle(1) })
        .await
        .unwrap();
    storage.delete(&missing).await.unwrap();
    ana.sync().await;
    assert!(matches!(storage.read(&missing).await,
        Err(error) if error.failure() == StorageFailure::NotFound));
}

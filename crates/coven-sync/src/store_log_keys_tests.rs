use super::*;
use coven_storage::{
    AccessGrant, ByteRange, MemberAccess, MemberRemoval, StorageError, StoredObject, UploadSession,
};
use std::sync::Mutex;
use tokio::sync::Barrier;

#[tokio::test]
async fn copies_use_one_contiguous_writer_sequence_across_audiences() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Shared".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    let paths: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.as_str().to_owned())
        .collect();
    assert_eq!(
        paths,
        (1..=3)
            .map(|number| format!("{}/keys/1/{number}", store(1)))
            .collect::<Vec<_>>()
    );
}

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
    FailComparison,
    Race(Barrier),
}

struct CopyStorage {
    storage: Arc<MemoryStorage>,
    paths: Vec<ObjectPath>,
    action: CopyAttempt,
    attempts: Mutex<Vec<(ObjectPath, Vec<u8>)>>,
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
        if self.paths.contains(path) {
            let first = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push((path.clone(), bytes.to_vec()));
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
        if self.paths.contains(path) && matches!(self.action, CopyAttempt::FailComparison) {
            return Err(StorageFailure::Network.into());
        }
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

pub(super) fn failing_creation(
    storage: Arc<MemoryStorage>,
    path: ObjectPath,
    lost_reply: bool,
) -> Arc<dyn Storage> {
    Arc::new(CopyStorage {
        storage,
        paths: vec![path],
        action: if lost_reply {
            CopyAttempt::LoseReplyOnce
        } else {
            CopyAttempt::InterruptOnce
        },
        attempts: Mutex::new(Vec::new()),
    })
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

async fn next_copy(storage: &dyn Storage, writer: DeviceId) -> ObjectPath {
    let last = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|object| object.path.key_copy_position())
        .filter(|(device, _)| *device == writer)
        .map(|(_, number)| number.get())
        .max()
        .unwrap_or(0);
    ObjectPath::key_copy(store(1), writer, (last + 1).try_into().unwrap())
}

#[tokio::test]
async fn a_missing_or_damaged_holder_copy_waits_and_a_later_copy_enables_sharing() {
    for damaged in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, _dan, erin] = household(&storage).await;
        let rotation = ben.sync.test_rotate(Audience::Store, key(3)).await;
        let holder = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &ana.member.member_id(),
        )
        .await;
        let recipient = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id(),
        )
        .await;
        storage.delete(&recipient).await.unwrap();
        let original = storage.read(&holder).await.unwrap();
        storage.delete(&holder).await.unwrap();
        if damaged {
            storage.create(&holder, b"damaged").await.unwrap();
        }
        let damages = ana.sync.step().await.unwrap();
        assert_eq!(damages.len(), usize::from(damaged));
        assert_eq!(
            ana.log().await.replay.entries[&rotation],
            EntryOutcome::Kept
        );
        assert!(find_copy(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id()
        )
        .await
        .is_none());
        storage.delete(&holder).await.unwrap();
        storage.create(&holder, &original).await.unwrap();
        ana.sync().await;
        let copy = find_copy(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id(),
        )
        .await
        .unwrap();
        assert_eq!(copy.path.device(), Some(ana.device().await));
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&copy.bytes)
                .unwrap()
                .open_store_key(&erin.member, copy.path.as_str())
                .unwrap()
                .id(),
            key(3)
        );
    }
}

#[tokio::test]
async fn two_suppliers_publish_distinct_copies_for_the_same_member() {
    let storage = storage();
    let [mut ana, mut ben, _dan, mut erin] = household(&storage).await;
    ben.sync.test_rotate(Audience::Store, key(3)).await;
    ana.sync().await;
    let original = copy_path(
        storage.as_ref(),
        Audience::Store,
        key(3),
        &erin.member.member_id(),
    )
    .await;
    storage.delete(&original).await.unwrap();
    let paths = vec![
        next_copy(storage.as_ref(), ana.device().await).await,
        next_copy(storage.as_ref(), ben.device().await).await,
    ];
    let racing = Arc::new(CopyStorage {
        storage: storage.clone(),
        paths: paths.clone(),
        action: CopyAttempt::Race(Barrier::new(2)),
        attempts: Mutex::new(Vec::new()),
    });
    ana.sync.storage = Some(racing.clone());
    ben.sync.storage = Some(racing.clone());
    let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(ana.sync.sync_store_log(), ben.sync.sync_store_log())
    })
    .await
    .unwrap();
    assert!(a.unwrap().is_empty());
    assert!(b.unwrap().is_empty());
    let attempts = racing.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0].0, attempts[1].0);
    assert_ne!(attempts[0].1, attempts[1].1);
    let expected = ben
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .unwrap()
        .hash();
    for (path, bytes) in attempts {
        assert!(paths.contains(&path));
        assert_eq!(storage.read(&path).await.unwrap(), bytes);
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&bytes)
                .unwrap()
                .open_store_key(&erin.member, path.as_str())
                .unwrap()
                .hash(),
            expected
        );
    }
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
    assert!(ben.db.key_uploads().await.unwrap().is_empty());
    erin.sync().await;
    assert_eq!(ana.log().await, erin.log().await);
}

#[tokio::test]
async fn interrupted_and_lost_reply_copies_retry_identical_bytes_after_restart() {
    for lost_reply in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, _dan, mut erin] = household(&storage).await;
        ben.sync.test_rotate(Audience::Store, key(3)).await;
        ana.sync().await;
        let original = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id(),
        )
        .await;
        storage.delete(&original).await.unwrap();
        let path = next_copy(storage.as_ref(), ana.device().await).await;
        let fault = Arc::new(CopyStorage {
            storage: storage.clone(),
            paths: vec![path.clone()],
            action: if lost_reply {
                CopyAttempt::LoseReplyOnce
            } else {
                CopyAttempt::InterruptOnce
            },
            attempts: Mutex::new(Vec::new()),
        });
        ana.sync.storage = Some(fault.clone());
        assert!(
            matches!(ana.sync.sync_store_log().await.unwrap_err(), SyncFailure::Storage(error) if error.failure() == StorageFailure::Network)
        );
        let fixed = ana.db.key_uploads().await.unwrap().remove(0);
        assert_eq!(fixed.path(store(1)), path);
        assert_eq!(fault.attempts.lock().unwrap()[0].1, fixed.bytes);
        ana.restart(storage.clone()).await;
        assert_eq!(
            ana.db.key_uploads().await.unwrap(),
            std::slice::from_ref(&fixed)
        );
        ana.sync.storage = Some(fault.clone());
        ana.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(20),
                name: "Ana phone".into(),
                replaces: None,
            })
            .await
            .unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), fixed.bytes);
        assert_eq!(
            *fault.attempts.lock().unwrap(),
            vec![(path, fixed.bytes); 2]
        );
        assert!(ana.db.key_uploads().await.unwrap().is_empty());
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
async fn reserved_copy_settles_even_after_another_supplier_or_recipient_removal() {
    for removed in [false, true] {
        let storage = storage();
        let [mut ana, mut ben, _dan, erin] = household(&storage).await;
        ben.sync.test_rotate(Audience::Store, key(3)).await;
        ana.sync().await;
        let original = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id(),
        )
        .await;
        storage.delete(&original).await.unwrap();
        let path = next_copy(storage.as_ref(), ana.device().await).await;
        let fault = Arc::new(CopyStorage {
            storage: storage.clone(),
            paths: vec![path.clone()],
            action: CopyAttempt::InterruptOnce,
            attempts: Mutex::new(Vec::new()),
        });
        ana.sync.storage = Some(fault);
        assert!(ana.sync.sync_store_log().await.is_err());
        let fixed = ana.db.key_uploads().await.unwrap().remove(0);
        // The other writer supplies the same need at its own path.
        ben.sync().await;
        let supplied = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member_id(),
        )
        .await;
        assert_ne!(supplied, path);
        if removed {
            let id = ben
                .sync
                .make_and_upload_entry(StoreChange::RemoveMember {
                    member: erin.member.member_id(),
                })
                .await
                .unwrap();
            let entry = ben
                .log()
                .await
                .entries
                .into_iter()
                .find(|entry| entry.entry.position == id)
                .unwrap()
                .entry;
            let (entry, replay) = crate::replay::tests::replay_entry(&ana.log().await, entry);
            ana.db
                .apply_store_log::<crate::PendingRecord>(entry, replay)
                .await
                .unwrap();
        }
        ana.restart(storage.clone()).await;
        ana.sync().await;
        assert_eq!(storage.read(&path).await.unwrap(), fixed.bytes);
        let expected_next = fixed.number.get() + 1;
        ana.sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle: circle(9),
                name: "Next".into(),
                key: key(9),
                key_hash: ana.sync.test_key(Audience::Circle(circle(9)), key(9)),
            })
            .await
            .unwrap();
        let next = copy_path(
            storage.as_ref(),
            Audience::Circle(circle(9)),
            key(9),
            &ana.member.member_id(),
        )
        .await;
        assert_eq!(next.key_copy_position().unwrap().1.get(), expected_next);
    }
}

#[tokio::test]
async fn cancelling_publication_retains_prerequisites_and_leaves_no_number_gap() {
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(4096, 4096)
            .build()
            .unwrap(),
    );
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (_resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_creation(ObjectPrefix::keys(store(1)), started, held)
        .await;
    let change = StoreChange::CreateCircle {
        circle: circle(1),
        name: "Retained".into(),
        key: key(2),
        key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
    };
    let mut call = Box::pin(ana.sync.make_and_upload_entry(change));
    tokio::select! { result = &mut call => panic!("publication returned {result:?}"), result = waiting => result.unwrap() }
    drop(call);
    let queued = ana.db.key_uploads().await.unwrap();
    let entry = ana
        .db
        .local_store_log()
        .await
        .unwrap()
        .upload
        .unwrap()
        .entry
        .position;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].before_entry, Some(entry));
    assert_eq!(queued[0].number.get(), 2);
    assert!(
        matches!(storage.read(&object::path(store(1), entry)).await, Err(error) if error.failure() == StorageFailure::NotFound)
    );
    ana.restart(storage.clone()).await;
    ana.sync().await;
    assert_eq!(
        storage.read(&queued[0].path(store(1))).await.unwrap(),
        queued[0].bytes
    );
    ana.add(&member(2), MemberRole::Member).await;
    let mut numbers: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.key_copy_position().unwrap().1.get())
        .collect();
    numbers.sort();
    assert_eq!(numbers, [1, 2, 3]);
}

#[tokio::test]
async fn unequal_occupant_keeps_the_copy_pending() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let copy = ana
        .db
        .prepare_key_upload(|_| Ok::<_, coven_database::DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    storage
        .create(&copy.path(store(1)), b"different")
        .await
        .unwrap();
    assert!(matches!(ana.sync.step().await, Err(SyncError::Damaged(_))));
    assert_eq!(ana.db.key_uploads().await.unwrap(), [copy]);
}

#[tokio::test]
async fn failed_occupied_comparison_keeps_the_copy_pending() {
    let storage = storage();
    let ana = device(storage.clone(), 1, member(1), store(1)).await;
    let copy = ana
        .db
        .prepare_key_upload(|_| Ok::<_, coven_database::DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    storage
        .create(&copy.path(store(1)), &copy.bytes)
        .await
        .unwrap();
    let fault = CopyStorage {
        storage: storage.clone(),
        paths: vec![copy.path(store(1))],
        action: CopyAttempt::FailComparison,
        attempts: Mutex::new(Vec::new()),
    };
    assert!(
        matches!(object::upload_keys(store(1), Some(&fault), &ana.db).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network)
    );
    assert_eq!(ana.db.key_uploads().await.unwrap(), [copy]);
    object::upload_keys(store(1), Some(storage.as_ref()), &ana.db)
        .await
        .unwrap();
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_cleanup_failure_is_reported_without_confirming_the_copy() {
    let storage = storage();
    let ana = device(storage.clone(), 1, member(1), store(1)).await;
    let copy = ana
        .db
        .prepare_key_upload(|_| Ok::<_, coven_database::DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    storage
        .create(&copy.path(store(1)), &copy.bytes)
        .await
        .unwrap();
    storage
        .set_faults(Faults {
            fail_duplicate_cleanup: true,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        object::upload_keys(store(1), Some(storage.as_ref()), &ana.db).await,
        Err(SyncError::Storage(StorageError::Cleanup { .. }))
    ));
    assert_eq!(
        ana.db.key_uploads().await.unwrap(),
        std::slice::from_ref(&copy)
    );
    storage.set_faults(Faults::none()).await;
    object::upload_keys(store(1), Some(storage.as_ref()), &ana.db)
        .await
        .unwrap();
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
}

#[tokio::test]
async fn partially_confirmed_prerequisites_resume_before_their_entry() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.add(&member(3), MemberRole::Member).await;
    let second = ObjectPath::key_copy(store(1), DeviceId(1), 5.try_into().unwrap());
    ana.sync.storage = Some(failing_creation(storage.clone(), second, false));
    let key_hash = ana.sync.test_key(Audience::Store, key(2));
    assert!(matches!(
        ana.sync
            .make_and_upload_entry(StoreChange::RotateKey {
                audience: Audience::Store,
                key: key(2),
                key_hash,
            })
            .await,
        Err(SyncError::Storage(_))
    ));
    let upload = ana.db.local_store_log().await.unwrap().upload.unwrap();
    let pending = ana.db.key_uploads().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|copy| copy.number.get())
            .collect::<Vec<_>>(),
        [5, 6]
    );
    assert!(pending
        .iter()
        .all(|copy| copy.before_entry == Some(upload.entry.position)));
    let confirmed = ObjectPath::key_copy(store(1), DeviceId(1), 4.try_into().unwrap());
    let confirmed_bytes = storage.read(&confirmed).await.unwrap();
    let entry_path = object::path(store(1), upload.entry.position);
    assert_eq!(
        storage.read(&entry_path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    ana.restart(storage.clone()).await;
    assert_eq!(ana.db.key_uploads().await.unwrap(), pending);
    ana.sync().await;
    assert_eq!(storage.read(&confirmed).await.unwrap(), confirmed_bytes);
    for copy in pending {
        assert_eq!(
            storage.read(&copy.path(store(1))).await.unwrap(),
            copy.bytes
        );
    }
    assert_eq!(
        storage.read(&entry_path).await.unwrap(),
        ana.reseal(&upload)
    );
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    ana.add(&member(4), MemberRole::Member).await;
    let mut numbers: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.key_copy_position().unwrap().1.get())
        .collect();
    numbers.sort();
    assert_eq!(numbers, (1..=8).collect::<Vec<_>>());
}

#[tokio::test]
async fn creation_recovers_after_copy_confirmation_without_resealing_the_copy() {
    for lost_reply in [false, true] {
        let storage = storage();
        let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
        let entry_path = ObjectPath::store_log(store(1), DeviceId(1), 1.try_into().unwrap());
        ana.sync.storage = Some(failing_creation(
            storage.clone(),
            entry_path.clone(),
            lost_reply,
        ));
        let key_hash = ana.sync.test_key(Audience::Store, key(1));
        assert!(matches!(
            ana.sync
                .make_and_upload_entry(StoreChange::CreateStore {
                    store: store(1),
                    name: "Home".into(),
                    admin: public(&ana.member),
                    access: coven_format::MemberAccess::S3AccessKey {
                        access_key_id: "owner".into()
                    },
                    device_name: "Ana".into(),
                    key: key(1),
                    key_hash,
                })
                .await,
            Err(SyncError::Storage(_))
        ));
        let upload = ana.db.local_store_log().await.unwrap().upload.unwrap();
        let expected = ana.reseal(&upload);
        assert!(upload.sealing.keys.is_empty());
        assert!(ana.db.key_uploads().await.unwrap().is_empty());
        let copy_path = ObjectPath::key_copy(store(1), DeviceId(1), 1.try_into().unwrap());
        let copy = storage.read(&copy_path).await.unwrap();
        ana.custody.forget().unwrap();
        ana.restart(storage.clone()).await;
        ana.sync().await;
        assert_eq!(storage.read(&copy_path).await.unwrap(), copy);
        assert_eq!(storage.read(&entry_path).await.unwrap(), expected);
        assert_eq!(
            ana.custody
                .read()
                .unwrap()
                .unwrap()
                .store_key(key(1))
                .unwrap()
                .hash(),
            key_hash
        );
        ana.add(&member(2), MemberRole::Member).await;
        assert!(storage
            .read(&ObjectPath::key_copy(
                store(1),
                DeviceId(1),
                2.try_into().unwrap()
            ))
            .await
            .is_ok());
        assert_eq!(
            storage
                .list(&ObjectPrefix::keys(store(1)))
                .await
                .unwrap()
                .len(),
            2
        );
    }
}

#[tokio::test]
async fn observed_historical_copies_do_not_reserve_numbers_when_a_member_returns() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    let before = storage.list(&ObjectPrefix::keys(store(1))).await.unwrap();
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: member(2).member_id(),
        })
        .await
        .unwrap();
    ana.add(&member(2), MemberRole::Member).await;
    assert_eq!(
        storage.list(&ObjectPrefix::keys(store(1))).await.unwrap(),
        before
    );
    ana.add(&member(3), MemberRole::Member).await;
    assert!(storage
        .read(&ObjectPath::key_copy(
            store(1),
            DeviceId(1),
            3.try_into().unwrap()
        ))
        .await
        .is_ok());
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
    let missing = copy_path(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member_id(),
    )
    .await;
    storage.delete(&missing).await.unwrap();
    ana.sync().await;
    erin.sync().await;
    assert_eq!(
        erin.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle(1), key(11))
            .unwrap()
            .hash(),
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
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &dan.member.member_id()
    )
    .await
    .is_none());
    ana.sync
        .make_and_upload_entry(StoreChange::DeleteCircle { circle: circle(1) })
        .await
        .unwrap();
    let copy = copy_path(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member_id(),
    )
    .await;
    storage.delete(&copy).await.unwrap();
    ana.sync().await;
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member_id()
    )
    .await
    .is_none());
}

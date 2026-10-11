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

/// A key disclosed while its removal is dropped becomes current again when
/// that removal returns. The alternative arrival never discloses the key.
#[tokio::test]
async fn a_rekept_removal_uses_a_key_disclosed_while_it_was_dropped() {
    for temporary_drop in [false, true] {
        let storage = storage();
        let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
        let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
        let mut carol = device(storage.clone(), 3, member(3), store(1)).await;
        let mut tablet = device(storage.clone(), 4, member(1), store(1)).await;
        ana.create(key(1)).await;
        ana.add(&ben.member, MemberRole::Admin).await;
        ana.add(&carol.member, MemberRole::Member).await;
        for device in [&mut ben, &mut carol, &mut tablet] {
            device.sync().await;
            device
                .sync
                .make_and_upload_entry(StoreChange::AddDevice {
                    device: device.device().await,
                    name: "registered".into(),
                })
                .await
                .unwrap();
        }
        for device in [&mut ana, &mut ben, &mut carol, &mut tablet] {
            device.sync().await;
        }
        tablet.clock.set(UNIX_EPOCH + Duration::from_secs(2));
        ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
        ana.clock.set(UNIX_EPOCH + Duration::from_secs(4));
        let demotion = tablet
            .sync
            .make_and_upload_entry(StoreChange::ChangeRole {
                member: ben.member.member_id(),
                role: MemberRole::Member,
            })
            .await
            .unwrap();
        // Hold back delivery without changing any device's author view.
        let delayed = object::path(demotion);
        let bytes = storage.read(&delayed).await.unwrap();
        storage.delete(&delayed).await.unwrap();
        let removal = remove(&mut ana, &carol.member, key(3)).await;
        assert_eq!(ana.log().await.replay.entries[&removal], EntryOutcome::Kept);
        remove(&mut ben, &ana.member, key(2)).await;
        let copy = ObjectPath::store_key(key(3), &carol.member.member_id());
        if temporary_drop {
            ben.sync().await;
            assert!(matches!(
                ben.log().await.replay.entries[&removal],
                EntryOutcome::Dropped(_)
            ));
            // Carol must read Ben's removal first: seeing her own removal
            // before its opponent stops her sync before the next listing.
            let path = object::path(removal);
            let bytes = storage.read(&path).await.unwrap();
            storage.delete(&path).await.unwrap();
            carol.sync().await;
            storage.create(&path, &bytes).await.unwrap();
            carol.sync().await;
            assert!(carol
                .custody
                .read()
                .unwrap()
                .unwrap()
                .store_key(key(3))
                .is_ok());
        }
        storage.create(&delayed, &bytes).await.unwrap();
        if !temporary_drop {
            // Downloads apply each entry before reading the next device's
            // listing. Deliver the demotion before Carol's removal on Ben.
            let path = object::path(removal);
            let bytes = storage.read(&path).await.unwrap();
            storage.delete(&path).await.unwrap();
            ben.sync().await;
            storage.create(&path, &bytes).await.unwrap();
        }
        ben.sync().await;
        let log = ben.log().await;
        assert_eq!(log.replay.entries[&removal], EntryOutcome::Kept);
        assert_eq!(log.replay.state.store.unwrap().key, key(3));
        assert!(log.replay.state.members[&carol.member.member_id()].removed);
        assert_eq!(storage.read(&copy).await.is_ok(), temporary_drop);
        if temporary_drop {
            let ring = ben.custody.read().unwrap().unwrap();
            let encrypted = ring
                .store_key(key(3))
                .unwrap()
                .derive()
                .seal_object_chunk("after-removal", b"header", 0, 0, b"new private data")
                .unwrap();
            let ring = carol.custody.read().unwrap().unwrap();
            assert_eq!(
                ring.store_key(key(3))
                    .unwrap()
                    .derive()
                    .open_object_chunk("after-removal", b"header", 0, 0, &encrypted)
                    .unwrap(),
                b"new private data"
            );
        }
    }
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
    ben.sync().await;
    assert_eq!(
        ben.log().await.replay.entries[&dropped],
        EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(winner))
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
        .read()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .is_ok());
    assert!(matches!(
        storage.read(&excluded).await,
        Err(error) if error.failure() == StorageFailure::NotFound
    ));
}

/// The two-member counterexample in spec/proofs/storelog-data.md: the only
/// holder stops before it can distribute the losing removal's key.
#[tokio::test]
async fn a_removed_sole_holder_cannot_share_a_dropped_removals_key() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    for device in [&mut ana, &mut ben] {
        device
            .reopen(
                storage.clone(),
                vec![SyncedTable::new(
                    "notes",
                    coven_database::RowIdentity::SharedKey,
                )],
                vec![Migration::sql(
                    1,
                    "notes",
                    "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL)",
                )],
            )
            .await;
    }
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Admin).await;
    ben.sync().await;
    ben.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: ben.device().await,
            name: "Ben".into(),
        })
        .await
        .unwrap();
    ana.sync().await;
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let winner = remove(&mut ana, &ben.member, key(2)).await;
    let loser = remove(&mut ben, &ana.member, key(3)).await;
    ben.db
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('one','Ben wrote this')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let writes = ben.writes.upload_writes().await.unwrap();
    assert_eq!(writes.len(), 1);
    let missing = ObjectPath::store_key(key(3), &ana.member.member_id());
    ana.sync().await;
    assert_eq!(
        ana.log().await.replay.entries[&loser],
        EntryOutcome::Dropped(coven_database::DropReason::NoAdminLeft)
    );
    assert_eq!(ana.log().await.replay.entries[&winner], EntryOutcome::Kept);
    assert!(matches!(
        ben.sync.sync_store_log().await,
        Err(SyncFailure::Removed)
    ));
    assert!(ben
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .is_ok());
    // A later call still stops before share_dropped_keys. The surviving
    // member waits on the encrypted header and never advances over the write.
    assert!(matches!(
        ben.sync.sync_store_log().await,
        Err(SyncFailure::Removed)
    ));
    ana.sync().await;
    ana.writes.download_writes().await.unwrap();
    assert!(matches!(storage.read(&missing).await,
        Err(error) if error.failure() == StorageFailure::NotFound));
    assert!(!ana
        .db
        .sync_state(Vec::new())
        .await
        .unwrap()
        .positions
        .covers(writes[0]));
    assert_eq!(
        ana.db
            .read(
                |sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        0
    );
    assert!(ana.db.lost_values().await.unwrap().is_empty());
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
        let damages = ana.sync.step().await.unwrap();
        assert_eq!(damages.len(), usize::from(damaged));
        if damaged {
            assert_eq!(damages[0].path, holder.as_str());
        }
        assert!(matches!(
            ana.log().await.replay.entries[&dropped],
            EntryOutcome::Dropped(_)
        ));
        assert!(matches!(
            storage.read(&recipient).await,
            Err(error) if error.failure() == StorageFailure::NotFound
        ));
        storage.delete(&holder).await.unwrap();
        storage.create(&holder, &original).await.unwrap();
        ana.sync().await;
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
    let [mut ana, mut ben, dan, erin] = household(&storage).await;
    remove(&mut ana, &dan.member, key(2)).await;
    remove(&mut ben, &erin.member, key(3)).await;
    let path = ObjectPath::store_key(key(3), &erin.member.member_id());
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
            .read()
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
            .read()
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
        let ring = erin.custody.read().unwrap().unwrap();
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
            Err(error) if error.failure() == StorageFailure::NotFound
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
            matches!(storage.read(&missing).await, Err(error) if error.failure() == StorageFailure::NotFound),
            "deleted circles have no audience for redistribution"
        );
    }
}

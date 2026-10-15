use super::*;
use coven_database::{LogObject, LogRefusal};
use coven_format::pending::RefusalCode;

use crate::replay::tests::checked_log;

#[test]
fn entry_sealing_reproduces_the_fixture_and_binds_plaintext_and_prefix() {
    let bytes =
        hex::decode(include_str!("../../coven-format/fixtures/sealed-store-log.hex").trim())
            .unwrap();
    let store = StoreId(Uuid::from_bytes([0x10; 16]));
    let key = StoreKey::from_bytes(KeyId(Uuid::from_bytes([1; 16])), [17; 32]);
    let member = member(0x33);
    let envelope = SingleChunkObject::decode(&bytes).unwrap();
    let path = ObjectPath::store_log(store, DeviceId(1), 1.try_into().unwrap());
    let opened = object::open(&envelope, &path, &key).unwrap();
    // Creation verifies with the initial pair it supplies, without a view.
    opened
        .verify(&coven_database::StoreLogState::default())
        .unwrap();
    let entry = opened.entry;
    let seal = |entry: &StoreLogEntry, key: &StoreKey| {
        object::seal(
            store,
            entry,
            coven_format::FormatVersion::CURRENT,
            key,
            &member,
        )
        .unwrap()
    };
    assert_eq!(seal(&entry, &key), bytes);
    assert_eq!(seal(&entry, &key), seal(&entry, &key));

    let mut changed = entry.clone();
    let StoreChange::CreateStore { name, .. } = &mut changed.change else {
        panic!("creation fixture")
    };
    *name = "Another store name".into();
    // The second key changes only the clear key id, retaining the encryption key.
    let other_key = StoreKey::from_bytes(KeyId(Uuid::from_bytes([2; 16])), [17; 32]);
    for (entry, key) in [(&changed, &key), (&entry, &other_key)] {
        let changed = seal(entry, key);
        let other = SingleChunkObject::decode(&changed).unwrap();
        assert_ne!(envelope.chunk()[..24], other.chunk()[..24]);
        assert_eq!(object::open(&other, &path, key).unwrap().entry, *entry);
        assert_eq!(changed, seal(entry, key));
    }
}

#[test]
fn folder_ownership_includes_implicit_own_registration_and_waits_for_it() {
    use crate::replay::tests as history;
    let mut h = history::History::new();
    h.all(0, 0, history::add(1, MemberRole::Member));
    h.all(1, 1, history::device(1));
    h.all(0, 0, history::add(2, MemberRole::Member));
    h.push(1, 1, &[0, 1], history::device(90));
    let candidate = &h.entries[4];
    let waiting = checked_log(&h.entries[..2]);
    assert!(!object::ready(&waiting, candidate, store(1)).unwrap());
    let log = checked_log(&h.entries[..4]);
    assert!(object::ready(&log, candidate, store(1)).unwrap());
}

#[test]
fn folder_ownership_waits_for_a_registration_in_the_recorded_past() {
    use crate::replay::tests as history;
    let mut h = history::History::new();
    h.all(0, 0, history::device(70));
    h.all(0, 70, history::device(90));
    let candidate = &h.entries[2];
    assert!(!object::ready(&checked_log(&h.entries[..1]), candidate, store(1)).unwrap());
    assert!(object::ready(&checked_log(&h.entries[..2]), candidate, store(1)).unwrap());
}

#[test]
fn a_dropped_registration_does_not_prevent_registering_again() {
    use crate::replay::tests as history;
    let mut h = history::History::new();
    // This self-registration has a valid identity but the signer is not yet a member.
    h.all(1, 1, history::device(1));
    h.all(0, 0, history::add(1, MemberRole::Member));
    let log = checked_log(&h.entries);
    assert_eq!(
        log.replay.entries[&h.entries[1].position],
        EntryOutcome::Dropped(coven_database::DropReason::NotAllowed)
    );
    h.all(1, 1, history::device(1));
    let candidate = h.entries.last().unwrap();
    assert!(object::ready(&log, candidate, store(1)).unwrap());
    let (_, replay) = crate::replay::tests::replay_entry(&log, candidate.clone());
    assert_eq!(replay.entries[&candidate.position], EntryOutcome::Kept);
    assert_eq!(replay.state.devices[&DeviceId(1)].member, candidate.author);
}

#[test]
fn folder_ownership_uses_registration_in_the_recorded_view() {
    use crate::replay::tests as history;
    let mut h = history::History::new();
    h.all(0, 0, history::add(1, MemberRole::Member));
    h.all(1, 1, history::device(1));
    h.all(1, 1, history::device(70));
    h.all(1, 70, history::device(90));
    h.all(
        0,
        0,
        StoreChange::RemoveDevice {
            device: DeviceId(70),
        },
    );
    h.all(0, 0, history::device(70));
    let log = checked_log(&h.entries);
    assert_eq!(
        log.replay.state.devices[&DeviceId(70)].member,
        history::member(0)
    );

    h.push(1, 70, &[0, 1, 2, 3, 4], history::device(91));
    let mut candidate = h.entries.last().unwrap().clone();
    assert!(object::ready(&log, &candidate, store(1)).unwrap());
    candidate.author = history::member(0);
    assert!(matches!(
        object::ready(&log, &candidate, store(1)),
        Err(Refusal::WrongIdentity { .. })
    ));

    candidate.had_read = log.positions();
    candidate
        .had_read
        .0
        .retain(|id| id.device != candidate.position.device);
    assert!(object::ready(&log, &candidate, store(1)).unwrap());
    candidate.author = history::member(1);
    assert!(matches!(
        object::ready(&log, &candidate, store(1)),
        Err(Refusal::WrongIdentity { .. })
    ));
}

#[tokio::test]
async fn another_member_cannot_claim_an_empty_registered_store_log_folder() {
    for added_device in [DeviceId(70), DeviceId(90)] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(2), store(1)).await;
        a.create(key(1)).await;
        a.add(&b.member, MemberRole::Member).await;
        b.sync().await;
        for id in [DeviceId(2), DeviceId(70)] {
            b.sync
                .make_and_upload_entry(StoreChange::AddDevice {
                    device: id,
                    name: "registered".into(),
                    replaces: None,
                })
                .await
                .unwrap();
        }
        a.sync().await;
        let log = a.log().await;
        assert_eq!(
            log.replay.state.devices[&DeviceId(70)].member,
            b.member.member()
        );
        assert!(log
            .entries
            .iter()
            .all(|e| e.entry.position.device != DeviceId(70)));
        let entry = StoreLogEntry {
            position: EntryId {
                device: DeviceId(70),
                number: 1,
            },
            timestamp: coven_merge::Timestamp::new(2000, 0, DeviceId(70)).unwrap(),
            author: a.member.member(),
            had_read: log.positions(),
            change: StoreChange::AddDevice {
                device: added_device,
                name: "impostor".into(),
                replaces: None,
            },
        };
        let ring = a.custody.read().unwrap().unwrap();
        let bytes = object::seal(
            store(1),
            &entry,
            coven_format::FormatVersion::CURRENT,
            ring.store_key(key(1)).unwrap(),
            &a.member,
        )
        .unwrap();
        storage
            .create(&object::path(store(1), entry.position), &bytes)
            .await
            .unwrap();
        a.sync().await;
        assert_eq!(
            a.db.pending::<crate::PendingRecord>().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(
                LogRefusal {
                    object: LogObject::Entry(entry.position),
                    failure: RefusalCode::WrongIdentity,
                },
                a.device().await,
            )]
        );
        assert_eq!(a.log().await, log);
    }
}

#[tokio::test]
async fn a_new_device_registers_itself_before_publishing_other_entries() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    assert!(b.sync.bootstrap_member().await.unwrap());
    let unrelated = StoreChange::AddDevice {
        device: DeviceId(90),
        name: "another install".into(),
        replaces: None,
    };
    assert!(matches!(
        b.sync.make_and_upload_entry(unrelated.clone()).await,
        Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
    ));
    assert!(b.db.local_store_log().await.unwrap().upload.is_none());
    b.sync.load_new_device("new install", None).await.unwrap();
    let first = EntryId {
        device: DeviceId(2),
        number: 1,
    };
    let log = b.log().await;
    assert_eq!(log.replay.entries[&first], EntryOutcome::Kept);
    assert_eq!(
        log.replay.state.devices[&DeviceId(2)].member,
        b.member.member()
    );
    let second = b.sync.make_and_upload_entry(unrelated).await.unwrap();
    assert_eq!(second, EntryId { number: 2, ..first });
    a.sync().await;
    assert_eq!(a.log().await, b.log().await);
    assert!(a
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_replacement_device_registers_itself_in_its_first_entry() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    a.create(key(1)).await;
    a.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(70),
            name: "old install".into(),
            replaces: None,
        })
        .await
        .unwrap();
    assert!(b.sync.bootstrap_member().await.unwrap());
    let registration = StoreChange::AddDevice {
        device: DeviceId(2),
        name: "replacement install".into(),
        replaces: Some(DeviceId(70)),
    };
    let first = b
        .sync
        .make_and_upload_entry(registration.clone())
        .await
        .unwrap();
    assert_eq!(
        first,
        EntryId {
            device: DeviceId(2),
            number: 1,
        }
    );
    a.sync().await;
    let log = a.log().await;
    assert_eq!(log.replay.entries[&first], EntryOutcome::Kept);
    assert_eq!(
        log.replay.state.devices[&DeviceId(2)].member,
        b.member.member()
    );
    assert_eq!(log.entries.last().unwrap().entry.change, registration);
    assert_eq!(log, b.log().await);
    assert!(a
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn queued_creation_rejects_a_copy_with_the_right_identity_and_wrong_material() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let key_hash = a.sync.test_key(Audience::Store, key(1));
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        a.sync
            .make_and_upload_entry(StoreChange::CreateStore {
                store: store(1),
                name: a.directory.settings().unwrap().name,
                admin: public(&a.member),
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "owner".into(),
                    principal: coven_format::S3Principal::Console,
                },
                device_name: "Ana".into(),
                key: key(1),
                key_hash,
            })
            .await,
        Err(SyncError::Storage(_))
    ));
    let mut upload = a.db.local_store_log().await.unwrap().upload.unwrap();
    object::seal_upload(store(1), &StoreLog::default(), &upload, None, &a.member).unwrap();
    let wrong = StoreKey::from_bytes(key(1), [0x91; 32]);
    assert_ne!(wrong.hash(), key_hash);
    upload.sealing.keys[0].bytes = keys::store_copy(
        &upload.sealing.keys[0].path(store(1)),
        &wrong,
        &a.member.member(),
        &a.member.keys().public_keys().sealing,
    )
    .unwrap();
    assert!(
        matches!(object::seal_upload(store(1), &StoreLog::default(), &upload, None, &a.member),
        Err(SyncError::Key(coven_crypto::MaterialError::StoreKeyConflict(id))) if id == key(1))
    );
}

#[tokio::test]
async fn stores_with_the_same_member_and_device_create_concurrently_without_colliding() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let mut b = device(storage.clone(), 1, member(1), store(2)).await;
    let (first, second) = tokio::join!(a.create(key(1)), b.create(key(1)));
    assert_eq!(first, second);
    let a_log = a.log().await;
    let b_log = b.log().await;
    assert_ne!(a_log, b_log);
    a.sync().await;
    b.sync().await;
    assert_eq!(a.log().await, a_log);
    assert_eq!(b.log().await, b_log);
    for d in [&a, &b] {
        assert!(d
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        assert!(storage
            .list(&ObjectPrefix::all(d.directory.id()))
            .await
            .unwrap()
            .iter()
            .all(|object| object.path.store() == d.directory.id()));
    }
}

#[tokio::test]
async fn creation_identity_is_checked_in_the_authenticated_plaintext() {
    use coven_format::Object;

    let storage = storage();
    let mut a = device(storage, 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let entry = a.log().await.entries.remove(0).entry;
    let ring = a.custody.read().unwrap().unwrap();
    let key = ring.store_key(key(1)).unwrap();
    let path = object::path(store(1), entry.position);
    let bytes = object::seal(
        store(1),
        &entry,
        coven_format::FormatVersion::CURRENT,
        key,
        &a.member,
    )
    .unwrap();
    let envelope = SingleChunkObject::decode(&bytes).unwrap();
    let prefix = envelope.prefix();
    for defect in ["number", "timestamp", "admin", "path", "store"] {
        let mut plain = Object::StoreLog(entry.clone()).encode().unwrap();
        let mut stored_path = path.clone();
        match defect {
            "number" => plain[15..23].copy_from_slice(&2u64.to_be_bytes()),
            "timestamp" => plain[31..39].copy_from_slice(&2u64.to_be_bytes()),
            // Another signing key in place of the supplied initial pair's.
            "admin" => {
                let ours = *a.member.keys().public_keys().signing.as_bytes();
                let at = plain
                    .windows(32)
                    .position(|window| window == ours)
                    .expect("creation supplies its admin's signing key");
                plain[at..at + 32]
                    .copy_from_slice(member(2).keys().public_keys().signing.as_bytes());
            }
            "path" => {
                stored_path = ObjectPath::store_log(store(1), DeviceId(2), 1.try_into().unwrap())
            }
            "store" => stored_path = object::path(store(2), entry.position),
            _ => unreachable!(),
        }
        let chunk = key
            .derive()
            .seal_object_chunk(
                stored_path.as_str(),
                &prefix.encode().unwrap(),
                0,
                0,
                &plain,
            )
            .unwrap();
        let mut bytes = prefix.encode_chunk(&chunk).unwrap();
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend_from_slice(
            a.member
                .keys()
                .sign_object(stored_path.as_str(), &hash.finish())
                .as_bytes(),
        );
        let opened = object::open(
            &SingleChunkObject::decode(&bytes).unwrap(),
            &stored_path,
            key,
        );
        match defect {
            "path" | "timestamp" | "store" => {
                assert!(matches!(opened, Err(Refusal::WrongIdentity { .. })))
            }
            // Creation's own supplied pair must verify it.
            "admin" => assert!(matches!(
                opened
                    .unwrap()
                    .verify(&coven_database::StoreLogState::default()),
                Err(Refusal::Signature { .. })
            )),
            _ => assert!(
                matches!(opened, Err(Refusal::Parse { .. })),
                "{defect}: {opened:?}"
            ),
        }
    }
}

#[tokio::test]
async fn setup_checks_its_known_creation_before_committing() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let first = a.create(key(1)).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    b.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "second".into(),
            replaces: None,
        })
        .await
        .unwrap();
    a.sync().await;
    let before = a.log().await;
    let access = coven_format::MemberAccess::S3AccessKey {
        access_key_id: "fixture-access-key".into(),
        principal: coven_format::S3Principal::Console,
    };
    a.sync
        .setup_storage(
            storage.clone(),
            access.clone(),
            "first".into(),
            Box::new(|| Ok(Box::new(|| Ok(())))),
        )
        .await
        .unwrap();
    assert_eq!(a.log().await, before);
    let path = object::path(store(1), first);
    let size = storage.read(&path).await.unwrap().len();
    storage.corrupt_byte(&path, size - 1).await.unwrap();
    let failure = a
        .sync
        .setup_storage(
            storage,
            access,
            "first".into(),
            Box::new(|| panic!("damaged creation must not commit connection state")),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        failure,
        SyncError::Damaged(DamagedObject {
            failure: Refusal::Signature { .. },
            ..
        })
    ));
    assert_eq!(a.log().await, before);
}

#[tokio::test]
async fn damaged_entries_block_only_their_device_and_retain_each_cause() {
    for damage in ["decryption", "signature", "moved", "parse"] {
        let storage = Arc::new(
            MemoryStorage::builder()
                .transfer_limits(1024 * 1024, 64 * 1024)
                .build()
                .unwrap(),
        );
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        let mut c = device(storage.clone(), 3, member(1), store(1)).await;
        a.create(key(1)).await;
        a.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(3),
                name: "receiver".into(),
                replaces: None,
            })
            .await
            .unwrap();
        b.sync().await;
        b.sync.load_new_device("second", None).await.unwrap();
        a.sync().await;
        c.catch_up().await;
        let broken = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(21),
                name: "one".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let later = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(22),
                name: "two".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let independent = b
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(23),
                name: "three".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let path = object::path(store(1), broken);
        let original = storage.read(&path).await.unwrap();
        let mut bytes = original.clone();
        match damage {
            "decryption" => bytes[24] ^= 1,
            "signature" => {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            "moved" => bytes = storage.read(&object::path(store(1), later)).await.unwrap(),
            "parse" => {
                use coven_format::sealed_single::SingleChunkPrefix;
                let prefix = SingleChunkPrefix::StoreLog { key: key(1) };
                let ring = a.custody.read().unwrap().unwrap();
                let chunk = ring
                    .store_key(key(1))
                    .unwrap()
                    .derive()
                    .seal_object_chunk(
                        path.as_str(),
                        &prefix.encode().unwrap(),
                        0,
                        0,
                        b"invalid frame",
                    )
                    .unwrap();
                bytes = prefix.encode_chunk(&chunk).unwrap();
                let mut hash = coven_crypto::ObjectHasher::new();
                hash.update(&bytes);
                bytes.extend_from_slice(
                    a.member
                        .keys()
                        .sign_object(path.as_str(), &hash.finish())
                        .as_bytes(),
                );
            }
            _ => unreachable!(),
        }
        storage.delete(&path).await.unwrap();
        storage.create(&path, &bytes).await.unwrap();
        c.sync.step().await.unwrap();
        let record = LogRefusal {
            object: LogObject::Entry(broken),
            failure: match damage {
                "decryption" | "moved" => RefusalCode::Decryption,
                "signature" => RefusalCode::Signature,
                "parse" => RefusalCode::Parse,
                _ => unreachable!(),
            },
        };
        assert_eq!(
            c.db.pending::<crate::PendingRecord>().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(3))]
        );
        let received = c.log().await;
        assert!(received.replay.entries.contains_key(&independent));
        assert!(received.replay.entries.contains_key(&EntryId {
            number: broken.number - 1,
            ..broken
        }));
        assert!(!received.replay.entries.contains_key(&broken));
        assert!(!received.replay.entries.contains_key(&later));
        let before = storage.reads().await;
        c.sync().await;
        let entry_reads = |reads: Vec<(ObjectPath, u64, u64)>| {
            reads
                .into_iter()
                .filter(|(path, _, _)| path.store_log_position().is_some())
                .collect::<Vec<_>>()
        };
        assert_eq!(entry_reads(storage.reads().await), entry_reads(before));
        c.writes().post_positions().await.unwrap();
        // C's post names B's entry in its signed view; Ana applies it before
        // the post can verify (D9).
        a.sync.sync_store_log().await.unwrap();
        a.writes().download_writes().await.unwrap();
        assert_eq!(
            a.db.pending::<crate::PendingRecord>().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(3))]
        );
        assert!(c.log().await.replay.entries.contains_key(&independent));
        assert!(!c.log().await.replay.entries.contains_key(&broken));
        assert!(!c.log().await.replay.entries.contains_key(&later));
        // A different installed version permits one new attempt, which checks
        // the retained immutable bytes rather than downloading them again.
        c.db.test_pending_version("previous-version").await.unwrap();
        c.restart(storage.clone()).await;
        let before = storage.reads().await;
        c.sync().await;
        assert_eq!(entry_reads(storage.reads().await), entry_reads(before));
        assert!(!c.log().await.replay.entries.contains_key(&broken));
        assert!(!c.log().await.replay.entries.contains_key(&later));
        assert_eq!(
            c.db.pending::<crate::PendingRecord>().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(3))]
        );
    }
}

#[tokio::test]
async fn invalid_causal_past_and_timestamps_are_damaged() {
    for defect in ["closure", "timestamp", "own position", "author", "signer"] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        let creation = a.create(key(1)).await;
        b.sync().await;
        b.sync.load_new_device("second", None).await.unwrap();
        a.sync().await;
        let prior = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(30),
                name: "prior".into(),
                replaces: None,
            })
            .await
            .unwrap();
        b.sync().await;
        let indirect = b
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(31),
                name: "indirect".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let mut bad = coven_format::store_log::StoreLogEntry {
            position: EntryId {
                device: DeviceId(70),
                number: 1,
            },
            timestamp: coven_merge::Timestamp::new(2000, 0, DeviceId(70)).unwrap(),
            author: a.member.member(),
            had_read: coven_format::value::EntryPositions(vec![creation, indirect]),
            change: StoreChange::AddDevice {
                device: DeviceId(40),
                name: "bad".into(),
                replaces: None,
            },
        };
        match defect {
            "closure" => (),
            "timestamp" => {
                bad.had_read.0 = vec![prior];
                bad.timestamp = coven_merge::Timestamp::new(1, 0, bad.position.device).unwrap();
            }
            "own position" => {
                bad.position = EntryId {
                    number: prior.number + 1,
                    ..prior
                };
                bad.timestamp = coven_merge::Timestamp::new(2000, 0, prior.device).unwrap();
                // Encode a valid other-device past, then alter its authenticated bytes below.
                bad.had_read.0 = vec![EntryId {
                    device: DeviceId(70),
                    ..prior
                }];
            }
            "author" | "signer" => {
                bad.position = EntryId {
                    number: prior.number + 1,
                    ..prior
                };
                bad.timestamp = coven_merge::Timestamp::new(2000, 0, prior.device).unwrap();
                if defect == "author" {
                    bad.author = member(2).member();
                }
                bad.had_read = a.log().await.positions();
                bad.had_read.0.retain(|id| id.device != bad.position.device);
            }
            _ => unreachable!(),
        }
        let ring = a.custody.read().unwrap().unwrap();
        let signer = match defect {
            "author" => member(2),
            // The device owner's own id with a key its recorded view never
            // bound to it: a key change happens only by a kept replacement.
            "signer" => {
                coven_crypto::MemberIdentity::new(a.member.member(), member(2).keys().clone())
            }
            _ => a.member.clone(),
        };
        let path = object::path(store(1), bad.position);
        let key = ring.store_key(key(1)).unwrap();
        let mut bytes = object::seal(
            store(1),
            &bad,
            coven_format::FormatVersion::CURRENT,
            key,
            &signer,
        )
        .unwrap();
        if defect == "own position" {
            let sealed = SingleChunkObject::decode(&bytes).unwrap();
            let prefix = sealed.prefix();
            let aad = prefix.encode().unwrap();
            let mut plain = key
                .derive()
                .open_object_chunk(path.as_str(), &aad, 0, 0, sealed.chunk())
                .unwrap();
            // D6: frame prefix, entry id, timestamp, author, then the had-read count.
            assert_eq!(&plain[55..59], &1u32.to_be_bytes());
            plain[59..67].copy_from_slice(&bad.position.device.0.to_be_bytes());
            let chunk = key
                .derive()
                .seal_object_chunk(path.as_str(), &aad, 0, 0, &plain)
                .unwrap();
            bytes = prefix.encode_chunk(&chunk).unwrap();
            let mut hash = coven_crypto::ObjectHasher::new();
            hash.update(&bytes);
            bytes.extend_from_slice(
                signer
                    .keys()
                    .sign_object(path.as_str(), &hash.finish())
                    .as_bytes(),
            );
        }
        storage.create(&path, &bytes).await.unwrap();
        let c = device(storage.clone(), 3, member(1), store(1)).await;
        c.sync.step().await.unwrap();
        let records = c.db.pending::<crate::PendingRecord>().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].as_log_refusal().unwrap().object,
            LogObject::Entry(bad.position)
        );
        assert_eq!(records[0].reported_by, DeviceId(3));
        assert_eq!(
            u8::from(records[0].as_log_refusal().unwrap().failure),
            match defect {
                "closure" | "timestamp" => 5,
                "author" => 6,
                "signer" => 1,
                "own position" => 2,
                _ => unreachable!(),
            },
            "{defect} must retain the refused check's D8 tag"
        );
    }
}

#[tokio::test]
async fn bootstrap_reports_a_stuck_entry_instead_of_waiting_for_membership_forever() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let first = a.create(key(1)).await;
    let path = object::path(store(1), first);
    let size = storage.read(&path).await.unwrap().len();
    storage.corrupt_byte(&path, size - 1).await.unwrap();
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    assert!(
        matches!(b.sync.bootstrap_member().await, Err(SyncError::Pending(record)) if record.subject == crate::PendingSubject::Entry(first))
    );
    let reads = storage.reads().await;
    assert!(matches!(
        b.sync.bootstrap_member().await,
        Err(SyncError::Pending(_))
    ));
    assert_eq!(storage.reads().await, reads);
}

#[tokio::test]
async fn an_entry_that_cannot_be_read_after_listing_is_not_a_permanent_judgment() {
    for missing in [true, false] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        a.create(key(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        b.sync().await;
        let entry = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(3),
                name: "third".into(),
                replaces: None,
            })
            .await
            .unwrap();
        let path = object::path(store(1), entry);
        let bytes = storage.read(&path).await.unwrap();
        let (listed, waiting) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        storage
            .hold_next_listing(ObjectPrefix::store_logs(store(1)), listed, held)
            .await;
        let interrupt = async {
            waiting.await.unwrap();
            if missing {
                storage.delete(&path).await.unwrap();
            } else {
                storage
                    .set_faults(Faults {
                        fail_next: 1,
                        ..Faults::none()
                    })
                    .await;
            }
            resume.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(b.sync.step(), interrupt);
        if missing {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(SyncError::Storage(_))));
        }
        assert!(b
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        assert!(!b.log().await.replay.entries.contains_key(&entry));
        if missing {
            storage.create(&path, &bytes).await.unwrap();
        }
        b.sync().await;
        assert!(b.log().await.replay.entries.contains_key(&entry));
    }
}

#[tokio::test]
async fn a_waiting_entry_reseals_with_its_recorded_key_after_rotation() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.add(&member(2), MemberRole::Member).await;
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    b.sync.load_new_device("second", None).await.unwrap();
    a.sync().await;
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
            replaces: None,
        })
        .await
        .is_err());
    let pending = a.db.local_store_log().await.unwrap().upload.unwrap();
    let path = object::path(store(1), pending.entry.position);
    let first = storage.read(&path).await.unwrap();
    b.sync.test_rotate(Audience::Store, key(2)).await;
    // Apply the downloaded rotation while the local entry is still queued.
    let rotation = b.log().await.entries.last().unwrap().entry.clone();
    let (entry, replay) = crate::replay::tests::replay_entry(&a.log().await, rotation);
    a.db.apply_store_log::<crate::PendingRecord>(entry, replay, Vec::new())
        .await
        .unwrap();
    a.custody
        .persist(&b.custody.read().unwrap().unwrap())
        .unwrap();
    a.restart(storage.clone()).await;
    assert_eq!(a.newest(Audience::Store).await, key(2));
    assert_eq!(a.reseal(&pending).await, first);
    // Remove the fixture's first copy so the retry must actually reproduce it.
    storage.delete(&path).await.unwrap();
    a.sync().await;
    assert_eq!(storage.read(&path).await.unwrap(), first);
    assert!(a.db.local_store_log().await.unwrap().upload.is_none());
}

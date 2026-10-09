use super::*;
use coven_database::StuckLog;
use coven_format::stuck::{LogObject, StuckFailure, StuckRecord};

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
            })
            .await
            .unwrap();
        b.sync().await;
        let broken = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(21),
                name: "one".into(),
            })
            .await
            .unwrap();
        let later = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(22),
                name: "two".into(),
            })
            .await
            .unwrap();
        let independent = b
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(23),
                name: "three".into(),
            })
            .await
            .unwrap();
        let path = object::path(broken);
        let original = storage.read(&path).await.unwrap();
        let mut bytes = original.clone();
        match damage {
            "decryption" => bytes[24] ^= 1,
            "signature" => {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            "moved" => bytes = storage.read(&object::path(later)).await.unwrap(),
            "parse" => {
                use coven_format::sealed_single::SingleChunkPrefix;
                let prefix = SingleChunkPrefix::StoreLog {
                    key: key(1),
                    origin: None,
                };
                let ring = a.custody.unlock().unwrap().unwrap();
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
                        .sign_object(path.as_str(), &hash.finish())
                        .as_bytes(),
                );
            }
            _ => unreachable!(),
        }
        storage.delete(&path).await.unwrap();
        storage.create(&path, &bytes).await.unwrap();
        assert!(c.sync.step().await.unwrap().is_empty());
        let record = StuckRecord {
            object: LogObject::Entry(broken),
            failure: match damage {
                "decryption" | "moved" => StuckFailure::Decryption,
                "signature" => StuckFailure::Signature,
                "parse" => StuckFailure::Parse,
                _ => unreachable!(),
            },
        };
        assert_eq!(
            c.db.stuck_logs().await.unwrap(),
            vec![StuckLog {
                record,
                reported_by: None
            }]
        );
        let before = storage.reads().await;
        c.sync().await;
        assert_eq!(storage.reads().await, before);
        c.writes().post_positions().await.unwrap();
        a.writes().download_writes().await.unwrap();
        assert_eq!(
            a.db.stuck_logs().await.unwrap(),
            vec![StuckLog {
                record,
                reported_by: Some(DeviceId(3))
            }]
        );
        assert!(c.log().await.replay.entries.contains_key(&independent));
        assert!(!c.log().await.replay.entries.contains_key(&broken));
        assert!(!c.log().await.replay.entries.contains_key(&later));
        storage.delete(&path).await.unwrap();
        storage.create(&path, &original).await.unwrap();
        c.sync().await;
        assert!(!c.log().await.replay.entries.contains_key(&later));
        c.db.test_stuck_version("previous-version").await.unwrap();
        c.restart(storage.clone()).await;
        c.sync().await;
        assert!(c.log().await.replay.entries.contains_key(&later));
        assert!(c.db.stuck_logs().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn invalid_causal_past_and_timestamps_are_damaged() {
    for defect in ["closure", "timestamp", "own position", "author"] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        let creation = a.create(key(1)).await;
        b.sync().await;
        let prior = a
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(30),
                name: "prior".into(),
            })
            .await
            .unwrap();
        b.sync().await;
        let indirect = b
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(31),
                name: "indirect".into(),
            })
            .await
            .unwrap();
        let mut bad = coven_format::store_log::StoreLogEntry {
            position: EntryId {
                device: DeviceId(70),
                number: 1,
            },
            timestamp: coven_merge::Timestamp::new(2000, 0, DeviceId(70)).unwrap(),
            author: a.member.member_id(),
            had_read: coven_format::value::EntryPositions(vec![creation, indirect]),
            change: StoreChange::AddDevice {
                device: DeviceId(40),
                name: "bad".into(),
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
            "author" => {
                bad.position = EntryId {
                    number: prior.number + 1,
                    ..prior
                };
                bad.timestamp = coven_merge::Timestamp::new(2000, 0, prior.device).unwrap();
                bad.author = member(2).member_id();
                bad.had_read.0.clear();
            }
            _ => unreachable!(),
        }
        let ring = a.custody.unlock().unwrap().unwrap();
        let signer = if defect == "author" {
            member(2)
        } else {
            a.member.clone()
        };
        let path = object::path(bad.position);
        let key = ring.store_key(key(1)).unwrap();
        let mut bytes = object::seal(&bad, key, &signer).unwrap();
        if defect == "own position" {
            let sealed = SingleChunkObject::decode(&bytes).unwrap();
            let prefix = sealed.prefix();
            let aad = prefix.encode().unwrap();
            let mut plain = key
                .derive()
                .open_object_chunk(path.as_str(), &aad, 0, 0, sealed.chunk())
                .unwrap();
            // D6: frame prefix, entry id, timestamp, author, then the had-read count.
            assert_eq!(&plain[71..75], &1u32.to_be_bytes());
            plain[75..83].copy_from_slice(&bad.position.device.0.to_be_bytes());
            let chunk = key
                .derive()
                .seal_object_chunk(path.as_str(), &aad, 0, 0, &plain)
                .unwrap();
            bytes = prefix.encode_chunk(&chunk).unwrap();
            let mut hash = coven_crypto::ObjectHasher::new();
            hash.update(&bytes);
            bytes.extend_from_slice(signer.sign_object(path.as_str(), &hash.finish()).as_bytes());
        }
        storage.create(&path, &bytes).await.unwrap();
        let c = device(storage.clone(), 3, member(1), store(1)).await;
        assert!(c.sync.step().await.unwrap().is_empty());
        assert_eq!(
            c.db.stuck_logs().await.unwrap(),
            vec![StuckLog {
                record: StuckRecord {
                    object: LogObject::Entry(bad.position),
                    failure: StuckFailure::Parse
                },
                reported_by: None,
            }]
        );
    }
}

#[tokio::test]
async fn bootstrap_reports_a_stuck_entry_instead_of_waiting_for_membership_forever() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let first = a.create(key(1)).await;
    let path = object::path(first);
    let size = storage.read(&path).await.unwrap().len();
    storage.corrupt_byte(&path, size - 1).await.unwrap();
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    assert!(
        matches!(b.sync.bootstrap_member().await, Err(SyncError::StuckLog(record)) if record.object == LogObject::Entry(first))
    );
    let reads = storage.reads().await;
    assert!(matches!(
        b.sync.bootstrap_member().await,
        Err(SyncError::StuckLog(_))
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
            })
            .await
            .unwrap();
        let path = object::path(entry);
        let bytes = storage.read(&path).await.unwrap();
        let (listed, waiting) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        storage
            .hold_next_listing(ObjectPrefix::store_logs(), listed, held)
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
            assert!(result.unwrap().is_empty());
        } else {
            assert!(matches!(result, Err(SyncError::Storage(_))));
        }
        assert!(b.db.stuck_logs().await.unwrap().is_empty());
        assert!(!b.log().await.replay.entries.contains_key(&entry));
        if missing {
            storage.create(&path, &bytes).await.unwrap();
        }
        b.sync().await;
        assert!(b.log().await.replay.entries.contains_key(&entry));
    }
}

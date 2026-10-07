use super::*;

#[tokio::test]
async fn damaged_entries_block_only_their_device_and_retain_each_cause() {
    for damage in ["decryption", "signature", "moved", "parse"] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        let mut b = device(storage.clone(), 2, member(1), store(1)).await;
        let mut c = device(storage.clone(), 3, member(1), store(1)).await;
        a.create(key(1)).await;
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
        let report = c.sync().await;
        assert_eq!(report.damaged_objects.len(), 1, "{damage}");
        assert_eq!(report.damaged_objects[0].path, path.as_str());
        match (&report.damaged_objects[0].failure, damage) {
            (ObjectCheckFailure::Decryption(_), "decryption" | "moved")
            | (ObjectCheckFailure::Signature(_), "signature")
            | (ObjectCheckFailure::Parse(_), "parse") => (),
            other => panic!("wrong check: {other:?}"),
        }
        assert!(c.log().await.replay.entries.contains_key(&independent));
        assert!(!c.log().await.replay.entries.contains_key(&broken));
        assert!(!c.log().await.replay.entries.contains_key(&later));
        storage.delete(&path).await.unwrap();
        storage.create(&path, &original).await.unwrap();
        c.sync().await;
        assert!(c.log().await.replay.entries.contains_key(&later));
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
        let mut c = device(storage.clone(), 3, member(1), store(1)).await;
        let report = c.sync().await;
        assert_eq!(report.damaged_objects.len(), 1, "{defect}");
        assert_eq!(report.damaged_objects[0].path, path.as_str());
        assert!(matches!(
            report.damaged_objects[0].failure,
            ObjectCheckFailure::Parse(_)
        ));
    }
}

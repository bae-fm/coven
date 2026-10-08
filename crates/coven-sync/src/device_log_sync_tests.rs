use super::*;
use coven_crypto::{MemberKeys, StoreKey, StoreKeyring};
use coven_database::{Migration, RowIdentity, SyncedTable};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::id_source::{DeviceId, IdSource, KeyId, SequentialIds, StoreId};
use coven_storage::test_utils::{Faults, MemoryStorage};
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

use crate::store_log_sync::tests::Device;

fn member() -> MemberKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([3; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
fn storage() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .transfer_limits(1024 * 1024, 64 * 1024)
            .build()
            .unwrap(),
    )
}
async fn device(storage: Arc<MemoryStorage>, number: u64) -> Device {
    let ids = Arc::new(SequentialIds::new());
    for _ in 0..number {
        ids.new_device_id();
    }
    let device = Device::new(
        storage,
        number,
        member(),
        StoreId(Uuid::from_u128(1)),
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
        vec![schema::initial()],
        ids,
    )
    .await;
    device
        .custody
        .persist(&StoreKeyring::new(StoreKey::from_bytes(
            KeyId(Uuid::from_u128(1)),
            [7; 32],
        )))
        .unwrap();
    device
}
async fn group(storage: Arc<MemoryStorage>, count: u64) -> Vec<Device> {
    let mut devices = Vec::new();
    for number in 1..=count {
        devices.push(device(storage.clone(), number).await);
    }
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: StoreId(Uuid::from_u128(1)),
            name: "Store".into(),
            admin: MemberPublicKeys {
                signing: member().member_id(),
                sealing: member().sealing_public_key(),
            },
            key: KeyId(Uuid::from_u128(1)),
            device_name: "one".into(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner-key".into(),
            },
        })
        .await
        .unwrap();
    for number in 2..=count {
        devices[0]
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(number),
                name: number.to_string(),
            })
            .await
            .unwrap();
    }
    for device in &mut devices {
        device.sync.sync_store_log().await.unwrap();
    }
    devices
}
async fn sql(db: &Database, sql: &'static str) {
    db.write(move |context| {
        context.execute_batch(sql)?;
        Ok(())
    })
    .await
    .unwrap();
}
async fn rows(db: &Database) -> Vec<(String, String, String)> {
    db.read(|sql| {
        Ok(
            sql.query("SELECT id,title,body FROM notes ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?,
        )
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn offline_writes_converge_on_two_and_three_devices() {
    for count in [2, 3] {
        let storage = storage();
        let mut devices = group(storage, count).await;
        for (i, device) in devices.iter_mut().enumerate() {
            device
                .db
                .write(move |sql| {
                    sql.execute(
                        "INSERT INTO notes VALUES('same',?1,?2)",
                        (format!("title-{i}"), format!("body-{i}")),
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(device.writes.upload_writes().await.unwrap().len(), 1);
        }
        for device in &mut devices {
            device.writes.download_writes().await.unwrap();
        }
        let expected = rows(&devices[0].db).await;
        for device in &devices {
            assert_eq!(rows(&device.db).await, expected);
        }
    }
}

#[tokio::test]
async fn causal_wait_applies_when_the_missing_write_arrives() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    devices[0].writes.upload_writes().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    sql(&devices[1].db, "UPDATE notes SET title='second'").await;
    devices[1].writes.upload_writes().await.unwrap();
    let path = ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap());
    let bytes = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    devices[2].writes.download_writes().await.unwrap();
    assert!(rows(&devices[2].db).await.is_empty());

    devices[2].clock.set(UNIX_EPOCH + Duration::from_secs(6));
    devices[2].writes.download_writes().await.unwrap();
    assert!(rows(&devices[2].db).await.is_empty());
    storage.create(&path, &bytes).await.unwrap();
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await, rows(&devices[1].db).await);
}

async fn posted(storage: &MemoryStorage, device: &Device, number: u64) -> PostedPositions {
    let path = ObjectPath::positions(DeviceId(number));
    let bytes = storage.read(&path).await.unwrap();
    let sealed = coven_format::sealed_single::SingleChunkObject::decode(&bytes).unwrap();
    let SingleChunkPrefix::PostedPositions(key_id) = sealed.prefix() else {
        panic!("positions prefix");
    };
    let prefix = sealed.prefix().encode().unwrap();
    let key = device.custody.unlock().unwrap().unwrap();
    let plain = key
        .store_key(key_id)
        .unwrap()
        .derive()
        .open_object_chunk(path.as_str(), &prefix, 0, 0, sealed.chunk())
        .unwrap();
    let Object::PostedPositions(posted) = Object::decode(&plain).unwrap() else {
        panic!("positions");
    };
    posted
}

#[tokio::test]
async fn positions_never_include_an_unpublished_local_write() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    assert!(devices[0].writes.post_positions().await.unwrap());
    let path = ObjectPath::positions(DeviceId(1));
    let before = storage.read(&path).await.unwrap();
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    assert!(!devices[0].writes.post_positions().await.unwrap());
    assert_eq!(storage.read(&path).await.unwrap(), before);
    devices[0].writes.upload_writes().await.unwrap();
    assert!(devices[0].writes.post_positions().await.unwrap());
    let posted = posted(&storage, &devices[0], 1).await;
    assert_eq!(posted.schema_version, 1);
    assert_eq!(
        posted.writes.0,
        vec![WriteId {
            device: DeviceId(1),
            number: 1
        }]
    );
}

#[tokio::test]
async fn fixed_bytes_survive_restart_and_a_lost_completion_reply() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let device = &mut devices[0];
    device
        .db
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('one','first',?1)",
                ["x".repeat(100_000)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let mut faults = Faults::none();
    faults.fail_next = 1;
    storage.set_faults(faults).await;
    assert!(device.writes.upload_writes().await.is_err());
    let expected = writes::resealed(device).await.1;
    device
        .reopen(
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![schema::initial()],
        )
        .await;
    let mut faults = Faults::none();
    faults.lose_completion_reply = true;
    storage.set_faults(faults).await;
    assert!(device.writes.upload_writes().await.is_err());
    let path = ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap());
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert_eq!(device.writes.upload_writes().await.unwrap().len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert!(device.writes.upload_writes().await.unwrap().is_empty());
}

#[path = "write_seal_tests.rs"]
mod circles;

#[path = "schema_sync_tests.rs"]
mod schema;
#[path = "write_upload_tests.rs"]
mod streams;
#[path = "write_download_tests.rs"]
mod validation;
mod writes {
    use super::*;
    use coven_crypto::ObjectHasher;
    use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
    use coven_format::write::WriteRecord;
    use coven_format::write_stream::{decode_plaintext, WriteEncoder};

    pub(super) async fn queued(db: &Database) -> WriteRecord {
        db.read_oldest_upload(|upload| {
            let coven_database::WaitingUpload {
                header_frame,
                parts,
                ..
            } = upload;
            let mut bytes = header_frame;
            for (_, part) in parts {
                for chunk in part {
                    bytes.extend(chunk?);
                }
            }
            Ok::<_, DbError>(decode_plaintext(&bytes).unwrap())
        })
        .await
        .unwrap()
        .unwrap()
    }

    pub(super) async fn resealed(device: &Device) -> (WriteId, Vec<u8>) {
        let ring = device.custody.unlock().unwrap().unwrap();
        let member = device.identity.unlock().unwrap().unwrap();
        device
            .db
            .read_oldest_upload(move |upload| {
                let write = upload.header.header.position;
                let mut bytes = Vec::new();
                crate::write_seal::seal(upload, &ring, &member, &mut |piece| {
                    bytes.extend_from_slice(piece);
                    Ok(())
                })?;
                Ok::<_, SyncError>((write, bytes))
            })
            .await
            .unwrap()
            .unwrap()
    }

    pub(super) fn seal(
        record: &WriteRecord,
        path: &ObjectPath,
        alter: impl Fn(u64, &mut Vec<u8>),
    ) -> Vec<u8> {
        let encoder = WriteEncoder::new(record).unwrap();
        let prefix = WriteObjectPrefix {
            store_key: KeyId(Uuid::from_u128(1)),
            part_keys: vec![KeyId(Uuid::from_u128(1)); record.parts.len()],
        };
        let mut layout = WriteObjectLayout::new(
            prefix,
            encoder.header_frame(),
            encoder
                .header()
                .parts
                .iter()
                .map(|p| p.plaintext_length)
                .collect(),
        )
        .unwrap();
        let aad = layout.prefix().unwrap();
        let mut bytes = aad.clone();
        let key = StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32]).derive();
        let mut put = |plain: &[u8]| {
            let coordinate = layout.next_chunk().unwrap();
            let mut plain = plain.to_vec();
            alter(coordinate.section, &mut plain);
            let sealed = key
                .seal_object_chunk(
                    path.as_str(),
                    &aad,
                    coordinate.section,
                    coordinate.index,
                    &plain,
                )
                .unwrap();
            bytes.extend(layout.encode_chunk(&sealed).unwrap());
        };
        put(encoder.header_frame());
        for part in 0..record.parts.len() {
            for chunk in encoder.part_chunks(part).unwrap() {
                put(&chunk.unwrap());
            }
        }
        let mut hash = ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend(
            layout
                .signature(&member().sign_object(path.as_str(), &hash.finish()))
                .unwrap(),
        );
        layout.finish(&[]).unwrap();
        bytes
    }

    pub(super) async fn publish(storage: &MemoryStorage, record: &WriteRecord) {
        let path = crate::write_seal::path(record.header.position);
        storage
            .create(&path, &seal(record, &path, |_, _| {}))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn fingerprint_comparison_leaves_recovery_to_an_explicit_reload() {
    for count in [2, 3] {
        let storage = storage();
        let mut devices = group(storage.clone(), count).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','shared','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        for device in &mut devices {
            device.writes.download_writes().await.unwrap();
            assert!(device.writes.post_positions().await.unwrap());
        }
        devices[0]
            .sync
            .write_snapshot(Audience::Store)
            .await
            .unwrap();
        let expected = devices[0]
            .writes
            .current_positions()
            .await
            .unwrap()
            .unwrap()
            .fingerprints;
        devices[1]
            .db
            .test_damage_fingerprint(Audience::Store)
            .await
            .unwrap();
        devices[1].writes.post_positions().await.unwrap();
        let damaged = devices[1]
            .writes
            .current_positions()
            .await
            .unwrap()
            .unwrap()
            .fingerprints;
        assert_ne!(damaged, expected);
        for _ in 0..2 {
            for device in &mut devices {
                device.writes.compare_fingerprints().await.unwrap();
                assert!(device.db.operations().await.unwrap().is_empty());
            }
            assert_eq!(
                devices[1]
                    .writes
                    .current_positions()
                    .await
                    .unwrap()
                    .unwrap()
                    .fingerprints,
                damaged
            );
        }
        devices[1].sync.reload_from_snapshots().await.unwrap();
        devices[1].writes.post_positions().await.unwrap();
        for device in &mut devices {
            assert_eq!(
                device
                    .writes
                    .current_positions()
                    .await
                    .unwrap()
                    .unwrap()
                    .fingerprints,
                expected
            );
        }
    }
}

#[tokio::test]
async fn groceries_example_converges_in_every_arrival_order() {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let storage = storage();
        let mut devices = group(storage.clone(), 4).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('42','Grocery list','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        for device in devices.iter_mut().skip(1) {
            device.writes.download_writes().await.unwrap();
        }
        devices[0].clock.set(UNIX_EPOCH + Duration::from_secs(2));
        sql(&devices[0].db, "UPDATE notes SET title='Groceries'").await;
        devices[0].writes.upload_writes().await.unwrap();
        devices[1].writes.download_writes().await.unwrap();
        sql(&devices[1].db, "UPDATE notes SET title='Weekly groceries'").await;
        devices[1].writes.upload_writes().await.unwrap();
        devices[2].clock.set(UNIX_EPOCH + Duration::from_secs(3));
        sql(&devices[2].db, "UPDATE notes SET title='Shopping'").await;
        devices[2].writes.upload_writes().await.unwrap();
        let mut objects = Vec::new();
        for (device, number) in [(1, 2), (2, 1), (3, 1)] {
            let path = ObjectPath::device_log(DeviceId(device), number.try_into().unwrap());
            objects.push((path.clone(), storage.read(&path).await.unwrap()));
            storage.delete(&path).await.unwrap();
        }
        for index in order {
            let (path, bytes) = &objects[index];
            storage.create(path, bytes).await.unwrap();
            devices[3].writes.download_writes().await.unwrap();
        }
        assert_eq!(rows(&devices[3].db).await[0].1, "Shopping", "{order:?}");
        devices[3].writes.download_writes().await.unwrap();
        for device in devices.iter_mut().take(3) {
            device.writes.download_writes().await.unwrap();
        }
        let expected = devices[3]
            .db
            .sync_state(vec![(
                Audience::Store,
                StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32])
                    .derive()
                    .fingerprint_hasher(),
            )])
            .await
            .unwrap()
            .fingerprints;
        for device in &devices[..3] {
            let actual = device
                .db
                .sync_state(vec![(
                    Audience::Store,
                    StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32])
                        .derive()
                        .fingerprint_hasher(),
                )])
                .await
                .unwrap()
                .fingerprints;
            assert_eq!(actual, expected, "{order:?}");
        }
    }
}

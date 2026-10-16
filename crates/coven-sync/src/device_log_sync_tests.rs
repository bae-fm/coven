use super::*;
use coven_crypto::{MemberIdentity, StoreKey, StoreKeyring};
use coven_database::{ApplyOutcome, LogRefusal, Migration, RowIdentity, SyncedTable};
use coven_format::pending::RefusalCode;
use coven_format::store_log::StoreChange;
use coven_format::{objects::PostedPositions, sealed_single::SingleChunkPrefix, Object};
use coven_foundation::id_source::{DeviceId, IdSource, KeyId, SequentialIds, StoreId};
use coven_storage::test_utils::{Faults, MemoryStorage};
use coven_storage::{ObjectPath, ObjectPrefix};
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

use crate::posted_positions::tests::signed_positions;
use crate::store_log_sync::tests::Device;

pub(crate) const STORE_ID: StoreId = StoreId(Uuid::from_u128(1));

fn member() -> MemberIdentity {
    crate::store_log_sync::tests::member(3)
}
pub(crate) fn storage() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .transfer_limits(1024 * 1024, 64 * 1024)
            .build()
            .unwrap(),
    )
}
async fn device(storage: Arc<MemoryStorage>, number: u64) -> Device {
    device_of(storage, number, member()).await
}
/// Device `number` of `member`, holding the group's store key.
pub(crate) async fn device_of(
    storage: Arc<MemoryStorage>,
    number: u64,
    member: MemberIdentity,
) -> Device {
    let ids = Arc::new(SequentialIds::new());
    for _ in 0..number {
        ids.new_device_id();
    }
    let device = Device::new(
        storage,
        number,
        member,
        STORE_ID,
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
pub(crate) async fn group(storage: Arc<MemoryStorage>, count: u64) -> Vec<Device> {
    let mut devices = Vec::new();
    for number in 1..=count {
        devices.push(device(storage.clone(), number).await);
    }
    let key_hash = devices[0]
        .sync
        .test_key(coven_merge::Audience::Store, KeyId(Uuid::from_u128(1)));
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: STORE_ID,
            name: "Store".into(),
            admin: member().keys().public_keys(),
            key: KeyId(Uuid::from_u128(1)),
            key_hash,
            device_name: "one".into(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner-key".into(),
                principal: coven_format::S3Principal::Console,
            },
        })
        .await
        .unwrap();
    // Each further installation registers itself (§10).
    for (device, number) in devices[1..].iter_mut().zip(2..) {
        device.sync.sync_store_log().await.unwrap();
        device
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                name: number.to_string(),
                replaces: None,
            })
            .await
            .unwrap();
    }
    for device in &mut devices {
        device.sync.sync_store_log().await.unwrap();
    }
    devices
}
pub(crate) async fn sql(db: &Database, sql: &'static str) {
    db.write(move |context| {
        context.execute_batch(sql)?;
        Ok(())
    })
    .await
    .unwrap();
}
pub(crate) async fn rows(db: &Database) -> Vec<(String, String, String)> {
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
    let path = ObjectPath::device_log(STORE_ID, DeviceId(1), 1.try_into().unwrap());
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
    let path = ObjectPath::positions(STORE_ID, DeviceId(number));
    let bytes = storage.read(&path).await.unwrap();
    let sealed = coven_format::sealed_single::SingleChunkObject::decode(&bytes).unwrap();
    let SingleChunkPrefix::PostedPositions(key_id) = sealed.prefix() else {
        panic!("positions prefix");
    };
    let prefix = sealed.prefix().encode().unwrap();
    let key = device.custody.read().unwrap().unwrap();
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
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
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
    assert_eq!(&expected[..3], &[32, 0, 1]);
    let fixed = device
        .db
        .read_oldest_upload(|upload| Ok::<_, DbError>(upload.keys.unwrap()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixed.format, coven_format::FormatVersion::V1);
    device
        .reopen(
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![schema::initial()],
        )
        .await;
    device
        .db
        .prepare_write_upload(|_, _, _| {
            panic!("reopening an attempted write must not select a new format or keys")
        })
        .await
        .unwrap();
    assert_eq!(writes::resealed(device).await.1, expected);
    device.sync.sync_store_log().await.unwrap();
    assert_eq!(
        device
            .db
            .read_oldest_upload(|upload| { Ok::<_, DbError>(upload.keys.unwrap()) })
            .await
            .unwrap()
            .unwrap(),
        fixed
    );
    let mut faults = Faults::none();
    faults.lose_completion_reply = true;
    storage.set_faults(faults).await;
    assert!(device.writes.upload_writes().await.is_err());
    let path = ObjectPath::device_log(STORE_ID, DeviceId(1), 1.try_into().unwrap());
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert_eq!(device.writes.upload_writes().await.unwrap().len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert!(device.writes.upload_writes().await.unwrap().is_empty());
}

/// A removed device's write counts only when storage first published it no
/// more than thirty storage days after the removal; a later one is consumed
/// without effects and reported as landed too late (§10, D5).
#[tokio::test]
async fn a_removed_devices_write_counts_only_within_thirty_storage_days() {
    use coven_foundation::clock::{Clock, FixedClock};
    for (offset, counts) in [(0, true), (1, false)] {
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .transfer_limits(1024 * 1024, 64 * 1024)
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut devices = group(storage.clone(), 3).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        let record = writes::queued(&devices[0].db).await;
        devices[1]
            .sync
            .make_and_upload_entry(StoreChange::RemoveDevice {
                device: DeviceId(1),
            })
            .await
            .unwrap();
        clock.set(clock.now() + Duration::from_secs(30 * 24 * 60 * 60 + offset));
        writes::publish(&storage, &record).await;
        devices[2].sync.sync_store_log().await.unwrap();
        devices[2].writes.download_writes().await.unwrap();
        assert_eq!(rows(&devices[2].db).await.len(), usize::from(counts));
        let dropped = devices[2]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .any(|r| {
                r.subject == crate::PendingSubject::Write(record.header.position)
                    && r.reason
                        == crate::PendingReason::Dropped(coven_database::DropReason::LandedTooLate)
            });
        assert_eq!(dropped, !counts);
        // Consumed either way: a later write of the same log is not held back.
        assert!(devices[2]
            .db
            .sync_state(vec![])
            .await
            .unwrap()
            .positions
            .covers(record.header.position));
    }
}

#[path = "write_seal_tests.rs"]
pub(crate) mod circles;

#[path = "pending_logs_tests.rs"]
mod routing;
#[path = "schema_sync_tests.rs"]
pub(crate) mod schema;
#[path = "write_upload_tests.rs"]
mod streams;
#[path = "write_download_tests.rs"]
mod validation;
pub(crate) mod writes {
    use super::*;
    use coven_crypto::ObjectHasher;
    use coven_format::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
    use coven_format::write::WriteRecord;
    use coven_format::write_stream::{decode_plaintext, WriteEncoder};

    pub(crate) async fn queued(db: &Database) -> WriteRecord {
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

    pub(crate) async fn resealed(device: &Device) -> (WriteId, Vec<u8>) {
        let ring = device.custody.read().unwrap().unwrap();
        let member = device.identity.read().unwrap().unwrap();
        device
            .db
            .read_oldest_upload(move |upload| {
                let write = upload.header.header.position;
                let mut bytes = Vec::new();
                crate::write_seal::seal(STORE_ID, upload, &ring, &member, &mut |piece| {
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
            format: coven_format::FormatVersion::CURRENT,
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
        let put = |layout: &mut WriteObjectLayout, plain: &[u8]| {
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
            layout.encode_chunk(&sealed).unwrap()
        };
        let header = put(&mut layout, encoder.header_frame());
        let hasher = key.authorship_hasher();
        let roots: Vec<_> = record
            .parts
            .iter()
            .map(|part| {
                coven_format::authorship::root(
                    &hasher,
                    &coven_format::authorship::part_leaves(part),
                )
                .unwrap()
            })
            .collect();
        let signature = member()
            .keys()
            .sign_write_header(path.as_str(), &aad, &header, &roots);
        bytes.extend(&header);
        bytes.extend(
            layout
                .tail(&coven_format::sealed_write::HeaderTail { roots, signature })
                .unwrap(),
        );
        for part in 0..record.parts.len() {
            for chunk in encoder.part_chunks(part).unwrap() {
                let piece = put(&mut layout, &chunk.unwrap());
                bytes.extend(piece);
            }
        }
        let mut hash = ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend(
            layout
                .signature(&member().keys().sign_object(path.as_str(), &hash.finish()))
                .unwrap(),
        );
        layout.finish(&[]).unwrap();
        bytes
    }

    pub(crate) async fn publish(storage: &MemoryStorage, record: &WriteRecord) {
        let path = crate::write_seal::path(STORE_ID, record.header.position);
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
        devices[0].write_snapshot(Audience::Store).await.unwrap();
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
            let path =
                ObjectPath::device_log(STORE_ID, DeviceId(device), number.try_into().unwrap());
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

mod stuck {
    use super::writes::{publish, queued};
    use super::*;
    use coven_format::pending::RefusalCode;

    async fn refuse(storage: &MemoryStorage, devices: &mut [Device], reload: bool) -> LogRefusal {
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        let mut record = queued(&devices[0].db).await;
        let coven_merge::Operation::Insert(values) = &mut record.parts[0].rows[0].change.operation
        else {
            panic!("insert")
        };
        values.get_mut("id").unwrap().value = coven_format::value::Value::Text("another".into());
        devices[0].writes.upload_writes().await.unwrap();
        storage
            .delete(&crate::write_seal::path(STORE_ID, record.header.position))
            .await
            .unwrap();
        publish(storage, &record).await;
        if reload {
            // Without a snapshot the reload's target stops at local positions;
            // the refused write is first seen by ordinary discovery after it.
            devices[1].sync.reload_from_snapshots().await.unwrap();
            assert!(devices[1]
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .is_empty());
        }
        devices[1].writes.download_writes().await.unwrap();
        assert!(rows(&devices[1].db).await.is_empty());
        LogRefusal {
            object: LogObject::Write(record.header.position),
            failure: RefusalCode::InvalidWrite,
        }
    }

    #[tokio::test]
    async fn a_refused_merge_is_observed_without_retries_and_reported_to_its_author() {
        let storage = storage();
        let mut devices = group(storage.clone(), 3).await;
        let mut live = devices[1].db.subscribe_pending::<crate::PendingRecord>();
        assert!(live.next().await.unwrap().is_empty());
        let record = refuse(&storage, &mut devices, false).await;
        assert_eq!(
            live.next().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(2))]
        );
        let before = storage.reads().await;
        devices[1].writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
        devices[0].writes.download_writes().await.unwrap();
        assert!(devices[0]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        for receiver in &mut devices[1..] {
            receiver.writes.download_writes().await.unwrap();
            receiver.writes.post_positions().await.unwrap();
        }
        // Reports are read even while the author has unposted local edits.
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('pending','local','body')",
        )
        .await;
        let mut author = devices[0].db.subscribe_pending::<crate::PendingRecord>();
        assert!(author.next().await.unwrap().is_empty());
        devices[0].writes.download_writes().await.unwrap();
        assert_eq!(
            author.next().await.unwrap(),
            vec![
                crate::PendingRecord::log_refusal(record, DeviceId(2)),
                crate::PendingRecord::log_refusal(record, DeviceId(3)),
            ]
        );
        devices[0].writes.upload_writes().await.unwrap();
        devices[0].writes.post_positions().await.unwrap();
        assert!(posted(&storage, &devices[0], 1).await.pending.is_empty());
        // A damaged report has no authority; a device that stops posting is not inferred stuck.
        let path = ObjectPath::positions(STORE_ID, DeviceId(2));
        let mut bytes = storage.read(&path).await.unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        storage.replace(&path, &bytes).await.unwrap();
        storage
            .delete(&ObjectPath::positions(STORE_ID, DeviceId(3)))
            .await
            .unwrap();
        devices[0].writes.download_writes().await.unwrap();
        assert_eq!(
            author.next().await.unwrap(),
            vec![crate::PendingRecord {
                subject: crate::PendingSubject::Positions(DeviceId(2)),
                reason: crate::PendingReason::InvalidPositions(RefusalCode::Signature.into()),
                reported_by: DeviceId(1),
            }]
        );
    }

    async fn reopen(storage: Arc<MemoryStorage>, device: &mut Device) {
        device
            .reopen(
                storage,
                vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                vec![schema::initial()],
            )
            .await;
    }

    #[tokio::test]
    async fn a_library_version_change_retries_each_stuck_write_once() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        let record = refuse(&storage, &mut devices, false).await;
        let device = &mut devices[1];
        let before = storage.reads().await;
        reopen(storage.clone(), device).await;
        device.writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
        device
            .db
            .test_pending_version("previous-version")
            .await
            .unwrap();
        reopen(storage.clone(), device).await;
        assert_eq!(
            device.db.pending::<crate::PendingRecord>().await.unwrap(),
            [crate::PendingRecord::log_refusal(record, DeviceId(2))]
        );
        assert!(crate::pending_logs::local_refusals(&device.db)
            .await
            .unwrap()
            .is_empty());
        device.writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
        assert_eq!(
            crate::pending_logs::local_refusals(&device.db)
                .await
                .unwrap(),
            vec![record]
        );
        assert_eq!(
            device.db.pending::<crate::PendingRecord>().await.unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(2))]
        );
        let before = storage.reads().await;
        device.writes.download_writes().await.unwrap();
        reopen(storage.clone(), device).await;
        device.writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
    }

    #[tokio::test]
    async fn a_reset_reload_clears_judgments_and_the_next_post_withdraws_reports() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        refuse(&storage, &mut devices, false).await;
        devices[1].writes.post_positions().await.unwrap();
        devices[0].writes.download_writes().await.unwrap();
        assert_eq!(
            devices[0]
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .len(),
            1
        );
        let snapshot = devices[0].write_snapshot(Audience::Store).await.unwrap();
        devices[0]
            .sync
            .make_and_upload_entry(StoreChange::Reset { snapshot })
            .await
            .unwrap();
        // The resetting device reloads its own snapshot too (§19.3).
        devices[0].sync.sync_store_log().await.unwrap();
        devices[1].sync.sync_store_log().await.unwrap();
        assert!(devices[1]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(rows(&devices[1].db).await, rows(&devices[0].db).await);
        devices[1].writes.post_positions().await.unwrap();
        devices[0].writes.download_writes().await.unwrap();
        assert!(devices[0]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn retention_does_not_read_refused_logs_or_delete_files_without_their_references() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        refuse(&storage, &mut devices, false).await;
        let unused = ObjectPath::file(
            STORE_ID,
            DeviceId(2),
            coven_foundation::id_source::FileId(Uuid::from_u128(99)),
        );
        storage.create(&unused, b"file").await.unwrap();
        devices[1]
            .db
            .add_file_candidates(std::collections::BTreeSet::from([(
                unused.device().unwrap(),
                unused.file_id().unwrap(),
            )]))
            .await
            .unwrap();
        let before = storage.reads().await;
        devices[1].sync.run_retention().await.unwrap();
        assert_eq!(storage.reads().await, before);
        assert_eq!(storage.read(&unused).await.unwrap(), b"file");
    }

    #[tokio::test]
    async fn failed_reads_are_retried_without_refusal() {
        // The GET of the next write fails before or after its response
        // starts. Neither is a miss or a refusal, and a later pass applies it.
        use coven_storage::test_utils::BodyFailure;
        for interrupted in [false, true] {
            let storage = storage();
            let mut devices = group(storage.clone(), 2).await;
            sql(
                &devices[0].db,
                "INSERT INTO notes VALUES('one','title','body')",
            )
            .await;
            devices[0].writes.upload_writes().await.unwrap();
            let path = ObjectPath::device_log(STORE_ID, DeviceId(1), 1.try_into().unwrap());
            let mut faults = Faults::none();
            if interrupted {
                faults
                    .body_failures
                    .insert(path.clone(), BodyFailure::Interrupted(10));
            } else {
                faults
                    .read_failures
                    .insert(path.clone(), coven_storage::StorageFailure::Network);
            }
            storage.set_faults(faults).await;
            let result = devices[1].writes.download_writes().await;
            assert!(
                matches!(result, Err(SyncError::Storage(_))),
                "{interrupted}: {result:?}"
            );
            assert!(devices[1]
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .is_empty());
            storage.set_faults(Faults::none()).await;
            devices[1].writes.download_writes().await.unwrap();
            assert_eq!(rows(&devices[1].db).await, rows(&devices[0].db).await);
        }
    }

    #[tokio::test]
    async fn a_refusal_seen_after_a_reload_is_recorded_without_advancing_its_log() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        let record = refuse(&storage, &mut devices, true).await;
        assert_eq!(
            devices[1]
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap(),
            vec![crate::PendingRecord::log_refusal(record, DeviceId(2))]
        );
        let before = storage.reads().await;
        devices[1].writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
    }

    #[tokio::test]
    async fn a_required_stuck_gap_waits_for_a_covering_snapshot_and_keeps_local_edits() {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        devices[1].writes.download_writes().await.unwrap();
        sql(&devices[1].db, "UPDATE notes SET title='local'").await;
        let expected = rows(&devices[1].db).await;
        let queued = queued(&devices[1].db).await;
        // Without eviction, the reload correctly reuses its authenticated copy.
        let maximum = devices[1].db.input_budget().await.unwrap().maximum;
        devices[1].db.set_input_budget(0).await.unwrap();
        devices[1].db.set_input_budget(maximum).await.unwrap();
        let object = storage
            .list(&ObjectPrefix::device_logs(STORE_ID))
            .await
            .unwrap()
            .pop()
            .unwrap();
        storage
            .corrupt_byte(&object.path, object.size as usize - 1)
            .await
            .unwrap();
        // The reload waits on the refused write, whose own record is the
        // refusal; the wait retries, without downloading the write again (§15).
        for attempt in 0..2 {
            devices[1].elapse_retry_delays();
            let before = storage.reads().await;
            assert!(matches!(
                devices[1].sync.reload_from_snapshots().await,
                Err(SyncError::Pending(record))
                    if record.reason == crate::PendingReason::Waits(
                        crate::Prerequisite::Object(object.path.clone())
                    )
            ));
            assert!(devices[1]
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .iter()
                .any(|r| r.subject
                    == crate::PendingSubject::Write(object.path.write_id().unwrap())
                    && r.reason == crate::PendingReason::Refused(RefusalCode::Signature.into())));
            if attempt == 1 {
                assert_eq!(storage.reads().await, before);
            }
            assert_eq!(rows(&devices[1].db).await, expected);
            assert_eq!(super::writes::queued(&devices[1].db).await, queued);
        }
        // A snapshot covering the refused write lands: the waiting reload
        // loads it without reading the write, and the local edit stays on top.
        devices[0].write_snapshot(Audience::Store).await.unwrap();
        let before = storage.reads().await;
        devices[1].elapse_retry_delays();
        devices[1].sync.reload_from_snapshots().await.unwrap();
        assert!(!storage.reads().await[before.len()..]
            .iter()
            .any(|(path, _, _)| *path == object.path));
        assert_eq!(devices[1].sync.pending_reload().await.unwrap(), None);
        assert_eq!(rows(&devices[1].db).await, expected);
        assert_eq!(super::writes::queued(&devices[1].db).await, queued);
    }
}

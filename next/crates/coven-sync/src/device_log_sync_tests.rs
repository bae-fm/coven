use super::*;
use crate::StoreLogSync;
use coven_crypto::{custody::InMemoryCustody, MemberKeys, StoreKey, StoreKeyring};
use coven_database::{CovenMigrationPolicy, DatabaseBuilder, Migration, RowIdentity, SyncedTable};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::{
    clock::FixedClock,
    files::{StoreDir, StoreLayout},
    id_source::{DeviceId, IdSource, KeyId, SequentialIds, StoreId},
};
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    StorageConfig,
};
use std::time::{Duration, UNIX_EPOCH};
use uuid::Uuid;

struct Device {
    sync: DeviceLogSync,
    log: StoreLogSync,
    db: Database,
    directory: StoreDir,
    keys: Arc<InMemoryCustody<StoreKeyring>>,
    identity: Arc<InMemoryCustody<MemberKeys>>,
    clock: Arc<FixedClock>,
    ids: Arc<SequentialIds>,
    _temporary: tempfile::TempDir,
}

fn member() -> MemberKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([3; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
fn storage() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::new(
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "test".into(),
                endpoint: None,
                prefix: "store".into(),
            },
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )
        .unwrap()
        .with_transfer_limits(1024 * 1024, 64 * 1024)
        .unwrap(),
    )
}
async fn open(directory: StoreDir, clock: Arc<FixedClock>) -> Database {
    DatabaseBuilder::new(directory)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL);")])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending).clock(clock).open().await.unwrap()
}
async fn device(storage: Arc<MemoryStorage>, number: u64) -> Device {
    let temporary = tempfile::tempdir().unwrap();
    let ids = Arc::new(SequentialIds::new());
    for _ in 1..number {
        ids.new_device_id();
    }
    let directory = StoreLayout::new(temporary.path().into())
        .create_store_dir(StoreId(Uuid::from_u128(1)), "Store", ids.as_ref())
        .unwrap();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let db = open(directory.clone(), clock.clone()).await;
    let keys = Arc::new(InMemoryCustody::new(StoreKeyring::new(
        StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32]),
    )));
    let identity = Arc::new(InMemoryCustody::new(member()));
    let sync = DeviceLogSync::new(storage.clone(), db.clone(), keys.clone(), identity.clone());
    let log = StoreLogSync::new(
        storage,
        db.clone(),
        keys.clone(),
        identity.clone(),
        clock.clone(),
        ids.clone(),
        directory.clone(),
    );
    Device {
        sync,
        log,
        db,
        directory,
        keys,
        identity,
        clock,
        ids,
        _temporary: temporary,
    }
}
async fn group(storage: Arc<MemoryStorage>, count: u64) -> Vec<Device> {
    let mut devices = Vec::new();
    for number in 1..=count {
        devices.push(device(storage.clone(), number).await);
    }
    devices[0]
        .log
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
            .log
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(number),
                name: number.to_string(),
            })
            .await
            .unwrap();
    }
    for device in &mut devices {
        device.log.sync_store_log().await.unwrap();
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
            assert_eq!(device.sync.upload_writes().await.unwrap().len(), 1);
        }
        for device in &mut devices {
            let report = device.sync.download_writes().await.unwrap();
            assert!(report.waiting.is_empty(), "{report:?}");
            assert!(report.damaged_objects.is_empty(), "{report:?}");
        }
        let expected = rows(&devices[0].db).await;
        for device in &devices {
            assert_eq!(rows(&device.db).await, expected);
        }
    }
}

#[tokio::test]
async fn causal_wait_is_reported_then_applies_when_the_missing_write_arrives() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    devices[0].sync.upload_writes().await.unwrap();
    devices[1].sync.download_writes().await.unwrap();
    sql(&devices[1].db, "UPDATE notes SET title='second'").await;
    devices[1].sync.upload_writes().await.unwrap();
    let path = ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap());
    let bytes = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let report = devices[2].sync.download_writes().await.unwrap();
    assert_eq!(report.waiting.len(), 1);
    assert_eq!(
        report.waiting[0].waiting_for,
        vec![WriteId {
            device: DeviceId(1),
            number: 1
        }]
    );
    assert!(rows(&devices[2].db).await.is_empty());
    let since = report.waiting[0].since;
    devices[2].clock.set(UNIX_EPOCH + Duration::from_secs(6));
    assert_eq!(
        devices[2].sync.download_writes().await.unwrap().waiting[0].since,
        since
    );
    storage.create(&path, &bytes).await.unwrap();
    assert!(devices[2]
        .sync
        .download_writes()
        .await
        .unwrap()
        .waiting
        .is_empty());
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
    let key = device.keys.unlock().unwrap().unwrap();
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
    assert!(devices[0].sync.post_positions().await.unwrap());
    let path = ObjectPath::positions(DeviceId(1));
    let before = storage.read(&path).await.unwrap();
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    assert!(!devices[0].sync.post_positions().await.unwrap());
    assert_eq!(storage.read(&path).await.unwrap(), before);
    devices[0].sync.upload_writes().await.unwrap();
    assert!(devices[0].sync.post_positions().await.unwrap());
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
    assert!(device.sync.upload_writes().await.is_err());
    let expected = device
        .db
        .read_oldest_upload(|upload| {
            let coven_database::WaitingUpload::Sealed { bytes, .. } = upload else {
                panic!("seal committed before send")
            };
            Ok::<_, DbError>(bytes.collect::<Result<Vec<_>, _>>()?.concat())
        })
        .await
        .unwrap()
        .unwrap();
    device.db.close().await.unwrap();
    device.db = open(device.directory.clone(), device.clock.clone()).await;
    device.sync = DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.keys.clone(),
        device.identity.clone(),
    );
    let mut faults = Faults::none();
    faults.lose_completion_reply = true;
    storage.set_faults(faults).await;
    assert!(device.sync.upload_writes().await.is_err());
    let path = ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap());
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert_eq!(device.sync.upload_writes().await.unwrap().len(), 1);
    assert_eq!(storage.read(&path).await.unwrap(), expected);
    assert!(device.sync.upload_writes().await.unwrap().is_empty());
}

#[path = "write_seal_tests.rs"]
mod circles;

#[path = "schema_sync_tests.rs"]
mod schema;
#[path = "write_upload_tests.rs"]
mod streams;
#[path = "write_download_tests.rs"]
mod validation;
#[path = "write_test_utils.rs"]
mod writes;

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
        devices[0].sync.upload_writes().await.unwrap();
        for device in devices.iter_mut().skip(1) {
            device.sync.download_writes().await.unwrap();
        }
        devices[0].clock.set(UNIX_EPOCH + Duration::from_secs(2));
        sql(&devices[0].db, "UPDATE notes SET title='Groceries'").await;
        devices[0].sync.upload_writes().await.unwrap();
        devices[1].sync.download_writes().await.unwrap();
        sql(&devices[1].db, "UPDATE notes SET title='Weekly groceries'").await;
        devices[1].sync.upload_writes().await.unwrap();
        devices[2].clock.set(UNIX_EPOCH + Duration::from_secs(3));
        sql(&devices[2].db, "UPDATE notes SET title='Shopping'").await;
        devices[2].sync.upload_writes().await.unwrap();
        let mut objects = Vec::new();
        for (device, number) in [(1, 2), (2, 1), (3, 1)] {
            let path = ObjectPath::device_log(DeviceId(device), number.try_into().unwrap());
            objects.push((path.clone(), storage.read(&path).await.unwrap()));
            storage.delete(&path).await.unwrap();
        }
        for index in order {
            let (path, bytes) = &objects[index];
            storage.create(path, bytes).await.unwrap();
            let report = devices[3].sync.download_writes().await.unwrap();
            assert!(report.damaged_objects.is_empty(), "{order:?}: {report:?}");
        }
        assert_eq!(rows(&devices[3].db).await[0].1, "Shopping", "{order:?}");
        assert!(devices[3]
            .sync
            .download_writes()
            .await
            .unwrap()
            .waiting
            .is_empty());
        for device in devices.iter_mut().take(3) {
            device.sync.download_writes().await.unwrap();
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

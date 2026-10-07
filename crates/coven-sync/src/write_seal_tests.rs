use super::*;
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::CircleId;

fn identity(seed: u8) -> MemberKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([seed; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
pub(super) async fn household(storage: Arc<MemoryStorage>) -> Vec<Device> {
    let mut devices = Vec::new();
    for number in 1..=3 {
        let mut device = device(storage.clone(), number).await;
        device
            .identity
            .persist(&identity(number as u8 + 2))
            .unwrap();
        device.db.close().await.unwrap();
        device.db=DatabaseBuilder::new(device.directory.clone())
            .synced_tables(vec![SyncedTable::new("notes",RowIdentity::SharedKey),SyncedTable::new("pins",RowIdentity::IndependentUuid).audience_column("audience")])
            .migrations(vec![Migration::sql(1,"notes","CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL);"),Migration::sql(2,"circles","CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT NOT NULL);")])
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending).clock(device.clock.clone()).open().await.unwrap();
        device.sync = DeviceLogSync::new(
            storage.clone(),
            device.db.clone(),
            device.keys.clone(),
            device.identity.clone(),
        );
        device.log = StoreLogSync::new(
            storage.clone(),
            device.db.clone(),
            device.keys.clone(),
            device.identity.clone(),
            device.clock.clone(),
            device.ids.clone(),
            device.directory.clone(),
        );
        devices.push(device);
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
    for seed in [4, 5] {
        let identity = identity(seed);
        devices[0]
            .log
            .make_and_upload_entry(StoreChange::AddMember {
                keys: MemberPublicKeys {
                    signing: identity.member_id(),
                    sealing: identity.sealing_public_key(),
                },
                role: MemberRole::Admin,
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: format!("member-{seed}"),
                },
            })
            .await
            .unwrap();
    }
    for (i, device) in devices.iter_mut().enumerate().skip(1) {
        device.log.sync_store_log().await.unwrap();
        device
            .log
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(i as u64 + 1),
                name: i.to_string(),
            })
            .await
            .unwrap();
    }
    devices[0].log.sync_store_log().await.unwrap();
    let circle = CircleId(Uuid::from_u128(10));
    devices[0]
        .log
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle,
            name: "private".into(),
            key: KeyId(Uuid::from_u128(10)),
        })
        .await
        .unwrap();
    devices[0]
        .log
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(4).member_id(),
        })
        .await
        .unwrap();
    for device in &mut devices {
        device.log.sync_store_log().await.unwrap();
    }
    devices
}
async fn count(db: &Database) -> i64 {
    db.read(|sql| Ok(sql.query_row("SELECT count(*) FROM pins", [], |row| row.get(0))?))
        .await
        .unwrap()
}

#[tokio::test]
async fn unreadable_circle_parts_are_skipped_but_missing_members_keys_wait() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    sql(&devices[0].db,"INSERT INTO notes VALUES('one','public','body'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
    devices[0].sync.upload_writes().await.unwrap();
    devices[1]
        .keys
        .persist(&StoreKeyring::new(StoreKey::from_bytes(
            KeyId(Uuid::from_u128(1)),
            [7; 32],
        )))
        .unwrap();
    let report = devices[1].sync.download_writes().await.unwrap();
    assert_eq!(report.waiting.len(), 1, "{report:?}");
    assert!(rows(&devices[1].db).await.is_empty());
    assert!(matches!(
        devices[1].log.reload_from_snapshots().await,
        Err(SyncError::KeyUnavailable(_))
    ));
    let report = devices[2].sync.download_writes().await.unwrap();
    assert!(report.waiting.is_empty());
    assert!(report.damaged_objects.is_empty());
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[2].log.reload_from_snapshots().await.unwrap();
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[1].log.sync_store_log().await.unwrap();
    assert!(devices[1]
        .sync
        .download_writes()
        .await
        .unwrap()
        .waiting
        .is_empty());
    assert_eq!(count(&devices[1].db).await, 1);
}

#[tokio::test]
async fn new_uploads_wait_for_a_current_members_replacement_circle_key() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let ring = devices[1].keys.unlock().unwrap().unwrap();
    devices[0]
        .log
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(3).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].log.sync_store_log().await.unwrap();
    devices[1].keys.persist(&ring).unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    assert!(!log.replay.state.circles[&CircleId(Uuid::from_u128(10))].deleted);
    sql(&devices[1].db,"INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','new')").await;
    assert!(
        matches!(devices[1].sync.upload_writes().await, Err(SyncError::KeyUnavailable(key)) if key == KeyId(Uuid::from_u128(11)))
    );
    assert!(storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
    devices[1].log.sync_store_log().await.unwrap();
    devices[1].sync.upload_writes().await.unwrap();
    let path = ObjectPath::device_log(DeviceId(2), 1.try_into().unwrap());
    let bytes = storage.read(&path).await.unwrap();
    let length = coven_format::sealed_write::WriteObjectPrefix::length(&bytes).unwrap();
    assert_eq!(
        coven_format::sealed_write::WriteObjectPrefix::decode(&bytes[..length])
            .unwrap()
            .part_keys,
        [KeyId(Uuid::from_u128(11))]
    );
}

#[tokio::test]
async fn a_former_circle_member_still_waits_for_its_missing_earlier_key() {
    let storage = storage();
    let mut devices = household(storage).await;
    sql(&devices[0].db,"INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','old')").await;
    devices[0].sync.upload_writes().await.unwrap();
    devices[0]
        .log
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].log.sync_store_log().await.unwrap();
    devices[1]
        .keys
        .persist(&StoreKeyring::new(StoreKey::from_bytes(
            KeyId(Uuid::from_u128(1)),
            [7; 32],
        )))
        .unwrap();
    assert_eq!(
        devices[1]
            .sync
            .download_writes()
            .await
            .unwrap()
            .waiting
            .len(),
        1
    );
    devices[1].log.sync_store_log().await.unwrap();
    assert!(devices[1]
        .sync
        .download_writes()
        .await
        .unwrap()
        .waiting
        .is_empty());
    assert_eq!(count(&devices[1].db).await, 1);
}

#[tokio::test]
async fn posted_fingerprints_stop_when_a_member_leaves_the_circle() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    assert!(devices[1].sync.post_positions().await.unwrap());
    assert_eq!(posted(&storage, &devices[1], 2).await.fingerprints.len(), 2);
    devices[0]
        .log
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].log.sync_store_log().await.unwrap();
    assert!(devices[1].sync.post_positions().await.unwrap());
    let fingerprints = posted(&storage, &devices[1], 2).await.fingerprints;
    assert_eq!(fingerprints.len(), 1);
    assert_eq!(fingerprints[0].audience, Audience::Store);
}

#[tokio::test]
async fn dropped_removal_parts_wait_for_redistributed_keys_and_converge() {
    for store_removal in [false, true] {
        for carol_applies_before_drop in [false, true] {
            for ben_reads_before_copy in [false, true] {
                dropped_removal_parts(
                    store_removal,
                    carol_applies_before_drop,
                    ben_reads_before_copy,
                )
                .await;
            }
        }
    }
}

async fn dropped_removal_parts(
    store_removal: bool,
    carol_applies_before_drop: bool,
    ben_reads_before_copy: bool,
) {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let [ana, ben, carol] = devices.as_mut_slice() else {
        panic!("three members");
    };
    let circle = CircleId(Uuid::from_u128(10));
    ana.log
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(5).member_id(),
        })
        .await
        .unwrap();
    for device in [&mut *ben, &mut *carol] {
        device.log.sync_store_log().await.unwrap();
    }
    let removal = |member, number| {
        let key = KeyId(Uuid::from_u128(number));
        if store_removal {
            StoreChange::RemoveMember {
                member,
                key,
                circle_keys: vec![coven_format::store_log::CircleKeyId {
                    circle,
                    key: KeyId(Uuid::from_u128(number + 10)),
                }],
            }
        } else {
            StoreChange::RemoveCircleMember {
                circle,
                member,
                key,
            }
        }
    };
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let dropped = ana
        .log
        .make_and_upload_entry(removal(identity(4).member_id(), 2))
        .await
        .unwrap();
    sql(&ana.db, "INSERT INTO notes VALUES('one','public','body'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
    let writes = ana.sync.upload_writes().await.unwrap();
    assert_eq!(writes.len(), 1);
    carol.log.sync_store_log().await.unwrap();
    if carol_applies_before_drop {
        let report = carol.sync.download_writes().await.unwrap();
        assert!(report.waiting.is_empty(), "{report:?}");
        assert!(report.damaged_objects.is_empty(), "{report:?}");
        assert_eq!(count(&carol.db).await, 1);
    }

    // Ben authors against his unchanged view, earlier than Ana's removal.
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let winner = ben
        .log
        .make_and_upload_entry(removal(identity(3).member_id(), 3))
        .await
        .unwrap();
    let audience = if store_removal {
        Audience::Store
    } else {
        Audience::Circle(circle)
    };
    let dropped_key = KeyId(Uuid::from_u128(2));
    let copy = crate::store_log_keys::path(&audience, dropped_key, &identity(4).member_id());
    assert!(matches!(
        storage.read(&copy).await,
        Err(coven_storage::StorageError::NotFound)
    ));
    if ben_reads_before_copy {
        ben.log.sync_store_log().await.unwrap();
        assert!(matches!(
            ben.db.store_log().await.unwrap().replay.entries[&dropped],
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(id))
                if id == winner
        ));
        assert!(!crate::write_seal::holds(
            &ben.keys.unlock().unwrap().unwrap(),
            &audience,
            dropped_key,
        ));
        let report = ben.sync.download_writes().await.unwrap();
        assert!(report.damaged_objects.is_empty(), "{report:?}");
        assert_eq!(report.waiting.len(), 1, "{report:?}");
        assert_eq!(report.waiting[0].write, writes[0]);
        assert!(report.waiting[0].waiting_for.is_empty());
        let since = report.waiting[0].since;
        ben.clock.set(UNIX_EPOCH + Duration::from_secs(4));
        assert_eq!(
            ben.sync.download_writes().await.unwrap().waiting[0].since,
            since
        );
        assert!(rows(&ben.db).await.is_empty());
        assert_eq!(count(&ben.db).await, 0);
        assert!(matches!(
            ben.log.reload_from_snapshots().await,
            Err(SyncError::KeyUnavailable(_))
        ));
        assert!(ben.sync.post_positions().await.unwrap());
        assert!(!posted(&storage, ben, 2).await.writes.covers(writes[0]));
    }

    // Carol owns K2 and shares it through StoreLogSync when she learns the drop.
    carol.log.sync_store_log().await.unwrap();
    assert!(!storage.read(&copy).await.unwrap().is_empty());
    assert!(matches!(
        carol.db.store_log().await.unwrap().replay.entries[&dropped],
        coven_database::EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(id))
            if id == winner
    ));
    if carol_applies_before_drop {
        assert_eq!(rows(&carol.db).await, rows(&ana.db).await);
        assert_eq!(count(&carol.db).await, 1);
    }
    ben.log.sync_store_log().await.unwrap();
    assert!(crate::write_seal::holds(
        &ben.keys.unlock().unwrap().unwrap(),
        &audience,
        dropped_key,
    ));
    let expected = rows(&ana.db).await;
    for device in [&mut *ben, &mut *carol] {
        let report = device.sync.download_writes().await.unwrap();
        assert!(report.waiting.is_empty(), "{report:?}");
        assert!(report.damaged_objects.is_empty(), "{report:?}");
        assert_eq!(rows(&device.db).await, expected);
        assert_eq!(count(&device.db).await, 1);
        assert!(device.db.lost_values().await.unwrap().is_empty());
        device.log.reload_from_snapshots().await.unwrap();
        assert_eq!(rows(&device.db).await, expected);
        assert_eq!(count(&device.db).await, 1);
        assert!(device.sync.post_positions().await.unwrap());
    }
    let ben = posted(&storage, ben, 2).await;
    let carol = posted(&storage, carol, 3).await;
    assert_eq!(ben.writes, carol.writes);
    assert_eq!(ben.store_log, carol.store_log);
    assert_eq!(ben.schema_version, carol.schema_version);
    assert_eq!(ben.fingerprints.len(), 2);
    assert_eq!(ben.fingerprints, carol.fingerprints);
}

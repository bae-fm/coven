use super::*;
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::CircleId;

fn identity(seed: u8) -> MemberKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([seed; 64]);
    MemberKeys::from_secret_bytes(&bytes).unwrap()
}
async fn circle_device(storage: Arc<MemoryStorage>, number: u64, seed: u8) -> Device {
    let mut device = device(storage.clone(), number).await;
    device.identity.persist(&identity(seed)).unwrap();
    device.reopen(storage,
            vec![SyncedTable::new("notes", RowIdentity::SharedKey), SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience")],
            vec![schema::initial(), Migration::sql(2, "circles", "CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT NOT NULL);")]).await;
    device
}
pub(super) async fn household(storage: Arc<MemoryStorage>) -> Vec<Device> {
    let mut devices = Vec::new();
    for number in 1..=3 {
        devices.push(circle_device(storage.clone(), number, number as u8 + 2).await);
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
    for seed in [4, 5] {
        let identity = identity(seed);
        devices[0]
            .sync
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
        device.sync.sync_store_log().await.unwrap();
        device
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(i as u64 + 1),
                name: i.to_string(),
            })
            .await
            .unwrap();
    }
    devices[0].sync.sync_store_log().await.unwrap();
    let circle = CircleId(Uuid::from_u128(10));
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle,
            name: "private".into(),
            key: KeyId(Uuid::from_u128(10)),
        })
        .await
        .unwrap();
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(4).member_id(),
        })
        .await
        .unwrap();
    for device in &mut devices {
        device.sync.sync_store_log().await.unwrap();
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
    devices[0].writes.upload_writes().await.unwrap();
    devices[1]
        .custody
        .persist(&StoreKeyring::new(StoreKey::from_bytes(
            KeyId(Uuid::from_u128(1)),
            [7; 32],
        )))
        .unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert!(rows(&devices[1].db).await.is_empty());
    assert!(matches!(
        devices[1].sync.reload_from_snapshots().await,
        Err(SyncError::KeyUnavailable(_))
    ));
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[2].sync.reload_from_snapshots().await.unwrap();
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(count(&devices[1].db).await, 1);
}

#[tokio::test]
async fn a_circle_restored_after_deletion_keeps_a_previously_skipped_part_missing() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let mut tablet = circle_device(storage.clone(), 4, 3).await;
    let [ana, ben, carol] = devices.as_mut_slice() else {
        panic!("three members")
    };
    let circle = CircleId(Uuid::from_u128(10));
    ana.sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(4),
            name: "Ana tablet".into(),
        })
        .await
        .unwrap();
    ana.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(5).member_id(),
        })
        .await
        .unwrap();
    for device in [&mut *ben, &mut *carol, &mut tablet] {
        device.sync.sync_store_log().await.unwrap();
    }
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let removal = ben
        .sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle,
            member: identity(3).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    for device in [&mut *ana, &mut *carol] {
        device.sync.sync_store_log().await.unwrap();
    }
    carol.clock.set(UNIX_EPOCH + Duration::from_secs(4));
    sql(&carol.db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','Carol wrote this')").await;
    let writes = carol.writes.upload_writes().await.unwrap();
    assert_eq!(writes.len(), 1);
    ana.writes.download_writes().await.unwrap();
    assert_eq!(count(&ana.db).await, 0);
    assert!(ana
        .db
        .sync_state(Vec::new())
        .await
        .unwrap()
        .positions
        .covers(writes[0]));

    // Ben has not downloaded Carol's row. Deleting the empty local circle
    // therefore publishes an entry without an ordinary row-delete write.
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(5));
    let crate::operations::Begun::Operation(id) = ben
        .sync
        .begin_operation_call(crate::operations::Command::DeleteCircle(circle))
        .await
        .unwrap()
    else {
        panic!("circle deletion")
    };
    loop {
        let record = ben.operation(id).await;
        match ben
            .sync
            .operation_step(&record, crate::operation_data::Data::read(&record).unwrap())
            .await
            .unwrap()
        {
            crate::operations::Progress::Finished(_) => break,
            crate::operations::Progress::Advanced => (),
            _ => panic!("empty circle deletion must finish"),
        }
    }
    assert!(ben.db.test_queued_writes().await.unwrap().is_empty());
    ana.sync.sync_store_log().await.unwrap();
    assert!(ana.db.store_log().await.unwrap().replay.state.circles[&circle].deleted);

    // Ana's tablet still has the common prefix. Its earlier removal beats
    // both of Ben's entries; Carol can share the dropped circle key.
    tablet.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    tablet
        .sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle,
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(20)),
        })
        .await
        .unwrap();
    carol.sync.sync_store_log().await.unwrap();
    for device in [&mut *ana, &mut tablet] {
        device.sync.sync_store_log().await.unwrap();
        device.writes.download_writes().await.unwrap();
        let log = device.db.store_log().await.unwrap();
        assert!(!log.replay.state.circles[&circle].deleted);
        assert!(log.replay.state.circles[&circle]
            .members
            .contains(&identity(3).member_id()));
        assert!(matches!(
            log.replay.entries[&removal],
            coven_database::EntryOutcome::Dropped(_)
        ));
        assert!(crate::store_log_keys::holds(
            Some(&device.custody.read().unwrap().unwrap()),
            &Audience::Circle(circle),
            KeyId(Uuid::from_u128(11)),
        ));
        assert!(device
            .db
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions
            .covers(writes[0]));
    }
    assert_eq!(
        ana.db.store_log().await.unwrap().replay,
        tablet.db.store_log().await.unwrap().replay
    );
    assert_eq!(count(&ana.db).await, 0);
    assert_eq!(count(&tablet.db).await, 1);
    assert!(ana.db.lost_values().await.unwrap().is_empty());
}

#[tokio::test]
async fn new_uploads_wait_for_a_current_members_replacement_circle_key() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let ring = devices[1].custody.read().unwrap().unwrap();
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(3).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].custody.persist(&ring).unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    assert!(!log.replay.state.circles[&CircleId(Uuid::from_u128(10))].deleted);
    sql(&devices[1].db,"INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','new')").await;
    assert!(
        matches!(devices[1].writes.upload_writes().await, Err(SyncError::KeyUnavailable(key)) if key == KeyId(Uuid::from_u128(11)))
    );
    assert!(storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.upload_writes().await.unwrap();
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
    devices[0].writes.upload_writes().await.unwrap();
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1]
        .custody
        .persist(&StoreKeyring::new(StoreKey::from_bytes(
            KeyId(Uuid::from_u128(1)),
            [7; 32],
        )))
        .unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(count(&devices[1].db).await, 0);
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(count(&devices[1].db).await, 1);
}

#[tokio::test]
async fn posted_fingerprints_stop_when_a_member_leaves_the_circle() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    assert!(devices[1].writes.post_positions().await.unwrap());
    assert_eq!(posted(&storage, &devices[1], 2).await.fingerprints.len(), 2);
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(11)),
        })
        .await
        .unwrap();
    devices[1].sync.sync_store_log().await.unwrap();
    assert!(devices[1].writes.post_positions().await.unwrap());
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
    ana.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(5).member_id(),
        })
        .await
        .unwrap();
    for device in [&mut *ben, &mut *carol] {
        device.sync.sync_store_log().await.unwrap();
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
        .sync
        .make_and_upload_entry(removal(identity(4).member_id(), 2))
        .await
        .unwrap();
    sql(&ana.db, "INSERT INTO notes VALUES('one','public','body'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
    let writes = ana.writes.upload_writes().await.unwrap();
    assert_eq!(writes.len(), 1);
    carol.sync.sync_store_log().await.unwrap();
    if carol_applies_before_drop {
        carol.writes.download_writes().await.unwrap();
        assert_eq!(count(&carol.db).await, 1);
    }

    // Ben authors against his unchanged view, earlier than Ana's removal.
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    let winner = ben
        .sync
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
        Err(error) if error.failure() == coven_storage::StorageFailure::NotFound
    ));
    if ben_reads_before_copy {
        ben.sync.sync_store_log().await.unwrap();
        assert!(matches!(
            ben.db.store_log().await.unwrap().replay.entries[&dropped],
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::BeatenBy(id))
                if id == winner
        ));
        assert!(!crate::store_log_keys::holds(
            Some(&ben.custody.read().unwrap().unwrap()),
            &audience,
            dropped_key,
        ));
        ben.writes.download_writes().await.unwrap();
        ben.clock.set(UNIX_EPOCH + Duration::from_secs(4));
        ben.writes.download_writes().await.unwrap();
        assert!(rows(&ben.db).await.is_empty());
        assert_eq!(count(&ben.db).await, 0);
        assert!(matches!(
            ben.sync.reload_from_snapshots().await,
            Err(SyncError::KeyUnavailable(_))
        ));
        assert!(ben.writes.post_positions().await.unwrap());
        assert!(!posted(&storage, ben, 2).await.writes.covers(writes[0]));
    }

    // Carol owns K2 and shares it through StoreLogSync when she learns the drop.
    carol.sync.sync_store_log().await.unwrap();
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
    ben.sync.sync_store_log().await.unwrap();
    assert!(crate::store_log_keys::holds(
        Some(&ben.custody.read().unwrap().unwrap()),
        &audience,
        dropped_key,
    ));
    let expected = rows(&ana.db).await;
    for device in [&mut *ben, &mut *carol] {
        device.writes.download_writes().await.unwrap();
        assert_eq!(rows(&device.db).await, expected);
        assert_eq!(count(&device.db).await, 1);
        assert!(device.db.lost_values().await.unwrap().is_empty());
        device.sync.reload_from_snapshots().await.unwrap();
        assert_eq!(rows(&device.db).await, expected);
        assert_eq!(count(&device.db).await, 1);
        assert!(device.writes.post_positions().await.unwrap());
    }
    let ben = posted(&storage, ben, 2).await;
    let carol = posted(&storage, carol, 3).await;
    assert_eq!(ben.writes, carol.writes);
    assert_eq!(ben.store_log, carol.store_log);
    assert_eq!(ben.schema_version, carol.schema_version);
    assert_eq!(ben.fingerprints.len(), 2);
    assert_eq!(ben.fingerprints, carol.fingerprints);
}

#[tokio::test]
async fn retention_waits_for_key_copies_without_failing() {
    let mut failed = Vec::new();
    for reader in [0, 1] {
        let storage = storage();
        let mut devices = household(storage.clone()).await;
        sql(&devices[0].db, "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
        devices[0].writes.upload_writes().await.unwrap();
        let keys = devices[reader].custody.read().unwrap().unwrap();
        devices[reader]
            .custody
            .persist(&StoreKeyring::new(StoreKey::from_bytes(
                KeyId(Uuid::from_u128(1)),
                [7; 32],
            )))
            .unwrap();
        let unused = ObjectPath::file(
            DeviceId(reader as u64 + 1),
            coven_foundation::id_source::FileId(Uuid::from_u128(99)),
        );
        storage.create(&unused, b"unused").await.unwrap();
        match devices[reader].sync.run_retention().await {
            Ok(()) => (),
            Err(error) => failed.push((reader, error)),
        }
        assert_eq!(storage.read(&unused).await.unwrap(), b"unused");
        devices[reader].custody.persist(&keys).unwrap();
        devices[reader].sync.run_retention().await.unwrap();
        assert!(matches!(
            storage.read(&unused).await,
            Err(error) if error.failure() == coven_storage::StorageFailure::NotFound
        ));
    }
    assert!(
        failed.is_empty(),
        "waiting keys failed retention: {failed:?}"
    );
}

#[tokio::test]
async fn retries_keep_each_parts_keys_across_store_and_circle_rotations() {
    let storage = storage();
    let mut devices = household(storage.clone()).await;
    let [ana, ben, _carol] = devices.as_mut_slice() else {
        panic!("three members")
    };
    ana.db.write(|sql| {
        sql.execute("INSERT INTO notes VALUES('one','public',?1)", ["body".repeat(50_000)])?;
        sql.execute("INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a',?1)", ["private".repeat(20_000)])?;
        Ok(())
    }).await.unwrap();
    storage
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(ana.writes.upload_writes().await.is_err());
    let (write, first) = writes::resealed(ana).await;
    sql(
        &ana.db,
        "UPDATE notes SET body='later'; UPDATE pins SET title='later'",
    )
    .await;
    ben.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: identity(5).member_id(),
            key: KeyId(Uuid::from_u128(20)),
            circle_keys: vec![],
        })
        .await
        .unwrap();
    ben.sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: CircleId(Uuid::from_u128(10)),
            member: identity(4).member_id(),
            key: KeyId(Uuid::from_u128(21)),
        })
        .await
        .unwrap();
    ana.sync.sync_store_log().await.unwrap();
    assert_eq!(writes::resealed(ana).await.1, first);
    assert_eq!(ana.writes.upload_writes().await.unwrap().len(), 2);
    assert_eq!(
        storage.read(&crate::write_seal::path(write)).await.unwrap(),
        first
    );
    let prefix = |bytes: &[u8]| {
        use coven_format::sealed_write::WriteObjectPrefix;
        WriteObjectPrefix::decode(&bytes[..WriteObjectPrefix::length(bytes).unwrap()]).unwrap()
    };
    assert_eq!(prefix(&first).store_key, KeyId(Uuid::from_u128(1)));
    assert_eq!(
        prefix(&first).part_keys,
        [KeyId(Uuid::from_u128(1)), KeyId(Uuid::from_u128(10))]
    );
    let second = storage
        .read(&ObjectPath::device_log(write.device, 2.try_into().unwrap()))
        .await
        .unwrap();
    assert_eq!(prefix(&second).store_key, KeyId(Uuid::from_u128(20)));
    assert_eq!(
        prefix(&second).part_keys,
        [KeyId(Uuid::from_u128(20)), KeyId(Uuid::from_u128(21))]
    );
}

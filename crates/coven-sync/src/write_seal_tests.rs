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
    let key_hash = devices[0]
        .sync
        .test_key(coven_merge::Audience::Store, KeyId(Uuid::from_u128(1)));
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
            key_hash,
            device_name: "one".into(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner-key".into(),
                principal: coven_format::S3Principal::Console,
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
                    principal: coven_format::S3Principal::Console,
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
                replaces: None,
            })
            .await
            .unwrap();
    }
    devices[0].sync.sync_store_log().await.unwrap();
    let circle = CircleId(Uuid::from_u128(10));
    let key_hash = devices[0].sync.test_key(
        coven_merge::Audience::Circle(circle),
        KeyId(Uuid::from_u128(10)),
    );
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle,
            name: "private".into(),
            key: KeyId(Uuid::from_u128(10)),
            key_hash,
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
async fn write_sealing_binds_each_chunks_plaintext_and_the_complete_prefix() {
    use coven_format::sealed_write::WriteObjectPrefix;

    async fn seal(device: &Device, header_key: u128, changed_header: bool) -> Vec<Vec<u8>> {
        let mut ring = StoreKeyring::new(StoreKey::from_bytes(KeyId(Uuid::from_u128(1)), [7; 32]));
        ring.insert_store_key(StoreKey::from_bytes(KeyId(Uuid::from_u128(2)), [7; 32]))
            .unwrap();
        device
            .db
            .read_oldest_upload(move |mut upload| {
                upload.keys = Some(WriteObjectPrefix {
                    format: coven_format::FormatVersion::CURRENT,
                    store_key: KeyId(Uuid::from_u128(header_key)),
                    part_keys: vec![KeyId(Uuid::from_u128(1)); upload.header.parts.len()],
                });
                if changed_header {
                    upload.header.header.timestamp =
                        coven_merge::Timestamp::new(2000, 0, DeviceId(1)).unwrap();
                    upload.header_frame = upload.header.encode().unwrap();
                }
                let mut pieces = Vec::new();
                crate::write_seal::seal(STORE_ID, upload, &ring, &member(), &mut |piece| {
                    pieces.push(piece.to_vec());
                    Ok(())
                })?;
                Ok::<_, SyncError>(pieces)
            })
            .await
            .unwrap()
            .unwrap()
    }

    let mut sealed = Vec::new();
    for first_byte in [b'A', b'B'] {
        // Two copies use the same device, write number, clock and encryption key.
        let device = device(storage(), 1).await;
        device
            .db
            .write(move |sql| {
                let mut body = vec![b'A'; 3 * coven_format::chunks::CHUNK_SIZE];
                body[0] = first_byte;
                sql.execute(
                    "INSERT INTO notes VALUES('one','title',?1)",
                    [String::from_utf8(body).unwrap()],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let first = seal(&device, 1, false).await;
        assert_eq!(first, seal(&device, 1, false).await);
        assert!(
            first.len() > 4,
            "prefix, header, multiple part chunks, signature"
        );
        let header = seal(&device, 1, true).await;
        assert_eq!(first[0], header[0]);
        assert_ne!(first[1][4..28], header[1][4..28]);
        assert_eq!(first[2..first.len() - 1], header[2..header.len() - 1]);
        let prefix = seal(&device, 2, false).await;
        assert_ne!(first[0], prefix[0]);
        for (a, b) in first[1..first.len() - 1]
            .iter()
            .zip(&prefix[1..prefix.len() - 1])
        {
            assert_ne!(
                a[4..28],
                b[4..28],
                "changed prefix must separate every chunk"
            );
        }
        sealed.push(first);
    }
    assert_eq!(sealed[0][..2], sealed[1][..2], "same prefix and header");
    assert_ne!(
        sealed[0][2][4..28],
        sealed[1][2][4..28],
        "changed part plaintext"
    );
    assert_eq!(
        sealed[0][3..sealed[0].len() - 1],
        sealed[1][3..sealed[1].len() - 1]
    );
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
    // A reload without a snapshot has no target past local positions; the
    // key wait stays with discovery.
    devices[1].sync.reload_from_snapshots().await.unwrap();
    assert!(rows(&devices[1].db).await.is_empty());
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[2].sync.reload_from_snapshots().await.unwrap();
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await.len(), 1);
    assert_eq!(count(&devices[2].db).await, 0);
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(count(&devices[1].db).await, 1);
}

#[tokio::test]
async fn a_circle_restored_and_rejoined_after_deletion_reloads_previously_skipped_parts() {
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
            replaces: None,
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
        })
        .await
        .unwrap();
    ben.sync
        .test_rotate(Audience::Circle(circle), KeyId(Uuid::from_u128(11)))
        .await;
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

    // Ben has not downloaded Carol's row. His deletion hides it on other
    // devices without authoring any row changes.
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
    assert!(ana
        .db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));

    // Ana's tablet still has the common prefix. Its earlier store removal
    // defeats Ben's deletion. The two membership removals both survive.
    tablet.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    tablet
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: identity(4).member_id(),
        })
        .await
        .unwrap();
    carol.sync.sync_store_log().await.unwrap();
    // Carol explicitly readmits Ana and shares the rotation's key.
    carol
        .sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle,
            member: identity(3).member_id(),
        })
        .await
        .unwrap();
    for device in [&mut *ana, &mut tablet] {
        device.sync.sync_store_log().await.unwrap();
        device.writes.download_writes().await.unwrap();
        let log = device.db.store_log().await.unwrap();
        assert!(!log.replay.state.circle_deletions.contains_key(&circle));
        assert!(log.replay.state.circles[&circle]
            .members
            .contains(&identity(3).member_id()));
        assert_eq!(
            log.replay.entries[&removal],
            coven_database::EntryOutcome::Kept
        );
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
    assert_eq!(count(&ana.db).await, 1);
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
        })
        .await
        .unwrap();
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1]
        .sync
        .test_rotate(
            Audience::Circle(CircleId(Uuid::from_u128(10))),
            KeyId(Uuid::from_u128(11)),
        )
        .await;
    devices[1].custody.persist(&ring).unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    assert!(!log
        .replay
        .state
        .circle_deletions
        .contains_key(&CircleId(Uuid::from_u128(10))));
    sql(&devices[1].db,"INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','new')").await;
    assert!(
        matches!(devices[1].writes.upload_writes().await, Err(SyncError::KeyUnavailable(key)) if key == KeyId(Uuid::from_u128(11)))
    );
    assert!(storage
        .list(&ObjectPrefix::device_logs(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.upload_writes().await.unwrap();
    let path = ObjectPath::device_log(STORE_ID, DeviceId(2), 1.try_into().unwrap());
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
async fn reversed_removal_parts_wait_for_redistributed_keys_and_converge() {
    for store_rotation in [false, true] {
        for carol_applies_before_drop in [false, true] {
            for ben_reads_before_copy in [false, true] {
                reversed_removal_parts(
                    store_rotation,
                    carol_applies_before_drop,
                    ben_reads_before_copy,
                )
                .await;
            }
        }
    }
}

async fn reversed_removal_parts(
    store_rotation: bool,
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
    // With two admins, the later mutual removal would leave none and drops.
    ana.sync
        .make_and_upload_entry(StoreChange::ChangeRole {
            member: identity(5).member_id(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    for device in [&mut *ben, &mut *carol] {
        device.sync.sync_store_log().await.unwrap();
    }
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let dropped = ana
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: identity(4).member_id(),
        })
        .await
        .unwrap();
    let audience = if store_rotation {
        Audience::Store
    } else {
        Audience::Circle(circle)
    };
    ana.sync
        .test_rotate(audience.clone(), KeyId(Uuid::from_u128(2)))
        .await;
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
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: identity(3).member_id(),
        })
        .await
        .unwrap();
    let rotation_key = KeyId(Uuid::from_u128(2));
    let has_copy = |copies: Vec<crate::store_log_keys::ObservedCopy>| {
        copies.into_iter().any(|copy| {
            copy.prefix.audience == audience
                && copy.prefix.key == rotation_key
                && copy.prefix.member == identity(4).member_id()
        })
    };
    assert!(!has_copy(
        crate::store_log_keys::read_copies(
            &ana.db,
            &crate::pass_reads::PassReads::default(),
            std::num::NonZeroUsize::MIN,
            storage.as_ref(),
            STORE_ID,
            &mut Vec::new()
        )
        .await
        .unwrap()
    ));
    if ben_reads_before_copy {
        ben.sync.sync_store_log().await.unwrap();
        assert!(matches!(
            ben.db.store_log().await.unwrap().replay.entries[&dropped],
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::NoAdminLeft)
        ));
        assert!(!crate::store_log_keys::holds(
            Some(&ben.custody.read().unwrap().unwrap()),
            &audience,
            rotation_key,
        ));
        for time in [2, 4] {
            ben.clock.set(UNIX_EPOCH + Duration::from_secs(time));
            let download = ben.writes.download_writes().await;
            if store_rotation {
                assert!(
                    matches!(download, Err(SyncError::KeyUnavailable(key)) if key == rotation_key)
                );
            } else {
                download.unwrap();
            }
        }
        assert!(rows(&ben.db).await.is_empty());
        assert_eq!(count(&ben.db).await, 0);
        // Without a snapshot the reload has no target past local positions;
        // the key wait stays with discovery.
        ben.sync.reload_from_snapshots().await.unwrap();
        assert!(rows(&ben.db).await.is_empty());
        assert!(!ben
            .db
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions
            .covers(writes[0]));
        let post = ben.writes.post_positions().await;
        if store_rotation {
            assert!(matches!(post, Err(SyncError::KeyUnavailable(key)) if key == rotation_key));
            assert!(matches!(
                storage.read(&ObjectPath::positions(STORE_ID, DeviceId(2))).await,
                Err(error) if error.failure() == coven_storage::StorageFailure::NotFound
            ));
        } else {
            assert!(post.unwrap());
            assert!(!posted(&storage, ben, 2).await.writes.covers(writes[0]));
        }
    }

    // Carol owns K2 and shares it through StoreLogSync when she learns the drop.
    carol.sync.sync_store_log().await.unwrap();
    assert!(has_copy(
        crate::store_log_keys::read_copies(
            &ana.db,
            &crate::pass_reads::PassReads::default(),
            std::num::NonZeroUsize::MIN,
            storage.as_ref(),
            STORE_ID,
            &mut Vec::new()
        )
        .await
        .unwrap()
    ));
    assert!(matches!(
        carol.db.store_log().await.unwrap().replay.entries[&dropped],
        coven_database::EntryOutcome::Dropped(coven_database::DropReason::NoAdminLeft)
    ));
    assert_eq!(
        carol.db.store_log().await.unwrap().replay.entries[&winner],
        coven_database::EntryOutcome::Kept
    );
    if carol_applies_before_drop {
        assert_eq!(rows(&carol.db).await, rows(&ana.db).await);
        assert_eq!(count(&carol.db).await, 1);
    }
    ben.sync.sync_store_log().await.unwrap();
    assert!(crate::store_log_keys::holds(
        Some(&ben.custody.read().unwrap().unwrap()),
        &audience,
        rotation_key,
    ));
    let expected = rows(&ana.db).await;
    for device in [&mut *ben, &mut *carol] {
        device.writes.download_writes().await.unwrap();
        assert_eq!(rows(&device.db).await, expected);
        assert_eq!(count(&device.db).await, 1);
        assert!(device.db.lost_values().await.unwrap().is_empty());
        device.sync.reload_from_snapshots().await.unwrap();
        device.writes.download_writes().await.unwrap();
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
            STORE_ID,
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
        .test_rotate(Audience::Store, KeyId(Uuid::from_u128(20)))
        .await;
    ben.sync
        .test_rotate(
            Audience::Circle(CircleId(Uuid::from_u128(10))),
            KeyId(Uuid::from_u128(21)),
        )
        .await;
    ana.sync.sync_store_log().await.unwrap();
    assert_eq!(writes::resealed(ana).await.1, first);
    assert_eq!(ana.writes.upload_writes().await.unwrap().len(), 2);
    assert_eq!(
        storage
            .read(&crate::write_seal::path(STORE_ID, write))
            .await
            .unwrap(),
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
        .read(&ObjectPath::device_log(
            STORE_ID,
            write.device,
            2.try_into().unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(prefix(&second).store_key, KeyId(Uuid::from_u128(20)));
    assert_eq!(
        prefix(&second).part_keys,
        [KeyId(Uuid::from_u128(20)), KeyId(Uuid::from_u128(21))]
    );
}

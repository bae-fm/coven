use super::writes::{publish, queued, seal};
use super::*;
use coven_format::value::EntryPositions;
use coven_merge::Timestamp;
use std::error::Error;

#[tokio::test]
async fn damaged_objects_roll_back_and_block_only_their_device() {
    for failure in ["decryption", "signature", "moved", "parse"] {
        let storage = storage();
        let mut devices = group(storage.clone(), 3).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('bad','first','body')",
        )
        .await;
        let record = queued(&devices[0].db).await;
        devices[0].writes.upload_writes().await.unwrap();
        sql(&devices[0].db, "UPDATE notes SET title='later'").await;
        devices[0].writes.upload_writes().await.unwrap();
        sql(
            &devices[1].db,
            "INSERT INTO notes VALUES('independent','good','body')",
        )
        .await;
        devices[1].writes.upload_writes().await.unwrap();
        let path = crate::write_seal::path(record.header.position);
        let mut bytes = storage.read(&path).await.unwrap();
        match failure {
            "decryption" => bytes[60] ^= 1,
            "signature" => {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            "moved" => {
                bytes = seal(
                    &record,
                    &ObjectPath::device_log(DeviceId(99), 1.try_into().unwrap()),
                    |_, _| {},
                )
            }
            "parse" => {
                bytes = seal(&record, &path, |section, plain| {
                    if section > 0 {
                        plain[0] = 255;
                    }
                })
            }
            _ => unreachable!(),
        }
        storage.delete(&path).await.unwrap();
        storage.create(&path, &bytes).await.unwrap();
        let object = storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .into_iter()
            .find(|object| object.path == path)
            .unwrap();
        let log = devices[2].db.store_log().await.unwrap();
        let ring = devices[2].custody.read().unwrap().unwrap();
        let mut replays = crate::replay_cache::ReplayCache::new(&log);
        let error = devices[2]
            .writes
            .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
            .await
            .unwrap_err();
        let SyncError::Damaged(damage) = error else {
            panic!("{error:?}")
        };
        let cause = damage.failure.source().unwrap();
        match failure {
            "decryption" | "moved" => assert!(matches!(
                cause.downcast_ref::<coven_crypto::CryptoError>(),
                Some(coven_crypto::CryptoError::Authentication)
            )),
            "signature" => assert!(matches!(
                cause.downcast_ref::<coven_crypto::CryptoError>(),
                Some(coven_crypto::CryptoError::Signature)
            )),
            "parse" => assert!(cause.is::<coven_format::Error>()),
            _ => unreachable!(),
        }
        devices[2].writes.download_writes().await.unwrap();
        assert_eq!(
            rows(&devices[2].db).await,
            vec![("independent".into(), "good".into(), "body".into())]
        );
        let before = storage.reads().await;
        devices[2].writes.download_writes().await.unwrap();
        assert_eq!(storage.reads().await, before);
        let stuck = devices[2].db.stuck_logs().await.unwrap();
        assert_eq!(stuck.len(), 1);
        assert_eq!(
            damage.failure,
            crate::Refusal::from(stuck[0].record.failure)
        );
        assert_eq!(
            stuck[0].record.object,
            LogObject::Write(record.header.position)
        );
        use coven_format::stuck::StuckFailure;
        assert_eq!(
            stuck[0].record.failure,
            match failure {
                "decryption" | "moved" => StuckFailure::Decryption,
                "signature" => StuckFailure::Signature,
                "parse" => StuckFailure::Parse,
                _ => unreachable!(),
            }
        );
    }
}

#[tokio::test]
async fn removal_checks_the_authors_past_not_the_receivers_present() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','before','body')",
    )
    .await;
    let mut after = queued(&devices[0].db).await;
    devices[0].writes.upload_writes().await.unwrap();
    devices[1]
        .sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: DeviceId(1),
        })
        .await
        .unwrap();
    devices[2].sync.sync_store_log().await.unwrap();
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await[0].1, "before");
    devices[2].sync.reload_from_snapshots().await.unwrap();
    assert_eq!(rows(&devices[2].db).await[0].1, "before");
    let log = devices[2].db.local_store_log().await.unwrap();
    after.header.position.number = 2;
    after.header.store_log_read = EntryPositions(
        log.log
            .entries
            .iter()
            .fold(BTreeMap::new(), |mut map, e| {
                map.insert(e.entry.position.device, e.entry.position);
                map
            })
            .into_values()
            .collect(),
    );
    after.header.timestamp = Timestamp::new(2_000, 0, DeviceId(1)).unwrap();
    publish(&storage, &after).await;
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await[0].1, "before");
    let before = storage
        .reads()
        .await
        .into_iter()
        .filter(|(path, _, _)| path.write_id() == Some(after.header.position))
        .count();
    devices[2].sync.reload_from_snapshots().await.unwrap();
    assert_eq!(
        storage
            .reads()
            .await
            .into_iter()
            .filter(|(path, _, _)| path.write_id() == Some(after.header.position))
            .count(),
        before
    );
    assert_eq!(rows(&devices[2].db).await[0].1, "before");
}

#[tokio::test]
async fn newer_write_waits_until_the_app_schema_updates() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','new','body')",
    )
    .await;
    let mut record = queued(&devices[0].db).await;
    record.header.schema_version = 2;
    publish(&storage, &record).await;
    devices[1].writes.download_writes().await.unwrap();
    assert!(rows(&devices[1].db).await.is_empty());
    let device = &mut devices[1];
    device
        .reopen(
            storage,
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                schema::initial(),
                Migration::sql(2, "extra", "CREATE TABLE extra(value TEXT);"),
            ],
        )
        .await;
    device.writes.download_writes().await.unwrap();
    assert_eq!(rows(&device.db).await[0].1, "new");
}

#[tokio::test]
async fn newer_store_stops_uploads_and_waits_for_newer_downloads() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','new','body')",
    )
    .await;
    let mut record = queued(&devices[0].db).await;
    record.header.schema_version = 2;
    publish(&storage, &record).await;
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::RaiseSchema {
            version: 2,
            snapshot: coven_format::store_log::SnapshotId {
                audience: Audience::Store,
                device: DeviceId(1),
                number: 1,
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        devices[1].sync.sync_store_log().await,
        Err(SyncFailure::UpdateRequired)
    ));
    sql(
        &devices[1].db,
        "INSERT INTO notes VALUES('local','old','body')",
    )
    .await;
    assert!(matches!(
        devices[1].writes.upload_writes().await,
        Err(SyncError::Stopped(SyncFailure::UpdateRequired))
    ));
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(
        rows(&devices[1].db).await,
        vec![("local".into(), "old".into(), "body".into())]
    );
}

#[tokio::test]
async fn only_a_newer_format_requires_an_update() {
    for version in [0, 2] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        let path = ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap());
        let original = storage.read(&path).await.unwrap();
        let mut bytes = original.clone();
        bytes[2] = version;
        storage.delete(&path).await.unwrap();
        storage.create(&path, &bytes).await.unwrap();
        sql(
            &devices[1].db,
            "INSERT INTO notes VALUES('local','waiting','body')",
        )
        .await;
        let waiting = queued(&devices[1].db).await;
        let before = devices[1]
            .db
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions;
        let result = devices[1].writes.download_writes().await;
        if version == 2 {
            assert!(matches!(
                result,
                Err(SyncError::Stopped(SyncFailure::UpdateRequired))
            ));
        } else {
            result.unwrap();
        }
        assert_eq!(
            devices[1]
                .db
                .sync_state(Vec::new())
                .await
                .unwrap()
                .positions,
            before
        );
        assert_eq!(queued(&devices[1].db).await, waiting);
        assert_eq!(rows(&devices[1].db).await.len(), 1);
        assert!(devices[1].db.operations().await.unwrap().is_empty());
        assert!(storage
            .list(&ObjectPrefix::snapshots())
            .await
            .unwrap()
            .is_empty());
        assert_eq!(storage.read(&path).await.unwrap(), bytes);
        storage.delete(&path).await.unwrap();
        storage.create(&path, &original).await.unwrap();
        devices[1].writes.download_writes().await.unwrap();
        assert_eq!(
            rows(&devices[1].db).await.len(),
            if version == 2 { 2 } else { 1 }
        );
        assert_eq!(queued(&devices[1].db).await, waiting);
    }
}

#[tokio::test]
async fn a_newer_schema_does_not_hide_a_damaged_signature() {
    let storage = storage();
    let devices = group(storage.clone(), 2).await;
    devices[0]
        .db
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('one','title',?1)",
                ["x".repeat(200_000)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let mut record = queued(&devices[0].db).await;
    record.header.schema_version = 2;
    let path = crate::write_seal::path(record.header.position);
    let mut bytes = seal(&record, &path, |_, _| {});
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    storage.create(&path, &bytes).await.unwrap();
    let object = storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .remove(0);
    let log = devices[1].db.store_log().await.unwrap();
    let ring = devices[1].custody.read().unwrap().unwrap();
    let mut replays = crate::replay_cache::ReplayCache::new(&log);
    assert!(matches!(
        devices[1]
            .writes
            .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
            .await,
        Err(SyncError::Damaged(crate::DamagedObject {
            failure: crate::Refusal::Signature { cause: Some(_) },
            ..
        }))
    ));

    assert!(rows(&devices[1].db).await.is_empty());
}

#[tokio::test]
async fn a_cached_read_view_still_checks_each_writes_timestamp() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','before','body')",
    )
    .await;
    let first = queued(&devices[0].db).await;
    devices[0].writes.upload_writes().await.unwrap();
    sql(&devices[0].db, "UPDATE notes SET title='after'").await;
    let mut second = queued(&devices[0].db).await;
    assert_eq!(first.header.store_log_read, second.header.store_log_read);
    second.header.timestamp = Timestamp::new(0, 0, DeviceId(1)).unwrap();
    publish(&storage, &second).await;
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[1].db).await[0].1, "before");
    devices[1].sync.reload_from_snapshots().await.unwrap();
    assert_eq!(
        devices[1].db.stuck_logs().await.unwrap()[0].record.object,
        LogObject::Write(second.header.position)
    );
    assert_eq!(rows(&devices[1].db).await[0].1, "before");
}

#[tokio::test]
async fn missing_read_view_waits_then_replays_after_the_store_log_arrives() {
    for reload in [false, true] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        devices[0]
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(3),
                name: "third".into(),
            })
            .await
            .unwrap();
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','before','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        sql(&devices[0].db, "UPDATE notes SET title='after'").await;
        devices[0].writes.upload_writes().await.unwrap();
        let object = storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .remove(0);
        let log = devices[1].db.store_log().await.unwrap();
        let ring = devices[1].custody.read().unwrap().unwrap();
        let mut replays = crate::replay_cache::ReplayCache::new(&log);
        let result = devices[1]
            .writes
            .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
            .await
            .unwrap();
        assert!(
            matches!(result, ApplyOutcome::Waiting(coven_database::WriteWait::StoreLog(ref missing))
            if missing.len() == 1 && missing[0].device == DeviceId(1) && missing[0].number == 3)
        );
        devices[1].writes.download_writes().await.unwrap();
        assert!(rows(&devices[1].db).await.is_empty());
        if reload {
            assert!(matches!(
                devices[1].sync.reload_from_snapshots().await,
                Err(SyncError::Database(coven_database::DbError::Snapshot(
                    coven_database::SnapshotError::WriteWaiting(
                        coven_database::WriteWait::StoreLog(_)
                    )
                )))
            ));
            assert!(rows(&devices[1].db).await.is_empty());
        }
        devices[1].sync.sync_store_log().await.unwrap();
        if reload {
            devices[1].sync.reload_from_snapshots().await.unwrap();
            assert_eq!(rows(&devices[1].db).await[0].1, "after");
        }
        devices[1].writes.download_writes().await.unwrap();
        assert_eq!(rows(&devices[1].db).await[0].1, "after");
    }
}

#[tokio::test]
async fn retention_waits_for_store_log_entries() {
    for removed in [false, true] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        if removed {
            devices[1]
                .sync
                .make_and_upload_entry(StoreChange::RemoveDevice {
                    device: DeviceId(1),
                })
                .await
                .unwrap();
        }
        devices[0]
            .sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(3),
                name: "third".into(),
            })
            .await
            .unwrap();
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        devices[0].writes.upload_writes().await.unwrap();
        let unused = ObjectPath::file(
            DeviceId(2),
            coven_foundation::id_source::FileId(Uuid::from_u128(99)),
        );
        storage.create(&unused, b"unused").await.unwrap();
        devices[1].sync.run_retention().await.unwrap();
        assert_eq!(storage.read(&unused).await.unwrap(), b"unused");
        devices[1].sync.sync_store_log().await.unwrap();
        devices[1].sync.run_retention().await.unwrap();
        assert!(matches!(
            storage.read(&unused).await,
            Err(error) if error.failure() == coven_storage::StorageFailure::NotFound
        ));
    }
}

#[tokio::test]
async fn authorization_causality_and_identity_refusals_keep_their_report_tags() {
    for (defect, tag) in [("authorization", 4), ("causality", 5), ("identity", 6)] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        let mut record = queued(&devices[0].db).await;
        let path = crate::write_seal::path(record.header.position);
        match defect {
            "authorization" => record.header.store_log_read.0.clear(),
            "causality" => record.header.timestamp = Timestamp::new(0, 0, DeviceId(1)).unwrap(),
            "identity" => record.header.position.number = 2,
            _ => unreachable!(),
        }
        storage
            .create(&path, &seal(&record, &path, |_, _| {}))
            .await
            .unwrap();
        let object = storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .remove(0);
        let log = devices[1].db.store_log().await.unwrap();
        let ring = devices[1].custody.read().unwrap().unwrap();
        let mut replays = crate::replay_cache::ReplayCache::new(&log);
        let error = devices[1]
            .writes
            .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
            .await
            .unwrap_err();
        let SyncError::Damaged(damage) = error else {
            panic!("{error:?}")
        };
        assert_eq!(
            u8::from(coven_format::stuck::StuckFailure::from(&damage.failure)),
            tag,
            "{defect}"
        );
        devices[1].writes.download_writes().await.unwrap();
        assert!(rows(&devices[1].db).await.is_empty());
        let saved = devices[1].db.stuck_logs().await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            damage.failure,
            crate::Refusal::from(saved[0].record.failure)
        );
        assert_eq!(u8::from(saved[0].record.failure), tag, "{defect}");
        devices[1].writes.post_positions().await.unwrap();
        let post = posted(&storage, &devices[1], 2).await;
        assert_eq!(post.stuck[0], saved[0].record);
    }
}

#[tokio::test]
async fn a_write_with_incomplete_causal_history_is_refused_as_causality() {
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
    devices[2].writes.download_writes().await.unwrap();
    sql(&devices[1].db, "UPDATE notes SET title='third'").await;
    let mut record = queued(&devices[1].db).await;
    assert!(!record.header.had_read.0.is_empty());
    record.header.had_read.0.clear();
    publish(&storage, &record).await;
    devices[2].writes.download_writes().await.unwrap();
    let refused = devices[2].db.stuck_logs().await.unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(u8::from(refused[0].record.failure), 5);
    assert_eq!(rows(&devices[2].db).await[0].1, "second");
}

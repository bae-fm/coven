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
        let path = crate::write_seal::path(STORE_ID, record.header.position);
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
                    &ObjectPath::device_log(STORE_ID, DeviceId(99), 1.try_into().unwrap()),
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
            .list(&ObjectPrefix::device_logs(STORE_ID))
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
        let stuck = devices[2]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap();
        assert_eq!(stuck.len(), 1);
        assert_eq!(
            damage.failure,
            crate::Refusal::from(stuck[0].as_log_refusal().unwrap().failure)
        );
        assert_eq!(
            stuck[0].as_log_refusal().unwrap().object,
            LogObject::Write(record.header.position)
        );
        use coven_format::pending::RefusalCode;
        assert_eq!(
            stuck[0].as_log_refusal().unwrap().failure,
            match failure {
                "decryption" | "moved" => RefusalCode::Decryption,
                "signature" => RefusalCode::Signature,
                "parse" => RefusalCode::Parse,
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
    // A reload stops at its target; discovery reads the rest again.
    devices[2].writes.download_writes().await.unwrap();
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
    devices[2].writes.download_writes().await.unwrap();
    // The refused write is never read again, by the reload or by discovery.
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
    let path = crate::write_seal::path(STORE_ID, record.header.position);
    let reads = storage
        .reads()
        .await
        .into_iter()
        .filter(|(read, _, _)| read == &path)
        .count();
    for _ in 0..2 {
        devices[1].writes.download_writes().await.unwrap();
    }
    assert_eq!(
        storage
            .reads()
            .await
            .into_iter()
            .filter(|(read, _, _)| read == &path)
            .count(),
        reads,
        "a schema wait must reuse retained encrypted input"
    );
    let device = &mut devices[1];
    device
        .reopen(
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                schema::initial(),
                Migration::sql(2, "extra", "CREATE TABLE extra(value TEXT);"),
            ],
        )
        .await;
    device.writes.download_writes().await.unwrap();
    assert_eq!(rows(&device.db).await[0].1, "new");
    assert_eq!(
        storage
            .reads()
            .await
            .into_iter()
            .filter(|(read, _, _)| read == &path)
            .count(),
        reads,
        "schema arrival after reopen must open the retained input"
    );
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
        Err(SyncError::UpdateRequired(_))
    ));
    sql(
        &devices[1].db,
        "INSERT INTO notes VALUES('local','old','body')",
    )
    .await;
    assert!(matches!(
        devices[1].writes.upload_writes().await,
        Err(SyncError::UpdateRequired(_))
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
        let path = ObjectPath::device_log(STORE_ID, DeviceId(1), 1.try_into().unwrap());
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
            assert!(matches!(result, Err(SyncError::UpdateRequired(_))));
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
            .list(&ObjectPrefix::snapshots(STORE_ID))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(storage.read(&path).await.unwrap(), bytes);
        let reads = storage.reads().await;
        let repeated = devices[1].writes.download_writes().await;
        if version == 2 {
            assert!(matches!(repeated, Err(SyncError::UpdateRequired(_))));
        } else {
            repeated.unwrap();
        }
        assert_eq!(storage.reads().await, reads);
        assert_eq!(rows(&devices[1].db).await.len(), 1);
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
    let path = crate::write_seal::path(STORE_ID, record.header.position);
    let mut bytes = seal(&record, &path, |_, _| {});
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    storage.create(&path, &bytes).await.unwrap();
    let object = storage
        .list(&ObjectPrefix::device_logs(STORE_ID))
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
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(
        devices[1]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()[0]
            .as_log_refusal()
            .unwrap()
            .object,
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
                replaces: None,
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
            .list(&ObjectPrefix::device_logs(STORE_ID))
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
            // No snapshot gives the reload a target past local positions; the
            // waiting write stays with discovery.
            devices[1].sync.reload_from_snapshots().await.unwrap();
            assert!(rows(&devices[1].db).await.is_empty());
        }
        devices[1].sync.sync_store_log().await.unwrap();
        if reload {
            devices[1].sync.reload_from_snapshots().await.unwrap();
            assert!(rows(&devices[1].db).await.is_empty());
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
                replaces: None,
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
            STORE_ID,
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
        let path = crate::write_seal::path(STORE_ID, record.header.position);
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
            .list(&ObjectPrefix::device_logs(STORE_ID))
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
            u8::from(coven_format::pending::RefusalCode::from(&damage.failure)),
            tag,
            "{defect}"
        );
        devices[1].writes.download_writes().await.unwrap();
        assert!(rows(&devices[1].db).await.is_empty());
        let saved = devices[1]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            damage.failure,
            crate::Refusal::from(saved[0].as_log_refusal().unwrap().failure)
        );
        assert_eq!(
            u8::from(saved[0].as_log_refusal().unwrap().failure),
            tag,
            "{defect}"
        );
        devices[1].writes.post_positions().await.unwrap();
        let post = posted(&storage, &devices[1], 2).await;
        assert_eq!(post.pending[0], saved[0].as_log_refusal().unwrap().into());
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
    sql(
        &devices[1].db,
        "INSERT INTO notes VALUES('two','third','body')",
    )
    .await;
    let mut record = queued(&devices[1].db).await;
    assert!(!record.header.had_read.0.is_empty());
    record.header.had_read.0.clear();
    publish(&storage, &record).await;
    devices[2].writes.download_writes().await.unwrap();
    let refused = devices[2]
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(u8::from(refused[0].as_log_refusal().unwrap().failure), 5);
    assert_eq!(rows(&devices[2].db).await[0].1, "second");
}

async fn streamed_write_requests(inspect_header: bool) {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    devices[0]
        .db
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('stream','title',?1)",
                ["x".repeat(200_000)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let record = queued(&devices[0].db).await;
    devices[0].writes.upload_writes().await.unwrap();
    let path = crate::write_seal::path(STORE_ID, record.header.position);
    let object = storage.status(&path).await.unwrap().unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    let ring = devices[1].custody.read().unwrap().unwrap();
    let before = storage.reads().await.len();
    if inspect_header {
        let retained = crate::object_input::ObjectInput::observe(&devices[1].db, &object)
            .await
            .unwrap();
        crate::write_object::open(storage.as_ref(), &object, Some(&ring), &retained)
            .await
            .unwrap();
        let reads = storage.reads().await;
        assert!(
            reads[before..]
                .iter()
                .filter(|(read, _, _)| read == &path)
                .count()
                <= 2
        );
    }
    let before_body = storage.reads().await.len();
    let mut replays = crate::replay_cache::ReplayCache::new(&log);
    assert_eq!(
        devices[1]
            .writes
            .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    let reads = storage.reads().await;
    let bodies: Vec<_> = reads[before_body..]
        .iter()
        .filter(|(read, _, _)| read == &path)
        .collect();
    assert_eq!(
        bodies.len(),
        1,
        "one response must carry the whole uncached body: {bodies:?}"
    );
    let mut through = 0;
    for (_, offset, length) in reads[before..].iter().filter(|(read, _, _)| read == &path) {
        assert_eq!(*offset, through, "received bytes must not overlap or skip");
        through += length;
    }
    assert_eq!(through, object.size);
    assert_eq!(rows(&devices[1].db).await[0].2.len(), 200_000);
}

#[tokio::test]
async fn direct_write_uses_one_body_response_without_header_requests() {
    streamed_write_requests(false).await;
}

#[tokio::test]
async fn inspected_write_uses_two_header_ranges_and_one_remaining_body() {
    streamed_write_requests(true).await;
}

async fn streamed_disposition(
    defect: &str,
    split: usize,
) -> (serde_json::Value, serde_json::Value) {
    use coven_storage::test_utils::BodyFailure;
    use serde_json::json;
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    devices[0]
        .db
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('stream','title',?1)",
                ["x".repeat(150_000)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let record = queued(&devices[0].db).await;
    devices[0].writes.upload_writes().await.unwrap();
    let path = crate::write_seal::path(STORE_ID, record.header.position);
    let object = storage.status(&path).await.unwrap().unwrap();
    if matches!(
        defect,
        "header" | "chunk" | "signature" | "chunk_interrupted"
    ) {
        let bytes = storage.read(&path).await.unwrap();
        let offset = match defect {
            "header" => 60,
            "chunk" | "chunk_interrupted" => {
                coven_format::sealed_write::WriteObjectPrefix::header_end(&bytes).unwrap() + 8
            }
            "signature" => bytes.len() - 1,
            _ => unreachable!(),
        };
        storage.corrupt_byte(&path, offset).await.unwrap();
    }
    let mut faults = Faults::none();
    faults.read_chunk_size = split;
    let failure = match defect {
        "truncated" => Some(BodyFailure::Truncated(object.size - 1)),
        "interrupted" | "chunk_interrupted" => Some(BodyFailure::Interrupted(object.size)),
        "extra" => Some(BodyFailure::ExtraByte),
        _ => None,
    };
    if let Some(failure) = failure {
        faults.body_failures.insert(path.clone(), failure);
    }
    storage.set_faults(faults).await;
    if defect == "disk" {
        let retained = devices[1].db.input_budget().await.unwrap().retained;
        devices[1].db.set_input_budget(retained).await.unwrap();
    }
    let log = devices[1].db.store_log().await.unwrap();
    let ring = devices[1].custody.read().unwrap().unwrap();
    let before = storage.reads().await.len();
    let mut replays = crate::replay_cache::ReplayCache::new(&log);
    let result = devices[1]
        .writes
        .receive_write(&object, &ring, &log, &mut replays, &member().member_id())
        .await;
    let complete = matches!(&result, Ok(ApplyOutcome::Applied));
    let automatic = matches!(&result, Err(SyncError::Storage(_) | SyncError::Database(_)));
    if let Err(SyncError::Damaged(damage)) = &result {
        crate::write_object::record_damage(
            &devices[1].db,
            storage.as_ref(),
            &object,
            &damage.failure,
        )
        .await
        .unwrap();
    }
    let refused = !crate::pending_logs::local_refusals(&devices[1].db)
        .await
        .unwrap()
        .is_empty();
    assert_eq!(complete, defect == "valid", "{defect}: {result:?}");
    assert_eq!(
        refused,
        matches!(defect, "header" | "chunk" | "signature"),
        "{defect}: {result:?}"
    );
    let reads = storage.reads().await;
    assert_eq!(
        reads[before..]
            .iter()
            .filter(|(read, _, _)| read == &path)
            .count(),
        1,
        "{defect}"
    );
    assert_eq!(rows(&devices[1].db).await.len(), usize::from(complete));
    let case = json!({"check":"download", "fetched": !matches!(defect, "truncated" | "interrupted" | "chunk_interrupted" | "extra"), "authenticated": !matches!(defect, "header" | "chunk" | "chunk_interrupted"), "hash": defect != "signature", "disk": defect != "disk"});
    (
        case,
        json!({"complete":complete,"refused":refused,"automatic":automatic}),
    )
}

#[tokio::test]
async fn streaming_dispositions_require_eof_and_complete_authentication() {
    for defect in [
        "valid",
        "header",
        "chunk",
        "signature",
        "truncated",
        "interrupted",
        "chunk_interrupted",
        "extra",
        "disk",
    ] {
        streamed_disposition(defect, 3).await;
    }
}

#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_streaming_download_dispositions() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for defect in [
        "valid",
        "header",
        "chunk",
        "signature",
        "truncated",
        "interrupted",
        "chunk_interrupted",
        "extra",
        "disk",
    ] {
        let (case, result) = streamed_disposition(defect, 65_537).await;
        cases.push(case);
        actual.push(result);
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

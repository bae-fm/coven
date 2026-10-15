//! Each object's wait or refusal is its own subject's pending record (§19.1),
//! and neither fails the pass for independent logs.

use super::*;
use crate::{PendingReason, PendingRecord, PendingSubject, Prerequisite};

async fn pending(db: &Database) -> Vec<PendingRecord> {
    db.pending::<PendingRecord>().await.unwrap()
}

fn write(device: u64, number: u64) -> WriteId {
    WriteId {
        device: DeviceId(device),
        number,
    }
}

#[tokio::test]
async fn a_write_waits_on_its_authors_unapplied_entry_until_that_entry_arrives() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(4),
            name: "fourth".into(),
            replaces: None,
        })
        .await
        .unwrap();
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','waits','body')",
    )
    .await;
    devices[0].writes.upload_writes().await.unwrap();
    sql(
        &devices[2].db,
        "INSERT INTO notes VALUES('two','independent','body')",
    )
    .await;
    devices[2].writes.upload_writes().await.unwrap();
    // The reader has not applied entry 4 of the author's log, which the
    // write read: the write waits on that object, and the other log applies.
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(
        pending(&devices[1].db).await,
        vec![PendingRecord {
            subject: PendingSubject::Write(write(1, 1)),
            reason: PendingReason::Waits(Prerequisite::Object(crate::store_log_object::path(
                STORE_ID,
                coven_format::value::EntryId {
                    device: DeviceId(1),
                    number: 4,
                },
            ))),
            reported_by: DeviceId(2),
        }]
    );
    assert_eq!(rows(&devices[1].db).await[0].1, "independent");
    let reads = storage.reads().await;
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert!(pending(&devices[1].db).await.is_empty());
    assert_eq!(rows(&devices[1].db).await.len(), 2);
    // The waiting write applied from its retained bytes.
    let path = crate::write_seal::path(STORE_ID, write(1, 1));
    assert!(!storage.reads().await[reads.len()..]
        .iter()
        .any(|(read, _, _)| read == &path));
}

#[tokio::test]
async fn a_missing_store_key_is_the_writes_wait_and_its_arrival_applies_retained_bytes() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','title','body')",
    )
    .await;
    devices[0].writes.upload_writes().await.unwrap();
    let path = crate::write_seal::path(STORE_ID, write(1, 1));
    let object = storage.status(&path).await.unwrap().unwrap();
    let log = devices[1].db.store_log().await.unwrap();
    let held = devices[1].custody.read().unwrap().unwrap();
    let other = StoreKeyring::new(StoreKey::from_bytes(KeyId(Uuid::from_u128(2)), [8; 32]));
    let member = devices[1].identity.read().unwrap().unwrap().member();
    let mut replays = crate::replay_cache::ReplayCache::new(&log);
    let result = devices[1]
        .writes
        .receive_write(&object, &other, &log, &mut replays, &member, DeviceId(2))
        .await;
    assert!(
        matches!(result, Err(SyncError::KeyUnavailable(key)) if key == KeyId(Uuid::from_u128(1)))
    );
    assert_eq!(
        pending(&devices[1].db).await,
        vec![PendingRecord {
            subject: PendingSubject::Write(write(1, 1)),
            reason: PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: KeyId(Uuid::from_u128(1)),
            },
            reported_by: DeviceId(2),
        }]
    );
    let reads = storage.reads().await;
    assert_eq!(
        devices[1]
            .writes
            .receive_write(&object, &held, &log, &mut replays, &member, DeviceId(2))
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert!(pending(&devices[1].db).await.is_empty());
    assert_eq!(storage.reads().await, reads);
}

#[tokio::test]
async fn a_newer_format_waits_for_an_update_and_independent_logs_continue() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    for (device, title) in [(0, "newer"), (2, "independent")] {
        devices[device]
            .db
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,?1,'body')", [title])?;
                Ok(())
            })
            .await
            .unwrap();
        devices[device].writes.upload_writes().await.unwrap();
    }
    let path = crate::write_seal::path(STORE_ID, write(1, 1));
    let mut bytes = storage.read(&path).await.unwrap();
    bytes[2] = 2;
    storage.delete(&path).await.unwrap();
    storage.create(&path, &bytes).await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    let record = PendingRecord {
        subject: PendingSubject::Write(write(1, 1)),
        reason: PendingReason::UpdateRequired(crate::RequiredUpdate::CovenFormat { version: 2 }),
        reported_by: DeviceId(2),
    };
    assert_eq!(pending(&devices[1].db).await, vec![record.clone()]);
    assert_eq!(record.retry(), crate::Retry::AfterUpdate);
    assert_eq!(
        rows(&devices[1].db).await,
        vec![("independent".into(), "independent".into(), "body".into())]
    );
    // No refusal: the bytes need a newer coven, not another download.
    assert!(crate::pending_logs::local_refusals(&devices[1].db)
        .await
        .unwrap()
        .is_empty());
    let reads = storage.reads().await;
    devices[1].writes.download_writes().await.unwrap();
    assert!(!storage.reads().await[reads.len()..]
        .iter()
        .any(|(read, _, _)| read == &path));
}

#[tokio::test]
async fn a_complete_invalid_write_refuses_its_log_while_others_apply() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    for title in ["first", "second"] {
        devices[0]
            .db
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,?1,'body')", [title])?;
                Ok(())
            })
            .await
            .unwrap();
        devices[0].writes.upload_writes().await.unwrap();
    }
    sql(
        &devices[2].db,
        "INSERT INTO notes VALUES('independent','x','body')",
    )
    .await;
    devices[2].writes.upload_writes().await.unwrap();
    let path = crate::write_seal::path(STORE_ID, write(1, 1));
    let size = storage.read(&path).await.unwrap().len();
    storage.corrupt_byte(&path, size - 1).await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(
        pending(&devices[1].db).await,
        vec![PendingRecord {
            subject: PendingSubject::Write(write(1, 1)),
            reason: PendingReason::Refused(crate::Refusal::Signature { cause: None }),
            reported_by: DeviceId(2),
        }]
    );
    assert_eq!(rows(&devices[1].db).await[0].0, "independent");
    // The refused number stops its log: nothing at or past it is requested.
    let reads = storage.reads().await;
    devices[1].writes.download_writes().await.unwrap();
    let later = crate::write_seal::path(STORE_ID, write(1, 2));
    assert!(!storage.reads().await[reads.len()..]
        .iter()
        .any(|(read, _, _)| read == &path || read == &later));
}

#[tokio::test]
async fn unregistered_peers_positions_wait_then_check_from_retained_bytes() {
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
    let mut third = device(storage.clone(), 3).await;
    third.sync.sync_store_log().await.unwrap();
    assert!(third.writes.post_positions().await.unwrap());
    let path = ObjectPath::positions(STORE_ID, DeviceId(3));
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(
        pending(&devices[1].db).await,
        vec![PendingRecord {
            subject: PendingSubject::Positions(DeviceId(3)),
            // The registration is the entry the post's signed view names (D9).
            reason: PendingReason::Waits(Prerequisite::Object(crate::store_log_object::path(
                STORE_ID,
                coven_format::value::EntryId {
                    device: DeviceId(1),
                    number: 3
                }
            ))),
            reported_by: DeviceId(2),
        }]
    );
    let reads = storage.reads().await;
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert!(pending(&devices[1].db).await.is_empty());
    assert!(!storage.reads().await[reads.len()..]
        .iter()
        .any(|(read, _, _)| read == &path));
}

#[tokio::test]
async fn invalid_positions_count_as_not_posted_and_disappear_with_their_object() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let path = ObjectPath::positions(STORE_ID, DeviceId(1));
    // A store-log entry's envelope at a positions path has the wrong identity.
    let entry = storage
        .read(&crate::store_log_object::path(
            STORE_ID,
            coven_format::value::EntryId {
                device: DeviceId(1),
                number: 1,
            },
        ))
        .await
        .unwrap();
    storage.replace(&path, &entry).await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    let record = PendingRecord {
        subject: PendingSubject::Positions(DeviceId(1)),
        reason: PendingReason::InvalidPositions(crate::Refusal::WrongIdentity { cause: None }),
        reported_by: DeviceId(2),
    };
    assert_eq!(pending(&devices[1].db).await, vec![record.clone()]);
    assert_eq!(record.retry(), crate::Retry::Automatic);
    storage.delete(&path).await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert!(pending(&devices[1].db).await.is_empty());
}

#[tokio::test]
async fn multiple_reported_subjects_in_one_log_retain_each_condition() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    for title in ["first", "second"] {
        devices[0]
            .db
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,?1,'body')", [title])?;
                Ok(())
            })
            .await
            .unwrap();
        devices[0].writes.upload_writes().await.unwrap();
    }
    let first = coven_database::LogRefusal {
        object: LogObject::Write(WriteId {
            device: DeviceId(1),
            number: 1,
        }),
        failure: RefusalCode::Parse,
    };
    let second = coven_database::LogRefusal {
        object: LogObject::Write(WriteId {
            device: DeviceId(1),
            number: 2,
        }),
        failure: RefusalCode::Signature,
    };
    let mut positions = devices[1]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap();
    positions.pending = [first, second]
        .into_iter()
        .map(|refusal| {
            crate::pending_logs::reports::publish(
                crate::PendingRecord::log_refusal(refusal, DeviceId(2)),
                DeviceId(2),
            )
            .unwrap()
        })
        .collect();
    let path = ObjectPath::positions(STORE_ID, positions.device);
    storage
        .replace(&path, &signed_positions(&devices[1], positions))
        .await
        .unwrap();
    devices[0].writes.compare_fingerprints().await.unwrap();
    assert_eq!(
        devices[0]
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap(),
        vec![
            crate::PendingRecord::log_refusal(first, DeviceId(2)),
            crate::PendingRecord::log_refusal(second, DeviceId(2))
        ]
    );
    assert!(devices[0]
        .writes
        .current_positions()
        .await
        .unwrap()
        .unwrap()
        .pending
        .is_empty());
}

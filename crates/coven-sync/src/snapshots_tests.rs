use super::operations::operation_owner;
use super::*;
use crate::{
    operation_data::Data,
    operations::Progress,
    snapshot_data::{SnapshotJob, SnapshotTask},
};
use coven_database::{Migration, RowIdentity, SyncedTable};
use std::num::NonZeroU64;

#[path = "snapshot_boundaries_tests.rs"]
mod boundaries;
#[path = "snapshot_retention_tests.rs"]
mod retention;
#[path = "snapshot_return_tests.rs"]
mod returns;
#[path = "snapshot_catalog_tests.rs"]
mod signatures;

/// This device's record for the snapshot at `path`, if any.
async fn snapshot_record(device: &Device, path: &ObjectPath) -> Option<crate::PendingReason> {
    let subject = crate::PendingSubject::Snapshot(path.snapshot_id().unwrap());
    let own = device.device().await;
    device
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.subject == subject && record.reported_by == own)
        .map(|record| record.reason)
}

fn snapshot_storage() -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    )
}

fn notes_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)
        .key_columns(["audience", "id"])
        .audience_column("audience")]
}
fn notes_migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL,audience TEXT NOT NULL,title TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(audience,id))")]
}
async fn notes_device(storage: Arc<MemoryStorage>, n: u64) -> Device {
    Device::new(
        storage,
        n,
        member(1),
        store(1),
        notes_tables(),
        notes_migrations(),
        Arc::new(coven_foundation::id_source::UuidIds),
    )
    .await
}
async fn add_device(device: &mut Device) {
    device.sync().await;
    let id = device.device().await;
    device
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: id,
            name: id.0.to_string(),
            replaces: None,
        })
        .await
        .unwrap();
}

async fn write_rows(device: &Device, start: usize, count: usize, bytes: usize, audience: Audience) {
    let audience = match audience {
        Audience::Store => "store".to_owned(),
        Audience::Circle(id) => id.to_string(),
    };
    device
        .db
        .write(move |sql| {
            for number in start..start + count {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,?2,?3,?4)",
                    coven_database::params![
                        number.to_string(),
                        audience,
                        format!("note {number}"),
                        vec![number as u8; bytes]
                    ],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
}

async fn upload(device: &Device, storage: &Arc<MemoryStorage>) {
    if !device.send.can_send() {
        device.catch_up().await;
    }
    crate::DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.custody.clone(),
        device.identity.clone(),
        device.clock.clone(),
        device.send.clone(),
    )
    .upload_writes()
    .await
    .unwrap();
}

async fn tables(device: &Device) -> Vec<(String, String, Vec<u8>)> {
    device
        .db
        .read(|sql| {
            Ok(
                sql.query("SELECT id,title,body FROM notes ORDER BY id", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?,
            )
        })
        .await
        .unwrap()
}

async fn fingerprint(device: &Device) -> Vec<(Audience, coven_crypto::Fingerprint)> {
    let ring = device.custody.read().unwrap().unwrap();
    let log = device.log().await;
    let state = log.replay.state.clone();
    let mut keys = vec![(
        Audience::Store,
        ring.store_key(crate::key_introductions::newest(&log, &Audience::Store).unwrap())
            .unwrap()
            .derive()
            .fingerprint_hasher(),
    )];
    for circle in state.circles.into_keys() {
        let key = crate::key_introductions::newest(&log, &Audience::Circle(circle)).unwrap();
        keys.push((
            Audience::Circle(circle),
            ring.circle_key(circle, key)
                .unwrap()
                .derive()
                .fingerprint_hasher(),
        ));
    }
    device.db.sync_state(keys).await.unwrap().fingerprints
}

#[tokio::test]
async fn far_future_threshold_snapshot_and_later_writes_reload_on_a_new_device() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    write_rows(&a, 0, 10, 32768, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .is_empty());
    write_rows(&a, 10, 30, 32768, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::snapshots(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    a.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    write_rows(&a, 40, 1, 17, Audience::Store).await;
    let tail = a.db.test_queued_writes().await.unwrap().pop().unwrap();
    upload(&a, &storage).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    b.sync.reload_from_snapshots().await.unwrap();
    b.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert!(a.db.operations().await.unwrap().is_empty());
    assert!(b.db.operations().await.unwrap().is_empty());
    write_rows(&b, 41, 1, 17, Audience::Store).await;
    let next = b.db.test_queued_writes().await.unwrap().pop().unwrap();
    assert!(next.header.timestamp > tail.header.timestamp);
}

/// A snapshot load alone moves the clock past every write it covers: no store-log
/// entry or downloaded write carries the newer time (§7.2).
#[tokio::test]
async fn loading_a_snapshot_advances_the_timestamp_clock() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    write_rows(&a, 0, 40, 32768, Audience::Store).await;
    let covered = a.db.test_queued_writes().await.unwrap().pop().unwrap();
    upload(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::snapshots(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    let write_reads = || async {
        storage
            .reads()
            .await
            .into_iter()
            .filter(|(path, _, _)| path.write_id().is_some())
            .count()
    };
    let before = write_reads().await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.clock.set(UNIX_EPOCH - Duration::from_secs(1));
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(write_reads().await, before);
    assert!(b
        .log()
        .await
        .entries
        .iter()
        .all(|e| e.entry.timestamp < covered.header.timestamp));
    assert_eq!(tables(&a).await, tables(&b).await);
    write_rows(&b, 40, 1, 17, Audience::Store).await;
    let next = b.db.test_queued_writes().await.unwrap().pop().unwrap();
    assert!(next.header.timestamp > covered.header.timestamp);
}

#[tokio::test]
async fn the_signed_catch_up_time_is_a_provider_sample_never_device_time() {
    let sampled = UNIX_EPOCH + Duration::new(1_760_400_000, 250_000_000);
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(sampled));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .time_resolution(Duration::from_millis(1))
            .build()
            .unwrap(),
    );
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.clock.set(sampled + Duration::from_secs(365 * 86400));
    write_rows(&a, 0, 2, 17, Audience::Store).await;
    upload(&a, &storage).await;
    // Each preparation observes storage time first, including on a reopened
    // owner and after storage time has moved on without new store objects.
    for (reopen, later) in [(false, 0), (true, 0), (false, 2 * 86400)] {
        if reopen {
            a.reopen(storage.clone(), notes_tables(), notes_migrations())
                .await;
            a.catch_up().await;
        }
        let sampled = sampled + Duration::from_secs(later);
        clock.set(sampled);
        let expected = coven_format::value::StorageTime::from_system_time(sampled).unwrap();
        let id = a.sync.write_snapshot(Audience::Store).await.unwrap();
        let path = ObjectPath::snapshot(
            store(1),
            id.audience,
            id.device,
            NonZeroU64::new(id.number).unwrap(),
        );
        let bytes = storage.read(&path).await.unwrap();
        use coven_format::sealed_snapshot::SnapshotObjectPrefix;
        let length = SnapshotObjectPrefix::length(&bytes).unwrap();
        let prefix = SnapshotObjectPrefix::decode(&bytes[..length]).unwrap();
        assert_eq!(
            prefix.caught_at, expected,
            "reopened {reopen}, {later}s later"
        );
        assert_eq!(prefix.caught_at.nanos(), 250_000_000);
        assert!(prefix.counts(
            coven_format::value::StorageTime::from_system_time(
                storage.status(&path).await.unwrap().unwrap().stored_at
            )
            .unwrap()
        ));
    }
}

#[tokio::test]
async fn writing_resumes_after_each_recorded_step_with_identical_ciphertext() {
    'crashes: for crash_step in 0.. {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        write_rows(&a, 0, 35, 32768, Audience::Store).await;
        upload(&a, &storage).await;
        let data = Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Write {
                audience: Audience::Store,
                device: a.device().await,
                trigger: crate::snapshot_data::SnapshotTrigger::Growth,
                session: None,
            },
            temporary: Vec::new(),
        });
        let id =
            a.db.start_operation(data.new_operation("coven").unwrap())
                .await
                .unwrap();
        for _ in 0..crash_step {
            let record = a.operation(id).await;
            match a
                .sync
                .operation_step(&record, Data::read(&record).unwrap())
                .await
                .unwrap()
            {
                Progress::Advanced => (),
                Progress::Finished(crate::operations::Output::Unit) => break 'crashes,
                _ => panic!("unexpected snapshot progress"),
            }
        }
        let before = a.db.operations().await.unwrap().remove(0);
        let Data::Snapshots(task) = Data::read(&before).unwrap() else {
            panic!()
        };
        let fixed = if before.last_step > 0 {
            Some(
                a.directory
                    .file(
                        coven_foundation::files::FileArea::AppProvided,
                        &coven_foundation::files::FileName::new(task.temporary[0].clone()).unwrap(),
                    )
                    .open_reader()
                    .map(|reader| reader.read_at(0, reader.size() as usize).unwrap())
                    .unwrap(),
            )
        } else {
            None
        };
        a.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        a.catch_up().await;
        a.sync.write_snapshots().await.unwrap();
        let objects = storage
            .list(&ObjectPrefix::snapshots(store(1)))
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        if let Some(bytes) = fixed {
            assert_eq!(storage.read(&objects[0].path).await.unwrap(), bytes);
        }
        assert!(a.db.operations().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn uncovered_logs_survive_thirty_days_and_never_posting_devices_hold_covered_logs() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 35, 32768, Audience::Store).await;
    upload(&a, &storage).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    a.sync.run_retention().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    a.clock.set(UNIX_EPOCH + Duration::from_secs(1));
    a.catch_up().await;
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
}

#[tokio::test]
async fn concurrent_snapshots_covering_different_pasts_converge_from_either_one() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    let first = a.db.test_queued_writes().await.unwrap().remove(0);
    b.db.apply_downloaded::<crate::PendingRecord>(first.into(), std::time::UNIX_EPOCH)
        .await
        .unwrap();
    upload(&a, &storage).await;
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    write_rows(&b, 2, 1, 17, Audience::Store).await;
    let a_write = a.db.test_queued_writes().await.unwrap().remove(0);
    let b_write = b.db.test_queued_writes().await.unwrap().remove(0);
    let a_snapshot = a.sync.write_snapshot(Audience::Store).await.unwrap();
    let b_snapshot = b.sync.write_snapshot(Audience::Store).await.unwrap();
    upload(&a, &storage).await;
    upload(&b, &storage).await;
    a.db.apply_downloaded::<crate::PendingRecord>(b_write.into(), std::time::UNIX_EPOCH)
        .await
        .unwrap();
    b.db.apply_downloaded::<crate::PendingRecord>(a_write.into(), std::time::UNIX_EPOCH)
        .await
        .unwrap();
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&c).await);
    let path = ObjectPath::snapshot(
        store(1),
        a_snapshot.audience,
        a_snapshot.device,
        NonZeroU64::new(a_snapshot.number).unwrap(),
    );
    storage.delete(&path).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .iter()
        .any(|o| o.path.snapshot_id() == Some(b_snapshot.clone())));
    c.sync.reload_from_snapshots().await.unwrap();
    c.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&c).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&c).await);
}

#[tokio::test]
async fn store_and_gifts_snapshots_at_different_points_load_in_one_transaction() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let gifts = circle(1);
    a.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: gifts,
            name: "Gifts".into(),
            key: key(2),
            key_hash: a
                .sync
                .test_key(coven_merge::Audience::Circle(gifts), key(2)),
        })
        .await
        .unwrap();
    for number in 0..3 {
        let audience = gifts.to_string();
        a.db.write(move |sql| {
            for audience in ["store".to_owned(), audience] {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,?2,?3,?4)",
                    coven_database::params![
                        number.to_string(),
                        audience,
                        "gift",
                        vec![number as u8; 13]
                    ],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
        upload(&a, &storage).await;
        if number == 0 {
            a.sync
                .write_snapshot(Audience::Circle(gifts))
                .await
                .unwrap();
        }
        if number == 1 {
            a.sync.write_snapshot(Audience::Store).await.unwrap();
        }
    }
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    b.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
}

#[tokio::test]
async fn deleted_history_reload_preserves_waiting_numbers_and_late_writes_converge() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded::<crate::PendingRecord>(
        a.db.test_queued_writes().await.unwrap().remove(0).into(),
        std::time::UNIX_EPOCH,
    )
    .await
    .unwrap();
    upload(&a, &storage).await;
    b.db.write(|sql| {
        sql.execute("UPDATE notes SET title='offline'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let waiting = b.db.test_queued_writes().await.unwrap();
    a.db.write(|sql| {
        sql.execute("UPDATE notes SET title='online'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    upload(&a, &storage).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    a.catch_up().await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(b.db.test_queued_writes().await.unwrap(), waiting);
    upload(&b, &storage).await;
    a.sync.reload_from_snapshots().await.unwrap();
    // A reload stops at its target; ordinary discovery reads the later tail.
    a.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
}

#[tokio::test]
async fn waiting_parts_alone_can_trigger_a_snapshot() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 35, 32768, Audience::Store).await;
    let waiting = a.db.test_queued_writes().await.unwrap();
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::snapshots(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(a.db.test_queued_writes().await.unwrap(), waiting);
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
}

async fn selected_snapshot(device: &mut Device) -> ObjectPath {
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            scope: crate::snapshot_data::ReloadScope::All,
            files: None,
        },
        temporary: Vec::new(),
    });
    let id = device
        .db
        .start_operation(data.new_operation("coven").unwrap())
        .await
        .unwrap();
    let record = device.operation(id).await;
    device.sync.operation_step(&record, data).await.unwrap();
    let record = device.operation(id).await;
    let Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            files: Some(files), ..
        },
        ..
    }) = Data::read(&record).unwrap()
    else {
        panic!()
    };
    let selected = files.snapshots[0].path.clone();
    device.sync.sync_store_log().await.unwrap();
    selected
}

#[tokio::test]
async fn replay_reload_is_journaled_and_keeps_its_boundary_snapshot() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let reset = a.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync
        .make_and_upload_entry(StoreChange::Reset {
            snapshot: reset.clone(),
        })
        .await
        .unwrap();
    assert!(!a.db.operations().await.unwrap().is_empty());
    a.sync.sync_store_log().await.unwrap();
    b.sync.sync_store_log().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .iter()
        .any(|o| o.path.snapshot_id() == Some(reset.clone())));
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
}

#[tokio::test]
async fn a_snapshot_exceeding_the_transfer_memory_budget_streams_in_bounded_buffers() {
    const MEMORY_BUDGET: u64 = 32 * 1024 * 1024;
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    for batch in 0..36 {
        write_rows(&a, batch * 32, 32, 32768, Audience::Store).await;
    }
    a.sync.write_snapshots().await.unwrap();
    let snapshots = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap();
    assert!(snapshots[0].size > MEMORY_BUDGET);
    let mut b = notes_device(storage.clone(), 2).await;
    b.db.set_input_budget(128 * 1024 * 1024).await.unwrap();
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert!(storage.transferred().await.1 <= 65536);
    assert!(storage
        .received_buffers()
        .await
        .iter()
        .all(|length| *length <= 65536));
}

#[tokio::test]
async fn reload_reselects_after_store_log_changes_and_preserves_interleaved_app_writes() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            scope: crate::snapshot_data::ReloadScope::All,
            files: None,
        },
        temporary: Vec::new(),
    });
    let id =
        a.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let record = a.operation(id).await;
    a.sync.operation_step(&record, data).await.unwrap();
    assert!(matches!(
        a.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(77),
                name: "pending".into(),
                replaces: None,
            })
            .await,
        Err(SyncError::ReloadPending(_))
    ));
    b.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Gifts".into(),
            key: key(2),
            key_hash: b
                .sync
                .test_key(coven_merge::Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    a.sync.step().await.unwrap();
    write_rows(&a, 1, 1, 17, Audience::Circle(circle(1))).await;
    let waiting = a.db.test_queued_writes().await.unwrap();
    let record = a.operation(id).await;
    a.sync
        .operation_step(&record, Data::read(&record).unwrap())
        .await
        .unwrap();
    let record = a.operation(id).await;
    assert_eq!(record.last_step, 0, "stale selection must not commit");
    a.sync.sync_store_log().await.unwrap();
    assert_eq!(a.db.test_queued_writes().await.unwrap(), waiting);
    assert_eq!(tables(&a).await.len(), 2);
}

#[tokio::test]
async fn idle_pass_uses_bounded_header_ranges_and_reuses_overread_bytes() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    for device in [&mut a, &mut b, &mut c] {
        device.sync().await;
    }
    for (i, device) in [&a, &b, &c].into_iter().enumerate() {
        for batch in 0..10 {
            write_rows(device, i * 1000 + batch * 100, 100, 1024, Audience::Store).await;
        }
        upload(device, &storage).await;
    }
    for device in [&mut a, &mut b, &mut c] {
        device.writes().download_writes().await.unwrap();
        device.sync.write_snapshot(Audience::Store).await.unwrap();
    }
    let mut bounds = std::collections::BTreeMap::new();
    for object in storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
    {
        let bytes = storage.read(&object.path).await.unwrap();
        bounds.insert(
            object.path,
            (
                (coven_format::sealed_snapshot::SnapshotObjectPrefix::length(&bytes).unwrap() + 64)
                    as u64,
                object.size.min(
                    coven_format::sealed_snapshot::SnapshotObjectPrefix::MAX_SIGNED_PREFIX as u64,
                ),
            ),
        );
    }
    for object in storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
    {
        let bytes = storage.read(&object.path).await.unwrap();
        let prefix = coven_format::sealed_write::WriteObjectPrefix::length(&bytes).unwrap();
        let header =
            coven_format::sealed_write::WriteObjectPrefix::header_chunk_length(&bytes[prefix..])
                .unwrap();
        bounds.insert(
            object.path,
            (
                (prefix + header) as u64,
                object
                    .size
                    .min(coven_format::sealed_write::WriteObjectPrefix::MAX_HEADER_END as u64),
            ),
        );
    }
    assert_eq!(bounds.len(), 33);
    let files = crate::Files::new(
        coven_database::FileDatabase::new(a.db.clone()),
        a.directory.clone(),
        Some(storage.clone()),
        a.clock.clone(),
        a.send.clone(),
        a.sync.ids.clone(),
        crate::TransferLimits::default(),
    );
    let operations = operation_owner(a.sync, files.clone());
    let mut through = std::collections::BTreeMap::new();
    let mut requests = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let start = storage.reads().await.len();
        operations.sync().await.unwrap();
        let reads = storage.reads().await;
        for (path, offset, size) in &reads[start..] {
            if let Some((_, maximum)) = bounds.get(path) {
                let next = through.entry(path.clone()).or_insert(0);
                assert_eq!(*offset, *next, "repeated or skipped bytes in {path:?}");
                *next += size;
                assert!(*next <= *maximum, "range exceeds the format's header bound");
                let count = requests.entry(path.clone()).or_insert(0);
                *count += 1;
                assert!(*count <= 2, "more than two header requests for {path:?}");
            }
        }
    }
    assert!(!through.is_empty(), "fixture must inspect uncached headers");
    for (path, size) in through {
        assert!(size >= bounds[&path].0, "header is incomplete");
    }
    operations.close().await.unwrap();
    files.close().await;
}

#[tokio::test]
async fn a_pass_reuses_loaded_snapshot_and_write_references() {
    for deleted_history in [false, true] {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = notes_device(storage.clone(), 2).await;
        add_device(&mut b).await;
        write_rows(&b, 0, 10, 1024, Audience::Store).await;
        upload(&b, &storage).await;
        if deleted_history {
            b.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
            b.catch_up().await;
            b.sync.write_snapshot(Audience::Store).await.unwrap();
            assert!(storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .is_empty());
        }
        let objects = storage
            .list(&if deleted_history {
                ObjectPrefix::snapshots(store(1))
            } else {
                ObjectPrefix::device_logs(store(1))
            })
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        // This device owns an unreferenced upload, so retention must prove
        // absence across the object the pass is about to load.
        let orphan = ObjectPath::file(
            store(1),
            a.device().await,
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
        );
        storage.create(&orphan, b"orphan").await.unwrap();
        let files = crate::Files::new(
            coven_database::FileDatabase::new(a.db.clone()),
            a.directory.clone(),
            Some(storage.clone()),
            a.clock.clone(),
            a.send.clone(),
            a.sync.ids.clone(),
            crate::TransferLimits::default(),
        );
        let database = a.db.clone();
        let operations = operation_owner(a.sync, files.clone());
        let start = storage.reads().await.len();
        operations.sync().await.unwrap();
        assert!(database
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        let reads = storage.reads().await;
        let object = &objects[0];
        let bytes: u64 = reads[start..]
            .iter()
            .filter(|(path, _, _)| path == &object.path)
            .map(|(_, _, size)| size)
            .sum();
        assert_eq!(
            bytes, object.size,
            "each object is read once, including its prefix/header"
        );
        assert!(storage
            .list(&ObjectPrefix::files(store(1)))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            database
                .read(|sql| Ok(
                    sql.query_row("SELECT count(*) FROM notes", [], |row| row.get::<_, i64>(0))?
                ))
                .await
                .unwrap(),
            10
        );
        operations.close().await.unwrap();
        files.close().await;
    }
}

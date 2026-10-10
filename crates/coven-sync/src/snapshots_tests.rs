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
#[path = "snapshot_catalog_tests.rs"]
mod signatures;

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
    crate::DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.custody.clone(),
        Arc::new(InMemoryCustody::new(device.member.clone())),
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
    let ring = device.custody.unlock().unwrap().unwrap();
    let state = device.log().await.replay.state;
    let mut keys = vec![(
        Audience::Store,
        ring.store_key(state.store.unwrap().key)
            .unwrap()
            .derive()
            .fingerprint_hasher(),
    )];
    for (circle, state) in state.circles {
        keys.push((
            Audience::Circle(circle),
            ring.circle_key(circle, state.key)
                .unwrap()
                .derive()
                .fingerprint_hasher(),
        ));
    }
    device.db.sync_state(keys).await.unwrap().fingerprints
}

#[tokio::test]
async fn threshold_snapshot_and_later_writes_reload_on_a_new_device() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 10, 32768, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .is_empty());
    write_rows(&a, 10, 30, 32768, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::snapshots())
            .await
            .unwrap()
            .len(),
        1
    );
    write_rows(&a, 40, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert!(a.db.operations().await.unwrap().is_empty());
    assert!(b.db.operations().await.unwrap().is_empty());
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
        a.sync.write_snapshots().await.unwrap();
        let objects = storage.list(&ObjectPrefix::snapshots()).await.unwrap();
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
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .len(),
        1
    );
    a.clock.set(UNIX_EPOCH + Duration::from_secs(1));
    a.sync.write_snapshots().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .len(),
        1
    );
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs())
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
    b.db.apply_downloaded(first.into()).await.unwrap();
    upload(&a, &storage).await;
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    write_rows(&b, 2, 1, 17, Audience::Store).await;
    let a_write = a.db.test_queued_writes().await.unwrap().remove(0);
    let b_write = b.db.test_queued_writes().await.unwrap().remove(0);
    let a_snapshot = a.sync.write_snapshot(Audience::Store).await.unwrap();
    let b_snapshot = b.sync.write_snapshot(Audience::Store).await.unwrap();
    upload(&a, &storage).await;
    upload(&b, &storage).await;
    a.db.apply_downloaded(b_write.into()).await.unwrap();
    b.db.apply_downloaded(a_write.into()).await.unwrap();
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert_eq!(fingerprint(&a).await, fingerprint(&c).await);
    let path = ObjectPath::snapshot(
        a_snapshot.audience,
        a_snapshot.device,
        NonZeroU64::new(a_snapshot.number).unwrap(),
    );
    storage.delete(&path).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .iter()
        .any(|o| o.path.snapshot_id() == Some(b_snapshot.clone())));
    c.sync.reload_from_snapshots().await.unwrap();
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
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
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
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(b.db.test_queued_writes().await.unwrap(), waiting);
    upload(&b, &storage).await;
    a.sync.reload_from_snapshots().await.unwrap();
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
            .list(&ObjectPrefix::snapshots())
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
async fn selection_orders_coverage_then_path_and_passes_over_damaged_objects() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
        .await
        .unwrap();
    upload(&a, &storage).await;
    let a_id = a.sync.write_snapshot(Audience::Store).await.unwrap();
    let b_id = b.sync.write_snapshot(Audience::Store).await.unwrap();
    let path = |s: coven_format::store_log::SnapshotId| {
        ObjectPath::snapshot(s.audience, s.device, NonZeroU64::new(s.number).unwrap())
    };
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    assert_eq!(
        selected_snapshot(&mut c).await,
        path(a_id.clone()).min(path(b_id.clone()))
    );
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let newer = path(a.sync.write_snapshot(Audience::Store).await.unwrap());
    assert_eq!(selected_snapshot(&mut c).await, newer);
    let size = storage
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .into_iter()
        .find(|o| o.path == newer)
        .unwrap()
        .size;
    storage
        .corrupt_byte(&newer, size as usize - 1)
        .await
        .unwrap();
    let damages = c.sync.reload_from_snapshots().await.unwrap();
    assert!(damages.iter().any(|d| d.path == newer.as_str()));
    assert_eq!(selected_snapshot(&mut c).await, path(b_id));
    assert_eq!(tables(&a).await, tables(&c).await);
    // A malformed clear prefix is also passed over without reading a body.
    let malformed = ObjectPath::snapshot(
        Audience::Store,
        a.device().await,
        NonZeroU64::new(999).unwrap(),
    );
    storage.create_once(&malformed, b"x").await.unwrap();
    assert!(c
        .sync
        .reload_from_snapshots()
        .await
        .unwrap()
        .iter()
        .any(|d| d.path == malformed.as_str()));
}

#[tokio::test]
async fn newer_schema_snapshot_is_passed_over_for_an_older_supported_one() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
        .await
        .unwrap();
    upload(&a, &storage).await;
    let supported = b.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let mut migrations = notes_migrations();
    migrations.push(Migration::sql(
        2,
        "addition",
        "ALTER TABLE notes ADD COLUMN added TEXT",
    ));
    a.reopen(storage.clone(), notes_tables(), migrations).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    assert_eq!(
        selected_snapshot(&mut c).await.snapshot_id(),
        Some(supported)
    );
    assert_eq!(tables(&a).await, tables(&c).await);
}

#[tokio::test]
async fn a_snapshot_sealed_by_a_dropped_key_introduction_is_not_a_candidate() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    b.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    a.add(&b.member, MemberRole::Admin).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
        .await
        .unwrap();
    let selected = a.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    let write = b.db.test_queued_writes().await.unwrap()[0].header.position;
    upload(&b, &storage).await;
    let path = ObjectPath::device_log(write.device, NonZeroU64::new(write.number).unwrap());
    let stored = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    b.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    b.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: a.member.member_id(),
            key: key(2),
            circle_keys: Vec::new(),
        })
        .await
        .unwrap();
    b.sync.write_snapshot(Audience::Store).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member_id(),
            key: key(3),
            circle_keys: Vec::new(),
        })
        .await
        .unwrap();
    a.sync().await;
    let before = tables(&a).await;
    assert!(
        matches!(a.sync.reload_from_snapshots().await, Err(SyncError::Database(
        coven_database::DbError::Snapshot(coven_database::SnapshotError::MissingWrites { missing })
    )) if missing == vec![write])
    );
    assert_eq!(tables(&a).await, before);
    storage.create(&path, &stored).await.unwrap();
    a.sync.sync_store_log().await.unwrap();
    assert_eq!(
        selected_snapshot(&mut a).await.snapshot_id(),
        Some(selected)
    );
    assert_eq!(tables(&a).await, tables(&b).await);
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
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .iter()
        .any(|o| o.path.snapshot_id() == Some(reset.clone())));
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
}

#[tokio::test]
async fn a_snapshot_exceeding_the_transfer_memory_budget_streams_in_bounded_requests() {
    const MEMORY_BUDGET: u64 = 32 * 1024 * 1024;
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    for batch in 0..36 {
        write_rows(&a, batch * 32, 32, 32768, Audience::Store).await;
    }
    a.sync.write_snapshots().await.unwrap();
    let snapshots = storage.list(&ObjectPrefix::snapshots()).await.unwrap();
    assert!(snapshots[0].size > MEMORY_BUDGET);
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    assert!(storage.transferred().await.1 <= 65536);
    assert!(storage
        .ranges()
        .await
        .iter()
        .all(|range| range.end() - range.start() <= 65536 + 44));
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
                name: "pending".into()
            })
            .await,
        Err(SyncError::ReloadPending(_))
    ));
    b.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Gifts".into(),
            key: key(2),
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
async fn idle_pass_reads_only_snapshot_prefixes_and_log_headers() {
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
    for object in storage.list(&ObjectPrefix::snapshots()).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        bounds.insert(
            object.path,
            (coven_format::sealed_snapshot::SnapshotObjectPrefix::length(&bytes).unwrap() + 64)
                as u64,
        );
    }
    for object in storage.list(&ObjectPrefix::device_logs()).await.unwrap() {
        let bytes = storage.read(&object.path).await.unwrap();
        let prefix = coven_format::sealed_write::WriteObjectPrefix::length(&bytes).unwrap();
        let header =
            coven_format::sealed_write::WriteObjectPrefix::header_chunk_length(&bytes[prefix..])
                .unwrap();
        bounds.insert(object.path, (prefix + header) as u64);
    }
    assert_eq!(bounds.len(), 33);
    let files = crate::Files::new(
        coven_database::FileDatabase::new(a.db.clone()),
        a.directory.clone(),
        Some(storage.clone()),
        a.clock.clone(),
        a.sync.ids.clone(),
        crate::TransferLimits::default(),
    );
    let operations = operation_owner(a.sync, files.clone());
    let mut excess_reads = 0;
    for pass in 0..2 {
        let start = storage.reads().await.len();
        operations.sync().await.unwrap();
        let reads = storage.reads().await;
        let reads = &reads[start..];
        let bytes: u64 = reads.iter().map(|(_, _, size)| size).sum();
        eprintln!(
            "idle pass {pass}: {bytes} storage bytes, {} reads",
            reads.len()
        );
        let excess: Vec<_> = reads
            .iter()
            .filter(|(path, offset, size)| bounds.get(path).is_some_and(|end| offset + size > *end))
            .collect();
        excess_reads += excess.len();
        let mut through = std::collections::BTreeMap::new();
        for (path, offset, size) in reads {
            if bounds.contains_key(path) {
                let next = through.entry(path).or_insert(0);
                assert_eq!(*offset, *next, "repeated or skipped bytes in {path:?}");
                *next += size;
            }
        }
        for (path, size) in through {
            assert_eq!(size, bounds[path]);
        }
    }
    assert_eq!(excess_reads, 0, "snapshot bodies or log parts were read");
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
            b.sync.write_snapshot(Audience::Store).await.unwrap();
            assert!(storage
                .list(&ObjectPrefix::device_logs())
                .await
                .unwrap()
                .is_empty());
        }
        let objects = storage
            .list(&if deleted_history {
                ObjectPrefix::snapshots()
            } else {
                ObjectPrefix::device_logs()
            })
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        // This device owns an unreferenced upload, so retention must prove
        // absence across the object the pass is about to load.
        let orphan = ObjectPath::file(
            a.device().await,
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
        );
        storage.create(&orphan, b"orphan").await.unwrap();
        let files = crate::Files::new(
            coven_database::FileDatabase::new(a.db.clone()),
            a.directory.clone(),
            Some(storage.clone()),
            a.clock.clone(),
            a.sync.ids.clone(),
            crate::TransferLimits::default(),
        );
        let database = a.db.clone();
        let operations = operation_owner(a.sync, files.clone());
        let start = storage.reads().await.len();
        operations.sync().await.unwrap();
        assert!(operations.pending_operations().await.unwrap().is_empty());
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
            .list(&ObjectPrefix::files())
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

use super::*;
use coven_format::write::WriteDisposition;

async fn update(device: &mut Device, storage: Arc<MemoryStorage>, rename: bool, convert: bool) {
    let change = if rename {
        "ALTER TABLE notes RENAME COLUMN title TO name"
    } else {
        "ALTER TABLE notes ADD COLUMN color TEXT"
    };
    let mut migration = Migration::sql(2, "change", change);
    if convert {
        migration = migration.writes(|row| {
            row.rename_column("title", "name");
            Ok(())
        });
    }
    reopen(
        device,
        storage,
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
        vec![initial(), migration],
    )
    .await;
}

fn initial() -> Migration {
    Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL);",
    )
}

fn rename() -> Migration {
    Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name").writes(|row| {
        row.rename_column("title", "name");
        Ok(())
    })
}

async fn reopen(
    device: &mut Device,
    storage: Arc<MemoryStorage>,
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
) {
    device.db.close().await.unwrap();
    device.db = DatabaseBuilder::new(device.directory.clone())
        .synced_tables(tables)
        .migrations(migrations)
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .clock(device.clock.clone())
        .open()
        .await
        .unwrap();
    device.sync = DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.keys.clone(),
        device.identity.clone(),
    );
    device.log = StoreLogSync::new(
        storage,
        device.db.clone(),
        device.keys.clone(),
        device.identity.clone(),
        device.clock.clone(),
        device.ids.clone(),
        device.directory.clone(),
    );
}

async fn sync_all(devices: &mut [Device]) {
    for device in &mut *devices {
        device.sync.upload_writes().await.unwrap();
    }
    for device in &mut *devices {
        device.log.sync_store_log().await.unwrap();
    }
    for device in &mut *devices {
        device.sync.upload_writes().await.unwrap();
    }
    for device in &mut *devices {
        let report = device.sync.download_writes().await.unwrap();
        assert!(report.damaged_objects.is_empty(), "{report:?}");
        assert!(report.waiting.is_empty(), "{report:?}");
    }
}

async fn name(db: &Database) -> String {
    db.read(|sql| Ok(sql.query_row("SELECT name FROM notes WHERE id='42'", [], |r| r.get(0))?))
        .await
        .unwrap()
}

async fn seed(devices: &mut [Device]) {
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('42','Groceries','body')",
    )
    .await;
    devices[0].sync.upload_writes().await.unwrap();
    for device in devices.iter_mut().skip(1) {
        device.sync.download_writes().await.unwrap();
    }
}

#[tokio::test]
async fn adding_color_accepts_older_edits_without_raising_the_store() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    update(&mut devices[0], storage, false, false).await;
    devices[0].log.sync_store_log().await.unwrap();
    sql(&devices[0].db, "UPDATE notes SET color='blue'").await;
    devices[0].sync.upload_writes().await.unwrap();
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
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    devices[1].sync.upload_writes().await.unwrap();
    let report = devices[0].sync.download_writes().await.unwrap();
    assert!(report.damaged_objects.is_empty(), "{report:?}");
    assert_eq!(rows(&devices[0].db).await[0].1, "Shopping");
    assert!(devices[0]
        .db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .schema
        .is_empty());
}

#[tokio::test]
async fn rename_holds_uploads_then_converts_or_loses_offline_edits_everywhere() {
    for count in [2, 3] {
        for convert in [false, true] {
            let storage = storage();
            let mut devices = group(storage.clone(), count).await;
            seed(&mut devices).await;
            devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
            sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
            let original = devices[1].db.test_queued_writes().await.unwrap().remove(0);
            update(&mut devices[0], storage.clone(), true, convert).await;
            assert!(devices[0].sync.upload_writes().await.unwrap().is_empty());
            devices[0].log.sync_store_log().await.unwrap();
            let log = devices[0].db.store_log().await.unwrap();
            assert_eq!(log.replay.state.schema[&Audience::Store].number, 2);
            assert!(matches!(
                devices[1].log.sync_store_log().await,
                Err(SyncFailure::UpdateRequired)
            ));
            assert!(matches!(
                devices[1].sync.upload_writes().await,
                Err(SyncError::Stopped(SyncFailure::UpdateRequired))
            ));
            for device in devices.iter_mut().skip(1) {
                update(device, storage.clone(), true, convert).await;
                device.log.sync_store_log().await.unwrap();
            }
            let converted = devices[1].db.test_queued_writes().await.unwrap().remove(0);
            assert_eq!(converted.header.position, original.header.position);
            assert_eq!(converted.header.timestamp, original.header.timestamp);
            assert_eq!(converted.header.had_read, original.header.had_read);
            assert_eq!(
                converted.header.disposition,
                if convert {
                    WriteDisposition::Apply
                } else {
                    WriteDisposition::Lost(2)
                }
            );
            for device in &mut devices {
                device.sync.upload_writes().await.unwrap();
            }
            for device in &mut devices {
                let report = device.sync.download_writes().await.unwrap();
                assert!(report.damaged_objects.is_empty(), "{report:?}");
                assert!(report.waiting.is_empty(), "{report:?}");
                assert_eq!(
                    name(&device.db).await,
                    if convert { "Shopping" } else { "Groceries" }
                );
            }
            let losses = devices[0].db.lost_values().await.unwrap();
            assert_eq!(losses.is_empty(), convert);
            for device in devices.iter().skip(1) {
                assert_eq!(device.db.lost_values().await.unwrap(), losses);
            }
        }
    }
}

#[tokio::test]
async fn an_old_write_uploaded_without_being_read_by_the_change_is_lost_everywhere() {
    for tried in [false, true] {
        let storage = storage();
        let mut devices = group(storage.clone(), 3).await;
        seed(&mut devices).await;
        sql(&devices[1].db, "UPDATE notes SET title='Unseen'").await;
        if tried {
            storage
                .set_faults(Faults {
                    fail_next: 1,
                    ..Faults::none()
                })
                .await;
            assert!(devices[1].sync.upload_writes().await.is_err());
        } else {
            devices[1].sync.upload_writes().await.unwrap();
            devices[2].sync.download_writes().await.unwrap();
            assert_eq!(rows(&devices[2].db).await[0].1, "Unseen");
        }
        update(&mut devices[0], storage.clone(), true, true).await;
        devices[0].log.sync_store_log().await.unwrap();
        for device in devices.iter_mut().skip(1) {
            assert!(matches!(
                device.log.sync_store_log().await,
                Err(SyncFailure::UpdateRequired)
            ));
            update(device, storage.clone(), true, true).await;
        }
        sync_all(&mut devices).await;
        let losses = devices[0].db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        for device in &devices {
            assert_eq!(name(&device.db).await, "Groceries");
            assert_eq!(device.db.lost_values().await.unwrap(), losses);
        }
    }
}

#[tokio::test]
async fn concurrent_raises_choose_the_earlier_timestamp_even_when_it_arrives_last() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    seed(&mut devices).await;
    devices[0].clock.set(UNIX_EPOCH + Duration::from_secs(3));
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    for device in devices.iter_mut().take(2) {
        update(device, storage.clone(), true, true).await;
    }
    devices[0].log.sync_store_log().await.unwrap();
    let first =
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    let path = crate::store_log_object::path(first.entry);
    let bytes = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let snapshot_path = coven_storage::ObjectPath::snapshot(
        first.snapshot.audience.clone(),
        first.snapshot.device,
        first.snapshot.number.try_into().unwrap(),
    );
    let snapshot = storage.read(&snapshot_path).await.unwrap();
    storage.delete(&snapshot_path).await.unwrap();
    devices[1].log.sync_store_log().await.unwrap();
    let winner =
        devices[1].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    assert_ne!(first.snapshot, winner.snapshot);
    storage.create(&snapshot_path, &snapshot).await.unwrap();
    storage.create(&path, &bytes).await.unwrap();
    devices[0].sync.upload_writes().await.unwrap();
    devices[1].sync.upload_writes().await.unwrap();
    assert!(matches!(
        devices[2].log.sync_store_log().await,
        Err(SyncFailure::UpdateRequired)
    ));
    update(&mut devices[2], storage.clone(), true, true).await;
    sync_all(&mut devices).await;
    for device in &devices {
        let log = device.db.store_log().await.unwrap();
        assert_eq!(log.replay.state.schema[&Audience::Store], winner);
        assert!(matches!(
            log.replay.entries[&first.entry],
            coven_database::EntryOutcome::Dropped(_)
        ));
        assert_eq!(name(&device.db).await, "Groceries");
    }
}

#[tokio::test]
async fn a_raise_resumes_after_reopening_at_every_publication_step() {
    for crash in 0..=7 {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        seed(&mut devices).await;
        let device = &mut devices[0];
        update(device, storage.clone(), true, true).await;
        device.log.schedule_version_changes().await.unwrap();
        let id = device.db.operations().await.unwrap()[0].id;
        for _ in 0..crash {
            let record = device
                .db
                .operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == id)
                .unwrap();
            device
                .log
                .operation_step(&record, crate::operation_data::Data::read(&record).unwrap())
                .await
                .unwrap();
        }
        let fixed = device.db.local_store_log().await.unwrap().upload;
        update(device, storage.clone(), true, true).await;
        device.log.sync_store_log().await.unwrap();
        let raised =
            device.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
        assert_eq!(raised.number, 2);
        if let Some(fixed) = fixed {
            assert_eq!(
                storage
                    .read(&crate::store_log_object::path(fixed.entry.position))
                    .await
                    .unwrap(),
                fixed.sealed.bytes
            );
        }
        assert!(device.db.operations().await.unwrap().is_empty());
        assert!(matches!(
            devices[1].log.sync_store_log().await,
            Err(SyncFailure::UpdateRequired)
        ));
        update(&mut devices[1], storage.clone(), true, true).await;
        sync_all(&mut devices).await;
        assert_eq!(name(&devices[0].db).await, name(&devices[1].db).await);
    }
}

#[tokio::test]
async fn a_circle_is_raised_by_its_first_updating_member_after_the_store() {
    use coven_foundation::id_source::CircleId;
    let storage = storage();
    let mut devices = super::circles::household(storage.clone()).await;
    // Both the store's first updater and the circle's first updater are
    // ordinary members; migration publication does not require an admin.
    for index in [2, 0] {
        let member = devices[index]
            .identity
            .unlock()
            .unwrap()
            .unwrap()
            .member_id();
        devices[0]
            .log
            .make_and_upload_entry(StoreChange::ChangeRole {
                member,
                role: coven_format::store_log::MemberRole::Member,
            })
            .await
            .unwrap();
    }
    for device in &mut devices {
        device.log.sync_store_log().await.unwrap();
    }
    let circle = Audience::Circle(CircleId(Uuid::from_u128(10)));
    sql(&devices[0].db,"INSERT INTO notes VALUES('42','Groceries','body'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
    devices[0].sync.upload_writes().await.unwrap();
    for device in devices.iter_mut().skip(1) {
        device.sync.download_writes().await.unwrap();
    }
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    sql(&devices[1].db, "UPDATE pins SET title='edited offline'").await;
    for index in [2, 0, 1] {
        if index != 2 {
            assert!(matches!(
                devices[index].log.sync_store_log().await,
                Err(SyncFailure::UpdateRequired)
            ));
        }
        reopen(&mut devices[index], storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey), SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience")],
            vec![initial(), Migration::sql(2,"circles","CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT NOT NULL);"),
                Migration::sql(3,"names","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE pins RENAME COLUMN title TO name").writes(|row| {row.rename_column("title", "name"); Ok(())})]).await;
        devices[index].log.sync_store_log().await.unwrap();
        devices[index].sync.upload_writes().await.unwrap();
        let log = devices[index].db.store_log().await.unwrap();
        assert_eq!(log.replay.state.schema[&Audience::Store].number, 3);
        assert_eq!(log.replay.state.schema.contains_key(&circle), index != 2);
    }
    sync_all(&mut devices).await;
    for (index, device) in devices.iter().enumerate() {
        let pins: Vec<String> = device
            .db
            .read(|sql| Ok(sql.query("SELECT name FROM pins", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(
            pins,
            if index == 2 {
                Vec::<String>::new()
            } else {
                vec!["edited offline".into()]
            }
        );
        assert_eq!(name(&device.db).await, "Groceries");
        let log = device.db.store_log().await.unwrap();
        assert_eq!(
            log.replay.state.schema[&circle].snapshot.device,
            DeviceId(1)
        );
        assert_eq!(
            log.entries
                .iter()
                .filter(|e| matches!(e.entry.change, StoreChange::RaiseSchema { .. }))
                .count(),
            2
        );
    }
}

#[tokio::test]
async fn a_batch_raises_its_final_version_and_accepts_an_already_converted_waiting_write() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    // Ben ran the rename in a previous app update but has not published it.
    update(&mut devices[1], storage.clone(), true, true).await;
    for index in [0, 1] {
        reopen(
            &mut devices[index],
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                initial(),
                rename(),
                Migration::sql(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
            ],
        )
        .await;
        devices[index].log.sync_store_log().await.unwrap();
        assert_eq!(
            devices[index]
                .db
                .store_log()
                .await
                .unwrap()
                .replay
                .state
                .schema[&Audience::Store]
                .number,
            3
        );
        devices[index].sync.upload_writes().await.unwrap();
    }
    sync_all(&mut devices).await;
    for device in &devices {
        assert_eq!(name(&device.db).await, "Shopping");
        assert!(device.db.lost_values().await.unwrap().is_empty());
        assert_eq!(
            device
                .db
                .store_log()
                .await
                .unwrap()
                .entries
                .iter()
                .filter(|e| matches!(e.entry.change, StoreChange::RaiseSchema { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn concurrent_raises_to_different_versions_are_both_kept() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    seed(&mut devices).await;
    update(&mut devices[0], storage.clone(), true, true).await;
    devices[0].log.sync_store_log().await.unwrap();
    devices[0].sync.upload_writes().await.unwrap();
    let first =
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    // Ben's migration and snapshot have not read Ana's raise.
    reopen(
        &mut devices[1],
        storage.clone(),
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
        vec![
            initial(),
            rename(),
            Migration::sql(3, "bodies", "UPDATE notes SET body='version three'").writes(|_| Ok(())),
        ],
    )
    .await;
    devices[1].log.schedule_version_changes().await.unwrap();
    let id = devices[1].db.operations().await.unwrap()[0].id;
    for _ in 0..3 {
        let record = devices[1]
            .db
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
        devices[1]
            .log
            .operation_step(&record, crate::operation_data::Data::read(&record).unwrap())
            .await
            .unwrap();
    }
    devices[1].log.sync_store_log().await.unwrap();
    devices[1].sync.upload_writes().await.unwrap();
    for index in [0, 2] {
        reopen(
            &mut devices[index],
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                initial(),
                rename(),
                Migration::sql(3, "bodies", "UPDATE notes SET body='version three'")
                    .writes(|_| Ok(())),
            ],
        )
        .await;
    }
    sync_all(&mut devices).await;
    for device in &devices {
        let log = device.db.store_log().await.unwrap();
        assert_eq!(log.replay.state.schema[&Audience::Store].number, 3);
        assert_eq!(
            log.replay.entries[&first.entry],
            coven_database::EntryOutcome::Kept
        );
        let body: String = device
            .db
            .read(|sql| Ok(sql.query_row("SELECT body FROM notes", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(body, "version three");
    }
}

#[tokio::test]
async fn a_migrated_device_waits_to_judge_old_writes_until_its_raise_is_applied() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    update(&mut devices[0], storage, true, true).await;
    sql(&devices[1].db, "UPDATE notes SET title='Unseen'").await;
    devices[1].sync.upload_writes().await.unwrap();
    let report = devices[0].sync.download_writes().await.unwrap();
    assert!(report.damaged_objects.is_empty(), "{report:?}");
    assert_eq!(report.waiting.len(), 1);
    assert_eq!(name(&devices[0].db).await, "Groceries");
    devices[0].log.sync_store_log().await.unwrap();
    assert!(devices[0]
        .sync
        .download_writes()
        .await
        .unwrap()
        .waiting
        .is_empty());
    assert_eq!(devices[0].db.lost_values().await.unwrap().len(), 1);
}

use crate::{
    operation_data::Data,
    snapshot_data::{RaisedVersion, SnapshotJob, SnapshotTask, SnapshotTrigger},
};

#[tokio::test]
async fn the_current_format_uses_raise_publication_without_losing_waiting_or_late_writes() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    seed(&mut devices).await;
    sql(&devices[1].db, "UPDATE notes SET title='Offline'").await;
    sql(&devices[2].db, "UPDATE notes SET body='Late'").await;
    devices[2].sync.upload_writes().await.unwrap();
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: DeviceId(1),
            trigger: SnapshotTrigger::Raise {
                version: RaisedVersion::Format(coven_format::FORMAT_VERSION),
                entry: None,
            },
            session: None,
        },
        temporary: Vec::new(),
    });
    devices[0]
        .db
        .start_operation(data.new_operation("coven").unwrap())
        .await
        .unwrap();
    devices[0].log.sync_store_log().await.unwrap();
    sync_all(&mut devices).await;
    for device in &devices {
        let log = device.db.store_log().await.unwrap();
        assert_eq!(
            log.replay.state.format[&Audience::Store].number,
            coven_format::FORMAT_VERSION
        );
        assert_eq!(
            rows(&device.db).await[0],
            ("42".into(), "Offline".into(), "Late".into())
        );
        assert!(device.db.lost_values().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_newer_snapshot_format_requires_an_update_and_can_resume_after_replacement() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    let id = devices[0]
        .log
        .write_snapshot(Audience::Store)
        .await
        .unwrap();
    let path = ObjectPath::snapshot(id.audience, id.device, id.number.try_into().unwrap());
    let original = storage.read(&path).await.unwrap();
    let mut future = original.clone();
    future[1..3].copy_from_slice(&(coven_format::FORMAT_VERSION + 1).to_be_bytes());
    storage.delete(&path).await.unwrap();
    storage.create(&path, &future).await.unwrap();
    assert!(matches!(
        devices[1].log.reload_from_snapshots().await,
        Err(SyncError::Stopped(SyncFailure::UpdateRequired))
    ));
    storage.delete(&path).await.unwrap();
    storage.create(&path, &original).await.unwrap();
    devices[1].log.sync_store_log().await.unwrap();
    assert_eq!(rows(&devices[0].db).await, rows(&devices[1].db).await);
    assert!(devices[1].db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn publication_intent_can_be_scheduled_while_member_keys_are_unavailable() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    update(&mut devices[0], storage, true, true).await;
    devices[0].identity.forget().unwrap();
    devices[0].log.schedule_version_changes().await.unwrap();
    assert_eq!(devices[0].db.operations().await.unwrap().len(), 1);
    devices[0].identity.persist(&member()).unwrap();
    devices[0].log.sync_store_log().await.unwrap();
    assert_eq!(
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].number,
        2
    );
}

#[tokio::test]
async fn a_migration_waits_for_an_operation_that_already_reserved_the_entry_number() {
    for journaled in [false, true] {
        let storage = storage();
        let mut device = group(storage.clone(), 1).await.pop().unwrap();
        if journaled {
            let crate::operations::Begun::Operation(id) = device
                .log
                .begin_operation_call(crate::operations::Command::CreateCircle("waiting".into()))
                .await
                .unwrap()
            else {
                panic!("operation")
            };
            let record = device
                .db
                .operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == id)
                .unwrap();
            device
                .log
                .operation_step(&record, Data::read(&record).unwrap())
                .await
                .unwrap();
        } else {
            storage
                .set_faults(Faults {
                    fail_next: 1,
                    ..Faults::none()
                })
                .await;
            assert!(device
                .log
                .make_and_upload_entry(StoreChange::CreateCircle {
                    circle: coven_foundation::id_source::CircleId(Uuid::from_u128(77)),
                    name: "waiting".into(),
                    key: KeyId(Uuid::from_u128(77)),
                })
                .await
                .is_err());
        }
        assert!(device.db.local_store_log().await.unwrap().upload.is_some());
        update(&mut device, storage.clone(), true, true).await;
        let files = crate::Files::new(
            coven_database::FileDatabase::new(device.db.clone()),
            device.directory.clone(),
            Some(storage),
            device.clock.clone(),
            device.ids.clone(),
        );
        let operations = crate::Operations::new(device.log, files.clone());
        operations.get_members().await.unwrap();
        assert!(operations
            .report()
            .await
            .unwrap()
            .blocked_operations
            .is_empty());
        assert_eq!(operations.circles().await.unwrap()[0].name, "waiting");
        assert_eq!(
            device.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].number,
            2
        );
        operations.close().await.unwrap();
        files.close().await;
    }
}

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

pub(crate) fn initial() -> Migration {
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
    device.reopen(storage, tables, migrations).await;
}

/// A migrated device sends its old prefix and empty marker, confirmed before
/// its raise snapshot, then publishes the raise (§17.1, §18.1).
async fn publish_raise(device: &mut Device) {
    if !device.send.can_send() {
        device.catch_up().await;
    }
    device.writes.upload_writes().await.unwrap();
    device.sync.sync_store_log().await.unwrap();
}

async fn sync_all(devices: &mut [Device]) {
    for device in &mut *devices {
        if !device.send.can_send() {
            device.catch_up().await;
        }
        device.writes.upload_writes().await.unwrap();
    }
    for device in &mut *devices {
        device.sync.sync_store_log().await.unwrap();
    }
    for device in &mut *devices {
        device.writes.upload_writes().await.unwrap();
    }
    for device in &mut *devices {
        device.writes.download_writes().await.unwrap();
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
    devices[0].writes.upload_writes().await.unwrap();
    for device in devices.iter_mut().skip(1) {
        device.writes.download_writes().await.unwrap();
    }
}

#[tokio::test]
async fn adding_color_accepts_older_edits_without_raising_the_store() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    update(&mut devices[0], storage, false, false).await;
    devices[0].sync.sync_store_log().await.unwrap();
    sql(&devices[0].db, "UPDATE notes SET color='blue'").await;
    devices[0].writes.upload_writes().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    devices[1].writes.upload_writes().await.unwrap();
    devices[0].writes.download_writes().await.unwrap();
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
            publish_raise(&mut devices[0]).await;
            let log = devices[0].db.store_log().await.unwrap();
            assert_eq!(log.replay.state.schema[&Audience::Store].number, 2);
            assert!(matches!(
                devices[1].sync.sync_store_log().await,
                Err(SyncError::UpdateRequired(_))
            ));
            assert!(matches!(
                devices[1].writes.upload_writes().await,
                Err(SyncError::UpdateRequired(_))
            ));
            for device in devices.iter_mut().skip(1) {
                update(device, storage.clone(), true, convert).await;
                device.sync.sync_store_log().await.unwrap();
            }
            let waiting = devices[1].db.test_queued_writes().await.unwrap().remove(0);
            assert_eq!(waiting, original);
            for device in &mut devices {
                device.writes.upload_writes().await.unwrap();
            }
            for device in &mut devices {
                device.writes.download_writes().await.unwrap();
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
async fn an_old_write_converts_on_receipt_independently_of_attempt_status() {
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
            assert!(devices[1].writes.upload_writes().await.is_err());
        } else {
            devices[1].writes.upload_writes().await.unwrap();
            devices[2].writes.download_writes().await.unwrap();
            assert_eq!(rows(&devices[2].db).await[0].1, "Unseen");
        }
        update(&mut devices[0], storage.clone(), true, true).await;
        publish_raise(&mut devices[0]).await;
        for device in devices.iter_mut().skip(1) {
            assert!(matches!(
                device.sync.sync_store_log().await,
                Err(SyncError::UpdateRequired(_))
            ));
            update(device, storage.clone(), true, true).await;
        }
        sync_all(&mut devices).await;
        let losses = devices[0].db.lost_values().await.unwrap();
        assert!(losses.is_empty());
        for device in &devices {
            assert_eq!(name(&device.db).await, "Unseen");
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
    publish_raise(&mut devices[0]).await;
    let first =
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    let path = crate::store_log_object::path(STORE_ID, first.entry);
    let bytes = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let snapshot_path = coven_storage::ObjectPath::snapshot(
        STORE_ID,
        first.snapshot.audience.clone(),
        first.snapshot.device,
        first.snapshot.number.try_into().unwrap(),
    );
    let snapshot = storage.read(&snapshot_path).await.unwrap();
    storage.delete(&snapshot_path).await.unwrap();
    publish_raise(&mut devices[1]).await;
    let winner =
        devices[1].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    assert_ne!(first.snapshot, winner.snapshot);
    storage.create(&snapshot_path, &snapshot).await.unwrap();
    storage.create(&path, &bytes).await.unwrap();
    devices[0].writes.upload_writes().await.unwrap();
    devices[1].writes.upload_writes().await.unwrap();
    assert!(matches!(
        devices[2].sync.sync_store_log().await,
        Err(SyncError::UpdateRequired(_))
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
    'crashes: for crash in 0.. {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        seed(&mut devices).await;
        let device = &mut devices[0];
        update(device, storage.clone(), true, true).await;
        // The captured prefix, through the marker, is confirmed first.
        device.catch_up().await;
        device.writes.upload_writes().await.unwrap();
        device.sync.schedule_version_changes().await.unwrap();
        let id = device.db.operations().await.unwrap()[0].id;
        for _ in 0..crash {
            match device.step(id).await.unwrap() {
                crate::operations::Progress::Finished(_) => break 'crashes,
                crate::operations::Progress::Advanced => (),
                _ => panic!("unexpected schema publication progress"),
            }
        }
        let local = device.db.local_store_log().await.unwrap();
        let fixed = local.upload.clone();
        let expected = fixed.as_ref().map(|upload| {
            crate::store_log_object::seal_upload(
                STORE_ID,
                &local.log,
                upload,
                device.custody.read().unwrap().as_ref(),
                &device.identity.read().unwrap().unwrap(),
            )
            .unwrap()
        });
        update(device, storage.clone(), true, true).await;
        device.sync.sync_store_log().await.unwrap();
        let raised =
            device.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
        assert_eq!(raised.number, 2);
        if let Some(fixed) = fixed {
            assert_eq!(
                storage
                    .read(&crate::store_log_object::path(
                        STORE_ID,
                        fixed.entry.position
                    ))
                    .await
                    .unwrap(),
                expected.unwrap()
            );
        }
        assert!(device.db.operations().await.unwrap().is_empty());
        assert!(matches!(
            devices[1].sync.sync_store_log().await,
            Err(SyncError::UpdateRequired(_))
        ));
        update(&mut devices[1], storage.clone(), true, true).await;
        sync_all(&mut devices).await;
        assert_eq!(name(&devices[0].db).await, name(&devices[1].db).await);
    }
}

#[tokio::test]
async fn a_losing_migrations_derived_value_disappears_without_a_loss() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    devices[0].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(3));
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    for device in &mut devices {
        reopen(
            device,
            storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                initial(),
                Migration::sql(2, "copy title", "UPDATE notes SET body=title").writes(|_| Ok(())),
            ],
        )
        .await;
    }
    let body = |db: Database| async move {
        db.read(|sql| Ok(sql.query_row("SELECT body FROM notes", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap()
    };
    assert_eq!(body(devices[1].db.clone()).await, "Shopping");
    let marker = devices[1]
        .db
        .test_queued_writes()
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.header.disposition == WriteDisposition::Migration)
        .unwrap();
    assert!(marker.parts.is_empty());
    // Publish Ben's snapshot first while Ana still sees only their common log.
    publish_raise(&mut devices[1]).await;
    let loser =
        devices[1].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    let path = crate::store_log_object::path(STORE_ID, loser.entry);
    let entry = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let snapshot_path = ObjectPath::snapshot(
        STORE_ID,
        loser.snapshot.audience.clone(),
        loser.snapshot.device,
        loser.snapshot.number.try_into().unwrap(),
    );
    let snapshot = storage.read(&snapshot_path).await.unwrap();
    storage.delete(&snapshot_path).await.unwrap();
    publish_raise(&mut devices[0]).await;
    storage.create(&snapshot_path, &snapshot).await.unwrap();
    storage.create(&path, &entry).await.unwrap();
    sync_all(&mut devices).await;
    for device in &devices {
        assert!(matches!(
            device.db.store_log().await.unwrap().replay.entries[&loser.entry],
            coven_database::EntryOutcome::Dropped(_)
        ));
        assert_eq!(rows(&device.db).await[0].1, "Shopping");
        assert_eq!(body(device.db.clone()).await, "Groceries");
        assert!(device.db.lost_values().await.unwrap().is_empty());
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
        let member = devices[index].identity.read().unwrap().unwrap().member();
        devices[0]
            .sync
            .make_and_upload_entry(StoreChange::ChangeRole {
                member,
                role: coven_format::store_log::MemberRole::Member,
            })
            .await
            .unwrap();
    }
    for device in &mut devices {
        device.sync.sync_store_log().await.unwrap();
    }
    let circle = Audience::Circle(CircleId(Uuid::from_u128(10)));
    sql(&devices[0].db,"INSERT INTO notes VALUES('42','Groceries','body'); INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','private')").await;
    devices[0].writes.upload_writes().await.unwrap();
    for device in devices.iter_mut().skip(1) {
        device.writes.download_writes().await.unwrap();
    }
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    sql(&devices[1].db, "UPDATE pins SET title='edited offline'").await;
    for index in [2, 0, 1] {
        if index != 2 {
            assert!(matches!(
                devices[index].sync.sync_store_log().await,
                Err(SyncError::UpdateRequired(_))
            ));
        }
        reopen(&mut devices[index], storage.clone(),
            vec![SyncedTable::new("notes", RowIdentity::SharedKey), SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience")],
            vec![initial(), Migration::sql(2,"circles","CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT NOT NULL);"),
                Migration::sql(3,"names","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE pins RENAME COLUMN title TO name").writes(|row| {row.rename_column("title", "name"); Ok(())})]).await;
        publish_raise(&mut devices[index]).await;
        devices[index].writes.upload_writes().await.unwrap();
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
async fn a_batch_raises_its_final_version_and_converts_an_earlier_migrations_waiting_write() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    // Ben ran the rename in a previous app update but has not published it;
    // his edit stays in its original schema in the queue.
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
        publish_raise(&mut devices[index]).await;
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
        devices[index].writes.upload_writes().await.unwrap();
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
    devices[1].catch_up().await;
    devices[1].writes.upload_writes().await.unwrap();
    devices[1].sync.schedule_version_changes().await.unwrap();
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
            .sync
            .operation_step(&record, crate::operation_data::Data::read(&record).unwrap())
            .await
            .unwrap();
    }
    publish_raise(&mut devices[0]).await;
    let first =
        devices[0].db.store_log().await.unwrap().replay.state.schema[&Audience::Store].clone();
    devices[1].sync.sync_store_log().await.unwrap();
    devices[1].writes.upload_writes().await.unwrap();
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
async fn reference_checks_do_not_reject_writes_from_before_a_table_rename() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    sql(&devices[1].db, "UPDATE notes SET title='Unseen'").await;
    devices[1].writes.upload_writes().await.unwrap();
    reopen(
        &mut devices[0],
        storage,
        vec![SyncedTable::new("entries", RowIdentity::SharedKey)],
        vec![
            initial(),
            Migration::sql(2, "rename", "ALTER TABLE notes RENAME TO entries"),
        ],
    )
    .await;
    devices[0].writes.download_writes().await.unwrap();
    assert!(devices[0].db.lost_values().await.unwrap().is_empty());
    publish_raise(&mut devices[0]).await;
    devices[0].writes.download_writes().await.unwrap();
    assert_eq!(devices[0].db.lost_values().await.unwrap().len(), 1);
    let title: String = devices[0]
        .db
        .read(|sql| Ok(sql.query_row("SELECT title FROM entries WHERE id='42'", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(title, "Groceries");
}

use crate::operation_data::Data;

#[path = "snapshot_seal_tests.rs"]
mod raise_publication;

#[tokio::test]
async fn a_newer_snapshot_format_stays_pending_until_the_required_update() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    let id = devices[0]
        .sync
        .write_snapshot(Audience::Store)
        .await
        .unwrap();
    let path = ObjectPath::snapshot(
        STORE_ID,
        id.audience.clone(),
        id.device,
        id.number.try_into().unwrap(),
    );
    let original = storage.read(&path).await.unwrap();
    let mut future = original.clone();
    future[1..3].copy_from_slice(&(coven_format::FORMAT_VERSION + 1).to_be_bytes());
    storage.delete(&path).await.unwrap();
    storage.create(&path, &future).await.unwrap();
    assert!(matches!(
        devices[1].sync.reload_from_snapshots().await,
        Err(SyncError::UpdateRequired(_))
    ));
    storage.delete(&path).await.unwrap();
    storage.create(&path, &original).await.unwrap();
    devices[1].sync.sync_store_log().await.unwrap();
    assert_eq!(rows(&devices[0].db).await, rows(&devices[1].db).await);
    let pending = devices[1]
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap();
    // The snapshot's own record and the reload waiting on it both name the update.
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|record| matches!(
        record.reason,
        crate::PendingReason::UpdateRequired(crate::RequiredUpdate::CovenFormat { .. })
    )));
    assert!(pending
        .iter()
        .any(|record| record.subject == crate::PendingSubject::Snapshot(id.clone())));
    assert_eq!(devices[1].db.operations().await.unwrap().len(), 1);
}

#[tokio::test]
async fn publication_intent_can_be_scheduled_while_member_keys_are_unavailable() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    update(&mut devices[0], storage, true, true).await;
    devices[0].identity.forget().unwrap();
    devices[0].sync.schedule_version_changes().await.unwrap();
    assert_eq!(devices[0].db.operations().await.unwrap().len(), 1);
    devices[0].identity.persist(&member()).unwrap();
    publish_raise(&mut devices[0]).await;
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
                .sync
                .begin_operation_call(crate::operations::Command::CreateCircle("waiting".into()))
                .await
                .unwrap()
            else {
                panic!("operation")
            };
            let record = device.operation(id).await;
            device
                .sync
                .operation_step(&record, Data::read(&record).unwrap())
                .await
                .unwrap();
        } else {
            let (started, waiting) = tokio::sync::oneshot::channel();
            let (resume, held) = tokio::sync::oneshot::channel();
            storage
                .hold_next_creation(ObjectPrefix::keys(STORE_ID), started, held)
                .await;
            let publication = device
                .sync
                .make_and_upload_entry(StoreChange::CreateCircle {
                    circle: coven_foundation::id_source::CircleId(Uuid::from_u128(77)),
                    name: "waiting".into(),
                    key: KeyId(Uuid::from_u128(77)),
                    key_hash: device.sync.test_key(
                        coven_merge::Audience::Circle(coven_foundation::id_source::CircleId(
                            Uuid::from_u128(77),
                        )),
                        KeyId(Uuid::from_u128(77)),
                    ),
                });
            let (result, ()) = tokio::join!(publication, async {
                waiting.await.unwrap();
                drop(resume);
            });
            assert!(matches!(result, Err(SyncError::Storage(_))));
        }
        assert!(device.db.local_store_log().await.unwrap().upload.is_some());
        update(&mut device, storage.clone(), true, true).await;
        let files = crate::Files::new(
            coven_database::FileDatabase::new(device.db.clone()),
            device.directory.clone(),
            Some(storage),
            device.send.clone(),
            device.waits.clone(),
            device.ids.clone(),
            crate::TransferLimits::default(),
        );
        let operations = crate::Operations::new(
            device.sync,
            files.clone(),
            device.writes,
            device.clock.clone(),
        );
        operations.barrier().await.unwrap();
        operations.sync().await.unwrap();
        assert!(device
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(device.db.circles().await.unwrap()[0].name, "waiting");
        assert_eq!(
            device.db.store_log().await.unwrap().replay.state.schema[&Audience::Store].number,
            2
        );
        operations.close().await.unwrap();
        files.close().await;
    }
}

mod recovery {
    use super::*;

    fn recovery_migrations(convert: bool) -> Vec<Migration> {
        let mut change =
            Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name");
        if convert {
            change = change.writes(|row| {
                row.rename_column("title", "name");
                Ok(())
            });
        }
        vec![
            initial(),
            change,
            Migration::sql(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
        ]
    }

    #[tokio::test]
    async fn reloading_in_place_converts_old_waiting_writes_and_preserves_attempted_bytes() {
        {
            for convert in [false, true] {
                for attempted in [false, true] {
                    let storage = storage();
                    let mut devices = group(storage.clone(), 3).await;
                    seed(&mut devices).await;
                    // Damage the fingerprint before the device authors offline work.
                    devices[1]
                        .db
                        .test_damage_fingerprint(Audience::Store)
                        .await
                        .unwrap();
                    for device in &mut devices {
                        device.writes.post_positions().await.unwrap();
                    }
                    assert_ne!(
                        devices[1]
                            .writes
                            .current_positions()
                            .await
                            .unwrap()
                            .unwrap()
                            .fingerprints,
                        devices[0]
                            .writes
                            .current_positions()
                            .await
                            .unwrap()
                            .unwrap()
                            .fingerprints
                    );
                    devices[1].clock.set(UNIX_EPOCH + Duration::from_secs(2));
                    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
                    let original = devices[1].db.test_queued_writes().await.unwrap().remove(0);
                    if attempted {
                        storage
                            .set_faults(Faults {
                                fail_next: 1,
                                ..Faults::none()
                            })
                            .await;
                        assert!(devices[1].writes.upload_writes().await.is_err());
                    }
                    let sealed = if attempted {
                        Some(writes::resealed(&devices[1]).await.1)
                    } else {
                        None
                    };
                    reopen(
                        &mut devices[0],
                        storage.clone(),
                        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                        recovery_migrations(convert),
                    )
                    .await;
                    publish_raise(&mut devices[0]).await;
                    let device = &mut devices[1];
                    reopen(
                        device,
                        storage.clone(),
                        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                        recovery_migrations(convert),
                    )
                    .await;
                    let queue = device.db.test_queued_writes().await.unwrap();
                    let waiting = queue
                        .iter()
                        .find(|w| w.header.position == original.header.position)
                        .unwrap();
                    assert_eq!(waiting, &original);
                    if attempted {
                        assert_eq!(writes::resealed(device).await.1, sealed.unwrap());
                    }
                    device.sync.sync_store_log().await.unwrap();
                    device.sync.reload_from_snapshots().await.unwrap();
                    reopen(
                        &mut devices[2],
                        storage.clone(),
                        vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                        recovery_migrations(convert),
                    )
                    .await;
                    sync_all(&mut devices).await;
                    let losses = devices[0].db.lost_values().await.unwrap();
                    assert_eq!(losses.is_empty(), convert);
                    for device in &mut devices {
                        assert_eq!(
                            name(&device.db).await,
                            if convert { "Shopping" } else { "Groceries" }
                        );
                        assert_eq!(device.db.lost_values().await.unwrap(), losses);
                        device.writes.post_positions().await.unwrap();
                    }
                    let expected = devices[0]
                        .writes
                        .current_positions()
                        .await
                        .unwrap()
                        .unwrap()
                        .fingerprints;
                    for device in &mut devices {
                        assert_eq!(
                            device
                                .writes
                                .current_positions()
                                .await
                                .unwrap()
                                .unwrap()
                                .fingerprints,
                            expected
                        );
                        device.db.close().await.unwrap();
                    }
                }
            }
        }
    }
}

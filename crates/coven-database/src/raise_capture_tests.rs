use crate::snapshot_write::tests::{decode, prefix, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use crate::{Database, Migration};
use coven_format::admin::SnapshotId;
use coven_format::snapshot::SnapshotRecord;
use coven_format::value::Value;
use coven_format::write::WriteDisposition;
use coven_merge::{Audience, WriteId};
use std::sync::{Arc, Mutex};

/// Make this database's own device the creator of a store, so a breaking
/// migration has audiences to capture.
pub(crate) async fn connect(db: &Database) {
    let device = db.local_device().unwrap();
    TestStore::register(db, device, 1);
    let mut state = db.store_state().await.unwrap();
    state.store = Some(crate::StoreIdentity {
        id: crate::tests::store_id(),
        name: crate::tests::STORE_NAME.into(),
    });
    db.admin_fixture(
        coven_format::admin::StoreChange::AddDevice {
            name: "Local".into(),
            replaces: None,
        },
        state,
        None,
    )
    .await
    .unwrap();
}

fn initial() -> Migration {
    Migration::addition(1, "notes", NOTES)
}
fn index(version: u32, old_writes: crate::OldWrites) -> Migration {
    Migration::breaking(
        version,
        "index",
        if version == 2 {
            "CREATE INDEX titles ON notes(title)"
        } else {
            "CREATE INDEX bodies ON notes(body)"
        },
        old_writes,
    )
}

fn queue(db: &Database) -> Vec<(u64, Vec<u8>, Option<Vec<u8>>)> {
    db.inspect_writer(|db| {
        db.query(
            "SELECT number,record,sealing_keys FROM _coven_uploads ORDER BY number",
            [],
            |r| {
                Ok((
                    crate::write_encoding::counter(r.get(0)?),
                    r.get(1)?,
                    r.get(2)?,
                ))
            },
        )
        .unwrap()
    })
}

async fn raise_frames(db: &Database, number: u64, version: u32) -> SnapshotFrames {
    let through = db.raise_captures().await.unwrap()[0].through.number;
    crate::tests::publish_through(db, through).await.unwrap();
    db.prepare_raise_capture(Audience::Store, version)
        .await
        .unwrap();
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let (output, start) = (frames.clone(), metadata.clone());
    let device = db.local_device().unwrap();
    db.write_raise_snapshot(
        SnapshotId {
            audience: Audience::Store,
            device,
            number,
        },
        version,
        move |header| {
            *start.lock().unwrap() = Some(prefix(header));
            Ok::<_, std::convert::Infallible>(())
        },
        move |frame| {
            output.lock().unwrap().push(frame);
            Ok(())
        },
    )
    .await
    .unwrap();
    let prefix = metadata.lock().unwrap().take().unwrap();
    let frames = std::mem::take(&mut *frames.lock().unwrap());
    SnapshotFrames { prefix, frames }
}

#[tokio::test]
async fn attempted_and_untried_records_survive_successive_migrations() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('9','attempted','body')")
        .await
        .unwrap();
    crate::upload::tests::attempt(&db, 9).await.unwrap();
    sql(&db, "UPDATE notes SET title='untried'").await.unwrap();
    let original = queue(&db);
    assert!(original[0].2.is_some() && original[1].2.is_none());
    db.close().await.unwrap();
    let mut migrations = vec![initial(), index(2, crate::OldWrites::Drop)];
    for version in [2, 3] {
        if version == 3 {
            migrations.push(index(3, crate::OldWrites::Drop));
        }
        let db = store
            .builder(notes(), migrations.clone())
            .open()
            .await
            .unwrap();
        // Every original record keeps its bytes and fixed keys; each
        // migration appends only its own empty marker (§17.1).
        let queued = queue(&db);
        assert_eq!(queued[..2], original[..]);
        let markers = records(&db)[2..]
            .iter()
            .map(|r| {
                (
                    r.header.position.number,
                    r.header.disposition,
                    r.parts.len(),
                )
            })
            .collect::<Vec<_>>();
        let expected: Vec<_> = (3..=version as u64 + 1)
            .map(|number| (number, WriteDisposition::Migration, 0))
            .collect();
        assert_eq!(markers, expected);
        // The newest migration's capture replaces the unpublished one.
        let captures = db.raise_captures().await.unwrap();
        assert_eq!(captures.len(), 1);
        assert_eq!(captures[0].version, version);
        assert_eq!(captures[0].through.number, version as u64 + 1);
        assert_eq!(
            db.raise_captures()
                .await
                .unwrap()
                .first()
                .map_or(0, |c| c.version),
            version
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn the_capture_stops_at_the_marker_and_later_app_writes_stay_outside() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('n','old title','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(notes(), vec![initial(), index(2, crate::OldWrites::Drop)])
        .open()
        .await
        .unwrap();
    let device = db.local_device().unwrap();
    sql(&db, "UPDATE notes SET title='after the marker'")
        .await
        .unwrap();
    let capture = db.raise_captures().await.unwrap().remove(0);
    assert_eq!(capture.audience, Audience::Store);
    assert_eq!(capture.through, WriteId { device, number: 2 });
    assert_eq!(capture.covers, 1);
    // Sealing names the published identity; the rows are the captured ones.
    let (header, records) = decode(&raise_frames(&db, 7, 2).await);
    assert_eq!(header.id.number, 7);
    assert_eq!(header.covers, db.applied_slot().await.unwrap());
    let titles: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            SnapshotRecord::Synced(row) => Some(row.columns["title"].value.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(titles, [Value::Text("old title".into())]);
    // A raise captured at another version cannot be sealed under this one.
    assert!(db
        .write_raise_snapshot(
            SnapshotId {
                audience: Audience::Store,
                device,
                number: 8
            },
            3,
            |_| Ok::<_, std::convert::Infallible>(()),
            |_| Ok(())
        )
        .await
        .is_err());
    db.discard_raise_capture(Audience::Store, 2).await.unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_unconfirmed_prefix_names_each_own_write_through_the_marker() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(notes(), vec![initial(), index(2, crate::OldWrites::Drop)])
        .open()
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='later'").await.unwrap();
    let through = db.raise_captures().await.unwrap()[0].through;
    for number in [1, 2] {
        assert_eq!(
            db.unconfirmed_own_prefix(through).await.unwrap(),
            Some(WriteId {
                device: through.device,
                number
            })
        );
        crate::tests::publish_through(&db, number).await.unwrap();
    }
    // The later app write is still queued; it is not part of the prefix.
    assert_eq!(db.unconfirmed_own_prefix(through).await.unwrap(), None);
    assert_eq!(queue(&db).len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn without_a_store_nothing_is_captured_and_no_raise_is_intended() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(notes(), vec![initial(), index(2, crate::OldWrites::Drop)])
        .open()
        .await
        .unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    assert_eq!(
        db.raise_captures()
            .await
            .unwrap()
            .first()
            .map_or(0, |c| c.version),
        0
    );
    assert_eq!(
        records(&db)[1].header.disposition,
        WriteDisposition::Migration
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_migration_leaves_no_capture() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let original = queue(&db);
    db.close().await.unwrap();
    assert!(store
        .builder(
            notes(),
            vec![
                initial(),
                index(2, crate::OldWrites::Drop),
                Migration::breaking(
                    3,
                    "broken",
                    "CREATE INDEX broken ON missing(x)",
                    crate::OldWrites::Drop
                )
            ]
        )
        .open()
        .await
        .is_err());
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    assert_eq!(queue(&db), original);
    db.close().await.unwrap();
}

/// Index version 2 with a converter that keeps an old write's changes.
fn converted_index() -> Migration {
    index(
        2,
        crate::OldWrites::Convert(std::sync::Arc::new(|_| Ok(()))),
    )
}

async fn migrated(store: &TestStore) -> Database {
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    store
        .builder(notes(), vec![initial(), converted_index()])
        .open()
        .await
        .unwrap()
}

async fn title(db: &Database) -> String {
    db.read(|sql| Ok(sql.query_row("SELECT title FROM notes WHERE id='n'", [], |r| r.get(0))?))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_migrated_device_judges_received_writes_under_its_capture() {
    let store = TestStore::new();
    let db = migrated(&store).await;
    let capture = db.raise_captures().await.unwrap().remove(0);
    let mut later = records(&db).remove(0);
    // An old-schema write from a peer that has not updated converts as the
    // kept raise will judge it, instead of waiting for that raise (§17.1).
    let outcome = crate::tests::try_remote_update_at(
        &db,
        "notes",
        "n",
        &[("title", Value::Text("remote".into()))],
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(outcome, crate::ApplyOutcome::Applied);
    assert_eq!(title(&db).await, "remote");
    let remote = db.applied_slot().await.unwrap();
    assert!(capture.covers < remote);
    let again = db
        .prepare_raise_capture(Audience::Store, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.covers, remote);
    assert_eq!(again.through, capture.through);
    let (header, records) = decode(&raise_frames(&db, 3, 2).await);
    assert_eq!(header.covers, again.covers);
    assert!(records.iter().any(|record| matches!(
        record,
        SnapshotRecord::Synced(row) if row.columns["title"].value == Value::Text("remote".into())
    )));
    // Without a capture of its audience the write still waits for the raise.
    db.discard_raise_capture(Audience::Store, 2).await.unwrap();
    let device = coven_foundation::id_source::DeviceId(50_000);
    later.header.position = WriteId { device, number: 1 };
    later.header.timestamp =
        coven_merge::Timestamp::new(later.header.timestamp.milliseconds() + 100, 0, device)
            .unwrap();
    later.header.read_through = db.applied_slot().await.unwrap();
    let waiting = db.receive(later.into()).await.unwrap();
    assert_eq!(
        waiting,
        crate::ApplyOutcome::Waiting(crate::WriteWait::SchemaPublication(2))
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn refreshing_coverage_preserves_the_original_captured_prefix() {
    let store = TestStore::new();
    let db = migrated(&store).await;
    let capture = db.raise_captures().await.unwrap().remove(0);
    sql(&db, "UPDATE notes SET title='after the marker'")
        .await
        .unwrap();
    crate::tests::publish_through(&db, capture.through.number)
        .await
        .unwrap();
    let again = db
        .prepare_raise_capture(Audience::Store, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.through, capture.through);
    assert_eq!(again.covers, db.applied_slot().await.unwrap());
    assert_eq!(
        db.captured_through().await.unwrap(),
        Some(capture.through.number)
    );
    assert_eq!(queue(&db).len(), 1);
    assert_eq!(
        db.unconfirmed_own_prefix(again.through).await.unwrap(),
        None
    );
    assert_eq!(
        db.prepare_raise_capture(Audience::Store, 3).await.unwrap(),
        None
    );
    let (_, records) = decode(&raise_frames(&db, 3, 2).await);
    assert!(records.iter().any(|r|matches!(r,SnapshotRecord::Synced(row) if row.columns["title"].value==Value::Text("title".into()))));
    assert_eq!(title(&db).await, "after the marker");
    db.close().await.unwrap();
}

/// An addition-only update while a raise waits moves that raise to the
/// update's version, captured again at its schema so the snapshot's header
/// names the version it raises to; once no capture waits, an addition raises
/// nothing (§17.1; queue `RaiseCaptures.capture_at_raise_version`).
#[tokio::test]
async fn an_addition_moves_a_waiting_raise_to_its_version() {
    let store = TestStore::new();
    let db = migrated(&store).await;
    let before = db.raise_captures().await.unwrap().remove(0);
    db.close().await.unwrap();
    let color = Migration::addition(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT");
    let three = vec![initial(), converted_index(), color];
    let db = store.builder(notes(), three.clone()).open().await.unwrap();
    let moved = db.raise_captures().await.unwrap();
    assert_eq!(moved.len(), 1);
    assert_eq!((moved[0].version, moved[0].through), (3, before.through));
    assert_eq!(
        db.raise_captures()
            .await
            .unwrap()
            .first()
            .map_or(0, |c| c.version),
        3
    );
    let (header, _) = decode(&raise_frames(&db, 1, 3).await);
    assert_eq!(header.schema_version, 3);
    // Its raise published, a later addition raises nothing.
    db.discard_raise_capture(Audience::Store, 3).await.unwrap();
    db.close().await.unwrap();
    let mut four = three;
    four.push(Migration::addition(
        4,
        "size",
        "ALTER TABLE notes ADD COLUMN size INTEGER",
    ));
    let db = store.builder(notes(), four).open().await.unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    assert_eq!(
        db.raise_captures()
            .await
            .unwrap()
            .first()
            .map_or(0, |c| c.version),
        0
    );
    db.close().await.unwrap();
}

/// Another device's established raise replaces what this device's own
/// unpublished migration computed: its marker sets nothing here, so a later
/// write names the raise's migration write as the setter, a source every
/// receiver holds (§17.1).
#[tokio::test]
async fn an_adopted_raise_replaces_an_unpublished_migrations_setters() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    connect(&a).await;
    connect(&b).await;
    for db in [&a, &b] {
        TestStore::register(db, a_store.device(), 1);
        TestStore::register(db, b_store.device(), 1);
    }
    sql(&a, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    let fill = || {
        Migration::breaking(
            2,
            "fill",
            "UPDATE notes SET body='migrated'",
            crate::OldWrites::Drop,
        )
    };
    let mut migrated = Vec::new();
    for (store, db) in [(&a_store, a), (&b_store, b)] {
        db.close().await.unwrap();
        migrated.push(
            store
                .builder(notes(), vec![initial(), fill()])
                .open()
                .await
                .unwrap(),
        );
    }
    let (a, b) = (&migrated[0], &migrated[1]);
    let marker = records(a).pop().unwrap().header.position;
    let raise = raise_frames(a, 1, 2).await;
    let own = records(b).pop().unwrap().header;
    assert_eq!(own.disposition, WriteDisposition::Migration);
    assert_ne!(own.position, marker);
    crate::snapshot_write::tests::load_one(
        b,
        decode(&raise).0.id,
        raise.prefix.clone(),
        std::io::Cursor::new(raise.concat()),
    )
    .await
    .unwrap();
    sql(b, "UPDATE notes SET body='later'").await.unwrap();
    let later = records(b).pop().unwrap();
    assert_eq!(
        later.parts[0].rows[0].sources.setters["body"].change.write,
        marker
    );
    for db in migrated {
        db.close().await.unwrap();
    }
}

/// An audience's snapshot at the applied slot, leaving the queue unpublished.
async fn unpublished_frames(db: &Database, audience: Audience) -> SnapshotFrames {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let (output, start) = (frames.clone(), metadata.clone());
    db.write_snapshot(
        crate::snapshot_write::tests::id(audience),
        move |header| {
            *start.lock().unwrap() = Some(prefix(header));
            Ok::<_, std::convert::Infallible>(())
        },
        move |frame| {
            output.lock().unwrap().push(frame);
            Ok(())
        },
    )
    .await
    .unwrap();
    let prefix = metadata.lock().unwrap().take().unwrap();
    let frames = std::mem::take(&mut *frames.lock().unwrap());
    SnapshotFrames { prefix, frames }
}

/// A reload replays its slots only into the audiences it loads. This
/// device's own write in its captured prefix stays applied once in the store
/// audience the reload keeps, while the reload loads a circle (§15, §17.1).
#[tokio::test]
async fn a_reload_replays_a_captured_write_only_into_the_audiences_it_loads() {
    use crate::{RowIdentity, SyncedTable};
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,value INTEGER NOT NULL)";
    let tables = || {
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")]
    };
    let schema = || Migration::addition(1, "notes", SCHEMA);
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(10));
    let store = TestStore::new();
    let db = store
        .builder(tables(), vec![schema()])
        .open()
        .await
        .unwrap();
    connect(&db).await;
    store.join_circle(&db, circle);
    sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store',1)",
    )
    .await
    .unwrap();
    let written = records(&db).remove(0);
    db.close().await.unwrap();
    let index = Migration::breaking(
        2,
        "index",
        "CREATE INDEX values_index ON notes(value)",
        crate::OldWrites::Drop,
    );
    let db = store
        .builder(tables(), vec![schema(), index])
        .open()
        .await
        .unwrap();
    // The circle's snapshot covers the slots before the write's.
    let circle_snapshot = unpublished_frames(&db, Audience::Circle(circle)).await;
    crate::tests::publish_through(&db, written.header.position.number)
        .await
        .unwrap();
    db.load_snapshots::<crate::pending::tests::Record, _, _>(crate::tests::reload(
        &db,
        vec![circle_snapshot.input()],
        vec![crate::snapshot_write::tests::stream(&written)],
        Arc::new(crate::test_utils::TestAuthorship),
    ))
    .await
    .unwrap();
    let values = db
        .read(|sql| {
            Ok(sql.query("SELECT audience,value FROM notes", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?)
        })
        .await
        .unwrap();
    assert_eq!(values, [("store".to_string(), 1)]);
    db.close().await.unwrap();
}

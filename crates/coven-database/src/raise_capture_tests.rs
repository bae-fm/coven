use crate::snapshot_write::tests::{decode, prefix, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use crate::{Database, Migration};
use coven_format::snapshot::SnapshotRecord;
use coven_format::store_log::SnapshotId;
use coven_format::value::Value;
use coven_format::write::WriteDisposition;
use coven_merge::{Audience, Timestamp, WriteId};
use std::sync::{Arc, Mutex};

/// Make this database's own device the creator of a store, so a breaking
/// migration has audiences to capture.
pub(crate) async fn connect(db: &Database) {
    let device = db.sync_state(Vec::new()).await.unwrap().device;
    let (mut entry, mut replay) = crate::store_log::tests::circle_history(
        coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(1)),
    )
    .remove(0);
    let creator = entry.position;
    entry.position.device = device;
    entry.timestamp = Timestamp::new(1, 0, device).unwrap();
    let added = replay.state.devices.remove(&creator.device).unwrap();
    replay.state.devices.insert(device, added);
    replay.entries.clear();
    replay
        .entries
        .insert(entry.position, crate::EntryOutcome::Kept);
    db.apply_store_log::<crate::pending::tests::Record>(
        crate::store_log::tests::checked(entry),
        replay,
        Vec::new(),
    )
    .await
    .unwrap();
}

fn initial() -> Migration {
    Migration::sql(1, "notes", NOTES)
}
fn index(version: u32) -> Migration {
    Migration::sql(
        version,
        "index",
        if version == 2 {
            "CREATE INDEX titles ON notes(title)"
        } else {
            "CREATE INDEX bodies ON notes(body)"
        },
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
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let (output, start) = (frames.clone(), metadata.clone());
    let device = db.sync_state(Vec::new()).await.unwrap().device;
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
    let mut migrations = vec![initial(), index(2)];
    for version in [2, 3] {
        if version == 3 {
            migrations.push(index(3));
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
            db.sync_state(Vec::new()).await.unwrap().breaking_version,
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
        .builder(notes(), vec![initial(), index(2)])
        .open()
        .await
        .unwrap();
    let device = db.sync_state(Vec::new()).await.unwrap().device;
    sql(&db, "UPDATE notes SET title='after the marker'")
        .await
        .unwrap();
    let capture = db.raise_captures().await.unwrap().remove(0);
    assert_eq!(capture.audience, Audience::Store);
    assert_eq!(capture.through, WriteId { device, number: 2 });
    assert!(capture.writes.covers(capture.through));
    assert!(!capture.writes.covers(WriteId { device, number: 3 }));
    // Sealing names the published identity; the rows are the captured ones.
    let (header, records) = decode(&raise_frames(&db, 7, 2).await);
    assert_eq!(header.id.number, 7);
    assert_eq!(header.writes, capture.writes);
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
        .builder(notes(), vec![initial(), index(2)])
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
        let write = crate::upload::tests::attempt(&db, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(db.upload_succeeded(write).await.unwrap());
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
        .builder(notes(), vec![initial(), index(2)])
        .open()
        .await
        .unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    assert_eq!(db.sync_state(Vec::new()).await.unwrap().breaking_version, 0);
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
                index(2),
                Migration::sql(3, "broken", "CREATE INDEX broken ON missing(x)")
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
    index(2).writes(|_| Ok(()))
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
    let remote = db
        .sync_state(Vec::new())
        .await
        .unwrap()
        .positions
        .0
        .into_iter()
        .find(|id| id.device != capture.through.device)
        .unwrap();
    assert!(!capture.writes.covers(remote));
    // That write is past the capture, so preparing the raise captures again;
    // the snapshot then holds the converted row (§15).
    let again = db
        .prepare_raise_capture(Audience::Store, 2)
        .await
        .unwrap()
        .unwrap();
    assert!(again.writes.covers(remote));
    assert_eq!(again.through, capture.through);
    let (header, records) = decode(&raise_frames(&db, 3, 2).await);
    assert_eq!(header.writes, again.writes);
    assert!(records.iter().any(|record| matches!(
        record,
        SnapshotRecord::Synced(row) if row.columns["title"].value == Value::Text("remote".into())
    )));
    // Without a capture of its audience the write still waits for the raise.
    db.discard_raise_capture(Audience::Store, 2).await.unwrap();
    let waiting = crate::tests::try_remote_update_at(
        &db,
        "notes",
        "n",
        &[("title", Value::Text("later".into()))],
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(
        waiting,
        crate::ApplyOutcome::Waiting(crate::WriteWait::SchemaPublication(2))
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_unchanged_capture_is_kept_and_a_recapture_takes_in_later_own_writes() {
    let store = TestStore::new();
    let db = migrated(&store).await;
    let capture = db.raise_captures().await.unwrap().remove(0);
    sql(&db, "UPDATE notes SET title='after the marker'")
        .await
        .unwrap();
    // Nothing remote applied since: the migration's capture is sent, and
    // the later app write stays outside it, waiting for a kept raise.
    let kept = db
        .prepare_raise_capture(Audience::Store, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept, capture);
    assert_eq!(
        db.sync_state(Vec::new()).await.unwrap().captured_through,
        Some(capture.through.number)
    );
    crate::tests::remote_update(&db, "notes", "n", &[("body", Value::Text("peer".into()))]).await;
    // Now the capture is behind: it is taken again through the newest own
    // write, which joins the prefix the upload gate sends first (§17.1).
    let again = db
        .prepare_raise_capture(Audience::Store, 2)
        .await
        .unwrap()
        .unwrap();
    let own = WriteId {
        device: capture.through.device,
        number: capture.through.number + 1,
    };
    assert_eq!(again.through, own);
    assert!(again.writes.covers(own));
    assert_eq!(
        db.sync_state(Vec::new()).await.unwrap().captured_through,
        Some(own.number)
    );
    assert_eq!(
        db.unconfirmed_own_prefix(again.through).await.unwrap(),
        Some(WriteId {
            device: own.device,
            number: 1
        })
    );
    // A capture replaced by a later migration prepares nothing.
    assert_eq!(
        db.prepare_raise_capture(Audience::Store, 3).await.unwrap(),
        None
    );
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
    let color = Migration::sql(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT");
    let three = vec![initial(), converted_index(), color];
    let db = store.builder(notes(), three.clone()).open().await.unwrap();
    let moved = db.raise_captures().await.unwrap();
    assert_eq!(moved.len(), 1);
    assert_eq!((moved[0].version, moved[0].through), (3, before.through));
    assert_eq!(db.sync_state(Vec::new()).await.unwrap().breaking_version, 3);
    let (header, _) = decode(&raise_frames(&db, 1, 3).await);
    assert_eq!(header.schema_version, 3);
    // Its raise published, a later addition raises nothing.
    db.discard_raise_capture(Audience::Store, 3).await.unwrap();
    db.close().await.unwrap();
    let mut four = three;
    four.push(Migration::sql(
        4,
        "size",
        "ALTER TABLE notes ADD COLUMN size INTEGER",
    ));
    let db = store.builder(notes(), four).open().await.unwrap();
    assert!(db.raise_captures().await.unwrap().is_empty());
    assert_eq!(db.sync_state(Vec::new()).await.unwrap().breaking_version, 3);
    db.close().await.unwrap();
}

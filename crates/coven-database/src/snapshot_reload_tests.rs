//! Installing a snapshot gives the device the snapshot author's state: the
//! same merge records, losses, claims and written values, and the app rows
//! the removal rules leave over them (§16.2, §8.4). Each case compares the
//! loading device with the device that wrote the snapshot.
use crate::snapshot_write::tests::{decode, frames, id, load_one, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_foundation::id_source::{CircleId, SequentialIds};
use coven_merge::Audience;
use std::collections::BTreeMap;
use std::io::Cursor;

/// A music library: releases, their tracks and the tracks' files, with
/// references, unique values, CHECKs, a default and a generated column. A
/// track's cover names a release, and reads NULL once that release is gone.
pub(crate) const LIBRARY: &str = "
CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL,
  first INTEGER, last INTEGER, CHECK(first IS NULL OR last IS NULL OR first <= last));
CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
  release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
  position INTEGER NOT NULL CHECK(position > 0), title TEXT NOT NULL,
  label TEXT GENERATED ALWAYS AS (title || ' #' || position) VIRTUAL,
  cover TEXT REFERENCES releases(id) ON DELETE SET NULL,
  UNIQUE(release_id, position));
CREATE INDEX tracks_release ON tracks(release_id);
CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,
  track_id TEXT NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
  path TEXT NOT NULL UNIQUE, size INTEGER NOT NULL DEFAULT 0);
CREATE INDEX files_track ON files(track_id);";

pub(crate) const TABLES: [&str; 3] = ["releases", "tracks", "files"];

fn library() -> Vec<SyncedTable> {
    TABLES.iter().map(|table| SyncedTable::new(*table, RowIdentity::SharedKey)).collect()
}

pub(crate) async fn open(store: &TestStore) -> Database {
    store.schema(library(), LIBRARY).await.unwrap()
}

/// Apply `writer`'s queued writes on each of `devices`, in their order.
pub(crate) async fn publish(writer: &Database, devices: &[&Database]) {
    let writes = records(writer);
    writer.inspect_writer(|db| db.internal_execute("DELETE FROM _coven_uploads", []).unwrap());
    for write in writes {
        for device in devices {
            device.receive(write.clone().into()).await.unwrap();
        }
    }
}

/// Every app row of the library's tables.
pub(crate) fn app_rows(db: &Database) -> BTreeMap<String, Vec<Vec<crate::types::Value>>> {
    let contents = crate::tests::contents(db);
    TABLES.iter().map(|table| (table.to_string(), contents[*table].clone())).collect()
}

/// What the removal rules recorded, by row identity rather than local id:
/// unique claims and written values.
pub(crate) fn recorded(db: &Database) -> Vec<Vec<crate::types::Value>> {
    db.inspect_writer(|db| {
        db.query(
                "SELECT 'claim',r.table_name,r.key,r.audience,k.identity,c.value FROM _coven_claims c
                 JOIN _coven_rows r ON r.id=c.row_id JOIN _coven_constraints k ON k.id=c.constraint_id
                 UNION ALL
                 SELECT 'written',r.table_name,r.key,r.audience,l.column_name,v.value FROM _coven_written_values v
                 JOIN _coven_rows r ON r.id=v.row_id JOIN _coven_columns l ON l.id=v.column_id
                 ORDER BY 1,2,3,4,5",
                [],
                |r| (0..6).map(|i| r.get(i)).collect(),
            )
        .unwrap()
    })
}

/// Every table's rows, ordered.
pub(crate) fn everything(
    db: &crate::sqlite::DatabaseConnection,
) -> Vec<(String, Vec<Vec<crate::types::Value>>)> {
    let tables = db
        .query(
            "SELECT name FROM main.sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let rows = db
                .query(
                    &format!("SELECT * FROM main.{} ORDER BY 1", crate::sql::identifier(&table)),
                    [],
                    |r| (0..r.as_ref().column_count()).map(|i| r.get(i)).collect(),
                )
                .unwrap();
            (table, rows)
        })
        .collect()
}

/// Running the removal rules over every row of every audience changes
/// nothing an install left: the rows the rules didn't reach show what the
/// rules give them, with their losses, claims and written values.
pub(crate) fn assert_rules_settled(db: &Database) {
    db.inspect_writer_schema(|writer, schema| {
        writer
            .discarded_transaction(|writer| {
                let before = everything(writer);
                crate::download_stage::begin(writer)?;
                let rows = writer.query(
                    "SELECT table_name,key,audience FROM _coven_rows UNION SELECT table_name,key,audience FROM _coven_lost",
                    [],
                    crate::row_queries::read_identity,
                )?;
                let deleted = crate::store_state_tables::deleted_circles(writer)?;
                crate::download_stage::rules(writer, writer, schema, &deleted, rows.into_iter().collect())?;
                crate::download_stage::finish(writer)?;
                assert_eq!(everything(writer), before);
                Ok::<_, crate::DbError>(())
            })
            .unwrap();
    });
}

/// The loading device holds what the source device holds.
pub(crate) async fn assert_installed(source: &Database, loaded: &Database, audiences: &[Audience]) {
    assert_rules_settled(loaded);
    assert_eq!(app_rows(loaded), app_rows(source));
    assert_eq!(recorded(loaded), recorded(source));
    for audience in audiences {
        assert_eq!(
            crate::tests::data(loaded, audience.clone()),
            crate::tests::data(source, audience.clone())
        );
        assert_eq!(frames(loaded, audience.clone()).await, frames(source, audience.clone()).await);
    }
}

/// A local log of every app row change, for the order and count of what the
/// app's triggers see.
pub(crate) fn log_changes(db: &Database) {
    let mut sql = String::from(
        "CREATE TABLE changes(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, tbl TEXT NOT NULL, row TEXT NOT NULL);",
    );
    for table in TABLES {
        for (kind, row) in [("INSERT", "NEW"), ("UPDATE", "NEW"), ("DELETE", "OLD")] {
            sql.push_str(&format!(
                "CREATE TRIGGER {table}_{kind} AFTER {kind} ON {table} BEGIN \
                 INSERT INTO changes(kind,tbl,row) VALUES('{kind}','{table}',{row}.id); END;"
            ));
        }
    }
    db.inspect_writer(|db| db.batch(&sql).unwrap());
}

pub(crate) fn changes(db: &Database) -> Vec<(String, String, String)> {
    db.inspect_writer(|db| {
        db.query("SELECT kind,tbl,row FROM changes ORDER BY id", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
    })
}

/// The app row changes that turn `before` into `after`: each row deleted,
/// inserted or changed once.
pub(crate) fn expected_changes(
    before: &BTreeMap<String, Vec<Vec<crate::types::Value>>>,
    after: &BTreeMap<String, Vec<Vec<crate::types::Value>>>,
) -> Vec<(String, String, String)> {
    let mut expected = Vec::new();
    for table in TABLES {
        let by_key = |rows: &[Vec<crate::types::Value>]| {
            rows.iter()
                .map(|row| (format!("{:?}", row[0]), row.clone()))
                .collect::<BTreeMap<_, _>>()
        };
        let old = by_key(&before[table]);
        let new = by_key(&after[table]);
        for (key, row) in &old {
            match new.get(key) {
                None => expected.push(("DELETE".into(), table.into(), key.clone())),
                Some(changed) if changed != row => {
                    expected.push(("UPDATE".into(), table.into(), key.clone()))
                }
                Some(_) => {}
            }
        }
        for key in new.keys().filter(|key| !old.contains_key(*key)) {
            expected.push(("INSERT".into(), table.into(), key.clone()));
        }
    }
    expected.sort();
    expected
}

/// The logged changes, sorted, with keys printed as `expected_changes` does.
pub(crate) fn logged(db: &Database) -> Vec<(String, String, String)> {
    let mut logged: Vec<_> = changes(db)
        .into_iter()
        .map(|(kind, table, row)| (kind, table, format!("{:?}", crate::types::Value::Text(row))))
        .collect();
    logged.sort();
    logged
}

/// Each logged insert or update of a track or file comes after its parent's
/// change, and each delete of one before its parent's delete (§8.4).
fn assert_parents_first(db: &Database, before: &BTreeMap<String, Vec<Vec<crate::types::Value>>>) {
    let log = changes(db);
    let after = app_rows(db);
    let mut pairs = Vec::new();
    for rows in [before, &after] {
        for (table, column) in [("tracks", 1), ("files", 1)] {
            for row in &rows[table] {
                if let (crate::types::Value::Text(child), crate::types::Value::Text(parent)) =
                    (&row[0], &row[column])
                {
                    pairs.push((child.clone(), parent.clone()));
                }
            }
        }
    }
    let at = |kinds: &[&str], row: &str| {
        log.iter().position(|(kind, _, r)| kinds.contains(&kind.as_str()) && r == row)
    };
    for (child, parent) in pairs {
        if let (Some(child_at), Some(parent_at)) =
            (at(&["INSERT", "UPDATE"], &child), at(&["INSERT", "UPDATE"], &parent))
        {
            assert!(parent_at < child_at, "{parent} changes before {child}: {log:?}");
        }
        if let (Some(child_at), Some(parent_at)) =
            (at(&["DELETE"], &child), at(&["DELETE"], &parent))
        {
            assert!(child_at < parent_at, "{child} is deleted before {parent}: {log:?}");
        }
    }
}

/// Two devices writing the library concurrently, so the merged state has a
/// unique value held twice, a reference to a deleted release, a failing
/// CHECK with the rows it takes out, a lost cell, and a cover whose release
/// is gone, which SQLite reads as NULL though its cell keeps the release.
pub(crate) async fn concurrent_library(a: &Database, b: &Database) {
    sql(
        a,
        "INSERT INTO releases VALUES('r1','One',NULL,NULL),('r2','Two',NULL,NULL),('r3','Three',1,5);
         INSERT INTO tracks(id,release_id,position,title) VALUES('t1','r1',1,'a'),('t2','r1',2,'b'),('t6','r3',1,'c'),('t7','r2',1,'d');
         INSERT INTO files(id,track_id,path) VALUES('f1','t1','/one'),('f2','t2','/two'),('f6','t6','/six')",
    )
    .await
    .unwrap();
    publish(a, &[a, b]).await;
    sql(a, "INSERT INTO tracks(id,release_id,position,title) VALUES('t3','r1',3,'from a')")
        .await
        .unwrap();
    sql(a, "DELETE FROM releases WHERE id='r2'").await.unwrap();
    sql(a, "UPDATE releases SET first=4 WHERE id='r3'").await.unwrap();
    sql(a, "UPDATE tracks SET title='a on a' WHERE id='t1'").await.unwrap();
    sql(a, "UPDATE tracks SET title='b on a' WHERE id='t2'").await.unwrap();
    sql(b, "INSERT INTO tracks(id,release_id,position,title) VALUES('t4','r1',3,'from b')")
        .await
        .unwrap();
    sql(b, "INSERT INTO tracks(id,release_id,position,title) VALUES('t5','r2',2,'late')")
        .await
        .unwrap();
    sql(b, "UPDATE releases SET last=3 WHERE id='r3'").await.unwrap();
    sql(b, "UPDATE tracks SET title='a on b' WHERE id='t1'").await.unwrap();
    sql(b, "UPDATE tracks SET cover='r2' WHERE id='t1'").await.unwrap();
    sql(b, "UPDATE tracks SET position=7 WHERE id='t2'").await.unwrap();
    publish(a, &[a]).await;
    publish(b, &[a]).await;
}

#[tokio::test]
async fn a_new_device_installs_the_library_with_its_removals_and_written_values() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    concurrent_library(&a, &b).await;
    // The source's own state has every kind of removal and written value.
    assert!(count(&a, "_coven_claims") > 0);
    assert!(count(&a, "_coven_written_values") > 0);
    let lost = a.inspect_writer(|db| {
        db.query("SELECT DISTINCT replacement_kind FROM _coven_lost ORDER BY 1", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
    });
    assert_eq!(lost, ["rules", "write"]);
    log_changes(&c);
    let before = app_rows(&c);
    load_one(
        &c,
        id(Audience::Store),
        frames(&a, Audience::Store).await.prefix,
        Cursor::new(frames(&a, Audience::Store).await.concat()),
    )
    .await
    .unwrap();
    assert_installed(&a, &c, &[Audience::Store]).await;
    assert_eq!(logged(&c), expected_changes(&before, &app_rows(&c)));
    assert_parents_first(&c, &before);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn reloading_a_device_that_has_rows_changes_only_what_differs() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    sql(
        &a,
        "INSERT INTO releases VALUES('r1','One',NULL,NULL),('r2','Two',NULL,NULL),('r3','Three',1,5),('r4','Four',NULL,NULL);
         INSERT INTO tracks(id,release_id,position,title) VALUES('t1','r1',1,'a'),('t2','r1',2,'b'),('t6','r3',1,'c'),('t7','r2',1,'d'),('t8','r4',1,'e');
         INSERT INTO files(id,track_id,path) VALUES('f1','t1','/one'),('f2','t2','/two'),('f6','t6','/six'),('f8','t8','/eight')",
    )
    .await
    .unwrap();
    publish(&a, &[&a, &b]).await;
    load_one(
        &c,
        id(Audience::Store),
        frames(&a, Audience::Store).await.prefix,
        Cursor::new(frames(&a, Audience::Store).await.concat()),
    )
    .await
    .unwrap();
    assert_installed(&a, &c, &[Audience::Store]).await;
    // Later: rows deleted, added, renamed, moved between releases, a path
    // given up by one file and taken by another, and the concurrent writes
    // that leave rows taken out.
    sql(&a, "DELETE FROM releases WHERE id='r4'").await.unwrap();
    sql(&a, "UPDATE releases SET title='One again' WHERE id='r1'").await.unwrap();
    sql(&a, "UPDATE tracks SET release_id='r3', position=2 WHERE id='t2'").await.unwrap();
    sql(&a, "UPDATE files SET path='/gone' WHERE id='f1'").await.unwrap();
    sql(&a, "INSERT INTO files(id,track_id,path) VALUES('f9','t6','/one')").await.unwrap();
    sql(&a, "INSERT INTO releases VALUES('r5','Five',NULL,NULL); INSERT INTO tracks(id,release_id,position,title) VALUES('t9','r5',1,'new')")
        .await
        .unwrap();
    sql(&b, "UPDATE tracks SET title='b on b' WHERE id='t1'").await.unwrap();
    sql(&b, "INSERT INTO tracks(id,release_id,position,title) VALUES('t5','r4',2,'late')")
        .await
        .unwrap();
    sql(&a, "UPDATE releases SET last=3 WHERE id='r3'").await.unwrap();
    sql(&b, "UPDATE releases SET first=4 WHERE id='r3'").await.unwrap();
    publish(&a, &[&a]).await;
    publish(&b, &[&a]).await;
    log_changes(&c);
    let before = app_rows(&c);
    let snapshot = frames(&a, Audience::Store).await;
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
        .await
        .unwrap();
    assert_installed(&a, &c, &[Audience::Store]).await;
    assert_eq!(logged(&c), expected_changes(&before, &app_rows(&c)));
    assert_parents_first(&c, &before);
    // Loading the same snapshot again changes no app row.
    c.inspect_writer(|db| db.batch("DELETE FROM changes").unwrap());
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
        .await
        .unwrap();
    assert_eq!(changes(&c), []);
    assert_installed(&a, &c, &[Audience::Store]).await;
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

pub(crate) const CIRCLE_NOTES: &str = "
CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
  folder TEXT REFERENCES folders(id) ON DELETE CASCADE, title TEXT NOT NULL,
  UNIQUE(audience, title));
CREATE INDEX notes_folder ON notes(folder);";

pub(crate) fn circle_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("folders", RowIdentity::SharedKey),
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
    ]
}

pub(crate) const GIFTS: CircleId = CircleId(uuid::Uuid::from_u128(10));

pub(crate) async fn circle_device(store: &TestStore) -> Database {
    let db = store
        .builder(circle_tables(), vec![Migration::addition(1, "notes", CIRCLE_NOTES)])
        .open()
        .await
        .unwrap();
    store.join_circle(&db, GIFTS);
    db
}

pub(crate) fn circle_rows(db: &Database) -> BTreeMap<String, Vec<Vec<crate::types::Value>>> {
    let contents = crate::tests::contents(db);
    ["folders", "notes"].iter().map(|table| (table.to_string(), contents[*table].clone())).collect()
}

pub(crate) async fn assert_circles_installed(source: &Database, loaded: &Database) {
    assert_rules_settled(loaded);
    assert_eq!(circle_rows(loaded), circle_rows(source));
    assert_eq!(recorded(loaded), recorded(source));
    for audience in [Audience::Store, Audience::Circle(GIFTS)] {
        assert_eq!(
            crate::tests::data(loaded, audience.clone()),
            crate::tests::data(source, audience)
        );
    }
}

/// Circle notes in store folders: a note taken out with its deleted folder,
/// a moved note, one key in both audiences, and a title two circle notes
/// hold at once.
pub(crate) async fn circle_history(a: &Database, b: &Database, outside: &Database) {
    sql(
        a,
        "INSERT INTO folders VALUES('home','Home'),('old','Old');
         INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store','home','store note'),('00000000-0000-4000-8000-000000000002','00000000-0000-0000-0000-00000000000a','home','gift'),('00000000-0000-4000-8000-000000000003','00000000-0000-0000-0000-00000000000a','old','old gift')",
    )
    .await
    .unwrap();
    publish(a, &[a, b, outside]).await;
    sql(a, "DELETE FROM folders WHERE id='old'").await.unwrap();
    sql(a, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000004','00000000-0000-0000-0000-00000000000a','home','same')").await.unwrap();
    // Moving n1 into the circle deletes the store's n1. Meanwhile n6 is
    // made in the circle on one device and in the store on another.
    sql(a, "UPDATE notes SET audience='00000000-0000-0000-0000-00000000000a' WHERE id='00000000-0000-4000-8000-000000000001'").await.unwrap();
    sql(a, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000006','00000000-0000-0000-0000-00000000000a','home','circle six')").await.unwrap();
    sql(b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000005','00000000-0000-0000-0000-00000000000a','home','same')").await.unwrap();
    sql(b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000007','00000000-0000-0000-0000-00000000000a','old','in old')").await.unwrap();
    sql(outside, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000006','store','home','store six')").await.unwrap();
    publish(a, &[a]).await;
    publish(b, &[a]).await;
    publish(outside, &[a]).await;
}

#[tokio::test]
async fn circles_install_with_their_store_parents_and_keys_in_two_audiences() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store, d_store] = [(); 4].map(|()| TestStore::with_ids(&ids));
    let a = circle_device(&a_store).await;
    let b = circle_device(&b_store).await;
    let c = circle_device(&c_store).await;
    let d = d_store
        .builder(circle_tables(), vec![Migration::addition(1, "notes", CIRCLE_NOTES)])
        .open()
        .await
        .unwrap();
    for db in [&a, &b, &c, &d] {
        for (seed, store) in [&a_store, &b_store, &c_store, &d_store].into_iter().enumerate() {
            TestStore::register(db, store.device(), seed as u8 + 2);
        }
    }
    circle_history(&a, &b, &d).await;
    let rules = |db: &Database| {
        db.inspect_writer(|db| {
            db.query(
                "SELECT count(*) FROM _coven_lost WHERE replacement_kind='rules' GROUP BY audience ORDER BY audience",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        })
    };
    // The note in the deleted folder, the second holder of a title and the
    // circle's copy of a store key.
    assert_eq!(rules(&a), [3]);
    let both = || async {
        vec![
            frames(&a, Audience::Store).await.input(),
            frames(&a, Audience::Circle(GIFTS)).await.input(),
        ]
    };
    c.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &c,
            both().await,
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_circles_installed(&a, &c).await;
    // Reloading the store alone recomputes the circle's rows over it.
    sql(&a, "DELETE FROM notes WHERE id='00000000-0000-4000-8000-000000000006'").await.unwrap();
    publish(&a, &[&a]).await;
    let store = frames(&a, Audience::Store).await;
    c.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &c,
            vec![store.input()],
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_circles_installed(&a, &c).await;
    // A deleted circle's rows are installed taken out.
    crate::tests::delete_circle(&a, GIFTS).await.unwrap();
    crate::tests::delete_circle(&c, GIFTS).await.unwrap();
    c.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &c,
            both().await,
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_circles_installed(&a, &c).await;
    for db in [a, b, c, d] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn queued_writes_apply_once_over_the_installed_library() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    concurrent_library(&a, &b).await;
    let snapshot = frames(&a, Audience::Store).await;
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
        .await
        .unwrap();
    // c writes while offline: renames, a file under a visible track, and a
    // track taking a position the later snapshot gives another track.
    sql(&c, "UPDATE releases SET title='Renamed' WHERE id='r1'").await.unwrap();
    sql(&c, "UPDATE tracks SET title='queued' WHERE id='t1'").await.unwrap();
    sql(&c, "INSERT INTO files(id,track_id,path) VALUES('f4','t3','/four')").await.unwrap();
    sql(&c, "INSERT INTO tracks(id,release_id,position,title) VALUES('t11','r1',4,'queued four')")
        .await
        .unwrap();
    let waiting = records(&c);
    assert!(!waiting.is_empty());
    sql(&a, "UPDATE tracks SET title='later title' WHERE id='t1'").await.unwrap();
    sql(&a, "INSERT INTO tracks(id,release_id,position,title) VALUES('t10','r1',4,'four')")
        .await
        .unwrap();
    publish(&a, &[&a]).await;
    let later = frames(&a, Audience::Store).await;
    for _ in 0..2 {
        load_one(&c, id(Audience::Store), later.prefix.clone(), Cursor::new(later.concat()))
            .await
            .unwrap();
        assert_eq!(records(&c), waiting);
        // The queue reapplied over the snapshot gives what the source has
        // once it applies the same writes.
        let source = TestStore::with_ids(&ids);
        let d = open(&source).await;
        load_one(&d, id(Audience::Store), later.prefix.clone(), Cursor::new(later.concat()))
            .await
            .unwrap();
        for write in &waiting {
            d.receive(write.clone().into()).await.unwrap();
        }
        assert_eq!(app_rows(&c), app_rows(&d));
        assert_eq!(recorded(&c), recorded(&d));
        assert_rules_settled(&c);
        d.close().await.unwrap();
    }
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

/// Re-encode a snapshot after `edit` changes its records.
pub(crate) fn edited(
    snapshot: &SnapshotFrames,
    edit: impl FnOnce(&mut SnapshotHeader, &mut Vec<SnapshotRecord>),
) -> Vec<u8> {
    let (mut header, mut records) = decode(snapshot);
    edit(&mut header, &mut records);
    let (mut encoder, mut bytes) = coven_format::snapshot::SnapshotEncoder::start(header).unwrap();
    for record in records {
        bytes.extend(encoder.record(record).unwrap());
    }
    bytes.extend(encoder.finish().unwrap());
    bytes
}

pub(crate) fn section(record: &SnapshotRecord) -> usize {
    match record {
        SnapshotRecord::Synced(_) => 0,
        SnapshotRecord::Write(_) => 1,
        SnapshotRecord::Column(_) => 2,
        SnapshotRecord::Merge(_) => 3,
        SnapshotRecord::Loss(_) => 4,
        SnapshotRecord::Authored(_) => 5,
        SnapshotRecord::Creation(_) => 6,
        SnapshotRecord::Evidence(_) => 7,
        SnapshotRecord::Admin(_) => 8,
        SnapshotRecord::KeyCopy(_) => 9,
        SnapshotRecord::Announcement(_) => 10,
        SnapshotRecord::Claim(_) => 11,
    }
}

/// Drop the first record `which` selects, keeping the header's counts.
fn without(
    header: &mut SnapshotHeader,
    records: &mut Vec<SnapshotRecord>,
    which: impl Fn(&SnapshotRecord) -> bool,
) {
    let at = records.iter().position(which).unwrap();
    header.counts[section(&records[at])] -= 1;
    records.remove(at);
}

fn synced_track<'a>(
    records: &'a mut [SnapshotRecord],
    key: &str,
) -> &'a mut coven_format::snapshot_rows::SyncedRow {
    records
        .iter_mut()
        .find_map(|record| match record {
            SnapshotRecord::Synced(row)
                if row.row.table == "tracks"
                    && row.row.key
                        == coven_format::key::encode_key(&[coven_format::value::Value::Text(
                            key.into(),
                        )])
                        .unwrap() =>
            {
                Some(row)
            }
            _ => None,
        })
        .unwrap()
}

fn is_track(record: &SnapshotRecord, kind: usize, key: &str) -> bool {
    let row = match record {
        SnapshotRecord::Synced(row) => &row.row,
        SnapshotRecord::Merge(row) => row.state.row(),
        _ => return false,
    };
    section(record) == kind
        && row.table == "tracks"
        && row.key
            == coven_format::key::encode_key(&[coven_format::value::Value::Text(key.into())])
                .unwrap()
}

#[tokio::test]
async fn inconsistent_library_snapshots_are_refused_with_their_reason_and_change_nothing() {
    use coven_format::value::Value;
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    concurrent_library(&a, &b).await;
    let snapshot = frames(&a, Audience::Store).await;
    let original = crate::tests::contents(&c);
    type Edit = Box<dyn FnOnce(&mut SnapshotHeader, &mut Vec<SnapshotRecord>)>;
    let cases: Vec<(&str, Edit)> = vec![
        (
            "synced and merge values disagree",
            Box::new(|_, records| {
                synced_track(records, "t1").columns.get_mut("title").unwrap().value =
                    Value::Text("not merged".into());
            }),
        ),
        (
            "synced values disagree with their key or schema",
            Box::new(|_, records| {
                synced_track(records, "t1").columns.get_mut("position").unwrap().value =
                    Value::Text("1".into());
            }),
        ),
        (
            "row names an unknown column",
            Box::new(|_, records| {
                let row = synced_track(records, "t1");
                let value = row.columns["title"].clone();
                row.columns.insert("nothing".into(), value);
            }),
        ),
        (
            "row names a generated column",
            Box::new(|_, records| {
                let row = synced_track(records, "t1");
                let value = row.columns["title"].clone();
                row.columns.insert("label".into(), value);
            }),
        ),
        (
            "synced row has no merge record",
            Box::new(|header, records| without(header, records, |r| is_track(r, 3, "t3"))),
        ),
        (
            "synced row has no merge record",
            Box::new(|header, records| without(header, records, |r| is_track(r, 0, "t3"))),
        ),
        (
            "merge cell has no column record",
            Box::new(|header, records| {
                without(
                    header,
                    records,
                    |r| matches!(r, SnapshotRecord::Column(c) if c.table == "tracks" && c.column == "title"),
                )
            }),
        ),
    ];
    for (reason, edit) in cases {
        let bytes = edited(&snapshot, edit);
        let error = load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(bytes))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, crate::DbError::Snapshot(crate::SnapshotError::Inconsistent(r)) if *r == reason),
            "{reason}: {error:?}"
        );
        assert_eq!(crate::tests::contents(&c), original);
    }
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_synced_value_without_a_merge_cell_must_be_the_columns_default() {
    use coven_format::value::Value;
    let ids = SequentialIds::new();
    let [a_store, c_store] = [(); 2].map(|()| TestStore::with_ids(&ids));
    let first = || vec![Migration::addition(1, "notes", crate::write::tests::NOTES)];
    let both = || {
        let mut migrations = first();
        migrations.push(Migration::addition(
            2,
            "color",
            "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
        ));
        migrations
    };
    let a = a_store.builder(crate::write::tests::notes(), first()).open().await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','before color','')").await.unwrap();
    publish(&a, &[&a]).await;
    a.close().await.unwrap();
    let a = a_store.builder(crate::write::tests::notes(), both()).open().await.unwrap();
    let c = c_store.builder(crate::write::tests::notes(), both()).open().await.unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let original = crate::tests::contents(&c);
    let bytes = edited(&snapshot, |_, records| {
        let SnapshotRecord::Synced(row) = &mut records[0] else {
            panic!("synced row first");
        };
        row.columns.get_mut("color").unwrap().value = Value::Text("red".into());
    });
    let error = load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(bytes))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, crate::DbError::Snapshot(crate::SnapshotError::Inconsistent(r)) if *r == "synced values disagree with merge defaults"),
        "{error:?}"
    );
    assert_eq!(crate::tests::contents(&c), original);
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
        .await
        .unwrap();
    assert_eq!(crate::tests::data(&c, Audience::Store), crate::tests::data(&a, Audience::Store));
    for db in [a, c] {
        db.close().await.unwrap();
    }
}

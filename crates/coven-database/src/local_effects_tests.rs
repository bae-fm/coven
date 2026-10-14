use crate::entry_effects::tests::fate;
use crate::pending::tests::Record;
use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Boundary, BoundaryCoverage, EntryId, SnapshotReload, SnapshotSource, SyncedTable};
use coven_format::{
    store_log::SnapshotId,
    value::{Value, WritePositions},
    write::WriteRecord,
};
use coven_foundation::id_source::{CircleId, DeviceId, SequentialIds};
use coven_merge::{Audience, Operation};
use std::collections::BTreeMap;

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '');
    CREATE TABLE pins(id TEXT PRIMARY KEY, note TEXT REFERENCES notes(id) ON DELETE CASCADE, label TEXT);
    CREATE TABLE marks(id INTEGER PRIMARY KEY, pin TEXT REFERENCES pins(id) ON DELETE CASCADE);
    CREATE TABLE links(id TEXT PRIMARY KEY, note TEXT REFERENCES notes(id) ON DELETE SET NULL, title TEXT);
    CREATE TABLE tags(note TEXT REFERENCES notes(id) ON DELETE CASCADE, tag TEXT);";

type Rows = Vec<Vec<Option<String>>>;

/// Every local table's rows, including the hidden rowid of the keyless table.
fn local(db: &crate::Database) -> BTreeMap<&'static str, Rows> {
    db.inspect_writer(|sql| {
        [
            ("pins", "SELECT id,note,label FROM pins ORDER BY id"),
            (
                "marks",
                "SELECT CAST(id AS TEXT),pin FROM marks ORDER BY id",
            ),
            ("links", "SELECT id,note,title FROM links ORDER BY id"),
            (
                "tags",
                "SELECT CAST(rowid AS TEXT),note,tag FROM tags ORDER BY rowid",
            ),
        ]
        .into_iter()
        .map(|(table, query)| {
            let rows = sql
                .query(query, [], |r| {
                    (0..r.as_ref().column_count())
                        .map(|i| r.get::<_, Option<String>>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap();
            (table, rows)
        })
        .collect()
    })
}

fn kept(db: &crate::Database) -> (i64, i64) {
    db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT (SELECT count(*) FROM _coven_local_effects),(SELECT count(*) FROM _coven_local_effect_rows)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    })
}

fn row(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_owned)).collect()
}

/// Reset the store to an empty snapshot whose coverage names `included`.
async fn reset(db: &crate::Database, writes: &[WriteRecord], included: WritePositions) -> EntryId {
    let entry = EntryId {
        device: DeviceId(99),
        number: 1,
    };
    db.check_boundary_fixture(Boundary::Reset {
        entry,
        snapshot: SnapshotId {
            audience: Audience::Store,
            device: entry.device,
            number: entry.number,
        },
    })
    .await
    .unwrap();
    let mut reload = SnapshotReload::<std::io::Empty, _>::new(
        vec![],
        writes
            .iter()
            .map(crate::snapshot_write::tests::stream)
            .collect(),
    );
    reload.snapshots = vec![SnapshotSource::Empty(Audience::Store)];
    reload.boundaries = Some(vec![BoundaryCoverage { entry, included }]);
    db.load_snapshots::<crate::pending::tests::Record, _, _>(reload)
        .await
        .unwrap();
    entry
}

#[tokio::test]
async fn reversed_reset_restores_cascaded_and_nulled_local_rows_after_local_edits() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let tables = || vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)];
    let source = source_store.schema(tables(), NOTES).await.unwrap();
    let target = target_store.schema(tables(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    target
        .receive(records(&source).remove(0).into())
        .await
        .unwrap();
    sql(
        &target,
        "INSERT INTO pins VALUES('p1','n','one'),('p2','n','two');
         INSERT INTO marks VALUES(7,'p1');
         INSERT INTO links VALUES('l1','n','first'),('l2','n','second');
         INSERT INTO tags(rowid,note,tag) VALUES(40,'n','red')",
    )
    .await
    .unwrap();
    let original = local(&target);
    let entry = reset(&target, &[], WritePositions(vec![])).await;
    assert_eq!(count(&target, "notes"), 0);
    let hidden = BTreeMap::from([
        ("pins", vec![]),
        ("marks", vec![]),
        (
            "links",
            vec![
                row(&[Some("l1"), None, Some("first")]),
                row(&[Some("l2"), None, Some("second")]),
            ],
        ),
        ("tags", vec![]),
    ]);
    assert_eq!(local(&target), hidden);
    // Explicit local edits while the note is hidden.
    sql(
        &target,
        "INSERT INTO pins VALUES('p2',NULL,'replaced');
         DELETE FROM links WHERE id='l1';
         UPDATE links SET title='edited' WHERE id='l2'",
    )
    .await
    .unwrap();
    fate(&target, entry, false).await.unwrap();
    assert_eq!(count(&target, "notes"), 1);
    let returned = BTreeMap::from([
        (
            "pins",
            vec![
                row(&[Some("p1"), Some("n"), Some("one")]),
                row(&[Some("p2"), None, Some("replaced")]),
            ],
        ),
        ("marks", vec![row(&[Some("7"), Some("p1")])]),
        ("links", vec![row(&[Some("l2"), Some("n"), Some("edited")])]),
        ("tags", original["tags"].clone()),
    ]);
    assert_eq!(local(&target), returned);
    assert_eq!(kept(&target), (0, 0));
    // Repeated replay keeps and restores again.
    fate(&target, entry, true).await.unwrap();
    assert_eq!(count(&target, "notes"), 0);
    assert!(local(&target)["marks"].is_empty());
    assert_eq!(
        local(&target)["links"],
        vec![row(&[Some("l2"), None, Some("edited")])]
    );
    assert!(kept(&target).0 > 0);
    target.close().await.unwrap();
    let target = target_store.schema(tables(), NOTES).await.unwrap();
    fate(&target, entry, false).await.unwrap();
    assert_eq!(local(&target), returned);
    // Once the reset is final its removal stands and nothing is kept.
    fate(&target, entry, true).await.unwrap();
    assert!(kept(&target).0 > 0);
    target
        .advance_store_log_horizon(
            target.store_log().await.unwrap(),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(kept(&target), (0, 0));
    assert!(local(&target)["pins"].iter().all(|pin| pin[1].is_none()));
    for db in [source, target] {
        db.close().await.unwrap();
    }
}

const CIRCLE_ROW: &str = "00000000-0000-4000-8000-000000000001";
const STORE_ROW: &str = "00000000-0000-4000-8000-000000000002";
const OTHER_ROW: &str = "00000000-0000-4000-8000-000000000003";

fn circle_write(n: u64, id: &str, audience: &Audience) -> WriteRecord {
    let mut write = crate::test_utils::tests::insert(n, n as usize);
    write.parts[0].audience = audience.clone();
    let row = &mut write.parts[0].rows[0];
    row.row.audience = audience.clone();
    let key = Value::Text(id.to_owned());
    row.row.key = coven_format::key::encode_key(std::slice::from_ref(&key)).unwrap();
    let Operation::Insert(values) = &mut row.change.operation else {
        unreachable!()
    };
    values.get_mut("id").unwrap().value = key;
    values.insert(
        "audience".into(),
        coven_merge::ColumnValue {
            value: Value::Text(crate::write_encoding::audience_text(audience)),
            parents: BTreeMap::new(),
        },
    );
    write
}

#[tokio::test]
async fn a_child_of_two_provisionally_hidden_parents_returns_when_both_do() {
    use crate::store_log::tests::{checked, circle_history, keys};
    use coven_format::store_log::StoreChange;
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let history = circle_history(circle);
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("rows", crate::RowIdentity::IndependentUuid).audience_column("audience")],
            "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a TEXT,b TEXT);
             CREATE TABLE pins(row TEXT REFERENCES rows(id) ON DELETE CASCADE);
             CREATE TABLE pairs(id TEXT PRIMARY KEY,first TEXT REFERENCES rows(id) ON DELETE CASCADE,second TEXT REFERENCES rows(id) ON DELETE CASCADE)",
        )
        .await
        .unwrap();
    for (entry, replay) in &history[..4] {
        db.apply_store_log::<Record>(checked(entry.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
    }
    let inside = circle_write(0, CIRCLE_ROW, &Audience::Circle(circle));
    let other = circle_write(1, OTHER_ROW, &Audience::Store);
    let mut outside = circle_write(2, STORE_ROW, &Audience::Store);
    outside.header.had_read = WritePositions(vec![other.header.position]);
    for write in [&inside, &other, &outside] {
        db.receive(write.clone().into()).await.unwrap();
    }
    sql(
        &db,
        "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001');
         INSERT INTO pairs VALUES('x','00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000002')",
    )
    .await
    .unwrap();
    let local = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query_row(
                "SELECT (SELECT count(*) FROM pins),(SELECT count(*) FROM pairs)",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap()
        })
    };
    // The circle's deletion hides its row: the pin and the pair cascade.
    let (deletion, replay) = &history[4];
    db.apply_store_log::<Record>(checked(deletion.clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(local(&db), (0, 0));
    // A store reset then takes out the pair's other parent too: its write
    // read a write outside the reset's coverage and missed the one it names.
    let entry = reset(
        &db,
        &[inside.clone(), other.clone(), outside.clone()],
        WritePositions(vec![inside.header.position]),
    )
    .await;
    assert_eq!(count(&db, "rows"), 1);
    // Removing the deleting member beats the deletion: the circle row and its
    // pin return, while the pair waits for its other parent.
    let mut removal = history[0].0.clone();
    removal.position.number = 5;
    removal.timestamp = coven_merge::Timestamp::new(5, 0, removal.position.device).unwrap();
    removal.change = StoreChange::RemoveMember {
        member: keys(2).signing,
    };
    let mut replay = db.store_log().await.unwrap().replay;
    replay
        .state
        .members
        .get_mut(&keys(2).signing)
        .unwrap()
        .removed = true;
    replay
        .state
        .circles
        .get_mut(&circle)
        .unwrap()
        .members
        .remove(&keys(2).signing);
    replay.state.circle_deletions.remove(&circle);
    replay
        .entries
        .insert(removal.position, crate::EntryOutcome::Kept);
    replay.entries.insert(
        deletion.position,
        crate::EntryOutcome::Dropped(crate::DropReason::BeatenBy(removal.position)),
    );
    db.apply_store_log::<Record>(checked(removal), replay, Vec::new())
        .await
        .unwrap();
    assert_eq!(count(&db, "rows"), 2);
    assert_eq!(local(&db), (1, 0));
    fate(&db, entry, false).await.unwrap();
    assert_eq!(count(&db, "rows"), 3);
    assert_eq!(local(&db), (1, 1));
    assert_eq!(kept(&db), (0, 0));
    db.close().await.unwrap();
}

//! The rows the removal rules start from on a reload (`snapshot_conflicts`):
//! each kind is needed, as a case where the snapshot's author's view and
//! this device's differ shows. Every case also checks that running the rules
//! over every row changes nothing the install left.
use crate::snapshot_reload::tests::*;
use crate::snapshot_write::tests::{frames, id, load_one};
use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::snapshot::SnapshotRecord;
use coven_foundation::id_source::{CircleId, SequentialIds};
use coven_merge::Audience;
use std::collections::BTreeMap;
use std::io::Cursor;

/// `snapshot` with its author's removals undone: no rules loss, and each row
/// its author's rules took out shown as synced, as a body that only lists its
/// author's rows would show it. The loading device's own rules must find them.
/// Also returns how many rows its author's rules took out.
fn without_rule_losses(
    snapshot: &crate::snapshot_write::tests::SnapshotFrames,
) -> (Vec<u8>, usize) {
    let mut taken_out = 0;
    let bytes = edited(snapshot, |header, records| {
        let merged: BTreeMap<_, _> = records
            .iter()
            .filter_map(|record| match record {
                SnapshotRecord::Merge(row) => Some((row.state.row().clone(), row.state.clone())),
                _ => None,
            })
            .collect();
        let removed: Vec<_> = records
            .iter()
            .filter_map(|record| match record {
                SnapshotRecord::Loss(loss)
                    if matches!(loss.cause, coven_format::loss::LossCause::Rules(_)) =>
                {
                    Some(loss.row.clone())
                }
                _ => None,
            })
            .collect();
        taken_out = removed.len();
        records.retain(|record| {
                !matches!(record, SnapshotRecord::Loss(loss) if matches!(loss.cause, coven_format::loss::LossCause::Rules(_)))
            });
        header.counts[coven_format::snapshot::section::LOSS] -= removed.len() as u64;
        for row in removed {
            let state = &merged[&row];
            records.push(SnapshotRecord::Synced(coven_format::snapshot_rows::SyncedRow {
                row: row.clone(),
                columns: state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.clone()))
                    .collect(),
            }));
            header.counts[coven_format::snapshot::section::SYNCED] += 1;
        }
        let order = |record: &SnapshotRecord| match record {
            SnapshotRecord::Synced(row) => (record.section(), Some(row.row.clone())),
            _ => (record.section(), None),
        };
        // Synced rows in row order, every other section as it was.
        let mut indexed: Vec<_> = records.drain(..).enumerate().collect();
        indexed.sort_by(|(i, x), (j, y)| match (order(x), order(y)) {
            ((0, Some(a)), (0, Some(b))) => a.cmp(&b),
            ((s, _), (t, _)) => s.cmp(&t).then(i.cmp(j)),
        });
        records.extend(indexed.into_iter().map(|(_, record)| record));
    });
    (bytes, taken_out)
}

/// The rows a snapshot's author showed are only what its author's rules
/// gave: a device runs the rules itself. A body that shows the rows its
/// author took out, without their losses, has them taken out here again.
#[tokio::test]
async fn rows_a_snapshot_shows_are_taken_out_by_this_devices_rules() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    concurrent_library(&a, &b).await;
    let snapshot = frames(&a, Audience::Store).await;
    let (bytes, taken_out) = without_rule_losses(&snapshot);
    assert!(taken_out > 0);
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(bytes)).await.unwrap();
    assert_installed(&a, &c, &[Audience::Store]).await;
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

/// A key in two circles shows in the circle that started it first, and only
/// on a device that has both rows (§15.4): one device's circle snapshot can
/// show a row another device takes out, and the other way round. Each
/// device's own rules decide.
#[tokio::test]
async fn a_key_in_two_circles_is_judged_by_the_rows_this_device_has() {
    const OTHER: CircleId = CircleId(uuid::Uuid::from_u128(11));
    const KEY: &str = "00000000-0000-4000-8000-000000000009";
    let ids = SequentialIds::new();
    let stores: [TestStore; 5] = [(); 5].map(|()| TestStore::with_ids(&ids));
    async fn open(store: &TestStore) -> Database {
        let db = circle_device(store).await;
        store.join_circle(&db, OTHER);
        db
    }
    // `both` has both circles' rows; `gifts` only its own.
    let both = open(&stores[0]).await;
    let gifts = open(&stores[1]).await;
    let earlier = open(&stores[2]).await;
    sql(&both, "INSERT INTO folders VALUES('home','Home')").await.unwrap();
    let folder = records(&both);
    publish(&both, &[&both, &gifts, &earlier]).await;
    sql(&earlier, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000009','00000000-0000-0000-0000-00000000000b','home','in other')")
        .await
        .unwrap();
    let in_other = records(&earlier);
    publish(&earlier, &[&both]).await;
    sql(&gifts, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000009','00000000-0000-0000-0000-00000000000a','home','in gifts')")
        .await
        .unwrap();
    publish(&gifts, &[&both, &gifts]).await;
    let key = crate::tests::text_key(KEY);
    let shown = |db: &Database| {
        db.inspect_writer(|db| {
            db.query("SELECT audience FROM notes WHERE id=?1", [KEY], |r| r.get::<_, String>(0))
                .unwrap()
        })
    };
    let removed = |db: &Database| {
        db.inspect_writer(|db| {
            db.query_row(
                "SELECT count(*) FROM _coven_lost WHERE key=?1 AND audience=?2 AND replacement_kind='rules'",
                rusqlite::params![key, GIFTS.to_string()],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        })
    };
    let other = "00000000-0000-0000-0000-00000000000b";
    let in_gifts = "00000000-0000-0000-0000-00000000000a";
    assert_eq!(shown(&both), [other]);
    assert_eq!(removed(&both), 1);
    assert_eq!(shown(&gifts), [in_gifts]);
    // Without the other circle's row, both's Gifts snapshot shows the row
    // both took out.
    let without = open(&stores[3]).await;
    without
        .load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
            crate::tests::reload(
                &without,
                vec![
                    frames(&both, Audience::Store).await.input(),
                    frames(&both, Audience::Circle(GIFTS)).await.input(),
                ],
                vec![],
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await,
            None,
        )
        .await
        .unwrap();
    assert_eq!(shown(&without), [in_gifts]);
    assert_eq!(removed(&without), 0);
    assert_rules_settled(&without);
    // With it, gifts's Gifts snapshot loses the row gifts shows.
    let with = open(&stores[4]).await;
    for write in folder.iter().chain(&in_other) {
        with.receive(write.clone().into()).await.unwrap();
    }
    assert_eq!(shown(&with), [other]);
    with.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &with,
            vec![frames(&gifts, Audience::Circle(GIFTS)).await.input()],
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_eq!(shown(&with), [other]);
    assert_eq!(removed(&with), 1);
    assert_rules_settled(&with);
    for db in [both, gifts, earlier, without, with] {
        db.close().await.unwrap();
    }
}

/// A circle note shows while its store folder is at the generation it
/// names. Reloading the store with that folder deleted and made again takes
/// the note out, though the circle itself isn't reloaded.
#[tokio::test]
async fn reloading_a_parent_audience_takes_out_a_child_whose_parent_was_remade() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = circle_device(&a_store).await;
    let b = circle_device(&b_store).await;
    let c = circle_device(&c_store).await;
    sql(&a, "INSERT INTO folders VALUES('home','Home')").await.unwrap();
    let folder = records(&a);
    publish(&a, &[&a, &b]).await;
    sql(&b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','home','in home')")
        .await
        .unwrap();
    let note = records(&b);
    for write in folder.iter().chain(&note) {
        c.receive(write.clone().into()).await.unwrap();
    }
    assert_eq!(count(&c, "notes"), 1);
    sql(&a, "DELETE FROM folders WHERE id='home'").await.unwrap();
    sql(&a, "INSERT INTO folders VALUES('home','Home again')").await.unwrap();
    publish(&a, &[&a]).await;
    c.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &c,
            vec![frames(&a, Audience::Store).await.input()],
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    assert_eq!(count(&c, "notes"), 0);
    assert_eq!(
        c.inspect_writer(|db| db
            .query_row(
                "SELECT count(*) FROM _coven_lost WHERE table_name='notes' AND replacement_kind='rules'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap()),
        1
    );
    assert_rules_settled(&c);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

/// A device that has applied the deletion of a circle takes out the rows of
/// a circle snapshot its author made before that deletion applied there.
#[tokio::test]
async fn a_deleted_circles_snapshot_rows_are_taken_out() {
    let ids = SequentialIds::new();
    let [a_store, b_store, c_store] = [(); 3].map(|()| TestStore::with_ids(&ids));
    let a = circle_device(&a_store).await;
    let b = circle_device(&b_store).await;
    let c = circle_device(&c_store).await;
    circle_history(&a, &b, &c).await;
    assert!(count(&a, "notes") > 0);
    crate::tests::delete_circle(&c, GIFTS).await.unwrap();
    c.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::tests::reload(
            &c,
            vec![
                frames(&a, Audience::Store).await.input(),
                frames(&a, Audience::Circle(GIFTS)).await.input(),
            ],
            vec![],
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await,
        None,
    )
    .await
    .unwrap();
    let shown = c.inspect_writer(|db| {
        db.query_row("SELECT count(*) FROM notes WHERE audience<>'store'", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    });
    assert_eq!(shown, 0);
    assert_rules_settled(&c);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

/// A unique value one shown row gives up and another takes: the row giving
/// it up changes first, so SQLite never holds the value twice.
#[tokio::test]
async fn a_unique_value_moves_between_rows_in_a_reload() {
    let ids = SequentialIds::new();
    let [a_store, c_store] = [(); 2].map(|()| TestStore::with_ids(&ids));
    let a = open(&a_store).await;
    let c = open(&c_store).await;
    sql(
        &a,
        "INSERT INTO releases VALUES('r1','One',NULL,NULL);
         INSERT INTO tracks(id,release_id,position,title) VALUES('t1','r1',1,'a'),('t2','r1',2,'b');
         INSERT INTO files(id,track_id,path) VALUES('f1','t1','/one')",
    )
    .await
    .unwrap();
    publish(&a, &[&a]).await;
    let first = frames(&a, Audience::Store).await;
    load_one(&c, id(Audience::Store), first.prefix.clone(), Cursor::new(first.concat()))
        .await
        .unwrap();
    sql(&a, "UPDATE files SET path='/gone' WHERE id='f1'; INSERT INTO files(id,track_id,path) VALUES('f2','t2','/one')")
        .await
        .unwrap();
    publish(&a, &[&a]).await;
    log_changes(&c);
    let before = app_rows(&c);
    let second = frames(&a, Audience::Store).await;
    load_one(&c, id(Audience::Store), second.prefix.clone(), Cursor::new(second.concat()))
        .await
        .unwrap();
    assert_installed(&a, &c, &[Audience::Store]).await;
    assert_eq!(logged(&c), expected_changes(&before, &app_rows(&c)));
    for db in [a, c] {
        db.close().await.unwrap();
    }
}

/// Rows of a table that references itself are put in order one at a time,
/// so the app's triggers see a parent's insert before its child's even when
/// the child's key sorts first.
#[tokio::test]
async fn a_table_referencing_itself_installs_parents_first() {
    const NODES: &str = "CREATE TABLE nodes(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES nodes(id) ON DELETE CASCADE);
                         CREATE INDEX nodes_parent ON nodes(parent);
                         CREATE TABLE changes(id INTEGER PRIMARY KEY, row TEXT NOT NULL);
                         CREATE TRIGGER nodes_insert AFTER INSERT ON nodes BEGIN INSERT INTO changes(row) VALUES(NEW.id); END;";
    let tables = || vec![SyncedTable::new("nodes", RowIdentity::SharedKey)];
    let ids = SequentialIds::new();
    let [a_store, c_store] = [(); 2].map(|()| TestStore::with_ids(&ids));
    let a = a_store.schema(tables(), NODES).await.unwrap();
    let c = c_store.schema(tables(), NODES).await.unwrap();
    sql(&a, "INSERT INTO nodes VALUES('z',NULL),('m','z'),('a','m')").await.unwrap();
    publish(&a, &[&a]).await;
    let snapshot = frames(&a, Audience::Store).await;
    load_one(&c, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(snapshot.concat()))
        .await
        .unwrap();
    let order = c.inspect_writer(|db| {
        db.query("SELECT row FROM changes ORDER BY id", [], |r| r.get::<_, String>(0)).unwrap()
    });
    assert_eq!(order, ["z", "m", "a"]);
    assert_rules_settled(&c);
    for db in [a, c] {
        db.close().await.unwrap();
    }
}

/// Random concurrent histories of the library on two devices: deletes whose
/// children the other device kept writing, CHECKs that fail only once both
/// sides' cells merge, positions and paths two rows claim, covers naming
/// deleted releases. Each installs on a new device as the snapshot holds it,
/// and again with every removal its author made undone, so this device's own
/// seeds (`snapshot_conflicts::rows`) and `recompute`'s closure must find each
/// row with a reason of its own, both rows of every rival claim, and each row
/// referencing them (proofs/merge.md B20's `Region`). Every install shows
/// what its author shows, and running the rules over every row changes
/// nothing it left.
#[tokio::test]
async fn random_histories_install_what_replay_gives() {
    let mut seed = 0x853c_49e6_748f_ea9bu64;
    let mut pick = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut removals = 0;
    for _ in 0..16 {
        let ids = SequentialIds::new();
        let [a_store, b_store, c_store, d_store] = [(); 4].map(|()| TestStore::with_ids(&ids));
        let a = open(&a_store).await;
        let b = open(&b_store).await;
        sql(
            &a,
            "INSERT INTO releases VALUES('r1','One',NULL,NULL),('r2','Two',1,NULL),('r3','Three',NULL,5);
             INSERT INTO tracks(id,release_id,position,title,cover) VALUES('t1','r1',1,'a','r2'),('t2','r1',2,'b',NULL),('t3','r2',1,'c','r3'),('t4','r3',1,'d',NULL);
             INSERT INTO files(id,track_id,path) VALUES('f1','t1','/1'),('f2','t2','/2'),('f3','t3','/3')",
        )
        .await
        .unwrap();
        publish(&a, &[&a, &b]).await;
        for (device, name) in [(&a, "a"), (&b, "b")] {
            for n in 0..14 {
                let release = format!("r{}", 1 + pick(3));
                let track = format!("t{}", 1 + pick(4));
                let statement = match pick(9) {
                    // New tracks and files often claim a position or a path
                    // the other device's new row claims too.
                    0 => format!(
                        "INSERT INTO tracks(id,release_id,position,title) VALUES('{name}t{n}','{release}',{},'new')",
                        3 + pick(2)
                    ),
                    1 => format!(
                        "INSERT INTO files(id,track_id,path) VALUES('{name}f{n}','{track}','/{}')",
                        4 + pick(2)
                    ),
                    2 => format!("DELETE FROM releases WHERE id='{release}'"),
                    // Device a raises first and b lowers last, so the merged
                    // cells often fail the CHECK neither device's own did.
                    3 | 4 if name == "a" => {
                        format!("UPDATE releases SET first={} WHERE id='{release}'", 3 + pick(3))
                    }
                    3 | 4 => format!("UPDATE releases SET last={} WHERE id='{release}'", pick(3)),
                    5 => format!("UPDATE tracks SET position={} WHERE id='{track}'", 1 + pick(3)),
                    6 => format!("UPDATE tracks SET cover='{release}' WHERE id='{track}'"),
                    7 => format!("UPDATE tracks SET release_id='{release}' WHERE id='{track}'"),
                    _ => format!("DELETE FROM tracks WHERE id='{track}'"),
                };
                // A statement this device's own state refuses is no write.
                let _refused = device
                    .write(move |context| {
                        context.execute_batch(&statement)?;
                        Ok(())
                    })
                    .await;
            }
        }
        publish(&a, &[&a]).await;
        publish(&b, &[&a]).await;
        let snapshot = frames(&a, Audience::Store).await;
        let (undone, taken_out) = without_rule_losses(&snapshot);
        removals += usize::from(taken_out > 0);
        let installed = open(&c_store).await;
        load_one(
            &installed,
            id(Audience::Store),
            snapshot.prefix.clone(),
            Cursor::new(snapshot.concat()),
        )
        .await
        .unwrap();
        assert_installed(&a, &installed, &[Audience::Store]).await;
        let found = open(&d_store).await;
        load_one(&found, id(Audience::Store), snapshot.prefix.clone(), Cursor::new(undone))
            .await
            .unwrap();
        assert_installed(&a, &found, &[Audience::Store]).await;
        for db in [a, b, installed, found] {
            db.close().await.unwrap();
        }
    }
    // Most histories leave rows the rules take out.
    assert!(removals >= 8, "only {removals} histories had removals");
}

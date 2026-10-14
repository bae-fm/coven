//! Compare retained-input rebuilds with the store-log/data model after checked fates.
use crate::pending::tests::{Record, Subject};
use crate::snapshot_write::tests::{frames_for, stream};
use crate::test_utils::tests::{column_id, insert, run_lean, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{Boundary, BoundaryCoverage, EntryId, PendingValue, SnapshotReload, StoreVersion};
use coven_format::{
    loss::LossCause, store_log::SnapshotId, value::WritePositions, write::WriteRecord,
};
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::{Audience, Operation};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn writes() -> Vec<WriteRecord> {
    let mut writes: Vec<_> = (0..6).map(|n| insert(n, n as usize)).collect();
    for n in 2..6 {
        let parent = if n == 2 { 0 } else { 1 };
        writes[n].header.had_read = WritePositions(vec![writes[parent].header.position]);
        if n >= 4 {
            let original = writes[1].parts[0].rows[0].clone();
            let row = &mut writes[n].parts[0].rows[0];
            row.row = original.row;
            row.change.generation = 1;
            let Operation::Insert(mut values) = row.change.operation.clone() else {
                unreachable!()
            };
            values.retain(|c, _| c == "a");
            row.change.operation = Operation::Update(values);
            let Operation::Insert(original) = original.change.operation else {
                unreachable!()
            };
            row.old = BTreeMap::from([("a".into(), original["a"].value.clone())]);
        }
    }
    writes
}

fn input_writes(writes: &[WriteRecord]) -> Vec<Value> {
    writes.iter().map(|w| {
        let r = &w.parts[0].rows[0];
        let (kind, columns) = match &r.change.operation {
            Operation::Insert(c) => ("insert", c.keys().map(|c| column_id(c)).collect::<Vec<_>>()),
            Operation::Update(c) => ("update", c.keys().map(|c| column_id(c)).collect()),
            Operation::Delete => ("delete", vec![]),
        };
        let key = coven_format::key::decode_key(&r.row.key).unwrap();
        let coven_format::value::Value::Text(key) = &key[0] else { unreachable!() };
        json!({"row": key.parse::<usize>().unwrap(), "generation": r.change.generation,
            "kind": kind, "columns": columns, "version": w.header.schema_version,
            "past": w.header.had_read.0.iter().map(|id| writes.iter().position(|w| w.header.position == *id).unwrap()).collect::<Vec<_>>()})
    }).collect()
}

async fn observed(db: &crate::Database, writes: &[WriteRecord], trace: &mut Vec<Value>) -> Value {
    let index = |id| writes.iter().position(|w| w.header.position == id).unwrap() as u64;
    let (rows, mut rejected) = db.inspect_writer(|db| {
        let schema = crate::schema::Schema::read(db).unwrap();
        let store = crate::merge_store::MergeStore::from_schema(db, &schema);
        let rows: Vec<_> = writes[..4].iter().enumerate().map(|(n,w)| {
            let state = store.row(&w.parts[0].rows[0].row).unwrap().state;
            let mut setters: Vec<_> = state.cells().iter().map(|(c,v)| [column_id(c) as u64, index(v.write)]).collect();
            setters.sort();
            let mut losses: Vec<_> = state.lost().iter().map(|(k,v)| [column_id(&k.column) as u64, index(k.write), v.incarnation, index(v.replaced_by)]).collect();
            losses.sort();
            json!({"row": n, "generation": state.generation(), "setters": setters, "losses": losses})
        }).collect();
        let mut rejected = Vec::new();
        crate::loss_record::visit(db, &Audience::Store, |loss| {
            if let LossCause::Excluded { write, cause } = loss.cause {
                let cause = match cause {
                    coven_format::snapshot_rows::LostWriteCause::Reset(id) => json!({"reset": id.number}),
                    coven_format::snapshot_rows::LostWriteCause::SchemaChange(v) => json!({"schema": v}),
                };
                rejected.push((index(write), cause));
            }
            Ok::<_, crate::DbError>(())
        }).unwrap();
        (rows, rejected)
    });
    rejected.sort_by_key(|(w, _)| *w);
    let rejected: Vec<_> = rejected.into_iter().map(|(w, c)| json!([w, c])).collect();
    let pending: Vec<_> = db
        .pending::<Record>()
        .await
        .unwrap()
        .iter()
        .map(|r| match r.subject() {
            Subject::Number(n) => *n,
            s => panic!(
                "unexpected pending subject {s:?} after {} observations",
                trace.len()
            ),
        })
        .collect();
    trace.push(json!([!rejected.is_empty(), pending]));
    json!({"rows":rows,"rejected":rejected,"pending":pending,"trace":trace})
}

#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_entry_effects() {
    let runner = std::env::var_os("COVEN_STORELOG_DATA_LEAN").expect("built storelogDataRunner");
    let writes = writes();
    let ids = SequentialIds::new();
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for order in [vec![0, 1, 2, 3, 4, 5], vec![1, 0, 5, 4, 3, 2]] {
        let source_store = TestStore::with_ids(&ids);
        let target_store = TestStore::with_ids(&ids);
        let source = source_store.schema(tables(), SCHEMA).await.unwrap();
        // The target's app already supports the raised version 2 (§17.1).
        let target = target_store
            .builder(
                tables(),
                vec![
                    crate::Migration::sql(1, "schema", SCHEMA),
                    crate::Migration::sql(
                        2,
                        "addition",
                        "ALTER TABLE rows ADD COLUMN retired TEXT",
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        source
            .apply_downloaded(writes[1].clone().into())
            .await
            .unwrap();
        // The target received writes 0 and 1 before any boundary; the model's
        // original state is that receipt. Writes covered by a loaded snapshot
        // but never received individually could not be rebuilt locally.
        for write in &writes[..2] {
            target.apply_downloaded(write.clone().into()).await.unwrap();
        }
        let id = |number| SnapshotId {
            audience: Audience::Store,
            device: DeviceId(99),
            number,
        };
        let reset_snapshot = frames_for(&source, id(1)).await;
        let raise_snapshot = frames_for(&target, id(2)).await;
        let entry = |number| EntryId {
            device: DeviceId(99),
            number,
        };
        target
            .record_pending(Record::dropped_entry(
                EntryId {
                    device: target.sync_state(vec![]).await.unwrap().device,
                    number: 200,
                },
                crate::DropReason::NotAllowed,
            ))
            .await
            .unwrap();
        let mut trace = Vec::new();
        let mut results = Vec::new();
        let mut cases = Vec::new();
        for (number, boundary, snapshot) in [
            (
                1,
                Boundary::Reset {
                    entry: entry(1),
                    snapshot: id(1),
                },
                reset_snapshot,
            ),
            (
                2,
                Boundary::SchemaChange(StoreVersion {
                    number: 2,
                    entry: entry(2),
                    snapshot: id(2),
                }),
                raise_snapshot,
            ),
        ] {
            target.check_boundary_fixture(boundary).await.unwrap();
            let mut reload = SnapshotReload::new(
                vec![snapshot.input()],
                order.iter().map(|i| stream(&writes[*i])).collect(),
            );
            reload.boundaries = Some(
                (1..=number)
                    .map(|n| BoundaryCoverage {
                        entry: entry(n),
                        included: WritePositions(if n == 1 {
                            vec![writes[1].header.position]
                        } else {
                            vec![writes[0].header.position, writes[1].header.position]
                        }),
                    })
                    .collect(),
            );
            target.load_snapshots(reload).await.unwrap();
            results.push(observed(&target, &writes, &mut trace).await);
            cases.push(json!({"kept": (1..=number).collect::<Vec<_>>(), "received": [0,1,2,3,4,5], "observations": [{"subject":200,"resets":[1]}]}));
        }
        for (number, kept, effective) in [
            (1, false, vec![2]),
            (2, false, vec![]),
            (1, true, vec![1]),
            (2, true, vec![1, 2]),
            (2, false, vec![1]),
            (1, false, vec![]),
        ] {
            crate::entry_effects::tests::fate(&target, entry(number), kept)
                .await
                .unwrap();
            results.push(observed(&target, &writes, &mut trace).await);
            cases.push(json!({"kept":effective,"received":[0,1,2,3,4,5],"observations":[{"subject":200,"resets":[1]}]}));
        }
        inputs.push(json!({"writes": input_writes(&writes), "original":[0,1],
            "boundaries":[{"kind":"reset","snapshot":1},{"kind":"schema","snapshot":2,"version":2}],
            "snapshots":[{"number":1,"covered":[1]},{"number":2,"covered":[0,1]}],"cases":cases}));
        expected.push(results);
        source.close().await.unwrap();
        target.close().await.unwrap();
    }
    assert_eq!(
        run_lean(&runner, &json!({"effects":inputs})),
        json!(expected)
    );
}

#[tokio::test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
async fn data_differential_entry_effects_circles() {
    use crate::store_log::tests::{checked, circle_history, keys};
    use coven_format::store_log::StoreChange;
    let runner = std::env::var_os("COVEN_STORELOG_DATA_LEAN").expect("built storelogDataRunner");
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(2));
    let history = circle_history(circle);
    let mut expected = Vec::new();
    let mut inputs = Vec::new();
    for late in [false, true] {
        let store = TestStore::new();
        let db = store.schema(vec![crate::SyncedTable::new("rows", crate::RowIdentity::IndependentUuid).audience_column("audience")],
            "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a TEXT,b TEXT);
             CREATE TABLE pins(row TEXT REFERENCES rows(id) ON DELETE CASCADE)").await.unwrap();
        for (entry, replay) in &history[..4] {
            db.apply_store_log::<Record>(checked(entry.clone()), replay.clone())
                .await
                .unwrap();
        }
        let mut write = insert(0, 0);
        write.parts[0].audience = Audience::Circle(circle);
        let row = &mut write.parts[0].rows[0];
        row.row.audience = Audience::Circle(circle);
        row.row.key = coven_format::key::encode_key(&[coven_format::value::Value::Text(
            "00000000-0000-4000-8000-000000000001".into(),
        )])
        .unwrap();
        let Operation::Insert(values) = &mut row.change.operation else {
            unreachable!()
        };
        values.get_mut("id").unwrap().value = coven_format::key::decode_key(&row.row.key)
            .unwrap()
            .remove(0);
        values.insert(
            "audience".into(),
            coven_merge::ColumnValue {
                value: coven_format::value::Value::Text(circle.to_string()),
                parents: BTreeMap::new(),
            },
        );
        let mut cases = Vec::new();
        let mut results = Vec::new();
        for stage in 0..3 {
            if stage == 1 {
                let (entry, replay) = &history[4];
                db.apply_store_log::<Record>(checked(entry.clone()), replay.clone())
                    .await
                    .unwrap();
            } else if stage == 2 {
                let mut removal = history[0].0.clone();
                removal.position.number = 5;
                removal.timestamp =
                    coven_merge::Timestamp::new(5, 0, removal.position.device).unwrap();
                removal.change = StoreChange::RemoveMember {
                    member: keys(2).signing,
                };
                let mut replay = history[3].1.clone();
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
                replay
                    .entries
                    .insert(removal.position, crate::EntryOutcome::Kept);
                replay.entries.insert(
                    history[4].0.position,
                    crate::EntryOutcome::Dropped(crate::DropReason::BeatenBy(removal.position)),
                );
                db.apply_store_log::<Record>(checked(removal), replay)
                    .await
                    .unwrap();
            }
            if stage == usize::from(late) {
                db.apply_downloaded(write.clone().into()).await.unwrap();
                // A local child can point only at a row the app sees.
                if !late {
                    crate::write::tests::sql(
                        &db,
                        "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001')",
                    )
                    .await
                    .unwrap();
                }
            }
            let (generation, setter) = db.inspect_writer(|sql| {
                let schema = crate::schema::Schema::read(sql).unwrap();
                let merged = crate::merge_store::MergeStore::from_schema(sql, &schema)
                    .row(&write.parts[0].rows[0].row)
                    .unwrap()
                    .state;
                (merged.generation(), merged.cells().contains_key("a"))
            });
            let cause = db
                .lost_values()
                .await
                .unwrap()
                .iter()
                .flat_map(|loss| match &loss.replaced_by {
                    crate::Replacement::Rules(rules) => rules.as_slice(),
                    _ => panic!("circle rule expected"),
                })
                .find_map(|rule| match rule {
                    crate::RemovalRule::DeletedCircle { entry } => {
                        assert_eq!(*entry, history[4].0.position);
                        Some(6)
                    }
                    _ => None,
                });
            let shown = crate::write::tests::count(&db, "rows") != 0;
            let child = crate::write::tests::count(&db, "pins") != 0;
            results.push(
                json!({"shown":shown,"generation":generation,"setter":setter,
                "cause":cause,"child":child}),
            );
            cases.push(json!({"arrivals": match stage { 0 => vec![], 1 => vec![6], _ => vec![6,5] }, "write":stage >= usize::from(late)}));
        }
        expected.push(results);
        inputs.push(json!({"states":cases,"children":!late}));
        db.close().await.unwrap();
    }
    assert_eq!(
        run_lean(&runner, &json!({"circles":inputs})),
        json!(expected)
    );
}

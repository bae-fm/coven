//! Compare retained-input rebuilds with the store-log/data model after checked fates.
use crate::pending::tests::{Record, Subject};
use crate::snapshot_write::tests::{frames_for, stream};
use crate::test_utils::tests::{column_id, insert, run_lean, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{Boundary, BoundaryCoverage, EntryId, PendingValue, SnapshotReload, StoreVersion};
use coven_format::{
    loss::LossCause,
    store_log::SnapshotId,
    value::WritePositions,
    write::WriteRecord,
    write_sources::{CellSource, ChangeId, WriteSources},
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
            let original_write = writes[1].header.position;
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
            // Write 1's insert established generation 1 and set `a` (Appendix D).
            let source = ChangeId {
                write: original_write,
                row: row.row.clone(),
            };
            row.sources = WriteSources {
                generation: Some(source.clone()),
                setters: BTreeMap::from([(
                    "a".into(),
                    CellSource {
                        change: source,
                        column: "a".into(),
                    },
                )]),
            };
        }
    }
    // Writes 3 and 5 were authored after reading the reset entry; the reset
    // ignores the others outside its snapshot (§19.3).
    for n in [3, 5] {
        writes[n].header.store_log_read = coven_format::value::EntryPositions(vec![EntryId {
            device: DeviceId(99),
            number: 1,
        }]);
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
            "storeRead": w.header.store_log_read.0.iter().map(|e| e.number).collect::<Vec<_>>(),
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
            if let LossCause::Excluded { write, version } = loss.cause {
                rejected.push((index(write), json!({"schema": version})));
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
        source.receive(writes[1].clone().into()).await.unwrap();
        // The target received writes 0 and 1 before any boundary; the model's
        // original state is that receipt. Writes covered by a loaded snapshot
        // but never received individually could not be rebuilt locally.
        for write in &writes[..2] {
            target.receive(write.clone().into()).await.unwrap();
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
            target
                .load_snapshots::<crate::pending::tests::Record, _, _>(reload)
                .await
                .unwrap();
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
    use crate::store_log::tests::{checked, circle_history};
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
            db.apply_store_log::<Record>(checked(entry.clone()), replay.clone(), Vec::new())
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
                db.apply_store_log::<Record>(checked(entry.clone()), replay.clone(), Vec::new())
                    .await
                    .unwrap();
            } else if stage == 2 {
                crate::store_log::tests::drop_circle_deletion(&db, circle)
                    .await
                    .unwrap();
            }
            if stage == usize::from(late) {
                db.receive(write.clone().into()).await.unwrap();
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

/// Replay changing a retirement after the write it decides, and changes built
/// on that write, have applied: rows, source verdicts, losses and positions
/// are rebuilt together from the input retained when the write landed (§9,
/// §10, B17).
mod retirement_replay {
    use crate::entry_effects::tests::fate;
    use crate::pending::tests::Record;
    use crate::tests::TestStore;
    use crate::write::tests::{count, notes, records, sql, NOTES};
    use crate::write_landing::tests::{at, dropped_reports, retire, DAY};
    use crate::{ApplyOutcome, DbError, Replacement};
    use coven_format::merge_fields;
    use coven_format::snapshot_rows::ChangeVerdict;
    use coven_foundation::id_source::{DeviceId, SequentialIds};
    use coven_merge::{Audience, WriteId};
    use std::collections::BTreeMap;

    fn rows(db: &crate::Database) -> Vec<(String, String)> {
        db.inspect_writer(|sql| {
            sql.query("SELECT id,title FROM notes ORDER BY id", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
        })
    }

    /// This device's verdict for every retained original change, by write and key.
    fn verdicts(db: &crate::Database) -> BTreeMap<(WriteId, String), ChangeVerdict> {
        db.inspect_writer(|sql| {
            let mut all = BTreeMap::new();
            let mut last = BTreeMap::<DeviceId, u64>::new();
            for id in sql
                .query("SELECT write_id FROM _coven_authored_changes", [], |r| {
                    r.get::<_, Vec<u8>>(0)
                })
                .unwrap()
            {
                let id = merge_fields::decode_write_id(&id).unwrap();
                let number = last.entry(id.device).or_default();
                *number = id.number.max(*number);
            }
            let covered = coven_format::value::WritePositions(
                last.into_iter()
                    .map(|(device, number)| WriteId { device, number })
                    .collect(),
            );
            crate::write_sources::visit_evidence(sql, &Audience::Store, &covered, |change| {
                let key = coven_format::key::decode_key(&change.change.row.key).unwrap();
                all.insert((change.header.position, format!("{key:?}")), change.verdict);
                Ok::<_, DbError>(())
            })
            .unwrap();
            all
        })
    }

    fn verdict(db: &crate::Database, write: WriteId, key: &str) -> ChangeVerdict {
        let wanted = format!("{:?}", vec![coven_format::value::Value::Text(key.into())]);
        verdicts(db)
            .into_iter()
            .find(|((w, k), _)| *w == write && *k == wanted)
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("no verdict for {write:?} {key}"))
    }

    fn dependency_causes(losses: &[crate::LostValue]) -> Vec<(WriteId, Vec<WriteId>)> {
        let mut causes: Vec<_> = losses
            .iter()
            .map(|loss| match &loss.replaced_by {
                Replacement::DataDependency { write, sources } => {
                    (*write, sources.iter().map(|s| s.write).collect::<Vec<_>>())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        causes.sort_by_key(|(write, _)| format!("{write:?}"));
        causes
    }

    fn reloading(db: &crate::Database) -> bool {
        db.inspect_writer(|sql| crate::entry_effects::reloading(sql, &Audience::Store).unwrap())
    }

    struct History {
        _stores: Vec<TestStore>,
        dbs: Vec<crate::Database>,
        creation: coven_format::write::WriteRecord,
        ben: Vec<coven_format::write::WriteRecord>,
        delete: coven_format::write::WriteRecord,
    }

    /// B17's history: Ana creates a note; Ben, who read it, edits it and renames
    /// his own playlist in one write; Carol, who read it, deletes it. Device 3
    /// and 4 are receivers.
    async fn history() -> History {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..5).map(|_| TestStore::with_ids(&ids)).collect();
        let mut dbs = Vec::new();
        for store in &stores {
            dbs.push(store.schema(notes(), NOTES).await.unwrap());
        }
        let [ana, ben, carol, ..] = &dbs[..] else {
            unreachable!()
        };
        sql(ana, "INSERT INTO notes VALUES('note','draft','body')")
            .await
            .unwrap();
        let creation = records(ana).remove(0);
        sql(ben, "INSERT INTO notes VALUES('playlist','Mix','')")
            .await
            .unwrap();
        for reader in [ben, carol] {
            reader.receive(creation.clone().into()).await.unwrap();
        }
        sql(
            ben,
            "UPDATE notes SET title='edited' WHERE id='note'; UPDATE notes SET title='Road trip' WHERE id='playlist'",
        )
        .await
        .unwrap();
        sql(carol, "DELETE FROM notes WHERE id='note'")
            .await
            .unwrap();
        let ben = records(ben);
        let delete = records(carol).remove(0);
        History {
            _stores: stores,
            dbs,
            creation,
            ben,
            delete,
        }
    }

    /// The removal was temporarily dropped when Ana's creation landed one second
    /// outside its window, so the creation applied and Ben's and Carol's changes
    /// merged on it. Replay keeping the removal again rebuilds in its own
    /// transaction: the creation is dropped, both dependent changes are frozen
    /// losses naming it, and Ben's playlist rename applies. Both receive orders
    /// agree; dropping the removal once more restores the ordinary merge.
    #[tokio::test]
    async fn a_returning_retirement_freezes_changes_built_on_the_write() {
        let h = history().await;
        let created = h.creation.header.position;
        let edit = h.ben[1].header.position;
        let deleted = h.delete.header.position;
        let mut results = Vec::new();
        for (target, delete_first) in [(&h.dbs[3], false), (&h.dbs[4], true)] {
            let removal = retire(target, created.device, 1, "device", 0).await;
            fate(target, removal, false).await.unwrap();
            assert_eq!(
                target
                    .apply_downloaded::<Record>(
                        h.creation.clone().into(),
                        crate::test_utils::published(at(30 * DAY + 1))
                    )
                    .await
                    .unwrap(),
                ApplyOutcome::Applied
            );
            target.receive(h.ben[0].clone().into()).await.unwrap();
            let (first, second) = if delete_first {
                (&h.delete, &h.ben[1])
            } else {
                (&h.ben[1], &h.delete)
            };
            target.receive(first.clone().into()).await.unwrap();
            target.receive(second.clone().into()).await.unwrap();
            // While dropped: the delete wins and Ben's title is an ordinary loss.
            assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
            let losses = target.lost_values().await.unwrap();
            assert_eq!(losses.len(), 1);
            assert_eq!(losses[0].replaced_by, Replacement::Write(deleted));

            fate(target, removal, true).await.unwrap();
            assert!(!reloading(target), "rebuilt in the replay, not reloaded");
            assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
            let causes = dependency_causes(&target.lost_values().await.unwrap());
            let mut expected = vec![
                (edit, vec![created, created]),
                // Generation, then the id, title and body setters it replaced.
                (deleted, vec![created; 4]),
            ];
            expected.sort_by_key(|(write, _)| format!("{write:?}"));
            assert_eq!(causes, expected);
            assert_eq!(verdict(target, created, "note"), ChangeVerdict::Dropped);
            assert!(matches!(
                verdict(target, edit, "note"),
                ChangeVerdict::Dependent(_)
            ));
            assert_eq!(verdict(target, edit, "playlist"), ChangeVerdict::Applied);
            assert!(matches!(
                verdict(target, deleted, "note"),
                ChangeVerdict::Dependent(_)
            ));
            assert_eq!(
                dropped_reports(target)
                    .iter()
                    .map(|r| r.dropped_write().unwrap())
                    .collect::<Vec<_>>(),
                vec![created]
            );
            let positions = target.sync_state(vec![]).await.unwrap().positions;
            assert!([created, edit, deleted]
                .iter()
                .all(|w| positions.covers(*w)));
            // App writes proceed: nothing waits for a reload.
            sql(target, "INSERT INTO notes VALUES('other','t','b')")
                .await
                .unwrap();
            sql(target, "DELETE FROM notes WHERE id='other'")
                .await
                .unwrap();
            results.push((rows(target), causes));

            fate(target, removal, false).await.unwrap();
            let losses = target.lost_values().await.unwrap();
            assert_eq!(losses.len(), 1);
            assert_eq!(losses[0].replaced_by, Replacement::Write(deleted));
            assert_eq!(verdict(target, created, "note"), ChangeVerdict::Applied);
            assert_eq!(verdict(target, edit, "note"), ChangeVerdict::Applied);
            assert!(dropped_reports(target).is_empty());
        }
        assert_eq!(results[0], results[1]);
    }

    /// Setter-only dependence: Carol created the note, Ana's late write retitled
    /// it and Ben, who read both, retitled it again. Ben's change used Carol's
    /// generation and Ana's title setter; only the setter fails, so his change is
    /// a loss naming Ana's write alone and Carol's title returns (§8).
    #[tokio::test]
    async fn a_change_replacing_only_a_late_setter_is_frozen() {
        let h = history().await;
        let [ana, ben, carol, target, _] = &h.dbs[..] else {
            unreachable!()
        };
        sql(carol, "INSERT INTO notes VALUES('song','carol','')")
            .await
            .unwrap();
        let insert = records(carol).remove(1);
        for reader in [ana, ben] {
            reader.receive(h.delete.clone().into()).await.unwrap();
            reader.receive(insert.clone().into()).await.unwrap();
        }
        sql(ana, "UPDATE notes SET title='ana' WHERE id='song'")
            .await
            .unwrap();
        let retitle = records(ana).remove(1);
        ben.receive(retitle.clone().into()).await.unwrap();
        sql(ben, "UPDATE notes SET title='ben' WHERE id='song'")
            .await
            .unwrap();
        let again = records(ben).pop().unwrap();
        assert_eq!(
            again.parts[0].rows[0]
                .sources
                .generation
                .as_ref()
                .map(|s| s.write),
            Some(insert.header.position)
        );
        let removal = retire(target, retitle.header.position.device, 1, "device", 0).await;
        fate(target, removal, false).await.unwrap();
        target.receive(h.creation.clone().into()).await.unwrap();
        target.receive(h.delete.clone().into()).await.unwrap();
        target.receive(insert.clone().into()).await.unwrap();
        target
            .apply_downloaded::<Record>(
                retitle.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        target.receive(h.ben[0].clone().into()).await.unwrap();
        target.receive(h.ben[1].clone().into()).await.unwrap();
        target.receive(again.clone().into()).await.unwrap();
        let song = |db: &crate::Database| {
            rows(db)
                .into_iter()
                .find(|(id, _)| id == "song")
                .map(|(_, title)| title)
        };
        assert_eq!(song(target).as_deref(), Some("ben"));
        fate(target, removal, true).await.unwrap();
        assert_eq!(song(target).as_deref(), Some("carol"));
        let mut losses = target.lost_values().await.unwrap();
        // Ben's note edit lost to Carol's delete as usual.
        losses.retain(|loss| matches!(loss.replaced_by, Replacement::DataDependency { .. }));
        assert_eq!(
            dependency_causes(&losses),
            vec![(again.header.position, vec![retitle.header.position])]
        );
        // The creation was published within the epoch-stored window: only Ana's
        // retitle is late.
        assert_eq!(
            verdict(target, h.creation.header.position, "note"),
            ChangeVerdict::Applied
        );
        fate(target, removal, false).await.unwrap();
        assert_eq!(song(target).as_deref(), Some("ben"));
        assert!(target
            .lost_values()
            .await
            .unwrap()
            .iter()
            .all(|loss| !matches!(loss.replaced_by, Replacement::DataDependency { .. })));
    }

    /// A late write's window precedes its schema verdict: it gains no schema
    /// loss. Admitted again, the same excluded write is a frozen schema loss
    /// (§10, §17.1).
    #[tokio::test]
    async fn a_late_excluded_write_has_no_schema_loss_until_replay_admits_it() {
        let ids = SequentialIds::new();
        let (source_store, target_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let source = source_store.schema(notes(), NOTES).await.unwrap();
        let target = target_store.schema(notes(), NOTES).await.unwrap();
        sql(&source, "INSERT INTO notes VALUES('n','title','body')")
            .await
            .unwrap();
        let mut write = records(&source).remove(0);
        write.header.schema_version = 0;
        target.exclude_version_zero().await;
        // As a completed reload of the raise leaves the audience: its data is the
        // raise's, so the write's landing captures an input under that selection.
        target.inspect_writer(|sql| {
            crate::entry_inputs::forget(sql, &Audience::Store, None).unwrap();
        });
        let removal = retire(&target, write.header.position.device, 1, "device", 0).await;
        target
            .apply_downloaded::<Record>(
                write.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        assert!(target.lost_values().await.unwrap().is_empty());
        fate(&target, removal, false).await.unwrap();
        let losses = target.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert!(matches!(
            losses[0].replaced_by,
            Replacement::SchemaChange { .. }
        ));
        assert_eq!(count(&target, "notes"), 0);
        fate(&target, removal, true).await.unwrap();
        assert!(target.lost_values().await.unwrap().is_empty());
        assert_eq!(count(&target, "notes"), 0);
    }

    /// Dismissing the dependent losses is a later local write. Restoring the
    /// write and dropping it again rebuilds those losses from the originals; the
    /// retained dismissal applies to them again, and no verdict changes with it.
    #[tokio::test]
    async fn dismissed_dependency_losses_stay_dismissed_through_rebuilds() {
        let h = history().await;
        let target = &h.dbs[3];
        let created = h.creation.header.position;
        let edit = h.ben[1].header.position;
        let removal = retire(target, created.device, 1, "device", 0).await;
        fate(target, removal, false).await.unwrap();
        target
            .apply_downloaded::<Record>(
                h.creation.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        target.receive(h.ben[0].clone().into()).await.unwrap();
        target.receive(h.ben[1].clone().into()).await.unwrap();
        fate(target, removal, true).await.unwrap();
        let losses = target.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        target.dismiss_lost_values(&losses).await.unwrap();
        assert!(target.lost_values().await.unwrap().is_empty());
        fate(target, removal, false).await.unwrap();
        assert!(rows(target).contains(&("note".into(), "edited".into())));
        fate(target, removal, true).await.unwrap();
        assert!(target.lost_values().await.unwrap().is_empty());
        assert!(matches!(
            verdict(target, edit, "note"),
            ChangeVerdict::Dependent(_)
        ));
        assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
    }

    /// Once the deciding removal is final the late verdict cannot change: its
    /// retained inputs and landing go, but the source verdict stays, so a later
    /// change built on the dropped creation is still a dependency loss.
    #[tokio::test]
    async fn released_inputs_keep_the_dropped_source_dropped() {
        let h = history().await;
        let target = &h.dbs[3];
        let created = h.creation.header.position;
        retire(target, created.device, 1, "device", 0).await;
        target
            .apply_downloaded::<Record>(
                h.creation.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        target
            .advance_store_log_horizon(target.store_log().await.unwrap(), at(1), at(1))
            .await
            .unwrap();
        let retained = target.inspect_writer(|sql| {
            sql.query_row(
                "SELECT (SELECT count(*) FROM _coven_data_inputs),(SELECT count(*) FROM _coven_write_landings)",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap()
        });
        assert_eq!(retained, (0, 0));
        target.receive(h.ben[0].clone().into()).await.unwrap();
        target.receive(h.ben[1].clone().into()).await.unwrap();
        assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
        assert_eq!(
            dependency_causes(&target.lost_values().await.unwrap()),
            vec![(h.ben[1].header.position, vec![created, created])]
        );
    }

    /// A rebuild that fails rolls the whole replay back: the retirement keeps its
    /// previous outcome and rows, verdicts, losses and landings are unchanged,
    /// also after reopening. With the input repaired the same replay installs.
    #[tokio::test]
    async fn a_failed_rebuild_leaves_the_previous_state_and_reports() {
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let target_store = TestStore::with_ids(&ids);
        let source = source_store.schema(notes(), NOTES).await.unwrap();
        let mut target = target_store.schema(notes(), NOTES).await.unwrap();
        sql(&source, "INSERT INTO notes VALUES('n','title','body')")
            .await
            .unwrap();
        let write = records(&source).remove(0);
        let removal = retire(&target, write.header.position.device, 1, "device", 0).await;
        fate(&target, removal, false).await.unwrap();
        target
            .apply_downloaded::<Record>(
                write.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        type Frame = (String, Vec<u8>, i64, Vec<u8>);
        let frame: Frame = target.inspect_writer(|sql| {
            sql.query_row(
                "SELECT audience,timestamp,ordinal,frame FROM _coven_write_input_frames LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap()
        });
        let damage = |db: &crate::Database, bytes: Vec<u8>| {
            db.inspect_writer(|sql| {
                sql.internal_execute(
                    "UPDATE _coven_write_input_frames SET frame=?4 WHERE audience=?1 AND timestamp=?2 AND ordinal=?3",
                    rusqlite::params![frame.0, frame.1, frame.2, bytes],
                )
                .unwrap();
            })
        };
        damage(&target, vec![0xff]);
        let state = |db: &crate::Database| {
            (
                count(db, "notes"),
                late(db, write.header.position),
                verdict(db, write.header.position, "n"),
                db.inspect_writer(|sql| {
                    sql.query_row(
                        "SELECT outcome FROM _coven_store_log WHERE device=?1 AND number=?2",
                        rusqlite::params![
                            removal.device.0.to_be_bytes().as_slice(),
                            removal.number.to_be_bytes().as_slice()
                        ],
                        |r| r.get::<_, String>(0),
                    )
                    .unwrap()
                }),
            )
        };
        let before = state(&target);
        assert_eq!(before.0, 1);
        assert!(fate(&target, removal, true).await.is_err());
        assert_eq!(state(&target), before);
        target.close().await.unwrap();
        target = target_store.schema(notes(), NOTES).await.unwrap();
        assert_eq!(state(&target), before);
        damage(&target, frame.3.clone());
        fate(&target, removal, true).await.unwrap();
        assert_eq!(count(&target, "notes"), 0);
        assert!(late(&target, write.header.position));
        assert_eq!(
            verdict(&target, write.header.position, "n"),
            ChangeVerdict::Dropped
        );
        target.close().await.unwrap();
        source.close().await.unwrap();
    }

    fn late(db: &crate::Database, write: WriteId) -> bool {
        db.inspect_writer(|sql| crate::write_landing::late(sql, write).unwrap())
    }

    /// A snapshot made under another retirement verdict for a write this device
    /// judged itself cannot be rebuilt here: its contributions would count a
    /// write this device excludes. Loading it fails and changes nothing; a
    /// snapshot made under the same verdict loads (§10, §15).
    #[tokio::test]
    async fn a_snapshot_judging_a_held_write_differently_does_not_load() {
        use crate::snapshot_write::tests::{frames, id, load_one};
        let h = history().await;
        let [_, admitted, _, target, _] = &h.dbs[..] else {
            unreachable!()
        };
        let created = h.creation.header.position;
        // `admitted` received the creation within the epoch window, so applied it.
        let applied = frames(admitted, Audience::Store).await;
        let fresh = TestStore::new();
        let dropping = fresh.schema(notes(), NOTES).await.unwrap();
        retire(&dropping, created.device, 1, "device", 0).await;
        dropping
            .apply_downloaded::<Record>(
                h.creation.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        let dropped = frames(&dropping, Audience::Store).await;

        retire(target, created.device, 1, "device", 0).await;
        target
            .apply_downloaded::<Record>(
                h.creation.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        assert_eq!(count(target, "notes"), 0);
        let error = load_one(
            target,
            id(Audience::Store),
            applied.prefix.clone(),
            std::io::Cursor::new(applied.concat()),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                DbError::Snapshot(crate::SnapshotError::RetirementVerdict { write }) if write == created
            ),
            "{error:?}"
        );
        assert_eq!(count(target, "notes"), 0);
        assert_eq!(verdict(target, created, "note"), ChangeVerdict::Dropped);
        load_one(
            target,
            id(Audience::Store),
            dropped.prefix.clone(),
            std::io::Cursor::new(dropped.concat()),
        )
        .await
        .unwrap();
        assert_eq!(count(target, "notes"), 0);
        assert_eq!(verdict(target, created, "note"), ChangeVerdict::Dropped);
        dropping.close().await.unwrap();
    }
}

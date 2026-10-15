use crate::test_utils::tests::*;
use crate::tests::TestStore;
use crate::write::tests::{count, records};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::value::{Value as Scalar, WritePositions};
use coven_format::write::WriteRecord;
use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, ColumnValue, Operation, RowId};
use coven_merge::{ForeignKey, Parent, ReferenceValue, RemovalRow};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;

const MIGRATION_SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a INTEGER CHECK(a>=0),b TEXT,parent TEXT REFERENCES rows(id) ON DELETE CASCADE,UNIQUE(audience,b)); CREATE INDEX rows_parent ON rows(parent)";
fn migration_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::IndependentUuid).audience_column("audience")]
}
fn circle_id() -> CircleId {
    CircleId(uuid::Uuid::from_u128(1))
}

#[derive(Clone, Copy)]
enum Finality {
    NonFinal,
    Final,
}
#[derive(Clone)]
struct MigrationHistory {
    writes: Vec<WriteRecord>,
    deletion: Option<Finality>,
}
fn migration_insert(n: u64, key: usize, audience: Audience) -> WriteRecord {
    let mut write = insert(n, key);
    let key =
        uuid::Uuid::from_u128(0x00000000000040008000000000000000 + key as u128 + 1).to_string();
    let row = &mut write.parts[0].rows[0];
    row.row.key = coven_format::key::encode_key(&[Scalar::Text(key.clone())]).unwrap();
    row.row.audience = audience.clone();
    let Operation::Insert(cols) = &mut row.change.operation else {
        unreachable!()
    };
    cols.get_mut("id").unwrap().value = Scalar::Text(key);
    cols.get_mut("a").unwrap().value = Scalar::Integer(1);
    cols.insert(
        "parent".into(),
        ColumnValue {
            value: Scalar::Null,
            parents: BTreeMap::new(),
        },
    );
    cols.insert(
        "audience".into(),
        ColumnValue {
            value: Scalar::Text(crate::write_encoding::audience_text(&audience)),
            parents: BTreeMap::new(),
        },
    );
    write.parts[0].audience = audience;
    write
}
impl MigrationHistory {
    fn generate(rng: &mut Generator) -> Self {
        let mut writes = Vec::<WriteRecord>::new();
        for n in 0..20 {
            let audience = if rng.pick(3) == 0 {
                Audience::Store
            } else {
                Audience::Circle(circle_id())
            };
            let mut write = migration_insert(n, rng.pick(8), audience.clone());
            let change = &mut write.parts[0].rows[0];
            let Operation::Insert(cols) = &mut change.change.operation else {
                unreachable!()
            };
            cols.get_mut("a").unwrap().value = Scalar::Integer(rng.pick(4) as i64 - 1);
            cols.get_mut("b").unwrap().value = Scalar::Text(rng.pick(4).to_string());
            let parents: Vec<_> = writes
                .iter()
                .filter(|w| {
                    let parent = &w.parts[0].rows[0].row;
                    parent != &change.row
                        && (parent.audience == Audience::Store || parent.audience == audience)
                })
                .collect();
            if !parents.is_empty() && rng.pick(2) == 0 {
                let source = parents[rng.pick(parents.len())];
                let parent = source.parts[0].rows[0].row.clone();
                cols.insert(
                    "parent".into(),
                    ColumnValue {
                        value: coven_format::key::decode_key(&parent.key)
                            .unwrap()
                            .remove(0),
                        parents: BTreeMap::from([(
                            ForeignKey::new(["parent"], "rows", ["id"]),
                            Parent {
                                row: parent,
                                generation: 1,
                            },
                        )]),
                    },
                );
                let mut past: BTreeSet<_> = source.header.had_read.0.iter().copied().collect();
                past.insert(source.header.position);
                write.header.had_read = WritePositions(past.into_iter().collect());
            }
            writes.push(write);
        }
        Self {
            writes: rng.order(&writes),
            deletion: Some(if rng.pick(3) == 0 {
                Finality::Final
            } else {
                Finality::NonFinal
            }),
        }
    }
    fn valid(&self) -> bool {
        ReloadHistory {
            writes: self.writes.clone(),
            covered: BTreeSet::new(),
            reset: false,
        }
        .valid()
    }
    fn reductions(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        for i in 0..self.writes.len() {
            let mut candidate = self.clone();
            let removed = candidate.writes.remove(i).header.position;
            for w in &mut candidate.writes {
                w.header.had_read.0.retain(|id| *id != removed);
            }
            candidates.push(candidate);
            for column in ["a", "b", "parent"] {
                let mut candidate = self.clone();
                let Operation::Insert(cols) =
                    &mut candidate.writes[i].parts[0].rows[0].change.operation
                else {
                    unreachable!()
                };
                if column == "parent" {
                    let parent = cols.get_mut(column).unwrap();
                    if parent.value != Scalar::Null {
                        *parent = ColumnValue {
                            value: Scalar::Null,
                            parents: BTreeMap::new(),
                        };
                        candidates.push(candidate);
                    }
                } else if cols.remove(column).is_some() {
                    candidates.push(candidate);
                }
            }
        }
        if self.deletion.is_some() {
            candidates.push(Self {
                deletion: None,
                ..self.clone()
            });
        }
        candidates
    }
}

// Read the actual SQL predicates, resolved references and claims. Only the
// finality/entry selection is supplied, as it is to the Lean model's theorem.
fn migration_input(db: &Database, history: &MigrationHistory) -> (Value, Vec<(RowId, u64)>) {
    db.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &app);
        let deleted = crate::store_log_tables::deleted_circles(db)?;
        let updates = BTreeMap::new();
        let view = crate::removal_view::DatabaseRemovalView::new(
            db, &store, schema, &app, &updates, &deleted, None,
        )?;
        let mut identities: BTreeSet<_> = history
            .writes
            .iter()
            .flat_map(|w| &w.parts)
            .flat_map(|p| &p.rows)
            .map(|r| r.row.clone())
            .collect();
        for r in identities.clone() {
            for reading in view.evaluated(&r)?.readings.values() {
                if let ReferenceValue::Original { parent, .. } = reading {
                    identities.insert(parent.row.clone());
                }
            }
        }
        let rows: Vec<_> = identities.into_iter().collect();
        let mut groups = BTreeMap::new();
        let mut input_rows = Vec::new();
        let mut entries = Vec::new();
        let mut before = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            let evaluated = view.evaluated(row)?;
            before.push((row.clone(), evaluated.facts.generation()));
            let refs: Vec<_> = evaluated
                .readings
                .values()
                .filter_map(|reading| match reading {
                    ReferenceValue::Original { parent, stale } => Some(json!({
                        "parent": rows.iter().position(|r| *r == parent.row).unwrap(),
                        "stale": stale
                    })),
                    ReferenceValue::Null => None,
                })
                .collect();
            let mut claims = Vec::new();
            for (constraint, claim) in &evaluated.constraints.unique {
                let group = coven_merge::Group::Claim {
                    table: row.table.clone(),
                    audience: row.audience.clone(),
                    constraint: constraint.clone(),
                    value: claim.value.clone(),
                };
                let next = groups.len();
                let value = *groups.entry(group).or_insert(next);
                claims.push(json!({
                    "con":{"terms":constraint.terms,"partial":constraint.partial},
                    "value":value,"ts":stamp(claim.timestamp).to_string(),"other":false
                }));
            }
            let mut deleted = false;
            if let RemovalRow::Present {
                started,
                deleted_circle,
                ..
            } = evaluated.facts {
                deleted = deleted_circle.is_some();
                if deleted {
                    entries.push(json!([i, 1]));
                }
                let next = groups.len();
                let value = *groups.entry(coven_merge::Group::Key {
                    table: row.table.clone(),
                    key: row.key.clone(),
                }).or_insert(next);
                let ts = if row.audience == Audience::Store {
                    0
                } else {
                    stamp(started) + 1
                };
                claims.push(json!({
                    "con":{"terms":[],"partial":null},"value":value,
                    "ts":ts.to_string(),"other":true
                }));
            }
            input_rows.push(json!({"id":i,"present":evaluated.facts.present(),"refs":refs,
                "check":!evaluated.constraints.failed_checks.is_empty(),"deleted":deleted,"claims":claims,"rank":i}));
        }
        let final_entries = match history.deletion {
            Some(Finality::Final) => vec![1],
            Some(Finality::NonFinal) | None => Vec::new(),
        };
        Ok::<_, crate::DbError>((json!({
            "mode":"migration",
            "rows":input_rows,"entries":entries,"final":final_entries
        }), before))
    }).unwrap()
}
struct Comparison {
    input: Value,
    rust: Value,
}
async fn comparison(history: &MigrationHistory) -> Comparison {
    assert!(history.valid(), "invalid migration history");
    let store = TestStore::new();
    let db = store
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
    for w in &history.writes {
        assert_eq!(
            db.receive(w.clone().into()).await.unwrap(),
            crate::ApplyOutcome::Applied
        );
    }
    if let Some(finality) = history.deletion {
        crate::store_log::tests::delete_circle(&db, circle_id())
            .await
            .unwrap();
        // Replay supplies finality: a final deletion lies before the horizon.
        if let Finality::Final = finality {
            let second =
                coven_format::value::StorageTime::since_epoch(std::time::Duration::from_secs(1));
            db.advance_store_log_horizon(db.store_log().await.unwrap(), second, second)
                .await
                .unwrap();
        }
    }
    let (input, before) = migration_input(&db, history);
    db.close().await.unwrap();
    // An index change runs the real breaking-migration path without changing
    // app values. Observe its row selection, independently of deletion format.
    let db = store
        .builder(
            migration_tables(),
            vec![
                crate::Migration::sql(1, "rows", MIGRATION_SCHEMA),
                crate::Migration::sql(2, "index", "CREATE INDEX rows_a ON rows(a)"),
            ],
        )
        .open()
        .await
        .unwrap();
    let rust = db.inspect_writer(|db| {
        let schema = crate::schema::Schema::read(db).unwrap();
        let store = crate::merge_store::MergeStore::from_schema(db, &schema);
        json!(before
            .iter()
            .enumerate()
            .map(|(i, (row, generation))| {
                let after = store.row(row).unwrap().state;
                // A selected row changes generation: the migration write's
                // ordinary delete at it (§17.1).
                json!({"id":i,"selected":after.generation()!=*generation})
            })
            .collect::<Vec<_>>())
    });
    db.close().await.unwrap();
    Comparison { input, rust }
}
async fn mismatch(runner: &OsStr, history: &MigrationHistory) -> bool {
    let case = comparison(history).await;
    case.rust != run_lean(runner, &case.input)
}
async fn reduce(runner: &OsStr, mut history: MigrationHistory) -> MigrationHistory {
    loop {
        let mut smaller = None;
        for candidate in history.reductions() {
            if candidate.valid() && mismatch(runner, &candidate).await {
                smaller = Some(candidate);
                break;
            }
        }
        match smaller {
            Some(candidate) => history = candidate,
            None => return history,
        }
    }
}

#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh runs the migration selection histories"]
async fn migration_differential_histories() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    for seed in 0..128 {
        let mut rng = Generator::new(seed + 50_000);
        let history = MigrationHistory::generate(&mut rng);
        for prefix in [history.writes.len(), 1 + rng.pick(history.writes.len())] {
            let mut case = history.clone();
            case.writes.truncate(prefix);
            for _ in 0..3 {
                case.writes = rng.order(&case.writes);
                if mismatch(&runner, &case).await {
                    let reduced = reduce(&runner, case).await;
                    let result = comparison(&reduced).await;
                    panic!("migration differential, seed {seed}\nreduced writes: {:?}\nremoval inputs: {}\nRust: {}\nLean: {}",
                        reduced.writes,result.input,result.rust,run_lean(&runner,&result.input));
                }
            }
        }
    }
}

/// B16's former counterexample: one key-only row in a circle whose deletion
/// is not final. Both sides leave it out of the migration; removing the row
/// or the deletion, or making the deletion final, also agrees.
#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh runs the migration agreement"]
async fn migration_differential_agreement() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let mut write = migration_insert(15, 0, Audience::Circle(circle_id()));
    let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    // Inserts must supply the key, audience and reference cells. The null
    // parent contributes no reference and there are no competing claims.
    columns.retain(|c, _| matches!(c.as_str(), "id" | "audience" | "parent"));
    let history = MigrationHistory {
        writes: vec![write],
        deletion: Some(Finality::NonFinal),
    };
    let case = comparison(&history).await;
    assert_eq!(case.rust, json!([{"id":0,"selected":false}]));
    assert_eq!(run_lean(&runner, &case.input), case.rust);
    let mut empty = history.clone();
    empty.writes.clear();
    assert!(!mismatch(&runner, &empty).await);
    let mut no_deletion = history.clone();
    no_deletion.deletion = None;
    assert!(!mismatch(&runner, &no_deletion).await);
    let mut final_deletion = history;
    final_deletion.deletion = Some(Finality::Final);
    let case = comparison(&final_deletion).await;
    assert_eq!(case.rust, json!([{"id":0,"selected":true}]));
    assert_eq!(run_lean(&runner, &case.input), case.rust);
}

fn generations(db: &Database, row: &RowId) -> Vec<(u64, coven_merge::WriteId)> {
    db.inspect_writer_schema(|db, schema| {
        let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema);
        Ok::<_, crate::DbError>(
            store
                .row(row)?
                .state
                .generations()
                .iter()
                .map(|(g, w)| (*g, *w))
                .collect(),
        )
    })
    .unwrap()
}

fn setters(loss: &crate::LostValue) -> Vec<(String, coven_merge::WriteId)> {
    let mut cells: Vec<_> = match &loss.lost {
        crate::Lost::Cell(cell) => vec![(cell.column.clone(), cell.set_by)],
        crate::Lost::Row(cells) => cells.iter().map(|c| (c.column.clone(), c.set_by)).collect(),
    };
    cells.sort();
    cells
}

fn row_of(write: &WriteRecord) -> RowId {
    write.parts[0].rows[0].row.clone()
}

fn migrated(store: &TestStore, converter: bool) -> crate::DatabaseBuilder {
    let mut index = crate::Migration::sql(2, "index", "CREATE INDEX rows_a ON rows(a)");
    if converter {
        index = index.writes(|_| Ok(()));
    }
    store.builder(
        migration_tables(),
        vec![crate::Migration::sql(1, "rows", MIGRATION_SCHEMA), index],
    )
}

/// §17.1: a circle row and its child hidden only by a non-final circle
/// deletion are left out of the migration write, keeping their merge records
/// and removal records; a store row losing a unique claim independently is
/// deleted at its generation with one frozen whole-row loss naming the
/// migration write and its original setter. When the deletion drops, the
/// parent and child return; the deleted row stays deleted (B12).
#[tokio::test]
async fn a_non_final_circle_deletion_leaves_its_rows_out_of_the_migration() {
    use crate::Replacement;
    let parent = migration_insert(0, 0, Audience::Circle(circle_id()));
    let mut child = migration_insert(1, 1, Audience::Circle(circle_id()));
    let Operation::Insert(columns) = &mut child.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.insert(
        "parent".into(),
        ColumnValue {
            value: coven_format::key::decode_key(&row_of(&parent).key)
                .unwrap()
                .remove(0),
            parents: BTreeMap::from([(
                ForeignKey::new(["parent"], "rows", ["id"]),
                Parent {
                    row: row_of(&parent),
                    generation: 1,
                },
            )]),
        },
    );
    child.header.had_read = WritePositions(vec![parent.header.position]);
    let mut first = migration_insert(2, 2, Audience::Store);
    let mut second = migration_insert(3, 3, Audience::Store);
    for write in [&mut first, &mut second] {
        let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
            unreachable!()
        };
        columns.get_mut("b").unwrap().value = Scalar::Text("same".into());
    }
    let store = TestStore::new();
    let db = store
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
    for write in [&parent, &child, &first, &second] {
        assert_eq!(
            db.receive(write.clone().into()).await.unwrap(),
            crate::ApplyOutcome::Applied
        );
    }
    crate::store_log::tests::delete_circle(&db, circle_id())
        .await
        .unwrap();
    assert_eq!(count(&db, "rows"), 1);
    let hidden = db.lost_values().await.unwrap();
    assert_eq!(hidden.len(), 3);
    db.close().await.unwrap();
    let db = migrated(&store, false).open().await.unwrap();
    let marker = records(&db).last().unwrap().header.position;
    for write in [&parent, &child] {
        assert_eq!(
            generations(&db, &row_of(write)),
            [(1, write.header.position)]
        );
    }
    assert_eq!(
        generations(&db, &row_of(&second)),
        [(1, second.header.position), (2, marker)]
    );
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 3, "{losses:#?}");
    let frozen: Vec<_> = losses
        .iter()
        .filter(|l| matches!(l.replaced_by, Replacement::SchemaChange { version: 2 }))
        .collect();
    assert_eq!(frozen.len(), 1);
    assert!(setters(frozen[0])
        .iter()
        .all(|(_, write)| *write == second.header.position));
    assert_eq!(
        losses
            .iter()
            .filter(|l| matches!(l.replaced_by, Replacement::Rules(_)))
            .count(),
        2,
        "the provisional rows keep their removal records"
    );
    let deletion = crate::store_log::tests::circle_history(circle_id())
        .last()
        .unwrap()
        .0
        .position;
    drop_entry(&db, deletion, circle_id()).await;
    assert_eq!(count(&db, "rows"), 3, "parent and child return");
    assert_eq!(
        generations(&db, &row_of(&second)),
        [(1, second.header.position), (2, marker)]
    );
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    assert!(matches!(
        losses[0].replaced_by,
        Replacement::SchemaChange { version: 2 }
    ));
    db.close().await.unwrap();
}

/// After the migration deleted a hidden row, a late edit whose sources
/// survive is converted, applies at the old generation and loses to the
/// retained generation delete; an edit built on a write outside its own
/// retirement window is a dependency loss; the late write itself leaves
/// nothing. A device that loads the raise's snapshot and then receives the
/// same writes reaches the same rows, verdicts and losses (§8.3, §10, §17.1).
#[tokio::test]
async fn late_edits_to_a_migration_deleted_row_lose_to_its_delete() {
    use crate::write::tests::sql;
    use crate::Replacement;
    const DAY: u64 = 24 * 60 * 60;
    let ids = coven_foundation::id_source::SequentialIds::new();
    let stores: Vec<_> = (0..6).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores[..5] {
        dbs.push(
            store
                .schema(migration_tables(), MIGRATION_SCHEMA)
                .await
                .unwrap(),
        );
    }
    let [ana, carol, late, mia, target] = &dbs[..] else {
        unreachable!()
    };
    sql(ana, "INSERT INTO rows(id,audience,a,b) VALUES('00000000-0000-4000-8000-000000000001','store',1,'x')")
        .await
        .unwrap();
    sql(carol, "INSERT INTO rows(id,audience,a,b) VALUES('00000000-0000-4000-8000-000000000002','store',1,'x')")
        .await
        .unwrap();
    let kept = records(ana).remove(0);
    let created = records(carol).remove(0);
    sql(carol, "UPDATE rows SET a=5").await.unwrap();
    let edit = records(carol).remove(1);
    late.receive(created.clone().into()).await.unwrap();
    sql(late, "UPDATE rows SET a=7").await.unwrap();
    let dropped = records(late).remove(0);
    for write in [&created, &dropped] {
        mia.receive(write.clone().into()).await.unwrap();
    }
    sql(mia, "UPDATE rows SET a=8").await.unwrap();
    let dependent = records(mia).remove(0);
    // Carol's later claim of 'x' loses to Ana's: her row is hidden.
    for write in [&kept, &created] {
        target.receive(write.clone().into()).await.unwrap();
    }
    assert_eq!(count(target, "rows"), 1);
    target.close().await.unwrap();
    let target = migrated(&stores[4], true).open().await.unwrap();
    target.exclude_later_writes(2).await;
    let marker = records(&target).last().unwrap().header.position;
    let snapshot = crate::snapshot_write::tests::frames(&target, Audience::Store).await;
    let fresh = migrated(&stores[5], true).open().await.unwrap();
    // The kept raise is replayed before its snapshot loads.
    fresh.exclude_later_writes(2).await;
    crate::snapshot_write::tests::load_one(
        &fresh,
        crate::snapshot_write::tests::id(Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
    let row = row_of(&created);
    let mut results = Vec::new();
    for db in [&target, &fresh] {
        assert_eq!(
            generations(db, &row),
            [(1, created.header.position), (2, marker)]
        );
        crate::write_landing::tests::retire(db, dropped.header.position.device, 1, "device", 0)
            .await;
        db.receive(edit.clone().into()).await.unwrap();
        db.apply_downloaded::<crate::pending::tests::Record>(
            dropped.clone().into(),
            crate::test_utils::published(coven_format::value::StorageTime::since_epoch(
                std::time::Duration::from_secs(30 * DAY + 1),
            )),
        )
        .await
        .unwrap();
        db.receive(dependent.clone().into()).await.unwrap();
        assert_eq!(
            generations(db, &row),
            [(1, created.header.position), (2, marker)]
        );
        let mut losses: Vec<_> = db
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .map(|loss| {
                let cause = match &loss.replaced_by {
                    Replacement::Write(write) => format!("write {write:?}"),
                    Replacement::SchemaChange { version } => format!("schema {version}"),
                    Replacement::DataDependency { write, sources } => format!(
                        "dependency {write:?} {:?}",
                        sources.iter().map(|s| s.write).collect::<Vec<_>>()
                    ),
                    other => panic!("{other:?}"),
                };
                (cause, setters(&loss))
            })
            .collect();
        losses.sort();
        let expected = {
            let mut expected = vec![
                (
                    format!("write {marker:?}"),
                    vec![("a".to_string(), edit.header.position)],
                ),
                (
                    format!(
                        "dependency {:?} {:?}",
                        dependent.header.position,
                        [dropped.header.position]
                    ),
                    vec![("a".to_string(), dependent.header.position)],
                ),
            ];
            let mut frozen: Vec<_> = ["a", "audience", "b", "id", "parent"]
                .iter()
                .map(|c| (c.to_string(), created.header.position))
                .collect();
            frozen.sort();
            expected.push(("schema 2".to_string(), frozen));
            expected.sort();
            expected
        };
        assert_eq!(losses, expected);
        results.push(losses);
    }
    assert_eq!(results[0], results[1]);
    for db in [target, fresh] {
        db.close().await.unwrap();
    }
    for db in dbs {
        db.close().await.ok();
    }
}

/// Replay drops a circle deletion: the circle is no longer deleted.
pub(crate) async fn drop_entry(db: &Database, deletion: crate::EntryId, circle: CircleId) {
    let log = db.store_log().await.unwrap();
    let mut next = log.clone();
    next.replay.entries.insert(
        deletion,
        crate::EntryOutcome::Dropped(crate::DropReason::NotAllowed),
    );
    next.replay.state.circle_deletions.remove(&circle);
    db.apply_store_log_batch::<crate::pending::tests::Record>(
        log,
        vec![],
        next.replay,
        vec![],
        vec![],
        Vec::new(),
        Vec::new(),
    )
    .await
    .unwrap();
}

pub(crate) fn circle_tables() -> Vec<SyncedTable> {
    migration_tables()
}

/// The circle fixture schema, optionally with a droppable column `c`.
pub(crate) fn circle_schema(with_c: bool) -> &'static str {
    if with_c {
        "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a INTEGER CHECK(a>=0),b TEXT,parent TEXT REFERENCES rows(id) ON DELETE CASCADE,c TEXT,UNIQUE(audience,b)); CREATE INDEX rows_parent ON rows(parent)"
    } else {
        MIGRATION_SCHEMA
    }
}

/// One circle row setting every column of `circle_schema(true)`.
pub(crate) fn circle_row() -> WriteRecord {
    let mut write = migration_insert(0, 0, Audience::Circle(circle_id()));
    let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.insert(
        "c".into(),
        ColumnValue {
            value: Scalar::Text("extra".into()),
            parents: BTreeMap::new(),
        },
    );
    write
}

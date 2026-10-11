use crate::test_utils::tests::*;
use crate::tests::TestStore;
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
                deleted = deleted_circle;
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
            db.apply_downloaded(w.clone().into()).await.unwrap(),
            crate::ApplyOutcome::Applied
        );
    }
    if history.deletion.is_some() {
        crate::store_log::tests::delete_circle(&db, circle_id())
            .await
            .unwrap();
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
                // A selected row changes generation (an ordinary delete per the
                // spec, or erased generation records in Rust's retirement path).
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
#[ignore = "requires COVEN_MERGE_LEAN; stops at the first migration selection mismatch"]
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

#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh checks the migration discrepancy"]
async fn migration_differential_counterexample() {
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
    assert_eq!(case.rust, json!([{"id":0,"selected":true}]));
    assert_eq!(
        run_lean(&runner, &case.input),
        json!([{"id":0,"selected":false}])
    );
    let mut empty = history.clone();
    empty.writes.clear();
    assert!(!mismatch(&runner, &empty).await);
    let mut no_deletion = history.clone();
    no_deletion.deletion = None;
    assert!(!mismatch(&runner, &no_deletion).await);
    let mut final_deletion = history;
    final_deletion.deletion = Some(Finality::Final);
    assert!(!mismatch(&runner, &final_deletion).await);
}

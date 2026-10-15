//! Rust / Lean `Migration.freezeDropped` (§17.1, D7): a migration that drops
//! tables and columns, compared over generated losses with the merge model's
//! frozen records and the live records the dropped schema leaves. A hidden
//! row's whole-row copy is `Hidden::freeze_dropped`'s; a dropped table's or
//! column's losses are `migration_state::freeze`'s.

use crate::migration_state::tests::{circle_id, migration_insert};
use crate::test_utils::tests::{run_lean, Generator};
use crate::tests::TestStore;
use crate::write::tests::records;
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_format::loss::{Loss, LossCause, LossValues};
use coven_format::value::Value as Scalar;
use coven_merge::{Audience, ColumnValue, Operation, Rule};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;

const SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a INTEGER CHECK(a>=0),b TEXT,parent TEXT REFERENCES rows(id) ON DELETE CASCADE,c TEXT,d TEXT,UNIQUE(audience,b)); CREATE INDEX rows_parent ON rows(parent); CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL)";

/// One schema change a migration step makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Op {
    DropC,
    RenameD,
    DropD,
    DropTags,
}

/// A cell two devices edit concurrently: a store row's `c` or `d`, or a tag.
#[derive(Clone, Copy, Debug)]
enum Edit {
    Row { row: usize, column: &'static str },
    Tag { tag: usize },
}

#[derive(Clone, Debug)]
struct FreezeHistory {
    store_rows: usize,
    tags: usize,
    edits: Vec<Edit>,
    /// Circle rows the non-final deletion hides; `true` sets `c` and `d`.
    circle_rows: Vec<bool>,
    /// Each later migration's changes, in version order.
    steps: Vec<Vec<Op>>,
}

impl FreezeHistory {
    fn generate(rng: &mut Generator) -> Self {
        let store_rows = 1 + rng.pick(3);
        let tags = rng.pick(3);
        let mut edits = Vec::new();
        for _ in 0..rng.pick(5) {
            if tags > 0 && rng.pick(3) == 0 {
                edits.push(Edit::Tag {
                    tag: rng.pick(tags),
                });
            } else {
                edits.push(Edit::Row {
                    row: rng.pick(store_rows),
                    column: ["c", "d"][rng.pick(2)],
                });
            }
        }
        let circle_rows = (0..rng.pick(3)).map(|_| rng.pick(2) == 0).collect();
        let mut remaining = vec![Op::DropC, Op::RenameD, Op::DropD, Op::DropTags];
        let mut steps = Vec::new();
        for _ in 0..1 + rng.pick(2) {
            let mut step = Vec::new();
            for op in remaining.clone() {
                if rng.pick(2) == 0 {
                    step.push(op);
                }
            }
            if step.is_empty() {
                step.push(remaining[rng.pick(remaining.len())]);
            }
            // A dropped `d` cannot be renamed later.
            remaining.retain(|op| {
                let renames_dropped = *op == Op::RenameD && step.contains(&Op::DropD);
                !(step.contains(op) || renames_dropped)
            });
            steps.push(step);
            if remaining.is_empty() {
                break;
            }
        }
        Self {
            store_rows,
            tags,
            edits,
            circle_rows,
            steps,
        }
    }

    fn reductions(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        for i in 0..self.edits.len() {
            let mut candidate = self.clone();
            candidate.edits.remove(i);
            candidates.push(candidate);
        }
        for i in 0..self.circle_rows.len() {
            let mut candidate = self.clone();
            candidate.circle_rows.remove(i);
            candidates.push(candidate);
        }
        for (s, step) in self.steps.iter().enumerate() {
            if self.steps.len() > 1 {
                let mut candidate = self.clone();
                candidate.steps.remove(s);
                candidates.push(candidate);
            }
            for i in 0..step.len() {
                if step.len() > 1 {
                    let mut candidate = self.clone();
                    candidate.steps[s].remove(i);
                    candidates.push(candidate);
                }
            }
        }
        candidates
            .into_iter()
            .filter(FreezeHistory::valid)
            .collect()
    }

    /// Renaming `d` before dropping it drops `e`; a dropped `d` cannot be
    /// renamed later.
    fn valid(&self) -> bool {
        let mut renamed = false;
        let mut dropped = BTreeSet::new();
        for step in &self.steps {
            if step.contains(&Op::RenameD) && (renamed || dropped.contains(&Op::DropD)) {
                return false;
            }
            renamed |= step.contains(&Op::RenameD);
            for op in step {
                if *op != Op::RenameD && !dropped.insert(*op) {
                    return false;
                }
            }
        }
        self.edits.iter().all(|edit| match edit {
            Edit::Row { row, .. } => *row < self.store_rows,
            Edit::Tag { tag } => *tag < self.tags,
        })
    }

    /// The schema each migration version reaches: `d`'s current name and
    /// whether `tags` remains.
    fn names(&self, through: usize) -> (&'static str, bool) {
        let mut d = "d";
        let mut tags = true;
        for step in &self.steps[..through] {
            if step.contains(&Op::RenameD) {
                d = "e";
            }
            tags &= !step.contains(&Op::DropTags);
        }
        (d, tags)
    }

    fn migrations(&self, through: usize) -> Vec<Migration> {
        let mut migrations = vec![Migration::sql(1, "rows and tags", SCHEMA)];
        for (i, step) in self.steps[..through].iter().enumerate() {
            let (d, _) = self.names(i);
            let mut statements = Vec::new();
            for op in step {
                statements.push(match op {
                    Op::DropC => "ALTER TABLE rows DROP COLUMN c".to_owned(),
                    Op::RenameD => format!("ALTER TABLE rows RENAME COLUMN {d} TO e"),
                    Op::DropD => format!(
                        "ALTER TABLE rows DROP COLUMN {}",
                        if step.contains(&Op::RenameD) { "e" } else { d }
                    ),
                    Op::DropTags => "DROP TABLE tags".to_owned(),
                });
            }
            // Renaming precedes dropping the renamed column.
            statements.sort_by_key(|s| !s.contains("RENAME"));
            let statements = statements.join("; ");
            migrations.push(Migration::run(i as u32 + 2, "drop schema", move |sql| {
                sql.execute_batch(&statements)?;
                Ok(())
            }));
        }
        migrations
    }
}

fn tables(tags: bool) -> Vec<SyncedTable> {
    let mut tables =
        vec![SyncedTable::new("rows", RowIdentity::IndependentUuid).audience_column("audience")];
    if tags {
        tables.push(SyncedTable::new("tags", RowIdentity::SharedKey));
    }
    tables
}

async fn execute(db: &Database, statement: String) {
    db.write(move |sql| {
        sql.execute_batch(&statement)?;
        Ok(())
    })
    .await
    .unwrap();
}

fn row_id(i: usize) -> String {
    format!("00000000-0000-4000-8000-{:012}", 100 + i)
}

/// A's losses after the history, then after each migration step.
async fn losses(history: &FreezeHistory) -> Vec<Vec<Loss>> {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let stores = [TestStore::with_ids(&ids), TestStore::with_ids(&ids)];
    let a = stores[0].schema(tables(true), SCHEMA).await.unwrap();
    let b = stores[1].schema(tables(true), SCHEMA).await.unwrap();
    for i in 0..history.store_rows {
        execute(
            &a,
            format!(
                "INSERT INTO rows(id,audience,a,b,c,d) VALUES('{}','store',1,'s{i}','c{i}','d{i}')",
                row_id(i)
            ),
        )
        .await;
    }
    for i in 0..history.tags {
        execute(&a, format!("INSERT INTO tags VALUES('t{i}','label {i}')")).await;
    }
    for write in records(&a) {
        b.receive(write.into()).await.unwrap();
    }
    for (n, edit) in history.edits.iter().enumerate() {
        let statement = |side: &str| match edit {
            Edit::Row { row, column } => format!(
                "UPDATE rows SET {column}='{side}{n}' WHERE id='{}'",
                row_id(*row)
            ),
            Edit::Tag { tag } => format!("UPDATE tags SET label='{side}{n}' WHERE id='t{tag}'"),
        };
        execute(&b, statement("b")).await;
        execute(&a, statement("a")).await;
        a.receive(records(&b).last().unwrap().clone().into())
            .await
            .unwrap();
    }
    for (n, both) in history.circle_rows.iter().enumerate() {
        let mut write = migration_insert(n as u64, n, Audience::Circle(circle_id()));
        if *both {
            let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
                unreachable!()
            };
            for column in ["c", "d"] {
                columns.insert(
                    column.into(),
                    ColumnValue {
                        value: Scalar::Text(format!("{column}{n}")),
                        parents: BTreeMap::new(),
                    },
                );
            }
        }
        a.receive(write.into()).await.unwrap();
    }
    if !history.circle_rows.is_empty() {
        crate::store_log::tests::delete_circle(&a, circle_id())
            .await
            .unwrap();
    }
    let mut all = vec![read(&a)];
    a.close().await.unwrap();
    b.close().await.unwrap();
    for through in 1..=history.steps.len() {
        let (_, tags) = history.names(through);
        let a = stores[0]
            .builder(tables(tags), history.migrations(through))
            .open()
            .await
            .unwrap();
        all.push(read(&a));
        a.close().await.unwrap();
    }
    all
}

fn read(db: &Database) -> Vec<Loss> {
    db.inspect_writer(|sql| {
        sql.query(
            &format!("{} ORDER BY l.id", crate::loss_record::SELECT),
            [],
            crate::loss_record::read,
        )
        .unwrap()
    })
}

/// Numbers for the model's rows, columns and setters, from the losses
/// before a step. Active losses after it name columns by their new names.
#[derive(Default)]
struct Encoding {
    rows: BTreeMap<coven_merge::RowId, usize>,
    columns: BTreeMap<(String, String), usize>,
    ids: BTreeMap<String, usize>,
}

impl Encoding {
    fn row(&mut self, row: &coven_merge::RowId) -> usize {
        let next = self.rows.len();
        *self.rows.entry(row.clone()).or_insert(next)
    }
    fn column(&mut self, table: &str, column: &str) -> usize {
        let next = self.columns.len();
        *self
            .columns
            .entry((table.to_owned(), column.to_owned()))
            .or_insert(next)
    }
    fn id(&mut self, id: impl std::fmt::Debug) -> usize {
        let next = self.ids.len();
        *self.ids.entry(format!("{id:?}")).or_insert(next)
    }

    /// `renamed` maps an active loss's new column name to its old one.
    fn loss(&mut self, loss: &Loss, renamed: &BTreeMap<&str, &str>) -> Value {
        let table = loss.row.table.clone();
        let mut name = |column: &str| {
            let old = if loss.frozen {
                column
            } else {
                renamed.get(column).copied().unwrap_or(column)
            };
            self.column(&table, old)
        };
        let (column, mut values) = match &loss.values {
            LossValues::Cell { column, cell } => {
                let c = name(column);
                (Some(c), vec![(c, cell.write)])
            }
            LossValues::Row(cells) => (
                None,
                cells
                    .iter()
                    .map(|(column, cell)| (name(column), cell.write))
                    .collect::<Vec<_>>(),
            ),
        };
        values.sort();
        let values: Vec<_> = values
            .into_iter()
            .map(|(c, setter)| json!([c, self.id(setter)]))
            .collect();
        let cause = match &loss.cause {
            LossCause::Write(write) => json!({"kind":"write","write":self.id(write)}),
            LossCause::Rules(rules) => {
                let rules: Vec<_> = rules
                    .iter()
                    .map(|rule| match rule {
                        Rule::ForeignKey(_) => json!({"kind":0}),
                        Rule::Check(_) => json!({"kind":1}),
                        Rule::DeletedCircle(entry) => json!({"kind":2,"entry":self.id(entry)}),
                        Rule::OtherAudience => json!({"kind":3}),
                        Rule::Unique(_) => json!({"kind":4}),
                    })
                    .collect();
                json!({"kind":"rules","rules":rules})
            }
            LossCause::Excluded { write, version } => {
                json!({"kind":"schema","version":version,"write":self.id(write)})
            }
            LossCause::Dependency { write, sources } => {
                let sources: Vec<_> = sources.iter().map(|s| self.id(s)).collect();
                json!({"kind":"dependency","write":self.id(write),"sources":sources})
            }
            LossCause::NotAuthorized { .. } => panic!("the freeze model has no owner tables"),
        };
        let row = self.row(&loss.row);
        json!({"frozen":loss.frozen,"row":row,"gen":loss.generation,"column":column,
            "values":values,"cause":cause})
    }
}

fn sorted(mut records: Vec<Value>) -> Value {
    for record in &mut records {
        record.as_object_mut().unwrap().remove("frozen");
    }
    records.sort_by_key(Value::to_string);
    json!(records)
}

/// The model's input for step `step` and Rust's result, in the runner's form.
fn case(history: &FreezeHistory, all: &[Vec<Loss>], step: usize) -> (Value, Value) {
    let ops = &history.steps[step];
    let (d, _) = history.names(step);
    let mut encoding = Encoding::default();
    let before: Vec<_> = all[step]
        .iter()
        .map(|loss| encoding.loss(loss, &BTreeMap::new()))
        .collect();
    let mut dropped = BTreeSet::new();
    for op in ops {
        match op {
            Op::DropC => {
                dropped.insert(("rows", "c"));
            }
            Op::DropD => {
                dropped.insert(("rows", d));
            }
            Op::RenameD | Op::DropTags => {}
        }
    }
    let tags = ops.contains(&Op::DropTags);
    let mut dropped_rows = Vec::new();
    let mut dropped_columns = Vec::new();
    let rows: Vec<_> = encoding
        .rows
        .iter()
        .map(|(row, i)| (row.table.clone(), *i))
        .collect();
    for (table, r) in &rows {
        if tags && table == "tags" {
            dropped_rows.push(*r);
        }
        for (dropped_table, column) in &dropped {
            if table == dropped_table {
                let c = encoding.column(table, column);
                dropped_columns.push(json!([r, c]));
            }
        }
    }
    let renamed: BTreeMap<&str, &str> = if ops.contains(&Op::RenameD) {
        BTreeMap::from([("e", d)])
    } else {
        BTreeMap::new()
    };
    let after: Vec<_> = all[step + 1]
        .iter()
        .map(|loss| encoding.loss(loss, &renamed))
        .collect();
    let input = json!({"mode":"freeze","columns":encoding.columns.len(),
        "droppedRows":dropped_rows,"droppedColumns":dropped_columns,"losses":before});
    let (frozen, active): (Vec<_>, Vec<_>) = after
        .into_iter()
        .partition(|loss| loss["frozen"] == json!(true));
    (
        input,
        json!({"frozen":sorted(frozen),"active":sorted(active)}),
    )
}

fn lean(runner: &OsStr, input: &Value) -> Value {
    let result = run_lean(runner, input);
    let records = |key: &str| {
        let mut records = result[key].as_array().unwrap().clone();
        records.sort_by_key(Value::to_string);
        json!(records)
    };
    json!({"frozen":records("frozen"),"active":records("active")})
}

/// The first step where Rust and the model differ, or Rust's results.
async fn compare(
    runner: &OsStr,
    history: &FreezeHistory,
) -> Result<Vec<Value>, (usize, Value, Value)> {
    let all = losses(history).await;
    let mut results = Vec::new();
    for step in 0..history.steps.len() {
        let (input, rust) = case(history, &all, step);
        if lean(runner, &input) != rust {
            return Err((step, input, rust));
        }
        results.push(rust);
    }
    Ok(results)
}

#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh runs the frozen-loss histories"]
async fn freeze_differential_histories() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    // Frozen cell losses, frozen whole rows, and live removed-row records.
    let mut seen = [0; 3];
    for seed in 0..48 {
        let mut history = FreezeHistory::generate(&mut Generator::new(seed + 70_000));
        assert!(history.valid(), "{history:?}");
        match compare(&runner, &history).await {
            Ok(results) => {
                for result in results {
                    for frozen in result["frozen"].as_array().unwrap() {
                        seen[usize::from(frozen["column"].is_null())] += 1;
                    }
                    for active in result["active"].as_array().unwrap() {
                        seen[2] += usize::from(active["column"].is_null());
                    }
                }
            }
            Err(_) => {
                'reduce: loop {
                    for candidate in history.reductions() {
                        if compare(&runner, &candidate).await.is_err() {
                            history = candidate;
                            continue 'reduce;
                        }
                    }
                    break;
                }
                let (step, input, rust) = compare(&runner, &history).await.unwrap_err();
                panic!(
                    "frozen-loss differential, seed {seed}, step {step}\nhistory: {history:?}\ninput: {input}\nRust: {rust}\nLean: {}",
                    lean(&runner, &input)
                );
            }
        }
    }
    assert!(seen.iter().all(|n| *n > 0), "{seen:?}");
}

/// Every kind of affected loss in one migration: a dropped column's cell
/// loss, a dropped table's cell loss, and a hidden row whose removal record
/// names the dropped column, frozen whole while its live record follows the
/// new schema; an unaffected cell loss and hidden row stay active.
#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh runs the frozen-loss agreement"]
async fn freeze_differential_agreement() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let history = FreezeHistory {
        store_rows: 1,
        tags: 1,
        edits: vec![
            Edit::Row {
                row: 0,
                column: "c",
            },
            Edit::Row {
                row: 0,
                column: "d",
            },
            Edit::Tag { tag: 0 },
        ],
        circle_rows: vec![true, false],
        steps: vec![vec![Op::DropC, Op::DropTags]],
    };
    let all = losses(&history).await;
    let (input, rust) = case(&history, &all, 0);
    assert_eq!(rust["frozen"].as_array().unwrap().len(), 3, "{rust}");
    assert_eq!(rust["active"].as_array().unwrap().len(), 3, "{rust}");
    assert_eq!(lean(&runner, &input), rust, "{input}");
}

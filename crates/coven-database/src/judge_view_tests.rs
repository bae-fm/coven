//! Real receipts against the merge model's receiver (B18): for each write a
//! device received, the runner's `receive` mode decides `Admission.check`
//! and `Admission.guard` for every row from the receiver's reconstructed
//! execution, recomputing every foreign-key effect from resolved references
//! with `evaluate`. The device's recorded verdicts must agree, including the
//! roots of each derived change (`applied_derived_recorded`) and the failed
//! sources of each dependent one (`dependencies`,
//! `dropped_contributing_root_no_effect`).
use crate::judge::tests::{receipts, verdict, CAROL};
use crate::judge::{consistent, judge, Judgement};
use crate::owner_permission::tests::{member, ANA, BEN};
use crate::sqlite::DatabaseConnection;
use crate::test_utils::tests::run_lean;
use crate::write_rows::{column_name, key_columns, target_columns};
use crate::write_schema::WriteSchema;
use crate::Database;
use coven_format::loss::AuthorizationFailure;
use coven_format::snapshot_rows::ChangeVerdict;
use coven_format::value::Value;
use coven_format::write::Step;
use coven_format::write::WriteRecord;
use coven_format::write_sources::ChangeId;
use coven_merge::RowId;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Dense numbers for rows, columns, values and other writes' changes.
#[derive(Default)]
struct Ids {
    rows: BTreeMap<RowId, usize>,
    columns: BTreeMap<(String, String), usize>,
    values: Vec<Value>,
    external: BTreeMap<ChangeId, usize>,
}

impl Ids {
    fn row(&mut self, row: &RowId) -> usize {
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
    fn value(&mut self, value: &Value) -> Option<usize> {
        if *value == Value::Null {
            return None;
        }
        Some(match self.values.iter().position(|v| v == value) {
            Some(index) => index,
            None => {
                self.values.push(value.clone());
                self.values.len() - 1
            }
        })
    }
    /// Other writes' changes are numbered apart from this write's rows.
    fn external(&mut self, source: &ChangeId) -> usize {
        let next = 1_000_000 + self.external.len();
        *self.external.entry(source.clone()).or_insert(next)
    }
}

const TABLES: [&str; 12] = [
    "tracks", "reviews", "comments", "pins", "keys", "labels", "albums", "logs", "notes", "codes",
    "tags", "artists",
];

fn action(rule: &str) -> &'static str {
    match rule.to_ascii_uppercase().as_str() {
        "CASCADE" => "cascade",
        "SET NULL" => "setNull",
        "RESTRICT" => "restrict",
        _ => "noAction",
    }
}

fn key_values(schema: &WriteSchema, row: &RowId) -> BTreeMap<String, Value> {
    key_columns(schema.table(&row.table))
        .into_iter()
        .map(|c| c.name.clone())
        .zip(coven_format::key::decode_key(&row.key).unwrap())
        .collect()
}

/// The write's steps, by row.
fn steps(record: &WriteRecord) -> BTreeMap<RowId, Vec<Step>> {
    record
        .parts
        .iter()
        .flat_map(|part| part.records())
        .map(|record| (record.row().clone(), record.execution().steps.clone()))
        .collect()
}

/// A step as the model's effect: a delete has no cells; a key update's old
/// half updates the key columns to the new half's values.
fn effect(
    ids: &mut Ids,
    schema: &WriteSchema,
    steps: &BTreeMap<RowId, Vec<Step>>,
    row: &RowId,
    step: &Step,
) -> Json {
    let id = ids.row(row);
    let (kind, cells) = match &step.change.operation {
        coven_merge::Operation::Delete if step.key_update => {
            let new = steps
                .iter()
                .find(|(r, s)| {
                    *r != row && r.table == row.table && s.iter().any(|s| s.sequence == step.sequence)
                })
                .map(|(r, _)| r)
                .expect("the key update's new half");
            let old = key_values(schema, row);
            let cells: Vec<_> = key_values(schema, new)
                .into_iter()
                .filter(|(c, v)| old.get(c) != Some(v))
                .map(|(c, v)| json!({"column": ids.column(&row.table, &c), "value": ids.value(&v)}))
                .collect();
            (1, cells)
        }
        coven_merge::Operation::Delete => (2, Vec::new()),
        coven_merge::Operation::Insert(columns) | coven_merge::Operation::Update(columns) => (
            if matches!(step.change.operation, coven_merge::Operation::Insert(_)) {
                0
            } else {
                1
            },
            columns
                .iter()
                .map(|(c, v)| json!({"column": ids.column(&row.table, c), "value": ids.value(&v.value)}))
                .collect(),
        ),
    };
    json!({"row": id, "generation": step.change.generation, "kind": kind, "cells": cells})
}

/// The runner input for one received write, and the ids it used.
fn input(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    record: &WriteRecord,
    owners: &BTreeMap<coven_foundation::id_source::MemberId, u8>,
) -> (Json, Ids, Judgement) {
    let judgement = judge(db, schema, &record.header).unwrap();
    let steps = steps(record);
    let step = |row: &RowId, sequence: u32| {
        steps[row]
            .iter()
            .find(|s| s.sequence == sequence)
            .unwrap()
            .clone()
    };
    let mut ids = Ids::default();
    let mut references = Vec::new();
    let mut seen = BTreeSet::new();
    let mut roots = BTreeMap::new();
    let mut derived = Vec::new();
    let mut store = Vec::new();
    for (row, checked) in &judgement.rows {
        let mut order: Vec<String> = Vec::new();
        let mut paths = Vec::new();
        for (sequence, chain) in &checked.actions {
            let child = step(row, *sequence);
            let (parent, parent_sequence) = chain.last().unwrap();
            let parent_step = step(parent, *parent_sequence);
            let child_table = schema.table(&row.table);
            let parent_table = schema.table(&parent.table);
            // The reference the action follows: the key whose columns it
            // writes, or for a delete, the key into the parent's table.
            let written: BTreeSet<String> = match &child.change.operation {
                coven_merge::Operation::Update(columns) => columns.keys().cloned().collect(),
                _ => BTreeSet::new(),
            };
            let fk = child_table
                .foreign_keys
                .iter()
                .filter(|fk| fk.target.eq_ignore_ascii_case(&parent.table))
                .find(|fk| {
                    written.is_empty()
                        || fk
                            .columns
                            .iter()
                            .all(|c| written.contains(column_name(child_table, c)))
                })
                .expect("the action's foreign key");
            let columns: Vec<String> = fk
                .columns
                .iter()
                .map(|c| column_name(child_table, c).to_owned())
                .collect();
            let targets = target_columns(parent_table, fk);
            let old = key_values(schema, parent);
            if parent_step.key_update {
                let (new, _) = steps
                    .iter()
                    .find(|(r, s)| {
                        *r != parent
                            && r.table == parent.table
                            && s.iter().any(|s| s.sequence == *parent_sequence)
                    })
                    .expect("the key update's new half");
                let new_key = key_values(schema, new);
                let new: Vec<Value> = targets.iter().map(|t| new_key[t].clone()).collect();
                let stored =
                    crate::removal_sql::reference_values(db, child_table, &columns, &new).unwrap();
                for ((column, from), to) in columns.iter().zip(&new).zip(&stored) {
                    if let (Some(from), Some(to)) = (ids.value(from), ids.value(to)) {
                        store.push(json!([ids.column(&row.table, column), from, to]));
                    }
                }
            }
            if seen.insert((parent.clone(), row.clone(), columns.clone())) {
                references.push(json!({
                    "parent": ids.row(parent), "parentGeneration": parent_step.change.generation,
                    "child": ids.row(row), "childGeneration": child.change.generation,
                    "columns": columns.iter().map(|c| ids.column(&row.table, c)).collect::<Vec<_>>(),
                    "oldKey": targets.iter().map(|t| ids.value(&old[t])).collect::<Vec<_>>(),
                    "onDelete": action(&fk.on_delete), "onUpdate": action(&fk.on_update),
                    "parentColumns": targets.iter().map(|t| ids.column(&parent.table, t)).collect::<Vec<_>>(),
                }));
            }
            let (root, root_sequence) = &chain[0];
            roots.insert(root.clone(), *root_sequence);
            let mut path: Vec<usize> = chain.iter().map(|(r, _)| ids.row(r)).collect();
            path.push(ids.row(row));
            paths.push(path);
            if !matches!(child.change.operation, coven_merge::Operation::Delete) {
                order.retain(|c| !columns.contains(c));
                order.extend(columns);
            }
        }
        if checked.actions.is_empty() {
            continue;
        }
        // The proposed payload is the net change, in the order the model
        // combines the actions' cells. A row the write inserted and its
        // actions deleted again has none: the model checks its first step.
        let Some(net) = record
            .parts
            .iter()
            .flat_map(|p| &p.rows)
            .find(|change| change.row == *row)
        else {
            continue;
        };
        let (kind, cells) = match &net.change.operation {
            coven_merge::Operation::Delete => (2, Vec::new()),
            coven_merge::Operation::Insert(columns) | coven_merge::Operation::Update(columns) => {
                for name in columns.keys() {
                    if !order.contains(name) {
                        order.push(name.clone());
                    }
                }
                (
                    if matches!(net.change.operation, coven_merge::Operation::Insert(_)) {
                        0
                    } else {
                        1
                    },
                    order
                        .iter()
                        .map(|c| {
                            json!({"column": ids.column(&row.table, c),
                                   "value": columns.get(c).and_then(|v| ids.value(&v.value))})
                        })
                        .collect(),
                )
            }
        };
        derived.push(json!({
            "row": ids.row(row), "paths": paths, "direct": !checked.derived,
            "effect": {"row": ids.row(row), "generation": net.change.generation, "kind": kind, "cells": cells},
        }));
    }
    // Each row's direct effect: the step its actions start from, or its first
    // direct step.
    let mut direct = Vec::new();
    for (row, checked) in &judgement.rows {
        let actions: BTreeSet<u32> = checked.actions.iter().map(|(s, _)| *s).collect();
        let first = roots.get(row).copied().or_else(|| {
            steps[row]
                .iter()
                .map(|s| s.sequence)
                .find(|s| !actions.contains(s))
        });
        if let Some(sequence) = first {
            direct.push(effect(&mut ids, schema, &steps, row, &step(row, sequence)));
        }
    }
    let mut external = Vec::new();
    let mut dropped = Vec::new();
    let mut rows = Vec::new();
    let mut payloads = Vec::new();
    for part in &record.parts {
        for original in part.records() {
            let row = original.row();
            // A direct change's proposed payload is where its steps end
            // (`Capture`); a net change elsewhere is proposed as itself.
            if let coven_format::write::PartRecord::Change(change) = original {
                let owned = coven_format::snapshot_rows::OriginalRow::Change(change.clone());
                if !consistent(&owned) {
                    let step = Step {
                        sequence: 0,
                        change: change.change.clone(),
                        key_update: false,
                    };
                    payloads.push(effect(&mut ids, schema, &steps, row, &step));
                }
            }
            let mut sources: Vec<ChangeId> = Vec::new();
            if let coven_format::write::PartRecord::Change(change) = original {
                sources.extend(change.sources.changes().cloned());
            }
            sources.extend(original.execution().sources.changes().cloned());
            let numbered: Vec<usize> = sources.iter().map(|s| ids.external(s)).collect();
            for (source, id) in sources.iter().zip(&numbered) {
                if !crate::write_sources::retained(db, source).unwrap() {
                    dropped.push(*id);
                }
            }
            external.push(json!({"row": ids.row(row), "sources": numbered}));
            let owner = crate::ownership::creation(db, row)
                .unwrap()
                .map(|creation| {
                    let member = crate::ownership::member(db, creation.creator.write.device)
                        .unwrap()
                        .unwrap();
                    owners[&member]
                })
                .unwrap_or(BEN);
            rows.push(json!({"row": ids.row(row), "owner": owner,
                "table": TABLES.iter().position(|t| *t == row.table).unwrap_or(100)}));
        }
    }
    (
        json!({
            "mode": "receive", "references": references, "direct": direct, "derived": derived,
            "rows": rows, "author": BEN,
            "owned": (0..TABLES.len())
                .filter(|t| crate::ownership::owner_table(schema, TABLES[*t]))
                .collect::<Vec<_>>(),
            "external": external, "droppedExternal": dropped, "prior": Vec::<usize>::new(),
            "payloads": payloads, "store": store,
        }),
        ids,
        judgement,
    )
}

/// The model's cause for a recorded verdict, in the runner's terms.
fn expected(
    verdict: &ChangeVerdict,
    ids: &mut Ids,
    record: &WriteRecord,
    owners: &BTreeMap<coven_foundation::id_source::MemberId, u8>,
) -> Json {
    match verdict {
        ChangeVerdict::Applied | ChangeVerdict::Derived(_) => Json::Null,
        ChangeVerdict::NotAuthorized(AuthorizationFailure::NotOwner { owner }) => {
            json!({"kind": "notOwner", "owner": owners[owner]})
        }
        ChangeVerdict::NotAuthorized(AuthorizationFailure::InvalidForeignKeyEffect) => {
            json!({"kind": "foreignKey"})
        }
        ChangeVerdict::Dependent(sources) => {
            let mut sources: Vec<Json> = sources
                .iter()
                .map(|s| {
                    if s.write == record.header.position {
                        json!([0, ids.row(&s.row)])
                    } else {
                        json!([1, ids.external(s)])
                    }
                })
                .collect();
            sources.sort_by_key(|s| s.to_string());
            sources.dedup();
            json!({"kind": "dependency", "sources": sources})
        }
        verdict => panic!("no model verdict for {verdict:?}"),
    }
}

/// Compare one received write's recorded verdicts with the model's.
fn compare(runner: &std::ffi::OsStr, db: &Database, record: &WriteRecord) -> usize {
    let owners: BTreeMap<_, _> = [ANA, BEN, CAROL]
        .into_iter()
        .map(|seed| (member(seed), seed))
        .collect();
    let (input, mut ids, judgement) =
        db.inspect_writer_schema(|c, schema| input(c, schema, record, &owners));
    let result = run_lean(runner, &input);
    let mut compared = 0;
    for lean in result.as_array().unwrap() {
        let id = lean["row"].as_u64().unwrap() as usize;
        let row = ids.rows.iter().find(|(_, n)| **n == id).unwrap().0.clone();
        let checked = &judgement.rows[&row];
        assert_eq!(
            lean["check"],
            checked.check.is_ok(),
            "{row:?}: {input} -> {lean}"
        );
        let recorded = verdict(db, record, &row);
        let mut guard = lean["guard"].clone();
        if let Some(sources) = guard.get_mut("sources") {
            let mut list = sources.as_array().unwrap().clone();
            list.sort_by_key(|s| s.to_string());
            list.dedup();
            *sources = Json::Array(list);
        }
        assert_eq!(
            guard,
            expected(&recorded, &mut ids, record, &owners),
            "{row:?}: {input} -> {lean}"
        );
        if let ChangeVerdict::Derived(_) = recorded {
            let mut lean_roots: Vec<u64> = lean["roots"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r.as_u64().unwrap())
                .collect();
            lean_roots.sort();
            lean_roots.dedup();
            let mut roots: Vec<u64> = checked
                .actions
                .iter()
                .map(|(_, chain)| ids.rows[&chain[0].0] as u64)
                .collect();
            roots.sort();
            roots.dedup();
            assert_eq!(lean_roots, roots, "{row:?}");
        }
        compared += 1;
    }
    compared
}

/// Each received write of the judge's receipt cases is compared.
#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
async fn receipt_matches_the_model() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let mut compared = 0;
    receipts(|db, record| compared += compare(&runner, db, record)).await;
    assert!(compared >= 30, "{compared} rows compared");
}

/// One received write's classification as the receiver ran it, against the
/// model's `Classification.cause` with `heldRule` over the same steps and
/// held references (B18). Returns how many steps were compared, and how
/// many of them are actions.
fn classify(runner: &std::ffi::OsStr, db: &Database, record: &WriteRecord) -> (usize, usize) {
    use super::{key_names, key_values, Authoring, ChildView, HeldReference};
    use crate::judge::{visit_write, StepIndex};
    db.inspect_writer_schema(|c, schema| {
        let header = &record.header;
        let authoring = Authoring::new(c, schema, header.schema_version).unwrap();
        let index = StepIndex::of(c, header).unwrap();
        let judgement = judge(c, schema, header).unwrap();
        let mut all: Vec<(RowId, Step, Vec<HeldReference>)> = Vec::new();
        visit_write(c, header, |original| {
            let row = original.row().clone();
            let mut view = ChildView::new(&authoring, &row, &original, header.schema_version);
            for step in &original.execution().steps {
                all.push((row.clone(), step.clone(), view.held(c, &index, step)?));
                view.apply(step);
            }
            Ok(())
        })
        .unwrap();
        // Execution order; a key update's old half before its new one.
        all.sort_by_key(|(_, step, _)| {
            (
                step.sequence,
                !matches!(step.change.operation, coven_merge::Operation::Delete),
            )
        });
        let by_row: BTreeMap<RowId, Vec<Step>> =
            all.iter().fold(BTreeMap::new(), |mut rows, (row, step, _)| {
                rows.entry(row.clone()).or_insert_with(Vec::new).push(step.clone());
                rows
            });
        let position = |row: &RowId, sequence: u32| {
            all.iter()
                .position(|(r, s, _)| r == row && s.sequence == sequence)
                .unwrap()
        };
        let mut ids = Ids::default();
        let mut names: BTreeMap<coven_merge::ForeignKey, usize> = BTreeMap::new();
        let mut name = |key: &coven_merge::ForeignKey| {
            let next = names.len();
            *names.entry(key.clone()).or_insert(next)
        };
        let mut steps = Vec::new();
        let mut refs = Vec::new();
        for (i, (row, step, held)) in all.iter().enumerate() {
            let mut parents = Vec::new();
            if let coven_merge::Operation::Insert(columns) | coven_merge::Operation::Update(columns) =
                &step.change.operation
            {
                for (column, cell) in columns {
                    for (key, parent) in &cell.parents {
                        parents.push(json!([
                            ids.column(&row.table, column),
                            name(key),
                            ids.row(&parent.row),
                            parent.generation
                        ]));
                    }
                }
            }
            let becomes = step.key_update.then(|| {
                let (new, new_step) = index.new_half(row, step.sequence).unwrap();
                json!([ids.row(new), new_step.after()])
            });
            steps.push(json!({
                "effect": effect(&mut ids, schema, &by_row, row, step),
                "parents": parents, "becomes": becomes,
            }));
            let table = schema.table(&row.table);
            let mut entries = Vec::new();
            for held in held {
                let key = &table.foreign_keys[held.key];
                let parent_table = schema.table(&key.target);
                let columns = key_names(table, key);
                let targets = target_columns(parent_table, key);
                // How the child stores each new key a key update of this
                // parent's incarnation could cascade.
                let mut store = Vec::new();
                for parent_step in index.before(&held.parent, step.sequence) {
                    if !parent_step.key_update || parent_step.generation != held.generation {
                        continue;
                    }
                    let (new, _) = index.new_half(&held.parent, parent_step.sequence).unwrap();
                    let new = key_values(parent_table, &targets, &new.key).unwrap();
                    let stored =
                        crate::removal_sql::reference_values(c, table, &columns, &new).unwrap();
                    for ((column, from), to) in columns.iter().zip(&new).zip(&stored) {
                        if let (Some(from), Some(to)) = (ids.value(from), ids.value(to)) {
                            store.push(json!([ids.column(&row.table, column), from, to]));
                        }
                    }
                }
                let old = key_values(parent_table, &targets, &held.parent.key).unwrap();
                let others = held.others.as_ref().map(|others| {
                    others
                        .iter()
                        .flat_map(|(column, parents)| {
                            parents.iter().map(move |(key, parent)| (column, key, parent))
                        })
                        .map(|(column, key, parent)| {
                            json!([
                                ids.column(&row.table, column),
                                name(key),
                                ids.row(&parent.row),
                                parent.generation
                            ])
                        })
                        .collect::<Vec<_>>()
                });
                entries.push(json!({
                    "reference": {
                        "parent": ids.row(&held.parent), "parentGeneration": held.generation,
                        "child": ids.row(row), "childGeneration": step.change.generation,
                        "columns": columns.iter().map(|c| ids.column(&row.table, c)).collect::<Vec<_>>(),
                        "oldKey": old.iter().map(|v| ids.value(v)).collect::<Vec<_>>(),
                        "onDelete": action(&key.on_delete), "onUpdate": action(&key.on_update),
                        "parentColumns": targets.iter().map(|t| ids.column(&parent_table.name, t)).collect::<Vec<_>>(),
                    },
                    "name": name(&schema.foreign_key(table, key)),
                    "store": store,
                    "carried": held.carried.iter().map(|(column, value)| {
                        json!({"column": ids.column(&row.table, column), "value": ids.value(value)})
                    }).collect::<Vec<_>>(),
                    "others": others,
                }));
            }
            if !entries.is_empty() {
                refs.push(json!({"step": i, "held": entries}));
            }
        }
        let input = json!({"mode": "classify", "steps": steps, "refs": refs});
        let result = run_lean(runner, &input);
        let lean = result.as_array().unwrap();
        assert_eq!(lean.len(), all.len());
        for (i, (row, step, _)) in all.iter().enumerate() {
            let rust = judgement.rows[row]
                .actions
                .iter()
                .find(|(sequence, _)| *sequence == step.sequence)
                .map(|(_, chain)| {
                    let (parent, sequence) = chain.last().unwrap();
                    position(parent, *sequence)
                });
            assert_eq!(
                lean[i].as_u64().map(|cause| cause as usize),
                rust,
                "{row:?} {step:?}: {input}"
            );
        }
        (all.len(), lean.iter().filter(|cause| !cause.is_null()).count())
    })
}

/// The receiver's classification of every receipt case's steps is the
/// model's (`Classification.cause`).
#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
async fn classification_matches_the_model() {
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let (mut compared, mut actions) = (0, 0);
    receipts(|db, record| {
        let (steps, found) = classify(&runner, db, record);
        compared += steps;
        actions += found;
    })
    .await;
    assert!(
        compared >= 30 && actions >= 10,
        "{compared} steps, {actions} actions"
    );
}

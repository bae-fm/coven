//! Histories and result adapters shared by the database/Lean differentials.
use crate::{Database, EntryId, RowIdentity, SyncedTable};
use coven_format::loss::{LossCause, LossValues};
use coven_format::value::{EntryPositions, Value as Scalar, WritePositions};
use coven_format::write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord};
use coven_foundation::id_source::DeviceId;
use coven_merge::{
    Audience, Change, ColumnValue, History, Operation, RowId, RowState, Timestamp, Write, WriteId,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io::Write as _;
use std::process::{Command, Stdio};

pub(crate) const SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)";
const COLUMNS: [&str; 4] = ["id", "a", "b", "retired"];
pub(crate) const RESET: EntryId = EntryId {
    device: DeviceId(99),
    number: 1,
};

pub(crate) fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::SharedKey)]
}
pub(crate) fn run_lean(runner: &OsStr, input: &Value) -> Value {
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Lean merge runner");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "Lean input {input}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let mut result: Value = serde_json::from_slice(&result.stdout).expect("Lean JSON result");
    if let Some(frozen) = result.get_mut("frozen") {
        frozen
            .as_array_mut()
            .expect("frozen records")
            .sort_by_key(Value::to_string);
    }
    result
}

// Deterministic xorshift and causal-order sampling, as in the core differential,
// operating on format records rather than the core's string-valued writes.
pub(crate) struct Generator(u64);
impl Generator {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed + 1)
    }
    pub(crate) fn pick(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
    pub(crate) fn order(&mut self, records: &[WriteRecord]) -> Vec<WriteRecord> {
        let mut remaining = records.to_vec();
        let mut applied = BTreeSet::new();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let available: Vec<_> = remaining
                .iter()
                .enumerate()
                .filter(|(_, w)| w.header.had_read.0.iter().all(|id| applied.contains(id)))
                .map(|(i, _)| i)
                .collect();
            assert!(!available.is_empty(), "causal generator stalled");
            let write = remaining.remove(available[self.pick(available.len())]);
            applied.insert(write.header.position);
            order.push(write);
        }
        order
    }
}
pub(crate) fn merge_write(record: &WriteRecord) -> Write<Scalar> {
    Write {
        id: record.header.position,
        timestamp: record.header.timestamp,
        had_read: record.header.had_read.0.iter().copied().collect(),
        changes: record
            .parts
            .iter()
            .flat_map(|p| &p.rows)
            .map(|r| (r.row.clone(), r.change.clone()))
            .collect(),
    }
}
fn row(n: usize) -> RowId {
    RowId {
        table: "rows".into(),
        key: coven_format::key::encode_key(&[Scalar::Text(n.to_string())]).unwrap(),
        audience: Audience::Store,
    }
}
pub(crate) fn insert(n: u64, r: usize) -> WriteRecord {
    let position = WriteId {
        device: DeviceId(100 + n),
        number: 1,
    };
    WriteRecord {
        header: WriteHeader {
            position,
            timestamp: Timestamp::new(n + 1, 0, position.device).unwrap(),
            had_read: WritePositions(Vec::new()),
            store_log_read: EntryPositions(Vec::new()),
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            rows: vec![RowChange {
                row: row(r),
                change: Change {
                    generation: 0,
                    operation: Operation::Insert(
                        [
                            ("id", r.to_string()),
                            ("a", format!("a{n}")),
                            ("b", format!("b{n}")),
                        ]
                        .into_iter()
                        .map(|(c, value)| {
                            (
                                c.into(),
                                ColumnValue {
                                    value: Scalar::Text(value),
                                    parents: BTreeMap::new(),
                                },
                            )
                        })
                        .collect(),
                    ),
                },
                old: BTreeMap::new(),
                sources: coven_format::write_sources::WriteSources {
                    generation: None,
                    setters: BTreeMap::new(),
                },
            }],
            dismissals: Vec::new(),
        }],
    }
}

#[derive(Clone)]
pub(crate) struct ReloadHistory {
    pub(crate) writes: Vec<WriteRecord>,
    pub(crate) covered: BTreeSet<WriteId>,
    pub(crate) reset: bool,
}
impl ReloadHistory {
    pub(crate) fn generate(rng: &mut Generator) -> Self {
        let mut writes = Vec::<WriteRecord>::new();
        // Snapshot rows supply generation witnesses even when earlier tail
        // writes are discarded. Tail inserts use distinct keys.
        for n in 0..4 {
            writes.push(insert(n, n as usize));
        }
        let covered: BTreeSet<_> = writes.iter().map(|w| w.header.position).collect();
        for n in 4..24 {
            let mut write = insert(n, n as usize);
            let mut past = BTreeSet::new();
            for previous in &writes {
                if rng.pick(4) == 0 {
                    past.insert(previous.header.position);
                    past.extend(previous.header.had_read.0.iter().copied());
                }
            }
            let reads_reset = rng.pick(2) == 0
                || writes.iter().any(|w| {
                    past.contains(&w.header.position) && w.header.store_log_read.covers(RESET)
                });
            if reads_reset {
                past.extend(covered.iter().copied());
                write.header.store_log_read = EntryPositions(vec![RESET]);
            }
            if rng.pick(2) == 0 {
                let source = &writes[rng.pick(4)];
                past.insert(source.header.position);
                let target = &mut write.parts[0].rows[0];
                target.row = source.parts[0].rows[0].row.clone();
                // Author from the real merge, after the reset has discarded
                // unread tail changes when this author has observed the entry.
                let author: Vec<_> = writes
                    .iter()
                    .filter(|w| past.contains(&w.header.position))
                    .map(|w| {
                        let mut merged = merge_write(w);
                        if reads_reset
                            && !covered.contains(&w.header.position)
                            && !w.header.store_log_read.covers(RESET)
                        {
                            merged.changes.clear();
                        }
                        merged
                    })
                    .collect();
                let mut state = RowState::new(target.row.clone());
                for (index, previous) in author.iter().enumerate() {
                    if previous.changes.contains_key(&target.row) {
                        let oracle = History::new(author[..index].iter().cloned()).unwrap();
                        state = coven_merge::apply(&state, previous, &oracle).unwrap().state;
                    }
                }
                target.change.generation = state.generation();
                target.sources.generation =
                    state.generations().get(&state.generation()).map(|write| {
                        coven_format::write_sources::ChangeId {
                            write: *write,
                            row: target.row.clone(),
                        }
                    });
                if state.present() {
                    if rng.pick(4) == 0 {
                        target.change.operation = Operation::Delete;
                        target.old = state
                            .cells()
                            .iter()
                            .map(|(c, v)| (c.clone(), v.value.value.clone()))
                            .collect();
                    } else {
                        let column = COLUMNS[1 + rng.pick(2)].to_owned();
                        target.change.operation = Operation::Update(BTreeMap::from([(
                            column.clone(),
                            ColumnValue {
                                value: Scalar::Text(format!("edit{n}")),
                                parents: BTreeMap::new(),
                            },
                        )]));
                        target.old = BTreeMap::from([(
                            column.clone(),
                            state.cells()[&column].value.value.clone(),
                        )]);
                    }
                } else {
                    let Operation::Insert(columns) = &mut target.change.operation else {
                        unreachable!()
                    };
                    columns.get_mut("id").unwrap().value =
                        coven_format::key::decode_key(&target.row.key)
                            .unwrap()
                            .remove(0);
                }
                target.sources.setters = state
                    .cells()
                    .iter()
                    .filter(|(column, _)| match &target.change.operation {
                        Operation::Insert(_) => false,
                        Operation::Update(columns) => columns.contains_key(*column),
                        Operation::Delete => true,
                    })
                    .map(|(column, cell)| {
                        (
                            column.clone(),
                            coven_format::write_sources::CellSource {
                                change: coven_format::write_sources::ChangeId {
                                    write: cell.write,
                                    row: target.row.clone(),
                                },
                                column: column.clone(),
                            },
                        )
                    })
                    .collect();
            }
            write.header.had_read = WritePositions(past.into_iter().collect());
            writes.push(write);
        }
        Self {
            writes: rng.order(&writes),
            covered,
            reset: true,
        }
    }
    pub(crate) fn valid(&self) -> bool {
        if History::new(self.writes.iter().map(merge_write)).is_err() {
            return false;
        }
        // ResetGenerations requires every retained generation witness to
        // survive filtering, including after a reducer removes a reset read.
        let effective = self.writes.iter().map(|w| {
            let mut merged = merge_write(w);
            if self.reset
                && !self.covered.contains(&w.header.position)
                && !w.header.store_log_read.covers(RESET)
            {
                merged.changes.clear();
            }
            merged
        });
        if History::new(effective).is_err() {
            return false;
        }
        let mut applied = BTreeSet::new();
        for w in &self.writes {
            if !w.header.had_read.0.iter().all(|id| applied.contains(id)) {
                return false;
            }
            if self.covered.contains(&w.header.position)
                && !w
                    .header
                    .had_read
                    .0
                    .iter()
                    .all(|id| self.covered.contains(id))
            {
                return false;
            }
            for id in &w.header.had_read.0 {
                let earlier = self
                    .writes
                    .iter()
                    .find(|w| w.header.position == *id)
                    .unwrap();
                if !earlier
                    .header
                    .had_read
                    .0
                    .iter()
                    .all(|id| w.header.had_read.0.contains(id))
                {
                    return false;
                }
            }
            if coven_format::write_stream::WriteEncoder::new(w).is_err() {
                return false;
            }
            applied.insert(w.header.position);
        }
        self.covered.is_subset(&applied)
    }
    pub(crate) fn reductions(&self) -> Vec<Self> {
        let mut candidates = Vec::new();
        for i in 0..self.writes.len() {
            let mut candidate = self.clone();
            let removed = candidate.writes.remove(i).header.position;
            candidate.covered.remove(&removed);
            for w in &mut candidate.writes {
                w.header.had_read.0.retain(|id| *id != removed);
            }
            candidates.push(candidate);
            for p in 0..self.writes[i].parts.len() {
                for r in 0..self.writes[i].parts[p].rows.len() {
                    let mut candidate = self.clone();
                    candidate.writes[i].parts[p].rows.remove(r);
                    candidates.push(candidate);
                    for column in &COLUMNS[1..] {
                        let mut candidate = self.clone();
                        let change = &mut candidate.writes[i].parts[p].rows[r];
                        match &mut change.change.operation {
                            Operation::Insert(cols) | Operation::Update(cols) => {
                                cols.remove(*column);
                            }
                            Operation::Delete => {}
                        }
                        change.old.remove(*column);
                        if candidate.writes != self.writes {
                            candidates.push(candidate);
                        }
                    }
                }
            }
            if !self.writes[i].header.store_log_read.0.is_empty() {
                let mut candidate = self.clone();
                candidate.writes[i].header.store_log_read.0.clear();
                candidates.push(candidate);
            }
            if self.covered.contains(&self.writes[i].header.position) {
                let mut candidate = self.clone();
                candidate.covered.remove(&self.writes[i].header.position);
                candidates.push(candidate);
            }
        }
        candidates
    }
    pub(crate) fn rows(&self) -> Vec<RowId> {
        self.writes
            .iter()
            .flat_map(|w| &w.parts)
            .flat_map(|p| &p.rows)
            .map(|r| r.row.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub(crate) fn input(&self) -> Value {
        let rows = self.rows();
        let writes: Vec<_> = self.writes.iter().map(|w| {
            let changes: Vec<_> = w.parts.iter().flat_map(|p| &p.rows).map(|r| {
                let (kind, columns) = match &r.change.operation {
                    Operation::Insert(cols) => (0, cols.keys().collect::<Vec<_>>()),
                    Operation::Update(cols) => (1, cols.keys().collect()),
                    Operation::Delete => (2, Vec::new()),
                };
                json!({"row": rows.iter().position(|x| x == &r.row).unwrap(), "kind": kind,
                    "gen": r.change.generation, "cols": columns.iter().map(|c| column_id(c)).collect::<Vec<_>>()})
            }).collect();
            json!({"id": write_id(w.header.position), "ts": stamp(w.header.timestamp).to_string(),
                "past": w.header.had_read.0.iter().map(|id| write_id(*id)).collect::<Vec<_>>(), "changes": changes})
        }).collect();
        let snapshot: Vec<_> = self
            .writes
            .iter()
            .filter(|w| self.covered.contains(&w.header.position))
            .map(|w| write_id(w.header.position))
            .collect();
        json!({"mode": "reset", "writes": writes,
            "order": self.writes.iter().map(|w| write_id(w.header.position)).collect::<Vec<_>>(),
            "snapshot": snapshot, "covered": self.covered.iter().map(|w| write_id(*w)).collect::<Vec<_>>(),
            "read": self.writes.iter().filter(|w| w.header.store_log_read.covers(RESET))
                .map(|w| write_id(w.header.position)).collect::<Vec<_>>(),
            "reset": self.reset.then_some(1), "rowIds": (0..rows.len()).collect::<Vec<_>>(), "columns": COLUMNS.len()})
    }
}
pub(crate) fn column_id(column: &str) -> usize {
    COLUMNS
        .iter()
        .position(|c| *c == column)
        .expect("fixture column")
}
pub(crate) fn write_id(id: WriteId) -> u64 {
    assert_eq!(id.number, 1, "fixture uses one write per device");
    id.device.0
}
pub(crate) fn stamp(ts: Timestamp) -> u128 {
    ((ts.milliseconds() as u128) << 80) | ((ts.counter() as u128) << 64) | ts.device().0 as u128
}
fn state_json(id: usize, state: &RowState<Scalar>) -> Value {
    let mut cells: Vec<_> = state
        .cells()
        .iter()
        .map(|(c, v)| [column_id(c) as u64, write_id(v.write)])
        .collect();
    cells.sort();
    let mut lost: Vec<_> = state
        .lost()
        .iter()
        .map(|(k, v)| {
            [
                column_id(&k.column) as u64,
                write_id(k.write),
                v.incarnation,
                write_id(v.replaced_by),
            ]
        })
        .collect();
    lost.sort();
    json!({"id": id, "gen": state.generation(),
        "gw": state.generations().iter().map(|(g, w)| [*g, write_id(*w)]).collect::<Vec<_>>(), "cells": cells, "lost": lost})
}
pub(crate) fn result_json(db: &Database, rows: &[RowId]) -> Value {
    db.inspect_writer(|db| {
        let schema = crate::schema::Schema::read(db).unwrap();
        let store = crate::merge_store::MergeStore::from_schema(db, &schema);
        let merged: Vec<_> = rows.iter().enumerate().map(|(i, r)| state_json(i, &store.row(r).unwrap().state)).collect();
        let losses = db.query(crate::loss_record::SELECT, [], crate::loss_record::read).unwrap();
        let mut frozen = Vec::new();
        for loss in losses.into_iter().filter(|loss| loss.retired) {
            let LossValues::Row(cells) = loss.values else { panic!("boundary loss must hold a row") };
            let cause = match loss.cause {
                LossCause::Excluded { write, version } => json!({"schema": version, "write": write_id(write)}),
                cause => panic!("unexpected boundary cause {cause:?}"),
            };
            let mut values: Vec<_> = cells.iter().map(|(c, v)| [column_id(c) as u64, write_id(v.write)]).collect();
            values.sort();
            frozen.push(json!({"row": rows.iter().position(|r| *r == loss.row).unwrap(), "gen": loss.generation, "values": values, "cause": cause}));
        }
        frozen.sort_by_key(Value::to_string);
        json!({"rows": merged, "frozen": frozen})
    })
}

//! Rust/Lean comparisons with the queue model's per-change classifier and row
//! merge (spec/proofs/queue.md). Each case runs the real database receive or
//! reload path; the Lean side runs `classifyChange` and `mergeRows` on the same
//! original records, and the two sides' verdicts, losses and rows must agree.
use crate::pending::tests::Record as PendingRecord;
use crate::snapshot_write::tests::{frames, stream};
use crate::test_utils::tests::{run_lean, tables, Generator, RESET, SCHEMA};
use crate::tests::TestStore;
use crate::{ApplyOutcome, BoundaryCoverage, Database, SnapshotReload};
use coven_format::loss::LossCause;
use coven_format::snapshot_rows::ChangeVerdict;
use coven_format::value::{EntryPositions, Value as Scalar, WritePositions};
use coven_format::write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord};
use coven_format::write_sources::{CellSource, ChangeId, WriteSources};
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::{
    Audience, Change, ColumnValue, History, Operation, RowId, RowState, Timestamp, Write, WriteId,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;

/// Lean's `rowMetadata` sets a prefix of the columns, so updates set `a` or
/// `a` and `b`, and inserts all three.
const COLUMNS: [&str; 3] = ["a", "b", "id"];

fn column(name: &str) -> usize {
    COLUMNS
        .iter()
        .position(|c| *c == name)
        .expect("fixture column")
}

fn row(n: usize) -> RowId {
    RowId {
        table: "rows".into(),
        key: coven_format::key::encode_key(&[Scalar::Text(n.to_string())]).unwrap(),
        audience: Audience::Store,
    }
}

fn value(text: String) -> ColumnValue<Scalar> {
    ColumnValue {
        value: Scalar::Text(text),
        parents: BTreeMap::new(),
    }
}

/// Every write this one read: its had-read frontier expanded through each
/// gap-free log, plus its own earlier writes (D5).
fn full_past(header: &WriteHeader) -> BTreeSet<WriteId> {
    let own = WriteId {
        device: header.position.device,
        number: header.position.number - 1,
    };
    header
        .had_read
        .0
        .iter()
        .chain((own.number > 0).then_some(&own))
        .flat_map(|last| {
            (1..=last.number).map(|number| WriteId {
                device: last.device,
                number,
            })
        })
        .collect()
}

fn merge_write(record: &WriteRecord) -> Write<Scalar> {
    Write {
        id: record.header.position,
        timestamp: record.header.timestamp,
        had_read: full_past(&record.header),
        changes: record
            .parts
            .iter()
            .flat_map(|p| &p.rows)
            .map(|r| (r.row.clone(), r.change.clone()))
            .collect(),
    }
}

/// The boundary the covered prefix ends at: a kept reset, or a kept raise to
/// version 2 whose migration does or does not supply a converter.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Reset,
    Raise { convert: bool },
}

/// Original records in timestamp order, the snapshot's covered prefix and the
/// kept boundary every later author may or may not have read.
#[derive(Clone)]
struct QueueHistory {
    writes: Vec<WriteRecord>,
    covered: BTreeSet<WriteId>,
    kind: Kind,
}

impl QueueHistory {
    /// Authors observe their whole causal past. One that read the reset may
    /// not have reloaded yet: then it still sees ignored pre-reset data and its
    /// changes can use it as sources (§19.3).
    fn generate(rng: &mut Generator, kind: Kind) -> Self {
        let mut writes = Vec::<WriteRecord>::new();
        let mut numbers = BTreeMap::<DeviceId, u64>::new();
        let mut header = |device: u64, ms: u64| {
            let device = DeviceId(device);
            let number = numbers.entry(device).or_default();
            *number += 1;
            WriteHeader {
                position: WriteId {
                    device,
                    number: *number,
                },
                timestamp: Timestamp::new(ms, 0, device).unwrap(),
                had_read: WritePositions(Vec::new()),
                store_log_read: EntryPositions(Vec::new()),
                schema_version: 1,
                disposition: WriteDisposition::Apply,
            }
        };
        for n in 0..3 {
            let header = header(100 + n as u64, n as u64 + 1);
            writes.push(WriteRecord {
                header,
                parts: vec![WritePart {
                    audience: Audience::Store,
                    rows: vec![insert(row(n), n)],
                    dismissals: Vec::new(),
                }],
            });
        }
        let covered: BTreeSet<_> = writes.iter().map(|w| w.header.position).collect();
        let mut next_row = 3;
        for n in 3..16 {
            let mut write = WriteRecord {
                header: header(104 + rng.pick(3) as u64, n as u64 + 1),
                parts: Vec::new(),
            };
            let own = write.header.position.device;
            let mut past = BTreeSet::new();
            for previous in &writes {
                let id = previous.header.position;
                if id.device == own || rng.pick(3) == 0 {
                    past.insert(id);
                    past.extend(full_past(&previous.header));
                }
            }
            // A reset reader may not have reloaded yet; a migrated author has
            // reloaded from the raise, and an older app cannot read new writes.
            let reloaded = match kind {
                Kind::Reset => {
                    let reads_reset = rng.pick(2) == 0
                        || writes.iter().any(|w| {
                            past.contains(&w.header.position)
                                && w.header.store_log_read.covers(RESET)
                        });
                    if reads_reset {
                        past.extend(covered.iter().copied());
                        write.header.store_log_read = EntryPositions(vec![RESET]);
                    }
                    reads_reset && rng.pick(2) == 0
                }
                Kind::Raise { .. } => {
                    let migrated = rng.pick(2) == 0
                        || writes.iter().any(|w| {
                            past.contains(&w.header.position) && w.header.schema_version == 2
                        });
                    if migrated {
                        past.extend(covered.iter().copied());
                        write.header.schema_version = 2;
                    }
                    // A migrated author whose own raise lost still holds its
                    // own late old-schema data until it reloads.
                    migrated && rng.pick(2) == 0
                }
            };
            // A reloaded author drops what receivers drop and freezes every
            // change using it, as receivers do; one that has not reloaded
            // still holds that data.
            let mut kept = BTreeSet::new();
            for w in writes.iter().filter(|w| past.contains(&w.header.position)) {
                let ignored = !covered.contains(&w.header.position)
                    && match kind {
                        Kind::Reset => !w.header.store_log_read.covers(RESET),
                        Kind::Raise { convert } => w.header.schema_version < 2 && !convert,
                    };
                for change in w.parts.iter().flat_map(|p| &p.rows) {
                    let sources = change
                        .sources
                        .changes()
                        .all(|s| kept.contains(&(s.write, s.row.clone())));
                    if !(reloaded && (ignored || !sources)) {
                        kept.insert((w.header.position, change.row.clone()));
                    }
                }
            }
            let author: Vec<_> = writes
                .iter()
                .filter(|w| past.contains(&w.header.position))
                .map(|w| {
                    let mut merged = merge_write(w);
                    merged
                        .changes
                        .retain(|row, _| kept.contains(&(w.header.position, row.clone())));
                    merged
                })
                .collect();
            let mut targets = BTreeSet::new();
            for _ in 0..1 + rng.pick(2) {
                let target = if rng.pick(4) == 0 {
                    next_row += 1;
                    row(next_row - 1)
                } else {
                    row(rng.pick(next_row))
                };
                targets.insert(target);
            }
            let mut rows = Vec::new();
            for target in targets {
                let mut state = RowState::new(target.clone());
                for (index, previous) in author.iter().enumerate() {
                    if previous.changes.contains_key(&target) {
                        let oracle = History::new(author[..index].iter().cloned()).unwrap();
                        state = coven_merge::apply(&state, previous, &oracle).unwrap().state;
                    }
                }
                rows.push(authored(rng, &state, target, n));
            }
            let mut frontier = BTreeMap::new();
            for id in past.iter().filter(|id| id.device != own) {
                let number = frontier.entry(id.device).or_insert(0);
                *number = id.number.max(*number);
            }
            write.header.had_read = WritePositions(
                frontier
                    .into_iter()
                    .map(|(device, number)| WriteId { device, number })
                    .collect(),
            );
            write.parts = vec![WritePart {
                audience: Audience::Store,
                rows,
                dismissals: Vec::new(),
            }];
            writes.push(write);
        }
        Self {
            writes,
            covered,
            kind,
        }
    }

    fn index(&self, id: WriteId) -> usize {
        self.writes
            .iter()
            .position(|w| w.header.position == id)
            .expect("history write")
    }

    fn rows(&self) -> Vec<RowId> {
        self.writes
            .iter()
            .flat_map(|w| &w.parts)
            .flat_map(|p| &p.rows)
            .map(|r| r.row.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn input(&self, received: &[WriteId]) -> Value {
        let rows = self.rows();
        let writes: Vec<_> = self
            .writes
            .iter()
            .map(|w| {
                let changes: Vec<_> = w.parts.iter().flat_map(|p| &p.rows).map(|r| {
                    let (kind, values) = match &r.change.operation {
                        Operation::Insert(cols) => (0, cols.len()),
                        Operation::Update(cols) => (1, cols.len()),
                        Operation::Delete => (2, 0),
                    };
                    json!({"row": rows.iter().position(|x| *x == r.row).unwrap(), "kind": kind,
                        "generation": r.change.generation, "values": values,
                        "source": r.sources.generation.as_ref().map(|s| self.index(s.write)),
                        "setters": r.sources.setters.iter()
                            .map(|(c, s)| (column(c), self.index(s.change.write))).collect::<Vec<_>>(),
                        "frozen": Value::Null})
                }).collect();
                json!({"id": [w.header.position.device.0, w.header.position.number],
                    "timestamp": w.header.timestamp.milliseconds(),
                    "read": full_past(&w.header).iter().filter(|p| p.device != w.header.position.device)
                        .map(|p| [p.device.0, p.number]).collect::<Vec<_>>(),
                    "storeRead": if w.header.store_log_read.covers(RESET) { vec![1] } else { vec![] },
                    "version": w.header.schema_version, "disposition": "ordinary", "changes": changes})
            })
            .collect();
        let snapshot: Vec<_> = self
            .writes
            .iter()
            .enumerate()
            .filter(|(_, w)| self.covered.contains(&w.header.position))
            .map(|(i, _)| i)
            .collect();
        let covers: Vec<_> = self
            .covered
            .iter()
            .map(|p| [p.device.0, p.number])
            .collect();
        let (reset, raise, conversion) = match self.kind {
            Kind::Reset => (
                json!({"entry": 1, "covers": covers}),
                Value::Null,
                Value::Null,
            ),
            Kind::Raise { convert } => (
                Value::Null,
                json!({"entry": 2, "covers": covers, "version": 2}),
                json!({"version": 2, "convert": convert}),
            ),
        };
        json!({"mode": "receive", "writes": writes, "reset": reset, "raise": raise,
            "conversion": conversion, "drops": [], "snapshot": snapshot,
            "received": received.iter().map(|id| self.index(*id)).collect::<Vec<_>>(),
            "rows": rows.len(), "columns": COLUMNS.len()})
    }

    /// A causal arrival order of the uncovered writes, own predecessors first.
    fn arrival(&self, rng: &mut Generator) -> Vec<WriteId> {
        let mut applied: BTreeSet<_> = self.covered.clone();
        let mut remaining: Vec<_> = self
            .writes
            .iter()
            .filter(|w| !self.covered.contains(&w.header.position))
            .collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let ready: Vec<_> = (0..remaining.len())
                .filter(|i| {
                    let h = &remaining[*i].header;
                    h.had_read.0.iter().all(|id| applied.contains(id))
                        && (1..h.position.number).all(|number| {
                            applied.contains(&WriteId {
                                device: h.position.device,
                                number,
                            })
                        })
                })
                .collect();
            let write = remaining.remove(ready[rng.pick(ready.len())]);
            applied.insert(write.header.position);
            order.push(write.header.position);
        }
        order
    }
}

fn insert(target: RowId, n: usize) -> RowChange {
    let id = coven_format::key::decode_key(&target.key)
        .unwrap()
        .remove(0);
    RowChange {
        row: target,
        change: Change {
            generation: 0,
            operation: Operation::Insert(BTreeMap::from([
                ("a".into(), value(format!("a{n}"))),
                ("b".into(), value(format!("b{n}"))),
                (
                    "id".into(),
                    ColumnValue {
                        value: id,
                        parents: BTreeMap::new(),
                    },
                ),
            ])),
        },
        old: BTreeMap::new(),
        sources: WriteSources {
            generation: None,
            setters: BTreeMap::new(),
        },
    }
}

/// A change authored from the author's actual merge state, naming the write
/// that started the generation it read and every cell setter it replaced.
fn authored(rng: &mut Generator, state: &RowState<Scalar>, target: RowId, n: usize) -> RowChange {
    let mut change = insert(target.clone(), n);
    change.change.generation = state.generation();
    change.sources.generation =
        state
            .generations()
            .get(&state.generation())
            .map(|write| ChangeId {
                write: *write,
                row: target.clone(),
            });
    if state.present() {
        if rng.pick(4) == 0 {
            change.change.operation = Operation::Delete;
            change.old = state
                .cells()
                .iter()
                .map(|(c, v)| (c.clone(), v.value.value.clone()))
                .collect();
        } else {
            let columns = &COLUMNS[..1 + rng.pick(2)];
            change.change.operation = Operation::Update(
                columns
                    .iter()
                    .map(|c| ((*c).to_owned(), value(format!("{c}{n}"))))
                    .collect(),
            );
            change.old = columns
                .iter()
                .map(|c| ((*c).to_owned(), state.cells()[*c].value.value.clone()))
                .collect();
        }
    }
    change.sources.setters = state
        .cells()
        .iter()
        .filter(|(c, _)| match &change.change.operation {
            Operation::Insert(_) => false,
            Operation::Update(columns) => columns.contains_key(*c),
            Operation::Delete => true,
        })
        .map(|(c, cell)| {
            (
                c.clone(),
                CellSource {
                    change: ChangeId {
                        write: cell.write,
                        row: target.clone(),
                    },
                    column: c.clone(),
                },
            )
        })
        .collect();
    change
}

async fn install_reset(db: &Database) {
    let entry = coven_format::store_log::StoreLogEntry {
        position: RESET,
        timestamp: Timestamp::new(1_000_000, 0, RESET.device).unwrap(),
        author: crate::store_log::tests::member(1),
        had_read: EntryPositions(Vec::new()),
        change: coven_format::store_log::StoreChange::Reset {
            snapshot: crate::snapshot_write::tests::id(Audience::Store),
        },
    };
    let mut replay = crate::StoreLogReplay::default();
    replay.entries.insert(RESET, crate::EntryOutcome::Kept);
    replay.state.resets.insert(
        Audience::Store,
        crate::snapshot_write::tests::id(Audience::Store),
    );
    db.apply_store_log::<PendingRecord>(
        crate::ReplayEntry {
            entry,
            check: crate::StoreLogCheck::Allowed,
            stored_at: std::time::UNIX_EPOCH,
        },
        replay,
        Vec::new(),
    )
    .await
    .unwrap();
}

/// The real receiver: a reset reload from the covered prefix, then the
/// uncovered writes either inside that reload or received afterwards.
async fn rust_result(history: &QueueHistory, received: &[WriteId], streamed: bool) -> Value {
    if let Kind::Raise { convert } = history.kind {
        return raise_result(history, received, convert).await;
    }
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(tables(), SCHEMA).await.unwrap();
    let target = target_store.schema(tables(), SCHEMA).await.unwrap();
    for write in history
        .writes
        .iter()
        .filter(|w| history.covered.contains(&w.header.position))
    {
        assert_eq!(
            source.receive(write.clone().into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    let snapshot = frames(&source, Audience::Store).await;
    install_reset(&target).await;
    let arriving: Vec<_> = received
        .iter()
        .map(|id| history.writes[history.index(*id)].clone())
        .collect();
    let mut reload = SnapshotReload::new(
        vec![snapshot.input()],
        if streamed {
            arriving.iter().map(stream).collect()
        } else {
            Vec::new()
        },
    );
    reload.boundaries = Some(vec![BoundaryCoverage {
        entry: RESET,
        included: snapshot.prefix.writes.clone(),
    }]);
    target
        .load_snapshots::<PendingRecord, _, _>(reload)
        .await
        .unwrap();
    if !streamed {
        for write in arriving {
            assert_eq!(
                target.receive(write.into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
    }
    let positions = target.sync_state(Vec::new()).await.unwrap().positions;
    assert!(
        history
            .writes
            .iter()
            .all(|w| positions.covers(w.header.position)),
        "every original position is consumed"
    );
    let result = observed(&target, history, received);
    source.close().await.unwrap();
    target.close().await.unwrap();
    result
}

/// The real receiver of a kept raise: it applied the covered prefix before its
/// app migrated, then receives the uncovered writes through §17.1's rule.
async fn raise_result(history: &QueueHistory, received: &[WriteId], convert: bool) -> Value {
    let store = TestStore::new();
    let target = store.schema(tables(), SCHEMA).await.unwrap();
    for write in history
        .writes
        .iter()
        .filter(|w| history.covered.contains(&w.header.position))
    {
        target.receive(write.clone().into()).await.unwrap();
    }
    target.close().await.unwrap();
    let mut raise = crate::Migration::sql(2, "index", "CREATE INDEX rows_a ON rows(a)");
    if convert {
        raise = raise.writes(|_| Ok(()));
    }
    let target = store
        .builder(
            tables(),
            vec![crate::Migration::sql(1, "rows", SCHEMA), raise],
        )
        .open()
        .await
        .unwrap();
    target.exclude_later_writes(2).await;
    for id in received {
        assert_eq!(
            target
                .receive(history.writes[history.index(*id)].clone().into())
                .await
                .unwrap(),
            ApplyOutcome::Applied
        );
    }
    let positions = target.sync_state(Vec::new()).await.unwrap().positions;
    assert!(history
        .writes
        .iter()
        .all(|w| positions.covers(w.header.position)));
    let result = observed(&target, history, received);
    target.close().await.unwrap();
    result
}

fn verdict_json(history: &QueueHistory, verdict: &ChangeVerdict) -> Value {
    match verdict {
        ChangeVerdict::Applied => json!({"kind": "applied"}),
        ChangeVerdict::Ignored(entry) => {
            assert_eq!(*entry, RESET);
            json!({"kind": "ignored", "entry": 1})
        }
        ChangeVerdict::Excluded(version) => {
            json!({"kind": "frozen", "cause": {"schema": version}})
        }
        ChangeVerdict::Dependent(sources) => json!({"kind": "frozen", "cause": {
            "sources": sources.iter().map(|s| history.index(s.write)).collect::<Vec<_>>()}}),
        verdict => panic!("unexpected verdict {verdict:?}"),
    }
}

/// Per-change verdicts from the retained originals, the frozen losses those
/// verdicts recorded, and every row's merge record.
fn observed(db: &Database, history: &QueueHistory, received: &[WriteId]) -> Value {
    let rows = history.rows();
    db.inspect_writer(|db| {
        let mut verdicts = BTreeMap::new();
        crate::write_sources::visit_evidence(
            db,
            &Audience::Store,
            &WritePositions(
                history
                    .writes
                    .iter()
                    .map(|w| w.header.position)
                    .fold(BTreeMap::new(), |mut max: BTreeMap<_, u64>, p| {
                        let n = max.entry(p.device).or_default();
                        *n = (*n).max(p.number);
                        max
                    })
                    .into_iter()
                    .map(|(device, number)| WriteId { device, number })
                    .collect(),
            ),
            |change| {
                verdicts.insert(
                    (history.index(change.header.position), change.change.row.clone()),
                    change.verdict,
                );
                Ok::<_, crate::DbError>(())
            },
        )
        .unwrap();
        let mut changes = Vec::new();
        for id in received {
            let w = history.index(*id);
            for change in history.writes[w].parts.iter().flat_map(|p| &p.rows) {
                let mut verdict = verdict_json(history, &verdicts[&(w, change.row.clone())]);
                if let Some(cause) = verdict.get_mut("cause") {
                    cause["write"] = json!(w);
                }
                changes.push(json!({"write": w, "row": rows.iter().position(|r| *r == change.row).unwrap(),
                    "verdict": verdict}));
            }
        }
        let mut frozen = Vec::new();
        // Ordinary cell losses are compared through the merge rows below.
        for loss in db
            .query(crate::loss_record::SELECT, [], crate::loss_record::read)
            .unwrap()
            .into_iter()
            .filter(|loss| loss.retired)
        {
            let row = rows.iter().position(|r| *r == loss.row).unwrap();
            frozen.push(match loss.cause {
                LossCause::Dependency { write, sources } => json!({"write": history.index(write),
                    "row": row, "sources": sources.iter().map(|s| history.index(s.write)).collect::<Vec<_>>()}),
                LossCause::Excluded { write, version } => {
                    json!({"write": history.index(write), "row": row, "schema": version})
                }
                cause => panic!("unexpected loss cause {cause:?}"),
            });
        }
        let schema = crate::schema::Schema::read(db).unwrap();
        let store = crate::merge_store::MergeStore::from_schema(db, &schema);
        let merged: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let state = store.row(r).unwrap().state;
                let mut cells: Vec<_> = state
                    .cells()
                    .iter()
                    .map(|(c, v)| [column(c), history.index(v.write)])
                    .collect();
                cells.sort();
                let mut lost: Vec<_> = state
                    .lost()
                    .iter()
                    .map(|(k, v)| {
                        [
                            column(&k.column) as u64,
                            history.index(k.write) as u64,
                            v.incarnation,
                            history.index(v.replaced_by) as u64,
                        ]
                    })
                    .collect();
                lost.sort();
                json!({"id": i, "gen": state.generation(),
                    "gw": state.generations().iter().map(|(g, w)| json!([g, history.index(*w)])).collect::<Vec<_>>(),
                    "cells": cells, "lost": lost})
            })
            .collect();
        json!({"changes": changes, "frozen": frozen, "rows": merged})
    })
}

/// Lean's verdicts plus the frozen records they imply.
fn lean_result(runner: &OsStr, history: &QueueHistory, received: &[WriteId]) -> Value {
    let mut result = run_lean(runner, &history.input(received));
    assert_eq!(
        result["dataValid"],
        json!(true),
        "generated history is valid"
    );
    let mut frozen = Vec::new();
    for change in result["changes"].as_array().unwrap() {
        if let Some(cause) = change["verdict"].get("cause") {
            frozen.push(match cause.get("schema") {
                Some(version) => json!({"write": change["write"], "row": change["row"], "schema": version}),
                None => json!({"write": change["write"], "row": change["row"], "sources": cause["sources"]}),
            });
        }
    }
    let object = result.as_object_mut().unwrap();
    object.remove("dataValid");
    object.insert("frozen".into(), json!(frozen));
    result
}

fn sorted(mut result: Value) -> Value {
    for key in ["changes", "frozen"] {
        result[key]
            .as_array_mut()
            .unwrap()
            .sort_by_key(Value::to_string);
    }
    result
}

#[tokio::test]
#[ignore = "requires COVEN_QUEUE_LEAN; scripts/check.sh supplies the queue runner"]
async fn queue_reset_differential_histories() {
    let runner = std::env::var_os("COVEN_QUEUE_LEAN").expect("built queueRunner");
    let mut dependent = 0;
    for seed in 0..64 {
        let mut rng = Generator::new(seed + 40_000);
        let history = QueueHistory::generate(&mut rng, Kind::Reset);
        for streamed in [true, false] {
            let received = history.arrival(&mut rng);
            let rust = sorted(rust_result(&history, &received, streamed).await);
            let lean = sorted(lean_result(&runner, &history, &received));
            assert_eq!(
                rust,
                lean,
                "queue reset differential, seed {seed}, streamed {streamed}\ninput: {}",
                history.input(&received)
            );
            dependent += rust["frozen"].as_array().unwrap().len();
        }
    }
    assert!(
        dependent > 0,
        "histories exercise changes using ignored data"
    );
}

/// §17.1's receive rule after a kept raise, with and without a converter: a
/// late old-schema change converts, or is a frozen schema loss, and a change
/// built on dropped data is a dependency loss beside its write's others.
#[tokio::test]
#[ignore = "requires COVEN_QUEUE_LEAN; scripts/check.sh supplies the queue runner"]
async fn queue_conversion_differential_histories() {
    let runner = std::env::var_os("COVEN_QUEUE_LEAN").expect("built queueRunner");
    let (mut dependent, mut schema) = (0, 0);
    for seed in 0..48 {
        for convert in [true, false] {
            let mut rng = Generator::new(seed + 50_000);
            let history = QueueHistory::generate(&mut rng, Kind::Raise { convert });
            let received = history.arrival(&mut rng);
            let rust = sorted(rust_result(&history, &received, false).await);
            let lean = sorted(lean_result(&runner, &history, &received));
            assert_eq!(
                rust,
                lean,
                "queue conversion differential, seed {seed}, convert {convert}\ninput: {}",
                history.input(&received)
            );
            for loss in rust["frozen"].as_array().unwrap() {
                if loss.get("schema").is_some() {
                    schema += 1;
                } else {
                    dependent += 1;
                }
            }
        }
    }
    assert!(dependent > 0 && schema > 0, "{dependent} {schema}");
}

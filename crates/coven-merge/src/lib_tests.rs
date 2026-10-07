use crate::*;
use coven_foundation::id_source::{CircleId, DeviceId};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(crate) fn row(n: u64) -> RowId {
    RowId {
        table: "rows".into(),
        key: n.to_be_bytes().to_vec(),
        audience: Audience::Store,
    }
}
pub(crate) fn circle(n: u64, audience: u8) -> RowId {
    RowId {
        audience: Audience::Circle(CircleId(Uuid::from_u128(audience.into()))),
        ..row(n)
    }
}

#[test]
fn audiences_order_store_then_uuid_bytes() {
    let mut bytes = vec![[0; 16], [255; 16]];
    for i in 0..16 {
        let mut id = [0; 16];
        id[i] = 1;
        bytes.push(id);
    }
    bytes.sort();
    let audiences: Vec<_> = bytes
        .into_iter()
        .map(|bytes| Audience::Circle(CircleId(Uuid::from_bytes(bytes))))
        .collect();
    assert!(Audience::Store < audiences[0]);
    assert!(audiences.windows(2).all(|pair| pair[0] < pair[1]));
}

pub(crate) fn id(n: u64) -> WriteId {
    WriteId {
        device: DeviceId(n),
        number: 1,
    }
}
pub(crate) fn stamp(n: u64) -> Timestamp {
    Timestamp::new(n, 0, DeviceId(n)).unwrap()
}
pub(crate) fn columns(values: &[(&str, &str)]) -> BTreeMap<String, ColumnValue<String>> {
    values
        .iter()
        .map(|(c, v)| {
            (
                (*c).into(),
                ColumnValue {
                    value: (*v).into(),
                    parents: BTreeMap::new(),
                },
            )
        })
        .collect()
}
pub(crate) fn change(generation: u64, kind: u8, values: &[(&str, &str)]) -> Change<String> {
    let operation = match kind {
        0 => Operation::Insert(columns(values)),
        1 => Operation::Update(columns(values)),
        2 => {
            assert!(values.is_empty());
            Operation::Delete
        }
        _ => panic!("unknown fixture operation"),
    };
    Change {
        generation,
        operation,
    }
}
pub(crate) fn write(
    n: u64,
    ms: u64,
    past: &[u64],
    changes: Vec<(RowId, Change<String>)>,
) -> Write<String> {
    Write {
        id: id(n),
        timestamp: Timestamp::new(ms, 0, DeviceId(n)).unwrap(),
        had_read: past.iter().copied().map(id).collect(),
        changes: changes.into_iter().collect(),
    }
}
pub(crate) fn fold(writes: &[Write<String>], order: &[usize]) -> BTreeMap<RowId, RowState<String>> {
    let mut states = BTreeMap::new();
    let mut applied = Vec::new();
    for &n in order {
        let write = &writes[n];
        let oracle = History::new(applied.clone()).unwrap();
        for row in write.changes.keys() {
            let state = states
                .entry(row.clone())
                .or_insert_with(|| RowState::new(row.clone()));
            let restored = RowState::from_parts(
                row.clone(),
                state.generations().clone(),
                state.cells().clone(),
                state.lost().clone(),
                &oracle,
            )
            .unwrap();
            assert_eq!(&restored, state);
            let update = apply(&restored, write, &oracle).unwrap();
            let mut lost = state.lost().clone();
            for delta in &update.lost_changes {
                match delta {
                    LostChange::Put(key, value) => {
                        lost.insert(key.clone(), value.clone());
                    }
                    LostChange::Remove(key) => {
                        assert!(lost.remove(key).is_some());
                    }
                }
            }
            assert_eq!(&lost, update.state.lost());
            *state = update.state;
        }
        applied.push(write.clone());
    }
    states
}
pub(crate) fn agree(
    writes: &[Write<String>],
    orders: &[&[usize]],
) -> BTreeMap<RowId, RowState<String>> {
    let reference = from_writes(&History::new(writes.to_vec()).unwrap()).unwrap();
    for order in orders {
        assert_eq!(fold(writes, order), reference);
    }
    reference
}
pub(crate) fn value<'a>(
    states: &'a BTreeMap<RowId, RowState<String>>,
    r: &RowId,
    column: &str,
) -> &'a str {
    &states[r].cells()[column].value.value
}

#[derive(Clone, Default)]
pub(crate) struct MemoryView {
    pub(crate) data: BTreeMap<RowId, RemovalRow>,
    pub(crate) checks: BTreeMap<RowId, Constraints>,
    pub(crate) order: Vec<RowId>,
}
impl MemoryView {
    pub(crate) fn present(&mut self, r: RowId, generation: u64, started: Timestamp) {
        self.data.insert(
            r,
            RemovalRow::Present {
                generation,
                started,
                references: BTreeMap::new(),
                deleted_circle: false,
            },
        );
    }
    pub(crate) fn reference(
        &mut self,
        child: &RowId,
        parent: RowId,
        generation: u64,
        on_delete: OnDelete,
    ) {
        match self.data.get_mut(child).unwrap() {
            RemovalRow::Present { references, .. } => {
                references.insert(
                    crate::ForeignKey::new(["parent"], parent.table.clone(), ["id"]),
                    Reference {
                        parent: Parent {
                            row: parent,
                            generation,
                        },
                        on_delete,
                    },
                );
            }
            _ => panic!("reference fixture needs a present child"),
        }
    }
    pub(crate) fn claim<const N: usize>(
        &mut self,
        row: &RowId,
        columns: [&str; N],
        value: &str,
        timestamp: Timestamp,
    ) {
        self.checks.entry(row.clone()).or_default().unique.insert(
            columns.into(),
            UniqueClaim {
                value: value.as_bytes().to_vec(),
                timestamp,
            },
        );
    }
    pub(crate) fn from_states(
        states: &BTreeMap<RowId, RowState<String>>,
        history: &History<String>,
    ) -> Self {
        let mut view = Self::default();
        for (r, s) in states {
            if s.present() {
                view.present(
                    r.clone(),
                    s.generation(),
                    history.timestamp(s.generations()[&s.generation()]).unwrap(),
                );
            } else {
                view.data.insert(
                    r.clone(),
                    RemovalRow::Absent {
                        generation: s.generation(),
                    },
                );
            }
        }
        view
    }
}
impl RemovalView for MemoryView {
    type Error = MergeError;
    fn rows(&self) -> Result<Vec<RowId>, MergeError> {
        if self.order.is_empty() {
            Ok(self.data.keys().cloned().collect())
        } else {
            Ok(self.order.clone())
        }
    }
    fn row(&self, row: &RowId) -> Result<RemovalRow, MergeError> {
        Ok(self
            .data
            .get(row)
            .cloned()
            .unwrap_or(RemovalRow::Absent { generation: 0 }))
    }
    fn constraints(
        &self,
        row: &RowId,
        _: &BTreeMap<crate::ForeignKey, ReferenceValue>,
    ) -> Result<Constraints, MergeError> {
        Ok(self.checks.get(row).cloned().unwrap_or_default())
    }
    fn related(&self, r: &RowId) -> Result<BTreeSet<RowId>, MergeError> {
        let mut edges = BTreeSet::new();
        for (child, facts) in &self.data {
            if let RemovalRow::Present { references, .. } = facts {
                for reference in references.values() {
                    let parent = &reference.parent.row;
                    if child == r {
                        edges.insert(parent.clone());
                    }
                    if parent == r {
                        edges.insert(child.clone());
                    }
                }
            }
        }
        Ok(edges)
    }
    fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, MergeError> {
        view_groups(self, row)
    }
    fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, MergeError> {
        view_members(self, self.data.keys(), group)
    }
}

// These adapters evaluate each test view's own constraint implementation so
// null substitutions participate in region discovery too.
pub(crate) fn view_groups(
    view: &impl RemovalView<Error = MergeError>,
    row: &RowId,
) -> Result<BTreeSet<Group>, MergeError> {
    let mut groups = BTreeSet::from([Group::Key {
        table: row.table.clone(),
        key: row.key.clone(),
    }]);
    let mut resolved = BTreeMap::new();
    if let RemovalRow::Present { references, .. } = view.row(row)? {
        for (name, reference) in references {
            let generation = view.row(&reference.parent.row)?.generation();
            resolved.insert(name, resolve_reference(row, &reference, generation)?);
        }
    }
    for (constraint, claim) in view.constraints(row, &resolved)?.unique.into_iter() {
        groups.insert(Group::Claim {
            table: row.table.clone(),
            audience: row.audience.clone(),
            constraint,
            value: claim.value,
        });
    }
    Ok(groups)
}

pub(crate) fn view_members<'a>(
    view: &impl RemovalView<Error = MergeError>,
    rows: impl IntoIterator<Item = &'a RowId>,
    group: &Group,
) -> Result<BTreeSet<RowId>, MergeError> {
    let mut members = BTreeSet::new();
    for row in rows {
        if view.groups(row)?.contains(group) {
            members.insert(row.clone());
        }
    }
    Ok(members)
}

/// Deterministic xorshift: reproducible histories without an ambient random
/// source or another dependency. Each seed is printed by failing assertions.
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
    pub(crate) fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            let j = self.pick(i + 1);
            values.swap(i, j);
        }
    }
    pub(crate) fn order(&mut self, history: &[Write<String>]) -> Vec<usize> {
        let mut applied = BTreeSet::new();
        let mut order = Vec::new();
        while order.len() < history.len() {
            let available: Vec<_> = history
                .iter()
                .enumerate()
                .filter(|(_, w)| !applied.contains(&w.id) && w.had_read.is_subset(&applied))
                .map(|(i, _)| i)
                .collect();
            assert!(!available.is_empty());
            let i = available[self.pick(available.len())];
            applied.insert(history[i].id);
            order.push(i);
        }
        order
    }
    pub(crate) fn history(&mut self, count: usize) -> Vec<Write<String>> {
        let mut writes: Vec<Write<String>> = Vec::new();
        let mut read = vec![BTreeSet::new(); 4];
        let mut numbers = [0; 4];
        let rows = random_rows();
        for _ in 0..count {
            let device = self.pick(4);
            for previous in &writes {
                if self.pick(5) == 0 {
                    read[device].insert(previous.id);
                    read[device].extend(previous.had_read.iter().copied());
                }
            }
            let author = History::new(
                writes
                    .iter()
                    .filter(|w| read[device].contains(&w.id))
                    .cloned(),
            )
            .unwrap();
            let states = from_writes(&author).unwrap();
            let mut changes = BTreeMap::new();
            for _ in 0..1 + self.pick(3) {
                let row = rows[self.pick(rows.len())].clone();
                let generation = states.get(&row).map_or(0, RowState::generation);
                let operation = if generation % 2 == 1 && self.pick(4) == 0 {
                    Operation::Delete
                } else {
                    let mut columns = BTreeMap::new();
                    for col in 0..3 {
                        if generation % 2 == 0 || self.pick(2) == 0 {
                            columns.insert(
                                col.to_string(),
                                ColumnValue {
                                    value: self.pick(12).to_string(),
                                    parents: BTreeMap::new(),
                                },
                            );
                        }
                    }
                    if generation % 2 == 0 {
                        Operation::Insert(columns)
                    } else {
                        Operation::Update(columns)
                    }
                };
                changes.insert(
                    row,
                    Change {
                        generation,
                        operation,
                    },
                );
            }
            numbers[device] += 1;
            let id = WriteId {
                device: DeviceId(device as u64 + 1),
                number: numbers[device],
            };
            let latest = writes
                .iter()
                .filter(|w| read[device].contains(&w.id))
                .map(|w| w.timestamp)
                .max();
            let timestamp = Timestamp::next(latest, self.pick(40) as u64, id.device).unwrap();
            writes.push(Write {
                id,
                timestamp,
                had_read: read[device].clone(),
                changes,
            });
            read[device].insert(id);
        }
        History::new(writes.clone()).unwrap();
        writes
    }
}
pub(crate) fn random_rows() -> Vec<RowId> {
    vec![
        row(0),
        row(1),
        row(2),
        row(3),
        circle(0, 1),
        circle(1, 1),
        circle(0, 2),
        circle(1, 2),
    ]
}
pub(crate) fn generated_view(
    states: &BTreeMap<RowId, RowState<String>>,
    history: &History<String>,
    seed: u64,
) -> MemoryView {
    let mut view = MemoryView::from_states(states, history);
    for r in random_rows() {
        view.data
            .entry(r)
            .or_insert(RemovalRow::Absent { generation: 0 });
    }
    for (r, state) in states {
        if state.present() {
            let number = |c: &str| {
                state
                    .cells()
                    .get(c)
                    .map(|v| v.value.value.parse::<u64>().unwrap())
            };
            if let Some(a) = number("0") {
                if a % 4 == 0 {
                    let action = match (a / 4 + seed) % 4 {
                        0 => OnDelete::Cascade,
                        1 => OnDelete::Restrict,
                        2 => OnDelete::NoAction,
                        _ => OnDelete::SetNull {
                            permitted: seed.is_multiple_of(2),
                        },
                    };
                    view.reference(
                        r,
                        row(number("2").expect("generated inserts set all columns") % 4),
                        1,
                        action,
                    );
                }
                if a % 3 != 0 {
                    view.claim(
                        r,
                        ["title"],
                        &(a % 3).to_string(),
                        state.claim_timestamp(&["0".into()], history).unwrap(),
                    );
                }
            }
            if let (Some(a), Some(b)) = (number("0"), number("1")) {
                if b % 4 == 2 {
                    view.claim(
                        r,
                        ["folder", "title"],
                        &format!("{}/{}", a % 2, b % 3),
                        state
                            .claim_timestamp(&["0".into(), "1".into()], history)
                            .unwrap(),
                    );
                }
                if b.is_multiple_of(2) {
                    view.checks.entry(r.clone()).or_default().unique.insert(
                        UniqueConstraint {
                            terms: vec!["lower(title)".into()],
                            partial: seed.is_multiple_of(2).then(|| "active=1".into()),
                        },
                        UniqueClaim {
                            value: (a % 2).to_string().into_bytes(),
                            timestamp: state.claim_timestamp(&["0".into()], history).unwrap(),
                        },
                    );
                }
                if a > b && seed.is_multiple_of(3) {
                    view.checks
                        .entry(r.clone())
                        .or_default()
                        .failed_checks
                        .insert("range".into());
                }
            }
            if let RemovalRow::Present { deleted_circle, .. } = view.data.get_mut(r).unwrap() {
                *deleted_circle = r.audience == circle(0, 2).audience && seed.is_multiple_of(7);
            }
        }
    }
    view
}

#[test]
fn random_causal_orders_match_the_set_definition_and_rule_orders() {
    for seed in 0..256 {
        let mut rng = Generator::new(seed);
        let writes = rng.history(28);
        let history = History::new(writes.clone()).unwrap();
        let expected = from_writes(&history).unwrap();
        let view = generated_view(&expected, &history, seed);
        let removed = removals(&view).unwrap();
        for _ in 0..6 {
            let order = rng.order(&writes);
            let states = fold(&writes, &order);
            assert_eq!(states, expected, "seed {seed}, order {order:?}");
            let mut reordered = generated_view(&states, &history, seed);
            reordered.order = reordered.data.keys().cloned().collect();
            rng.shuffle(&mut reordered.order);
            assert_eq!(
                removals(&reordered).unwrap(),
                removed,
                "rule order, seed {seed}"
            );
        }
        // Apply one extra write and patch only the old/new dependency region.
        let last = writes.last().unwrap();
        let previous = History::new(writes[..writes.len() - 1].to_vec()).unwrap();
        let before = generated_view(&from_writes(&previous).unwrap(), &previous, seed);
        let mut patched = removals(&before).unwrap();
        let patch = recompute(&before, &view, last.changes.keys().cloned()).unwrap();
        for r in &patch.region {
            patched.removed.remove(r);
            patched.references.remove(r);
        }
        patched.removed.extend(patch.removed);
        patched.references.extend(patch.references);
        patched.region.extend(patch.region);
        assert_eq!(patched, removed, "locality, seed {seed}");
    }
}

mod differential {
    use super::*;
    use serde_json::{json, Value};
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    fn scalar(ts: Timestamp) -> u128 {
        ((ts.milliseconds() as u128) << 80) | ((ts.counter() as u128) << 64) | ts.device().0 as u128
    }
    fn rule_number(rule: &Rule) -> u64 {
        match rule {
            Rule::ForeignKey(_) => 0,
            Rule::Check(_) => 1,
            Rule::DeletedCircle => 2,
            Rule::OtherAudience => 3,
            Rule::Unique(_) => 4,
        }
    }
    fn compare(runner: &std::ffi::OsStr, writes: &[Write<String>], order: &[usize], seed: u64) {
        let history = History::new(writes.to_vec()).unwrap();
        let states = fold(writes, order);
        let mut view = generated_view(&states, &history, seed);
        let mut rng = Generator::new(seed);
        view.order = view.data.keys().cloned().collect();
        rng.shuffle(&mut view.order);
        let removed = removals(&view).unwrap();
        let rows: Vec<_> = view.data.keys().cloned().collect();
        let row_ids: BTreeMap<_, _> = rows
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, r)| (r, i))
            .collect();
        let write_ids: BTreeMap<_, _> = writes.iter().enumerate().map(|(i, w)| (w.id, i)).collect();
        let mut claims = BTreeMap::new();
        let mut input_rows = Vec::new();
        for r in &view.order {
            let data = view.row(r).unwrap();
            let references = removed.references.get(r).cloned().unwrap_or_default();
            let constraints = view.constraints(r, &references).unwrap();
            let refs: Vec<_> = references
                .values()
                .filter_map(|reference| match reference {
                    ReferenceValue::Original { parent, stale } => {
                        Some(json!({"parent": row_ids[&parent.row], "stale": stale}))
                    }
                    ReferenceValue::Null => None,
                })
                .collect();
            let mut row_claims = Vec::new();
            for (name, claim) in constraints.unique.iter() {
                let key = (
                    r.table.clone(),
                    r.audience.clone(),
                    name.clone(),
                    claim.value.clone(),
                );
                let next = claims.len();
                let key = *claims.entry(key).or_insert(next);
                row_claims.push(json!({"con": {"terms":name.terms,"partial":name.partial}, "value": key, "ts": scalar(claim.timestamp).to_string(), "other": false}));
            }
            let (deleted, started) = match data {
                RemovalRow::Present {
                    deleted_circle,
                    started,
                    ..
                } => (deleted_circle, Some(started)),
                RemovalRow::Absent { .. } => (false, None),
            };
            if let Some(started) = started {
                // Store rows precede every circle claim, including a circle
                // generation stamped at zero. Shift circle stamps by one.
                let ts = if r.audience == Audience::Store {
                    0
                } else {
                    scalar(started) + 1
                };
                let key = u64::from_be_bytes(r.key.clone().try_into().unwrap());
                row_claims
                    .push(json!({"con": {"terms":[],"partial":null}, "value": key, "ts": ts.to_string(), "other": true}));
            }
            input_rows.push(json!({"id": row_ids[r], "present": data.present(), "refs": refs,
                "check": !constraints.failed_checks.is_empty(), "deleted": deleted,
                "claims": row_claims, "rank": u64::from_be_bytes(r.key.clone().try_into().unwrap())}));
        }
        let input_writes: Vec<_> = writes.iter().map(|w| {
            let changes: Vec<_> = w.changes.iter().map(|(r, ch)| {
                let (kind, cols): (_, Vec<_>) = match &ch.operation {
                    Operation::Insert(c) => (0, c.keys().map(|c| c.parse::<u64>().unwrap()).collect()),
                    Operation::Update(c) => (1, c.keys().map(|c| c.parse::<u64>().unwrap()).collect()),
                    Operation::Delete => (2, vec![]),
                };
                json!({"row": row_ids[r], "kind": kind, "gen": ch.generation, "cols": cols})
            }).collect();
            json!({"id": write_ids[&w.id], "ts": scalar(w.timestamp).to_string(),
                "past": w.had_read.iter().map(|id| write_ids[id]).collect::<Vec<_>>(), "changes": changes})
        }).collect();
        let input =
            json!({"writes": input_writes, "order": order, "rows": input_rows, "columns": 3});
        let expected: Vec<_> = view
            .order
            .iter()
            .map(|r| {
                let empty = RowState::new(r.clone());
                let state = states.get(r).unwrap_or(&empty);
                let gw: Vec<_> = state
                    .generations()
                    .iter()
                    .map(|(g, w)| json!([g, write_ids[w]]))
                    .collect();
                let cells: Vec<_> = state
                    .cells()
                    .iter()
                    .map(|(c, v)| json!([c.parse::<u64>().unwrap(), write_ids[&v.write]]))
                    .collect();
                let mut lost: Vec<_> = state
                    .lost()
                    .iter()
                    .map(|(k, v)| {
                        [
                            k.column.parse::<u64>().unwrap(),
                            write_ids[&k.write] as u64,
                            v.incarnation,
                            write_ids[&v.replaced_by] as u64,
                        ]
                    })
                    .collect();
                lost.sort();
                let rules: BTreeSet<_> = removed
                    .removed
                    .get(r)
                    .into_iter()
                    .flatten()
                    .map(rule_number)
                    .collect();
                json!({"id": row_ids[r], "gen": state.generation(), "gw": gw, "cells": cells,
                "lost": lost, "removed": removed.removed.contains_key(r), "rules": rules})
            })
            .collect();
        let mut child = Command::new(runner)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start Lean model runner");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.to_string().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "Lean runner failed, seed {seed}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let actual: Value = serde_json::from_slice(&output.stdout).expect("Lean JSON result");
        assert_eq!(
            actual,
            json!(expected),
            "Lean differential, seed {seed}, order {order:?}, input {input}"
        );
    }

    #[test]
    #[ignore = "requires the Lean executable; scripts/check.sh supplies COVEN_MERGE_LEAN"]
    fn lean_differential() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN")
            .expect("COVEN_MERGE_LEAN must name the built Lean runner");
        for seed in 0..128 {
            let mut rng = Generator::new(seed + 10_000);
            let writes = rng.history(24);
            for _ in 0..3 {
                compare(&runner, &writes, &rng.order(&writes), seed);
            }
            let prefix = 1 + rng.pick(writes.len());
            let writes = &writes[..prefix];
            compare(&runner, writes, &rng.order(writes), seed);
        }
    }
}

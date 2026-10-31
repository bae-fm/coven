//! Histories and result adapters shared by the database/Lean differentials.
use crate::{RowIdentity, SyncedTable};
use coven_format::value::Value as Scalar;
use coven_format::write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Change, ColumnValue, Operation, RowId, Timestamp, Write, WriteId};
use serde_json::Value;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Write as _;
use std::process::{Command, Stdio};

pub(crate) const SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)";
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
    child.stdin.take().unwrap().write_all(input.to_string().as_bytes()).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "Lean input {input}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let mut result: Value = serde_json::from_slice(&result.stdout).expect("Lean JSON result");
    if let Some(frozen) = result.get_mut("frozen") {
        frozen.as_array_mut().expect("frozen records").sort_by_key(Value::to_string);
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
}
pub(crate) fn merge_write(record: &WriteRecord, earlier: &[WriteRecord]) -> Write<Scalar> {
    Write {
        id: record.header.position,
        timestamp: record.header.timestamp,
        had_read: earlier
            .iter()
            .enumerate()
            .filter(|(slot, r)| {
                (*slot as u64) < record.header.read_through
                    || (r.header.position.device == record.header.position.device
                        && r.header.position.number < record.header.position.number)
            })
            .map(|(_, r)| r.header.position)
            .collect(),
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
    let position = WriteId { device: DeviceId(100 + n), number: 1 };
    WriteRecord {
        header: WriteHeader {
            position,
            timestamp: Timestamp::new(n + 1, 0, position.device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            executions: Vec::new(),
            audience: Audience::Store,
            rows: vec![RowChange {
                creator: None,
                execution: Default::default(),
                row: row(r),
                change: Change {
                    generation: 0,
                    operation: Operation::Insert(
                        [("id", r.to_string()), ("a", format!("a{n}")), ("b", format!("b{n}"))]
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

pub(crate) fn stamp(ts: Timestamp) -> u128 {
    ((ts.milliseconds() as u128) << 80) | ((ts.counter() as u128) << 64) | ts.device().0 as u128
}

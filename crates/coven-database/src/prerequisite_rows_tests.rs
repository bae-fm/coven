use super::*;
use crate::pending::tests::Record;
use crate::tests::TestStore;
use crate::CommitScope;
use std::collections::{BTreeMap, BTreeSet};

fn key_columns(scope: &KeyScope, into: &mut BTreeSet<String>) {
    match scope {
        KeyScope::All => {}
        KeyScope::In { column, .. } | KeyScope::Compare { column, .. } => {
            into.insert(column.clone());
        }
        KeyScope::And(left, right) | KeyScope::Or(left, right) => {
            key_columns(left, into);
            key_columns(right, into);
        }
    }
}

/// Each read keys its table's rows by primary-key columns and names every
/// other column, so any change to a named row counts.
#[test]
fn reads_key_rows_by_primary_key_and_name_every_other_column() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&crate::internal_schema::initial_schema())
        .unwrap();
    let device = DeviceId(1);
    let samples = [
        PrerequisiteRows::Entry(EntryId { device, number: 1 }),
        PrerequisiteRows::KeyCopy {
            writer: device,
            number: 1,
        },
        PrerequisiteRows::Write(WriteId { device, number: 1 }),
        PrerequisiteRows::Device(device),
        PrerequisiteRows::PeerPositions(device),
        PrerequisiteRows::Reload(Audience::Store),
        PrerequisiteRows::Finality,
        PrerequisiteRows::Boundaries,
        PrerequisiteRows::ProviderAccess("ana@example.com".into()),
        PrerequisiteRows::Operation(crate::OperationId(1)),
    ];
    for sample in samples {
        for read in sample.reads() {
            let columns: Vec<(String, i64)> = connection
                .prepare("SELECT name,pk FROM pragma_table_info(?1)")
                .unwrap()
                .query_map([&read.table], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert!(!columns.is_empty(), "{} exists", read.table);
            let others: BTreeSet<String> = columns
                .iter()
                .filter(|(_, pk)| *pk == 0)
                .map(|(name, _)| name.clone())
                .collect();
            assert_eq!(read.columns, others, "{}", read.table);
            let primary: BTreeSet<String> = columns
                .iter()
                .filter(|(_, pk)| *pk > 0)
                .map(|(name, _)| name.clone())
                .collect();
            let mut keyed = BTreeSet::new();
            key_columns(&read.keys, &mut keyed);
            assert!(keyed.is_subset(&primary), "{}", read.table);
        }
    }
}

/// IO `WaitingEvidence`: a wait observes only the rows it names, never the
/// commits of the attempt it began with; other work's commit to its rows
/// while that attempt ran counts at once (`racing_commit_wakes`), as does one
/// from a task the attempt spawned.
#[tokio::test]
async fn a_wait_observes_its_rows_outside_its_attempt() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let observed = std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let observe = |device: u64| {
        let db = db.clone();
        let observed = observed.clone();
        async move {
            let next = {
                let mut observed = observed.lock().unwrap();
                let value: &mut u8 = observed.entry(DeviceId(device)).or_insert(0);
                *value += 1;
                observed
                    .iter()
                    .map(|(device, value)| (*device, vec![*value]))
                    .collect::<BTreeMap<_, _>>()
            };
            db.complete_positions_observation(next, Vec::<Record>::new(), vec![], vec![])
                .await
                .unwrap();
        }
    };
    let seven = [PrerequisiteRows::PeerPositions(DeviceId(7))];
    // The attempt changes its own prerequisite: no evidence.
    let attempt = CommitScope::new();
    let commits = db.attempt_commits(attempt);
    attempt.run(observe(7)).await;
    let mut wait = commits.watch(&seven);
    assert!(!wait.take_changed().unwrap());
    // Another row: no evidence. Its row, by other work: evidence, and the
    // wake resolves.
    observe(8).await;
    assert!(!wait.take_changed().unwrap());
    let wake = wait.changed();
    observe(7).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), wake)
        .await
        .unwrap();
    assert!(wait.take_changed().unwrap());
    assert!(!wait.take_changed().unwrap());
    // Other work changes it while an attempt runs, or a task the attempt
    // spawned does: the wait is due at once. Another row changing is none.
    let commits = db.attempt_commits(CommitScope::new());
    observe(7).await;
    assert!(commits.watch(&seven).take_changed().unwrap());
    let attempt = CommitScope::new();
    let commits = db.attempt_commits(attempt);
    attempt
        .run(async { tokio::spawn(observe(7)).await.unwrap() })
        .await;
    assert!(commits.watch(&seven).take_changed().unwrap());
    let commits = db.attempt_commits(CommitScope::new());
    observe(8).await;
    assert!(!commits.watch(&seven).take_changed().unwrap());
    db.close().await.unwrap();
}

/// IO `PrerequisiteRows.only_partial_advances`: a wait on Ben's write 5 counts
/// his position reaching 3, short of the write, as evidence; another writer's
/// position is none.
#[tokio::test]
async fn a_write_wait_counts_a_partial_advance_of_its_writer() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let write = [PrerequisiteRows::Write(WriteId {
        device: DeviceId(2),
        number: 5,
    })];
    let mut wait = db.attempt_commits(CommitScope::new()).watch(&write);
    db.test_cover_positions(vec![(3, 9)]).await.unwrap();
    assert!(!wait.take_changed().unwrap());
    db.test_cover_positions(vec![(2, 3)]).await.unwrap();
    assert!(wait.take_changed().unwrap());
    db.close().await.unwrap();
}

fn key_values(scope: &KeyScope, into: &mut serde_json::Map<String, serde_json::Value>) {
    match scope {
        KeyScope::All => {}
        KeyScope::In { column, values } => {
            let [value] = values.as_slice() else {
                panic!("one key value per column: {column}")
            };
            let value = match value {
                SqlValue::Blob(bytes) => {
                    serde_json::json!(u64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
                }
                SqlValue::Text(text) => serde_json::json!(String::from_utf8(text.clone()).unwrap()),
                SqlValue::Integer(n) => serde_json::json!(n),
                _ => panic!("unexpected key value in {column}"),
            };
            into.insert(column.clone(), value);
        }
        KeyScope::And(left, right) => {
            key_values(left, into);
            key_values(right, into);
        }
        _ => panic!("a prerequisite keys rows by equality"),
    }
}

/// IO `PrerequisiteRows.Prereq.rows`: each prerequisite reads the tables and
/// primary keys the model names, in order (`reading_frame`,
/// `named_row_matters`, `only_partial_advances`).
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_prerequisite_rows() {
    use serde_json::json;
    let device = DeviceId(2);
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(4));
    let samples = [
        (
            PrerequisiteRows::Entry(EntryId { device, number: 3 }),
            json!({"kind": "entry", "device": 2, "number": 3}),
        ),
        (
            PrerequisiteRows::KeyCopy {
                writer: device,
                number: 4,
            },
            json!({"kind": "key_copy", "writer": 2, "number": 4}),
        ),
        (
            PrerequisiteRows::Write(WriteId { device, number: 5 }),
            json!({"kind": "write", "device": 2, "number": 5}),
        ),
        (
            PrerequisiteRows::Device(device),
            json!({"kind": "device", "device": 2}),
        ),
        (
            PrerequisiteRows::PeerPositions(device),
            json!({"kind": "peer_positions", "device": 2}),
        ),
        (
            PrerequisiteRows::Reload(Audience::Store),
            json!({"kind": "reload", "audience": "store"}),
        ),
        (
            PrerequisiteRows::Reload(Audience::Circle(circle)),
            json!({"kind": "reload", "audience": circle.to_string()}),
        ),
        (PrerequisiteRows::Finality, json!({"kind": "finality"})),
        (PrerequisiteRows::Boundaries, json!({"kind": "boundaries"})),
        (
            PrerequisiteRows::ProviderAccess("ana@example.com".into()),
            json!({"kind": "provider_access", "account": "ana@example.com"}),
        ),
        (
            PrerequisiteRows::Operation(crate::OperationId(6)),
            json!({"kind": "operation", "id": 6}),
        ),
    ];
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for (rows, model) in samples {
        cases.push(json!({"check": "prerequisite_rows", "prereq": model}));
        actual.push(serde_json::Value::Array(
            rows.reads()
                .iter()
                .map(|read| {
                    let mut key = serde_json::Map::new();
                    key_values(&read.keys, &mut key);
                    json!({"table": read.table, "key": key})
                })
                .collect(),
        ));
    }
    assert_eq!(actual, crate::database_changes::tests::io_runner(&cases));
}

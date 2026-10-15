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

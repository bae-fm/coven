use super::*;
use crate::snapshot_write::tests::{decode, frames, id, load_one, stream, SnapshotFrames};
use crate::test_utils::tests::{insert, SCHEMA};
use crate::test_utils::TestAuthorship;
use crate::tests::TestStore;
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::snapshot::{SnapshotEncoder, SnapshotRecord};
use coven_format::value::Value;
use coven_format::write::{RowChange, WriteRecord};
use coven_format::write_sources::{CellSource, ChangeId, WriteSources};
use coven_merge::{Change, Operation};
use std::io::Cursor;

fn owned() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::SharedKey).owner_only()]
}

fn row() -> RowId {
    RowId {
        table: "rows".into(),
        key: coven_format::key::encode_key(&[Value::Text("0".into())]).unwrap(),
        audience: Audience::Store,
    }
}

/// Device 101 replaces column `a` of device 100's row, having read it.
fn overwrite() -> WriteRecord {
    let created = insert(0, 0);
    let mut write = insert(1, 0);
    write.header.read_through = 1;
    let creation = ChangeId {
        write: created.header.position,
        row: row(),
    };
    let Operation::Insert(columns) = &created.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    let mut value = columns["a"].clone();
    value.value = Value::Text("replaced".into());
    write.parts[0].rows[0] = RowChange {
        row: row(),
        change: Change {
            generation: 1,
            operation: Operation::Update(BTreeMap::from([("a".into(), value)])),
        },
        old: BTreeMap::from([("a".into(), columns["a"].value.clone())]),
        sources: WriteSources {
            generation: Some(creation.clone()),
            setters: BTreeMap::from([(
                "a".into(),
                CellSource {
                    change: creation,
                    column: "a".into(),
                },
            )]),
        },
        creator: None,
        execution: Default::default(),
    };
    write
}

/// A database holding device 100's owned row, as streamed writes left it.
async fn source(store: &TestStore, writes: &[WriteRecord]) -> Database {
    let db = store.schema(owned(), SCHEMA).await.unwrap();
    // Devices 100 and 101 are one member's, who may edit the row (§9.6).
    for write in writes {
        TestStore::register(&db, write.header.position.device, 1);
    }
    for write in writes {
        db.receive_stream(stream(write), || Ok(())).await.unwrap();
    }
    db
}

fn evidence(snapshot: &SnapshotFrames) -> Vec<WriteEvidence> {
    decode(snapshot)
        .1
        .into_iter()
        .filter_map(|record| match record {
            SnapshotRecord::Evidence(evidence) => Some(evidence),
            _ => None,
        })
        .collect()
}

/// Re-encode a snapshot after changing its evidence or creation records.
fn edited(snapshot: &SnapshotFrames, edit: impl FnOnce(&mut Vec<SnapshotRecord>)) -> Vec<u8> {
    let (mut header, mut records) = decode(snapshot);
    edit(&mut records);
    header.counts[7] = records
        .iter()
        .filter(|record| matches!(record, SnapshotRecord::Evidence(_)))
        .count() as u64;
    let (mut encoder, mut bytes) = SnapshotEncoder::start(header).unwrap();
    for record in records {
        bytes.extend(encoder.record(record).unwrap());
    }
    bytes.extend(encoder.finish().unwrap());
    bytes
}

async fn load(snapshot: &SnapshotFrames, bytes: Vec<u8>) -> Result<(), DbError> {
    let store = TestStore::new();
    let target = store.schema(owned(), SCHEMA).await.unwrap();
    let result = load_one(
        &target,
        id(Audience::Store),
        snapshot.prefix.clone(),
        Cursor::new(bytes),
    )
    .await;
    target.close().await.unwrap();
    result
}

fn refused(result: Result<(), DbError>) -> &'static str {
    match result {
        Err(DbError::Snapshot(crate::SnapshotError::Inconsistent(reason))) => reason,
        other => panic!("expected an inconsistent snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn prepared_authorship_is_not_decoded_during_installation() {
    let store = TestStore::new();
    let db = source(&store, &[insert(0, 0), overwrite()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    assert!(!evidence(&snapshot).is_empty());
    let counter = coven_foundation::test_work::Counter::default();
    counter
        .run(load(&snapshot, snapshot.concat()))
        .await
        .unwrap();
    let counts = counter.take();
    assert!(counts.snapshot_decodes > 0);
    assert_eq!(
        counts.snapshot_decodes_with_writer, 0,
        "snapshot evidence decoded while the application writer was held"
    );
    db.close().await.unwrap();
}

/// The snapshot opens the creation marker and each surviving cell of
/// another member's row, and hides the replaced value behind a hash (§9.6).
#[tokio::test]
async fn openings_name_the_creation_and_surviving_cells_but_not_replaced_ones() {
    let store = TestStore::new();
    let db = source(&store, &[insert(0, 0), overwrite()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let carried = evidence(&snapshot);
    assert_eq!(carried.len(), 2, "the creating write and the replacing one");
    let created = &carried[0];
    assert_eq!(created.write, insert(0, 0).header.position);
    let opened = created.proof.opened().unwrap();
    assert!(matches!(
        &opened[0],
        Leaf::Creation {
            generation: 0,
            creator: None,
            ..
        }
    ));
    let columns: Vec<_> = opened
        .iter()
        .filter_map(|leaf| match leaf {
            Leaf::Cell { column, .. } => Some(column.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(columns, ["b", "id"]);
    assert!(!created.proof.hidden.is_empty());
    let replaced = &carried[1];
    assert!(matches!(
        replaced.proof.opened().unwrap().as_slice(),
        [Leaf::Cell { kind: CellKind::Update, column, .. }, ..] if column == "a"
    ));
    load(&snapshot, snapshot.concat()).await.unwrap();
    db.close().await.unwrap();
}

/// Without the creating write's evidence, the snapshot names another
/// member's authorship it cannot show, and nothing loads.
#[tokio::test]
async fn missing_evidence_refuses_the_snapshot() {
    let store = TestStore::new();
    let db = source(&store, &[insert(0, 0)]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let bytes = edited(&snapshot, |records| {
        records.retain(|record| !matches!(record, SnapshotRecord::Evidence(_)))
    });
    assert_eq!(
        refused(load(&snapshot, bytes).await),
        "snapshot names another member's authorship without its write's evidence"
    );
    db.close().await.unwrap();
}

/// An opening must reproduce the signed root, and a creation claimed for a
/// write must be among its opened leaves.
#[tokio::test]
async fn forged_openings_and_invented_creations_are_refused() {
    let store = TestStore::new();
    let db = source(&store, &[insert(0, 0), overwrite()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let forged = edited(&snapshot, |records| {
        for record in records.iter_mut() {
            if let SnapshotRecord::Evidence(evidence) = record {
                let (_, leaf) = &mut evidence.proof.revealed[0];
                *leaf = Leaf::Deletion {
                    row: row(),
                    generation: 0,
                }
                .encode()
                .unwrap();
            }
        }
    });
    assert_eq!(
        refused(load(&snapshot, forged).await),
        "write evidence opening differs from its signed root"
    );
    // Device 101's write carries authentic evidence, but no creation of the row.
    let replacing = overwrite().header.position;
    let invented = edited(&snapshot, |records| {
        for record in records.iter_mut() {
            if let SnapshotRecord::Creation(creation) = record {
                creation.write = replacing;
                creation.creator = ChangeId {
                    write: replacing,
                    row: row(),
                };
            }
        }
    });
    assert_eq!(
        refused(load(&snapshot, invented).await),
        "snapshot names another member's authorship without its write's evidence"
    );
    db.close().await.unwrap();
}

/// A device that loaded the snapshot holds only the openings, and its own
/// snapshot reuses them: authorship survives a second reload.
#[tokio::test]
async fn a_reloaded_device_carries_the_same_evidence_on() {
    let store = TestStore::new();
    let db = source(&store, &[insert(0, 0), overwrite()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let middle_store = TestStore::new();
    let middle = middle_store.schema(owned(), SCHEMA).await.unwrap();
    let (expected, prefix, input, _) = snapshot.input();
    load_one(&middle, expected, prefix, input).await.unwrap();
    let again = frames(&middle, Audience::Store).await;
    assert_eq!(evidence(&again), evidence(&snapshot));
    load(&again, again.concat()).await.unwrap();
    for db in [db, middle] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_reloaded_snapshot_hides_cells_overwritten_after_loading() {
    let mut created = insert(0, 0);
    let Operation::Insert(columns) = &mut created.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.get_mut("a").unwrap().value = Value::Text("7".into());
    columns.get_mut("b").unwrap().value = Value::Text("8".into());
    let mut replaced = overwrite();
    let change = &mut replaced.parts[0].rows[0];
    let Operation::Update(columns) = &mut change.change.operation else {
        unreachable!()
    };
    let mut cell = columns.remove("a").unwrap();
    cell.value = Value::Text("9".into());
    columns.insert("b".into(), cell);
    change.old = BTreeMap::from([("b".into(), Value::Text("8".into()))]);
    let mut setter = change.sources.setters.remove("a").unwrap();
    setter.column = "b".into();
    change.sources.setters.insert("b".into(), setter);

    let store = TestStore::new();
    let db = source(&store, &[created.clone()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let middle_store = TestStore::new();
    let middle = middle_store.schema(owned(), SCHEMA).await.unwrap();
    let (expected, prefix, input, _) = snapshot.input();
    load_one(&middle, expected, prefix, input).await.unwrap();
    TestStore::register(&middle, created.header.position.device, 1);
    TestStore::register(&middle, replaced.header.position.device, 1);
    middle
        .receive_stream(stream(&replaced), || Ok(()))
        .await
        .unwrap();
    let again = frames(&middle, Audience::Store).await;
    let carried = evidence(&again);
    let proof = &carried
        .iter()
        .find(|e| e.write == created.header.position)
        .unwrap()
        .proof;
    let expected = Multiproof::prove(
        &TestAuthorship::key().derive().authorship_hasher(),
        &coven_format::authorship::part_leaves(&created.parts[0]),
        |leaf| {
            matches!(leaf, Leaf::Creation { .. })
                || matches!(leaf, Leaf::Cell { column, .. } if column != "b")
        },
    )
    .unwrap();
    assert_eq!(proof, &expected);
    assert_eq!(
        proof
            .root(&TestAuthorship::key().derive().authorship_hasher())
            .unwrap(),
        carried[0].roots[0]
    );
    load(&again, again.concat()).await.unwrap();
    for db in [db, middle] {
        db.close().await.unwrap();
    }
}

/// The storage model's `prune` over `neededLeaves`, as `authorshipRunner` computes it.
fn lean_proof(runner: &std::ffi::OsStr, input: serde_json::Value) -> serde_json::Value {
    use std::io::Write as _;
    let mut child = std::process::Command::new(runner)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("start Lean authorship runner");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "Lean input {input}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn rust_proof(proof: &Multiproof) -> serde_json::Value {
    serde_json::json!({
        "opened": proof.revealed.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        "hashes": proof.hidden.len(),
    })
}

/// Every tree size and many keep sets: the multiproof opens exactly the kept
/// positions with the model's minimal hash count (`multiproof_minimal`).
#[test]
#[ignore = "requires COVEN_AUTHORSHIP_LEAN; scripts/check.sh supplies the storage runner"]
fn multiproofs_match_the_storage_model() {
    let runner = std::env::var_os("COVEN_AUTHORSHIP_LEAN").expect("built authorshipRunner");
    let hasher = TestAuthorship::key().derive().authorship_hasher();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for count in 1..40u64 {
        let leaves: Vec<_> = (0..count)
            .map(|generation| Leaf::Deletion {
                row: row(),
                generation,
            })
            .collect();
        for _ in 0..6 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let keep = |generation: u64| state >> (generation % 64) & 1 == 1;
            let proof = Multiproof::prove(
                &hasher,
                &leaves,
                |leaf| matches!(leaf, Leaf::Deletion { generation, .. } if keep(*generation) || generation % 2 == 0),
            )
            .unwrap();
            let (_, hashes) = proof.root_with_leaf_hashes(&hasher).unwrap();
            let retained: Vec<_> = proof.revealed.iter().map(|(i, _)| keep(*i)).collect();
            let proof = proof.retain_opened(&hashes, &retained);
            let input = serde_json::json!({
                "leaves": (0..count).map(|g| serde_json::json!({"kind": "deletion", "key": 0, "generation": g})).collect::<Vec<_>>(),
                "creations": [], "cells": [],
                "deletions": (0..count).filter(|g| keep(*g)).map(|g| [0, g]).collect::<Vec<_>>(),
            });
            assert_eq!(
                rust_proof(&proof),
                lean_proof(&runner, input),
                "{count} leaves"
            );
        }
    }
}

/// A snapshot's retained opening of another member's part is the model's
/// `prune` of that part over `neededLeaves`: the row's creation and the
/// cells it still shows from that write (`cell_complete`,
/// `creation_complete`, `overwritten_not_opened`).
#[tokio::test]
#[ignore = "requires COVEN_AUTHORSHIP_LEAN; scripts/check.sh supplies the storage runner"]
async fn retained_openings_match_the_cell_proof_model() {
    let runner = std::env::var_os("COVEN_AUTHORSHIP_LEAN").expect("built authorshipRunner");
    let store = TestStore::new();
    let created = insert(0, 0);
    let db = source(&store, &[created.clone(), overwrite()]).await;
    let snapshot = frames(&db, Audience::Store).await;
    let carried = evidence(&snapshot);
    // The cells the row still shows from the creating write, by the merge.
    let shown: Vec<(String, Value)> = db.inspect_writer_schema(|c, schema| {
        let store = crate::merge_store::MergeStore::from_schema(c, &schema.schema);
        let state = store.row(&row()).unwrap().state;
        state
            .cells()
            .iter()
            .filter(|(_, cell)| cell.write == created.header.position)
            .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
            .collect()
    });
    let leaves = coven_format::authorship::part_leaves(&created.parts[0]);
    let mut values = Vec::<Value>::new();
    let mut value = |v: &Value| match values.iter().position(|x| x == v) {
        Some(i) => i,
        None => {
            values.push(v.clone());
            values.len() - 1
        }
    };
    let columns = ["a", "b", "id"];
    let column = |c: &str| columns.iter().position(|x| *x == c).unwrap();
    let mut json_leaves = Vec::new();
    for leaf in &leaves {
        json_leaves.push(match leaf {
            Leaf::Creation { generation, .. } => {
                serde_json::json!({"kind": "creation", "key": 0, "generation": generation})
            }
            Leaf::Cell {
                generation,
                kind,
                column: c,
                value: v,
                ..
            } => serde_json::json!({
                "kind": if *kind == CellKind::Insert { "insert" } else { "update" },
                "key": 0, "generation": generation, "column": column(c), "value": value(&v.value)}),
            // A step is never opened: a key no snapshot needs.
            _ => serde_json::json!({"kind": "deletion", "key": 1, "generation": 0}),
        });
    }
    let cells: Vec<_> = shown
        .iter()
        .map(|(c, v)| {
            serde_json::json!({"key": 0, "generation": 0, "kind": "insert",
                "column": column(c), "value": value(v)})
        })
        .collect();
    let input = serde_json::json!({
        "leaves": json_leaves, "creations": [[0, 0]], "deletions": [], "cells": cells,
    });
    assert_eq!(rust_proof(&carried[0].proof), lean_proof(&runner, input));
    db.close().await.unwrap();
}

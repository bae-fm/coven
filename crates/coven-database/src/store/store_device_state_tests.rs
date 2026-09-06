use super::*;
use coven_protocol::causal_grants::AuthorStreamId;
use coven_protocol::objects::{ExactObjectRef, ObjectSlot};
use coven_protocol::store_commit::{ObjectHash, StoreCommitCoord};

fn database() -> Connection {
    let conn = Connection::open_in_memory().expect("open database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys");
    crate::apply_coven_schema(&conn).expect("create schema");
    conn
}

fn reference(sequence: u64) -> StoreBatchCommitRef {
    let bytes = format!("device-state commit {sequence}");
    let hash = ObjectHash::digest(bytes.as_bytes());
    StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: AuthorStreamId::from_digest(ObjectHash::digest(b"device-state stream")),
            sequence,
        },
        commit_hash: hash,
        object: ExactObjectRef::new(
            ObjectSlot::logical(format!("store-v1/tests/device-state/{sequence}.json"))
                .expect("test slot"),
            bytes.len() as u64,
            hash,
        ),
    }
}

fn state() -> ResolvedStoreDeviceState {
    ResolvedStoreDeviceState::merge([]).expect("empty device state")
}

fn record(conn: &Connection, reference: &StoreBatchCommitRef) {
    let transaction = conn.unchecked_transaction().expect("begin recording");
    record_store_device_snapshot_on(&transaction, reference, &state()).expect("record state");
    transaction.commit().expect("commit recording");
}

fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .expect("count rows")
}

#[test]
fn covered_history_shares_bodies_and_preserves_exact_commit_identity() {
    let conn = database();
    let first = reference(1);
    let second = reference(2);
    let later = reference(3);
    for reference in [&first, &second, &later] {
        record(&conn, reference);
    }
    let coverage = CommitFrontier(BTreeMap::from([(second.coord.stream_id, second.clone())]));
    let covered = load_covered_store_device_snapshots_on(&conn, &coverage).expect("load history");
    assert_eq!(covered.len(), 2);
    assert!(Arc::ptr_eq(&covered[&first], &covered[&second]));
    assert_eq!(count(&conn, "store_device_states"), 1);
    assert!(!covered.contains_key(&later));
    let mut different = first.clone();
    different.commit_hash = ObjectHash::digest(b"another candidate at the same coordinate");
    assert!(!covered.contains_key(&different));
    load_store_device_snapshot_on(&conn, &different).expect_err("exact reference is required");
}

#[test]
fn referenced_state_cannot_be_deleted_or_replaced_by_a_missing_body() {
    let conn = database();
    record(&conn, &reference(1));
    conn.execute("DELETE FROM store_device_states", [])
        .expect_err("body is referenced");
    conn.execute(
        "UPDATE store_device_state_snapshots SET state_hash = ?1",
        [ObjectHash::digest(b"absent state").to_string()],
    )
    .expect_err("reference requires its body");
    assert_eq!(
        load_store_device_snapshot_on(&conn, &reference(1)).expect("state remains"),
        state()
    );
}

#[test]
fn missing_or_corrupted_bodies_fail_exact_and_covered_reads() {
    enum Corruption {
        Missing,
        BodyHash,
        StoredHash,
    }
    for corruption in [
        Corruption::Missing,
        Corruption::BodyHash,
        Corruption::StoredHash,
    ] {
        let conn = database();
        let first = reference(1);
        record(&conn, &first);
        match corruption {
            Corruption::Missing => {
                conn.pragma_update(None, "foreign_keys", "OFF")
                    .expect("allow sabotage");
                conn.execute("DELETE FROM store_device_states", [])
                    .expect("remove body");
                conn.pragma_update(None, "foreign_keys", "ON")
                    .expect("restore foreign keys");
            }
            Corruption::BodyHash => {
                conn.pragma_update(None, "ignore_check_constraints", "ON")
                    .expect("allow sabotage");
                conn.execute(
                    "UPDATE store_device_states SET state = json_set(state, '$.state_hash', ?1)",
                    [ObjectHash::digest(b"forged body hash").to_string()],
                )
                .expect("forge the hash inside an otherwise parseable body");
                conn.pragma_update(None, "ignore_check_constraints", "OFF")
                    .expect("restore checks");
                let transaction = conn.unchecked_transaction().expect("begin recording");
                record_store_device_snapshot_on(&transaction, &reference(2), &state())
                    .expect_err("recording cannot overwrite a corrupted shared body");
                transaction.rollback().expect("roll back recording");
                assert_eq!(count(&conn, "store_device_state_snapshots"), 1);
            }
            Corruption::StoredHash => {
                conn.pragma_update(None, "ignore_check_constraints", "ON")
                    .expect("allow sabotage");
                let transaction = conn.unchecked_transaction().expect("begin sabotage");
                transaction
                    .pragma_update(None, "defer_foreign_keys", "ON")
                    .expect("defer references");
                let wrong_hash = ObjectHash::digest(b"wrong state address").to_string();
                transaction
                    .execute(
                        "UPDATE store_device_states SET state_hash = ?1",
                        [&wrong_hash],
                    )
                    .expect("change body address");
                transaction
                    .execute(
                        "UPDATE store_device_state_snapshots SET state_hash = ?1",
                        [&wrong_hash],
                    )
                    .expect("change reference address");
                transaction.commit().expect("commit sabotage");
                conn.pragma_update(None, "ignore_check_constraints", "OFF")
                    .expect("restore checks");
            }
        }
        load_store_device_snapshot_on(&conn, &first).expect_err("invalid body must fail");
        let coverage = CommitFrontier(BTreeMap::from([(first.coord.stream_id, first)]));
        load_covered_store_device_snapshots_on(&conn, &coverage)
            .expect_err("covered reads must fail");
    }
}

#[test]
fn state_body_and_reference_roll_back_together() {
    let conn = database();
    conn.execute_batch(
        "CREATE TEMP TRIGGER fail_device_state_reference
         BEFORE INSERT ON store_device_state_snapshots
         BEGIN SELECT RAISE(ABORT, 'injected reference failure'); END;",
    )
    .expect("install failure");
    let transaction = conn.unchecked_transaction().expect("begin recording");
    let error = record_store_device_snapshot_on(&transaction, &reference(1), &state())
        .expect_err("reference insertion must fail");
    assert!(error.to_string().contains("injected reference failure"));
    transaction.rollback().expect("roll back recording");
    assert_eq!(count(&conn, "store_device_states"), 0);
    assert_eq!(count(&conn, "store_device_state_snapshots"), 0);
}

#[test]
fn pruning_keeps_a_shared_body_until_its_last_reference_is_removed() {
    let conn = database();
    for sequence in [1, 2] {
        record(&conn, &reference(sequence));
    }
    for (sequence, remaining) in [(1, 1), (2, 0)] {
        let transaction = conn.unchecked_transaction().expect("begin removal");
        transaction
            .execute(
                "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
                [serde_json::to_string(&reference(sequence)).expect("encode reference")],
            )
            .expect("remove reference");
        prune_unreferenced_store_device_states_on(&transaction).expect("prune unreferenced states");
        transaction.commit().expect("commit removal");
        assert_eq!(count(&conn, "store_device_states"), remaining);
        if remaining == 1 {
            assert_eq!(
                load_store_device_snapshot_on(&conn, &reference(2)).expect("remaining state"),
                state()
            );
        }
    }
}

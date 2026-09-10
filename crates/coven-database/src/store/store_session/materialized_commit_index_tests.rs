use super::*;
use coven_protocol::causal_grants::AuthorStreamId;
use coven_protocol::objects::{ExactObjectRef, ObjectSlot};
use coven_protocol::store_commit::StoreCommitCoord;

fn reference(stream: &str, sequence: u64) -> StoreBatchCommitRef {
    let bytes = format!("frontier commit {stream}/{sequence}");
    let hash = ObjectHash::digest(bytes.as_bytes());
    StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: AuthorStreamId::from_digest(ObjectHash::digest(stream.as_bytes())),
            sequence,
        },
        commit_hash: hash,
        object: ExactObjectRef::new(
            ObjectSlot::logical(format!("store-v1/tests/frontier/{stream}/{sequence}.json"))
                .expect("test slot"),
            bytes.len() as u64,
            hash,
        ),
    }
}

fn record_materialized(conn: &Connection, reference: &StoreBatchCommitRef) {
    let encoded = serde_json::to_string(reference).expect("encode reference");
    // These readers inspect the retained identity, not the payload contents.
    let input = b"retained frontier fixture";
    let input_hash = ObjectHash::digest(input).to_string();
    let stream = reference.coord.stream_id.to_string();
    let sequence =
        Database::sequence_to_sqlite(&stream, reference.coord.sequence()).expect("sequence");
    conn.execute(
        "INSERT INTO retained_merge_materializations
         (device_id, seq, commit_ref, input_hash, canonical_input)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![stream, sequence, encoded, input_hash, input.as_slice()],
    )
    .expect("retain the materialized identity");
    conn.execute(
        "INSERT INTO materialized_commits
         (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
         VALUES (?1, ?2, ?3, ?3, ?4)",
        rusqlite::params![stream, sequence, encoded, input_hash],
    )
    .expect("record materialized position with its exact foreign key");
}

fn record_coverage(conn: &Connection, reference: &StoreBatchCommitRef) {
    let stream = reference.coord.stream_id.to_string();
    conn.execute(
        "INSERT INTO snapshot_coverage (device_id, seq, commit_ref, snapshot_hash)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            stream,
            Database::sequence_to_sqlite(&stream, reference.coord.sequence()).expect("sequence"),
            serde_json::to_string(reference).expect("encode coverage"),
            ObjectHash::digest(b"frontier snapshot").to_string(),
        ],
    )
    .expect("record snapshot coverage");
}

#[test]
fn frontier_readers_agree_on_empty_matching_and_advancing_positions() {
    for (materialized, coverage, expected) in [
        (None, None, None),
        (Some(7), None, Some(7)),
        (None, Some(7), Some(7)),
        (Some(7), Some(7), Some(7)),
        (Some(9), Some(7), Some(9)),
        (Some(7), Some(9), Some(9)),
    ] {
        let conn = Connection::open_in_memory().expect("open database");
        conn.pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        crate::apply_coven_schema(&conn).expect("create schema");
        if let Some(sequence) = materialized {
            record_materialized(&conn, &reference("writer", sequence));
        }
        if let Some(sequence) = coverage {
            record_coverage(&conn, &reference("writer", sequence));
        }
        let stream = reference("writer", 7).coord.stream_id.to_string();
        let expected = expected.map(|sequence| reference("writer", sequence));
        assert_eq!(
            latest_position_for_device_on(&conn, &stream).expect("read position"),
            expected
        );
        assert_eq!(
            materialized_frontier_on(&conn, None).expect("read frontier"),
            expected
                .into_iter()
                .map(|reference| (stream.clone(), reference))
                .collect(),
        );
    }
}

#[test]
fn frontier_readers_reject_conflicting_exact_references_at_one_position() {
    let materialized = reference("writer", 7);
    let mut different_hash = materialized.clone();
    different_hash.commit_hash = ObjectHash::digest(b"another commit at this position");
    let mut different_object = materialized.clone();
    different_object.object = ExactObjectRef::new(
        ObjectSlot::logical("store-v1/tests/frontier/another-object.json".to_string())
            .expect("other slot"),
        materialized.object.stored_size(),
        materialized.object.stored_hash(),
    );
    for coverage in [different_hash, different_object] {
        let conn = Connection::open_in_memory().expect("open database");
        conn.pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        crate::apply_coven_schema(&conn).expect("create schema");
        record_materialized(&conn, &materialized);
        record_coverage(&conn, &coverage);
        let stream = materialized.coord.stream_id.to_string();
        assert_eq!(
            materialized_commit_ref_on(&conn, &stream, 7).expect("read materialized"),
            Some(materialized.clone())
        );
        assert_eq!(
            snapshot_coverage_on(&conn).expect("read coverage")[&stream],
            coverage
        );
        latest_position_for_device_on(&conn, &stream)
            .expect_err("a position cannot name two exact commits");
        materialized_frontier_on(&conn, None)
            .expect_err("the whole frontier must reject the same conflict");
    }
}

#[test]
fn excluding_a_stream_omits_only_its_position_conflict() {
    let conn = Connection::open_in_memory().expect("open database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys");
    crate::apply_coven_schema(&conn).expect("create schema");
    let materialized = reference("conflicting", 7);
    let mut coverage = materialized.clone();
    coverage.commit_hash = ObjectHash::digest(b"conflicting coverage");
    record_materialized(&conn, &materialized);
    record_coverage(&conn, &coverage);
    let healthy = reference("healthy", 4);
    record_coverage(&conn, &healthy);
    let excluded = materialized.coord.stream_id.to_string();
    let included = healthy.coord.stream_id.to_string();
    assert_eq!(
        materialized_frontier_on(&conn, Some(&excluded)).expect("exclude conflicting stream"),
        BTreeMap::from([(included.clone(), healthy)]),
    );
    materialized_frontier_on(&conn, Some(&included))
        .expect_err("the included stream still conflicts");
}

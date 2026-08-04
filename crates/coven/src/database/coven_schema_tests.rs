use super::*;

#[test]
fn schema_sql_normalization_ignores_formatting_but_keeps_constraints() {
    let formatted = normalize_schema_sql(
        "CREATE TABLE t (
                id INTEGER PRIMARY KEY, -- explanatory text
                value TEXT CHECK (length(value) = 64)
             ) STRICT",
    );
    let compact = normalize_schema_sql(
        "create table t(id integer primary key,value text check(length(value)=64)) strict",
    );
    let changed = normalize_schema_sql(
        "create table t(id integer primary key,value text check(length(value)=32)) strict",
    );

    assert_eq!(formatted, compact);
    assert_ne!(formatted, changed);
}

/// Every bookkeeping table [`apply_coven_schema`] creates is declared STRICT
/// — the same guarantee the synced-table contract now requires of the host's
/// own tables, so coven does not exempt itself from the invariant it enforces
/// on the host.
#[test]
fn every_bookkeeping_table_is_strict() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    for name in table_names() {
        let sql = format!("PRAGMA table_list({})", crate::database::quote_ident(name));
        let mut stmt = conn.prepare(&sql).expect("prepare table_list");
        let strict: i64 = stmt
            .query_row([], |row| row.get(5))
            .unwrap_or_else(|e| panic!("PRAGMA table_list({name}): {e}"));
        assert_eq!(strict, 1, "{name} must be STRICT");
    }
}

#[test]
fn active_and_protocol_inert_object_identities_are_disjoint() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    let active_first = "a".repeat(64);
    conn.execute(
        "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
        [&active_first],
    )
    .expect("insert active object");
    assert!(
        conn.execute(
            "INSERT INTO protocol_inert_objects (object_id, state) VALUES (?1, '{}')",
            [&active_first],
        )
        .is_err(),
        "an active object identity must not also become protocol-inert"
    );

    let inert_first = "b".repeat(64);
    conn.execute(
        "INSERT INTO protocol_inert_objects (object_id, state) VALUES (?1, '{}')",
        [&inert_first],
    )
    .expect("insert protocol-inert object");
    assert!(
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
            [&inert_first],
        )
        .is_err(),
        "a protocol-inert object identity must not return to active ownership"
    );
    assert!(
        conn.execute(
            "UPDATE remote_objects SET object_id = ?1 WHERE object_id = ?2",
            [&inert_first, &active_first],
        )
        .is_err(),
        "an active identity update must not collide with protocol-inert ownership"
    );
    assert!(
        conn.execute(
            "UPDATE protocol_inert_objects SET object_id = ?1 WHERE object_id = ?2",
            [&active_first, &inert_first],
        )
        .is_err(),
        "a protocol-inert identity update must not collide with active ownership"
    );
}

#[test]
fn circle_operation_journal_has_one_progress_representation() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    let mut statement = conn
        .prepare("PRAGMA table_info(circle_operations)")
        .expect("prepare circle_operations table_info");
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query circle_operations columns")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read circle_operations columns");

    assert_eq!(columns, ["operation_id", "circle_id", "payload"]);
}

#[test]
fn author_exclusion_activation_locator_has_one_exact_row_shape() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    let columns = conn
        .prepare("PRAGMA table_info(store_author_exclusion_activations)")
        .expect("prepare exclusion activation table_info")
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })
        .expect("query exclusion activation columns")
        .map(|row| row.expect("read exclusion activation column"))
        .collect::<Vec<_>>();

    assert_eq!(
        columns,
        [
            ("exclusion_ref".to_string(), 1),
            ("accepted_cut".to_string(), 0),
            ("activation_commit".to_string(), 0),
            ("activation_head".to_string(), 0),
        ]
    );
}

#[test]
fn merge_materialization_requires_its_exact_retained_input() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys");
    apply_coven_schema(&conn).expect("apply coven schema");

    conn.execute(
        "INSERT INTO materialized_commits (device_id, seq, commit_ref)
             VALUES ('merge-stream', 1, '{}')",
        [],
    )
    .expect_err("Merge materialization without retained input must fail");

    conn.execute(
        "INSERT INTO retained_merge_materializations
             (device_id, seq, commit_ref, input_hash, canonical_input)
             VALUES ('merge-stream', 1, '{}', ?1, x'7b7d')",
        ["a".repeat(64)],
    )
    .expect("retain exact Merge input");
    conn.execute(
        "INSERT INTO materialized_commits
             (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
             VALUES ('merge-stream', 1, '{}', '{}', ?1)",
        ["a".repeat(64)],
    )
    .expect("materialize Merge commit with retained input");
}

#[test]
fn retained_replay_baseline_has_one_closed_active_row() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    let insert = |singleton: i64, generation: i64| {
        conn.execute(
            "INSERT INTO retained_replay_baselines
                 (singleton, generation, exact_cut, schema_version,
                  routing_hash, image_hash, image_bytes, authority_bytes)
                 VALUES (?1, ?2, '{}', 1, ?3, ?4, x'01', x'01')",
            rusqlite::params![singleton, generation, "a".repeat(64), "b".repeat(64)],
        )
    };

    insert(1, -1).expect_err("baseline generation cannot be negative");
    insert(1, 0).expect("insert generation-zero baseline");
    insert(1, 1).expect_err("a second active baseline must fail");
    insert(2, 1).expect_err("the baseline key must remain the singleton");
    insert(0, 1).expect_err("baseline generation cannot use another singleton key");
}

#[test]
fn retained_replay_object_index_requires_both_exact_records() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys");
    apply_coven_schema(&conn).expect("apply coven schema");
    let input_hash = "a".repeat(64);
    let object_id = "b".repeat(64);
    conn.execute(
        "INSERT INTO retained_merge_materializations
             (device_id, seq, commit_ref, input_hash, canonical_input)
             VALUES ('merge-stream', 1, '{}', ?1, x'7b7d')",
        [&input_hash],
    )
    .expect("retain exact Merge input");
    conn.execute(
        "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
        [&object_id],
    )
    .expect("insert exact remote object");
    conn.execute(
        "INSERT INTO retained_replay_objects
             (device_id, seq, commit_ref, input_hash, object_id)
             VALUES ('merge-stream', 1, '{}', ?1, ?2)",
        [&input_hash, &object_id],
    )
    .expect("index retained replay ownership");

    assert!(conn
        .execute(
            "DELETE FROM retained_merge_materializations
                 WHERE device_id = 'merge-stream' AND seq = 1",
            [],
        )
        .is_err());
    assert!(conn
        .execute(
            "DELETE FROM remote_objects WHERE object_id = ?1",
            [&object_id],
        )
        .is_err());
}

#[test]
fn circle_operation_journal_allows_one_pending_operation_per_circle() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    conn.execute(
        "INSERT INTO circle_operations (operation_id, circle_id, payload)
             VALUES ('operation-a', 'circle-a', x'7b7d')",
        [],
    )
    .expect("insert first pending Circle operation");

    let error = conn
        .execute(
            "INSERT INTO circle_operations (operation_id, circle_id, payload)
                 VALUES ('operation-b', 'circle-a', x'7b7d')",
            [],
        )
        .expect_err("a Circle cannot have a second pending operation");

    assert!(error.to_string().contains("circle_operations.circle_id"));
}

#[test]
fn prepared_blob_identity_is_the_exact_remote_object() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_schema(&conn).expect("apply coven schema");
    let primary_key = conn
        .prepare("PRAGMA table_info(store_write_blobs)")
        .expect("prepare store_write_blobs table_info")
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })
        .expect("query store_write_blobs columns")
        .filter_map(|row| {
            let (name, position) = row.expect("read store_write_blobs column");
            (position != 0).then_some((position, name))
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    assert_eq!(
        primary_key.into_values().collect::<Vec<_>>(),
        ["write_id", "audience", "remote_object_id"]
    );
}

#[test]
fn routing_tables_are_strict_without_rowid() {
    let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
    apply_coven_routing_schema(&conn).expect("apply routing schema");
    for name in ["_coven_audience", "_coven_row_routes"] {
        let sql = format!("PRAGMA table_list({})", crate::database::quote_ident(name));
        let (wr, strict): (i64, i64) = conn
            .query_row(&sql, [], |row| Ok((row.get(4)?, row.get(5)?)))
            .expect("table_list");
        assert_eq!((wr, strict), (1, 1), "{name}");
    }
}

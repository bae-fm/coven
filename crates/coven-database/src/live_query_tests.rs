use super::*;

#[test]
fn query_key_parser_handles_bound_equality_in_and_range() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT) STRICT;")
        .expect("schema");
    let reads = BTreeMap::from([("notes".to_string(), TableRead::default())]);

    for sql in [
        "SELECT body FROM notes WHERE id = 'one'",
        "SELECT body FROM notes WHERE id IN ('one', 'two')",
        "SELECT body FROM notes WHERE id >= 'one' AND id < 'three'",
        "SELECT body FROM notes WHERE id BETWEEN 'one' AND 'three'",
    ] {
        assert!(
            key_scope_for_statement(&connection, sql, &reads)
                .expect("analyze key scope")
                .is_some(),
            "expected key scope for {sql}"
        );
    }
}

#[test]
fn query_key_parser_handles_integer_blob_and_composite_keys() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch(
            "CREATE TABLE integer_keys (id INTEGER PRIMARY KEY, body TEXT) STRICT;
             CREATE TABLE blob_keys (id BLOB PRIMARY KEY, body TEXT) STRICT;
             CREATE TABLE composite_keys (
                 tenant TEXT NOT NULL,
                 id TEXT NOT NULL,
                 body TEXT,
                 PRIMARY KEY (tenant, id)
             ) STRICT;",
        )
        .expect("schema");
    for (table, sql) in [
        (
            "integer_keys",
            "SELECT body FROM integer_keys WHERE id = 42",
        ),
        ("blob_keys", "SELECT body FROM blob_keys WHERE id = x'0102'"),
        (
            "composite_keys",
            "SELECT body FROM composite_keys WHERE tenant = 'one' AND id = 'two'",
        ),
    ] {
        let reads = BTreeMap::from([(table.to_string(), TableRead::default())]);
        assert!(
            key_scope_for_statement(&connection, sql, &reads)
                .expect("analyze key scope")
                .is_some(),
            "expected key scope for {sql}"
        );
    }
}

#[test]
fn unsupported_or_branch_falls_back_to_all_keys() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT) STRICT;")
        .expect("schema");
    let reads = BTreeMap::from([("notes".to_string(), TableRead::default())]);
    assert!(key_scope_for_statement(
        &connection,
        "SELECT body FROM notes WHERE id = 'one' OR body = 'other'",
        &reads,
    )
    .expect("analyze key scope")
    .is_none());
}

#[test]
fn same_table_subqueries_fall_back_to_all_keys() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT) STRICT;")
        .expect("schema");
    let reads = BTreeMap::from([("notes".to_string(), TableRead::default())]);
    assert!(key_scope_for_statement(
        &connection,
        "SELECT body FROM notes WHERE id = 'one' AND EXISTS (\
                 SELECT 1 FROM notes AS other WHERE other.body = 'present'\
             )",
        &reads,
    )
    .expect("analyze key scope")
    .is_none());
}

#[test]
fn non_binary_primary_key_collations_fall_back_to_all_keys() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch(
            "CREATE TABLE notes (\
                id TEXT COLLATE NOCASE PRIMARY KEY,\
                body TEXT\
            ) STRICT;",
        )
        .expect("schema");
    let reads = BTreeMap::from([("notes".to_string(), TableRead::default())]);
    assert!(key_scope_for_statement(
        &connection,
        "SELECT body FROM notes WHERE id = 'one'",
        &reads,
    )
    .expect("analyze key scope")
    .is_none());
}

#[test]
fn virtual_table_dependencies_fail_instead_of_missing_changes() {
    let connection = Connection::open_in_memory().expect("open");
    connection
        .execute_batch("CREATE VIRTUAL TABLE search USING fts5(body);")
        .expect("virtual table schema");
    let capture = ReadDependencyCapture::default();
    capture
        .state
        .lock()
        .expect("capture mutex poisoned")
        .current = BTreeMap::from([("search".to_string(), TableRead::default())]);

    let error = capture
        .dependencies(&connection)
        .expect_err("virtual table mutations are not in SQLite session changesets");
    assert!(error.to_string().contains("virtual table"));
}

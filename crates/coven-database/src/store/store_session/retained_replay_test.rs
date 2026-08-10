use super::*;
use rusqlite::OptionalExtension;

#[test]
fn every_coven_table_has_one_retained_replay_disposition() {
    let classified = REPLAY_TABLES
        .iter()
        .map(|(table, _)| *table)
        .collect::<BTreeSet<_>>();
    assert_eq!(classified.len(), REPLAY_TABLES.len());
    assert_eq!(classified, crate::all_table_names());

    let without_routing = projection_table_names(false)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let with_routing = projection_table_names(true)
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        with_routing
            .difference(&without_routing)
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "_coven_audience".to_string(),
            "_coven_row_routes".to_string(),
        ])
    );
}

fn populate_fixture(connection: &Connection) {
    connection
        .execute_batch(
            "CREATE TABLE host_rows (
                 id TEXT PRIMARY KEY,
                 secret TEXT NOT NULL
             ) STRICT;",
        )
        .expect("create host table");
    crate::apply_coven_schema(connection).expect("create Coven tables");
    crate::apply_coven_routing_schema(connection).expect("create routing tables");
    connection
        .execute_batch(
            "INSERT INTO host_rows VALUES ('host', 'projection-secret-marker');
             INSERT INTO remote_objects
             VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '{}');
             INSERT INTO store_device_registration_activations
             VALUES ('founder',
                     'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                     'author', 'device', x'01', '{}', '{}');",
        )
        .expect("insert projection rows");
    let mut keys = required_generation_zero_protocol_keys()
        .iter()
        .map(|key| ((*key).to_string(), "{}".to_string()))
        .collect::<Vec<_>>();
    keys.iter_mut()
        .find(|(key, _)| key == SYNC_ROUTING_HASH_STATE_KEY)
        .expect("routing hash key")
        .1 = ObjectHash::digest(b"routing").to_string();
    keys.push(("local_device_id".to_string(), "excluded-device".to_string()));
    for (key, value) in keys {
        connection
            .execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                (key, value),
            )
            .expect("insert protocol state");
    }
    let database = crate::DatabaseTestSql::new(connection);
    database
        .install_test_store_root_authority("retained-replay-fixture")
        .expect("install retained-replay Store root authority");
    let cursor = founder_membership_cursor_key(connection)
        .expect("derive founder membership cursor")
        .expect("founder membership cursor");
    connection
        .execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, '{}')",
            [cursor],
        )
        .expect("insert founder membership cursor");
}

#[test]
fn generation_zero_projection_accepts_a_wal_database() {
    let directory = tempfile::tempdir().expect("create projection directory");
    let connection =
        Connection::open(directory.path().join("store.sqlite3")).expect("open file fixture");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .expect("enable WAL");
    assert_eq!(journal_mode, "wal");
    populate_fixture(&connection);

    let bytes = project_generation_zero_image(&connection).expect("project WAL database");
    let mut image = Connection::open_in_memory().expect("open projected WAL image");
    crate::connection_io::deserialize_database_image_into(&mut image, &bytes)
        .expect("load projected WAL image");
    assert_eq!(
        image
            .query_row("SELECT COUNT(*) FROM host_rows", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count projected host rows"),
        0
    );
}

#[test]
fn generation_zero_projection_reads_uncommitted_founder_state_and_removes_local_bytes() {
    let mut source = Connection::open_in_memory().expect("open projection fixture");
    populate_fixture(&source);
    source
        .execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset_hash, base, blob_facts)
             VALUES ('excluded-write', '\"pending\"', '[]', ?1,
                     '{\"dependencies\":{}}', '{\"blobs\":[]}')",
            ["00".repeat(32)],
        )
        .expect("insert excluded autoincrement row");
    source
        .execute("DELETE FROM store_device_registration_activations", [])
        .expect("remove committed founder fixture");
    let transaction = source.transaction().expect("begin founder transaction");
    transaction
        .execute(
            "INSERT INTO store_device_registration_activations
             VALUES ('founder',
                     'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                     'author', 'device', x'01', '{}', '{}')",
            [],
        )
        .expect("insert uncommitted founder");

    let bytes =
        project_generation_zero_image(&transaction).expect("project uncommitted founder state");
    assert!(!bytes
        .windows(b"projection-secret-marker".len())
        .any(|window| window == b"projection-secret-marker"));
    let mut image = Connection::open_in_memory().expect("open projected image");
    crate::connection_io::deserialize_database_image_into(&mut image, &bytes)
        .expect("load projected image");
    assert_eq!(
        image
            .query_row(
                "SELECT device_id FROM store_device_registration_activations",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read uncommitted founder from image"),
        "founder"
    );
    assert_eq!(
        image
            .query_row("SELECT COUNT(*) FROM host_rows", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count projected host rows"),
        0
    );
    assert_eq!(
        image
            .query_row("SELECT COUNT(*) FROM remote_objects", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count projected remote objects"),
        0
    );
    assert!(image
        .query_row(
            "SELECT value FROM protocol_state WHERE key = 'local_device_id'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("query excluded local state")
        .is_none());
    assert!(image
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'store_writes'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .expect("query excluded autoincrement state")
        .is_none());
    transaction.rollback().expect("rollback founder fixture");
}

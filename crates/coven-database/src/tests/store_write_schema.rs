use super::*;
use coven_foundation::store_dir::StoreDir;

fn seed_unversioned_effect(path: &Path, migrations: &[Migration]) -> (String, Vec<u8>) {
    drop(
        open_writer_with_migrations(path, CovenMigrationPolicy::ApplyPending, &migrations[..1])
            .unwrap(),
    );
    let raw = Connection::open(path).unwrap();
    let connection = raw.unchecked_transaction().unwrap();
    let mut session = rusqlite::session::Session::new(&connection).unwrap();
    session.attach(Some("notes")).unwrap();
    connection
        .execute(
            "INSERT INTO notes (id, _updated_at) VALUES ('note', '0000000001000-0000-writer')",
            [],
        )
        .unwrap();
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).unwrap();
    drop(session);
    let directory = StoreDir::new(path.parent().unwrap());
    let hash = crate::payload_store::write_payload_blocking(
        &connection,
        &directory,
        &bytes,
        crate::payload_store::CreatedPayloadFiles::untracked(),
    )
    .unwrap();
    connection.execute("INSERT INTO store_writes (write_id, status, affected_rows, changeset_hash, base, blob_facts) VALUES ('captured', '\"local_only\"', '[]', ?1, '{\"dependencies\":{}}', '{\"blobs\":[]}')", [hash.to_string()]).unwrap();
    connection.execute("INSERT INTO store_write_partitions (write_id, audience, changeset_hash) VALUES ('captured', 'local', ?1)", [hash.to_string()]).unwrap();
    let receipt = serde_json::to_string(&coven_protocol::write::WriteStatus::Resolved(
        coven_protocol::write::WriteResolution::Discarded,
    ))
    .unwrap();
    connection
        .execute(
            "INSERT INTO store_writes (write_id, status) VALUES ('folded-receipt', ?1)",
            [&receipt],
        )
        .unwrap();
    crate::run_migrations_in_transaction(&connection, migrations).unwrap();
    connection
        .execute_batch("DROP TABLE store_write_schemas")
        .unwrap();
    crate::set_protocol_state_on(&connection, COVEN_SCHEMA_VERSION_STATE_KEY, "2").unwrap();
    let manifest =
        serde_json::to_string(expected_coven_schema_v2_manifest(false).unwrap()).unwrap();
    crate::set_protocol_state_on(&connection, COVEN_SCHEMA_MANIFEST_STATE_KEY, &manifest).unwrap();
    connection.commit().unwrap();
    (hash.to_string(), bytes)
}

#[test]
fn internal_upgrade_recovers_old_local_effect_layout_after_host_already_advanced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recovered.sqlite");
    let migrations = [
        notes_migration(),
        Migration::sql(2, "note title", "ALTER TABLE notes ADD COLUMN title TEXT;"),
    ];
    let (hash, original) = seed_unversioned_effect(&path, &migrations);
    drop(
        open_writer_with_migrations(&path, CovenMigrationPolicy::ApplyPending, &migrations)
            .unwrap(),
    );
    let connection = Connection::open(&path).unwrap();
    let (ordinal, version, retained_hash): (i64, u32, String) = connection.query_row("SELECT ordinal, schema_version, changeset_hash FROM store_writes JOIN store_write_schemas USING(write_id) WHERE write_id='captured'", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!((ordinal, version, retained_hash), (1, 1, hash.clone()));
    assert_eq!(
        crate::payload_store::read_payload_blocking(
            &connection,
            &StoreDir::new(directory.path()),
            hash.parse().unwrap()
        )
        .unwrap(),
        original
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM store_write_partitions WHERE write_id='captured'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let receipt = connection
        .query_row(
            "SELECT status FROM store_writes WHERE write_id='folded-receipt'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<coven_protocol::write::WriteStatus>(&receipt).unwrap(),
        coven_protocol::write::WriteStatus::Resolved(
            coven_protocol::write::WriteResolution::Discarded
        )
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM store_write_schemas WHERE write_id='folded-receipt'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(stored_state(&path).0.as_deref(), Some("3"));
}

#[test]
fn ambiguous_unversioned_effect_rolls_back_internal_schema_upgrade() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ambiguous.sqlite");
    let first = Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL, title TEXT) STRICT;",
    );
    let migrations = [
        first,
        Migration::sql(
            2,
            "rename title",
            "ALTER TABLE notes RENAME COLUMN title TO body;",
        ),
    ];
    seed_unversioned_effect(&path, &migrations);
    let before = stored_state(&path);
    let error =
        match open_writer_with_migrations(&path, CovenMigrationPolicy::ApplyPending, &migrations) {
            Ok(_) => panic!("ambiguous capture schema must refuse open"),
            Err(error) => error,
        };
    assert!(error.to_string().contains("ambiguous"), "{error}");
    assert_eq!(stored_state(&path), before);
    let connection = Connection::open(path).unwrap();
    assert!(!connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='store_write_schemas')",
            [],
            |row| row.get::<_, bool>(0)
        )
        .unwrap());
}

#[test]
fn current_schema_does_not_recover_a_missing_effect_version() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection.execute("INSERT INTO store_writes (write_id,status,changeset_hash) VALUES ('unversioned','\"pending\"',?1)", ["a".repeat(64)]).unwrap();
    drop(connection);
    assert!(open_writer(&path, CovenMigrationPolicy::ApplyPending).is_err());
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM store_write_schemas", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

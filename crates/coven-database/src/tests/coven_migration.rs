use std::path::Path;
use std::sync::Arc;

use coven_foundation::clock::SystemClock;
use coven_protocol::blob::{TransferLimits, BLOB_TOMBSTONE_GRACE};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::{Connection, OptionalExtension};

use crate::{
    coven_migration::{
        run_coven_migrations_with_ladder_for_test,
        run_uninitialized_snapshot_migrations_with_ladder_for_test, CovenMigrationStep,
    },
    coven_schema::{
        apply_coven_schema, downgrade_coven_schema_to_v0_for_test, expected_coven_schema_manifest,
        live_coven_schema_manifest,
    },
    CovenMigrationError, CovenMigrationPolicy, Database, Migration, OpenError,
    COVEN_SCHEMA_MANIFEST_STATE_KEY, COVEN_SCHEMA_VERSION_STATE_KEY,
};

fn notes_table() -> SyncedTable {
    SyncedTable::new("notes", RowIdentity::SharedKey)
}

fn notes_migration() -> Migration {
    Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
    )
}

fn open_writer(path: &Path, policy: CovenMigrationPolicy) -> Result<Database, OpenError> {
    open_writer_with_migrations(path, policy, &[notes_migration()])
}

fn open_writer_with_migrations(
    path: &Path,
    policy: CovenMigrationPolicy,
    migrations: &[Migration],
) -> Result<Database, OpenError> {
    Database::open(
        path,
        vec![notes_table()],
        BLOB_TOMBSTONE_GRACE,
        TransferLimits::one_at_a_time(),
        "coven-migration-tests".to_string(),
        Arc::new(SystemClock),
        policy,
        migrations,
    )
}

fn seed_v0(path: &Path) {
    drop(open_writer(path, CovenMigrationPolicy::ApplyPending).expect("create current store"));
    let conn = Connection::open(path).expect("open store for v0 fixture");
    downgrade_coven_schema_to_v0_for_test(&conn, false).expect("install v0 Coven schema");
}

fn stored_state(path: &Path) -> (Option<String>, String, bool, bool) {
    let conn = Connection::open(path).expect("inspect store");
    let version = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_VERSION_STATE_KEY],
            |row| row.get(0),
        )
        .optional()
        .expect("read Coven schema version");
    let manifest = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_MANIFEST_STATE_KEY],
            |row| row.get(0),
        )
        .expect("read Coven schema manifest");
    let outbox_has_label = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('cloud_outbox') WHERE name = 'root_label')",
            [],
            |row| row.get(0),
        )
        .expect("read cloud_outbox columns");
    let intent_has_label = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('blob_make_remote_intents') WHERE name = 'root_label')",
            [],
            |row| row.get(0),
        )
        .expect("read intent columns");
    (version, manifest, outbox_has_label, intent_has_label)
}

fn transition_table_schema(path: &Path) -> (String, String) {
    let conn = Connection::open(path).expect("inspect transition table schema");
    let sql = |table: &str| {
        conn.query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .expect("read transition table schema")
    };
    (sql("cloud_outbox"), sql("blob_make_remote_intents"))
}

#[test]
fn refuse_pending_preserves_v0() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("refuse-v0.sqlite");
    seed_v0(&path);
    let before = stored_state(&path);

    let error = match open_writer(&path, CovenMigrationPolicy::RefusePending) {
        Ok(_) => panic!("pending Coven migration must be refused"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OpenError::CovenMigration(CovenMigrationError::Pending {
            current: 0,
            target: 1
        })
    ));
    assert_eq!(stored_state(&path), before);
}

#[test]
fn apply_pending_migrates_empty_v0_and_refuse_reopens_it() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("apply-v0.sqlite");
    seed_v0(&path);

    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("migrate v0"));
    let (version, _, outbox_has_label, intent_has_label) = stored_state(&path);
    assert_eq!(version.as_deref(), Some("1"));
    assert!(outbox_has_label);
    assert!(intent_has_label);
    drop(open_writer(&path, CovenMigrationPolicy::RefusePending).expect("reopen current store"));
}

fn assert_nonempty_table_rolls_back(expected_table: &str, insert: &str) {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory
        .path()
        .join(format!("nonempty-{expected_table}.sqlite"));
    seed_v0(&path);
    let conn = Connection::open(&path).expect("seed migration blocker");
    conn.execute_batch(insert)
        .expect("insert migration blocker");
    drop(conn);
    let before = stored_state(&path);

    let error = match open_writer(&path, CovenMigrationPolicy::ApplyPending) {
        Ok(_) => panic!("nonempty transition table must refuse migration"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OpenError::CovenMigration(CovenMigrationError::NonEmptyTable { ref table })
            if table == expected_table
    ));
    assert_eq!(stored_state(&path), before);
    let conn = Connection::open(&path).expect("inspect migration blocker");
    let rows: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {expected_table}"),
            [],
            |row| row.get(0),
        )
        .expect("count preserved rows");
    assert_eq!(rows, 1);
}

#[test]
fn nonempty_cloud_outbox_fails_and_rolls_back() {
    assert_nonempty_table_rolls_back(
        "cloud_outbox",
        "INSERT INTO cloud_outbox (operation, stored_ref, created_at) VALUES ('delete', '{}', 'now');",
    );
}

#[test]
fn nonempty_make_remote_intents_fails_and_rolls_back() {
    assert_nonempty_table_rolls_back(
        "blob_make_remote_intents",
        "INSERT INTO blob_make_remote_intents
         (root_table, root_id, retain_pinned, state, write_id)
         VALUES ('notes', 'n1', 0, 'uploading', NULL);",
    );
}

#[test]
fn later_host_migration_failure_rolls_back_applied_coven_migration() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("host-failure-after-coven.sqlite");
    seed_v0(&path);
    let before_state = stored_state(&path);
    let before_schema = transition_table_schema(&path);

    let migrations = [
        notes_migration(),
        Migration::sql(
            2,
            "fail after Coven migration",
            "CREATE TABLE host_migration_started (id TEXT PRIMARY KEY) STRICT;
             INSERT INTO table_that_does_not_exist VALUES ('fail');",
        ),
    ];
    assert!(
        open_writer_with_migrations(&path, CovenMigrationPolicy::ApplyPending, &migrations,)
            .is_err()
    );

    assert_eq!(stored_state(&path), before_state);
    assert_eq!(transition_table_schema(&path), before_schema);
    let conn = Connection::open(&path).expect("inspect rolled-back host migration");
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
            .expect("read host schema version"),
        1
    );
    assert!(!conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'host_migration_started')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .expect("check rolled-back host table"));
}

#[test]
fn read_only_refuses_v0_without_writing() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("read-only-v0.sqlite");
    seed_v0(&path);
    let before = stored_state(&path);

    let error = match Database::open_read_only(
        &path,
        vec![notes_table()],
        BLOB_TOMBSTONE_GRACE,
        TransferLimits::one_at_a_time(),
        "coven-migration-tests".to_string(),
        Arc::new(SystemClock),
        &[notes_migration()],
    ) {
        Ok(_) => panic!("read-only open must refuse a pending Coven migration"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OpenError::CovenMigration(CovenMigrationError::Pending {
            current: 0,
            target: 1
        })
    ));
    assert_eq!(stored_state(&path), before);
}

#[test]
fn fresh_refuse_initializes_latest_coven_schema() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("fresh-refuse.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::RefusePending).expect("open fresh store"));
    let (version, _, outbox_has_label, intent_has_label) = stored_state(&path);
    assert_eq!(version.as_deref(), Some("1"));
    assert!(outbox_has_label);
    assert!(intent_has_label);
}

#[test]
fn apply_pending_installs_missing_ledger_on_exact_current_schema() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("install-ledger.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("create current store"));
    let conn = Connection::open(&path).expect("remove schema version");
    conn.execute(
        "DELETE FROM protocol_state WHERE key = ?1",
        [COVEN_SCHEMA_VERSION_STATE_KEY],
    )
    .expect("remove schema version");
    drop(conn);

    let error = match open_writer(&path, CovenMigrationPolicy::RefusePending) {
        Ok(_) => panic!("missing current-schema ledger must be refused"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OpenError::CovenMigration(CovenMigrationError::PendingLedgerInstallation { version: 1 })
    ));
    assert_eq!(stored_state(&path).0, None);

    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("install schema ledger"));
    assert_eq!(stored_state(&path).0.as_deref(), Some("1"));
}

fn synthetic_v2_migration(conn: &Connection) -> Result<(), CovenMigrationError> {
    conn.execute_batch(
        "CREATE INDEX cloud_outbox_root_label_test_idx ON cloud_outbox(root_label);",
    )?;
    Ok(())
}

fn synthetic_v3_migration(conn: &Connection) -> Result<(), CovenMigrationError> {
    conn.execute_batch(
        "CREATE INDEX blob_make_remote_intents_root_label_test_idx
         ON blob_make_remote_intents(root_label);",
    )?;
    Ok(())
}

fn skipped_v1_migration(_: &Connection) -> Result<(), CovenMigrationError> {
    panic!("version 1 must skip its already-applied migration")
}

#[test]
fn generic_ladder_advances_a_known_version_through_an_additional_test_rung() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("synthetic-v2.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("create version 1 store"));

    let version_1_manifest = expected_coven_schema_manifest(false)
        .expect("version 1 manifest")
        .clone();
    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 1 schema");
    synthetic_v2_migration(&expected).expect("apply synthetic version 2 schema");
    let version_2_manifest = live_coven_schema_manifest(&expected).expect("version 2 manifest");
    let ladder = [
        CovenMigrationStep::new_for_test(&version_1_manifest, skipped_v1_migration),
        CovenMigrationStep::new_for_test(&version_2_manifest, synthetic_v2_migration),
    ];

    let conn = Connection::open(&path).expect("open version 1 store");
    run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance version 1 through synthetic rung");

    assert_eq!(
        conn.query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_VERSION_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .expect("read synthetic schema version"),
        "2"
    );
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read migrated schema"),
        version_2_manifest
    );
}

#[test]
fn ledgerless_schema_does_not_reconstruct_a_synthetic_later_version() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("ledgerless-synthetic-v2.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("create version 1 store"));

    let version_1_manifest = expected_coven_schema_manifest(false)
        .expect("version 1 manifest")
        .clone();
    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 1 schema");
    synthetic_v2_migration(&expected).expect("apply synthetic version 2 schema");
    let version_2_manifest = live_coven_schema_manifest(&expected).expect("version 2 manifest");
    let ladder = [
        CovenMigrationStep::new_for_test(&version_1_manifest, skipped_v1_migration),
        CovenMigrationStep::new_for_test(&version_2_manifest, synthetic_v2_migration),
    ];

    let conn = Connection::open(&path).expect("open version 1 store");
    run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance version 1 through synthetic rung");
    conn.execute(
        "DELETE FROM protocol_state WHERE key = ?1",
        [COVEN_SCHEMA_VERSION_STATE_KEY],
    )
    .expect("remove synthetic schema version");

    let error = run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect_err("ledgerless synthetic version 2 must not be reconstructed");
    assert!(matches!(
        error,
        CovenMigrationError::UnknownUnversionedSchema
    ));
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read unchanged synthetic schema"),
        version_2_manifest
    );
}

#[test]
fn exact_uninitialized_snapshot_advances_from_every_known_rung() {
    let version_1_manifest = expected_coven_schema_manifest(false)
        .expect("version 1 manifest")
        .clone();
    let conn = Connection::open_in_memory().expect("open synthetic snapshot database");
    apply_coven_schema(&conn).expect("apply version 1 schema");
    synthetic_v2_migration(&conn).expect("apply synthetic version 2 schema");
    let version_2_manifest =
        live_coven_schema_manifest(&conn).expect("read version 2 snapshot manifest");

    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 1 expected schema");
    synthetic_v2_migration(&expected).expect("apply synthetic version 2 expected schema");
    synthetic_v3_migration(&expected).expect("apply synthetic version 3 expected schema");
    let version_3_manifest =
        live_coven_schema_manifest(&expected).expect("read version 3 manifest");
    let ladder = [
        CovenMigrationStep::new_for_test(&version_1_manifest, skipped_v1_migration),
        CovenMigrationStep::new_for_test(&version_2_manifest, skipped_v1_migration),
        CovenMigrationStep::new_for_test(&version_3_manifest, synthetic_v3_migration),
    ];

    let error = run_uninitialized_snapshot_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::RefusePending,
        &ladder,
    )
    .expect_err("refuse exact version 2 snapshot with pending version 3");
    assert!(matches!(
        error,
        CovenMigrationError::Pending {
            current: 2,
            target: 3
        }
    ));
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read refused snapshot manifest"),
        version_2_manifest
    );

    run_uninitialized_snapshot_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance exact version 2 snapshot through version 3 only");
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read migrated snapshot manifest"),
        version_3_manifest
    );
}

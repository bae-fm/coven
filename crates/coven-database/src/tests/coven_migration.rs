use std::path::Path;
use std::sync::Arc;

use coven_foundation::clock::SystemClock;
use coven_protocol::blob::{TransferLimits, BLOB_TOMBSTONE_GRACE};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::{Connection, OptionalExtension};

use crate::{
    coven_migration::{
        run_coven_migrations_in_transaction, run_coven_migrations_with_ladder_for_test,
        run_uninitialized_snapshot_migrations_with_ladder_for_test, CovenMigrationStep,
    },
    coven_schema::{
        apply_coven_schema, downgrade_coven_schema_to_v0_for_test,
        downgrade_coven_schema_to_v1_for_test, expected_coven_schema_manifest,
        expected_coven_schema_v1_manifest, live_coven_schema_manifest,
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

fn seed_v1(path: &Path) {
    drop(open_writer(path, CovenMigrationPolicy::ApplyPending).expect("create current store"));
    let conn = Connection::open(path).expect("open store for v1 fixture");
    downgrade_coven_schema_to_v1_for_test(&conn, false).expect("install v1 Coven schema");
}

/// The columns version 2 drops, by table, as the live schema has them.
fn derived_columns(path: &Path) -> Vec<(&'static str, &'static str, bool)> {
    let conn = Connection::open(path).expect("inspect derived columns");
    [
        ("retained_replay_baselines", "generation"),
        ("retained_replay_baselines", "exact_cut"),
        ("outbound_store_snapshot", "image_ref"),
        ("outbound_store_snapshot", "rollup_ref"),
        ("outbound_circle_snapshot", "image_ref"),
    ]
    .into_iter()
    .map(|(table, column)| {
        let present = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
                [table, column],
                |row| row.get(0),
            )
            .expect("read table columns");
        (table, column, present)
    })
    .collect()
}

fn assert_derived_columns_present(path: &Path, present: bool) {
    for (table, column, found) in derived_columns(path) {
        assert_eq!(found, present, "{table}.{column}");
    }
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
            target: 2
        })
    ));
    assert_eq!(stored_state(&path), before);
}

#[test]
fn apply_pending_migrates_empty_v0_and_refuse_reopens_it() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("apply-v0.sqlite");
    seed_v0(&path);
    assert_derived_columns_present(&path, true);

    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("migrate v0"));
    let (version, _, outbox_has_label, intent_has_label) = stored_state(&path);
    assert_eq!(version.as_deref(), Some("2"));
    assert!(outbox_has_label);
    assert!(intent_has_label);
    assert_derived_columns_present(&path, false);
    drop(open_writer(&path, CovenMigrationPolicy::RefusePending).expect("reopen current store"));
}

/// A version 1 store's rows survive version 2: the columns it drops carried
/// values that live elsewhere, and everything else about each row stays.
/// Runs the shipped migrations directly: a full open would go on to read the
/// baseline's authority payload, which this fixture row does not carry.
#[test]
fn apply_pending_migrates_v1_keeping_its_rows() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("apply-v1.sqlite");
    seed_v1(&path);
    let conn = Connection::open(&path).expect("seed version 1 rows");
    conn.execute_batch(
        "INSERT INTO retained_replay_baselines
         (singleton, generation, exact_cut, schema_version,
          routing_hash, image_payload_hash, authority_hash)
         VALUES (1, 0, '{}', 7, replace(hex(zeroblob(32)), '0', 'a'),
                 replace(hex(zeroblob(32)), '0', 'b'), replace(hex(zeroblob(32)), '0', 'c'));
         INSERT INTO outbound_store_snapshot
         (singleton, snapshot_ref, meta_prepared, image_ref, rollup_ref, meta_bytes, blobs)
         VALUES (1, '{\"snapshot\": 1}', '{}', '{}', '{}', X'01', '[\"blob\"]');
         INSERT INTO outbound_circle_snapshot
         (circle_id, snapshot_ref, meta_prepared, image_ref, meta_bytes)
         VALUES ('circle', '{\"circle\": 1}', '{}', '{}', X'02');",
    )
    .expect("insert version 1 rows");
    drop(conn);
    let before = stored_state(&path);

    let conn = Connection::open(&path).expect("open version 1 store");
    let error =
        run_coven_migrations_in_transaction(&conn, false, CovenMigrationPolicy::RefusePending)
            .expect_err("pending Coven migration must be refused");
    assert!(matches!(
        error,
        CovenMigrationError::Pending {
            current: 1,
            target: 2
        }
    ));
    drop(conn);
    assert_eq!(stored_state(&path), before);
    assert_derived_columns_present(&path, true);

    let conn = Connection::open(&path).expect("open version 1 store");
    let tx = conn.unchecked_transaction().expect("begin migration");
    run_coven_migrations_in_transaction(&tx, false, CovenMigrationPolicy::ApplyPending)
        .expect("migrate v1");
    tx.commit().expect("commit migration");
    drop(conn);
    assert_eq!(stored_state(&path).0.as_deref(), Some("2"));
    assert_derived_columns_present(&path, false);
    let conn = Connection::open(&path).expect("inspect migrated rows");
    let baseline: (i64, String) = conn
        .query_row(
            "SELECT schema_version, routing_hash FROM retained_replay_baselines WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read migrated baseline");
    assert_eq!(baseline, (7, "a".repeat(64)));
    let snapshot: (String, Vec<u8>, String) = conn
        .query_row(
            "SELECT snapshot_ref, meta_bytes, blobs FROM outbound_store_snapshot WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated Store snapshot");
    assert_eq!(
        snapshot,
        (
            "{\"snapshot\": 1}".to_string(),
            vec![1],
            "[\"blob\"]".to_string()
        )
    );
    let circle: (String, Vec<u8>) = conn
        .query_row(
            "SELECT snapshot_ref, meta_bytes FROM outbound_circle_snapshot WHERE circle_id = 'circle'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read migrated Circle snapshot");
    assert_eq!(circle, ("{\"circle\": 1}".to_string(), vec![2]));
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
            target: 2
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
    assert_eq!(version.as_deref(), Some("2"));
    assert!(outbox_has_label);
    assert!(intent_has_label);
    assert_derived_columns_present(&path, false);
}

/// The ledger arrived while version 1 was the top of the ladder, so a
/// version 1 database without one is a known rung and advances from it.
#[test]
fn ledgerless_v1_schema_advances_to_current() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("ledgerless-v1.sqlite");
    seed_v1(&path);
    let conn = Connection::open(&path).expect("remove schema version");
    conn.execute(
        "DELETE FROM protocol_state WHERE key = ?1",
        [COVEN_SCHEMA_VERSION_STATE_KEY],
    )
    .expect("remove schema version");
    drop(conn);
    let before = stored_state(&path);

    let error = match open_writer(&path, CovenMigrationPolicy::RefusePending) {
        Ok(_) => panic!("ledgerless version 1 must be refused as pending"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OpenError::CovenMigration(CovenMigrationError::Pending {
            current: 1,
            target: 2
        })
    ));
    assert_eq!(stored_state(&path), before);

    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("advance ledgerless v1"));
    assert_eq!(stored_state(&path).0.as_deref(), Some("2"));
    assert_derived_columns_present(&path, false);
}

/// The current schema without a ledger is what a retained replay image
/// looks like at every version: nothing but the ledger is pending, a writer
/// installs it, a reader refuses.
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
        OpenError::CovenMigration(CovenMigrationError::PendingLedgerInstallation { version: 2 })
    ));
    assert_eq!(stored_state(&path).0, None);

    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("install schema ledger"));
    assert_eq!(stored_state(&path).0.as_deref(), Some("2"));
}

fn synthetic_v3_migration(conn: &Connection) -> Result<(), CovenMigrationError> {
    conn.execute_batch(
        "CREATE INDEX cloud_outbox_root_label_test_idx ON cloud_outbox(root_label);",
    )?;
    Ok(())
}

fn synthetic_v4_migration(conn: &Connection) -> Result<(), CovenMigrationError> {
    conn.execute_batch(
        "CREATE INDEX blob_make_remote_intents_root_label_test_idx
         ON blob_make_remote_intents(root_label);",
    )?;
    Ok(())
}

fn skipped_migration(_: &Connection) -> Result<(), CovenMigrationError> {
    panic!("an already-applied rung must be skipped")
}

/// The real ladder's rungs, as the synthetic rungs below extend them.
fn shipped_rungs() -> [CovenMigrationStep<'static>; 2] {
    [
        CovenMigrationStep::new_for_test(
            expected_coven_schema_v1_manifest(false).expect("version 1 manifest"),
            skipped_migration,
        ),
        CovenMigrationStep::new_for_test(
            expected_coven_schema_manifest(false).expect("version 2 manifest"),
            skipped_migration,
        ),
    ]
}

#[test]
fn generic_ladder_advances_a_known_version_through_an_additional_test_rung() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("synthetic-v3.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("create version 2 store"));

    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 2 schema");
    synthetic_v3_migration(&expected).expect("apply synthetic version 3 schema");
    let version_3_manifest = live_coven_schema_manifest(&expected).expect("version 3 manifest");
    let [rung_1, rung_2] = shipped_rungs();
    let ladder = [
        rung_1,
        rung_2,
        CovenMigrationStep::new_for_test(&version_3_manifest, synthetic_v3_migration),
    ];

    let conn = Connection::open(&path).expect("open version 2 store");
    run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance version 2 through synthetic rung");

    assert_eq!(
        conn.query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_VERSION_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .expect("read synthetic schema version"),
        "3"
    );
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read migrated schema"),
        version_3_manifest
    );
}

/// A ledgerless database is at whichever version its exact manifest is,
/// however far up the ladder that is: a replay image migrated to a later
/// version carries no ledger either.
#[test]
fn ledgerless_schema_at_a_later_version_installs_its_ledger() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("ledgerless-synthetic-v3.sqlite");
    drop(open_writer(&path, CovenMigrationPolicy::ApplyPending).expect("create version 2 store"));

    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 2 schema");
    synthetic_v3_migration(&expected).expect("apply synthetic version 3 schema");
    let version_3_manifest = live_coven_schema_manifest(&expected).expect("version 3 manifest");
    let [rung_1, rung_2] = shipped_rungs();
    let ladder = [
        rung_1,
        rung_2,
        CovenMigrationStep::new_for_test(&version_3_manifest, synthetic_v3_migration),
    ];

    let conn = Connection::open(&path).expect("open version 2 store");
    run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance version 2 through synthetic rung");
    conn.execute(
        "DELETE FROM protocol_state WHERE key = ?1",
        [COVEN_SCHEMA_VERSION_STATE_KEY],
    )
    .expect("remove synthetic schema version");

    let error = run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::RefusePending,
        &ladder,
    )
    .expect_err("ledgerless synthetic version 3 must be refused by a reader");
    assert!(matches!(
        error,
        CovenMigrationError::PendingLedgerInstallation { version: 3 }
    ));
    run_coven_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("install the ledger at synthetic version 3");
    assert_eq!(
        conn.query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [COVEN_SCHEMA_VERSION_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .expect("read installed schema version"),
        "3"
    );
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read unchanged synthetic schema"),
        version_3_manifest
    );
}

#[test]
fn exact_uninitialized_snapshot_advances_from_every_known_rung() {
    let conn = Connection::open_in_memory().expect("open synthetic snapshot database");
    apply_coven_schema(&conn).expect("apply version 2 schema");
    synthetic_v3_migration(&conn).expect("apply synthetic version 3 schema");
    let version_3_manifest =
        live_coven_schema_manifest(&conn).expect("read version 3 snapshot manifest");

    let expected = Connection::open_in_memory().expect("open expected schema database");
    apply_coven_schema(&expected).expect("apply version 2 expected schema");
    synthetic_v3_migration(&expected).expect("apply synthetic version 3 expected schema");
    synthetic_v4_migration(&expected).expect("apply synthetic version 4 expected schema");
    let version_4_manifest =
        live_coven_schema_manifest(&expected).expect("read version 4 manifest");
    let [rung_1, rung_2] = shipped_rungs();
    let ladder = [
        rung_1,
        rung_2,
        CovenMigrationStep::new_for_test(&version_3_manifest, skipped_migration),
        CovenMigrationStep::new_for_test(&version_4_manifest, synthetic_v4_migration),
    ];

    let error = run_uninitialized_snapshot_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::RefusePending,
        &ladder,
    )
    .expect_err("refuse exact version 3 snapshot with pending version 4");
    assert!(matches!(
        error,
        CovenMigrationError::Pending {
            current: 3,
            target: 4
        }
    ));
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read refused snapshot manifest"),
        version_3_manifest
    );

    run_uninitialized_snapshot_migrations_with_ladder_for_test(
        &conn,
        false,
        CovenMigrationPolicy::ApplyPending,
        &ladder,
    )
    .expect("advance exact version 3 snapshot through version 4 only");
    assert_eq!(
        live_coven_schema_manifest(&conn).expect("read migrated snapshot manifest"),
        version_4_manifest
    );
}

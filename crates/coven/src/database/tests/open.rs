use crate::database::connection_io::attach_session;
use crate::database::connection_io::capture_changeset;

use super::fixtures::*;
use crate::database::*;
use crate::protocol::blob::BLOB_TOMBSTONE_GRACE;

#[tokio::test]
async fn required_store_root_hash_rejects_missing_and_malformed_exact_authority() {
    let db = Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "required-store-root".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[notes_migration()],
    )
    .expect("open database");

    let missing = crate::database::StoreDatabase::new(&db)
        .required_store_root_hash()
        .await
        .expect_err("missing Store root must fail");
    assert!(matches!(missing, DbError::StoreRootHashMissing));

    db.call(|conn| {
        conn.execute(
            "INSERT INTO store_protocol_root_authority
             (singleton, store_root_hash, store_protocol_root_bytes, store_root_object)
             VALUES (1, ?1, X'00', '{}')",
            ["00".repeat(32)],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("write malformed exact Store root authority");
    let malformed = crate::database::StoreDatabase::new(&db)
        .required_store_root_hash()
        .await
        .expect_err("malformed Store root must fail");
    assert!(matches!(
        malformed,
        DbError::Message(reason) if !reason.is_empty()
    ));

    db.call(|conn| {
        conn.execute("DELETE FROM store_protocol_root_authority", [])
            .map(|_| ())
            .map_err(DbError::from)
    })
    .await
    .expect("remove malformed authority");
    let signer = crate::keys::UserKeypair::generate();
    let founder_provider_admin =
        crate::protocol::provider::FounderProviderAdminGrant::from_test_label(
            "required-store-root",
        );
    let descriptor = crate::protocol::store_commit::StoreCreationDescriptor {
        version: crate::protocol::store_commit::STORE_PROTOCOL_VERSION,
        creation_id: crate::protocol::store_commit::StoreCreationId::from_nonce(
            "required-store-root",
        ),
        provider: crate::protocol::objects::StoreProviderBinding::S3 {
            endpoint: crate::protocol::objects::S3EndpointBinding::Custom {
                origin: "https://test.invalid".to_string(),
            },
            region: "test-region".to_string(),
            bucket: "required-store-root-bucket".to_string(),
            key_prefix: None,
        },
        schema_version: db.schema_version(),
        sync_routing_hash: db.sync_routing_hash(),
        founder_pubkey: crate::keys::public_key_hex(&signer),
        founder_grant: crate::protocol::causal_grants::MembershipGrantId::from_test_label(
            "required-store-root founder",
        ),
        root_slot: crate::protocol::objects::ObjectSlot::logical(
            crate::protocol::store_commit::STORE_PROTOCOL_ROOT_LOGICAL_KEY.to_string(),
        )
        .expect("valid Store root slot"),
        founder_registration: crate::protocol::objects::ObjectSlot::logical(
            "store-v1/test/required-store-root/registration.json".to_string(),
        )
        .expect("valid founder registration slot"),
        founder_provider_admin,
        founder_membership: crate::protocol::store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: crate::protocol::objects::ObjectSlot::logical(
                "store-v1/test/required-store-root/membership/1.json".to_string(),
            )
            .expect("valid membership slot"),
        },
        founder_recovery: crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
            first_slot: crate::protocol::objects::ObjectSlot::logical(
                "store-v1/test/required-store-root/recovery/1.json".to_string(),
            )
            .expect("valid recovery slot"),
        },
    };
    let root = crate::protocol::store_commit::StoreProtocolRoot::signed(descriptor, &signer)
        .expect("sign Store root authority");
    let bytes = root.to_bytes();
    let expected = root.object_hash();
    let reference = crate::protocol::store_commit::StoreRootRef {
        store_root_id: root.descriptor.store_root_id(),
        store_root_hash: expected,
        object: ExactObjectRef::new(
            crate::protocol::objects::ObjectSlot::logical(
                "store-v1/store-protocol-root/required-store-root.json".to_string(),
            )
            .expect("valid Store root slot"),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        ),
    };
    db.call(move |conn| install_store_root_authority_on(conn, &reference, &bytes))
        .await
        .expect("install exact Store root authority");
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .required_store_root_hash()
            .await
            .unwrap(),
        expected
    );
}

#[test]
fn fresh_open_rolls_back_host_schema_and_coven_metadata_when_routing_is_invalid() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("fresh-routing-failure.sqlite");
    let result = Database::open(
        &path,
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "fresh-routing-failure".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "invalid routing",
            "CREATE TABLE local_parents (id TEXT PRIMARY KEY) STRICT;
             CREATE TABLE things (
                id TEXT PRIMARY KEY,
                local_parent_id TEXT NOT NULL REFERENCES local_parents(id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    );
    let error = match result {
        Ok(_) => panic!("fresh open must reject a synced-to-local foreign key"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("local_parents"), "{error}");

    let conn = Connection::open(&path).expect("inspect rolled-back database");
    let user_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user version");
    let durable_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .expect("durable tables");
    assert_eq!(user_version, 0);
    assert_eq!(durable_tables, 0);
}

#[test]
fn initialized_open_commits_ordinary_migration_without_changing_routing_contract() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("ordinary-migration.sqlite");
    let migrations = [things_migration()];
    let database = Database::open(
        &path,
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "ordinary-first-open".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("initial open");
    let pinned_hash = database.sync_routing_hash();
    drop(database);

    let migrations = [
        things_migration(),
        Migration::sql(
            2,
            "ordinary column and index",
            "ALTER TABLE things ADD COLUMN ordinary TEXT DEFAULT 'ordinary';
             CREATE INDEX things_ordinary ON things(ordinary);",
        ),
    ];
    let database = Database::open(
        &path,
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "ordinary-first-open".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("ordinary migration open");
    assert_eq!(database.schema_version(), 2);
    assert_eq!(database.sync_routing_hash(), pinned_hash);
    drop(database);

    let conn = Connection::open(&path).expect("inspect migrated database");
    let ordinary_ordinal: i64 = conn
        .query_row(
            "SELECT cid FROM pragma_table_info('things') WHERE name = 'ordinary'",
            [],
            |row| row.get(0),
        )
        .expect("ordinary column");
    assert_eq!(ordinary_ordinal, 3);
}

#[test]
fn initialized_open_rolls_back_routing_migration_and_user_version() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("routing-migration.sqlite");
    let v1 = || {
        Migration::sql(
            1,
            "gated things",
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                audience TEXT COLLATE BINARY NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )
    };
    let table = || {
        SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("audience")
    };
    let database = Database::open(
        &path,
        vec![table()],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "routing-first-open".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[v1()],
    )
    .expect("initial open");
    drop(database);

    let v2 = Migration::sql(
        2,
        "change audience collation",
        "CREATE TABLE things_next (
            id TEXT PRIMARY KEY,
            audience TEXT COLLATE NOCASE NOT NULL,
            _updated_at TEXT NOT NULL
         ) STRICT;
         INSERT INTO things_next SELECT * FROM things;
         DROP TABLE things;
         ALTER TABLE things_next RENAME TO things;",
    );
    let result = Database::open(
        &path,
        vec![table()],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "routing-first-open".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[v1(), v2],
    );
    let error = match result {
        Ok(_) => panic!("routing migration must not commit"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("sync-routing hash"), "{error}");

    let conn = Connection::open(&path).expect("inspect rolled-back migration");
    let user_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user version");
    let things_next: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'things_next'",
            [],
            |row| row.get(0),
        )
        .expect("things_next presence");
    let (_, collation, _, _, _) = conn
        .column_metadata(None::<&str>, "things", "audience")
        .expect("audience metadata");
    assert_eq!(user_version, 1);
    assert_eq!(things_next, 0);
    assert_eq!(collation.unwrap().to_bytes(), b"BINARY");
}

#[test]
fn writer_and_read_only_open_reject_every_coven_schema_shape_change_without_rewriting() {
    let cases = [
        ("missing-table", "DROP TABLE published_store_acks;"),
        (
            "changed-primary-key-and-constraints",
            "DROP TABLE published_store_acks;
             CREATE TABLE published_store_acks (
                revision INTEGER,
                ack_hash TEXT NOT NULL,
                PRIMARY KEY (ack_hash)
             ) STRICT;",
        ),
        (
            "unexpected-index",
            "CREATE INDEX coven_unexpected_store_write_status ON store_writes(status);",
        ),
        (
            "missing-strict",
            "DROP TABLE local_cleanup_intents;
             CREATE TABLE local_cleanup_intents (
                namespace TEXT NOT NULL,
                blob_id TEXT NOT NULL,
                PRIMARY KEY (namespace, blob_id)
             );",
        ),
        (
            "missing-without-rowid",
            "DROP TABLE _coven_audience;
             CREATE TABLE _coven_audience (
                routing_id TEXT PRIMARY KEY,
                circle_id TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        ),
    ];

    for (name, mutation) in cases {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join(format!("{name}.sqlite"));
        let tables = vec![scoped_things_table()];
        let migrations = vec![scoped_things_migration()];
        let database = Database::open(
            &path,
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::protocol::blob::TransferLimits::one_at_a_time(),
            "schema-seed".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &migrations,
        )
        .expect("seed database");
        drop(database);

        let conn = Connection::open(&path).expect("open database for schema mutation");
        conn.execute_batch(mutation).expect("mutate Coven schema");
        drop(conn);

        let writer_error = match Database::open(
            &path,
            tables.clone(),
            BLOB_TOMBSTONE_GRACE,
            crate::protocol::blob::TransferLimits::one_at_a_time(),
            "schema-writer".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &migrations,
        ) {
            Ok(_) => panic!("writer must reject Coven schema mutation {name}"),
            Err(error) => error.to_string(),
        };
        assert!(
            writer_error.contains("Coven schema"),
            "{name}: {writer_error}"
        );

        let reader_error = match Database::open_read_only(
            &path,
            tables,
            BLOB_TOMBSTONE_GRACE,
            crate::protocol::blob::TransferLimits::one_at_a_time(),
            "schema-reader".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &migrations,
        ) {
            Ok(_) => panic!("read-only open must reject Coven schema mutation {name}"),
            Err(error) => error.to_string(),
        };
        assert!(
            reader_error.contains("Coven schema"),
            "{name}: {reader_error}"
        );

        let conn = Connection::open(&path).expect("inspect rejected database");
        let host_device_id: String = conn
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [HOST_DEVICE_ID_STATE_KEY],
                |row| row.get(0),
            )
            .expect("host device id");
        assert_eq!(host_device_id, "schema-seed", "{name} was rewritten");
    }
}

#[test]
fn first_open_rolls_back_host_migration_when_gate_model_is_invalid() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("invalid-gate-migration.sqlite");
    let migration = Migration::sql(
        1,
        "composite gate relation",
        "CREATE TABLE parents (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            shared INTEGER NOT NULL,
            _updated_at TEXT NOT NULL,
            UNIQUE (id, code)
         ) STRICT;
         CREATE TABLE children (
            id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            parent_code TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            FOREIGN KEY (parent_id, parent_code) REFERENCES parents(id, code)
         ) STRICT;",
    );
    let tables = vec![
        SyncedTable::new(
            "parents",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "children",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];

    let error = match Database::open(
        &path,
        tables,
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "invalid-gate-open".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[migration],
    ) {
        Ok(_) => panic!("an invalid gate model must reject the open"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("composite foreign key"),
        "{error}"
    );

    let conn = Connection::open(&path).expect("inspect rejected database");
    let user_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user version");
    assert_eq!(user_version, 0);
    let host_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('parents', 'children')",
            [],
            |row| row.get(0),
        )
        .expect("host table count");
    assert_eq!(host_tables, 0);
}

#[test]
fn sqlite_session_representation_preserves_upsert_but_loses_primary_key_update_intent() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(
        "CREATE TABLE things (
            id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            _updated_at TEXT NOT NULL
         ) STRICT;
         INSERT INTO things VALUES ('old', 'base', '0000000001000-0000-writer');",
    )
    .expect("schema and seed");
    let tables = vec![things_table(
        crate::protocol::synced_schema::RowIdentity::SharedKey,
    )];

    let mut primary_key_session = attach_session(&conn, &tables).expect("attach session");
    let primary_key_tx = conn.unchecked_transaction().expect("transaction");
    primary_key_tx
        .execute(
            "UPDATE things SET id = 'new', _updated_at = '0000000002000-0000-writer' WHERE id = 'old'",
            [],
        )
        .expect("update primary key");
    let primary_key_changes =
        capture_changeset(&mut primary_key_session).expect("capture primary-key update");
    drop(primary_key_tx);
    drop(primary_key_session);
    let primary_key_changes =
        crate::database::walk_changeset(&primary_key_changes).expect("walk primary-key update");
    assert_eq!(
        primary_key_changes
            .iter()
            .map(|change| (change.op, change.pk()))
            .collect::<Vec<_>>(),
        vec![
            (crate::changeset::ChangeOp::Insert, Some("new")),
            (crate::changeset::ChangeOp::Delete, Some("old")),
        ]
    );

    let mut upsert_session = attach_session(&conn, &tables).expect("attach session");
    let upsert_tx = conn.unchecked_transaction().expect("transaction");
    upsert_tx
        .execute(
            "INSERT INTO things VALUES ('old', 'upserted', '0000000003000-0000-writer')
             ON CONFLICT(id) DO UPDATE SET
                body = excluded.body,
                _updated_at = excluded._updated_at",
            [],
        )
        .expect("same-id upsert");
    let upsert_changes = capture_changeset(&mut upsert_session).expect("capture upsert");
    drop(upsert_tx);
    let upsert_changes = crate::database::walk_changeset(&upsert_changes).expect("walk upsert");
    assert_eq!(upsert_changes.len(), 1);
    assert_eq!(upsert_changes[0].op, crate::changeset::ChangeOp::Update);
    assert_eq!(upsert_changes[0].pk(), Some("old"));
}

#[test]
fn writer_and_read_only_open_reject_existing_invalid_independent_uuid() {
    let writer_error = match Database::open(
        Path::new(":memory:"),
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::IndependentUuid,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "invalid-uuid-writer".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "things",
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO things VALUES ('2', 'invalid', '0000000001000-0000-seed');",
        )],
    ) {
        Ok(_) => panic!("writer open must reject an existing non-UUID id"),
        Err(error) => error.to_string(),
    };
    assert!(
        writer_error.contains("things") && writer_error.contains("\"2\""),
        "writer error identifies the table and value: {writer_error}",
    );

    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("read-only-invalid.sqlite");
    let writer = Database::open(
        &path,
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "invalid-uuid-seed".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "things",
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO things VALUES ('2', 'invalid', '0000000001000-0000-seed');",
        )],
    )
    .expect("seed database under its declared SharedKey contract");
    drop(writer);

    let reader_error = match Database::open_read_only(
        &path,
        vec![things_table(
            crate::protocol::synced_schema::RowIdentity::IndependentUuid,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "invalid-uuid-reader".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[things_migration()],
    ) {
        Ok(_) => panic!("read-only open must reject an existing non-UUID id"),
        Err(error) => error.to_string(),
    };
    assert!(
        reader_error.contains("things") && reader_error.contains("\"2\""),
        "reader error identifies the table and value: {reader_error}",
    );
}

#[test]
fn database_open_rejects_duplicate_synced_table_declarations() {
    let error = match Database::open(
        Path::new(":memory:"),
        vec![
            things_table(crate::protocol::synced_schema::RowIdentity::SharedKey),
            things_table(crate::protocol::synced_schema::RowIdentity::IndependentUuid),
        ],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "duplicate-things".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[things_migration()],
    ) {
        Ok(_) => panic!("one table cannot have two identity declarations"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("things") && error.contains("declared"),
        "{error}"
    );
}

#[tokio::test]
async fn invalid_host_identity_rolls_back_rows_and_preserves_existing_write() {
    let tables = vec![things_table(
        crate::protocol::synced_schema::RowIdentity::IndependentUuid,
    )];
    let db = Database::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "invalid-host-identity".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[things_migration()],
    )
    .expect("open");
    let existing_changeset = vec![0x45, 0x58, 0x41, 0x43, 0x54];
    let existing_for_insert = existing_changeset.clone();
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
             VALUES (
                'existing-write', '\"pending\"', '[]', ?1, ?1,
                '{\"dependencies\":{}}',
                '{\"blobs\":[]}'
             )",
            [existing_for_insert],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("seed existing write records");

    let result = crate::database::StoreDatabase::new(&db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "INSERT INTO things VALUES (?1, 'valid', '0000000002000-0000-writer')",
                ["f47ac10b-58cc-4372-a567-0e02b2c3d479"],
            )?;
            tx.execute(
                "INSERT INTO things VALUES ('2', 'invalid', '0000000002001-0000-writer')",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await;
    let error = result.expect_err("invalid UUID must reject the host transaction");
    assert!(error.to_string().contains("things") && error.to_string().contains("2"));

    db.call(move |conn| {
        let row_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM things", [], |row| row.get(0))
            .map_err(DbError::from)?;
        let pending = conn
            .prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(DbError::from)?;
        assert_eq!(row_count, 0);
        assert_eq!(pending, vec![existing_changeset]);
        Ok(())
    })
    .await
    .expect("inspect rollback");
}

#[tokio::test]
async fn valid_identity_changes_updates_and_upserts_succeed_but_invalid_new_uuid_rolls_back() {
    let tables = vec![things_table(
        crate::protocol::synced_schema::RowIdentity::IndependentUuid,
    )];
    let db = Database::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "host-identity-changes".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[things_migration()],
    )
    .expect("open");
    let original = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO things VALUES (?1, 'base', '0000000001000-0000-writer')",
            [original],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("seed row");

    let renamed = "01890a5d-ac96-774b-bcce-b302099c3f74";
    crate::database::StoreDatabase::new(&db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "UPDATE things SET id = ?1, _updated_at = '0000000002000-0000-writer' WHERE id = ?2",
                [renamed, original],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("valid primary-key change succeeds");

    let replaced = "8b1a9953-c461-4e20-8c66-826115d53552";
    crate::database::StoreDatabase::new(&db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute("DELETE FROM things WHERE id = ?1", [renamed])?;
            tx.execute(
                "INSERT INTO things VALUES (?1, 'replaced', '0000000003000-0000-writer')",
                [replaced],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("explicit delete and insert succeeds");

    crate::database::StoreDatabase::new(&db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "UPDATE things SET body = 'ordinary', _updated_at = '0000000004000-0000-writer' WHERE id = ?1",
                [replaced],
            )?;
            tx.execute(
                "INSERT INTO things VALUES (?1, 'upserted', '0000000005000-0000-writer')
                 ON CONFLICT(id) DO UPDATE SET body = excluded.body, _updated_at = excluded._updated_at",
                [replaced],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("ordinary update and same-id upsert succeed");

    let pending_before = db
        .call(|conn| {
            conn.prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
                .and_then(|mut statement| {
                    statement
                        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(DbError::from)
        })
        .await
        .expect("read existing write records");
    assert_eq!(pending_before.len(), 3);

    let invalid = crate::database::StoreDatabase::new(&db)
        .run_host_store_write_for_test(None, None, move |tx| {
                    tx.execute(
                        "UPDATE things SET id = 'not-a-uuid', _updated_at = '0000000006000-0000-writer' WHERE id = ?1",
                        [replaced],
                    )?;
                    Ok::<_, DbError>(())
        })
        .await;
    let error = invalid.expect_err("invalid new UUID rejects the primary-key change");
    assert!(error.to_string().contains("not-a-uuid"));

    db.call(move |conn| {
        let row = conn
            .query_row("SELECT id, body FROM things", [], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?;
        let pending_after = conn
            .prepare("SELECT changeset FROM store_writes ORDER BY ordinal")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(DbError::from)?;
        assert_eq!(row, (replaced.to_string(), "upserted".to_string()));
        assert_eq!(pending_after, pending_before);
        Ok(())
    })
    .await
    .expect("invalid identity change rolls back row and write records");
}

#[tokio::test]
async fn database_open_rejects_empty_device_id() {
    let result = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        String::new(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[],
    );
    let error = match result {
        Ok(_) => panic!("empty device_id must be rejected"),
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains("device_id") && error.contains("empty"),
        "error names the empty device id: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_host_declared_reserved_tables() {
    for table_name in ["cloud_outbox", "protocol_state"] {
        let result = Database::open(
            Path::new(":memory:"),
            vec![SyncedTable::new(
                table_name,
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )],
            BLOB_TOMBSTONE_GRACE,
            crate::protocol::blob::TransferLimits::one_at_a_time(),
            format!("reserved-{table_name}"),
            std::sync::Arc::new(crate::clock::SystemClock),
            &[notes_migration()],
        );
        let error = match result {
            Ok(_) => panic!("reserved table {table_name} must be rejected"),
            Err(error) => error.to_string(),
        };

        assert!(
            error.contains(table_name),
            "error names reserved table {table_name}: {error}",
        );
    }
}

#[test]
fn database_open_rejects_host_triggers_using_coven_cleanup_guard_names() {
    let result = Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "reserved-cleanup-trigger".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "reserved cleanup trigger",
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TRIGGER coven_cleanup_guard_forged
             BEFORE INSERT ON notes
             BEGIN SELECT 1; END;",
        )],
    );

    let error = match result {
        Ok(_) => panic!("host migration must not reserve a Coven cleanup guard name"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("coven_cleanup_guard_forged"), "{error}");
}

#[tokio::test]
async fn database_open_rejects_empty_synced_table_name() {
    let result = Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "empty-synced-table".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[notes_migration()],
    );
    let error = match result {
        Ok(_) => panic!("empty synced table name must be rejected"),
        Err(error) => error.to_string(),
    };

    assert!(
        error.contains("empty"),
        "error names empty synced table: {error}",
    );
}

#[tokio::test]
async fn database_open_accepts_normal_host_synced_table() {
    Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "normal-synced-table".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[notes_migration()],
    )
    .expect("normal host table opens");
}

/// Open with a single host migration and the given synced-table set, expecting
/// the contract validation to refuse the open. Returns the error text so a test
/// asserts it names the offending table and the violated requirement.
fn open_contract_error(
    migration_sql: &'static str,
    tables: Vec<SyncedTable>,
    device_id: &str,
) -> String {
    let result = Database::open(
        Path::new(":memory:"),
        tables,
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(1, "contract", migration_sql)],
    );
    match result {
        Ok(_) => panic!("open must reject the synced-table contract violation"),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn database_open_rejects_integer_primary_key() {
    let error = open_contract_error(
        "CREATE TABLE things (id INTEGER PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "integer-pk",
    );
    assert!(
        error.contains("things") && error.contains("TEXT"),
        "error names the table and the TEXT requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_primary_key_not_at_column_zero() {
    let error = open_contract_error(
        "CREATE TABLE things (body TEXT NOT NULL, id TEXT PRIMARY KEY, \
         _updated_at TEXT NOT NULL) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "pk-not-first",
    );
    assert!(
        error.contains("things") && error.contains("column 0"),
        "error names the table and the column-0 requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_primary_key_named_other_than_id() {
    let error = open_contract_error(
        "CREATE TABLE things (thing_id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "pk-misnamed",
    );
    assert!(
        error.contains("things") && error.contains("`id`"),
        "error names the table and the `id` requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_composite_primary_key() {
    let error = open_contract_error(
        "CREATE TABLE things (id TEXT NOT NULL, part TEXT NOT NULL, \
         _updated_at TEXT NOT NULL, PRIMARY KEY (id, part)) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "composite-pk",
    );
    assert!(
        error.contains("things") && error.contains("composite"),
        "error names the table and the single-primary-key requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_nullable_updated_at() {
    let error = open_contract_error(
        "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "nullable-updated-at",
    );
    assert!(
        error.contains("things") && error.contains("_updated_at"),
        "error names the table and the `_updated_at` requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_non_strict_synced_table() {
    let error = open_contract_error(
        "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL);",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "non-strict",
    );
    assert!(
        error.contains("things") && error.contains("STRICT"),
        "error names the table and the STRICT requirement: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_declared_table_no_migration_creates() {
    let error = open_contract_error(
        "CREATE TABLE other (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "declared-never-created",
    );
    assert!(
        error.contains("things") && error.contains("no migration creates it"),
        "error says the declared table was never created: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_synced_table_spelling_that_differs_from_live_schema() {
    let error = open_contract_error(
        "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        vec![SyncedTable::new(
            "Things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        "case-variant-table-name",
    );
    assert!(
        error.contains("Things") && error.contains("things") && error.contains("exact"),
        "error names both spellings and requires the live spelling: {error}",
    );
}

#[tokio::test]
async fn database_open_rejects_case_variant_duplicate_synced_tables() {
    let error = open_contract_error(
        "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        vec![
            SyncedTable::new(
                "things",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "THINGS",
                crate::protocol::synced_schema::RowIdentity::IndependentUuid,
            ),
        ],
        "case-variant-duplicate-table",
    );
    assert!(
        error.contains("things") && error.contains("THINGS") && error.contains("more than once"),
        "error names both duplicate declarations: {error}",
    );
}

#[tokio::test]
async fn database_open_accepts_strict_synced_table() {
    Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "strict-synced-table".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "contract",
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;",
        )],
    )
    .expect("a STRICT synced table satisfying the rest of the contract opens");
}

#[tokio::test]
async fn database_open_ignores_undeclared_non_strict_local_table() {
    // `things` is declared and STRICT; `scratch` is a host-local table never
    // passed to `synced_tables` — its own business, not coven's, so it stays
    // non-strict with no open error.
    Database::open(
        Path::new(":memory:"),
        vec![SyncedTable::new(
            "things",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )],
        BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "undeclared-local-table".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "contract",
            "CREATE TABLE things (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT; \
             CREATE TABLE scratch (id INTEGER PRIMARY KEY, note TEXT);",
        )],
    )
    .expect("an undeclared non-strict local table is not coven's business");
}

#[tokio::test]
async fn database_open_rejects_duplicate_blob_namespace() {
    let blob = |namespace| {
        crate::protocol::synced_schema::BlobDecl::new(
            namespace,
            crate::protocol::blob::Provenance::HostProvided,
            crate::protocol::blob::CacheFill::CacheLazy,
        )
    };
    let error = open_contract_error(
        "CREATE TABLE covers (id TEXT PRIMARY KEY, size INTEGER NOT NULL, \
         hash TEXT, _updated_at TEXT NOT NULL) STRICT;\
         CREATE TABLE thumbs (id TEXT PRIMARY KEY, size INTEGER NOT NULL, \
         hash TEXT, _updated_at TEXT NOT NULL) STRICT;",
        vec![
            SyncedTable::new(
                "covers",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .carries_blob(blob("images")),
            SyncedTable::new(
                "thumbs",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .carries_blob(blob("images")),
        ],
        "dup-namespace",
    );
    assert!(
        error.contains("covers") && error.contains("thumbs") && error.contains("images"),
        "error names both tables and the shared blob namespace: {error}",
    );
}

use super::super::*;
use crate::blob::BLOB_TOMBSTONE_GRACE;
use crate::database::StoreDatabase;

#[tokio::test]
async fn fresh_open_requires_each_make_remote_intent_to_name_retain_pinned() {
    let (db, _stamper) = Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[],
    )
    .expect("open database");

    let column = db
        .call(|conn| {
            let mut stmt = conn
                .prepare("PRAGMA table_info(blob_make_remote_intents)")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(DbError::from)?;
            for row in rows {
                let (name, notnull, default_value) = row.map_err(DbError::from)?;
                if name == "retain_pinned" {
                    return Ok(Some((notnull, default_value)));
                }
            }
            Ok(None)
        })
        .await
        .expect("read make_remote intent schema")
        .expect("retain_pinned column exists");

    assert_eq!(column.0, 1, "retain_pinned must be NOT NULL");
    assert_eq!(
        column.1, None,
        "retain_pinned must be supplied by every make_remote intent",
    );
}

async fn capture_scoped_write_then_reopen(
    name: &str,
) -> (
    tempfile::TempDir,
    Database,
    Vec<(String, Option<String>, Vec<u8>)>,
) {
    let temp = tempfile::tempdir().expect("temporary scoped Store");
    let path = temp.path().join(format!("{name}.db"));
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let migrations = vec![Migration::sql(
        1,
        "accounts",
        "CREATE TABLE accounts (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
         ) STRICT;",
    )];
    let (db, _) = Database::open(
        &path,
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        format!("{name}-device"),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("open scoped Store");
    crate::sync::test_helpers::TestStore::create(&db, name, crate::keys::UserKeypair::generate())
        .await
        .expect("install exact scoped Store authority");
    let circle_label = format!("{name}-circle");
    let (circle_id, _control) = db
        .call(move |conn| {
            let database = crate::database::DatabaseTestSql::new(conn);
            Ok(crate::sync::test_helpers::install_test_active_circle(
                &database,
                &circle_label,
            ))
        })
        .await
        .expect("install active Circle current state");
    let circle_id = circle_id.to_string();

    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let write_id = db.new_write_id();
    let routing = EncryptionService::from_key([7; 32]);
    let capture_tables = tables.clone();
    let capture_circle_id = circle_id.clone();
    db.call(move |conn| {
        StoreDatabase::run_store_write_transaction_on(
            conn,
            &capture_tables,
            &gates,
            &blob_decls,
            Some(&routing),
            None,
            write_id,
            |tx| {
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('store-account', NULL, '0000000001000-0000-restart')",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('circle-account', ?1, '0000000001001-0000-restart')",
                    [&capture_circle_id],
                )?;
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('local-account', 'local', '0000000001002-0000-restart')",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("capture Store and Circle partitions");
    let expected = db
        .call(|conn| {
            conn.prepare(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions
                 ORDER BY CASE audience WHEN 'store' THEN 0 WHEN 'local' THEN 2 ELSE 1 END,
                          audience, control_coord",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(DbError::from)
        })
        .await
        .expect("read exact persisted audience partitions");
    assert_eq!(expected.len(), 3);
    assert!(expected.iter().any(|(audience, control, changeset)| {
        audience == "local" && control.is_none() && !changeset.is_empty()
    }));
    drop(db);

    let (reopened, _) = Database::open(
        &path,
        tables,
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        format!("{name}-device"),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("reopen scoped Store");
    (temp, reopened, expected)
}

fn assert_prepared_partitions(
    actual: &PreparedStoreWritePartitions,
    expected: &[(String, Option<String>, Vec<u8>)],
) {
    let actual = actual
        .iter()
        .map(|partition| {
            let audience = match partition.audience {
                crate::protocol::circle::Audience::Store => "store".to_string(),
                crate::protocol::circle::Audience::Circle(circle) => circle.to_string(),
                crate::protocol::circle::Audience::Local => "local".to_string(),
            };
            (
                audience,
                partition.control.as_ref().map(|control| {
                    let parsed =
                        serde_json::from_str(control.stored_json()).expect("parse stored control");
                    assert_eq!(control.coordinate(), &parsed);
                    control.stored_json().to_string()
                }),
                partition.changeset.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn merge_preparation_reloads_exact_scoped_partitions_after_restart() {
    let (_temp, reopened, expected) = capture_scoped_write_then_reopen("merge-restart").await;
    let prepared = StoreDatabase::new(&reopened)
        .prepare_store_write()
        .await
        .expect("prepare restarted Merge write")
        .expect("pending Merge write");

    assert_prepared_partitions(&prepared.partitions, &expected);
}

#[tokio::test]
async fn preparation_rejects_a_local_partition_with_circle_control() {
    let (_temp, reopened, _expected) =
        capture_scoped_write_then_reopen("controlled-local-restart").await;
    reopened
        .call(|conn| {
            conn.pragma_update(None, "ignore_check_constraints", true)
                .map_err(DbError::from)?;
            conn.execute(
                "UPDATE store_write_partitions
                 SET control_coord = '{}'
                 WHERE audience = 'local'",
                [],
            )
            .map_err(DbError::from)?;
            conn.pragma_update(None, "ignore_check_constraints", false)
                .map_err(DbError::from)
        })
        .await
        .expect("plant controlled Local partition");

    let error = match StoreDatabase::new(&reopened).prepare_store_write().await {
        Err(error) => error,
        Ok(_) => panic!("controlled Local partition must fail preparation"),
    };
    assert!(error
        .to_string()
        .contains("Local partition carries a Circle control"));
}

async fn assert_local_only_scoped_write(name: &str) {
    let (_temp, db, _) = capture_scoped_write_then_reopen(name).await;
    let store_database = StoreDatabase::new(&db);
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let write_id = db.new_write_id();
    let routing = EncryptionService::from_key([7; 32]);
    let receipt = db
        .call(move |conn| {
            StoreDatabase::run_store_write_transaction_on(
                conn,
                &tables,
                &gates,
                &blob_decls,
                Some(&routing),
                None,
                write_id,
                |tx| {
                    tx.execute(
                        "INSERT INTO accounts (id, audience, _updated_at)
                         VALUES ('second-local-account', 'local', '0000000002000-0000-local')",
                        [],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect("capture local-only partition");

    assert_eq!(receipt.status, WriteStatus::LocalOnly);
    assert_eq!(
        store_database
            .write_status(&receipt.write_id)
            .await
            .expect("reload local-only status"),
        WriteStatus::LocalOnly
    );
    assert!(store_database
        .pending_writes()
        .await
        .expect("list pending writes")
        .iter()
        .all(|write| write.write_id != receipt.write_id));
    let stored_write_id = receipt.write_id.clone();
    db.call(move |conn| {
        let (affected_rows, store_changeset): (String, Vec<u8>) = conn
            .query_row(
                "SELECT affected_rows, changeset FROM store_writes WHERE write_id = ?1",
                [stored_write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        assert_eq!(affected_rows, "[]");
        assert!(store_changeset.is_empty());
        let partition: (String, Option<String>, Vec<u8>) = conn
            .query_row(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions WHERE write_id = ?1",
                [stored_write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        assert_eq!(partition.0, "local");
        assert_eq!(partition.1, None);
        assert!(!partition.2.is_empty());
        Ok(())
    })
    .await
    .expect("verify durable local-only journal");
}

#[tokio::test]
async fn merge_local_only_scoped_write_is_not_pending() {
    assert_local_only_scoped_write("merge-local-only").await;
}

/// A transaction whose rows are all Circle-scoped emits one package per Circle
/// plus a Store package carrying only the audience mirror — never an empty Store
/// package. The Store partition exists so a peer without the Circle still learns
/// each row's routing (`_coven_audience`), but it must carry that mirror and no
/// scoped row bytes; an empty Store changeset would be a Store commit activating
/// nothing.
#[tokio::test]
async fn circle_only_write_emits_a_mirror_only_store_package() {
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let (db, _) = Database::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "circle-only-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
    .expect("open Circle-only Store");
    crate::sync::test_helpers::TestStore::create(
        &db,
        "circle-only",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("install scoped Store authority");
    let (first, second) = db
        .call(|conn| {
            let database = crate::database::DatabaseTestSql::new(conn);
            let (first, _) = crate::sync::test_helpers::install_test_active_circle(
                &database,
                "circle-only-first",
            );
            let (second, _) = crate::sync::test_helpers::install_test_active_circle(
                &database,
                "circle-only-second",
            );
            Ok((first, second))
        })
        .await
        .expect("install two Circles");

    let routing = EncryptionService::from_key([7; 32]);
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let write_id = db.new_write_id();
    let stored_write_id = write_id.clone();
    let write_tables = tables.clone();
    let (first_audience, second_audience) = (first.to_string(), second.to_string());
    let (write_first, write_second) = (first_audience.clone(), second_audience.clone());
    db.call(move |conn| {
        StoreDatabase::run_store_write_transaction_on(
            conn,
            &write_tables,
            &gates,
            &blob_decls,
            Some(&routing),
            None,
            write_id,
            |tx| {
                tx.execute(
                    "INSERT INTO accounts VALUES ('first-account', ?1, '0000000001000-0000-circle')",
                    [&write_first],
                )?;
                tx.execute(
                    "INSERT INTO accounts VALUES ('second-account', ?1, '0000000001001-0000-circle')",
                    [&write_second],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("capture Circle-only partitions");

    let partitions = db
        .call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )
                .map_err(DbError::from)?;
            let partitions = statement
                .query_map([stored_write_id.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            Ok(partitions)
        })
        .await
        .expect("read Circle-only partitions");

    let mut audiences = partitions
        .iter()
        .map(|(audience, _)| audience.clone())
        .collect::<Vec<_>>();
    audiences.sort();
    let mut expected = vec![first.to_string(), second.to_string(), "store".to_string()];
    expected.sort();
    assert_eq!(
        audiences, expected,
        "a Circle-only write emits one package per Circle plus one Store package",
    );

    let store_changeset = &partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Circle-only write emits a Store package")
        .1;
    let store_rows =
        crate::database::walk_changeset(store_changeset).expect("walk Store partition");
    assert!(
        !store_rows.is_empty(),
        "the Store package is not empty — an empty Store commit would activate nothing",
    );
    assert!(
        store_rows.iter().all(|row| row.table == "_coven_audience"),
        "the Store package carries only the audience mirror, no scoped rows: {:?}",
        store_rows.iter().map(|row| &row.table).collect::<Vec<_>>(),
    );

    for (circle, audience) in [(first, &first_audience), (second, &second_audience)] {
        let circle_changeset = &partitions
            .iter()
            .find(|(partition_audience, _)| partition_audience == audience)
            .unwrap_or_else(|| panic!("Circle {circle} package is present"))
            .1;
        let circle_rows =
            crate::database::walk_changeset(circle_changeset).expect("walk Circle partition");
        assert!(
            circle_rows.iter().any(|row| row.table == "accounts"),
            "each Circle package carries its scoped rows",
        );
    }
}

#[tokio::test]
async fn cross_circle_move_emits_only_the_destination_image_and_store_mirror() {
    let tables = vec![
        SyncedTable::new("accounts", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
    ];
    let (db, _) = Database::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "move-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
    .expect("open scoped move Store");
    crate::sync::test_helpers::TestStore::create(
        &db,
        "cross-circle-move",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("install scoped Store authority");
    let (source, destination) = db
        .call(|conn| {
            let database = crate::database::DatabaseTestSql::new(conn);
            let (source, _) =
                crate::sync::test_helpers::install_test_active_circle(&database, "move-source");
            let (destination, _) = crate::sync::test_helpers::install_test_active_circle(
                &database,
                "move-destination",
            );
            Ok((source, destination))
        })
        .await
        .expect("install source and destination Circles");
    let routing = EncryptionService::from_key([7; 32]);
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let insert_tables = tables.clone();
    let insert_id = db.new_write_id();
    db.call(move |conn| {
        StoreDatabase::run_store_write_transaction_on(
            conn,
            &insert_tables,
            &gates,
            &blob_decls,
            Some(&routing),
            None,
            insert_id,
            |tx| {
                tx.execute(
                    "INSERT INTO accounts VALUES ('account', ?1, '0000000001000-0000-move')",
                    [source.to_string()],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("insert source Circle row");

    let routing = EncryptionService::from_key([7; 32]);
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let move_id = db.new_write_id();
    let stored_move_id = move_id.clone();
    db.call(move |conn| {
        StoreDatabase::run_store_write_transaction_on(
            conn,
            &tables,
            &gates,
            &blob_decls,
            Some(&routing),
            None,
            move_id,
            |tx| {
                tx.execute(
                    "UPDATE accounts
                     SET audience = ?1, _updated_at = '0000000002000-0000-move'
                     WHERE id = 'account'",
                    [destination.to_string()],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("move row between Circles");

    let partitions = db
        .call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT audience, changeset
                     FROM store_write_partitions
                     WHERE write_id = ?1
                     ORDER BY audience",
                )
                .map_err(DbError::from)?;
            let partitions = statement
                .query_map([stored_move_id.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            Ok(partitions)
        })
        .await
        .expect("read move partitions");
    assert_eq!(partitions.len(), 2);
    assert!(partitions
        .iter()
        .all(|(audience, _)| audience != &source.to_string()));

    let store = partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("move has Store mirror partition");
    let store_rows = crate::database::walk_changeset(&store.1).expect("walk Store move partition");
    assert_eq!(store_rows.len(), 1);
    assert_eq!(store_rows[0].table, "_coven_audience");
    assert_eq!(store_rows[0].op, crate::changeset::ChangeOp::Update);

    let destination_rows = crate::database::walk_changeset(
        &partitions
            .iter()
            .find(|(audience, _)| audience == &destination.to_string())
            .expect("move has destination Circle partition")
            .1,
    )
    .expect("walk destination move partition");
    assert!(destination_rows
        .iter()
        .any(|row| { row.table == "accounts" && row.op == crate::changeset::ChangeOp::Insert }));
    assert!(destination_rows.iter().any(|row| {
        row.table == "_coven_row_routes" && row.op == crate::changeset::ChangeOp::Insert
    }));
    assert!(destination_rows
        .iter()
        .all(|row| row.op != crate::changeset::ChangeOp::Delete));
}

#[tokio::test]
async fn root_move_rejects_an_unchanged_descendants_cross_circle_foreign_key() {
    let tables = vec![
        SyncedTable::new("notes", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
        SyncedTable::new("categories", crate::sync::session::RowIdentity::SharedKey)
            .scoped_by("audience"),
        SyncedTable::new("comments", crate::sync::session::RowIdentity::SharedKey)
            .inherits_audience_through("note_id"),
    ];
    let (db, _) = Database::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "foreign-key-move-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &[Migration::sql(
            1,
            "scoped relationship",
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE categories (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id),
                 category_id TEXT NOT NULL REFERENCES categories(id),
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
    .expect("open scoped relationship Store");
    crate::sync::test_helpers::TestStore::create(
        &db,
        "foreign-key-move",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("install scoped Store authority");
    let (source, destination) = db
        .call(|conn| {
            let database = crate::database::DatabaseTestSql::new(conn);
            let (source, _) = crate::sync::test_helpers::install_test_active_circle(
                &database,
                "relationship-source",
            );
            let (destination, _) = crate::sync::test_helpers::install_test_active_circle(
                &database,
                "relationship-destination",
            );
            Ok((source, destination))
        })
        .await
        .expect("install relationship Circles");
    let routing = EncryptionService::from_key([7; 32]);
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let insert_tables = tables.clone();
    let insert_id = db.new_write_id();
    db.call(move |conn| {
        StoreDatabase::run_store_write_transaction_on(
            conn,
            &insert_tables,
            &gates,
            &blob_decls,
            Some(&routing),
            None,
            insert_id,
            |tx| {
                tx.execute(
                    "INSERT INTO notes VALUES ('note', ?1, '0000000001000-0000-move')",
                    [source.to_string()],
                )?;
                tx.execute(
                    "INSERT INTO categories VALUES ('category', ?1, '0000000001000-0000-move')",
                    [source.to_string()],
                )?;
                tx.execute(
                    "INSERT INTO comments
                     VALUES ('comment', 'note', 'category', '0000000001000-0000-move')",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
    })
    .await
    .expect("insert valid scoped relationship");

    let routing = EncryptionService::from_key([7; 32]);
    let gates = db.gates();
    let blob_decls = db.blob_decls();
    let move_id = db.new_write_id();
    let error = db
        .call(move |conn| {
            StoreDatabase::run_store_write_transaction_on(
                conn,
                &tables,
                &gates,
                &blob_decls,
                Some(&routing),
                None,
                move_id,
                |tx| {
                    tx.execute(
                        "UPDATE notes
                         SET audience = ?1, _updated_at = '0000000002000-0000-move'
                         WHERE id = 'note'",
                        [destination.to_string()],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
        })
        .await
        .expect_err("move must reject the unchanged cross-Circle relationship");
    assert!(error
        .to_string()
        .contains("relationship through category_id"));
    let audience = db
        .call(|conn| {
            conn.query_row("SELECT audience FROM notes WHERE id = 'note'", [], |row| {
                row.get::<_, String>(0)
            })
            .map_err(DbError::from)
        })
        .await
        .expect("read rolled-back note audience");
    assert_eq!(audience, source.to_string());
}

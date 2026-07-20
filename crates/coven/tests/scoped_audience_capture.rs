use coven::{
    Config, Coven, KeyCustody, MasterKeyring, Migration, RowIdentity, StoreDir, SyncedTable,
    WritePolicy,
};

const CIRCLE_LABEL: &str = "circle-a";
fn routing_keyring() -> MasterKeyring {
    coven_core::encryption::EncryptionService::from_key([7; 32]).into()
}

fn routing_id(conn: &rusqlite::Connection, table: &str, row_id: &str) -> String {
    coven_core::sync::test_helpers::test_row_routing_id(conn, [7; 32], table, row_id).to_string()
}

fn seed_store_root(conn: &rusqlite::Connection) {
    coven_core::sync::test_helpers::install_test_store_root_authority(conn, "scoped-routing-root");
}

fn seed_active_circle(
    conn: &rusqlite::Connection,
    label: &str,
    policy: WritePolicy,
) -> (String, String) {
    if policy == WritePolicy::MergeConcurrent {
        seed_store_root(conn);
    }
    let (circle_id, control) =
        coven_core::sync::test_helpers::install_test_active_circle(conn, label, policy);
    (
        circle_id.to_string(),
        serde_json::to_string(&control).expect("serialize active Circle control"),
    )
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn has_change(
    changes: &[coven_core::changeset::RowChange],
    table: &str,
    op: coven_core::changeset::ChangeOp,
    primary_key: &str,
) -> bool {
    changes
        .iter()
        .any(|change| change.table == table && change.op == op && change.pk() == Some(primary_key))
}

#[derive(Debug, PartialEq, Eq)]
struct ReparentRollbackState {
    account: String,
    requirement: String,
    descendant_count: i64,
    writes: i64,
    partitions: i64,
    routes: Vec<(String, String, String)>,
    mirror: Vec<(String, Option<String>)>,
}

fn reparent_rollback_state(
    conn: &rusqlite::Connection,
    transaction_id: &str,
    policy: WritePolicy,
) -> rusqlite::Result<ReparentRollbackState> {
    let (account, requirement) = conn.query_row(
        "SELECT account_id, requirement_id FROM transactions WHERE id = ?1",
        [transaction_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let descendant_count = conn.query_row(
        "SELECT count(*) FROM line_items WHERE transaction_id = ?1",
        [transaction_id],
        |row| row.get::<_, i64>(0),
    )?;
    let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let partitions = conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let (routes, mirror) = if policy == WritePolicy::MergeConcurrent {
        let routes = conn
            .prepare(
                "SELECT routing_id, table_name, row_id FROM _coven_row_routes
                 ORDER BY routing_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mirror = conn
            .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        (routes, mirror)
    } else {
        (Vec::new(), Vec::new())
    };
    Ok(ReparentRollbackState {
        account,
        requirement,
        descendant_count,
        writes,
        partitions,
        routes,
        mirror,
    })
}

#[derive(Debug, PartialEq, Eq)]
struct ScopedAncestorRollbackState {
    folders: Vec<(String, String, String)>,
    documents: Vec<(String, String, String, String)>,
    details: Vec<(String, String, String)>,
    writes: i64,
    partitions: i64,
    routes: Vec<(String, String, String)>,
    mirror: Vec<(String, Option<String>)>,
}

fn scoped_ancestor_rollback_state(
    conn: &rusqlite::Connection,
    policy: WritePolicy,
) -> rusqlite::Result<ScopedAncestorRollbackState> {
    let folders = conn
        .prepare("SELECT id, name, _updated_at FROM folders ORDER BY id")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let documents = conn
        .prepare("SELECT id, folder_id, audience, _updated_at FROM documents ORDER BY id")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let details = conn
        .prepare("SELECT id, document_id, _updated_at FROM details ORDER BY id")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let partitions = conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let (routes, mirror) = if policy == WritePolicy::MergeConcurrent {
        let routes = conn
            .prepare(
                "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                 ORDER BY table_name, row_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mirror = conn
            .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        (routes, mirror)
    } else {
        (Vec::new(), Vec::new())
    };
    Ok(ScopedAncestorRollbackState {
        folders,
        documents,
        details,
        writes,
        partitions,
        routes,
        mirror,
    })
}

#[tokio::test]
async fn scoped_insert_captures_store_and_circle_while_local_stays_on_device() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "scoped-capture".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped capture".to_string(),
    );
    let handle = Coven::builder(config)
        .key_custody(KeyCustody::InMemory(routing_keyring()))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience")
        ])
        .migrations(vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open scoped Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, control_coord) =
        seed_active_circle(&authority, CIRCLE_LABEL, WritePolicy::MergeConcurrent);
    let store_account_route = routing_id(&authority, "accounts", "store-account");
    let circle_account_route = routing_id(&authority, "accounts", "circle-account");
    let local_account_route = routing_id(&authority, "accounts", "local-account");
    drop(authority);

    let write_circle_id = circle_id.clone();
    let receipt = handle
        .sql(move |sql| {
            for (id, name, audience) in [
                ("store-account", "Store", None),
                ("circle-account", "Circle", Some(write_circle_id.as_str())),
                ("local-account", "Local", Some("local")),
            ] {
                sql.execute(
                    "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    (id, name, audience, sql.stamp()),
                )?;
            }
            Ok(())
        })
        .await
        .expect("capture scoped host transaction");

    let write_id = receipt.write_id.to_string();
    let affected_write_id = write_id.clone();
    let partitions = handle
        .sql_read(move |conn| {
            let mut statement = conn.prepare(
                "SELECT audience, control_coord, changeset
                 FROM store_write_partitions
                 WHERE write_id = ?1
                 ORDER BY audience",
            )?;
            let rows = statement
                .query_map([write_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into);
            rows
        })
        .await
        .expect("read durable audience partitions");
    let affected_rows = handle
        .sql_read(move |conn| {
            conn.query_row(
                "SELECT affected_rows FROM store_writes WHERE write_id = ?1",
                [affected_write_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await
        .expect("read public affected rows");
    assert!(
        !affected_rows.contains("_coven_audience") && !affected_rows.contains("_coven_row_routes"),
        "private routing tables must not appear in the public write receipt"
    );

    assert_eq!(partitions.len(), 3);
    let partition = |audience: &str| {
        partitions
            .iter()
            .find(|(candidate, _, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} partition"))
    };
    let store = partition("store");
    assert_eq!(store.1, None);
    assert!(contains_bytes(&store.2, b"store-account"));
    assert!(!contains_bytes(&store.2, b"circle-account"));
    assert!(!contains_bytes(&store.2, b"local-account"));
    let store_changes = coven_core::changeset::walk(&store.2).expect("walk Store partition");
    for routing_id in [&store_account_route, &circle_account_route] {
        assert!(store_changes.iter().any(|change| {
            change.table == "_coven_audience"
                && change.op == coven_core::changeset::ChangeOp::Insert
                && change.pk() == Some(routing_id)
        }));
    }

    let circle = partition(&circle_id);
    assert_eq!(circle.1.as_deref(), Some(control_coord.as_str()));
    assert!(contains_bytes(&circle.2, b"circle-account"));
    assert!(!contains_bytes(&circle.2, b"store-account"));
    assert!(!contains_bytes(&circle.2, b"local-account"));
    let circle_changes = coven_core::changeset::walk(&circle.2).expect("walk Circle partition");
    assert!(circle_changes.iter().any(|change| {
        change.table == "_coven_row_routes"
            && change.op == coven_core::changeset::ChangeOp::Insert
            && change.pk() == Some(&circle_account_route)
    }));
    let local = partition("local");
    assert_eq!(local.1, None);
    assert!(contains_bytes(&local.2, b"local-account"));
    assert!(!contains_bytes(&local.2, b"store-account"));
    assert!(!contains_bytes(&local.2, b"circle-account"));
    let local_changes = coven_core::changeset::walk(&local.2).expect("walk Local partition");
    assert!(has_change(
        &local_changes,
        "accounts",
        coven_core::changeset::ChangeOp::Insert,
        "local-account"
    ));
    assert!(has_change(
        &local_changes,
        "_coven_row_routes",
        coven_core::changeset::ChangeOp::Insert,
        &local_account_route
    ));

    let routes = handle
        .sql_read(|conn| {
            let mut statement = conn.prepare(
                "SELECT routing_id, table_name, row_id
                 FROM _coven_row_routes ORDER BY row_id",
            )?;
            let routes = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut mirror = conn
                .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?;
            let mirror = mirror
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((routes, mirror))
        })
        .await
        .expect("read deterministic scoped routes");
    assert_eq!(
        routes.0,
        vec![
            (
                circle_account_route.clone(),
                "accounts".to_string(),
                "circle-account".to_string(),
            ),
            (
                local_account_route,
                "accounts".to_string(),
                "local-account".to_string(),
            ),
            (
                store_account_route.clone(),
                "accounts".to_string(),
                "store-account".to_string(),
            ),
        ]
    );
    assert_eq!(routes.1.len(), 2, "Local rows have no Store mirror");
    assert!(routes.1.contains(&(store_account_route, None)));
    assert!(routes.1.contains(&(circle_account_route, Some(circle_id))));
}

#[tokio::test]
async fn store_to_circle_move_materializes_the_root_and_inherited_child_atomically() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "scoped-update-move".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped update move".to_string(),
    );
    let handle = Coven::builder(config)
        .key_custody(KeyCustody::InMemory(routing_keyring()))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                memo TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open scoped Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) =
        seed_active_circle(&authority, CIRCLE_LABEL, WritePolicy::MergeConcurrent);
    let store_account_route = routing_id(&authority, "accounts", "store-account");
    let store_transaction_route = routing_id(&authority, "transactions", "store-transaction");
    drop(authority);

    handle
        .sql(|sql| {
            sql.execute(
                "INSERT INTO accounts (id, name, audience, _updated_at)
                 VALUES ('store-account', 'Store', NULL, ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO transactions (id, account_id, memo, _updated_at)
                 VALUES ('store-transaction', 'store-account', 'Inherited', ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("seed scoped rows");

    let before = handle
        .sql_read(|conn| {
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            let mirror = conn
                .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((writes, partitions, mirror))
        })
        .await
        .expect("count durable writes before injected failure");
    let fault = rusqlite::Connection::open(store_dir.db_path()).expect("open fault injector");
    fault
        .execute_batch(
            "CREATE TRIGGER fail_audience_partition_insert
             BEFORE INSERT ON store_write_partitions
             BEGIN
                 SELECT RAISE(ABORT, 'forced audience partition failure');
             END;",
        )
        .expect("install partition journal failure");

    let failed_circle_id = circle_id.clone();
    let failed = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'store-account'",
                (&failed_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await;
    assert!(failed.is_err(), "the injected journal failure must surface");
    let after_failure = handle
        .sql_read(move |conn| {
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'store-account'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )?;
            let child_count = conn.query_row(
                "SELECT count(*) FROM transactions WHERE id = 'store-transaction'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            let mirror = conn
                .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((audience, child_count, writes, partitions, mirror))
        })
        .await
        .expect("read state after injected partition failure");
    assert_eq!(after_failure.0, None, "the root audience must roll back");
    assert_eq!(after_failure.1, 1, "the inherited child must remain");
    assert_eq!(after_failure.2, before.0, "no write journal row may commit");
    assert_eq!(
        after_failure.3, before.1,
        "no audience partition may commit"
    );
    assert_eq!(
        after_failure.4, before.2,
        "the Store audience mirror must roll back with the host move"
    );
    fault
        .execute_batch("DROP TRIGGER fail_audience_partition_insert;")
        .expect("remove partition journal failure");
    drop(fault);

    let moved_circle_id = circle_id.clone();
    let moved = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'store-account'",
                (&moved_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .expect("move Store row into Circle");
    let move_write_id = moved.write_id.to_string();
    let (host_audience, child_count, move_partitions, routes, mirror) = handle
        .sql_read(move |conn| {
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'store-account'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )?;
            let child_count = conn.query_row(
                "SELECT count(*) FROM transactions WHERE id = 'store-transaction'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let mut statement = conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?;
            let rows = statement
                .query_map([move_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let routes = conn
                .prepare(
                    "SELECT routing_id, table_name, row_id
                     FROM _coven_row_routes ORDER BY row_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mirror = conn
                .prepare("SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id")?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((audience, child_count, rows, routes, mirror))
        })
        .await
        .expect("read Store to Circle move partitions");

    assert_eq!(host_audience.as_deref(), Some(circle_id.as_str()));
    assert_eq!(child_count, 1);
    assert_eq!(
        routes,
        vec![
            (
                store_account_route.clone(),
                "accounts".to_string(),
                "store-account".to_string(),
            ),
            (
                store_transaction_route.clone(),
                "transactions".to_string(),
                "store-transaction".to_string(),
            ),
        ]
    );
    let mut expected_mirror = vec![
        (store_transaction_route.clone(), Some(circle_id.clone())),
        (store_account_route.clone(), Some(circle_id.clone())),
    ];
    expected_mirror.sort();
    assert_eq!(
        mirror, expected_mirror,
        "the root and inherited child mirrors must change with the host move"
    );
    assert_eq!(
        move_partitions.len(),
        2,
        "the committed move must durably contain exactly Store and Circle partitions"
    );
    let circle = move_partitions
        .iter()
        .find(|(audience, _)| audience == &circle_id)
        .expect("Circle destination partition");
    let circle_changes = coven_core::changeset::walk(&circle.1).expect("walk Circle partition");
    assert!(circle_changes.iter().any(|change| {
        change.table == "accounts"
            && change.op == coven_core::changeset::ChangeOp::Insert
            && change.pk() == Some("store-account")
    }));
    assert!(circle_changes.iter().any(|change| {
        change.table == "transactions"
            && change.op == coven_core::changeset::ChangeOp::Insert
            && change.pk() == Some("store-transaction")
    }));
    for routing_id in [&store_account_route, &store_transaction_route] {
        assert!(circle_changes.iter().any(|change| {
            change.table == "_coven_row_routes"
                && change.op == coven_core::changeset::ChangeOp::Update
                && change.pk() == Some(routing_id)
        }));
    }
    let store = move_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("old Store materialization partition");
    let store_changes = coven_core::changeset::walk(&store.1).expect("walk Store partition");
    assert!(store_changes.iter().any(|change| {
        change.table == "accounts"
            && change.op == coven_core::changeset::ChangeOp::Delete
            && change.pk() == Some("store-account")
    }));
    assert!(store_changes.iter().any(|change| {
        change.table == "transactions"
            && change.op == coven_core::changeset::ChangeOp::Delete
            && change.pk() == Some("store-transaction")
    }));
    for routing_id in [&store_account_route, &store_transaction_route] {
        assert!(store_changes.iter().any(|change| {
            change.table == "_coven_audience"
                && change.op == coven_core::changeset::ChangeOp::Update
                && change.pk() == Some(routing_id)
        }));
    }
    assert!(
        move_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "Local must receive neither side of a Store-to-Circle move"
    );
}

#[tokio::test]
async fn invalid_circle_audiences_and_authority_roll_back_the_entire_host_write() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "scoped-authority-rejections".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped authority rejections".to_string(),
    );
    let handle = Coven::builder(config)
        .key_custody(KeyCustody::InMemory(routing_keyring()))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience")
        ])
        .migrations(vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open scoped Store");

    let unknown_circle = coven_core::sync::circle::CircleId::from_bytes([3; 16]).to_string();
    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    seed_store_root(&authority);
    let (inactive_circle, _) = coven_core::sync::test_helpers::install_test_inactive_circle(
        &authority,
        "inactive-circle",
        WritePolicy::MergeConcurrent,
    );
    let (mismatched_circle, _) = coven_core::sync::test_helpers::install_test_active_circle(
        &authority,
        "mismatched-circle",
        WritePolicy::Serial,
    );
    drop(authority);

    for (id, audience, expected_error) in [
        (
            "malformed-audience",
            "not-a-circle".to_string(),
            "invalid audience",
        ),
        (
            "unknown-circle",
            unknown_circle,
            "active local access records",
        ),
        (
            "inactive-circle",
            inactive_circle.to_string(),
            "active local access records",
        ),
        (
            "policy-mismatch",
            mismatched_circle.to_string(),
            "control uses Serial, expected Store policy MergeConcurrent",
        ),
    ] {
        let attempted_id = id.to_string();
        let attempted_audience = audience.clone();
        let error = handle
            .sql(move |sql| {
                sql.execute(
                    "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, 'Rejected', ?2, ?3)",
                    (attempted_id, attempted_audience, sql.stamp()),
                )?;
                Ok(())
            })
            .await
            .expect_err("invalid scoped write must fail loudly");
        assert!(
            error.to_string().contains(expected_error),
            "{id} surfaced the wrong error: {error}"
        );

        let rejected_id = id.to_string();
        let state = handle
            .sql_read(move |conn| {
                let host_rows = conn.query_row(
                    "SELECT count(*) FROM accounts WHERE id = ?1",
                    [rejected_id],
                    |row| row.get::<_, i64>(0),
                )?;
                let routes =
                    conn.query_row("SELECT count(*) FROM _coven_row_routes", [], |row| {
                        row.get::<_, i64>(0)
                    })?;
                let mirror = conn.query_row("SELECT count(*) FROM _coven_audience", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                let partitions =
                    conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                        row.get::<_, i64>(0)
                    })?;
                Ok((host_rows, routes, mirror, writes, partitions))
            })
            .await
            .expect("read state after rejected scoped write");
        assert_eq!(state, (0, 0, 0, 0, 0), "{id} left durable state");
    }
}

#[tokio::test]
async fn circle_moves_and_delete_materialize_destinations_retract_sources_and_preserve_routes() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "scoped-circle-transitions".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped Circle transitions".to_string(),
    );
    let handle = Coven::builder(config)
        .key_custody(KeyCustody::InMemory(routing_keyring()))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "accounts-and-transactions",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                memo TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open scoped Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) =
        seed_active_circle(&authority, CIRCLE_LABEL, WritePolicy::MergeConcurrent);
    let local_move_account_route = routing_id(&authority, "accounts", "local-move-account");
    let local_move_transaction_route =
        routing_id(&authority, "transactions", "local-move-transaction");
    let store_move_account_route = routing_id(&authority, "accounts", "store-move-account");
    let store_move_transaction_route =
        routing_id(&authority, "transactions", "store-move-transaction");
    let deleted_account_route = routing_id(&authority, "accounts", "deleted-account");
    let deleted_transaction_route = routing_id(&authority, "transactions", "deleted-transaction");
    drop(authority);

    let seed_circle_id = circle_id.clone();
    handle
        .sql(move |sql| {
            for (account, transaction) in [
                ("local-move-account", "local-move-transaction"),
                ("store-move-account", "store-move-transaction"),
                ("deleted-account", "deleted-transaction"),
            ] {
                sql.execute(
                    "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, 'Circle', ?2, ?3)",
                    (account, &seed_circle_id, sql.stamp()),
                )?;
                sql.execute(
                    "INSERT INTO transactions (id, account_id, memo, _updated_at)
                     VALUES (?1, ?2, 'Inherited', ?3)",
                    (transaction, account, sql.stamp()),
                )?;
            }
            Ok(())
        })
        .await
        .expect("seed three Circle subtrees");

    let before_failure = handle
        .sql_read(|conn| {
            let routes = conn
                .prepare(
                    "SELECT routing_id, table_name, row_id, _updated_at
                     FROM _coven_row_routes ORDER BY routing_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mirror = conn
                .prepare(
                    "SELECT routing_id, circle_id, _updated_at
                     FROM _coven_audience ORDER BY routing_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            Ok((routes, mirror, writes, partitions))
        })
        .await
        .expect("read state before injected failure");
    let fault = rusqlite::Connection::open(store_dir.db_path()).expect("open fault injector");
    fault
        .execute_batch(
            "CREATE TRIGGER fail_circle_transition_partition
             BEFORE INSERT ON store_write_partitions
             BEGIN
                 SELECT RAISE(ABORT, 'forced Circle transition partition failure');
             END;",
        )
        .expect("install Circle transition partition failure");
    let failed = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = 'local', _updated_at = ?1
                 WHERE id = 'local-move-account'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await;
    assert!(failed.is_err(), "injected partition failure must surface");
    let after_failure = handle
        .sql_read(|conn| {
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'local-move-account'",
                [],
                |row| row.get::<_, String>(0),
            )?;
            let routes = conn
                .prepare(
                    "SELECT routing_id, table_name, row_id, _updated_at
                     FROM _coven_row_routes ORDER BY routing_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mirror = conn
                .prepare(
                    "SELECT routing_id, circle_id, _updated_at
                     FROM _coven_audience ORDER BY routing_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            Ok((audience, routes, mirror, writes, partitions))
        })
        .await
        .expect("read rolled-back Circle transition");
    assert_eq!(after_failure.0, circle_id);
    assert_eq!(after_failure.1, before_failure.0);
    assert_eq!(after_failure.2, before_failure.1);
    assert_eq!(after_failure.3, before_failure.2);
    assert_eq!(after_failure.4, before_failure.3);
    fault
        .execute_batch("DROP TRIGGER fail_circle_transition_partition;")
        .expect("remove Circle transition partition failure");
    drop(fault);

    let local_move = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = 'local', _updated_at = ?1
                 WHERE id = 'local-move-account'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move Circle subtree to Local");
    let local_write_id = local_move.write_id.to_string();
    let (local_partitions, local_store_bytes, local_routes, local_mirror_count) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([&local_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let store_bytes = conn.query_row(
                "SELECT changeset FROM store_writes WHERE write_id = ?1",
                [&local_write_id],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            let routes = conn
                .prepare(
                    "SELECT routing_id, row_id FROM _coven_row_routes
                     WHERE row_id IN ('local-move-account', 'local-move-transaction')
                     ORDER BY row_id",
                )?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mirror_count = conn.query_row(
                "SELECT count(*) FROM _coven_audience audience
                 JOIN _coven_row_routes route USING (routing_id)
                 WHERE route.row_id IN ('local-move-account', 'local-move-transaction')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, store_bytes, routes, mirror_count))
        })
        .await
        .expect("read Circle-to-Local transition");
    assert_eq!(local_partitions.len(), 3);
    let local_partition = |audience: &str| {
        local_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Local partition"))
    };
    let circle_retract =
        coven_core::changeset::walk(&local_partition(&circle_id).1).expect("walk Circle retract");
    assert!(has_change(
        &circle_retract,
        "accounts",
        coven_core::changeset::ChangeOp::Delete,
        "local-move-account"
    ));
    assert!(has_change(
        &circle_retract,
        "transactions",
        coven_core::changeset::ChangeOp::Delete,
        "local-move-transaction"
    ));
    let local_destination =
        coven_core::changeset::walk(&local_partition("local").1).expect("walk Local destination");
    for (table, row_id) in [
        ("accounts", "local-move-account"),
        ("transactions", "local-move-transaction"),
    ] {
        assert!(has_change(
            &local_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            row_id
        ));
    }
    let store_mirror_retract =
        coven_core::changeset::walk(&local_partition("store").1).expect("walk Store mirror");
    for route in [&local_move_account_route, &local_move_transaction_route] {
        assert!(has_change(
            &store_mirror_retract,
            "_coven_audience",
            coven_core::changeset::ChangeOp::Delete,
            route
        ));
    }
    assert_eq!(local_store_bytes, local_partition("store").1);
    assert!(!contains_bytes(&local_store_bytes, b"local-move-account"));
    assert!(!contains_bytes(
        &local_store_bytes,
        b"local-move-transaction"
    ));
    assert_eq!(
        local_routes,
        vec![
            (
                local_move_account_route.clone(),
                "local-move-account".to_string(),
            ),
            (
                local_move_transaction_route.clone(),
                "local-move-transaction".to_string(),
            ),
        ]
    );
    assert_eq!(local_mirror_count, 0);

    let store_move = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = NULL, _updated_at = ?1
                 WHERE id = 'store-move-account'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move Circle subtree to Store");
    let store_write_id = store_move.write_id.to_string();
    let (store_partitions, store_routes, store_mirror) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([store_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let routes = conn
                .prepare(
                    "SELECT routing_id, row_id FROM _coven_row_routes
                     WHERE row_id IN ('store-move-account', 'store-move-transaction')
                     ORDER BY row_id",
                )?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mirror = conn
                .prepare(
                    "SELECT audience.routing_id, audience.circle_id
                     FROM _coven_audience audience
                     JOIN _coven_row_routes route USING (routing_id)
                     WHERE route.row_id IN ('store-move-account', 'store-move-transaction')
                     ORDER BY audience.routing_id",
                )?
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((partitions, routes, mirror))
        })
        .await
        .expect("read Circle-to-Store transition");
    assert_eq!(store_partitions.len(), 2);
    let store_partition = |audience: &str| {
        store_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Store partition"))
    };
    let circle_retract =
        coven_core::changeset::walk(&store_partition(&circle_id).1).expect("walk Circle retract");
    assert!(has_change(
        &circle_retract,
        "accounts",
        coven_core::changeset::ChangeOp::Delete,
        "store-move-account"
    ));
    assert!(has_change(
        &circle_retract,
        "transactions",
        coven_core::changeset::ChangeOp::Delete,
        "store-move-transaction"
    ));
    let store_destination =
        coven_core::changeset::walk(&store_partition("store").1).expect("walk Store destination");
    assert!(has_change(
        &store_destination,
        "accounts",
        coven_core::changeset::ChangeOp::Insert,
        "store-move-account"
    ));
    assert!(has_change(
        &store_destination,
        "transactions",
        coven_core::changeset::ChangeOp::Insert,
        "store-move-transaction"
    ));
    assert_eq!(
        store_routes,
        vec![
            (
                store_move_account_route.clone(),
                "store-move-account".to_string(),
            ),
            (
                store_move_transaction_route.clone(),
                "store-move-transaction".to_string(),
            ),
        ]
    );
    let mut expected_store_mirror = vec![
        (store_move_transaction_route.clone(), None),
        (store_move_account_route.clone(), None),
    ];
    expected_store_mirror.sort();
    assert_eq!(store_mirror, expected_store_mirror);

    let deleted = handle
        .sql(|sql| {
            sql.execute("DELETE FROM accounts WHERE id = 'deleted-account'", [])?;
            Ok(())
        })
        .await
        .expect("delete Circle subtree");
    let delete_write_id = deleted.write_id.to_string();
    let deleted_routes_for_query = [
        deleted_account_route.clone(),
        deleted_transaction_route.clone(),
    ];
    let (delete_partitions, host_count, route_count, mirror_count) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([delete_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let host_count = conn.query_row(
                "SELECT
                    (SELECT count(*) FROM accounts WHERE id = 'deleted-account') +
                    (SELECT count(*) FROM transactions WHERE id = 'deleted-transaction')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let route_count = conn.query_row(
                "SELECT count(*) FROM _coven_row_routes WHERE routing_id IN (?1, ?2)",
                [&deleted_routes_for_query[0], &deleted_routes_for_query[1]],
                |row| row.get::<_, i64>(0),
            )?;
            let mirror_count = conn.query_row(
                "SELECT count(*) FROM _coven_audience WHERE routing_id IN (?1, ?2)",
                [&deleted_routes_for_query[0], &deleted_routes_for_query[1]],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, host_count, route_count, mirror_count))
        })
        .await
        .expect("read Circle delete transition");
    assert_eq!(delete_partitions.len(), 2);
    let delete_partition = |audience: &str| {
        delete_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-delete partition"))
    };
    let circle_delete =
        coven_core::changeset::walk(&delete_partition(&circle_id).1).expect("walk Circle delete");
    for (table, row_id, route_id) in [
        (
            "accounts",
            "deleted-account",
            deleted_account_route.as_str(),
        ),
        (
            "transactions",
            "deleted-transaction",
            deleted_transaction_route.as_str(),
        ),
    ] {
        assert!(has_change(
            &circle_delete,
            table,
            coven_core::changeset::ChangeOp::Delete,
            row_id
        ));
        assert!(has_change(
            &circle_delete,
            "_coven_row_routes",
            coven_core::changeset::ChangeOp::Delete,
            route_id
        ));
    }
    let store_delete =
        coven_core::changeset::walk(&delete_partition("store").1).expect("walk Store delete");
    for route in [&deleted_account_route, &deleted_transaction_route] {
        assert!(has_change(
            &store_delete,
            "_coven_audience",
            coven_core::changeset::ChangeOp::Delete,
            route
        ));
    }
    assert!(delete_partitions
        .iter()
        .all(|(audience, _)| audience != "local"));
    assert_eq!((host_count, route_count, mirror_count), (0, 0, 0));
}

#[tokio::test]
async fn serial_routes_from_captured_audiences_without_merge_metadata() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "serial-scoped-transitions".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Serial scoped transitions".to_string(),
    );
    let handle = Coven::builder(config)
        .write_policy(WritePolicy::Serial)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "accounts-and-transactions",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                memo TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open Serial scoped Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = seed_active_circle(&authority, CIRCLE_LABEL, WritePolicy::Serial);
    drop(authority);

    let seed_circle_id = circle_id.clone();
    let inserted = handle
        .sql(move |sql| {
            for (account, transaction, audience) in [
                ("serial-moving-account", "serial-moving-child", None),
                (
                    "serial-deleted-account",
                    "serial-deleted-child",
                    Some(seed_circle_id.as_str()),
                ),
                ("serial-local-account", "serial-local-child", Some("local")),
            ] {
                sql.execute(
                    "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, 'Serial', ?2, ?3)",
                    (account, audience, sql.stamp()),
                )?;
                sql.execute(
                    "INSERT INTO transactions (id, account_id, memo, _updated_at)
                     VALUES (?1, ?2, 'Inherited', ?3)",
                    (transaction, account, sql.stamp()),
                )?;
            }
            Ok(())
        })
        .await
        .expect("insert Store, Circle, and Local Serial subtrees");
    let insert_write_id = inserted.write_id.to_string();
    let (insert_partitions, routing_table_count) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([insert_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let routing_tables = conn.query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('_coven_audience', '_coven_row_routes')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, routing_tables))
        })
        .await
        .expect("read Serial insert partitions");
    assert_eq!(routing_table_count, 0, "Serial has no Merge routing tables");
    assert_eq!(insert_partitions.len(), 3);
    let local_insert = insert_partitions
        .iter()
        .find(|(audience, _)| audience == "local")
        .expect("durable Serial Local partition");
    let local_insert =
        coven_core::changeset::walk(&local_insert.1).expect("walk Serial Local partition");
    for (table, row_id) in [
        ("accounts", "serial-local-account"),
        ("transactions", "serial-local-child"),
    ] {
        assert!(has_change(
            &local_insert,
            table,
            coven_core::changeset::ChangeOp::Insert,
            row_id
        ));
    }

    let before_failure = handle
        .sql_read(|conn| {
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'serial-moving-account'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )?;
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            Ok((audience, writes, partitions))
        })
        .await
        .expect("read Serial state before injected failure");
    let fault = rusqlite::Connection::open(store_dir.db_path()).expect("open fault injector");
    fault
        .execute_batch(
            "CREATE TRIGGER fail_serial_partition_insert
             BEFORE INSERT ON store_write_partitions
             BEGIN
                 SELECT RAISE(ABORT, 'forced Serial partition failure');
             END;",
        )
        .expect("install Serial partition failure");
    let failed_circle_id = circle_id.clone();
    let failed = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'serial-moving-account'",
                (&failed_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await;
    assert!(
        failed.is_err(),
        "injected Serial journal failure must surface"
    );
    let after_failure = handle
        .sql_read(|conn| {
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'serial-moving-account'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )?;
            let child_count = conn.query_row(
                "SELECT count(*) FROM transactions WHERE id = 'serial-moving-child'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                row.get::<_, i64>(0)
            })?;
            let partitions =
                conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            let routing_tables = conn.query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('_coven_audience', '_coven_row_routes')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((audience, child_count, writes, partitions, routing_tables))
        })
        .await
        .expect("read rolled-back Serial move");
    assert_eq!(after_failure.0, before_failure.0);
    assert_eq!(after_failure.1, 1);
    assert_eq!(after_failure.2, before_failure.1);
    assert_eq!(after_failure.3, before_failure.2);
    assert_eq!(after_failure.4, 0);
    fault
        .execute_batch("DROP TRIGGER fail_serial_partition_insert;")
        .expect("remove Serial partition failure");
    drop(fault);

    let destination_circle_id = circle_id.clone();
    let to_circle = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'serial-moving-account'",
                (&destination_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .expect("move Serial Store subtree to Circle");
    let to_circle_write_id = to_circle.write_id.to_string();
    let to_circle_partitions = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([to_circle_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(partitions)
        })
        .await
        .expect("read Serial Store-to-Circle partitions");
    assert_eq!(to_circle_partitions.len(), 2);
    let to_circle_partition = |audience: &str| {
        to_circle_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Serial Store-to-Circle partition"))
    };
    let store_source =
        coven_core::changeset::walk(&to_circle_partition("store").1).expect("walk Store source");
    let circle_destination = coven_core::changeset::walk(&to_circle_partition(&circle_id).1)
        .expect("walk Circle destination");
    for (table, row_id) in [
        ("accounts", "serial-moving-account"),
        ("transactions", "serial-moving-child"),
    ] {
        assert!(has_change(
            &store_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            row_id
        ));
        assert!(has_change(
            &circle_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            row_id
        ));
    }

    let to_local = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = 'local', _updated_at = ?1
                 WHERE id = 'serial-moving-account'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move Serial Circle subtree to Local");
    let to_local_write_id = to_local.write_id.to_string();
    let (to_local_partitions, local_audience) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([to_local_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'serial-moving-account'",
                [],
                |row| row.get::<_, String>(0),
            )?;
            Ok((partitions, audience))
        })
        .await
        .expect("read Serial Circle-to-Local transition");
    assert_eq!(local_audience, "local");
    assert_eq!(to_local_partitions.len(), 2);
    let to_local_partition = |audience: &str| {
        to_local_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Serial Circle-to-Local partition"))
    };
    let circle_source =
        coven_core::changeset::walk(&to_local_partition(&circle_id).1).expect("walk Circle source");
    let local_destination = coven_core::changeset::walk(&to_local_partition("local").1)
        .expect("walk Local destination");
    for (table, row_id) in [
        ("accounts", "serial-moving-account"),
        ("transactions", "serial-moving-child"),
    ] {
        assert!(has_change(
            &circle_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            row_id
        ));
        assert!(has_change(
            &local_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            row_id
        ));
    }

    let to_store = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = NULL, _updated_at = ?1
                 WHERE id = 'serial-moving-account'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move Serial Local subtree to Store");
    let to_store_write_id = to_store.write_id.to_string();
    let (to_store_partitions, store_audience) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([to_store_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let audience = conn.query_row(
                "SELECT audience FROM accounts WHERE id = 'serial-moving-account'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )?;
            Ok((partitions, audience))
        })
        .await
        .expect("read Serial Local-to-Store transition");
    assert_eq!(store_audience, None);
    assert_eq!(to_store_partitions.len(), 2);
    let to_store_partition = |audience: &str| {
        to_store_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Serial Local-to-Store partition"))
    };
    let local_source =
        coven_core::changeset::walk(&to_store_partition("local").1).expect("walk Local source");
    let store_destination = coven_core::changeset::walk(&to_store_partition("store").1)
        .expect("walk Store destination");
    for (table, row_id) in [
        ("accounts", "serial-moving-account"),
        ("transactions", "serial-moving-child"),
    ] {
        assert!(has_change(
            &store_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            row_id
        ));
        assert!(has_change(
            &local_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            row_id
        ));
    }

    let deleted = handle
        .sql(|sql| {
            sql.execute(
                "DELETE FROM accounts WHERE id = 'serial-deleted-account'",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("delete Serial Circle subtree");
    let delete_write_id = deleted.write_id.to_string();
    let (delete_partitions, deleted_rows, routing_tables) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([delete_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let deleted_rows = conn.query_row(
                "SELECT
                    (SELECT count(*) FROM accounts WHERE id = 'serial-deleted-account') +
                    (SELECT count(*) FROM transactions WHERE id = 'serial-deleted-child')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let routing_tables = conn.query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('_coven_audience', '_coven_row_routes')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, deleted_rows, routing_tables))
        })
        .await
        .expect("read Serial Circle delete");
    assert_eq!(delete_partitions.len(), 1);
    assert_eq!(delete_partitions[0].0, circle_id);
    let circle_delete =
        coven_core::changeset::walk(&delete_partitions[0].1).expect("walk Serial Circle delete");
    for (table, row_id) in [
        ("accounts", "serial-deleted-account"),
        ("transactions", "serial-deleted-child"),
    ] {
        assert!(has_change(
            &circle_delete,
            table,
            coven_core::changeset::ChangeOp::Delete,
            row_id
        ));
    }
    assert_eq!((deleted_rows, routing_tables), (0, 0));
}

#[tokio::test]
async fn scoped_move_does_not_cross_a_store_parent_into_a_sibling_scoped_root() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        "scoped-sibling-isolation".to_string(),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped sibling isolation".to_string(),
    );
    let handle = Coven::builder(config)
        .key_custody(KeyCustody::InMemory(routing_keyring()))
        .write_policy(WritePolicy::MergeConcurrent)
        .synced_tables(vec![
            SyncedTable::new("folders", RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
        ])
        .migrations(vec![Migration::sql(
            1,
            "scoped siblings",
            "CREATE TABLE folders (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                folder_id TEXT NOT NULL REFERENCES folders(id),
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE documents (
                id TEXT PRIMARY KEY,
                folder_id TEXT NOT NULL REFERENCES folders(id),
                title TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )])
        .open()
        .expect("open scoped sibling Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) =
        seed_active_circle(&authority, CIRCLE_LABEL, WritePolicy::MergeConcurrent);
    drop(authority);

    handle
        .sql(|sql| {
            sql.execute(
                "INSERT INTO folders (id, name, _updated_at)
                 VALUES ('shared-folder', 'Shared', ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO accounts (id, folder_id, name, audience, _updated_at)
                 VALUES ('moved-account', 'shared-folder', 'Moved', NULL, ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO documents (id, folder_id, title, audience, _updated_at)
                 VALUES ('store-document', 'shared-folder', 'Unrelated', NULL, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("seed scoped siblings");

    let moved_circle_id = circle_id.clone();
    let moved = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'moved-account'",
                (&moved_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .expect("move one scoped root");
    let write_id = moved.write_id.to_string();
    let partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read scoped sibling move partitions");

    for (audience, changeset) in partitions {
        let changes = coven_core::changeset::walk(&changeset)
            .unwrap_or_else(|error| panic!("walk {audience} partition: {error}"));
        assert!(
            !changes.iter().any(|change| {
                change.table == "documents" && change.pk() == Some("store-document")
            }),
            "moving accounts must not publish a sibling documents row to {audience}"
        );
        assert!(
            !changes
                .iter()
                .any(|change| change.table == "folders" && change.pk() == Some("shared-folder")),
            "moving accounts must not re-emit its Store parent to {audience}"
        );
    }
}

async fn assert_every_outgoing_synced_fk_matches_its_child_audience(policy: WritePolicy) {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        format!("{policy:?}-cross-audience-fk"),
        "capture-device".to_string(),
        store_dir.clone(),
        "Cross-audience relationship".to_string(),
    );
    let mut builder = Coven::builder(config)
        .write_policy(policy)
        .synced_tables(vec![
            SyncedTable::new("homes", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("targets", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("links", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "cross-audience relationship",
            "CREATE TABLE homes (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE targets (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE links (
                id TEXT PRIMARY KEY,
                home_id TEXT NOT NULL REFERENCES homes(id),
                target_id TEXT NOT NULL REFERENCES targets(id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )]);
    if policy == WritePolicy::MergeConcurrent {
        builder = builder.key_custody(KeyCustody::InMemory(routing_keyring()));
    }
    let handle = builder.open().expect("open cross-audience Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = seed_active_circle(&authority, CIRCLE_LABEL, policy);
    let (other_circle_id, _other_control) = seed_active_circle(&authority, "circle-b", policy);
    drop(authority);

    let seeded_circle_id = circle_id.clone();
    let seeded_other_circle_id = other_circle_id.clone();
    handle
        .sql(move |sql| {
            for (id, audience) in [
                ("circle-a-home", Some(seeded_circle_id.as_str())),
                ("store-home", None),
                ("local-home", Some("local")),
            ] {
                sql.execute(
                    "INSERT INTO homes (id, audience, _updated_at) VALUES (?1, ?2, ?3)",
                    (id, audience, sql.stamp()),
                )?;
            }
            for (id, audience) in [
                ("circle-a-target", Some(seeded_circle_id.as_str())),
                ("circle-b-target", Some(seeded_other_circle_id.as_str())),
                ("store-target", None),
            ] {
                sql.execute(
                    "INSERT INTO targets (id, audience, _updated_at) VALUES (?1, ?2, ?3)",
                    (id, audience, sql.stamp()),
                )?;
            }
            Ok(())
        })
        .await
        .expect("seed Store, Circle, and Local relationship parents");

    let allowed = handle
        .sql(|sql| {
            sql.execute(
                "INSERT INTO links (id, home_id, target_id, _updated_at)
                 VALUES ('same-circle-link', 'circle-a-home', 'circle-a-target', ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO links (id, home_id, target_id, _updated_at)
                 VALUES ('store-parent-link', 'circle-a-home', 'store-target', ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("same-audience and Store-parent relationships must succeed");
    let allowed_write_id = allowed.write_id.to_string();
    let allowed_state = handle
        .sql_read(move |conn| {
            let host_rows = conn.query_row(
                "SELECT count(*) FROM links
                 WHERE id IN ('same-circle-link', 'store-parent-link')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let audiences = conn
                .prepare(
                    "SELECT audience FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([allowed_write_id], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((host_rows, audiences))
        })
        .await
        .expect("read allowed relationships");
    let expected_audiences = match policy {
        WritePolicy::MergeConcurrent => vec![circle_id.clone(), "store".to_string()],
        WritePolicy::Serial => vec![circle_id],
    };
    assert_eq!(allowed_state, (2, expected_audiences));

    for (id, home, target, description) in [
        (
            "circle-cross-link",
            "circle-a-home",
            "circle-b-target",
            "Circle A child to Circle B parent",
        ),
        (
            "store-private-link",
            "store-home",
            "circle-a-target",
            "Store child to private parent",
        ),
        (
            "local-private-link",
            "local-home",
            "circle-a-target",
            "Local child to another private audience",
        ),
    ] {
        let before = handle
            .sql_read(move |conn| {
                let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                let partitions =
                    conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                        row.get::<_, i64>(0)
                    })?;
                let (routes, mirror) = if policy == WritePolicy::MergeConcurrent {
                    (
                        conn.query_row("SELECT count(*) FROM _coven_row_routes", [], |row| {
                            row.get::<_, i64>(0)
                        })?,
                        conn.query_row("SELECT count(*) FROM _coven_audience", [], |row| {
                            row.get::<_, i64>(0)
                        })?,
                    )
                } else {
                    (0, 0)
                };
                Ok((writes, partitions, routes, mirror))
            })
            .await
            .expect("read state before rejected relationship");

        let attempted_id = id.to_string();
        let attempted_home = home.to_string();
        let attempted_target = target.to_string();
        let error = handle
            .sql(move |sql| {
                sql.execute(
                    "INSERT INTO links (id, home_id, target_id, _updated_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    (attempted_id, attempted_home, attempted_target, sql.stamp()),
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("relationship through target_id"),
            "{description} surfaced the wrong error: {error}"
        );

        let rejected_id = id.to_string();
        let after = handle
            .sql_read(move |conn| {
                let host_rows = conn.query_row(
                    "SELECT count(*) FROM links WHERE id = ?1",
                    [rejected_id],
                    |row| row.get::<_, i64>(0),
                )?;
                let writes = conn.query_row("SELECT count(*) FROM store_writes", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                let partitions =
                    conn.query_row("SELECT count(*) FROM store_write_partitions", [], |row| {
                        row.get::<_, i64>(0)
                    })?;
                let (routes, mirror) = if policy == WritePolicy::MergeConcurrent {
                    (
                        conn.query_row("SELECT count(*) FROM _coven_row_routes", [], |row| {
                            row.get::<_, i64>(0)
                        })?,
                        conn.query_row("SELECT count(*) FROM _coven_audience", [], |row| {
                            row.get::<_, i64>(0)
                        })?,
                    )
                } else {
                    (0, 0)
                };
                Ok((host_rows, writes, partitions, routes, mirror))
            })
            .await
            .expect("read state after rejected relationship");
        assert_eq!(
            after,
            (0, before.0, before.1, before.2, before.3),
            "{description} left durable state"
        );
    }
}

#[tokio::test]
async fn merge_validates_every_outgoing_synced_fk_audience() {
    assert_every_outgoing_synced_fk_matches_its_child_audience(WritePolicy::MergeConcurrent).await;
}

#[tokio::test]
async fn serial_validates_every_outgoing_synced_fk_audience() {
    assert_every_outgoing_synced_fk_matches_its_child_audience(WritePolicy::Serial).await;
}

async fn assert_inherited_reparenting_materializes_subtree(policy: WritePolicy) {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        format!("{policy:?}-inherited-reparent"),
        "capture-device".to_string(),
        store_dir.clone(),
        "Inherited reparent".to_string(),
    );
    let mut builder = Coven::builder(config)
        .write_policy(policy)
        .synced_tables(vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("requirements", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey),
            SyncedTable::new("line_items", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "inherited reparent",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE requirements (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL REFERENCES accounts(id),
                requirement_id TEXT NOT NULL REFERENCES requirements(id),
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE line_items (
                id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )]);
    if policy == WritePolicy::MergeConcurrent {
        builder = builder.key_custody(KeyCustody::InMemory(routing_keyring()));
    }
    let handle = builder.open().expect("open inherited reparent Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = seed_active_circle(&authority, CIRCLE_LABEL, policy);
    let (circle_b, _circle_b_control) = seed_active_circle(&authority, "circle-b", policy);
    drop(authority);

    let seeded_circle_id = circle_id.clone();
    let seeded_circle_b = circle_b.clone();
    handle
        .sql(move |sql| {
            for (id, audience) in [
                ("store-account", None),
                ("circle-a-account", Some(seeded_circle_id.as_str())),
                ("circle-b-account", Some(seeded_circle_b.as_str())),
                ("local-account", Some("local")),
            ] {
                sql.execute(
                    "INSERT INTO accounts (id, audience, _updated_at) VALUES (?1, ?2, ?3)",
                    (id, audience, sql.stamp()),
                )?;
            }
            for (id, audience) in [
                ("store-requirement", None),
                ("circle-a-requirement", Some(seeded_circle_id.as_str())),
                ("circle-b-requirement", Some(seeded_circle_b.as_str())),
                ("local-requirement", Some("local")),
            ] {
                sql.execute(
                    "INSERT INTO requirements (id, audience, _updated_at) VALUES (?1, ?2, ?3)",
                    (id, audience, sql.stamp()),
                )?;
            }
            for (transaction, account, requirement) in [
                (
                    "to-circle-transaction",
                    "store-account",
                    "store-requirement",
                ),
                (
                    "to-local-transaction",
                    "circle-a-account",
                    "circle-a-requirement",
                ),
                (
                    "to-store-transaction",
                    "circle-a-account",
                    "circle-a-requirement",
                ),
                (
                    "invalid-target-transaction",
                    "circle-a-account",
                    "circle-a-requirement",
                ),
                (
                    "journal-failure-transaction",
                    "store-account",
                    "store-requirement",
                ),
            ] {
                sql.execute(
                    "INSERT INTO transactions
                     (id, account_id, requirement_id, _updated_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    (transaction, account, requirement, sql.stamp()),
                )?;
                sql.execute(
                    "INSERT INTO line_items (id, transaction_id, _updated_at)
                     VALUES (?1, ?2, ?3)",
                    (format!("{transaction}-line"), transaction, sql.stamp()),
                )?;
            }
            Ok(())
        })
        .await
        .expect("seed inherited reparenting cases");

    let before_routes = handle
        .sql_read(move |conn| {
            if policy == WritePolicy::Serial {
                return Ok(Vec::new());
            }
            conn.prepare(
                "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                 WHERE table_name IN ('transactions', 'line_items')
                 ORDER BY table_name, row_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read routes before reparent");

    let moved = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE transactions SET account_id = 'circle-b-account', _updated_at = ?1
                 WHERE id = 'to-circle-transaction'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("reparent Store child to Circle B");
    let write_id = moved.write_id.to_string();
    let (partitions, after_routes, routing_tables) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let after_routes = if policy == WritePolicy::MergeConcurrent {
                conn.prepare(
                    "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                     WHERE table_name IN ('transactions', 'line_items')
                     ORDER BY table_name, row_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            let routing_tables = conn.query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('_coven_audience', '_coven_row_routes')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, after_routes, routing_tables))
        })
        .await
        .expect("read inherited reparent result");

    let partition = |audience: &str| {
        partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} reparent partition"))
    };
    let store = coven_core::changeset::walk(&partition("store").1).expect("walk Store retract");
    let circle = coven_core::changeset::walk(&partition(&circle_b).1).expect("walk Circle insert");
    for (table, id) in [
        ("transactions", "to-circle-transaction"),
        ("line_items", "to-circle-transaction-line"),
    ] {
        assert!(has_change(
            &store,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
        assert!(has_change(
            &circle,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }

    let to_local = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE transactions
                 SET account_id = 'local-account',
                     requirement_id = 'local-requirement',
                     _updated_at = ?1
                 WHERE id = 'to-local-transaction'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("reparent Circle A child to Local with a Local requirement");
    let local_write_id = to_local.write_id.to_string();
    let (local_partitions, local_state) = handle
        .sql_read(move |conn| {
            let partitions = conn
                .prepare(
                    "SELECT audience, changeset FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
                )?
                .query_map([local_write_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let state = conn.query_row(
                "SELECT account_id, requirement_id,
                        (SELECT count(*) FROM line_items
                         WHERE transaction_id = 'to-local-transaction')
                 FROM transactions WHERE id = 'to-local-transaction'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )?;
            Ok((partitions, state))
        })
        .await
        .expect("read Circle-to-Local reparent");
    assert_eq!(
        local_state,
        (
            "local-account".to_string(),
            "local-requirement".to_string(),
            1
        )
    );
    let local_source = local_partitions
        .iter()
        .find(|(audience, _)| audience == &circle_id)
        .expect("Circle A Local-move source");
    let local_source =
        coven_core::changeset::walk(&local_source.1).expect("walk Circle-to-Local source");
    for (table, id) in [
        ("transactions", "to-local-transaction"),
        ("line_items", "to-local-transaction-line"),
    ] {
        assert!(has_change(
            &local_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }
    let local_destination = local_partitions
        .iter()
        .find(|(audience, _)| audience == "local")
        .expect("Local reparent destination");
    let local_destination =
        coven_core::changeset::walk(&local_destination.1).expect("walk Local reparent destination");
    for (table, id) in [
        ("transactions", "to-local-transaction"),
        ("line_items", "to-local-transaction-line"),
    ] {
        assert!(has_change(
            &local_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }

    let to_store = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE transactions
                 SET account_id = 'store-account',
                     requirement_id = 'store-requirement',
                     _updated_at = ?1
                 WHERE id = 'to-store-transaction'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("reparent Circle A child to Store");
    let store_write_id = to_store.write_id.to_string();
    let store_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([store_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Circle-to-Store reparent");
    let store_partition = |audience: &str| {
        store_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Store partition"))
    };
    let circle_source = coven_core::changeset::walk(&store_partition(&circle_id).1)
        .expect("walk Circle-to-Store source");
    let store_destination = coven_core::changeset::walk(&store_partition("store").1)
        .expect("walk Circle-to-Store destination");
    for (table, id) in [
        ("transactions", "to-store-transaction"),
        ("line_items", "to-store-transaction-line"),
    ] {
        assert!(has_change(
            &circle_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
        assert!(has_change(
            &store_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }

    let invalid_before = handle
        .sql_read(move |conn| {
            Ok(reparent_rollback_state(
                conn,
                "invalid-target-transaction",
                policy,
            )?)
        })
        .await
        .expect("read state before invalid reparent");
    let invalid_error = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE transactions
                 SET account_id = 'circle-b-account', _updated_at = ?1
                 WHERE id = 'invalid-target-transaction'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect_err("Circle B child must not retain its Circle A requirement");
    assert!(
        invalid_error
            .to_string()
            .contains("relationship through requirement_id"),
        "invalid reparent surfaced the wrong error: {invalid_error}"
    );
    let invalid_after = handle
        .sql_read(move |conn| {
            Ok(reparent_rollback_state(
                conn,
                "invalid-target-transaction",
                policy,
            )?)
        })
        .await
        .expect("read state after invalid reparent");
    assert_eq!(invalid_after, invalid_before);

    let journal_before = handle
        .sql_read(move |conn| {
            Ok(reparent_rollback_state(
                conn,
                "journal-failure-transaction",
                policy,
            )?)
        })
        .await
        .expect("read state before journal failure");
    let fault = rusqlite::Connection::open(store_dir.db_path()).expect("open fault injector");
    fault
        .execute_batch(
            "CREATE TRIGGER fail_inherited_reparent_partition
             BEFORE INSERT ON store_write_partitions
             BEGIN
                 SELECT RAISE(ABORT, 'forced inherited reparent journal failure');
             END;",
        )
        .expect("install inherited reparent journal failure");
    let journal_error = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE transactions
                 SET account_id = 'circle-b-account', _updated_at = ?1
                 WHERE id = 'journal-failure-transaction'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect_err("journal failure must abort inherited reparent");
    assert!(journal_error
        .to_string()
        .contains("forced inherited reparent journal failure"));
    let journal_after = handle
        .sql_read(move |conn| {
            Ok(reparent_rollback_state(
                conn,
                "journal-failure-transaction",
                policy,
            )?)
        })
        .await
        .expect("read state after journal failure");
    assert_eq!(journal_after, journal_before);
    fault
        .execute_batch("DROP TRIGGER fail_inherited_reparent_partition;")
        .expect("remove inherited reparent journal failure");
    drop(fault);

    let final_routes = handle
        .sql_read(move |conn| {
            if policy == WritePolicy::Serial {
                return Ok((Vec::new(), 0));
            }
            let routes = conn
                .prepare(
                    "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                     WHERE table_name IN ('transactions', 'line_items')
                     ORDER BY table_name, row_id",
                )?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let routing_tables = conn.query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'table'
                   AND name IN ('_coven_audience', '_coven_row_routes')",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((routes, routing_tables))
        })
        .await
        .expect("read routes after inherited reparent matrix");
    match policy {
        WritePolicy::MergeConcurrent => {
            assert_eq!(after_routes, before_routes);
            assert_eq!(final_routes.0, before_routes);
        }
        WritePolicy::Serial => {
            assert_eq!(routing_tables, 0);
            assert_eq!(final_routes.1, 0);
        }
    }
}

#[tokio::test]
async fn merge_reparenting_an_inherited_row_materializes_its_subtree() {
    assert_inherited_reparenting_materializes_subtree(WritePolicy::MergeConcurrent).await;
}

#[tokio::test]
async fn serial_reparenting_an_inherited_row_materializes_its_subtree() {
    assert_inherited_reparenting_materializes_subtree(WritePolicy::Serial).await;
}

async fn assert_non_local_scoped_descendant_keeps_store_ancestor(policy: WritePolicy) {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new(temp.path());
    let config = Config::with_defaults(
        format!("{policy:?}-scoped-ancestor"),
        "capture-device".to_string(),
        store_dir.clone(),
        "Scoped descendant ancestor".to_string(),
    );
    let mut builder = Coven::builder(config)
        .write_policy(policy)
        .synced_tables(vec![
            SyncedTable::new("folders", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("details", RowIdentity::SharedKey),
        ])
        .migrations(vec![Migration::sql(
            1,
            "scoped descendant ancestor",
            "CREATE TABLE folders (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE documents (
                id TEXT PRIMARY KEY,
                folder_id TEXT NOT NULL REFERENCES folders(id),
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE details (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )]);
    if policy == WritePolicy::MergeConcurrent {
        builder = builder.key_custody(KeyCustody::InMemory(routing_keyring()));
    }
    let handle = builder.open().expect("open scoped ancestor Store");

    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = seed_active_circle(&authority, CIRCLE_LABEL, policy);
    let (circle_b, _circle_b_control) = seed_active_circle(&authority, "circle-b", policy);
    drop(authority);

    let seeded = handle
        .sql(|sql| {
            sql.execute(
                "INSERT INTO folders (id, name, _updated_at)
                 VALUES ('required-folder', 'Required', ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO documents (id, folder_id, audience, _updated_at)
                 VALUES ('moving-document', 'required-folder', 'local', ?1)",
                [sql.stamp()],
            )?;
            sql.execute(
                "INSERT INTO details (id, document_id, _updated_at)
                 VALUES ('moving-detail', 'moving-document', ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("seed Local-only ancestor subtree");
    let seed_write_id = seeded.write_id.to_string();
    let local_only_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1",
            )?
            .query_map([seed_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Local-only ancestor journal");
    assert_eq!(local_only_partitions.len(), 1);
    assert_eq!(local_only_partitions[0].0, "local");
    let local_seed =
        coven_core::changeset::walk(&local_only_partitions[0].1).expect("walk Local-only journal");
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &local_seed,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }

    let destination_circle_id = circle_id.clone();
    let moved = handle
        .sql(move |sql| {
            sql.execute(
                "UPDATE documents SET audience = ?1, _updated_at = ?2
                 WHERE id = 'moving-document'",
                (&destination_circle_id, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .expect("move Local descendant into Circle A");
    let write_id = moved.write_id.to_string();
    let partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Local-to-Circle ancestor move");
    let partition = |audience: &str| {
        partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} scoped ancestor partition"))
    };
    let store = coven_core::changeset::walk(&partition("store").1)
        .expect("walk required ancestor Store partition");
    assert!(has_change(
        &store,
        "folders",
        coven_core::changeset::ChangeOp::Insert,
        "required-folder"
    ));
    let circle = coven_core::changeset::walk(&partition(&circle_id).1)
        .expect("walk Circle descendant partition");
    let local = coven_core::changeset::walk(&partition("local").1)
        .expect("walk Local descendant source partition");
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &circle,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
        assert!(has_change(
            &local,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }

    let sibling_circle = circle_b.clone();
    let inserted_sibling = handle
        .sql(move |sql| {
            sql.execute(
                "INSERT INTO documents (id, folder_id, audience, _updated_at)
                 VALUES ('sibling-document', 'required-folder', ?1, ?2)",
                (sibling_circle, sql.stamp()),
            )?;
            sql.execute(
                "INSERT INTO details (id, document_id, _updated_at)
                 VALUES ('sibling-detail', 'sibling-document', ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("insert Circle B sibling under required ancestor");
    let sibling_write_id = inserted_sibling.write_id.to_string();
    let sibling_circle = circle_b.clone();
    let sibling_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([sibling_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Circle B sibling insert");
    let sibling = sibling_partitions
        .iter()
        .find(|(audience, _)| audience == &sibling_circle)
        .expect("Circle B sibling partition");
    let sibling = coven_core::changeset::walk(&sibling.1).expect("walk Circle B sibling insert");
    for (table, id) in [
        ("documents", "sibling-document"),
        ("details", "sibling-detail"),
    ] {
        assert!(has_change(
            &sibling,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }
    let sibling_store = sibling_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store ancestor partition for Circle B insert");
    let sibling_store = coven_core::changeset::walk(&sibling_store.1)
        .expect("walk Store ancestor partition for Circle B insert");
    assert!(has_change(
        &sibling_store,
        "folders",
        coven_core::changeset::ChangeOp::Insert,
        "required-folder"
    ));
    for (_, changeset) in &sibling_partitions {
        let changes = coven_core::changeset::walk(changeset).expect("walk sibling partition");
        for (table, id) in [
            ("documents", "moving-document"),
            ("details", "moving-detail"),
        ] {
            assert!(!changes.iter().any(|change| change.table == table
                && change
                    .columns
                    .iter()
                    .any(|value| value.as_deref() == Some(id))));
        }
    }

    let moved_local = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE documents SET audience = 'local', _updated_at = ?1
                 WHERE id = 'moving-document'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move Circle A descendant to Local while Circle B remains");
    let local_write_id = moved_local.write_id.to_string();
    let local_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([local_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Circle A to Local move");
    let circle_source = local_partitions
        .iter()
        .find(|(audience, _)| audience == &circle_id)
        .expect("Circle A source partition");
    let circle_source =
        coven_core::changeset::walk(&circle_source.1).expect("walk Circle A source partition");
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &circle_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }
    let local_destination = local_partitions
        .iter()
        .find(|(audience, _)| audience == "local")
        .expect("Local descendant destination partition");
    let local_destination = coven_core::changeset::walk(&local_destination.1)
        .expect("walk Local descendant destination partition");
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &local_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }
    for (_, changeset) in &local_partitions {
        let changes =
            coven_core::changeset::walk(changeset).expect("walk Circle-to-Local partition");
        assert!(!has_change(
            &changes,
            "folders",
            coven_core::changeset::ChangeOp::Delete,
            "required-folder"
        ));
        for id in ["sibling-document", "sibling-detail"] {
            assert!(!changes.iter().any(|change| change
                .columns
                .iter()
                .any(|value| value.as_deref() == Some(id))));
        }
    }

    let rollback_before = handle
        .sql_read(move |conn| Ok(scoped_ancestor_rollback_state(conn, policy)?))
        .await
        .expect("read state before ancestor retraction failure");
    let fault = rusqlite::Connection::open(store_dir.db_path()).expect("open fault injector");
    fault
        .execute_batch(
            "CREATE TRIGGER fail_scoped_ancestor_partition
             BEFORE INSERT ON store_write_partitions
             BEGIN
                 SELECT RAISE(ABORT, 'forced scoped ancestor journal failure');
             END;",
        )
        .expect("install scoped ancestor journal failure");
    let failed_move = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE documents SET audience = 'local', _updated_at = ?1
                 WHERE id = 'sibling-document'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect_err("journal failure must abort final non-Local descendant move");
    assert!(failed_move
        .to_string()
        .contains("forced scoped ancestor journal failure"));
    let rollback_after = handle
        .sql_read(move |conn| Ok(scoped_ancestor_rollback_state(conn, policy)?))
        .await
        .expect("read state after ancestor retraction failure");
    assert_eq!(rollback_after, rollback_before);
    fault
        .execute_batch("DROP TRIGGER fail_scoped_ancestor_partition;")
        .expect("remove scoped ancestor journal failure");
    drop(fault);

    let moved_sibling_local = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE documents SET audience = 'local', _updated_at = ?1
                 WHERE id = 'sibling-document'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move final Circle descendant to Local");
    let sibling_local_write_id = moved_sibling_local.write_id.to_string();
    let sibling_local_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([sibling_local_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read final Circle descendant move to Local");
    let store_retraction = sibling_local_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store ancestor retraction partition");
    let store_retraction =
        coven_core::changeset::walk(&store_retraction.1).expect("walk Store ancestor retraction");
    assert!(has_change(
        &store_retraction,
        "folders",
        coven_core::changeset::ChangeOp::Delete,
        "required-folder"
    ));
    let circle_b_source = sibling_local_partitions
        .iter()
        .find(|(audience, _)| audience == &circle_b)
        .expect("Circle B source partition");
    let circle_b_source =
        coven_core::changeset::walk(&circle_b_source.1).expect("walk Circle B source partition");
    for (table, id) in [
        ("documents", "sibling-document"),
        ("details", "sibling-detail"),
    ] {
        assert!(has_change(
            &circle_b_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }
    let local_destination = sibling_local_partitions
        .iter()
        .find(|(audience, _)| audience == "local")
        .expect("final Local descendant destination partition");
    let local_destination = coven_core::changeset::walk(&local_destination.1)
        .expect("walk final Local descendant destination partition");
    for (table, id) in [
        ("documents", "sibling-document"),
        ("details", "sibling-detail"),
    ] {
        assert!(has_change(
            &local_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }

    let moved_store = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE documents SET audience = NULL, _updated_at = ?1
                 WHERE id = 'moving-document'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("move selected Local descendant to Store");
    let store_write_id = moved_store.write_id.to_string();
    let store_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([store_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read Local descendant move to Store");
    let store_destination = store_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store descendant destination partition");
    let local_source = store_partitions
        .iter()
        .find(|(audience, _)| audience == "local")
        .expect("Local descendant source partition");
    let local_source = coven_core::changeset::walk(&local_source.1)
        .expect("walk Local descendant source partition");
    let store_destination = coven_core::changeset::walk(&store_destination.1)
        .expect("walk Store descendant destination");
    for (table, id) in [
        ("folders", "required-folder"),
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &store_destination,
            table,
            coven_core::changeset::ChangeOp::Insert,
            id
        ));
    }
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &local_source,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }

    let deleted_store = handle
        .sql(|sql| {
            sql.execute("DELETE FROM documents WHERE id = 'moving-document'", [])?;
            Ok(())
        })
        .await
        .expect("delete final Store descendant");
    let delete_write_id = deleted_store.write_id.to_string();
    let delete_partitions = handle
        .sql_read(move |conn| {
            conn.prepare(
                "SELECT audience, changeset FROM store_write_partitions
                 WHERE write_id = ?1 ORDER BY audience",
            )?
            .query_map([delete_write_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
        })
        .await
        .expect("read final Store descendant delete");
    let store_delete = delete_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store descendant delete partition");
    let store_delete =
        coven_core::changeset::walk(&store_delete.1).expect("walk Store descendant delete");
    for (table, id) in [
        ("folders", "required-folder"),
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &store_delete,
            table,
            coven_core::changeset::ChangeOp::Delete,
            id
        ));
    }
}

#[tokio::test]
async fn merge_non_local_scoped_descendant_keeps_store_ancestor() {
    assert_non_local_scoped_descendant_keeps_store_ancestor(WritePolicy::MergeConcurrent).await;
}

#[tokio::test]
async fn serial_non_local_scoped_descendant_keeps_store_ancestor() {
    assert_non_local_scoped_descendant_keeps_store_ancestor(WritePolicy::Serial).await;
}

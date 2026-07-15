use coven::{
    Config, Coven, KeyCustody, MasterKeyring, Migration, RowIdentity, StoreDir, SyncedTable,
    WritePolicy,
};

const CIRCLE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaa";
const STORE_ACCOUNT_ROUTE: &str =
    "be5c8ac272e36dd2427439c7a63010f5ca71e44ab35231f81645e150e8de2ace";
const CIRCLE_ACCOUNT_ROUTE: &str =
    "56163029bd37372a5bc08a2b6170a3db28acd412f49046bc00305c92b9fc3749";
const LOCAL_ACCOUNT_ROUTE: &str =
    "efeaad310c9239967251a9552be25c7cf9ad7c15717559e7b6b32141ead862ee";
const STORE_TRANSACTION_ROUTE: &str =
    "a953ac0a8a207ef79295b2c55d4681ca2c62e2b42bcfce648a3be37b3739127c";
const LOCAL_MOVE_ACCOUNT_ROUTE: &str =
    "1ecfca618a3bbe5a756dda7d0add09bac4dd58ca1818e5875b7923d0aaf82060";
const LOCAL_MOVE_TRANSACTION_ROUTE: &str =
    "7433b9419027492b717246bcd5f47d3d8573488ea90f008860220e0f9cd3ef9f";
const STORE_MOVE_ACCOUNT_ROUTE: &str =
    "65c04acbf4869d3975c2b7e208dff6b8fb442eab3f7819e86170b5bb523e52f9";
const STORE_MOVE_TRANSACTION_ROUTE: &str =
    "53718f88cd05d7d0fc28e5bbc9320bc722f31201938ff9075b0e1dc0d649b3af";
const DELETED_ACCOUNT_ROUTE: &str =
    "792998009887b7ee0399d3bbc094a3bcd85a186cf77d99d368f822159cd337fe";
const DELETED_TRANSACTION_ROUTE: &str =
    "2757753305d0f66bc5d2a55a5d230477596d6d045ede5cb8a5e7e81dd06932e7";

fn routing_keyring() -> MasterKeyring {
    coven_core::encryption::EncryptionService::from_key([7; 32]).into()
}

fn seed_store_root(conn: &rusqlite::Connection) {
    let store_root = coven_core::sync::store_commit::ObjectHash::digest(b"scoped-routing-root");
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES ('store_root_hash', ?1)",
        [store_root.to_string()],
    )
    .expect("seed Store protocol root");
}

fn circle_control_coord(policy: WritePolicy) -> String {
    match policy {
        WritePolicy::MergeConcurrent => serde_json::json!({
            "merge_concurrent": {
                "device_id": "control-device",
                "author_pubkey": "owner-pubkey",
                "author_owner_grant": "11".repeat(32),
                "seq": 1,
                "control_hash": "22".repeat(32)
            }
        }),
        WritePolicy::Serial => serde_json::json!({
            "serial": {
                "author_pubkey": "owner-pubkey",
                "generation": 1,
                "control_hash": "44".repeat(32)
            }
        }),
    }
    .to_string()
}

fn seed_active_circle(conn: &rusqlite::Connection, circle_id: &str, policy: WritePolicy) -> String {
    if policy == WritePolicy::MergeConcurrent {
        seed_store_root(conn);
    }
    let control_coord = circle_control_coord(policy);
    let (stream_id, commit_hash) = match policy {
        WritePolicy::MergeConcurrent => ("control-device", "33".repeat(32)),
        WritePolicy::Serial => ("serial", "55".repeat(32)),
    };
    conn.execute(
        "INSERT INTO circle_control_activations
         (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
         VALUES (?1, ?2, ?3, 1, ?4, X'01')",
        (circle_id, &control_coord, stream_id, commit_hash),
    )
    .expect("seed activated circle control");
    conn.execute(
        "INSERT INTO circle_access_cache
         (circle_id, control_coord, owner_pubkey, disposition, access_bytes)
         VALUES (?1, ?2, 'owner-pubkey', 'active', X'02')",
        (circle_id, &control_coord),
    )
    .expect("seed exact active circle access");
    control_coord
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
    let control_coord = seed_active_circle(&authority, CIRCLE_ID, WritePolicy::MergeConcurrent);
    drop(authority);

    let receipt = handle
        .sql(|sql| {
            for (id, name, audience) in [
                ("store-account", "Store", None),
                ("circle-account", "Circle", Some(CIRCLE_ID)),
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

    assert_eq!(partitions.len(), 2);
    assert!(partitions
        .iter()
        .all(|(audience, _, _)| audience != "local"));
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
    for routing_id in [STORE_ACCOUNT_ROUTE, CIRCLE_ACCOUNT_ROUTE] {
        assert!(store_changes.iter().any(|change| {
            change.table == "_coven_audience"
                && change.op == coven_core::changeset::ChangeOp::Insert
                && change.pk() == Some(routing_id)
        }));
    }

    let circle = partition(CIRCLE_ID);
    assert_eq!(circle.1.as_deref(), Some(control_coord.as_str()));
    assert!(contains_bytes(&circle.2, b"circle-account"));
    assert!(!contains_bytes(&circle.2, b"store-account"));
    assert!(!contains_bytes(&circle.2, b"local-account"));
    let circle_changes = coven_core::changeset::walk(&circle.2).expect("walk Circle partition");
    assert!(circle_changes.iter().any(|change| {
        change.table == "_coven_row_routes"
            && change.op == coven_core::changeset::ChangeOp::Insert
            && change.pk() == Some(CIRCLE_ACCOUNT_ROUTE)
    }));

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
                CIRCLE_ACCOUNT_ROUTE.to_string(),
                "accounts".to_string(),
                "circle-account".to_string(),
            ),
            (
                LOCAL_ACCOUNT_ROUTE.to_string(),
                "accounts".to_string(),
                "local-account".to_string(),
            ),
            (
                STORE_ACCOUNT_ROUTE.to_string(),
                "accounts".to_string(),
                "store-account".to_string(),
            ),
        ]
    );
    assert_eq!(routes.1.len(), 2, "Local rows have no Store mirror");
    assert!(routes.1.contains(&(STORE_ACCOUNT_ROUTE.to_string(), None)));
    assert!(routes.1.contains(&(
        CIRCLE_ACCOUNT_ROUTE.to_string(),
        Some(CIRCLE_ID.to_string())
    )));
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
    seed_active_circle(&authority, CIRCLE_ID, WritePolicy::MergeConcurrent);
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

    let failed = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'store-account'",
                (CIRCLE_ID, sql.stamp()),
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

    let moved = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'store-account'",
                (CIRCLE_ID, sql.stamp()),
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

    assert_eq!(host_audience.as_deref(), Some(CIRCLE_ID));
    assert_eq!(child_count, 1);
    assert_eq!(
        routes,
        vec![
            (
                STORE_ACCOUNT_ROUTE.to_string(),
                "accounts".to_string(),
                "store-account".to_string(),
            ),
            (
                STORE_TRANSACTION_ROUTE.to_string(),
                "transactions".to_string(),
                "store-transaction".to_string(),
            ),
        ]
    );
    assert_eq!(
        mirror,
        vec![
            (
                STORE_TRANSACTION_ROUTE.to_string(),
                Some(CIRCLE_ID.to_string()),
            ),
            (STORE_ACCOUNT_ROUTE.to_string(), Some(CIRCLE_ID.to_string()),),
        ],
        "the root and inherited child mirrors must change with the host move"
    );
    assert_eq!(
        move_partitions.len(),
        2,
        "the committed move must durably contain exactly Store and Circle partitions"
    );
    let circle = move_partitions
        .iter()
        .find(|(audience, _)| audience == CIRCLE_ID)
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
    for routing_id in [STORE_ACCOUNT_ROUTE, STORE_TRANSACTION_ROUTE] {
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
    for routing_id in [STORE_ACCOUNT_ROUTE, STORE_TRANSACTION_ROUTE] {
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

    let inactive_circle = coven_core::sync::circle::CircleId::from_bytes([1; 16]).to_string();
    let mismatched_circle = coven_core::sync::circle::CircleId::from_bytes([2; 16]).to_string();
    let unknown_circle = coven_core::sync::circle::CircleId::from_bytes([3; 16]).to_string();
    let merge_control = circle_control_coord(WritePolicy::MergeConcurrent);
    let serial_control = circle_control_coord(WritePolicy::Serial);
    let authority = rusqlite::Connection::open(store_dir.db_path()).expect("open authority db");
    seed_store_root(&authority);
    for (circle_id, control, disposition, commit_hash) in [
        (
            inactive_circle.as_str(),
            merge_control.as_str(),
            "inactive",
            "33".repeat(32),
        ),
        (
            mismatched_circle.as_str(),
            serial_control.as_str(),
            "active",
            "55".repeat(32),
        ),
    ] {
        authority
            .execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, 'control-device', 1, ?3, X'01')",
                (circle_id, control, commit_hash),
            )
            .expect("seed circle control");
        authority
            .execute(
                "INSERT INTO circle_access_cache
                 (circle_id, control_coord, owner_pubkey, disposition, access_bytes)
                 VALUES (?1, ?2, 'owner-pubkey', ?3, X'02')",
                (circle_id, control, disposition),
            )
            .expect("seed circle access disposition");
    }
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
            inactive_circle,
            "active local access records",
        ),
        (
            "policy-mismatch",
            mismatched_circle,
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
    seed_active_circle(&authority, CIRCLE_ID, WritePolicy::MergeConcurrent);
    drop(authority);

    handle
        .sql(|sql| {
            for (account, transaction) in [
                ("local-move-account", "local-move-transaction"),
                ("store-move-account", "store-move-transaction"),
                ("deleted-account", "deleted-transaction"),
            ] {
                sql.execute(
                    "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, 'Circle', ?2, ?3)",
                    (account, CIRCLE_ID, sql.stamp()),
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
    assert_eq!(after_failure.0, CIRCLE_ID);
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
                "SELECT count(*) FROM _coven_audience
                 WHERE routing_id IN (?1, ?2)",
                [LOCAL_MOVE_ACCOUNT_ROUTE, LOCAL_MOVE_TRANSACTION_ROUTE],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((partitions, store_bytes, routes, mirror_count))
        })
        .await
        .expect("read Circle-to-Local transition");
    assert_eq!(local_partitions.len(), 2);
    assert!(local_partitions
        .iter()
        .all(|(audience, _)| audience != "local"));
    let local_partition = |audience: &str| {
        local_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Local partition"))
    };
    let circle_retract =
        coven_core::changeset::walk(&local_partition(CIRCLE_ID).1).expect("walk Circle retract");
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
    let store_mirror_retract =
        coven_core::changeset::walk(&local_partition("store").1).expect("walk Store mirror");
    for route in [LOCAL_MOVE_ACCOUNT_ROUTE, LOCAL_MOVE_TRANSACTION_ROUTE] {
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
                LOCAL_MOVE_ACCOUNT_ROUTE.to_string(),
                "local-move-account".to_string(),
            ),
            (
                LOCAL_MOVE_TRANSACTION_ROUTE.to_string(),
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
                    "SELECT routing_id, circle_id FROM _coven_audience
                     WHERE routing_id IN (?1, ?2) ORDER BY routing_id",
                )?
                .query_map(
                    [STORE_MOVE_ACCOUNT_ROUTE, STORE_MOVE_TRANSACTION_ROUTE],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )?
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
        coven_core::changeset::walk(&store_partition(CIRCLE_ID).1).expect("walk Circle retract");
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
                STORE_MOVE_ACCOUNT_ROUTE.to_string(),
                "store-move-account".to_string(),
            ),
            (
                STORE_MOVE_TRANSACTION_ROUTE.to_string(),
                "store-move-transaction".to_string(),
            ),
        ]
    );
    assert_eq!(
        store_mirror,
        vec![
            (STORE_MOVE_TRANSACTION_ROUTE.to_string(), None),
            (STORE_MOVE_ACCOUNT_ROUTE.to_string(), None),
        ]
    );

    let deleted = handle
        .sql(|sql| {
            sql.execute("DELETE FROM accounts WHERE id = 'deleted-account'", [])?;
            Ok(())
        })
        .await
        .expect("delete Circle subtree");
    let delete_write_id = deleted.write_id.to_string();
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
                [DELETED_ACCOUNT_ROUTE, DELETED_TRANSACTION_ROUTE],
                |row| row.get::<_, i64>(0),
            )?;
            let mirror_count = conn.query_row(
                "SELECT count(*) FROM _coven_audience WHERE routing_id IN (?1, ?2)",
                [DELETED_ACCOUNT_ROUTE, DELETED_TRANSACTION_ROUTE],
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
        coven_core::changeset::walk(&delete_partition(CIRCLE_ID).1).expect("walk Circle delete");
    for (table, row_id, route_id) in [
        ("accounts", "deleted-account", DELETED_ACCOUNT_ROUTE),
        (
            "transactions",
            "deleted-transaction",
            DELETED_TRANSACTION_ROUTE,
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
    for route in [DELETED_ACCOUNT_ROUTE, DELETED_TRANSACTION_ROUTE] {
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
    seed_active_circle(&authority, CIRCLE_ID, WritePolicy::Serial);
    drop(authority);

    let inserted = handle
        .sql(|sql| {
            for (account, transaction, audience) in [
                ("serial-moving-account", "serial-moving-child", None),
                (
                    "serial-deleted-account",
                    "serial-deleted-child",
                    Some(CIRCLE_ID),
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
    assert_eq!(insert_partitions.len(), 2, "Local has no Serial package");
    assert!(insert_partitions
        .iter()
        .all(|(audience, _)| audience != "local"));

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
    let failed = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'serial-moving-account'",
                (CIRCLE_ID, sql.stamp()),
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

    let to_circle = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'serial-moving-account'",
                (CIRCLE_ID, sql.stamp()),
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
    let circle_destination = coven_core::changeset::walk(&to_circle_partition(CIRCLE_ID).1)
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
    assert_eq!(
        to_local_partitions.len(),
        1,
        "Local has no destination package"
    );
    assert_eq!(to_local_partitions[0].0, CIRCLE_ID);
    let circle_source =
        coven_core::changeset::walk(&to_local_partitions[0].1).expect("walk Circle source");
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
    assert_eq!(to_store_partitions.len(), 1, "Local has no source package");
    assert_eq!(to_store_partitions[0].0, "store");
    let store_destination =
        coven_core::changeset::walk(&to_store_partitions[0].1).expect("walk Store destination");
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
    assert_eq!(delete_partitions[0].0, CIRCLE_ID);
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
    seed_active_circle(&authority, CIRCLE_ID, WritePolicy::MergeConcurrent);
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

    let moved = handle
        .sql(|sql| {
            sql.execute(
                "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'moved-account'",
                (CIRCLE_ID, sql.stamp()),
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

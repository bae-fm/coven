use crate::{
    Database, DbError, HostWriteError, HostWriteOperation, SqlContext, StoreDatabase,
    StoreRowWrites,
};
use crate::{Migration, WriteBatch};

use coven_protocol::synced_schema::RowIdentity;

use coven_foundation::store_dir::StoreDir;

use coven_protocol::synced_schema::SyncedTable;

use coven_protocol::write::WriteReceipt;

const CIRCLE_LABEL: &str = "circle-a";

/// The key a scoped store routes its rows under.
fn routing_encryption() -> coven_keys::encryption::EncryptionService {
    coven_keys::encryption::EncryptionService::from_key([7; 32])
}

/// A scoped store over its own file, and the host-write service that captures
/// into it.
fn scoped_store(
    store_dir: &StoreDir,
    device: &str,
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
) -> (Database, StoreRowWrites) {
    let database = Database::open(
        &store_dir.db_path(),
        tables,
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device.to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
    )
    .expect("open scoped Store");
    let writes = StoreRowWrites::new(StoreDatabase::new(&database));
    (database, writes)
}

/// Run `sql` as one host write under the scoped store's routing key.
async fn capture<R>(
    writes: &StoreRowWrites,
    sql: impl for<'context, 'connection> FnOnce(SqlContext<'context, 'connection>) -> Result<R, DbError>
        + Send
        + 'static,
) -> Result<WriteReceipt<R>, HostWriteError<DbError>>
where
    R: Send + 'static,
{
    writes
        .execute(
            HostWriteOperation::new(WriteBatch::new(), sql),
            Some(routing_encryption()),
            None,
        )
        .await
}

/// The database failure a rejected host write carries.
fn write_failure(error: HostWriteError<DbError>) -> DbError {
    match error {
        HostWriteError::Database(error) | HostWriteError::Host(error) => error,
        other => panic!("expected a database failure, got {other:?}"),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn inspect_database<R>(
    store_dir: &StoreDir,
    operation: impl FnOnce(&crate::DatabaseImageTest) -> rusqlite::Result<R>,
) -> Result<R, crate::DbError> {
    let database = crate::DatabaseImageTest::open(&store_dir.db_path())?;
    operation(&database).map_err(crate::DbError::from)
}

fn payload_bytes(store_dir: &StoreDir, encoded_hash: String) -> Result<Vec<u8>, crate::DbError> {
    let hash = encoded_hash
        .parse()
        .map_err(|error| crate::DbError::context("parse captured payload hash", error))?;
    crate::payload_spool::read_payload_blocking(store_dir, hash).map_err(crate::DbError::from)
}

fn audience_partitions(
    store_dir: &StoreDir,
    write_id: &str,
) -> Result<Vec<crate::test_sql::StoreWritePartitionRow>, crate::DbError> {
    let rows = inspect_database(store_dir, |conn| {
        conn.query(
            "SELECT audience, control_coord, changeset_hash
             FROM store_write_partitions
             WHERE write_id = ?1
             ORDER BY audience",
            [write_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
    })?;
    rows.into_iter()
        .map(|(audience, control, hash)| Ok((audience, control, payload_bytes(store_dir, hash)?)))
        .collect()
}

fn audience_partition_changesets(
    store_dir: &StoreDir,
    write_id: &str,
) -> Result<Vec<(String, Vec<u8>)>, crate::DbError> {
    audience_partitions(store_dir, write_id).map(|partitions| {
        partitions
            .into_iter()
            .map(|(audience, _, changeset)| (audience, changeset))
            .collect()
    })
}

fn captured_changeset(store_dir: &StoreDir, write_id: &str) -> Result<Vec<u8>, crate::DbError> {
    let hash = inspect_database(store_dir, |conn| {
        conn.query_row(
            "SELECT changeset_hash FROM store_writes WHERE write_id = ?1",
            [write_id],
            |row| row.get::<_, String>(0),
        )
    })?;
    payload_bytes(store_dir, hash)
}

fn has_change(
    changes: &[coven_foundation::changeset::RowChange],
    table: &str,
    op: coven_foundation::changeset::ChangeOp,
    primary_key: &str,
) -> bool {
    changes
        .iter()
        .any(|change| change.table == table && change.op == op && change.pk() == Some(primary_key))
}

/// Every row of a query whose columns are three TEXT values.
fn text_triples(
    conn: &crate::DatabaseImageTest,
    sql: &str,
) -> rusqlite::Result<Vec<(String, String, String)>> {
    conn.query(sql, [], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })
}

/// Every row of a query whose columns are four TEXT values.
fn text_quads(
    conn: &crate::DatabaseImageTest,
    sql: &str,
) -> rusqlite::Result<Vec<(String, String, String, String)>> {
    conn.query(sql, [], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })
}

/// The private audience mirror, as `(routing id, Circle)` pairs.
fn audience_mirror(
    conn: &crate::DatabaseImageTest,
) -> rusqlite::Result<Vec<(String, Option<String>)>> {
    conn.query(
        "SELECT routing_id, circle_id FROM _coven_audience ORDER BY routing_id",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
    )
}

/// The number of rows in `table`.
fn row_count(conn: &crate::DatabaseImageTest, table: &str) -> rusqlite::Result<i64> {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    })
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
    conn: &crate::DatabaseImageTest,
    transaction_id: &str,
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
    let writes = row_count(conn, "store_writes")?;
    let partitions = row_count(conn, "store_write_partitions")?;
    let routes = text_triples(
        conn,
        "SELECT routing_id, table_name, row_id FROM _coven_row_routes
                 ORDER BY routing_id",
    )?;
    let mirror = audience_mirror(conn)?;
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
    conn: &crate::DatabaseImageTest,
) -> rusqlite::Result<ScopedAncestorRollbackState> {
    let folders = text_triples(
        conn,
        "SELECT id, name, _updated_at FROM folders ORDER BY id",
    )?;
    let documents = text_quads(
        conn,
        "SELECT id, folder_id, audience, _updated_at FROM documents ORDER BY id",
    )?;
    let details = text_triples(
        conn,
        "SELECT id, document_id, _updated_at FROM details ORDER BY id",
    )?;
    let writes = row_count(conn, "store_writes")?;
    let partitions = row_count(conn, "store_write_partitions")?;
    let routes = text_triples(
        conn,
        "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                 ORDER BY table_name, row_id",
    )?;
    let mirror = audience_mirror(conn)?;
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
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience")],
        vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, control_coord) = authority.seed_active_circle(CIRCLE_LABEL);
    let store_account_route = authority.scoped_routing_id("accounts", "store-account");
    let circle_account_route = authority.scoped_routing_id("accounts", "circle-account");
    let local_account_route = authority.scoped_routing_id("accounts", "local-account");
    drop(authority);

    let write_circle_id = circle_id.clone();
    let receipt = capture(&writes, move |sql| {
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
    let partitions =
        audience_partitions(&store_dir, &write_id).expect("read durable audience partitions");
    let affected_rows = inspect_database(&store_dir, move |conn| {
        conn.query_row(
            "SELECT affected_rows FROM store_writes WHERE write_id = ?1",
            [affected_write_id],
            |row| row.get::<_, String>(0),
        )
    })
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
    let store_changes = crate::walk_changeset(&store.2).expect("walk Store partition");
    for routing_id in [&store_account_route, &circle_account_route] {
        assert!(store_changes.iter().any(|change| {
            change.table == "_coven_audience"
                && change.op == coven_foundation::changeset::ChangeOp::Insert
                && change.pk() == Some(routing_id)
        }));
    }

    let circle = partition(&circle_id);
    assert_eq!(circle.1.as_deref(), Some(control_coord.as_str()));
    assert!(contains_bytes(&circle.2, b"circle-account"));
    assert!(!contains_bytes(&circle.2, b"store-account"));
    assert!(!contains_bytes(&circle.2, b"local-account"));
    let circle_changes = crate::walk_changeset(&circle.2).expect("walk Circle partition");
    assert!(circle_changes.iter().any(|change| {
        change.table == "_coven_row_routes"
            && change.op == coven_foundation::changeset::ChangeOp::Insert
            && change.pk() == Some(&circle_account_route)
    }));
    let local = partition("local");
    assert_eq!(local.1, None);
    assert!(contains_bytes(&local.2, b"local-account"));
    assert!(!contains_bytes(&local.2, b"store-account"));
    assert!(!contains_bytes(&local.2, b"circle-account"));
    let local_changes = crate::walk_changeset(&local.2).expect("walk Local partition");
    assert!(has_change(
        &local_changes,
        "accounts",
        coven_foundation::changeset::ChangeOp::Insert,
        "local-account"
    ));
    assert!(
        local_changes
            .iter()
            .all(|change| change.table != "_coven_row_routes"),
        "Local routing metadata must remain in the local database"
    );

    let routes = inspect_database(&store_dir, |conn| {
        let routes = text_triples(
            conn,
            "SELECT routing_id, table_name, row_id
                 FROM _coven_row_routes ORDER BY row_id",
        )?;
        let mirror = audience_mirror(conn)?;
        Ok((routes, mirror))
    })
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
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey)
                .inherits_audience_through("account_id"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    let store_account_route = authority.scoped_routing_id("accounts", "store-account");
    let store_transaction_route = authority.scoped_routing_id("transactions", "store-transaction");
    drop(authority);

    capture(&writes, |sql| {
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

    let before = inspect_database(&store_dir, |conn| {
        let writes = row_count(conn, "store_writes")?;
        let partitions = row_count(conn, "store_write_partitions")?;
        let mirror = audience_mirror(conn)?;
        Ok((writes, partitions, mirror))
    })
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
    let failed = capture(&writes, move |sql| {
        sql.execute(
            "UPDATE accounts SET audience = ?1, _updated_at = ?2
                 WHERE id = 'store-account'",
            (&failed_circle_id, sql.stamp()),
        )?;
        Ok(())
    })
    .await;
    assert!(failed.is_err(), "the injected journal failure must surface");
    let after_failure = inspect_database(&store_dir, move |conn| {
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
        let writes = row_count(conn, "store_writes")?;
        let partitions = row_count(conn, "store_write_partitions")?;
        let mirror = audience_mirror(conn)?;
        Ok((audience, child_count, writes, partitions, mirror))
    })
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
    let moved = capture(&writes, move |sql| {
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
    let move_partitions = audience_partition_changesets(&store_dir, &move_write_id)
        .expect("read Store to Circle move partitions");
    let (host_audience, child_count, routes, mirror) = inspect_database(&store_dir, move |conn| {
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
        let routes = text_triples(
            conn,
            "SELECT routing_id, table_name, row_id
                     FROM _coven_row_routes ORDER BY row_id",
        )?;
        let mirror = audience_mirror(conn)?;
        Ok((audience, child_count, routes, mirror))
    })
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
    let circle_changes = crate::walk_changeset(&circle.1).expect("walk Circle partition");
    assert!(circle_changes.iter().any(|change| {
        change.table == "accounts"
            && change.op == coven_foundation::changeset::ChangeOp::Insert
            && change.pk() == Some("store-account")
    }));
    assert!(circle_changes.iter().any(|change| {
        change.table == "transactions"
            && change.op == coven_foundation::changeset::ChangeOp::Insert
            && change.pk() == Some("store-transaction")
    }));
    for routing_id in [&store_account_route, &store_transaction_route] {
        assert!(circle_changes.iter().any(|change| {
            change.table == "_coven_row_routes"
                && change.op == coven_foundation::changeset::ChangeOp::Insert
                && change.pk() == Some(routing_id)
        }));
    }
    let store = move_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store audience mirror partition");
    let store_changes = crate::walk_changeset(&store.1).expect("walk Store partition");
    assert!(store_changes
        .iter()
        .all(|change| change.table != "accounts" && change.table != "transactions"));
    for routing_id in [&store_account_route, &store_transaction_route] {
        assert!(store_changes.iter().any(|change| {
            change.table == "_coven_audience"
                && change.op == coven_foundation::changeset::ChangeOp::Update
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
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience")],
        vec![Migration::sql(
            1,
            "accounts",
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    );

    let unknown_circle = coven_protocol::circle::CircleId::from_bytes([3; 16]).to_string();
    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let inactive_circle = authority.seed_inactive_circle("inactive-circle");
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
    ] {
        let attempted_id = id.to_string();
        let attempted_audience = audience.clone();
        let error = capture(&writes, move |sql| {
            sql.execute(
                "INSERT INTO accounts (id, name, audience, _updated_at)
                     VALUES (?1, 'Rejected', ?2, ?3)",
                (attempted_id, attempted_audience, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .expect_err("invalid scoped write must fail loudly");
        let error = write_failure(error);
        assert!(
            error.to_string().contains(expected_error),
            "{id} surfaced the wrong error: {error}"
        );

        let rejected_id = id.to_string();
        let state = inspect_database(&store_dir, move |conn| {
            let host_rows = conn.query_row(
                "SELECT count(*) FROM accounts WHERE id = ?1",
                [rejected_id],
                |row| row.get::<_, i64>(0),
            )?;
            let routes = row_count(conn, "_coven_row_routes")?;
            let mirror = row_count(conn, "_coven_audience")?;
            let writes = row_count(conn, "store_writes")?;
            let partitions = row_count(conn, "store_write_partitions")?;
            Ok((host_rows, routes, mirror, writes, partitions))
        })
        .expect("read state after rejected scoped write");
        assert_eq!(state, (0, 0, 0, 0, 0), "{id} left durable state");
    }
}

#[tokio::test]
async fn circle_moves_materialize_destinations_and_delete_removes_current_rows() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey)
                .inherits_audience_through("account_id"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    let local_move_account_route = authority.scoped_routing_id("accounts", "local-move-account");
    let local_move_transaction_route =
        authority.scoped_routing_id("transactions", "local-move-transaction");
    let store_move_account_route = authority.scoped_routing_id("accounts", "store-move-account");
    let store_move_transaction_route =
        authority.scoped_routing_id("transactions", "store-move-transaction");
    let deleted_account_route = authority.scoped_routing_id("accounts", "deleted-account");
    let deleted_transaction_route =
        authority.scoped_routing_id("transactions", "deleted-transaction");
    drop(authority);

    let seed_circle_id = circle_id.clone();
    capture(&writes, move |sql| {
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

    let before_failure = inspect_database(&store_dir, |conn| {
        let routes = text_quads(
            conn,
            "SELECT routing_id, table_name, row_id, _updated_at
                     FROM _coven_row_routes ORDER BY routing_id",
        )?;
        let mirror = conn.query(
            "SELECT routing_id, circle_id, _updated_at
                     FROM _coven_audience ORDER BY routing_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let writes = row_count(conn, "store_writes")?;
        let partitions = row_count(conn, "store_write_partitions")?;
        Ok((routes, mirror, writes, partitions))
    })
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
    let failed = capture(&writes, |sql| {
        sql.execute(
            "UPDATE accounts SET audience = 'local', _updated_at = ?1
                 WHERE id = 'local-move-account'",
            [sql.stamp()],
        )?;
        Ok(())
    })
    .await;
    assert!(failed.is_err(), "injected partition failure must surface");
    let after_failure = inspect_database(&store_dir, |conn| {
        let audience = conn.query_row(
            "SELECT audience FROM accounts WHERE id = 'local-move-account'",
            [],
            |row| row.get::<_, String>(0),
        )?;
        let routes = text_quads(
            conn,
            "SELECT routing_id, table_name, row_id, _updated_at
                     FROM _coven_row_routes ORDER BY routing_id",
        )?;
        let mirror = conn.query(
            "SELECT routing_id, circle_id, _updated_at
                     FROM _coven_audience ORDER BY routing_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let writes = row_count(conn, "store_writes")?;
        let partitions = row_count(conn, "store_write_partitions")?;
        Ok((audience, routes, mirror, writes, partitions))
    })
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

    let local_move = capture(&writes, |sql| {
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
    let local_partitions = audience_partition_changesets(&store_dir, &local_write_id)
        .expect("read Circle-to-Local transition partitions");
    let local_raw_changeset = captured_changeset(&store_dir, &local_write_id)
        .expect("read Circle-to-Local captured changeset");
    let (local_routes, local_mirror_count) = inspect_database(&store_dir, move |conn| {
        let routes = conn.query(
            "SELECT routing_id, row_id FROM _coven_row_routes
                     WHERE row_id IN ('local-move-account', 'local-move-transaction')
                     ORDER BY row_id",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mirror_count = conn.query_row(
            "SELECT count(*) FROM _coven_audience audience
                 JOIN _coven_row_routes route USING (routing_id)
                 WHERE route.row_id IN ('local-move-account', 'local-move-transaction')",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        Ok((routes, mirror_count))
    })
    .expect("read Circle-to-Local transition");
    assert_eq!(local_partitions.len(), 1);
    let local_partition = |audience: &str| {
        local_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Local partition"))
    };
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_id),
        "a move must not publish deletes to its old Circle"
    );
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "the host database is already the Local materialization"
    );
    let store_mirror_retract =
        crate::walk_changeset(&local_partition("store").1).expect("walk Store mirror");
    for route in [&local_move_account_route, &local_move_transaction_route] {
        assert!(has_change(
            &store_mirror_retract,
            "_coven_audience",
            coven_foundation::changeset::ChangeOp::Delete,
            route
        ));
    }
    assert_ne!(local_raw_changeset, local_partition("store").1);
    let raw_changes =
        crate::walk_changeset(&local_raw_changeset).expect("walk raw Circle-to-Local changeset");
    assert!(has_change(
        &raw_changes,
        "accounts",
        coven_foundation::changeset::ChangeOp::Update,
        "local-move-account"
    ));
    assert!(raw_changes
        .iter()
        .any(|change| change.table == "_coven_audience"));
    assert!(raw_changes
        .iter()
        .any(|change| change.table == "_coven_row_routes"));
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

    let store_move = capture(&writes, |sql| {
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
    let store_partitions = audience_partition_changesets(&store_dir, &store_write_id)
        .expect("read Circle-to-Store transition partitions");
    let (store_routes, store_mirror) = inspect_database(&store_dir, move |conn| {
        let routes = conn.query(
            "SELECT routing_id, row_id FROM _coven_row_routes
                     WHERE row_id IN ('store-move-account', 'store-move-transaction')
                     ORDER BY row_id",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mirror = conn.query(
            "SELECT audience.routing_id, audience.circle_id
                     FROM _coven_audience audience
                     JOIN _coven_row_routes route USING (routing_id)
                     WHERE route.row_id IN ('store-move-account', 'store-move-transaction')
                     ORDER BY audience.routing_id",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )?;
        Ok((routes, mirror))
    })
    .expect("read Circle-to-Store transition");
    assert_eq!(store_partitions.len(), 1);
    let store_partition = |audience: &str| {
        store_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Store partition"))
    };
    assert!(
        store_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_id),
        "a move must not publish deletes to its old Circle"
    );
    let store_destination =
        crate::walk_changeset(&store_partition("store").1).expect("walk Store destination");
    assert!(has_change(
        &store_destination,
        "accounts",
        coven_foundation::changeset::ChangeOp::Insert,
        "store-move-account"
    ));
    assert!(has_change(
        &store_destination,
        "transactions",
        coven_foundation::changeset::ChangeOp::Insert,
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

    let deleted = capture(&writes, |sql| {
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
    let delete_partitions = audience_partition_changesets(&store_dir, &delete_write_id)
        .expect("read Circle delete partitions");
    let (host_count, route_count, mirror_count) = inspect_database(&store_dir, move |conn| {
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
        Ok((host_count, route_count, mirror_count))
    })
    .expect("read Circle delete transition");
    assert_eq!(delete_partitions.len(), 2);
    let delete_partition = |audience: &str| {
        delete_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-delete partition"))
    };
    let circle_delete =
        crate::walk_changeset(&delete_partition(&circle_id).1).expect("walk Circle delete");
    for (table, row_id) in [
        ("accounts", "deleted-account"),
        ("transactions", "deleted-transaction"),
    ] {
        assert!(has_change(
            &circle_delete,
            table,
            coven_foundation::changeset::ChangeOp::Delete,
            row_id
        ));
    }
    assert!(
        circle_delete
            .iter()
            .all(|change| change.table != "_coven_row_routes"),
        "routing metadata must never be deleted through a private audience"
    );
    let store_delete =
        crate::walk_changeset(&delete_partition("store").1).expect("walk Store delete");
    for route in [&deleted_account_route, &deleted_transaction_route] {
        assert!(has_change(
            &store_delete,
            "_coven_audience",
            coven_foundation::changeset::ChangeOp::Delete,
            route
        ));
    }
    assert!(delete_partitions
        .iter()
        .all(|(audience, _)| audience != "local"));
    assert_eq!((host_count, route_count, mirror_count), (0, 0, 0));
}

#[tokio::test]
async fn scoped_move_does_not_cross_a_store_parent_into_a_sibling_scoped_root() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("folders", RowIdentity::SharedKey).remote_root(),
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    drop(authority);

    capture(&writes, |sql| {
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
    let moved = capture(&writes, move |sql| {
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
    let partitions = audience_partition_changesets(&store_dir, &write_id)
        .expect("read scoped sibling move partitions");

    for (audience, changeset) in partitions {
        let changes = crate::walk_changeset(&changeset)
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

#[tokio::test]
async fn validates_every_outgoing_synced_fk_audience() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("homes", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("targets", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("links", RowIdentity::SharedKey).inherits_audience_through("home_id"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    let (other_circle_id, _other_control) = authority.seed_active_circle("circle-b");
    drop(authority);

    let seeded_circle_id = circle_id.clone();
    let seeded_other_circle_id = other_circle_id.clone();
    capture(&writes, move |sql| {
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

    let allowed = capture(&writes, |sql| {
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
    let allowed_state = inspect_database(&store_dir, move |conn| {
        let host_rows = conn.query_row(
            "SELECT count(*) FROM links
                 WHERE id IN ('same-circle-link', 'store-parent-link')",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let audiences = conn.query(
            "SELECT audience FROM store_write_partitions
                     WHERE write_id = ?1 ORDER BY audience",
            [allowed_write_id],
            |row| row.get::<_, String>(0),
        )?;
        Ok((host_rows, audiences))
    })
    .expect("read allowed relationships");
    let expected_audiences = vec![circle_id, "store".to_string()];
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
        let before = inspect_database(&store_dir, move |conn| {
            let writes = row_count(conn, "store_writes")?;
            let partitions = row_count(conn, "store_write_partitions")?;
            let routes = row_count(conn, "_coven_row_routes")?;
            let mirror = row_count(conn, "_coven_audience")?;
            Ok((writes, partitions, routes, mirror))
        })
        .expect("read state before rejected relationship");

        let attempted_id = id.to_string();
        let attempted_home = home.to_string();
        let attempted_target = target.to_string();
        let error = capture(&writes, move |sql| {
            sql.execute(
                "INSERT INTO links (id, home_id, target_id, _updated_at)
                     VALUES (?1, ?2, ?3, ?4)",
                (attempted_id, attempted_home, attempted_target, sql.stamp()),
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
        let error = write_failure(error);
        assert!(
            error.to_string().contains("relationship through target_id"),
            "{description} surfaced the wrong error: {error}"
        );

        let rejected_id = id.to_string();
        let after = inspect_database(&store_dir, move |conn| {
            let host_rows = conn.query_row(
                "SELECT count(*) FROM links WHERE id = ?1",
                [rejected_id],
                |row| row.get::<_, i64>(0),
            )?;
            let writes = row_count(conn, "store_writes")?;
            let partitions = row_count(conn, "store_write_partitions")?;
            let routes = row_count(conn, "_coven_row_routes")?;
            let mirror = row_count(conn, "_coven_audience")?;
            Ok((host_rows, writes, partitions, routes, mirror))
        })
        .expect("read state after rejected relationship");
        assert_eq!(
            after,
            (0, before.0, before.1, before.2, before.3),
            "{description} left durable state"
        );
    }
}

#[tokio::test]
async fn reparenting_an_inherited_row_materializes_its_subtree() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("requirements", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("transactions", RowIdentity::SharedKey)
                .inherits_audience_through("account_id"),
            SyncedTable::new("line_items", RowIdentity::SharedKey)
                .inherits_audience_through("transaction_id"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    let (circle_b, _circle_b_control) = authority.seed_active_circle("circle-b");
    drop(authority);

    let seeded_circle_id = circle_id.clone();
    let seeded_circle_b = circle_b.clone();
    capture(&writes, move |sql| {
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

    let before_routes = inspect_database(&store_dir, move |conn| {
        text_triples(
            conn,
            "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                 WHERE table_name IN ('transactions', 'line_items')
                 ORDER BY table_name, row_id",
        )
    })
    .expect("read routes before reparent");

    let moved = capture(&writes, |sql| {
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
    let partitions = audience_partition_changesets(&store_dir, &write_id)
        .expect("read inherited reparent partitions");
    let after_routes = inspect_database(&store_dir, move |conn| {
        let after_routes = text_triples(
            conn,
            "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                     WHERE table_name IN ('transactions', 'line_items')
                     ORDER BY table_name, row_id",
        )?;
        Ok(after_routes)
    })
    .expect("read inherited reparent result");

    let partition = |audience: &str| {
        partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} reparent partition"))
    };
    let store = crate::walk_changeset(&partition("store").1).expect("walk Store audience mirror");
    let circle = crate::walk_changeset(&partition(&circle_b).1).expect("walk Circle insert");
    for (table, id) in [
        ("transactions", "to-circle-transaction"),
        ("line_items", "to-circle-transaction-line"),
    ] {
        assert!(has_change(
            &circle,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }
    assert!(store
        .iter()
        .all(|change| change.table != "transactions" && change.table != "line_items"));

    let to_local = capture(&writes, |sql| {
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
    let local_partitions = audience_partition_changesets(&store_dir, &local_write_id)
        .expect("read Circle-to-Local reparent partitions");
    let local_state = inspect_database(&store_dir, move |conn| {
        conn.query_row(
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
        )
    })
    .expect("read Circle-to-Local reparent");
    assert_eq!(
        local_state,
        (
            "local-account".to_string(),
            "local-requirement".to_string(),
            1
        )
    );
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_id),
        "a reparent must not publish deletes to its old Circle"
    );
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "the host database is already the Local materialization"
    );

    let to_store = capture(&writes, |sql| {
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
    let store_partitions = audience_partition_changesets(&store_dir, &store_write_id)
        .expect("read Circle-to-Store reparent");
    let store_partition = |audience: &str| {
        store_partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} Circle-to-Store partition"))
    };
    let store_destination = crate::walk_changeset(&store_partition("store").1)
        .expect("walk Circle-to-Store destination");
    assert!(
        store_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_id),
        "a reparent must not publish deletes to its old Circle"
    );
    for (table, id) in [
        ("transactions", "to-store-transaction"),
        ("line_items", "to-store-transaction-line"),
    ] {
        assert!(has_change(
            &store_destination,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }

    let invalid_before = inspect_database(&store_dir, move |conn| {
        reparent_rollback_state(conn, "invalid-target-transaction")
    })
    .expect("read state before invalid reparent");
    let invalid_error = capture(&writes, |sql| {
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
    let invalid_error = write_failure(invalid_error);
    assert!(
        invalid_error
            .to_string()
            .contains("relationship through requirement_id"),
        "invalid reparent surfaced the wrong error: {invalid_error}"
    );
    let invalid_after = inspect_database(&store_dir, move |conn| {
        reparent_rollback_state(conn, "invalid-target-transaction")
    })
    .expect("read state after invalid reparent");
    assert_eq!(invalid_after, invalid_before);

    let journal_before = inspect_database(&store_dir, move |conn| {
        reparent_rollback_state(conn, "journal-failure-transaction")
    })
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
    let journal_error = capture(&writes, |sql| {
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
    let journal_error = write_failure(journal_error);
    assert!(journal_error
        .to_string()
        .contains("forced inherited reparent journal failure"));
    let journal_after = inspect_database(&store_dir, move |conn| {
        reparent_rollback_state(conn, "journal-failure-transaction")
    })
    .expect("read state after journal failure");
    assert_eq!(journal_after, journal_before);
    fault
        .execute_batch("DROP TRIGGER fail_inherited_reparent_partition;")
        .expect("remove inherited reparent journal failure");
    drop(fault);

    let final_routes = inspect_database(&store_dir, move |conn| {
        text_triples(
            conn,
            "SELECT table_name, row_id, routing_id FROM _coven_row_routes
                     WHERE table_name IN ('transactions', 'line_items')
                     ORDER BY table_name, row_id",
        )
    })
    .expect("read routes after inherited reparent matrix");
    assert_eq!(after_routes, before_routes);
    assert_eq!(final_routes, before_routes);
}

#[tokio::test]
async fn scoped_descendant_keeps_store_ancestor() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let (_database, writes) = scoped_store(
        &store_dir,
        "capture-device",
        vec![
            SyncedTable::new("folders", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
            SyncedTable::new("details", RowIdentity::SharedKey)
                .inherits_audience_through("document_id"),
        ],
        vec![Migration::sql(
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
        )],
    );

    let authority =
        crate::DatabaseImageTest::open(&store_dir.db_path()).expect("open authority db");
    let (circle_id, _control) = authority.seed_active_circle(CIRCLE_LABEL);
    let (circle_b, _circle_b_control) = authority.seed_active_circle("circle-b");
    drop(authority);

    let seeded = capture(&writes, |sql| {
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
    let local_only_partitions = audience_partition_changesets(&store_dir, &seed_write_id)
        .expect("read Local-only ancestor journal");
    assert_eq!(local_only_partitions.len(), 1);
    assert_eq!(local_only_partitions[0].0, "local");
    let local_seed =
        crate::walk_changeset(&local_only_partitions[0].1).expect("walk Local-only journal");
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &local_seed,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }

    let destination_circle_id = circle_id.clone();
    let moved = capture(&writes, move |sql| {
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
    let partitions = audience_partition_changesets(&store_dir, &write_id)
        .expect("read Local-to-Circle ancestor move");
    let partition = |audience: &str| {
        partitions
            .iter()
            .find(|(candidate, _)| candidate == audience)
            .unwrap_or_else(|| panic!("missing {audience} scoped ancestor partition"))
    };
    let store = crate::walk_changeset(&partition("store").1)
        .expect("walk required ancestor Store partition");
    assert!(has_change(
        &store,
        "folders",
        coven_foundation::changeset::ChangeOp::Insert,
        "required-folder"
    ));
    let circle =
        crate::walk_changeset(&partition(&circle_id).1).expect("walk Circle descendant partition");
    assert!(
        partitions.iter().all(|(audience, _)| audience != "local"),
        "a move must not publish deletes to its old Local audience"
    );
    for (table, id) in [
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &circle,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }

    let sibling_circle = circle_b.clone();
    let inserted_sibling = capture(&writes, move |sql| {
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
    let sibling_partitions = audience_partition_changesets(&store_dir, &sibling_write_id)
        .expect("read Circle B sibling insert");
    let sibling = sibling_partitions
        .iter()
        .find(|(audience, _)| audience == &sibling_circle)
        .expect("Circle B sibling partition");
    let sibling = crate::walk_changeset(&sibling.1).expect("walk Circle B sibling insert");
    for (table, id) in [
        ("documents", "sibling-document"),
        ("details", "sibling-detail"),
    ] {
        assert!(has_change(
            &sibling,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }
    let sibling_store = sibling_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store ancestor partition for Circle B insert");
    let sibling_store = crate::walk_changeset(&sibling_store.1)
        .expect("walk Store ancestor partition for Circle B insert");
    assert!(has_change(
        &sibling_store,
        "folders",
        coven_foundation::changeset::ChangeOp::Insert,
        "required-folder"
    ));
    for (_, changeset) in &sibling_partitions {
        let changes = crate::walk_changeset(changeset).expect("walk sibling partition");
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

    let moved_local = capture(&writes, |sql| {
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
    let local_partitions = audience_partition_changesets(&store_dir, &local_write_id)
        .expect("read Circle A to Local move");
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_id),
        "a move must not publish deletes to its old Circle"
    );
    assert!(
        local_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "the host database is already the Local materialization"
    );
    for (_, changeset) in &local_partitions {
        let changes = crate::walk_changeset(changeset).expect("walk Circle-to-Local partition");
        assert!(!has_change(
            &changes,
            "folders",
            coven_foundation::changeset::ChangeOp::Delete,
            "required-folder"
        ));
        for id in ["sibling-document", "sibling-detail"] {
            assert!(!changes.iter().any(|change| change
                .columns
                .iter()
                .any(|value| value.as_deref() == Some(id))));
        }
    }

    let rollback_before = inspect_database(&store_dir, scoped_ancestor_rollback_state)
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
    let failed_move = capture(&writes, |sql| {
        sql.execute(
            "UPDATE documents SET audience = 'local', _updated_at = ?1
                 WHERE id = 'sibling-document'",
            [sql.stamp()],
        )?;
        Ok(())
    })
    .await
    .expect_err("journal failure must abort final non-Local descendant move");
    let failed_move = write_failure(failed_move);
    assert!(failed_move
        .to_string()
        .contains("forced scoped ancestor journal failure"));
    let rollback_after = inspect_database(&store_dir, scoped_ancestor_rollback_state)
        .expect("read state after ancestor retraction failure");
    assert_eq!(rollback_after, rollback_before);
    fault
        .execute_batch("DROP TRIGGER fail_scoped_ancestor_partition;")
        .expect("remove scoped ancestor journal failure");
    drop(fault);

    let moved_sibling_local = capture(&writes, |sql| {
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
    let sibling_local_partitions =
        audience_partition_changesets(&store_dir, &sibling_local_write_id)
            .expect("read final Circle descendant move to Local");
    let store_retraction = sibling_local_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store ancestor retraction partition");
    let store_retraction =
        crate::walk_changeset(&store_retraction.1).expect("walk Store ancestor retraction");
    assert!(has_change(
        &store_retraction,
        "folders",
        coven_foundation::changeset::ChangeOp::Delete,
        "required-folder"
    ));
    assert!(
        sibling_local_partitions
            .iter()
            .all(|(audience, _)| audience != &circle_b),
        "a move must not publish deletes to its old Circle"
    );
    assert!(
        sibling_local_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "the host database is already the Local materialization"
    );

    let moved_store = capture(&writes, |sql| {
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
    let store_partitions = audience_partition_changesets(&store_dir, &store_write_id)
        .expect("read Local descendant move to Store");
    let store_destination = store_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store descendant destination partition");
    assert!(
        store_partitions
            .iter()
            .all(|(audience, _)| audience != "local"),
        "a move must not publish deletes to its old Local audience"
    );
    let store_destination =
        crate::walk_changeset(&store_destination.1).expect("walk Store descendant destination");
    for (table, id) in [
        ("folders", "required-folder"),
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &store_destination,
            table,
            coven_foundation::changeset::ChangeOp::Insert,
            id
        ));
    }

    let deleted_store = capture(&writes, |sql| {
        sql.execute("DELETE FROM documents WHERE id = 'moving-document'", [])?;
        Ok(())
    })
    .await
    .expect("delete final Store descendant");
    let delete_write_id = deleted_store.write_id.to_string();
    let delete_partitions = audience_partition_changesets(&store_dir, &delete_write_id)
        .expect("read final Store descendant delete");
    let store_delete = delete_partitions
        .iter()
        .find(|(audience, _)| audience == "store")
        .expect("Store descendant delete partition");
    let store_delete =
        crate::walk_changeset(&store_delete.1).expect("walk Store descendant delete");
    for (table, id) in [
        ("folders", "required-folder"),
        ("documents", "moving-document"),
        ("details", "moving-detail"),
    ] {
        assert!(has_change(
            &store_delete,
            table,
            coven_foundation::changeset::ChangeOp::Delete,
            id
        ));
    }
}

//! Scoped host writes over a real Store: what the durable write partitions hold
//! after a restart, and which audiences a scoped move emits packages for.
//!
//! The routing itself is the database's, but every one of these needs the Store
//! authority Store creation signs, so they run the production creation path.

use crate::sync::test_helpers::{test_cloud_home, TestStore};
use coven_database::Migration;
use coven_database::StoreDatabase;
use coven_database::*;
use coven_keys::encryption::EncryptionService;
use coven_protocol::blob::BLOB_TOMBSTONE_GRACE;
use coven_protocol::synced_schema::SyncedTable;
use coven_protocol::write::WriteStatus;
use std::path::Path;

async fn capture_scoped_write_then_reopen(
    name: &str,
) -> (
    tempfile::TempDir,
    SyntheticStoreFixture,
    Vec<(String, Option<String>, Vec<u8>)>,
) {
    let temp = tempfile::tempdir().expect("temporary scoped Store");
    let path = temp.path().join(format!("{name}.db"));
    let tables = vec![SyncedTable::new(
        "accounts",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .scoped_by("audience")];
    let migrations = vec![Migration::sql(
        1,
        "accounts",
        "CREATE TABLE accounts (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
         ) STRICT;",
    )];
    let db = SyntheticStoreFixture::open(
        &path,
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        format!("{name}-device"),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
    )
    .expect("open scoped Store");
    TestStore::create(
        &db,
        name,
        coven_keys::keys::UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("install exact scoped Store authority");
    let circle_label = format!("{name}-circle");
    let circle_id = StoreDatabase::new(&db.database)
        .install_test_active_circle(circle_label)
        .await
        .expect("install active Circle current state");
    let circle_id = circle_id.to_string();

    let routing = EncryptionService::from_key([7; 32]);
    let capture_circle_id = circle_id.clone();
    StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
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
        })
        .await
        .expect("capture Store and Circle partitions");
    let expected = db
        .database
        .store_write_partitions_in_audience_order_for_test()
        .await
        .expect("read exact persisted audience partitions");
    assert_eq!(expected.len(), 3);
    assert!(expected.iter().any(|(audience, control, changeset)| {
        audience == "local" && control.is_none() && !changeset.is_empty()
    }));
    drop(db);

    let reopened = SyntheticStoreFixture::open(
        &path,
        tables,
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        format!("{name}-device"),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
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
                coven_protocol::circle::Audience::Store => "store".to_string(),
                coven_protocol::circle::Audience::Circle(circle) => circle.to_string(),
                coven_protocol::circle::Audience::Local => "local".to_string(),
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
    let prepared = StoreDatabase::new(&reopened.database)
        .prepare_store_write()
        .await
        .expect("prepare restarted Merge write")
        .expect("pending Merge write");

    assert_prepared_partitions(&prepared.partitions, &expected);
}

#[tokio::test]
async fn merge_preparation_fails_when_a_partition_payload_is_missing() {
    let (_temp, reopened, _) = capture_scoped_write_then_reopen("missing-partition-payload").await;
    let database = StoreDatabase::new(&reopened.database);
    let write_id = database
        .pending_writes()
        .await
        .expect("read captured write")
        .into_iter()
        .next()
        .expect("captured write exists")
        .write_id;
    let hash = reopened
        .database
        .first_store_write_partition_hash_for_test(write_id)
        .await
        .expect("read partition payload hash");
    database
        .remove_payload_bytes_for_test(hash)
        .await
        .expect("remove partition payload");

    let error = match database.prepare_store_write().await {
        Err(error) => error,
        Ok(_) => panic!("missing partition payload must fail preparation"),
    };
    assert!(
        error.to_string().contains("absent from the spool"),
        "{error}"
    );
}

#[tokio::test]
async fn preparation_rejects_a_local_partition_with_circle_control() {
    let (_temp, reopened, _expected) =
        capture_scoped_write_then_reopen("controlled-local-restart").await;
    reopened
        .database
        .plant_control_on_local_partition_for_test()
        .await
        .expect("plant controlled Local partition");

    let error = match StoreDatabase::new(&reopened.database)
        .prepare_store_write()
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("controlled Local partition must fail preparation"),
    };
    assert!(error
        .to_string()
        .contains("Local partition carries a Circle control"));
}

#[tokio::test]
async fn merge_local_only_scoped_write_is_not_pending() {
    let (_temp, db, _) = capture_scoped_write_then_reopen("merge-local-only").await;
    let store_database = StoreDatabase::new(&db.database);
    let routing = EncryptionService::from_key([7; 32]);
    let receipt = store_database
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
            tx.execute(
                "INSERT INTO accounts (id, audience, _updated_at)
                         VALUES ('second-local-account', 'local', '0000000002000-0000-local')",
                [],
            )?;
            Ok::<_, DbError>(())
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
    let ((affected_rows, captured_changeset), partition) = db
        .database
        .store_write_row_and_only_partition_for_test(receipt.write_id.clone())
        .await
        .expect("verify durable local-only journal");
    assert_eq!(affected_rows, "[]");
    let raw_changes = coven_database::walk_changeset(&captured_changeset)
        .expect("walk captured Local-only changeset");
    assert!(raw_changes.iter().any(|change| {
        change.table == "accounts" && change.pk() == Some("second-local-account")
    }));
    assert_eq!(partition.0, "local");
    assert_eq!(partition.1, None);
    assert!(!partition.2.is_empty());
}

#[tokio::test]
async fn discarding_a_scoped_write_reverses_its_private_routing_rows() {
    let (_temp, db, _) = capture_scoped_write_then_reopen("discard-scoped-routing").await;
    let circle_id = StoreDatabase::new(&db.database)
        .install_test_active_circle("discard-scoped-circle".to_string())
        .await
        .expect("install discard test Circle");
    let write_circle_id = circle_id;
    let database = StoreDatabase::new(&db.database);
    let receipt = database
        .run_host_store_write_for_test(
            Some(EncryptionService::from_key([7; 32])),
            None,
            move |tx| {
                tx.execute(
                    "INSERT INTO accounts (id, audience, _updated_at)
                     VALUES ('discarded-circle-account', ?1, '0000000003000-0000-discard')",
                    [write_circle_id.to_string()],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture scoped write to discard");
    database
        .set_write_status(
            &receipt.write_id,
            WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "discard scoped routing test".to_string(),
            }),
        )
        .await
        .expect("block scoped write");

    assert_eq!(
        database
            .discard_blocked_write(&receipt.write_id)
            .await
            .expect("discard scoped write"),
        coven_database::BlockedWriteDiscard::Discarded(vec![receipt.write_id])
    );
    let state = db
        .database
        .row_and_private_routing_presence_for_test("accounts", "discarded-circle-account")
        .await
        .expect("read discarded scoped state");
    assert_eq!(state, (false, false, false));
}

/// A transaction whose rows are all Circle-scoped emits one package per Circle
/// plus a Store package carrying only the audience mirror — never an empty Store
/// package. The Store partition exists so a peer without the Circle still learns
/// each row's routing (`_coven_audience`), but it must carry that mirror and no
/// scoped row bytes; an empty Store changeset would be a Store commit activating
/// nothing.
#[tokio::test]
async fn circle_only_write_emits_a_mirror_only_store_package() {
    let tables = vec![SyncedTable::new(
        "accounts",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .scoped_by("audience")];
    let db = SyntheticStoreFixture::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "circle-only-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
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
    TestStore::create(
        &db,
        "circle-only",
        coven_keys::keys::UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("install scoped Store authority");
    let circles = StoreDatabase::new(&db.database)
        .install_test_active_circles(vec![
            "circle-only-first".to_string(),
            "circle-only-second".to_string(),
        ])
        .await
        .expect("install two Circles");
    let [first, second]: [_; 2] = circles.try_into().expect("installed exactly two Circles");

    let routing = EncryptionService::from_key([7; 32]);
    let (first_audience, second_audience) = (first.to_string(), second.to_string());
    let (write_first, write_second) = (first_audience.clone(), second_audience.clone());
    let receipt = StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
            tx.execute(
                "INSERT INTO accounts VALUES ('first-account', ?1, '0000000001000-0000-circle')",
                [&write_first],
            )?;
            tx.execute(
                "INSERT INTO accounts VALUES ('second-account', ?1, '0000000001001-0000-circle')",
                [&write_second],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture Circle-only partitions");
    let stored_write_id = receipt.write_id;

    let partitions = db
        .database
        .store_write_partition_changesets_for_test(stored_write_id)
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
    let store_rows = coven_database::walk_changeset(store_changeset).expect("walk Store partition");
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
            coven_database::walk_changeset(circle_changeset).expect("walk Circle partition");
        assert!(
            circle_rows.iter().any(|row| row.table == "accounts"),
            "each Circle package carries its scoped rows",
        );
    }
}

#[tokio::test]
async fn cross_circle_move_emits_only_the_destination_image_and_store_mirror() {
    let tables = vec![SyncedTable::new(
        "accounts",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .scoped_by("audience")];
    let db = SyntheticStoreFixture::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "move-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
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
    TestStore::create(
        &db,
        "cross-circle-move",
        coven_keys::keys::UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("install scoped Store authority");
    let circles = StoreDatabase::new(&db.database)
        .install_test_active_circles(vec![
            "move-source".to_string(),
            "move-destination".to_string(),
        ])
        .await
        .expect("install source and destination Circles");
    let [source, destination]: [_; 2] = circles.try_into().expect("installed exactly two Circles");
    let routing = EncryptionService::from_key([7; 32]);
    StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
            tx.execute(
                "INSERT INTO accounts VALUES ('account', ?1, '0000000001000-0000-move')",
                [source.to_string()],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("insert source Circle row");

    let routing = EncryptionService::from_key([7; 32]);
    let receipt = StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
            tx.execute(
                "UPDATE accounts
                     SET audience = ?1, _updated_at = '0000000002000-0000-move'
                     WHERE id = 'account'",
                [destination.to_string()],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("move row between Circles");
    let stored_move_id = receipt.write_id;

    let partitions = db
        .database
        .store_write_partition_changesets_for_test(stored_move_id)
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
    let store_rows = coven_database::walk_changeset(&store.1).expect("walk Store move partition");
    assert_eq!(store_rows.len(), 1);
    assert_eq!(store_rows[0].table, "_coven_audience");
    assert_eq!(
        store_rows[0].op,
        coven_foundation::changeset::ChangeOp::Update
    );

    let destination_rows = coven_database::walk_changeset(
        &partitions
            .iter()
            .find(|(audience, _)| audience == &destination.to_string())
            .expect("move has destination Circle partition")
            .1,
    )
    .expect("walk destination move partition");
    assert!(destination_rows.iter().any(|row| {
        row.table == "accounts" && row.op == coven_foundation::changeset::ChangeOp::Insert
    }));
    assert!(destination_rows.iter().any(|row| {
        row.table == "_coven_row_routes" && row.op == coven_foundation::changeset::ChangeOp::Insert
    }));
    assert!(destination_rows
        .iter()
        .all(|row| row.op != coven_foundation::changeset::ChangeOp::Delete));
}

#[tokio::test]
async fn root_move_rejects_an_unchanged_descendants_cross_circle_foreign_key() {
    let tables = vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .scoped_by("audience"),
        SyncedTable::new(
            "categories",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .scoped_by("audience"),
        SyncedTable::new(
            "comments",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .inherits_audience_through("note_id"),
    ];
    let db = SyntheticStoreFixture::open(
        Path::new(":memory:"),
        tables.clone(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "foreign-key-move-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
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
    TestStore::create(
        &db,
        "foreign-key-move",
        coven_keys::keys::UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("install scoped Store authority");
    let circles = StoreDatabase::new(&db.database)
        .install_test_active_circles(vec![
            "relationship-source".to_string(),
            "relationship-destination".to_string(),
        ])
        .await
        .expect("install relationship Circles");
    let [source, destination]: [_; 2] = circles.try_into().expect("installed exactly two Circles");
    let routing = EncryptionService::from_key([7; 32]);
    StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
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
        })
        .await
        .expect("insert valid scoped relationship");

    let routing = EncryptionService::from_key([7; 32]);
    let error = StoreDatabase::new(&db.database)
        .run_host_store_write_for_test(Some(routing), None, move |tx| {
            tx.execute(
                "UPDATE notes
                         SET audience = ?1, _updated_at = '0000000002000-0000-move'
                         WHERE id = 'note'",
                [destination.to_string()],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect_err("move must reject the unchanged cross-Circle relationship");
    assert!(error
        .to_string()
        .contains("relationship through category_id"));
    let audience = StoreDatabase::new(&db.database)
        .read(|sql| {
            sql.query_row("SELECT audience FROM notes WHERE id = 'note'", [], |row| {
                row.get::<_, String>(0)
            })
        })
        .await
        .expect("read rolled-back note audience")
        .expect("read note audience row");
    assert_eq!(audience, source.to_string());
}

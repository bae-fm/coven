use crate::sync::test_helpers::{
    test_cloud_home, test_migrations, test_store_dir, test_synced_tables, TestStore,
};
use coven_database::{CovenMigrationPolicy, Database, Migration, StoreDatabase};
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn pending_write_publication_keeps_its_captured_schema_after_upgrade() {
    assert_publication_schema_after_upgrade(false, JournalState::Pending, false).await;
}

#[tokio::test]
async fn unversioned_pending_write_recovers_capture_schema_before_publication() {
    assert_publication_schema_after_upgrade(true, JournalState::Pending, false).await;
}

#[tokio::test]
async fn unversioned_prepared_write_recovers_signed_schema_and_preserves_candidate() {
    assert_publication_schema_after_upgrade(true, JournalState::Prepared, false).await;
}

#[derive(Clone, Copy)]
enum JournalState {
    Pending,
    Prepared,
    Published,
}

async fn assert_publication_schema_after_upgrade(
    legacy_journal: bool,
    state: JournalState,
    mismatched_partition: bool,
) {
    let prepare_before_upgrade = !matches!(state, JournalState::Pending);
    let publish_before_upgrade = matches!(state, JournalState::Published);
    let directory = test_store_dir();
    let path = directory.db_path();
    let open = |migrations: &[Migration]| {
        Database::open_in_store_dir_for_test(
            &path,
            directory.clone(),
            test_synced_tables(),
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "pending-schema-history".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            CovenMigrationPolicy::ApplyPending,
            migrations,
        )
    };
    let db = open(&test_migrations()).expect("open capture schema");
    let founder = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &db,
        directory.clone(),
        "pending-schema-history",
        founder.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store authority");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, shared, _updated_at, created_at)
         VALUES ('captured-note', 'Captured before upgrade', 1,
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    let captured = StoreDatabase::new(&db)
        .prepare_store_write()
        .await
        .expect("read captured write")
        .expect("write is pending");
    let write_id = captured.write_id;
    let original = captured.partitions;
    let prior_candidate = if prepare_before_upgrade {
        let device = store
            .bind_device_in(&db, directory.clone(), &founder)
            .await
            .unwrap();
        let mut writer = device.authorize_writer().await.unwrap();
        assert!(writer.prepare_pending_store_write().await.unwrap());
        let prior = Some(
            StoreDatabase::new(&db)
                .oldest_prepared_store_write()
                .await
                .unwrap()
                .unwrap()
                .commit
                .value
                .reference()
                .clone(),
        );
        if publish_before_upgrade {
            assert_eq!(writer.drain_store_writes().await.unwrap(), 1);
        }
        prior
    } else {
        None
    };
    if mismatched_partition {
        db.execute_test_host_write("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('different-note','Different bytes',1,'0000000002000-0000-writer','2026-01-01')").await;
    }
    let root = store.root();
    drop(store);
    drop(db);

    let mut migrations = test_migrations();
    if publish_before_upgrade {
        migrations.push(
            Migration::sql(
                2,
                "qualify titles",
                "UPDATE notes SET title = 'migrated:' || title;",
            )
            .changesets(vec![coven_database::TableChangesetMigration::new(
                "notes",
                &[],
                |row, _| {
                    for column in &mut row.columns {
                        if column.name == "title" {
                            for value in [&mut column.old, &mut column.new] {
                                if let Some(rusqlite::types::Value::Text(text)) = value {
                                    *text = format!("migrated:{text}");
                                }
                            }
                        }
                    }
                    Ok(())
                },
            )]),
        );
        drop(
            open(&migrations).expect("advance host schema before recovering older published bytes"),
        );
    } else {
        migrations.push(Migration::sql(
            2,
            "local preferences",
            "CREATE TABLE local_preferences (id TEXT PRIMARY KEY, value TEXT) STRICT;",
        ));
    }
    if mismatched_partition {
        coven_database::DatabaseImageTest::open(&path)
            .unwrap()
            .corrupt_first_write_partition_from_last()
            .unwrap();
    }
    if legacy_journal {
        coven_database::DatabaseImageTest::open(&path)
            .unwrap()
            .downgrade_coven_schema_to_v2(false)
            .unwrap();
    }
    if mismatched_partition {
        let error = match open(&migrations) {
            Ok(_) => panic!("prepared schema evidence must bind the captured partition bytes"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("partition"), "{error}");
        return;
    }
    let upgraded = open(&migrations).expect("upgrade with an unpublished write");
    assert_eq!(upgraded.schema_version(), 2);
    let database = StoreDatabase::new(&upgraded);
    if publish_before_upgrade {
        assert_eq!(
            coven_database::DatabaseImageTest::open(&path)
                .unwrap()
                .store_write_schema_versions(&write_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            upgraded
                .query_test_text("SELECT title FROM notes WHERE id='captured-note'")
                .await,
            "migrated:Captured before upgrade"
        );
        return;
    }
    if !prepare_before_upgrade {
        let pending = database.prepare_store_write().await.unwrap().unwrap();
        assert_eq!(
            pending.partitions, original,
            "schema upgrade preserves the captured bytes"
        );
        assert_eq!(pending.schema_version, 1);
    }
    let resumed = crate::sync::store::Store::open(
        database.clone(),
        storage,
        directory,
        &root,
        &founder,
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
    )
    .await
    .expect("open upgraded writer")
    .into_parts()
    .0;
    let mut writer = resumed.authorize_writer().await.unwrap();
    if !prepare_before_upgrade {
        assert!(writer.prepare_pending_store_write().await.unwrap());
    }
    let prepared = database
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .unwrap();
    if let Some(prior) = prior_candidate {
        assert_eq!(prepared.commit.value.reference(), &prior);
    }
    assert_eq!(
        prepared
            .commit
            .value
            .store_package()
            .unwrap()
            .schema_version,
        1,
        "the signed package must name the schema that captured its bytes"
    );
}

#[tokio::test]
async fn unversioned_prepared_write_rejects_mismatched_captured_partition() {
    assert_publication_schema_after_upgrade(true, JournalState::Prepared, true).await;
}

#[tokio::test]
async fn legacy_published_write_recovers_signed_schema_after_same_layout_value_migration() {
    assert_publication_schema_after_upgrade(true, JournalState::Published, false).await;
}

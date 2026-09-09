use crate::sync::test_helpers::{
    open_test_db, test_cloud_home, test_migrations, test_store_dir, test_synced_tables, TestDevice,
    TestStore,
};
use coven_database::{Database, Migration, StoreDatabase};
use coven_keys::keys::UserKeypair;
use std::sync::Arc;

#[path = "concurrent_publication_install_tests.rs"]
mod concurrent_install;

#[path = "seeded_snapshot_adoption_tests.rs"]
mod seeded_adoption;

#[path = "snapshot_schema_adoption_tests.rs"]
mod schema_adoption;

#[tokio::test]
async fn warm_snapshot_adoption_migrates_an_older_image_and_preserves_local_work() {
    exercise_warm_adoption(false).await;
}

#[tokio::test]
async fn warm_snapshot_adoption_rolls_back_image_journal_and_configuration_together() {
    exercise_warm_adoption(true).await;
}

async fn exercise_warm_adoption(interrupt: bool) {
    let directory = tempfile::tempdir().expect("receiver database directory");
    let receiver_path = directory.path().join("receiver.db");
    let administrator_dir = test_store_dir();
    let administrator_database = open_test_db(administrator_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, _) = TestStore::create_with_connection(
        &administrator_database,
        administrator_dir.clone(),
        "warm-snapshot-adoption",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let administrator = store
        .bind_device_in(&administrator_database, administrator_dir.clone(), &signer)
        .await
        .expect("bind administrator");
    administrator_database.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) VALUES \
         ('shared', 'Original title', 'Original body', 1, '0000000001000-0000-admin', '2026-01-01')",
    ).await;
    publish(&administrator).await;
    let receiver_dir = test_store_dir();
    let initial = open_receiver(
        &receiver_path,
        receiver_dir.clone(),
        "warm-receiver",
        &test_migrations(),
    );
    let receiver = store
        .activate_joined_device(
            &administrator_database,
            administrator_dir,
            &initial,
            receiver_dir.clone(),
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate returning Owner");
    initial
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('folded-private', 'Folded private row', 0, '0000000002000-0000-receiver', '2026-01-01')",
        )
        .await;
    receiver
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish receiver baseline");
    receiver
        .stand_on_accepted_snapshot()
        .await
        .expect("fold private write into local baseline");
    assert_eq!(
        StoreDatabase::new(&initial)
            .store_write_status_count_for_test(coven_protocol::write::WriteStatus::LocalOnly)
            .await
            .expect("read remaining private journal entries"),
        0,
    );
    let original = StoreDatabase::new(&initial)
        .latest_local_store_snapshot()
        .await
        .unwrap()
        .unwrap();
    administrator
        .pull_store()
        .await
        .expect("administrator observes receiver snapshot");
    let receiver_device_id = receiver.device_id();
    drop(receiver);
    drop(initial);

    let mut migrations = test_migrations();
    migrations.push(Migration::run(2, "receiver-note-metadata", |sql| {
        sql.execute_batch(
            "ALTER TABLE notes ADD COLUMN annotation TEXT NOT NULL DEFAULT 'Migrated image';
             UPDATE notes SET annotation = 'Receiver private annotation' WHERE id = 'folded-private';
             CREATE TABLE device_configuration (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
             INSERT INTO device_configuration VALUES ('choice', 'Migration default');",
        )?;
        Ok(())
    }));
    let database = open_receiver(
        &receiver_path,
        receiver_dir.clone(),
        &receiver_device_id,
        &migrations,
    );
    let receiver = store
        .bind_device_in(&database, receiver_dir.clone(), &signer)
        .await
        .expect("reopen receiver with its registered migration ladder");
    database.execute_test_host_write(
        "UPDATE device_configuration SET value = 'Receiver choice' WHERE id = 'choice';
         UPDATE notes SET title = 'Recorded local title', _updated_at = '0000000003000-0000-receiver' WHERE id = 'shared';
         INSERT INTO notes (id, title, shared, _updated_at, created_at, annotation) VALUES
         ('pending-private', 'Pending private row', 0, '0000000003000-0000-receiver', '2026-01-01', 'Recorded private annotation');",
    ).await;
    let mut writer = receiver
        .authorize_writer()
        .await
        .expect("authorize prepared local edit");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed write"));
    drop(writer);
    let records = StoreDatabase::new(&database);
    let active = records.active_store_publication().await.unwrap().unwrap();
    let write_id = active
        .commit_reservation()
        .expect("reserved host write")
        .0
        .clone();
    let captured = records
        .store_write_capture_for_test(write_id.clone())
        .await
        .expect("read original captured edit");
    let before = records.store_current_publication().await.unwrap();
    let before_baseline = records.installed_replay_baseline().await.unwrap();

    administrator_database.execute_test_host_write(
        "UPDATE notes SET body = 'Accepted peer body', _updated_at = '0000000004000-0000-admin' WHERE id = 'shared'",
    ).await;
    publish(&administrator).await;
    administrator
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish older-schema successor");
    administrator
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt successor for retirement");
    administrator
        .reclaim_packages()
        .await
        .expect("retire receiver snapshot artifacts");
    assert!(!home.contains_exact_object(&original.reference.object));
    administrator
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish snapshot after inventory retirement");
    let latest = StoreDatabase::new(&administrator_database)
        .latest_local_store_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(!latest
        .meta
        .history_summary
        .reclaim
        .snapshots
        .contains_key(&original.reference.snapshot_hash));
    home.clear_exact_reads();

    if interrupt {
        database.fail_next_merge_materialization_at(
            coven_database::MergeMaterializationFailurePoint::ProjectionReplacement,
        );
        let error = receiver
            .pull_store()
            .await
            .expect_err("projection failure aborts adoption");
        assert!(
            error
                .to_string()
                .contains("injected failure after Merge projection replacement"),
            "{error:?}"
        );
        assert_eq!(records.store_current_publication().await.unwrap(), before);
        assert_eq!(
            records.active_store_publication().await.unwrap(),
            Some(active.clone())
        );
        assert_eq!(
            records
                .installed_replay_baseline()
                .await
                .unwrap()
                .coverage(),
            before_baseline.coverage()
        );
        assert_eq!(
            database
                .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
                .await,
            "Original body"
        );
        assert_eq!(
            database
                .query_test_text("SELECT value FROM device_configuration WHERE id = 'choice'")
                .await,
            "Receiver choice"
        );
    }
    let (_, pulled) = receiver
        .pull_store()
        .await
        .expect("adopt current snapshot without retired history");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert!(
        !home
            .exact_reads()
            .contains(original.reference.object.slot()),
        "adoption must not request retired metadata"
    );
    assert_eq!(database.schema_version(), 2);
    assert_eq!(
        database
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Recorded local title"
    );
    assert_eq!(
        database
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Accepted peer body"
    );
    assert_eq!(
        database
            .query_test_text("SELECT annotation FROM notes WHERE id = 'shared'")
            .await,
        "Migrated image"
    );
    assert_eq!(
        database
            .query_test_text("SELECT annotation FROM notes WHERE id = 'folded-private'")
            .await,
        "Receiver private annotation"
    );
    assert_eq!(
        database
            .query_test_text("SELECT annotation FROM notes WHERE id = 'pending-private'")
            .await,
        "Recorded private annotation"
    );
    assert_eq!(
        database
            .query_test_text("SELECT value FROM device_configuration WHERE id = 'choice'")
            .await,
        "Receiver choice"
    );
    let awaiting = records.active_store_publication().await.unwrap().unwrap();
    assert!(awaiting.is_awaiting_preparation());
    assert_eq!(awaiting.commit_reservation(), active.commit_reservation());
    assert_eq!(
        records
            .store_write_capture_for_test(write_id)
            .await
            .expect("read retained captured edit"),
        captured,
        "original observations and recorded edit survive image adoption",
    );
}

async fn publish(device: &TestDevice) {
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize captured write");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare captured write"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish captured write"),
        1
    );
}

fn open_receiver(
    path: &std::path::Path,
    directory: coven_foundation::store_dir::StoreDir,
    device_id: &str,
    migrations: &[Migration],
) -> Database {
    Database::open_synthetic_for_test(
        path,
        directory,
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.into(),
        Arc::new(coven_foundation::clock::SystemClock),
        migrations,
    )
    .expect("open returning receiver")
}

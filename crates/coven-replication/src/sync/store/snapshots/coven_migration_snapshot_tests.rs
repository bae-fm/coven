use super::*;

fn direct_open_fixture(
    bootstrap: PreparedSnapshotBootstrap<'_>,
) -> (
    SnapshotDatabaseImage,
    coven_database::VerifiedSnapshotBootstrapInstall,
) {
    let PreparedSnapshotBootstrap {
        database_image,
        history_verifier,
        founder_registration,
        snapshot,
        authority,
        membership,
        ..
    } = bootstrap;
    let root = history_verifier.verified_root().clone();
    let install = verified_snapshot_bootstrap_install(
        snapshot,
        &root,
        founder_registration,
        authority,
        &membership,
        None,
    )
    .expect("construct verified snapshot install")
    .with_circle_installs(coven_database::StagedCircleRestore {
        access: Vec::new(),
        bases: Vec::new(),
        packages: None,
    });
    (database_image, install)
}

fn assert_v0_uninitialized(path: &std::path::Path) {
    coven_database::DatabaseImageTest::open(path)
        .expect("open snapshot image")
        .validate_uninitialized_coven_schema_v0(false)
        .expect("validate exact uninitialized Coven v0 schema");
}

fn assert_current_initialized(path: &std::path::Path) {
    coven_database::DatabaseImageTest::open(path)
        .expect("open installed image")
        .validate_current_initialized_coven_schema(false)
        .expect("validate exact initialized current Coven schema");
}

#[tokio::test]
async fn direct_snapshot_open_rejects_image_bytes_outside_its_verified_metadata() {
    assert_direct_open_rejects_unaccepted_rows(false).await;
}

#[tokio::test]
async fn direct_snapshot_open_rejects_unaccepted_rows_in_a_write_ahead_log() {
    assert_direct_open_rejects_unaccepted_rows(true).await;
}

async fn assert_direct_open_rejects_unaccepted_rows(in_write_ahead_log: bool) {
    let source_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_dir.clone());
    let signer = coven_keys::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-image-binding",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create the snapshot Store");
    let device = store
        .open_into(&source, source_dir)
        .await
        .expect("open the snapshot publisher");
    device
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish the accepted snapshot");
    let membership = device.membership_for_test().await.unwrap();
    let destination = tempfile::tempdir().unwrap();
    let path = destination.path().join("received.db");
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            1,
            &path,
            &signer,
        )
        .await
        .expect("prepare the authenticated snapshot");
    let (image, install) = direct_open_fixture(bootstrap);
    let accepted_image = std::fs::read(image.path()).unwrap();
    let changed = coven_database::DatabaseImageTest::open(image.path()).unwrap();
    if in_write_ahead_log {
        changed.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    }
    changed
        .execute(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('unaccepted-row', 'Not in the accepted image', 1,
                     '0000000001000-0000-owner', '2026-09-09')",
            [],
        )
        .expect("replace the downloaded image with different valid SQLite bytes");
    // Replacing the main image does not remove a prior writer's committed WAL.
    // Keep that writer open to prevent its close from checkpointing those pages.
    if in_write_ahead_log {
        std::fs::write(image.path(), &accepted_image).unwrap();
        let visible: i64 = coven_database::DatabaseImageTest::open(image.path())
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE id = 'unaccepted-row'",
                [],
                |row| row.get(0),
            )
            .expect("a new SQLite reader sees the committed sidecar row");
        assert_eq!(visible, 1);
        assert_eq!(
            coven_protocol::store_commit::ObjectHash::digest(&std::fs::read(image.path()).unwrap()),
            coven_protocol::store_commit::ObjectHash::digest(&accepted_image),
        );
        let journal_path = std::path::PathBuf::from(format!("{}-wal", image.path().display()));
        assert!(std::fs::metadata(journal_path).unwrap().len() > 0);
    }
    let changed_image = std::fs::read(image.path()).unwrap();
    let error = match Database::open_initialized_store(
        image.path(),
        &install,
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "snapshot-image-recipient".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        coven_database::CovenMigrationPolicy::ApplyPending,
        &crate::sync::test_helpers::test_migrations(),
    ) {
        Ok(_) => panic!("the direct database opener accepted an unauthenticated image"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        coven_database::OpenError::Db(coven_database::DbError::Message(message))
            if message == "snapshot database image differs from its authenticated plaintext hash"
    ));
    assert_eq!(
        std::fs::read(image.path()).unwrap(),
        changed_image,
        "image authentication must precede migrations and installation"
    );
}

#[tokio::test]
async fn exact_v0_snapshot_obeys_writer_coven_migration_policy() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let signer = coven_keys::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        source_store_dir.clone(),
        "snapshot-coven-migration-policy",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create snapshot migration Store");
    let device = store
        .open_into(&source, source_store_dir.clone())
        .await
        .expect("open snapshot migration Store membership");
    let membership = device
        .membership_for_test()
        .await
        .expect("project snapshot migration membership");
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = coven_database::StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture snapshot migration image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        coven_database::StoreDatabase::new(&source)
            .materialized_frontier()
            .await
            .expect("load snapshot migration coverage"),
    )
    .expect("parse snapshot migration coverage");
    let image = coven_database::DatabaseImageTest::from_bytes(&image)
        .expect("open the image before publication");
    image
        .downgrade_coven_schema_to_v0(false)
        .expect("prepare the old schema before authenticating the snapshot");
    let image = image
        .into_bytes()
        .expect("serialize the old snapshot schema");
    device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish snapshot migration image");

    let destination = tempfile::tempdir().expect("snapshot migration destination");
    let apply_path = destination.path().join("apply.db");
    let refuse_path = destination.path().join("refuse.db");
    let floor = coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec());
    let apply = store
        .prepare_snapshot_bootstrap(&floor, 1, &apply_path, &signer)
        .await
        .expect("prepare apply snapshot bootstrap");
    let refuse = store
        .prepare_snapshot_bootstrap(&floor, 1, &refuse_path, &signer)
        .await
        .expect("prepare refuse snapshot bootstrap");
    let (apply_image, apply_install) = direct_open_fixture(apply);
    let (refuse_image, refuse_install) = direct_open_fixture(refuse);
    assert_v0_uninitialized(apply_image.path());
    assert_v0_uninitialized(refuse_image.path());

    let tables = crate::sync::test_helpers::test_synced_tables();
    let migrations = crate::sync::test_helpers::test_migrations();
    let applied = Database::open_initialized_store(
        apply_image.path(),
        &apply_install,
        tables.clone(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "apply-snapshot-migration".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        coven_database::CovenMigrationPolicy::ApplyPending,
        &migrations,
    )
    .expect("apply pending Coven snapshot migration");
    drop(applied);
    assert_current_initialized(apply_image.path());

    let error = match Database::open_initialized_store(
        refuse_image.path(),
        &refuse_install,
        tables,
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "refuse-snapshot-migration".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        coven_database::CovenMigrationPolicy::RefusePending,
        &migrations,
    ) {
        Ok(_) => panic!("refuse pending Coven snapshot migration"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        coven_database::OpenError::CovenMigration(coven_database::CovenMigrationError::Pending {
            current: 0,
            target: 2
        })
    ));
    assert_v0_uninitialized(refuse_image.path());
}

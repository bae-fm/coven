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
    .with_circle_installs(Vec::new());
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
    coven_database::DatabaseImageTest::open(apply_image.path())
        .expect("open apply snapshot fixture")
        .downgrade_coven_schema_to_v0(false)
        .expect("downgrade apply snapshot fixture");
    coven_database::DatabaseImageTest::open(refuse_image.path())
        .expect("open refuse snapshot fixture")
        .downgrade_coven_schema_to_v0(false)
        .expect("downgrade refuse snapshot fixture");
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
            target: 1
        })
    ));
    assert_v0_uninitialized(refuse_image.path());
}

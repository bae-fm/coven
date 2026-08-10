use super::*;
use crate::sync::test_helpers::{open_test_db, temp_store_dir, TestStore, TestStoreFixture};

#[tokio::test]
async fn loaded_store_authorization_retains_its_verified_root() {
    let db = open_test_db();
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: fixture,
        storage,
    } = TestStoreFixture::create(&db, "retained-root-authority", signer.clone(), home.clone())
        .await
        .expect("create Store");
    let (_store_dir_temp, store_dir) = temp_store_dir();
    let store = Store::load(
        coven_database::StoreDatabase::new(&db.database),
        storage,
        store_dir,
        signer,
    )
    .await
    .expect("load Store");

    home.remove_exact_object(fixture.root.object.slot());

    store
        .authorize()
        .await
        .expect("authorize from the root verified while loading");
}

#[tokio::test]
async fn failed_owner_anchor_install_does_not_publish_connection_authority() {
    let source = open_test_db();
    let signer = UserKeypair::generate();
    let fixture = TestStore::create(
        &source,
        "owner-anchor-rollback",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create source Store");
    let target = open_test_db();
    target
        .database
        .test_sql(|sql| {
            sql.execute_batch(
                "CREATE TEMP TRIGGER fail_owner_anchor_baseline
                 BEFORE INSERT ON retained_replay_baselines
                 BEGIN
                     SELECT RAISE(ABORT, 'injected owner anchor failure');
                 END",
            )
            .map_err(coven_database::DbError::from)
        })
        .await
        .expect("install owner anchor failure trigger");

    let error = match fixture.open_into(&target).await {
        Ok(_) => panic!("late owner anchor failure must abort Store loading"),
        Err(error) => error,
    };
    assert!(
        error.contains("injected owner anchor failure"),
        "unexpected owner anchor failure: {error}"
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&target.database)
            .local_store_root_ref()
            .await
            .expect("read Store root after rolled-back installation"),
        None,
        "rolled-back Store authority was published into the connection cache",
    );

    target
        .database
        .test_sql(|sql| {
            sql.execute_batch("DROP TRIGGER fail_owner_anchor_baseline")
                .map_err(coven_database::DbError::from)
        })
        .await
        .expect("remove owner anchor failure trigger");
    fixture
        .open_into(&target)
        .await
        .expect("retry Store loading after rollback");
    assert_eq!(
        coven_database::StoreDatabase::new(&target.database)
            .local_store_root_ref()
            .await
            .expect("read Store root after committed retry"),
        Some(fixture.root.clone()),
        "the connection must replace its earlier absent read with the committed authority",
    );
}

#[tokio::test]
async fn committed_owner_anchor_publishes_its_verified_replay_baseline() {
    let db = open_test_db();
    let signer = UserKeypair::generate();
    let fixture = TestStore::create(
        &db,
        "retained-replay-authority",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");

    db.database.remove_retained_replay_baseline_for_test().await;

    coven_database::StoreDatabase::new(&db.database)
        .validated_store_owner(&fixture.root)
        .await
        .expect("use replay baseline verified during owner installation");
}

#[tokio::test]
async fn failed_owner_recovery_materialization_does_not_publish_registration_authority() {
    let source = open_test_db();
    let owner = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture = TestStore::create(
        &source,
        "owner-recovery-authority-rollback",
        owner,
        home.clone(),
    )
    .await
    .expect("create source Store");
    let target = open_test_db();
    let target_device = fixture.open_into(&target).await.expect("open target Store");
    let authority = fixture.founder_recovery_authority().await;
    target.database.fail_next_merge_materialization_at(
        coven_database::MergeMaterializationFailurePoint::SummaryMaterialization,
    );

    let mut recovery = target_device
        .owner_recovery_for_test()
        .await
        .expect("bind Owner recovery");
    let error = recovery
        .recover_owner_device(&authority)
        .await
        .expect_err("injected materialization failure must roll back Owner recovery");
    assert!(
        error.to_string().contains("injected failure"),
        "unexpected Owner recovery failure: {error}"
    );

    let database = coven_database::StoreDatabase::new(&target.database);
    let staged = database
        .latest_local_store_device_registration()
        .await
        .expect("load staged recovery registration")
        .expect("recovery registration remains staged for retry");
    let registration = coven_protocol::store_commit::StoreDeviceRegistration::parse_at(
        &staged.registration_bytes,
        &fixture.root,
        staged.device_id,
    )
    .expect("parse staged recovery registration");
    let reference = coven_protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        staged.prepared.reference().clone(),
    );
    assert!(
        database
            .activated_store_device_registration(reference.clone())
            .await
            .is_err(),
        "rolled-back recovery registration was published into the connection cache",
    );

    let publication = database
        .owner_recovery_publication()
        .await
        .expect("load staged Owner recovery publication")
        .expect("failed materialization retains the exact publication");
    let head_slot = publication.head.prepared.reference().slot().clone();
    home.replace_exact_object(&head_slot, b"competing Owner recovery head".to_vec());
    let collision = recovery
        .recover_owner_device(&authority)
        .await
        .expect_err("retry must refuse a different object in the staged head slot");
    assert!(
        collision
            .to_string()
            .contains("slot contains different bytes"),
        "unexpected Owner recovery collision: {collision}"
    );
    home.replace_exact_object(
        &head_slot,
        publication.head.prepared.stored_bytes().to_vec(),
    );

    let recovered = recovery
        .recover_owner_device(&authority)
        .await
        .expect("retry Owner recovery after rollback");
    assert_eq!(recovered, reference);
    database
        .activated_store_device_registration(reference)
        .await
        .expect("committed recovery registration becomes connection authority");
}

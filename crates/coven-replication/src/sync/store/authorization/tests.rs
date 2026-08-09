use super::*;
use crate::sync::test_helpers::{open_test_db, temp_store_dir, TestStore};

#[tokio::test]
async fn loaded_store_authorization_retains_its_verified_root() {
    let db = open_test_db();
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let fixture = TestStore::create(&db, "retained-root-authority", signer.clone(), home.clone())
        .await
        .expect("create Store");
    let (_store_dir_temp, store_dir) = temp_store_dir();
    let store = Store::load(
        coven_database::StoreDatabase::new(&db),
        fixture.storage(),
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
        coven_database::StoreDatabase::new(&target)
            .local_store_root_ref()
            .await
            .expect("read Store root after rolled-back installation"),
        None,
        "rolled-back Store authority was published into the connection cache",
    );

    target
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
}

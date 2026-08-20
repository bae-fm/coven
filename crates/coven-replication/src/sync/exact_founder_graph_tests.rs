use crate::sync::test_helpers::{test_cloud_home, TestStore};
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn created_merge_store_immediately_has_its_exact_founder_chain() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &db,
        db_store_dir.clone(),
        "exact-founder-graph",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store with founder graph");
    store
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("created Store founder chain is immediately readable");
}

#[tokio::test]
async fn opened_store_persists_the_exact_root_needed_by_membership_pull() {
    let creator_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let creator_db = crate::sync::test_helpers::open_test_db(creator_db_store_dir.clone());
    let founder = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &creator_db,
        creator_db_store_dir.clone(),
        "opened-exact-root",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create source Store");
    let opened_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let opened_db = crate::sync::test_helpers::open_test_db(opened_db_store_dir.clone());
    store
        .open_into(&opened_db, opened_db_store_dir.clone())
        .await
        .expect("opened Store membership uses its durable exact root");
}

#[tokio::test]
async fn opened_store_cannot_mint_a_second_founder_registration() {
    let creator_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let creator_db = crate::sync::test_helpers::open_test_db(creator_db_store_dir.clone());
    let founder = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &creator_db,
        creator_db_store_dir.clone(),
        "one-founder-registration",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create source Store");
    let opened_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let opened_db = crate::sync::test_helpers::open_test_db(opened_db_store_dir.clone());
    let creates_before = home.exact_create_count();
    let opened = store
        .open_into(&opened_db, opened_db_store_dir.clone())
        .await
        .expect("open existing Store through its founder registration");

    assert_eq!(home.exact_create_count(), creates_before);
    let local_registrations = opened_db
        .table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "local_store_device_registration",
        ))
        .await
        .expect("count local Store device registrations");
    let activations = opened_db
        .table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "store_device_registration_activations",
        ))
        .await
        .expect("count Store device registration activations");
    assert_eq!(local_registrations, 1);
    assert_eq!(activations, 1);

    let rebound = store
        .bind_device_in(&opened_db, opened_db_store_dir.clone(), &founder)
        .await
        .expect("bind the installed founder registration");
    assert_eq!(rebound.device_id(), opened.device_id());
    assert_eq!(home.exact_create_count(), creates_before);
    assert_eq!(
        opened_db
            .table_row_count_for_test(coven_database::DatabaseTestTable::named(
                "local_store_device_registration",
            ))
            .await
            .expect("recount local Store device registrations"),
        local_registrations,
    );
    assert_eq!(
        opened_db
            .table_row_count_for_test(coven_database::DatabaseTestTable::named(
                "store_device_registration_activations",
            ))
            .await
            .expect("recount Store device registration activations"),
        activations,
    );
}

#[tokio::test]
async fn opened_store_pulls_a_production_commit_through_exact_refs() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_store_dir.clone(),
        "opened-exact-pull",
        founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create source Store");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('note-1', 'exact pull', 1, '0000000001000-0000-source', '2026-07-16')",
        )
        .await;
    assert!(store
        .publish_pending(&source, &source_store_dir)
        .await
        .expect("publish production Store commit"));

    let destination_store_dir = crate::sync::test_helpers::test_store_dir();
    let destination = crate::sync::test_helpers::open_test_db(destination_store_dir.clone());
    let (_, result) = store.pull_into(&destination, &destination_store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(
        destination
            .query_test_text("SELECT title FROM notes WHERE id = 'note-1'")
            .await,
        "exact pull"
    );

    source
        .execute_test_host_write(
            "UPDATE notes SET title = 'exact successor', \
         _updated_at = '0000000002000-0000-source' WHERE id = 'note-1'",
        )
        .await;
    assert!(store
        .publish_pending(&source, &source_store_dir)
        .await
        .expect("publish exact successor commit"));
    let (_, successor) = store.pull_into(&destination, &destination_store_dir).await;
    assert_eq!(successor.changesets_applied, 1);
    assert_eq!(
        destination
            .query_test_text("SELECT title FROM notes WHERE id = 'note-1'")
            .await,
        "exact successor"
    );
}

#[tokio::test]
async fn store_creation_installs_generation_zero_replay_baseline() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let store = TestStore::create(
        &db,
        db_store_dir.clone(),
        "retained-replay-genesis",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let baseline = coven_database::StoreDatabase::new(&db)
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");

    assert_eq!(baseline.schema_version, db.schema_version());
    assert_eq!(baseline.routing_hash, db.sync_routing_hash());
    match &baseline.authority {
        coven_database::RetainedReplayAuthority::Genesis(authority) => {
            assert_eq!(authority.store_root, store.root())
        }
        coven_database::RetainedReplayAuthority::InstalledSnapshot(_) => {
            panic!("Store creation installed a snapshot replay baseline")
        }
    }
}

/// The baseline's database image and authority are payloads its row names: the
/// row claims both hashes, the payload store holds exactly those bytes, and a
/// baseline whose image bytes are gone fails to load instead of
/// producing an image from anywhere else.
#[tokio::test]
async fn generation_zero_replay_baseline_names_its_owned_payloads() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    TestStore::create(
        &db,
        db_store_dir.clone(),
        "retained-replay-payloads",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let database = coven_database::StoreDatabase::new(&db);
    let baseline = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");
    let authority_bytes = baseline
        .canonical_authority_bytes()
        .expect("canonical authority bytes");
    let authority_hash = coven_protocol::store_commit::ObjectHash::digest(&authority_bytes);

    let claimed = database
        .retained_replay_payload_claims_for_test()
        .await
        .expect("read baseline payload claims");
    let mut expected = vec![baseline.image_payload_hash, authority_hash];
    expected.sort();
    assert_eq!(claimed, expected);

    let image_bytes = database
        .payload_for_test(baseline.image_payload_hash)
        .await
        .expect("read image payload");
    assert_eq!(
        coven_protocol::store_commit::ObjectHash::digest(&image_bytes),
        baseline.image_payload_hash
    );
    assert_eq!(
        database
            .payload_for_test(authority_hash)
            .await
            .expect("read authority payload"),
        authority_bytes
    );

    database
        .remove_payload_bytes_for_test(baseline.image_payload_hash)
        .await
        .expect("remove image payload");
    let error = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect_err("a baseline whose image payload is gone must not load");
    assert!(
        error.to_string().contains("absent from the spool"),
        "{error}"
    );

    database
        .install_payload_for_test(image_bytes)
        .await
        .expect("restore image payload");
    database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("baseline loads again once its image payload is back");
}

#[tokio::test]
async fn generation_zero_replay_baseline_rejects_an_image_payload_under_the_wrong_hash() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    TestStore::create(
        &db,
        db_store_dir.clone(),
        "retained-replay-content-address",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let database = coven_database::StoreDatabase::new(&db);
    let baseline = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("load installed replay baseline");

    database
        .corrupt_payload_for_test(
            baseline.image_payload_hash,
            b"not the content-addressed replay image".to_vec(),
        )
        .await
        .expect("replace replay image payload bytes");

    let error = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect_err("a replay image payload under the wrong hash must be rejected");
    assert!(
        error.to_string().contains("contains bytes hashing to"),
        "{error}"
    );
}

/// Replacing the baseline's authority replaces its claim set, and the flow that
/// drops the last claim on the superseded payload pays for it before returning:
/// the old authority payload is gone, the image the row still names is not.
#[tokio::test]
async fn replacing_the_replay_authority_deletes_the_superseded_payload() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    TestStore::create(
        &db,
        db_store_dir.clone(),
        "retained-replay-authority-swap",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let database = coven_database::StoreDatabase::new(&db);
    let baseline = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");
    let superseded = coven_protocol::store_commit::ObjectHash::digest(
        &baseline
            .canonical_authority_bytes()
            .expect("canonical authority bytes"),
    );
    assert!(database
        .has_payload_for_test(superseded)
        .await
        .expect("check superseded authority payload"));

    database
        .replace_generation_zero_replay_authority_for_test(b"{\"kind\":\"other\"}".to_vec())
        .await
        .expect("replace retained replay authority");

    assert!(!database
        .has_payload_for_test(superseded)
        .await
        .expect("check removed authority payload"));
    assert!(database
        .has_payload_for_test(baseline.image_payload_hash)
        .await
        .expect("check retained image payload"));
    assert_eq!(
        database
            .owed_payload_cleanup()
            .await
            .expect("read owed payload cleanup"),
        Vec::new()
    );
}

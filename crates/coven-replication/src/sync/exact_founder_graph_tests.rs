use crate::sync::test_helpers::{open_test_db, temp_store_dir, test_cloud_home, TestStore};
use coven_keys::keys::UserKeypair;

trait ExactFounderGraphDatabaseOps {
    async fn table_count(&self, table: coven_database::DatabaseTestTable) -> i64;
}

impl ExactFounderGraphDatabaseOps for coven_database::Database {
    async fn table_count(&self, table: coven_database::DatabaseTestTable) -> i64 {
        self.test_sql(move |database| database.table_row_count(table))
            .await
            .expect("count lifecycle rows")
    }
}

#[tokio::test]
async fn created_merge_store_immediately_has_its_exact_founder_chain() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &db,
        "exact-founder-graph",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store with founder graph");
    store
        .open_into(&db)
        .await
        .expect("created Store founder chain is immediately readable");
}

#[tokio::test]
async fn opened_store_persists_the_exact_root_needed_by_membership_pull() {
    let creator_db = open_test_db();
    let founder = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &creator_db,
        "opened-exact-root",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create source Store");
    let opened_db = open_test_db();
    store
        .open_into(&opened_db)
        .await
        .expect("opened Store membership uses its durable exact root");
}

#[tokio::test]
async fn opened_store_cannot_mint_a_second_founder_registration() {
    let creator_db = open_test_db();
    let founder = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &creator_db,
        "one-founder-registration",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create source Store");
    let opened_db = open_test_db();
    let creates_before = home.exact_create_count();
    let opened = store
        .open_into(&opened_db)
        .await
        .expect("open existing Store through its founder registration");

    assert_eq!(home.exact_create_count(), creates_before);
    let local_registrations = opened_db
        .database
        .table_count(coven_database::DatabaseTestTable::named(
            "local_store_device_registration",
        ))
        .await;
    let activations = opened_db
        .database
        .table_count(coven_database::DatabaseTestTable::named(
            "store_device_registration_activations",
        ))
        .await;
    assert_eq!(local_registrations, 1);
    assert_eq!(activations, 1);

    let rebound = store
        .bind_device(&opened_db, &founder)
        .await
        .expect("bind the installed founder registration");
    assert_eq!(rebound.device_id, opened.device_id);
    assert_eq!(home.exact_create_count(), creates_before);
    assert_eq!(
        opened_db
            .database
            .table_count(coven_database::DatabaseTestTable::named(
                "local_store_device_registration",
            ))
            .await,
        local_registrations,
    );
    assert_eq!(
        opened_db
            .database
            .table_count(coven_database::DatabaseTestTable::named(
                "store_device_registration_activations",
            ))
            .await,
        activations,
    );
}

#[tokio::test]
async fn opened_store_pulls_a_production_commit_through_exact_refs() {
    let source = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        "opened-exact-pull",
        founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create source Store");
    source
        .database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('note-1', 'exact pull', 1, '0000000001000-0000-source', '2026-07-16')",
        )
        .await;
    let (_source_temp, source_dir) = temp_store_dir();
    assert!(store
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish production Store commit"));

    let destination = open_test_db();
    let (_destination_temp, destination_dir) = temp_store_dir();
    let (_, result) = store.pull_into(&destination, &destination_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(
        destination
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'note-1'")
            .await,
        "exact pull"
    );

    source
        .database
        .execute_test_host_write(
            "UPDATE notes SET title = 'exact successor', \
         _updated_at = '0000000002000-0000-source' WHERE id = 'note-1'",
        )
        .await;
    assert!(store
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish exact successor commit"));
    let (_, successor) = store.pull_into(&destination, &destination_dir).await;
    assert_eq!(successor.changesets_applied, 1);
    assert_eq!(
        destination
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'note-1'")
            .await,
        "exact successor"
    );
}

#[tokio::test]
async fn store_creation_installs_generation_zero_replay_baseline() {
    let db = open_test_db();
    let store = TestStore::create(
        &db,
        "retained-replay-genesis",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let baseline = coven_database::StoreDatabase::new(&db.database)
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");

    assert_eq!(baseline.schema_version, db.database.schema_version());
    assert_eq!(baseline.routing_hash, db.database.sync_routing_hash());
    match &baseline.authority {
        coven_database::RetainedReplayAuthority::Genesis(authority) => {
            assert_eq!(authority.store_root, store.root)
        }
        coven_database::RetainedReplayAuthority::StableSnapshot(_) => {
            panic!("Store creation installed a snapshot replay baseline")
        }
    }
    baseline
        .validate_image(&db.store_dir)
        .expect("validate replay image");
}

/// The baseline's database image and authority are payload files its row names,
/// not columns it carries: the row claims both hashes, the files hold exactly
/// those bytes, and a baseline whose image file is gone fails to load instead of
/// producing an image from anywhere else.
#[tokio::test]
async fn generation_zero_replay_baseline_names_its_payloads_in_the_spool() {
    let db = open_test_db();
    TestStore::create(
        &db,
        "retained-replay-payloads",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let database = coven_database::StoreDatabase::new(&db.database);
    let store_dir = db.store_dir.clone();
    let baseline = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");
    let authority_bytes = baseline
        .canonical_authority_bytes()
        .expect("canonical authority bytes");
    let authority_hash = coven_protocol::store_commit::ObjectHash::digest(&authority_bytes);

    let claimed = database
        .payload_owner_claims(coven_database::payload_spool::RETAINED_REPLAY_BASELINE_OWNER_KEY)
        .await
        .expect("read baseline payload claims");
    let mut expected = vec![baseline.image_hash, authority_hash];
    expected.sort();
    assert_eq!(claimed, expected);

    let image_path = store_dir.payload_spool_path(baseline.image_hash);
    let image_bytes = std::fs::read(&image_path).expect("read image payload");
    assert_eq!(
        coven_protocol::store_commit::ObjectHash::digest(&image_bytes),
        baseline.image_hash
    );
    assert_eq!(
        std::fs::read(store_dir.payload_spool_path(authority_hash))
            .expect("read authority payload"),
        authority_bytes
    );

    std::fs::remove_file(&image_path).expect("remove image payload");
    let error = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect_err("a baseline whose image payload is gone must not load");
    assert!(
        error.to_string().contains("absent from the spool"),
        "{error}"
    );

    std::fs::write(&image_path, &image_bytes).expect("restore image payload");
    database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("baseline loads again once its image payload is back");
}

/// Replacing the baseline's authority replaces its claim set, and the flow that
/// drops the last claim on the superseded payload pays for it before returning:
/// the old authority file is gone, the image the row still names is not.
#[tokio::test]
async fn replacing_the_replay_authority_deletes_the_superseded_payload() {
    let db = open_test_db();
    TestStore::create(
        &db,
        "retained-replay-authority-swap",
        UserKeypair::generate(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let database = coven_database::StoreDatabase::new(&db.database);
    let store_dir = db.store_dir.clone();
    let baseline = database
        .generation_zero_replay_baseline_for_test()
        .await
        .expect("Store creation installs a retained replay baseline");
    let superseded = coven_protocol::store_commit::ObjectHash::digest(
        &baseline
            .canonical_authority_bytes()
            .expect("canonical authority bytes"),
    );
    assert!(store_dir.payload_spool_path(superseded).is_file());

    database
        .replace_generation_zero_replay_authority_for_test(b"{\"kind\":\"other\"}".to_vec())
        .await
        .expect("replace retained replay authority");

    assert!(
        !store_dir.payload_spool_path(superseded).exists(),
        "the superseded authority payload outlived the last row naming it"
    );
    assert!(
        store_dir.payload_spool_path(baseline.image_hash).is_file(),
        "the image the row still names must survive its authority being replaced"
    );
    assert_eq!(
        database
            .owed_payload_spool_cleanup()
            .await
            .expect("read owed payload cleanup"),
        Vec::new()
    );
}

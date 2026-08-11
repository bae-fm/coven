use std::sync::Arc;

use super::*;
use crate::sync::test_helpers::{temp_store_dir, test_migrations, test_synced_tables};
use coven_database::Database;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

fn store_database(database: &Database) -> StoreDatabase {
    StoreDatabase::new(database)
}

#[tokio::test]
async fn created_merge_store_immediately_has_its_exact_founder_chain() {
    let home = InMemoryCloudHome::new();
    let founder = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "exact-founder-graph",
        founder.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let store_dir = db_store_dir.clone();

    let initialized = crate::sync::store::Store::create(
        store_database(&db),
        storage,
        store_dir,
        "0000000000001-0000-founder",
        &founder,
    )
    .await
    .expect("create Store with founder graph");
    let root_ref = store_database(&db)
        .local_store_root_ref()
        .await
        .expect("read exact Store root")
        .expect("created Store root exists");

    let (store, _device_id) = initialized.into_parts();
    let membership = store
        .membership_for_test()
        .await
        .expect("created Store founder chain is immediately readable");
    assert!(membership.is_founded_by(&coven_keys::keys::public_key_hex(&founder)));
    assert_eq!(store.store_root(), &root_ref);
}

#[tokio::test]
async fn merge_store_creation_failure_removes_every_founder_object_before_returning() {
    for failing_create in 1..=5 {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = Arc::new(CloudSyncConnection::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            format!("founder-rollback-{failing_create}"),
            founder.clone(),
        ));
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let (_store_dir_temp, store_dir) = temp_store_dir();
        let timestamp = "0000000000001-0000-founder";
        let staged = FounderStoreCreation::begin(
            store_database(&db),
            storage.clone(),
            &store_dir,
            crate::sync::store::blob::StoreBlobCache::new(store_database(&db), store_dir.clone()),
            timestamp,
            &founder,
        )
        .await
        .stage()
        .await
        .expect("stage exact founder graph");
        let graph = staged.graph.clone();
        let mut exact_objects = vec![
            graph.root.prepared.reference().clone(),
            graph.registration.prepared.reference().clone(),
            graph.initial_ack.prepared.reference().clone(),
        ];
        exact_objects.push(graph.membership.entry.prepared.reference().clone());
        exact_objects.push(graph.membership.head.prepared.reference().clone());
        home.fail_exact_create_before_call(failing_create);

        assert!(
            staged.publish().await.is_err(),
            "injected founder publication failure must abort creation"
        );

        for object in &exact_objects {
            assert!(
                home.get(object.slot().logical_key()).is_none(),
                "founder object {} remains after create call {failing_create} failed",
                object.slot().logical_key(),
            );
        }
        crate::sync::store::Store::create(
            store_database(&db),
            storage.clone(),
            store_dir,
            timestamp,
            &founder,
        )
        .await
        .expect("retry creates the Store after complete rollback");
    }
}

#[tokio::test]
async fn failed_founder_rollback_is_resumed_before_publication_retry() {
    let home = InMemoryCloudHome::new();
    let founder = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "founder-rollback-retry",
        founder.clone(),
    ));
    let temp = tempfile::tempdir().expect("create founder rollback database directory");
    let path = temp.path().join("founder-rollback.sqlite");
    let open = || {
        Database::open(
            &path,
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "founder-rollback-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open founder rollback database")
    };
    let db = open();
    let (_store_dir_temp, store_dir) = temp_store_dir();
    let timestamp = "0000000000001-0000-founder";
    let staged = FounderStoreCreation::begin(
        store_database(&db),
        storage.clone(),
        &store_dir,
        crate::sync::store::blob::StoreBlobCache::new(store_database(&db), store_dir.clone()),
        timestamp,
        &founder,
    )
    .await
    .stage()
    .await
    .expect("stage exact founder graph");
    home.fail_exact_create_before_call(3);
    home.fail_exact_delete_on_call(1);

    let failure = match staged.publish().await {
        Err(error) => error,
        Ok(_) => panic!("failed exact deletion must fail the creation call"),
    };
    assert!(failure.to_string().contains("rollback"));
    drop(db);
    let db = open();

    crate::sync::store::Store::create(
        store_database(&db),
        storage.clone(),
        store_dir,
        timestamp,
        &founder,
    )
    .await
    .expect("retry resumes rollback before publishing the founder graph");
}

#[tokio::test]
async fn concurrent_store_creation_calls_do_not_rollback_each_other() {
    let home = InMemoryCloudHome::new();
    let founder = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "concurrent-founder-publication",
        founder.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let store_dir = db_store_dir.clone();
    let timestamp = "0000000000001-0000-founder";
    let staged = FounderStoreCreation::begin(
        store_database(&db),
        storage.clone(),
        &store_dir,
        crate::sync::store::blob::StoreBlobCache::new(store_database(&db), store_dir.clone()),
        timestamp,
        &founder,
    )
    .await
    .stage()
    .await
    .expect("stage exact founder graph");
    drop(staged);
    let deletes_before = home.deletes_seen();
    let (reached, release) = home.pause_after_exact_create_call(1);
    let first_db = db.clone();
    let first_storage = storage.clone();
    let first_founder = founder.clone();
    let first_store_dir = store_dir.clone();
    let first = tokio::spawn(async move {
        crate::sync::store::Store::create(
            store_database(&first_db),
            first_storage,
            first_store_dir,
            timestamp,
            &first_founder,
        )
        .await
    });
    reached.notified().await;
    let second_db = db.clone();
    let second_storage = storage.clone();
    let second_founder = founder.clone();
    let second_store_dir = store_dir;
    let second = tokio::spawn(async move {
        crate::sync::store::Store::create(
            store_database(&second_db),
            second_storage,
            second_store_dir,
            timestamp,
            &second_founder,
        )
        .await
    });
    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "second creation call bypassed founder publication serialization",
    );
    release.notify_one();

    first
        .await
        .expect("first creation task joins")
        .expect("first creation call succeeds");
    second
        .await
        .expect("second creation task joins")
        .expect("second creation call observes the activated founder graph");
    assert_eq!(home.deletes_seen(), deletes_before);
}

#[tokio::test]
async fn founder_rollback_preserves_a_different_object_in_the_reserved_slot() {
    let home = InMemoryCloudHome::new();
    let founder = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "founder-rollback-slot-collision",
        founder.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (_store_dir_temp, store_dir) = temp_store_dir();
    let timestamp = "0000000000001-0000-founder";
    let staged = FounderStoreCreation::begin(
        store_database(&db),
        storage,
        &store_dir,
        crate::sync::store::blob::StoreBlobCache::new(store_database(&db), store_dir.clone()),
        timestamp,
        &founder,
    )
    .await
    .stage()
    .await
    .expect("stage exact founder graph");
    let competing = b"different Store root occupant".to_vec();
    home.insert_exact_object(
        staged.graph.root.prepared.reference().slot().logical_key(),
        competing.clone(),
    );

    let root_slot = staged
        .graph
        .root
        .prepared
        .reference()
        .slot()
        .logical_key()
        .to_string();
    assert!(
        staged.publish().await.is_err(),
        "different root occupant must prevent Store creation"
    );

    assert_eq!(
        home.get(&root_slot),
        Some(competing),
        "founder rollback erased a different object in the reserved root slot",
    );
}

#[tokio::test]
async fn opaque_store_reopens_exact_founder_root_registration_and_ack() {
    let home = InMemoryCloudHome::new();
    let founder = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(coven_keys::encryption::EncryptionService::from_key(
            [41; 32],
        )),
        BlobPathScheme::Hashed,
        "opaque-founder-graph",
        founder.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (_store_dir_temp, store_dir) = temp_store_dir();

    crate::sync::store::Store::create(
        store_database(&db),
        storage.clone(),
        store_dir.clone(),
        "0000000000001-0000-opaque-founder",
        &founder,
    )
    .await
    .expect("create opaque Store");
    let root_ref = store_database(&db)
        .local_store_root_ref()
        .await
        .expect("read Store root reference")
        .expect("Store root exists");
    let opened = crate::sync::store::Store::open(
        store_database(&db),
        storage,
        store_dir,
        &root_ref,
        &founder,
    )
    .await
    .expect("open exact opaque root");
    let (store, _device_id) = opened.into_parts();
    let founder_slot = store
        .protocol_root_for_test()
        .descriptor
        .founder_registration
        .clone();
    home.clear_exact_reads();
    store
        .load_founder_registration_twice_for_test()
        .await
        .expect("open the founder registration twice in one verifier");
    assert_eq!(
        home.exact_reads()
            .into_iter()
            .filter(|slot| slot == &founder_slot)
            .count(),
        1,
        "one verifier must reuse its exact founder registration"
    );
    let registration = store
        .load_founder_registration_for_test()
        .await
        .expect("open exact opaque founder registration");
    let durable = coven_database::StoreDatabase::new(&db)
        .latest_local_store_device_registration()
        .await
        .expect("read durable founder registration")
        .expect("founder registration exists");
    let ack = store
        .load_store_ack_for_test(&durable.initial_ack_ref, &registration.value)
        .await
        .expect("open exact opaque founder acknowledgement");

    assert_eq!(
        store.protocol_root_for_test().object_hash(),
        root_ref.store_root_hash
    );
    assert_eq!(registration.value.device_id, durable.device_id);
    assert_eq!(ack, durable.initial_ack.value);
}

use super::*;

/// Founder-at-creation + owner anchoring (issue #102): the first cloud connect of
/// a created store writes the founder Owner entry and pins the owner; later
/// connects anchor the chain to that pinned owner; and a wiped or refounded chain
/// is refused as a takeover attempt.
#[tokio::test]
async fn owner_membership_anchor_founds_pins_and_refuses_tampering() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let fixture = TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");
    let (storage, cloud_storage) = fixture;

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in protocol_state",
    );
    let membership = storage
        .bind_device_in(&db, db_store_dir.clone(), &owner)
        .await
        .expect("load founder Store")
        .membership_for_test()
        .await
        .expect("load exact founder membership");
    assert!(
        membership.is_founded_by(&owner_pk),
        "the persisted chain is founded by the owner",
    );

    storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("re-open Store through the pinned founder");
    let owner_before = db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap();
    let graph = store_database(&db)
        .local_store_founder_graph()
        .await
        .expect("read founder graph")
        .expect("founder graph exists");
    let coven_database::DurableFounderMembership { head, .. } = graph.membership;
    cloud_storage
        .delete_protocol_object(head.prepared.reference())
        .await
        .expect("delete exact founder head");
    assert!(
        storage.open_into(&db, db_store_dir.clone()).await.is_err(),
        "a missing exact founder head is refused",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        owner_before,
    );
}

#[tokio::test]
async fn owner_anchor_installs_founder_device_genesis() {
    let owner = UserKeypair::generate();
    let creator_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let creator_db = crate::sync::test_helpers::open_test_db(creator_db_store_dir.clone());
    let storage = TestStore::create(
        &creator_db,
        creator_db_store_dir.clone(),
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");
    let opened_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let opened_db = crate::sync::test_helpers::open_test_db(opened_db_store_dir.clone());
    assert_eq!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read founder device genesis before anchoring"),
        None,
    );

    storage
        .open_into(&opened_db, opened_db_store_dir.clone())
        .await
        .expect("open Store through its founder");

    assert!(
        opened_db
            .get_protocol_state("store_device_genesis_state")
            .await
            .expect("read anchored founder device genesis")
            .is_some(),
        "owner anchoring installs the founder state required by its first commit",
    );
}

/// Founding writes the cloud founder entry before pinning the owner, so a crash
/// between the two leaves a chain founded by our own key with no pin. The next
/// connect completes the pin (the founder is provably ours). A chain founded by a
/// DIFFERENT key with no pin is a first-connect takeover seed and is refused — the
/// branch that previously adopted any founder on trust.
#[tokio::test]
async fn exact_root_reanchors_own_founder_and_open_refuses_foreign_founder() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let directory = tempfile::tempdir().expect("owner reanchor database directory");
    let path = directory.path().join("owner-reanchor.sqlite3");
    let db_store_dir = crate::sync::test_helpers::store_dir_for_test_database(&path);
    let open = || {
        Database::open_synthetic_for_test(
            &path,
            db_store_dir.clone(),
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "owner-reanchor-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open owner reanchor database")
    };
    let db = open();
    let storage = TestStore::create(
        &db,
        db_store_dir.clone(),
        "test-store",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store");
    db.delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .expect("remove local owner pin");
    let reopened = open();
    storage
        .open_into(&reopened, db_store_dir.clone())
        .await
        .expect("re-open Store through its founder");
    assert_eq!(
        reopened
            .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap(),
        Some(owner_pk.clone()),
        "the exact founder restores its owner pin",
    );

    let attacker = UserKeypair::generate();
    let attacker_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let attacker_db = crate::sync::test_helpers::open_test_db(attacker_db_store_dir.clone());
    let seeded = TestStore::create_with_connection(
        &attacker_db,
        attacker_db_store_dir.clone(),
        "foreign-store",
        attacker,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create foreign exact Store");
    let (seeded_store, seeded_connection) = seeded;
    let fresh_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let fresh_db = crate::sync::test_helpers::open_test_db(fresh_db_store_dir.clone());
    assert!(
        crate::sync::store::Store::open(
            store_database(&fresh_db),
            seeded_connection,
            fresh_db_store_dir,
            &seeded_store.root(),
            &owner,
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32]
            )),
        )
        .await
        .is_err(),
        "an exact root founded by another identity is refused",
    );
}

fn cloud_objects(home: &InMemoryCloudHome) -> BTreeMap<String, Vec<u8>> {
    home.keys()
        .into_iter()
        .map(|key| {
            let bytes = home.get(&key).expect("listed cloud object");
            (key, bytes)
        })
        .collect()
}

#[tokio::test]
async fn initializing_plaintext_storage_commits_and_pins_its_founder() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let cipher = CloudCipher::Plaintext;
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let store_dir = db_store_dir.clone();

    cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&db),
        store_dir.clone(),
        storage,
        owner,
        cycle::StoreInitialization::CreateStore,
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await
    .expect("prepare plaintext storage")
    .initialize(None)
    .await
    .expect("initialize plaintext storage");

    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let cursor_count = db
        .protocol_state_prefix_count_for_test("membership_head_cursor/")
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn initialization_refuses_a_founder_entry_without_its_store_protocol_root() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let seeded_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    ));
    let seed_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let seed_db = crate::sync::test_helpers::open_test_db(seed_db_store_dir.clone());
    let seeded_device = crate::sync::test_helpers::TestDevice::create(
        &seed_db,
        seed_db_store_dir.clone(),
        seeded_storage.clone(),
        "test-lib",
        owner.clone(),
    )
    .await
    .expect("create exact Store fixture");
    let root = seeded_device.store_root().clone();
    seeded_storage
        .delete_protocol_object(&root.object)
        .await
        .expect("remove exact Store root while retaining its founder graph");

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let store_dir = db_store_dir.clone();

    let prepared = cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&db),
        store_dir.clone(),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await
    .expect("prepare Store opening");
    let error = match prepared.initialize(None).await {
        Err(error) => error,
        Ok(_) => panic!("an exact founder graph without its Store root must fail loud"),
    };
    assert!(
        matches!(
            error,
            cycle::InitSyncError::Initialization(
                crate::sync::store::StoreInitializationError::ProtocolRoot(_)
            )
        ),
        "{error}"
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .local_store_root_ref()
            .await
            .unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_refuses_a_foreign_founder_without_store_protocol_root() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    ));
    let attacker_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let attacker_db = crate::sync::test_helpers::open_test_db(attacker_db_store_dir.clone());
    let attacker_device = crate::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_db_store_dir.clone(),
        attacker_storage.clone(),
        "test-lib",
        attacker.clone(),
    )
    .await
    .expect("create foreign exact Store fixture");
    let root = attacker_device.store_root().clone();
    attacker_storage
        .delete_protocol_object(&root.object)
        .await
        .expect("remove foreign exact Store root");

    let owner = UserKeypair::generate();
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let store_dir = db_store_dir.clone();
    let prepared = cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&db),
        store_dir.clone(),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await
    .expect("prepare foreign Store opening");
    let error = match prepared.initialize(None).await {
        Err(error) => error,
        Ok(_) => panic!("a foreign founder graph without its exact root must fail loud"),
    };
    assert!(
        matches!(
            error,
            cycle::InitSyncError::Initialization(
                crate::sync::store::StoreInitializationError::ProtocolRoot(_)
            )
        ),
        "{error}"
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
}

#[tokio::test]
async fn initialization_pins_a_committed_self_founder_without_cloud_rewrite() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let seeded_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    ));
    let seed_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let seed_db = crate::sync::test_helpers::open_test_db(seed_db_store_dir.clone());
    let seeded_device = crate::sync::test_helpers::TestDevice::create(
        &seed_db,
        seed_db_store_dir.clone(),
        seeded_storage.clone(),
        "test-lib",
        owner.clone(),
    )
    .await
    .expect("create committed exact Store fixture");
    let root = seeded_device.store_root().clone();
    let cloud_before = cloud_objects(&home);

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner.clone(),
    );
    let store_dir = db_store_dir.clone();
    cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&db),
        store_dir.clone(),
        storage,
        owner,
        cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await
    .expect("prepare committed founder opening")
    .initialize(None)
    .await
    .expect("accept the identity's committed founder");

    assert_eq!(cloud_objects(&home), cloud_before);
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
    );
    let cursor_count = db
        .protocol_state_prefix_count_for_test("membership_head_cursor/")
        .await
        .unwrap();
    assert_eq!(cursor_count, 1);
}

#[tokio::test]
async fn plaintext_initialization_refuses_a_committed_foreign_founder_without_mutation() {
    use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;

    let home = InMemoryCloudHome::new();
    let attacker = UserKeypair::generate();
    let attacker_storage = Arc::new(cycle_cloud_storage(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        attacker.clone(),
    ));
    let attacker_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let attacker_db = crate::sync::test_helpers::open_test_db(attacker_db_store_dir.clone());
    let attacker_device = crate::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_db_store_dir.clone(),
        attacker_storage.clone(),
        "test-lib",
        attacker.clone(),
    )
    .await
    .expect("create committed foreign Store");
    let root = attacker_device.store_root().clone();
    let cloud_before = cloud_objects(&home);

    let victim = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let cipher = CloudCipher::Plaintext;
    let victim_storage = cycle_cloud_storage(
        Arc::new(home.clone()),
        cipher.clone(),
        BlobPathScheme::Plain,
        "test-lib",
        victim.clone(),
    );
    let store_dir = db_store_dir.clone();

    assert!(
        cycle::PreparedSyncComponents::prepare(
            coven_database::StoreDatabase::new(&db),
            store_dir.clone(),
            victim_storage,
            victim,
            cycle::StoreInitialization::OpenStore {
                expected_store_root: root,
            },
            None,
            std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
        )
        .await
        .expect("prepare foreign founder opening")
        .initialize(None)
        .await
        .is_err(),
        "a committed foreign founder prevents initialization",
    );
    assert_eq!(
        db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        None,
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .local_store_root_ref()
            .await
            .unwrap(),
        None,
    );
    let watermark_count = db
        .protocol_state_prefix_count_for_test("membership_head_seq/")
        .await
        .unwrap();
    assert_eq!(watermark_count, 0);
    let cloud_after = cloud_objects(&home);
    assert_eq!(cloud_after, cloud_before, "cloud objects are unchanged");
}

#[tokio::test]
async fn initialization_rejects_an_identity_other_than_the_storage_identity() {
    let owner = UserKeypair::generate();
    let other = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_cloud_storage(
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        owner,
    );
    let store_dir = db_store_dir.clone();

    let result = cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&db),
        store_dir.clone(),
        storage,
        other,
        cycle::StoreInitialization::CreateStore,
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await;

    assert!(matches!(
        result,
        Err(cycle::InitSyncError::StorageIdentityMismatch)
    ));
}

#[tokio::test]
async fn initialization_rejects_incoherent_cipher_and_blob_path_scheme() {
    for (cipher, blob_paths) in [
        (CloudCipher::Plaintext, BlobPathScheme::Hashed),
        (
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Plain,
        ),
    ] {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let storage = Arc::new(cycle_cloud_storage(
            Arc::new(home.clone()),
            cipher.clone(),
            blob_paths,
            "test-lib",
            owner.clone(),
        ));
        let store_dir = db_store_dir.clone();
        db.set_protocol_state(
            coven_protocol::objects::ROTATION_GATE_STATE_KEY,
            "invalid rotation gate",
        )
        .await
        .unwrap();
        assert!(
            cycle::PreparedSyncComponents::prepare(
                coven_database::StoreDatabase::new(&db),
                store_dir.clone(),
                storage.clone(),
                owner,
                cycle::StoreInitialization::CreateStore,
                None,
                std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
            )
            .await
            .is_err(),
            "incoherent at-rest representation must be refused",
        );
        assert!(home.is_empty(), "the cloud is unchanged");
        assert_eq!(
            db.get_protocol_state("owner_pubkey").await.unwrap(),
            None,
            "the local owner is not pinned",
        );
        assert_eq!(
            storage.pending_rotation_generation_for_test(),
            None,
            "the in-memory pending-rotation marker is not restored",
        );
        assert_eq!(
            db.get_protocol_state(coven_protocol::objects::ROTATION_GATE_STATE_KEY)
                .await
                .unwrap(),
            Some("invalid rotation gate".to_string()),
            "the durable pending-rotation state is unchanged",
        );
    }
}

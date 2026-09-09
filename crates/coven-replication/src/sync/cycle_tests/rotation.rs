use super::*;

/// A committed store-key rotation this device has not adopted pauses sealing
/// without taking down the cycle: a pending write that references a
/// host-provided blob stays queued while `rotation_pending` is set. A cycle after
/// adoption publishes the write and uploads its blob under the adopted key.
#[tokio::test]
async fn rotation_pending_defers_a_host_blob_changeset_until_adoption() {
    let keypair = UserKeypair::generate();
    // The live cipher is generation 1; the cloud has committed generation 2.
    let (db, db_store_dir, storage, cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "hponly",
        b"cover",
    )
    .await
    .expect("store host-provided blob");
    let write_id = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read rotation-paused Store write")
        .into_iter()
        .next()
        .expect("host write is queued")
        .write_id
        .clone();

    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    device.mark_rotation_committed_for_test(2).unwrap();

    device
        .run_cycle(None)
        .await
        .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert!(
        db.pending_write_count().await > 0,
        "the host-blob changeset stays queued while sealing is paused",
    );
    let activated_bindings = db
        .exact_row_blob_locator_count_for_test(
            "note_photos",
            "hponly",
            "id",
            "0000000001000-0000-M",
        )
        .await
        .expect("count exact host-blob bindings");
    assert_eq!(
        activated_bindings, 0,
        "rotation pause installs no activated host-blob binding",
    );
    let exact_outbox_rows = db
        .exact_upload_outbox_count_for_test("note_photos", "hponly", "id", "0000000001000-0000-M")
        .await
        .expect("count exact host-blob upload handoffs");
    assert_eq!(
        exact_outbox_rows, 0,
        "rotation pause creates neither a cloud upload nor a Created handoff",
    );
    assert_eq!(
        coven_foundation::store_dir::StoreDir::read_local_blob(
            &db_store_dir,
            "photos",
            "hponly",
            5
        )
        .await
        .expect("read rotation-paused local blob"),
        Some(b"cover".to_vec()),
        "the pending Store write retains its exact local blob source",
    );

    // Adoption clears the retained gate; the first cycle after publishes the
    // queued changeset and uploads its blob.
    device.clear_rotation_gate_for_test();
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the queued changeset publishes on the first cycle after adoption",
    );
    let published = match coven_database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read adopted Store write status")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Store write has an exact commit")
            .clone(),
        status => panic!("adopted Store write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(
        materialized_history_reaches(&db, &stream_id, published.coord.sequence()).await,
        "the published Store write is materialized",
    );
    let activated = db
        .stored_blob_for_row("note_photos", "hponly")
        .await
        .expect("adoption activates the exact host-blob binding");
    cloud_storage
        .verify_blob_object(&activated)
        .await
        .expect("the activated host blob reads back exactly");
    assert_eq!(
        tokio::fs::read(
            &db_store_dir
                .cache_blob_path("photos", activated.locator().locator_hash())
                .expect("host-blob cache path"),
        )
        .await
        .expect("read adopted host-blob cache"),
        b"cover",
        "CacheEager policy retains the published blob in the evictable cache",
    );
    assert!(
        db_store_dir
            .read_local_blob("photos", "hponly", 5)
            .await
            .expect("read adopted local source")
            .is_none(),
        "publication removes the superseded local source",
    );
}

/// The sibling of the host-blob-changeset case for the other newly-gated seal
/// path: a ready host-provided make_remote intent. With a rotation pending,
/// `complete_host_provided_make_remotes` is skipped — the root's gate does not
/// flip, its blob is not sealed, and the intent stays queued — yet the cycle
/// completes. The first cycle after adoption flips the gate, uploads the blob,
/// and consumes the intent. Without the gate this cycle would abort at
/// the store-key seal gate before the pull.
#[tokio::test]
async fn rotation_pending_defers_a_ready_make_remote_intent_until_adoption() {
    let keypair = UserKeypair::generate();
    let (db, db_store_dir, storage, _cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Release', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "hponly",
        b"cover",
    )
    .await
    .expect("store host-provided blob");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        coven_database::StoreDatabase::new(&db),
        db_store_dir.clone(),
    )
    .make_remote("notes", "n1", "Notes Root", false)
    .await
    .expect("queue the host-provided make_remote intent");

    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    device.mark_rotation_committed_for_test(2).unwrap();

    device
        .run_cycle(None)
        .await
        .expect("the cycle completes; a pending rotation pauses sealing, it does not abort");

    assert_eq!(
        db.query_test_text("SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'")
            .await,
        "0",
        "the make_remote gate does not flip while sealing is paused",
    );
    assert!(
        db.make_remote_intent_present("notes", "n1").await,
        "the make_remote intent stays queued while sealing is paused",
    );
    assert!(
        !storage
            .stored_blob_exists(&db, "note_photos", "hponly")
            .await,
        "no host-provided blob is sealed to the cloud while sealing is paused",
    );

    // Adoption clears the pause; the first cycle after completes the intent.
    device.clear_rotation_gate_for_test();
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("first cycle after adoption succeeds");
    assert_eq!(
        db.query_test_text("SELECT CAST(shared AS TEXT) FROM notes WHERE id = 'n1'")
            .await,
        "1",
        "the make_remote gate flips on the first cycle after adoption",
    );
    assert!(
        !db.make_remote_intent_present("notes", "n1").await,
        "completing the make_remote consumes its intent",
    );
    assert!(
        storage
            .stored_blob_exists(&db, "note_photos", "hponly")
            .await,
        "the host-provided blob uploads on the first cycle after adoption",
    );
}

#[tokio::test]
async fn ready_make_remote_provider_transport_is_offline() {
    let keypair = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
    );
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('transport-root', 'Root', NULL, 0, \
                 '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('transport-blob', 'transport-root', 'cover', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "transport-blob",
        b"cover",
    )
    .await
    .expect("store host-provided blob");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        coven_database::StoreDatabase::new(&db),
        db_store_dir.clone(),
    )
    .make_remote("notes", "transport-root", "Notes Root", false)
    .await
    .expect("queue make_remote intent");
    fail_exact_create_on(&storage, 1);
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");

    let failed = device
        .run_cycle(None)
        .await
        .expect_err("provider transport prevents make_remote completion");

    assert!(
        failed.contains("forced failure before exact create call 1"),
        "unexpected ready make_remote failure: {failed}"
    );
    assert!(
        failed.is_offline(),
        "make_remote transport is offline: {failed}"
    );
}

#[tokio::test]
async fn malformed_durable_pending_rotation_blocks_session_reopen() {
    let directory = tempfile::tempdir().expect("pending-rotation database directory");
    let path = directory.path().join("store.sqlite3");
    let store_dir = crate::sync::test_helpers::store_dir_for_test_database(&path);
    let open = || {
        coven_database::Database::open_synthetic_for_test(
            &path,
            store_dir.clone(),
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "pending-rotation-reopen-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open pending-rotation database")
    };
    let home = coven_storage::InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let encryption = coven_keys::encryption::EncryptionService::from_key([17; 32]);
    let db = open();
    let store_database = coven_database::StoreDatabase::new(&db);
    let storage = coven_storage::CloudSyncConnection::new(
        Arc::new(home.clone()),
        coven_storage::CloudCipher::Encrypted(encryption.clone()),
        coven_storage::BlobPathScheme::Hashed,
        "pending-rotation-reopen",
        signer.clone(),
    );
    let components = crate::sync::cycle::PreparedSyncComponents::prepare(
        store_database.clone(),
        store_dir.clone(),
        storage,
        signer.clone(),
        crate::sync::cycle::StoreInitialization::CreateStore,
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await
    .expect("prepare pending-rotation Store")
    .initialize(None)
    .await
    .expect("initialize pending-rotation Store");
    let root = store_database
        .local_store_root_ref()
        .await
        .expect("read pending-rotation Store root")
        .expect("pending-rotation Store root exists");
    db.set_protocol_state(
        coven_protocol::objects::ROTATION_GATE_STATE_KEY,
        "not-a-rotation-gate",
    )
    .await
    .expect("persist malformed pending rotation");
    drop(components);
    drop(db);

    let reopened = open();
    let storage = coven_storage::CloudSyncConnection::new(
        Arc::new(home),
        coven_storage::CloudCipher::Encrypted(encryption),
        coven_storage::BlobPathScheme::Hashed,
        "pending-rotation-reopen",
        signer.clone(),
    );
    let result = crate::sync::cycle::PreparedSyncComponents::prepare(
        coven_database::StoreDatabase::new(&reopened),
        store_dir.clone(),
        storage,
        signer,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: root,
        },
        None,
        std::sync::Arc::new(crate::sync::test_helpers::TestCustody::default()),
    )
    .await;

    assert!(matches!(
        result,
        Err(crate::sync::cycle::InitSyncError::PendingRotationRestore(_))
    ));
}

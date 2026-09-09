use super::*;

/// A pending cloud upload does not hold back a gated-true changeset: the gate
/// column decides per-row visibility, so a row that is shareable now reaches
/// peers without waiting for unrelated uploads to finish. The gate still cuts a
/// gated-false row, which is what withholds a not-yet-uploaded unit.
#[tokio::test]
async fn pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                let blob_decl =
                    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
                let db_store_dir = crate::sync::test_helpers::test_store_dir();
                let db = crate::sync::test_helpers::open_test_db_with_blob(
                    db_store_dir.clone(),
                    blob_decl.clone(),
                );
                let storage = cycle_test_store(
                    &db,
                    db_store_dir.clone(),
                    &keypair,
                    crate::sync::test_helpers::test_cloud_home(),
                )
                .await;
                storage
                    .retain_store_packages_for_assertion(&db, db_store_dir.clone())
                    .await;
                let peer = UserKeypair::generate();
                storage
                    .admit_member(
                        &db,
                        db_store_dir.clone(),
                        &keypair,
                        &pubkey_hex(&peer),
                        None,
                        coven_protocol::membership::MemberRole::Member,
                        &EncryptionService::from_key([42; 32]),
                        "Test Store",
                    )
                    .await
                    .expect("admit exact pending-upload peer");
                let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
                let db_b = crate::sync::test_helpers::open_test_db_with_blob(
                    db_b_store_dir.clone(),
                    blob_decl,
                );
                let peer_device = storage
                    .activate_joined_device(
                        &db,
                        db_store_dir.clone(),
                        &db_b,
                        db_b_store_dir.clone(),
                        &peer,
                        T0,
                    )
                    .await
                    .expect("activate exact joined test device");
                let device = storage
                    .open_into(&db, db_store_dir.clone())
                    .await
                    .expect("bind exact pending-upload device");
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                )
                .await
                .expect("settle exact pending-upload peer activation");

                // A slow/stuck upload for some OTHER unit is pending the whole time.
                db.seed_stuck_blob_upload_for_test(T0)
                    .await
                    .expect("seed exact pending upload");

                // One shareable note (its blobs are up → gate on) and one still-private note
                // (its blobs aren't up yet → gate off; the host hasn't flipped it).
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
                )
                .await;
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('priv', 'NotYet', NULL, 0, '0000000002000-0000-M', '2026-01-01')",
                )
                .await;

                // The changeset pushes despite the pending upload — no global deferral.
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device,
                )
                .await
                .expect("publish gated-true write beside pending upload");

                // The activated peer pulls: it gets the shareable row, never the gated-false one.
                peer_device
                    .pull_store()
                    .await
                    .expect("pull exact pending-upload peer");
                assert_eq!(
                    db_b.query_test_text("SELECT title FROM notes WHERE id = 'pub'")
                        .await,
                    "Shareable",
                    "the shareable note reaches the peer",
                );
                assert!(
        !db_b
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'priv'")
            .await,
        "a gated-false row is still withheld — that is what holds a not-yet-uploaded unit",
    );
            })
            .await
            .expect("pending-upload gate orchestration");
        })
        .await;
}

/// A gated-false row is withheld until its gate flips on, then it propagates: the
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (coven
/// flips the gate when a manage's blobs land; here the flip is written directly.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                let blob_decl =
                    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
                let db_store_dir = crate::sync::test_helpers::test_store_dir();
                let db = crate::sync::test_helpers::open_test_db_with_blob(
                    db_store_dir.clone(),
                    blob_decl.clone(),
                );
                let storage = cycle_test_store(
                    &db,
                    db_store_dir.clone(),
                    &keypair,
                    crate::sync::test_helpers::test_cloud_home(),
                )
                .await;
                let peer = UserKeypair::generate();
                storage
                    .admit_member(
                        &db,
                        db_store_dir.clone(),
                        &keypair,
                        &pubkey_hex(&peer),
                        None,
                        coven_protocol::membership::MemberRole::Member,
                        &EncryptionService::from_key([42; 32]),
                        "Test Store",
                    )
                    .await
                    .expect("admit exact gate-flip peer");
                let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
                let db_b = crate::sync::test_helpers::open_test_db_with_blob(
                    db_b_store_dir.clone(),
                    blob_decl,
                );
                let peer_device = storage
                    .activate_joined_device(
                        &db,
                        db_store_dir.clone(),
                        &db_b,
                        db_b_store_dir.clone(),
                        &peer,
                        T0,
                    )
                    .await
                    .expect("activate exact joined test device");
                let device = storage
                    .open_into(&db, db_store_dir.clone())
                    .await
                    .expect("bind exact gate-flip device");
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                )
                .await
                .expect("settle exact gate-flip peer activation");

                // A note whose blobs aren't up yet: gate off.
                db.execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
                )
                .await;
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device.clone(),
                )
                .await
                .expect("publish gated-false Store write");

                peer_device
                    .pull_store()
                    .await
                    .expect("pull gated-false Store state");
                assert!(
                    !db_b
                        .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
                        .await,
                    "a gated-false row must not reach a peer",
                );

                // The blobs land; the host flips the gate on. The next cycle re-emits the
                // now-shareable row.
                db.execute_test_host_write(
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
                run_cycle_in_task(
                    Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
                    device,
                )
                .await
                .expect("publish gate-flip Store write");

                // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
                // re-emits it at seq 1. Re-pull from empty positions to pick it up wherever it
                // landed.
                peer_device
                    .pull_store()
                    .await
                    .expect("pull gate-flip Store state");
                assert_eq!(
                    db_b.query_test_text("SELECT title FROM notes WHERE id = 'n1'")
                        .await,
                    "Album Title",
                    "once its gate flips on, the row reaches the peer",
                );
            })
            .await
            .expect("gate-flip propagation orchestration");
        })
        .await;
}

/// The snapshot is the second propagation channel and runs the same row-level
/// gate (`delete_gated_false`), so a pending upload does not withhold it: the
/// snapshot carries the gated-true rows and excludes the gated-false ones, which
/// is the blob-before-row guarantee at snapshot granularity.
#[tokio::test]
async fn snapshot_is_not_withheld_by_pending_uploads() {
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
    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    db.seed_stuck_blob_upload_for_test(T0)
        .await
        .expect("seed exact pending upload");

    let cycle_device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    cycle_device
        .run_cycle(None)
        .await
        .expect("run snapshot cycle");
    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

#[tokio::test]
async fn initial_snapshot_follows_accepted_remote_root_host_blob_writes() {
    let keypair = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_schema(
        db_store_dir.clone(),
        crate::sync::test_helpers::test_synced_tables_remote_root_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        )),
        test_migrations(),
    );
    let fixture = cycle_test_store_fixture(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (storage, cloud_storage) = fixture;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Existing', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('cover1', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "cover1",
        b"cover",
    )
    .await
    .expect("store host-provided blob");

    let cycle_device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    cycle_device
        .run_cycle(None)
        .await
        .expect("run initial snapshot cycle");

    let stored = db
        .stored_blob_for_row("note_photos", "cover1")
        .await
        .expect("the accepted write activates its exact host blob binding");
    cloud_storage
        .verify_blob_object(&stored)
        .await
        .expect("the blob referenced by the initial snapshot exists");
    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "the snapshot metadata publishes after its referenced blob exists",
    );
}

#[tokio::test]
async fn initial_snapshot_reuses_all_accepted_bindings_for_shared_blob_content() {
    let keypair = UserKeypair::generate();
    let tables = vec![SyncedTable::new(
        "assets",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .remote_root()
    .carries_blob(
        BlobDecl::new("assets", Provenance::HostProvided, CacheFill::CacheEager)
            .with_id_column("blob_id"),
    )];
    let migrations = vec![coven_database::Migration::sql(
        1,
        "shared snapshot blob",
        "CREATE TABLE assets (
            id TEXT PRIMARY KEY,
            blob_id TEXT NOT NULL,
            size INTEGER NOT NULL,
            hash TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )];
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db =
        crate::sync::test_helpers::open_test_db_schema(db_store_dir.clone(), tables, migrations);
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let store_dir = db_store_dir.clone();
    let hash = coven_protocol::blob::content_hash(b"shared");
    db.execute_test_host_write(&format!(
        "INSERT INTO assets (id, blob_id, size, hash, _updated_at) VALUES
             ('row-a', 'blob-shared', 6, '{hash}', '0000000001000-0000-M'),
             ('row-b', 'blob-shared', 6, '{hash}', '0000000001000-0000-M')"
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &store_dir,
        "assets",
        "blob-shared",
        b"shared",
    )
    .await
    .expect("store shared snapshot blob");
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open shared blob Store");
    assert!(device
        .publish_pending_store_database()
        .await
        .expect("publish the shared-content rows before snapshot capture"));
    let first = db
        .stored_blob_for_row("assets", "row-a")
        .await
        .expect("first accepted exact blob binding");
    let second = db
        .stored_blob_for_row("assets", "row-b")
        .await
        .expect("second accepted exact blob binding");
    let before_objects = db
        .table_row_count_for_test(coven_database::DatabaseTestTable::named("blob_locators"))
        .await
        .expect("count accepted exact objects");
    let interceptor = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(Arc::clone(&interceptor), device)
        .await
        .expect("snapshot reuses every accepted blob binding");
    assert!(
        interceptor.rejected_blobs().is_empty(),
        "snapshot never uploads a replacement blob"
    );
    assert!(db.latest_store_snapshot_meta().await.is_some());
    assert_eq!(
        db.stored_blob_for_row("assets", "row-a")
            .await
            .expect("first retained binding"),
        first
    );
    assert_eq!(
        db.stored_blob_for_row("assets", "row-b")
            .await
            .expect("second retained binding"),
        second
    );
    assert_eq!(
        db.table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "row_blob_locators"
        ))
        .await
        .expect("snapshot bindings"),
        2
    );
    assert_eq!(
        db.table_row_count_for_test(coven_database::DatabaseTestTable::named("blob_locators"))
            .await
            .expect("snapshot exact objects"),
        before_objects
    );
}

#[tokio::test]
async fn snapshot_reuses_accepted_user_blob_without_source_or_reupload() {
    let signer = UserKeypair::generate();
    let store_dir = test_store_dir();
    let db = open_test_db_with_blob(
        store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let (storage, cloud) =
        cycle_test_store_fixture(&db, store_dir.clone(), &signer, test_cloud_home()).await;
    let external_dir = tempfile::tempdir().expect("external source directory");
    let external_path = external_dir.path().join("audio.flac");
    tokio::fs::write(&external_path, b"AUDIO")
        .await
        .expect("write user source");
    db.execute_test_host_write(&format!(
        "INSERT INTO notes (id, title, shared, _updated_at, created_at)
         VALUES ('root', 'Audio', 0, '0000000001000-0000-owner', '2026-01-01');
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
         VALUES ('audio', 'root', 'audio', 5, '{}', '0000000001000-0000-owner', '2026-01-01')",
        coven_protocol::blob::content_hash(b"AUDIO"),
    ))
    .await;
    let database = StoreDatabase::new(&db);
    database
        .register_external_blob_for_test("note_photos", "audio", &external_path)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(database.clone(), store_dir.clone())
        .make_remote("notes", "root", "Audio", false)
        .await
        .expect("request sharing");
    let device = storage
        .open_into(&db, store_dir)
        .await
        .expect("open user blob Store");
    device
        .drain_uploads(&SystemClock, None, None)
        .await
        .expect("upload user blob");
    device.run_cycle(None).await.expect("publish uploaded row");
    let accepted = db
        .stored_blob_for_row("note_photos", "audio")
        .await
        .expect("accepted binding");
    cloud
        .verify_blob_object(&accepted)
        .await
        .expect("accepted bytes exist");
    tokio::fs::remove_file(&external_path)
        .await
        .expect("remove external source");
    storage.clear_exact_creates();

    let snapshot = publish_current_snapshot(&device, T0).await;

    assert!(
        storage
            .exact_creates()
            .iter()
            .all(|slot| !slot.logical_key().starts_with("audio/")),
        "snapshot publication must reuse the accepted exact blob"
    );
    assert_eq!(
        db.stored_blob_for_row("note_photos", "audio")
            .await
            .expect("preserved binding"),
        accepted
    );
    assert_eq!(
        database
            .latest_local_store_snapshot()
            .await
            .expect("read snapshot")
            .expect("snapshot accepted")
            .meta
            .snapshot_hash(),
        snapshot.snapshot_hash()
    );
    cloud
        .verify_blob_object(&accepted)
        .await
        .expect("accepted bytes remain available");
}

use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn peer_retirement_cannot_delete_a_new_snapshot_candidates_rollup() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let home = std::sync::Arc::new(coven_storage::cloud::test_utils::InMemoryCloudHome::new());
    let (store, _) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-artifact-peer-retirement",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db(peer_dir.clone());
    let candidate_author = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate peer Owner device");
    let administrator = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind first Owner");
    let (_, pulled) = candidate_author
        .pull_store()
        .await
        .expect("observe peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    super::tests::publish_current_snapshot(&candidate_author).await;
    let database = StoreDatabase::new(&peer_database);
    let original = database
        .latest_local_store_snapshot()
        .await
        .expect("read original")
        .expect("original exists");
    let (_, pulled) = administrator
        .pull_store()
        .await
        .expect("peer observes original snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    super::tests::publish_current_snapshot(&administrator).await;
    administrator
        .stand_on_accepted_snapshot()
        .await
        .expect("peer adopts successor");
    let (_, pulled) = candidate_author
        .pull_store()
        .await
        .expect("observe accepted peer snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    candidate_author
        .reclaim_packages()
        .await
        .expect("a device without provider administration authority can finish its reclaim stage");
    assert!(
        home.contains_exact_object(&original.meta.membership_rollup.object),
        "the pending old artifact remains for the provider administrator"
    );
    let mut writer = candidate_author
        .authorize_writer()
        .await
        .expect("authorize new snapshot");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let mut snapshots = writer.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&encryption))
        .await
        .expect("capture new accepted prefix");
    // Image and rollup are uploaded first; metadata is the third exact create.
    // The other device retires the old owner while this candidate is visible.
    let (reached, release) = home.pause_after_exact_create_call(3);
    let publication = snapshots.push_snapshot_cut(cut, "2026-09-08T00:00:03Z".into());
    tokio::pin!(publication);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            _ = reached.notified() => {},
            result = &mut publication => panic!("snapshot returned before metadata barrier: {result:?}"),
        }
    }).await.expect("pause candidate after upload");
    let pending = database
        .outbound_snapshot_publication()
        .await
        .expect("read pending")
        .expect("candidate remains pending");
    administrator
        .reclaim_packages()
        .await
        .expect("peer retires old accepted artifacts");
    let candidate_survives =
        home.contains_exact_object(&pending.meta.value.membership_rollup.object);
    let original_retired = !home.contains_exact_object(&original.meta.membership_rollup.object);
    release.notify_one();
    assert!(
        original_retired,
        "peer must physically retire the superseded rollup"
    );
    assert!(
        candidate_survives,
        "retiring an old accepted snapshot deleted the new candidate's exact rollup"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), &mut publication)
        .await
        .expect("resume snapshot")
        .expect("publish despite peer artifact retirement");
}

#[tokio::test]
async fn accepted_snapshot_retires_old_artifacts_without_publishing_more_history() {
    assert_accepted_snapshot_artifact_retirement(false).await;
}

#[tokio::test]
async fn accepted_snapshot_artifact_retirement_resumes_after_reopen() {
    assert_accepted_snapshot_artifact_retirement(true).await;
}

async fn assert_accepted_snapshot_artifact_retirement(interrupt: bool) {
    let temporary = tempfile::tempdir().expect("durable retirement directory");
    let path = temporary.path().join("publisher.db");
    let directory = test_store_dir();
    let database = open_retirement_database(&path, directory.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "accepted-snapshot-artifact-retirement",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&database, directory.clone(), &signer)
        .await
        .expect("bind snapshot publisher");
    let database = StoreDatabase::new(&database);
    super::tests::publish_current_snapshot(&device).await;
    let previous = database
        .latest_local_store_snapshot()
        .await
        .expect("read first snapshot")
        .expect("first snapshot exists");
    let previous_boundary = database
        .store_current_publication()
        .await
        .expect("read first accepted boundary");
    let previous_publication = previous_boundary
        .record()
        .accepted()
        .expect("first snapshot is accepted")
        .clone();
    super::tests::publish_current_snapshot(&device).await;
    let current = database
        .latest_local_store_snapshot()
        .await
        .expect("read second snapshot")
        .expect("second snapshot exists");
    let current_boundary = database
        .store_current_publication()
        .await
        .expect("read second accepted boundary");
    assert_ne!(previous.reference, current.reference);
    assert_ne!(previous.meta.image.object, current.meta.image.object);
    device
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt accepted successor");
    let previous_objects = [
        previous.meta.image.object.clone(),
        previous.meta.membership_rollup.object.clone(),
        previous.reference.object.clone(),
        previous_publication.object.clone(),
    ];
    if interrupt {
        home.fail_nth_exact_delete_of(
            &previous_objects
                .iter()
                .map(|object| object.slot())
                .collect::<Vec<_>>(),
            2,
        );
        let interrupted = device.reclaim_packages().await;
        assert!(
            interrupted.is_err(),
            "physical deletion failure must remain visible: {interrupted:?}"
        );
        assert_eq!(
            previous_objects
                .iter()
                .filter(|object| home.contains_exact_object(object))
                .count(),
            3,
            "one exact deletion survives interruption"
        );
        assert_eq!(
            database.store_current_publication().await.unwrap().record(),
            current_boundary.record()
        );
        assert!(
            database
                .store_reclaim_operations()
                .await
                .unwrap()
                .is_empty(),
            "artifact deletion needs no new signed authorization operation"
        );
    }
    let reopened = open_retirement_database(&path, directory.clone());
    let device = store
        .bind_device_in(&reopened, directory, &signer)
        .await
        .expect("reopen artifact retirement owner");
    let database = StoreDatabase::new(&reopened);
    device
        .reclaim_packages()
        .await
        .expect("retire obsolete accepted snapshot artifacts");
    for object in &previous_objects {
        assert!(
            !home.contains_exact_object(object),
            "accepted successor must retire exact obsolete artifact {object:?}"
        );
    }
    for object in [
        &current.meta.image.object,
        &current.reference.object,
        &current.meta.membership_rollup.object,
        &current_boundary
            .record()
            .accepted()
            .expect("current accepted entry")
            .object,
    ] {
        assert!(
            home.contains_exact_object(object),
            "current artifact remains: {object:?}"
        );
    }
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read publication after physical cleanup")
            .record(),
        current_boundary.record(),
        "retiring history cannot generate more accepted history"
    );
    super::tests::publish_current_snapshot(&device).await;
    let successor = database
        .latest_local_store_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        successor
            .meta
            .history_summary
            .reclaim
            .snapshots
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![current.reference.snapshot_hash],
        "confirmed absent artifacts leave the next signed inventory"
    );
    device
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt third snapshot");
    device
        .reclaim_packages()
        .await
        .expect("retire second snapshot artifacts");
    home.clear_exact_reads();
    let mut cold = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), device.store_root())
        .await
        .expect("open cold snapshot verifier");
    let selected = cold
        .current_accepted_snapshot()
        .await
        .expect("bootstrap after exact artifact retirement")
        .expect("current snapshot exists");
    assert_eq!(selected.snapshot().reference, successor.reference);
    for object in previous_objects.iter().chain([
        &current.meta.image.object,
        &current.meta.membership_rollup.object,
        &current.reference.object,
        &current_boundary.record().accepted().unwrap().object,
    ]) {
        assert!(
            !home.exact_reads().contains(object.slot()),
            "cold restore must not reread retired artifact {object:?}"
        );
    }
}

fn open_retirement_database(
    path: &std::path::Path,
    directory: coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    coven_database::Database::open_synthetic_for_test(
        path,
        directory,
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "snapshot-retirement".into(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open durable retirement database")
}

#[tokio::test]
async fn a_returning_snapshot_publisher_releases_already_retired_local_artifacts() {
    assert_returning_publisher_releases_artifacts(false).await;
}

#[tokio::test]
async fn a_returning_publisher_releases_its_retired_baseline_artifacts() {
    assert_returning_publisher_releases_artifacts(true).await;
}

async fn assert_returning_publisher_releases_artifacts(stand_on_original: bool) {
    let administrator_dir = test_store_dir();
    let declaration = coven_protocol::synced_schema::BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::HostProvided,
        coven_protocol::blob::CacheFill::CacheEager,
    )
    .with_id_column("blob_id");
    let administrator_database = crate::sync::test_helpers::open_test_db_with_blob(
        administrator_dir.clone(),
        declaration.clone(),
    );
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, _) = TestStore::create_with_connection(
        &administrator_database,
        administrator_dir.clone(),
        "offline-snapshot-owner",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let returning_dir = test_store_dir();
    let returning_database =
        crate::sync::test_helpers::open_test_db_with_blob(returning_dir.clone(), declaration);
    let returning = store
        .activate_joined_device(
            &administrator_database,
            administrator_dir.clone(),
            &returning_database,
            returning_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate returning Owner device");
    let administrator = store
        .bind_device_in(&administrator_database, administrator_dir, &signer)
        .await
        .expect("bind provider administrator");
    let bytes = b"returning publisher exact photo";
    let mut batch = coven_database::WriteBatch::new();
    batch.put_blob("photos", "returning-photo", bytes.to_vec());
    let photo_hash = coven_protocol::blob::content_hash(bytes);
    let photo_size = bytes.len();
    coven_database::StoreRowWrites::new(StoreDatabase::new(&returning_database))
        .execute(coven_database::HostWriteOperation::new(batch, move |sql| {
            sql.execute_batch(&format!(
                "INSERT INTO notes(id, title, shared, _updated_at, created_at) VALUES
                 ('returning-note', 'Photo', 1, '0000000002000-0000-returning', '2026-09-08');
                 INSERT INTO note_photos(id, note_id, kind, blob_id, size, hash, _updated_at, created_at)
                 VALUES ('returning-photo', 'returning-note', 'image', 'returning-photo',
                 {photo_size}, '{photo_hash}', '0000000002000-0000-returning', '2026-09-08');"
            ))?;
            Ok::<_, coven_database::DbError>(())
        }), None, None).await.expect("publishable photo source and row");
    assert!(returning
        .publish_pending_store_database()
        .await
        .expect("accept exact photo"));
    super::tests::publish_current_snapshot(&returning).await;
    let local = StoreDatabase::new(&returning_database);
    let original = local.latest_local_store_snapshot().await.unwrap().unwrap();
    let stored = local
        .row_blob_ref("note_photos", "returning-photo")
        .await
        .expect("read exact published photo")
        .stored()
        .cloned()
        .expect("photo is remote");
    let object_id = coven_protocol::remote_object::remote_object_id(stored.object());
    let object_query = format!("SELECT state FROM remote_objects WHERE object_id = '{object_id}'");
    let original_remote: coven_protocol::remote_object::RemoteObjectRecord =
        serde_json::from_str(&returning_database.query_test_text(&object_query).await)
            .expect("read original photo ownership");
    let original_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: original.reference.object.slot().clone(),
    };
    assert!(original_remote
        .snapshot_owners()
        .any(|owner| owner == &original_owner));
    if stand_on_original {
        returning
            .stand_on_accepted_snapshot()
            .await
            .expect("adopt the original local snapshot before going offline");
    }
    let (_, pulled) = administrator
        .pull_store()
        .await
        .expect("observe original snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    super::tests::publish_current_snapshot(&administrator).await;
    administrator
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt successor");
    administrator
        .reclaim_packages()
        .await
        .expect("retire old publisher's remote artifacts");
    assert!(!home.contains_exact_object(&original.meta.image.object));
    assert!(!home.contains_exact_object(&original.reference.object));
    super::tests::publish_current_snapshot(&administrator).await;
    let newest = StoreDatabase::new(&administrator_database)
        .latest_local_store_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert!(
        !newest
            .meta
            .history_summary
            .reclaim
            .snapshots
            .contains_key(&original.reference.snapshot_hash),
        "the completed old deletion is absent from the current inventory"
    );
    let (_, pulled) = returning
        .pull_store()
        .await
        .expect("adopt a snapshot beyond the deleted inventory");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    returning
        .stand_on_accepted_snapshot()
        .await
        .expect("advance returning publisher baseline");
    let after_remote: coven_protocol::remote_object::RemoteObjectRecord =
        serde_json::from_str(&returning_database.query_test_text(&object_query).await)
            .expect("read adopted photo ownership");
    assert!(!after_remote.snapshot_owners().any(|owner| owner == &original_owner),
        "adoption releases the local blob's retired snapshot owner even after its remote inventory is pruned");
    assert_eq!(
        after_remote.stored_blob_commit_owners(),
        original_remote.stored_blob_commit_owners(),
        "retiring a snapshot owner preserves exact blob publication provenance"
    );
    assert_eq!(
        local
            .row_blob_ref("note_photos", "returning-photo")
            .await
            .unwrap()
            .stored(),
        Some(&stored)
    );
    assert!(
        home.contains_exact_object(stored.object()),
        "the accepted live photo remains available"
    );
    assert!(local.local_store_snapshots().await.unwrap().is_empty(),
        "returning publisher must release obsolete locally accepted metadata even after peers prune its remote inventory");
    local
        .verify_snapshot_artifacts_released(vec![
            original.meta.image.object.clone(),
            original.meta.membership_rollup.object.clone(),
        ])
        .await
        .expect("old local exact leases are released with adoption");
}

#[tokio::test]
async fn repeated_snapshot_adoption_retires_covered_device_state_mappings() {
    let temporary = tempfile::tempdir().expect("device-state retirement database");
    let path = temporary.path().join("store.db");
    let directory = test_store_dir();
    let database = open_retirement_database(&path, directory.clone());
    let signer = UserKeypair::generate();
    let (store, _) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "bounded-snapshot-device-states",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&database, directory.clone(), &signer)
        .await
        .expect("bind publisher");
    let mut first_count = None;
    for round in 0..8 {
        coven_database::StoreRowWrites::new(StoreDatabase::new(&database))
            .execute(
                coven_database::HostWriteOperation::new(
                    coven_database::WriteBatch::new(),
                    move |sql| {
                        sql.execute(
                            "INSERT INTO notes(id, title, shared, _updated_at, created_at)
                         VALUES (?1, 'Retained row', 1, ?2, '2026-09-08')",
                            rusqlite::params![
                                format!("row-{round}"),
                                format!("000000000{}000-0000-device", round + 1)
                            ],
                        )?;
                        Ok::<_, coven_database::DbError>(())
                    },
                ),
                None,
                None,
            )
            .await
            .expect("capture another accepted row");
        assert!(device
            .publish_pending_store_database()
            .await
            .expect("publish row"));
        super::tests::publish_current_snapshot(&device).await;
        device
            .stand_on_accepted_snapshot()
            .await
            .expect("adopt snapshot");
        let count = database
            .table_row_count_for_test(coven_database::DatabaseTestTable::named(
                "store_device_state_snapshots",
            ))
            .await
            .expect("count retained exact device-state mappings");
        if round == 0 {
            first_count = Some(count);
        }
        if round == 7 {
            assert_eq!(Some(count), first_count,
                "snapshot adoption must retire covered device-state mappings instead of retaining every accepted position");
        }
    }
    drop(device);
    drop(database);
    let reopened = open_retirement_database(&path, directory.clone());
    let device = store
        .bind_device_in(&reopened, directory, &signer)
        .await
        .expect("reopen bounded state");
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay retained rows"),
        8
    );
    for round in 0..8 {
        assert!(
            reopened
                .test_row_exists(&format!(
                    "SELECT 1 FROM notes WHERE id = 'row-{round}' AND title = 'Retained row'"
                ))
                .await
        );
    }
}

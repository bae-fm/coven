use super::*;

/// A device join installs a whole history at once. Fetching the blob content
/// every commit in that history ever bound is the entire library — the live
/// joins that did it ran twenty-eight minutes at full CPU over a hundred
/// commits and never finished, and the bytes prove nothing the read that wants
/// them will not prove against the binding's plaintext hash.
#[tokio::test]
async fn a_device_join_reads_no_blob_content() {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let blob_decl = coven_protocol::synced_schema::BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::UserProvided,
        coven_protocol::blob::CacheFill::CacheLazy,
    );
    let owner_db = crate::sync::test_helpers::open_test_db_with_blob(
        owner_db_store_dir.clone(),
        blob_decl.clone(),
    );
    let home = cross_principal_test_home();
    let (storage, _cloud) =
        cycle_test_store_fixture(&owner_db, owner_db_store_dir.clone(), &owner, home.clone()).await;
    let member = UserKeypair::generate();
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([43; 32]),
    )
    .await;

    // Publish rows carrying blobs, so the history the joiner installs binds them.
    let database = coven_database::StoreDatabase::new(&owner_db);
    let rows = (0..4)
        .map(|index| (format!("joined-blob-{index}"), vec![b'x'; 4096]))
        .collect::<Vec<_>>();
    let borrowed = rows
        .iter()
        .map(|(id, bytes)| (id.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    owner_db
        .insert_local_upload_rows_for_test("joined-root", &borrowed)
        .await
        .expect("plant the blob rows");
    for (id, bytes) in &rows {
        let path = owner_db_store_dir
            .db_path()
            .parent()
            .expect("Store directory has a parent")
            .join(format!("{id}.source"));
        coven_foundation::local_file::AtomicStagedFile::write_for_test(&path, bytes)
            .await
            .expect("write the blob source");
        database
            .register_external_blob_for_test("note_photos", id, &path)
            .await;
    }
    crate::sync::test_owner_graph::TestOwnerGraph::new(database, owner_db_store_dir.clone())
        .make_remote("notes", "joined-root", "Notes Root", false)
        .await
        .expect("make the planted blobs remote");
    let device = storage
        .open_into(&owner_db, owner_db_store_dir.clone())
        .await
        .expect("open the owner Store");
    device
        .drain_uploads(&coven_foundation::clock::SystemClock, None, None)
        .await
        .expect("upload the planted blobs");
    device
        .run_cycle(None)
        .await
        .expect("publish the blob-bearing rows");

    let uploaded = home
        .exact_creates()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .filter(|key| key.starts_with("photos/"))
        .collect::<Vec<_>>();
    assert_eq!(
        uploaded.len(),
        4,
        "the four blobs reached the home: {uploaded:?}"
    );
    home.clear_exact_reads();

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let _member_db = storage
        .install_cross_principal_device(
            member_db_store_dir.clone(),
            crate::sync::test_helpers::test_synced_tables_with_blob(blob_decl),
            crate::sync::test_helpers::test_migrations(),
            SCHEMA_VERSION,
            &member,
            "member-account",
            T0,
        )
        .await
        .expect("complete cross-principal device join");

    let read = home
        .exact_reads()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let blob_reads = read
        .iter()
        .filter(|key| uploaded.contains(key))
        .collect::<Vec<_>>();
    assert!(
        blob_reads.is_empty(),
        "the join fetched blob content it did not need: {blob_reads:?}",
    );
}

/// The provider reads one same-provider join activation issues, over a Store
/// whose history is `commits` deep.
async fn same_provider_activation_reads(commits: usize) -> Vec<String> {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, _cloud) =
        cycle_test_store_fixture(&owner_db, owner_db_store_dir.clone(), &owner, home.clone()).await;
    for index in 0..commits {
        owner_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('depth-{index}', 'Depth {index}', NULL, 1, '{:013}-0000-M', '2026-01-01')",
                (index + 2) * 1000
            ))
            .await;
        storage
            .open_into(&owner_db, owner_db_store_dir.clone())
            .await
            .expect("open the owner Store")
            .run_cycle(None)
            .await
            .expect("publish the row");
    }

    let observer = storage
        .open_into(&owner_db, owner_db_store_dir.clone())
        .await
        .expect("open the observing owner device");
    observer
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("author the join snapshot");
    let pending_dir = tempfile::tempdir().expect("pending journal directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        pending_dir.path().join("pending-device-join.sqlite"),
    )
    .expect("open the pending join journal");
    let offer = observer
        .begin_device_join(&crate::sync::test_helpers::pubkey_hex(&owner))
        .await
        .expect("publish the join offer");
    let mut pending_join = observer
        .open_pending_device_join_for_test(&pending, &owner, offer)
        .await
        .expect("open the pending join");
    let access_request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("prepare the provider access request");
    let approval = observer
        .authorize_device_provider_access(access_request, None)
        .await
        .expect("authorize provider access");
    let registration_request = pending_join
        .prepare_registration_request(approval)
        .await
        .expect("prepare the registration request");

    home.clear_exact_reads();
    observer
        .activate_same_principal_join_for_test(registration_request)
        .await
        .expect("activate the same-provider join");
    home.exact_reads()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect()
}

/// Activating a same-provider join must cost the same number of provider reads
/// whatever the Store's history is.
///
/// Unseeded, the walk over retained history re-read every commit, its
/// activation head and its acknowledgement from the provider — five reads per
/// commit, serial. Measured here: 64 reads over ten commits and 214 over forty.
/// On a real provider at ~120 ms a read that is the sixty-odd seconds an owner
/// spent in this one step of the Add-a-device flow, growing with every commit
/// the Store ever published.
///
/// Counts, not milliseconds: the round trips are the thing that scales, and
/// they are deterministic.
#[tokio::test]
async fn a_same_provider_activation_reads_the_same_however_deep_its_history() {
    let shallow = same_provider_activation_reads(10).await;
    let deep = same_provider_activation_reads(40).await;

    assert_eq!(
        deep.len(),
        shallow.len(),
        "four times the history changed what the activation reads:\n  10 commits: {shallow:#?}\n  40 commits: {deep:#?}",
    );
    assert!(
        deep.len() <= 24,
        "the activation reads {} objects before it can act: {deep:#?}",
        deep.len(),
    );
}

/// Publish one owner row and the Store commit that carries it.
async fn publish_owner_row_commit(
    storage: &TestStore,
    owner_db: &Database,
    owner_db_store_dir: &StoreDir,
    stamp: u64,
    id: &str,
) {
    owner_db
        .execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{id}', '{id}', NULL, 1, '{stamp:013}-0000-owner', '2026-01-01')"
        ))
        .await;
    assert!(
        storage
            .publish_pending(owner_db, owner_db_store_dir)
            .await
            .expect("publish the owner's own Store write"),
        "the owner's row write at {id} produced no Store commit",
    );
}

/// Capture and publish a Store snapshot covering everything the owner has
/// materialized, and acknowledge it from the owner.
async fn publish_owner_snapshot(
    storage: &TestStore,
    owner_db: &Database,
    owner_db_store_dir: &StoreDir,
    owner: &UserKeypair,
) {
    let owner_device = storage
        .bind_device_in(owner_db, owner_db_store_dir.clone(), owner)
        .await
        .expect("bind the owner device");
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let database = StoreDatabase::new(owner_db);
    let image = database
        .capture_snapshot_image_for_test(
            storage.root().clone(),
            image_dir.path().to_path_buf(),
            Some(EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("capture the Store snapshot image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("read the materialized frontier"),
    )
    .expect("the materialized frontier is a commit frontier");
    owner_device
        .publish_snapshot(image, coverage.clone())
        .await
        .expect("publish the Store snapshot");
    owner_device
        .publish_acknowledgement(coverage)
        .await
        .expect("acknowledge the Store snapshot");
}

/// Drive a same-provider join far enough to see which snapshot the owner offers
/// the joining device.
async fn offered_join_snapshot(
    storage: &TestStore,
    owner_db: &Database,
    owner_db_store_dir: &StoreDir,
    owner: &UserKeypair,
    joiner: &UserKeypair,
) -> coven_protocol::store_commit::StoreSnapshotRef {
    let pending_dir = tempfile::tempdir().expect("pending join journal directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        pending_dir.path().join("pending-device-join.sqlite"),
    )
    .expect("open the pending join journal");
    let observer = storage
        .bind_device_in(owner_db, owner_db_store_dir.clone(), owner)
        .await
        .expect("bind the observing owner device");
    let offer = observer
        .begin_device_join(&crate::sync::test_helpers::pubkey_hex(joiner))
        .await
        .expect("publish the join offer");
    let mut pending_join = observer
        .open_pending_device_join_for_test(&pending, joiner, offer)
        .await
        .expect("open the pending join");
    let access_request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("prepare the provider access request");
    let approval = observer
        .authorize_device_provider_access(access_request, None)
        .await
        .expect("authorize provider access");
    let registration_request = pending_join
        .prepare_registration_request(approval)
        .await
        .expect("prepare the registration request");
    observer
        .activate_same_principal_join_for_test(registration_request)
        .await
        .expect("activate the same-provider join")
        .installation
        .authority
        .snapshot
}

/// A device that joined and was never used again stays active in membership and
/// authors no commit, so it can never acknowledge anything. Installing a
/// snapshot does not depend on that device having caught up — a laggard
/// converges through an ordinary pull whichever image the joiner installed — so
/// the next join is still offered the newest snapshot.
///
/// Requiring unanimity here pinned a live store to its generation-zero image,
/// whose coverage is empty: every join re-resolved all hundred and ninety-seven
/// commits it had just installed.
#[tokio::test]
async fn a_join_is_offered_the_newest_snapshot_despite_an_idle_device() {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let storage = cycle_test_store(
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let idle = UserKeypair::generate();
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &idle,
        &EncryptionService::from_key([42; 32]),
    )
    .await;
    // The idle device joins, is activated, and then authors nothing.
    let idle_store_dir = crate::sync::test_helpers::test_store_dir();
    let idle_db = crate::sync::test_helpers::open_test_db(idle_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &idle_db,
            idle_store_dir.clone(),
            &idle,
            T0,
        )
        .await
        .expect("activate the device that then goes idle");

    for index in 0..4u64 {
        publish_owner_row_commit(
            &storage,
            &owner_db,
            &owner_db_store_dir,
            3000 + index,
            &format!("after-join-{index}"),
        )
        .await;
    }
    publish_owner_snapshot(&storage, &owner_db, &owner_db_store_dir, &owner).await;
    let newest = StoreDatabase::new(&owner_db)
        .latest_local_store_snapshot()
        .await
        .expect("read the owner's latest accepted snapshot")
        .expect("the owner published a snapshot");
    assert!(
        !newest.meta.coverage.commits().is_empty(),
        "the snapshot covers accepted edits after the idle device joined",
    );

    let joiner = UserKeypair::generate();
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &joiner,
        &EncryptionService::from_key([42; 32]),
    )
    .await;
    let offered =
        offered_join_snapshot(&storage, &owner_db, &owner_db_store_dir, &owner, &joiner).await;
    assert_eq!(
        offered, newest.reference,
        "the join uses the accepted snapshot despite the idle device",
    );
}

/// A journal record this binary cannot read belongs to some other attempt —
/// abandoned, or written before the journal's shape changed, which it does
/// freely. Beginning a new join sweeps every attempt's record to find one
/// already outstanding for the member, and failing that sweep on the first
/// unreadable row stopped every later pairing on the device: a live owner could
/// not approve any device until its journal was edited by hand.
///
/// The attempt actually being driven still reads its own record by key and
/// still refuses anything it cannot parse.
#[tokio::test]
async fn a_journal_record_this_binary_cannot_read_does_not_stop_the_next_join() {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let storage = cycle_test_store(
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let member = UserKeypair::generate();
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([42; 32]),
    )
    .await;

    // A record left by another attempt, in a shape this binary does not know.
    StoreDatabase::new(&owner_db)
        .set_protocol_state(
            "device_join/00000000-0000-4000-8000-00000000dead/owner",
            r#"{"attempt_id":"00000000-0000-4000-8000-00000000dead",
                "progress":{"owner":{"same_principal_completed":{"join":{"stability":"gone"}}}}}"#,
        )
        .await
        .expect("plant the unreadable journal record");

    let observer = storage
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("bind the owner device");
    let offer = observer
        .begin_device_join(&crate::sync::test_helpers::pubkey_hex(&member))
        .await
        .expect("an unreadable record from another attempt does not stop a new join");
    assert_eq!(
        offer.member_pubkey,
        crate::sync::test_helpers::pubkey_hex(&member),
    );
}

/// A completed Join installs its selected snapshot authority. Its first cycle
/// preserves those rows without rediscovering snapshots, regardless of how many
/// accepted snapshots preceded the selected one.
#[tokio::test]
async fn a_joined_device_reads_the_same_snapshots_however_many_generations_stand() {
    let shallow = Box::pin(first_cycle_snapshot_operations(1)).await;
    let deep = Box::pin(first_cycle_snapshot_operations(6)).await;

    assert_eq!(
        shallow, deep,
        "snapshot discovery grows with retired generations: shallow={shallow}, deep={deep}",
    );
    assert_eq!(
        deep, 0,
        "an installed Join needs no snapshot discovery reads"
    );
}

/// Publish `generations` Store snapshots, each covering one more of the owner's
/// own commits than the last, and acknowledged by the cycle that follows it.
async fn publish_snapshot_generations(
    storage: &std::sync::Arc<TestStore>,
    owner_db: &Database,
    owner_db_store_dir: &StoreDir,
    generations: usize,
) {
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    for generation in 0..generations {
        owner_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('gen-{generation}', 'Generation {generation}', NULL, 1, \
                 '{:013}-0000-G', '2026-01-01')",
                (generation + 2) * 1000
            ))
            .await;
        let device = storage
            .open_into(owner_db, owner_db_store_dir.clone())
            .await
            .expect("open the owner Store");
        device
            .run_cycle(None)
            .await
            .expect("publish the owner's row");
        let store_database = StoreDatabase::new(owner_db);
        let image = store_database
            .capture_snapshot_image_for_test(
                storage.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture the snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            store_database
                .materialized_frontier()
                .await
                .expect("read the owner's materialized frontier"),
        )
        .expect("shape the owner's materialized frontier");
        device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish a Store snapshot generation");
    }
}

/// Provider operations under the snapshot prefix that a freshly joined device's
/// first cycle issues, over a Store that has published `generations` of them.
async fn first_cycle_snapshot_operations(generations: usize) -> usize {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let storage =
        cycle_test_store(&owner_db, owner_db_store_dir.clone(), &owner, home.clone()).await;
    publish_snapshot_generations(&storage, &owner_db, &owner_db_store_dir, generations).await;

    let joining = UserKeypair::generate();
    storage
        .admit_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &crate::sync::test_helpers::pubkey_hex(&joining),
            None,
            coven_protocol::membership::MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .expect("admit the joining member");
    let joined = storage
        .activate_joined_device_from_snapshot(
            &owner_db,
            owner_db_store_dir.clone(),
            crate::sync::test_helpers::test_store_dir(),
            &joining,
            T0,
            crate::sync::test_helpers::test_synced_tables(),
            crate::sync::test_helpers::test_migrations(),
            SCHEMA_VERSION,
        )
        .await
        .expect("join a device from the published snapshot");

    home.clear_exact_reads();
    home.clear_exact_listings();
    joined
        .run_cycle(None)
        .await
        .expect("run the joined device's first cycle");
    for generation in 0..generations {
        assert_eq!(
            joined
                .query_test_text(&format!(
                    "SELECT title FROM notes WHERE id = 'gen-{generation}'"
                ))
                .await,
            format!("Generation {generation}"),
            "the joined device retains the row covered by generation {generation}",
        );
    }
    snapshot_operations(&home)
}

/// Reads and listings under the Store snapshot prefix, which is what choosing a
/// snapshot spends and nothing else does.
fn snapshot_operations(home: &InMemoryCloudHome) -> usize {
    let counted = |key: &str| key.starts_with("store-v1/snapshots/");
    home.exact_reads()
        .iter()
        .filter(|slot| counted(slot.logical_key()))
        .count()
        + home
            .exact_listed_prefixes()
            .iter()
            .filter(|prefix| counted(prefix))
            .count()
}

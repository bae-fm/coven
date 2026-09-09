use super::*;
use crate::sync::test_helpers::{
    open_test_db, test_cloud_home, test_migrations, test_store_dir, test_synced_tables,
    TestCustody, TestStore,
};

fn open_durable_receiver(
    directory: &coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    coven_database::Database::open_in_store_dir_for_test(
        &directory.db_path(),
        directory.clone(),
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "partial-publication-reader".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        coven_database::CovenMigrationPolicy::ApplyPending,
        &test_migrations(),
    )
    .expect("open durable receiver database")
}

#[tokio::test]
async fn a_held_package_allows_independent_progress_and_retries_after_reopen() {
    assert_held_publication_progress(false).await;
}

#[tokio::test]
async fn an_accepted_snapshot_supersedes_an_unavailable_covered_package() {
    assert_held_publication_progress(true).await;
}

async fn assert_held_publication_progress(with_snapshot: bool) {
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let identity = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &owner_db,
        owner_dir.clone(),
        "held-package-independent-progress",
        identity.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &owner_db,
            owner_dir.clone(),
            &peer_db,
            peer_dir,
            &identity,
            "2026-07-23T00:00:00Z",
        )
        .await
        .expect("activate independent publisher");
    let owner = store
        .bind_device_in(&owner_db, owner_dir, &identity)
        .await
        .expect("bind owner");

    let receiver_dir = test_store_dir();
    let open_receiver = || open_durable_receiver(&receiver_dir);
    let receiver_db = open_receiver();
    let receiver = store
        .open_into(&receiver_db, receiver_dir.clone())
        .await
        .expect("open receiver before either row publication");
    let (_, initial) = receiver.pull_store().await.expect("pull initial authority");
    assert!(initial.held_positions.is_empty(), "{initial:?}");

    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('held-row', 'First publisher', 1, '0000000002000-0000-owner', '2026-07-23')",
        )
        .await;
    peer_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('independent-row', 'Independent publisher', 1, '0000000002000-0000-peer', '2026-07-23')",
        )
        .await;
    let mut owner_writer = owner.authorize_writer().await.expect("authorize owner");
    let mut peer_writer = peer.authorize_writer().await.expect("authorize peer");
    assert!(owner_writer
        .prepare_pending_store_write()
        .await
        .expect("capture owner row"));
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("capture independent row"));
    assert_eq!(
        owner_writer
            .drain_store_writes()
            .await
            .expect("publish owner row"),
        1
    );
    assert_eq!(
        peer_writer
            .drain_store_writes()
            .await
            .expect("publish independent row"),
        1
    );
    drop(owner_writer);
    drop(peer_writer);

    let held_ref = owner
        .latest_local_store_position()
        .await
        .expect("read owner position")
        .expect("owner row is published");
    let independent_ref = peer
        .latest_local_store_position()
        .await
        .expect("read peer position")
        .expect("independent row is published");
    let held_commit = owner
        .load_commit_for_test(&held_ref)
        .await
        .expect("load owner commit");
    let independent_commit = peer
        .load_commit_for_test(&independent_ref)
        .await
        .expect("load independent commit");
    assert!(!independent_commit
        .merge_dependencies()
        .values()
        .any(|reference| reference == &held_ref));
    let (_, synchronized) = owner
        .pull_store()
        .await
        .expect("read both accepted rows on the owner");
    assert!(synchronized.held_positions.is_empty(), "{synchronized:?}");
    if with_snapshot {
        let database = StoreDatabase::new(&owner_db);
        let image_dir = tempfile::tempdir().expect("create snapshot image directory");
        let image = database
            .capture_snapshot_image_for_test(
                store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture the accepted rows");
        let coverage = CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("read accepted row frontier"),
        )
        .expect("build snapshot coverage");
        owner
            .publish_snapshot(image, coverage)
            .await
            .expect("publish snapshot after both rows");
        owner
            .stand_on_accepted_snapshot()
            .await
            .expect("adopt the published snapshot");
        owner_db.execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('after-snapshot', 'After the snapshot', 1, '0000000003000-0000-owner', '2026-07-23')",
        ).await;
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize the next interval");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare the edit after the snapshot"));
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish after the snapshot"),
            1
        );
    }
    let package = held_commit
        .store_package()
        .expect("owner commit carries a package");
    let bytes = home.stored_exact_object(package.object.slot());
    home.remove_exact_object(package.object.slot());
    let accepted = StoreDatabase::new(&owner_db)
        .store_current_publication()
        .await
        .expect("read actual accepted tip and provider revision");

    let (_, partial) = receiver
        .pull_store()
        .await
        .expect("pull with one package unavailable");
    if with_snapshot {
        assert!(partial.held_positions.is_empty(), "{partial:?}");
        assert_eq!(
            receiver_db
                .query_test_text("SELECT title FROM notes WHERE id = 'after-snapshot'")
                .await,
            "After the snapshot",
            "the interval after the snapshot applies without reading a retired package",
        );
    } else {
        assert!(
            partial.held_positions.iter().any(|held| {
                matches!(&held.coordinate, HeldStoreCoordinate::Package { device_id, seq, package_hash }
                if device_id == &held_ref.coord.stream_id.to_string()
                    && *seq == held_ref.coord.sequence() && *package_hash == package.content_hash)
            }),
            "{partial:?}"
        );
        assert!(
            !receiver_db
                .test_row_exists("SELECT 1 FROM notes WHERE id = 'after-snapshot'")
                .await
        );
    }
    assert_eq!(partial.changesets_applied, 1, "{partial:?}");
    assert_eq!(
        receiver_db
            .query_test_text("SELECT title FROM notes WHERE id = 'independent-row'")
            .await,
        "Independent publisher"
    );
    assert_eq!(
        receiver_db
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'held-row'")
            .await,
        with_snapshot,
        "the snapshot supplies its covered row without the unavailable package",
    );
    let receiver_store = StoreDatabase::new(&receiver_db);
    assert_eq!(
        receiver_store
            .store_current_publication()
            .await
            .expect("read observed tip"),
        accepted
    );
    if !with_snapshot {
        assert_eq!(
            receiver_store
                .exact_materialized_ref(
                    &independent_ref.coord.stream_id.to_string(),
                    independent_ref.coord.sequence()
                )
                .await
                .expect("read independent progress"),
            Some(independent_ref)
        );
        assert!(receiver_store
            .exact_materialized_ref(
                &held_ref.coord.stream_id.to_string(),
                held_ref.coord.sequence()
            )
            .await
            .expect("read held progress")
            .is_none());
    }
    let capture_dir = tempfile::tempdir().expect("snapshot capture directory");
    let capture = receiver_store
        .capture_store_snapshot_cut(store.root().clone(), capture_dir.path().to_path_buf(), None)
        .await;
    if with_snapshot {
        capture.expect("a fully installed snapshot and its following interval can be captured");
    } else {
        let error = match capture {
            Err(error) => error,
            Ok(_) => panic!("snapshot capture must refuse the accepted but unavailable package"),
        };
        assert!(
            error
                .to_string()
                .contains("snapshot capture is missing accepted commit"),
            "{error}"
        );
        assert!(!capture_dir.path().join("snapshot.db").exists());
    }
    assert_eq!(
        receiver_store
            .store_current_publication()
            .await
            .expect("unchanged accepted tip"),
        accepted
    );
    drop(receiver_store);
    let installed_snapshot = StoreDatabase::new(&receiver_db)
        .installed_replay_baseline()
        .await
        .expect("read receiver baseline")
        .snapshot()
        .expect("receiver has an installed snapshot")
        .reference
        .clone();
    drop(receiver);
    drop(receiver_db);

    if !with_snapshot {
        home.restore_exact_object(package.object.slot(), bytes);
    }
    home.remove_exact_object(installed_snapshot.object.slot());
    let reopened_db = open_receiver();
    let reopened = store
        .bind_device_in(&reopened_db, receiver_dir.clone(), &identity)
        .await
        .expect("bind a fresh reader from durable state");
    let (_, retried) = reopened
        .pull_store()
        .await
        .expect("retry retained accepted work without a new publication");
    assert!(retried.held_positions.is_empty(), "{retried:?}");
    assert_eq!(
        retried.changesets_applied,
        if with_snapshot { 0 } else { 1 },
        "{retried:?}"
    );
    assert_eq!(
        reopened_db
            .query_test_text("SELECT title FROM notes WHERE id = 'held-row'")
            .await,
        "First publisher"
    );
    assert_eq!(
        reopened_db
            .query_test_text("SELECT title FROM notes WHERE id = 'independent-row'")
            .await,
        "Independent publisher"
    );
    assert_eq!(
        StoreDatabase::new(&reopened_db)
            .store_current_publication()
            .await
            .expect("read unchanged observed tip"),
        accepted
    );
    if with_snapshot {
        assert_eq!(
            reopened_db
                .query_test_text("SELECT title FROM notes WHERE id = 'after-snapshot'")
                .await,
            "After the snapshot"
        );
        assert_eq!(
            StoreDatabase::new(&reopened_db)
                .installed_replay_baseline()
                .await
                .expect("read adopted snapshot")
                .snapshot()
                .expect("snapshot installed")
                .reference,
            installed_snapshot
        );
    }
}

#[tokio::test]
async fn a_held_owner_retirement_still_constrains_authorization_after_reopen() {
    let founder_dir = test_store_dir();
    let founder_db = open_test_db(founder_dir.clone());
    let founder = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &founder_db,
        founder_dir.clone(),
        "held-owner-retirement",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let target = UserKeypair::generate();
    let target_pubkey = coven_keys::keys::public_key_hex(&target);
    let target_dir = test_store_dir();
    let target_db = open_test_db(target_dir.clone());
    store
        .admit_member(
            &founder_db,
            founder_dir.clone(),
            &founder,
            &target_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Held retirement Store",
        )
        .await
        .expect("admit the future Owner");
    store
        .activate_joined_device(
            &founder_db,
            founder_dir.clone(),
            &target_db,
            target_dir.clone(),
            &target,
            "2026-07-23T00:00:00Z",
        )
        .await
        .expect("activate the future Owner");
    store
        .promote_active_member_fixture(
            &founder_db,
            founder_dir.clone(),
            &target_db,
            target_dir,
            &founder,
            &target,
            &encryption,
        )
        .await
        .expect("promote the target to Owner");
    let owner = store
        .bind_device_in(&founder_db, founder_dir.clone(), &founder)
        .await
        .expect("bind founder");
    let grant = owner
        .membership_for_test()
        .await
        .expect("read the accepted Owner grant")
        .active_owner_grant(&target_pubkey)
        .expect("target is an Owner");
    let receiver_dir = test_store_dir();
    let receiver_db = open_durable_receiver(&receiver_dir);
    let receiver = store
        .open_into(&receiver_db, receiver_dir.clone())
        .await
        .expect("open receiver before retirement");
    let (_, initial) = receiver
        .pull_store()
        .await
        .expect("pull the initial authority");
    assert!(initial.held_positions.is_empty(), "{initial:?}");

    founder_db.execute_test_host_write(
        "INSERT INTO notes (id, title, shared, _updated_at, created_at)
         VALUES ('held-before-retirement', 'Held row', 1, '0000000002000-0000-founder', '2026-07-23')",
    ).await;
    let mut writer = owner.authorize_writer().await.expect("authorize founder");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare row"));
    assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
    drop(writer);
    let held_ref = owner
        .latest_local_store_position()
        .await
        .expect("read row position")
        .expect("row was published");
    let held_commit = owner
        .load_commit_for_test(&held_ref)
        .await
        .expect("load row commit");
    let custody = TestCustody::default();
    custody.set_initial_key([42; 32]);
    store
        .remove_member(
            &founder_db,
            founder_dir,
            &founder,
            &target_pubkey,
            &encryption,
            &custody,
        )
        .await
        .expect("retire the Owner grant after the row");
    let retirement = owner
        .latest_local_store_position()
        .await
        .expect("read retirement position")
        .expect("Owner retirement was published");
    assert_ne!(retirement, held_ref);
    let package = held_commit
        .store_package()
        .expect("row has a Store package");
    let bytes = home.stored_exact_object(package.object.slot());
    home.remove_exact_object(package.object.slot());
    let (_, partial) = receiver
        .pull_store()
        .await
        .expect("hold the unavailable row");
    assert!(
        partial.held_positions.iter().any(|held| {
            matches!(&held.coordinate, HeldStoreCoordinate::Package { device_id, seq, package_hash }
            if device_id == &held_ref.coord.stream_id.to_string()
                && *seq == held_ref.coord.sequence() && *package_hash == package.content_hash)
        }),
        "{partial:?}"
    );
    assert!(partial.held_positions.iter().any(|held| {
        matches!((&held.coordinate, &held.reason),
            (HeldStoreCoordinate::Commit { commit, .. }, HeldStorePositionReason::MissingPredecessor(required))
                if commit == &retirement && required == &held_ref)
    }), "{partial:?}");
    let database = StoreDatabase::new(&receiver_db);
    let accepted = database
        .store_current_publication()
        .await
        .expect("read accepted tip");
    assert_eq!(
        accepted,
        StoreDatabase::new(&founder_db)
            .store_current_publication()
            .await
            .expect("read founder tip")
    );
    assert!(database
        .exact_materialized_ref(
            &retirement.coord.stream_id.to_string(),
            retirement.coord.sequence()
        )
        .await
        .expect("read held retirement")
        .is_none());
    drop(database);
    drop(receiver);
    drop(receiver_db);

    let reopened_db = open_durable_receiver(&receiver_dir);
    let reopened = store
        .bind_device_in(&reopened_db, receiver_dir, &founder)
        .await
        .expect("reopen the receiver");
    let membership = reopened
        .membership_for_test()
        .await
        .expect("authorize from accepted held history");
    assert!(membership.active_grant(&grant).is_none());
    assert!(!membership.is_owner_now(&target_pubkey));
    assert!(
        !reopened_db
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'held-before-retirement'")
            .await
    );
    assert!(StoreDatabase::new(&reopened_db)
        .exact_materialized_ref(
            &retirement.coord.stream_id.to_string(),
            retirement.coord.sequence()
        )
        .await
        .expect("authorization must not install the held retirement")
        .is_none());
    home.restore_exact_object(package.object.slot(), bytes);
    let (_, retried) = reopened
        .pull_store()
        .await
        .expect("retry retained authority and rows");
    assert!(retried.held_positions.is_empty(), "{retried:?}");
    assert_eq!(
        StoreDatabase::new(&reopened_db)
            .exact_materialized_ref(
                &retirement.coord.stream_id.to_string(),
                retirement.coord.sequence()
            )
            .await
            .expect("the retried retirement is installed"),
        Some(retirement)
    );
    assert_eq!(
        reopened_db
            .query_test_text("SELECT title FROM notes WHERE id = 'held-before-retirement'")
            .await,
        "Held row"
    );
    assert_eq!(
        StoreDatabase::new(&reopened_db)
            .store_current_publication()
            .await
            .expect("read unchanged accepted tip"),
        accepted
    );
}

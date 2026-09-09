use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::{
    DatabaseImageTest, DatabaseTestTable, MergeMaterializationFailurePoint, StoreDatabase,
};
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::CommitFrontier;

#[path = "duplicate_publication_tests.rs"]
mod duplicate_publication_tests;

#[path = "snapshot_stream_tests.rs"]
mod snapshot_stream_tests;

#[tokio::test]
async fn pulling_a_peer_commit_preserves_the_pending_publication_attempt() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "pending-publication-observation",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &target,
            target_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    let (_, initial) = owner.pull_store().await.expect("observe peer activation");
    assert!(initial.held_positions.is_empty());
    for (database, device, id) in [
        (&source, &owner, "pending-row"),
        (&target, &peer, "accepted-peer-row"),
    ] {
        database
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('{id}', '{id}', 1, '0000000002000-0000-writer', '2026-01-01')"
            ))
            .await;
        let mut writer = device.authorize_writer().await.expect("authorize writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare row"));
    }
    let database = StoreDatabase::new(&source);
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("load pending row")
        .expect("row has a durable publication attempt");
    let reservation = database
        .active_store_publication()
        .await
        .expect("load pending reservation");
    let mut writer = peer
        .authorize_writer()
        .await
        .expect("authorize peer publisher");
    assert_eq!(
        writer.drain_store_writes().await.expect("publish peer row"),
        1
    );
    drop(writer);
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("observe the winning publication");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_ne!(
        database
            .store_current_publication()
            .await
            .expect("read advanced boundary")
            .record(),
        &pending.publication.previous,
    );
    let retained = database
        .oldest_prepared_store_write()
        .await
        .expect("an older attempt remains readable after observing a peer publication")
        .expect("the unresolved row stays prepared");
    assert_eq!(
        retained.commit.value.reference(),
        pending.commit.value.reference()
    );
    assert_eq!(retained.commit.bytes, pending.commit.bytes);
    assert_eq!(retained.publication, pending.publication);
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("read reservation"),
        reservation
    );
    for id in ["pending-row", "accepted-peer-row"] {
        assert_eq!(
            source
                .query_test_text(&format!("SELECT title FROM notes WHERE id = '{id}'"))
                .await,
            id
        );
    }
}

#[tokio::test]
async fn pulling_a_commit_accepts_its_exact_dependency_behind_the_current_frontier() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "older-exact-dependency",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = crate::sync::test_helpers::TestDevice::activate_joined(
        owner.clone(),
        StoreDatabase::new(&target),
        target_dir,
        &signer,
        "0000000001000-0000-peer",
        storage.clone(),
    )
    .await
    .expect("activate peer");
    let observer_dir = test_store_dir();
    let observer_database = open_test_db(observer_dir.clone());
    let observer = crate::sync::test_helpers::TestDevice::activate_joined(
        owner.clone(),
        StoreDatabase::new(&observer_database),
        observer_dir,
        &signer,
        "0000000001001-0000-observer",
        storage,
    )
    .await
    .expect("activate observer before either source row");
    let (_, initial) = owner.pull_store().await.expect("pull join activation");
    assert!(initial.held_positions.is_empty(), "{initial:?}");
    for index in 0..2 {
        source
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('source-{index}', 'Source {index}', 1, \
                 '000000000200{index}-0000-owner', '2026-01-01')"
            ))
            .await;
        let mut writer = owner.authorize_writer().await.expect("authorize owner");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare source row"));
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish source row"),
            1
        );
        drop(writer);
        if index == 0 {
            let (_, pulled) = peer.pull_store().await.expect("observe first source row");
            assert!(pulled.held_positions.is_empty(), "{pulled:?}");
            target
                .execute_test_host_write(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                     VALUES ('peer-row', 'Captured before the second source row', 1, \
                     '0000000003000-0000-peer', '2026-01-01')",
                )
                .await;
        }
    }
    let (_, pulled) = peer.pull_store().await.expect("pull newer source history");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let mut writer = peer.authorize_writer().await.expect("authorize peer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare captured peer row"));
    assert_eq!(
        writer.drain_store_writes().await.expect("publish peer row"),
        1
    );
    drop(writer);
    let inputs = StoreDatabase::new(&target)
        .retained_merge_replay_inputs(store.root())
        .await
        .expect("read accepted peer dependencies");
    let peer_input = inputs
        .iter()
        .find(|input| {
            input.commit().author_registration.device_id.to_string() == peer.device_id()
                && !input.packages().is_empty()
        })
        .expect("accepted peer row");
    let frontier = StoreDatabase::new(&source)
        .materialized_frontier()
        .await
        .expect("read source frontier");
    assert!(
        peer_input
            .commit()
            .merge_dependencies()
            .iter()
            .any(|(stream, dependency)| {
                frontier
                    .get(&stream.to_string())
                    .is_some_and(|current| current.coord.sequence() > dependency.coord.sequence())
            }),
        "the captured dependency must precede the receiver's frontier"
    );
    let (_, accepted) = owner
        .pull_store()
        .await
        .expect("pull older exact dependency");
    assert!(accepted.held_positions.is_empty(), "{accepted:?}");
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'peer-row'")
            .await,
        "Captured before the second source row"
    );
    let (_, received) = observer.pull_store().await.expect("pull all rows together");
    assert!(
        received.held_positions.is_empty(),
        "older dependencies prepared in this same pull: {:?}",
        received.held_positions
    );
    assert_eq!(
        observer_database
            .query_test_text("SELECT title FROM notes WHERE id = 'peer-row'")
            .await,
        "Captured before the second source row"
    );
}

#[tokio::test]
async fn materialization_rejects_another_accepted_commits_publication() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "materialization-publication-binding",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    for index in 0..2 {
        source
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('row-{index}', 'Accepted row', 1, \
                 '000000000100{index}-0000-owner', '2026-01-01')"
            ))
            .await;
        let mut writer = owner.authorize_writer().await.expect("authorize writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare row"));
        assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
    }
    let materializations = StoreDatabase::new(&source)
        .retained_merge_replay_inputs(store.root().clone())
        .await
        .expect("load accepted materializations");
    assert_eq!(materializations.len(), 2);
    let first = &materializations[0];
    let second = &materializations[1];
    assert_eq!(
        first.commit().author_registration,
        second.commit().author_registration
    );
    assert_ne!(first.commit_ref(), second.commit_ref());
    for (acceptance, accepted) in [(first.acceptance(), true), (second.acceptance(), false)] {
        let verified = coven_database::VerifiedMergeMaterialization::verify(
            first.root(),
            first.verified_commit(),
            first.registrations(),
            first.device_operations(),
            first.circle_activations(),
            acceptance,
            first.history_evidence(),
            first.membership_objects(),
            first.packages(),
            first.package_application(),
        );
        assert_eq!(
            verified.is_ok(),
            accepted,
            "materialization must bind the publication to its exact commit"
        );
    }
}

#[tokio::test]
async fn repeated_snapshots_preserve_retained_circle_activation_receipts() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-circle-receipts",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    let circle = owner
        .create_circle("0000000001000-0000-owner", "Retained Circle")
        .await
        .expect("create Circle");
    let database = StoreDatabase::new(&source);
    let directory = tempfile::tempdir().expect("create snapshot directory");
    for index in 0..2 {
        source
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('row-{index}', 'Covered row', 1, \
                 '000000000200{index}-0000-owner', '2026-01-01')"
            ))
            .await;
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize row writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare row"));
        assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
        drop(writer);
        let image = database
            .capture_snapshot_image_for_test(
                store.root().clone(),
                directory.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture image with retained Circle control");
        let coverage = CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("read frontier"),
        )
        .expect("build coverage");
        owner
            .publish_snapshot(image, coverage)
            .await
            .expect("publish snapshot with retained Circle evidence");
        let advance = owner
            .stand_on_accepted_snapshot()
            .await
            .expect("retire the covered publication interval");
        assert!(
            matches!(
                advance,
                crate::sync::store::ReplayBaselineAdvance::Advanced(_)
            ),
            "{advance:?}"
        );
        assert_eq!(
            source
                .table_row_count_for_test(DatabaseTestTable::named("store_publication_entries"))
                .await
                .expect("count retained publication entries"),
            1,
            "only the accepted snapshot entry remains"
        );
        let retained = database
            .retained_merge_replay_inputs(store.root().clone())
            .await
            .expect("open retained Circle acceptance after retirement");
        assert!(!retained.is_empty(), "the Circle control remains retained");
        for materialization in &retained {
            assert_eq!(
                materialization.acceptance().commit_ref(),
                materialization.commit_ref(),
            );
            assert!(
                materialization.acceptance().exact_publication().is_none(),
                "snapshot coverage must not fabricate the retired upload receipt",
            );
        }
        owner
            .authorize_writer()
            .await
            .expect("reopen installed history after snapshot");
    }
    owner
        .delete_circle(circle)
        .await
        .expect("continue Circle control after snapshot retirement");
}

#[tokio::test]
async fn fresh_snapshot_bootstrap_verifies_covered_publications_before_replay() {
    assert_fresh_snapshot_bootstrap(false).await;
}

#[tokio::test]
async fn fresh_snapshot_bootstrap_does_not_read_retired_commits_or_publications() {
    assert_fresh_snapshot_bootstrap(true).await;
}

async fn assert_fresh_snapshot_bootstrap(retire_covered_history: bool) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-bootstrap-covered-publication",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('covered-row', 'In the snapshot', NULL, 1, \
             '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let mut writer = owner.authorize_writer().await.expect("authorize owner");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare row"));
    assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
    drop(writer);
    let database = StoreDatabase::new(&source);
    let directory = tempfile::tempdir().expect("create snapshot directory");
    let image = database
        .capture_snapshot_image_for_test(store.root().clone(), directory.path().to_path_buf(), None)
        .await
        .expect("capture shared image");
    let coverage = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("read covered commits"),
    )
    .expect("build coverage");
    assert!(!coverage.commits().is_empty());
    let metadata = owner
        .publish_snapshot(image, coverage)
        .await
        .expect("publish snapshot");
    let membership = owner.membership_for_test().await.expect("load membership");
    if retire_covered_history {
        let (_, entries) = database
            .retained_store_publication()
            .await
            .expect("read the published snapshot interval");
        let mut removed = 0;
        for entry in entries {
            if let coven_protocol::store_commit::StorePublicationPayload::Commit(reference) =
                &entry.value.payload
            {
                home.remove_exact_object(reference.object.slot());
                home.remove_exact_object(entry.prepared.reference().slot());
                removed += 1;
            }
        }
        assert!(removed > 0, "the snapshot must cover retired history");
    }
    let target = directory.path().join("bootstrap.sqlite3");
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            database.schema_version(),
            &target,
            &signer,
        )
        .await
        .expect("bootstrap with a fresh history verifier");
    assert_eq!(
        bootstrap.selected_snapshot_hash_for_test(),
        metadata.snapshot_hash()
    );
    let image = DatabaseImageTest::open(&target).expect("open staged image");
    assert_eq!(
        image
            .query_row(
                "SELECT title FROM notes WHERE id = 'covered-row'",
                [],
                |row| { row.get::<_, String>(0) }
            )
            .expect("read snapshot row"),
        "In the snapshot"
    );
}

#[tokio::test]
async fn snapshot_boundaries_preserve_private_rows_and_roll_back_with_their_tail() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-boundary-transaction",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &target,
            target_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('shared-row', 'before snapshot', NULL, 1, \
         '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let mut writer = owner.authorize_writer().await.expect("authorize owner");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare row"));
    assert_eq!(writer.drain_store_writes().await.expect("publish row"), 1);
    drop(writer);
    let (_, first) = peer.pull_store().await.expect("pull initial row");
    assert!(first.held_positions.is_empty(), "{first:?}");
    target
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('private-row', 'recipient private row', NULL, 0, \
         '0000000001500-0000-peer', '2026-01-01')",
        )
        .await;
    let source_database = StoreDatabase::new(&source);
    let target_database = StoreDatabase::new(&target);
    let before_publication = target_database
        .store_current_publication()
        .await
        .expect("read peer publication");
    let before_coverage = target_database
        .snapshot_coverage_frontier()
        .await
        .expect("read peer baseline");
    let snapshot_dir = tempfile::tempdir().expect("create snapshot directory");
    let mut last_coverage = None;
    for (title, stamp) in [
        ("after first snapshot", "0000000002000-0000-owner"),
        ("after second snapshot", "0000000003000-0000-owner"),
    ] {
        let image = source_database
            .capture_snapshot_image_for_test(
                store.root().clone(),
                snapshot_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture snapshot");
        let coverage = CommitFrontier::from_refs(
            source_database
                .materialized_frontier()
                .await
                .expect("read source frontier"),
        )
        .expect("build coverage");
        owner
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish snapshot");
        last_coverage = Some(coverage);
        source
            .execute_test_host_write(&format!(
            "UPDATE notes SET title = '{title}', _updated_at = '{stamp}' WHERE id = 'shared-row'",
        ))
            .await;
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize tail writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare tail"));
        assert_eq!(writer.drain_store_writes().await.expect("publish tail"), 1);
    }
    target.fail_next_merge_materialization_at(
        MergeMaterializationFailurePoint::ProjectionReplacement,
    );
    let error = peer
        .pull_store()
        .await
        .expect_err("fail while installing snapshot tail");
    assert!(error.to_string().contains("injected"), "{error}");
    assert_eq!(
        target_database
            .store_current_publication()
            .await
            .expect("read rolled back publication"),
        before_publication
    );
    assert_eq!(
        target_database
            .snapshot_coverage_frontier()
            .await
            .expect("read rolled back baseline"),
        before_coverage
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'shared-row'")
            .await,
        "before snapshot"
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "recipient private row"
    );

    let (_, retried) = peer.pull_store().await.expect("retry complete interval");
    assert!(retried.held_positions.is_empty(), "{retried:?}");
    assert_eq!(
        target_database
            .snapshot_coverage_frontier()
            .await
            .expect("read adopted baseline"),
        last_coverage.expect("published snapshots")
    );
    assert_eq!(
        target_database
            .store_current_publication()
            .await
            .expect("read installed publication"),
        source_database
            .store_current_publication()
            .await
            .expect("read source publication")
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'shared-row'")
            .await,
        "after second snapshot"
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "recipient private row"
    );
    let (_, repeated) = peer.pull_store().await.expect("repeat installed interval");
    assert!(repeated.held_positions.is_empty(), "{repeated:?}");
    assert_eq!(repeated.changesets_applied, 0);
}

#[tokio::test]
async fn shared_snapshot_omits_publisher_circle_access() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-private-circle-access",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    owner
        .create_circle("0000000001000-0000-owner", "Publisher private Circle")
        .await
        .expect("create private Circle");
    let directory = tempfile::tempdir().expect("create snapshot directory");
    let image = StoreDatabase::new(&source)
        .capture_snapshot_image_for_test(store.root(), directory.path().to_path_buf(), None)
        .await
        .expect("capture shared image");
    let image = DatabaseImageTest::from_bytes(&image).expect("open shared image");
    let inputs = image
        .retained_materialization_bytes()
        .expect("read retained inputs");
    assert!(
        !inputs.is_empty(),
        "public Circle control remains in the image"
    );
    for bytes in inputs {
        let input: serde_json::Value =
            serde_json::from_slice(&bytes).expect("decode retained input");
        let circle_bytes: Vec<u8> =
            serde_json::from_value(input["activation"]["circle_activations"].clone())
                .expect("decode retained Circle bytes");
        let activations: serde_json::Value =
            serde_json::from_slice(&circle_bytes).expect("decode retained Circle activations");
        for circle in activations["circles"].as_array().expect("Circle controls") {
            assert!(
                circle["local_access"].is_null(),
                "shared image contains publisher-specific Circle access"
            );
        }
        assert!(
            activations["bootstraps"]
                .as_array()
                .expect("Circle bootstraps")
                .is_empty(),
            "recipient-specific Circle images do not belong in a Store snapshot"
        );
    }
    let cached_access = image
        .coven_table_row_count(DatabaseTestTable::named("circle_access_cache"))
        .expect("count snapshot access cache");
    assert_eq!(cached_access, 0);
    let private_metadata = image
        .circle_states_containing("Publisher private Circle")
        .expect("check snapshot Circle metadata");
    assert_eq!(private_metadata, 0);
    assert_ne!(
        source
            .table_row_count_for_test(DatabaseTestTable::named("circle_access_cache"))
            .await
            .expect("count publisher Circle access"),
        0,
        "capturing a shared snapshot preserves the publisher's local access"
    );
}

#[tokio::test]
async fn joining_after_a_shared_snapshot_restores_its_own_circle_access() {
    assert_join_restores_circle_access(false).await;
}

#[tokio::test]
async fn joining_restores_circle_access_created_after_the_snapshot() {
    assert_join_restores_circle_access(true).await;
}

async fn assert_join_restores_circle_access(snapshot_precedes_circle: bool) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-recipient-circle-access",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    if snapshot_precedes_circle {
        owner
            .ensure_device_join_snapshot_for_test()
            .await
            .expect("publish the snapshot before Circle creation");
    }
    let circle_id = owner
        .create_circle("0000000001000-0000-owner", "Recipient Circle")
        .await
        .expect("create Circle");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let joined = crate::sync::test_helpers::TestDevice::activate_joined(
        owner,
        StoreDatabase::new(&target),
        target_dir,
        &signer,
        "0000000002000-0000-joiner",
        storage,
    )
    .await
    .expect("join from the shared snapshot");
    let identity = coven_keys::keys::public_key_hex(&signer);
    StoreDatabase::new(&target)
        .circle_authoring_context(circle_id, &identity)
        .await
        .expect("joining device restores its own Circle access before completion");
    let (_, repeated) = joined.pull_store().await.expect("pull after joining");
    assert!(repeated.held_positions.is_empty(), "{repeated:?}");
    StoreDatabase::new(&target)
        .circle_authoring_context(circle_id, &identity)
        .await
        .expect("recipient Circle access survives replay");
}

#[tokio::test]
async fn joining_installs_circle_rows_after_a_tail_control() {
    fn open_database(directory: coven_foundation::store_dir::StoreDir) -> coven_database::Database {
        crate::sync::test_helpers::open_test_db_schema(
            directory,
            vec![coven_protocol::synced_schema::SyncedTable::new(
                "documents",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience")],
            vec![coven_database::Migration::sql(
                1,
                "Circle join row schema",
                "CREATE TABLE documents (
                    id TEXT PRIMARY KEY,
                    audience TEXT,
                    body TEXT NOT NULL,
                    _updated_at TEXT NOT NULL
                ) STRICT;",
            )],
        )
    }
    let source_dir = test_store_dir();
    let source = open_database(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "join-circle-tail-rows",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create scoped Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish empty snapshot");
    let circle = owner
        .create_circle("0000000001000-0000-owner", "Tail Circle")
        .await
        .expect("create Circle after snapshot");
    owner
        .rename_circle("0000000001500-0000-owner", circle, "Renamed tail Circle")
        .await
        .expect("publish a successor Circle control before its row");
    let insert = format!(
        "INSERT INTO documents (id, audience, body, _updated_at)
         VALUES ('00000000-0000-4000-8000-000000000001', '{circle}',
                 'Published after Circle creation', '0000000002000-0000-owner')"
    );
    StoreDatabase::new(&source)
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |transaction| {
                transaction
                    .execute_batch(&insert)
                    .map_err(coven_database::DbError::from)
            },
        )
        .await
        .expect("capture Circle row");
    let mut writer = owner
        .authorize_writer()
        .await
        .expect("authorize row writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare Circle row"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish Circle row"),
        1
    );
    drop(writer);
    let target_dir = test_store_dir();
    let target = open_database(target_dir.clone());
    let joined = crate::sync::test_helpers::TestDevice::activate_joined(
        owner,
        StoreDatabase::new(&target),
        target_dir,
        &signer,
        "0000000003000-0000-joiner",
        storage,
    )
    .await
    .expect("join through Circle control and row tail");
    assert_eq!(
        target
            .query_test_text(
                "SELECT body FROM documents WHERE id = '00000000-0000-4000-8000-000000000001'"
            )
            .await,
        "Published after Circle creation"
    );
    let (_, pulled) = joined.pull_store().await.expect("repeat joined history");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(pulled.changesets_applied, 0);
}

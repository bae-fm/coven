use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn accepted_peer_commits_trigger_the_owner_snapshot_policy() {
    let signer = UserKeypair::generate();
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let (store, _) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "shared-snapshot-cadence",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &signer)
        .await
        .unwrap();
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &owner_db,
            owner_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    {
        let mut writer = owner.authorize_writer().await.unwrap();
        let cut = writer.snapshots().capture_snapshot_cut(None).await.unwrap();
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-07-16T00:00:01Z".into())
            .await
            .expect("establish the accepted snapshot");
    }
    peer.pull_store().await.unwrap();
    let database = StoreDatabase::new(&owner_db);
    let before = database.store_current_publication().await.unwrap();
    let threshold = std::num::NonZeroU64::new(3).unwrap();
    for index in 0..threshold.get() {
        peer_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('peer-{index}', 'peer edit', NULL, 1, \
                 '0000000001000-0000-peer', '2026-07-16')"
            ))
            .await;
        assert!(peer.prepare_pending_store_write().await.unwrap());
        assert_eq!(peer.drain_store_writes().await.unwrap(), 1);
        owner.pull_store().await.expect("observe peer publication");
        if index + 1 < threshold.get() {
            owner
                .authorize_writer()
                .await
                .unwrap()
                .snapshots()
                .publish_due_snapshots("2026-07-18T00:00:00Z", None, false, threshold)
                .await
                .expect("evaluate below the configured threshold");
            assert_eq!(
                database
                    .store_current_publication()
                    .await
                    .unwrap()
                    .record()
                    .latest_snapshot(),
                before.record().latest_snapshot(),
                "elapsed time does not replace the configured accepted-commit threshold",
            );
        }
    }
    let frontier = database.materialized_frontier().await.unwrap();
    owner
        .authorize_writer()
        .await
        .unwrap()
        .snapshots()
        .publish_due_snapshots("2026-07-18T00:00:01Z", None, false, threshold)
        .await
        .expect("evaluate shared snapshot cadence");
    let after = database.store_current_publication().await.unwrap();
    assert_ne!(
        after.record().latest_snapshot(),
        before.record().latest_snapshot(),
        "accepted peer commits count even when the owner's author sequence has not advanced",
    );
    assert_eq!(database.materialized_frontier().await.unwrap(), frontier);
    owner
        .authorize_writer()
        .await
        .unwrap()
        .snapshots()
        .publish_due_snapshots("2026-07-20T00:00:00Z", None, false, threshold)
        .await
        .expect("evaluate after the accepted snapshot resets the count");
    assert_eq!(database.store_current_publication().await.unwrap(), after);
}

#[tokio::test]
async fn pending_circle_snapshot_resumes_below_the_store_commit_threshold() {
    exercise_circle_snapshot(CircleSnapshotCase::ResumeBelowThreshold).await;
}

#[tokio::test]
async fn pending_circle_snapshot_resumes_while_rotation_is_pending() {
    exercise_circle_snapshot(CircleSnapshotCase::ResumeDuringRotation).await;
}

#[tokio::test]
async fn circle_snapshot_publication_failure_reaches_the_initiator() {
    exercise_circle_snapshot(CircleSnapshotCase::ReportFailure).await;
}

#[tokio::test]
async fn circle_failure_does_not_reset_the_store_snapshot_threshold() {
    exercise_circle_snapshot(CircleSnapshotCase::PreserveThreshold).await;
}

#[tokio::test]
async fn circle_snapshot_exports_accepted_rows_while_preserving_unpublished_edits() {
    exercise_circle_snapshot(CircleSnapshotCase::CaptureUnpublished).await;
}

enum CircleSnapshotCase {
    ResumeBelowThreshold,
    ResumeDuringRotation,
    ReportFailure,
    PreserveThreshold,
    CaptureUnpublished,
}

async fn exercise_circle_snapshot(case: CircleSnapshotCase) {
    let signer = UserKeypair::generate();
    let directory = test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_schema(
        directory.clone(),
        vec![
            coven_protocol::synced_schema::SyncedTable::new(
                "documents",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
        ],
        vec![coven_database::Migration::sql(
            1,
            "Circle documents",
            "CREATE TABLE documents (id TEXT PRIMARY KEY, title TEXT NOT NULL, audience TEXT, _updated_at TEXT NOT NULL) STRICT;",
        )],
    );
    let home = test_cloud_home();
    let (store, _) = TestStore::create_with_connection(
        &db,
        directory.clone(),
        "circle-snapshot-retry",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&db, directory.clone(), &signer)
        .await
        .unwrap();
    let circle = owner
        .create_circle("0000000001000-0000-owner", "Household")
        .await
        .unwrap();
    let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let database = StoreDatabase::new(&db);
    let inputs = database
        .circle_acknowledgement_publication_inputs()
        .await
        .unwrap();
    assert_eq!(
        inputs.len(),
        1,
        "the created Circle has active publication access"
    );
    assert_eq!(inputs[0].circle_id(), circle);
    {
        let mut writer = owner.authorize_writer().await.unwrap();
        let cut = writer
            .snapshots()
            .capture_snapshot_cut(Some(&routing))
            .await
            .unwrap();
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-07-16T00:00:01Z".into())
            .await
            .unwrap();
    }
    if matches!(case, CircleSnapshotCase::CaptureUnpublished) {
        database.run_host_store_write_for_test(Some(routing.clone()), None, move |transaction| {
            transaction.execute(
                "INSERT INTO documents (id, title, audience, _updated_at) VALUES \
                 ('12345678-1234-4234-8234-123456789abc', 'Accepted title', ?1, '0000000002000-0000-owner')",
                [circle.to_string()],
            ).map(|_| ()).map_err(coven_database::DbError::from)
        }).await.unwrap();
        assert!(owner.prepare_pending_store_write().await.unwrap());
        assert_eq!(owner.drain_store_writes().await.unwrap(), 1);
        database.run_host_store_write_for_test(Some(routing.clone()), None, |transaction| {
            transaction.execute_batch(
                "UPDATE documents SET title = 'Unpublished title', _updated_at = '0000000003000-0000-owner'",
            ).map_err(coven_database::DbError::from)
        }).await.unwrap();
        let journal = database.store_write_journal_for_test().await.unwrap();
        let before = database.store_current_publication().await.unwrap();
        let cut = owner
            .authorize_writer()
            .await
            .unwrap()
            .circles()
            .snapshots()
            .capture_circle_snapshot_cut(&routing, circle)
            .await
            .expect("capture the accepted Circle state while a local edit remains pending");
        let staged = database
            .circle_bootstrap_rows_image_for_test(cut.rows().to_vec())
            .await
            .expect("stage the captured Circle bootstrap rows");
        let image = coven_database::DatabaseImageTest::from_bytes(&staged).unwrap();
        let row: (String, String) = image.query_row(
            "SELECT title, audience FROM documents WHERE id = '12345678-1234-4234-8234-123456789abc'",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(row, ("Accepted title".into(), circle.to_string()));
        assert_eq!(
            db.query_test_text("SELECT title FROM documents").await,
            "Unpublished title"
        );
        assert_eq!(
            database.store_write_journal_for_test().await.unwrap(),
            journal
        );
        assert_eq!(database.store_current_publication().await.unwrap(), before);
        return;
    }
    if matches!(case, CircleSnapshotCase::PreserveThreshold) {
        database.run_host_store_write_for_test(Some(routing.clone()), None, |transaction| {
            transaction.execute_batch(
                "INSERT INTO documents (id, title, audience, _updated_at) VALUES \
                 ('12345678-1234-4234-8234-123456789abc', 'Accepted title', NULL, '0000000002000-0000-owner')",
            ).map_err(coven_database::DbError::from)
        }).await.unwrap();
        assert!(owner.prepare_pending_store_write().await.unwrap());
        assert_eq!(owner.drain_store_writes().await.unwrap(), 1);
    }
    let accepted = database.store_current_publication().await.unwrap();
    home.fail_exact_create_before_call(1);
    if matches!(case, CircleSnapshotCase::PreserveThreshold) {
        let threshold = std::num::NonZeroU64::new(1).unwrap();
        owner
            .authorize_writer()
            .await
            .unwrap()
            .snapshots()
            .publish_due_snapshots("2026-07-16T00:00:02Z", Some(&routing), false, threshold)
            .await
            .expect_err("a Circle failure aborts the snapshot attempt");
        assert!(
            database
                .outbound_circle_snapshot_publication(circle)
                .await
                .unwrap()
                .is_some(),
            "the failed operation must be the Circle snapshot required before Store compaction"
        );
        assert_eq!(
            database.store_current_publication().await.unwrap(),
            accepted,
            "failed Circle work must leave the Store snapshot threshold due"
        );
        owner
            .authorize_writer()
            .await
            .unwrap()
            .snapshots()
            .publish_due_snapshots("2026-07-16T00:00:03Z", Some(&routing), false, threshold)
            .await
            .expect("retry both snapshots from retained work");
        assert!(database
            .outbound_circle_snapshot_publication(circle)
            .await
            .unwrap()
            .is_none());
        assert_ne!(
            database
                .store_current_publication()
                .await
                .unwrap()
                .record()
                .latest_snapshot(),
            accepted.record().latest_snapshot()
        );
        return;
    }
    if matches!(
        case,
        CircleSnapshotCase::ResumeBelowThreshold | CircleSnapshotCase::ResumeDuringRotation
    ) {
        let error = store
            .push_circle_snapshots(
                &db,
                directory.clone(),
                directory.as_ref().join("snapshot-retry"),
                db.schema_version(),
                "2026-07-16T00:00:02Z",
                &routing,
            )
            .await
            .expect_err("interrupt the prepared Circle image upload");
        assert!(
            error
                .to_string()
                .contains("forced failure before exact create"),
            "{error:?}"
        );
    } else {
        let result = owner
            .authorize_writer()
            .await
            .unwrap()
            .circles()
            .snapshots()
            .push_circle_snapshots(db.schema_version(), "2026-07-16T00:00:02Z", Some(&routing))
            .await;
        assert!(database
            .outbound_circle_snapshot_publication(circle)
            .await
            .unwrap()
            .is_some());
        result.expect_err("the initiator must see an unfinished Circle publication");
        return;
    }
    let pending = database
        .outbound_circle_snapshot_publication(circle)
        .await
        .unwrap()
        .expect("retain the exact interrupted publication");
    let reopened = store.bind_device_in(&db, directory, &signer).await.unwrap();
    reopened
        .authorize_writer()
        .await
        .unwrap()
        .snapshots()
        .publish_due_snapshots(
            "2026-07-16T00:00:03Z",
            Some(&routing),
            matches!(case, CircleSnapshotCase::ResumeDuringRotation),
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("resume durable Circle work without a new Store snapshot");
    assert!(
        database
            .outbound_circle_snapshot_publication(circle)
            .await
            .unwrap()
            .is_none(),
        "the Store cadence must not strand a pending Circle publication"
    );
    assert_eq!(
        database
            .latest_local_circle_snapshot(circle)
            .await
            .unwrap()
            .unwrap()
            .reference,
        pending.reference
    );
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        accepted
    );
}

#[tokio::test]
async fn member_publication_overshoots_the_shared_threshold_while_the_owner_is_offline() {
    let signer = UserKeypair::generate();
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let (store, _) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "offline-owner-snapshot-cadence",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let first_dir = test_store_dir();
    let first_db = open_test_db(first_dir.clone());
    let first_signer = UserKeypair::generate();
    let first = store
        .admit_and_activate_peer(
            &owner_db,
            owner_dir.clone(),
            &first_db,
            first_dir,
            &first_signer,
        )
        .await
        .expect("admit the first Member author");
    let second_dir = test_store_dir();
    let second_db = open_test_db(second_dir.clone());
    let second_signer = UserKeypair::generate();
    let second = store
        .admit_and_activate_peer(
            &owner_db,
            owner_dir.clone(),
            &second_db,
            second_dir,
            &second_signer,
        )
        .await
        .expect("admit the second Member author");
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &signer)
        .await
        .unwrap();
    owner.pull_store().await.unwrap();
    {
        let mut writer = owner.authorize_writer().await.unwrap();
        let cut = writer.snapshots().capture_snapshot_cut(None).await.unwrap();
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".into())
            .await
            .unwrap();
    }
    first.pull_store().await.unwrap();
    second.pull_store().await.unwrap();
    let database = StoreDatabase::new(&owner_db);
    let baseline = database.store_current_publication().await.unwrap();
    let baseline_snapshot = baseline.record().latest_snapshot().unwrap();
    assert_eq!(
        baseline.record().accepted().unwrap(),
        &baseline_snapshot.publication
    );
    drop(owner);
    let threshold = std::num::NonZeroU64::new(4).unwrap();
    let mut author_streams = std::collections::BTreeSet::new();
    for index in 0..6 {
        let (member, member_db) = if index % 2 == 0 {
            (&first, &first_db)
        } else {
            (&second, &second_db)
        };
        member
            .pull_store()
            .await
            .expect("observe the preceding author's commit");
        member_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('overshoot-{index}', 'Member edit', 1, '0000000003000-0000-member', '2026-09-08')"
            ))
            .await;
        assert!(member.prepare_pending_store_write().await.unwrap());
        assert_eq!(
            member.drain_store_writes().await.unwrap(),
            1,
            "ordinary publication must continue without an available Owner"
        );
        author_streams.insert(
            member
                .latest_local_store_position()
                .await
                .unwrap()
                .unwrap()
                .coord
                .stream_id,
        );
        let member_database = StoreDatabase::new(member_db);
        let accepted = member_database.store_current_publication().await.unwrap();
        assert_eq!(
            accepted.record().accepted().unwrap().position.get()
                - baseline_snapshot.publication.position.get(),
            index + 1
        );
        member
            .authorize_writer()
            .await
            .unwrap()
            .snapshots()
            .publish_due_snapshots("2026-09-08T00:00:02Z", None, false, threshold)
            .await
            .expect("an overdue snapshot must not reject Member publication");
        assert_eq!(
            member_database.store_current_publication().await.unwrap(),
            accepted
        );
        assert_eq!(accepted.record().latest_snapshot(), Some(baseline_snapshot));
    }
    assert_eq!(author_streams.len(), 2);
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        baseline,
        "the offline Owner has not observed or published during the overshoot"
    );
    let owner = store
        .bind_device_in(&owner_db, owner_dir, &signer)
        .await
        .unwrap();
    owner
        .pull_store()
        .await
        .expect("the returning Owner observes all Member commits");
    let overdue = database.store_current_publication().await.unwrap();
    assert_eq!(
        overdue.record().accepted().unwrap().position.get()
            - overdue
                .record()
                .latest_snapshot()
                .unwrap()
                .publication
                .position
                .get(),
        6
    );
    let frontier = database.materialized_frontier().await.unwrap();
    assert_eq!(owner.replay_row_count_for_test("notes").await.unwrap(), 6);
    owner
        .authorize_writer()
        .await
        .unwrap()
        .snapshots()
        .publish_due_snapshots("2026-09-08T00:00:03Z", None, false, threshold)
        .await
        .expect("publish the aggregate accepted cut when an Owner returns");
    let after = database.store_current_publication().await.unwrap();
    assert_ne!(after.record().latest_snapshot(), Some(baseline_snapshot));
    assert_eq!(
        after.record().accepted().unwrap(),
        &after.record().latest_snapshot().unwrap().publication
    );
    assert_eq!(database.materialized_frontier().await.unwrap(), frontier);
    assert_eq!(owner.replay_row_count_for_test("notes").await.unwrap(), 6);
    owner
        .authorize_writer()
        .await
        .unwrap()
        .snapshots()
        .publish_due_snapshots("2026-09-08T00:00:04Z", None, false, threshold)
        .await
        .expect("the accepted snapshot resets the aggregate threshold");
    assert_eq!(database.store_current_publication().await.unwrap(), after);
    for member in [&first, &second] {
        member.pull_store().await.unwrap();
        assert_eq!(member.replay_row_count_for_test("notes").await.unwrap(), 6);
    }
}

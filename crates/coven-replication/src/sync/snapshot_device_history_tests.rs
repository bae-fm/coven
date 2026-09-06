use super::*;

#[tokio::test]
async fn data_commits_share_one_device_state_body() {
    let fixture = PublishedHistory::publish(12).await;
    assert_eq!(
        fixture
            .db
            .table_row_count_for_test(coven_database::DatabaseTestTable::named(
                "store_device_state_snapshots",
            ))
            .await
            .expect("count exact commit references"),
        12,
    );
    assert_eq!(
        fixture
            .db
            .table_row_count_for_test(coven_database::DatabaseTestTable::named(
                "store_device_states",
            ))
            .await
            .expect("count distinct device states"),
        1,
        "data commits with an unchanged roster share its stored body",
    );
    for entry in fixture.retained_history().await {
        let (_, state) = store_database(&fixture.db)
            .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
                std::collections::BTreeMap::from([(
                    entry.commit_ref().coord.stream_id,
                    entry.commit_ref().clone(),
                )]),
            ))
            .await
            .expect("resolve each exact commit");
        assert_eq!(state.state_hash, entry.commit().device_state.state_hash());
    }
}

#[tokio::test]
async fn a_lagging_peer_can_publish_after_the_owner_advances_its_snapshot() {
    exercise_lagging_peer(false).await;
}

#[tokio::test]
async fn accepted_history_supplies_a_device_state_pruned_from_the_replay_image() {
    exercise_lagging_peer(true).await;
}

async fn exercise_lagging_peer(prune_image: bool) {
    let fixture = AcknowledgedHistory::publish(2).await;
    let peer_frontier = store_database(&fixture.peer_db)
        .materialized_frontier()
        .await
        .expect("peer's accepted history");
    HistoryPublisher::new(&fixture.db, &fixture.device)
        .publish_note(100)
        .await;
    for _ in 0..3 {
        fixture.publish_snapshot_now().await;
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("owner publishes its snapshot acknowledgement");
        fixture
            .peer
            .run_cycle(None)
            .await
            .expect("peer crosses and acknowledges the snapshot");
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("owner observes every active writer past the snapshot");
    }

    let baseline = store_database(&fixture.db)
        .installed_replay_baseline()
        .await
        .expect("read owner's baseline");
    let reference = peer_frontier
        .values()
        .find(|reference| {
            baseline.coverage().covers_commit(reference)
                && !baseline.coverage().0.values().any(|tip| tip == *reference)
        })
        .expect("peer depends on a position below the snapshot tips")
        .clone();
    if prune_image {
        store_database(&fixture.db)
            .replace_replay_baseline_device_state_for_test(reference, None)
            .await
            .expect("remove a covered state from the replay image");
    }

    HistoryPublisher::new(&fixture.peer_db, &fixture.peer)
        .publish_note(101)
        .await;
    fixture
        .device
        .pull_store()
        .await
        .expect("apply lagging peer's update");
    assert!(
        fixture
            .db
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'history-101'")
            .await
    );
    HistoryPublisher::new(&fixture.db, &fixture.device)
        .publish_note(102)
        .await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("publish after replay");
    fixture
        .peer
        .run_cycle(None)
        .await
        .expect("peer reads owner's publication");
    for db in [&fixture.db, &fixture.peer_db] {
        for id in [101, 102] {
            assert!(
                db.test_row_exists(&format!("SELECT 1 FROM notes WHERE id = 'history-{id}'"))
                    .await
            );
        }
    }
}

#[tokio::test]
async fn snapshots_keep_device_states_below_their_coverage_tips() {
    let fixture = AcknowledgedHistory::publish(2).await;
    let earlier = store_database(&fixture.db)
        .materialized_frontier()
        .await
        .expect("earlier frontier");
    fixture.publish_round(3).await;
    let image_dir = tempfile::tempdir().expect("snapshot directory");
    let (image, _) = store_database(&fixture.db)
        .capture_store_snapshot_cut(
            fixture._store.root().clone(),
            image_dir.path().to_path_buf(),
            None,
        )
        .await
        .expect("capture snapshot");
    for reference in earlier.values() {
        image
            .device_state_for_test(reference)
            .await
            .expect("snapshot retains the accepted device state below its coverage tips");
    }
}

#[tokio::test]
async fn advancing_snapshots_preserve_shared_device_states_and_old_references() {
    let fixture = AcknowledgedHistory::publish(2).await;
    let states_table = coven_database::DatabaseTestTable::named("store_device_states");
    let initial_bodies = fixture
        .db
        .table_row_count_for_test(states_table)
        .await
        .expect("count initial state bodies");
    assert!(
        initial_bodies > 1,
        "device activation produced distinct states"
    );
    let references = fixture
        .db
        .store_device_state_snapshot_refs_for_test()
        .await
        .expect("initial exact references");
    let mut expected = Vec::new();
    for reference in references {
        let (_, state) = store_database(&fixture.db)
            .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
                std::collections::BTreeMap::from([(reference.coord.stream_id, reference.clone())]),
            ))
            .await
            .expect("initial state");
        expected.push((reference, state));
    }
    for round in 3..=5 {
        fixture.publish_round(round).await;
        let image_dir = tempfile::tempdir().expect("snapshot directory");
        let (image, coverage) = store_database(&fixture.db)
            .capture_store_snapshot_cut(
                fixture._store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture snapshot");
        let bytes = image.read_image().await.expect("snapshot bytes");
        let inspected = coven_database::DatabaseImageTest::from_bytes(&bytes).expect("open image");
        assert_eq!(
            inspected
                .coven_table_row_count(states_table)
                .expect("snapshot bodies"),
            initial_bodies
        );
        for (reference, state) in &expected {
            assert_eq!(
                &image
                    .device_state_for_test(reference)
                    .await
                    .expect("old exact reference"),
                state
            );
        }
        fixture
            .device
            .publish_snapshot(bytes, coverage)
            .await
            .expect("publish snapshot");
        fixture.settle_onto_the_published_snapshot().await;
        for (database_index, db) in [&fixture.db, &fixture.peer_db].into_iter().enumerate() {
            assert_eq!(
                db.table_row_count_for_test(states_table)
                    .await
                    .expect("live bodies"),
                initial_bodies
            );
            let baseline = store_database(db)
                .installed_replay_baseline()
                .await
                .expect("advanced baseline");
            for (reference, state) in &expected {
                assert_eq!(
                    baseline.covered_state(reference),
                    Some(state),
                    "round {round}, database {database_index}, reference {reference:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn conflicting_replay_device_history_does_not_change_accepted_data() {
    let fixture = AcknowledgedHistory::publish(2).await;
    let earlier = store_database(&fixture.db)
        .materialized_frontier()
        .await
        .expect("earlier frontier");
    let genesis = store_database(&fixture.db)
        .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
            std::collections::BTreeMap::new(),
        ))
        .await
        .expect("genesis device state")
        .1;
    HistoryPublisher::new(&fixture.db, &fixture.device)
        .publish_note(100)
        .await;
    fixture.publish_snapshot_now().await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("owner acknowledges snapshot");
    fixture
        .peer
        .run_cycle(None)
        .await
        .expect("peer crosses and acknowledges snapshot");
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("owner observes every active writer past the snapshot");
    let baseline = store_database(&fixture.db)
        .installed_replay_baseline()
        .await
        .expect("baseline");
    let reference = earlier
        .values()
        .find(|reference| {
            baseline.coverage().covers_commit(reference)
                && !baseline.coverage().0.values().any(|tip| tip == *reference)
        })
        .expect("non-tip reference")
        .clone();
    store_database(&fixture.db)
        .replace_replay_baseline_device_state_for_test(reference, Some(genesis))
        .await
        .expect("install conflicting canonical state in image");
    let before = store_database(&fixture.db)
        .materialized_frontier()
        .await
        .expect("accepted frontier");
    HistoryPublisher::new(&fixture.peer_db, &fixture.peer)
        .publish_note(101)
        .await;
    let error = fixture
        .device
        .pull_store()
        .await
        .expect_err("conflicting state must stop replay");
    assert!(
        format!("{error:?}").contains("replay image device state disagrees with accepted history"),
        "{error:?}"
    );
    assert_eq!(
        before,
        store_database(&fixture.db)
            .materialized_frontier()
            .await
            .expect("unchanged frontier")
    );
    assert!(
        fixture
            .db
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'history-100'")
            .await
    );
    assert!(
        !fixture
            .db
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'history-101'")
            .await
    );
}

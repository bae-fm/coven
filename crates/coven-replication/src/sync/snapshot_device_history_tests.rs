use super::*;

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
    fixture.publish_snapshot_now().await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("owner acknowledges snapshot");

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
        .expect("acknowledge snapshot");
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

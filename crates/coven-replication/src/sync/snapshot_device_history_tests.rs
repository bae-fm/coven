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
async fn a_peer_can_publish_after_adopting_compacted_device_history() {
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
            .expect("owner adopts its accepted snapshot");
        fixture
            .peer
            .run_cycle(None)
            .await
            .expect("peer adopts the accepted snapshot");
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
        .expect("peer's earlier history lies below the snapshot tips")
        .clone();
    assert!(
        baseline.covered_state(&reference).is_none(),
        "ordinary covered history no longer retains each device-state mapping"
    );

    HistoryPublisher::new(&fixture.peer_db, &fixture.peer)
        .publish_note(101)
        .await;
    fixture
        .device
        .pull_store()
        .await
        .expect("apply peer's update after adoption");
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
async fn snapshots_retire_device_states_below_their_coverage_tips() {
    let fixture = AcknowledgedHistory::publish(2).await;
    let earlier = store_database(&fixture.db)
        .materialized_frontier()
        .await
        .expect("earlier frontier");
    fixture.publish_round(3).await;
    let image_dir = tempfile::tempdir().expect("snapshot directory");
    let (image, coverage) = store_database(&fixture.db)
        .capture_store_snapshot_cut(
            fixture._store.root().clone(),
            image_dir.path().to_path_buf(),
            None,
        )
        .await
        .expect("capture snapshot");
    let retired = earlier
        .values()
        .filter(|reference| !coverage.commits().values().any(|tip| tip == *reference))
        .collect::<Vec<_>>();
    assert!(!retired.is_empty());
    for reference in retired {
        image
            .device_state_for_test(reference)
            .await
            .expect_err("ordinary covered state has no continuing exact consumer");
    }
    for reference in coverage.commits().values() {
        image
            .device_state_for_test(reference)
            .await
            .expect("checkpoint tip state");
    }
}

#[tokio::test]
async fn advancing_snapshots_preserve_checkpoint_states_and_retire_old_references() {
    let fixture = AcknowledgedHistory::publish(2).await;
    let old_references = fixture
        .db
        .store_device_state_snapshot_refs_for_test()
        .await
        .expect("initial exact references");
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
        let mut expected = Vec::new();
        for reference in coverage.commits().values() {
            let (_, state) = store_database(&fixture.db)
                .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
                    std::collections::BTreeMap::from([(
                        reference.coord.stream_id,
                        reference.clone(),
                    )]),
                ))
                .await
                .expect("accepted checkpoint state");
            assert_eq!(
                image
                    .device_state_for_test(reference)
                    .await
                    .expect("image checkpoint state"),
                state
            );
            expected.push((reference.clone(), state));
        }
        let bytes = image.read_image().await.expect("snapshot bytes");
        let inspected = coven_database::DatabaseImageTest::from_bytes(&bytes).expect("open image");
        assert_eq!(
            inspected
                .store_device_state_snapshot_refs()
                .expect("snapshot state refs")
                .len(),
            expected.len(),
            "this Store has no retained Circle or exclusion state consumer"
        );
        fixture
            .device
            .publish_snapshot(bytes, coverage)
            .await
            .expect("publish snapshot");
        fixture.settle_onto_the_published_snapshot().await;
        for db in [&fixture.db, &fixture.peer_db] {
            let baseline = store_database(db)
                .installed_replay_baseline()
                .await
                .expect("advanced baseline");
            for (reference, state) in &expected {
                assert_eq!(baseline.covered_state(reference), Some(state));
            }
            let retired = old_references
                .iter()
                .filter(|reference| !expected.iter().any(|(tip, _)| tip == *reference))
                .collect::<Vec<_>>();
            assert!(!retired.is_empty());
            for reference in retired {
                assert!(
                    baseline.covered_state(reference).is_none(),
                    "retired state {reference:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn conflicting_replay_device_history_does_not_change_accepted_data() {
    let fixture = AcknowledgedHistory::publish(2).await;
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
    let reference = baseline
        .coverage()
        .commits()
        .values()
        .next()
        .expect("checkpoint has an exact tip")
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
    assert!(format!("{error:?}").contains("device state"), "{error:?}");
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

use super::*;

#[tokio::test]
async fn an_observed_snapshot_predecessor_with_missing_rows_downloads_the_image() {
    let context = "accepted predecessor with an unavailable row package";
    let source_directory = test_store_dir();
    let source = open_database(&source_directory, "held-source", context);
    let signer = user_keypair_from_seed([62; 32]);
    let home = test_cloud_home();
    let store = TestStore::create(
        &source,
        source_directory.clone(),
        "observed-incomplete-snapshot-predecessor",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect(context);
    let receiver =
        SnapshotAdoptionReceiver::join(&store, &source, &source_directory, &signer, context).await;
    let owner = store
        .bind_device_in(&source, source_directory, &signer)
        .await
        .expect(context);
    owner.pull_store().await.expect(context);
    receiver.pull(context).await;
    capture(
        &source,
        &owner,
        WriteBatch::new(),
        "INSERT INTO notes(id, title, body, shared, _updated_at, created_at)
         VALUES ('held-row', 'Accepted but unavailable', 'Retained body', 1,
         '0000000009000-0000-source', '2026-09-08');"
            .into(),
        context,
    )
    .await;
    publish(&owner, context).await;
    let reference = owner
        .latest_local_store_position()
        .await
        .expect(context)
        .expect(context);
    let commit = owner.load_commit_for_test(&reference).await.expect(context);
    let package = commit.store_package().expect(context);
    home.remove_exact_object(package.object.slot());
    let (_, held) = receiver.device.pull_store().await.expect(context);
    assert!(
        held.held_positions.iter().any(|held| {
            matches!(&held.coordinate,
                crate::sync::store::pull::HeldStoreCoordinate::Package { device_id, seq, package_hash }
                if device_id == &reference.coord.stream_id.to_string()
                    && *seq == reference.coord.sequence()
                    && *package_hash == package.content_hash)
        }),
        "{context}: {held:?}"
    );
    assert!(
        !receiver
            .database
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'held-row'")
            .await,
        "{context}: missing package was materialized"
    );
    let observed = StoreDatabase::new(&receiver.database)
        .store_current_publication()
        .await
        .expect(context);
    assert_eq!(
        observed,
        StoreDatabase::new(&source)
            .store_current_publication()
            .await
            .expect(context),
        "{context}: accepted prefix was not observed"
    );
    let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
        receiver
            .device
            .materialized_frontier()
            .await
            .expect(context),
    )
    .expect(context);
    let snapshot = owner
        .publish_snapshot_generation_for_test()
        .await
        .expect(context);
    assert_eq!(
        &snapshot.meta.publication_predecessor,
        observed.record(),
        "{context}: snapshot predecessor differs from the observed prefix"
    );
    assert!(
        !frontier.covers(&snapshot.meta.coverage),
        "{context}: receiver already materialized the snapshot cut"
    );
    home.clear_exact_reads();
    receiver.pull(context).await;
    assert!(
        home.exact_reads()
            .contains(snapshot.meta.image.object.slot()),
        "{context}: incomplete local history did not select the accepted image"
    );
    assert_eq!(
        receiver
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'held-row'")
            .await,
        "Accepted but unavailable",
        "{context}: image did not install the covered row"
    );
}

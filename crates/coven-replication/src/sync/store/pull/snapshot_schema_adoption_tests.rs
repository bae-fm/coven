use super::*;

#[tokio::test]
async fn locally_reconstructable_snapshot_rejects_a_newer_schema_without_retiring_history() {
    let paths = tempfile::tempdir().expect("schema adoption databases");
    let source_path = paths.path().join("source.db");
    let source_directory = test_store_dir();
    let source_host_device = "schema-source";
    let source = open_receiver(
        &source_path,
        source_directory.clone(),
        source_host_device,
        &test_migrations(),
    );
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_directory.clone(),
        "same-cut-newer-schema",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create source");
    let owner = store
        .bind_device_in(&source, source_directory.clone(), &signer)
        .await
        .expect("bind source");
    source
        .execute_test_host_write(
            "INSERT INTO notes(id, title, body, shared, _updated_at, created_at)
         VALUES ('shared', 'Original title', 'Original body', 1,
         '0000000001000-0000-source', '2026-09-08')",
        )
        .await;
    publish(&owner).await;
    let receiver_directory = test_store_dir();
    let receiver_database = open_receiver(
        &paths.path().join("receiver.db"),
        receiver_directory.clone(),
        "schema-receiver",
        &test_migrations(),
    );
    let receiver = store
        .activate_joined_device(
            &source,
            source_directory.clone(),
            &receiver_database,
            receiver_directory,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("join receiver before the migration");
    owner.pull_store().await.expect("source observes join");
    receiver
        .pull_store()
        .await
        .expect("receiver observes full prefix");
    receiver_database
        .execute_test_host_write(
            "INSERT INTO notes(id, title, shared, _updated_at, created_at)
         VALUES ('pending-private', 'Unpublished private row', 0,
         '0000000002000-0000-receiver', '2026-09-08')",
        )
        .await;
    let records = StoreDatabase::new(&receiver_database);
    let before_current = records.store_current_publication().await.unwrap();
    let before_baseline = records.installed_replay_baseline().await.unwrap();
    let before_journal = records.store_write_journal_for_test().await.unwrap();
    let before_retained = receiver
        .retained_merge_replay_inputs_for_test()
        .await
        .unwrap();
    let before_frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
        receiver.materialized_frontier().await.unwrap(),
    )
    .unwrap();
    assert_eq!(
        before_current,
        StoreDatabase::new(&source)
            .store_current_publication()
            .await
            .unwrap()
    );
    drop(owner);
    drop(store);
    drop(source);

    let mut migrations = test_migrations();
    migrations.push(Migration::run(2, "schema-only-note-annotation", |sql| {
        sql.execute_batch(
            "ALTER TABLE notes ADD COLUMN annotation TEXT NOT NULL DEFAULT 'Migrated annotation'",
        )?;
        Ok(())
    }));
    let migrated_source = open_receiver(
        &source_path,
        source_directory.clone(),
        source_host_device,
        &migrations,
    );
    let owner = TestDevice::load(&migrated_source, source_directory, storage, signer)
        .await
        .expect("reopen publisher after its real schema migration");
    let snapshot = owner.publish_snapshot_generation_for_test().await.unwrap();
    assert_eq!(snapshot.meta.schema_version, 2);
    assert_eq!(receiver_database.schema_version(), 1);
    assert_eq!(
        &snapshot.meta.publication_predecessor,
        before_current.record()
    );
    assert_eq!(snapshot.meta.coverage, before_frontier);
    home.clear_exact_reads();

    let error = receiver.pull_store().await.expect_err(
        "an installed older schema cannot reconstruct a newer-schema snapshot at the same cut",
    );
    let mut cause = Some(&error as &(dyn std::error::Error + 'static));
    let mut pull_error = None;
    while let Some(error) = cause {
        if let Some(pull) = error.downcast_ref::<crate::sync::store::StorePullError>() {
            pull_error = Some(pull);
            break;
        }
        cause = error.source();
    }
    assert!(
        matches!(
            pull_error,
            Some(crate::sync::store::StorePullError::SnapshotRestoration(snapshot))
                if matches!(snapshot.as_ref(),
                    crate::sync::store::snapshots::SnapshotError::SchemaTooNew {
                        snapshot_version: 2,
                        supported: 1,
                    })
        ),
        "{error:?}"
    );
    assert_eq!(
        records.store_current_publication().await.unwrap(),
        before_current
    );
    let baseline = records.installed_replay_baseline().await.unwrap();
    assert_eq!(baseline.coverage(), before_baseline.coverage());
    assert_eq!(baseline.snapshot(), before_baseline.snapshot());
    assert_eq!(
        records.store_write_journal_for_test().await.unwrap(),
        before_journal
    );
    let retained = receiver
        .retained_merge_replay_inputs_for_test()
        .await
        .unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|input| input.commit_ref())
            .collect::<Vec<_>>(),
        before_retained
            .iter()
            .map(|input| input.commit_ref())
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        receiver_database
            .query_test_text("SELECT title FROM notes WHERE id = 'pending-private'")
            .await,
        "Unpublished private row"
    );
    assert!(
        !home
            .exact_reads()
            .contains(snapshot.meta.image.object.slot()),
        "schema refusal must precede any snapshot image download"
    );
}

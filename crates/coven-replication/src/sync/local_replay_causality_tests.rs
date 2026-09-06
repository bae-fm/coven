use crate::sync::store::pull::HeldStorePositionReason;
use crate::sync::test_helpers::*;
use coven_database::{
    DbError, HostWriteOperation, Migration, StoreDatabase, StoreRowWrites, WriteBatch,
};
use coven_protocol::store_commit::CommitFrontier;
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};

pub(super) async fn drain(device: &TestDevice) {
    for attempt in 0..8 {
        if !device
            .publish_pending_store_database()
            .await
            .expect("publish pending Store write")
        {
            return;
        }
        assert!(attempt < 7, "publication did not drain");
    }
}

pub(super) async fn advance_current_baseline(
    store: &std::sync::Arc<TestStore>,
    database: &StoreDatabase,
    device: &TestDevice,
) {
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = database
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture snapshot image");
    let cut = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("valid materialized frontier");
    device
        .publish_snapshot(image, cut.clone())
        .await
        .expect("publish snapshot");
    device
        .advance_baseline_by_acknowledging(cut)
        .await
        .expect("acknowledge snapshot")
        .expect("advance replay baseline");
}

#[tokio::test]
async fn baseline_replay_does_not_restore_a_deleted_private_origin_child() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([31; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-origin-delete",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('release', 'Causal Constellations', NULL, 0, \
                  '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO note_tags VALUES \
                 ('old-discogs', 'release', 'discogs', \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private rows");
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET shared = 1, \
                 _updated_at = '0000000002000-0000-author' WHERE id = 'release';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("share rows");
    drain(&device).await;
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch("DELETE FROM note_tags WHERE id = 'old-discogs';")?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("delete child");
    drain(&device).await;

    advance_current_baseline(&store, &database, &device).await;

    assert_eq!(
        device
            .replay_row_count_for_test("note_tags")
            .await
            .expect("replay baseline"),
        0,
        "the replay baseline preserves the accepted deletion",
    );
}

#[tokio::test]
async fn baseline_replay_keeps_a_shared_row_after_it_becomes_private() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([32; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-withdrawal",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('release', 'Causal Constellations', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture shared row");
    drain(&device).await;
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET shared = 0, \
                 _updated_at = '0000000002000-0000-author' WHERE id = 'release';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("make row private");
    drain(&device).await;

    advance_current_baseline(&store, &database, &device).await;

    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay baseline"),
        1,
        "the replay baseline retains the author's private row",
    );
}

#[tokio::test]
async fn late_peer_update_does_not_replace_a_withdrawn_private_row() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([33; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "withdrawn-concurrent-row",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&db, store_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let author_database = store_database(&db);
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &db,
            store_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('release', 'Shared title', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture shared row");
    drain(&author).await;
    peer.pull_store().await.expect("peer pulls shared row");

    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET title = 'Concurrent peer title', \
                 _updated_at = '0000000003000-0000-peer' WHERE id = 'release';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture peer edit");
    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET shared = 0, \
                 _updated_at = '0000000002000-0000-author' WHERE id = 'release';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("withdraw author row");
    drain(&author).await;
    drain(&peer).await;

    let (_, pull) = author.pull_store().await.expect("pull peer update");
    assert!(
        pull.held_positions.is_empty()
            || pull.held_positions.iter().any(|held| matches!(
                &held.reason,
                HeldStorePositionReason::PrivateSharedConflict { table, row_id, .. }
                    if table == "notes" && row_id == "release"
            ))
    );
    assert_eq!(
        author
            .query_test_text(
                "SELECT title || ':shared=' || shared FROM notes WHERE id = 'release'",
            )
            .await,
        "Shared title:shared=0",
        "the held peer commit leaves the private row intact",
    );
}

#[tokio::test]
async fn equivalent_private_row_adopts_the_incoming_shared_version() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([37; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "equivalent-private-adoption",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('same-row', 'Same title', 'Same body', 0, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private row");
    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('same-row', 'Same title', 'Same body', 1, \
                  '0000000002000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture equivalent shared row");
    drain(&peer).await;

    let (_, pull) = author.pull_store().await.expect("pull equivalent row");

    assert!(pull.held_positions.is_empty());
    assert_eq!(
        author
            .query_test_text(
                "SELECT title || ':' || body || ':shared=' || shared || ':stamp=' || _updated_at \
                 FROM notes WHERE id = 'same-row'",
            )
            .await,
        "Same title:Same body:shared=1:stamp=0000000002000-0000-peer",
        "accepted locality and version metadata replace the equivalent private metadata",
    );
}

#[tokio::test]
async fn different_binary_private_row_is_not_adopted_as_equivalent_shared_state() {
    let author_dir = test_store_dir();
    let tables = vec![SyncedTable::new("documents", RowIdentity::SharedKey).gated_by("shared")];
    let migrations = vec![Migration::sql(
        1,
        "binary-private-row",
        "CREATE TABLE documents (
             id TEXT PRIMARY KEY,
             payload BLOB NOT NULL,
             shared INTEGER NOT NULL,
             _updated_at TEXT NOT NULL,
             created_at TEXT NOT NULL
         ) STRICT;",
    )];
    let author_db = open_test_db_schema(author_dir.clone(), tables.clone(), migrations);
    let signer = user_keypair_from_seed([44; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "binary-private-conflict",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db_schema(
        peer_dir.clone(),
        tables,
        vec![Migration::sql(
            1,
            "binary-private-row",
            "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 payload BLOB NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    );
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-18T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO documents VALUES \
                 ('same-row', X'80', 0, '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private binary row");
    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO documents VALUES \
                 ('same-row', X'81', 1, '0000000002000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture different shared binary row");
    drain(&peer).await;

    let (_, pull) = author.pull_store().await.expect("pull binary conflict");

    assert!(pull.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::PrivateSharedConflict { table, row_id, .. }
            if table == "documents" && row_id == "same-row"
    )));
    assert_eq!(
        author
            .query_test_text("SELECT HEX(payload) FROM documents WHERE id = 'same-row'")
            .await,
        "80",
    );
}

#[tokio::test]
async fn one_write_can_edit_and_share_its_own_private_row() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([39; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-edit-and-share",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('private-share', 'Private title', NULL, 0, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private row");
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET title = 'Shared title', shared = 1, \
                 _updated_at = '0000000002000-0000-author' \
                 WHERE id = 'private-share';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture edit and share");

    drain(&device).await;
    assert_eq!(
        device
            .query_test_text(
                "SELECT title || ':shared=' || shared FROM notes WHERE id = 'private-share'",
            )
            .await,
        "Shared title:shared=1",
    );
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay published edit and share before compaction"),
        1,
    );

    advance_current_baseline(&store, &database, &device).await;
    assert_eq!(
        device
            .query_test_text(
                "SELECT title || ':shared=' || shared FROM notes WHERE id = 'private-share'",
            )
            .await,
        "Shared title:shared=1",
    );
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay advanced baseline"),
        1,
    );
}

#[tokio::test]
async fn local_delete_cannot_remove_a_concurrently_shared_row() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([40; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "concurrent-private-delete",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");
    let author_before = author
        .materialized_frontier()
        .await
        .expect("author frontier");
    let peer_before = peer.materialized_frontier().await.expect("peer frontier");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('author-probe', 'Author probe', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture author probe");
    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('peer-probe', 'Peer probe', NULL, 1, \
                  '0000000001000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture peer probe");
    drain(&author).await;
    drain(&peer).await;

    let advanced_reference = |before: &std::collections::BTreeMap<
        String,
        coven_protocol::store_commit::StoreBatchCommitRef,
    >,
                              after: &std::collections::BTreeMap<
        String,
        coven_protocol::store_commit::StoreBatchCommitRef,
    >| {
        after
            .iter()
            .find(|(stream, reference)| {
                before
                    .get(*stream)
                    .is_none_or(|prior| prior.coord.sequence() < reference.coord.sequence())
            })
            .map(|(_, reference)| reference.clone())
            .expect("one local stream advanced")
    };
    let author_commit = advanced_reference(
        &author_before,
        &author
            .materialized_frontier()
            .await
            .expect("author probe frontier"),
    );
    let peer_commit = advanced_reference(
        &peer_before,
        &peer
            .materialized_frontier()
            .await
            .expect("peer probe frontier"),
    );
    author.pull_store().await.expect("author pulls common base");
    peer.pull_store().await.expect("peer pulls common base");

    let (private_device, private_database, private_probe, sharing_device, sharing_database) =
        if author_commit.coord.stream_id > peer_commit.coord.stream_id {
            (
                &author,
                store_database(&author_db),
                "author-probe",
                &peer,
                store_database(&peer_db),
            )
        } else {
            (
                &peer,
                store_database(&peer_db),
                "peer-probe",
                &author,
                store_database(&author_db),
            )
        };

    private_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('same-row', 'Same title', 'Same body', 0, \
                  '0000000002000-0000-private', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private row");
    let private_probe = private_probe.to_string();
    private_database
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute("DELETE FROM notes WHERE id = 'same-row'", [])?;
            tx.execute(
                "UPDATE notes SET title = 'Private device update', \
                 _updated_at = '0000000003000-0000-private' WHERE id = ?1",
                [&private_probe],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private delete with shared update");
    sharing_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('same-row', 'Same title', 'Same body', 1, \
                  '0000000003000-0000-sharing', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture concurrent shared row");
    drain(private_device).await;
    drain(sharing_device).await;
    let frontier_before_pull = private_device
        .materialized_frontier()
        .await
        .expect("frontier before conflicting pull");

    let (_, pull) = private_device
        .pull_store()
        .await
        .expect("pull concurrent shared row");
    assert!(pull.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::PrivateSharedConflict { table, row_id, .. }
            if table == "notes" && row_id == "same-row"
    )));
    assert_eq!(
        private_device
            .materialized_frontier()
            .await
            .expect("frontier after conflicting pull"),
        frontier_before_pull,
        "the conflicting package is not installed",
    );
    assert_eq!(
        private_device
            .query_test_text("SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'same-row'")
            .await,
        "0",
        "the conflicting package leaves the pre-pull projection unchanged",
    );
}

#[tokio::test]
async fn a_new_commit_is_not_installed_when_prior_accepted_history_becomes_held() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([41; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "atomic-canonical-replay",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('parent', 'Parent', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture parent");
    drain(&author).await;

    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-17T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('author-probe-atomic', 'Author', NULL, 1, \
                  '0000000002000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture author probe");
    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('peer-probe-atomic', 'Peer', NULL, 1, \
                  '0000000002000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture peer probe");
    drain(&author).await;
    drain(&peer).await;
    let author_stream = author
        .latest_local_store_position()
        .await
        .expect("read author position")
        .expect("author published a probe")
        .coord
        .stream_id;
    let peer_stream = peer
        .latest_local_store_position()
        .await
        .expect("read peer position")
        .expect("peer published a probe")
        .coord
        .stream_id;
    author.pull_store().await.expect("author pulls common base");
    peer.pull_store().await.expect("peer pulls common base");

    let (deleter, deleter_database, child_device, child_database) = if author_stream < peer_stream {
        (
            &author,
            store_database(&author_db),
            &peer,
            store_database(&peer_db),
        )
    } else {
        (
            &peer,
            store_database(&peer_db),
            &author,
            store_database(&author_db),
        )
    };
    deleter_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute("DELETE FROM notes WHERE id = 'parent'", [])?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture concurrent parent deletion");
    child_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO note_tags VALUES \
                 ('child', 'parent', 'tag', \
                  '0000000003000-0000-child', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture concurrent child");
    drain(child_device).await;
    let child_commit = child_device
        .latest_local_store_position()
        .await
        .expect("read child position")
        .expect("child write was published");
    drain(deleter).await;

    assert!(child_device
        .retained_merge_replay_inputs_for_test()
        .await
        .expect("read retained child input")
        .iter()
        .any(|input| input.commit_ref() == &child_commit));
    let frontier_before = child_device
        .materialized_frontier()
        .await
        .expect("read frontier before conflicting pull");
    let (_, pull) = child_device
        .pull_store()
        .await
        .expect("pull earlier canonical deletion");

    assert!(pull
        .held_positions
        .iter()
        .any(|held| held.reason == HeldStorePositionReason::ForeignKeyDependency));
    assert!(
        child_device
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'parent'")
            .await
    );
    assert!(
        child_device
            .test_row_exists("SELECT 1 FROM note_tags WHERE id = 'child'")
            .await
    );
    assert_eq!(
        child_device
            .materialized_frontier()
            .await
            .expect("read frontier after held pull"),
        frontier_before,
        "a held canonical replay cannot install a partial frontier",
    );
}

#[tokio::test]
async fn successive_baselines_skip_payload_free_publication_receipts() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([34; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "successive-baselines",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('release', 'First title', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture first write");
    drain(&device).await;
    advance_current_baseline(&store, &database, &device).await;

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET title = 'Second title', \
                 _updated_at = '0000000002000-0000-author' WHERE id = 'release';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture second write");
    drain(&device).await;
    advance_current_baseline(&store, &database, &device).await;

    assert_eq!(
        device
            .query_test_text("SELECT title FROM notes WHERE id = 'release'")
            .await,
        "Second title",
    );
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay second baseline"),
        1,
    );
}

#[tokio::test]
async fn same_cut_baseline_consumes_a_new_private_write() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([35; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "same-cut-private-write",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = database
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture snapshot image");
    let cut = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("valid materialized frontier");
    device
        .publish_snapshot(image, cut.clone())
        .await
        .expect("publish snapshot");
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('private', 'Private', NULL, 0, \
                  '0000000002000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private write");
    let advanced = device
        .advance_baseline_by_acknowledging(cut)
        .await
        .expect("acknowledge snapshot")
        .expect("private journal input must advance the baseline");
    assert_eq!(advanced.folded_writes, 1);
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay same-cut baseline"),
        1,
    );
}

#[tokio::test]
async fn same_cut_baseline_retains_a_private_write_that_observed_later_history() {
    let store_dir = test_store_dir();
    let db = open_test_db(store_dir.clone());
    let signer = user_keypair_from_seed([36; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "same-cut-post-ack-private-write",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = database
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture snapshot image");
    let cut = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("valid materialized frontier");
    device
        .publish_snapshot(image, cut.clone())
        .await
        .expect("publish snapshot");
    device
        .publish_acknowledgement_without_advancing(cut)
        .await
        .expect("publish acknowledgement after the snapshot cut");
    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('post-ack-private', 'Private', NULL, 0, \
                  '0000000003000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private write after acknowledgement");
    let journal_before = database
        .store_write_journal_counts_for_test()
        .await
        .expect("read retained private write");

    let outcome = device
        .stand_on_acknowledged_snapshot()
        .await
        .expect("evaluate same-cut retirement");

    assert!(matches!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::BaselineAtCoverage { .. }
        )
    ));
    assert_eq!(
        database
            .store_write_journal_counts_for_test()
            .await
            .expect("read retained private write after decline"),
        journal_before,
        "a write that observed a post-cut commit cannot be folded into the older cut",
    );
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay retained private write"),
        1,
    );
}

#[tokio::test]
async fn publication_releases_shared_blob_lease_and_keeps_private_blob_lease() {
    let store_dir = test_store_dir();
    let db = open_test_db_with_blob(store_dir.clone(), photo_decl());
    let signer = user_keypair_from_seed([43; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "mixed-publication-blobs",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir, &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);
    let private_bytes = b"private image bytes";
    let shared_bytes = b"shared image bytes";
    let staging = device.host_write_blob_staging();
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", "private-photo", private_bytes.to_vec());
    batch.put_blob("photos", "shared-photo", shared_bytes.to_vec());
    let receipt = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes VALUES \
                     ('private-note', 'Private', NULL, 0, \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO notes VALUES \
                     ('shared-note', 'Shared', NULL, 1, \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('private-photo', 'private-note', 'image', {}, '{}', \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('shared-photo', 'shared-note', 'image', {}, '{}', \
                      '0000000001000-0000-author', '2026-01-01');",
                    private_bytes.len(),
                    coven_protocol::blob::content_hash(private_bytes),
                    shared_bytes.len(),
                    coven_protocol::blob::content_hash(shared_bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(staging)),
        )
        .await
        .expect("capture mixed private and shared blob write");
    assert_eq!(
        database
            .write_blob_lease_count_for_test(&receipt.write_id)
            .await
            .expect("count captured blob leases"),
        2,
    );

    drain(&device).await;

    assert_eq!(
        database
            .write_blob_lease_count_for_test(&receipt.write_id)
            .await
            .expect("count retained private blob lease"),
        1,
        "publication keeps only the lease needed by retained Local replay",
    );
}

#[tokio::test]
async fn replay_baseline_owns_private_blob_bytes_until_a_later_cut_removes_them() {
    let store_dir = test_store_dir();
    let db = open_test_db_with_blob(store_dir.clone(), photo_decl());
    let signer = user_keypair_from_seed([42; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-baseline-blob",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, store_dir.clone(), &signer)
        .await
        .expect("bind device");
    let database = store_database(&db);
    let bytes = b"private cover bytes";
    let blob_id = "private-cover";
    let mut create_batch = WriteBatch::new();
    create_batch.put_blob("photos", blob_id, bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(create_batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes VALUES \
                     ('private-release', 'Private', NULL, 0, \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('{blob_id}', 'private-release', 'cover', {}, '{}', \
                      '0000000001000-0000-author', '2026-01-01');",
                    bytes.len(),
                    coven_protocol::blob::content_hash(bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture private blob row");
    let blob = database
        .row_blob_ref("note_photos", blob_id)
        .await
        .expect("read private blob reference");

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = database
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture pre-delete snapshot image");
    let cut = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("valid materialized frontier");
    device
        .publish_snapshot(image, cut.clone())
        .await
        .expect("publish pre-delete snapshot");
    device
        .publish_acknowledgement_without_advancing(cut.clone())
        .await
        .expect("publish acknowledgement after snapshot cut");

    let mut delete_batch = WriteBatch::new();
    delete_batch.delete_blob(blob.blob().clone());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(delete_batch, |sql| {
                sql.execute("DELETE FROM note_photos WHERE id = 'private-cover'", [])?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture private blob deletion after acknowledgement");

    device
        .stand_on_acknowledged_snapshot()
        .await
        .expect("advance baseline over private blob creation");
    assert!(
        coven_database::LocalBlobCleanup::new(&database)
            .drain()
            .await
            .expect("drain cleanup protected by baseline"),
        "the retained baseline blocks deletion of bytes needed before the suffix",
    );
    let path = store_dir
        .local_blob_path("photos", blob_id)
        .expect("private blob path");
    assert_eq!(std::fs::read(&path).expect("read retained bytes"), bytes);

    let mut overwrite_batch = WriteBatch::new();
    overwrite_batch.put_blob("photos", blob_id, b"replacement".to_vec());
    let overwrite = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(overwrite_batch, |_sql| Ok::<_, DbError>(())),
            None,
            None,
        )
        .await;
    assert!(matches!(
        overwrite,
        Err(coven_database::HostWriteError::BlobOwnedByPendingWrite { .. })
    ));

    advance_current_baseline(&store, &database, &device).await;
    assert!(!coven_database::LocalBlobCleanup::new(&database)
        .drain()
        .await
        .expect("drain cleanup after deletion reaches baseline"));
    assert!(!path.exists());
}

#[tokio::test]
async fn retirement_declines_when_current_replay_crosses_the_snapshot_cut() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([38; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "non-prefix-retirement",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");
    let author_before = author
        .materialized_frontier()
        .await
        .expect("author frontier");
    let peer_before = peer.materialized_frontier().await.expect("peer frontier");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('author-concurrent', 'Author', NULL, 1, \
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture author commit");
    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES \
                 ('peer-concurrent', 'Peer', NULL, 1, \
                  '0000000001000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture peer commit");
    drain(&author).await;
    drain(&peer).await;

    let author_frontier = author.materialized_frontier().await.expect("author commit");
    let peer_frontier = peer.materialized_frontier().await.expect("peer commit");
    let advanced_reference = |before: &std::collections::BTreeMap<
        String,
        coven_protocol::store_commit::StoreBatchCommitRef,
    >,
                              after: &std::collections::BTreeMap<
        String,
        coven_protocol::store_commit::StoreBatchCommitRef,
    >| {
        after
            .iter()
            .find(|(stream, reference)| {
                before
                    .get(*stream)
                    .is_none_or(|prior| prior.coord.sequence() < reference.coord.sequence())
            })
            .map(|(_, reference)| reference.clone())
            .expect("one local stream advanced")
    };
    let author_commit = advanced_reference(&author_before, &author_frontier);
    let peer_commit = advanced_reference(&peer_before, &peer_frontier);
    let (snapshot_device, snapshot_database, other_device, snapshot_frontier) =
        if author_commit.coord.stream_id > peer_commit.coord.stream_id {
            (&author, store_database(&author_db), &peer, author_frontier)
        } else {
            (&peer, store_database(&peer_db), &author, peer_frontier)
        };
    let snapshot_cut = CommitFrontier::from_refs(snapshot_frontier)
        .expect("snapshot frontier has exact stream references");
    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = snapshot_database
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture one side of concurrent history");
    snapshot_device
        .publish_snapshot(image, snapshot_cut.clone())
        .await
        .expect("publish non-prefix snapshot");

    snapshot_device
        .pull_store()
        .await
        .expect("pull lower stream");
    other_device.pull_store().await.expect("pull higher stream");
    let snapshot_current = CommitFrontier::from_refs(
        snapshot_device
            .materialized_frontier()
            .await
            .expect("snapshot device current frontier"),
    )
    .expect("snapshot device current cut");
    let other_current = CommitFrontier::from_refs(
        other_device
            .materialized_frontier()
            .await
            .expect("other device current frontier"),
    )
    .expect("other device current cut");
    snapshot_device
        .publish_acknowledgement_without_advancing(snapshot_current)
        .await
        .expect("snapshot device crosses the snapshot cut");
    other_device
        .publish_acknowledgement_without_advancing(other_current)
        .await
        .expect("other device crosses the snapshot cut");
    snapshot_device
        .pull_store()
        .await
        .expect("materialize both crossing acknowledgements");

    let outcome = snapshot_device
        .stand_on_acknowledged_snapshot()
        .await
        .expect("evaluate non-prefix snapshot");

    assert!(matches!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::NonPrefixCut { .. }
        )
    ));
}

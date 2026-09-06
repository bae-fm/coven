use super::local_replay_causality_tests::{advance_current_baseline, drain};
use crate::sync::store::pull::HeldStorePositionReason;
use crate::sync::test_helpers::*;
use coven_database::{DbError, Migration};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};

fn independent_parent_child_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey).gated_by("shared"),
            SyncedTable::new("children", RowIdentity::SharedKey).gated_by("shared"),
        ],
        vec![Migration::sql(
            1,
            "independent-parent-child",
            "CREATE TABLE parents (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE children (
                 id TEXT PRIMARY KEY,
                 parent_id TEXT NOT NULL REFERENCES parents(id),
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

fn inherited_parent_child_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey).gated_by("shared"),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ],
        vec![Migration::sql(
            1,
            "inherited-parent-child",
            "CREATE TABLE parents (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE children (
                 id TEXT PRIMARY KEY,
                 parent_id TEXT NOT NULL REFERENCES parents(id),
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

fn gated_parent_scoped_child_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey).gated_by("shared"),
            SyncedTable::new("notes", RowIdentity::SharedKey).scoped_by("audience"),
        ],
        vec![Migration::sql(
            1,
            "gated-parent-scoped-child",
            "CREATE TABLE parents (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 parent_id TEXT NOT NULL REFERENCES parents(id),
                 title TEXT NOT NULL,
                 audience TEXT,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn local_parent_cannot_supply_a_concurrent_shared_child() {
    let author_dir = test_store_dir();
    let (tables, migrations) = inherited_parent_child_schema();
    let author_db = open_test_db_schema(author_dir.clone(), tables.clone(), migrations);
    let signer = user_keypair_from_seed([45; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "shared-child-private-parent",
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
    let (peer_tables, peer_migrations) = inherited_parent_child_schema();
    let peer_db = open_test_db_schema(peer_dir.clone(), peer_tables, peer_migrations);
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-19T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    store_database(&author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO parents VALUES
                 ('parent', 'Parent', 1, '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture shared parent");
    drain(&author).await;
    peer.pull_store().await.expect("peer pulls parent");

    for (database, id, stamp) in [
        (
            store_database(&author_db),
            "author-probe",
            "0000000002000-0000-author",
        ),
        (
            store_database(&peer_db),
            "peer-probe",
            "0000000002000-0000-peer",
        ),
    ] {
        let id = id.to_string();
        let stamp = stamp.to_string();
        database
            .run_host_store_write_for_test(None, None, move |tx| {
                tx.execute(
                    "INSERT INTO parents VALUES (?1, ?1, 1, ?2, '2026-01-01')",
                    [&id, &stamp],
                )?;
                Ok::<_, DbError>(())
            })
            .await
            .expect("capture stream probe");
    }
    drain(&author).await;
    drain(&peer).await;
    let author_stream = author
        .latest_local_store_position()
        .await
        .expect("read author position")
        .expect("author published probe")
        .coord
        .stream_id;
    let peer_stream = peer
        .latest_local_store_position()
        .await
        .expect("read peer position")
        .expect("peer published probe")
        .coord
        .stream_id;
    author
        .pull_store()
        .await
        .expect("author pulls common probes");
    peer.pull_store().await.expect("peer pulls common probes");

    let (withdrawer, withdrawer_database, child_writer, child_database) =
        if author_stream > peer_stream {
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
    withdrawer_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute(
                "UPDATE parents SET shared = 0,
                 _updated_at = '0000000003000-0000-withdrawer' WHERE id = 'parent'",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture parent withdrawal");
    child_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO children VALUES
                 ('child', 'parent', '0000000003000-0000-child', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture concurrent shared child");
    drain(child_writer).await;
    drain(withdrawer).await;
    let frontier_before = withdrawer
        .materialized_frontier()
        .await
        .expect("read frontier before conflicting pull");

    let (_, pull) = withdrawer
        .pull_store()
        .await
        .expect("pull concurrent child");

    assert!(pull.held_positions.iter().any(|held| matches!(
        held.reason,
        HeldStorePositionReason::ForeignKeyDependency
            | HeldStorePositionReason::PrivateSharedConflict { .. }
    )));
    assert_eq!(
        withdrawer
            .materialized_frontier()
            .await
            .expect("read frontier after held pull"),
        frontier_before,
    );
    assert!(
        !withdrawer
            .test_row_exists("SELECT 1 FROM children WHERE id = 'child'")
            .await
    );
    assert_eq!(
        withdrawer
            .query_test_text("SELECT CAST(shared AS TEXT) FROM parents WHERE id = 'parent'")
            .await,
        "0",
    );
}

#[tokio::test]
async fn withdrawing_a_parent_preserves_an_independently_private_child() {
    let store_dir = test_store_dir();
    let (tables, migrations) = independent_parent_child_schema();
    let db = open_test_db_schema(store_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([49; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-child-withdrawal",
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
                "INSERT INTO parents VALUES
                 ('parent', 'Parent', 1, '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO children VALUES
                 ('child', 'parent', 0, '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture parent and private child");
    drain(&device).await;

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute(
                "UPDATE parents SET shared = 0,
                 _updated_at = '0000000002000-0000-author' WHERE id = 'parent'",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture parent withdrawal");
    let partitions = db
        .store_write_partitions_in_audience_order_for_test()
        .await
        .expect("read withdrawal partitions");
    let store_partition = partitions
        .iter()
        .find(|(audience, _, _)| audience == "store")
        .expect("withdrawal has Store partition");
    let store_rows = coven_database::walk_changeset(&store_partition.2)
        .expect("walk withdrawal Store partition");
    assert!(
        store_rows
            .iter()
            .all(|row| row.table != "children" || row.pk() != Some("child")),
        "a private root must not appear in another root's Store retraction",
    );
    drain(&device).await;

    advance_current_baseline(&store, &database, &device).await;
    assert_eq!(
        device
            .query_test_text("SELECT parent_id FROM children WHERE id = 'child'")
            .await,
        "parent",
    );
}

#[tokio::test]
async fn withdrawing_a_parent_preserves_a_private_scoped_child() {
    let store_dir = test_store_dir();
    let (tables, migrations) = gated_parent_scoped_child_schema();
    let db = open_test_db_schema(store_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([52; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-scoped-child-withdrawal",
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
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            |tx| {
                tx.execute_batch(
                    "INSERT INTO parents VALUES
                 ('parent', 'Parent', 1, '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO notes VALUES
                 ('note', 'parent', 'Private note', 'local',
                  '0000000001000-0000-author', '2026-01-01');",
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture parent and private scoped child");
    drain(&device).await;

    database
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            |tx| {
                tx.execute(
                    "UPDATE parents SET shared = 0,
                 _updated_at = '0000000002000-0000-author' WHERE id = 'parent'",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture parent withdrawal");
    drain(&device).await;

    let image_dir = tempfile::tempdir().expect("snapshot image directory");
    let image = database
        .capture_snapshot_image_for_test(
            store.root().clone(),
            image_dir.path().to_path_buf(),
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
        )
        .await
        .expect("capture scoped snapshot image");
    let cut = coven_protocol::store_commit::CommitFrontier::from_refs(
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
    assert_eq!(
        device
            .query_test_text(
                "SELECT notes.parent_id || ':' || notes.audience || ':' || parents.shared
                 FROM notes JOIN parents ON parents.id = notes.parent_id
                 WHERE notes.id = 'note'",
            )
            .await,
        "parent:local:0",
    );
}

#[tokio::test]
async fn withdrawal_and_private_reparent_replay_as_one_write() {
    let store_dir = test_store_dir();
    let (tables, migrations) = independent_parent_child_schema();
    let db = open_test_db_schema(store_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([50; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-reparent-withdrawal",
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
                "INSERT INTO parents VALUES
                 ('shared-parent', 'Shared parent', 1,
                  '0000000001000-0000-author', '2026-01-01'),
                 ('private-parent', 'Private parent', 0,
                  '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO children VALUES
                 ('child', 'shared-parent', 0,
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture initial rows");
    drain(&device).await;

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE children SET parent_id = 'private-parent',
                    _updated_at = '0000000002000-0000-author' WHERE id = 'child';
                 UPDATE parents SET shared = 0,
                    _updated_at = '0000000002000-0000-author'
                    WHERE id = 'shared-parent';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private reparent and withdrawal");
    drain(&device).await;

    advance_current_baseline(&store, &database, &device).await;
    assert_eq!(
        device
            .query_test_text("SELECT parent_id FROM children WHERE id = 'child'")
            .await,
        "private-parent",
    );
}

fn sideways_ancestor_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("artists", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("albums", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("releases", RowIdentity::SharedKey).gated_by("shared"),
            SyncedTable::new("album_artists", RowIdentity::SharedKey),
        ],
        vec![Migration::sql(
            1,
            "sideways-ancestor",
            "CREATE TABLE artists (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE albums (
                 id TEXT PRIMARY KEY,
                 artist_id TEXT NOT NULL REFERENCES artists(id),
                 title TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE releases (
                 id TEXT PRIMARY KEY,
                 album_id TEXT NOT NULL REFERENCES albums(id),
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE album_artists (
                 id TEXT PRIMARY KEY,
                 album_id TEXT NOT NULL REFERENCES albums(id),
                 artist_id TEXT NOT NULL REFERENCES artists(id),
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn withdrawing_a_graph_retains_sideways_foreign_key_ancestors() {
    let store_dir = test_store_dir();
    let (tables, migrations) = sideways_ancestor_schema();
    let db = open_test_db_schema(store_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([46; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "private-sideways-ancestor",
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
                "INSERT INTO artists VALUES
                 ('main-artist', 'Main', '0000000001000-0000-author', '2026-01-01'),
                 ('featured-artist', 'Featured', '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO albums VALUES
                 ('album', 'main-artist', 'Album', '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO releases VALUES
                 ('release', 'album', 1, '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO album_artists VALUES
                 ('credit', 'album', 'featured-artist',
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture shared graph");
    drain(&device).await;

    database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute(
                "UPDATE releases SET shared = 0,
                 _updated_at = '0000000002000-0000-author' WHERE id = 'release'",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture graph withdrawal");
    drain(&device).await;

    advance_current_baseline(&store, &database, &device).await;
    for table in ["artists", "albums", "releases", "album_artists"] {
        assert!(
            device
                .replay_row_count_for_test(table)
                .await
                .expect("replay private graph")
                > 0,
            "replay omitted {table}",
        );
    }
    assert_eq!(
        device
            .query_test_text("SELECT artist_id FROM album_artists WHERE id = 'credit'",)
            .await,
        "featured-artist",
    );
}

fn scoped_account_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![SyncedTable::new("accounts", RowIdentity::SharedKey).scoped_by("audience")],
        vec![Migration::sql(
            1,
            "scoped-account",
            "CREATE TABLE accounts (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 audience TEXT,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn pending_circle_write_conflicts_with_a_later_circle_deletion() {
    let author_dir = test_store_dir();
    let (tables, migrations) = scoped_account_schema();
    let author_db = open_test_db_schema(author_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([47; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "pending-deleted-circle",
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
    let (peer_tables, peer_migrations) = scoped_account_schema();
    let peer_db = open_test_db_schema(peer_dir.clone(), peer_tables, peer_migrations);
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    let circle_id = author
        .create_circle("0000000001000-0000-author", "Shared circle")
        .await
        .expect("create Circle");
    peer.pull_store().await.expect("peer pulls Circle");
    let encoded_circle = circle_id.to_string();
    store_database(&author_db)
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |tx| {
                tx.execute(
                    "INSERT INTO accounts VALUES
                     ('pending', 'Pending', ?1,
                      '0000000002000-0000-author', '2026-01-01')",
                    [&encoded_circle],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture pending Circle write");
    let frontier_before = author
        .materialized_frontier()
        .await
        .expect("read frontier before Circle deletion");

    peer.delete_circle(circle_id)
        .await
        .expect("delete Circle on peer");
    let (_, pull) = author.pull_store().await.expect("pull Circle deletion");

    assert!(pull.held_positions.iter().any(|held| matches!(
        held.reason,
        HeldStorePositionReason::InvalidLocalCircleContext { circle_id: held }
            if held == circle_id
    )));
    assert_eq!(
        author
            .materialized_frontier()
            .await
            .expect("read frontier after held Circle deletion"),
        frontier_before,
    );
    assert!(
        author
            .test_row_exists("SELECT 1 FROM accounts WHERE id = 'pending'")
            .await
    );
}

#[tokio::test]
async fn filtered_circle_move_cannot_adopt_and_prune_a_private_row() {
    let author_dir = test_store_dir();
    let (tables, migrations) = scoped_account_schema();
    let author_db = open_test_db_schema(author_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([48; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "filtered-circle-private-row",
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
    let (peer_tables, peer_migrations) = scoped_account_schema();
    let peer_db = open_test_db_schema(peer_dir.clone(), peer_tables, peer_migrations);
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    let first_circle = author
        .create_circle("0000000001000-0000-author", "First circle")
        .await
        .expect("create first Circle");
    let second_circle = author
        .create_circle("0000000001001-0000-author", "Second circle")
        .await
        .expect("create second Circle");
    peer.pull_store().await.expect("peer pulls Circles");
    let first_circle_id = first_circle.to_string();
    store_database(&author_db)
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |tx| {
                tx.execute(
                    "INSERT INTO accounts VALUES
                     ('account', 'Same title', ?1,
                      '0000000002000-0000-author', '2026-01-01')",
                    [&first_circle_id],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture initial Circle row");
    drain(&author).await;
    peer.pull_store().await.expect("peer pulls Circle row");

    for (database, id, stamp) in [
        (
            store_database(&author_db),
            "author-probe",
            "0000000003000-0000-author",
        ),
        (
            store_database(&peer_db),
            "peer-probe",
            "0000000003000-0000-peer",
        ),
    ] {
        let id = id.to_string();
        let stamp = stamp.to_string();
        database
            .run_host_store_write_for_test(
                Some(coven_keys::encryption::EncryptionService::from_key(
                    [42; 32],
                )),
                None,
                move |tx| {
                    tx.execute(
                        "INSERT INTO accounts VALUES (?1, ?1, NULL, ?2, '2026-01-01')",
                        [&id, &stamp],
                    )?;
                    Ok::<_, DbError>(())
                },
            )
            .await
            .expect("capture stream probe");
    }
    drain(&author).await;
    drain(&peer).await;
    let author_stream = author
        .latest_local_store_position()
        .await
        .expect("read author position")
        .expect("author published probe")
        .coord
        .stream_id;
    let peer_stream = peer
        .latest_local_store_position()
        .await
        .expect("read peer position")
        .expect("peer published probe")
        .coord
        .stream_id;
    author
        .pull_store()
        .await
        .expect("author pulls common probes");
    peer.pull_store().await.expect("peer pulls common probes");

    let (withdrawer, withdrawer_database, mover, mover_database) = if author_stream < peer_stream {
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
    withdrawer_database
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            |tx| {
                tx.execute(
                    "UPDATE accounts SET audience = 'local',
                     _updated_at = '0000000004000-0000-withdrawer' WHERE id = 'account'",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture Circle withdrawal");
    let second_circle_id = second_circle.to_string();
    mover_database
        .run_host_store_write_for_test(
            Some(coven_keys::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            None,
            move |tx| {
                tx.execute(
                    "UPDATE accounts SET audience = ?1,
                     _updated_at = '0000000004000-0000-mover' WHERE id = 'account'",
                    [&second_circle_id],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture concurrent Circle move");
    drain(withdrawer).await;
    drain(mover).await;

    let (_, pull) = withdrawer
        .pull_store()
        .await
        .expect("pull filtered Circle move");

    assert!(pull.held_positions.is_empty());
    assert_eq!(
        withdrawer
            .query_test_text("SELECT title || ':' || audience FROM accounts WHERE id = 'account'",)
            .await,
        "Same title:local",
    );
}

#[tokio::test]
async fn later_local_edit_cannot_modify_a_row_adopted_by_accepted_history() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([51; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "adopted-row-local-edit",
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
            "2026-07-22T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");
    let author_database = store_database(&author_db);

    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('same-row', 'Same title', 'Original body', 0,
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture private row");
    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('pending-row', 'Pending title', NULL, 1,
                  '0000000002000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture pending shared write");
    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "UPDATE notes SET body = 'Private body',
                 _updated_at = '0000000003000-0000-author' WHERE id = 'same-row';",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture later private edit");

    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('same-row', 'Same title', 'Original body', 1,
                  '0000000002000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture equivalent shared row");
    drain(&peer).await;

    let (_, pull) = author
        .pull_store()
        .await
        .expect("pull equivalent shared row");

    assert!(pull.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::PrivateSharedConflict { table, row_id, .. }
            if table == "notes" && row_id == "same-row"
    )));
    assert_eq!(
        author
            .query_test_text(
                "SELECT body || ':shared=' || shared FROM notes WHERE id = 'same-row'",
            )
            .await,
        "Private body:shared=0",
    );
}

#[tokio::test]
async fn delayed_local_insert_cannot_replace_a_concurrent_shared_insert() {
    let author_dir = test_store_dir();
    let author_db = open_test_db(author_dir.clone());
    let signer = user_keypair_from_seed([52; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "concurrent-local-insert",
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
            "2026-07-23T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");
    let author_database = store_database(&author_db);

    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('pending-row', 'Pending title', NULL, 1,
                  '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture pending shared write");
    author_database
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('same-row', 'Private title', NULL, 0,
                  '0000000003000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture delayed private insert");

    store_database(&peer_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO notes VALUES
                 ('same-row', 'Shared title', NULL, 1,
                  '0000000002000-0000-peer', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture concurrent shared insert");
    drain(&peer).await;

    let (_, pull) = author
        .pull_store()
        .await
        .expect("pull concurrent shared insert");

    assert!(pull.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::PrivateSharedConflict { table, row_id, .. }
            if table == "notes" && row_id == "same-row"
    )));
    assert_eq!(
        author
            .query_test_text(
                "SELECT title || ':shared=' || shared FROM notes WHERE id = 'same-row'"
            )
            .await,
        "Private title:shared=0",
    );
}

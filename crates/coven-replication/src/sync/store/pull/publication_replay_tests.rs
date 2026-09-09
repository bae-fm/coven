use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn replay_preserves_a_mixed_write_when_its_shared_update_loses_to_deletion() {
    assert_mixed_write_replay(true).await;
}

#[tokio::test]
async fn replay_preserves_a_mixed_write_when_its_shared_update_merges_columns() {
    assert_mixed_write_replay(false).await;
}

async fn assert_mixed_write_replay(deleted: bool) {
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &owner_db,
        owner_dir.clone(),
        "mixed-write-replay-outcome",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &owner_db,
            owner_dir.clone(),
            &peer_db,
            peer_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&owner_db, owner_dir, &signer)
        .await
        .expect("bind owner");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("observe the peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) VALUES \
             ('shared-row', 'Initial title', 'Initial body', 1, '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
    let mut owner_writer = owner
        .authorize_writer()
        .await
        .expect("authorize initial row");
    assert!(owner_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare initial row"));
    assert_eq!(
        owner_writer
            .drain_store_writes()
            .await
            .expect("publish initial row"),
        1
    );
    drop(owner_writer);
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer observes the shared row");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer_db
        .execute_test_host_write(
            "UPDATE notes SET title = 'Peer title', _updated_at = '0000000002000-0000-peer' \
             WHERE id = 'shared-row'; \
             INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('private-row', 'Private effect', 0, '0000000002000-0000-peer', '2026-01-01')",
        )
        .await;
    let mut peer_writer = peer
        .authorize_writer()
        .await
        .expect("authorize mixed write");
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed write"));
    let peer_database = StoreDatabase::new(&peer_db);
    let pending = peer_database
        .oldest_prepared_store_write()
        .await
        .expect("read mixed write")
        .expect("mixed write owns a candidate");
    owner_db
        .execute_test_host_write(if deleted {
            "DELETE FROM notes WHERE id = 'shared-row'"
        } else {
            "UPDATE notes SET body = 'Owner body', _updated_at = '0000000003000-0000-owner' \
             WHERE id = 'shared-row'"
        })
        .await;
    let mut owner_writer = owner
        .authorize_writer()
        .await
        .expect("authorize competing edit");
    assert!(owner_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare competing edit"));
    assert_eq!(
        owner_writer
            .drain_store_writes()
            .await
            .expect("publish competing edit"),
        1
    );
    drop(owner_writer);

    // This pull must replay the unaccepted mixed write on top of the winning
    // shared boundary before its publication envelope can be replaced.
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("replay the captured mixed write");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let retained = peer_database
        .oldest_prepared_store_write()
        .await
        .expect("read preserved candidate")
        .expect("pull does not complete publication");
    assert_eq!(retained.commit.bytes, pending.commit.bytes);
    assert_eq!(
        retained.commit.value.write_id,
        pending.commit.value.write_id
    );
    assert_shared_row(&peer_db, deleted).await;
    assert_eq!(
        peer_db
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "Private effect"
    );
    assert_eq!(
        peer_writer
            .drain_store_writes()
            .await
            .expect("publish the same logical write"),
        1
    );
    assert_eq!(
        peer_writer.drain_store_writes().await.expect("drain again"),
        0
    );
    drop(peer_writer);
    assert!(peer_database
        .oldest_prepared_store_write()
        .await
        .expect("read completed write journal")
        .is_none());
    assert!(peer_database
        .active_store_publication()
        .await
        .expect("read released publication reservation")
        .is_none());
    let receipt = peer_database
        .installed_store_commit_evidence(pending.commit.value.clone())
        .await
        .expect("read accepted original candidate")
        .expect("mixed write is installed");
    assert_eq!(receipt.commit_ref(), pending.commit.value.reference());
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("owner observes the peer publication");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    for database in [&owner_db, &peer_db] {
        assert_shared_row(database, deleted).await;
    }
    assert_eq!(
        peer_db
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "Private effect"
    );
    assert_eq!(
        owner_db
            .query_test_text("SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'private-row'")
            .await,
        "0"
    );
}

async fn assert_shared_row(database: &coven_database::Database, deleted: bool) {
    if deleted {
        assert_eq!(
            database
                .query_test_text("SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'shared-row'")
                .await,
            "0"
        );
    } else {
        assert_eq!(
            database.query_test_text("SELECT title || ':' || body || ':' || _updated_at FROM notes WHERE id = 'shared-row'").await,
            "Peer title:Owner body:0000000003000-0000-owner"
        );
    }
}

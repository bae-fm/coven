use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::semantic_prefix_from_exact_object;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn snapshot_capture_excludes_unpublished_edits_without_discarding_them() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-unpublished-image",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind snapshot author");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('shared-note', 'Accepted title', 1, '0000000002000-0000-owner', '2026-09-08')",
        )
        .await;
    let mut writer = owner.authorize_writer().await.expect("authorize writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare accepted note"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish accepted note"),
        1
    );
    source
        .execute_test_host_write(
            "UPDATE notes SET title = 'Unpublished title', _updated_at = '0000000003000-0000-owner'
         WHERE id = 'shared-note';
         INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('private-note', 'Keep private', 0, '0000000003000-0000-owner', '2026-09-08')",
        )
        .await;
    let database = StoreDatabase::new(&source);
    let journal_before = database.store_write_journal_for_test().await.unwrap();
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let mut snapshots = writer.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&encryption))
        .await
        .expect("capture accepted history while preserving unpublished work");
    let published = snapshots
        .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
        .await
        .expect("publish the accepted shared prefix");
    let bytes = storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root().store_root_hash,
                ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &published.image.object,
            &semantic_prefix_from_exact_object(&published.image.object, ".db")
                .expect("derive the exact image prefix"),
        )
        .await
        .expect("read accepted snapshot image");
    let image =
        coven_database::DatabaseImageTest::from_bytes(&bytes).expect("open published image");
    let rows = image
        .query("SELECT id, title FROM notes ORDER BY id", [], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("read rows");
    assert_eq!(
        rows,
        [("shared-note".to_string(), "Accepted title".to_string())]
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'shared-note'")
            .await,
        "Unpublished title"
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'private-note'")
            .await,
        "Keep private"
    );
    assert_eq!(
        database.store_write_journal_for_test().await.unwrap(),
        journal_before,
        "snapshot publication preserves the unresolved write journal"
    );
}

#[tokio::test]
async fn a_snapshot_retry_includes_an_intervening_accepted_commit() {
    assert_snapshot_retry_preserves_the_accepted_prefix(false, false).await;
}

#[tokio::test]
async fn a_snapshot_retry_continues_from_an_intervening_accepted_snapshot() {
    assert_snapshot_retry_preserves_the_accepted_prefix(true, false).await;
}

#[tokio::test]
async fn snapshot_candidate_cleanup_resumes_after_reopen_and_preserves_replacement_artifacts() {
    assert_snapshot_retry_preserves_the_accepted_prefix(false, true).await;
}

#[tokio::test]
async fn compacted_snapshot_supersession_resumes_cleanup_after_reopen() {
    assert_snapshot_retry_preserves_the_accepted_prefix(true, true).await;
}

async fn assert_snapshot_retry_preserves_the_accepted_prefix(
    peer_publishes_snapshot: bool,
    interrupt_cleanup: bool,
) {
    let directory = tempfile::tempdir().expect("snapshot publisher directory");
    let path = directory.path().join("publisher.db");
    let source_dir = test_store_dir();
    let source = open_snapshot_retry_database(&path, source_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-retry-accepted-prefix",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate another owner device");
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind snapshot author");
    let (_, pulled) = owner.pull_store().await.expect("observe joined owner");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('private-note', 'Keep private', 0, '0000000002000-0000-owner', '2026-09-08')",
        )
        .await;
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    {
        let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
        let mut snapshots = writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&encryption))
            .await
            .expect("capture the accepted shared image");
        home.fail_exact_create_before_call(if interrupt_cleanup { 3 } else { 1 });
        snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
            .await
            .expect_err("interrupt the prepared snapshot before publication");
    }
    let database = StoreDatabase::new(&source);
    let pending = database
        .outbound_snapshot_publication()
        .await
        .expect("read snapshot reservation")
        .expect("snapshot remains pending");
    peer_database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('accepted-note', 'Include accepted edit', 1, '0000000003000-0000-peer', '2026-09-08')",
        )
        .await;
    let mut peer_writer = peer.authorize_writer().await.expect("authorize peer edit");
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare peer edit"));
    assert_eq!(
        peer_writer
            .drain_store_writes()
            .await
            .expect("publish peer edit"),
        1
    );
    let peer_commit = peer
        .latest_local_store_position()
        .await
        .expect("read peer receipt")
        .expect("peer edit is accepted");
    if peer_publishes_snapshot {
        let mut snapshots = peer_writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&encryption))
            .await
            .expect("capture peer's accepted prefix");
        snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:02Z".to_string())
            .await
            .expect("publish intervening peer snapshot");
    }
    drop(peer_writer);
    if interrupt_cleanup {
        home.fail_nth_exact_delete_of(&[pending.meta.value.image.object.slot()], 1);
        owner
            .resume_snapshot_publication()
            .await
            .expect_err("interrupt exact old image cleanup");
        let active = database
            .active_store_publication()
            .await
            .expect("read interrupted cleanup")
            .expect("cleanup retains the publication reservation");
        assert!(active
            .retired_snapshot_objects()
            .contains(&pending.meta.value.image.object));
        let replacement = database
            .outbound_snapshot_publication()
            .await
            .expect("read replacement")
            .expect("snapshot journal survives cleanup failure");
        if peer_publishes_snapshot {
            assert_eq!(replacement.reference, pending.reference);
            assert!(
                active.superseding_snapshot().is_some(),
                "compaction records checkpoint supersession without claiming the original won or lost"
            );
        } else {
            assert_ne!(replacement.reference, pending.reference);
            assert_ne!(
                replacement.reference.object.slot(),
                pending.reference.object.slot()
            );
            assert!(!active
                .retired_snapshot_objects()
                .contains(&replacement.meta.value.membership_rollup.object));
            assert!(!active
                .retired_snapshot_objects()
                .contains(&replacement.meta.value.image.object));
        }
    }
    let reopened = open_snapshot_retry_database(&path, source_dir.clone());
    let reopened_owner = store
        .bind_device_in(&reopened, source_dir, &signer)
        .await
        .expect("reopen snapshot publisher");
    let published = reopened_owner
        .resume_snapshot_publication()
        .await
        .expect("retry snapshot after an accepted competing publication")
        .expect("resolve the pending snapshot request");
    assert_ne!(published.snapshot_hash(), pending.reference.snapshot_hash);
    assert_eq!(
        published
            .coverage
            .commits()
            .get(&peer_commit.coord.stream_id),
        Some(&peer_commit),
        "the resulting snapshot must cover the accepted peer edit",
    );
    let image = storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root().store_root_hash,
                ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &published.image.object,
            &semantic_prefix_from_exact_object(&published.image.object, ".db")
                .expect("derive the exact image prefix"),
        )
        .await
        .expect("read the exact published image");
    let connection =
        coven_database::DatabaseImageTest::from_bytes(&image).expect("open published image");
    let rows = connection
        .query("SELECT id, title FROM notes ORDER BY id", [], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("read shared rows");
    assert_eq!(
        rows,
        vec![(
            "accepted-note".to_string(),
            "Include accepted edit".to_string()
        )]
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'private-note'")
            .await,
        "Keep private"
    );
    if interrupt_cleanup {
        assert!(storage
            .observe_exact_slot(pending.meta.value.image.object.slot())
            .await
            .expect("observe retired candidate image")
            .is_none());
    }
    assert!(database
        .outbound_snapshot_publication()
        .await
        .expect("read completed snapshot journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("read released publication reservation")
        .is_none());
}

fn open_snapshot_retry_database(
    path: &std::path::Path,
    store_dir: coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    coven_database::Database::open_synthetic_for_test(
        path,
        store_dir,
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "snapshot-retry-owner".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open durable snapshot publisher")
}

#[tokio::test]
async fn a_lost_snapshot_response_settles_after_a_peer_advances_the_head() {
    assert_lost_snapshot_response_settles(false).await;
}

#[tokio::test]
async fn a_lost_snapshot_response_is_superseded_after_its_accepted_history_is_compacted() {
    assert_lost_snapshot_response_settles(true).await;
}

async fn assert_lost_snapshot_response_settles(peer_compacts: bool) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-lost-response-after-successor",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate peer owner");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    let (_, pulled) = owner.pull_store().await.expect("observe peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let database = StoreDatabase::new(&source);
    let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
    let mut snapshots = writer.snapshots();
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let cut = snapshots
        .capture_snapshot_cut(Some(&encryption))
        .await
        .expect("capture snapshot");
    home.lose_next_conditional_replace_response();
    let (accepted, release) = home.pause_next_conditional_replace();
    let publication = snapshots.push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string());
    tokio::pin!(publication);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            _ = accepted.notified() => {},
            result = &mut publication => panic!("snapshot returned before provider acceptance: {result:?}"),
        }
    }).await.expect("pause after snapshot acceptance");
    let pending = database
        .outbound_snapshot_publication()
        .await
        .expect("read pending snapshot")
        .expect("accepted snapshot awaits local completion");
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer observes accepted snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer_database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('after-snapshot', 'Peer successor', 1, '0000000003000-0000-peer', '2026-09-08')",
        )
        .await;
    let mut peer_writer = peer.authorize_writer().await.expect("authorize successor");
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare successor"));
    assert_eq!(
        peer_writer
            .drain_store_writes()
            .await
            .expect("publish successor"),
        1
    );
    let superseding = if peer_compacts {
        let mut snapshots = peer_writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&encryption))
            .await
            .expect("capture the accepted original snapshot and successor");
        Some(
            snapshots
                .push_snapshot_cut(cut, "2026-09-08T00:00:02Z".to_string())
                .await
                .expect("compact the original accepted snapshot"),
        )
    } else {
        None
    };
    drop(peer_writer);
    let peer_boundary = StoreDatabase::new(&peer_database)
        .store_current_publication()
        .await
        .expect("read successor boundary");
    release.notify_one();
    let published = tokio::time::timeout(std::time::Duration::from_secs(10), &mut publication)
        .await
        .expect("settle snapshot response")
        .expect("complete the accepted snapshot");
    match superseding {
        Some(snapshot) => {
            assert_eq!(
                published, snapshot,
                "return the checkpoint that fulfills the request without claiming the original exact outcome"
            );
            assert_ne!(published.snapshot_hash(), pending.reference.snapshot_hash);
        }
        None => assert_eq!(published.snapshot_hash(), pending.reference.snapshot_hash),
    }
    assert!(database
        .outbound_snapshot_publication()
        .await
        .expect("read snapshot journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("read active reservation")
        .is_none());
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read settled boundary"),
        peer_boundary
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'after-snapshot'")
            .await,
        "Peer successor"
    );
}

#[tokio::test]
async fn a_snapshot_cannot_publish_a_cut_older_than_its_accepted_predecessor() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-stale-accepted-cut",
        signer.clone(),
        home,
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate peer owner");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind snapshot author");
    let (_, pulled) = owner.pull_store().await.expect("observe peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let cut = {
        let mut writer = owner.authorize_writer().await.expect("authorize capture");
        writer
            .snapshots()
            .capture_snapshot_cut(Some(&encryption))
            .await
            .expect("capture the accepted prefix before the peer edit")
    };
    peer_database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('after-capture', 'Preserve the accepted edit', 1, '0000000003000-0000-peer', '2026-09-08')",
        )
        .await;
    {
        let mut writer = peer.authorize_writer().await.expect("authorize peer edit");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare peer edit"));
        assert_eq!(
            writer.drain_store_writes().await.expect("accept peer edit"),
            1
        );
    }
    let (_, pulled) = owner.pull_store().await.expect("install the peer edit");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'after-capture'")
            .await,
        "Preserve the accepted edit"
    );
    let database = StoreDatabase::new(&source);
    let accepted_before = database
        .store_current_publication()
        .await
        .expect("read the accepted predecessor");
    {
        let mut writer = owner
            .authorize_writer()
            .await
            .expect("authorize publication");
        let error = writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
            .await
            .expect_err("a snapshot must include every commit accepted before its publication");
        assert!(
            error.to_string().contains(
                "Store snapshot coverage differs from its accepted publication predecessor"
            ),
            "{error}"
        );
    }
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read the unchanged accepted boundary"),
        accepted_before
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'after-capture'")
            .await,
        "Preserve the accepted edit"
    );
    let (_, pulled) = peer.pull_store().await.expect("read the provider boundary");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        StoreDatabase::new(&peer_database)
            .store_current_publication()
            .await
            .expect("read peer's accepted boundary"),
        accepted_before,
        "rejected snapshot publication must not advance the provider boundary"
    );
}

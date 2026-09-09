use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn a_lost_publication_response_settles_before_a_held_peer_successor() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "settle-with-held-peer-successor",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_db = open_test_db(peer_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &peer_db,
            peer_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind publisher");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("observe the peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('owner-shared', 'Shared effect', 1, '0000000002000-0000-owner', '2026-01-01'), \
             ('owner-private', 'Private effect', 0, '0000000002000-0000-owner', '2026-01-01')",
        )
        .await;
    let database = StoreDatabase::new(&source);
    let mut writer = owner.authorize_writer().await.expect("authorize publisher");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare the mixed write"));
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("read the prepared write")
        .expect("the write owns a publication attempt");
    home.lose_next_conditional_replace_response();
    let (accepted, release) = home.pause_next_conditional_replace();
    let successor;
    let successor_package;
    let package_bytes;
    let prepared_package;
    let package_prefix;
    let peer_boundary;
    let context = ProtocolObjectContext::store_encrypted(
        store.root().store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    {
        let publication = writer.drain_store_writes();
        tokio::pin!(publication);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = accepted.notified() => {},
                result = &mut publication => panic!("publication returned before acceptance: {result:?}"),
            }
        })
        .await
        .expect("pause after provider acceptance and before returning its lost response");
        assert!(database
            .installed_store_commit_evidence(pending.commit.value.clone())
            .await
            .expect("read publisher receipt before settlement")
            .is_none());
        let (_, pulled) = peer
            .pull_store()
            .await
            .expect("peer observes the accepted write");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert_eq!(
            peer_db
                .query_test_text("SELECT title FROM notes WHERE id = 'owner-shared'")
                .await,
            "Shared effect"
        );
        peer_db
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                 ('peer-successor', 'Later peer effect', 1, '0000000003000-0000-peer', '2026-01-01')",
            )
            .await;
        let mut peer_writer = peer.authorize_writer().await.expect("authorize successor");
        assert!(peer_writer
            .prepare_pending_store_write()
            .await
            .expect("prepare the successor"));
        let peer_pending = StoreDatabase::new(&peer_db)
            .oldest_prepared_store_write()
            .await
            .expect("read prepared successor")
            .expect("peer owns its successor");
        successor = peer_pending.commit.value.reference().clone();
        successor_package = peer_pending
            .commit
            .value
            .store_package()
            .expect("successor has a Store package")
            .clone();
        assert_eq!(
            peer_writer
                .drain_store_writes()
                .await
                .expect("publish successor"),
            1
        );
        drop(peer_writer);
        peer_boundary = StoreDatabase::new(&peer_db)
            .store_current_publication()
            .await
            .expect("read the accepted peer boundary");
        package_prefix = coven_protocol::store_commit::package_semantic_prefix(
            peer_pending.commit.value.candidate_family(),
            &successor.coord.stream_id.to_string(),
            successor.coord.sequence(),
            successor_package.content_hash,
        );
        (package_bytes, prepared_package) = storage
            .read_prepared_protocol_slot(&context, successor_package.object.slot(), &package_prefix)
            .await
            .expect("retain exact peer package bytes");
        storage
            .delete_protocol_object(&successor_package.object)
            .await
            .expect("make only the later peer package unavailable");
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut publication)
                .await
                .expect("publisher settles its lost response")
                .expect("an unrelated held package cannot prevent completing the installed write"),
            1
        );
    }
    assert_eq!(writer.drain_store_writes().await.expect("drain again"), 0);
    drop(writer);
    assert!(database
        .active_store_publication()
        .await
        .expect("read released reservation")
        .is_none());
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("read completed journal")
        .is_none());
    let (observed, entries) = database
        .retained_store_publication()
        .await
        .expect("read accepted history after settlement");
    assert_eq!(observed, peer_boundary);
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.value.payload == pending.publication.entry.payload)
            .count(),
        1
    );
    let receipt = database
        .installed_store_commit_evidence(pending.commit.value.clone())
        .await
        .expect("read original installed receipt")
        .expect("the exact original candidate was installed");
    assert_eq!(receipt.commit_ref(), pending.commit.value.reference());
    for (id, title) in [
        ("owner-shared", "Shared effect"),
        ("owner-private", "Private effect"),
    ] {
        assert_eq!(
            source
                .query_test_text(&format!("SELECT title FROM notes WHERE id = '{id}'"))
                .await,
            title
        );
    }
    assert_eq!(
        source
            .query_test_text("SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'peer-successor'")
            .await,
        "0"
    );
    assert!(database
        .exact_materialized_ref(
            &successor.coord.stream_id.to_string(),
            successor.coord.sequence()
        )
        .await
        .expect("read held peer position")
        .is_none());
    storage
        .create_verified_protocol_object(
            &context,
            &prepared_package,
            &package_prefix,
            &package_bytes,
        )
        .await
        .expect("restore the peer package");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("ordinary pull retries the peer successor");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'peer-successor'")
            .await,
        "Later peer effect"
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read unchanged accepted boundary"),
        peer_boundary
    );
}

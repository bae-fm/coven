use super::RebaseFixture;
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::synced_schema::BlobDecl;
use coven_protocol::write::WriteStatus;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn snapshot_rebase_publishes_surviving_blob_content_under_a_newer_peer_row_stamp() {
    exercise_blob_merge(true).await;
}

#[tokio::test]
async fn newer_metadata_preserves_previously_published_concurrent_blob_content() {
    exercise_blob_merge(false).await;
}

async fn exercise_blob_merge(snapshot_before_content: bool) {
    let declaration = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id");
    let fixture = RebaseFixture::with_tables(
        coven_database::synthetic_store::test_synced_tables_with_blob(declaration),
    )
    .await;
    let database = StoreDatabase::new(&fixture.source);
    let accepted_bytes = b"original shared attachment";
    let replacement_bytes = b"replacement captured before the peer metadata edit";
    let mut initial = WriteBatch::new();
    initial.put_blob("photos", "accepted-content", accepted_bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(initial, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO note_photos \
                     (id, note_id, kind, blob_id, size, hash, _updated_at, created_at) VALUES \
                     ('merged-photo', 'shared', 'original metadata', 'accepted-content', {}, '{}', \
                     '0000000001000-0000-owner', '2026-01-01')",
                    accepted_bytes.len(),
                    coven_protocol::blob::content_hash(accepted_bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture the original shared attachment");
    RebaseFixture::publish(&fixture.owner).await;
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer installs the original attachment and exact blob binding");
    let original_binding = database
        .row_blob_ref("note_photos", "merged-photo")
        .await
        .expect("read original attachment authority");
    let original_blob = original_binding
        .stored()
        .expect("original blob is uploaded");
    assert_eq!(original_blob.locator().blob_id(), "accepted-content");

    let mut replacement = WriteBatch::new();
    replacement.put_blob("photos", "replacement-content", replacement_bytes.to_vec());
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(replacement, move |sql| {
                sql.execute_batch(&format!(
                    "UPDATE note_photos SET blob_id = 'replacement-content', size = {}, hash = '{}', \
                     _updated_at = '0000000002000-0000-owner' WHERE id = 'merged-photo'",
                    replacement_bytes.len(),
                    coven_protocol::blob::content_hash(replacement_bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture older replacement content without changing metadata");
    let original_capture = database
        .store_write_capture_for_test(captured.write_id.clone())
        .await
        .expect("read immutable replacement capture");
    let original_facts: coven_database::StoreWriteBlobFacts =
        serde_json::from_str(&original_capture.2).expect("decode captured blob facts");
    let [fact] = original_facts.blobs.as_slice() else {
        panic!("replacement captures exactly one blob: {original_facts:?}");
    };
    assert_eq!(fact.row_stamp, "0000000002000-0000-owner");
    assert_eq!(fact.blob.id, "replacement-content");
    assert_eq!(
        fact.plaintext_hash,
        coven_protocol::store_commit::ObjectHash::digest(replacement_bytes)
    );
    assert!(fact.audience_move.is_none());
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("reserve the captured content replacement"));
    drop(writer);
    let original = database
        .active_store_publication()
        .await
        .unwrap()
        .expect("replacement reserves an author coordinate");
    let (write_id, registration, coord) = original.commit_reservation().unwrap();
    assert_eq!(write_id, &captured.write_id);

    let peer_database = StoreDatabase::new(&fixture.target);
    let peer_capture = StoreRowWrites::new(peer_database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "UPDATE note_photos SET kind = 'peer metadata', \
                     _updated_at = '0000000003000-0000-peer' WHERE id = 'merged-photo'",
                )?;
                Ok::<_, DbError>(())
            }),
            Some(fixture.routing.clone()),
            None,
        )
        .await
        .expect("capture peer metadata through the host write owner");
    let peer_write_id = peer_capture.write_id;
    let peer_capture = peer_database
        .store_write_capture_for_test(peer_write_id.clone())
        .await
        .expect("read peer metadata's exact captured blob facts");
    let peer_facts: coven_database::StoreWriteBlobFacts =
        serde_json::from_str(&peer_capture.2).expect("decode peer metadata blob facts");
    let [peer_fact] = peer_facts.blobs.as_slice() else {
        panic!("metadata edit must retain its unchanged blob: {peer_facts:?}");
    };
    assert_eq!(peer_fact.blob.id, "accepted-content");
    assert_eq!(peer_fact.row_stamp, "0000000003000-0000-peer");
    assert_eq!(
        peer_fact.plaintext_hash,
        original_blob.locator().plaintext_hash()
    );
    if snapshot_before_content {
        RebaseFixture::publish(&fixture.peer).await;
        let peer_binding = peer_database
            .row_blob_ref("note_photos", "merged-photo")
            .await
            .expect("published metadata retains an accepted exact blob binding");
        assert_eq!(
            peer_binding
                .stored()
                .expect("peer metadata blob is remote")
                .locator()
                .blob_id(),
            "accepted-content"
        );
        assert_eq!(
            fixture.target.query_test_text(
                "SELECT blob_id || ':' || kind || ':' || _updated_at FROM note_photos WHERE id = 'merged-photo'",
            ).await,
            "accepted-content:peer metadata:0000000003000-0000-peer",
            "the snapshot contains peer metadata and the original content",
        );
        fixture
            .peer
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish the newer peer metadata snapshot");
        let (_, pulled) = fixture
            .owner
            .pull_store()
            .await
            .expect("merge older blob replacement with newer peer metadata");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        let awaiting = database
            .active_store_publication()
            .await
            .unwrap()
            .expect("snapshot rebase preserves the replacement reservation");
        assert!(awaiting.is_awaiting_preparation());
        assert_eq!(
            awaiting.commit_reservation(),
            Some((write_id, registration, coord))
        );
        assert_eq!(
            database
                .store_write_capture_for_test(captured.write_id.clone())
                .await
                .unwrap(),
            original_capture,
        );
        assert_eq!(
            fixture.source.query_test_text(
                "SELECT blob_id || ':' || kind || ':' || _updated_at FROM note_photos WHERE id = 'merged-photo'",
            ).await,
            "replacement-content:peer metadata:0000000003000-0000-peer",
            "content survives without promoting its captured timestamp",
        );
    } else {
        let mut writer = fixture.peer.authorize_writer().await.unwrap();
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("reserve metadata captured against the original blob"));
        assert_eq!(
            peer_database
                .store_write_capture_for_test(peer_write_id.clone())
                .await
                .unwrap(),
            peer_capture,
            "metadata must remain captured against the original content",
        );
    }
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish the merged blob replacement and its exact binding"),
        1
    );
    drop(writer);
    if !snapshot_before_content {
        let mut writer = fixture.peer.authorize_writer().await.unwrap();
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish newer metadata without replacing concurrent blob content"),
            1,
        );
        drop(writer);
        assert!(matches!(
            peer_database.write_status(&peer_write_id).await.unwrap(),
            WriteStatus::Published(_)
        ));
        assert_eq!(
            peer_database
                .store_write_capture_for_test(peer_write_id.clone())
                .await
                .unwrap(),
            peer_capture,
            "publishing must not rewrite the independently captured metadata",
        );
        let (_, pulled) = fixture
            .owner
            .pull_store()
            .await
            .expect("content author installs the concurrent metadata publication");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    }
    let status = database.write_status(&captured.write_id).await.unwrap();
    let WriteStatus::Published(receipt) = status else {
        panic!("replacement needs its original write receipt: {status:?}");
    };
    assert_eq!(&receipt.exact_commit().unwrap().coord, coord);
    assert!(database.active_store_publication().await.unwrap().is_none());
    let (_, pulled) = fixture
        .peer
        .pull_store()
        .await
        .expect("peer installs the replacement publication");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");

    let mut source_binding = None;
    for (name, host) in [("source", &fixture.source), ("peer", &fixture.target)] {
        assert_eq!(
            host.query_test_text(
                "SELECT blob_id || ':' || kind || ':' || _updated_at FROM note_photos WHERE id = 'merged-photo'",
            ).await,
            "replacement-content:peer metadata:0000000003000-0000-peer",
            "{name} retains both merged values and the winning row stamp",
        );
        let binding = StoreDatabase::new(host)
            .row_blob_ref("note_photos", "merged-photo")
            .await
            .unwrap_or_else(|error| panic!("{name} must resolve the merged blob binding: {error}"));
        let stored = binding
            .stored()
            .unwrap_or_else(|| panic!("{name} must retain remote replacement authority"));
        assert_eq!(stored.locator().blob_id(), "replacement-content");
        assert_eq!(
            stored.locator().plaintext_hash(),
            coven_protocol::store_commit::ObjectHash::digest(replacement_bytes)
        );
        assert_ne!(stored.object(), original_blob.object());
        match &source_binding {
            Some(expected) => assert_eq!(stored, expected, "both devices bind the exact object"),
            None => source_binding = Some(stored.clone()),
        }
        let destination = fixture
            .source_dir
            .storage_dir()
            .join(format!("verified-{name}-merged-content"));
        let stage = fixture
            .source_dir
            .stage_atomic_file(&destination)
            .await
            .unwrap();
        let plaintext = fixture
            .storage
            .stage_verified_store_blob_plaintext(
                stored,
                stage,
                coven_storage::cloud::no_download_progress(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{name} must open the exact replacement object: {error}")
            });
        assert_eq!(
            tokio::fs::read(plaintext.path()).await.unwrap(),
            replacement_bytes
        );
    }
}

use super::RebaseFixture;
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn snapshot_rebase_retains_an_audience_move_payload_until_its_write_is_folded() {
    let fixture = RebaseFixture::with_tables(
        coven_database::synthetic_store::test_synced_tables_with_blob(
            coven_database::synthetic_store::photo_decl(),
        ),
    )
    .await;
    let database = StoreDatabase::new(&fixture.source);
    let bytes = b"audience move with a partially uploaded candidate";
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", "moved-photo", bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                     ('moved-root', 'Local root', 0, '0000000002000-0000-owner', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('moved-photo', 'moved-root', 'image', {}, '{}', \
                      '0000000002000-0000-owner', '2026-01-01')",
                    bytes.len(),
                    coven_protocol::blob::content_hash(bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture private blob source");
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "UPDATE notes SET shared = 1, _updated_at = '0000000002500-0000-owner' \
                     WHERE id = 'moved-root'",
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(fixture.owner.host_write_blob_staging())),
        )
        .await
        .expect("capture exact audience-move plaintext");
    let original_capture = database
        .store_write_capture_for_test(captured.write_id.clone())
        .await
        .expect("read immutable captured source ownership");
    let original: coven_database::StoreWriteBlobFacts =
        serde_json::from_str(&original_capture.2).expect("decode captured blob facts");
    let [fact] = original.blobs.as_slice() else {
        panic!("one captured file: {original:?}");
    };
    assert_eq!(
        fact.audience_move,
        Some(coven_database::StoreWriteBlobMoveMaterialization::Payload)
    );
    let source_hash = fact.plaintext_hash;
    assert_eq!(fact.plaintext_size, bytes.len() as u64);
    assert_captured_payload(
        &database,
        &captured.write_id,
        &original_capture,
        source_hash,
        bytes,
    )
    .await;
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize sharing");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare sharing"));
    let prepared = database
        .oldest_prepared_store_write()
        .await
        .expect("read sharing candidate")
        .expect("candidate prepared");
    assert_eq!(prepared.commit.value.write_id, captured.write_id);
    let spool_path = prepared
        .audiences
        .blobs
        .iter()
        .find_map(|blob| blob.spool_path())
        .expect("candidate owns a sealed upload spool")
        .to_path_buf();
    let objects = database
        .prepared_remote_objects(&captured.write_id)
        .await
        .expect("read production upload order");
    let package_position = objects
        .iter()
        .position(|object| object.closed.object() == prepared.audiences.packages[0].object())
        .expect("sharing package belongs to candidate");
    fixture
        .store
        .fail_exact_create_before_call(package_position + 1);
    writer
        .drain_store_writes()
        .await
        .expect_err("package failure leaves independently uploaded blob pending");
    drop(writer);
    let objects = database
        .prepared_remote_objects(&captured.write_id)
        .await
        .expect("read partial upload state");
    assert!(objects.iter().any(|object| {
        matches!(
            object.closed.payloads(),
            coven_protocol::remote_object::RemoteObjectPayloads::RowBlob { .. }
        ) && object.closed.records_verified_upload()
    }));
    assert!(
        spool_path.is_file(),
        "failed candidate retains its upload spool"
    );
    assert_captured_payload(
        &database,
        &captured.write_id,
        &original_capture,
        source_hash,
        bytes,
    )
    .await;

    fixture.snapshot_peer_edit(false).await;
    fixture
        .owner
        .pull_store()
        .await
        .expect("rebase sharing after peer snapshot");
    assert!(database
        .active_store_publication()
        .await
        .expect("read reserved replacement")
        .expect("same write reserved")
        .is_awaiting_preparation());
    assert!(
        spool_path.is_file(),
        "retirement awaits a prepared replacement"
    );
    assert_captured_payload(
        &database,
        &captured.write_id,
        &original_capture,
        source_hash,
        bytes,
    )
    .await;
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume sharing");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish replacement"),
        1
    );
    drop(writer);
    let published = database
        .row_blob_ref("note_photos", "moved-photo")
        .await
        .expect("replacement blob authority");
    let stored = published
        .stored()
        .expect("replacement retains remote blob authority");
    assert!(fixture
        .store
        .contains_stored_blob_object(stored)
        .await
        .expect("verify transferred remote source after candidate cleanup"));
    assert!(matches!(
        database
            .write_status(&captured.write_id)
            .await
            .expect("same write receipt"),
        coven_protocol::write::WriteStatus::Published(_)
    ));
    assert!(
        !spool_path.exists(),
        "the retired candidate no longer owns its upload spool"
    );
    assert_captured_payload(
        &database,
        &captured.write_id,
        &original_capture,
        source_hash,
        bytes,
    )
    .await;

    fixture
        .owner
        .publish_snapshot_generation_for_test()
        .await
        .expect("fold the accepted sharing write into a snapshot");
    assert!(database
        .store_write_payload_claims_for_test(&captured.write_id)
        .await
        .expect("read folded write payload ownership")
        .is_empty());
    assert!(!database
        .has_payload_for_test(source_hash)
        .await
        .expect("captured plaintext is released after folding"));
    assert!(matches!(
        database
            .write_status(&captured.write_id)
            .await
            .expect("folding retains the original write receipt"),
        coven_protocol::write::WriteStatus::Published(_)
    ));
    assert_eq!(
        database
            .row_blob_ref("note_photos", "moved-photo")
            .await
            .expect("snapshot retains the exact published blob"),
        published
    );
    let destination = fixture
        .source_dir
        .storage_dir()
        .join("verified-rebased-photo");
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
        .expect("read published blob after releasing the captured payload");
    assert_eq!(tokio::fs::read(plaintext.path()).await.unwrap(), bytes);
}

async fn assert_captured_payload(
    database: &StoreDatabase,
    write_id: &coven_protocol::write::WriteId,
    original_capture: &(String, String, String),
    source_hash: coven_foundation::object_hash::ObjectHash,
    bytes: &[u8],
) {
    assert_eq!(
        &database
            .store_write_capture_for_test(write_id.clone())
            .await
            .expect("read immutable capture"),
        original_capture
    );
    assert!(database
        .store_write_payload_claims_for_test(write_id)
        .await
        .expect("read the write's exact payload claims")
        .contains(&source_hash));
    assert_eq!(
        database
            .payload_for_test(source_hash)
            .await
            .expect("read retained captured plaintext"),
        bytes
    );
}

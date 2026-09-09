use super::RebaseFixture;
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};

#[tokio::test]
async fn snapshot_rebase_releases_an_audience_move_spool_after_remote_source_transfer() {
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
        .expect("stage exact audience-move spool");
    let original: coven_database::StoreWriteBlobFacts = serde_json::from_str(
        &fixture
            .source
            .query_test_text(&format!(
                "SELECT blob_facts FROM store_writes WHERE write_id = '{}'",
                captured.write_id.as_str(),
            ))
            .await,
    )
    .expect("read captured source ownership");
    let [fact] = original.blobs.as_slice() else {
        panic!("one captured file: {original:?}");
    };
    let Some(coven_database::StoreWriteBlobMoveDestination::Remote { spool_path, .. }) =
        &fact.audience_move
    else {
        panic!("sharing owns a staged remote source: {fact:?}");
    };
    let spool_path = spool_path.clone();
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
        "failed publication retains its exact source"
    );

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
        "the replacement uses the verified remote object, so the original captured spool no longer has a reader"
    );
}

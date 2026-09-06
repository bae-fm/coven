use super::local_replay_causality_tests::drain;
use super::test_helpers::*;
use coven_database::{DbError, HostWriteOperation, StoreRowWrites, WriteBatch};

#[tokio::test]
async fn replay_projection_and_suffix_reversal_preserve_a_leased_published_blob_drop() {
    let store_dir = test_store_dir();
    let db = open_test_db_with_blob(store_dir.clone(), photo_decl().with_id_column("blob_id"));
    let signer = user_keypair_from_seed([44; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "published-drop-replay",
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
    let bytes = b"private source bytes";
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", "private-source", bytes.to_vec());
    let private_write = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes VALUES \
                     ('private-root', 'Private', NULL, 0, \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at, blob_id) VALUES \
                     ('photo-row', 'private-root', 'image', {}, '{}', \
                      '0000000001000-0000-author', '2026-01-01', 'private-source');",
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
    let staging = device.host_write_blob_staging();
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute(
                    "UPDATE notes SET shared = 1, \
                     _updated_at = '0000000002000-0000-author' WHERE id = 'private-root'",
                    [],
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(staging)),
        )
        .await
        .expect("share private root");
    drain(&device).await;

    assert_eq!(
        database
            .write_blob_lease_count_for_test(&private_write.write_id)
            .await
            .expect("count retained private source lease"),
        1,
    );
    let (published_drop_sequence, published_drop_disposition) = db
        .first_published_blob_drop_intent_for_test("photos", "private-source")
        .await
        .expect("read published drop intent");
    assert_eq!(
        published_drop_disposition,
        coven_protocol::blob::DeferredLocalBlobDisposition::Cache,
    );
    assert!(database
        .row_blob_ref("note_photos", "photo-row")
        .await
        .expect("read published blob row")
        .stored()
        .is_some());

    database
        .run_host_store_write_for_test(None, None, |sql| {
            sql.execute(
                "UPDATE notes SET title = 'Shared update', \
                 _updated_at = '0000000003000-0000-author' WHERE id = 'private-root'",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture shared root update");
    db.execute_test_sql("UPDATE note_photos SET kind = 'stale' WHERE id = 'photo-row'")
        .await;
    drain(&device).await;

    assert_eq!(
        db.first_published_blob_drop_intent_for_test("photos", "private-source")
            .await
            .expect("read preserved published drop intent"),
        (
            published_drop_sequence,
            coven_protocol::blob::DeferredLocalBlobDisposition::Cache,
        ),
        "projection replacement preserves the earlier deferred disposition",
    );
    assert_eq!(
        db.query_test_text("SELECT kind FROM note_photos WHERE id = 'photo-row'")
            .await,
        "image",
    );
    assert!(database
        .row_blob_ref("note_photos", "photo-row")
        .await
        .expect("read restored blob row")
        .stored()
        .is_some());
    let source_path = store_dir
        .local_blob_path("photos", "private-source")
        .expect("private source path");
    assert_eq!(
        std::fs::read(source_path).expect("read leased private source"),
        bytes,
    );

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('blocked-root', 'Blocked', NULL, 1, \
                 '0000000004000-0000-author', '2026-01-01')",
    )
    .await;
    let blocked = database.pending_writes().await.unwrap()[0].write_id.clone();
    database
        .set_write_status(
            &blocked,
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "discard test precondition".to_string(),
                },
            ),
        )
        .await
        .expect("block shared write");
    let staging = device.host_write_blob_staging();
    database
        .run_host_store_write_for_test(None, Some(Box::new(staging)), |sql| {
            sql.execute(
                "UPDATE notes SET shared = 0, \
                 _updated_at = '0000000005000-0000-author' WHERE id = 'private-root'",
                [],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture unpublished withdrawal");
    for (kind, stamp) in [
        ("first-unpublished", "0000000006000-0000-author"),
        ("second-unpublished", "0000000007000-0000-author"),
    ] {
        database
            .run_host_store_write_for_test(None, None, move |sql| {
                sql.execute(
                    "UPDATE note_photos SET kind = ?1, _updated_at = ?2 WHERE id = 'photo-row'",
                    (kind, stamp),
                )?;
                Ok::<_, DbError>(())
            })
            .await
            .expect("capture unpublished blob metadata update");
    }
    let discarded = database
        .discard_blocked_write(&blocked)
        .await
        .expect("discard blocked suffix");
    let coven_database::BlockedWriteDiscard::Discarded(discarded) = discarded else {
        panic!("blocked suffix unexpectedly requires remote resolution");
    };
    assert_eq!(discarded.len(), 4);
    assert_eq!(
        db.query_test_text(
            "SELECT kind || ':' || _updated_at FROM note_photos WHERE id = 'photo-row'",
        )
        .await,
        "image:0000000002000-0000-author",
    );
    assert_eq!(
        db.query_test_text(
            "SELECT disposition FROM published_blob_drop_intents
             WHERE namespace = 'photos' AND blob_id = 'private-source'
             ORDER BY seq LIMIT 1",
        )
        .await,
        "cache",
        "the complete reversal reevaluates the deferred disposition against its final rows",
    );
    assert_eq!(
        db.published_blob_drop_intent_count_for_test(
            published_drop_sequence,
            "photos",
            "private-source",
        )
        .await
        .expect("count original published drop after suffix reversal"),
        1,
    );
    assert_eq!(
        database
            .write_blob_lease_count_for_test(&private_write.write_id)
            .await
            .expect("count source lease after suffix reversal"),
        1,
    );
}

#[tokio::test]
async fn suffix_reversal_preserves_a_published_drop_across_intermediate_private_rows() {
    let store_dir = test_store_dir();
    let db = open_test_db_with_blob(store_dir.clone(), photo_decl());
    let signer = user_keypair_from_seed([45; 32]);
    let (store, _) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "published-drop-suffix",
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
    let bytes = b"suffix source bytes";
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", "suffix-source", bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes VALUES \
                     ('suffix-root', 'Private', NULL, 0, \
                      '0000000001000-0000-author', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('suffix-source', 'suffix-root', 'image', {}, '{}', \
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
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute(
                    "UPDATE notes SET shared = 1, \
                     _updated_at = '0000000002000-0000-author' \
                     WHERE id = 'suffix-root'",
                    [],
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(device.host_write_blob_staging())),
        )
        .await
        .expect("share private root");
    drain(&device).await;

    let (published_drop_sequence, published_drop_disposition) = db
        .first_published_blob_drop_intent_for_test("photos", "suffix-source")
        .await
        .expect("read published drop intent");
    assert_eq!(
        published_drop_disposition,
        coven_protocol::blob::DeferredLocalBlobDisposition::Cache,
    );

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('suffix-blocked', 'Blocked', NULL, 1, \
                 '0000000003000-0000-author', '2026-01-01')",
    )
    .await;
    let blocked = database.pending_writes().await.unwrap()[0].write_id.clone();
    database
        .set_write_status(
            &blocked,
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "discard test precondition".to_string(),
                },
            ),
        )
        .await
        .expect("block shared write");
    database
        .run_host_store_write_for_test(
            None,
            Some(Box::new(device.host_write_blob_staging())),
            |sql| {
                sql.execute(
                    "UPDATE notes SET shared = 0, \
                     _updated_at = '0000000004000-0000-author' \
                     WHERE id = 'suffix-root'",
                    [],
                )?;
                Ok::<_, DbError>(())
            },
        )
        .await
        .expect("capture unpublished withdrawal");
    for (kind, stamp) in [
        ("first-private", "0000000005000-0000-author"),
        ("second-private", "0000000006000-0000-author"),
    ] {
        database
            .run_host_store_write_for_test(None, None, move |sql| {
                sql.execute(
                    "UPDATE note_photos SET kind = ?1, _updated_at = ?2 \
                     WHERE id = 'suffix-source'",
                    (kind, stamp),
                )?;
                Ok::<_, DbError>(())
            })
            .await
            .expect("capture private blob metadata update");
    }

    let discarded = database
        .discard_blocked_write(&blocked)
        .await
        .expect("discard blocked suffix");
    let coven_database::BlockedWriteDiscard::Discarded(discarded) = discarded else {
        panic!("blocked suffix unexpectedly requires remote resolution");
    };
    assert_eq!(discarded.len(), 4);
    assert_eq!(
        db.published_blob_drop_intent_count_for_test(
            published_drop_sequence,
            "photos",
            "suffix-source",
        )
        .await
        .expect("count original published drop after suffix reversal"),
        1,
    );
    assert_eq!(
        db.first_published_blob_drop_intent_for_test("photos", "suffix-source")
            .await
            .expect("read preserved published drop intent"),
        (
            published_drop_sequence,
            coven_protocol::blob::DeferredLocalBlobDisposition::Cache,
        ),
    );
}

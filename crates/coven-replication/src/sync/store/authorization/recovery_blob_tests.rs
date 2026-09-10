use crate::sync::test_helpers::{test_cloud_home, test_store_dir, TestStore};
use coven_database::synthetic_store::{photo_decl, test_migrations, test_synced_tables_with_blob};
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_keys::keys::UserKeypair;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn recovery_preserves_an_unreserved_audience_move_and_publishes_its_exact_bytes() {
    recover_captured_blob(CapturedWrite::AudienceMove).await;
}

#[tokio::test]
async fn recovered_blob_publication_reopens_after_upload_failure_without_losing_its_source() {
    recover_captured_blob(CapturedWrite::AudienceMoveWithUploadRetry).await;
}

#[tokio::test]
async fn recovery_reseals_a_previously_uploaded_blob_for_an_unreserved_metadata_edit() {
    recover_captured_blob(CapturedWrite::RemoteMetadataEdit).await;
}

enum CapturedWrite {
    AudienceMove,
    AudienceMoveWithUploadRetry,
    RemoteMetadataEdit,
}

async fn recover_captured_blob(case: CapturedWrite) {
    let restart_after_upload_failure = matches!(case, CapturedWrite::AudienceMoveWithUploadRetry);
    let owns_source_payload = !matches!(case, CapturedWrite::RemoteMetadataEdit);
    let owner = UserKeypair::generate();
    let store_dir = test_store_dir();
    store_dir
        .ensure_created()
        .expect("create physical database directory");
    let open = || {
        coven_database::Database::open_synthetic_for_test(
            &store_dir.db_path(),
            store_dir.clone(),
            test_synced_tables_with_blob(photo_decl()),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "captured-blob-host".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open physical captured-blob database")
    };
    let mut database = open();
    let mut records = StoreDatabase::new(&database);
    let (store, storage) = TestStore::create_with_connection(
        &database,
        store_dir.clone(),
        "recovery-with-captured-blob",
        owner.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store with a blob-bearing schema");
    let device = store
        .bind_device(&database, store_dir.clone(), &owner)
        .await
        .expect("bind original writer");
    let original_registration = records
        .local_activated_registration_ref()
        .await
        .unwrap()
        .unwrap();
    let bytes = if restart_after_upload_failure {
        let mut state = 0x3ad9_9f2d_u64;
        (0..128 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>()
    } else {
        b"captured private photo survives owner recovery".to_vec()
    };
    let captured_bytes = bytes.clone();
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", "recovery-photo", bytes.to_vec());
    StoreRowWrites::new(records.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                     ('recovery-root', 'Private photo', 0, '0000000002000-0000-owner', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('recovery-photo', 'recovery-root', 'image', {}, '{}', \
                      '0000000002000-0000-owner', '2026-01-01')",
                    captured_bytes.len(),
                    coven_protocol::blob::content_hash(&captured_bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture private photo and its local plaintext");
    let mut captured = StoreRowWrites::new(records.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "UPDATE notes SET shared = 1, _updated_at = '0000000002500-0000-owner' \
                     WHERE id = 'recovery-root'",
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(device.host_write_blob_staging())),
        )
        .await
        .expect("capture sharing and retain its exact source before publication");
    if matches!(case, CapturedWrite::RemoteMetadataEdit) {
        assert!(device.prepare_pending_store_write().await.unwrap());
        assert_eq!(device.drain_store_writes().await.unwrap(), 1);
        let previous = records
            .row_blob_ref("note_photos", "recovery-photo")
            .await
            .unwrap();
        assert_eq!(
            previous.stored().unwrap().locator().uploader(),
            &original_registration
        );
        captured = StoreRowWrites::new(records.clone())
            .execute(
                HostWriteOperation::new(WriteBatch::new(), |sql| {
                    sql.execute_batch(
                        "UPDATE note_photos SET created_at = '2026-01-02', \
                     _updated_at = '0000000003000-0000-owner' WHERE id = 'recovery-photo'",
                    )?;
                    Ok::<_, DbError>(())
                }),
                None,
                Some(Box::new(device.host_write_blob_staging())),
            )
            .await
            .expect("capture a metadata edit referring to the already published blob");
    }
    let original_capture = records
        .store_write_capture_for_test(captured.write_id.clone())
        .await
        .expect("read immutable captured base, changeset hash, and blob facts");
    let original_facts: coven_database::StoreWriteBlobFacts =
        serde_json::from_str(&original_capture.2).expect("read real captured blob authority");
    let [fact] = original_facts.blobs.as_slice() else {
        panic!("sharing captures exactly one photo: {original_facts:?}");
    };
    if owns_source_payload {
        assert_eq!(
            fact.audience_move,
            Some(coven_database::StoreWriteBlobMoveMaterialization::Payload)
        );
    } else {
        assert_eq!(
            fact.audience_move, None,
            "metadata edit does not move its audience"
        );
        assert_eq!(
            fact.previous.as_ref().unwrap().stored.locator().uploader(),
            &original_registration
        );
    }
    let source_hash = fact.plaintext_hash;
    assert_eq!(
        source_hash,
        coven_protocol::store_commit::ObjectHash::digest(&bytes)
    );
    if owns_source_payload {
        assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
    }
    assert_eq!(
        records
            .store_write_payload_claims_for_test(&captured.write_id)
            .await
            .unwrap()
            .contains(&source_hash),
        owns_source_payload
    );
    if restart_after_upload_failure {
        assert!(store_dir.payload_spool_path(source_hash).is_file());
    }
    assert_eq!(
        records.write_status(&captured.write_id).await.unwrap(),
        coven_protocol::write::WriteStatus::Pending,
    );
    assert!(records.active_store_publication().await.unwrap().is_none());
    assert!(records
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .is_none());

    let recovery_authority = store.founder_recovery_authority().await;
    let recovered_registration = device
        .owner_recovery_for_test()
        .await
        .expect("authorize recovery without a prepared publication")
        .recover_owner_device(&recovery_authority, None)
        .await
        .expect("recover while preserving the unreserved capture");
    assert_ne!(recovered_registration, original_registration);
    assert_eq!(
        records.local_activated_registration_ref().await.unwrap(),
        Some(recovered_registration.clone()),
    );
    assert_eq!(
        records
            .store_write_capture_for_test(captured.write_id.clone())
            .await
            .unwrap(),
        original_capture
    );
    if owns_source_payload {
        assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
    }
    assert!(records.active_store_publication().await.unwrap().is_none());
    let mut recovered = store
        .bind_device(&database, store_dir.clone(), &owner)
        .await
        .expect("bind recovered writer instead of using cached original authority");
    if restart_after_upload_failure {
        assert!(recovered.prepare_pending_store_write().await.unwrap());
        store.fail_exact_create_before_call(1);
        recovered
            .drain_store_writes()
            .await
            .expect_err("provider failure retains exact pending publication");
        assert_eq!(
            records
                .store_write_capture_for_test(captured.write_id.clone())
                .await
                .unwrap(),
            original_capture
        );
        if owns_source_payload {
            assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
        }
        assert!(records
            .store_write_payload_claims_for_test(&captured.write_id)
            .await
            .unwrap()
            .contains(&source_hash));
        database = open();
        records = StoreDatabase::new(&database);
        recovered = store
            .bind_device(&database, store_dir.clone(), &owner)
            .await
            .expect("reopen the recovered writer from its physical database and payload directory");
        if owns_source_payload {
            assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
        }
        assert_eq!(
            records
                .store_write_capture_for_test(captured.write_id.clone())
                .await
                .unwrap(),
            original_capture
        );
    }
    let publication = async {
        if !restart_after_upload_failure {
            assert!(
                recovered.prepare_pending_store_write().await?,
                "the recovered writer prepares the unreserved shared capture"
            );
        }
        recovered.drain_store_writes().await
    }
    .await;

    // Check durable source preservation before reporting any publication error.
    assert_eq!(
        records
            .store_write_capture_for_test(captured.write_id.clone())
            .await
            .unwrap(),
        original_capture
    );
    assert_eq!(
        database
            .query_test_text("SELECT title FROM notes WHERE id = 'recovery-root' AND shared = 1")
            .await,
        "Private photo",
    );
    if publication.is_err() {
        if owns_source_payload {
            assert_eq!(
                tokio::fs::read(
                    store_dir
                        .local_blob_path("photos", "recovery-photo")
                        .unwrap()
                )
                .await
                .unwrap(),
                bytes
            );
            assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
        } else {
            assert!(store
                .contains_stored_blob_object(&fact.previous.as_ref().unwrap().stored)
                .await
                .unwrap());
        }
        assert!(records
            .pending_writes()
            .await
            .unwrap()
            .iter()
            .any(|write| write.write_id == captured.write_id));
    }
    assert_eq!(
        publication.expect("publish the unreserved capture after recovery"),
        1
    );
    let coven_protocol::write::WriteStatus::Published(position) =
        records.write_status(&captured.write_id).await.unwrap()
    else {
        panic!("the captured write must have an exact publication receipt");
    };
    let reference = position
        .exact_commit()
        .expect("the recovered write published its commit");
    let published = records.published_write_commits().await.unwrap();
    assert_eq!(published.len(), if owns_source_payload { 1 } else { 2 });
    assert!(published.contains(reference));
    let commit = recovered.load_commit_for_test(reference).await.unwrap();
    assert_eq!(commit.write_id, captured.write_id);
    assert_eq!(commit.author_registration, recovered_registration);
    let row_blob = records
        .row_blob_ref("note_photos", "recovery-photo")
        .await
        .expect("read published row's exact blob authority");
    let stored = row_blob
        .stored()
        .expect("sharing publishes remote blob authority");
    assert_eq!(stored.locator().uploader(), &recovered_registration);
    assert!(store.contains_stored_blob_object(stored).await.unwrap());
    let destination = store_dir.storage_dir().join("verified-recovered-photo");
    let stage = store_dir.stage_atomic_file(&destination).await.unwrap();
    let plaintext = storage
        .stage_verified_store_blob_plaintext(
            stored,
            stage,
            coven_storage::cloud::no_download_progress(),
        )
        .await
        .expect("open exact published blob using its retained authority");
    assert_eq!(tokio::fs::read(plaintext.path()).await.unwrap(), bytes);
    assert!(records.active_store_publication().await.unwrap().is_none());
    assert!(records.pending_writes().await.unwrap().is_empty());
    if owns_source_payload {
        assert_eq!(records.payload_for_test(source_hash).await.unwrap(), bytes);
    }
    assert_eq!(
        records
            .store_write_payload_claims_for_test(&captured.write_id)
            .await
            .unwrap()
            .contains(&source_hash),
        owns_source_payload
    );
}

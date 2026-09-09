use super::{RebaseFixture, StoreDatabase};

#[tokio::test]
async fn snapshot_rebase_preparation_failure_preserves_the_reserved_blob_source() {
    exercise_blob_preparation_failure(BlobSourceRecovery::RetryPreparation).await;
}

#[tokio::test]
async fn snapshot_rebase_awaiting_discard_reverses_effects_after_exact_cleanup() {
    exercise_blob_preparation_failure(BlobSourceRecovery::Discard).await;
}

#[tokio::test]
async fn snapshot_rebase_discard_resumes_after_cleanup_failure_and_reopen() {
    exercise_blob_preparation_failure(BlobSourceRecovery::ResumeDiscard).await;
}

enum BlobSourceRecovery {
    RetryPreparation,
    Discard,
    ResumeDiscard,
}

async fn exercise_blob_preparation_failure(recovery: BlobSourceRecovery) {
    use coven_database::{DbError, HostWriteOperation, StoreRowWrites, WriteBatch};
    let fixture = RebaseFixture::with_tables(
        coven_database::synthetic_store::test_synced_tables_with_blob(
            coven_database::synthetic_store::photo_decl(),
        ),
    )
    .await;
    let database = StoreDatabase::new(&fixture.source);
    let bytes = b"recorded blob source";
    let mut blobs = WriteBatch::new();
    blobs.put_blob("photos", "reserved-photo", bytes.to_vec());
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(blobs, move |sql| {
                sql.execute_batch(&format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('reserved-photo', 'shared', 'image', {}, '{}', \
                 '0000000002000-0000-owner', '2026-01-01')",
                bytes.len(), coven_protocol::blob::content_hash(bytes),
            ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture blob through host owner");
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize blob write");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare original blob"));
    drop(writer);
    let original = database
        .active_store_publication()
        .await
        .expect("read original reservation")
        .expect("reserved blob write");
    let old = database
        .oldest_prepared_store_write()
        .await
        .expect("read prepared blob")
        .expect("prepared original");
    let old_spool = old
        .audiences
        .blobs
        .iter()
        .find_map(|blob| blob.spool_path())
        .expect("original spool")
        .to_path_buf();
    fixture.snapshot_peer_edit(false).await;
    fixture
        .owner
        .pull_store()
        .await
        .expect("rebase original blob and reserve replacement");
    let awaiting = database
        .active_store_publication()
        .await
        .expect("read awaiting phase")
        .expect("retained reservation");
    assert!(awaiting.is_awaiting_preparation());
    assert_eq!(awaiting.commit_reservation(), original.commit_reservation());
    let source = fixture
        .source_dir
        .local_blob_path("photos", "reserved-photo")
        .expect("source path");
    let saved = tokio::fs::read(&source)
        .await
        .expect("read captured source");
    tokio::fs::remove_file(&source)
        .await
        .expect("simulate unavailable source");
    // The old encrypted spool belongs to the unaccepted candidate. It does not
    // replace the plaintext source needed to seal the new exact candidate.
    assert!(old_spool.exists());
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("retry preparation");
    writer
        .drain_store_writes()
        .await
        .expect_err("unavailable recorded source stops preparation");
    drop(writer);
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved awaiting phase"),
        Some(awaiting)
    );
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("no partial signed candidate")
        .is_none());
    assert_eq!(
        database
            .write_blob_lease_count_for_test(&captured.write_id)
            .await
            .expect("source lease"),
        1
    );
    if !matches!(recovery, BlobSourceRecovery::RetryPreparation) {
        let removed = if matches!(recovery, BlobSourceRecovery::ResumeDiscard) {
            let retained_spool = tokio::fs::read(&old_spool)
                .await
                .expect("read retained encrypted source");
            tokio::fs::remove_file(&old_spool)
                .await
                .expect("replace spool with an obstructing directory");
            tokio::fs::create_dir(&old_spool)
                .await
                .expect("obstruct retired spool deletion");
            let error = fixture
                .owner
                .discard_blocked_write(captured.write_id.clone())
                .await
                .expect_err("interrupted exact cleanup keeps the discard reserved");
            assert!(
                error
                    .to_string()
                    .contains("remove retired Store write blob spool"),
                "{error}"
            );
            let discarding = database
                .active_store_publication()
                .await
                .expect("read durable discard")
                .expect("discard retains author reservation");
            assert!(discarding.is_discarding());
            assert_eq!(
                discarding.commit_reservation(),
                original.commit_reservation()
            );
            database
                .retry_blocked_write(&captured.write_id)
                .await
                .expect_err("discard cannot resume candidate publication after releasing sources");
            assert!(old_spool.exists());
            assert!(
                fixture
                    .source
                    .test_row_exists("SELECT 1 FROM note_photos WHERE id = 'reserved-photo'")
                    .await
            );
            tokio::fs::remove_dir(&old_spool)
                .await
                .expect("remove spool deletion obstruction");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(
                &old_spool,
                &retained_spool,
            )
            .await
            .expect("restore retained source before retry");
            let reopened = RebaseFixture::open_with_tables(
                &fixture.path,
                fixture.source_dir.clone(),
                coven_database::synthetic_store::test_synced_tables_with_blob(
                    coven_database::synthetic_store::photo_decl(),
                ),
            );
            let resumed = fixture
                .store
                .bind_device_in(&reopened, fixture.source_dir.clone(), &fixture.signer)
                .await
                .expect("reopen discarded reservation");
            resumed
                .discard_blocked_write(captured.write_id.clone())
                .await
                .expect("resume exact cleanup and inverse after reopen")
        } else {
            fixture
                .owner
                .discard_blocked_write(captured.write_id.clone())
                .await
                .expect("discard awaiting write after owned cleanup")
        };
        assert_eq!(removed, vec![captured.write_id.clone()]);
        assert!(database
            .active_store_publication()
            .await
            .expect("released discarded reservation")
            .is_none());
        assert_eq!(
            database
                .write_status(&captured.write_id)
                .await
                .expect("discard receipt"),
            coven_protocol::write::WriteStatus::Resolved(
                coven_protocol::write::WriteResolution::Discarded
            )
        );
        assert!(
            !fixture
                .source
                .test_row_exists("SELECT 1 FROM note_photos WHERE id = 'reserved-photo'")
                .await
        );
        assert_eq!(
            fixture
                .source
                .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
                .await,
            "Peer changed body"
        );
        assert_eq!(
            database
                .write_blob_lease_count_for_test(&captured.write_id)
                .await
                .expect("released source lease"),
            0
        );
        assert!(!old_spool.exists(), "discarded candidate spool is retired");
        return;
    }
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&source, &saved)
        .await
        .expect("restore original source");
    if matches!(
        database
            .write_status(&captured.write_id)
            .await
            .expect("preparation status"),
        coven_protocol::write::WriteStatus::Blocked(_)
    ) {
        database
            .retry_blocked_write(&captured.write_id)
            .await
            .expect("retry available source");
    }
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume original reservation");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("prepare and publish restored source"),
        1
    );
    drop(writer);
    assert!(database
        .active_store_publication()
        .await
        .expect("finished reservation")
        .is_none());
    assert!(database
        .row_blob_ref("note_photos", "reserved-photo")
        .await
        .expect("published blob binding")
        .stored()
        .is_some());
    assert!(
        !old_spool.exists(),
        "retired candidate no longer owns a spool"
    );
}

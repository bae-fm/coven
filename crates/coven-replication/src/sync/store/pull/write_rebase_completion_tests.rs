use super::*;
use crate::sync::test_helpers::{InterceptedStorage, StorageInterceptor};
use coven_storage::ExactSlotStorage;

struct PauseCandidatePackageCreate {
    object: coven_protocol::objects::ExactObjectRef,
    exercised: std::sync::atomic::AtomicBool,
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl StorageInterceptor for PauseCandidatePackageCreate {
    async fn before_protocol_create(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if prepared.reference() == &self.object
            && !self
                .exercised
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn covered_write_cleanup_restarts_without_recreating_candidates_or_deleting_a_shared_blob() {
    check_covered_write_cleanup(CleanupInterruption::ProviderFailure).await;
}

#[tokio::test]
async fn covered_write_cleanup_preserves_accepted_owners_imported_while_retiring_candidates() {
    check_covered_write_cleanup(CleanupInterruption::ConcurrentSnapshot).await;
}

enum CleanupInterruption {
    ProviderFailure,
    ConcurrentSnapshot,
}

async fn check_covered_write_cleanup(interruption: CleanupInterruption) {
    use coven_database::{DbError, HostWriteOperation, StoreRowWrites, WriteBatch};
    use coven_protocol::write::{PublishedWrite, WriteStatus};
    use coven_storage::CloudSyncObjectStorage;

    let tables = coven_database::synthetic_store::test_synced_tables_with_blob(
        coven_database::synthetic_store::photo_decl(),
    );
    let fixture = RebaseFixture::with_tables(tables.clone()).await;
    let database = StoreDatabase::new(&fixture.source);
    let bytes = b"shared blob retained through covered candidate cleanup";
    let mut blobs = WriteBatch::new();
    blobs.put_blob("photos", "covered-photo", bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(blobs, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
                     ('covered-root', 'Private source', 0, '0000000002000-0000-owner', '2026-01-01');
                     INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES ('covered-photo', 'covered-root', 'image', {}, '{}',
                             '0000000002000-0000-owner', '2026-01-01')",
                    bytes.len(), coven_protocol::blob::content_hash(bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture the private blob source");
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "UPDATE notes SET shared = 1, _updated_at = '0000000002500-0000-owner'
                     WHERE id = 'covered-root'",
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(fixture.owner.host_write_blob_staging())),
        )
        .await
        .expect("capture the sharing operation");
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    assert!(writer.prepare_pending_store_write().await.unwrap());
    let original = database
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.commit.value.write_id, captured.write_id);
    let original_commit = original.commit.value.reference().clone();
    let original_package = original.audiences.packages[0].object().clone();
    let shared_blob = original.audiences.blobs[0].blob().clone();
    let objects = database
        .prepared_remote_objects(&captured.write_id)
        .await
        .unwrap();
    let package_position = objects
        .iter()
        .position(|object| object.closed.object() == &original_package)
        .expect("the package belongs to the production upload manifest");
    fixture
        .store
        .fail_exact_create_before_call(package_position + 1);
    writer
        .drain_store_writes()
        .await
        .expect_err("interrupt the package while the blob uploads");
    drop(writer);
    database.retire_uploaded_blob_spools().await.unwrap();
    assert!(fixture
        .store
        .contains_stored_blob_object(&shared_blob)
        .await
        .unwrap());

    let pause = Arc::new(PauseCandidatePackageCreate {
        object: original_package.clone(),
        exercised: std::sync::atomic::AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let intercepted = fixture
        .store
        .open_founder_store_with_storage(
            database.clone(),
            Arc::new(InterceptedStorage::new(
                fixture.storage.clone(),
                pause.clone(),
            )),
            fixture.source_dir.clone(),
        )
        .await
        .unwrap();
    let (uploaded, _resume_upload) = fixture.source.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWriteCommitUploaded {
            write_id: captured.write_id.clone(),
        },
    );
    let mut writer = intercepted.authorize_writer().await.unwrap();
    {
        let upload = writer.drain_store_writes();
        tokio::pin!(upload);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                () = pause.reached.notified() => {},
                result = &mut upload => panic!("upload returned before its package create: {result:?}"),
            }
        }).await.expect("pause the original exact package upload");

        // The original upload remains in flight while a restored directory
        // continues its durable operation and retires the old candidate.
        let continuation_dir = test_store_dir();
        let continuation_path = continuation_dir.db_path();
        fixture
            .source
            .vacuum_into_for_test(continuation_path.to_str().unwrap().into())
            .await
            .unwrap();
        crate::sync::test_helpers::copy_payload_files(&fixture.source_dir, &continuation_dir);
        let source_blob = fixture
            .source_dir
            .local_blob_path("photos", "covered-photo")
            .unwrap();
        let copied_blob = continuation_dir
            .local_blob_path("photos", "covered-photo")
            .unwrap();
        tokio::fs::create_dir_all(copied_blob.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::copy(source_blob, copied_blob).await.unwrap();
        let continued = RebaseFixture::open_with_tables(
            &continuation_path,
            continuation_dir.clone(),
            tables.clone(),
        );
        fixture.snapshot_peer_edit(false).await;
        let continuation = fixture
            .store
            .bind_device_in(&continued, continuation_dir, &fixture.signer)
            .await
            .unwrap();
        let mut replacement_writer = continuation.authorize_writer().await.unwrap();
        assert_eq!(replacement_writer.drain_store_writes().await.unwrap(), 1);
        drop(replacement_writer);
        let replacement = continuation
            .latest_local_store_position()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.coord, original_commit.coord);
        assert_ne!(replacement.commit_hash, original_commit.commit_hash);
        let binding = StoreDatabase::new(&continued)
            .row_blob_ref("note_photos", "covered-photo")
            .await
            .unwrap();
        assert_eq!(
            binding.stored(),
            Some(&shared_blob),
            "the accepted replacement reuses A's exact blob"
        );
        fixture.peer.pull_store().await.unwrap();
        fixture
            .peer
            .publish_snapshot_generation_for_test()
            .await
            .unwrap();

        pause.resume.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                () = uploaded.notified() => {},
                result = &mut upload => panic!("upload returned before its commit interruption: {result:?}"),
            }
        }).await.expect("the original upload finishes after replacement cleanup");
    }
    drop(writer);
    drop(intercepted);
    for object in [&original_commit.object, &original_package] {
        fixture
            .home
            .read_at(object.slot())
            .await
            .expect("the delayed original candidate now exists");
    }
    fixture
        .owner
        .pull_store()
        .await
        .expect("install the covering accepted snapshot");
    let accepted = database.store_current_publication().await.unwrap();
    fixture.home.clear_exact_creates();
    if matches!(interruption, CleanupInterruption::ConcurrentSnapshot) {
        use coven_protocol::remote_object::PendingCandidateRelease;

        let (cleanup_reached, cleanup_resume) =
            database.arm_test_pause(coven_database::DatabaseTestPoint::CoveredWriteCleanupPrepared);
        let mut writer = fixture.owner.authorize_writer().await.unwrap();
        let cleanup = writer.drain_store_writes();
        tokio::pin!(cleanup);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                () = cleanup_reached.notified() => {},
                result = &mut cleanup => panic!("cleanup returned before selecting its objects: {result:?}"),
            }
        }).await.expect("pause before provider deletion");
        let before = fixture
            .source
            .remote_object_for_test(shared_blob.object().clone())
            .await
            .unwrap()
            .release_pending_candidate(&original_commit)
            .unwrap();
        assert!(
            matches!(before, PendingCandidateRelease::Retained(_)),
            "the installed accepted blob is retained before concurrent import"
        );
        // Keep the receiver behind the snapshot's row cut so this race
        // exercises downloaded-image installation, not local reconstruction.
        fixture
            .target
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
             ('concurrent-import', 'Imported during cleanup', 1,
              '0000000005000-0000-peer', '2026-01-01')",
            )
            .await;
        RebaseFixture::publish(&fixture.peer).await;
        assert_ne!(
            StoreDatabase::new(&fixture.target)
                .store_current_publication()
                .await
                .unwrap()
                .record(),
            accepted.record(),
            "the receiver has not observed the snapshot predecessor"
        );
        fixture
            .peer
            .publish_snapshot_generation_for_test()
            .await
            .unwrap();
        fixture.home.clear_exact_creates();
        let (install_reached, install_resume) = database
            .arm_test_pause(coven_database::DatabaseTestPoint::ReceivedSnapshotInstallRequested);
        let pull = fixture.owner.pull_store();
        tokio::pin!(pull);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                () = install_reached.notified() => {},
                result = &mut pull => panic!("Pull returned before requesting checkpoint installation: {result:?}"),
            }
        }).await.expect("concurrent Pull reaches the actual receiver");
        install_resume.notify_one();
        let imported =
            match tokio::time::timeout(std::time::Duration::from_millis(250), &mut pull).await {
                Ok(result) => {
                    result.expect("concurrent accepted checkpoint import");
                    let after = fixture
                        .source
                        .remote_object_for_test(shared_blob.object().clone())
                        .await
                        .unwrap()
                        .release_pending_candidate(&original_commit)
                        .unwrap();
                    assert!(
                        matches!(after, PendingCandidateRelease::Retained(_)),
                        "the concurrent import preserves accepted blob ownership"
                    );
                    assert_ne!(
                        after, before,
                        "the successor adds its actual snapshot owner"
                    );
                    true
                }
                Err(_) => {
                    assert_eq!(
                        database.store_current_publication().await.unwrap(),
                        accepted,
                        "an excluded import leaves the accepted boundary unchanged"
                    );
                    false
                }
            };
        cleanup_resume.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut cleanup)
                .await
                .expect("cleanup finishes")
                .expect("retained-owner additions must not strand cleanup"),
            1
        );
        if !imported {
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut pull)
                .await
                .expect("Pull proceeds after cleanup")
                .expect("install the accepted checkpoint");
        }
        assert_eq!(
            fixture
                .source
                .query_test_text("SELECT title FROM notes WHERE id = 'concurrent-import'")
                .await,
            "Imported during cleanup",
            "the downloaded checkpoint installs the unseen peer row"
        );
        assert!(database.active_store_publication().await.unwrap().is_none());
        assert!(
            matches!(database.write_status(&captured.write_id).await.unwrap(),
            WriteStatus::Published(receipt) if matches!(receipt.as_ref(), PublishedWrite::Snapshot(_)))
        );
        assert!(
            fixture.home.exact_creates().is_empty(),
            "cleanup never recreates the candidate"
        );
        for object in [&original_commit.object, &original_package] {
            assert!(matches!(
                fixture.home.read_at(object.slot()).await,
                Err(coven_storage::cloud::CloudHomeError::NotFound(_))
            ));
            assert!(!fixture
                .source
                .remote_object_exists_for_test(object.clone())
                .await
                .unwrap());
        }
        assert_eq!(
            database
                .row_blob_ref("note_photos", "covered-photo")
                .await
                .unwrap()
                .stored(),
            Some(&shared_blob)
        );
        fixture
            .storage
            .verify_blob_object(&shared_blob)
            .await
            .expect("the accepted blob survives concurrent import and cleanup");
        return;
    }
    fixture
        .store
        .fail_nth_exact_delete_of(&[original_commit.object.slot(), original_package.slot()], 2);
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("the second exact deletion fails");
    assert!(
        error.to_string().contains("forced exact delete failure"),
        "{error}"
    );
    drop(writer);
    let terminal = database.active_store_publication().await.unwrap().unwrap();
    let position = terminal
        .covered_write_position()
        .expect("cleanup is durably terminal")
        .clone();
    assert_eq!(position.coord, original_commit.coord);
    assert_eq!(
        database.write_status(&captured.write_id).await.unwrap(),
        WriteStatus::Publishing
    );
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        accepted
    );
    let mut absent = 0;
    for object in [&original_commit.object, &original_package] {
        match fixture.home.read_at(object.slot()).await {
            Ok(_) => {}
            Err(coven_storage::cloud::CloudHomeError::NotFound(_)) => absent += 1,
            Err(error) => panic!("inspect interrupted cleanup: {error}"),
        }
    }
    assert_eq!(absent, 1, "one provider deletion preceded the failure");
    fixture
        .storage
        .verify_blob_object(&shared_blob)
        .await
        .unwrap();
    assert!(
        fixture.home.exact_creates().is_empty(),
        "covered cleanup never republishes a candidate"
    );
    drop(database);
    let root = fixture.store.root();
    let RebaseFixture {
        _directory,
        path,
        source_dir,
        storage,
        home,
        signer,
        source,
        target,
        store,
        owner,
        peer,
        routing: _,
    } = fixture;
    // Drop every owner of the original database before reopening it.
    drop((source, target, store, owner, peer));
    let reopened = RebaseFixture::open_with_tables(&path, source_dir.clone(), tables);
    let database = StoreDatabase::new(&reopened);
    assert_eq!(
        database.active_store_publication().await.unwrap(),
        Some(terminal)
    );
    let resumed = crate::sync::store::Store::open(
        database.clone(),
        storage.clone(),
        source_dir,
        &root,
        &signer,
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
    )
    .await
    .unwrap()
    .into_parts()
    .0;
    let mut writer = resumed.authorize_writer().await.unwrap();
    assert_eq!(writer.drain_store_writes().await.unwrap(), 1);
    drop(writer);
    assert_eq!(
        database.write_status(&captured.write_id).await.unwrap(),
        WriteStatus::Published(Box::new(PublishedWrite::Snapshot(position))),
    );
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        accepted
    );
    assert!(
        home.exact_creates().is_empty(),
        "restart resumes deletion without exact creates"
    );
    for object in [&original_commit.object, &original_package] {
        assert!(matches!(
            home.read_at(object.slot()).await,
            Err(coven_storage::cloud::CloudHomeError::NotFound(_))
        ));
        assert!(!reopened
            .remote_object_exists_for_test(object.clone())
            .await
            .unwrap());
    }
    assert_eq!(
        database
            .row_blob_ref("note_photos", "covered-photo")
            .await
            .unwrap()
            .stored(),
        Some(&shared_blob)
    );
    storage
        .verify_blob_object(&shared_blob)
        .await
        .expect("accepted blob survives both cleanup attempts");
}

#[tokio::test]
async fn a_restarted_writer_settles_a_compacted_replacement_of_its_reserved_operation() {
    let fixture = RebaseFixture::new().await;
    let original = fixture.prepare_local().await;
    let (write_id, author_registration, coord) = original.commit_reservation().unwrap();
    let write_id = write_id.clone();
    let author_registration = author_registration.clone();
    let coord = coord.clone();
    let coven_protocol::store_commit::StorePublicationPayload::Commit(original_commit) =
        &original.attempt().unwrap().entry.payload
    else {
        panic!("the reserved edit has a commit candidate");
    };
    let original_commit = original_commit.clone();

    // A restored copy continues the same durable operation. Its replacement
    // keeps the WriteId and author sequence while acquiring different bytes.
    let continuation_dir = test_store_dir();
    let continuation_path = continuation_dir.db_path();
    fixture
        .source
        .vacuum_into_for_test(continuation_path.to_str().unwrap().to_string())
        .await
        .expect("copy the reserved operation database");
    crate::sync::test_helpers::copy_payload_files(&fixture.source_dir, &continuation_dir);
    let continuation_db = RebaseFixture::open(&continuation_path, continuation_dir.clone());
    fixture.snapshot_peer_edit(false).await;
    let continuation = fixture
        .store
        .bind_device_in(&continuation_db, continuation_dir, &fixture.signer)
        .await
        .expect("open the restored operation owner");
    let mut writer = continuation.authorize_writer().await.unwrap();
    assert_eq!(writer.drain_store_writes().await.unwrap(), 1);
    drop(writer);
    let replacement = continuation
        .latest_local_store_position()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement.coord, coord);
    assert_ne!(replacement.commit_hash, original_commit.commit_hash);
    assert_eq!(
        continuation
            .load_commit_for_test(&replacement)
            .await
            .unwrap()
            .write_id,
        write_id,
        "candidate replacement preserves the logical operation",
    );
    continuation_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
         VALUES ('later-operation', 'Later accepted edit', 1,
                 '0000000004000-0000-owner', '2026-09-08')",
        )
        .await;
    RebaseFixture::publish(&continuation).await;
    let later = continuation
        .latest_local_store_position()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later.coord.stream_id, coord.stream_id);
    assert!(later.coord.sequence() > coord.sequence());
    fixture.peer.pull_store().await.unwrap();
    fixture
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .unwrap();
    let accepted = StoreDatabase::new(&fixture.target)
        .store_current_publication()
        .await
        .unwrap();

    let RebaseFixture {
        _directory,
        source,
        owner,
        source_dir,
        path,
        store,
        signer,
        ..
    } = fixture;
    drop(owner);
    drop(source);
    let reopened = RebaseFixture::open(&path, source_dir.clone());
    let resumed = store
        .bind_device_in(&reopened, source_dir.clone(), &signer)
        .await
        .unwrap();
    let mut writer = resumed.authorize_writer().await.unwrap();
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("settle the same operation accepted through a different compacted candidate"),
        1,
    );
    drop(writer);
    let database = StoreDatabase::new(&reopened);
    let receipt = database.write_status(&write_id).await.unwrap();
    let coven_protocol::write::WriteStatus::Published(published) = &receipt else {
        panic!("the covered operation is unfinished: {receipt:?}");
    };
    let coven_protocol::write::PublishedWrite::Snapshot(position) = published.as_ref() else {
        panic!("compaction cannot establish the original candidate hash: {published:?}");
    };
    assert_eq!(position.author_registration, author_registration);
    assert_eq!(position.coord, coord);
    assert_eq!(published.exact_commit(), None);
    assert_eq!(
        coven_protocol::store_commit::StorePublicationBase::Snapshot(position.snapshot.clone()),
        accepted.record().publication_base(),
    );
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        accepted,
        "settlement must not publish the logical edit again"
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Recorded local title"
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Peer changed body"
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT title FROM notes WHERE id = 'private'")
            .await,
        "Private local effect"
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT title FROM notes WHERE id = 'later-operation'")
            .await,
        "Later accepted edit"
    );

    resumed
        .publish_snapshot_generation_for_test()
        .await
        .expect("fold the covered write's private partition into a later baseline");
    resumed
        .pull_store()
        .await
        .expect("install the later baseline");
    let journal: serde_json::Value =
        serde_json::from_str(&database.store_write_journal_for_test().await.unwrap()).unwrap();
    let folded = journal
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row[1].as_str() == Some(write_id.as_str()))
        .expect("the published receipt survives journal folding");
    assert!(
        folded[4].is_null(),
        "the original changeset is folded: {folded}"
    );
    assert_eq!(database.write_status(&write_id).await.unwrap(), receipt);
    drop(database);
    drop(resumed);
    drop(reopened);
    let reopened = RebaseFixture::open(&path, source_dir.clone());
    let resumed = store
        .bind_device_in(&reopened, source_dir, &signer)
        .await
        .unwrap();
    resumed
        .pull_store()
        .await
        .expect("replay from the folded baseline after restart");
    assert_eq!(
        StoreDatabase::new(&reopened)
            .write_status(&write_id)
            .await
            .unwrap(),
        receipt,
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT title FROM notes WHERE id = 'private'")
            .await,
        "Private local effect",
    );
    assert_eq!(
        reopened
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Peer changed body",
    );
}

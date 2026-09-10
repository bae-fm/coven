use super::*;

/// A user-initiated blob deletion writes a signed tombstone and the graced GC
/// performs the delete, but only after re-checking that no live row still
/// references the blob. That check has to be able to answer for a row whose
/// locality comes from an audience column rather than a keep gate — a Circle
/// document's attachment is exactly that shape — or the whole GC pass fails and no
/// tombstone anywhere is ever collected.
#[tokio::test]
async fn tombstone_gc_resolves_a_live_reference_through_an_audience_scoped_row() {
    let fixture = RotationFixture::build("audience-scoped-tombstone-gc").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    fixture
        .capture_document_with_file(
            "00000000-0000-4000-8000-0000000000e3",
            "00000000-0000-4000-8000-0000000000f3",
            Some(fixture.circle_id),
            b"circle attachment under a tombstone",
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the Circle document and its file");
    let published = fixture.stored_blobs().await;
    let [stored] = published.as_slice() else {
        panic!("the Circle document published exactly one blob: {published:?}");
    };
    let stored = stored.clone();

    // A stale tombstone over a blob whose row is still live: past the grace the GC
    // must resolve the reference, cancel the tombstone, and keep the ciphertext.
    let deleted_at = chrono::DateTime::parse_from_rfc3339("2026-07-23T00:11:00+00:00")
        .expect("valid tombstone instant")
        .with_timezone(&chrono::Utc);
    fixture
        .db
        .enqueue_blob_delete_for_test(&stored, "2026-07-23T00:11:00Z")
        .await
        .expect("enqueue the blob deletion");
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("load the Owner Store");
    let writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the Owner Store");
    assert_eq!(
        writer
            .drain_tombstones(&coven_foundation::clock::FixedClock(deleted_at))
            .await
            .expect("write the signed tombstone"),
        1,
        "the queued deletion writes one tombstone"
    );
    let tombstone_key = format!(
        "blob_tombstones/{}{}",
        coven_protocol::remote_object::remote_object_id(stored.object()),
        coven_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])).suffix(),
    );
    assert!(
        fixture.home.read(&tombstone_key).await.is_ok(),
        "the tombstone is written at its exact slot"
    );

    let past = coven_foundation::clock::FixedClock(
        deleted_at + coven_protocol::blob::BLOB_TOMBSTONE_GRACE + chrono::Duration::seconds(1),
    );
    let collected = writer
        .gc_tombstones(&past)
        .await
        .expect("run the graced tombstone GC over an audience-scoped blob");

    assert_eq!(
        collected, 0,
        "a live row still references the blob, so nothing is collected"
    );
    assert!(
        fixture
            .store
            .contains_stored_blob_object(&stored)
            .await
            .expect("read the exact stored blob"),
        "the referenced ciphertext stays in cloud storage"
    );
    assert!(
        fixture.home.read(&tombstone_key).await.is_err(),
        "the stale tombstone is canceled"
    );
}

/// An audience move re-seals every blob its rows carry under the destination
/// audience's key, which mints a new locator for content the row still binds at
/// its old `_updated_at` — and one row stamp binds one exact locator. The move
/// carries its own stamp onto the blob rows it drags along, so the new binding is
/// a new one and the host never has to know that publishing a moved subtree needs
/// its children restamped by hand.
#[tokio::test]
async fn an_audience_move_restamps_the_blob_rows_it_drags() {
    let fixture = RotationFixture::build("audience-move-restamps-blob-rows").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let document = "00000000-0000-4000-8000-0000000000e4";
    let file = "00000000-0000-4000-8000-0000000000f4";
    fixture
        .capture_document_with_file(
            document,
            file,
            Some(fixture.circle_id),
            b"attachment that moves with its document",
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the Circle document and its file");
    assert_eq!(
        fixture.document_file_stamp(file).await,
        "2026-07-23T00:10:00Z",
    );

    // The move touches the document alone: the file row keeps the stamp its
    // published blob is already bound at.
    fixture
        .move_document_audience(document, None, "2026-07-23T00:20:00Z")
        .await;

    assert_eq!(
        fixture.document_file_stamp(file).await,
        "2026-07-23T00:20:00Z",
        "the dragged blob row carries the stamp its move published it at",
    );
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("republish the document and its re-sealed file under the Store audience");
}

/// Moving a row between audiences republishes its blob under the destination
/// audience's locator and drops the binding to the source ciphertext, which
/// nothing else ever deletes. Reclamation deletes exactly the ciphertext no live
/// row binds any more — in both directions, since a document can leave a Circle
/// for the Store audience or join one from it — and leaves the destination
/// ciphertext, which a live row does bind, alone.
#[tokio::test]
async fn audience_blob_reclaim_deletes_the_stranded_source_ciphertext() {
    let fixture = RotationFixture::build("audience-blob-reclaim").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let leaving = "00000000-0000-4000-8000-0000000000e1";
    let joining = "00000000-0000-4000-8000-0000000000e2";
    fixture
        .capture_document_with_file(
            leaving,
            "00000000-0000-4000-8000-0000000000f1",
            Some(fixture.circle_id),
            b"circle attachment",
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture
        .capture_document_with_file(
            joining,
            "00000000-0000-4000-8000-0000000000f2",
            None,
            b"store attachment",
            "2026-07-23T00:10:01Z",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish both documents and their files");

    let sources = fixture.stored_blobs().await;
    assert_eq!(
        sources.len(),
        2,
        "one ciphertext per audience is published: {sources:?}"
    );
    for stored in &sources {
        assert!(
            fixture
                .store
                .contains_stored_blob_object(stored)
                .await
                .expect("read the exact stored blob"),
            "the published ciphertext is uploaded"
        );
    }

    // Nothing has moved yet: every ciphertext is still bound by a live row.
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation while every blob is still bound");
    for stored in &sources {
        assert!(
            fixture
                .store
                .contains_stored_blob_object(stored)
                .await
                .expect("read the exact stored blob"),
            "a blob a live row still binds is never reclaimed"
        );
    }

    fixture
        .move_document_audience(leaving, None, "2026-07-23T00:20:00Z")
        .await;
    fixture
        .move_document_audience(joining, Some(fixture.circle_id), "2026-07-23T00:20:01Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("republish both documents under their destination audiences");

    let after_move = fixture.stored_blobs().await;
    let destinations = after_move
        .into_iter()
        .filter(|stored| !sources.contains(stored))
        .collect::<Vec<_>>();
    assert_eq!(
        destinations.len(),
        2,
        "each move republished its blob under a new locator: {destinations:?}"
    );

    // A blob a published snapshot image lists is read by devices restoring from
    // that image, which have no rows at all — so an unbound blob an image still
    // names is held back rather than deleted.
    let mut pinned_by_an_image = Vec::new();
    for stored in &sources {
        if StoreDatabase::new(&fixture.db)
            .stored_blob_has_snapshot_owner_for_test(stored.clone())
            .await
            .expect("read the blob's snapshot ownership")
        {
            pinned_by_an_image.push(stored.clone());
        }
    }

    // Closing the Circle installs a successor bootstrap covering both moves.
    // The Store image then retires the covered package replay inputs. Earlier
    // published images retain their own blob ownership independently.
    fixture.close_epoch_by_removing_the_circle_member().await;
    fixture
        .owner_device
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish and install Store coverage after both audience moves");
    let indexed = fixture.stored_blobs().await;
    for source in &sources {
        assert!(
            indexed.contains(source),
            "accepted source blob provenance survives replay until exact reclamation: {:?}",
            source.locator().audience(),
        );
    }
    fixture
        .reclaim_packages()
        .await
        .expect("reclaim the stranded source ciphertext");

    let mut deleted = 0;
    for stored in &sources {
        if pinned_by_an_image.contains(stored) {
            assert!(
                fixture
                    .store
                    .contains_stored_blob_object(stored)
                    .await
                    .expect("read the exact stored blob"),
                "a blob a published snapshot image lists survives its rows moving away: {:?}",
                stored.locator().audience()
            );
            continue;
        }
        assert!(
            !fixture
                .store
                .contains_stored_blob_object(stored)
                .await
                .expect("read the exact stored blob"),
            "the stranded source ciphertext is deleted: {:?}; ownership: {:?}",
            stored.locator().audience(),
            fixture
                .db
                .remote_object_for_test(stored.object().clone())
                .await
        );
        deleted += 1;
    }
    assert!(
        deleted > 0,
        "at least one stranded source ciphertext was reclaimable"
    );
    for stored in &destinations {
        assert!(
            fixture
                .store
                .contains_stored_blob_object(stored)
                .await
                .expect("read the exact stored blob"),
            "the destination ciphertext a live row binds survives: {:?}",
            stored.locator().audience()
        );
    }
}

/// Every reclaim kind rides the same durable journal, so a delete that fails
/// between authorization and deletion must leave a plan the next run finishes.
/// Driven over a stranded audience blob because its eligibility is fully
/// controlled here — the move unbinds it and releasing replay ownership frees it —
/// so the interruption lands on the delete under test rather than on whatever a
/// setup cycle happened to reclaim first.
#[tokio::test]
async fn interrupted_audience_blob_reclaim_resumes_on_restart() {
    let fixture = RotationFixture::build("audience-blob-crash-resume").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let document = "00000000-0000-4000-8000-0000000000e5";
    fixture
        .capture_document_with_file(
            document,
            "00000000-0000-4000-8000-0000000000f5",
            Some(fixture.circle_id),
            b"circle attachment for the interrupted reclaim",
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the Circle document and its file");
    let published = fixture.stored_blobs().await;
    let [source] = published.as_slice() else {
        panic!("the Circle document published exactly one blob: {published:?}");
    };
    let source = source.clone();

    fixture
        .move_document_audience(document, None, "2026-07-23T00:20:00Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("republish the document under the Store audience");
    fixture.release_retained_replay_ownership().await;

    // The stranded ciphertext is now eligible. Fail its delete: the reclaim
    // authorizes the deletion, the delete fails, and the run surfaces the failure
    // to its initiator with the object still present.
    fixture
        .home
        .fail_nth_exact_delete_of(&[source.object().slot()], 1);
    let interrupted = fixture.reclaim_packages().await;
    assert!(
        interrupted.is_err(),
        "the delete failure fails the reclaim to its initiator: {interrupted:?}"
    );
    assert!(
        fixture
            .store
            .contains_stored_blob_object(&source)
            .await
            .expect("read the exact stored blob"),
        "the stranded ciphertext survives the interrupted deletion"
    );

    // The journal still holds the authorized reclaim, so a restart finishes it.
    fixture
        .reclaim_packages()
        .await
        .expect("restart resumes the interrupted audience blob reclaim");
    assert!(
        !fixture
            .store
            .contains_stored_blob_object(&source)
            .await
            .expect("read the exact stored blob"),
        "the restart deletes the stranded ciphertext"
    );

    // A further run is idempotent: the target is recorded as reclaimed, so nothing
    // re-authorizes or re-deletes it.
    fixture
        .reclaim_packages()
        .await
        .expect("a further run finds nothing left to reclaim");
    assert!(
        !fixture
            .store
            .contains_stored_blob_object(&source)
            .await
            .expect("read the exact stored blob"),
        "the reclaimed ciphertext stays absent"
    );
}

#[tokio::test]
async fn a_metadata_edit_after_circle_rotation_reseals_the_existing_blob() {
    use coven_storage::CloudSyncObjectStorage;

    let fixture = RotationFixture::build("circle-rotated-blob-metadata").await;
    fixture.member_device.pull().await;
    let document = "00000000-0000-4000-8000-0000000000e6";
    let destination = "00000000-0000-4000-8000-0000000000e7";
    let file = "00000000-0000-4000-8000-0000000000f6";
    let bytes = b"unchanged attachment bytes across a Circle key rotation";
    fixture
        .capture_document_with_file(
            document,
            file,
            Some(fixture.circle_id),
            bytes,
            "2026-07-23T00:10:00Z",
        )
        .await;
    fixture
        .capture_document(destination, Some(fixture.circle_id), "2026-07-23T00:10:01Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the attachment and both Circle documents");
    let database = StoreDatabase::new(&fixture.db);
    let original = database
        .row_blob_ref("document_files", file)
        .await
        .expect("read the original exact file binding");
    let original = original.stored().expect("file is uploaded").clone();
    assert!(fixture
        .store
        .contains_stored_blob_object(&original)
        .await
        .unwrap());

    fixture.close_epoch_by_removing_the_circle_member().await;
    let (successor, _) = database
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("read activated successor control");
    let successor_key = successor.control.value.key_fingerprint();
    assert_ne!(original.locator().key_fingerprint(), Some(successor_key));
    assert_eq!(
        database
            .local_activated_registration_ref()
            .await
            .unwrap()
            .as_ref(),
        Some(original.locator().uploader()),
        "rotation preserves the uploading device"
    );

    let captured = database
        .run_host_store_write_for_test(
            Some(EncryptionService::from_key([42; 32])),
            None,
            move |transaction| {
                transaction.execute(
                "UPDATE document_files SET document_id = ?2, _updated_at = ?3 WHERE id = ?1",
                rusqlite::params![file, destination, "2026-07-23T00:20:00Z"],
            ).map(|_| ()).map_err(DbError::from)
            },
        )
        .await
        .expect("capture a metadata edit within the same Circle");
    let capture = database
        .store_write_capture_for_test(captured.write_id.clone())
        .await
        .unwrap();
    let facts: coven_database::StoreWriteBlobFacts = serde_json::from_str(&capture.2).unwrap();
    let [fact] = facts.blobs.as_slice() else {
        panic!("metadata edit captures exactly one file: {facts:?}");
    };
    assert!(
        fact.audience_move.is_none(),
        "the document move stays in one audience"
    );
    assert_eq!(fact.plaintext_size, bytes.len() as u64);
    assert_eq!(
        fact.plaintext_hash,
        coven_protocol::store_commit::ObjectHash::digest(bytes)
    );
    assert_eq!(
        &fact
            .previous
            .as_ref()
            .expect("edit retains its uploaded source")
            .stored,
        &original
    );

    let mut writer = fixture.owner_device.authorize_writer().await.unwrap();
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare metadata under the current Circle key"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish the metadata edit"),
        1
    );
    drop(writer);
    let published = match database.write_status(&captured.write_id).await.unwrap() {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published edit has an exact commit")
            .clone(),
        status => panic!("metadata edit must publish: {status:?}"),
    };
    let commit = fixture
        .owner_device
        .load_commit_for_test(&published)
        .await
        .unwrap();
    let [package] = commit.value().circle_packages() else {
        panic!("metadata edit publishes one Circle package");
    };
    assert_eq!(package.control, successor.control.coord);
    assert_eq!(package.key_fingerprint, successor_key);
    let current = database.row_blob_ref("document_files", file).await.unwrap();
    let stored = current
        .stored()
        .expect("updated metadata retains remote blob authority");
    assert_eq!(stored.locator().uploader(), original.locator().uploader());
    assert_eq!(stored.locator().key_fingerprint(), Some(successor_key));
    assert_ne!(stored.object(), original.object());
    assert_eq!(
        fixture.document_file_stamp(file).await,
        "2026-07-23T00:20:00Z"
    );
    assert_eq!(fixture.db.query_test_text(
        "SELECT document_id FROM document_files WHERE id = '00000000-0000-4000-8000-0000000000f6'",
    ).await, destination);
    let access = fixture
        .owner_device
        .circle_epoch_access(fixture.circle_id, successor.control.coord)
        .await
        .unwrap()
        .expect("owner holds the successor Circle key");
    let stage = fixture
        .store_dir
        .stage_atomic_file(
            &fixture
                .store_dir
                .storage_dir()
                .join("verified-rotated-circle-file"),
        )
        .await
        .unwrap();
    let plaintext = fixture
        .cloud_storage
        .stage_verified_blob_plaintext(
            stored,
            access.blob_protection(),
            stage,
            coven_storage::cloud::no_download_progress(),
        )
        .await
        .expect("open the new exact ciphertext with the successor Circle key");
    assert_eq!(tokio::fs::read(plaintext.path()).await.unwrap(), bytes);
}

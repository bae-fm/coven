use super::*;

#[tokio::test]
async fn captured_changeset_retries_after_host_provided_blob_upload_failure() {
    let keypair = UserKeypair::generate();
    let (db, db_store_dir, storage, cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheEager).await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('hponly', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"cover"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "hponly",
        b"cover",
    )
    .await
    .expect("store host-provided blob");

    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    let failed = match run_cycle_in_task(Arc::clone(&reject_blob_create), device.clone()).await {
        Ok(_) => panic!("blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.to_string().contains("unexpected blob create call 1"),
        "cycle surfaces the blob upload failure: {failed}"
    );
    let pending = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read retryable Store writes");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].status,
        coven_protocol::write::WriteStatus::Publishing
    );
    let rejected_blobs = reject_blob_create.rejected_blobs();
    assert_eq!(rejected_blobs.len(), 1);
    let prepared_blob = rejected_blobs[0].clone();
    let prepared = coven_database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write after blob failure")
        .expect("provider failure retains the exact prepared publication");
    assert_eq!(prepared.audiences.blobs.len(), 1);
    assert_eq!(prepared.audiences.blobs[0].blob(), &prepared_blob);
    assert!(
        cloud_storage
            .verify_blob_object(&prepared_blob)
            .await
            .is_err(),
        "the failed blob upload did not publish the blob"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("host blob retry cycle succeeds");
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the pending writes clear once the retry publishes"
    );
    let activated_blob = db
        .stored_blob_for_row("note_photos", "hponly")
        .await
        .expect("retry activates the exact row blob binding");
    assert_eq!(activated_blob, prepared_blob);
    cloud_storage
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry uploads and reads back the exact host-provided blob");
}

#[tokio::test]
async fn each_host_write_publishes_the_blob_facts_from_its_own_commit() {
    let keypair = UserKeypair::generate();
    let blob_decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db =
        crate::sync::test_helpers::open_test_db_with_blob(db_store_dir.clone(), blob_decl.clone());
    let fixture = cycle_test_store_fixture(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (storage, cloud_storage) = fixture;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("bind package writer device");
    db.execute_test_host_write(&format!(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01'); \
         INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('photo', 'n1', 'cover', 5, '{}', 'blob-a', \
                 '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"first"),
    ))
    .await;
    db.execute_test_host_write(&format!(
        "UPDATE note_photos \
             SET blob_id = 'blob-b', size = 6, hash = '{}', \
                 _updated_at = '0000000002000-0000-M' \
             WHERE id = 'photo'",
        coven_protocol::blob::content_hash(b"second"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "blob-a",
        b"first",
    )
    .await
    .expect("store first write's blob");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "blob-b",
        b"second",
    )
    .await
    .expect("store second write's blob");

    let error = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::reject_ack_create(Arc::clone(
            &storage,
        ))),
        device,
    )
    .await
    .expect_err("acknowledgement create stops the cycle before package reclamation");
    assert!(
        error
            .to_string()
            .contains("unexpected Store acknowledgement create"),
        "unexpected post-package failure: {error}"
    );
    assert!(db.latest_store_snapshot_meta().await.is_some());

    let package_device = storage
        .bind_device_in(&db, db_store_dir.clone(), &keypair)
        .await
        .expect("bind package inspection Store");
    // Where the writes landed, from the write rows rather than the per-position
    // index: the cycle advanced this device's replay baseline over the snapshot
    // it acknowledged, which retires the positions that snapshot restates.
    let published = coven_database::StoreDatabase::new(&db)
        .published_write_commits()
        .await
        .expect("read where each host write landed");
    assert_eq!(published.len(), 2, "both host writes published");
    let mut published_blob_ids = Vec::new();
    for commit_ref in published {
        let package = package_device
            .load_store_package_for_test(&commit_ref)
            .await
            .expect("load exact Store package")
            .expect("commit has a package");
        let package = coven_protocol::audience_package::AudiencePackage::parse(&package.value)
            .expect("parse exact audience package");
        for binding in package.blob_bindings() {
            cloud_storage
                .verify_blob_object(binding.blob())
                .await
                .expect("committed blob object exists exactly");
        }
        published_blob_ids.push(
            package
                .blob_bindings()
                .iter()
                .map(|binding| binding.blob().locator().blob_id().to_string())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        published_blob_ids,
        vec![vec!["blob-a".to_string()], vec!["blob-b".to_string()]],
    );
}

#[tokio::test]
async fn captured_changeset_retry_recognizes_first_blob_uploaded_before_second_failed() {
    let keypair = UserKeypair::generate();
    let (db, db_store_dir, storage, cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('firstblob', 'n1', 'cover', 5, '{}', '0000000001000-0000-M', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('secondblob', 'n1', 'cover', 6, '{}', '0000000001001-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"first"),
        coven_protocol::blob::content_hash(b"second"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "firstblob",
        b"first",
    )
    .await
    .expect("store first host-provided blob");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "secondblob",
        b"second",
    )
    .await
    .expect("store second host-provided blob");

    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    let reject_second_blob = Arc::new(CycleStorageInterceptor::reject_blob_create_on(
        Arc::clone(&storage),
        2,
    ));
    let failed = match run_cycle_in_task(Arc::clone(&reject_second_blob), device.clone()).await {
        Ok(_) => panic!("second blob upload should fail before publish"),
        Err(error) => error,
    };
    assert!(
        failed.to_string().contains("unexpected blob create call 2"),
        "cycle surfaces the second blob upload failure: {failed}"
    );
    let attempted_blobs = reject_second_blob.rejected_blobs();
    assert_eq!(attempted_blobs.len(), 2);
    cloud_storage
        .verify_blob_object(&attempted_blobs[0])
        .await
        .expect("the first exact blob reached cloud before the second failed");
    assert!(cloud_storage
        .verify_blob_object(&attempted_blobs[1])
        .await
        .is_err());
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(
            &db_store_dir,
            "photos",
            "firstblob",
            5
        )
        .await
        .expect("read first local")
        .is_some(),
        "the first local copy remains because the changeset was not published"
    );

    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("two-blob retry cycle succeeds");
    let stream_id = db.local_store_stream_id().await;
    assert!(materialized_history_reaches(&db, &stream_id, 2).await);
    let activated_first = db
        .stored_blob_for_row("note_photos", "firstblob")
        .await
        .expect("retry activates the first exact blob binding");
    let activated_second = db
        .stored_blob_for_row("note_photos", "secondblob")
        .await
        .expect("retry activates the second exact blob binding");
    let attempted_first = attempted_blobs
        .iter()
        .find(|blob| blob.locator().blob_id() == "firstblob")
        .expect("first blob was attempted");
    let attempted_second = attempted_blobs
        .iter()
        .find(|blob| blob.locator().blob_id() == "secondblob")
        .expect("second blob was attempted");
    assert_eq!(&activated_first, attempted_first);
    assert_eq!(&activated_second, attempted_second);
    cloud_storage
        .verify_blob_object(&activated_first)
        .await
        .expect("the first exact blob remains readable after retry");
    cloud_storage
        .verify_blob_object(&activated_second)
        .await
        .expect("the second exact blob is readable after retry");
}

#[tokio::test]
async fn already_uploaded_host_blob_publishes_without_local_copy_or_reupload() {
    let keypair = UserKeypair::generate();
    let (db, db_store_dir, storage, cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("bind local Store device");
    let pass_through = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('remoteonly', 'n1', 'cover', 15, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"already durable"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "remoteonly",
        b"already durable",
    )
    .await
    .expect("store the first publication's host-provided blob");

    run_cycle_in_task(Arc::clone(&pass_through), device.clone())
        .await
        .expect("first host blob cycle succeeds");
    let stream_id = db.local_store_stream_id().await;
    assert!(materialized_history_reaches(&db, &stream_id, 1).await);
    let published_blob = db
        .row_blob_ref("note_photos", "remoteonly")
        .await
        .expect("read first exact remote blob binding")
        .stored()
        .cloned()
        .expect("first publication installs an exact remote blob binding");
    cloud_storage
        .verify_blob_object(&published_blob)
        .await
        .expect("read back the first exact remote blob object");
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(
            &db_store_dir,
            "photos",
            "remoteonly",
            15
        )
        .await
        .expect("read cache-lazy host blob after publication")
        .is_none(),
        "the first publication removes the cache-lazy local copy",
    );
    db.execute_test_host_write(
        "UPDATE note_photos \
         SET _updated_at = '0000000002000-0000-M' \
         WHERE id = 'remoteonly'",
    )
    .await;

    let reject_blob_create = Arc::new(CycleStorageInterceptor::reject_blob_create(Arc::clone(
        &storage,
    )));
    run_cycle_in_task(Arc::clone(&reject_blob_create), device)
        .await
        .expect("already-uploaded host blob cycle succeeds");
    assert!(materialized_history_reaches(&db, &stream_id, 2).await);
    assert!(reject_blob_create.rejected_blobs().is_empty());
    let republished_blob = db
        .row_blob_ref("note_photos", "remoteonly")
        .await
        .expect("read re-emitted exact remote blob binding")
        .stored()
        .cloned()
        .expect("re-emission retains an exact remote blob binding");
    assert_eq!(republished_blob, published_blob);
    cloud_storage
        .verify_blob_object(&republished_blob)
        .await
        .expect("read back the re-emitted exact remote blob object");
}

#[tokio::test]
async fn fresh_push_failure_keeps_cache_lazy_local_copy_until_retry_publishes() {
    let keypair = UserKeypair::generate();
    let (db, db_store_dir, storage, cloud_storage) =
        blob_cycle_store(&keypair, CacheFill::CacheLazy).await;
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("bind local Store device");
    let device_id = device.device_id().clone();
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('lazyblob', 'n1', 'cover', 4, '{}', '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"lazy"),
    ))
    .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &db_store_dir,
        "photos",
        "lazyblob",
        b"lazy",
    )
    .await
    .expect("store cache-lazy host-provided blob");
    let pending = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read pending Store write");
    let write_id = pending
        .iter()
        .find(|write| {
            write
                .affected_rows
                .iter()
                .any(|row| row.table == "note_photos" && row.primary_key == "lazyblob")
        })
        .expect("the blob host transaction has a durable Store write")
        .write_id
        .clone();

    fail_exact_create_on(&storage, 1);
    let error = run_cycle_in_task(Arc::clone(&cycle_storage), device.clone())
        .await
        .expect_err("the first Store package append fails");
    assert!(
        error.to_string().starts_with(
            "publish Store write: storage backend Transport failure while access cloud storage:"
        ),
        "cycle names the failed Store package publication: {error}",
    );
    assert!(
        error.contains("InMemoryCloudHome: forced failure before exact create call 1"),
        "cycle preserves the exact provider failure: {error}",
    );
    let prepared = coven_database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read outbound Store queue")
        .expect("the exact prepared Store write remains durable");
    assert_ne!(
        prepared.commit.value.write_id, write_id,
        "the failed predecessor remains prepared ahead of the blob write",
    );
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(
            &db_store_dir,
            "photos",
            "lazyblob",
            4
        )
        .await
        .expect("read lazy local")
        .is_some(),
        "the local copy remains until the changeset is published"
    );

    run_cycle_in_task(cycle_storage, device)
        .await
        .expect("prepared Store write retry succeeds");
    let status = coven_database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read retried Store write status");
    let commit = match status {
        coven_protocol::write::WriteStatus::Published(position) => match *position {
            coven_protocol::write::PublishedWrite::Commit(
                coven_protocol::write::PublishedPosition {
                    device_id: published_device,
                    commit,
                },
            ) if published_device == device_id => commit,
            position => panic!("retried Store write has wrong position: {position:?}"),
        },
        status => panic!("retried Store write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(
        materialized_history_reaches(&db, &stream_id, commit.coord.sequence()).await,
        "the retried Store write is materialized",
    );
    let device = storage
        .bind_device_in(&db, db_store_dir.clone(), &keypair)
        .await
        .expect("bind retried Store writer");
    // Read the commit by its published reference rather than out of the
    // per-position row: the cycle above adopted a snapshot covering it, which
    // retires that row.
    let published_commit = device
        .load_commit_for_test(&commit)
        .await
        .expect("load retried exact Store commit");
    assert_eq!(published_commit.value().write_id, write_id);
    assert!(
        published_commit.value().store_package().is_some(),
        "the blob Store write carries an exact Store package reference",
    );
    let activated_blob = db
        .stored_blob_for_row("note_photos", "lazyblob")
        .await
        .expect("retry activates the exact cache-lazy blob binding");
    cloud_storage
        .verify_blob_object(&activated_blob)
        .await
        .expect("retry leaves the exact cache-lazy blob readable");
    assert!(
        coven_foundation::store_dir::StoreDir::read_local_blob(
            &db_store_dir,
            "photos",
            "lazyblob",
            4
        )
        .await
        .expect("read lazy local after publish")
        .is_none(),
        "the local copy drops after the prepared write retry commits"
    );
}

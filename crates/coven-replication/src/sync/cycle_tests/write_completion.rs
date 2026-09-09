use super::*;

/// The main-push and post-pull paths stamp the acknowledgement with an RFC 3339
/// `last_sync`.
#[tokio::test]
async fn push_cycle_writes_rfc3339_ack_timestamp() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let keypair = UserKeypair::generate();
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read push-timestamp write")
        .into_iter()
        .next()
        .expect("push-timestamp write is pending")
        .write_id;

    let cycle_device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    cycle_device
        .run_cycle(None)
        .await
        .expect("run acknowledgement timestamp cycle");
    let published = match coven_database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read push-timestamp write status")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Store write has an exact commit")
            .clone(),
        status => panic!("push-timestamp write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(materialized_history_reaches(&db, &stream_id, published.coord.sequence()).await);
    storage
        .assert_latest_ack_timestamp_is_rfc3339(&db, db_store_dir.clone())
        .await;
}

#[tokio::test]
async fn completed_store_write_advances_the_shared_publication_record() {
    assert_completed_store_write_advances_the_shared_publication_record(false).await;
}

#[tokio::test]
async fn lost_conditional_response_settles_the_accepted_store_write() {
    assert_completed_store_write_advances_the_shared_publication_record(true).await;
}

async fn assert_completed_store_write_advances_the_shared_publication_record(
    lose_conditional_response: bool,
) {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let keypair = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, cloud_storage) =
        cycle_test_store_fixture(&db, db_store_dir.clone(), &keypair, home.clone()).await;
    let initial = StoreDatabase::new(&db)
        .store_current_publication()
        .await
        .expect("read initial Store publication record");
    assert!(initial.record().accepted().is_none());

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('publication-row', 'Publication row', NULL, 1, \
         '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read pending Store write")
        .into_iter()
        .next()
        .expect("host write is pending")
        .write_id;
    if lose_conditional_response {
        home.lose_next_conditional_replace_response();
    }
    storage
        .open_into(&db, db_store_dir)
        .await
        .expect("open exact test Store")
        .run_cycle(None)
        .await
        .expect("publish Store write");

    let observed = StoreDatabase::new(&db)
        .store_current_publication()
        .await
        .expect("read advanced Store publication record");
    let accepted = observed
        .record()
        .accepted()
        .expect("completed write advances shared publication");
    let published = match StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read completed Store write status")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Store write has an exact commit")
            .clone(),
        status => panic!("completed Store write is not published: {status:?}"),
    };
    assert_eq!(
        StoreDatabase::new(&db)
            .published_write_commits()
            .await
            .expect("read durable published Store writes"),
        vec![published]
    );
    let retained = StoreDatabase::new(&db)
        .store_publication_entries()
        .await
        .expect("read retained Store publication interval");
    let current_entry = retained
        .last()
        .expect("advanced Store publication retains its current entry");
    let retained_reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
        &current_entry.value,
        current_entry.prepared.reference().clone(),
    )
    .expect("reference retained Store publication entry");
    assert_eq!(&retained_reference, accepted);
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        storage.store_root_hash(),
        coven_protocol::objects::ProtocolObjectDomain::StoreCurrentPublication,
    );
    let slot = StoreDatabase::new(&db)
        .local_store_founder_graph()
        .await
        .expect("read Store founder graph")
        .expect("Store founder graph exists")
        .root
        .value
        .descriptor
        .current_publication_slot
        .clone();
    let (remote_bytes, remote_version) = cloud_storage
        .read_versioned_protocol_record(
            &context,
            &slot,
            coven_protocol::store_commit::store_current_publication_semantic_prefix(),
        )
        .await
        .expect("read remote Store publication record");
    assert_eq!(remote_bytes, observed.record().to_bytes());
    assert_eq!(
        &remote_version,
        observed
            .require_observed()
            .expect("published provider observation")
            .version()
    );
}

/// The prepared-write retry stamps the acknowledgement with an RFC 3339
/// `last_sync`.
#[tokio::test]
async fn prepared_write_retry_writes_rfc3339_ack_timestamp() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let keypair = UserKeypair::generate();
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("bind prepared-retry device");

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    // The first push fails at the package append, so the prepared write remains
    // owned by its durable record and no head is written for it yet.
    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device.clone(),
    )
    .await
    .expect_err("the first Store package append fails");
    assert!(
        coven_database::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
            .await
            .expect("read outbound Store queue")
            .is_some(),
        "the exact Store batch remains durable after append failure",
    );

    // The next cycle retries the prepared write.
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("retry prepared Store write");
    assert!(coven_database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read retried outbound Store queue")
        .is_none());
    storage
        .assert_latest_ack_timestamp_is_rfc3339(&db, db_store_dir.clone())
        .await;
}

#[tokio::test]
async fn missing_user_blob_blocks_prepared_write_before_publish() {
    let keypair = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let fixture = cycle_test_store_fixture(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let (storage, cloud_storage) = fixture;
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let planted = storage
        .create_exact_opaque_blob("audio", "audio1", b"AUDIO")
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Remote', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(&format!(
        "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('audio1', 'n1', 'audio', 5, '{}', \
                     '0000000001000-0000-M', '2026-01-01')",
        coven_protocol::blob::content_hash(b"AUDIO"),
    ))
    .await;

    fail_exact_create_on(&storage, 1);
    run_cycle_in_task(Arc::clone(&cycle_storage), device.clone())
        .await
        .expect_err("the first Store package append fails");
    let first_write_id = coven_database::StoreDatabase::new(&db)
        .oldest_prepared_store_write()
        .await
        .expect("read prepared Store write")
        .expect("the exact Store write remains after append failure")
        .commit
        .value
        .write_id
        .clone();
    assert!(
        !storage
            .local_store_package_exists(&db, db_store_dir.clone(), 2)
            .await
    );

    cloud_storage
        .delete_blob_object(&planted)
        .await
        .expect("delete exact user-provided blob");
    let retry = run_cycle_in_task(Arc::clone(&cycle_storage), device.clone()).await;
    let err = match retry {
        Err(err) => err,
        Ok(_) => panic!("prepared write must recheck the remote user-provided blob"),
    };

    assert!(
        err.to_string()
            .contains("prepare Store write: outbound blob audio/audio1 is absent from storage"),
        "prepared write surfaces the missing blob: {err}",
    );
    let first_write_status = coven_database::StoreDatabase::new(&db)
        .write_status(&first_write_id)
        .await
        .expect("read first write status");
    assert!(
        matches!(
            &first_write_status,
            coven_protocol::write::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    coven_protocol::write::PublishedWrite::Commit(coven_protocol::write::PublishedPosition { commit, .. })
                        if commit.coord.sequence() == 1
                )
        ),
        "first write status after blocking its successor: {first_write_status:?}",
    );
    let pending = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read pending writes");
    assert_eq!(pending.len(), 1);
    let blocked_write_id = pending[0].write_id.clone();
    let blocked = coven_protocol::write::WriteStatus::Blocked(
        coven_protocol::write::WriteBlock::MissingBlob {
            namespace: "audio".to_string(),
            id: "audio1".to_string(),
        },
    );
    assert_eq!(pending[0].status, blocked);
    assert!(
        !storage
            .local_store_package_exists(&db, db_store_dir.clone(), 2)
            .await,
        "the blocked write has no package or head",
    );

    let _restored = storage
        .create_exact_opaque_blob("audio", "audio1", b"AUDIO")
        .await;
    run_cycle_in_task(cycle_storage, device)
        .await
        .expect("restored missing user blob cycle succeeds");
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .write_status(&blocked_write_id)
            .await
            .expect("read blocked write status"),
        blocked,
        "a semantic block is not retried by reconnect",
    );
    assert!(
        !storage
            .local_store_package_exists(&db, db_store_dir.clone(), 2)
            .await
    );
}

#[tokio::test]
async fn outgoing_preparation_failure_keeps_pending_write_for_retry() {
    let keypair = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    storage
        .retain_store_packages_for_assertion(&db, db_store_dir.clone())
        .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('prepare-fail', 'Prepare Fail', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    let write_id = coven_database::StoreDatabase::new(&db)
        .pending_writes()
        .await
        .expect("read preparation-failure write")
        .into_iter()
        .next()
        .expect("preparation-failure write is pending")
        .write_id;

    db.install_outbound_preparation_failure_for_test()
        .await
        .expect("install Store preparation fault");
    let device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    let failed = run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device.clone(),
    )
    .await
    .expect_err("outgoing preparation should fail");
    assert!(
        failed.contains("injected Store preparation failure"),
        "cycle surfaces the outgoing preparation failure: {failed}"
    );
    assert_eq!(
        db.pending_write_count().await,
        1,
        "the pending write remains queued when outgoing preparation fails"
    );
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .write_status(&write_id)
            .await
            .expect("read failed-preparation write status"),
        coven_protocol::write::WriteStatus::Pending,
    );

    db.remove_outbound_preparation_failure_for_test()
        .await
        .expect("remove Store preparation fault");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("retry outgoing preparation");
    let published = match coven_database::StoreDatabase::new(&db)
        .write_status(&write_id)
        .await
        .expect("read retried preparation write status")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Store write has an exact commit")
            .clone(),
        status => panic!("retried preparation write is not published: {status:?}"),
    };
    let stream_id = db.local_store_stream_id().await;
    assert!(materialized_history_reaches(&db, &stream_id, published.coord.sequence()).await);
    assert_eq!(
        db.pending_write_count().await,
        0,
        "the pending write leaves the pending set after publication"
    );
}

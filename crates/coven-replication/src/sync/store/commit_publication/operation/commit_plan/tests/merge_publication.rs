use super::*;
use crate::sync::test_helpers::{open_test_db_with_blob, photo_decl};
use coven_database::{HostWriteOperation, StoreRowWrites, WriteBatch};

#[tokio::test]
async fn accepted_package_transfers_to_shared_live_set_ownership() {
    let fixture = PreparedWriteFixture::prepare().await;

    assert!(
        !fixture
            .remote_object_exists(&fixture.publication_object())
            .await,
        "publication entries belong to the active attempt, not the candidate graph",
    );
    let active = fixture
        .active_publication()
        .await
        .expect("prepared attempt is durable");
    assert_eq!(
        active.attempt().expect("prepared publication").entry_object,
        fixture.publication_object()
    );
    assert_eq!(
        active
            .attempt()
            .expect("prepared publication")
            .entry
            .payload,
        coven_protocol::store_commit::StorePublicationPayload::Commit(fixture.commit_ref()),
    );

    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("publish prepared Store package"),
        1,
    );

    let retained_input = fixture.retained_canonical_input().await;
    let retained_input: serde_json::Value =
        serde_json::from_slice(&retained_input).expect("parse retained local package application");
    assert_eq!(
        retained_input["activation"]["package_application"],
        serde_json::Value::String("locally_authored".to_string()),
    );

    let remote = fixture
        .stored_remote_object(&fixture.package_object())
        .await;
    assert!(matches!(
        remote,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                record.identity.domain,
                coven_protocol::remote_object::SharedLiveSetObjectDomain::StorePackage { .. }
            )
                && matches!(
                    &record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.is_empty()
                        && ownership.activated.contains(
                            &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(
                                fixture.commit_ref().clone()
                            )
                        )
                        && ownership.activated.iter().any(|owner| matches!(
                            owner,
                            coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(
                                coven_protocol::remote_object::RetainedReplayOwner::Commit {
                                    commit,
                                    ..
                                }
                            ) if commit == &fixture.commit_ref()
                        ))
                        && ownership.activated.len() == 2
                )
    ));
    let commit = fixture
        .stored_remote_object(&fixture.commit_ref().object)
        .await;
    assert!(matches!(
        commit,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                    reference
                } if reference == &fixture.commit_ref()
            ) && matches!(
                &record.state,
                coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                    ownership
                } if ownership.pending.is_empty()
                    && ownership.activated
                        == std::collections::BTreeSet::from([fixture.commit_ref().clone()])
            )
    ));
    assert!(fixture.active_publication().await.is_none());
    assert!(
        !fixture
            .remote_object_exists(&fixture.publication_object())
            .await
    );
    let publication = fixture.accepted_publication().await;
    assert_eq!(
        publication.value,
        active.attempt().expect("prepared publication").entry
    );
    assert_eq!(
        publication.prepared.reference(),
        &fixture.publication_object()
    );
    assert_eq!(
        publication.prepared.stored_bytes(),
        active
            .attempt()
            .expect("prepared publication")
            .entry
            .to_bytes()
    );
}

#[tokio::test]
async fn local_publication_extends_connection_owned_verified_history() {
    let fixture = PreparedWriteFixture::prepare().await;
    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("publish prepared Store package"),
        1,
    );

    fixture.corrupt_retained_input().await;
    let retained = fixture
        .retained_merge_replay_inputs()
        .await
        .expect("use the retained input verified by local publication");
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].commit_ref(), &fixture.commit_ref());
}

#[tokio::test]
async fn failures_before_package_commit_and_publication_keep_the_exact_prepared_write_retryable() {
    for failed_call in 1..=3 {
        let fixture = PreparedWriteFixture::prepare().await;
        fixture.fail_exact_create_before_call(failed_call);
        let first = fixture.drain_store_writes().await;
        assert!(first.is_err(), "exact create call {failed_call} fails");
        assert_eq!(
            fixture.write_status().await,
            coven_protocol::write::WriteStatus::Publishing,
            "transport failure retains the exact prepared write for retry",
        );
        assert!(
            fixture.prepared_write().await.commit.value.write_id == fixture.write_id(),
            "the exact prepared write remains after exact create call {failed_call}",
        );
        assert_eq!(
            fixture.exact_materialized_ref().await,
            None,
            "local position cannot advance before the shared publication is accepted",
        );
        assert_eq!(
            fixture.contains_exact_object(&fixture.package_object()),
            failed_call > 1,
        );
        assert_eq!(
            fixture.contains_exact_object(&fixture.commit_ref().object),
            failed_call > 2,
        );
        assert!(!fixture.contains_exact_object(&fixture.publication_object()),);

        assert_eq!(
            fixture
                .drain_store_writes()
                .await
                .expect("retry exact outbound batch"),
            1,
        );
        assert!(!fixture.prepared_write_exists().await);
        assert_eq!(
            fixture.exact_materialized_ref().await,
            Some(fixture.commit_ref().clone()),
        );
        assert!(matches!(
            fixture.write_status().await,
            coven_protocol::write::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    coven_protocol::write::PublishedWrite::Commit(coven_protocol::write::PublishedPosition { device_id, commit })
                        if device_id == &fixture.device_id()
                            && commit.coord.sequence() == 1
                            && commit.commit_hash == fixture.commit_ref().commit_hash
                )
        ));
    }
}

#[tokio::test]
async fn restart_fails_loud_when_a_prepared_write_has_no_usable_exact_root() {
    for invalid_root in [
        None,
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
    ] {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("store.sqlite3");
        let db_store_dir = crate::sync::test_helpers::store_dir_for_test_database(&path);
        let open = || {
            Database::open_synthetic_for_test(
                &path,
                db_store_dir.clone(),
                crate::sync::test_helpers::test_synced_tables(),
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                "dev-writer".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
            )
            .expect("open test database")
        };
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = Arc::new(CloudSyncConnection::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "prepared-root-status",
            keypair.clone(),
        ));
        let db = open();
        let device = crate::sync::test_helpers::TestDevice::create(
            &db,
            db_store_dir.clone(),
            storage.clone(),
            "prepared-root-status",
            keypair.clone(),
        )
        .await
        .expect("create prepared-root-status Store");
        db.execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('root-status', 'outbound', NULL, 1, \
                     '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        assert!(device
            .prepare_pending_store_write()
            .await
            .expect("prepare write"));
        let write_id = coven_database::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
            .await
            .expect("load prepared write")
            .expect("prepared write exists")
            .commit
            .value
            .write_id
            .clone();
        db.replace_store_root_hash_for_test(invalid_root.map(str::to_string))
            .await
            .expect("make root unusable");
        drop(device);
        drop(db);

        let reopened = open();
        let reopened_database = coven_database::StoreDatabase::new(&reopened);
        let result = match crate::sync::test_helpers::TestDevice::load(
            &reopened,
            db_store_dir.clone(),
            storage.clone(),
            keypair.clone(),
        )
        .await
        {
            Ok(device) => device.drain_store_writes().await,
            Err(error) => Err(error),
        };
        match (invalid_root, result) {
            (
                None,
                Err(StoreError::MissingState {
                    key: "store_root_authority",
                }),
            ) => {}
            (Some(_), Err(StoreError::Database(reason))) => {
                assert!(reason
                    .to_string()
                    .contains("Store root authority hash differs"));
            }
            (_, result) => panic!("unexpected Store root failure: {result:?}"),
        }
        assert!(matches!(
            reopened_database
                .write_status(&write_id)
                .await
                .expect("write status"),
            coven_protocol::write::WriteStatus::Publishing
        ));
    }
}

#[tokio::test]
async fn authorized_writer_retains_its_exact_root_without_reloading_durable_authority() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "blocked-retry",
        keypair.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let database = coven_database::StoreDatabase::new(&db);
    let device = crate::sync::test_helpers::TestDevice::create(
        &db,
        db_store_dir.clone(),
        storage.clone(),
        "blocked-retry",
        keypair.clone(),
    )
    .await
    .expect("create blocked-retry Store");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('blocked-first', 'first', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    let writes = database
        .pending_writes()
        .await
        .expect("load pending writes");
    let write_id = writes[0].write_id.clone();
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize writer before invalidating its durable root");
    db.remove_store_protocol_root_for_test().await;
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare with the root retained by the writer capability"));
    assert_eq!(
        database.write_status(&write_id).await.unwrap(),
        coven_protocol::write::WriteStatus::Publishing
    );
    drop(writer);
    crate::sync::test_helpers::TestDevice::load(
        &db,
        db_store_dir.clone(),
        storage.clone(),
        keypair.clone(),
    )
    .await
    .expect("same connection retains its verified Store authority");
}

#[tokio::test]
async fn discarding_a_blocked_write_atomically_reverses_its_unpublished_suffix() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "blocked-discard",
        keypair.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let database = coven_database::StoreDatabase::new(&db);
    let device = crate::sync::test_helpers::TestDevice::create(
        &db,
        db_store_dir.clone(),
        storage.clone(),
        "blocked-discard",
        keypair.clone(),
    )
    .await
    .expect("create blocked-discard Store");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('discard-first', 'first', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('discard-second', 'second', NULL, 1, \
                 '0000000001001-0000-writer', '2026-01-01')",
    )
    .await;
    let writes = database.pending_writes().await.unwrap();
    let first = writes[0].write_id.clone();
    let second = writes[1].write_id.clone();
    database
        .set_write_status(
            &first,
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "discard test precondition".to_string(),
                },
            ),
        )
        .await
        .expect("block the first unpublished write");

    assert_eq!(
        database.discard_blocked_write(&first).await.unwrap(),
        coven_database::BlockedWriteDiscard::Discarded(vec![first.clone(), second.clone()])
    );
    let note_count: i64 = database
        .read(|sql| sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(note_count, 0);
    assert!(database.pending_writes().await.unwrap().is_empty());
    for write_id in [first, second] {
        assert_eq!(
            database.write_status(&write_id).await.unwrap(),
            coven_protocol::write::WriteStatus::Resolved(
                coven_protocol::write::WriteResolution::Discarded
            )
        );
    }

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('after-discard', 'after', NULL, 1, \
                 '0000000001002-0000-writer', '2026-01-01')",
    )
    .await;
    assert!(device
        .prepare_pending_store_write()
        .await
        .expect("prepare write after discarded blocked writes"));
    assert_eq!(device.drain_store_writes().await.unwrap(), 1);
}

#[tokio::test]
async fn discarding_a_blocked_write_includes_later_local_only_writes() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "blocked-local-discard",
        keypair.clone(),
    ));
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let database = coven_database::StoreDatabase::new(&db);
    crate::sync::test_helpers::TestDevice::create(
        &db,
        db_store_dir,
        storage,
        "blocked-local-discard",
        keypair,
    )
    .await
    .expect("create blocked-local-discard Store");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('blocked', 'blocked', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
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
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('private', 'private', NULL, 0, \
                 '0000000001001-0000-writer', '2026-01-01')",
    )
    .await;

    let discarded = database
        .discard_blocked_write(&blocked)
        .await
        .expect("discard blocked suffix");
    let coven_database::BlockedWriteDiscard::Discarded(discarded) = discarded else {
        panic!("blocked suffix unexpectedly requires remote resolution");
    };
    assert_eq!(discarded.len(), 2);
    assert_eq!(discarded[0], blocked);
    assert_eq!(
        database
            .read(|sql| sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0)))
            .await
            .unwrap()
            .unwrap(),
        0,
    );
    for write_id in discarded {
        assert_eq!(
            database.write_status(&write_id).await.unwrap(),
            coven_protocol::write::WriteStatus::Resolved(
                coven_protocol::write::WriteResolution::Discarded
            ),
        );
    }
}

#[tokio::test]
async fn discarding_a_blocked_suffix_restores_its_retained_blob_and_reclaims_its_new_blob() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "blocked-blob-discard",
        keypair.clone(),
    ));
    let store_dir = crate::sync::test_helpers::test_store_dir();
    let db = open_test_db_with_blob(store_dir.clone(), photo_decl());
    let database = coven_database::StoreDatabase::new(&db);
    crate::sync::test_helpers::TestDevice::create(
        &db,
        store_dir.clone(),
        storage,
        "blocked-blob-discard",
        keypair,
    )
    .await
    .expect("create Store");

    let original_bytes = b"original private blob";
    let mut original_batch = WriteBatch::new();
    original_batch.put_blob("photos", "blob-original", original_bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(original_batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes VALUES \
                     ('private-row', 'Private', NULL, 0, \
                      '0000000001000-0000-writer', '2026-01-01'); \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('blob-original', 'private-row', 'image', {}, '{}', \
                      '0000000001000-0000-writer', '2026-01-01');",
                    original_bytes.len(),
                    coven_protocol::blob::content_hash(original_bytes),
                ))?;
                Ok::<_, coven_database::DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture original private blob");
    let original = database
        .row_blob_ref("note_photos", "blob-original")
        .await
        .expect("read original blob reference");

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('blocked-row', 'Blocked', NULL, 1, \
                 '0000000002000-0000-writer', '2026-01-01')",
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

    let replacement_bytes = b"replacement private blob";
    let mut replacement_batch = WriteBatch::new();
    replacement_batch.delete_blob(original.blob().clone());
    replacement_batch.put_blob("photos", "blob-replacement", replacement_bytes.to_vec());
    let replacement = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(replacement_batch, move |sql| {
                sql.execute_batch(&format!(
                    "DELETE FROM note_photos WHERE id = 'blob-original'; \
                     INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, _updated_at, created_at) VALUES \
                     ('blob-replacement', 'private-row', 'image', {}, '{}', \
                      '0000000003000-0000-writer', '2026-01-01');",
                    replacement_bytes.len(),
                    coven_protocol::blob::content_hash(replacement_bytes),
                ))?;
                Ok::<_, coven_database::DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("replace private blob after blocked write");
    let replacement_leases = database
        .write_blob_lease_count_for_test(&replacement.write_id)
        .await
        .expect("count replacement blob leases");
    assert!(replacement_leases > 0);

    let original_path = store_dir
        .local_blob_path("photos", "blob-original")
        .expect("original blob path");
    let replacement_path = store_dir
        .local_blob_path("photos", "blob-replacement")
        .expect("replacement blob path");
    db.execute_test_sql(
        "CREATE TEMP TRIGGER fail_blocked_suffix_reversal
         BEFORE DELETE ON notes
         WHEN OLD.id = 'blocked-row'
         BEGIN
             SELECT RAISE(ABORT, 'forced blocked suffix reversal failure');
         END;",
    )
    .await;

    let failed = database
        .discard_blocked_write(&blocked)
        .await
        .expect_err("later reversal must fail");
    assert!(failed.to_string().contains("reverse blocked-write suffix"));
    assert!(!database
        .read(|sql| sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM note_photos WHERE id = 'blob-original')",
            [],
            |row| row.get::<_, bool>(0),
        ))
        .await
        .expect("read original row after rollback")
        .expect("original row query after rollback"));
    assert!(database
        .row_blob_ref("note_photos", "blob-replacement")
        .await
        .is_ok());
    assert_eq!(
        db.query_test_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM local_cleanup_intents
             WHERE namespace = 'photos' AND blob_id = 'blob-original'
               AND copy_identity = 'local'",
        )
        .await,
        "1",
        "the failed reversal rolls back the temporarily removed original cleanup intent",
    );
    assert_eq!(
        db.query_test_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM local_cleanup_intents
             WHERE namespace = 'photos' AND blob_id = 'blob-replacement'
               AND copy_identity = 'local'",
        )
        .await,
        "0",
        "the failed reversal rolls back the replacement cleanup intent",
    );
    assert_eq!(
        database.write_status(&replacement.write_id).await.unwrap(),
        coven_protocol::write::WriteStatus::LocalOnly,
    );
    assert_eq!(
        database
            .write_blob_lease_count_for_test(&replacement.write_id)
            .await
            .expect("count replacement blob leases after rollback"),
        replacement_leases,
    );
    assert_eq!(
        std::fs::read(&original_path).expect("read retained original after rollback"),
        original_bytes,
    );
    assert_eq!(
        std::fs::read(&replacement_path).expect("read live replacement after rollback"),
        replacement_bytes,
    );
    db.execute_test_sql("DROP TRIGGER fail_blocked_suffix_reversal")
        .await;

    let discarded = database
        .discard_blocked_write(&blocked)
        .await
        .expect("discard blocked suffix");
    let coven_database::BlockedWriteDiscard::Discarded(discarded) = discarded else {
        panic!("blocked suffix unexpectedly requires remote resolution");
    };
    assert_eq!(discarded.len(), 2);
    assert_eq!(discarded[0], blocked);
    assert!(database
        .row_blob_ref("note_photos", "blob-original")
        .await
        .is_ok());
    assert!(!database
        .read(|sql| sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM note_photos WHERE id = 'blob-replacement')",
            [],
            |row| row.get::<_, bool>(0),
        ))
        .await
        .expect("read replacement row")
        .expect("replacement row query"));

    assert!(!coven_database::LocalBlobCleanup::new(&database)
        .drain()
        .await
        .expect("drain reversed suffix cleanup"));
    assert_eq!(
        std::fs::read(&original_path).expect("read restored original blob"),
        original_bytes,
    );
    assert!(!replacement_path.exists());
    assert!(!coven_database::LocalBlobCleanup::new(&database)
        .drain()
        .await
        .expect("repeat cleanup after restoration"));
    assert!(original_path.exists());
}

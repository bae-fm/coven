use crate::sync::test_helpers::*;
use coven_protocol::membership::{MembershipHeadAcceptance, MembershipHeadActivation};
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinRole, DeviceJoinRoleProgress, OwnerJoinProgress,
};

#[tokio::test]
async fn registration_authority_finalizes_after_a_lost_publication_response() {
    registration_authority_survives(RegistrationInterruption::LostResponse).await;
}

#[tokio::test]
async fn registration_authority_restarts_after_result_upload_failure() {
    registration_authority_survives(RegistrationInterruption::ResultUploadFailure).await;
}

#[tokio::test]
async fn registration_handoff_restarts_after_peer_compaction() {
    registration_authority_survives(RegistrationInterruption::PeerCompaction).await;
}

#[tokio::test]
async fn registration_handoff_keeps_its_accepted_endpoint_after_peer_publication() {
    registration_authority_survives(RegistrationInterruption::PeerPublication).await;
}

#[tokio::test]
async fn generic_join_transition_cannot_complete_a_reserved_publication() {
    registration_authority_survives(RegistrationInterruption::UnownedCompletion).await;
}

#[tokio::test]
async fn an_unconsumed_join_handoff_survives_peer_snapshot_retirement() {
    assert_pending_handoff_retention(false).await;
}

#[tokio::test]
async fn an_unconsumed_join_handoff_retains_its_deleted_blob_in_successor_images() {
    assert_pending_handoff_retention(true).await;
}

async fn assert_pending_handoff_retention(with_orphan_blob: bool) {
    use coven_storage::CloudSyncObjectStorage;
    let declaration = coven_protocol::synced_schema::BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::HostProvided,
        coven_protocol::blob::CacheFill::CacheLazy,
    )
    .with_id_column("blob_id");
    let directory = test_store_dir();
    let database = open_test_db_with_blob(directory.clone(), declaration.clone());
    let signer = user_keypair_from_seed([89; 32]);
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "unconsumed-registration-handoff",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_directory = test_store_dir();
    let peer_database = open_test_db_with_blob(peer_directory.clone(), declaration.clone());
    let peer = store
        .activate_joined_device(
            &database,
            directory.clone(),
            &peer_database,
            peer_directory,
            &signer,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate independent snapshot publisher");
    let owner = store
        .bind_device_in(&database, directory, &signer)
        .await
        .expect("bind provider administrator");
    let stored_blob = if with_orphan_blob {
        let bytes = b"lazy handoff photo";
        let mut batch = coven_database::WriteBatch::new();
        batch.put_blob("photos", "handoff-photo", bytes.to_vec());
        let size = bytes.len();
        let hash = coven_protocol::blob::content_hash(bytes);
        coven_database::StoreRowWrites::new(coven_database::StoreDatabase::new(&database))
            .execute(coven_database::HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO notes(id, title, shared, _updated_at, created_at)
                     VALUES ('handoff-note', 'Photo', 1, '0000000002000-0000-owner', '2026-07-20');
                     INSERT INTO note_photos(id, note_id, kind, blob_id, size, hash, _updated_at, created_at)
                     VALUES ('handoff-photo', 'handoff-note', 'image', 'handoff-photo', {size}, '{hash}',
                     '0000000002000-0000-owner', '2026-07-20');"
                ))?;
                Ok::<_, coven_database::DbError>(())
            }), None, None).await.expect("capture lazy photo");
        assert!(owner
            .publish_pending_store_database()
            .await
            .expect("accept lazy photo"));
        Some(
            coven_database::StoreDatabase::new(&database)
                .row_blob_ref("note_photos", "handoff-photo")
                .await
                .expect("accepted photo")
                .stored()
                .cloned()
                .expect("uploaded photo"),
        )
    } else {
        None
    };
    owner
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish joining snapshot");
    let pending_directory = tempfile::tempdir().expect("pending join directory");
    let pending = super::DeviceJoinJournalDatabase::open_for_test(
        pending_directory.path().join("join.sqlite"),
    )
    .expect("open pending join");
    let offer = owner
        .begin_device_join(&pubkey_hex(&signer))
        .await
        .expect("offer same-principal join");
    let mut joining = owner
        .open_pending_device_join_for_test(&pending, &signer, offer)
        .await
        .expect("open joining device");
    let access = joining
        .prepare_provider_access_request()
        .await
        .expect("request provider access");
    let approval = owner
        .authorize_device_provider_access(access, None)
        .await
        .expect("approve provider access");
    let request = joining
        .prepare_registration_request(approval)
        .await
        .expect("prepare registration");
    drop(joining);
    let joined = owner
        .activate_same_principal_join_for_test(request)
        .await
        .expect("complete the administrator's handoff journal");
    let bootstrap = &joined.installation;
    let protected = [
        bootstrap.authority.snapshot.object.clone(),
        bootstrap.authority.metadata.image.object.clone(),
        bootstrap
            .authority
            .metadata
            .membership_rollup
            .object
            .clone(),
        bootstrap
            .bootstrap
            .publication
            .current
            .latest_snapshot()
            .expect("handoff accepted snapshot")
            .publication
            .object
            .clone(),
    ];
    for object in &protected {
        assert!(home.contains_exact_object(object));
    }
    let (_, pulled) = peer.pull_store().await.expect("peer accepts registration");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    if with_orphan_blob {
        coven_database::StoreRowWrites::new(coven_database::StoreDatabase::new(&peer_database))
            .execute(
                coven_database::HostWriteOperation::new(coven_database::WriteBatch::new(), |sql| {
                    sql.execute("DELETE FROM note_photos WHERE id = 'handoff-photo'", [])?;
                    Ok::<_, coven_database::DbError>(())
                }),
                None,
                None,
            )
            .await
            .expect("capture later photo deletion");
        assert!(peer
            .publish_pending_store_database()
            .await
            .expect("accept photo deletion"));
    }
    peer.publish_snapshot_generation_for_test()
        .await
        .expect("peer compacts the accepted registration");
    let successor = coven_database::StoreDatabase::new(&peer_database)
        .latest_local_store_snapshot()
        .await
        .expect("read peer snapshot")
        .expect("peer snapshot exists");
    assert_ne!(successor.reference, bootstrap.authority.snapshot);
    assert!(successor
        .meta
        .coverage
        .covers_commit(&joined.activation.outcome_activation));
    if let Some(stored) = &stored_blob {
        let bytes = storage
            .read_protocol_object(
                &coven_protocol::objects::ProtocolObjectContext::store_encrypted(
                    store.root().store_root_hash,
                    coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotImage,
                ),
                &successor.meta.image.object,
                &coven_protocol::store_commit::semantic_prefix_from_exact_object(
                    &successor.meta.image.object,
                    ".db",
                )
                .expect("image prefix"),
            )
            .await
            .expect("read successor image");
        let image =
            coven_database::DatabaseImageTest::from_bytes(&bytes).expect("open successor image");
        let count: i64 = image
            .query_row("SELECT COUNT(*) FROM note_photos", [], |row| row.get(0))
            .expect("count accepted photo rows");
        assert_eq!(count, 0, "the new snapshot reflects the accepted deletion");
        let remote = image
            .remote_object(stored.object())
            .expect("pending old image retains the exact blob record");
        let required_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
            metadata_slot: bootstrap.authority.snapshot.object.slot().clone(),
        };
        assert!(
            remote
                .snapshot_owners()
                .any(|owner| owner == &required_owner),
            "the successor image must preserve the pending handoff's blob owner"
        );
    }
    let (_, pulled) = owner.pull_store().await.expect("adopt peer snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    owner
        .reclaim_packages()
        .await
        .expect("administrator executes accepted snapshot retirement");
    for object in &protected {
        assert!(
            home.contains_exact_object(object),
            "a completed admitting journal does not prove the joining device consumed {object:?}"
        );
    }

    let joining_directory = test_store_dir();
    let joining_storage: std::sync::Arc<dyn CloudSyncObjectStorage> = storage.clone();
    let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let progress: crate::sync::JoiningDeviceJoinProgressObserver = std::sync::Arc::new(|_| {});
    let cancel = tokio::sync::watch::channel(false).1;
    let device_id = joined
        .bootstrap
        .bootstrap
        .request
        .expected_registration()
        .device_id
        .to_string();
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(storage.as_ref(), &joined.installation.authority.store_root)
        .await
        .expect("pin the delayed joining root");
    let membership = history
        .load_accepted_membership_authority(
            &joined.installation.bootstrap.membership.0,
            Some(&pubkey_hex(&signer)),
        )
        .await
        .expect("verify the delayed joining registration");
    let prepared = crate::sync::store::PreparedDeviceJoinSnapshot::prepare(
        &joining_storage,
        (*joined.installation).clone(),
        &membership,
        database.schema_version(),
        &joining_directory.db_path(),
        &progress,
        &cancel,
    )
    .await
    .expect("download the protected original image after peer retirement");
    let installed = prepared
        .install(
            test_synced_tables_with_blob(declaration.clone()),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            device_id,
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &routing,
        )
        .expect("install the original handoff image");
    let completed = super::PendingDeviceJoinAuthority::prepare_same_principal_completion(
        &pending,
        &joining_storage,
        &joining_directory,
        &signer,
        joined,
        installed,
        "2026-07-20T00:02:00Z",
        Some(&routing),
        None,
    )
    .await
    .expect("prepare the delayed target completion")
    .complete()
    .await
    .expect("complete the target against its original accepted prefix");
    let target_database_owner = coven_database::StoreDatabase::from_database(
        coven_database::Database::open(
            &joining_directory.db_path(),
            test_synced_tables_with_blob(declaration),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            completed.registration.device_id.to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &test_migrations(),
        )
        .expect("open the completed joining database"),
    );
    if let Some(stored) = &stored_blob {
        let reference = target_database_owner
            .row_blob_ref("note_photos", "handoff-photo")
            .await
            .expect("the original image restores its lazy photo");
        assert_eq!(reference.stored(), Some(stored));
        let blobs = crate::sync::store::blob::RemoteStoreBlobAccess::new(
            crate::sync::store::blob::LocalStoreBlobAccess::new(
                target_database_owner.clone(),
                joining_directory.clone(),
                crate::sync::store::blob::StoreBlobCache::new(
                    target_database_owner.clone(),
                    joining_directory.clone(),
                ),
            ),
            crate::sync::store::blob::CurrentRemoteBlobSource::current(
                target_database_owner.clone(),
                storage.clone(),
            ),
        );
        assert!(!blobs
            .is_materialized(&reference)
            .await
            .expect("lazy cache state"));
        assert_eq!(
            blobs
                .read(&reference)
                .await
                .expect("read the retained original blob"),
            b"lazy handoff photo",
        );
    }
    let target = TestDevice::open_with_database(
        target_database_owner.clone(),
        joining_directory,
        storage.clone(),
        &store.root(),
        &signer,
    )
    .await
    .expect("open the completed joining device");
    coven_database::StoreRowWrites::new(target_database_owner)
        .execute(
            coven_database::HostWriteOperation::new(coven_database::WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "INSERT INTO notes(id, title, shared, _updated_at, created_at)
                     VALUES ('target-arrived', 'Target arrived', 1,
                             '0000000003000-0000-target', '2026-07-20')",
                )?;
                Ok::<_, coven_database::DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture target arrival");
    assert!(target
        .publish_pending_store_database()
        .await
        .expect("first target publication installs the current accepted baseline"));
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer observes target arrival");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer.publish_snapshot_generation_for_test()
        .await
        .expect("snapshot transfers ownership after target arrival");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("administrator observes arrival");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    owner
        .reclaim_packages()
        .await
        .expect("retire consumed handoff artifacts");
    for object in &protected {
        assert!(
            !home.contains_exact_object(object),
            "target publication releases the consumed handoff artifact {object:?}",
        );
    }
}

enum RegistrationInterruption {
    LostResponse,
    ResultUploadFailure,
    PeerCompaction,
    PeerPublication,
    UnownedCompletion,
}

async fn registration_authority_survives(interruption: RegistrationInterruption) {
    let restart = !matches!(interruption, RegistrationInterruption::LostResponse);
    let directory = test_store_dir();
    let host_device_id = "test-device".to_string();
    let db = coven_database::Database::open_synthetic_for_test(
        &directory.db_path(),
        directory.clone(),
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        host_device_id.clone(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &test_migrations(),
    )
    .expect("open durable admitting database");
    let signer = user_keypair_from_seed([87; 32]);
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &db,
        directory.clone(),
        "registration-authority-finalization",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let root = store.root();
    let peer = if matches!(
        interruption,
        RegistrationInterruption::PeerCompaction | RegistrationInterruption::PeerPublication
    ) {
        let peer_directory = test_store_dir();
        let peer_db = open_test_db(peer_directory.clone());
        let peer = store
            .activate_joined_device(
                &db,
                directory.clone(),
                &peer_db,
                peer_directory,
                &signer,
                "2026-07-20T00:00:00Z",
            )
            .await
            .expect("activate independent snapshot publisher");
        Some((peer, peer_db))
    } else {
        None
    };
    let owner = store
        .bind_device_in(&db, directory.clone(), &signer)
        .await
        .expect("bind owner");
    owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("prepare joining baseline");
    let pending_directory = tempfile::tempdir().expect("pending join directory");
    let pending = super::DeviceJoinJournalDatabase::open_for_test(
        pending_directory.path().join("join.sqlite"),
    )
    .expect("open pending join");
    let offer = owner
        .begin_device_join(&pubkey_hex(&signer))
        .await
        .expect("offer same-principal join");
    let mut joining = owner
        .open_pending_device_join_for_test(&pending, &signer, offer)
        .await
        .expect("open joiner");
    let access = joining
        .prepare_provider_access_request()
        .await
        .expect("request provider access");
    let approval = owner
        .authorize_device_provider_access(access, None)
        .await
        .expect("approve provider access");
    let request = joining
        .prepare_registration_request(approval)
        .await
        .expect("prepare registration");
    drop(joining);
    let attempt_id = request.approval().request.offer.attempt_id;
    let mut database = coven_database::StoreDatabase::new(&db);
    let previous = database
        .store_current_publication()
        .await
        .expect("accepted predecessor");
    let (accepted, release) = home.pause_next_conditional_replace();
    if !restart {
        home.lose_next_conditional_replace_response();
    }
    let mut publication = Box::pin(owner.activate_same_principal_join_for_test(request.clone()));
    tokio::select! {
        _ = accepted.notified() => {},
        result = &mut publication => panic!("join returned before acceptance: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("join never reached acceptance"),
    }
    let journal = database
        .load_device_join(attempt_id, DeviceJoinRole::Owner)
        .await
        .expect("read owner journal")
        .expect("owned join");
    let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(prepared)) =
        &*journal.progress
    else {
        panic!(
            "accepted join retains its prepared finalization: {:?}",
            journal.progress
        );
    };
    let authority = prepared
        .candidate
        .prepared_membership_publication()
        .expect("exact registration authority");
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot,
        commit,
    } = &authority.head.activation
    else {
        panic!("registration requires shared acceptance");
    };
    assert_eq!(commit, &prepared.candidate.reference);
    assert!(home.stored_exact_bytes(acceptance_slot).is_none());
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unmaterialized local predecessor"),
        previous
    );
    let active = database
        .active_store_publication()
        .await
        .expect("active reservation")
        .expect("join owns finalization");
    assert_eq!(
        active.attempt().expect("prepared attempt"),
        &prepared.candidate.publication
    );
    if matches!(interruption, RegistrationInterruption::UnownedCompletion) {
        let next = coven_protocol::store_commit::device_join_journal::DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                prepared
                    .accepted_progress(
                        attempt_id,
                        prepared.candidate.publication.replacement.clone(),
                    )
                    .expect("shape-valid completion"),
            )),
        };
        let result = database.advance_device_join(&journal, next).await;
        drop(publication);
        assert!(
            matches!(
                result,
                Err(coven_database::DeviceJoinJournalError::NonAdjacentJournalTransition)
            ),
            "generic journal transition bypassed publication completion: {result:?}"
        );
        assert_eq!(
            database
                .load_device_join(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("journal"),
            Some(journal.clone()),
        );
        assert_eq!(
            database
                .active_store_publication()
                .await
                .expect("reservation"),
            Some(active)
        );
        assert!(home.stored_exact_bytes(acceptance_slot).is_none());
        return;
    }
    if matches!(interruption, RegistrationInterruption::ResultUploadFailure) {
        home.fail_exact_create_before_call(1);
    }
    let result_pause = peer.as_ref().map(|_| home.pause_after_exact_create_call(1));
    release.notify_one();
    let joined = if restart {
        match result_pause {
            Some((uploaded, _resume)) => {
                tokio::select! {
                    _ = uploaded.notified() => {},
                    result = &mut publication => panic!("join ended before its result upload: {result:?}"),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("join result was not uploaded"),
                }
                assert!(home.stored_exact_bytes(acceptance_slot).is_some());
                let (peer, peer_db) = peer.as_ref().expect("independent publisher");
                let (_, pulled) = peer.pull_store().await.expect("peer accepts registration");
                assert!(pulled.held_positions.is_empty(), "{pulled:?}");
                if matches!(interruption, RegistrationInterruption::PeerCompaction) {
                    peer.publish_snapshot_generation_for_test()
                        .await
                        .expect("peer compacts the accepted registration");
                    assert!(coven_database::StoreDatabase::new(peer_db)
                        .installed_replay_baseline()
                        .await
                        .expect("peer replay baseline")
                        .coverage()
                        .covers_commit(&prepared.candidate.reference));
                } else {
                    peer_db.execute_test_host_write("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('join-handoff-peer', 'peer edit', 1, '0000000001000-0000-peer', '2026-07-20')").await;
                    let mut writer = peer.authorize_writer().await.expect("authorize peer");
                    assert!(writer
                        .prepare_pending_store_write()
                        .await
                        .expect("prepare peer edit"));
                    assert_eq!(
                        writer
                            .drain_store_writes()
                            .await
                            .expect("publish peer edit"),
                        1
                    );
                }
            }
            None => {
                let error = publication
                    .as_mut()
                    .await
                    .expect_err("result upload failure remains visible");
                assert!(
                    error
                        .to_string()
                        .contains("forced failure before exact create call 1"),
                    "{error}"
                );
            }
        }
        drop(publication);
        assert_eq!(
            database
                .load_device_join(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("retained journal"),
            Some(journal.clone())
        );
        assert_eq!(
            database
                .active_store_publication()
                .await
                .expect("retained reservation"),
            Some(active.clone())
        );
        if peer.is_none() {
            let error = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &root)
                .await
                .expect("open cold authority")
                .load_accepted_anchored_membership(&[], Some(&pubkey_hex(&signer)))
                .await
                .expect_err("missing registration result blocks compact authority");
            assert!(
                matches!(
                    error,
                    crate::sync::store::AnchoredChainError::IncompleteFinalization { .. }
                ),
                "{error}"
            );
        }
        drop(owner);
        drop(store);
        drop(database);
        drop(db);
        let reopened_db = coven_database::Database::open_synthetic_for_test(
            &directory.db_path(),
            directory.clone(),
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            host_device_id,
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("reopen durable database without prior caches");
        database = coven_database::StoreDatabase::new(&reopened_db);
        assert_eq!(
            database
                .load_device_join(attempt_id, DeviceJoinRole::Owner)
                .await
                .expect("reopened exact journal"),
            Some(journal.clone())
        );
        assert_eq!(
            database
                .active_store_publication()
                .await
                .expect("reopened exact reservation"),
            Some(active)
        );
        let reopened = TestDevice::open_with_database(
            database.clone(),
            directory,
            storage.clone(),
            &root,
            &signer,
        )
        .await
        .expect("reopen admitting device");
        if peer.is_some() {
            let (_, pulled) = reopened
                .pull_store()
                .await
                .expect("install peer compaction before resuming the handoff");
            assert!(pulled.held_positions.is_empty(), "{pulled:?}");
            if matches!(interruption, RegistrationInterruption::PeerCompaction) {
                assert!(database
                    .installed_replay_baseline()
                    .await
                    .expect("reopened replay baseline")
                    .coverage()
                    .covers_commit(&prepared.candidate.reference));
            }
        }
        reopened
            .activate_same_principal_join_for_test(request.clone())
            .await
            .expect("finish exact accepted registration")
    } else {
        publication
            .await
            .expect("settle lost response and finish registration")
    };
    assert_eq!(
        joined.activation.outcome_activation,
        prepared.candidate.reference
    );
    assert_eq!(
        joined.installation.bootstrap.publication.current.accepted(),
        Some(
            &prepared
                .candidate
                .publication
                .reference()
                .expect("original Join winner")
        ),
        "the handoff must end at its exact accepted registration"
    );
    assert!(database
        .active_store_publication()
        .await
        .expect("released reservation")
        .is_none());
    let completed = database
        .load_device_join(attempt_id, DeviceJoinRole::Owner)
        .await
        .expect("completed journal")
        .expect("join handoff retained");
    assert!(matches!(
        &*completed.progress,
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalCompleted { .. })
    ));
    let bytes = home
        .stored_exact_bytes(acceptance_slot)
        .expect("postacceptance authority result exists");
    let result: MembershipHeadAcceptance =
        coven_protocol::objects::decode_protocol_object(&bytes).expect("decode exact result");
    let author = database
        .activated_store_device_registration_with_authority(
            &root,
            prepared.candidate.commit.author_registration.clone(),
        )
        .await
        .expect("active admitting registration");
    result
        .verify_for(
            root.store_root_hash,
            &authority.head_ref,
            &authority.head,
            author.value(),
        )
        .expect("verify exact admitting signature");
    assert_eq!(
        result.publication().expect("accepted result publication"),
        &prepared
            .candidate
            .publication
            .reference()
            .expect("exact winning publication")
    );
    assert_eq!(
        result.accepted_current,
        joined.installation.bootstrap.publication.current
    );
    let entries = database
        .store_publication_entries()
        .await
        .expect("accepted entries");
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.value.payload
                == coven_protocol::store_commit::StorePublicationPayload::Commit(
                    prepared.candidate.reference.clone()
                ))
            .count(),
        usize::from(!matches!(
            interruption,
            RegistrationInterruption::PeerCompaction
        ))
    );
    let membership = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open cold accepted authority")
        .load_accepted_anchored_membership(&[], Some(&pubkey_hex(&signer)))
        .await
        .expect("finalized registration authority is discoverable");
    assert!(membership.head_refs().contains(&authority.head_ref));
}

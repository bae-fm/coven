use super::*;

#[tokio::test]
async fn two_serial_writes_publish_as_one_branch_with_one_head_cas() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "serial-outbound",
        keypair.clone(),
    )
    .expect("in-memory home supports immutable copies")
    .with_test_serial_coordination(Arc::new(home.clone()));
    let db = open_serial_test_db();
    let (root, device_id) =
        initialize_exact_store(&db, &storage, "serial-outbound", &keypair).await;
    let store_root_hash = root.store_root_hash;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-a', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-b', 'second', NULL, 1, '0000000001001-0000-writer', '2026-01-01')",
    )
    .await;
    let pending = db.pending_writes().await.expect("pending Serial writes");
    assert_eq!(pending.len(), 2);
    let (_temp, store_dir) = temp_store_dir();
    let head_mutations_before = home.head_mutation_count();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &device_id,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare one Serial branch"));

    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .expect("activate one Serial branch"),
        2,
    );
    assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
    let first = db
        .exact_materialized_ref(SERIAL_STREAM_ID, 1)
        .await
        .unwrap()
        .expect("first Serial commit");
    let second = db
        .exact_materialized_ref(SERIAL_STREAM_ID, 2)
        .await
        .unwrap()
        .expect("second Serial commit");
    assert!(matches!(
        db.write_status(&pending[0].write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                crate::PublishedPosition::Serial { commit }
                    if commit.coord.sequence() == 1
                        && commit.commit_hash == first.commit_hash
            )
    ));
    assert!(matches!(
        db.write_status(&pending[1].write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                crate::PublishedPosition::Serial { commit }
                    if commit.coord.sequence() == 2
                        && commit.commit_hash == second.commit_hash
            )
    ));
    let head = storage
        .serial_coordination()
        .unwrap()
        .read_head(serial_head_key())
        .await
        .expect("read activated Serial head");
    let head = parse_serial_head(&db, store_root_hash, &head.bytes).await;
    assert!(matches!(
        head.state,
        StoreSerialHeadState::Commit { commit, .. }
            if commit == second && commit.commit_hash == second.commit_hash
    ));
}

pub(super) async fn serial_fixture(
    name: &str,
) -> (
    InMemoryCloudHome,
    CloudSyncStorage,
    Database,
    UserKeypair,
    StoreRootRef,
    Vec<crate::PendingWrite>,
) {
    let name = name.to_string();
    tokio::spawn(async move {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            &name,
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies")
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_serial_test_db();
        let (root, _) = initialize_exact_store(&db, &storage, &name, &keypair).await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-a', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-b', 'second', NULL, 1, '0000000001001-0000-writer', '2026-01-01')",
        )
        .await;
        let pending = db.pending_writes().await.unwrap();
        (home, storage, db, keypair, root, pending)
    })
    .await
    .expect("Serial Store write fixture task")
}

pub(super) async fn competing_head(
    db: &Database,
    storage: &CloudSyncStorage,
    signer: &UserKeypair,
    marker: &str,
) -> StoreSerialHead {
    let authorization = current_serial_authorization(
        db,
        storage,
        storage.serial_coordination().expect("Serial coordination"),
    )
    .await
    .expect("load competing Serial authorization");
    let member = UserKeypair::generate();
    let root = db
        .local_store_root_ref()
        .await
        .expect("read competing Store root")
        .expect("competing Store root exists");
    let member_pubkey = crate::keys::public_key_hex(&member);
    let wrapped = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
        storage,
        root.store_root_hash,
        &member_pubkey,
        crate::sync::wrapped_store_key::WrappedStoreKey::signed(
            &root.store_root_id.to_string(),
            &member_pubkey,
            authorization.key_generation,
            b"competing Serial membership wrap".to_vec(),
            signer,
        ),
    )
    .await
    .expect("prepare competing membership wrap");
    let entry = authorization
        .membership
        .signed_set_member_with_wrapped_key(
            signer,
            member_pubkey,
            None,
            crate::sync::membership::MemberRole::Member,
            wrapped.reference.clone(),
            marker.to_string(),
        )
        .expect("sign competing membership control");
    let plan = prepare_store_operation_commit(
        db,
        storage,
        StoreOperationPreparation::Serial {
            coordination: storage.serial_coordination().expect("Serial coordination"),
        },
        &local_device_id(db).await,
        signer,
    )
    .await
    .expect("prepare competing Serial operation");
    let prepared = prepare_store_operation_candidate(
        db,
        storage,
        plan,
        StoreOperationBatch::Control(StoreControl::SerialMembership { entry }),
    )
    .await
    .expect("prepare exact competing Serial control");
    storage
        .create_protocol_object(&wrapped.object)
        .await
        .expect("publish exact competing membership wrap");
    storage
        .create_protocol_object(&prepared.prepared)
        .await
        .expect("publish exact competing Serial commit");
    prepared
        .serial_publication_for_test()
        .expect("competing candidate is Serial")
        .1
        .clone()
}

fn serial_commit_ref(head: &StoreSerialHead) -> Option<&StoreBatchCommitRef> {
    match &head.state {
        StoreSerialHeadState::Genesis { .. } => None,
        StoreSerialHeadState::Commit { commit, .. } => Some(commit),
    }
}

#[tokio::test]
async fn changed_serial_base_marks_the_whole_branch_conflict_before_uploading_candidates() {
    let (home, storage, db, keypair, _root, pending) = serial_fixture("serial-changed-base").await;
    let other = competing_head(&db, &storage, &keypair, "changed-base").await;
    let coordination = storage.serial_coordination().expect("Serial coordination");
    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read founder Serial head");
    let head_mutations_before = home.head_mutation_count();
    coordination
        .replace_head(serial_head_key(), &current.version, &other.to_bytes())
        .await
        .expect("replace founder Serial head");
    home.fail_exact_create_before_call(1);
    let (_temp, store_dir) = temp_store_dir();

    assert!(!prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("detect changed Serial base"));

    assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
    for write in pending {
        let status = db.write_status(&write.write_id).await.unwrap();
        assert!(matches!(
            status,
            crate::WriteStatus::Conflict(ref conflict)
                if matches!(
                    conflict.as_ref(),
                    crate::SerializationConflict {
                        base: StoreSerialPredecessor::Genesis { .. },
                        current: StoreSerialPredecessor::Commit(current),
                        ..
                    } if Some(current) == serial_commit_ref(&other)
                )
        ));
    }
}

#[tokio::test]
async fn lost_successful_serial_head_response_completes_from_the_exact_authoritative_tip() {
    let (home, storage, db, keypair, _root, pending) = serial_fixture("serial-lost-success").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .unwrap());
    let head_mutations_before = home.head_mutation_count();
    home.fail_next_head_mutation_after_visibility();

    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .expect("recognize exact tip after lost response"),
        2,
    );
    assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(position.as_ref(), crate::PublishedPosition::Serial { .. })
        ));
    }
}

#[tokio::test]
async fn serial_candidate_abandonment_persists_and_wins_the_branch_base() {
    let (_home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-candidate-abandonment").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial branch"));
    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    assert!(prepare_serial_candidate_abandonment(
        &db,
        &storage,
        &local_device_id(&db).await,
        &keypair,
        branch_id.clone(),
    )
    .await
    .expect("persist exact Serial abandonment"));
    let durable = db
        .prepared_serial_candidate_abandonment()
        .await
        .expect("reload Serial abandonment")
        .expect("Serial abandonment exists");
    assert_eq!(durable.branch_id, branch_id);
    assert_eq!(durable.authority.value.write_id, pending[0].write_id);
    assert!(matches!(
        durable.authority.value.body,
        super::super::super::store_commit::StoreCommitBody::AbandonCandidates { .. }
    ));

    let outcome = abandon_serial_branch(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir,
        branch_id,
    )
    .await
    .expect("activate and apply Serial abandonment");
    assert_eq!(outcome, SerialBranchAbandonment::Discarded);
    assert!(db
        .prepared_serial_candidate_abandonment()
        .await
        .expect("read completed abandonment")
        .is_none());
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        ));
    }
}

#[tokio::test]
async fn original_serial_branch_activation_wins_abandonment_race() {
    let (_home, storage, db, keypair, root, pending) =
        serial_fixture("serial-abandonment-original-wins").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial branch"));
    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    assert!(prepare_serial_candidate_abandonment(
        &db,
        &storage,
        &local_device_id(&db).await,
        &keypair,
        branch_id.clone(),
    )
    .await
    .expect("persist Serial abandonment"));
    let branch = db
        .prepared_serial_store_branch()
        .await
        .expect("load original Serial branch")
        .expect("original Serial branch exists");
    for write in &branch.writes {
        publish_prepared_remote_objects(
            &db,
            &storage,
            &write.commit.value.write_id,
            root.store_root_hash,
        )
        .await
        .expect("publish original candidate objects");
        storage
            .create_protocol_object(&write.commit.prepared)
            .await
            .expect("publish original candidate commit");
        let reference = StoreBatchCommitRef::from_commit(
            &write.commit.value,
            StoreCommitCoord::Serial {
                sequence: write.commit.value.seq(),
            },
            write.commit.object.clone(),
        )
        .expect("reference original candidate");
        db.mark_candidate_commit_uploaded(reference)
            .await
            .expect("record original candidate upload");
    }
    let coordination = storage.serial_coordination().unwrap();
    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read Serial base head");
    coordination
        .replace_head(serial_head_key(), &current.version, &branch.head.bytes)
        .await
        .expect("activate original Serial branch");

    assert_eq!(
        abandon_serial_branch(
            &db,
            &storage,
            coordination,
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id,
        )
        .await
        .expect("settle original Serial winner"),
        SerialBranchAbandonment::OriginalBranchActivated,
    );
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Published(_)
        ));
    }
    assert!(db
        .prepared_serial_candidate_abandonment()
        .await
        .expect("read completed abandonment")
        .is_none());
}

#[tokio::test]
async fn third_serial_successor_discards_both_losing_candidate_families() {
    let (_home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-abandonment-third-wins").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial branch"));
    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    assert!(prepare_serial_candidate_abandonment(
        &db,
        &storage,
        &local_device_id(&db).await,
        &keypair,
        branch_id.clone(),
    )
    .await
    .expect("persist Serial abandonment"));
    competing_head(&db, &storage, &keypair, "third-winner").await;

    assert_eq!(
        abandon_serial_branch(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id,
        )
        .await
        .expect("settle third Serial winner"),
        SerialBranchAbandonment::Discarded,
    );
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        ));
    }
    assert!(db
        .prepared_serial_candidate_abandonment()
        .await
        .expect("read completed abandonment")
        .is_none());
}

#[tokio::test]
async fn serial_abandonment_retries_commit_publication_and_candidate_cleanup() {
    for failure in ["commit", "cleanup"] {
        let (home, storage, db, keypair, root, pending) =
            serial_fixture(&format!("serial-abandonment-retry-{failure}")).await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_serial_store_write(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            &local_device_id(&db).await,
            &keypair,
            &store_dir
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        if failure == "cleanup" {
            let branch = db
                .prepared_serial_store_branch()
                .await
                .expect("load Serial branch")
                .expect("Serial branch exists");
            let first = branch.writes.first().expect("branch has a first candidate");
            publish_prepared_remote_objects(
                &db,
                &storage,
                &first.commit.value.write_id,
                root.store_root_hash,
            )
            .await
            .expect("publish first candidate objects");
            storage
                .create_protocol_object(&first.commit.prepared)
                .await
                .expect("publish first candidate commit");
            let reference = StoreBatchCommitRef::from_commit(
                &first.commit.value,
                StoreCommitCoord::Serial {
                    sequence: first.commit.value.seq(),
                },
                first.commit.object.clone(),
            )
            .expect("reference first candidate");
            db.mark_candidate_commit_uploaded(reference)
                .await
                .expect("record first candidate upload");
            home.fail_exact_delete_on_call(1);
        } else {
            home.fail_exact_create_before_call(1);
        }
        assert!(abandon_serial_branch(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id.clone(),
        )
        .await
        .is_err());
        assert_eq!(
            db.serial_branch_discard_state(&branch_id)
                .await
                .expect("read resumable Serial discard"),
            crate::database::SerialBranchDiscardState::Abandonment,
        );
        assert_eq!(
            abandon_serial_branch(
                &db,
                &storage,
                storage.serial_coordination().unwrap(),
                &local_device_id(&db).await,
                &keypair,
                &store_dir,
                branch_id,
            )
            .await
            .expect("resume Serial abandonment"),
            SerialBranchAbandonment::Discarded,
        );
    }
}

#[tokio::test]
async fn serial_abandonment_resumes_candidate_cleanup_after_database_reopen() {
    let database_temp = tempfile::tempdir().expect("temporary database directory");
    let database_path = database_temp.path().join("serial-abandonment.db");
    let tables = test_synced_tables();
    let migrations = test_migrations();
    let (db, _) = Database::open(
        &database_path,
        tables.clone(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::Serial,
        "test-device".to_string(),
        &migrations,
    )
    .expect("open file-backed Serial database");
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "serial-abandonment-reopen",
        keypair.clone(),
    )
    .expect("create Serial storage")
    .with_test_serial_coordination(Arc::new(home.clone()));
    let (root, _) =
        initialize_exact_store(&db, &storage, "serial-abandonment-reopen", &keypair).await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-reopen', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    let (_store_temp, store_dir) = temp_store_dir();
    let pending = db
        .pending_writes()
        .await
        .expect("read pending Serial write");
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().expect("Serial coordination"),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial branch"));
    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    let branch = db
        .prepared_serial_store_branch()
        .await
        .expect("load prepared Serial branch")
        .expect("prepared Serial branch exists");
    let first = branch.writes.first().expect("branch has a first candidate");
    publish_prepared_remote_objects(
        &db,
        &storage,
        &first.commit.value.write_id,
        root.store_root_hash,
    )
    .await
    .expect("publish first candidate objects");
    storage
        .create_protocol_object(&first.commit.prepared)
        .await
        .expect("publish first candidate commit");
    let reference = StoreBatchCommitRef::from_commit(
        &first.commit.value,
        StoreCommitCoord::Serial {
            sequence: first.commit.value.seq(),
        },
        first.commit.object.clone(),
    )
    .expect("reference first candidate");
    db.mark_candidate_commit_uploaded(reference)
        .await
        .expect("record first candidate upload");
    home.fail_exact_delete_on_call(1);
    assert!(abandon_serial_branch(
        &db,
        &storage,
        storage.serial_coordination().expect("Serial coordination"),
        &local_device_id(&db).await,
        &keypair,
        &store_dir,
        branch_id.clone(),
    )
    .await
    .is_err());
    drop(db);

    let (reopened, _) = Database::open(
        &database_path,
        tables,
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        crate::WritePolicy::Serial,
        "test-device".to_string(),
        &migrations,
    )
    .expect("reopen Serial database");
    assert_eq!(
        abandon_serial_branch(
            &reopened,
            &storage,
            storage.serial_coordination().expect("Serial coordination"),
            &local_device_id(&reopened).await,
            &keypair,
            &store_dir,
            branch_id,
        )
        .await
        .expect("resume Serial abandonment after reopen"),
        SerialBranchAbandonment::Discarded,
    );
    assert!(reopened
        .prepared_serial_candidate_abandonment()
        .await
        .expect("read completed abandonment")
        .is_none());
}

#[tokio::test]
async fn serial_abandonment_settles_a_lost_success_response_by_head_readback() {
    let (home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-abandonment-lost-response").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial branch"));
    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    home.fail_next_head_mutation_after_visibility();

    assert_eq!(
        abandon_serial_branch(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id,
        )
        .await
        .expect("settle lost Serial abandonment response"),
        SerialBranchAbandonment::Discarded,
    );
}

#[tokio::test]
async fn different_tip_after_ambiguous_serial_response_conflicts_the_whole_branch() {
    let (home, storage, db, keypair, root, pending) = serial_fixture("serial-lost-to-other").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .unwrap());
    let losing_commit_keys = db
        .prepared_serial_store_branch()
        .await
        .unwrap()
        .expect("prepared losing branch")
        .writes
        .into_iter()
        .map(|write| write.commit.object.slot().logical_key().to_string())
        .collect::<Vec<_>>();
    let other = competing_head(&db, &storage, &keypair, "other-winner").await;
    let head_mutations_before = home.head_mutation_count();
    home.replace_after_next_head_mutation(other.to_bytes());

    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .expect("record competing authoritative tip"),
        0,
    );
    assert_eq!(home.head_mutation_count(), head_mutations_before + 2);
    for write in &pending {
        let status = db.write_status(&write.write_id).await.unwrap();
        assert!(matches!(
            status,
            crate::WriteStatus::Conflict(ref conflict)
                if matches!(
                    conflict.as_ref(),
                    crate::SerializationConflict {
                        current: StoreSerialPredecessor::Commit(current),
                        ..
                    } if Some(current) == serial_commit_ref(&other)
                )
        ));
    }
    let retained_prepared: i64 = db
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM store_writes WHERE prepared IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(retained_prepared, 2);

    let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    let resolution = super::super::super::store_pull::prepare_serial_resolution(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        root.store_root_hash,
        &store_dir,
        None,
        &keypair,
    )
    .await
    .expect("prepare accepted successor resolution");
    home.fail_exact_delete_on_call(2);
    let interrupted = super::super::super::store_pull::cleanup_serial_candidates(
        &db,
        &storage,
        branch_id.clone(),
        &resolution,
    )
    .await;
    assert!(interrupted.is_err());
    db.discard_pending_serial_branch(branch_id.clone(), resolution)
        .await
        .expect_err("incomplete candidate cleanup must block resolution");

    let coordination = storage.serial_coordination().unwrap();
    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read accepted Serial head");
    coordination
        .replace_head(serial_head_key(), &current.version, &current.bytes)
        .await
        .expect("refresh accepted Serial head version");

    let resolution = super::super::super::store_pull::prepare_serial_resolution(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        root.store_root_hash,
        &store_dir,
        None,
        &keypair,
    )
    .await
    .expect("prepare retry resolution");
    super::super::super::store_pull::cleanup_serial_candidates(
        &db,
        &storage,
        branch_id.clone(),
        &resolution,
    )
    .await
    .expect("resume losing candidate cleanup");
    db.discard_pending_serial_branch(branch_id, resolution)
        .await
        .expect("discard cleaned branch");
    let deletes = home.deletes_seen();
    assert_eq!(
        &deletes[deletes.len() - losing_commit_keys.len()..],
        losing_commit_keys
    );
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        ));
    }
}

#[tokio::test]
async fn losing_serial_membership_wrap_becomes_protocol_inert_and_only_commit_is_deleted() {
    let (home, storage, db, keypair, root, _pending) =
        serial_fixture("serial-membership-wrap-loss").await;
    let coordination = storage.serial_coordination().expect("Serial coordination");
    let authorization = current_serial_authorization(&db, &storage, coordination)
        .await
        .expect("load Serial membership predecessor");
    let member = UserKeypair::generate();
    let member_pubkey = crate::keys::public_key_hex(&member);
    let wrapped = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
        &storage,
        root.store_root_hash,
        &member_pubkey,
        crate::sync::wrapped_store_key::WrappedStoreKey::signed(
            &root.store_root_id.to_string(),
            &member_pubkey,
            authorization.key_generation,
            b"losing Serial membership wrap".to_vec(),
            &keypair,
        ),
    )
    .await
    .expect("prepare losing membership wrap");
    let entry = authorization
        .membership
        .signed_set_member_with_wrapped_key(
            &keypair,
            member_pubkey,
            None,
            crate::sync::membership::MemberRole::Member,
            wrapped.reference.clone(),
            "losing-membership-control".to_string(),
        )
        .expect("sign losing membership control");
    let plan = prepare_store_operation_commit(
        &db,
        &storage,
        StoreOperationPreparation::Serial { coordination },
        &local_device_id(&db).await,
        &keypair,
    )
    .await
    .expect("prepare losing membership operation");
    let candidate = prepare_store_operation_candidate(
        &db,
        &storage,
        plan,
        StoreOperationBatch::Control(StoreControl::SerialMembership { entry }),
    )
    .await
    .expect("prepare losing membership candidate");
    let remotes = candidate
        .membership_control_remote_objects(std::slice::from_ref(&wrapped))
        .expect("close losing membership ownership");
    let intent_hash = db
        .stage_membership_candidate_mutation(
            serde_json::to_vec(&candidate).expect("serialize losing candidate"),
            b"candidate_pending".to_vec(),
            remotes,
            None,
        )
        .await
        .expect("atomically stage losing membership ownership");
    crate::sync::membership_ops::publish_serial_membership_wraps(
        &db,
        &storage,
        &root,
        &candidate,
        std::slice::from_ref(&wrapped),
    )
    .await
    .expect("publish authenticated losing membership wrap");
    let competing = competing_head(&db, &storage, &keypair, "membership-wrap-winner").await;
    home.replace_after_next_head_mutation(competing.to_bytes());

    let StoreOperationPublicationOutcome::NonactivatedCandidate {
        candidate: returned,
        nonactivation,
    } = publish_prepared_store_operation(
        &db,
        &storage,
        StoreOperationPublicationMode::Serial { coordination },
        Box::new(candidate.clone()),
    )
    .await
    .expect("verify losing membership candidate")
    else {
        panic!("competing Serial head must nonactivate the membership candidate")
    };
    assert_eq!(*returned, candidate);
    let cleanup = db
        .begin_membership_candidate_nonactivation(
            intent_hash,
            candidate.reference.clone(),
            vec![candidate.reference.object.clone()],
            vec![wrapped.reference.object.clone()],
            b"candidate_nonactivating".to_vec(),
            *nonactivation,
        )
        .await
        .expect("terminalize losing membership candidate");
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].object, candidate.reference.object);
    assert!(db
        .protocol_inert_object(wrapped.reference.object.clone())
        .await
        .expect("read protocol-inert membership wrap")
        .is_some());
    crate::sync::store_objects::delete_exact_object(&storage, &cleanup[0].object)
        .await
        .expect("delete only the losing membership commit");
    db.mark_candidate_cleanup_absent(cleanup[0].object.clone())
        .await
        .expect("record exact losing commit absence");
    db.complete_nonactivating_membership_candidate_mutation(
        intent_hash,
        candidate.reference.clone(),
        vec![candidate.reference.object.clone()],
        vec![wrapped.reference.object.clone()],
        None,
    )
    .await
    .expect("complete losing membership ownership");
    crate::sync::wrapped_store_key::load_wrapped_store_key(
        &storage,
        root.store_root_hash,
        &wrapped.reference,
    )
    .await
    .expect("protocol-inert wrap remains present but has no authority");
}

#[tokio::test]
async fn serial_control_rejects_an_exact_wrap_signed_for_another_store() {
    let (_home, storage, db, keypair, root, _pending) =
        serial_fixture("serial-membership-wrong-store-wrap").await;
    let authorization = current_serial_authorization(
        &db,
        &storage,
        storage.serial_coordination().expect("Serial coordination"),
    )
    .await
    .expect("load Serial membership predecessor");
    let member = UserKeypair::generate();
    let member_pubkey = crate::keys::public_key_hex(&member);
    let wrapped = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
        &storage,
        root.store_root_hash,
        &member_pubkey,
        crate::sync::wrapped_store_key::WrappedStoreKey::signed(
            "another-store",
            &member_pubkey,
            authorization.key_generation,
            b"wrong Store binding".to_vec(),
            &keypair,
        ),
    )
    .await
    .expect("prepare exact wrong-Store wrap");
    storage
        .create_protocol_object(&wrapped.object)
        .await
        .expect("publish exact wrong-Store wrap");
    let entry = authorization
        .membership
        .signed_set_member_with_wrapped_key(
            &keypair,
            member_pubkey,
            None,
            crate::sync::membership::MemberRole::Member,
            wrapped.reference,
            "wrong-store-membership-control".to_string(),
        )
        .expect("sign control naming wrong-Store wrap");
    let control = StoreControl::SerialMembership { entry };

    crate::sync::store_pull::validate_serial_control_wrapped_keys(&storage, &root, Some(&control))
        .await
        .expect_err("exact membership wrap must authenticate its Store binding");
}

#[tokio::test]
async fn serial_preparation_transport_failure_returns_the_reserved_branch_to_pending() {
    let (_home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-preparation-retry").await;
    let coordination = FailFirstCoordinationRead {
        inner: storage.serial_coordination().unwrap(),
        failed: AtomicBool::new(false),
    };
    let (_temp, store_dir) = temp_store_dir();

    let result = prepare_serial_store_write(
        &db,
        &storage,
        &coordination,
        &local_device_id(&db).await,
        &keypair,
        &store_dir,
    )
    .await;

    assert!(matches!(result, Err(StoreOutboundError::Coordination(_))));
    for write in pending {
        assert_eq!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Pending
        );
    }
}

#[tokio::test]
async fn serial_preparation_protocol_failure_blocks_the_reserved_branch() {
    let (_home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-preparation-blocked").await;
    remove_exact_store_root(&db).await;
    let (_temp, store_dir) = temp_store_dir();

    let result = prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir,
    )
    .await;

    assert!(matches!(
        result,
        Err(StoreOutboundError::MissingState { .. })
    ));
    for write in pending {
        assert!(matches!(
            db.write_status(&write.write_id).await.unwrap(),
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { .. })
        ));
    }
}

#[tokio::test]
async fn write_arriving_during_serial_publication_rebases_after_activation() {
    let (_home, storage, db, keypair, _root, _pending) =
        serial_fixture("serial-publishing-success-suffix").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .unwrap());
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-race-c', 'third', NULL, 1, '0000000001002-0000-writer', '2026-01-01')",
    )
    .await;
    let suffix = db.pending_writes().await.unwrap().pop().unwrap().write_id;

    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .unwrap(),
        2
    );
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare rebased suffix"));
    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        db.write_status(&suffix).await.unwrap(),
        crate::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                crate::PublishedPosition::Serial { commit }
                    if commit.coord.sequence() == 3
            )
    ));
}

#[tokio::test]
async fn write_arriving_during_serial_publication_conflicts_with_the_branch_on_cas_loss() {
    let (home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-publishing-lost-suffix").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .unwrap());
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('serial-race-c', 'third', NULL, 1, '0000000001002-0000-writer', '2026-01-01')",
    )
    .await;
    let all_writes = db.pending_writes().await.unwrap();
    let other = competing_head(&db, &storage, &keypair, "suffix-lost").await;
    home.replace_after_next_head_mutation(other.to_bytes());

    assert_eq!(
        drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap(),)
            .await
            .unwrap(),
        0
    );
    let expected_branch = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
    assert_eq!(all_writes.len(), 3);
    for write in all_writes {
        let status = db.write_status(&write.write_id).await.unwrap();
        assert!(matches!(
            status,
            crate::WriteStatus::Conflict(ref conflict)
                if conflict.branch_id == expected_branch
        ));
    }
}

#[tokio::test]
async fn missing_serial_head_fails_when_a_materialized_position_exists() {
    let (home, storage, db, keypair, _root, _pending) =
        serial_fixture("serial-missing-head-after-materialization").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .unwrap());
    drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap())
        .await
        .unwrap();
    home.remove(serial_head_key());

    assert!(matches!(
        current_serial_authorization(&db, &storage, storage.serial_coordination().unwrap()).await,
        Err(StoreOutboundError::MissingState {
            key: SERIAL_COORDINATION_HEAD
        })
    ));
    assert!(matches!(
        current_serial_head_ref(&db, storage.serial_coordination().unwrap()).await,
        Err(StoreOutboundError::MissingState {
            key: SERIAL_COORDINATION_HEAD
        })
    ));
}

#[tokio::test]
async fn missing_serial_genesis_head_fails_before_the_first_commit() {
    let (home, storage, db, _keypair, _root, _pending) =
        serial_fixture("serial-missing-genesis-head").await;
    home.remove(serial_head_key());

    assert!(matches!(
        current_serial_authorization(&db, &storage, storage.serial_coordination().unwrap()).await,
        Err(StoreOutboundError::MissingState {
            key: SERIAL_COORDINATION_HEAD
        })
    ));
    assert!(matches!(
        current_serial_head_ref(&db, storage.serial_coordination().unwrap()).await,
        Err(StoreOutboundError::MissingState {
            key: SERIAL_COORDINATION_HEAD
        })
    ));
}

#[tokio::test]
async fn missing_serial_head_during_activation_names_the_coordination_head() {
    let (home, storage, db, keypair, _root, pending) =
        serial_fixture("serial-missing-head-during-activation").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().unwrap(),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare exact Serial write"));
    home.remove(serial_head_key());

    let error = drain_serial_store_writes(&db, &storage, storage.serial_coordination().unwrap())
        .await
        .expect_err("an absent coordination head blocks activation");
    assert!(matches!(
        error,
        StoreOutboundError::MissingState {
            key: SERIAL_COORDINATION_HEAD
        }
    ));
    for write in pending {
        let status = db.write_status(&write.write_id).await.unwrap();
        assert!(
            matches!(status, crate::WriteStatus::Publishing),
            "unexpected missing-head status: {status:?}"
        );
    }
}

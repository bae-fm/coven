use super::*;

fn open_circle_routing_test_db() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![crate::migration::Migration::sql(
            1,
            "Circle routing schema",
            "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn merge_publication_handles_every_exact_create_failure_boundary() {
    tokio::spawn(async {
        for after_visible_write in [false, true] {
            let mut call = 1;
            loop {
                let db = open_test_db();
                let name = format!(
                    "circle-replay-{}-{call}",
                    if after_visible_write {
                        "after"
                    } else {
                        "before"
                    }
                );
                let (store, signer, expected) = persist_merge_operation(&db, &name).await;
                if call > expected.operation().prepared_objects.len() {
                    break;
                }
                assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
                assert!(StoreDatabase::new(&db)
                    .get_circles(&keys::public_key_hex(&signer))
                    .await
                    .expect("read active circles")
                    .is_empty());
                assert_eq!(
                    StoreDatabase::new(&db)
                        .get_circle_operations()
                        .await
                        .expect("read pending circle operations"),
                    vec![crate::sync::circle::CircleOperationInfo {
                        operation_id: expected.operation_id.clone(),
                        circle_id: expected.circle_id(),
                        kind: crate::sync::circle::CircleOperationKind::Create,
                        state: crate::sync::circle::CircleOperationState::Pending,
                    }]
                );
                if after_visible_write {
                    store.home.fail_exact_create_after_call(call);
                } else {
                    store.home.fail_exact_create_before_call(call);
                }

                let first = resume_circle_operations(&db, &store.storage, &signer).await;
                if after_visible_write {
                    first.expect("lost exact-create response is settled by exact readback");
                } else {
                    let error =
                        first.expect_err("failure before exact create interrupts activation");
                    assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
                    let persisted = StoreDatabase::new(&db)
                        .circle_operation(&expected.operation_id)
                        .await
                        .expect("read interrupted operation")
                        .expect("interrupted operation remains durable");
                    assert_exact_operation(&expected, &persisted);
                    assert_eq!(persisted.state(), CircleOperationState::Pending);
                    assert_eq!(activation_count(&db, expected.circle_id()).await, 0);

                    resume_circle_operations(&db, &store.storage, &signer)
                        .await
                        .expect("resume exact circle operation");
                }
                assert!(StoreDatabase::new(&db)
                    .circle_operation(&expected.operation_id)
                    .await
                    .expect("read completed operation")
                    .is_none());
                assert_eq!(activation_count(&db, expected.circle_id()).await, 1);
                assert_eq!(
                    StoreDatabase::new(&db)
                        .get_circles(&keys::public_key_hex(&signer))
                        .await
                        .expect("read activated circle"),
                    vec![crate::sync::circle::CircleInfo {
                        id: expected.circle_id(),
                        name: "Household".to_string(),
                        role: crate::sync::circle::CircleRole::Owner,
                    }]
                );
                assert!(StoreDatabase::new(&db)
                    .get_circle_operations()
                    .await
                    .expect("read completed circle operations")
                    .is_empty());
                call += 1;
            }
        }
    })
    .await
    .expect("Circle publication task completes");
}

#[tokio::test]
async fn pending_circle_operation_reopens_with_identical_signed_state() {
    let temp = tempfile::tempdir().expect("create database directory");
    let path = temp.path().join("circle-restart.sqlite3");
    let (db, _stamper) = Database::open(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "creator".to_string(),
        &test_migrations(),
    )
    .expect("open circle database");
    let (store, signer, expected) = persist_merge_operation(&db, "circle-restart").await;
    assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
    std::thread::spawn(move || drop(db))
        .join()
        .expect("close circle database");

    let (reopened, _stamper) = Database::open(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "creator".to_string(),
        &test_migrations(),
    )
    .expect("reopen circle database");
    assert_eq!(
        StoreDatabase::new(&reopened)
            .get_circle_operations()
            .await
            .expect("list reopened Circle operations")
            .into_iter()
            .map(|operation| operation.operation_id)
            .collect::<Vec<_>>(),
        vec![expected.operation_id.clone()]
    );
    let persisted = StoreDatabase::new(&reopened)
        .circle_operation(&expected.operation_id)
        .await
        .expect("read reopened circle operation")
        .expect("circle operation survives restart");
    assert_exact_operation(&expected, &persisted);
    assert_eq!(persisted.state(), CircleOperationState::Pending);

    resume_circle_operations(&reopened, &store.storage, &signer)
        .await
        .expect("resume reopened circle operation");
    assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
}

#[tokio::test]
async fn interrupted_rename_reopens_and_resumes_the_same_signed_transition() {
    let temp = tempfile::tempdir().expect("create database directory");
    let path = temp.path().join("circle-rename-restart.sqlite3");
    let (db, _stamper) = Database::open(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "creator".to_string(),
        &test_migrations(),
    )
    .expect("open circle database");
    let (store, signer, founder) = persist_merge_operation(&db, "circle-rename-restart").await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");
    let device_id = local_device_id(&db).await;

    store.home.fail_exact_create_before_call(1);
    let error = rename_circle(
        &db,
        &store.storage,
        &device_id,
        "0000000002000-0000-creator",
        circle_id,
        "Household money",
        &signer,
    )
    .await
    .expect_err("failed exact create interrupts rename publication");
    assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
    let operation_id = StoreDatabase::new(&db)
        .get_circle_operations()
        .await
        .expect("list interrupted rename")
        .into_iter()
        .find(|operation| operation.circle_id == circle_id)
        .expect("interrupted rename is listed")
        .operation_id;
    let expected = StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read interrupted rename")
        .expect("interrupted rename remains durable");
    assert_eq!(expected.kind(), CircleOperationKind::Rename);
    assert_eq!(expected.state(), CircleOperationState::Pending);
    assert_eq!(activation_count(&db, circle_id).await, 1);
    assert_eq!(
        expected.operation().creation.epoch_id,
        founder.operation().creation.epoch_id
    );
    assert_eq!(
        expected.operation().creation.keyring,
        founder.operation().creation.keyring
    );
    std::thread::spawn(move || drop(db))
        .join()
        .expect("close circle database");

    let (reopened, _stamper) = Database::open(
        &path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "creator".to_string(),
        &test_migrations(),
    )
    .expect("reopen circle database");
    let persisted = StoreDatabase::new(&reopened)
        .circle_operation(&operation_id)
        .await
        .expect("read reopened rename")
        .expect("rename survives restart");
    assert_exact_operation(&expected, &persisted);

    resume_circle_operations(&reopened, &store.storage, &signer)
        .await
        .expect("resume reopened rename");
    assert_eq!(activation_count(&reopened, circle_id).await, 2);
    assert_eq!(
        StoreDatabase::new(&reopened)
            .get_circles(&keys::public_key_hex(&signer))
            .await
            .expect("read renamed circle"),
        vec![crate::sync::circle::CircleInfo {
            id: circle_id,
            name: "Household money".to_string(),
            role: crate::sync::circle::CircleRole::Owner,
        }]
    );
    assert!(StoreDatabase::new(&reopened)
        .get_circle_operations()
        .await
        .expect("read completed rename operations")
        .is_empty());
}

#[tokio::test]
async fn member_addition_activates_a_recipient_bound_bootstrap_image() {
    let blob_decl = crate::sync::session::BlobDecl::new(
        "files",
        crate::blob::Provenance::HostProvided,
        crate::blob::CacheFill::CacheEager,
    );
    let db = crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")
        .carries_blob(blob_decl)],
        vec![crate::migration::Migration::sql(
            1,
            "Circle member bootstrap schema",
            "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 size INTEGER NOT NULL,
                 hash TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    );
    let (store, signer, founder) = persist_merge_operation(&db, "circle-member-bootstrap").await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");
    let (_temp, store_dir) = temp_store_dir();
    let blob_id = "00000000-0000-4000-8000-000000000001";
    let blob_bytes = b"Circle bootstrap attachment";
    let insert = format!(
        "INSERT INTO documents (id, audience, size, hash, _updated_at)
         VALUES ('{blob_id}', '{circle_id}', {}, '{}', '0000000001500-0000-owner')",
        blob_bytes.len(),
        crate::blob::content_hash(blob_bytes),
    );
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    let routing = EncryptionService::from_key([42; 32]);
    db.call(move |connection| {
        StoreDatabase::run_internal_store_write_transaction_on(
            connection,
            &tables,
            Some(&routing),
            write_id,
            |transaction| transaction.execute_batch(&insert).map_err(DbError::from),
        )
    })
    .await
    .expect("capture scoped Circle blob row");
    crate::blob::local_files::store(&store_dir, "files", blob_id, blob_bytes)
        .await
        .expect("stage Circle blob");
    let writer = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        "circle-member-bootstrap",
        signer.clone(),
    )
    .expect("construct Circle blob writer");
    let components = crate::sync::cycle::init_sync_over_storage(
        &StoreDatabase::new(&db),
        writer,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("open scoped Circle Store");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish Circle row and blob");
    let owner_pubkey = keys::public_key_hex(&signer);

    components
        .add_circle_member(
            &store_dir,
            circle_id,
            owner_pubkey.clone(),
            CircleRole::Owner,
        )
        .await
        .expect("activate Circle member successor");

    let (current, _) = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &owner_pubkey)
        .await
        .expect("load successor Circle state");
    let CircleAccessDisposition::Active {
        bootstrap: Some(bootstrap),
        ..
    } = current.access.disposition
    else {
        panic!("successor access must carry its bootstrap image");
    };
    assert_eq!(bootstrap.schema_version, db.schema_version());
    assert_eq!(bootstrap.sync_routing_hash, db.sync_routing_hash());
    let [blob] = bootstrap.blobs.as_slice() else {
        panic!("Circle bootstrap must pin its one exact blob");
    };
    assert_eq!(
        blob.locator().audience(),
        crate::blob::locator::RemoteAudience::Circle(circle_id)
    );
    let blob = blob.clone();
    assert_eq!(activation_count(&db, circle_id).await, 2);
    assert!(StoreDatabase::new(&db)
        .get_circle_operations()
        .await
        .expect("list completed Circle operations")
        .is_empty());
    let object_id = crate::sync::remote_object::remote_object_id(&bootstrap.image.object);
    let record = db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("load bootstrap image ownership");
    let record: crate::sync::remote_object::RemoteObjectRecord =
        serde_json::from_str(&record).expect("parse bootstrap image ownership");
    assert!(matches!(
        record,
        crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(ref shared)
            if matches!(
                shared.identity.domain,
                crate::sync::remote_object::SharedLiveSetObjectDomain::CircleBootstrapImage { .. }
            )
    ));
    let blob_object_id = crate::sync::remote_object::remote_object_id(blob.object());
    let blob_record = db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [blob_object_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read bootstrap blob ownership");
    let blob_record: crate::sync::remote_object::RemoteObjectRecord =
        serde_json::from_str(&blob_record).expect("parse bootstrap blob ownership");
    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(blob_record) = blob_record
    else {
        panic!("Circle bootstrap blob must remain shared");
    };
    let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        blob_record.state
    else {
        panic!("Circle bootstrap blob must remain verified");
    };
    assert_eq!(
        ownership
            .activated
            .iter()
            .filter(|owner| {
                matches!(
                    owner,
                    crate::sync::remote_object::SharedObjectOwner::StoreCommit(_)
                )
            })
            .count(),
        2,
        "row publication and Circle bootstrap must both own the blob"
    );
}

#[tokio::test]
async fn member_removal_finalizes_an_exact_epoch_close_after_verified_responses() {
    let db = open_circle_routing_test_db();
    let (store, signer, founder) = persist_merge_operation(&db, "circle-member-removal").await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");

    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("circle-removal-owner".to_string()),
        &member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle removal Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let remaining_member = UserKeypair::generate();
    let remaining_member_pubkey = keys::public_key_hex(&remaining_member);
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("circle-removal-second-member".to_string()),
        &remaining_member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle removal Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite remaining Store member");
    let member_db = open_circle_routing_test_db();
    install_active_device_fixture(&store, &db, &member_db, &member, "2026-07-23T00:00:00Z")
        .await
        .expect("activate Store member device");

    let (_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = crate::sync::cycle::init_sync_over_storage(
        &StoreDatabase::new(&db),
        owner_storage,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("initialize Circle owner sync");
    components
        .add_circle_member(
            &store_dir,
            circle_id,
            member_pubkey.clone(),
            CircleRole::Member,
        )
        .await
        .expect("add Circle member");
    components
        .add_circle_member(
            &store_dir,
            circle_id,
            remaining_member_pubkey.clone(),
            CircleRole::Member,
        )
        .await
        .expect("add remaining Circle member");
    let prior = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load pre-close Circle control")
        .0;
    let prior_control = prior.control.coord.clone();
    let prior_epoch = prior.control.value.epoch_id();
    let prior_fingerprint = prior.control.value.key_fingerprint();
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    let captured_write_id = write_id.clone();
    let routing = EncryptionService::from_key([42; 32]);
    db.call(move |connection| {
        StoreDatabase::run_internal_store_write_transaction_on(
            connection,
            &tables,
            Some(&routing),
            captured_write_id,
            |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            "00000000-0000-4000-8000-000000000001",
                            circle_id.to_string(),
                            "0000000002500-0000-owner",
                        ],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            },
        )
    })
    .await
    .expect("capture pre-close Circle row");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish pre-close Circle package");
    let package_commit_ref = match db
        .write_status(&write_id)
        .await
        .expect("load pre-close Circle write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("pre-close Circle write was not published: {status:?}"),
    };
    let (package_commit, package_author) = crate::sync::store::pull::load_commit_with_author(
        &store.storage,
        &store.root,
        &package_commit_ref,
    )
    .await
    .expect("load pre-close Circle package commit");
    assert_eq!(package_commit.circle_packages().len(), 1);

    let operation_id = components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("activate Circle epoch close");

    let operation = StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read Circle removal operation")
        .expect("Circle removal waits for close responses");
    assert_eq!(operation.kind(), CircleOperationKind::RemoveMember);
    assert_eq!(
        operation.state(),
        CircleOperationState::WaitingForCloseResponses
    );
    assert_eq!(activation_count(&db, circle_id).await, 4);
    assert!(StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .is_err());
    let historical_access = StoreDatabase::new(&db)
        .circle_package_access(circle_id, prior_control)
        .await
        .expect("load historical pre-close access")
        .expect("historical pre-close access remains retained");
    assert!(historical_access
        .writers
        .contains(&keys::public_key_hex(&signer)));
    let loaded_packages = match crate::sync::store::pull::load_applicable_circle_packages(
        &StoreDatabase::new(&db),
        &store.storage,
        &package_commit_ref,
        &package_commit,
        &[],
        &package_author,
    )
    .await
    {
        Ok(packages) => packages,
        Err(crate::sync::store::pull::PullCircleActivationError::Database(error)) => {
            panic!("load late pre-close Circle package from retained access: {error}")
        }
        Err(crate::sync::store::pull::PullCircleActivationError::Invalid(error)) => {
            panic!("load late pre-close Circle package from retained access: {error}")
        }
    };
    assert_eq!(loaded_packages.len(), 1);

    let controls = StoreDatabase::new(&db)
        .closing_circle_controls()
        .await
        .expect("load closing Circle controls");
    let [control] = controls.as_slice() else {
        panic!("member removal must leave one closing Circle");
    };
    let crate::sync::circle::CircleControlState::EpochClose(close) = control.value.state() else {
        panic!("closing Circle must contain an epoch close");
    };
    let [participant] = close.participants.as_slice() else {
        panic!("removed member must leave the owner device as the sole participant");
    };
    let prefix = crate::sync::circle::circle_epoch_close_response_semantic_prefix(
        circle_id,
        close.close_id,
        participant.registration.device_id,
    );
    let response_context = ProtocolObjectContext::store_encrypted(
        store.root.store_root_hash,
        ProtocolObjectDomain::CircleEpochCloseResponse,
    );
    let registration = StoreDatabase::new(&db)
        .activated_store_device_registration(participant.registration.clone())
        .await
        .expect("load response author registration");
    let authorized_store = crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
        .await
        .expect("authorize Circle close response");
    authorized_store
        .publish_circle_epoch_close_responses(&signer)
        .await
        .expect("publish local Circle epoch-close response");
    let (bytes, response_object) = store
        .storage
        .read_protocol_slot(&response_context, &participant.response_slot, &prefix)
        .await
        .expect("read exact Circle epoch-close response");
    let response =
        crate::sync::circle::CircleEpochCloseResponse::parse_for(&bytes, control, &registration)
            .expect("verify signed Circle epoch-close response");
    let response_ref =
        crate::sync::circle::CircleEpochCloseResponseRef::from_response(&response, response_object)
            .expect("bind exact Circle epoch-close response");
    let response_storage_key = match participant.response_slot.physical() {
        crate::storage::cloud::PhysicalObjectLocator::LogicalKey => {
            participant.response_slot.logical_key().to_string()
        }
        crate::storage::cloud::PhysicalObjectLocator::Opaque(provider_id) => format!(
            "{}#exact#{provider_id}",
            participant.response_slot.logical_key()
        ),
    };
    let correct_stored = store
        .home
        .get(&response_storage_key)
        .expect("read stored Circle epoch-close response fixture");
    let malformed = store
        .storage
        .prepare_protocol_object(
            &response_context,
            participant.response_slot.clone(),
            &prefix,
            b"{}".to_vec(),
        )
        .expect("seal malformed response fixture");
    store.home.replace_exact_object(
        &participant.response_slot,
        malformed.stored_bytes().to_vec(),
    );
    let error = authorized_store
        .finalize_ready_circle_epoch_closes(
            &participant.registration.device_id.to_string(),
            "2026-07-23T00:00:01Z",
            &store_dir,
            &EncryptionService::from_key([42; 32]),
            &signer,
        )
        .await
        .expect_err("malformed occupied response must prevent finalization");
    assert!(
        error.to_string().contains("Circle epoch-close response"),
        "{error}"
    );
    store
        .home
        .replace_exact_object(&participant.response_slot, correct_stored);

    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("activate the exact Circle epoch-close outcome");
    assert!(StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read finalized Circle removal operation")
        .is_none());
    assert!(StoreDatabase::new(&db)
        .closing_circle_controls()
        .await
        .expect("read closing Circle controls after finalization")
        .is_empty());
    let successor = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load successor Circle authoring state")
        .0;
    assert_ne!(successor.control.value.epoch_id(), prior_epoch);
    assert_ne!(successor.control.value.key_fingerprint(), prior_fingerprint);
    assert!(!successor.roster.members().contains_key(&member_pubkey));
    assert!(successor
        .roster
        .members()
        .contains_key(&remaining_member_pubkey));
    let crate::sync::circle::CircleControlState::ActiveEpoch(active) =
        successor.control.value.state()
    else {
        panic!("finalized Circle control must be active");
    };
    let crate::sync::circle::CircleEpochOrigin::Closed {
        closed_epoch_id,
        close_control,
        close_id,
        outcome_hash,
        cutoff,
    } = &active.common.origin
    else {
        panic!("successor Circle epoch must name its exact close outcome");
    };
    assert_eq!(*closed_epoch_id, prior_epoch);
    assert_eq!(close_control, &control.coord);
    assert_eq!(*close_id, close.close_id);
    assert!(cutoff.covers_commit(&package_commit_ref));

    let activation = StoreDatabase::new(&db)
        .verified_circle_activation(circle_id, successor.control.coord.clone())
        .await
        .expect("read successor Circle activation")
        .expect("successor Circle activation is retained");
    let outcome_ref = activation
        .reference
        .objects()
        .close_outcome
        .as_ref()
        .expect("successor activation names its close outcome");
    assert_eq!(
        activation
            .reference
            .objects()
            .access
            .iter()
            .filter(|access| access.bootstrap.is_some())
            .count(),
        2,
        "each remaining Circle member receives a successor bootstrap"
    );
    assert_eq!(outcome_ref.outcome_hash, *outcome_hash);
    let outcome_bytes = store
        .storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
                ProtocolObjectDomain::CircleEpochCloseOutcome,
            ),
            &outcome_ref.object,
            &crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
                circle_id,
                close.close_id,
            ),
        )
        .await
        .expect("read exact Circle epoch-close outcome");
    let outcome: crate::sync::circle::CircleEpochCloseOutcome =
        serde_json::from_slice(&outcome_bytes).expect("parse Circle epoch-close outcome");
    assert!(outcome.verify_for(
        control,
        operation
            .operation()
            .creation
            .close_intent
            .as_ref()
            .expect("Circle removal retains its signed close intent"),
        &[(response_ref, response)],
    ));
}

#[tokio::test]
async fn uploaded_circle_steps_are_read_back_after_restart_before_activation() {
    for corrupt in [false, true] {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join(if corrupt {
            "circle-corrupt-upload.sqlite3"
        } else {
            "circle-missing-upload.sqlite3"
        });
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("open circle database");
        let (store, signer, expected) =
            persist_merge_operation(&db, if corrupt { "corrupt" } else { "missing" }).await;
        store.home.fail_exact_create_before_call(2);
        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("second exact create failure interrupts publication");
        let persisted = StoreDatabase::new(&db)
            .circle_operation(&expected.operation_id)
            .await
            .expect("read interrupted circle operation")
            .expect("interrupted circle operation remains durable");
        assert!(persisted.operation().uploaded.contains("metadata"));

        let metadata = expected
            .operation()
            .prepared_objects
            .get("metadata")
            .expect("operation carries exact metadata object");
        if corrupt {
            store.home.replace_exact_object(
                metadata.reference().slot(),
                b"corrupt metadata bytes".to_vec(),
            );
        } else {
            store.home.remove_exact_object(metadata.reference().slot());
        }
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("reopen circle database");
        resume_circle_operations(&reopened, &store.storage, &signer)
            .await
            .expect_err("durable upload marker must not bypass readback");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 0);
        assert!(StoreDatabase::new(&reopened)
            .circle_operation(&expected.operation_id)
            .await
            .expect("read rejected circle operation")
            .is_some());
    }
}

#[tokio::test]
async fn uploaded_circle_candidate_fails_when_its_ownership_record_is_missing() {
    let db = open_test_db();
    let (_store, _signer, mut journal) =
        persist_merge_operation(&db, "circle-missing-candidate-ownership").await;
    let step = "access-leaf-0";
    let object_id = crate::sync::remote_object::remote_object_id(
        journal
            .operation()
            .prepared_objects
            .get(step)
            .expect("operation carries its access leaf")
            .reference(),
    );
    db.call(move |conn| {
        let deleted = conn
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "expected candidate ownership record was absent before deletion".to_string(),
            ));
        }
        Ok(())
    })
    .await
    .expect("remove candidate ownership record");
    journal.operation_mut().uploaded.insert(step.to_string());

    let error = StoreDatabase::new(&db)
        .update_circle_operation(journal.clone())
        .await
        .expect_err("an uploaded candidate must retain its ownership record");
    assert!(error.to_string().contains("remote object"), "{error}");
    let persisted = StoreDatabase::new(&db)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read operation after rejected update")
        .expect("operation remains durable after rejected update");
    assert!(!persisted.operation().uploaded.contains(step));
}

#[tokio::test]
async fn journal_update_rejects_an_uploaded_marker_without_a_prepared_object() {
    let db = open_test_db();
    let (_store, _signer, mut journal) =
        persist_merge_operation(&db, "circle-unknown-upload-marker").await;
    let unknown_step = "absent-prepared-object";
    journal
        .operation_mut()
        .uploaded
        .insert(unknown_step.to_string());

    let error = StoreDatabase::new(&db)
        .update_circle_operation(journal.clone())
        .await
        .expect_err("an upload marker must name a prepared object");
    assert!(error.to_string().contains(unknown_step), "{error}");
    let persisted = StoreDatabase::new(&db)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read operation after rejected upload marker")
        .expect("operation remains durable after rejected upload marker");
    assert!(!persisted.operation().uploaded.contains(unknown_step));
}

#[tokio::test]
async fn journal_update_rejects_a_tampered_leaf_disposition() {
    let db = open_test_db();
    let (_store, signer, mut journal) =
        persist_merge_operation(&db, "circle-tampered-local-access").await;
    let author = keys::public_key_hex(&signer);
    let own_access = journal
        .operation_mut()
        .creation
        .access
        .iter_mut()
        .find(|access| access.leaf.value.recipient_pubkey == author)
        .expect("founder access");
    assert!(matches!(
        own_access.leaf.value.disposition,
        CircleAccessDisposition::Active { .. }
    ));
    own_access.leaf.value.disposition = CircleAccessDisposition::Inactive;
    let error = StoreDatabase::new(&db)
        .update_circle_operation(journal.clone())
        .await
        .expect_err("journal update must verify its closed candidate graph");
    assert!(
        error.to_string().contains("stored reference differs"),
        "{error}"
    );

    assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    assert!(StoreDatabase::new(&db)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read rejected operation")
        .is_some());
}

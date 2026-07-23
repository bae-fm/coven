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
async fn member_removal_activates_an_exact_epoch_close_and_blocks_authoring() {
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

    let operation_id = components
        .remove_circle_member(circle_id, member_pubkey)
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
    assert_eq!(activation_count(&db, circle_id).await, 3);
    assert!(StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .is_err());

    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish local Circle epoch-close response");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("accept the exact existing Circle epoch-close response");
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
    let (bytes, _) = store
        .storage
        .read_protocol_slot(&response_context, &participant.response_slot, &prefix)
        .await
        .expect("read exact Circle epoch-close response");
    let registration = StoreDatabase::new(&db)
        .activated_store_device_registration(participant.registration.clone())
        .await
        .expect("load response author registration");
    let response =
        crate::sync::circle::CircleEpochCloseResponse::parse_for(&bytes, control, &registration)
            .expect("verify signed Circle epoch-close response");
    assert_eq!(response.registration, participant.registration);

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
    let error = components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect_err("malformed occupied response must fail the cycle");
    assert!(
        error.to_string().contains("Circle epoch-close responses"),
        "{error}"
    );
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

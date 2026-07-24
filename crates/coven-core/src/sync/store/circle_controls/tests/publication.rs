use super::*;
use std::collections::BTreeSet;

fn circle_routing_test_schema() -> (
    Vec<crate::sync::session::SyncedTable>,
    Vec<crate::migration::Migration>,
) {
    (
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

async fn circle_blob_opening_error(
    db: &Database,
    storage: &dyn SyncStorage,
    authority: &crate::blob::RowBlobAuthority,
    stored: &crate::blob::locator::StoredBlobRef,
) -> crate::blob::cache::BlobCacheError {
    match crate::sync::store::blob::opening_protection(
        &StoreDatabase::new(db),
        storage,
        authority,
        stored,
    )
    .await
    {
        Ok(_) => panic!("invalid Circle blob authority must fail"),
        Err(error) => error,
    }
}

fn open_circle_routing_test_db() -> Database {
    let (tables, migrations) = circle_routing_test_schema();
    crate::sync::test_helpers::open_test_db_schema(tables, migrations)
}

fn open_circle_routing_test_db_at(path: &std::path::Path) -> Database {
    let (tables, migrations) = circle_routing_test_schema();
    Database::open(
        path,
        tables,
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        &migrations,
    )
    .expect("open copied Circle routing database")
    .0
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
                    .get_circles(
                        &keys::public_key_hex(&signer),
                        BTreeSet::from([keys::public_key_hex(&signer)]),
                    )
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
                        .get_circles(
                            &keys::public_key_hex(&signer),
                            BTreeSet::from([keys::public_key_hex(&signer)]),
                        )
                        .await
                        .expect("read activated circle"),
                    vec![crate::sync::circle::CircleInfo::Active {
                        id: expected.circle_id(),
                        name: "Household".to_string(),
                        role: crate::sync::circle::CircleRole::Owner,
                        rotation_required: false,
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
            .get_circles(
                &keys::public_key_hex(&signer),
                BTreeSet::from([keys::public_key_hex(&signer)]),
            )
            .await
            .expect("read renamed circle"),
        vec![crate::sync::circle::CircleInfo::Active {
            id: circle_id,
            name: "Household money".to_string(),
            role: crate::sync::circle::CircleRole::Owner,
            rotation_required: false,
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
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("circle-bootstrap-member".to_string()),
        &member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle bootstrap Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let member_db = crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")
        .carries_blob(crate::sync::session::BlobDecl::new(
            "files",
            crate::blob::Provenance::HostProvided,
            crate::blob::CacheFill::CacheEager,
        ))],
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
    install_active_device_fixture(&store, &db, &member_db, &member, "2026-07-23T00:00:00Z")
        .await
        .expect("activate Store member device");
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
    let captured_write_id = write_id.clone();
    let routing = EncryptionService::from_key([42; 32]);
    db.call(move |connection| {
        StoreDatabase::run_internal_store_write_transaction_on(
            connection,
            &tables,
            Some(&routing),
            captured_write_id,
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
    let historical_commit = match db
        .write_status(&write_id)
        .await
        .expect("load historical Circle write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("historical Circle write was not published: {status:?}"),
    };
    let historical_blob = db
        .row_blob_ref("documents", blob_id)
        .await
        .expect("load blob reference from the founder control");

    let concurrent_writer = UserKeypair::generate();
    let concurrent_writer_pubkey = keys::public_key_hex(&concurrent_writer);
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("circle-bootstrap-concurrent-writer".to_string()),
        &concurrent_writer_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle bootstrap Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite concurrent Store writer");
    let concurrent_db = crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")
        .carries_blob(crate::sync::session::BlobDecl::new(
            "files",
            crate::blob::Provenance::HostProvided,
            crate::blob::CacheFill::CacheEager,
        ))],
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
    install_active_device_fixture(
        &store,
        &db,
        &concurrent_db,
        &concurrent_writer,
        "2026-07-23T00:00:01Z",
    )
    .await
    .expect("activate concurrent Store writer device");
    components
        .add_circle_member(
            &store_dir,
            circle_id,
            concurrent_writer_pubkey,
            CircleRole::Member,
        )
        .await
        .expect("add concurrent Circle writer");
    let concurrent_bootstrap_commit = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load concurrent writer Circle bootstrap commit")
        .1;
    let concurrent_store = store
        .loaded_store(&concurrent_db)
        .await
        .expect("load concurrent Circle writer Store");
    let (_concurrent_temp, concurrent_store_dir) = temp_store_dir();
    concurrent_store
        .authorize()
        .await
        .expect("authorize concurrent Circle writer")
        .pull(
            &concurrent_store_dir,
            &concurrent_writer,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("install concurrent Circle writer bootstrap");
    let late_id = "00000000-0000-4000-8000-000000000002";
    let late_bytes = b"late concurrent Circle attachment";
    let late_insert = format!(
        "INSERT INTO documents (id, audience, size, hash, _updated_at)
         VALUES ('{late_id}', '{circle_id}', {}, '{}', '0000000001600-0000-concurrent')",
        late_bytes.len(),
        crate::blob::content_hash(late_bytes),
    );
    let concurrent_tables = concurrent_db.synced_tables().to_vec();
    let late_write_id = concurrent_db.new_write_id();
    let captured_late_write_id = late_write_id.clone();
    concurrent_db
        .call(move |connection| {
            StoreDatabase::run_internal_store_write_transaction_on(
                connection,
                &concurrent_tables,
                Some(&EncryptionService::from_key([42; 32])),
                captured_late_write_id,
                |transaction| {
                    transaction
                        .execute_batch(&late_insert)
                        .map_err(DbError::from)
                },
            )
        })
        .await
        .expect("capture late concurrent Circle row");
    crate::blob::local_files::store(&concurrent_store_dir, "files", late_id, late_bytes)
        .await
        .expect("stage late concurrent Circle blob");
    let concurrent_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        concurrent_writer.clone(),
    )
    .expect("construct concurrent Circle writer storage");
    components
        .add_circle_member(
            &store_dir,
            circle_id,
            member_pubkey.clone(),
            CircleRole::Member,
        )
        .await
        .expect("activate Circle member successor");
    let concurrent_authorization =
        crate::sync::store::Store::authorize_borrowed(&concurrent_storage, &concurrent_db)
            .await
            .expect("authorize concurrent Circle writer Store");
    let concurrent_device_id = concurrent_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read concurrent Circle writer device id")
        .expect("concurrent Circle writer device id is installed");
    concurrent_authorization
        .drain_uploads(
            &concurrent_store_dir,
            &crate::clock::SystemClock,
            &concurrent_db.hlc(),
            Some(&EncryptionService::from_key([42; 32])),
            None,
        )
        .await
        .expect("publish late concurrent Circle blob");
    assert!(
        concurrent_authorization
            .prepare_pending_store_write(
                &concurrent_device_id,
                "2026-07-23T00:00:02Z",
                &concurrent_writer,
                &concurrent_store_dir,
            )
            .await
            .expect("prepare late concurrent Circle package"),
        "late concurrent Circle package must be prepared"
    );
    assert_eq!(
        concurrent_authorization
            .drain_store_writes()
            .await
            .expect("publish late concurrent Circle package"),
        1
    );
    let late_commit = match concurrent_db
        .write_status(&late_write_id)
        .await
        .expect("load late concurrent Circle write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("late concurrent Circle write was not published: {status:?}"),
    };
    let member_store = store
        .loaded_store(&member_db)
        .await
        .expect("load Circle member Store");
    let (member_temp, member_store_dir) = temp_store_dir();
    let target_control = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load target Circle bootstrap control")
        .0
        .control
        .coord;
    let encoded_target_control =
        serde_json::to_string(&target_control).expect("serialize target Circle bootstrap control");
    let target_blob_object = crate::sync::remote_object::remote_object_id(
        historical_blob
            .stored()
            .expect("published historical blob has an exact stored reference")
            .object(),
    )
    .to_string();
    member_db.fail_next_merge_materialization_at(
        crate::database::MergeMaterializationFailurePoint::ProjectionReplacement,
    );
    let injected = member_store
        .authorize()
        .await
        .expect("authorize Circle member Store for failed bootstrap")
        .pull(
            &member_store_dir,
            &member,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect_err("injected bootstrap projection replacement must fail");
    assert!(
        injected.to_string().contains("injected failure"),
        "{injected}"
    );
    let partial_state = member_db
        .call({
            let blob_id = blob_id.to_string();
            move |connection| {
                connection
                    .query_row(
                        "SELECT
                           EXISTS(SELECT 1 FROM documents WHERE id = ?1),
                           EXISTS(
                               SELECT 1 FROM circle_bootstrap_coverage WHERE circle_id = ?2
                           ),
                           EXISTS(
                               SELECT 1 FROM circle_control_activations
                               WHERE circle_id = ?2 AND control_coord = ?3
                           ),
                           EXISTS(
                               SELECT 1 FROM remote_objects WHERE object_id = ?4
                           )",
                        rusqlite::params![
                            blob_id,
                            circle_id.to_string(),
                            encoded_target_control,
                            target_blob_object,
                        ],
                        |row| {
                            Ok((
                                row.get::<_, bool>(0)?,
                                row.get::<_, bool>(1)?,
                                row.get::<_, bool>(2)?,
                                row.get::<_, bool>(3)?,
                            ))
                        },
                    )
                    .map_err(DbError::from)
            }
        })
        .await
        .expect("inspect failed bootstrap transaction");
    assert_eq!(partial_state, (false, false, false, false));
    let member_pull = member_store
        .authorize()
        .await
        .expect("authorize Circle member Store")
        .pull(
            &member_store_dir,
            &member,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("pull Circle member bootstrap");
    assert!(
        member_pull.held_positions.is_empty(),
        "{:?}",
        member_pull.held_positions
    );
    let installed_ids = member_db
        .call(|connection| {
            let mut statement = connection
                .prepare("SELECT id FROM documents ORDER BY id")
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            Ok(rows)
        })
        .await
        .expect("list installed recipient rows");
    assert!(
        installed_ids.iter().any(|installed| installed == blob_id),
        "recipient rows after bootstrap replay: {installed_ids:?}"
    );
    let installed_row = member_db
        .call({
            let blob_id = blob_id.to_string();
            move |connection| {
                connection
                    .query_row(
                        "SELECT audience, size, hash, _updated_at
                         FROM documents WHERE id = ?1",
                        [&blob_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .map_err(DbError::from)
            }
        })
        .await
        .expect("recipient bootstrap installs the preexisting Circle row");
    assert_eq!(
        installed_row,
        (
            circle_id.to_string(),
            blob_bytes.len() as i64,
            crate::blob::content_hash(blob_bytes),
            "0000000001500-0000-owner".to_string(),
        )
    );
    let installed_late_row = member_db
        .call({
            let late_id = late_id.to_string();
            move |connection| {
                connection
                    .query_row(
                        "SELECT audience, size, hash, _updated_at
                         FROM documents WHERE id = ?1",
                        [&late_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .map_err(DbError::from)
            }
        })
        .await
        .expect("recipient replay installs the late concurrent Circle row");
    assert_eq!(
        installed_late_row,
        (
            circle_id.to_string(),
            late_bytes.len() as i64,
            crate::blob::content_hash(late_bytes),
            "0000000001600-0000-concurrent".to_string(),
        )
    );
    let installed_blob = member_db
        .row_blob_ref("documents", blob_id)
        .await
        .expect("recipient bootstrap installs the exact row blob graph");
    assert_eq!(installed_blob, historical_blob);
    let replay_blob_decls = member_db.blob_decls();
    let replay_gates = member_db.gates();
    let replay_tables = member_db.synced_tables().to_vec();
    let replay_routing_key = crate::sync::circle::derive_row_routing_key(
        &EncryptionService::from_key([42; 32]),
        store.root.store_root_hash,
    )
    .expect("derive bootstrap replay routing key");
    let replay_historical_id = blob_id.to_string();
    let replay_late_id = late_id.to_string();
    let (retained_count, retained_late_count, sabotaged_count, sabotaged_late_count) = member_db
        .call(move |connection| {
            let tx = connection.unchecked_transaction().map_err(DbError::from)?;
            let retained = crate::sync::store::pull::replay_retained_merge_projection_on(
                &tx,
                &replay_blob_decls,
                &replay_gates,
                &replay_tables,
                Some(&replay_routing_key),
                &BTreeSet::new(),
                None,
                false,
                crate::sync::store::pull::LocalStoreMembership::Current,
            )?;
            let retained_count: i64 = retained.query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [replay_historical_id.as_str()],
                |row| row.get(0),
            )?;
            let retained_late_count: i64 = retained.query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [replay_late_id.as_str()],
                |row| row.get(0),
            )?;
            tx.execute("DELETE FROM circle_bootstrap_coverage", [])
                .map_err(DbError::from)?;
            let sabotaged = crate::sync::store::pull::replay_retained_merge_projection_on(
                &tx,
                &replay_blob_decls,
                &replay_gates,
                &replay_tables,
                Some(&replay_routing_key),
                &BTreeSet::new(),
                None,
                false,
                crate::sync::store::pull::LocalStoreMembership::Current,
            )?;
            let sabotaged_count: i64 = sabotaged.query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [replay_historical_id.as_str()],
                |row| row.get(0),
            )?;
            let sabotaged_late_count: i64 = sabotaged.query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [replay_late_id.as_str()],
                |row| row.get(0),
            )?;
            tx.rollback().map_err(DbError::from)?;
            Ok::<_, DbError>((
                retained_count,
                retained_late_count,
                sabotaged_count,
                sabotaged_late_count,
            ))
        })
        .await
        .expect("sabotage retained Circle bootstrap replay input");
    assert_eq!(retained_count, 1);
    assert_eq!(retained_late_count, 1);
    assert_eq!(sabotaged_count, 0);
    assert_eq!(sabotaged_late_count, 1);
    let coverage_count = member_db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                    [circle_id.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read durable recipient Circle bootstrap coverage");
    assert_eq!(coverage_count, 1);
    let (current, member_bootstrap_commit) = StoreDatabase::new(&member_db)
        .circle_authoring_context(circle_id, &member_pubkey)
        .await
        .expect("load Circle member successor state");
    member_db
        .call({
            let activation_commit = member_bootstrap_commit.clone();
            move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                transaction
                    .execute(
                        "UPDATE circle_bootstrap_coverage
                         SET image_hash = ?2
                         WHERE circle_id = ?1",
                        rusqlite::params![
                            circle_id.to_string(),
                            crate::sync::store_commit::ObjectHash::digest(
                                b"corrupt Circle bootstrap image hash"
                            )
                            .to_string(),
                        ],
                    )
                    .map_err(DbError::from)?;
                let retained = StoreDatabase::load_retained_merge_materialization_by_ref_on(
                    &transaction,
                    &activation_commit,
                )?;
                let verified = retained.as_verified()?;
                let error = StoreDatabase::record_circle_bootstrap_coverage_on(
                    &transaction,
                    &activation_commit,
                    verified.circle_activations(),
                )
                .expect_err("idempotent bootstrap recording must reject a changed image hash");
                assert!(
                    error
                        .to_string()
                        .contains("differs from its exact reference"),
                    "{error}"
                );
                transaction.rollback().map_err(DbError::from)
            }
        })
        .await
        .expect("reject corrupted Circle bootstrap coverage");
    crate::sync::store::blob::materialize_row_blob(
        &StoreDatabase::new(&db),
        &store_dir,
        Some(&store.storage),
        &historical_blob,
    )
    .await
    .expect("materialize a blob through its retained founder control");
    assert_eq!(
        crate::sync::store::blob::read_blob(
            &StoreDatabase::new(&db),
            &store_dir,
            Some(&store.storage),
            &historical_blob,
        )
        .await
        .expect("read a blob through its retained founder control"),
        blob_bytes,
    );
    let protection = crate::sync::store::blob::opening_protection(
        &StoreDatabase::new(&member_db),
        &store.storage,
        historical_blob.authority(),
        historical_blob
            .stored()
            .expect("published historical blob has an exact stored reference"),
    )
    .await
    .expect("new Circle member resolves the founder blob key from its successor grant");
    let opened_destination = member_temp.path().join("new-member-opened-founder-blob");
    let opened = store
        .storage
        .stage_verified_blob_plaintext(
            historical_blob
                .stored()
                .expect("published historical blob has an exact stored reference"),
            protection,
            &opened_destination,
        )
        .await
        .expect("new Circle member opens the exact founder blob");
    assert_eq!(
        tokio::fs::read(opened.path())
            .await
            .expect("read opened founder blob"),
        blob_bytes,
    );
    let substituted = crate::blob::RowBlobRef::new(
        historical_blob.table().to_string(),
        historical_blob.row_id().to_string(),
        historical_blob.row_stamp().to_string(),
        historical_blob.column().to_string(),
        historical_blob.blob().clone(),
        historical_blob.plaintext_size(),
        historical_blob.plaintext_hash(),
        crate::blob::RowBlobAuthority::Remote(
            crate::sync::audience_package::PackageAudience::Circle {
                circle_id,
                control: current.control.coord.clone(),
                key_fingerprint: current.control.value.key_fingerprint(),
            },
        ),
        historical_blob.stored().cloned(),
    )
    .expect("construct same-Circle successor-control substitution");
    let substitution_error = crate::sync::store::blob::read_blob(
        &StoreDatabase::new(&db),
        &store_dir,
        Some(&store.storage),
        &substituted,
    )
    .await
    .expect_err("row blob binding must reject a substituted Circle control");
    assert!(
        substitution_error.to_string().contains("is stale"),
        "{substitution_error}"
    );
    let CircleAccessDisposition::Active {
        bootstrap: Some(bootstrap),
        ..
    } = current.access.disposition
    else {
        panic!("successor access must carry its bootstrap image");
    };
    assert_eq!(bootstrap.schema_version, db.schema_version());
    assert_eq!(bootstrap.sync_routing_hash, db.sync_routing_hash());
    assert!(
        !bootstrap.coverage.covers_commit(&late_commit),
        "the bootstrap must not claim the concurrently published package"
    );
    let [blob] = bootstrap.blobs.as_slice() else {
        panic!("Circle bootstrap must pin its one exact blob");
    };
    assert_eq!(
        blob.stored()
            .expect("Circle bootstrap row blob has an exact locator")
            .locator()
            .audience(),
        crate::blob::locator::RemoteAudience::Circle(circle_id)
    );
    let blob = blob.clone();
    assert_eq!(activation_count(&db, circle_id).await, 3);
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
    let blob_object_id = crate::sync::remote_object::remote_object_id(
        blob.stored()
            .expect("Circle bootstrap row blob has an exact locator")
            .object(),
    );
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
    let store_commit_owners = ownership
        .activated
        .iter()
        .filter_map(|owner| match owner {
            crate::sync::remote_object::SharedObjectOwner::StoreCommit(commit) => {
                Some(commit.clone())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        store_commit_owners,
        BTreeSet::from([
            historical_commit,
            concurrent_bootstrap_commit,
            member_bootstrap_commit,
        ]),
        "the row package and both signed Circle bootstraps must own the blob"
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
    let historical_locator = crate::blob::locator::BlobLocator::opaque(
        "files",
        "old-epoch-blob",
        package_commit.author_registration.clone(),
        crate::blob::locator::RemoteAudience::Circle(circle_id),
        crate::blob::BlobScope::Master,
        prior_fingerprint,
        1,
        ObjectHash::digest(b"x"),
    )
    .expect("construct old-epoch Circle blob locator");
    let historical_stored = crate::blob::locator::StoredBlobRef::new(
        historical_locator.clone(),
        ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(historical_locator.semantic_key())
                .expect("old-epoch blob locator has a valid logical key"),
            b"ciphertext".len() as u64,
            ObjectHash::digest(b"ciphertext"),
        ),
    )
    .expect("construct exact old-epoch stored blob");
    let historical_authority = crate::blob::RowBlobAuthority::Remote(
        crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control: prior_control.clone(),
            key_fingerprint: prior_fingerprint,
        },
    );

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
        .circle_package_access(circle_id, prior_control.clone())
        .await
        .expect("load historical pre-close access")
        .expect("historical pre-close access remains retained");
    assert!(historical_access
        .writers
        .contains(&keys::public_key_hex(&signer)));
    let loaded_packages = match crate::sync::store::pull::load_applicable_circle_packages(
        &StoreDatabase::new(&db),
        &store.storage,
        &store.root,
        &package_commit_ref,
        &package_commit,
        &[],
        &package_author,
        crate::sync::store::pull::LocalStoreMembership::Current,
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
    let candidate_base_temp =
        tempfile::tempdir().expect("create candidate base database directory");
    let candidate_base_path = candidate_base_temp.path().join("pre-close.sqlite3");
    let copied_path = candidate_base_path.clone();
    db.call(move |connection| {
        connection
            .execute("VACUUM INTO ?1", [copied_path.to_string_lossy().as_ref()])
            .map(|_| ())
            .map_err(DbError::from)
    })
    .await
    .expect("copy pre-close Circle database");

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
    let (successor, successor_commit_ref) = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load successor Circle authoring state");
    assert_ne!(successor.control.value.epoch_id(), prior_epoch);
    assert_ne!(successor.control.value.key_fingerprint(), prior_fingerprint);
    assert!(!successor.roster.members().contains_key(&member_pubkey));
    assert!(successor
        .roster
        .members()
        .contains_key(&remaining_member_pubkey));
    let historical_protection = crate::sync::store::blob::opening_protection(
        &StoreDatabase::new(&db),
        &store.storage,
        &historical_authority,
        &historical_stored,
    )
    .await
    .expect("resolve old-epoch blob protection through retained Circle authority");
    let crate::sync::storage::BlobSpoolProtection::Opaque(historical_encryption) =
        historical_protection
    else {
        panic!("Circle blob protection must be opaque");
    };
    assert_eq!(
        historical_encryption.seal_key_fingerprint(),
        prior_fingerprint
    );
    let successor_substitution = crate::blob::RowBlobAuthority::Remote(
        crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control: successor.control.coord.clone(),
            key_fingerprint: successor.control.value.key_fingerprint(),
        },
    );
    let substitution_error = circle_blob_opening_error(
        &db,
        &store.storage,
        &successor_substitution,
        &historical_stored,
    )
    .await;
    assert!(
        substitution_error
            .to_string()
            .contains("key differs from its exact activated authority"),
        "{substitution_error}"
    );
    let mut absent_control = prior_control.clone();
    absent_control.control_hash = ObjectHash::digest(b"absent Circle control");
    let absent_authority = crate::blob::RowBlobAuthority::Remote(
        crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control: absent_control,
            key_fingerprint: prior_fingerprint,
        },
    );
    let absent_error =
        circle_blob_opening_error(&db, &store.storage, &absent_authority, &historical_stored).await;
    assert!(
        absent_error
            .to_string()
            .contains("has no retained authority"),
        "{absent_error}"
    );
    let wrong_circle_authority = crate::blob::RowBlobAuthority::Remote(
        crate::sync::audience_package::PackageAudience::Circle {
            circle_id: CircleId::from_bytes([0x77; 16]),
            control: successor.control.coord.clone(),
            key_fingerprint: successor.control.value.key_fingerprint(),
        },
    );
    let wrong_circle_error = circle_blob_opening_error(
        &db,
        &store.storage,
        &wrong_circle_authority,
        &historical_stored,
    )
    .await;
    assert!(
        wrong_circle_error
            .to_string()
            .contains("key differs from its exact activated authority"),
        "{wrong_circle_error}"
    );
    let wrong_fingerprint_authority = crate::blob::RowBlobAuthority::Remote(
        crate::sync::audience_package::PackageAudience::Circle {
            circle_id,
            control: successor.control.coord.clone(),
            key_fingerprint: crate::KeyFingerprint::from_bytes([0x55; 32]),
        },
    );
    let wrong_fingerprint_error = circle_blob_opening_error(
        &db,
        &store.storage,
        &wrong_fingerprint_authority,
        &historical_stored,
    )
    .await;
    assert!(
        wrong_fingerprint_error
            .to_string()
            .contains("key differs from its exact activated authority"),
        "{wrong_fingerprint_error}"
    );
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
    let crate::sync::circle::CircleEpochCloseSlotValue::Outcome(outcome) =
        crate::sync::circle::CircleEpochCloseSlotValue::parse(&outcome_bytes)
            .expect("parse Circle epoch-close outcome slot value")
    else {
        panic!("finalized close slot must hold a final outcome");
    };
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
    assert!(db
        .call(|connection| {
            connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM documents
                         WHERE id = '00000000-0000-4000-8000-000000000001'
                     )",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read accepted pre-cutoff Circle row after replay"));

    store.home.clear_exact_reads();
    let accepted_after_cutoff = match crate::sync::store::pull::load_applicable_circle_packages(
        &StoreDatabase::new(&db),
        &store.storage,
        &store.root,
        &package_commit_ref,
        &package_commit,
        &[],
        &package_author,
        crate::sync::store::pull::LocalStoreMembership::Current,
    )
    .await
    {
        Ok(packages) => packages,
        Err(crate::sync::store::pull::PullCircleActivationError::Database(error)) => {
            panic!("load accepted Circle package after cutoff: {error}")
        }
        Err(crate::sync::store::pull::PullCircleActivationError::Invalid(error)) => {
            panic!("load accepted Circle package after cutoff: {error}")
        }
    };
    assert_eq!(accepted_after_cutoff.len(), 1);
    let package_slot = package_commit.circle_packages()[0]
        .package
        .object
        .slot()
        .clone();
    assert!(store.home.exact_reads().contains(&package_slot));

    let accepted_stream_position = cutoff
        .commits()
        .get(&package_commit_ref.coord.stream_id)
        .expect("Circle cutoff covers the package author stream");
    let mut beyond_cutoff = accepted_stream_position.clone();
    beyond_cutoff.coord.sequence = beyond_cutoff
        .coord
        .sequence()
        .checked_add(1)
        .expect("test cutoff sequence has a successor");
    beyond_cutoff.commit_hash = ObjectHash::digest(b"Circle package beyond cutoff");
    store.home.clear_exact_reads();
    let omitted_after_cutoff = match crate::sync::store::pull::load_applicable_circle_packages(
        &StoreDatabase::new(&db),
        &store.storage,
        &store.root,
        &beyond_cutoff,
        &package_commit,
        &[],
        &package_author,
        crate::sync::store::pull::LocalStoreMembership::Current,
    )
    .await
    {
        Ok(packages) => packages,
        Err(crate::sync::store::pull::PullCircleActivationError::Database(error)) => {
            panic!("classify later Circle package against cutoff: {error}")
        }
        Err(crate::sync::store::pull::PullCircleActivationError::Invalid(error)) => {
            panic!("classify later Circle package against cutoff: {error}")
        }
    };
    assert!(omitted_after_cutoff.is_empty());
    assert!(!store.home.exact_reads().contains(&package_slot));

    let mut cutoff_collision = accepted_stream_position.clone();
    cutoff_collision.commit_hash = ObjectHash::digest(b"Circle cutoff coordinate collision");
    store.home.clear_exact_reads();
    let collision = crate::sync::store::pull::load_applicable_circle_packages(
        &StoreDatabase::new(&db),
        &store.storage,
        &store.root,
        &cutoff_collision,
        &package_commit,
        &[],
        &package_author,
        crate::sync::store::pull::LocalStoreMembership::Current,
    )
    .await;
    assert!(matches!(
        collision,
        Err(crate::sync::store::pull::PullCircleActivationError::Database(error))
            if error
                .to_string()
                .contains("conflicts with its accepted epoch cutoff")
    ));
    assert!(!store.home.exact_reads().contains(&package_slot));

    let (successor_commit, successor_author) = crate::sync::store::pull::load_commit_with_author(
        &store.storage,
        &store.root,
        &successor_commit_ref,
    )
    .await
    .expect("load exact successor Circle commit");
    let device_signer = successor_author
        .device_signer(&signer)
        .expect("open successor Circle commit signer");
    let candidate_coord = successor_commit_ref.coord.clone();
    let sequence = candidate_coord.sequence();
    let candidate_family = successor_commit.candidate_family();
    let candidate_write_id = successor_commit.write_id.clone();
    let original_package =
        crate::sync::audience_package::AudiencePackage::parse(&loaded_packages[0].bytes)
            .expect("parse accepted pre-close Circle package");
    let candidate_package = crate::sync::audience_package::AudiencePackage::circle(
        successor_commit.store_root_hash,
        candidate_family,
        candidate_write_id.clone(),
        candidate_coord.clone(),
        db.schema_version(),
        circle_id,
        prior_control.clone(),
        prior_fingerprint,
        original_package.changeset().to_vec(),
        original_package.blob_bindings().to_vec(),
    )
    .expect("build exact old-control package for combined Circle candidate");
    let candidate_package_bytes = candidate_package.to_bytes();
    let candidate_package_prefix = crate::sync::store_commit::circle_package_semantic_prefix(
        circle_id,
        candidate_family,
        &candidate_coord.stream_id.to_string(),
        sequence,
        ObjectHash::digest(&candidate_package_bytes),
    );
    let candidate_package_context = ProtocolObjectContext::circle(
        successor_commit.store_root_hash,
        ProtocolObjectDomain::CirclePackage,
        historical_access.encryption.clone(),
    );
    let candidate_package_slot = store
        .storage
        .allocate_protocol_slot(
            &candidate_package_context,
            &candidate_package_prefix,
            ".pkg",
        )
        .await
        .expect("allocate exact old-control package slot");
    let candidate_package_object = store
        .storage
        .prepare_protocol_object(
            &candidate_package_context,
            candidate_package_slot,
            &candidate_package_prefix,
            candidate_package_bytes.clone(),
        )
        .expect("prepare exact old-control package");
    crate::sync::store_objects::create_exact_object(&store.storage, &candidate_package_object)
        .await
        .expect("publish exact old-control package");
    let circle_packages = [crate::sync::store_commit::CirclePackageInput {
        circle_id,
        control: prior_control,
        key_fingerprint: prior_fingerprint,
        package: crate::sync::store_commit::StorePackageInput {
            candidate_family,
            schema_version: db.schema_version(),
            bytes: &candidate_package_bytes,
            object: candidate_package_object.reference().clone(),
        },
    }];
    let candidate_commit = StoreBatchCommit::signed_operations(
        successor_commit.store_root_hash,
        candidate_write_id,
        candidate_coord.clone(),
        successor_commit.author_registration.clone(),
        &successor_author,
        successor_commit.order.clone(),
        successor_commit.membership_state.clone(),
        successor_commit.device_state.clone(),
        crate::sync::store_commit::StoreOperationMembershipAuthority {
            predecessor: successor_commit
                .membership_authority
                .clone()
                .expect("successor Circle commit carries membership authority"),
        },
        crate::sync::store_commit::StoreCommitOperationsInput {
            acknowledgement: None,
            circle_acknowledgements: Vec::new(),
            control: None,
            device_join_attempt_decisions: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
            provider_access_withdrawals: Vec::new(),
            device_registrations: Vec::new(),
            device_exclusion_proposals: Vec::new(),
            device_exclusion_outcomes: Vec::new(),
            stream_activations: successor_commit.stream_activations().to_vec(),
            circle_controls: successor_commit.circle_controls().to_vec(),
            store_package: None,
            circle_packages: &circle_packages,
        },
        &device_signer,
    )
    .expect("sign combined successor-control and old-package candidate");
    let candidate_commit_prefix = commit_semantic_prefix(
        candidate_family,
        &candidate_coord.stream_id.to_string(),
        sequence,
        candidate_commit.commit_hash(),
    );
    let candidate_commit_context = ProtocolObjectContext::signed_plaintext(
        successor_commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let candidate_commit_slot = store
        .storage
        .allocate_protocol_slot(&candidate_commit_context, &candidate_commit_prefix, ".json")
        .await
        .expect("allocate combined Circle candidate slot");
    let candidate_commit_object = store
        .storage
        .prepare_protocol_object(
            &candidate_commit_context,
            candidate_commit_slot,
            &candidate_commit_prefix,
            candidate_commit.to_bytes(),
        )
        .expect("prepare combined Circle candidate");
    let candidate_commit_ref = StoreBatchCommitRef::from_commit(
        &candidate_commit,
        candidate_coord,
        candidate_commit_object.reference().clone(),
    )
    .expect("bind combined Circle candidate reference");
    assert!(!cutoff.covers_commit(&candidate_commit_ref));
    candidate_commit
        .verify_circle_package(circle_id, &candidate_package_bytes)
        .expect("combined Circle candidate binds its exact old-control package");

    let candidate_base_database = open_circle_routing_test_db_at(&candidate_base_path);
    store.home.clear_exact_reads();
    let omitted_by_candidate_successor =
        match crate::sync::store::pull::load_applicable_circle_packages(
            &StoreDatabase::new(&candidate_base_database),
            &store.storage,
            &store.root,
            &candidate_commit_ref,
            &candidate_commit,
            std::slice::from_ref(&activation),
            &successor_author,
            crate::sync::store::pull::LocalStoreMembership::Current,
        )
        .await
        {
            Ok(packages) => packages,
            Err(crate::sync::store::pull::PullCircleActivationError::Database(error)) => {
                panic!("classify package against candidate successor cutoff: {error}")
            }
            Err(crate::sync::store::pull::PullCircleActivationError::Invalid(error)) => {
                panic!("classify package against candidate successor cutoff: {error}")
            }
        };
    assert!(omitted_by_candidate_successor.is_empty());
    assert!(!store
        .home
        .exact_reads()
        .contains(candidate_package_object.reference().slot()));

    // Quiet-device equivalence: this device carries no unpublished writes, so the
    // successor bootstrap image is the accepted-history projection at the exact
    // cutoff. Its recorded coverage equals the close cutoff, its recorded image
    // hash equals its exact bytes, and installing it converges on the accepted
    // pre-cutoff Circle state.
    let crate::sync::circle::CircleAccessDisposition::Active {
        bootstrap: Some(successor_bootstrap),
        ..
    } = &successor.access.disposition
    else {
        panic!("the successor access leaf carries its bootstrap image");
    };
    assert_eq!(
        &successor_bootstrap.coverage, cutoff,
        "the successor bootstrap covers exactly the accepted close cutoff"
    );
    let retained_bootstrap = db
        .call({
            let control = successor.control.coord.clone();
            move |connection| {
                let bootstraps = StoreDatabase::circle_bootstrap_replay_inputs_on(connection)?;
                Ok(bootstraps.into_iter().find_map(|(_, bootstrap)| {
                    (bootstrap.circle_id() == circle_id && bootstrap.control() == &control)
                        .then_some(bootstrap)
                }))
            }
        })
        .await
        .expect("read retained successor bootstrap replay input")
        .expect("the device retains its successor bootstrap image");
    assert_eq!(
        crate::sync::store_commit::ObjectHash::digest(retained_bootstrap.image_bytes()),
        successor_bootstrap.image.image_hash,
        "the retained bootstrap image hashes to its recorded image hash"
    );
    let (_image_temp, image_dir) = temp_store_dir();
    let image_path = image_dir.as_ref().join("successor-bootstrap-image.sqlite3");
    std::fs::write(&image_path, retained_bootstrap.image_bytes())
        .expect("write the successor bootstrap image");
    let image =
        rusqlite::Connection::open(&image_path).expect("open the successor bootstrap image");
    let converges: bool = image
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM documents
                 WHERE id = '00000000-0000-4000-8000-000000000001'
             )",
            [],
            |row| row.get(0),
        )
        .expect("read the successor bootstrap image projection");
    assert!(
        converges,
        "a recipient installing the successor bootstrap converges on the accepted Circle row"
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

async fn member_pull(fixture: &ClosingFounderCircle) {
    crate::sync::store::Store::authorize_borrowed(&fixture.member_storage, &fixture.member_db)
        .await
        .expect("authorize Circle member Store")
        .pull(
            &fixture.member_store_dir,
            &fixture.member,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("member pull");
}

/// Publish the member device's oldest pending durable write, returning the number
/// of Store packages published. Errors while old-epoch publication is frozen —
/// the closing Circle has no active control to resolve.
async fn member_push(fixture: &ClosingFounderCircle, stamp: &str) -> Result<u64, String> {
    let authorized =
        crate::sync::store::Store::authorize_borrowed(&fixture.member_storage, &fixture.member_db)
            .await
            .map_err(|error| error.to_string())?;
    let device_id = fixture
        .member_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read member device id")
        .expect("member device id is installed");
    authorized
        .prepare_pending_store_write(
            &device_id,
            stamp,
            &fixture.member,
            &fixture.member_store_dir,
        )
        .await
        .map_err(|error| error.to_string())?;
    authorized
        .drain_store_writes()
        .await
        .map_err(|error| error.to_string())
}

struct ClosingFounderCircle {
    _temp: tempfile::TempDir,
    store_dir: crate::store_dir::StoreDir,
    db: Database,
    store: TestStore,
    signer: UserKeypair,
    components: crate::sync::cycle::SyncComponents,
    circle_id: CircleId,
    member: UserKeypair,
    member_db: Database,
    member_storage: crate::sync::cloud_storage::CloudSyncStorage,
    _member_temp: tempfile::TempDir,
    member_store_dir: crate::store_dir::StoreDir,
    member_pubkey: String,
    remaining_member_pubkey: String,
    operation_id: CircleOperationId,
    prior_epoch: crate::sync::circle::CircleEpochId,
    prior_fingerprint: crate::KeyFingerprint,
}

/// Drive a founder Circle to a member removal waiting on close responses, sharing
/// the removal-test fixture. The returned handles resume from
/// `CircleOperationState::WaitingForCloseResponses`.
async fn setup_closing_founder_circle(name: &str) -> ClosingFounderCircle {
    let db = open_circle_routing_test_db();
    let (store, signer, founder) = persist_merge_operation(&db, name).await;
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
        &crate::sync::hlc::Hlc::new("circle-cancel-owner".to_string()),
        &member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle cancel Store",
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
        &crate::sync::hlc::Hlc::new("circle-cancel-second-member".to_string()),
        &remaining_member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Circle cancel Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite remaining Store member");
    let member_db = open_circle_routing_test_db();
    install_active_device_fixture(&store, &db, &member_db, &member, "2026-07-24T00:00:00Z")
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

    // The member's own device installs the Circle bootstrap and becomes an active
    // effective member holding the epoch key.
    let member_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        member.clone(),
    )
    .expect("open Circle member storage");
    let (_member_temp, member_store_dir) = temp_store_dir();
    crate::sync::store::Store::authorize_borrowed(&member_storage, &member_db)
        .await
        .expect("authorize Circle member Store")
        .pull(
            &member_store_dir,
            &member,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("member installs the Circle bootstrap");

    let prior = StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&signer))
        .await
        .expect("load pre-close Circle control")
        .0;
    let prior_epoch = prior.control.value.epoch_id();
    let prior_fingerprint = prior.control.value.key_fingerprint();

    let operation_id = components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("activate Circle epoch close");
    assert_eq!(
        StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read Circle removal operation")
            .expect("Circle removal waits for close responses")
            .state(),
        CircleOperationState::WaitingForCloseResponses
    );

    ClosingFounderCircle {
        _temp,
        store_dir,
        db,
        store,
        signer,
        components,
        circle_id,
        member,
        member_db,
        member_storage,
        _member_temp,
        member_store_dir,
        member_pubkey,
        remaining_member_pubkey,
        operation_id,
        prior_epoch,
        prior_fingerprint,
    }
}

#[tokio::test]
async fn cancelling_a_waiting_close_reopens_the_frozen_epoch() {
    let fixture = setup_closing_founder_circle("circle-cancel-reopen").await;

    // Authoring is frozen while the epoch is closing; capture the exact close
    // control the reopen must name as its predecessor.
    let close_control = StoreDatabase::new(&fixture.db)
        .closing_circle_controls()
        .await
        .expect("read closing Circle controls")
        .into_iter()
        .next()
        .expect("member removal leaves one closing Circle")
        .coord
        .clone();
    assert!(StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .is_err());

    // The member captures an old-epoch write while its Circle is still active,
    // then pulls the close. While the epoch is closing, publication is frozen: the
    // write does not reach the Store.
    let member_row = "00000000-0000-4000-8000-0000000000c1";
    let member_tables = fixture.member_db.synced_tables().to_vec();
    let member_write_id = fixture.member_db.new_write_id();
    let captured_member_write_id = member_write_id.clone();
    let member_circle_id = fixture.circle_id;
    fixture
        .member_db
        .call(move |connection| {
            StoreDatabase::run_internal_store_write_transaction_on(
                connection,
                &member_tables,
                Some(&EncryptionService::from_key([42; 32])),
                captured_member_write_id,
                |transaction| {
                    transaction
                        .execute(
                            "INSERT INTO documents (id, audience, _updated_at)
                             VALUES (?1, ?2, ?3)",
                            rusqlite::params![
                                member_row,
                                member_circle_id.to_string(),
                                "0000000003000-0000-member",
                            ],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                },
            )
        })
        .await
        .expect("member captures an old-epoch Circle row");
    member_pull(&fixture).await;
    assert!(
        member_push(&fixture, "2026-07-24T00:00:10Z").await.is_err(),
        "old-epoch publication is frozen while the epoch is closing"
    );
    assert!(
        !matches!(
            fixture
                .member_db
                .write_status(&member_write_id)
                .await
                .expect("read member write status during close"),
            crate::WriteStatus::Published(_)
        ),
        "the frozen old-epoch write has not reached the Store"
    );

    fixture
        .components
        .cancel_circle_epoch_close(fixture.circle_id)
        .await
        .expect("cancel the Circle epoch close");

    // The durable removal operation is complete and no close remains.
    assert!(StoreDatabase::new(&fixture.db)
        .circle_operation(&fixture.operation_id)
        .await
        .expect("read cancelled Circle removal operation")
        .is_none());
    assert!(StoreDatabase::new(&fixture.db)
        .closing_circle_controls()
        .await
        .expect("read closing Circle controls after cancellation")
        .is_empty());

    // Authoring resumes on the identical epoch and key; the removal is undone.
    let (reopened, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load reopened Circle authoring state");
    assert_eq!(reopened.control.value.epoch_id(), fixture.prior_epoch);
    assert_eq!(
        reopened.control.value.key_fingerprint(),
        fixture.prior_fingerprint
    );
    assert!(reopened
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
    assert!(reopened
        .roster
        .members()
        .contains_key(&fixture.remaining_member_pubkey));
    let crate::sync::circle::CircleControlState::ActiveEpoch(active) =
        reopened.control.value.state()
    else {
        panic!("reopened Circle control must be active");
    };
    assert_eq!(
        active.common.origin,
        crate::sync::circle::CircleEpochOrigin::Founder
    );
    assert_eq!(
        reopened.control.value.previous_control_hash(),
        Some(close_control.control_hash())
    );

    // The reopening control retains its exact cancellation binding.
    let activation = StoreDatabase::new(&fixture.db)
        .verified_circle_activation(fixture.circle_id, reopened.control.coord.clone())
        .await
        .expect("read reopened Circle activation")
        .expect("reopened Circle activation is retained");
    let cancellation_ref = activation
        .reference
        .objects()
        .close_cancellation
        .as_ref()
        .expect("reopening control names its exact cancellation");
    assert!(activation.reference.objects().close_outcome.is_none());
    assert_eq!(
        cancellation_ref.close_id,
        crate::sync::circle::CircleEpochCloseId::from_operation_id(&fixture.operation_id)
    );

    // Liveness: the frozen member device pulls the reopened control and its
    // old-epoch write now publishes successfully under the restored epoch key.
    member_pull(&fixture).await;
    assert_eq!(
        member_push(&fixture, "2026-07-24T00:00:20Z")
            .await
            .expect("member publishes after the reopen"),
        1,
        "the member publishes its old-epoch package after the reopen"
    );
    let published = match fixture
        .member_db
        .write_status(&member_write_id)
        .await
        .expect("read member write status after reopen")
    {
        crate::WriteStatus::Published(position) => *position,
        status => panic!("old-epoch member write did not publish after reopen: {status:?}"),
    };
    let (member_package_commit, _) = crate::sync::store::pull::load_commit_with_author(
        &fixture.store.storage,
        &fixture.store.root,
        published.commit(),
    )
    .await
    .expect("load the member's published old-epoch package commit");
    let member_package = member_package_commit
        .circle_packages()
        .iter()
        .find(|package| package.circle_id == fixture.circle_id)
        .expect("member commit carries its Circle package");
    assert_eq!(
        member_package.key_fingerprint, fixture.prior_fingerprint,
        "the member published under the restored old-epoch key"
    );
}

#[tokio::test]
async fn cancelling_a_finalized_close_is_refused() {
    let fixture = setup_closing_founder_circle("circle-cancel-refused").await;

    // Finalize the close: publish the sole owner-device response, then drive the
    // cycle that activates the exact outcome.
    let authorized_store =
        crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
            .await
            .expect("authorize Circle close response");
    authorized_store
        .publish_circle_epoch_close_responses(&fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the exact Circle epoch-close outcome");
    assert!(StoreDatabase::new(&fixture.db)
        .circle_operation(&fixture.operation_id)
        .await
        .expect("read finalized Circle removal operation")
        .is_none());

    // The final outcome won the slot; the operation is no longer waiting, so a
    // cancellation is refused with a typed reason and the removal stands.
    let error = fixture
        .components
        .cancel_circle_epoch_close(fixture.circle_id)
        .await
        .expect_err("cancel after finalization must be refused");
    assert!(
        matches!(
            error,
            CircleOperationError::NoCloseToCancel { circle_id }
                if circle_id == fixture.circle_id
        ),
        "{error}"
    );
    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load finalized successor Circle authoring state");
    assert_ne!(successor.control.value.epoch_id(), fixture.prior_epoch);
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
}

#[tokio::test]
async fn reopen_control_without_a_slot_cancellation_is_invalid() {
    let fixture = setup_closing_founder_circle("circle-cancel-sabotage").await;
    let owner_pubkey = keys::public_key_hex(&fixture.signer);

    // Prepare a legitimate reopen but do not publish it.
    let (current, activation_commit_ref) = StoreDatabase::new(&fixture.db)
        .circle_closing_context(fixture.circle_id, &owner_pubkey)
        .await
        .expect("load closing Circle context");
    let (activation_commit, author) = crate::sync::store::pull::load_commit_with_author(
        &fixture.store.storage,
        &fixture.store.root,
        &activation_commit_ref,
    )
    .await
    .expect("load closing activation commit");
    let previous_control = activation_commit
        .circle_controls()
        .iter()
        .find(|reference| {
            reference.circle_id() == fixture.circle_id
                && reference.control() == &current.control.coord
        })
        .expect("closing control is present in its activating commit")
        .clone();
    let device_id = local_device_id(&fixture.db).await;
    let journal = prepare_circle_operation_request(
        &StoreDatabase::new(&fixture.db),
        &fixture.store.storage,
        &device_id,
        super::super::commands::CircleOperationRequest::CancelEpochClose(Box::new(
            super::super::commands::CircleCancelEpochCloseRequest {
                operation_id: fixture.operation_id.clone(),
                circle_id: fixture.circle_id,
                member_pubkey: fixture.member_pubkey.clone(),
                current,
                previous_control,
            },
        )),
        &fixture.signer,
    )
    .await
    .expect("prepare Circle reopen");

    // The prepared reopen is a founder-origin active successor of the close, so a
    // verifier that keyed on the epoch origin would accept it. Publish every exact
    // object except the cancellation, leaving the outcome slot empty, and strip the
    // cancellation reference from the re-signed activating commit.
    let old_commit = journal.commit().expect("parse prepared reopen commit");
    for (step, object) in &journal.operation().prepared_objects {
        if step == "epoch-close-cancellation" || step == "store-commit" || step == "store-head" {
            continue;
        }
        crate::sync::store_objects::create_exact_object(&fixture.store.storage, object)
            .await
            .expect("publish reopen exact object");
    }
    let mut objects = old_commit.circle_controls()[0].objects().clone();
    assert!(
        objects.close_cancellation.is_some(),
        "legitimate reopen names its cancellation"
    );
    objects.close_cancellation = None;
    let mut journal = journal;
    let forged_reference = journal.operation().creation.control_ref(
        objects,
        Some(old_commit.circle_controls()[0].head_object().clone()),
    );
    resign_merge_journal_with_reference(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &mut journal,
        forged_reference,
        |_| {},
    )
    .await;

    let forged_commit = journal.commit().expect("parse forged reopen commit");
    let error = load_circle_activations(
        &fixture.db,
        &fixture.store.storage,
        &fixture.store.root,
        &journal.operation().commit_ref,
        &forged_commit,
        &author,
        &fixture.signer,
        &owner_pubkey,
    )
    .await
    .expect_err("reopen without a slot cancellation must fail activation");
    assert!(
        error
            .to_string()
            .contains("active successor of an epoch close carries no settlement"),
        "{error}"
    );
}

/// Prepare the Circle's reopen, begin its finalization, and persist it durably —
/// the `Finalizing` state a crash leaves before any object is published.
async fn begin_cancellation_finalization(fixture: &ClosingFounderCircle) {
    let owner_pubkey = keys::public_key_hex(&fixture.signer);
    let device_id = local_device_id(&fixture.db).await;
    let (current, activation_commit_ref) = StoreDatabase::new(&fixture.db)
        .circle_closing_context(fixture.circle_id, &owner_pubkey)
        .await
        .expect("load closing Circle context");
    let (activation_commit, _) = crate::sync::store::pull::load_commit_with_author(
        &fixture.store.storage,
        &fixture.store.root,
        &activation_commit_ref,
    )
    .await
    .expect("load closing activation commit");
    let previous_control = activation_commit
        .circle_controls()
        .iter()
        .find(|reference| {
            reference.circle_id() == fixture.circle_id
                && reference.control() == &current.control.coord
        })
        .expect("closing control is present in its activating commit")
        .clone();
    let prepared = prepare_circle_operation_request(
        &StoreDatabase::new(&fixture.db),
        &fixture.store.storage,
        &device_id,
        super::super::commands::CircleOperationRequest::CancelEpochClose(Box::new(
            super::super::commands::CircleCancelEpochCloseRequest {
                operation_id: fixture.operation_id.clone(),
                circle_id: fixture.circle_id,
                member_pubkey: fixture.member_pubkey.clone(),
                current,
                previous_control,
            },
        )),
        &fixture.signer,
    )
    .await
    .expect("prepare Circle reopen");
    let mut journal = StoreDatabase::new(&fixture.db)
        .circle_operation(&fixture.operation_id)
        .await
        .expect("read waiting Circle operation")
        .expect("waiting Circle operation is durable");
    journal
        .begin_finalization(prepared.operation().clone())
        .expect("begin cancellation finalization");
    StoreDatabase::new(&fixture.db)
        .begin_circle_operation_finalization(journal)
        .await
        .expect("persist cancellation finalization");
}

async fn assert_cancellation_reopened(fixture: &ClosingFounderCircle) {
    assert!(StoreDatabase::new(&fixture.db)
        .circle_operation(&fixture.operation_id)
        .await
        .expect("read completed cancellation operation")
        .is_none());
    let (reopened, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load reopened Circle authoring state");
    assert_eq!(reopened.control.value.epoch_id(), fixture.prior_epoch);
    assert_eq!(
        reopened.control.value.key_fingerprint(),
        fixture.prior_fingerprint
    );
    assert!(reopened
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
}

#[tokio::test]
async fn interrupted_cancellation_resumes_idempotently() {
    // A crash between beginning the cancellation's finalization and publishing it:
    // the durable operation resumes and completes exactly once. Its distinct write
    // identity keeps it from being re-derived as a finalization.
    let before_publication = setup_closing_founder_circle("circle-cancel-restart-before").await;
    begin_cancellation_finalization(&before_publication).await;
    assert_eq!(
        StoreDatabase::new(&before_publication.db)
            .circle_operation(&before_publication.operation_id)
            .await
            .expect("read persisted cancellation operation")
            .expect("cancellation operation is durable")
            .state(),
        CircleOperationState::Finalizing
    );
    resume_circle_operations(
        &before_publication.db,
        &before_publication.store.storage,
        &before_publication.signer,
    )
    .await
    .expect("resume the interrupted cancellation");
    resume_circle_operations(
        &before_publication.db,
        &before_publication.store.storage,
        &before_publication.signer,
    )
    .await
    .expect("second resume is idempotent");
    assert_cancellation_reopened(&before_publication).await;

    // A crash between publication and activation: the reopening control commit
    // reaches durable storage, but the head create fails before the operation
    // claims its device-stream head and records the activation. Resume finds the
    // commit already published and completes idempotently. The reopen publishes
    // 2*access + 5 exact objects (cancellation, access leaves, control, control
    // head, access envelopes, then the commit and the head); failing before the
    // final head create leaves the commit published and activation not recorded.
    let after_publication = setup_closing_founder_circle("circle-cancel-restart-after").await;
    begin_cancellation_finalization(&after_publication).await;
    let journal = StoreDatabase::new(&after_publication.db)
        .circle_operation(&after_publication.operation_id)
        .await
        .expect("read finalizing cancellation operation")
        .expect("cancellation operation is durable");
    let activations_before =
        activation_count(&after_publication.db, after_publication.circle_id).await;
    let head_create_call = 2 * journal.operation().creation.access.len() + 5;
    after_publication
        .store
        .home
        .fail_exact_create_before_call(head_create_call);

    let interrupted = resume_circle_operations(
        &after_publication.db,
        &after_publication.store.storage,
        &after_publication.signer,
    )
    .await
    .expect_err("the head create fails after the commit is published");
    assert!(
        matches!(interrupted, CircleOperationError::Object(_)),
        "{interrupted}"
    );
    assert_eq!(
        activation_count(&after_publication.db, after_publication.circle_id).await,
        activations_before,
        "the interrupted cancellation has not activated"
    );
    assert_eq!(
        StoreDatabase::new(&after_publication.db)
            .circle_operation(&after_publication.operation_id)
            .await
            .expect("read interrupted cancellation")
            .expect("the interrupted cancellation remains durable")
            .state(),
        CircleOperationState::Finalizing
    );

    resume_circle_operations(
        &after_publication.db,
        &after_publication.store.storage,
        &after_publication.signer,
    )
    .await
    .expect("resume completes the published-but-unactivated cancellation");
    assert_cancellation_reopened(&after_publication).await;
    assert_eq!(
        activation_count(&after_publication.db, after_publication.circle_id).await,
        activations_before + 1,
        "the completed cancellation activates its reopening control exactly once"
    );
}

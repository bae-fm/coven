use super::*;

#[path = "recovery_retry_tests.rs"]
mod recovery_retry_tests;

/// A restore failure while saving `config.yaml`, after the key and identity are
/// durable, rolls both back. Retrying the complete restore reuses the remote
/// recovery publication left by the failed local completion.
#[tokio::test]
async fn late_config_failure_rolls_back_custody_and_retries_recovery() {
    coven_keys::keys::test_keyring::install();

    let store_id = "late-step-rollback-test";
    let tmp = tempfile::tempdir().expect("temp dir");
    let layout = StoreLayout::new(tmp.path());
    let store_dir = layout.store_dir(store_id);
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud = Arc::new(
        coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        ),
    );
    let owner_keypair = UserKeypair::generate();
    let master_key = coven_keys::encryption::MasterKeyring::from(
        coven_keys::encryption::EncryptionService::from_key([0xbb; 32]),
    );
    let serialized_keyring = master_key.to_serialized();
    let cipher = CloudCipher::Encrypted(EncryptionService::from(master_key.clone()));
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
    let owner_storage = Arc::new(CloudSyncConnection::new(
        cloud.clone(),
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    ));

    let tables = test_synced_tables();
    let db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db = open_test_db(db_store_dir.clone());
    let owner_device = TestDevice::create(
        &db,
        db_store_dir.clone(),
        owner_storage.clone(),
        store_id,
        owner_keypair.clone(),
    )
    .await
    .expect("initialize owner Store");
    let store_root = owner_device.store_root().clone();
    let membership = owner_device
        .membership()
        .await
        .expect("load owner membership");
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let store_database = coven_database::StoreDatabase::new(&db);
    crate::test_snapshots::publish_owner_snapshot(
        &owner_device,
        &store_database,
        store_root.clone(),
        snap_tmp.path(),
    )
    .await;

    let joiner_keypair = owner_keypair.clone();
    let store_keys = StoreKeys::bind(store_id.to_string());
    let identity_custody =
        coven_keys::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
    let authority = owner_device.published_owner_recovery_authority(&owner_keypair);
    let migrations = test_migrations();
    let reached_config_save = std::cell::Cell::new(false);
    let result = crate::restoration::restore_from_cloud(
        store_id,
        store_root.clone(),
        Some(&serialized_keyring),
        "Late Step Test",
        &tables,
        &migrations,
        coven_database::CovenMigrationPolicy::ApplyPending,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        crate::restoration::RestoreSource::new(
            CloudHomeJoinInfo::CloudKit,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops.clone()),
        ),
        &MembershipFloor(membership.head_refs().to_vec()),
        &joiner_keypair,
        &authority,
        None,
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device-late")),
        |status| {
            if status == "Saving configuration..." {
                reached_config_save.set(true);
                assert!(
                    store_keys
                        .get_encryption_key()
                        .expect("read keyring before config save")
                        .is_some(),
                    "the encryption key is durable before the config marker",
                );
                assert_eq!(
                    identity_custody
                        .unlock()
                        .expect("read identity before config save")
                        .map(|keypair| keypair.public_key()),
                    Some(joiner_keypair.public_key()),
                    "the signing identity is durable before the config marker",
                );
                std::fs::create_dir_all(store_dir.config_path())
                    .expect("block the config file with a directory");
            }
        },
        &tokio::sync::watch::channel(false).1,
    )
    .await;

    let err = result.expect_err("the blocked config.yaml write must fail restore");
    assert!(
        matches!(&err, BootstrapError::Config(_)),
        "restore must reach the blocked config save, got {err:?}"
    );
    assert!(
        reached_config_save.get(),
        "the restore reached the final completion marker"
    );
    let candidate_prefix = "store-v1/candidates/";
    assert!(
        !store_dir.exists(),
        "the failed restore removes its store directory",
    );
    assert!(
        store_keys
            .get_encryption_key()
            .expect("read keyring")
            .is_none(),
        "the encryption key must be rolled back",
    );
    assert!(
        identity_custody
            .unlock()
            .expect("read identity custody")
            .is_none(),
        "the imported identity must be rolled back",
    );
    owner_device
        .publish_fixture_position("history-after-recovery-head")
        .await;
    let candidates_before_retry = cloud
        .list(candidate_prefix)
        .await
        .expect("list candidate objects before recovery retry");

    let retry = Box::pin(crate::restoration::restore_from_cloud(
        store_id,
        store_root,
        Some(&serialized_keyring),
        "Late Step Test",
        &tables,
        &migrations,
        coven_database::CovenMigrationPolicy::ApplyPending,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        crate::restoration::RestoreSource::new(
            CloudHomeJoinInfo::CloudKit,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops),
        ),
        &MembershipFloor(membership.head_refs().to_vec()),
        &joiner_keypair,
        &authority,
        None,
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device-late")),
        |_status| {},
        &tokio::sync::watch::channel(false).1,
    ))
    .await
    .expect("retry reuses the activated recovery registration");
    assert_eq!(retry.device_id, "device-late-0");
    assert_eq!(
        cloud
            .list(candidate_prefix)
            .await
            .expect("list candidate objects after retry"),
        candidates_before_retry,
        "retry must reuse the recovery commit",
    );
}

#[tokio::test]
async fn merge_owner_recovery_restore_code_creates_an_activated_replacement_device() {
    let fixture = Box::pin(prepare_owner_recovery_restore("owner-recovery-restore")).await;
    Box::pin(fixture.assert_restored()).await;
}

struct OwnerRecoveryRestoreFixture {
    code: String,
    store_id: String,
    owner: UserKeypair,
    source_device: TestDevice,
    owner_pubkey: String,
    tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    cloudkit_ops: Arc<RestoreCloudKitOps>,
    app: tempfile::TempDir,
}

impl OwnerRecoveryRestoreFixture {
    async fn assert_restored(self) {
        let Self {
            code,
            store_id: _,
            owner: _,
            source_device: _,
            owner_pubkey,
            tables,
            migrations,
            cloudkit_ops,
            app,
        } = self;
        let layout = StoreLayout::new(app.path());
        let config = Box::pin(restore_from_code(
            &code,
            &tables,
            &migrations,
            coven_database::CovenMigrationPolicy::ApplyPending,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops),
            &layout,
            Arc::new(SystemClock),
            Arc::new(SequentialIdProvider::new("unused-recovery-device")),
            |_status: &str| {},
            &tokio::sync::watch::channel(false).1,
        ))
        .await
        .expect("restore through OwnerRecovery code");
        let store_dir = layout.store_dir(&config.store_id);
        let restored = Database::open(
            &store_dir.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &migrations,
        )
        .expect("open recovered database");
        let store_device_id = restored
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load recovered Store device identity")
            .expect("recovered Store device identity exists");
        assert_eq!(
            restored
                .get_protocol_state(coven_protocol::membership::OWNER_PUBKEY_STATE_KEY)
                .await
                .expect("load recovered Store owner"),
            Some(owner_pubkey),
            "restore pins the verified chain founder as the Store owner",
        );
        let activation: coven_protocol::store_commit::StoreDeviceRegistrationActivation = restored
            .store_device_registration_activation_for_test(&store_device_id)
            .await
            .expect("load config device activation");
        assert!(matches!(
            activation,
            coven_protocol::store_commit::StoreDeviceRegistrationActivation::Recovery { .. }
        ));
    }
}

/// `store_id` keys the process-global test keyring, so every test takes its
/// own: two tests restoring one id would find each other's owner identity.
async fn prepare_owner_recovery_restore(store_id: &str) -> OwnerRecoveryRestoreFixture {
    coven_keys::keys::test_keyring::install();
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud = Arc::new(
        coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        ),
    );
    let owner = UserKeypair::generate();
    let owner_storage = Arc::new(CloudSyncConnection::new(
        cloud.clone(),
        CloudCipher::Plaintext,
        BlobPathScheme::for_storage(HomeStorage::Browsable),
        store_id.to_string(),
        owner.clone(),
    ));
    let owner_db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let owner_db = open_test_db(owner_db_store_dir.clone());
    let owner_device = TestDevice::create(
        &owner_db,
        owner_db_store_dir.clone(),
        owner_storage.clone(),
        store_id,
        owner.clone(),
    )
    .await
    .expect("initialize recovery Store");
    let root = owner_device.store_root().clone();
    let membership = owner_device
        .membership()
        .await
        .expect("load recovery membership");
    let floor = MembershipFloor(membership.head_refs().to_vec());
    let tables = test_synced_tables();
    let snapshot_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let store_database = coven_database::StoreDatabase::new(&owner_db);
    crate::test_snapshots::publish_owner_snapshot(
        &owner_device,
        &store_database,
        root.clone(),
        snapshot_tmp.path(),
    )
    .await;
    let authority = owner_device.published_owner_recovery_authority(&owner);
    let code = encode_restore_code(&RestoreCode {
        v: RESTORE_CODE_VERSION,
        sid: store_id.to_string(),
        ek: None,
        name: "Recovered Store".to_string(),
        provider: CloudHomeJoinInfo::CloudKit,
        store_root: root,
        founder_pubkey: pubkey_hex(&owner),
        membership_floor: floor,
        authority,
    });
    let app = tempfile::tempdir().expect("restore app dir");
    OwnerRecoveryRestoreFixture {
        code,
        store_id: store_id.to_string(),
        owner_pubkey: pubkey_hex(&owner),
        owner,
        source_device: owner_device,
        tables,
        migrations: test_migrations(),
        cloudkit_ops,
        app,
    }
}

/// The device an Owner recovery code registers is a device like any other
/// once the restore completes: its first sync cycle pulls and publishes.
#[tokio::test]
async fn a_recovered_owner_device_runs_its_first_sync_cycle() {
    Box::pin(async {
        let fixture = Box::pin(prepare_owner_recovery_restore("owner-recovery-first-cycle")).await;
        let OwnerRecoveryRestoreFixture {
            code,
            store_id,
            owner,
            source_device: _,
            owner_pubkey: _,
            tables,
            migrations: _,
            cloudkit_ops,
            app,
        } = fixture;
        let layout = StoreLayout::new(app.path());
        let config = Box::pin(restore_from_code(
            &code,
            &tables,
            &test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops.clone()),
            &layout,
            Arc::new(SystemClock),
            Arc::new(SequentialIdProvider::new("recovery-device")),
            |_status: &str| {},
            &tokio::sync::watch::channel(false).1,
        ))
        .await
        .expect("restore through OwnerRecovery code");
        let store_dir = layout.store_dir(&config.store_id);
        let database = Database::open(
            &store_dir.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &test_migrations(),
        )
        .expect("open recovered database");
        let storage = Arc::new(CloudSyncConnection::new(
            Arc::new(
                coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                    cloudkit_ops,
                    coven_foundation::config::ExactUploadVerification::MetadataHash,
                ),
            ),
            CloudCipher::Plaintext,
            BlobPathScheme::for_storage(HomeStorage::Browsable),
            store_id.clone(),
            owner.clone(),
        ));
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            coven_database::StoreDatabase::new(&database),
            store_dir,
        )
        .run_sync_cycle(storage, owner)
        .await
        .expect("the recovered device's first cycle");
    })
    .await;
}

/// An Owner recovery code derives one device from its authority, so a second
/// restore through the same code adopts the device the first restore
/// registered. That device published after the first restore; the second
/// restore must adopt the accepted snapshot and resume the acknowledgement
/// head the provider holds instead of reusing occupied publication slots.
#[tokio::test]
async fn a_repeated_owner_recovery_restore_resumes_the_device_s_published_streams() {
    Box::pin(async {
        let fixture = Box::pin(prepare_owner_recovery_restore("owner-recovery-repeated")).await;
        let OwnerRecoveryRestoreFixture {
            code,
            store_id,
            owner,
            source_device: _,
            owner_pubkey: _,
            tables,
            migrations: _,
            cloudkit_ops,
            app: first_app,
        } = fixture;
        let restore = |app_dir: std::path::PathBuf| {
            let code = code.clone();
            let tables = tables.clone();
            let cloudkit_ops = cloudkit_ops.clone();
            async move {
                let layout = StoreLayout::new(&app_dir);
                let config = Box::pin(restore_from_code(
                    &code,
                    &tables,
                    &test_migrations(),
                    coven_database::CovenMigrationPolicy::ApplyPending,
                    coven_foundation::config::ExactUploadVerification::MetadataHash,
                    coven_protocol::blob::TransferLimits::one_at_a_time(),
                    coven_keys::custody::KeyCustody::Keyring,
                    coven_keys::identity_custody::IdentityCustody::Keyring,
                    coven_storage::oauth::OAuthClients::empty(),
                    None,
                    Some(cloudkit_ops),
                    &layout,
                    Arc::new(SystemClock),
                    Arc::new(SequentialIdProvider::new("recovery-device")),
                    |_status: &str| {},
                    &tokio::sync::watch::channel(false).1,
                ))
                .await
                .expect("restore through OwnerRecovery code");
                let store_dir = layout.store_dir(&config.store_id);
                let database = Database::open(
                    &store_dir.db_path(),
                    tables,
                    coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                    coven_protocol::blob::TransferLimits::one_at_a_time(),
                    config.device_id.clone(),
                    std::sync::Arc::new(coven_foundation::clock::SystemClock),
                    coven_database::CovenMigrationPolicy::ApplyPending,
                    &test_migrations(),
                )
                .expect("open recovered database");
                (database, store_dir)
            }
        };
        let storage = || {
            Arc::new(CloudSyncConnection::new(
                Arc::new(
                    coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                        cloudkit_ops.clone(),
                        coven_foundation::config::ExactUploadVerification::MetadataHash,
                    ),
                ),
                CloudCipher::Plaintext,
                BlobPathScheme::for_storage(HomeStorage::Browsable),
                store_id.clone(),
                owner.clone(),
            ))
        };

        // Publish through the recovered device before restoring the same code again.
        let (first_db, first_dir) = restore(first_app.path().to_path_buf()).await;
        let first_device = first_db
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load recovered Store device identity")
            .expect("recovered Store device identity exists");
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            coven_database::StoreDatabase::new(&first_db),
            first_dir.clone(),
        )
        .run_sync_cycle(storage(), owner.clone())
        .await
        .expect("the recovered device's first cycle");
        let first_store = coven_database::StoreDatabase::new(&first_db);
        let first_writer = TestDevice::load(&first_db, first_dir, storage(), owner.clone())
            .await
            .expect("reopen the recovered device");
        let snapshot_dir = tempfile::tempdir().expect("recovered snapshot directory");
        crate::test_snapshots::publish_owner_snapshot(
            &first_writer,
            &first_store,
            first_writer.store_root().clone(),
            snapshot_dir.path(),
        )
        .await;
        let published = first_store
            .latest_local_store_snapshot()
            .await
            .expect("read the recovered device's published snapshot")
            .expect("the recovered device published a snapshot");
        let accepted_snapshot = first_store
            .store_current_publication()
            .await
            .expect("read the accepted snapshot boundary")
            .record()
            .latest_snapshot()
            .cloned()
            .expect("the snapshot is accepted");
        assert_eq!(accepted_snapshot.snapshot, published.reference);
        let published_ack = first_store
            .latest_local_store_ack()
            .await
            .expect("read the recovered device's acknowledgement head")
            .expect("the first cycle acknowledged");
        assert!(
            published_ack.reference.sequence > 1,
            "the first cycle published past the initial acknowledgement"
        );
        let snapshot_objects = storage()
            .list_provider_keys_for_test("store-v1/snapshots/")
            .await
            .expect("list Store snapshot objects");

        // The second restore through the same code adopts the same device.
        let second_app = tempfile::tempdir().expect("second restore app dir");
        let (second_db, second_dir) = restore(second_app.path().to_path_buf()).await;
        let second_device = second_db
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load re-recovered Store device identity")
            .expect("re-recovered Store device identity exists");
        assert_eq!(
            second_device, first_device,
            "the same authority derives the same device"
        );
        let second_store = coven_database::StoreDatabase::new(&second_db);
        let restored_baseline = second_store
            .installed_replay_baseline()
            .await
            .expect("read the restored snapshot baseline");
        assert_eq!(restored_baseline.snapshot(), Some(&published));
        assert_eq!(
            second_store
                .store_current_publication()
                .await
                .expect("read the restored publication boundary")
                .record()
                .latest_snapshot(),
            Some(&accepted_snapshot),
            "the restore resumes from the exact accepted snapshot"
        );
        assert_eq!(
            second_store
                .latest_local_store_ack()
                .await
                .expect("read the resumed acknowledgement head")
                .map(|ack| ack.reference),
            Some(published_ack.reference.clone()),
            "the restore resumes at the acknowledgement head the device published"
        );

        // Its first cycle stands on the resumed streams: nothing is due, and
        // nothing it publishes lands on a slot the device already wrote.
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            second_store.clone(),
            second_dir,
        )
        .run_sync_cycle(storage(), owner.clone())
        .await
        .expect("the re-recovered device's first cycle");
        let baseline_after = second_store
            .installed_replay_baseline()
            .await
            .expect("read the snapshot baseline after the cycle");
        assert_eq!(baseline_after.snapshot(), Some(&published));
        assert_eq!(
            second_store
                .store_current_publication()
                .await
                .expect("read the publication after the cycle")
                .record()
                .latest_snapshot(),
            Some(&accepted_snapshot)
        );
        assert_eq!(
            storage()
                .list_provider_keys_for_test("store-v1/snapshots/")
                .await
                .expect("list Store snapshot objects"),
            snapshot_objects,
            "the cycle rewrote no snapshot object"
        );
    })
    .await;
}

/// A restored continuation adopts the accepted snapshot and publishes its next
/// snapshot after the current shared publication boundary.
#[tokio::test]
async fn a_restored_continuation_extends_the_accepted_snapshot() {
    Box::pin(async {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-anti-clobber-test";
        let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
        let cloud = coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        );
        let cipher = CloudCipher::Plaintext;
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Browsable);
        let tables = test_synced_tables();
        let owner_keypair = UserKeypair::generate();

        let owner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(cloud.clone()) as Arc<dyn coven_storage::cloud::ExactCloudHome>,
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));

        // Owner: a store with one shared note, captured straight into the published
        // snapshot — the shape a device sees the first time it opens a shared store.
        let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
        let db_owner = open_test_db(db_owner_store_dir.clone());
        let owner_device = TestDevice::create(
            &db_owner,
            db_owner_store_dir.clone(),
            owner_storage.clone(),
            store_id,
            owner_keypair.clone(),
        )
        .await
        .expect("initialize owner Store");
        let store_root = owner_device.store_root().clone();
        let membership = owner_device
            .membership()
            .await
            .expect("load owner membership");
        db_owner
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;
        let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
        let store_database = coven_database::StoreDatabase::new(&db_owner);
        crate::test_snapshots::publish_owner_snapshot(
            &owner_device,
            &store_database,
            store_root.clone(),
            snap_tmp.path(),
        )
        .await;

        let expected_snapshot = store_database
            .store_current_publication()
            .await
            .expect("read the accepted snapshot publication")
            .record()
            .latest_snapshot()
            .cloned()
            .expect("snapshot is accepted");

        // Device B restores through the public restore-code service over its own
        // CloudKit home onto the same records.
        let app = Arc::new(tempfile::tempdir().expect("restore app dir"));
        let joiner_keypair = owner_keypair.clone();
        let continuation = owner_device
            .export_activated_device_continuation()
            .await
            .expect("export exact activated continuation");
        let expected_position = continuation.latest_position.clone();
        let restore_code = encode_restore_code(&RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: store_id.to_string(),
            ek: None,
            name: "Restored Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKit,
            store_root: store_root.clone(),
            founder_pubkey: pubkey_hex(&owner_keypair),
            membership_floor: MembershipFloor(membership.head_refs().to_vec()),
            authority: RestoreAuthority::ActivatedContinuation(continuation),
        });
        let decoded = decode_restore_code(&restore_code).expect("decode continuation restore code");
        let RestoreAuthority::ActivatedContinuation(decoded_continuation) = decoded.authority
        else {
            panic!("decoded restore authority changed variants");
        };
        assert_eq!(decoded_continuation.latest_position, expected_position);
        let restore_app = app.clone();
        let restore_tables = tables.clone();
        let restore_cloudkit = cloudkit_ops.clone();
        let config = tokio::spawn(async move {
            let layout = StoreLayout::new(restore_app.path());
            let cancel = tokio::sync::watch::channel(false).1;
            restore_from_code(
                &restore_code,
                &restore_tables,
                &test_migrations(),
                coven_database::CovenMigrationPolicy::ApplyPending,
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                coven_keys::custody::KeyCustody::Keyring,
                coven_keys::identity_custody::IdentityCustody::Keyring,
                coven_storage::oauth::OAuthClients::empty(),
                None,
                Some(restore_cloudkit),
                &layout,
                Arc::new(SystemClock),
                Arc::new(SequentialIdProvider::new("device-b")),
                |_status: &str| {},
                &cancel,
            )
            .await
        })
        .await
        .expect("restore task completes")
        .expect("restore through code service");
        let layout = StoreLayout::new(app.path());
        let lib_b = layout.store_dir(&config.store_id);
        let store_keys = StoreKeys::bind(store_id.to_string());
        let identity_custody =
            coven_keys::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &lib_b);

        // The config is saved, and a saved config implies the identity was imported
        // before it — the restored device's signing identity resolves in custody.
        assert_eq!(
            identity_custody
                .unlock()
                .expect("read restored identity")
                .map(|kp| kp.public_key()),
            Some(joiner_keypair.public_key()),
            "a completed restore has its signing identity in custody",
        );
        let other_store_keys = StoreKeys::bind("restore-anti-clobber-other-store".to_string());
        let other_identity_custody = coven_keys::identity_custody::IdentityCustody::Keyring
            .resolve(
                &other_store_keys,
                &layout.store_dir("restore-anti-clobber-other-store"),
            );
        assert!(
            other_identity_custody
                .unlock()
                .expect("read unrelated store identity")
                .is_none(),
            "restoring one store establishes no identity for another store",
        );

        let db_b = Database::open(
            &lib_b.db_path(),
            tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &test_migrations(),
        )
        .expect("open B db");

        // B's first real sync cycle, with no local changes of its own.
        let joiner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(
                coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                    cloudkit_ops,
                    coven_foundation::config::ExactUploadVerification::MetadataHash,
                ),
            ),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            joiner_keypair.clone(),
        ));
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            coven_database::StoreDatabase::new(&db_b),
            lib_b.clone(),
        )
        .run_sync_cycle(joiner_storage.clone(), joiner_keypair.clone())
        .await
        .expect("run B sync cycle");
        let restored_database = coven_database::StoreDatabase::new(&db_b);
        let previous = restored_database
            .store_current_publication()
            .await
            .expect("read the restored shared publication boundary")
            .record()
            .clone();
        assert_eq!(previous.latest_snapshot(), Some(&expected_snapshot));
        let restored_device = TestDevice::load(&db_b, lib_b, joiner_storage, joiner_keypair)
            .await
            .expect("reopen the continued device");
        crate::test_snapshots::publish_owner_snapshot(
            &restored_device,
            &restored_database,
            store_root,
            snap_tmp.path(),
        )
        .await;
        let successor = restored_database
            .latest_local_store_snapshot()
            .await
            .expect("read the successor snapshot")
            .expect("successor was published");
        assert_eq!(successor.meta.publication_predecessor, previous);
        assert_eq!(
            restored_database
                .store_current_publication()
                .await
                .expect("read the successor publication")
                .record()
                .latest_snapshot()
                .expect("successor is accepted")
                .snapshot,
            successor.reference,
        );
    })
    .await;
}

/// Restore discovers the accepted cloud snapshot even when it was published
/// after the continuation code was exported, and can publish its successor.
#[tokio::test]
async fn restore_discovers_a_snapshot_published_after_the_code_was_exported() {
    Box::pin(async {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-stale-snapshot-cursor-test";
        let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
        let cloud = coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        );
        let cipher = CloudCipher::Plaintext;
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Browsable);
        let tables = test_synced_tables();
        let owner_keypair = UserKeypair::generate();

        let owner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(cloud.clone()) as Arc<dyn coven_storage::cloud::ExactCloudHome>,
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));

        let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
        let db_owner = open_test_db(db_owner_store_dir.clone());
        let owner_device = TestDevice::create(
            &db_owner,
            db_owner_store_dir.clone(),
            owner_storage.clone(),
            store_id,
            owner_keypair.clone(),
        )
        .await
        .expect("initialize owner Store");
        let store_root = owner_device.store_root().clone();
        let membership = owner_device
            .membership()
            .await
            .expect("load owner membership");
        db_owner
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;

        // The code is exported before the device publishes any snapshot.
        let continuation = owner_device
            .export_activated_device_continuation()
            .await
            .expect("export exact activated continuation");
        let store_database = coven_database::StoreDatabase::new(&db_owner);
        assert!(store_database
            .store_current_publication()
            .await
            .expect("read the publication at export")
            .record()
            .latest_snapshot()
            .is_none());

        let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
        crate::test_snapshots::publish_owner_snapshot(
            &owner_device,
            &store_database,
            store_root.clone(),
            snap_tmp.path(),
        )
        .await;
        let published = store_database
            .latest_local_store_snapshot()
            .await
            .expect("read the published snapshot")
            .expect("snapshot was published");

        let app = Arc::new(tempfile::tempdir().expect("restore app dir"));
        let restore_code = encode_restore_code(&RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: store_id.to_string(),
            ek: None,
            name: "Restored Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKit,
            store_root: store_root.clone(),
            founder_pubkey: pubkey_hex(&owner_keypair),
            membership_floor: MembershipFloor(membership.head_refs().to_vec()),
            authority: RestoreAuthority::ActivatedContinuation(continuation),
        });
        let restore_app = app.clone();
        let restore_tables = tables.clone();
        let restore_cloudkit = cloudkit_ops.clone();
        let config = tokio::spawn(async move {
            let layout = StoreLayout::new(restore_app.path());
            let cancel = tokio::sync::watch::channel(false).1;
            restore_from_code(
                &restore_code,
                &restore_tables,
                &test_migrations(),
                coven_database::CovenMigrationPolicy::ApplyPending,
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                coven_keys::custody::KeyCustody::Keyring,
                coven_keys::identity_custody::IdentityCustody::Keyring,
                coven_storage::oauth::OAuthClients::empty(),
                None,
                Some(restore_cloudkit),
                &layout,
                Arc::new(SystemClock),
                Arc::new(SequentialIdProvider::new("device-b")),
                |_status: &str| {},
                &cancel,
            )
            .await
        })
        .await
        .expect("restore task completes")
        .expect("restore through code service");
        let layout = StoreLayout::new(app.path());
        let lib_b = layout.store_dir(&config.store_id);
        let db_b = Database::open(
            &lib_b.db_path(),
            tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &test_migrations(),
        )
        .expect("open B db");

        let restored_database = coven_database::StoreDatabase::new(&db_b);
        let restored_baseline = restored_database
            .installed_replay_baseline()
            .await
            .expect("read the restored snapshot baseline");
        let restored = restored_baseline
            .snapshot()
            .expect("restore adopted the snapshot");
        assert_eq!(restored.reference, published.reference);
        assert_eq!(restored.meta, published.meta);

        let joiner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(
                coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                    cloudkit_ops,
                    coven_foundation::config::ExactUploadVerification::MetadataHash,
                ),
            ),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            restored_database.clone(),
            lib_b.clone(),
        )
        .run_sync_cycle(joiner_storage.clone(), owner_keypair.clone())
        .await
        .expect("run the restored device's first cycle");
        let previous = restored_database
            .store_current_publication()
            .await
            .expect("read the current publication")
            .record()
            .clone();
        assert_eq!(
            previous
                .latest_snapshot()
                .expect("restored snapshot is accepted")
                .snapshot,
            published.reference
        );
        let restored_device = TestDevice::load(&db_b, lib_b, joiner_storage, owner_keypair)
            .await
            .expect("reopen the continued device");
        crate::test_snapshots::publish_owner_snapshot(
            &restored_device,
            &restored_database,
            store_root,
            snap_tmp.path(),
        )
        .await;
        let successor = restored_database
            .latest_local_store_snapshot()
            .await
            .expect("read the successor snapshot")
            .expect("successor was published");
        assert_eq!(successor.meta.publication_predecessor, previous);
        assert_ne!(successor.reference, published.reference);
    })
    .await;
}

/// A restore cannot install a snapshot whose selected membership head has
/// disappeared from the root's successor path, even when the snapshot carries
/// that head's signed bytes and the restore code requires its exact floor.
#[tokio::test]
async fn a_fresh_restorer_refuses_a_rolled_back_membership_head_during_bootstrap() {
    let owner = UserKeypair::generate();
    let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db_owner = open_test_db(db_owner_store_dir.clone());
    let fixture = TestStore::create_with_connection(
        &db_owner,
        db_owner_store_dir.clone(),
        "test-lib",
        owner.clone(),
        coven_replication::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact owner Store");
    let (storage, cloud_storage) = fixture;
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    storage
        .admit_member(
            &db_owner,
            db_owner_store_dir.clone(),
            &owner,
            &pubkey_hex(&member),
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("add member");
    let owner_device = storage
        .bind_device(&db_owner, db_owner_store_dir.clone(), &owner)
        .await
        .expect("bind owner Store");
    let pre_removal_chain = owner_device
        .membership()
        .await
        .expect("load pre-removal membership");
    let pre_removal_heads = pre_removal_chain.head_refs().to_vec();
    let custody = coven_replication::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    storage
        .remove_member(
            &db_owner,
            db_owner_store_dir.clone(),
            &owner,
            &pubkey_hex(&member),
            &encryption,
            &custody,
        )
        .await
        .expect("remove member");
    let chain = owner_device
        .membership()
        .await
        .expect("load post-removal membership");

    // The restore code is minted right after the removal: its floor is the
    // current (post-removal) chain state.
    let membership_floor = MembershipFloor(chain.head_refs().to_vec());
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let store_database = coven_database::StoreDatabase::new(&db_owner);
    let snapshot = store_database
        .capture_snapshot_image_for_test(storage.root(), snap_dir, None)
        .await
        .expect("owner snapshot");
    // Deliberately unacknowledged: this case is about the membership floor, not
    // about stability. The coverage is still the frontier the image holds.
    owner_device
        .publish_snapshot(
            snapshot,
            crate::test_snapshots::captured_coverage(&store_database).await,
        )
        .await
        .expect("publish post-removal snapshot");

    for head in chain.head_refs() {
        if !pre_removal_heads.contains(head) {
            cloud_storage
                .delete_protocol_object(&head.object)
                .await
                .expect("remove post-removal membership head");
        }
    }

    let (_tmp_b, lib_b) = temp_store_dir();
    let error = storage
        .prepare_snapshot_bootstrap(
            &membership_floor,
            1,
            &lib_b.db_path(),
            &UserKeypair::generate(),
        )
        .await
        .expect_err("the restore must enforce its floor before accepting a snapshot");

    assert!(
        matches!(
            &error,
            coven_replication::sync::store::SnapshotError::StoreHistory(
                coven_replication::sync::store::StorePullError::MembershipChain(
                    coven_replication::sync::store::AnchoredChainError::LoadFailed(message)
                )
            ) if message == "snapshot membership does not select an exact rooted head in accepted authority"
        ),
        "{error}"
    );
    assert!(
        !lib_b.db_path().exists(),
        "refused membership must not install the snapshot image"
    );
}

/// Restore bootstrap installs the complete row graph without downloading eager
/// blobs. CacheEager materialization belongs to the connected post-open worker,
/// so restore completion is never coupled to artwork availability.
#[tokio::test]
async fn restore_bootstrap_defers_eager_blob_files_until_open() {
    Box::pin(async {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-blob-backfill-test";
        let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
        let cloud = Arc::new(
            coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                cloudkit_ops.clone(),
                coven_foundation::config::ExactUploadVerification::MetadataHash,
            ),
        );
        let master_key =
            coven_keys::encryption::MasterKeyring::from(EncryptionService::from_key([7u8; 32]));
        let serialized_keyring = master_key.to_serialized();
        let cipher = CloudCipher::Encrypted(EncryptionService::from(master_key));
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
        let tables = test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        ));
        let owner_keypair = UserKeypair::generate();

        let owner_storage = Arc::new(CloudSyncConnection::new(
            cloud.clone(),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));

        // Owner: a shared note with a cover photo, both captured into the snapshot.
        let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
        let db_owner = open_test_db_with_blob(
            db_owner_store_dir.clone(),
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
        );
        let owner_device = Box::pin(TestDevice::create(
            &db_owner,
            db_owner_store_dir.clone(),
            owner_storage,
            store_id,
            owner_keypair.clone(),
        ))
        .await
        .expect("initialize owner Store");
        let store_root = owner_device.store_root().clone();
        db_owner
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;
        db_owner
            .execute_test_host_write(&format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-owner', '2026-01-01')",
                coven_protocol::blob::content_hash(b"cover-bytes"),
            ))
            .await;
        coven_foundation::store_dir::StoreDir::store_local_blob(
            &db_owner_store_dir,
            "photos",
            "photo1",
            b"cover-bytes",
        )
        .await
        .expect("stage owner blob");
        let cycle_storage = CloudSyncConnection::new(
            cloud.clone(),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        );
        Box::pin(
            coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
                coven_database::StoreDatabase::new(&db_owner),
                db_owner_store_dir.clone(),
            )
            .run_sync_cycle(cycle_storage, owner_keypair.clone()),
        )
        .await
        .expect("publish owner row and blob");
        let membership = owner_device
            .membership()
            .await
            .expect("load owner membership");
        let restore_app = tempfile::tempdir().expect("restore app dir");
        let layout = StoreLayout::new(restore_app.path());
        let lib_b = layout.store_dir(store_id);
        let owner_blob = db_owner
            .row_blob_ref("note_photos", "photo1")
            .await
            .expect("capture exact snapshot blob");
        let expected_blob = lib_b
            .cache_blob_path(
                "photos",
                owner_blob
                    .stored()
                    .expect("published snapshot blob has exact storage")
                    .locator()
                    .locator_hash(),
            )
            .expect("cache blob path");

        let joiner_keypair = owner_keypair.clone();
        let continuation = owner_device
            .export_activated_device_continuation()
            .await
            .expect("export exact activated continuation");
        let materialized_commits_without_device_state = db_owner
            .materialized_commits_without_device_state_count_for_test()
            .await
            .expect("verify source device-state snapshots");
        assert_eq!(materialized_commits_without_device_state, 0);
        let published_snapshot_bytes = db_owner
            .latest_published_store_snapshot_bytes_for_test()
            .await
            .expect("read published snapshot metadata");
        let published_snapshot: coven_protocol::store_commit::SnapshotMeta =
            serde_json::from_slice(&published_snapshot_bytes)
                .expect("parse published snapshot metadata");
        let snapshot_coverage = published_snapshot.coverage.clone().into_refs();
        let snapshot_frontier =
            coven_protocol::store_commit::CommitFrontier::from_refs(snapshot_coverage.clone())
                .expect("snapshot coverage has valid stream ids");
        let latest_position = continuation
            .latest_position
            .as_ref()
            .expect("continuation has a latest Store position");
        let source_registration = coven_protocol::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &store_root,
            continuation.registration.device_id,
        )
        .expect("parse continuation Store registration");
        let mut expected_device_snapshots = db_owner
            .store_device_state_snapshot_refs_for_test()
            .await
            .expect("load accepted device-state references")
            .into_iter()
            .filter(|reference| snapshot_frontier.covers_commit(reference))
            .collect::<std::collections::BTreeSet<_>>();
        let ancestry = owner_device
            .load_commit_ancestry_until(latest_position.clone(), &snapshot_frontier)
            .await
            .expect("load continuation ancestry");
        for (reference, commit) in ancestry {
            expected_device_snapshots.insert(reference);
            assert_eq!(commit.author(), &source_registration);
        }
        let device_signing_key: [u8; coven_keys::keys::SIGN_SECRETKEYBYTES] =
            hex::decode(&continuation.device_signing_secret)
                .expect("decode continuation device signing key")
                .try_into()
                .expect("continuation device signing key length");
        let device_signer = UserKeypair::from_signing_key_bytes(&device_signing_key)
            .expect("restore continuation device signer");
        let authority = RestoreAuthority::ActivatedContinuation(continuation.clone());

        let config = Box::pin(crate::restoration::restore_from_cloud(
            store_id,
            store_root,
            Some(&serialized_keyring),
            "Restored Store",
            &tables,
            &test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            crate::restoration::RestoreSource::new(
                CloudHomeJoinInfo::CloudKit,
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                coven_storage::oauth::OAuthClients::empty(),
                None,
                Some(cloudkit_ops),
            ),
            &MembershipFloor(membership.head_refs().to_vec()),
            &joiner_keypair,
            &authority,
            Some(&device_signer),
            &layout,
            Arc::new(SystemClock),
            Arc::new(SequentialIdProvider::new("unused-continuation-device")),
            |_status| {},
            &tokio::sync::watch::channel(false).1,
        ))
        .await
        .expect("restore bootstrap installs the snapshot rows");

        assert!(
            !expected_blob.exists(),
            "the cover blob file must remain remote after restore at {}",
            expected_blob.display(),
        );

        let restored = Database::open(
            &lib_b.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id,
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            coven_database::CovenMigrationPolicy::ApplyPending,
            &test_migrations(),
        )
        .expect("open restored database");
        let (restored_notes, restored_photos, restored_parent_links, foreign_key_violations) =
            restored
                .restored_row_graph_counts_for_test()
                .await
                .expect("inspect restored snapshot rows");
        assert_eq!(
            (
                restored_notes,
                restored_photos,
                restored_parent_links,
                foreign_key_violations,
            ),
            (1, 1, 1, 0)
        );
        let restored_device_snapshots = restored
            .store_device_state_snapshot_refs_for_test()
            .await
            .expect("load restored device-state snapshots")
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(restored_device_snapshots, expected_device_snapshots);
        restored
            .execute_test_host_write(
                "UPDATE note_photos
         SET _updated_at = '0000000002000-0000-restored'
         WHERE id = 'photo1'",
            )
            .await;
        let restored_storage = CloudSyncConnection::new(
            cloud,
            cipher,
            blob_paths,
            store_id.to_string(),
            joiner_keypair.clone(),
        );
        Box::pin(
            coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
                coven_database::StoreDatabase::new(&restored),
                lib_b.clone(),
            )
            .run_sync_cycle(restored_storage, joiner_keypair),
        )
        .await
        .expect("publish restored row by reusing its exact remote blob");
    })
    .await;
}

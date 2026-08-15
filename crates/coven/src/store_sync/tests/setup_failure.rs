use super::*;

struct FailingSyncLoopRuntimeFactory;

impl coven_replication::sync::sync_loop::SyncLoopRuntimeFactory for FailingSyncLoopRuntimeFactory {
    fn prepare(
        &self,
    ) -> Result<
        coven_replication::sync::sync_loop::PreparedSyncLoopRuntime,
        coven_replication::sync::sync_loop::SyncLoopError,
    > {
        Err(
            coven_replication::sync::sync_loop::SyncLoopError::ThreadSpawn(std::io::Error::other(
                "forced sync runtime failure",
            )),
        )
    }
}

#[tokio::test]
async fn sync_runtime_failure_precedes_key_credentials_and_store_publication() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-cloud-home-runtime-failure";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Opaque;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let previous = coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "old-access".to_string(),
        secret_key: "old-secret".to_string(),
    };
    store_keys
        .set_cloud_home_credentials(&previous)
        .expect("seed previous credentials");
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let store_database = coven_database::StoreDatabase::from_database(database.clone());
    let sync = store_sync_with_runtime_factory(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        custody.clone(),
        established_identity_custody(),
        database,
        &store_dir,
        Arc::new(FailingSyncLoopRuntimeFactory),
    );
    let home = Arc::new(InMemoryCloudHome::new());
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };

    let error = sync
        .setup_with_test_home(
            proposed,
            home.clone(),
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "new-access".to_string(),
                secret_key: "new-secret".to_string(),
            }),
        )
        .await
        .expect_err("sync runtime preparation must fail");

    assert!(matches!(error, crate::CloudHomeSetupError::Connection(_)));
    assert!(custody
        .unlock()
        .expect("read untouched master key")
        .is_none());
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read untouched credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "old-access" && secret_key == "old-secret"
    ));
    assert!(store_database
        .local_store_root_ref()
        .await
        .expect("read local Store root")
        .is_none());
    assert_eq!(home.exact_create_count(), 0);
    assert!(!sync.is_connected());
}

#[tokio::test]
async fn store_initialization_failure_rolls_back_committed_key_and_credentials() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-cloud-home-initialization-failure";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Opaque;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let previous = coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "old-access".to_string(),
        secret_key: "old-secret".to_string(),
    };
    store_keys
        .set_cloud_home_credentials(&previous)
        .expect("seed previous credentials");
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let store_database = coven_database::StoreDatabase::from_database(database.clone());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        custody.clone(),
        established_identity_custody(),
        database,
        &store_dir,
    );
    let home = Arc::new(InMemoryCloudHome::new());
    home.fail_exact_create_before_call(1);
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };

    let error = sync
        .setup_with_test_home(
            proposed,
            home,
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "new-access".to_string(),
                secret_key: "new-secret".to_string(),
            }),
        )
        .await
        .expect_err("Store initialization must fail");

    assert!(matches!(error, crate::CloudHomeSetupError::Connection(_)));
    assert!(custody
        .unlock()
        .expect("read rolled-back master key")
        .is_none());
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read restored credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "old-access" && secret_key == "old-secret"
    ));
    assert!(store_database
        .local_store_root_ref()
        .await
        .expect("read local Store root")
        .is_none(),);
    assert!(!sync.is_connected());
}

#[tokio::test]
async fn replacement_initialization_failure_preserves_the_active_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-cloud-home-replacement-initialization-failure";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let previous = coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "old-access".to_string(),
        secret_key: "old-secret".to_string(),
    };
    store_keys
        .set_cloud_home_credentials(&previous)
        .expect("seed previous credentials");
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    connect_test_home(
        sync.clone(),
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Plaintext,
    )
    .await
    .expect("install active connection");
    let stopped_before = sync.stopped_loop_count_for_test();
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };

    let error = sync
        .setup_with_test_home(
            proposed,
            Arc::new(InMemoryCloudHome::new()),
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "new-access".to_string(),
                secret_key: "new-secret".to_string(),
            }),
        )
        .await
        .expect_err("replacement Store opening must fail against an empty home");

    assert!(matches!(error, crate::CloudHomeSetupError::Connection(_)));
    assert!(sync.is_syncing());
    assert!(sync.has_remote_storage_for_test());
    assert_eq!(sync.stopped_loop_count_for_test(), stopped_before);
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read restored credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "old-access" && secret_key == "old-secret"
    ));
}

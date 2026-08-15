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
async fn provider_probe_failure_leaves_no_store_creation_attempt_or_credentials() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "probe-before-store-creation";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let store_database = StoreDatabase::from_database(database.clone());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        Arc::new(UnexpectedMasterKeyAccess),
        established_identity_custody(),
        database,
        &store_dir,
    );
    let home = Arc::new(InMemoryCloudHome::new());
    home.fail_next_probe_with(coven_protocol::objects::StorageBackendFailure::Authentication);
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };

    let error = sync
        .setup_with_test_home(
            proposed,
            home,
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "rejected-access".to_string(),
                secret_key: "rejected-secret".to_string(),
            }),
        )
        .await
        .expect_err("the provider capability probe must reject setup");

    assert!(matches!(
        &error,
        crate::CloudHomeSetupError::Connection(error)
            if matches!(
                &**error,
                SyncError::CloudHome(error)
                    if error.backend_failure()
                        == Some(coven_protocol::objects::StorageBackendFailure::Authentication)
            )
    ));
    assert!(store_database
        .load_store_creation_attempt()
        .await
        .expect("read Store creation attempt")
        .is_none());
    assert!(store_keys
        .get_cloud_home_credentials()
        .expect("read durable cloud credentials")
        .is_none());
    assert!(!sync.is_connected());
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

#[tokio::test]
async fn another_store_at_the_same_cloud_location_is_reported_as_occupied() {
    test_keyring::install();
    let home = Arc::new(InMemoryCloudHome::new());
    let cloud_home = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };
    let credentials = || coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "shared-access".to_string(),
        secret_key: "shared-secret".to_string(),
    };

    let (_first_tmp, first_store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let first_config = Config::with_defaults(
        "occupied-location-first".to_string(),
        "first-device".to_string(),
        "First Store".to_string(),
    );
    let first = store_sync(
        Arc::new(move || first_config.clone()),
        StoreKeys::bind("occupied-location-first".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(first_store_dir.clone()),
        &first_store_dir,
    );
    first
        .setup_with_test_home(cloud_home.clone(), home.clone(), Some(credentials()))
        .await
        .expect("the empty cloud location accepts the first Store");

    let (_second_tmp, second_store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let second_config = Config::with_defaults(
        "occupied-location-second".to_string(),
        "second-device".to_string(),
        "Second Store".to_string(),
    );
    let second_keys = StoreKeys::bind("occupied-location-second".to_string());
    let second_database =
        coven_replication::sync::test_helpers::open_test_db(second_store_dir.clone());
    let second_store_database = StoreDatabase::from_database(second_database.clone());
    let second = store_sync(
        Arc::new(move || second_config.clone()),
        second_keys.clone(),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        second_database,
        &second_store_dir,
    );

    let error = second
        .setup_with_test_home(cloud_home, home, Some(credentials()))
        .await
        .expect_err("another Store cannot claim the occupied cloud location");

    assert_eq!(
        error.failure(),
        crate::CloudHomeSetupFailure::LocationOccupied
    );
    assert!(second_store_database
        .local_store_root_ref()
        .await
        .expect("read second local Store root")
        .is_none());
    assert!(second_keys
        .get_cloud_home_credentials()
        .expect("read second cloud credentials")
        .is_none());
}

#[tokio::test]
#[ignore]
async fn real_s3_prefix_accepts_one_store_and_reports_the_second_as_occupied() {
    use coven_storage::cloud::CloudHome as _;

    test_keyring::install();
    let bucket = std::env::var("COVEN_TEST_S3_BUCKET").expect("COVEN_TEST_S3_BUCKET");
    let region = std::env::var("COVEN_TEST_S3_REGION").expect("COVEN_TEST_S3_REGION");
    let endpoint = std::env::var("COVEN_TEST_S3_URL").expect("COVEN_TEST_S3_URL");
    let prefix = format!(
        "{}/cloud-home-setup-location-test",
        std::env::var("COVEN_TEST_S3_PREFIX")
            .expect("COVEN_TEST_S3_PREFIX")
            .trim_end_matches('/')
    );
    let access_key = std::env::var("COVEN_TEST_S3_KEY").expect("COVEN_TEST_S3_KEY");
    let secret_key = std::env::var("COVEN_TEST_S3_SECRET").expect("COVEN_TEST_S3_SECRET");
    let factory =
        coven_storage::cloud::CloudHomeFactory::new(coven_storage::oauth::OAuthClients::empty());
    let home = Arc::new(
        factory
            .open_s3(
                bucket.clone(),
                region.clone(),
                Some(endpoint.clone()),
                access_key.clone(),
                secret_key.clone(),
                Some(prefix.clone()),
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                Arc::new(SystemClock),
            )
            .await
            .expect("open configured S3 home"),
    );
    for key in home.list("").await.expect("list stale test objects") {
        home.delete(&key).await.expect("delete stale test object");
    }
    let cloud_home = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        s3_bucket: Some(bucket),
        s3_region: Some(region),
        s3_endpoint: Some(endpoint),
        s3_key_prefix: Some(prefix),
        ..Default::default()
    };
    let credentials = || coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: access_key.clone(),
        secret_key: secret_key.clone(),
    };

    let (_first_tmp, first_store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let first_config = Config::with_defaults(
        "real-s3-location-first".to_string(),
        "first-device".to_string(),
        "First Store".to_string(),
    );
    let first = store_sync(
        Arc::new(move || first_config.clone()),
        StoreKeys::bind("real-s3-location-first".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(first_store_dir.clone()),
        &first_store_dir,
    );
    first
        .setup_with_test_home(cloud_home.clone(), home.clone(), Some(credentials()))
        .await
        .expect("empty prefix accepts the first Store");

    let (_second_tmp, second_store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let second_config = Config::with_defaults(
        "real-s3-location-second".to_string(),
        "second-device".to_string(),
        "Second Store".to_string(),
    );
    let second = store_sync(
        Arc::new(move || second_config.clone()),
        StoreKeys::bind("real-s3-location-second".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(second_store_dir.clone()),
        &second_store_dir,
    );

    let error = second
        .setup_with_test_home(cloud_home, home.clone(), Some(credentials()))
        .await
        .expect_err("occupied prefix rejects a different Store");

    assert_eq!(
        error.failure(),
        crate::CloudHomeSetupFailure::LocationOccupied
    );
    first.stop_current();
    second.stop_current();
    for key in home.list("").await.expect("list test objects") {
        home.delete(&key).await.expect("delete test object");
    }
}

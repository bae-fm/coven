use super::*;

struct FailingForgetMasterKeyCustody {
    keyring: MasterKeyring,
}

impl MasterKeyCustody for FailingForgetMasterKeyCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(Some(self.keyring.clone()))
    }

    fn persist(&self, _keyring: &MasterKeyring) -> Result<(), KeyError> {
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        Err(KeyError::Keyring(keyring_unavailable()))
    }
}

#[tokio::test]
async fn credentialless_provider_setup_removes_previous_provider_credentials() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-credentialless-cloud-home";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    store_keys
        .set_cloud_home_credentials(&coven_keys::keys::CloudHomeCredentials::S3 {
            access_key: "old-access".to_string(),
            secret_key: "old-secret".to_string(),
        })
        .expect("seed previous credentials");
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::CloudKit),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };

    sync.setup_with_test_home(proposed, Arc::new(InMemoryCloudHome::new()), None)
        .await
        .expect("credentialless setup succeeds");

    assert!(store_keys
        .get_cloud_home_credentials()
        .expect("read removed credentials")
        .is_none());
}

#[tokio::test]
async fn disconnect_cloud_home_forgets_credentials_and_drops_the_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "disconnect-cloud-home-custody";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };

    sync.setup_with_test_home(
        proposed,
        Arc::new(InMemoryCloudHome::new()),
        Some(coven_keys::keys::CloudHomeCredentials::S3 {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
        }),
    )
    .await
    .expect("connect cloud home");

    sync.disconnect_cloud_home()
        .await
        .expect("disconnect cloud home");

    assert!(!sync.is_connected());
    assert!(store_keys
        .get_cloud_home_credentials()
        .expect("read cloud credentials")
        .is_none());
}

#[tokio::test]
async fn forgetting_the_master_key_disconnects_before_returning_locked() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "forget-master-key-custody";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys,
        custody,
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };

    sync.setup_with_test_home(proposed, Arc::new(InMemoryCloudHome::new()), None)
        .await
        .expect("connect opaque cloud home");

    sync.forget_master_key().await.expect("forget master key");

    assert!(!sync.is_connected());
    assert_eq!(
        sync.security
            .cloud_home_key_state(HomeStorage::Opaque)
            .expect("read key state"),
        crate::store_security::CloudHomeKeyState::Locked,
    );
}

#[tokio::test]
async fn failed_credential_removal_preserves_the_cloud_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "failed-disconnect-cloud-home-custody";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
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
    .expect("connect cloud home");
    store_keys
        .fail_next_cloud_home_credentials_operation_for_test(keyring_unavailable())
        .expect("arrange credential deletion failure");

    sync.disconnect_cloud_home()
        .await
        .expect_err("credential deletion must fail");

    assert!(sync.is_connected());
}

#[tokio::test]
async fn failed_master_key_removal_preserves_the_cloud_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "failed-forget-master-key-custody";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Opaque;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys,
        Arc::new(FailingForgetMasterKeyCustody {
            keyring: MasterKeyring::generate(),
        }),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );

    sync.connect_with_test_home_custody(Arc::new(InMemoryCloudHome::new()))
        .await
        .expect("connect opaque cloud home");

    sync.forget_master_key()
        .await
        .expect_err("master-key deletion must fail");

    assert!(sync.is_connected());
    assert_eq!(
        sync.security
            .cloud_home_key_state(HomeStorage::Opaque)
            .expect("read key state"),
        crate::store_security::CloudHomeKeyState::Available,
    );
}

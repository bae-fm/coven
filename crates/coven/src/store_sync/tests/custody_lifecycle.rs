use super::*;

struct FailingForgetMasterKeyCustody {
    keyring: MasterKeyring,
}

#[derive(Default)]
struct MemoryMasterKeyCustody {
    keyring: Mutex<Option<MasterKeyring>>,
}

impl MasterKeyCustody for MemoryMasterKeyCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(self.keyring.lock().expect("lock master key").clone())
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        *self.keyring.lock().expect("lock master key") = Some(keyring.clone());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.keyring.lock().expect("lock master key") = None;
        Ok(())
    }
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

#[tokio::test]
async fn unlocking_connects_before_committing_the_imported_master_key() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "unlock-cloud-home-custody";
    let mut initial_config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    initial_config.cloud_home = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };
    let config = Arc::new(RwLock::new(initial_config));
    let config_provider: ConfigProvider = {
        let config = config.clone();
        Arc::new(move || config.read().expect("read config").clone())
    };
    let keys = StoreKeys::bind(store_id.to_string());
    let custody = Arc::new(MemoryMasterKeyCustody::default());
    let identity = established_identity_custody();
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let home = Arc::new(InMemoryCloudHome::new());
    let creator = store_sync(
        config_provider.clone(),
        keys.clone(),
        custody.clone(),
        identity.clone(),
        database.clone(),
        &store_dir,
    );
    let cloud_home = config.read().expect("read config").cloud_home.clone();
    creator
        .setup_with_test_home(cloud_home, home.clone(), None)
        .await
        .expect("create encrypted cloud home");
    let correct_key = custody
        .unlock()
        .expect("read generated key")
        .expect("generated key exists")
        .to_serialized();
    creator.disconnect();
    drop(creator);
    custody.forget().expect("remove generated key");

    let returning = store_sync(
        config_provider,
        keys,
        custody.clone(),
        identity,
        database,
        &store_dir,
    );
    let wrong_key = MasterKeyring::generate().to_serialized();

    returning
        .unlock_with_test_home(&wrong_key, home.clone())
        .await
        .expect_err("wrong key must not connect");
    assert!(custody
        .unlock()
        .expect("read custody after rejected key")
        .is_none());
    assert!(!returning.is_connected());

    returning
        .unlock_with_test_home(&correct_key, home)
        .await
        .expect("correct key connects");
    assert!(custody.unlock().expect("read committed key").is_some());
    assert!(returning.is_connected());
}

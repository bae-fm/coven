use std::sync::{Arc, Mutex, RwLock};

#[path = "tests/setup_failure.rs"]
mod setup_failure;

use super::*;
use crate::store_membership::StoreMembership;
use coven_foundation::clock::SystemClock;
use coven_foundation::config::{CloudProvider, HomeStorage};
use coven_foundation::store_dir::StoreDir;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_keys::encryption::MasterKeyring;
use coven_keys::keys::{
    test_keyring, DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys,
};
use coven_replication::sync::store::blob::LocalStoreBlobAccess;
use coven_storage::cloud::setup::StorageSetupError;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::cloud::{CloudHomeError, CloudHomeJoinInfo};
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

struct NoKeyCustody;

impl MasterKeyCustody for NoKeyCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(None)
    }

    fn persist(&self, _keyring: &MasterKeyring) -> Result<(), KeyError> {
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
}

struct FailCredentialsAfterMasterCommit {
    keyring: Mutex<Option<MasterKeyring>>,
    credentials: StoreKeys,
}

impl FailCredentialsAfterMasterCommit {
    fn new(credentials: StoreKeys) -> Self {
        Self {
            keyring: Mutex::new(None),
            credentials,
        }
    }
}

impl MasterKeyCustody for FailCredentialsAfterMasterCommit {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(self.keyring.lock().expect("lock test master key").clone())
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        *self.keyring.lock().expect("lock test master key") = Some(keyring.clone());
        self.credentials
            .fail_next_cloud_home_credentials_operation_for_test(keyring_unavailable())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.keyring.lock().expect("lock test master key") = None;
        Ok(())
    }
}

fn keyring_unavailable() -> keyring_core::Error {
    keyring_core::Error::Invalid(
        "keyring unavailable".to_string(),
        "test failure".to_string(),
    )
}

struct NoIdentityCustody;

impl DeviceIdentityCustody for NoIdentityCustody {
    fn unlock(&self) -> Result<Option<coven_keys::keys::UserKeypair>, KeyError> {
        Ok(None)
    }

    fn persist(&self, _keypair: &coven_keys::keys::UserKeypair) -> Result<(), KeyError> {
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
}

struct UnexpectedMasterKeyAccess;

impl MasterKeyCustody for UnexpectedMasterKeyAccess {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        panic!("browsable cloud setup must not unlock master-key custody")
    }

    fn persist(&self, _keyring: &MasterKeyring) -> Result<(), KeyError> {
        panic!("browsable cloud setup must not persist a master key")
    }

    fn forget(&self) -> Result<(), KeyError> {
        panic!("browsable cloud setup must not delete a master key")
    }
}

struct LockedMasterKeyCustody;

impl MasterKeyCustody for LockedMasterKeyCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(None)
    }

    fn persist(&self, _keyring: &MasterKeyring) -> Result<(), KeyError> {
        panic!("returning cloud connect must not generate a master key")
    }

    fn forget(&self) -> Result<(), KeyError> {
        panic!("returning cloud connect must not delete a master key")
    }
}

fn established_identity_custody() -> Arc<dyn DeviceIdentityCustody> {
    test_keyring::install();
    let store_keys = StoreKeys::bind("unused-store-id".to_string());
    coven_keys::identity_custody::IdentityCustody::InMemory(
        coven_keys::keys::UserKeypair::generate(),
    )
    .resolve(&store_keys, &StoreDir::new_ephemeral("unused-store-dir"))
}

fn store_security(
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
    store_dir: &StoreDir,
) -> StoreSecurity {
    StoreSecurity::new(keys, master_keys, identity, store_dir.clone())
}

fn store_cloud_storage(
    keys: &StoreKeys,
    security: &StoreSecurity,
    clock: ClockRef,
) -> StoreCloudStorage {
    StoreCloudStorage::new(
        security.clone(),
        coven_storage::cloud::CloudHomeFactory::new(coven_storage::oauth::OAuthClients::empty()),
        coven_keys::keys::CloudHomeCredentialsOwner::new(keys.clone()),
        clock,
        None,
        BlobChunking::DEFAULT,
    )
}

fn store_sync(
    config_provider: ConfigProvider,
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
    database: coven_database::Database,
    store_dir: &StoreDir,
) -> StoreSync {
    store_sync_with_runtime_factory(
        config_provider,
        keys,
        master_keys,
        identity,
        database,
        store_dir,
        Arc::new(coven_replication::sync::sync_loop::SystemSyncLoopRuntimeFactory),
    )
}

fn store_sync_with_runtime_factory(
    config_provider: ConfigProvider,
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
    database: coven_database::Database,
    store_dir: &StoreDir,
    runtime_factory: Arc<dyn coven_replication::sync::sync_loop::SyncLoopRuntimeFactory>,
) -> StoreSync {
    let database = StoreDatabase::from_database(database.clone());
    let local_blob_access = LocalStoreBlobAccess::new(
        database.clone(),
        store_dir.clone(),
        coven_replication::sync::store::blob::StoreBlobCache::new(
            database.clone(),
            store_dir.clone(),
        ),
    );
    let cloud_keys = keys.clone();
    let security = store_security(keys, master_keys.clone(), identity, store_dir);
    let clock: ClockRef = Arc::new(SystemClock);
    let cloud_storage = store_cloud_storage(&cloud_keys, &security, clock.clone());
    let blob_storage = crate::store_blobs::StoreBlobAccess::new(
        database.clone(),
        config_provider.clone(),
        cloud_storage.clone(),
        local_blob_access.clone(),
    );
    StoreSync::new(
        config_provider,
        security,
        database,
        store_dir.clone(),
        clock,
        None,
        StoreOpenGuard::acquire_for_test(store_dir),
        cloud_storage,
        blob_storage,
        runtime_factory,
    )
}

async fn connect_test_home(
    sync: StoreSync,
    home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
    cipher: CloudCipher,
) -> Result<(), SyncError> {
    tokio::spawn(async move { sync.connect_with_test_home(home, cipher).await })
        .await
        .expect("join injected-home startup task")
}

#[tokio::test]
async fn membership_read_surfaces_malformed_cloud_credentials() {
    test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new_ephemeral(tmp.path());
    let store_id = "sync-enabled-malformed-credentials";
    let keys = StoreKeys::bind(store_id.to_string());
    keys.write_cloud_home_credentials_json_for_test("{")
        .expect("write malformed credentials");
    let join_info = CloudHomeJoinInfo::S3 {
        bucket: "bucket".to_string(),
        region: "region".to_string(),
        endpoint: None,
        access_key: "access".to_string(),
        secret_key: "secret".to_string(),
        key_prefix: None,
    };
    let config = coven_domain::joining::config::build_config(
        store_id,
        "device",
        "store",
        &join_info,
        &CloudCipher::Plaintext,
    );
    let cloud_keys = keys.clone();
    let master_keys: Arc<dyn MasterKeyCustody> = Arc::new(NoKeyCustody);
    let security = store_security(
        keys,
        master_keys.clone(),
        established_identity_custody(),
        &store_dir,
    );
    let database = StoreDatabase::from_database(
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()).clone(),
    );
    let local_blob_access = LocalStoreBlobAccess::new(
        database.clone(),
        store_dir.clone(),
        coven_replication::sync::store::blob::StoreBlobCache::new(
            database.clone(),
            store_dir.clone(),
        ),
    );
    let config_provider: ConfigProvider = Arc::new(move || config.clone());
    let clock: ClockRef = Arc::new(SystemClock);
    let cloud_storage = store_cloud_storage(&cloud_keys, &security, clock.clone());
    let blob_storage = crate::store_blobs::StoreBlobAccess::new(
        database.clone(),
        config_provider.clone(),
        cloud_storage.clone(),
        local_blob_access.clone(),
    );
    let sync = StoreSync::new(
        config_provider,
        security.clone(),
        database,
        store_dir.clone(),
        clock,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        cloud_storage,
        blob_storage,
        Arc::new(coven_replication::sync::sync_loop::SystemSyncLoopRuntimeFactory),
    );
    let membership = StoreMembership::new(sync);

    let error = membership
        .members()
        .await
        .expect_err("malformed stored credentials must fail");
    let SyncError::CloudHome(cloud_home_error) = &error else {
        panic!("expected CloudHome(_), got {error:?}");
    };
    let CloudHomeError::ConfigurationSource { operation, source } = cloud_home_error else {
        panic!("expected source-bearing configuration error, got {cloud_home_error:?}");
    };
    assert_eq!(operation, "read S3 credentials");
    assert!(matches!(
        source.downcast_ref::<KeyError>(),
        Some(KeyError::Json {
            operation: "parse cloud home credentials JSON",
            ..
        })
    ));
    assert!(!cloud_home_error.is_retryable());
}

#[tokio::test]
async fn connect_rejects_an_opaque_home_without_a_master_key() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let mut config = Config::with_defaults(
        "sync-opaque-no-encryption".to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::S3);
    config.cloud_home.s3_bucket = Some("bucket".to_string());
    config.cloud_home.s3_region = Some("us-east-1".to_string());
    let store_keys = StoreKeys::bind("sync-opaque-no-encryption".to_string());
    store_keys
        .set_cloud_home_credentials(&coven_keys::keys::CloudHomeCredentials::S3 {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
        })
        .expect("seed S3 credentials");
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys,
        Arc::new(LockedMasterKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );

    let error = sync
        .connect()
        .await
        .expect_err("opaque home without an established master key must fail");
    assert!(
        matches!(error, SyncError::MasterKeyNotEstablished),
        "expected MasterKeyNotEstablished, got {error:?}"
    );
    assert_eq!(
        sync.security
            .cloud_home_key_state(HomeStorage::Opaque)
            .expect("read locked key state"),
        crate::store_security::CloudHomeKeyState::Locked
    );
}

#[tokio::test]
async fn new_opaque_home_commits_its_key_and_credentials_with_the_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-new-cloud-home";
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
        store_keys.clone(),
        custody.clone(),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };

    let connected = sync
        .setup_with_test_home(
            proposed.clone(),
            Arc::new(InMemoryCloudHome::new()),
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
            }),
        )
        .await
        .expect("atomic setup succeeds");

    assert_eq!(connected.cloud_home, proposed);
    assert_eq!(
        connected.key_state,
        crate::store_security::CloudHomeKeyState::Available
    );
    assert!(custody.unlock().expect("unlock master key").is_some());
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read committed credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "access" && secret_key == "secret"
    ));
    assert!(sync.is_syncing());
}

#[tokio::test]
async fn browsable_home_setup_never_accesses_master_key_custody() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-browsable-cloud-home";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let store_keys = StoreKeys::bind(store_id.to_string());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys,
        Arc::new(UnexpectedMasterKeyAccess),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Browsable,
        ..Default::default()
    };

    let connected = sync
        .setup_with_test_home(
            proposed,
            Arc::new(InMemoryCloudHome::new()),
            Some(coven_keys::keys::CloudHomeCredentials::S3 {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
            }),
        )
        .await
        .expect("browsable setup succeeds without master-key custody");

    assert_eq!(
        connected.key_state,
        crate::store_security::CloudHomeKeyState::NotRequired
    );
    sync.disconnect();
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

#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn authorized_oauth_tokens_remain_absent_when_connection_preparation_fails() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-oauth-connection-failure";
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
        Arc::new(UnexpectedMasterKeyAccess),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::Dropbox),
        storage: HomeStorage::Browsable,
        dropbox_folder_path: Some("/Apps/coven/Test Store".to_string()),
        ..Default::default()
    };
    let prepared = coven_storage::cloud::PreparedOAuthCloudHome {
        cloud_home: proposed,
        credentials: coven_keys::keys::CloudHomeCredentials::OAuth {
            tokens: coven_keys::keys::OAuthTokens {
                access_token: "authorized-access-token".to_string(),
                refresh_token: Some("authorized-refresh-token".to_string()),
                expires_at: None,
            },
        },
    };

    let error = sync
        .setup_prepared_oauth_cloud_home_for_test(prepared)
        .await
        .expect_err("missing provider client configuration must reject the connection");

    assert!(matches!(error, crate::CloudHomeSetupError::Connection(_)));
    assert!(store_keys
        .get_cloud_home_credentials()
        .expect("read durable OAuth credentials")
        .is_none());
    assert!(!sync.is_connected());
}

#[tokio::test]
async fn credential_commit_failure_restores_the_previous_credentials_and_master_key() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-cloud-home-credential-failure";
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
    let custody = Arc::new(FailCredentialsAfterMasterCommit::new(store_keys.clone()));
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let store_database = coven_database::StoreDatabase::from_database(database.clone());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys.clone(),
        custody.clone(),
        established_identity_custody(),
        database.clone(),
        &store_dir,
    );
    let proposed = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
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
        .expect_err("credential commit must fail");

    assert!(
        matches!(
            &error,
            crate::CloudHomeSetupError::Commit {
                subject: "credentials",
                ..
            }
        ),
        "expected credential commit failure, got {error:?}"
    );
    assert!(custody
        .unlock()
        .expect("read rolled-back master key")
        .is_none());
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read preserved credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "old-access" && secret_key == "old-secret"
    ));
    assert!(!sync.is_connected());
    assert!(
        store_database
            .local_store_root_ref()
            .await
            .expect("read local Store root")
            .is_none(),
        "failed setup must not leave an initialized Store behind",
    );
}

#[tokio::test]
async fn credential_commit_failure_preserves_the_active_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "atomic-cloud-home-active-connection";
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
    let home = Arc::new(InMemoryCloudHome::new());
    connect_test_home(sync.clone(), home.clone(), CloudCipher::Plaintext)
        .await
        .expect("install the active connection");
    let stopped_before = sync.stopped_loop_count_for_test();
    store_keys
        .fail_next_cloud_home_credentials_operation_for_test(keyring_unavailable())
        .expect("fail the credential commit");
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
                access_key: "new-access".to_string(),
                secret_key: "new-secret".to_string(),
            }),
        )
        .await
        .expect_err("credential commit must fail");

    assert!(matches!(
        error,
        crate::CloudHomeSetupError::Commit {
            subject: "credentials",
            ..
        }
    ));
    assert!(sync.is_syncing());
    assert!(sync.has_remote_storage_for_test());
    assert_eq!(sync.stopped_loop_count_for_test(), stopped_before);
    assert!(matches!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read preserved credentials"),
        Some(coven_keys::keys::CloudHomeCredentials::S3 { access_key, secret_key })
            if access_key == "old-access" && secret_key == "old-secret"
    ));
}

#[tokio::test]
async fn capability_admission_refuses_before_stopping_the_active_loop() {
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let mut initial_config = Config::with_defaults(
        "immutable-admission-before-stop".to_string(),
        "test-device".to_string(),
        "Blob Store".to_string(),
    );
    initial_config.cloud_home.storage = HomeStorage::Browsable;
    let config = Arc::new(RwLock::new(initial_config));
    let database = coven_replication::sync::test_helpers::open_test_db_with_blob(
        store_dir.clone(),
        crate::BlobDecl::new(
            "photos",
            crate::Provenance::HostProvided,
            crate::CacheFill::CacheLazy,
        ),
    );
    let sync = store_sync(
        {
            let config = config.clone();
            Arc::new(move || config.read().expect("read config").clone())
        },
        StoreKeys::bind("immutable-admission-before-stop".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        database,
        &store_dir,
    );
    connect_test_home(
        sync.clone(),
        Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Plaintext,
    )
    .await
    .expect("install active loop");
    assert!(sync.is_syncing());

    {
        let mut config = config.write().expect("write config");
        config.cloud_home.provider = Some(CloudProvider::Dropbox);
        config.cloud_home.exact_upload_verification =
            coven_foundation::config::ExactUploadVerification::UploadChecksum;
    }
    let error = sync
        .connect()
        .await
        .expect_err("unsupported immutable-copy provider is refused");
    assert!(matches!(
        error,
        SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
            provider: CloudProvider::Dropbox,
        })
    ));
    assert!(sync.is_syncing());
    assert!(sync.loop_uses_connected_storage_for_test());
}

#[tokio::test]
async fn probe_applies_exact_slot_admission_before_opening_the_provider() {
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let mut config = Config::with_defaults(
        "probe-exact-slot-admission".to_string(),
        "test-device".to_string(),
        "Blob Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::Dropbox);
    config.cloud_home.exact_upload_verification =
        coven_foundation::config::ExactUploadVerification::UploadChecksum;
    let sync = store_sync(
        Arc::new(|| panic!("the proposed config is supplied directly")),
        StoreKeys::bind("probe-exact-slot-admission".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );

    let error = sync
        .probe_cloud_home(&config)
        .await
        .expect_err("unsupported verification must be refused before provider opening");
    assert!(matches!(
        error,
        SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
            provider: CloudProvider::Dropbox,
        })
    ));
}

#[tokio::test]
async fn test_home_replacement_stops_the_previous_loop() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let mut config = Config::with_defaults(
        "sync-restart".to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let sync = store_sync(
        Arc::new(move || config.clone()),
        StoreKeys::bind("sync-restart".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );
    let home = Arc::new(InMemoryCloudHome::new());
    connect_test_home(sync.clone(), home.clone(), CloudCipher::Plaintext)
        .await
        .expect("first test home starts");
    let stopped_before = sync.stopped_loop_count_for_test();
    connect_test_home(sync.clone(), home, CloudCipher::Plaintext)
        .await
        .expect("replacement test home starts");
    assert_eq!(sync.stopped_loop_count_for_test(), stopped_before + 1);
    assert!(sync.is_syncing());
}

#[tokio::test]
async fn failed_restart_preserves_the_active_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let mut initial_config = Config::with_defaults(
        "sync-failed-restart".to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    initial_config.cloud_home.storage = HomeStorage::Browsable;
    let config = Arc::new(RwLock::new(initial_config));
    let store_keys = StoreKeys::bind("sync-failed-restart".to_string());
    let custody = coven_keys::custody::KeyCustody::InMemory(MasterKeyring::generate())
        .resolve(&store_keys, &StoreDir::new_ephemeral("unused-store-dir"));
    let sync = store_sync(
        {
            let config = config.clone();
            Arc::new(move || config.read().unwrap().clone())
        },
        store_keys,
        custody,
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
    .expect("injected home starts");

    config.write().unwrap().cloud_home.provider = Some(CloudProvider::S3);
    let error = sync
        .connect()
        .await
        .expect_err("invalid configured provider fails restart");
    assert!(error.to_string().contains("failed to build cloud home"));
    assert!(sync.is_syncing());
    assert!(sync.has_remote_storage_for_test());
}

#[tokio::test]
async fn connect_rejects_a_missing_device_identity() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "sync-no-device-identity".to_string();
    let keys = StoreKeys::bind(store_id.clone());
    keys.set_cloud_home_credentials(&coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
    })
    .expect("seed S3 credentials");
    let mut config = Config::with_defaults(
        store_id.clone(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::S3);
    config.cloud_home.storage = HomeStorage::Browsable;
    config.cloud_home.s3_bucket = Some("bucket".to_string());
    config.cloud_home.s3_region = Some("us-east-1".to_string());
    let sync = store_sync(
        Arc::new(move || config.clone()),
        keys,
        Arc::new(NoKeyCustody),
        Arc::new(NoIdentityCustody),
        coven_replication::sync::test_helpers::open_test_db(store_dir.clone()),
        &store_dir,
    );

    let error = sync
        .connect()
        .await
        .expect_err("missing device identity must fail the connect");
    assert!(matches!(error, SyncError::Key(KeyError::NoDeviceIdentity)));
    assert!(!sync.is_connected());
    assert!(!sync.has_remote_storage_for_test());
}

#[tokio::test]
async fn foreign_founder_installs_no_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "sync-foreign-browsable-founder";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let home = Arc::new(InMemoryCloudHome::new());
    let attacker = coven_keys::keys::UserKeypair::generate();
    let attacker_storage = Arc::new(CloudSyncConnection::new(
        home.clone(),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        store_id,
        attacker.clone(),
    ));
    let attacker_db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let attacker_db =
        coven_replication::sync::test_helpers::open_test_db(attacker_db_store_dir.clone());
    let _attacker_device = coven_replication::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_db_store_dir,
        attacker_storage.clone(),
        store_id,
        attacker,
    )
    .await
    .expect("publish attacker Store root");

    let victim = coven_keys::keys::UserKeypair::generate();
    let database = coven_replication::sync::test_helpers::open_test_db(store_dir.clone());
    let store_keys = StoreKeys::bind(store_id.to_string());
    let identity_custody = coven_keys::identity_custody::IdentityCustody::InMemory(victim)
        .resolve(&store_keys, &store_dir);
    let sync = store_sync(
        Arc::new(move || config.clone()),
        store_keys,
        Arc::new(NoKeyCustody),
        identity_custody,
        database.clone(),
        &store_dir,
    );
    let error = connect_test_home(sync.clone(), home, CloudCipher::Plaintext)
        .await
        .expect_err("foreign founder must prevent sync startup");

    assert!(matches!(
        error,
        SyncError::Init(source) if matches!(
            *source,
            coven_replication::sync::cycle::InitSyncError::Initialization(
                coven_replication::sync::store::StoreInitializationError::ProtocolRoot(_)
            )
        )
    ));
    assert!(!sync.is_connected());
    assert!(!sync.has_remote_storage_for_test());
    assert_eq!(
        database
            .get_protocol_state(coven_protocol::membership::OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap(),
        None,
    );
}

#[test]
fn cipher_resolution_reads_current_custody_each_time() {
    test_keyring::install();
    let (_tmp, store_dir) = coven_replication::sync::test_helpers::temp_store_dir();
    let store_id = "sync-resolve-cipher-fresh";
    let store_keys = StoreKeys::bind(store_id.to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let key_a = MasterKeyring::generate();
    custody.persist(&key_a).expect("establish key A");
    let security = store_security(
        store_keys,
        custody.clone(),
        established_identity_custody(),
        &store_dir,
    );

    let fingerprint_a = security
        .cloud_cipher_fingerprint_for_test(HomeStorage::Opaque)
        .expect("resolve key A")
        .expect("opaque storage must resolve an encrypted cipher");
    let key_b = MasterKeyring::generate();
    custody.persist(&key_b).expect("replace custody with key B");
    let fingerprint_b = security
        .cloud_cipher_fingerprint_for_test(HomeStorage::Opaque)
        .expect("resolve key B")
        .expect("opaque storage must resolve an encrypted cipher");

    assert_eq!(fingerprint_a, key_a.fingerprint());
    assert_eq!(fingerprint_b, key_b.fingerprint());
    assert_ne!(fingerprint_a, fingerprint_b);
}

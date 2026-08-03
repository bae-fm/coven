use std::sync::{Arc, RwLock};

use super::*;
use crate::clock::SystemClock;
use crate::config::{CloudProvider, HomeStorage};
use crate::coven::StoreOpenGuard;
use crate::encryption::MasterKeyring;
use crate::keys::{test_keyring, DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys};
use crate::storage::cloud::setup::StorageSetupError;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{CloudHomeError, CloudHomeJoinInfo};
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::store_dir::StoreDir;
use crate::store_membership::StoreMembership;

struct NoImmutableCopyHome;

#[async_trait::async_trait]
impl CloudHome for NoImmutableCopyHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    fn multipart_threshold(&self) -> u64 {
        panic!("incapable home must be rejected before I/O")
    }

    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }

    async fn set_access(
        &self,
        _desired: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, CloudHomeError> {
        panic!("incapable home must be rejected before I/O")
    }
}

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

struct NoIdentityCustody;

impl DeviceIdentityCustody for NoIdentityCustody {
    fn unlock(&self) -> Result<Option<crate::keys::UserKeypair>, KeyError> {
        Ok(None)
    }

    fn persist(&self, _keypair: &crate::keys::UserKeypair) -> Result<(), KeyError> {
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
}

fn established_identity_custody() -> Arc<dyn DeviceIdentityCustody> {
    test_keyring::install();
    let store_keys = StoreKeys::bind("unused-store-id".to_string());
    crate::identity_custody::IdentityCustody::InMemory(crate::keys::UserKeypair::generate())
        .resolve(&store_keys, &StoreDir::new("unused-store-dir"))
}

fn store_security(
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
) -> StoreSecurity {
    let cloud_homes = crate::storage::cloud::CloudHomeFactory::new(
        keys.clone(),
        crate::oauth::OAuthClients::empty(),
    );
    StoreSecurity::new(keys, master_keys, identity, cloud_homes)
}

fn store_sync(
    config_provider: ConfigProvider,
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
    identity: Arc<dyn DeviceIdentityCustody>,
    database: crate::database::Database,
    store_dir: &StoreDir,
) -> StoreSync {
    let database = StoreDatabase::from_database(database);
    let owners =
        crate::sync::test_owner_graph::TestOwnerGraph::new(database.clone(), store_dir.clone());
    let security = store_security(keys, master_keys, identity);
    let clock: ClockRef = Arc::new(SystemClock);
    let blob_storage = crate::store_blobs::ReadOnlyBlobStorage::new(
        config_provider.clone(),
        security.clone(),
        clock.clone(),
        None,
        BlobChunking::DEFAULT,
    );
    StoreSync::new(
        config_provider,
        security,
        database,
        store_dir.clone(),
        clock,
        None,
        None,
        StoreOpenGuard::acquire_for_test(store_dir),
        BlobChunking::DEFAULT,
        owners.local_access(),
        blob_storage,
        owners.local_transitions(),
    )
}

async fn connect_test_home(
    sync: StoreSync,
    home: Arc<dyn CloudHome>,
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
    let store_dir = StoreDir::new(tmp.path());
    let store_id = "sync-enabled-malformed-credentials";
    let keys = StoreKeys::bind(store_id.to_string());
    keys.cloud_home_credentials_entry_for_test()
        .expect("create credentials entry")
        .set_password("{")
        .expect("write malformed credentials");
    let join_info = CloudHomeJoinInfo::S3 {
        bucket: "bucket".to_string(),
        region: "region".to_string(),
        endpoint: None,
        access_key: "access".to_string(),
        secret_key: "secret".to_string(),
        key_prefix: None,
    };
    let config = crate::joining::build_config(
        store_id,
        "device",
        &store_dir,
        "store",
        &join_info,
        &CloudCipher::Plaintext,
    );
    let security = store_security(keys, Arc::new(NoKeyCustody), established_identity_custody());
    let database = StoreDatabase::from_database(crate::sync::test_helpers::open_test_db());
    let owners =
        crate::sync::test_owner_graph::TestOwnerGraph::new(database.clone(), store_dir.clone());
    let config_provider: ConfigProvider = Arc::new(move || config.clone());
    let clock: ClockRef = Arc::new(SystemClock);
    let blob_storage = crate::store_blobs::ReadOnlyBlobStorage::new(
        config_provider.clone(),
        security.clone(),
        clock.clone(),
        None,
        BlobChunking::DEFAULT,
    );
    let sync = StoreSync::new(
        config_provider,
        security.clone(),
        database,
        store_dir.clone(),
        clock,
        None,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        BlobChunking::DEFAULT,
        owners.local_access(),
        blob_storage,
        owners.local_transitions(),
    );
    let membership = StoreMembership::new(security, sync);

    let error = membership
        .members()
        .await
        .expect_err("malformed stored credentials must fail");
    let SyncError::StorageSetup(StorageSetupError::CloudHome(cloud_home_error)) = &error else {
        panic!("expected StorageSetup(CloudHome(_)), got {error:?}");
    };
    assert!(matches!(cloud_home_error, CloudHomeError::Configuration(_)));
    assert!(!cloud_home_error.is_retryable());
    assert!(error
        .to_string()
        .contains("malformed cloud home credentials JSON"));
}

#[tokio::test]
async fn connect_rejects_an_opaque_home_without_a_master_key() {
    test_keyring::install();
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let mut config = Config::with_defaults(
        "sync-opaque-no-encryption".to_string(),
        "test-device".to_string(),
        store_dir.clone(),
        "Test Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::S3);
    let sync = store_sync(
        Arc::new(move || config.clone()),
        StoreKeys::bind("sync-opaque-no-encryption".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        crate::sync::test_helpers::open_test_db(),
        &store_dir,
    );

    let error = sync
        .connect()
        .await
        .expect_err("opaque home without an established master key must fail");
    assert!(matches!(error, SyncError::MasterKeyNotEstablished));
}

#[tokio::test]
async fn capability_admission_refuses_before_stopping_the_active_loop() {
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let mut initial_config = Config::with_defaults(
        "immutable-admission-before-stop".to_string(),
        "test-device".to_string(),
        store_dir.clone(),
        "Blob Store".to_string(),
    );
    initial_config.cloud_home.storage = HomeStorage::Browsable;
    let config = Arc::new(RwLock::new(initial_config));
    let database = crate::sync::test_helpers::open_test_db_with_blob(crate::BlobDecl::new(
        "photos",
        crate::Provenance::HostProvided,
        crate::CacheFill::CacheLazy,
    ));
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
        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        config.cloud_home.s3_exact_slots = None;
    }
    let error = sync
        .connect()
        .await
        .expect_err("unsupported immutable-copy provider is refused");
    assert!(matches!(
        error,
        SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
            provider: CloudProvider::S3,
        })
    ));
    assert!(sync.is_syncing());
    assert!(sync.loop_uses_connected_storage_for_test());

    let error = connect_test_home(
        sync.clone(),
        Arc::new(NoImmutableCopyHome),
        CloudCipher::Plaintext,
    )
    .await
    .expect_err("injected home without immutable-copy storage is refused");
    assert!(matches!(
        error,
        SyncError::StorageSetup(StorageSetupError::ExactSlotsUnavailable {
            provider: CloudProvider::S3,
        })
    ));
    assert!(sync.is_syncing());
    assert!(sync.loop_uses_connected_storage_for_test());
}

#[tokio::test]
async fn test_home_replacement_stops_the_previous_loop() {
    test_keyring::install();
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let mut config = Config::with_defaults(
        "sync-restart".to_string(),
        "test-device".to_string(),
        store_dir.clone(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let sync = store_sync(
        Arc::new(move || config.clone()),
        StoreKeys::bind("sync-restart".to_string()),
        Arc::new(NoKeyCustody),
        established_identity_custody(),
        crate::sync::test_helpers::open_test_db(),
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
async fn failed_restart_leaves_no_stale_connection() {
    test_keyring::install();
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let mut initial_config = Config::with_defaults(
        "sync-failed-restart".to_string(),
        "test-device".to_string(),
        store_dir.clone(),
        "Test Store".to_string(),
    );
    initial_config.cloud_home.storage = HomeStorage::Browsable;
    let config = Arc::new(RwLock::new(initial_config));
    let store_keys = StoreKeys::bind("sync-failed-restart".to_string());
    let custody = crate::custody::KeyCustody::InMemory(MasterKeyring::generate())
        .resolve(&store_keys, &StoreDir::new("unused-store-dir"));
    let sync = store_sync(
        {
            let config = config.clone();
            Arc::new(move || config.read().unwrap().clone())
        },
        store_keys,
        custody,
        established_identity_custody(),
        crate::sync::test_helpers::open_test_db(),
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
    assert!(!sync.is_connected());
    assert!(!sync.has_remote_storage_for_test());
}

#[tokio::test]
async fn connect_rejects_a_missing_device_identity() {
    test_keyring::install();
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let store_id = "sync-no-device-identity".to_string();
    let keys = StoreKeys::bind(store_id.clone());
    keys.set_cloud_home_credentials(&crate::keys::CloudHomeCredentials::S3 {
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
    })
    .expect("seed S3 credentials");
    let mut config = Config::with_defaults(
        store_id.clone(),
        "test-device".to_string(),
        store_dir.clone(),
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
        crate::sync::test_helpers::open_test_db(),
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
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let store_id = "sync-foreign-browsable-founder";
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        store_dir.clone(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let home = Arc::new(InMemoryCloudHome::new());
    let attacker = crate::keys::UserKeypair::generate();
    let attacker_storage = Arc::new(
        CloudSyncStorage::new(
            home.clone(),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            store_id,
            attacker.clone(),
        )
        .expect("build attacker storage"),
    );
    let attacker_db = crate::sync::test_helpers::open_test_db();
    let _attacker_device = crate::sync::test_helpers::TestDevice::create(
        &attacker_db,
        attacker_storage.clone(),
        store_id,
        attacker,
    )
    .await
    .expect("publish attacker Store root");

    let victim = crate::keys::UserKeypair::generate();
    let database = crate::sync::test_helpers::open_test_db();
    let store_keys = StoreKeys::bind(store_id.to_string());
    let identity_custody =
        crate::identity_custody::IdentityCustody::InMemory(victim).resolve(&store_keys, &store_dir);
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
        SyncError::Init(crate::sync::cycle::InitSyncError::StoreProtocolRoot(_))
    ));
    assert!(!sync.is_connected());
    assert!(!sync.has_remote_storage_for_test());
    assert_eq!(
        database
            .get_protocol_state(crate::sync::store::OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap(),
        None,
    );
}

#[test]
fn cipher_resolution_reads_current_custody_each_time() {
    test_keyring::install();
    let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    let store_id = "sync-resolve-cipher-fresh";
    let store_keys = StoreKeys::bind(store_id.to_string());
    let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let key_a = MasterKeyring::generate();
    custody.persist(&key_a).expect("establish key A");
    let security = store_security(store_keys, custody.clone(), established_identity_custody());

    let fingerprint_a = match security
        .resolve_cloud_cipher(HomeStorage::Opaque)
        .expect("resolve key A")
    {
        CloudCipher::Encrypted(encryption) => encryption.fingerprint(),
        CloudCipher::Plaintext => panic!("opaque storage must resolve an encrypted cipher"),
    };
    let key_b = MasterKeyring::generate();
    custody.persist(&key_b).expect("replace custody with key B");
    let fingerprint_b = match security
        .resolve_cloud_cipher(HomeStorage::Opaque)
        .expect("resolve key B")
    {
        CloudCipher::Encrypted(encryption) => encryption.fingerprint(),
        CloudCipher::Plaintext => panic!("opaque storage must resolve an encrypted cipher"),
    };

    assert_eq!(fingerprint_a, key_a.fingerprint());
    assert_eq!(fingerprint_b, key_b.fingerprint());
    assert_ne!(fingerprint_a, fingerprint_b);
}

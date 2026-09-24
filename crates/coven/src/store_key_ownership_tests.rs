use coven_keys::keys::{test_keyring, StoreKeys};
use std::sync::Arc;

/// Deleting a store from this device leaves nothing of it behind: its
/// directory, and every keyring entry held for it — the device signing
/// identity, the master key, the cloud-home credentials, and the host's own
/// secrets — in one call, while a store still open is refused whole.
#[test]
fn deleting_a_closed_store_removes_its_directory_and_every_keyring_entry() {
    use coven_keys::keys::DeviceIdentityCustody;

    test_keyring::install();
    let store_id = "deleted-store";
    let directory = tempfile::tempdir().expect("app directory");
    let store_dir = crate::StoreDir::new_ephemeral(directory.path().join(store_id));
    let handle = crate::Coven::builder(
        store_dir.clone(),
        crate::Config::with_defaults(
            store_id.to_string(),
            "device-test".to_string(),
            "Deleted Store".to_string(),
        ),
    )
    .synced_tables(coven_replication::sync::test_helpers::test_synced_tables())
    .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
    .migrations(coven_replication::sync::test_helpers::test_migrations())
    .open()
    .expect("open store");
    handle.initialize_identity().expect("establish identity");
    handle
        .set_host_secret("api_token", "host-owned")
        .expect("store a host secret");
    let keys = StoreKeys::bind(store_id.to_string());
    keys.set_encryption_key(&"11".repeat(32))
        .expect("seed master key");
    keys.set_cloud_home_credentials(&coven_keys::keys::CloudHomeCredentials::S3 {
        access_key: "access".to_string(),
        secret_key: "secret".to_string(),
    })
    .expect("seed cloud credentials");

    assert!(
        matches!(
            crate::Coven::delete_store(&store_dir, store_id, &["api_token"]),
            Err(crate::StoreDeletionError::Open(_))
        ),
        "an open store is not deleted out from under its handle"
    );
    assert!(keys.unlock().expect("read identity").is_some());

    drop(handle);
    crate::Coven::delete_store(&store_dir, store_id, &["api_token"]).expect("delete the store");

    assert!(!store_dir.exists(), "the store directory is gone");
    assert!(keys.unlock().expect("read identity").is_none());
    assert_eq!(keys.get_encryption_key().expect("read master key"), None);
    assert!(keys
        .get_cloud_home_credentials()
        .expect("read cloud credentials")
        .is_none());
    assert_eq!(
        keys.get_host_secret("api_token").expect("read secret"),
        None
    );

    crate::Coven::delete_store(&store_dir, store_id, &["api_token"])
        .expect("deleting an already-deleted store is a retry that succeeds");
    assert!(!store_dir.exists());
}

#[tokio::test]
async fn caller_driven_test_home_establishes_the_master_key_it_needs() {
    test_keyring::install();
    let directory = tempfile::tempdir().expect("store directory");
    let mut config = crate::Config::with_defaults(
        "caller-driven-key-owner".to_string(),
        "device-test".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.provider = Some(crate::CloudProvider::S3);
    config.cloud_home.storage = crate::HomeStorage::Opaque;
    let handle = crate::Coven::builder(
        crate::StoreDir::new_ephemeral(directory.path()),
        config.clone(),
    )
    .synced_tables(coven_replication::sync::test_helpers::test_synced_tables())
    .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
    .migrations(coven_replication::sync::test_helpers::test_migrations())
    .open()
    .expect("open store");
    handle.initialize_identity().expect("establish identity");

    let started_connection = handle.connect_sync_with_test_home(
        Arc::new(crate::InMemoryCloudHome::new()),
        coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([7; 32])),
    );
    assert!(
        std::mem::size_of_val(&started_connection) <= 128,
        "the public started connection future embeds {} bytes of Coven internals in its host",
        std::mem::size_of_val(&started_connection)
    );
    drop(started_connection);

    let connection = handle.connect_sync_with_test_home_caller_driven(
        Arc::new(crate::InMemoryCloudHome::new()),
        coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([7; 32])),
    );
    assert!(
        std::mem::size_of_val(&connection) <= 128,
        "the public connection future embeds {} bytes of Coven internals in its host",
        std::mem::size_of_val(&connection)
    );
    connection.await.expect("set up caller-driven cloud home");

    assert_eq!(
        handle
            .cloud_home_key_state(crate::HomeStorage::Opaque)
            .expect("read key state"),
        crate::CloudHomeKeyState::Available
    );
    assert!(handle.is_connected());
    assert!(!handle.is_syncing());
}

#[test]
fn public_s3_setup_survives_a_narrow_host_stack() {
    const CHILD: &str = "COVEN_S3_NARROW_STACK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        test_keyring::install();
        let directory = tempfile::tempdir().expect("store directory");
        let mut config = crate::Config::with_defaults(
            "narrow-s3-setup".to_string(),
            "device-test".to_string(),
            "Test Store".to_string(),
        );
        config.cloud_home.provider = Some(crate::CloudProvider::S3);
        config.cloud_home.storage = crate::HomeStorage::Opaque;
        config.cloud_home.s3_bucket = Some("unreachable-bucket".to_string());
        config.cloud_home.s3_region = Some("us-east-1".to_string());
        config.cloud_home.s3_endpoint = Some("http://127.0.0.1:1".to_string());
        let cloud_home = config.cloud_home.clone();
        let handle =
            crate::Coven::builder(crate::StoreDir::new_ephemeral(directory.path()), config)
                .synced_tables(coven_replication::sync::test_helpers::test_synced_tables())
                .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
                .migrations(coven_replication::sync::test_helpers::test_migrations())
                .open()
                .expect("open store");
        handle.initialize_identity().expect("establish identity");
        let runtime = tokio::runtime::Runtime::new().expect("build host runtime");

        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("narrow-s3-host".to_string())
                .stack_size(512 * 1024)
                .spawn_scoped(scope, move || {
                    runtime
                        .block_on(handle.setup_s3_cloud_home(
                            cloud_home,
                            "access".to_string(),
                            "secret".to_string(),
                        ))
                        .expect_err("unreachable endpoint must reject setup");
                })
                .expect("spawn narrow S3 host")
                .join()
                .expect("narrow S3 host completes");
        });
        return;
    }

    let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .arg("public_s3_setup_survives_a_narrow_host_stack")
        .arg("--nocapture")
        .env(CHILD, "1")
        .status()
        .expect("run narrow-stack S3 subprocess");
    assert!(
        status.success(),
        "S3 setup overflowed its host stack: {status}"
    );
}

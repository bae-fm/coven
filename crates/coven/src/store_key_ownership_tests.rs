use coven_keys::keys::{test_keyring, StoreKeys};
use std::sync::Arc;

#[test]
fn coven_forgets_a_closed_store_master_key_without_exposing_store_keys() {
    test_keyring::install();
    let store_id = "closed-store-master-key";
    let keys = StoreKeys::bind(store_id.to_string());
    keys.set_encryption_key(&"11".repeat(32))
        .expect("seed master key");

    crate::Coven::forget_keyring_master_key(store_id).expect("forget master key");

    assert_eq!(keys.get_encryption_key().expect("read master key"), None);
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
    .migrations(coven_replication::sync::test_helpers::test_migrations())
    .open()
    .expect("open store");
    handle.initialize_identity().expect("establish identity");

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

use super::*;
use crate::*;
use coven_crypto::custody::StoreKeychain;
use coven_storage::Storage;
use uuid::Uuid;

fn s3_location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "household".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
        prefix: "libraries".into(),
    }
}

fn code(store: u128, location: &StorageConfig) -> RestoreCode {
    RestoreCode {
        store: StoreId(Uuid::from_u128(store)),
        member: MemberId(Uuid::from_u128(100)),
        member_keys: MemberKeys::generate().unwrap(),
        storage: location.encode().unwrap(),
    }
}

fn save(app: &TestCoven, code: &RestoreCode) -> StoreKeychain {
    let store = app.store_keychain(code.store);
    store
        .set_synced_restore_code(&SecretBytes::new(code.to_bytes().unwrap().to_vec()))
        .unwrap();
    store
}

#[tokio::test]
async fn listing_reports_each_saved_location_and_separate_s3_key_presence() {
    let app = TestCoven::new();
    assert!(app.saved_restore_codes().await.unwrap().is_empty());
    let locations = [
        s3_location(),
        StorageConfig::Dropbox {
            location: DropboxLocation::SharedFolder {
                namespace_id: "ana-shared-folder".into(),
            },
        },
        StorageConfig::Dropbox {
            location: DropboxLocation::AppFolder,
        },
        StorageConfig::OneDrive {
            drive_id: "ana-drive".into(),
            folder_id: "household-folder".into(),
        },
        StorageConfig::CloudKit {
            container: "iCloud.notes".into(),
            owner: "ana-record".into(),
            zone: "household-zone".into(),
        },
        s3_location(),
    ];
    let mut expected = Vec::new();
    // Save out of order; discovery keeps the existing store-id ordering.
    for (index, location) in locations.into_iter().enumerate().rev() {
        let code = code(index as u128 + 1, &location);
        let store = save(&app, &code);
        let has_s3_credentials = index == 0;
        if has_s3_credentials {
            store
                .set_synced_s3_credentials(
                    &S3Credentials {
                        access_key_id: "ana-key".into(),
                        secret_access_key: SecretText::new("never-in-the-list".into()),
                    }
                    .encode()
                    .unwrap(),
                )
                .unwrap();
        }
        expected.push(SavedRestoreCode {
            store_id: code.store,
            location,
            has_s3_credentials,
        });
    }
    expected.reverse();
    // Neither device-only custody nor a separate key without a code is a restore choice.
    let orphan = app.store_keychain(StoreId(Uuid::from_u128(99)));
    orphan.set_device_id(DeviceId(7)).unwrap();
    orphan.set_host_secret("token", "device secret").unwrap();
    orphan
        .set_synced_s3_credentials(&SecretBytes::new(b"orphan key".to_vec()))
        .unwrap();
    let saved = app.saved_restore_codes().await.unwrap();
    assert_eq!(saved, expected);
    assert_eq!(
        saved
            .iter()
            .map(|entry| entry.location.provider())
            .collect::<Vec<_>>(),
        [
            CloudProvider::S3,
            CloudProvider::Dropbox,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
            CloudProvider::CloudKit,
            CloudProvider::S3
        ]
    );
    assert_eq!(
        saved[1].location.oauth_access(),
        Some(OAuthAccess::Dropbox(DropboxAccess::FullDropbox))
    );
    assert_eq!(
        saved[2].location.oauth_access(),
        Some(OAuthAccess::Dropbox(DropboxAccess::AppFolder))
    );
    assert!(!format!("{saved:?}").contains("never-in-the-list"));
}

#[tokio::test]
async fn listing_fails_as_a_whole_when_discovery_or_a_separate_key_read_fails() {
    let app = TestCoven::new();
    for id in [1, 2] {
        save(&app, &code(id, &s3_location()));
    }
    for after in 0..3 {
        app.fail_keychain_operations_after(&[after]);
        assert!(matches!(
            app.saved_restore_codes().await,
            Err(BootstrapError::SecureStorage(KeyError::Keychain(_)))
        ));
        assert_eq!(app.saved_restore_codes().await.unwrap().len(), 2);
    }
}

#[tokio::test]
async fn listing_rejects_malformed_codes_locations_and_mismatched_store_ids() {
    let app = TestCoven::new();
    let valid = code(1, &s3_location());
    save(&app, &valid);
    let other = app.store_keychain(StoreId(Uuid::from_u128(2)));
    let malformed_location = RestoreCode {
        storage: SecretBytes::new(b"invalid location".to_vec()),
        ..code(2, &s3_location())
    };
    for bytes in [
        SecretBytes::new(b"invalid code".to_vec()),
        SecretBytes::new(malformed_location.to_bytes().unwrap().to_vec()),
        SecretBytes::new(valid.to_bytes().unwrap().to_vec()),
    ] {
        other.set_synced_restore_code(&bytes).unwrap();
        assert!(matches!(
            app.saved_restore_codes().await,
            Err(BootstrapError::Code(CodeError::Invalid))
        ));
    }
    other.delete_synced_restore_code().unwrap();
    assert_eq!(app.saved_restore_codes().await.unwrap().len(), 1);
}

#[tokio::test]
async fn listing_reports_presence_without_decoding_the_separately_saved_key() {
    let app = TestCoven::new();
    let store = save(&app, &code(1, &s3_location()));
    store
        .set_synced_s3_credentials(&SecretBytes::new(b"damaged key".to_vec()))
        .unwrap();
    assert!(app.saved_restore_codes().await.unwrap()[0].has_s3_credentials);
    store.delete_synced_s3_credentials().unwrap();
    assert!(!app.saved_restore_codes().await.unwrap()[0].has_s3_credentials);
}

#[tokio::test]
async fn setup_refresh_selected_restore_and_delete_share_one_entry_per_store() {
    let network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let device = &network.devices[0];
    let app = &device.app;
    let initial = app.saved_restore_codes().await.unwrap();
    assert_eq!(
        initial,
        [SavedRestoreCode {
            store_id: device.store,
            location: device.storage.config(),
            has_s3_credentials: true,
        }]
    );
    let saved = initial[0].clone();
    let unrelated = code(
        99,
        &StorageConfig::CloudKit {
            container: "iCloud.other".into(),
            owner: "other-owner".into(),
            zone: "other-zone".into(),
        },
    );
    save(app, &unrelated);
    let expected = app.saved_restore_codes().await.unwrap();
    device.handle.rename_store("Renamed library").await.unwrap();
    let mut current = device.handle.restore_code().await.unwrap();
    for _ in 0..2 {
        let refreshed = device.handle.replace_member_keys().await.unwrap();
        assert_ne!(refreshed.as_str(), current);
        current = refreshed.as_str().to_owned();
        assert_eq!(app.saved_restore_codes().await.unwrap(), expected);
    }
    // A selection made before replacement reads the current pair and separate key.
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let (_, cancel) = tokio::sync::watch::channel(false);
    let restored = restore_from_keychain(
        app.builder(layout.clone())
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .clock(Arc::new(crate::tests::PollingClock(network.clock.clone())))
            .storage_connector(device.storage.clone()),
        &saved,
        None,
        "Ana's laptop",
        |_| {},
        &cancel,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(restored.restore_code().await.unwrap(), current);
    assert_eq!(restored.store_name().await.unwrap(), "Renamed library");
    assert_eq!(app.saved_restore_codes().await.unwrap(), expected);
    restored.delete_store(IfUnsent::Refuse).await.unwrap();
    let remaining = app.saved_restore_codes().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].store_id, unrelated.store);
    assert!(app
        .store_keychain(saved.store_id)
        .synced_s3_credentials()
        .unwrap()
        .is_none());
    assert!(restore_from_keychain(
        app.builder(layout),
        &saved,
        None,
        "Missing selection",
        |_| panic!("bootstrap began"),
        &cancel,
    )
    .await
    .unwrap()
    .is_none());
    network.close().await;
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
#[tokio::test]
async fn native_listing_is_unavailable_off_apple_without_opening_a_keychain() {
    assert!(matches!(
        crate::saved_restore_codes().await,
        Err(BootstrapError::SecureStorage(KeyError::Unsupported))
    ));
}

use super::*;
use crate::*;
use coven_crypto::custody::SyncedKeychain;
use coven_format::codes::{KeychainRestoreCode, RestoreCode};
use coven_storage::Storage;
use uuid::Uuid;

fn s3_location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "household".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
        prefix: crate::S3Prefix::parse("libraries").unwrap(),
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

/// The name each fixture saves its store under.
fn name(store: StoreId) -> String {
    format!("Store {}", store.0.as_u128())
}

fn entry(code: &RestoreCode) -> SecretBytes {
    SecretBytes::new(
        KeychainRestoreCode::encode(&name(code.store), 1, code)
            .unwrap()
            .to_vec(),
    )
}

fn save(app: &TestCoven, code: &RestoreCode) -> SyncedKeychain {
    let store = app.synced_keychain(code.store);
    store.set_restore_code(&entry(code)).unwrap();
    store
}

#[tokio::test]
async fn listing_reports_each_saved_location_and_separate_s3_key_presence() {
    let app = TestCoven::new();
    assert!(crate::tests::readable(app.saved_restore_codes().await.unwrap()).is_empty());
    let locations = [
        s3_location(),
        StorageConfig::Dropbox {
            account_label: Some("ana@example.com".into()),
            location: DropboxLocation::SharedFolder {
                namespace_id: "ana-shared-folder".into(),
            },
        },
        StorageConfig::Dropbox {
            account_label: Some("ben@example.com".into()),
            location: DropboxLocation::AppFolder,
        },
        StorageConfig::OneDrive {
            account_label: Some("ana@work.example".into()),
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
        let decoded = decode_code_info(&code.to_text().unwrap()).unwrap();
        assert_eq!(decoded.metadata().store_id, code.store);
        assert_eq!(decoded.metadata().location, location);
        let store = save(&app, &code);
        let has_s3_credentials = index == 0;
        if has_s3_credentials {
            store
                .set_s3_credentials(
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
            name: name(code.store),
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
    app.synced_keychain(StoreId(Uuid::from_u128(99)))
        .set_s3_credentials(&SecretBytes::new(b"orphan key".to_vec()))
        .unwrap();
    let saved = crate::tests::readable(app.saved_restore_codes().await.unwrap());
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
async fn dropbox_accounts_are_distinguishable_in_both_saved_and_decoded_choices() {
    let app = TestCoven::new();
    let mut decoded = Vec::new();
    for (index, label) in ["ana@example.com", "ana@work.example"]
        .into_iter()
        .enumerate()
    {
        let location = StorageConfig::Dropbox {
            location: DropboxLocation::AppFolder,
            account_label: Some(label.into()),
        };
        let code = code(index as u128 + 1, &location);
        save(&app, &code);
        decoded.push(decode_code_info(&code.to_text().unwrap()).unwrap());
    }
    let saved = crate::tests::readable(app.saved_restore_codes().await.unwrap());
    assert_eq!(saved.len(), 2);
    assert_ne!(saved[0].location, saved[1].location);
    assert!(saved[0].location.same_location(&saved[1].location));
    for (saved, decoded) in saved.iter().zip(decoded) {
        assert_eq!(saved.store_id, decoded.metadata().store_id);
        assert_eq!(saved.location, decoded.metadata().location);
        assert!(matches!(decoded, CodeInfo::Restore(_)));
    }
}

#[tokio::test]
async fn listing_fails_as_a_whole_when_discovery_or_a_separate_key_read_fails() {
    let app = TestCoven::new();
    for id in [1, 2] {
        save(&app, &code(id, &s3_location()));
    }
    // An unreadable entry beside them changes nothing: a refused read says
    // nothing about any entry.
    app.synced_keychain(StoreId(Uuid::from_u128(3)))
        .set_restore_code(&SecretBytes::new(b"damaged".to_vec()))
        .unwrap();
    for after in 0..3 {
        app.fail_keychain_operations_after(&[after]);
        assert!(matches!(
            app.saved_restore_codes().await,
            Err(BootstrapError::SecureStorage(
                KeyError::KeychainTemporarilyUnavailable(_)
            ))
        ));
        assert_eq!(app.saved_restore_codes().await.unwrap().len(), 3);
    }
    app.refuse_synced_keychain_operations(true);
    assert!(matches!(
        app.saved_restore_codes().await,
        Err(BootstrapError::SecureStorage(
            KeyError::KeychainTemporarilyUnavailable(_)
        ))
    ));
}

#[tokio::test]
async fn listing_shows_an_unreadable_entry_beside_a_readable_one() {
    let app = TestCoven::new();
    let valid = code(1, &s3_location());
    save(&app, &valid);
    let readable = SavedRestoreEntry::Readable(SavedRestoreCode {
        name: name(valid.store),
        store_id: valid.store,
        location: s3_location(),
        has_s3_credentials: false,
    });
    let store = StoreId(Uuid::from_u128(2));
    let other = app.synced_keychain(store);
    let malformed_location = RestoreCode {
        storage: SecretBytes::new(b"invalid location".to_vec()),
        ..code(2, &s3_location())
    };
    for (bytes, reason) in [
        (
            SecretBytes::new(b"invalid code".to_vec()),
            UnreadableSavedCode::Format,
        ),
        // A bare restore code, as saved before entries carried the store's name.
        (
            SecretBytes::new(code(2, &s3_location()).to_bytes().unwrap().to_vec()),
            UnreadableSavedCode::Format,
        ),
        (entry(&malformed_location), UnreadableSavedCode::Format),
        // Store 1's entry, saved under store 2's account.
        (entry(&valid), UnreadableSavedCode::OtherStore),
    ] {
        other.set_restore_code(&bytes).unwrap();
        let saved = app.saved_restore_codes().await.unwrap();
        assert_eq!(
            saved,
            [
                readable.clone(),
                SavedRestoreEntry::Unreadable {
                    store_id: store,
                    reason
                }
            ]
        );
        assert_eq!(saved[1].store_id(), store);
    }
    other.delete_restore_code().unwrap();
    assert_eq!(app.saved_restore_codes().await.unwrap(), [readable]);
}

#[tokio::test]
async fn an_entry_more_than_one_keychain_group_holds_is_unreadable_without_reading_it() {
    let keychain = coven_crypto::custody::Keychain::in_memory("ambiguous").unwrap();
    let store = StoreId(Uuid::from_u128(1));
    // Reading nothing, it can't fail on a refusing keychain.
    keychain.refuse_synced_operations(true);
    assert_eq!(
        crate::saved_restore::entry(
            &SyncedKeychain::new(keychain, store),
            coven_crypto::custody::SyncedRestoreCode::Ambiguous
        )
        .unwrap(),
        SavedRestoreEntry::Unreadable {
            store_id: store,
            reason: UnreadableSavedCode::Ambiguous
        }
    );
}

#[tokio::test]
async fn forgetting_an_unreadable_entry_deletes_its_code_and_key_and_keeps_the_others() {
    let phone = TestCoven::new();
    let laptop = phone.another_device();
    let household = save(&phone, &code(1, &s3_location()));
    let key = SecretBytes::new(b"S3 key".to_vec());
    household.set_s3_credentials(&key).unwrap();
    let store = StoreId(Uuid::from_u128(2));
    let old = phone.synced_keychain(store);
    old.set_restore_code(&SecretBytes::new(
        code(2, &s3_location()).to_bytes().unwrap().to_vec(),
    ))
    .unwrap();
    old.set_s3_credentials(&key).unwrap();
    let listed = laptop.saved_restore_codes().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        listed[1],
        SavedRestoreEntry::Unreadable {
            store_id: store,
            reason: UnreadableSavedCode::Format
        }
    );
    laptop
        .forget_saved_restore_code(listed[1].store_id())
        .await
        .unwrap();
    for app in [&phone, &laptop] {
        let saved = crate::tests::readable(app.saved_restore_codes().await.unwrap());
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].store_id, StoreId(Uuid::from_u128(1)));
        assert!(saved[0].has_s3_credentials);
        let synced = app.synced_keychain(store);
        assert!(synced.restore_code().unwrap().is_none());
        assert!(synced.s3_credentials().unwrap().is_none());
    }
}

#[tokio::test]
async fn listing_reports_presence_without_decoding_the_separately_saved_key() {
    let app = TestCoven::new();
    let store = save(&app, &code(1, &s3_location()));
    store
        .set_s3_credentials(&SecretBytes::new(b"damaged key".to_vec()))
        .unwrap();
    assert!(crate::tests::readable(app.saved_restore_codes().await.unwrap())[0].has_s3_credentials);
    store.delete_s3_credentials().unwrap();
    assert!(
        !crate::tests::readable(app.saved_restore_codes().await.unwrap())[0].has_s3_credentials
    );
}

#[tokio::test]
async fn setup_refresh_and_restore_on_another_device_share_one_entry_per_store() {
    let network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let device = &network.devices[0];
    let app = &device.app;
    let initial = crate::tests::readable(app.saved_restore_codes().await.unwrap());
    assert_eq!(
        initial,
        [SavedRestoreCode {
            name: "Household".into(),
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
    device.handle.rename_store("Renamed library").await.unwrap();
    let expected = crate::tests::readable(app.saved_restore_codes().await.unwrap());
    let renamed = expected.iter().find(|entry| entry.store_id == device.store);
    assert_eq!(renamed.unwrap().name, "Renamed library");
    let mut current = device.handle.restore_code().await.unwrap();
    for _ in 0..2 {
        let refreshed = device.handle.replace_member_keys().await.unwrap();
        assert_ne!(refreshed.as_str(), current);
        current = refreshed.as_str().to_owned();
        assert_eq!(
            crate::tests::readable(app.saved_restore_codes().await.unwrap()),
            expected
        );
    }
    // A selection made before replacement reads the current pair and separate
    // key, on Ana's laptop, which iCloud Keychain gave the same entries.
    let laptop = app.another_device();
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let (_, cancel) = tokio::sync::watch::channel(false);
    let restored = restore_from_keychain(
        laptop
            .builder(layout.clone())
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .clock(Arc::new(crate::tests::PollingClock::new(
                network.clock.clone(),
            )))
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
    assert_eq!(
        crate::tests::readable(app.saved_restore_codes().await.unwrap()),
        expected
    );
    assert_eq!(
        crate::tests::readable(laptop.saved_restore_codes().await.unwrap()),
        expected
    );
    restored.close().await.unwrap();
    network.close().await;
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
#[tokio::test]
async fn native_listing_and_forgetting_are_unavailable_off_apple_without_opening_a_keychain() {
    assert!(matches!(
        crate::saved_restore_codes().await,
        Err(BootstrapError::SecureStorage(KeyError::Unsupported))
    ));
    assert!(matches!(
        crate::forget_saved_restore_code(StoreId(Uuid::from_u128(1))).await,
        Err(BootstrapError::SecureStorage(KeyError::Unsupported))
    ));
}

/// The saved entry's name and the slot that set it, as `app` sees it.
fn saved_name(app: &TestCoven, store: StoreId) -> (String, u64) {
    let saved = crate::tests::saved_entry(app, store).unwrap();
    (saved.name, saved.named_at)
}

#[tokio::test]
async fn a_local_rename_rewrites_only_the_saved_name() {
    let network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let device = &network.devices[0];
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Household".into(), 1)
    );
    let code = crate::tests::saved_entry(&device.app, device.store)
        .unwrap()
        .code
        .to_bytes()
        .unwrap();
    device.handle.rename_store("Family").await.unwrap();
    let renamed_at = device.handle.applied_slot().await;
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Family".into(), renamed_at)
    );
    assert_eq!(
        crate::tests::readable(device.app.saved_restore_codes().await.unwrap())[0].name,
        "Family"
    );
    let saved = crate::tests::saved_entry(&device.app, device.store).unwrap();
    assert_eq!(saved.code.to_bytes().unwrap(), code);
    network.close().await;
}

#[tokio::test]
async fn applying_another_devices_rename_rewrites_this_persons_saved_name() {
    let network = crate::tests::Network::new(2).await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    assert_eq!(saved_name(&ben.app, ben.store).0, "Household");
    ana.handle.rename_store("Family").await.unwrap();
    let renamed_at = ana.handle.applied_slot().await;
    network.sync(1).await;
    assert_eq!(ben.handle.store_name().await.unwrap(), "Family");
    assert_eq!(
        saved_name(&ben.app, ben.store),
        ("Family".into(), renamed_at)
    );
    // Ben's entry keeps Ben's own code.
    assert_eq!(
        crate::tests::saved_entry(&ben.app, ben.store)
            .unwrap()
            .code
            .to_text()
            .unwrap()
            .as_str(),
        ben.handle.restore_code().await.unwrap()
    );
    network.close().await;
}

#[tokio::test]
async fn opening_rewrites_an_older_saved_name_and_keeps_a_newer_one() {
    let mut network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    network.devices[0]
        .handle
        .rename_store("Family")
        .await
        .unwrap();
    let renamed_at = network.devices[0].handle.applied_slot().await;
    let device = network.devices.remove(0);
    device.handle.close().await.unwrap();
    let synced = device.app.synced_keychain(device.store);
    let saved = crate::tests::saved_entry(&device.app, device.store).unwrap();
    let rewrite = |name: &str, named_at: u64| {
        synced
            .set_restore_code(&SecretBytes::new(
                KeychainRestoreCode::encode(name, named_at, &saved.code)
                    .unwrap()
                    .to_vec(),
            ))
            .unwrap();
    };
    let reopen = || async {
        network
            .reopen(&device)
            .open(device.store)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    };
    // Another device saved Household before seeing the rename.
    rewrite("Household", 1);
    reopen().await;
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Family".into(), renamed_at)
    );
    // Another device has seen a later rename this one hasn't applied yet.
    rewrite("Home", renamed_at + 5);
    reopen().await;
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Home".into(), renamed_at + 5)
    );
    // A forgotten entry stays forgotten.
    device
        .app
        .forget_saved_restore_code(device.store)
        .await
        .unwrap();
    reopen().await;
    assert!(synced.restore_code().unwrap().is_none());
    // A rewrite that fails fails the opening, and changes nothing.
    rewrite("Household", 1);
    device.app.refuse_synced_keychain_operations(true);
    assert!(matches!(
        network.reopen(&device).open(device.store).await,
        Err(CovenError::Key(KeyError::KeychainTemporarilyUnavailable(_)))
    ));
    device.app.refuse_synced_keychain_operations(false);
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Household".into(), 1)
    );
    reopen().await;
    assert_eq!(
        saved_name(&device.app, device.store),
        ("Family".into(), renamed_at)
    );
}

#[tokio::test]
async fn a_rename_after_forgetting_saves_nothing_and_a_key_replacement_saves_again() {
    let network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let device = &network.devices[0];
    device
        .app
        .forget_saved_restore_code(device.store)
        .await
        .unwrap();
    device.handle.rename_store("Family").await.unwrap();
    assert!(crate::tests::readable(device.app.saved_restore_codes().await.unwrap()).is_empty());
    let code = device.handle.replace_member_keys().await.unwrap();
    let saved = crate::tests::saved_entry(&device.app, device.store).unwrap();
    assert_eq!(saved.name, "Family");
    assert_eq!(saved.code.to_text().unwrap().as_str(), code.as_str());
    network.close().await;
}

#[tokio::test]
async fn forgetting_deletes_the_code_and_key_from_every_device_and_repeats() {
    let phone = TestCoven::new();
    let laptop = phone.another_device();
    let household = save(&phone, &code(1, &s3_location()));
    household
        .set_s3_credentials(&SecretBytes::new(b"S3 key".to_vec()))
        .unwrap();
    save(&phone, &code(2, &s3_location()));
    let store = StoreId(Uuid::from_u128(1));
    assert_eq!(
        crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).len(),
        2
    );
    laptop.forget_saved_restore_code(store).await.unwrap();
    for app in [&phone, &laptop] {
        let saved = crate::tests::readable(app.saved_restore_codes().await.unwrap());
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].store_id, StoreId(Uuid::from_u128(2)));
        let synced = app.synced_keychain(store);
        assert!(synced.restore_code().unwrap().is_none());
        assert!(synced.s3_credentials().unwrap().is_none());
    }
    laptop.forget_saved_restore_code(store).await.unwrap();
    // A store with no saved entry at all.
    phone
        .forget_saved_restore_code(StoreId(Uuid::from_u128(3)))
        .await
        .unwrap();
    assert_eq!(
        crate::tests::readable(phone.saved_restore_codes().await.unwrap()).len(),
        1
    );
}

#[tokio::test]
async fn forgetting_fails_part_way_with_the_store_still_listed() {
    let app = TestCoven::new();
    let store = save(&app, &code(1, &s3_location()));
    store
        .set_s3_credentials(&SecretBytes::new(b"S3 key".to_vec()))
        .unwrap();
    let id = StoreId(Uuid::from_u128(1));
    app.fail_keychain_operations_after(&[1]);
    assert!(matches!(
        app.forget_saved_restore_code(id).await,
        Err(BootstrapError::SecureStorage(
            KeyError::KeychainTemporarilyUnavailable(_)
        ))
    ));
    assert!(store.s3_credentials().unwrap().is_none());
    assert_eq!(
        crate::tests::readable(app.saved_restore_codes().await.unwrap()).len(),
        1
    );
    app.forget_saved_restore_code(id).await.unwrap();
    assert!(crate::tests::readable(app.saved_restore_codes().await.unwrap()).is_empty());
}

/// The entry bytes a device can't read: another format, damaged, or another
/// store's code.
fn unreadable_entries() -> [SecretBytes; 3] {
    [
        SecretBytes::new(b"damaged".to_vec()),
        // A bare restore code, as saved before entries carried the name.
        SecretBytes::new(code(1, &s3_location()).to_bytes().unwrap().to_vec()),
        entry(&code(99, &s3_location())),
    ]
}

#[tokio::test]
async fn opening_replaces_an_unreadable_entry_with_the_current_code_and_name() {
    let mut network =
        crate::tests::Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    network.devices[0]
        .handle
        .rename_store("Family")
        .await
        .unwrap();
    let renamed_at = network.devices[0].handle.applied_slot().await;
    let current = network.devices[0].handle.restore_code().await.unwrap();
    let device = network.devices.remove(0);
    device.handle.close().await.unwrap();
    let synced = device.app.synced_keychain(device.store);
    let reopen = || async {
        network
            .reopen(&device)
            .open(device.store)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    };
    let expect_current = || {
        let saved = crate::tests::saved_entry(&device.app, device.store).unwrap();
        assert_eq!(
            (saved.name.as_str(), saved.named_at),
            ("Family", renamed_at)
        );
        assert_eq!(saved.code.to_text().unwrap().as_str(), current);
    };
    for unreadable in unreadable_entries() {
        synced.set_restore_code(&unreadable).unwrap();
        reopen().await;
        expect_current();
    }
    // Copies in two keychain groups become one.
    synced.set_restore_code_in_another_group(&SecretBytes::new(b"old group".to_vec()));
    assert!(matches!(
        device.app.saved_restore_codes().await.unwrap()[..],
        [SavedRestoreEntry::Unreadable {
            reason: UnreadableSavedCode::Ambiguous,
            ..
        }]
    ));
    reopen().await;
    expect_current();
    assert!(matches!(
        device.app.saved_restore_codes().await.unwrap()[..],
        [SavedRestoreEntry::Readable(_)]
    ));
    // A forgotten entry stays forgotten.
    device
        .app
        .forget_saved_restore_code(device.store)
        .await
        .unwrap();
    reopen().await;
    assert!(synced.restore_code().unwrap().is_none());
    // A refused read still fails the opening and changes nothing.
    synced.set_restore_code(&unreadable_entries()[0]).unwrap();
    device.app.refuse_synced_keychain_operations(true);
    assert!(matches!(
        network.reopen(&device).open(device.store).await,
        Err(CovenError::Key(KeyError::KeychainTemporarilyUnavailable(_)))
    ));
    device.app.refuse_synced_keychain_operations(false);
    assert_eq!(
        synced.restore_code().unwrap().unwrap().as_bytes(),
        b"damaged"
    );
}

#[tokio::test]
async fn a_rename_replaces_an_unreadable_entry_with_the_current_code() {
    let network = crate::tests::Network::new(2).await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    for (index, unreadable) in unreadable_entries().into_iter().enumerate() {
        // Ana renames on her own device, then Ben's device applies her rename.
        for (device, renaming) in [(ana, true), (ben, false)] {
            let name = format!("Family {index}");
            device
                .app
                .synced_keychain(device.store)
                .set_restore_code(&SecretBytes::new(unreadable.as_bytes().to_vec()))
                .unwrap();
            if renaming {
                ana.handle.rename_store(&name).await.unwrap();
            } else {
                network.sync(1).await;
                assert_eq!(ben.handle.store_name().await.unwrap(), name);
            }
            let saved = crate::tests::saved_entry(&device.app, device.store).unwrap();
            assert_eq!(saved.name, name);
            assert_eq!(
                saved.code.to_text().unwrap().as_str(),
                device.handle.restore_code().await.unwrap()
            );
        }
    }
    network.close().await;
}

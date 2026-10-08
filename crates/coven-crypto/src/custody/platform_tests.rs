use super::*;

#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn native_device_entries_do_not_sync_and_cannot_move_to_another_device() {
    use apple_native_keyring_store::protected::{AccessPolicy, Cred};

    // Building entries inspects the real configuration without accessing secrets.
    let native = NativeKeychain::new().unwrap();
    for account in [
        "store-keys:1",
        "member-keys:1",
        "host-secret-names:1",
        "host-secret-746f6b656e:1",
    ] {
        let entry = native
            .entry(EntryScope::DeviceOnly, "coven-test", account)
            .unwrap();
        let credential = entry.as_any().downcast_ref::<Cred>().unwrap();
        assert!(!credential.cloud_synchronize);
        assert_eq!(
            credential.access_policy,
            AccessPolicy::WhenUnlockedThisDeviceOnly
        );
    }
    let entry = native
        .entry(EntryScope::Synced, "coven-test", "restore-code:1")
        .unwrap();
    assert!(
        entry
            .as_any()
            .downcast_ref::<Cred>()
            .unwrap()
            .cloud_synchronize
    );
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn native_search_discovers_restore_codes_and_propagates_read_failures() {
    use keyring_core::api::CredentialStoreApi;

    let synced = keyring_core::mock::Store::new().unwrap();
    let device = keyring_core::mock::Store::new().unwrap();
    let native = NativeKeychain {
        device_only: device.clone(),
        synced: synced.clone(),
    };
    let id = StoreId(uuid::Uuid::from_u128(1));
    let account = format!("restore-code:{id}");
    device
        .build("app", &account, None)
        .unwrap()
        .set_secret(b"device")
        .unwrap();
    synced
        .build("unrelated", &account, None)
        .unwrap()
        .set_secret(b"foreign")
        .unwrap();
    // An unrelated synced entry has no readable secret: filtering must happen
    // before get_secret, and a prefix match must include the separator.
    synced.build("app", "restore-code-other:1", None).unwrap();
    assert!(native.synced_restore_codes("app").unwrap().is_empty());
    let entry = synced.build("app", &account, None).unwrap();
    entry.set_secret(b"restore code").unwrap();
    let codes = native.synced_restore_codes("app").unwrap();
    assert_eq!(codes.len(), 1);
    assert_eq!(codes[0].0, id);
    assert_eq!(codes[0].1.as_bytes(), b"restore code");
    entry
        .as_any()
        .downcast_ref::<keyring_core::mock::Cred>()
        .unwrap()
        .set_error(keyring_core::Error::NoEntry);
    assert!(matches!(
        native.synced_restore_codes("app"),
        Err(KeyError::Keychain(_))
    ));
    assert_eq!(native.synced_restore_codes("app").unwrap().len(), 1);
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
#[test]
fn native_non_apple_sync_operations_are_unsupported() {
    // No live credential store or platform service is needed to reject sync.
    let native = NativeKeychain {
        device_only: keyring_core::mock::Store::new().unwrap(),
    };
    assert!(matches!(
        native.read(EntryScope::Synced, "app", "restore-code:1"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.write(EntryScope::Synced, "app", "restore-code:1", b"code"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.delete(EntryScope::Synced, "app", "restore-code:1"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.synced_restore_codes("app"),
        Err(KeyError::Unsupported)
    ));
}

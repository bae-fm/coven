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
    for account in ["restore-code:1", "s3-credentials:1"] {
        let entry = native
            .entry(EntryScope::Person, "coven-test", account)
            .unwrap();
        assert!(
            entry
                .as_any()
                .downcast_ref::<Cred>()
                .unwrap()
                .cloud_synchronize
        );
    }
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
    synced
        .build("app", &format!("s3-credentials:{id}"), None)
        .unwrap();
    assert!(native.person_restore_codes("app").unwrap().is_empty());
    let entry = synced.build("app", &account, None).unwrap();
    entry.set_secret(b"restore code").unwrap();
    let codes = native.person_restore_codes("app").unwrap();
    assert_eq!(codes.len(), 1);
    assert_eq!(codes[0].0, id);
    assert_eq!(codes[0].1.as_bytes(), b"restore code");
    entry
        .as_any()
        .downcast_ref::<keyring_core::mock::Cred>()
        .unwrap()
        .set_error(keyring_core::Error::NoEntry);
    assert!(matches!(
        native.person_restore_codes("app"),
        Err(KeyError::Keychain(_))
    ));
    assert_eq!(native.person_restore_codes("app").unwrap().len(), 1);
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
#[test]
fn native_non_apple_sync_operations_are_unsupported() {
    // No live credential store or platform service is needed to reject sync.
    let native = NativeKeychain {
        device_only: keyring_core::mock::Store::new().unwrap(),
    };
    assert!(matches!(
        native.read(EntryScope::Person, "app", "restore-code:1"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.write(EntryScope::Person, "app", "restore-code:1", b"code"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.delete(EntryScope::Person, "app", "restore-code:1"),
        Err(KeyError::Unsupported)
    ));
    assert!(matches!(
        native.person_restore_codes("app"),
        Err(KeyError::Unsupported)
    ));
}

// The core mock rejects all modifiers. Check the device policy here, then
// delegate entry operations to that mock so native failures remain injectable.
struct DeviceEntries(Arc<keyring_core::mock::Store>);

impl keyring_core::api::CredentialStoreApi for DeviceEntries {
    fn vendor(&self) -> String {
        self.0.vendor()
    }
    fn id(&self) -> String {
        self.0.id()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn persistence(&self) -> keyring_core::CredentialPersistence {
        self.0.persistence()
    }
    fn build(
        &self,
        service: &str,
        account: &str,
        modifiers: Option<&std::collections::HashMap<&str, &str>>,
    ) -> keyring_core::Result<keyring_core::Entry> {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        assert_eq!(
            modifiers,
            Some(&std::collections::HashMap::from([(
                "access-policy",
                "when-unlocked-this-device-only"
            )]))
        );
        #[cfg(not(any(target_os = "macos", target_os = "ios")))]
        assert!(modifiers.is_none());
        self.0.build(service, account, None)
    }
}

#[test]
fn inaccessible_native_custody_is_distinct_on_read_write_and_delete() {
    use keyring_core::api::CredentialStoreApi;
    use std::error::Error;
    let store = keyring_core::mock::Store::new().unwrap();
    let native = NativeKeychain {
        device_only: Arc::new(DeviceEntries(store.clone())),
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        synced: store.clone(),
    };
    for operation in ["read", "write", "delete"] {
        let entry = store.build("app", "secret", None).unwrap();
        entry.set_secret(b"retained secret").unwrap();
        let credential = entry
            .as_any()
            .downcast_ref::<keyring_core::mock::Cred>()
            .unwrap();
        credential.set_error(keyring_core::Error::NoStorageAccess(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "locked native session",
            ),
        )));
        let error = match operation {
            "read" => native
                .read(EntryScope::DeviceOnly, "app", "secret")
                .unwrap_err(),
            "write" => native
                .write(EntryScope::DeviceOnly, "app", "secret", b"replacement")
                .unwrap_err(),
            "delete" => native
                .delete(EntryScope::DeviceOnly, "app", "secret")
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert!(
            matches!(error, KeyError::KeychainTemporarilyUnavailable(_)),
            "{error:?}"
        );
        let cause = error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<keyring_core::Error>()
            .unwrap();
        let keyring_core::Error::NoStorageAccess(native_cause) = cause else {
            panic!("native cause lost");
        };
        let native_cause = native_cause.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(native_cause.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(native_cause.to_string(), "locked native session");
        assert!(!format!("{error:?} {error}").contains("locked native session"));
        assert_eq!(entry.get_secret().unwrap(), b"retained secret");
        credential.set_error(keyring_core::Error::PlatformFailure(Box::new(
            std::io::Error::other("native failure"),
        )));
        assert!(matches!(
            native.read(EntryScope::DeviceOnly, "app", "secret"),
            Err(KeyError::Keychain(_))
        ));
    }
    native
        .delete(EntryScope::DeviceOnly, "app", "secret")
        .unwrap();
    assert!(native
        .read(EntryScope::DeviceOnly, "app", "secret")
        .unwrap()
        .is_none());
    native
        .delete(EntryScope::DeviceOnly, "app", "secret")
        .unwrap();
}

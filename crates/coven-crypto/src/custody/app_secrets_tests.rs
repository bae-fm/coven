use super::*;
use crate::custody::StoreKeychain;
use coven_foundation::id_source::DeviceId;
use uuid::Uuid;

fn device_names(keychain: &Keychain) -> BTreeSet<String> {
    match keychain.read(EntryScope::DeviceOnly, DEVICE_SECRET_NAMES_ENTRY).unwrap() {
        Some(bytes) => {
            decode_names(bytes.as_bytes()).unwrap().into_iter().map(str::to_owned).collect()
        }
        None => BTreeSet::new(),
    }
}

/// A device secret is one entry for the whole device: every store reads the
/// same value, it never reaches the person's other devices, and deleting a
/// store leaves it.
#[test]
fn a_device_secret_outlives_every_store_and_stays_on_this_device() {
    let keychain = Keychain::in_memory("device-secrets").unwrap();
    let first = StoreKeychain::new(keychain.clone(), StoreId(Uuid::from_u128(1)));
    first.set_device_id(DeviceId(1)).unwrap();
    first.set_host_secret("speaker", "the store's own").unwrap();
    assert_eq!(keychain.device_secret("speaker").unwrap(), None);
    keychain.set_device_secret("speaker", "pairing keys").unwrap();
    keychain.set_device_secret("", "empty name").unwrap();
    assert_eq!(first.host_secret("speaker").unwrap().as_deref(), Some("the store's own"));
    first.delete_device_entries().unwrap();
    assert_eq!(keychain.device_secret("speaker").unwrap().as_deref(), Some("pairing keys"));
    assert_eq!(keychain.device_secret("").unwrap().as_deref(), Some("empty name"));
    for name in ["speaker", ""] {
        let value = device_secret_entry(name);
        assert!(!value.contains(':'));
        assert!(keychain.read(EntryScope::Person, &value).unwrap().is_none());
    }
    assert_eq!(
        keychain.another_device().device_secret("speaker").unwrap(),
        None,
        "another device of the person has no copy"
    );
    keychain.set_device_secret("speaker", "replaced").unwrap();
    assert_eq!(keychain.device_secret("speaker").unwrap().as_deref(), Some("replaced"));
    keychain.delete_device_secret("speaker").unwrap();
    keychain.delete_device_secret("speaker").unwrap();
    assert_eq!(keychain.device_secret("speaker").unwrap(), None);
    assert_eq!(device_names(&keychain), BTreeSet::from([String::new()]));
}

/// Deleting every device secret finds each one through the list, leaves each
/// store's entries, and leaves nothing of the device's behind.
#[test]
fn deleting_device_secrets_removes_every_listed_one_and_no_store_entry() {
    let keychain = Keychain::in_memory("device-secret-reset").unwrap();
    let store = StoreKeychain::new(keychain.clone(), StoreId(Uuid::from_u128(1)));
    store.set_device_id(DeviceId(7)).unwrap();
    store.set_host_secret("token", "kept").unwrap();
    for name in ["first", "second"] {
        keychain.set_device_secret(name, "value").unwrap();
    }
    keychain.delete_device_secrets().unwrap();
    keychain.delete_device_secrets().unwrap();
    for name in ["first", "second"] {
        assert_eq!(keychain.device_secret(name).unwrap(), None);
    }
    assert_nothing_left(&keychain);
    assert_eq!(store.device_id().unwrap(), Some(DeviceId(7)));
    assert_eq!(store.host_secret("token").unwrap().as_deref(), Some("kept"));
}

fn assert_nothing_left(keychain: &Keychain) {
    assert!(keychain.read(EntryScope::DeviceOnly, DEVICE_SECRET_NAMES_ENTRY).unwrap().is_none());
    for name in ["first", "second"] {
        let account = device_secret_entry(name);
        assert!(keychain.read(EntryScope::DeviceOnly, &account).unwrap().is_none());
    }
}

fn assert_list_covers_values(keychain: &Keychain) {
    let names = device_names(keychain);
    for name in ["first", "second"] {
        if keychain.device_secret(name).unwrap().is_some() {
            assert!(names.contains(name), "{name} has a value and no listed name");
        }
    }
}

/// Whichever keychain step fails, the list still names every device secret
/// that exists, and deleting them all again finishes.
#[test]
fn device_secret_steps_keep_the_list_covering_every_value_through_failures() {
    // Saving reads the list, writes it, then writes the value.
    for after in 0..3 {
        let keychain = Keychain::in_memory("device-secret-save-failures").unwrap();
        keychain.set_device_secret("first", "value").unwrap();
        keychain.fail_operations_after(&[after]);
        assert!(keychain.set_device_secret("second", "value").is_err());
        assert_list_covers_values(&keychain);
        keychain.delete_device_secrets().unwrap();
        assert_nothing_left(&keychain);
    }
    // Deleting all reads the list, deletes two values, then the list.
    for after in 0..4 {
        let keychain = Keychain::in_memory("device-secret-delete-failures").unwrap();
        keychain.set_device_secret("first", "value").unwrap();
        keychain.set_device_secret("second", "value").unwrap();
        keychain.fail_operations_after(&[after]);
        assert!(keychain.delete_device_secrets().is_err());
        assert_list_covers_values(&keychain);
        keychain.delete_device_secrets().unwrap();
        assert_nothing_left(&keychain);
    }
}

/// A damaged list refuses every change and deletion, and loses no value.
#[test]
fn a_damaged_device_secret_list_refuses_changes_without_losing_secrets() {
    let keychain = Keychain::in_memory("damaged-device-names").unwrap();
    keychain.set_device_secret("speaker", "pairing keys").unwrap();
    keychain.write(EntryScope::DeviceOnly, DEVICE_SECRET_NAMES_ENTRY, b"unknown version").unwrap();
    for result in [
        keychain.set_device_secret("new", "value"),
        keychain.delete_device_secret("speaker"),
        keychain.delete_device_secrets(),
    ] {
        assert!(matches!(result, Err(KeyError::Material(MaterialError::Encoding))));
    }
    assert_eq!(keychain.device_secret("speaker").unwrap().as_deref(), Some("pairing keys"));
    assert_eq!(keychain.device_secret("new").unwrap(), None);
}

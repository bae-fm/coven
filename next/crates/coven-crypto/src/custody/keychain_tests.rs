use super::*;
use crate::custody::{KeychainError, MemberKeyCustody, StoreKeyCustody};
use crate::{MemberKeys, StoreKey, StoreKeyring};
use coven_foundation::id_source::KeyId;
use uuid::Uuid;

#[test]
#[should_panic(expected = "keyring service name lock is poisoned")]
fn registration_propagates_a_poisoned_service_lock() {
    let service = Mutex::new(None);
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = service.lock().unwrap();
        panic!("poison service name");
    });
    assert!(poisoned.is_err());
    let _registration = register(&service, "service".into());
}

#[test]
#[should_panic(expected = "in-memory keychain entries lock is poisoned")]
fn discovery_propagates_a_poisoned_entries_lock() {
    let fake = Keychain::in_memory("poisoned").unwrap();
    let Backend::Memory(memory) = &fake.backend else {
        panic!("expected in-memory keychain");
    };
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = memory.lock().unwrap();
        panic!("poison keychain entries");
    });
    assert!(poisoned.is_err());
    let _codes = fake.synced_restore_codes();
}

#[test]
fn only_restore_codes_sync_and_stores_can_be_discovered_without_ids() {
    let fake = Keychain::in_memory("restore").unwrap();
    let a_id = StoreId(Uuid::from_u128(1));
    let b_id = StoreId(Uuid::from_u128(2));
    let a = Arc::new(StoreKeychain::new(fake.clone(), a_id));
    let b = StoreKeychain::new(fake.clone(), b_id);
    let keys = StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ));
    let member = MemberKeys::generate().unwrap();
    let store_custody = KeyringCustody::<StoreKeyring>::new(a.clone());
    let member_custody = KeyringCustody::<MemberKeys>::new(a.clone());
    store_custody.persist(&keys).unwrap();
    member_custody.persist(&member).unwrap();
    a.set_host_secret("api-token", "local secret").unwrap();
    a.set_storage_credentials(&SecretBytes::new(b"device credentials".to_vec()))
        .unwrap();
    assert!(fake.synced_restore_codes().unwrap().is_empty());
    for name in [
        STORE_KEYS_ENTRY,
        MEMBER_KEYS_ENTRY,
        CREDENTIALS_ENTRY,
        "api-token",
    ] {
        assert!(fake
            .read(EntryScope::DeviceOnly, &a.account(name))
            .unwrap()
            .is_some());
        assert!(fake
            .read(EntryScope::Synced, &a.account(name))
            .unwrap()
            .is_none());
    }
    assert!(a.synced_restore_code().unwrap().is_none());
    a.delete_synced_restore_code().unwrap();
    // Include non-UTF-8 bytes: custody keeps the complete opaque code unchanged.
    let first = SecretBytes::new(vec![0, 0xff, 42]);
    a.set_synced_restore_code(&first).unwrap();
    assert!(fake
        .read(EntryScope::DeviceOnly, &a.account(RESTORE_CODE_ENTRY))
        .unwrap()
        .is_none());
    assert_eq!(
        a.synced_restore_code().unwrap().unwrap().as_bytes(),
        first.as_bytes()
    );
    assert!(b.synced_restore_code().unwrap().is_none());
    b.set_synced_restore_code(&SecretBytes::new(b"code-b".to_vec()))
        .unwrap();
    a.set_synced_restore_code(&SecretBytes::new(b"replacement".to_vec()))
        .unwrap();
    let codes = fake.synced_restore_codes().unwrap();
    assert_eq!(
        codes.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![a_id, b_id]
    );
    assert_eq!(codes[0].1.as_bytes(), b"replacement");
    assert_eq!(codes[1].1.as_bytes(), b"code-b");
    a.delete_synced_restore_code().unwrap();
    a.delete_synced_restore_code().unwrap();
    assert!(a.synced_restore_code().unwrap().is_none());
    assert_eq!(fake.synced_restore_codes().unwrap()[0].0, b_id);
    assert_eq!(
        store_custody
            .unlock()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        keys.to_secret_bytes().as_bytes()
    );
    assert_eq!(
        member_custody.unlock().unwrap().unwrap().member_id(),
        member.member_id()
    );
    assert_eq!(
        a.host_secret("api-token").unwrap().as_deref(),
        Some("local secret")
    );
    a.set_synced_restore_code(&first).unwrap();
    store_custody.forget().unwrap();
    member_custody.forget().unwrap();
    a.delete_host_secret("api-token").unwrap();
    assert_eq!(
        a.synced_restore_code().unwrap().unwrap().as_bytes(),
        first.as_bytes()
    );
}

#[test]
fn synced_restore_code_failures_preserve_the_previous_code() {
    let fake = Keychain::in_memory("restore-failures").unwrap();
    let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let original = SecretBytes::new(b"original".to_vec());
    store.set_synced_restore_code(&original).unwrap();
    fake.fail_next_operation();
    assert!(matches!(
        store.set_synced_restore_code(&SecretBytes::new(b"replacement".to_vec())),
        Err(KeyError::Keychain(_))
    ));
    assert_eq!(
        store.synced_restore_code().unwrap().unwrap().as_bytes(),
        original.as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        store.delete_synced_restore_code(),
        Err(KeyError::Keychain(_))
    ));
    assert_eq!(
        store.synced_restore_code().unwrap().unwrap().as_bytes(),
        original.as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        store.synced_restore_code(),
        Err(KeyError::Keychain(_))
    ));
    fake.fail_next_operation();
    assert!(matches!(
        fake.synced_restore_codes(),
        Err(KeyError::Keychain(_))
    ));
    assert_eq!(
        fake.synced_restore_codes().unwrap()[0].1.as_bytes(),
        original.as_bytes()
    );
}

#[test]
fn discovery_filters_scope_and_kind_but_rejects_malformed_and_ambiguous_ids() {
    let fake = Keychain::in_memory("discovery").unwrap();
    fake.write(
        EntryScope::DeviceOnly,
        "restore-code:invalid",
        b"device only",
    )
    .unwrap();
    fake.write(EntryScope::Synced, "host-secret:invalid", b"another entry")
        .unwrap();
    assert!(fake.synced_restore_codes().unwrap().is_empty());
    fake.write(EntryScope::Synced, "restore-code:invalid", b"bad id")
        .unwrap();
    assert!(matches!(
        fake.synced_restore_codes(),
        Err(KeyError::RestoreCodeStoreId)
    ));
    fake.delete(EntryScope::Synced, "restore-code:invalid")
        .unwrap();
    let id = StoreId(Uuid::from_u128(1));
    for account in [
        format!("restore-code:{id}"),
        format!("restore-code:{}", id.0.simple()),
    ] {
        fake.write(EntryScope::Synced, &account, b"duplicate")
            .unwrap();
    }
    assert!(
        matches!(fake.synced_restore_codes(), Err(KeyError::AmbiguousRestoreCode(store)) if store == id)
    );
}

#[test]
fn host_names_cannot_collide_with_coven_or_another_store() {
    let fake = Keychain::in_memory("names").unwrap();
    let a = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let b = StoreKeychain::new(fake, StoreId(Uuid::from_u128(2)));
    for (name, expected) in [
        ("", SecretNameError::Empty),
        ("a:b", SecretNameError::Separator),
        ("a\0b", SecretNameError::Nul),
        (STORE_KEYS_ENTRY, SecretNameError::Reserved),
        (MEMBER_KEYS_ENTRY, SecretNameError::Reserved),
        (RESTORE_CODE_ENTRY, SecretNameError::Reserved),
        (CREDENTIALS_ENTRY, SecretNameError::Reserved),
    ] {
        assert_eq!(validate_host_name(name), Err(expected));
        assert!(matches!(
            a.set_host_secret(name, "secret"),
            Err(KeyError::SecretName(_))
        ));
        assert!(matches!(a.host_secret(name), Err(KeyError::SecretName(_))));
        assert!(matches!(
            a.delete_host_secret(name),
            Err(KeyError::SecretName(_))
        ));
    }
    assert_eq!(a.host_secret("api-token").unwrap(), None);
    a.set_host_secret("api-token", "token-a").unwrap();
    b.set_host_secret("api-token", "token-b").unwrap();
    assert_eq!(
        a.host_secret("api-token").unwrap().as_deref(),
        Some("token-a")
    );
    assert_eq!(
        b.host_secret("api-token").unwrap().as_deref(),
        Some("token-b")
    );
    a.set_host_secret("empty-value", "").unwrap();
    assert_eq!(a.host_secret("empty-value").unwrap().as_deref(), Some(""));
    a.delete_host_secret("api-token").unwrap();
    a.delete_host_secret("api-token").unwrap();
    assert_eq!(a.host_secret("api-token").unwrap(), None);
    assert_eq!(
        b.host_secret("api-token").unwrap().as_deref(),
        Some("token-b")
    );
}

#[test]
fn present_but_empty_or_malformed_key_entries_are_errors() {
    let fake = Keychain::in_memory("corrupt").unwrap();
    let store = Arc::new(StoreKeychain::new(fake, StoreId(Uuid::from_u128(1))));
    store.write(STORE_KEYS_ENTRY, &[]).unwrap();
    store.write(MEMBER_KEYS_ENTRY, &[0xff]).unwrap();
    assert!(matches!(
        KeyringCustody::<StoreKeyring>::new(store.clone()).unlock(),
        Err(KeyError::Material(_))
    ));
    assert!(matches!(
        KeyringCustody::<MemberKeys>::new(store.clone()).unlock(),
        Err(KeyError::Material(_))
    ));
    store.write("app-token", &[0xff]).unwrap();
    assert!(matches!(
        store.host_secret("app-token"),
        Err(KeyError::HostSecretEncoding)
    ));
}

#[test]
fn registration_is_atomic_and_cannot_switch_services() {
    let service = Mutex::new(None);
    for name in ["", "nul\0name"] {
        assert!(matches!(
            register(&service, name.into()),
            Err(KeyError::InvalidServiceName)
        ));
        assert!(service.lock().unwrap().is_none());
    }
    register(&service, "service".into()).unwrap();
    register(&service, "service".into()).unwrap();
    assert!(matches!(
        register(&service, "different".into()),
        Err(KeyError::ServiceAlreadyRegistered)
    ));
    for name in ["", "nul\0name"] {
        assert!(matches!(
            register(&service, name.into()),
            Err(KeyError::InvalidServiceName)
        ));
    }
    assert_eq!(service.lock().unwrap().as_deref(), Some("service"));
}

#[test]
fn native_errors_erase_malformed_secret_bytes_before_debugging() {
    let sentinel = b"secret-must-not-print";
    let cause = KeychainError::from(keyring_core::Error::BadEncoding(sentinel.to_vec()));
    let error = KeyError::from(cause);
    assert!(!format!("{error:?}").contains(&format!("{sentinel:?}")));
    let KeyError::Keychain(cause) = error else {
        panic!("keychain error")
    };
    assert!(!format!("{:?}", std::error::Error::source(&cause).unwrap()).contains("115, 101, 99"));
}

#[test]
fn deletion_checks_all_names_then_removes_only_this_stores_entries() {
    let fake = Keychain::in_memory("delete").unwrap();
    let first = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let second = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(2)));
    for store in [&first, &second] {
        store.set_device_id(DeviceId(42)).unwrap();
        store.write(STORE_KEYS_ENTRY, b"store keys").unwrap();
        store.write(MEMBER_KEYS_ENTRY, b"member keys").unwrap();
        store
            .set_storage_credentials(&SecretBytes::new(b"credentials".to_vec()))
            .unwrap();
        store.set_host_secret("token", "token").unwrap();
        store
            .set_synced_restore_code(&SecretBytes::new(b"restore".to_vec()))
            .unwrap();
    }
    assert!(matches!(
        first.delete_store_entries(&["token", "device-id"]),
        Err(KeyError::SecretName(SecretNameError::Reserved))
    ));
    assert_eq!(
        first.host_secret("token").unwrap().as_deref(),
        Some("token")
    );
    first.delete_store_entries(&["token"]).unwrap();
    first.delete_store_entries(&["token"]).unwrap();
    for name in [
        DEVICE_ID_ENTRY,
        STORE_KEYS_ENTRY,
        MEMBER_KEYS_ENTRY,
        CREDENTIALS_ENTRY,
        "token",
    ] {
        assert!(first.read(name).unwrap().is_none());
        assert!(second.read(name).unwrap().is_some());
    }
    assert!(first.synced_restore_code().unwrap().is_none());
    assert!(second.synced_restore_code().unwrap().is_some());
    assert_eq!(second.device_id().unwrap(), Some(DeviceId(42)));
    second.write(DEVICE_ID_ENTRY, &[1]).unwrap();
    assert!(matches!(
        second.device_id(),
        Err(KeyError::DeviceIdEncoding)
    ));
}

use super::*;
use crate::custody::person_entries::{RESTORE_CODE_ENTRY, S3_CREDENTIALS_ENTRY};
use crate::custody::{KeychainError, MemberKeyCustody, PersonEntries, StoreKeyCustody};
use crate::{MemberIdentity, StoreKey, StoreKeyring};
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
    let memory = fake.memory_backend();
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = memory.lock().unwrap();
        panic!("poison keychain entries");
    });
    assert!(poisoned.is_err());
    let _codes = fake.person_restore_codes();
}

#[test]
fn restore_codes_and_s3_keys_sync_separately_and_only_codes_discover_stores() {
    let fake = Keychain::in_memory("restore").unwrap();
    let a_id = StoreId(Uuid::from_u128(1));
    let b_id = StoreId(Uuid::from_u128(2));
    let a = Arc::new(StoreKeychain::new(fake.clone(), a_id));
    let a_person = PersonEntries::new(fake.clone(), a_id);
    let b_person = PersonEntries::new(fake.clone(), b_id);
    let keys = StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ));
    let member = MemberIdentity::generate(coven_foundation::id_source::MemberId(
        uuid::Uuid::from_u128(1),
    ))
    .unwrap();
    let store_custody = KeyringCustody::<StoreKeyring>::new(a.clone());
    let member_custody = KeyringCustody::<MemberIdentity>::new(a.clone());
    store_custody.persist(&keys).unwrap();
    member_custody.persist(&member).unwrap();
    a.set_host_secret("api-token", "local secret").unwrap();
    a.set_storage_credentials(&SecretBytes::new(b"device credentials".to_vec()))
        .unwrap();
    assert!(fake.person_restore_codes().unwrap().is_empty());
    for name in [
        STORE_KEYS_ENTRY,
        MEMBER_KEYS_ENTRY,
        CREDENTIALS_ENTRY,
        HOST_SECRET_NAMES_ENTRY,
        &host_secret_entry("api-token"),
    ] {
        assert!(fake
            .read(EntryScope::DeviceOnly, &a.account(name))
            .unwrap()
            .is_some());
        assert!(fake
            .read(EntryScope::Person, &a.account(name))
            .unwrap()
            .is_none());
    }
    assert!(a_person.restore_code().unwrap().is_none());
    a_person.delete_restore_code().unwrap();
    // Include non-UTF-8 bytes: custody keeps the complete opaque code unchanged.
    let first = SecretBytes::new(vec![0, 0xff, 42]);
    a_person.set_restore_code(&first).unwrap();
    a_person
        .set_s3_credentials(&SecretBytes::new(b"separate S3 key".to_vec()))
        .unwrap();
    assert!(fake
        .read(EntryScope::DeviceOnly, &a.account(S3_CREDENTIALS_ENTRY))
        .unwrap()
        .is_none());
    assert_eq!(
        a_person.s3_credentials().unwrap().unwrap().as_bytes(),
        b"separate S3 key"
    );
    assert!(b_person.s3_credentials().unwrap().is_none());
    assert!(fake
        .read(EntryScope::DeviceOnly, &a.account(RESTORE_CODE_ENTRY))
        .unwrap()
        .is_none());
    assert_eq!(
        a_person.restore_code().unwrap().unwrap().as_bytes(),
        first.as_bytes()
    );
    assert!(b_person.restore_code().unwrap().is_none());
    b_person
        .set_restore_code(&SecretBytes::new(b"code-b".to_vec()))
        .unwrap();
    a_person
        .set_restore_code(&SecretBytes::new(b"replacement".to_vec()))
        .unwrap();
    let codes = fake.person_restore_codes().unwrap();
    assert_eq!(
        codes.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![a_id, b_id]
    );
    assert!(
        matches!(&codes[0].1, PersonRestoreCode::Saved(bytes) if bytes.as_bytes() == b"replacement")
    );
    assert!(
        matches!(&codes[1].1, PersonRestoreCode::Saved(bytes) if bytes.as_bytes() == b"code-b")
    );
    a_person.delete_restore_code().unwrap();
    a_person.delete_restore_code().unwrap();
    assert!(a_person.restore_code().unwrap().is_none());
    assert_eq!(fake.person_restore_codes().unwrap()[0].0, b_id);
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
        member_custody.unlock().unwrap().unwrap().member(),
        member.member()
    );
    assert_eq!(
        a.host_secret("api-token").unwrap().as_deref(),
        Some("local secret")
    );
    a_person.set_restore_code(&first).unwrap();
    store_custody.forget().unwrap();
    member_custody.forget().unwrap();
    a.delete_host_secret("api-token").unwrap();
    assert_eq!(
        a_person.restore_code().unwrap().unwrap().as_bytes(),
        first.as_bytes()
    );
}

#[test]
fn discovery_filters_scope_and_kind_but_rejects_ids_coven_never_writes() {
    let fake = Keychain::in_memory("discovery").unwrap();
    fake.write(
        EntryScope::DeviceOnly,
        "restore-code:invalid",
        b"device only",
    )
    .unwrap();
    fake.write(EntryScope::Person, "host-secret:invalid", b"another entry")
        .unwrap();
    assert!(fake.person_restore_codes().unwrap().is_empty());
    fake.write(EntryScope::Person, "restore-code:invalid", b"bad id")
        .unwrap();
    assert!(matches!(
        fake.person_restore_codes(),
        Err(KeyError::RestoreCodeStoreId)
    ));
    fake.delete(EntryScope::Person, "restore-code:invalid")
        .unwrap();
    // Another spelling of a valid id is an account no store id reads or
    // deletes, so it can't be listed under one.
    let id = StoreId(Uuid::from_u128(0xabcdef));
    for account in [
        format!("restore-code:{}", id.0.simple()),
        format!("restore-code:{}", id.0.braced()),
        format!("restore-code:{}", id.to_string().to_uppercase()),
    ] {
        fake.write(EntryScope::Person, &account, b"other spelling")
            .unwrap();
        assert!(matches!(
            fake.person_restore_codes(),
            Err(KeyError::RestoreCodeStoreId)
        ));
        fake.delete(EntryScope::Person, &account).unwrap();
    }
    PersonEntries::new(fake.clone(), id)
        .set_restore_code(&SecretBytes::new(b"code".to_vec()))
        .unwrap();
    let codes = fake.person_restore_codes().unwrap();
    assert!(matches!(&codes[..], [(store, PersonRestoreCode::Saved(_))] if *store == id));
}

#[test]
fn a_store_found_in_more_than_one_keychain_group_is_ambiguous() {
    let (a, b, c) = (
        StoreId(Uuid::from_u128(1)),
        StoreId(Uuid::from_u128(2)),
        StoreId(Uuid::from_u128(3)),
    );
    let code = |bytes: &[u8]| SecretBytes::new(bytes.to_vec());
    let codes = by_store(vec![
        (c, code(b"c")),
        (b, code(b"b in one group")),
        (a, code(b"a")),
        (b, code(b"b in another")),
        (b, code(b"b in a third")),
    ]);
    assert_eq!(
        codes.iter().map(|(store, _)| *store).collect::<Vec<_>>(),
        [a, b, c]
    );
    assert!(matches!(&codes[0].1, PersonRestoreCode::Saved(bytes) if bytes.as_bytes() == b"a"));
    assert!(matches!(codes[1].1, PersonRestoreCode::Ambiguous));
    assert!(matches!(&codes[2].1, PersonRestoreCode::Saved(bytes) if bytes.as_bytes() == b"c"));
    assert_eq!(format!("{:?}", codes[0].1), "Saved([REDACTED])");
}

#[test]
fn host_names_cannot_collide_with_coven_or_another_store() {
    let fake = Keychain::in_memory("names").unwrap();
    let a = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let b = StoreKeychain::new(fake, StoreId(Uuid::from_u128(2)));
    a.set_device_id(DeviceId(42)).unwrap();
    let names = [
        "",
        "A",
        "a",
        "a:b",
        "a\0b",
        "密钥🔑",
        "é",
        "e\u{301}",
        "host-secret-61",
        STORE_KEYS_ENTRY,
        MEMBER_KEYS_ENTRY,
        RESTORE_CODE_ENTRY,
        S3_CREDENTIALS_ENTRY,
        DEVICE_ID_ENTRY,
        CREDENTIALS_ENTRY,
        HOST_SECRET_NAMES_ENTRY,
    ];
    for name in names {
        a.set_host_secret(name, &format!("秘密\0{name}")).unwrap();
        // Native account names contain only lowercase ASCII, digits and '-'.
        assert!(host_secret_entry(name)
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }
    assert_eq!(
        recorded_names(&a),
        names.into_iter().map(str::to_owned).collect()
    );
    for name in names {
        assert_eq!(
            a.host_secret(name).unwrap().as_deref(),
            Some(format!("秘密\0{name}").as_str())
        );
        assert_eq!(b.host_secret(name).unwrap(), None);
        a.delete_host_secret(name).unwrap();
        assert_eq!(a.host_secret(name).unwrap(), None);
    }
    assert_eq!(a.device_id().unwrap(), Some(DeviceId(42)));
    assert_eq!(a.host_secret("api-token").unwrap(), None);
    a.set_host_secret("api-token", "old-token").unwrap();
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
        KeyringCustody::<MemberIdentity>::new(store.clone()).unlock(),
        Err(KeyError::Material(_))
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
    for native in [
        keyring_core::Error::BadEncoding(sentinel.to_vec()),
        keyring_core::Error::BadDataFormat(sentinel.to_vec(), "invalid material".into()),
    ] {
        let expected_kind = std::mem::discriminant(&native);
        let error = KeyError::from(KeychainError::from(native));
        assert!(!format!("{error:?}").contains(&format!("{sentinel:?}")));
        let KeyError::Keychain(cause) = error else {
            panic!("keychain error")
        };
        let source = std::error::Error::source(&cause).unwrap();
        assert!(!format!("{source:?}").contains("115, 101, 99"));
        let source = source.downcast_ref::<keyring_core::Error>().unwrap();
        assert_eq!(std::mem::discriminant(source), expected_kind);
        match source {
            keyring_core::Error::BadEncoding(bytes)
            | keyring_core::Error::BadDataFormat(bytes, _) => assert!(bytes.is_empty()),
            other => panic!("changed native error kind: {other:?}"),
        }
    }
}

#[test]
fn deletion_discovers_only_this_stores_entries_from_custody() {
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
            .set_host_secret("another-token", "another secret")
            .unwrap();
        store
            .set_invite_code(&SecretText::new("CVI1-CODE".into()))
            .unwrap();
        let person = PersonEntries::new(fake.clone(), store.store);
        person
            .set_restore_code(&SecretBytes::new(b"restore".to_vec()))
            .unwrap();
        person
            .set_s3_credentials(&SecretBytes::new(b"S3 key".to_vec()))
            .unwrap();
    }
    // A new capability must discover everything from custody alone.
    let first = StoreKeychain::new(fake.clone(), first.store);
    first.delete_device_entries().unwrap();
    first.delete_device_entries().unwrap();
    for name in [
        DEVICE_ID_ENTRY,
        STORE_KEYS_ENTRY,
        MEMBER_KEYS_ENTRY,
        CREDENTIALS_ENTRY,
        HOST_SECRET_NAMES_ENTRY,
        INVITE_CODE_ENTRY,
        &host_secret_entry("token"),
        &host_secret_entry("another-token"),
    ] {
        assert!(first.read(name).unwrap().is_none());
        assert!(second.read(name).unwrap().is_some());
    }
    // The person's entries serve their other devices, for either store.
    for store in [&first, &second] {
        let person = PersonEntries::new(fake.clone(), store.store);
        assert!(person.restore_code().unwrap().is_some());
        assert!(person.s3_credentials().unwrap().is_some());
    }
    assert_eq!(second.device_id().unwrap(), Some(DeviceId(42)));
    second.write(DEVICE_ID_ENTRY, &[1]).unwrap();
    assert!(matches!(
        second.device_id(),
        Err(KeyError::DeviceIdEncoding)
    ));
}

#[test]
fn each_host_value_keeps_the_native_entry_size_limit() {
    let fake = Keychain::in_memory("entry-size").unwrap();
    let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let values = ["a".repeat(2560), "b".repeat(2560)];
    for (name, value) in ["first", "second"].into_iter().zip(&values) {
        store.set_host_secret(name, value).unwrap();
        assert_eq!(store.host_secret(name).unwrap().as_ref(), Some(value));
    }
    let memory = fake.memory_backend();
    let memory = memory.lock().unwrap();
    for bytes in memory.device.values() {
        assert!(
            bytes.as_bytes().len() <= 2560,
            "native entry exceeds 2,560 bytes"
        );
    }
    assert_eq!(memory.device.len(), 3);
    for value in &values {
        assert_eq!(
            memory
                .device
                .values()
                .filter(|bytes| bytes.as_bytes() == value.as_bytes())
                .count(),
            1
        );
    }
}

#[test]
fn host_secret_failures_keep_the_name_list_a_superset_and_allow_retry() {
    for keep_other in [false, true] {
        for original in [None, Some("original")] {
            for value in [Some("new"), None] {
                // Read names, first mutation, second mutation.
                for after in 0..3 {
                    let fake = Keychain::in_memory("host-failures").unwrap();
                    let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
                    if keep_other {
                        store.set_host_secret("other", "preserved").unwrap();
                    }
                    if let Some(original) = original {
                        store.set_host_secret("token", original).unwrap();
                    }
                    fail_operation(&fake, after);
                    let result = match value {
                        Some(value) => store.set_host_secret("token", value),
                        None => store.delete_host_secret("token"),
                    };
                    assert!(matches!(
                        result,
                        Err(KeyError::KeychainTemporarilyUnavailable(_))
                    ));
                    assert_host_names_cover_values(&store, &["token", "other"]);
                    let expected = if value.is_none() && after == 2 {
                        None
                    } else {
                        original
                    };
                    assert_eq!(store.host_secret("token").unwrap().as_deref(), expected);
                    match value {
                        Some(value) => store.set_host_secret("token", value).unwrap(),
                        None => store.delete_host_secret("token").unwrap(),
                    }
                    assert_eq!(store.host_secret("token").unwrap().as_deref(), value);
                    let mut expected_names = BTreeSet::new();
                    if value.is_some() {
                        expected_names.insert("token");
                    }
                    if keep_other {
                        expected_names.insert("other");
                        assert_eq!(
                            store.host_secret("other").unwrap().as_deref(),
                            Some("preserved")
                        );
                    }
                    assert_eq!(
                        recorded_names(&store),
                        expected_names.iter().copied().map(str::to_owned).collect()
                    );
                    if expected_names.is_empty() {
                        assert!(store.read(HOST_SECRET_NAMES_ENTRY).unwrap().is_none());
                    }
                }
            }
        }
    }
}

#[test]
fn crash_between_host_updates_leaves_only_a_harmless_extra_name() {
    for deleting in [false, true] {
        let fake = Keychain::in_memory("interrupted-host-update").unwrap();
        let id = StoreId(Uuid::from_u128(1));
        let store = StoreKeychain::new(fake.clone(), id);
        store.set_host_secret("other", "preserved").unwrap();
        if deleting {
            store.set_host_secret("token", "secret").unwrap();
        }
        // Model a crash before the second mutation commits: fail that native
        // call, then discard the capability and retain only durable entries.
        fail_operation(&fake, 2);
        let result = if deleting {
            store.delete_host_secret("token")
        } else {
            store.set_host_secret("token", "secret")
        };
        assert!(matches!(
            result,
            Err(KeyError::KeychainTemporarilyUnavailable(_))
        ));
        drop(store);
        let entries = std::mem::take(&mut fake.memory_backend().lock().unwrap().device);
        drop(fake);
        let reopened = Keychain::in_memory("interrupted-host-update").unwrap();
        let memory = reopened.memory_backend();
        memory.lock().unwrap().device = entries;
        let store = StoreKeychain::new(reopened.clone(), id);
        assert_eq!(
            recorded_names(&store),
            BTreeSet::from(["other".into(), "token".into()])
        );
        assert_eq!(store.host_secret("token").unwrap(), None);
        assert_eq!(
            store.host_secret("other").unwrap().as_deref(),
            Some("preserved")
        );
        store.delete_device_entries().unwrap();
        assert!(memory.lock().unwrap().device.is_empty());
    }
}

fn fail_operation(fake: &Keychain, after: usize) {
    let memory = fake.memory_backend();
    memory.lock().unwrap().fail_after = [after].into();
}

fn recorded_names(store: &StoreKeychain) -> BTreeSet<String> {
    match store.read(HOST_SECRET_NAMES_ENTRY).unwrap() {
        Some(bytes) => decode_host_secret_names(bytes.as_bytes())
            .unwrap()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        None => BTreeSet::new(),
    }
}

fn assert_host_names_cover_values(store: &StoreKeychain, names: &[&str]) {
    let recorded = recorded_names(store);
    for name in names {
        if store.host_secret(name).unwrap().is_some() {
            assert!(recorded.contains(*name));
        }
    }
}

#[test]
fn damaged_name_lists_refuse_mutations_without_losing_secrets() {
    let fake = Keychain::in_memory("damaged-host-names").unwrap();
    let store = StoreKeychain::new(fake, StoreId(Uuid::from_u128(1)));
    store.set_host_secret("token", "secret").unwrap();
    store.set_device_id(DeviceId(42)).unwrap();
    let valid = store.read(HOST_SECRET_NAMES_ENTRY).unwrap().unwrap();
    let mut duplicate = valid.as_bytes().to_vec();
    duplicate.extend_from_slice(&valid.as_bytes()[HOST_SECRET_NAMES_PREFIX.len()..]);
    let mut invalid_utf8 = valid.as_bytes().to_vec();
    *invalid_utf8.last_mut().unwrap() = 0xff;
    let mut huge_length = HOST_SECRET_NAMES_PREFIX.to_vec();
    huge_length.extend_from_slice(&u64::MAX.to_le_bytes());
    for damaged in [
        Vec::new(),
        b"unknown version".to_vec(),
        valid.as_bytes()[..valid.as_bytes().len() - 1].to_vec(),
        duplicate,
        invalid_utf8,
        huge_length,
    ] {
        store.write(HOST_SECRET_NAMES_ENTRY, &damaged).unwrap();
        for result in [
            store.set_host_secret("new", "value"),
            store.delete_host_secret("token"),
            store.delete_device_entries(),
        ] {
            assert!(matches!(
                result,
                Err(KeyError::Material(MaterialError::Encoding))
            ));
        }
        assert_eq!(
            store
                .read(HOST_SECRET_NAMES_ENTRY)
                .unwrap()
                .unwrap()
                .as_bytes(),
            damaged
        );
        assert_eq!(
            store.host_secret("token").unwrap().as_deref(),
            Some("secret")
        );
        assert_eq!(store.host_secret("new").unwrap(), None);
        assert_eq!(store.device_id().unwrap(), Some(DeviceId(42)));
    }
}

#[test]
fn malformed_host_values_do_not_prevent_deletion() {
    let fake = Keychain::in_memory("damaged-host-values").unwrap();
    let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    store.set_host_secret("token", "secret").unwrap();
    store.set_host_secret("other", "preserved").unwrap();
    store.write(&host_secret_entry("token"), &[0xff]).unwrap();
    assert!(matches!(
        store.host_secret("token"),
        Err(KeyError::Material(MaterialError::Encoding))
    ));
    assert_eq!(
        store.host_secret("other").unwrap().as_deref(),
        Some("preserved")
    );
    store.delete_device_entries().unwrap();
    let memory = fake.memory_backend();
    assert!(memory.lock().unwrap().device.is_empty());
}

#[test]
fn concurrent_host_secret_updates_keep_every_name() {
    let fake = Keychain::in_memory("concurrent-host-secrets").unwrap();
    let id = StoreId(Uuid::from_u128(1));
    let start = std::sync::Barrier::new(16);
    std::thread::scope(|scope| {
        for index in 0..16 {
            let store = StoreKeychain::new(fake.clone(), id);
            let start = &start;
            scope.spawn(move || {
                start.wait();
                store.set_host_secret(&index.to_string(), "value").unwrap();
            });
        }
    });
    let store = StoreKeychain::new(fake.clone(), id);
    assert_eq!(
        recorded_names(&store),
        (0..16).map(|index| index.to_string()).collect()
    );
    for index in 0..16 {
        assert_eq!(
            store.host_secret(&index.to_string()).unwrap().as_deref(),
            Some("value")
        );
    }
    store.delete_device_entries().unwrap();
    let memory = fake.memory_backend();
    assert!(memory.lock().unwrap().device.is_empty());
}

#[test]
fn deletion_can_retry_after_each_keychain_failure() {
    // Read names, remove two values, remove the list, then four coven entries.
    for after in 0..8 {
        let fake = Keychain::in_memory("delete-failures").unwrap();
        let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
        store.set_host_secret("token", "secret").unwrap();
        store.set_host_secret("other", "preserved").unwrap();
        for name in [
            STORE_KEYS_ENTRY,
            MEMBER_KEYS_ENTRY,
            DEVICE_ID_ENTRY,
            CREDENTIALS_ENTRY,
        ] {
            store.write(name, b"entry").unwrap();
        }
        fail_operation(&fake, after);
        assert!(matches!(
            store.delete_device_entries(),
            Err(KeyError::KeychainTemporarilyUnavailable(_))
        ));
        assert_host_names_cover_values(&store, &["token", "other"]);
        if after <= 3 {
            assert_eq!(
                recorded_names(&store),
                BTreeSet::from(["other".into(), "token".into()])
            );
            assert!(store.read(STORE_KEYS_ENTRY).unwrap().is_some());
        }
        store.delete_device_entries().unwrap();
        let memory = fake.memory_backend();
        assert!(memory.lock().unwrap().device.is_empty());
    }
}

/// A join's invite code stays on this device: the person's other devices
/// never see it, and it reads back exactly until the join deletes it (§13.2).
#[test]
fn an_invite_code_is_kept_on_this_device_only_until_deleted() {
    let fake = Keychain::in_memory("invite-code").unwrap();
    let store = StoreKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    assert!(store.invite_code().unwrap().is_none());
    store
        .set_invite_code(&SecretText::new("CVI1-CODE".into()))
        .unwrap();
    assert_eq!(store.invite_code().unwrap().unwrap().as_str(), "CVI1-CODE");
    assert!(fake
        .read(EntryScope::Person, &store.account(INVITE_CODE_ENTRY))
        .unwrap()
        .is_none());
    store.write(INVITE_CODE_ENTRY, &[0xff]).unwrap();
    assert!(matches!(
        store.invite_code(),
        Err(KeyError::Material(MaterialError::Encoding))
    ));
    store.delete_invite_code().unwrap();
    store.delete_invite_code().unwrap();
    assert!(store.invite_code().unwrap().is_none());
}

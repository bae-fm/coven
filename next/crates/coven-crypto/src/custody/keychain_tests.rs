use super::*;
use crate::custody::{MemberKeyCustody, StoreKeyCustody};
use crate::{MemberKeys, StoreKeyring};
use uuid::Uuid;

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

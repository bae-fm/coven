//! Process-global keyring registration contract. One ordered test because the
//! keyring store and service name are process-wide: parallel tests would race
//! the registry (and an unset store mid-test would install the real OS
//! keychain on a developer machine).

#[test]
fn keyring_registration_contract() {
    // Register the service against the mock so `set_keyring_service` keeps it
    // rather than installing the OS keychain.
    keyring_core::set_default_store(
        keyring_core::mock::Store::new().expect("create mock keyring store"),
    );
    // A store can bind without an OS keyring when its configured custody does
    // not use one. Any keyring operation still names the missing startup call.
    let unregistered = coven::StoreKeys::bind("keyring-registration-test".to_string());
    let err = unregistered
        .get_encryption_key()
        .expect_err("the bound capability has no registered keyring");
    assert!(
        matches!(err, coven::KeyError::ServiceNotRegistered),
        "expected ServiceNotRegistered, got {err:?}"
    );
    assert!(
        err.to_string().contains("set_keyring_service"),
        "error names the missing startup call: {err}"
    );

    coven::set_keyring_service("coven-store-test").expect("register keyring service");

    assert!(matches!(
        unregistered.get_encryption_key(),
        Err(coven::KeyError::ServiceNotRegistered)
    ));

    // Re-registration: same name is a no-op, a different name is a startup
    // contradiction.
    coven::set_keyring_service("coven-store-test").expect("same-name re-registration is a no-op");
    let err = coven::set_keyring_service("other-service")
        .expect_err("a different service name is a startup contradiction");
    assert!(
        err.to_string().contains("already registered"),
        "error names the conflict: {err}"
    );

    let keys = coven::StoreKeys::bind("keyring-registration-test".to_string());

    // The opened capability retains the exact store registered with it. A
    // later mutation of keyring-core's process default cannot silently route
    // this store's key operations through another backend.
    keyring_core::unset_default_store();
    let value = keys
        .get_encryption_key()
        .expect("read through the retained keyring store");
    assert_eq!(value, None);
}

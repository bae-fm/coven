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
    // Before the service is registered, a key operation names the missing
    // startup call rather than panicking.
    let err = coven::DeviceKeys::get_user_public_key().expect_err("service not registered yet");
    assert!(
        matches!(err, coven::KeyError::ServiceNotRegistered),
        "expected ServiceNotRegistered, got {err:?}"
    );
    assert!(
        err.to_string().contains("set_keyring_service"),
        "error names the missing startup call: {err}"
    );

    coven::set_keyring_service("coven-store-test").expect("register keyring service");

    // Re-registration: same name is a no-op, a different name is a startup
    // contradiction.
    coven::set_keyring_service("coven-store-test").expect("same-name re-registration is a no-op");
    let err = coven::set_keyring_service("other-service")
        .expect_err("a different service name is a startup contradiction");
    assert!(
        err.to_string().contains("already registered"),
        "error names the conflict: {err}"
    );

    // A key operation against a registry with no default store must name the
    // missing startup step, not surface a generic persistence error.
    keyring_core::unset_default_store();
    let err = coven::DeviceKeys::get_user_public_key().expect_err("no store installed");
    assert!(
        matches!(err, coven::KeyError::StoreNotInstalled),
        "expected StoreNotInstalled, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("set_keyring_service"),
        "error names the fix: {message}"
    );
    assert!(
        !message.contains("persistence") && !message.contains("Persistence"),
        "not the generic persistence error: {message}"
    );
}

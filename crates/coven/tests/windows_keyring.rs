//! Real Credential Manager round-trip through the bundled Windows keyring
//! store. An integration test binary is its own process, so it can register
//! the real store without colliding with the mock other tests install
//! (`keyring_backend::install_bundled_store` on other targets does the same
//! thing against the OS keychain; this file is the Windows proof).
#![cfg(target_os = "windows")]

use coven::StoreKeys;

/// Deletes the store's encryption-key entry on drop, so a failed assertion
/// still strands nothing on the runner.
struct CleanupGuard(StoreKeys);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Err(e) = self.0.delete_encryption_key() {
            tracing::warn!("failed to delete test keyring entry during cleanup: {e}");
        }
    }
}

#[test]
fn windows_credential_manager_round_trip() {
    coven::set_keyring_service("coven-ci-windows-keyring").expect("register keyring service");

    let store_id = format!("windows-keyring-test-{}", std::process::id());
    let keys = CleanupGuard(StoreKeys::bind(store_id));

    assert_eq!(
        keys.0.get_encryption_key().expect("read before write"),
        None,
        "no entry written yet"
    );

    let value = "aa".repeat(32); // 64 hex chars, well under the 2560-byte blob cap
    keys.0
        .set_encryption_key(&value)
        .expect("write encryption key");
    assert_eq!(
        keys.0.get_encryption_key().expect("read after write"),
        Some(value),
        "round-tripped value matches what was written"
    );

    keys.0
        .delete_encryption_key()
        .expect("delete encryption key");
    assert_eq!(
        keys.0.get_encryption_key().expect("read after delete"),
        None,
        "entry is gone after delete"
    );

    // Over Credential Manager's CRED_MAX_CREDENTIAL_BLOB_SIZE cap: the store
    // must surface this as an error, not truncate or drop it silently.
    let too_long = "a".repeat(3000);
    keys.0
        .set_encryption_key(&too_long)
        .expect_err("value over the blob cap is rejected");
}

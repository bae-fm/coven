//! Pins the Apple device-only entry-construction path against the real
//! protected-data store — not the mock every other test in this crate
//! installs. An integration test binary is its own process, so it can
//! install the real store without colliding with the mock (`keyring_backend
//! ::install_bundled_store` on other targets does the same thing against the
//! OS keychain; `windows_keyring.rs` is this file's Windows counterpart).
//!
//! What this file cannot do: perform an actual Keychain read or write.
//! Creating or reading a protected-data item is a syscall gated by a
//! code-signing entitlement (`keychain-access-groups`) that only a real
//! provisioning profile grants; a plain `cargo test` binary has none (Apple
//! platform limitation — `apple-native-keyring-store` itself documents that
//! its protected store "must be code-signed with a provisioning profile" and
//! ships its own protected-store tests as a static library linked into a
//! signed iOS test harness, not as a `cargo test` binary). So this pins
//! *construction*: which store the registered keyring service retains, and which
//! `AccessPolicy` it builds the entry with — that decision is made entirely
//! in memory, before any Keychain syscall, so it needs no entitlement. The
//! resulting item's actual on-disk accessibility class is verified
//! out-of-band, on a signed build.
#![cfg(target_os = "macos")]

use apple_native_keyring_store::protected::{AccessPolicy, Store};

#[test]
fn registered_service_builds_protected_device_only_entries() {
    coven::set_keyring_service("coven-apple-keyring-test").expect("register keyring service");

    let default_store =
        keyring_core::get_default_store().expect("set_keyring_service installs a store");
    assert!(
        default_store.as_any().downcast_ref::<Store>().is_some(),
        "a fresh process with no store pre-installed gets the real protected store",
    );

    let facts = coven::apple_keyring_entry_facts_for_test("coven-apple-keyring-test-account")
        .expect("the registered service builds an entry with no keychain I/O");

    assert_eq!(
        facts.access_policy,
        AccessPolicy::WhenUnlockedThisDeviceOnly,
        "the registered service must select the device-only policy directly (Cred::build), \
         never the modifier-string path apple-native-keyring-store maps to \
         the non-device-only AccessPolicy::WhenUnlocked",
    );
    assert!(
        !facts.cloud_synchronize,
        "coven never opts a keyring item into iCloud sync",
    );
    assert_eq!(facts.service, "coven-apple-keyring-test");
    assert_eq!(facts.account, "coven-apple-keyring-test-account");
}

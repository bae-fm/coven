use super::*;

/// Proves `map_keyring_error` — the real chokepoint every keyring
/// read/write/delete funnels through — recognizes `errSecMissingEntitlement`
/// (OSStatus -34018) when it arrives the way the real protected store
/// produces it: a `keyring_core::Error::PlatformFailure` boxing a
/// `security_framework::base::Error`. This does not exercise the real
/// Keychain — `cargo test` cannot reach it (see the Apple section of
/// `site/docs/keys.md`) — it constructs the exact error shape
/// `apple-native-keyring-store`'s `protected::decode_error` is documented
/// (and read, see `is_missing_keychain_entitlement`'s doc comment) to
/// produce for this OSStatus, and checks the mapping honestly, at the seam
/// coven controls.
#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn missing_entitlement_os_status_maps_to_a_typed_actionable_error() {
    let raw = keyring_core::Error::PlatformFailure(Box::new(
        security_framework::base::Error::from_code(-34018),
    ));

    let mapped = map_keyring_error(raw);

    assert!(
        matches!(mapped, KeyError::MissingKeychainEntitlement),
        "got {mapped:?}"
    );
    let message = mapped.to_string();
    assert!(message.contains("-34018"), "{message}");
    assert!(message.contains("errSecMissingEntitlement"), "{message}");
    assert!(message.contains("keychain-access-groups"), "{message}");
    assert!(message.contains("provisioning profile"), "{message}");
    assert!(message.contains("DEVELOPMENT_TEAM"), "{message}");
    assert!(
        !message.contains("must be signed"),
        "a bare 'signed binary' is not the fix and must not be implied: {message}"
    );
}

/// The match is scoped to exactly -34018, not "any `PlatformFailure`" —
/// another OSStatus wrapped the same way must still fall through to the
/// source-preserving keyring error rather than being
/// mis-reported as a missing entitlement.
#[cfg(any(target_os = "macos", target_os = "ios"))]
#[test]
fn a_different_platform_failure_os_status_is_not_reported_as_missing_entitlement() {
    let raw = keyring_core::Error::PlatformFailure(Box::new(
        security_framework::base::Error::from_code(-25291), // errSecNotAvailable
    ));

    let mapped = map_keyring_error(raw);

    assert!(matches!(mapped, KeyError::Keyring(_)), "got {mapped:?}");
}

#[test]
fn empty_keyring_entry_is_an_error_not_absence() {
    test_keyring::install();
    let slot = KeyringSlot::EncryptionMasterKey("empty-keyring-entry-store".to_string());
    let account = slot.account();
    registered_keyring()
        .expect("registered test keyring")
        .entry(&account)
        .expect("create keyring entry")
        .set_password("")
        .expect("write empty keyring entry");

    let error = registered_keyring()
        .expect("registered test keyring")
        .read(&slot)
        .expect_err("empty entry is corrupt");

    assert!(error.to_string().contains("present but empty"));
    assert!(error.to_string().contains(&account));
}

/// The keyring account names are a durable storage contract: a device's
/// already-stored keys are found only at these exact accounts, so
/// `StoreKeys` and every identity operation must keep using them
/// verbatim. Pin all five. `HostSecret`'s rendering (`{name}:{store_id}`)
/// is additionally a contract with any host that already stores a secret
/// at that account by name — a host's own already-stored secrets are
/// found only at the exact account its name renders to, so this must
/// stay byte-identical.
#[test]
fn keyring_account_names_are_a_stable_storage_contract() {
    assert_eq!(
        KeyringSlot::EncryptionMasterKey("store-42".to_string()).account(),
        "encryption_master_key:store-42"
    );
    assert_eq!(
        KeyringSlot::CloudHomeCredentials("store-42".to_string()).account(),
        "cloud_home_credentials:store-42"
    );
    assert_eq!(
        KeyringSlot::DeviceSigningKey("store-42".to_string()).account(),
        "coven_user_signing_key:store-42"
    );
    assert_eq!(
        KeyringSlot::PendingIdentity("deadbeef".to_string()).account(),
        "coven_pending_identity:deadbeef"
    );
    assert_eq!(
        KeyringSlot::HostSecret {
            name: "discogs_api_key".to_string(),
            store_id: "s1".to_string(),
        }
        .account(),
        "discogs_api_key:s1"
    );
}

// =========================================================================
// Host secrets
// =========================================================================

#[test]
fn host_secret_round_trips_and_absent_reads_none() {
    test_keyring::install();
    let keys = StoreKeys::bind("host-secret-round-trip".to_string());

    assert_eq!(
        keys.get_host_secret("discogs_api_key").expect("get"),
        None,
        "an unset host secret reads as absent",
    );

    keys.set_host_secret("discogs_api_key", "the-discogs-key")
        .expect("set");
    assert_eq!(
        keys.get_host_secret("discogs_api_key").expect("get"),
        Some("the-discogs-key".to_string()),
    );

    keys.delete_host_secret("discogs_api_key").expect("delete");
    assert_eq!(
        keys.get_host_secret("discogs_api_key")
            .expect("get after delete"),
        None,
    );
}

/// Every name coven itself reserves is refused, typed. Enumerated from
/// [`RESERVED_HOST_SECRET_NAMES`] rather than hand-listed, so this test
/// cannot drift from the validator it exercises.
#[test]
fn host_secret_refuses_every_reserved_name() {
    test_keyring::install();
    let keys = StoreKeys::bind("host-secret-reserved-names".to_string());

    for reserved in RESERVED_HOST_SECRET_NAMES {
        let error = keys.set_host_secret(reserved, "value").expect_err(&format!(
            "{reserved:?} must be refused as a host secret name"
        ));
        assert!(
            matches!(error, KeyError::InvalidSecretName { .. }),
            "got {error:?}",
        );
    }
}

#[test]
fn host_secret_refuses_a_name_containing_colon() {
    test_keyring::install();
    let keys = StoreKeys::bind("host-secret-colon-name".to_string());

    let error = keys
        .set_host_secret("discogs:api_key", "value")
        .expect_err("a name containing ':' must be refused");
    assert!(
        matches!(error, KeyError::InvalidSecretName { .. }),
        "{error:?}"
    );
}

#[test]
fn host_secret_refuses_an_empty_name() {
    test_keyring::install();
    let keys = StoreKeys::bind("host-secret-empty-name".to_string());

    let error = keys
        .set_host_secret("", "value")
        .expect_err("an empty name must be refused");
    assert!(
        matches!(error, KeyError::InvalidSecretName { .. }),
        "{error:?}"
    );
}

/// A host secret entry present but empty reads as corrupt, not absent —
/// the same discipline [`empty_keyring_entry_is_an_error_not_absence`]
/// pins for coven's own slots applies here too.
#[test]
fn host_secret_present_but_empty_is_an_error_not_absence() {
    test_keyring::install();
    let slot = KeyringSlot::HostSecret {
        name: "discogs_api_key".to_string(),
        store_id: "host-secret-empty-entry-store".to_string(),
    };
    let account = slot.account();
    registered_keyring()
        .expect("registered test keyring")
        .entry(&account)
        .expect("create keyring entry")
        .set_password("")
        .expect("write empty keyring entry");

    let keys = StoreKeys::bind("host-secret-empty-entry-store".to_string());
    let error = keys
        .get_host_secret("discogs_api_key")
        .expect_err("empty entry is corrupt");
    assert!(error.to_string().contains("present but empty"));
}

/// Host secrets are store-scoped: two `StoreKeys` over different
/// `store_id`s see independent values for the same secret name.
#[test]
fn host_secret_is_scoped_to_its_store() {
    test_keyring::install();
    let store_a = StoreKeys::bind("host-secret-scope-a".to_string());
    let store_b = StoreKeys::bind("host-secret-scope-b".to_string());

    store_a
        .set_host_secret("discogs_api_key", "key-for-store-a")
        .expect("set on store a");

    assert_eq!(
        store_a.get_host_secret("discogs_api_key").expect("get"),
        Some("key-for-store-a".to_string()),
    );
    assert_eq!(
        store_b.get_host_secret("discogs_api_key").expect("get"),
        None,
        "store b must not see store a's secret",
    );
}

/// A keypair written straight to the raw keyring under a store's signing-key
/// account reads back through `require_identity` unchanged — the account
/// math both sides use is the same, so the split doesn't strand an
/// already-stored key.
#[test]
fn require_identity_reads_a_keypair_written_at_the_stores_account() {
    test_keyring::install();
    let store_id = "require-identity-fixed-account-test";

    let keypair = UserKeypair::generate();
    let expected_pubkey = keypair.public_key();
    // Write via the raw keyring under the store's signing-key account, the
    // way the identity custody preset does — no `require_identity` involved
    // on the write side.
    registered_keyring()
        .expect("registered test keyring")
        .write(
            &KeyringSlot::DeviceSigningKey(store_id.to_string()),
            &hex::encode(keypair.to_keypair_bytes()),
        )
        .expect("write signing key to the raw keyring");

    let custody = std::sync::Arc::new(StoreKeys::bind(store_id.to_string()));
    let read = require_identity(custody.as_ref()).expect("read the identity back");
    assert_eq!(
        read.public_key(),
        expected_pubkey,
        "require_identity must read the keypair stored at the store's account",
    );
}

/// `require_identity` maps absence to the typed `KeyError::NoDeviceIdentity`
/// — every connect/join precondition that requires an existing identity
/// gets a matchable, actionable error.
#[test]
fn require_identity_maps_absence_to_no_device_identity() {
    test_keyring::install();
    let custody = std::sync::Arc::new(StoreKeys::bind("require-identity-absent-test".to_string()));

    match require_identity(custody.as_ref()) {
        Err(error) => assert!(matches!(error, KeyError::NoDeviceIdentity), "got {error:?}"),
        Ok(_) => panic!("no identity is established"),
    }
}

/// A same-pubkey re-import (the retry path a host takes if the first
/// import attempt's caller-side bookkeeping failed after the keyring
/// write) is idempotent — no error, and the identity reads back
/// unchanged.
#[test]
fn establishing_the_same_identity_again_is_idempotent() {
    test_keyring::install();
    let custody = std::sync::Arc::new(StoreKeys::bind(
        "import-identity-idempotent-test".to_string(),
    ));

    let keypair = UserKeypair::generate();
    custody
        .establish(&keypair)
        .expect("first call establishes the identity");
    custody
        .establish(&keypair)
        .expect("establishing the same key is idempotent");

    assert_eq!(
        require_identity(custody.as_ref())
            .expect("identity still readable")
            .public_key(),
        keypair.public_key(),
    );
}

/// Importing a DIFFERENT key over an already-established identity is
/// refused — silently swapping this store's identity would strand its
/// already-signed membership entries.
#[test]
fn establishing_identity_refuses_to_overwrite_a_different_identity() {
    test_keyring::install();
    let custody = std::sync::Arc::new(StoreKeys::bind("import-identity-mismatch-test".to_string()));

    let established = UserKeypair::generate();
    custody
        .establish(&established)
        .expect("establish the first identity");

    let different = UserKeypair::generate();
    let error = custody
        .establish(&different)
        .expect_err("establishing a different identity must be refused");
    match error {
        KeyError::IdentityMismatch {
            existing_pubkey_hex,
            imported_pubkey_hex,
        } => {
            assert_eq!(existing_pubkey_hex, public_key_hex(&established));
            assert_eq!(imported_pubkey_hex, public_key_hex(&different));
        }
        other => panic!("expected IdentityMismatch, got {other:?}"),
    }

    // The refusal must not have overwritten the established identity.
    assert_eq!(
        require_identity(custody.as_ref())
            .expect("the original identity is untouched")
            .public_key(),
        established.public_key(),
    );
}

/// A pending identity minted for a join request establishes into a store's
/// identity custody via `DeviceIdentityCustody::establish` while the pending slot still
/// serves it — the split the join relies on: establish before the
/// completion marker, discard the slot only once the whole join succeeds.
/// Re-establishing from the still-present slot is idempotent (the torn-
/// bootstrap retry), and the discard afterward empties the slot.
#[test]
fn pending_identity_establishes_then_discards() {
    test_keyring::install();
    let pending = mint_pending_identity().expect("mint pending identity");
    let request_pubkey = public_key_hex(&pending);
    let custody = std::sync::Arc::new(StoreKeys::bind(
        "pending-identity-establish-test".to_string(),
    ));

    custody
        .establish(&pending)
        .expect("establish the pending identity in store custody");
    assert_eq!(
        require_identity(custody.as_ref())
            .expect("the store now has an identity")
            .public_key(),
        pending.public_key(),
    );

    // The slot still serves the identity: a retry after a torn bootstrap
    // (whose wipe removed the store custody) re-establishes from it.
    let still_pending =
        peek_pending_identity(&request_pubkey).expect("the pending slot is not yet consumed");
    custody
        .establish(&still_pending)
        .expect("re-establishing the same identity is idempotent");

    discard_pending_identity(&request_pubkey).expect("discard the consumed slot");
    let error = peek_pending_identity(&request_pubkey)
        .map(|_| ())
        .expect_err("the discarded slot no longer serves the identity");
    assert!(
        matches!(error, KeyError::NoPendingIdentity { .. }),
        "{error:?}"
    );
    assert_eq!(
        require_identity(custody.as_ref())
            .expect("the established identity outlives the slot")
            .public_key(),
        pending.public_key(),
    );
}

/// An abandoned join request's pending identity is removed and no longer
/// served; discarding is `Ok` even when nothing was pending.
#[test]
fn discard_pending_identity_removes_it_and_is_idempotent() {
    test_keyring::install();
    let pending = mint_pending_identity().expect("mint pending identity");
    let request_pubkey = public_key_hex(&pending);

    discard_pending_identity(&request_pubkey).expect("discard the pending identity");
    discard_pending_identity(&request_pubkey)
        .expect("discarding an already-absent pending identity is not an error");

    let error = peek_pending_identity(&request_pubkey)
        .map(|_| ())
        .expect_err("a discarded pending identity is no longer served");
    assert!(
        matches!(error, KeyError::NoPendingIdentity { .. }),
        "{error:?}"
    );
}

/// Two concurrent join requests mint distinct pending identities, keyed by
/// their own public keys, and establishing one never touches the other.
#[test]
fn two_concurrent_pending_joins_do_not_cross() {
    test_keyring::install();
    let pending_a = mint_pending_identity().expect("mint pending identity a");
    let pending_b = mint_pending_identity().expect("mint pending identity b");
    assert_ne!(pending_a.public_key(), pending_b.public_key());

    let custody_a =
        std::sync::Arc::new(StoreKeys::bind("two-concurrent-joins-store-a".to_string()));
    let custody_b =
        std::sync::Arc::new(StoreKeys::bind("two-concurrent-joins-store-b".to_string()));
    custody_a
        .establish(&pending_a)
        .expect("establish a into store a");

    assert!(
        require_identity(custody_b.as_ref()).is_err(),
        "store b must not see store a's established identity",
    );
    custody_b
        .establish(&pending_b)
        .expect("establish b into store b");
    assert_ne!(
        require_identity(custody_a.as_ref())
            .expect("store a's identity")
            .public_key(),
        require_identity(custody_b.as_ref())
            .expect("store b's identity")
            .public_key(),
    );
}

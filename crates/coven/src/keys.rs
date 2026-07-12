use std::sync::{Arc, OnceLock};

use tracing::info;

pub use coven_core::keys::{
    public_key_hex, CloudHomeCredentials, DeviceIdentityCustody, KeyError, MasterKeyCustody,
    UserKeypair, SIGN_PUBLICKEYBYTES, SIGN_SECRETKEYBYTES,
};

/// Why a [`CovenHandle`](crate::CovenHandle) master-key lifecycle call
/// (`initialize_master_key`, `import_master_key`) failed.
#[derive(Debug, thiserror::Error)]
pub enum MasterKeyError {
    /// `initialize_master_key` found a master key already established —
    /// custody `unlock()` returned `Some`. coven never generates over an
    /// existing key; the host imports or forgets it first.
    #[error("a master key is already established for this store")]
    AlreadyEstablished,
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("invalid master key material: {0}")]
    Encryption(#[from] crate::encryption::EncryptionError),
}

static KEYRING_SERVICE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Register the process-wide keyring: the service name every entry is stored
/// under, and the platform keyring store that backs it. Both are one-time
/// startup registration and must run before any key operation. The store is
/// installed before the name is recorded, so a failed installation leaves no
/// registration behind. Re-registering the same name is a no-op; a different
/// name is a startup contradiction and fails. Fails with
/// [`KeyError::UnsupportedKeyringPlatform`] on a target with no bundled store.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError> {
    crate::keyring_backend::install_platform_store()?;
    let name = name.into();
    if KEYRING_SERVICE.set(name.clone()).is_err() {
        let registered = KEYRING_SERVICE
            .get()
            .map(String::as_str)
            .expect("a keyring service is registered when set() fails");
        if registered != name {
            return Err(KeyError::Persistence(format!(
                "keyring service is already registered as {registered:?}; cannot re-register as {name:?}"
            )));
        }
    }
    Ok(())
}

/// The registered keyring service name. `Err` when the host never ran the
/// startup [`set_keyring_service`] call — surfaced so a mis-ordered host gets a
/// typed error, not a panic deep inside a key operation.
pub fn keyring_service() -> Result<&'static str, KeyError> {
    KEYRING_SERVICE
        .get()
        .map(String::as_str)
        .ok_or(KeyError::ServiceNotRegistered)
}

static IDENTITY_CUSTODY: OnceLock<Arc<dyn DeviceIdentityCustody>> = OnceLock::new();

/// Register the process-wide custody for the device's signing identity —
/// deliberately process-global, not per-store: the identity is one keypair
/// per host, shared by every store (see [`UserKeypair`]'s doc), and every
/// [`DeviceKeys`] call site is a free function far from any builder, so a
/// per-store custody would pretend a variation that must not exist.
///
/// One-time startup registration, like [`set_keyring_service`], and stricter:
/// unlike a keyring *service name* (where re-registering the same name is a
/// no-op), a custody policy can't be compared for equality, so any second
/// call is refused — including one that resolved implicitly, the first time
/// any key operation ran under the [`IdentityCustody::Keyring`] default. Call
/// this before any key operation if a non-default policy is wanted.
pub fn set_identity_custody(
    custody: crate::identity_custody::IdentityCustody,
) -> Result<(), KeyError> {
    IDENTITY_CUSTODY.set(custody.resolve()).map_err(|_| {
        KeyError::Persistence(
            "identity custody is already registered (either by an earlier set_identity_custody \
             call, or implicitly defaulted to Keyring by an earlier key operation); \
             set_identity_custody must run before any key operation"
                .to_string(),
        )
    })
}

/// The registered identity custody, defaulting to
/// [`IdentityCustody::Keyring`] — today's behavior, byte-for-byte — the
/// first time this runs with none registered. That first resolution is
/// permanent for the process (see [`set_identity_custody`]'s doc).
fn identity_custody() -> Arc<dyn DeviceIdentityCustody> {
    IDENTITY_CUSTODY
        .get_or_init(|| crate::identity_custody::IdentityCustody::Keyring.resolve())
        .clone()
}

/// Get-or-create this device's signing identity through the registered
/// [`IdentityCustody`], returning its public key. The explicit,
/// host-facing entry point for identity establishment — hosts call it at
/// their own setup moments (store create, first run). Idempotent and
/// concurrent-call-safe: two callers racing this in the same process
/// converge on one identity, never two.
///
/// Connect paths never mint on their own absence — see
/// [`KeyError::NoDeviceIdentity`] — so this (or a completed join/restore,
/// which also establish one deliberately) is the only way a fresh device
/// gets a signing identity.
pub fn ensure_device_identity() -> Result<[u8; SIGN_PUBLICKEYBYTES], KeyError> {
    DeviceKeys::get_or_create_user_keypair().map(|kp| kp.public_key())
}

fn map_keyring_error(e: keyring_core::Error) -> KeyError {
    match e {
        keyring_core::Error::NoDefaultStore => KeyError::StoreNotInstalled,
        other => KeyError::Persistence(other.to_string()),
    }
}

/// Which key a keyring entry holds, and the sole owner of the account name it
/// is stored under. The device signing key is device-global (no `store_id`); the
/// encryption master key and cloud-home credentials are per store. Every keyring
/// read/write/delete names its entry with one of these variants, so the on-disk
/// account strings live in exactly one place: [`KeyringSlot::account`].
pub(crate) enum KeyringSlot {
    /// The device-global Ed25519 signing key, shared by every store on the
    /// device and stored under one fixed account.
    DeviceSigningKey,
    /// A store's encryption master key.
    EncryptionMasterKey(String),
    /// A store's cloud-home credentials.
    CloudHomeCredentials(String),
}

impl KeyringSlot {
    /// The keyring account name this slot is stored under. These strings are a
    /// durable storage contract: a device's already-stored keys are found only
    /// at these exact accounts, so changing any of them strands stored keys.
    pub(crate) fn account(&self) -> String {
        match self {
            KeyringSlot::DeviceSigningKey => "coven_user_signing_key".to_string(),
            KeyringSlot::EncryptionMasterKey(store_id) => {
                format!("encryption_master_key:{store_id}")
            }
            KeyringSlot::CloudHomeCredentials(store_id) => {
                format!("cloud_home_credentials:{store_id}")
            }
        }
    }
}

/// The sole entry-construction point for the OS keyring: every read, write,
/// and delete builds its [`keyring_core::Entry`] here, so a target's
/// protection policy is applied in exactly one place.
///
/// On Apple targets, when the process installed the real protected-data
/// store (as opposed to a test's mock store, or any other store reached
/// through this same code path), entries are created device-only
/// (`AccessPolicy::WhenUnlockedThisDeviceOnly`): an encrypted local
/// (Finder/iTunes) backup restored onto a different device does not restore
/// this item, because the item is bound to this device's Secure Enclave.
/// Apple's documented modifier-string path for this policy
/// (`Entry::new_with_modifiers(service, account, {"access-policy":
/// "when-unlocked-this-device-only"})`) is silently accepted by
/// `apple-native-keyring-store`'s string parser but mapped to the
/// non-device-only `AccessPolicy::WhenUnlocked` instead — no error, just the
/// wrong protection class — so this calls the store's own `Cred::build`
/// directly, which takes the `AccessPolicy` enum and bypasses that string
/// parser entirely. `AccessPolicy::RequireUserPresence` (biometric-gated
/// access) is the policy argument a future decision would change here; it is
/// not selected today.
///
/// Any other installed store (a test's mock store) gets a plain entry with
/// no modifier — device-only protection is meaningful only under the real
/// protected-data store.
///
/// Non-Apple targets always get a plain entry: Android and Windows have
/// their own at-rest protection, and "does not survive a device-to-device
/// backup restore" is an Apple concept tied to Apple's accessibility
/// classes.
///
/// The access policy an item is created under is fixed for its lifetime —
/// `SecItemUpdate` changes only the stored secret, never the accessibility
/// class — so an item created before this dispatch existed keeps its old
/// class until something deletes and re-creates it (a rotation, a
/// credential refresh, or an explicit
/// [`StoreKeys::reprotect_apple_keys`]/[`DeviceKeys::reprotect_apple_keys`]
/// call).
#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(crate) fn entry_for(account: &str) -> Result<keyring_core::Entry, KeyError> {
    let service = keyring_service()?;
    let store = keyring_core::get_default_store().ok_or(KeyError::StoreNotInstalled)?;
    match store
        .as_any()
        .downcast_ref::<apple_native_keyring_store::protected::Store>()
    {
        Some(_) => apple_native_keyring_store::protected::Cred::build(
            service,
            account,
            apple_native_keyring_store::protected::AccessPolicy::WhenUnlockedThisDeviceOnly,
            None,
            false,
        )
        .map_err(map_keyring_error),
        None => keyring_core::Entry::new(service, account).map_err(map_keyring_error),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
pub(crate) fn entry_for(account: &str) -> Result<keyring_core::Entry, KeyError> {
    keyring_core::Entry::new(keyring_service()?, account).map_err(map_keyring_error)
}

/// Test-only: reaches [`entry_for`] across the crate boundary. Exists so an
/// integration test can install a specific keyring store and assert which
/// entry-construction path the chokepoint took, without re-implementing its
/// dispatch.
#[cfg(any(test, feature = "test-utils"))]
pub fn entry_for_test(account: &str) -> Result<keyring_core::Entry, KeyError> {
    entry_for(account)
}

pub(crate) fn read(slot: &KeyringSlot) -> Result<Option<String>, KeyError> {
    let account = slot.account();
    let entry = entry_for(&account)?;
    match entry.get_password() {
        Ok(p) if p.is_empty() => Err(KeyError::Persistence(format!(
            "keyring entry {account} is present but empty (corrupt)"
        ))),
        Ok(p) => Ok(Some(p)),
        Err(keyring_core::Error::NoEntry) => Ok(None),
        Err(e) => Err(map_keyring_error(e)),
    }
}

pub(crate) fn write(slot: &KeyringSlot, value: &str) -> Result<(), KeyError> {
    entry_for(&slot.account())?
        .set_password(value)
        .map_err(map_keyring_error)
}

pub(crate) fn delete(slot: &KeyringSlot) -> Result<bool, KeyError> {
    match entry_for(&slot.account())?.delete_credential() {
        Ok(()) => Ok(true),
        Err(keyring_core::Error::NoEntry) => Ok(false),
        Err(e) => Err(map_keyring_error(e)),
    }
}

/// Re-create `slot` under whatever policy [`entry_for`] applies today —
/// delete the existing item and write its value back under a fresh entry.
/// Reads and deletes are no-ops when the slot is absent (nothing to
/// reprotect). Delete-then-write is the only sequence the Keychain API
/// exposes for changing an item's accessibility class; there is no atomic
/// "recreate under a new class" primitive, so a failure between the delete
/// and the write is reported to the caller as an error — the value is not
/// recoverable by this function once that happens — rather than retried or
/// covered up.
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn reprotect_slot(slot: &KeyringSlot) -> Result<(), KeyError> {
    let Some(value) = read(slot)? else {
        return Ok(());
    };
    delete(slot)?;
    write(slot, &value)
}

/// Serializes [`DeviceKeys::get_or_create_user_keypair`]'s check-then-act
/// sequence process-wide, so two callers racing identity establishment (two
/// threads, or two `ensure_device_identity` calls) converge on one generated
/// keypair rather than each generating its own and the last write winning.
static GET_OR_CREATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The device-global signing identity: one Ed25519 keypair per OS user,
/// shared by every store on the device (see [`UserKeypair`]'s doc), read from
/// and written to whatever [`crate::set_identity_custody`] registered —
/// [`crate::identity_custody::IdentityCustody::Keyring`] and its fixed
/// [`KeyringSlot::DeviceSigningKey`] account when nothing was registered.
/// This is an internal facade over that custody: its public methods keep
/// coven's existing signatures where possible, so the custody split's blast
/// radius stays here. Stateless — associated functions with no per-store
/// data, since the identity itself is process-global.
pub struct DeviceKeys;

impl DeviceKeys {
    /// The device's established signing identity, or
    /// [`KeyError::NoDeviceIdentity`] when none is established — the caller
    /// must run identity establishment first
    /// ([`crate::ensure_device_identity`], a completed join, or a completed
    /// restore) before any operation that requires an existing identity.
    /// Never mints: a connect/join precondition, not a query.
    pub fn require_user_keypair() -> Result<UserKeypair, KeyError> {
        identity_custody()
            .unlock()?
            .ok_or(KeyError::NoDeviceIdentity)
    }

    /// Get-or-create through the registered custody. Internal primitive
    /// behind [`crate::ensure_device_identity`] and the deliberate
    /// identity-establishing acts that mint as a side effect of what they're
    /// asked to do (requesting a join code, generating a restore code) — not
    /// exposed directly so every other call site goes through
    /// [`require_user_keypair`](Self::require_user_keypair) instead and never
    /// mints by accident.
    pub(crate) fn get_or_create_user_keypair() -> Result<UserKeypair, KeyError> {
        let _guard = GET_OR_CREATE_LOCK.lock().unwrap();
        let custody = identity_custody();
        if let Some(kp) = custody.unlock()? {
            return Ok(kp);
        }

        let kp = UserKeypair::generate();
        custody.persist(&kp)?;
        info!("Generated and saved new user Ed25519 keypair");
        Ok(kp)
    }

    /// A query, not a connect: `Ok(None)` when no identity is established,
    /// distinct from a key-store failure (`Err`). Never mints.
    pub fn get_user_public_key() -> Result<Option<[u8; SIGN_PUBLICKEYBYTES]>, KeyError> {
        Ok(identity_custody().unlock()?.map(|kp| kp.public_key()))
    }

    /// Import an already-generated signing key (a restore code's `sk`, or a
    /// keypair migrated from elsewhere) into the registered custody.
    /// Same-pubkey re-import is idempotent; importing over a DIFFERENT
    /// already-established identity is refused with
    /// [`KeyError::IdentityMismatch`] naming both — silently swapping the
    /// device identity would strand every store that pinned attestations to
    /// the old key.
    pub fn import_user_keypair(signing_key_bytes: &[u8]) -> Result<(), KeyError> {
        let signing_key: [u8; SIGN_SECRETKEYBYTES] =
            signing_key_bytes.try_into().map_err(|_| {
                KeyError::Crypto(format!(
                    "Signing key must be {SIGN_SECRETKEYBYTES} bytes, got {}",
                    signing_key_bytes.len()
                ))
            })?;
        let imported = UserKeypair::from_signing_key_bytes(&signing_key)?;

        let custody = identity_custody();
        if let Some(existing) = custody.unlock()? {
            if existing.public_key() != imported.public_key() {
                return Err(KeyError::IdentityMismatch {
                    existing_pubkey_hex: public_key_hex(&existing),
                    imported_pubkey_hex: public_key_hex(&imported),
                });
            }
        }
        custody.persist(&imported)?;
        info!("Imported user Ed25519 keypair");
        Ok(())
    }

    /// Re-create the device signing key's keyring item under today's Apple
    /// device-only accessibility policy (see [`entry_for`]). A no-op when no
    /// identity is established yet — there is nothing to reprotect. A host
    /// calls this explicitly, once, after upgrading to a coven build that
    /// writes device-only, to bring an item created under an older
    /// accessibility policy forward; coven never does this on its own on a
    /// read or write.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub fn reprotect_apple_keys() -> Result<(), KeyError> {
        reprotect_slot(&KeyringSlot::DeviceSigningKey)
    }
}

/// One store's key material: the encryption master key, cloud-home credentials,
/// and OAuth tokens, each stored under a store-scoped keyring account
/// (`{base}:{store_id}`). The device signing identity is *not* here — it is
/// device-global; see [`DeviceKeys`].
#[derive(Clone)]
pub struct StoreKeys {
    store_id: String,
}

impl StoreKeys {
    pub fn new(store_id: String) -> Self {
        Self { store_id }
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub fn get_encryption_key(&self) -> Result<Option<String>, KeyError> {
        read(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()))
    }

    pub fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        write(
            &KeyringSlot::EncryptionMasterKey(self.store_id.clone()),
            value,
        )?;
        info!("Encryption key saved to keyring");
        Ok(())
    }

    pub fn delete_encryption_key(&self) -> Result<(), KeyError> {
        if delete(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()))? {
            info!("Encryption key deleted from keyring");
        }
        Ok(())
    }

    pub fn get_cloud_home_credentials(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        match read(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()))? {
            None => Ok(None),
            Some(j) => serde_json::from_str(&j).map(Some).map_err(|e| {
                KeyError::Crypto(format!("malformed cloud home credentials JSON: {e}"))
            }),
        }
    }

    pub fn set_cloud_home_credentials(&self, creds: &CloudHomeCredentials) -> Result<(), KeyError> {
        let json = serde_json::to_string(creds)
            .map_err(|e| KeyError::Crypto(format!("serialize credentials: {e}")))?;
        write(
            &KeyringSlot::CloudHomeCredentials(self.store_id.clone()),
            &json,
        )?;
        info!("Cloud home credentials saved to keyring");
        Ok(())
    }

    #[cfg(feature = "oauth-providers")]
    pub fn set_cloud_home_oauth_tokens(
        &self,
        tokens: &crate::oauth::OAuthTokens,
    ) -> Result<(), KeyError> {
        let token_json = serde_json::to_string(tokens)
            .map_err(|e| KeyError::Crypto(format!("serialize OAuth tokens: {e}")))?;
        self.set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn cloud_home_credentials_entry_for_test(
        &self,
    ) -> Result<keyring_core::Entry, KeyError> {
        entry_for(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()).account())
    }

    pub fn delete_cloud_home_credentials(&self) -> Result<(), KeyError> {
        if delete(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()))? {
            info!("Cloud home credentials deleted from keyring");
        }
        Ok(())
    }

    /// Re-create this store's Apple keyring items (the encryption master
    /// key, the cloud-home credentials) under today's device-only
    /// accessibility policy (see [`entry_for`]). Each slot that is absent is
    /// left absent — nothing to reprotect there. A host calls this
    /// explicitly, once per store, after upgrading to a coven build that
    /// writes device-only, to bring items created under an older
    /// accessibility policy forward; coven never does this on its own on a
    /// read or write.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub fn reprotect_apple_keys(&self) -> Result<(), KeyError> {
        reprotect_slot(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()))?;
        reprotect_slot(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()))
    }
}

#[cfg(test)]
pub(crate) mod test_keyring {
    use std::sync::{Mutex, Once};

    static INSTALL: Once = Once::new();
    pub(crate) static SIGNING_KEY_GUARD: Mutex<()> = Mutex::new(());

    pub(crate) fn install() {
        INSTALL.call_once(|| {
            // Install the in-memory mock before registering the service so
            // `set_keyring_service` keeps it instead of reaching for the OS
            // keychain — a platform mechanism these tests never touch.
            keyring_core::set_default_store(
                keyring_core::mock::Store::new().expect("create mock keyring store"),
            );
            super::set_keyring_service("coven-tests").expect("register keyring service");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_keyring_entry_is_an_error_not_absence() {
        test_keyring::install();
        let slot = KeyringSlot::EncryptionMasterKey("empty-keyring-entry-store".to_string());
        let account = slot.account();
        entry_for(&account)
            .expect("create keyring entry")
            .set_password("")
            .expect("write empty keyring entry");

        let error = read(&slot).expect_err("empty entry is corrupt");

        assert!(error.to_string().contains("present but empty"));
        assert!(error.to_string().contains(&account));
    }

    /// The keyring account names are a durable storage contract: a device's
    /// already-stored keys are found only at these exact accounts, so
    /// `StoreKeys` and `DeviceKeys` must keep using them verbatim. Pin all three.
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
            KeyringSlot::DeviceSigningKey.account(),
            "coven_user_signing_key"
        );
    }

    /// The device signing key is store-independent and lives at one fixed
    /// account. A keypair written straight to the raw keyring under that account
    /// name reads back through `DeviceKeys` unchanged — the account math the two
    /// sides use is the same, so the split doesn't strand an already-stored key.
    #[test]
    fn device_keys_reads_a_keypair_written_at_the_fixed_account() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let keypair = UserKeypair::generate();
        let expected_pubkey = keypair.public_key();
        // Write via the raw keyring under the fixed signing-key account, the way
        // the storage layer does — no `DeviceKeys` involved on the write side.
        write(
            &KeyringSlot::DeviceSigningKey,
            &hex::encode(keypair.to_keypair_bytes()),
        )
        .expect("write signing key to the raw keyring");

        let read = DeviceKeys::require_user_keypair().expect("read the device keypair back");
        assert_eq!(
            read.public_key(),
            expected_pubkey,
            "DeviceKeys must read the keypair stored at the fixed account",
        );
    }

    /// `require_user_keypair` maps absence to the typed
    /// `KeyError::NoDeviceIdentity`, not the generic string error the old
    /// `get_user_keypair` returned — every connect/join precondition that
    /// requires an existing identity gets a matchable, actionable error.
    #[test]
    fn require_user_keypair_maps_absence_to_no_device_identity() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        match DeviceKeys::require_user_keypair() {
            Err(error) => assert!(matches!(error, KeyError::NoDeviceIdentity), "got {error:?}"),
            Ok(_) => panic!("no identity is established"),
        }
    }

    /// A same-pubkey re-import (the retry path a host takes if the first
    /// import attempt's caller-side bookkeeping failed after the keyring
    /// write) is idempotent — no error, and the identity reads back
    /// unchanged.
    #[test]
    fn import_user_keypair_same_pubkey_reimport_is_idempotent() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        let keypair = UserKeypair::generate();
        DeviceKeys::import_user_keypair(&keypair.to_keypair_bytes())
            .expect("first import establishes the identity");
        DeviceKeys::import_user_keypair(&keypair.to_keypair_bytes())
            .expect("re-importing the same key is idempotent");

        assert_eq!(
            DeviceKeys::require_user_keypair()
                .expect("identity still readable")
                .public_key(),
            keypair.public_key(),
        );
    }

    /// Importing a DIFFERENT key over an already-established identity is
    /// refused — silently swapping the device identity would strand every
    /// store that pinned attestations to the existing key.
    #[test]
    fn import_user_keypair_refuses_to_overwrite_a_different_identity() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        let established = UserKeypair::generate();
        DeviceKeys::import_user_keypair(&established.to_keypair_bytes())
            .expect("establish the first identity");

        let different = UserKeypair::generate();
        let error = DeviceKeys::import_user_keypair(&different.to_keypair_bytes())
            .expect_err("importing a different identity must be refused");
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
            DeviceKeys::require_user_keypair()
                .expect("the original identity is untouched")
                .public_key(),
            established.public_key(),
        );
    }

    /// Two callers racing `ensure_device_identity` in the same process
    /// converge on one generated identity — the `GET_OR_CREATE_LOCK` around
    /// `get_or_create_user_keypair`'s check-then-act sequence, not a race
    /// where the second generate-and-persist silently clobbers the first.
    #[test]
    fn ensure_device_identity_concurrent_calls_converge_on_one_key() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        let handles: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(crate::ensure_device_identity))
            .collect();
        let pubkeys: Vec<[u8; SIGN_PUBLICKEYBYTES]> = handles
            .into_iter()
            .map(|h| h.join().unwrap().expect("ensure_device_identity"))
            .collect();

        let first = pubkeys[0];
        assert!(
            pubkeys.iter().all(|pk| *pk == first),
            "every concurrent caller must observe the same established identity: {pubkeys:?}",
        );
    }

    // =========================================================================
    // reprotect_apple_keys
    //
    // These exercise `reprotect_slot`'s delete-then-write orchestration and
    // fail-loud behavior against the mock store — the same store every other
    // test in this module uses. What's Apple-specific about
    // `reprotect_apple_keys` is *which policy* `entry_for` applies, which
    // `entry_for_dispatches_to_the_protected_store_device_only` in
    // `tests/apple_keyring.rs` pins against the real protected store; the
    // orchestration under test here (read, delete, write, and what happens
    // when a step fails) does not depend on which store is installed.
    // =========================================================================

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn reprotect_apple_keys_round_trips_a_present_store_slot() {
        test_keyring::install();
        let keys = StoreKeys::new("reprotect-store-round-trip".to_string());
        keys.set_encryption_key("the-master-key").expect("set");

        keys.reprotect_apple_keys().expect("reprotect");

        assert_eq!(
            keys.get_encryption_key().expect("get after reprotect"),
            Some("the-master-key".to_string()),
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn reprotect_apple_keys_is_a_no_op_when_every_store_slot_is_absent() {
        test_keyring::install();
        let keys = StoreKeys::new("reprotect-store-absent".to_string());

        keys.reprotect_apple_keys()
            .expect("reprotecting an established-nothing store is not an error");

        assert_eq!(keys.get_encryption_key().expect("get"), None);
        assert!(keys.get_cloud_home_credentials().expect("get").is_none());
    }

    /// A failure on the first step of a slot's reprotect (here, the read)
    /// surfaces as `Err` from `reprotect_apple_keys` rather than being
    /// swallowed, and — because the failure lands before the destructive
    /// delete — the already-stored value is left exactly as it was.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn reprotect_apple_keys_fails_loud_and_leaves_the_value_untouched() {
        test_keyring::install();
        let keys = StoreKeys::new("reprotect-store-fails-loud".to_string());
        keys.set_encryption_key("untouched-value").expect("set");

        let account = KeyringSlot::EncryptionMasterKey(keys.store_id().to_string()).account();
        let entry = entry_for(&account).expect("build entry for the account under test");
        let mock: &keyring_core::mock::Cred = entry
            .as_any()
            .downcast_ref()
            .expect("the mock store is installed");
        mock.set_error(keyring_core::Error::Invalid(
            "induced".to_string(),
            "reprotect's first read fails".to_string(),
        ));

        keys.reprotect_apple_keys()
            .expect_err("an injected read failure must surface, not be swallowed");

        assert_eq!(
            keys.get_encryption_key()
                .expect("get after failed reprotect"),
            Some("untouched-value".to_string()),
            "a failure before the destructive delete must leave the stored value unchanged",
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn device_keys_reprotect_apple_keys_round_trips_the_signing_key() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        let keypair = UserKeypair::generate();
        write(
            &KeyringSlot::DeviceSigningKey,
            &hex::encode(keypair.to_keypair_bytes()),
        )
        .expect("establish a device identity");

        DeviceKeys::reprotect_apple_keys().expect("reprotect");

        assert_eq!(
            DeviceKeys::require_user_keypair()
                .expect("read after reprotect")
                .public_key(),
            keypair.public_key(),
        );
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn device_keys_reprotect_apple_keys_is_a_no_op_with_no_established_identity() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        DeviceKeys::reprotect_apple_keys()
            .expect("reprotecting with no established identity is not an error");

        assert!(matches!(
            DeviceKeys::require_user_keypair(),
            Err(KeyError::NoDeviceIdentity)
        ));
    }
}

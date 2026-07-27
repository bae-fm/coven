use tracing::info;

// The key types re-exported at the crate root (see lib.rs) stay public;
// `public_key_hex` and the SIGN_*BYTES constants are used only within coven, so
// they stay crate-internal.
pub(crate) use coven_core::keys::{public_key_hex, SIGN_PUBLICKEYBYTES, SIGN_SECRETKEYBYTES};
pub use coven_core::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, KeyError, MasterKeyCustody, UserKeypair,
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

/// Why a [`CovenHandle`](crate::CovenHandle) [`initialize_identity`](crate::CovenHandle::initialize_identity)
/// call failed.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// `initialize_identity` found an identity already established for this
    /// store — custody `unlock()` returned `Some`. coven never generates over
    /// an existing identity; a store's identity is established exactly once,
    /// by whichever of create/join/restore established it first.
    #[error("an identity is already established for this store")]
    AlreadyEstablished,
    #[error("key error: {0}")]
    Key(#[from] KeyError),
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

fn map_keyring_error(e: keyring_core::Error) -> KeyError {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    if is_missing_keychain_entitlement(&e) {
        return KeyError::MissingKeychainEntitlement;
    }
    match e {
        keyring_core::Error::NoDefaultStore => KeyError::StoreNotInstalled,
        other => KeyError::Persistence(other.to_string()),
    }
}

/// `errSecMissingEntitlement` (OSStatus -34018) arrives from
/// `apple-native-keyring-store`'s `protected::decode_error` as
/// `keyring_core::Error::PlatformFailure`, whose payload is a
/// `Box<dyn std::error::Error + Send + Sync>` — the OSStatus is not exposed as
/// a field on `keyring_core::Error` itself. The box's concrete type is always
/// `security_framework::base::Error` on this path (verified by reading
/// `apple-native-keyring-store`'s `protected.rs`: every `decode_error` arm
/// boxes the `security_framework::base::Error` it received), so `downcast_ref`
/// recovers it and `.code()` reads the real OSStatus — a structured match, not
/// a string search over the formatted error.
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn is_missing_keychain_entitlement(e: &keyring_core::Error) -> bool {
    let keyring_core::Error::PlatformFailure(inner) = e else {
        return false;
    };
    inner
        .downcast_ref::<security_framework::base::Error>()
        .is_some_and(|err| err.code() == -34018)
}

/// The base account name [`KeyringSlot::DeviceSigningKey`] renders as
/// `{base}:{store_id}` under.
const DEVICE_SIGNING_KEY_BASE: &str = "coven_user_signing_key";
/// The base account name [`KeyringSlot::EncryptionMasterKey`] renders as
/// `{base}:{store_id}` under.
const ENCRYPTION_MASTER_KEY_BASE: &str = "encryption_master_key";
/// The base account name [`KeyringSlot::CloudHomeCredentials`] renders as
/// `{base}:{store_id}` under.
const CLOUD_HOME_CREDENTIALS_BASE: &str = "cloud_home_credentials";
/// The base account name [`KeyringSlot::PendingIdentity`] renders as
/// `{base}:{request_public_key_hex}` under.
const PENDING_IDENTITY_BASE: &str = "coven_pending_identity";

/// Every name coven's own [`KeyringSlot`] variants reserve for themselves —
/// built from the same constants [`KeyringSlot::account`] renders accounts
/// from, so this list cannot drift from what coven actually stores under. A
/// host secret's `name` must not equal any of these: see
/// [`validate_host_secret_name`].
pub(crate) const RESERVED_HOST_SECRET_NAMES: &[&str] = &[
    DEVICE_SIGNING_KEY_BASE,
    ENCRYPTION_MASTER_KEY_BASE,
    CLOUD_HOME_CREDENTIALS_BASE,
    PENDING_IDENTITY_BASE,
];

/// Which key a keyring entry holds, and the sole owner of the account name it
/// is stored under. The device signing key, the encryption master key, the
/// cloud-home credentials, and a host secret are all per store; a pending
/// identity is keyed by its own join request instead of a store (it exists
/// before the joiner knows which store the invite names — see
/// [`crate::keys::mint_pending_identity`]). Every keyring read/write/delete
/// names its entry with one of these variants, so the on-disk account
/// strings live in exactly one place: [`KeyringSlot::account`].
pub(crate) enum KeyringSlot {
    /// A store's Ed25519 signing identity.
    DeviceSigningKey(String),
    /// A store's encryption master key.
    EncryptionMasterKey(String),
    /// A store's cloud-home credentials.
    CloudHomeCredentials(String),
    /// A join request's not-yet-store-scoped signing identity, keyed by the
    /// request's own public key.
    PendingIdentity(String),
    /// A host's own store-scoped secret, named by the host and validated
    /// against [`RESERVED_HOST_SECRET_NAMES`] before it ever reaches this
    /// variant (see [`validate_host_secret_name`]).
    HostSecret { name: String, store_id: String },
}

impl KeyringSlot {
    /// The keyring account name this slot is stored under. These strings are a
    /// durable storage contract: a device's already-stored keys are found only
    /// at these exact accounts, so changing any of them strands stored keys.
    /// `HostSecret`'s rendering is a storage contract with the *host*, not
    /// just coven: it must stay byte-identical to whatever account a host
    /// already wrote its secrets under before this API existed.
    pub(crate) fn account(&self) -> String {
        match self {
            KeyringSlot::DeviceSigningKey(store_id) => {
                format!("{DEVICE_SIGNING_KEY_BASE}:{store_id}")
            }
            KeyringSlot::EncryptionMasterKey(store_id) => {
                format!("{ENCRYPTION_MASTER_KEY_BASE}:{store_id}")
            }
            KeyringSlot::CloudHomeCredentials(store_id) => {
                format!("{CLOUD_HOME_CREDENTIALS_BASE}:{store_id}")
            }
            KeyringSlot::PendingIdentity(request_public_key_hex) => {
                format!("{PENDING_IDENTITY_BASE}:{request_public_key_hex}")
            }
            KeyringSlot::HostSecret { name, store_id } => format!("{name}:{store_id}"),
        }
    }
}

/// Reject a host secret name that would collide with, or otherwise misuse,
/// coven's own keyring account scheme: one of coven's own reserved names, the
/// empty string, or a name containing `:` (the scheme's separator — allowing
/// one would let a host secret's name forge another store's account). Called
/// at the API boundary before a [`KeyringSlot::HostSecret`] is ever built.
pub(crate) fn validate_host_secret_name(name: &str) -> Result<(), KeyError> {
    if name.is_empty() {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "a host secret name must not be empty".to_string(),
        });
    }
    if name.contains(':') {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "a host secret name must not contain ':', the keyring account scheme's \
                     separator"
                .to_string(),
        });
    }
    if RESERVED_HOST_SECRET_NAMES.contains(&name) {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "reserved for coven's own keyring entries".to_string(),
        });
    }
    Ok(())
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
/// The access policy an item is created under is fixed for its lifetime;
/// every Coven-created Apple keyring item therefore enters the device-only
/// class at its first write.
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

/// Test-only: reaches `entry_for` across the crate boundary. Exists so an
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

/// This store's established signing identity through `custody`, or
/// [`KeyError::NoDeviceIdentity`] when none is established — the caller must
/// complete create/join/restore for this store first. Never mints: a
/// connect/join precondition, not a query.
pub(crate) fn require_identity(
    custody: &dyn DeviceIdentityCustody,
) -> Result<UserKeypair, KeyError> {
    custody.unlock()?.ok_or(KeyError::NoDeviceIdentity)
}

/// A query, not a connect: `Ok(None)` when this store has no identity
/// established, distinct from a key-store failure (`Err`). Never mints.
pub(crate) fn identity_public_key(
    custody: &dyn DeviceIdentityCustody,
) -> Result<Option<[u8; SIGN_PUBLICKEYBYTES]>, KeyError> {
    Ok(custody.unlock()?.map(|kp| kp.public_key()))
}

/// Import an already-generated signing key (a restore code's `sk`) into
/// `custody`. Same-pubkey re-import is idempotent; importing over a
/// DIFFERENT already-established identity is refused with
/// [`KeyError::IdentityMismatch`] naming both — silently swapping this
/// store's identity would strand its already-signed membership entries.
pub(crate) fn import_identity(
    custody: &dyn DeviceIdentityCustody,
    signing_key_bytes: &[u8],
) -> Result<(), KeyError> {
    let signing_key: [u8; SIGN_SECRETKEYBYTES] = signing_key_bytes.try_into().map_err(|_| {
        KeyError::Crypto(format!(
            "Signing key must be {SIGN_SECRETKEYBYTES} bytes, got {}",
            signing_key_bytes.len()
        ))
    })?;
    let imported = UserKeypair::from_signing_key_bytes(&signing_key)?;

    if let Some(existing) = custody.unlock()? {
        if existing.public_key() != imported.public_key() {
            return Err(KeyError::IdentityMismatch {
                existing_pubkey_hex: public_key_hex(&existing),
                imported_pubkey_hex: public_key_hex(&imported),
            });
        }
    }
    custody.persist(&imported)?;
    info!("Imported this store's Ed25519 signing identity");
    Ok(())
}

/// Mint a fresh identity for a join request that has not yet named a store:
/// the joiner sends its public key before it learns which store the invite
/// is for (`JoinRequestCode`), so this keypair is generated now and held
/// under a pending slot keyed by its own public key. The join establishes it
/// in the joined store's own identity custody (via [`import_identity`],
/// before the store's completion marker) and discards the pending slot only
/// once the whole join succeeds; [`discard_pending_identity`] also removes it
/// if the request is abandoned instead. Always the OS keyring: unlike an
/// established store's identity, there is no store yet to select a custody
/// policy for, and a pending identity's lifetime is short (a join round trip,
/// not a store's lifetime).
pub(crate) fn mint_pending_identity() -> Result<UserKeypair, KeyError> {
    let keypair = UserKeypair::generate();
    write(
        &KeyringSlot::PendingIdentity(public_key_hex(&keypair)),
        &hex::encode(keypair.to_keypair_bytes()),
    )?;
    info!("Minted a pending identity for a join request");
    Ok(keypair)
}

/// Read (without consuming) the pending identity keyed by
/// `request_public_key_hex` — what a join in progress signs its bootstrap
/// traffic with, and what it establishes in the store's own custody before
/// the completion marker. [`KeyError::NoPendingIdentity`] if none is held
/// under that key.
pub(crate) fn peek_pending_identity(request_public_key_hex: &str) -> Result<UserKeypair, KeyError> {
    read_pending_identity_slot(&KeyringSlot::PendingIdentity(
        request_public_key_hex.to_string(),
    ))
}

fn read_pending_identity_slot(slot: &KeyringSlot) -> Result<UserKeypair, KeyError> {
    let KeyringSlot::PendingIdentity(request_public_key_hex) = slot else {
        unreachable!("read_pending_identity_slot is only ever called with a PendingIdentity slot");
    };
    let sk_hex = read(slot)?.ok_or_else(|| KeyError::NoPendingIdentity {
        request_public_key_hex: request_public_key_hex.clone(),
    })?;
    let signing_key: [u8; SIGN_SECRETKEYBYTES] = hex::decode(&sk_hex)
        .map_err(|e| KeyError::Crypto(format!("invalid pending identity hex: {e}")))?
        .try_into()
        .map_err(|_| KeyError::Crypto("pending identity wrong length".to_string()))?;
    UserKeypair::from_signing_key_bytes(&signing_key)
}

/// Discard the pending identity keyed by `request_public_key_hex` — a join
/// request abandoned without completing, or one whose identity the completed
/// join has already established in the store's own custody. `Ok` whether or
/// not one was pending.
pub(crate) fn discard_pending_identity(request_public_key_hex: &str) -> Result<(), KeyError> {
    delete(&KeyringSlot::PendingIdentity(
        request_public_key_hex.to_string(),
    ))
    .map(|_| ())
}

/// One store's key material: the encryption master key, cloud-home credentials,
/// and OAuth tokens, each stored under a store-scoped keyring account
/// (`{base}:{store_id}`). The store's signing identity is not here — it goes
/// through [`crate::identity_custody::IdentityCustody`], the same way the
/// master key goes through [`crate::custody::KeyCustody`].
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

    #[cfg(test)]
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

    fn host_secret_slot(&self, name: &str) -> KeyringSlot {
        KeyringSlot::HostSecret {
            name: name.to_string(),
            store_id: self.store_id.clone(),
        }
    }

    /// A host's own store-scoped secret — an API token, a service credential
    /// — read from the same keyring service and access policy as coven's own
    /// key material. `None` if never set. [`KeyError::InvalidSecretName`] if
    /// `name` collides with one of coven's own reserved slot names, is
    /// empty, or contains `:` (see `validate_host_secret_name`).
    pub fn get_host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        validate_host_secret_name(name)?;
        read(&self.host_secret_slot(name))
    }

    /// Set a host's own store-scoped secret. Same name restrictions as
    /// [`get_host_secret`](Self::get_host_secret).
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        validate_host_secret_name(name)?;
        write(&self.host_secret_slot(name), value)?;
        info!("Host secret {name:?} saved to keyring");
        Ok(())
    }

    /// Remove a host secret. `Ok` whether or not one was set. Same name
    /// restrictions as [`get_host_secret`](Self::get_host_secret).
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        validate_host_secret_name(name)?;
        if delete(&self.host_secret_slot(name))? {
            info!("Host secret {name:?} deleted from keyring");
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_keyring {
    use std::sync::Once;

    static INSTALL: Once = Once::new();

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
    /// generic, stringly-typed `Persistence` error rather than being
    /// mis-reported as a missing entitlement.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn a_different_platform_failure_os_status_is_not_reported_as_missing_entitlement() {
        let raw = keyring_core::Error::PlatformFailure(Box::new(
            security_framework::base::Error::from_code(-25291), // errSecNotAvailable
        ));

        let mapped = map_keyring_error(raw);

        assert!(matches!(mapped, KeyError::Persistence(_)), "got {mapped:?}");
    }

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
        let keys = StoreKeys::new("host-secret-round-trip".to_string());

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
        let keys = StoreKeys::new("host-secret-reserved-names".to_string());

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
        let keys = StoreKeys::new("host-secret-colon-name".to_string());

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
        let keys = StoreKeys::new("host-secret-empty-name".to_string());

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
        entry_for(&account)
            .expect("create keyring entry")
            .set_password("")
            .expect("write empty keyring entry");

        let keys = StoreKeys::new("host-secret-empty-entry-store".to_string());
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
        let store_a = StoreKeys::new("host-secret-scope-a".to_string());
        let store_b = StoreKeys::new("host-secret-scope-b".to_string());

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

    /// A per-store keyring identity custody. Each test names its own
    /// `store_id` so tests never race each other's keyring accounts.
    fn test_identity_custody(store_id: &str) -> std::sync::Arc<dyn DeviceIdentityCustody> {
        crate::identity_custody::IdentityCustody::Keyring.resolve(
            store_id,
            &crate::store_dir::StoreDir::new("unused-store-dir"),
        )
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
        write(
            &KeyringSlot::DeviceSigningKey(store_id.to_string()),
            &hex::encode(keypair.to_keypair_bytes()),
        )
        .expect("write signing key to the raw keyring");

        let custody = test_identity_custody(store_id);
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
        let custody = test_identity_custody("require-identity-absent-test");

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
    fn import_identity_same_pubkey_reimport_is_idempotent() {
        test_keyring::install();
        let custody = test_identity_custody("import-identity-idempotent-test");

        let keypair = UserKeypair::generate();
        import_identity(custody.as_ref(), &keypair.to_keypair_bytes())
            .expect("first import establishes the identity");
        import_identity(custody.as_ref(), &keypair.to_keypair_bytes())
            .expect("re-importing the same key is idempotent");

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
    fn import_identity_refuses_to_overwrite_a_different_identity() {
        test_keyring::install();
        let custody = test_identity_custody("import-identity-mismatch-test");

        let established = UserKeypair::generate();
        import_identity(custody.as_ref(), &established.to_keypair_bytes())
            .expect("establish the first identity");

        let different = UserKeypair::generate();
        let error = import_identity(custody.as_ref(), &different.to_keypair_bytes())
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
            require_identity(custody.as_ref())
                .expect("the original identity is untouched")
                .public_key(),
            established.public_key(),
        );
    }

    /// A pending identity minted for a join request establishes into a store's
    /// identity custody via `import_identity` while the pending slot still
    /// serves it — the split the join relies on: establish before the
    /// completion marker, discard the slot only once the whole join succeeds.
    /// Re-establishing from the still-present slot is idempotent (the torn-
    /// bootstrap retry), and the discard afterward empties the slot.
    #[test]
    fn pending_identity_establishes_then_discards() {
        test_keyring::install();
        let pending = mint_pending_identity().expect("mint pending identity");
        let request_pubkey = public_key_hex(&pending);
        let custody = test_identity_custody("pending-identity-establish-test");

        import_identity(custody.as_ref(), &pending.to_keypair_bytes())
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
        import_identity(custody.as_ref(), &still_pending.to_keypair_bytes())
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

        let custody_a = test_identity_custody("two-concurrent-joins-store-a");
        let custody_b = test_identity_custody("two-concurrent-joins-store-b");
        import_identity(custody_a.as_ref(), &pending_a.to_keypair_bytes())
            .expect("establish a into store a");

        assert!(
            require_identity(custody_b.as_ref()).is_err(),
            "store b must not see store a's established identity",
        );
        import_identity(custody_b.as_ref(), &pending_b.to_keypair_bytes())
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
}

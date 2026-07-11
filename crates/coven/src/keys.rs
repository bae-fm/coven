use tracing::info;

pub use coven_core::keys::{
    CloudHomeCredentials, KeyError, KeyPersistence, UserKeypair, SIGN_PUBLICKEYBYTES,
    SIGN_SECRETKEYBYTES,
};

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
    match e {
        keyring_core::Error::NoDefaultStore => KeyError::StoreNotInstalled,
        other => KeyError::Persistence(other.to_string()),
    }
}

pub fn read_keyring(account: &str) -> Result<Option<String>, KeyError> {
    let entry = keyring_core::Entry::new(keyring_service()?, account).map_err(map_keyring_error)?;
    match entry.get_password() {
        Ok(p) if p.is_empty() => Err(KeyError::Persistence(format!(
            "keyring entry {account} is present but empty (corrupt)"
        ))),
        Ok(p) => Ok(Some(p)),
        Err(keyring_core::Error::NoEntry) => Ok(None),
        Err(e) => Err(map_keyring_error(e)),
    }
}

fn write_keyring(account: &str, value: &str) -> Result<(), KeyError> {
    keyring_core::Entry::new(keyring_service()?, account)
        .map_err(map_keyring_error)?
        .set_password(value)
        .map_err(map_keyring_error)
}

fn delete_keyring(account: &str) -> Result<bool, KeyError> {
    match keyring_core::Entry::new(keyring_service()?, account)
        .map_err(map_keyring_error)?
        .delete_credential()
    {
        Ok(()) => Ok(true),
        Err(keyring_core::Error::NoEntry) => Ok(false),
        Err(e) => Err(map_keyring_error(e)),
    }
}

/// The device-global signing identity: one Ed25519 keypair per OS user, shared
/// by every store on the device. It lives under a single fixed keyring account
/// ([`SIGNING_KEY_KEYRING_ACCOUNT`](Self::SIGNING_KEY_KEYRING_ACCOUNT)) that no
/// store scope touches, so attestations accumulate under one public key across
/// all of this device's stores. Stateless — the keyring service is
/// process-global, so these are associated functions with no per-store data.
pub struct DeviceKeys;

impl DeviceKeys {
    /// The fixed keyring account the device signing key is stored under — the
    /// same for every store, which is exactly what makes the identity
    /// device-global rather than per-store. Changing this string strands every
    /// already-stored device key, so it is a stable storage name, not an
    /// implementation detail.
    const SIGNING_KEY_KEYRING_ACCOUNT: &'static str = "coven_user_signing_key";

    pub fn get_user_keypair() -> Result<UserKeypair, KeyError> {
        Self::get_user_keypair_inner()?
            .ok_or_else(|| KeyError::Crypto("No user keypair found in keyring".to_string()))
    }

    pub fn get_or_create_user_keypair() -> Result<UserKeypair, KeyError> {
        if let Some(kp) = Self::get_user_keypair_inner()? {
            return Ok(kp);
        }

        let kp = UserKeypair::generate();
        Self::write_signing_key(&kp.to_keypair_bytes())?;
        info!("Generated and saved new user Ed25519 keypair");
        Ok(kp)
    }

    pub fn get_user_public_key() -> Result<Option<[u8; SIGN_PUBLICKEYBYTES]>, KeyError> {
        Ok(Self::get_user_keypair_inner()?.map(|kp| kp.public_key()))
    }

    pub fn import_user_keypair(signing_key_bytes: &[u8]) -> Result<(), KeyError> {
        let signing_key: [u8; SIGN_SECRETKEYBYTES] =
            signing_key_bytes.try_into().map_err(|_| {
                KeyError::Crypto(format!(
                    "Signing key must be {SIGN_SECRETKEYBYTES} bytes, got {}",
                    signing_key_bytes.len()
                ))
            })?;
        ed25519_dalek::SigningKey::from_keypair_bytes(&signing_key)
            .map_err(|e| KeyError::Crypto(format!("Invalid keypair bytes: {e}")))?;

        Self::write_signing_key(&signing_key)?;
        info!("Imported user Ed25519 keypair");
        Ok(())
    }

    fn write_signing_key(signing_key: &[u8; SIGN_SECRETKEYBYTES]) -> Result<(), KeyError> {
        let sk_hex = hex::encode(signing_key);
        write_keyring(Self::SIGNING_KEY_KEYRING_ACCOUNT, &sk_hex)
    }

    fn get_user_keypair_inner() -> Result<Option<UserKeypair>, KeyError> {
        let Some(sk_hex) = read_keyring(Self::SIGNING_KEY_KEYRING_ACCOUNT)? else {
            return Ok(None);
        };

        let signing_key: [u8; SIGN_SECRETKEYBYTES] = hex::decode(&sk_hex)
            .map_err(|e| KeyError::Crypto(format!("Invalid signing key hex: {e}")))?
            .try_into()
            .map_err(|_| KeyError::Crypto("Signing key wrong length".to_string()))?;

        Ok(Some(UserKeypair::from_signing_key_bytes(&signing_key)?))
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

    fn account(&self, base: &str) -> String {
        format!("{}:{}", base, self.store_id)
    }

    pub fn get_encryption_key(&self) -> Result<Option<String>, KeyError> {
        read_keyring(&self.account("encryption_master_key"))
    }

    pub fn get_or_create_encryption_key(&self) -> Result<String, KeyError> {
        if let Some(key) = self.get_encryption_key()? {
            return Ok(key);
        }

        let key_hex = hex::encode(crate::encryption::generate_random_key());
        write_keyring(&self.account("encryption_master_key"), &key_hex)?;
        info!("Generated and saved new encryption key to keyring");
        Ok(key_hex)
    }

    pub fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        write_keyring(&self.account("encryption_master_key"), value)?;
        info!("Encryption key saved to keyring");
        Ok(())
    }

    pub fn delete_encryption_key(&self) -> Result<(), KeyError> {
        if delete_keyring(&self.account("encryption_master_key"))? {
            info!("Encryption key deleted from keyring");
        }
        Ok(())
    }

    pub fn get_cloud_home_credentials(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        match read_keyring(&self.account("cloud_home_credentials"))? {
            None => Ok(None),
            Some(j) => serde_json::from_str(&j).map(Some).map_err(|e| {
                KeyError::Crypto(format!("malformed cloud home credentials JSON: {e}"))
            }),
        }
    }

    pub fn set_cloud_home_credentials(&self, creds: &CloudHomeCredentials) -> Result<(), KeyError> {
        let json = serde_json::to_string(creds)
            .map_err(|e| KeyError::Crypto(format!("serialize credentials: {e}")))?;
        write_keyring(&self.account("cloud_home_credentials"), &json)?;
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
    ) -> keyring_core::Result<keyring_core::Entry> {
        keyring_core::Entry::new(
            keyring_service().expect("keyring service registered in tests"),
            &self.account("cloud_home_credentials"),
        )
    }

    pub fn delete_cloud_home_credentials(&self) -> Result<(), KeyError> {
        if delete_keyring(&self.account("cloud_home_credentials"))? {
            info!("Cloud home credentials deleted from keyring");
        }
        Ok(())
    }
}

impl KeyPersistence for StoreKeys {
    fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        self.set_encryption_key(value)
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
        let account = "empty-keyring-entry";
        keyring_core::Entry::new(keyring_service().expect("service registered"), account)
            .expect("create keyring entry")
            .set_password("")
            .expect("write empty keyring entry");

        let error = read_keyring(account).expect_err("empty entry is corrupt");

        assert!(error.to_string().contains("present but empty"));
        assert!(error.to_string().contains(account));
    }

    #[test]
    fn get_or_create_encryption_key_does_not_overwrite_a_corrupt_empty_entry() {
        test_keyring::install();
        let service = StoreKeys::new("empty-encryption-key-store".to_string());
        let account = service.account("encryption_master_key");
        let entry =
            keyring_core::Entry::new(keyring_service().expect("service registered"), &account)
                .expect("create encryption key entry");
        entry
            .set_password("")
            .expect("write empty encryption key entry");

        let error = service
            .get_or_create_encryption_key()
            .expect_err("empty entry is corrupt");

        assert!(error.to_string().contains("present but empty"));
        assert_eq!(
            entry.get_password().expect("read corrupt entry"),
            "",
            "corrupt value is not overwritten"
        );
    }

    /// The keyring account names are a durable storage contract: a device's
    /// already-stored keys are found only at these exact accounts, so
    /// `StoreKeys` and `DeviceKeys` must keep using them verbatim. Pin all three.
    #[test]
    fn keyring_account_names_are_a_stable_storage_contract() {
        let store = StoreKeys::new("store-42".to_string());
        assert_eq!(
            store.account("encryption_master_key"),
            "encryption_master_key:store-42"
        );
        assert_eq!(
            store.account("cloud_home_credentials"),
            "cloud_home_credentials:store-42"
        );
        assert_eq!(
            DeviceKeys::SIGNING_KEY_KEYRING_ACCOUNT,
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
        write_keyring(
            DeviceKeys::SIGNING_KEY_KEYRING_ACCOUNT,
            &hex::encode(keypair.to_keypair_bytes()),
        )
        .expect("write signing key to the raw keyring");

        let read = DeviceKeys::get_user_keypair().expect("read the device keypair back");
        assert_eq!(
            read.public_key(),
            expected_pubkey,
            "DeviceKeys must read the keypair stored at the fixed account",
        );
    }
}

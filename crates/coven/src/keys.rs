use tracing::info;

pub use coven_core::keys::{
    ed25519_to_x25519_public_key, seal_box_decrypt, seal_box_encrypt, verify_signature,
    verify_signature_hex, CloudHomeCredentials, KeyError, KeyPersistence, UserKeypair,
    CURVE25519_PUBLICKEYBYTES, CURVE25519_SECRETKEYBYTES, SEALBYTES, SIGN_BYTES,
    SIGN_PUBLICKEYBYTES, SIGN_SECRETKEYBYTES,
};

static KEYRING_SERVICE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub fn set_keyring_service(name: impl Into<String>) {
    let _ = KEYRING_SERVICE.set(name.into());
}

pub fn keyring_service() -> &'static str {
    KEYRING_SERVICE
        .get()
        .map(String::as_str)
        .expect("set_keyring_service(host_app_identity) must be called once at startup")
}

fn map_keyring_error(e: keyring_core::Error) -> KeyError {
    KeyError::Persistence(e.to_string())
}

pub fn read_keyring(account: &str) -> Result<Option<String>, KeyError> {
    let entry = keyring_core::Entry::new(keyring_service(), account).map_err(map_keyring_error)?;
    match entry.get_password() {
        Ok(p) if p.is_empty() => Ok(None),
        Ok(p) => Ok(Some(p)),
        Err(keyring_core::Error::NoEntry) => Ok(None),
        Err(e) => Err(map_keyring_error(e)),
    }
}

#[derive(Clone)]
pub struct KeyService {
    library_id: String,
}

impl KeyService {
    pub fn new(library_id: String) -> Self {
        Self { library_id }
    }

    pub fn library_id(&self) -> &str {
        &self.library_id
    }

    fn account(&self, base: &str) -> String {
        format!("{}:{}", base, self.library_id)
    }

    pub fn get_encryption_key(&self) -> Result<Option<String>, KeyError> {
        read_keyring(&self.account("encryption_master_key"))
    }

    pub fn get_or_create_encryption_key(&self) -> Result<String, KeyError> {
        if let Some(key) = self.get_encryption_key()? {
            return Ok(key);
        }

        let key_hex = hex::encode(crate::encryption::generate_random_key());
        keyring_core::Entry::new(keyring_service(), &self.account("encryption_master_key"))
            .map_err(map_keyring_error)?
            .set_password(&key_hex)
            .map_err(map_keyring_error)?;
        info!("Generated and saved new encryption key to keyring");
        Ok(key_hex)
    }

    pub fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        keyring_core::Entry::new(keyring_service(), &self.account("encryption_master_key"))
            .map_err(map_keyring_error)?
            .set_password(value)
            .map_err(map_keyring_error)?;
        info!("Encryption key saved to keyring");
        Ok(())
    }

    pub fn delete_encryption_key(&self) -> Result<(), KeyError> {
        match keyring_core::Entry::new(keyring_service(), &self.account("encryption_master_key"))
            .map_err(map_keyring_error)?
            .delete_credential()
        {
            Ok(()) => {
                info!("Encryption key deleted from keyring");
                Ok(())
            }
            Err(keyring_core::Error::NoEntry) => Ok(()),
            Err(e) => Err(map_keyring_error(e)),
        }
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
        keyring_core::Entry::new(keyring_service(), &self.account("cloud_home_credentials"))
            .map_err(map_keyring_error)?
            .set_password(&json)
            .map_err(map_keyring_error)?;
        info!("Cloud home credentials saved to keyring");
        Ok(())
    }

    pub fn delete_cloud_home_credentials(&self) -> Result<(), KeyError> {
        match keyring_core::Entry::new(keyring_service(), &self.account("cloud_home_credentials"))
            .map_err(map_keyring_error)?
            .delete_credential()
        {
            Ok(()) => {
                info!("Cloud home credentials deleted from keyring");
                Ok(())
            }
            Err(keyring_core::Error::NoEntry) => Ok(()),
            Err(e) => Err(map_keyring_error(e)),
        }
    }

    const SIGNING_KEY_KEYRING_ACCOUNT: &'static str = "coven_user_signing_key";

    pub fn get_user_keypair(&self) -> Result<UserKeypair, KeyError> {
        self.get_user_keypair_inner()?
            .ok_or_else(|| KeyError::Crypto("No user keypair found in keyring".to_string()))
    }

    pub fn get_or_create_user_keypair(&self) -> Result<UserKeypair, KeyError> {
        if let Some(kp) = self.get_user_keypair_inner()? {
            return Ok(kp);
        }

        let kp = UserKeypair::generate();
        self.write_signing_key(&kp.signing_key)?;
        info!("Generated and saved new user Ed25519 keypair");
        Ok(kp)
    }

    pub fn get_user_public_key(&self) -> Result<Option<[u8; SIGN_PUBLICKEYBYTES]>, KeyError> {
        Ok(self.get_user_keypair_inner()?.map(|kp| kp.public_key))
    }

    pub fn import_user_keypair(&self, signing_key_bytes: &[u8]) -> Result<(), KeyError> {
        let signing_key: [u8; SIGN_SECRETKEYBYTES] =
            signing_key_bytes.try_into().map_err(|_| {
                KeyError::Crypto(format!(
                    "Signing key must be {SIGN_SECRETKEYBYTES} bytes, got {}",
                    signing_key_bytes.len()
                ))
            })?;
        ed25519_dalek::SigningKey::from_keypair_bytes(&signing_key)
            .map_err(|e| KeyError::Crypto(format!("Invalid keypair bytes: {e}")))?;

        self.write_signing_key(&signing_key)?;
        info!("Imported user Ed25519 keypair");
        Ok(())
    }

    fn write_signing_key(&self, signing_key: &[u8; SIGN_SECRETKEYBYTES]) -> Result<(), KeyError> {
        let sk_hex = hex::encode(signing_key);
        keyring_core::Entry::new(keyring_service(), Self::SIGNING_KEY_KEYRING_ACCOUNT)
            .map_err(map_keyring_error)?
            .set_password(&sk_hex)
            .map_err(map_keyring_error)
    }

    fn get_user_keypair_inner(&self) -> Result<Option<UserKeypair>, KeyError> {
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

impl KeyPersistence for KeyService {
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
            keyring_core::set_default_store(
                keyring_core::mock::Store::new().expect("create mock keyring store"),
            );
            super::set_keyring_service("coven-tests");
        });
    }
}

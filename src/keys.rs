use ed25519_dalek::{Signer, Verifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{info, warn};

// Size constants matching libsodium conventions. Exported so callers (sync modules,
// envelope.rs, etc.) can use them for array sizes and length checks.
pub const SIGN_PUBLICKEYBYTES: usize = 32;
pub const SIGN_SECRETKEYBYTES: usize = 64;
pub const SIGN_BYTES: usize = 64;
pub const CURVE25519_PUBLICKEYBYTES: usize = 32;
pub const CURVE25519_SECRETKEYBYTES: usize = 32;
pub const SEALBYTES: usize = 48; // crypto_box PUBLICKEYBYTES + MACBYTES = 32 + 16

#[derive(Error, Debug)]
pub enum KeyError {
    #[error("Keyring error: {0}")]
    Keyring(#[from] keyring_core::Error),
    #[error("Cannot modify keys in dev mode (use environment variables)")]
    DevMode,
    #[error("Crypto error: {0}")]
    Crypto(String),
}

/// Credentials for the cloud home, stored as a single JSON keyring entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CloudHomeCredentials {
    /// S3-compatible providers: access key + secret key.
    S3 {
        access_key: String,
        secret_key: String,
    },
    /// Consumer cloud providers (Google Drive, Dropbox, OneDrive): OAuth token JSON.
    OAuth { token_json: String },
    /// iCloud: no credentials needed (macOS handles auth).
    None,
}

/// Ed25519 keypair for signing changesets and membership changes.
/// The same seed can derive an X25519 keypair for key wrapping.
///
/// This is a global identity (not per-library) so attestations accumulate
/// under one pubkey across all libraries.
#[derive(Clone)]
pub struct UserKeypair {
    pub signing_key: [u8; SIGN_SECRETKEYBYTES], // Ed25519 secret key (64 bytes: seed + public)
    pub public_key: [u8; SIGN_PUBLICKEYBYTES],  // Ed25519 public key (32 bytes)
}

impl UserKeypair {
    /// Generate a new random Ed25519 keypair. The unmanaged primitive behind
    /// [`KeyService::get_or_create_user_keypair`]; also lets host code (and its
    /// tests) mint an identity directly.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        let public_key = signing_key.verifying_key();
        Self {
            signing_key: signing_key.to_keypair_bytes(),
            public_key: public_key.to_bytes(),
        }
    }

    /// Sign a message, returning a 64-byte detached signature.
    pub fn sign(&self, message: &[u8]) -> [u8; SIGN_BYTES] {
        let sk = ed25519_dalek::SigningKey::from_keypair_bytes(&self.signing_key)
            .expect("valid keypair bytes");
        sk.sign(message).to_bytes()
    }

    /// Derive the X25519 secret key from this Ed25519 signing key.
    pub fn to_x25519_secret_key(&self) -> [u8; CURVE25519_SECRETKEYBYTES] {
        let sk = ed25519_dalek::SigningKey::from_keypair_bytes(&self.signing_key)
            .expect("valid keypair bytes");
        sk.to_scalar_bytes()
    }

    /// Derive the X25519 public key from this Ed25519 public key.
    pub fn to_x25519_public_key(&self) -> [u8; CURVE25519_PUBLICKEYBYTES] {
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&self.public_key)
            .expect("valid public key bytes");
        vk.to_montgomery().to_bytes()
    }
}

/// Verify a detached Ed25519 signature against a public key.
pub fn verify_signature(
    signature: &[u8; SIGN_BYTES],
    message: &[u8],
    public_key: &[u8; SIGN_PUBLICKEYBYTES],
) -> bool {
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    let sig = ed25519_dalek::Signature::from_bytes(signature);
    vk.verify(message, &sig).is_ok()
}

/// Encrypt a message to a recipient's X25519 public key using a sealed box.
/// The sender is anonymous -- only the recipient can decrypt.
///
/// Reimplements crypto_box::PublicKey::seal() to avoid rand_core version
/// mismatch (crypto_box uses rand_core 0.6, we use rand 0.9).
pub fn seal_box_encrypt(
    message: &[u8],
    recipient_x25519_pk: &[u8; CURVE25519_PUBLICKEYBYTES],
) -> Vec<u8> {
    use blake2::{digest::typenum::U24, Blake2b, Digest};
    use crypto_box::aead::Aead;

    let recipient_pk = crypto_box::PublicKey::from(*recipient_x25519_pk);

    // Generate ephemeral X25519 keypair
    let mut ephemeral_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut ephemeral_bytes);
    let ephemeral_sk = crypto_box::SecretKey::from(ephemeral_bytes);
    let ephemeral_pk = ephemeral_sk.public_key();

    // Nonce = Blake2b-192(ephemeral_pk || recipient_pk) -- matches libsodium sealed box spec
    let mut hasher = Blake2b::<U24>::new();
    hasher.update(ephemeral_pk.as_bytes());
    hasher.update(recipient_pk.as_bytes());
    let nonce = hasher.finalize();

    // Encrypt with XSalsa20-Poly1305
    let salsa_box = crypto_box::SalsaBox::new(&recipient_pk, &ephemeral_sk);
    let encrypted = salsa_box
        .encrypt(&nonce, message)
        .expect("sealed box encryption should not fail");

    // Output: ephemeral_pk || ciphertext (matches libsodium format)
    let mut out = Vec::with_capacity(32 + encrypted.len());
    out.extend_from_slice(ephemeral_pk.as_bytes());
    out.extend_from_slice(&encrypted);
    out
}

/// Decrypt a sealed box using the recipient's X25519 keypair.
pub fn seal_box_decrypt(
    ciphertext: &[u8],
    _recipient_x25519_pk: &[u8; CURVE25519_PUBLICKEYBYTES],
    recipient_x25519_sk: &[u8; CURVE25519_SECRETKEYBYTES],
) -> Result<Vec<u8>, KeyError> {
    if ciphertext.len() < SEALBYTES {
        return Err(KeyError::Crypto("Ciphertext too short".to_string()));
    }
    let sk = crypto_box::SecretKey::from(*recipient_x25519_sk);
    sk.unseal(ciphertext).map_err(|_| {
        KeyError::Crypto("Sealed box decryption failed (wrong key or tampered)".to_string())
    })
}

/// Convert an Ed25519 public key to an X25519 public key.
///
/// This is used when we only have a remote user's Ed25519 public key (hex string)
/// and need to encrypt something to them via sealed box. The `UserKeypair` methods
/// handle the local case; this handles the remote case.
pub fn ed25519_to_x25519_public_key(
    ed25519_pk: &[u8; SIGN_PUBLICKEYBYTES],
) -> [u8; CURVE25519_PUBLICKEYBYTES] {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(ed25519_pk)
        .expect("valid Ed25519 public key bytes");
    vk.to_montgomery().to_bytes()
}

/// Manages secret keys (Discogs API key, encryption key) with lazy reads.
///
/// In dev mode, reads from environment variables.
/// In prod mode, reads from the OS keyring. Each library_id gets its own
/// namespaced keyring entries so multiple libraries can have independent keys.
///
/// `new()` does no I/O -- keyring reads happen lazily in `get_*` methods,
/// because the macOS protected keyring triggers a system password prompt.
/// Keyring service name — the app's identity (e.g. "bae", "visible"), used as the
/// first namespace component of every keyring entry. Set once at startup via
/// [`set_keyring_service`]; defaults to "coven". Mirrors keyring_core's own
/// process-global default store, which this design already relies on.
static KEYRING_SERVICE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Set the keyring service name. Call once at startup, before any keyring access.
pub fn set_keyring_service(name: impl Into<String>) {
    let _ = KEYRING_SERVICE.set(name.into());
}

/// The configured keyring service name (see [`set_keyring_service`]). Public so
/// host apps store their own credentials under the same service.
pub fn keyring_service() -> &'static str {
    KEYRING_SERVICE.get().map(String::as_str).unwrap_or("coven")
}

/// Read a keyring password by account name, distinguishing "not set" (returns
/// `None` silently) from a genuine keyring failure (returns `None` but logs it).
/// An empty stored value is treated as not set. Public so host apps read their
/// own namespaced credentials with the same not-set/failure semantics.
pub fn read_keyring(account: &str) -> Option<String> {
    let entry = match keyring_core::Entry::new(keyring_service(), account) {
        Ok(e) => e,
        Err(e) => {
            warn!("keyring entry '{account}' unavailable: {e}");
            return None;
        }
    };
    match entry.get_password() {
        Ok(p) if !p.is_empty() => Some(p),
        Ok(_) => None,
        Err(keyring_core::Error::NoEntry) => None,
        Err(e) => {
            warn!("keyring read '{account}' failed: {e}");
            None
        }
    }
}

#[derive(Clone)]
pub struct KeyService {
    dev_mode: bool,
    library_id: String,
}

impl KeyService {
    pub fn new(dev_mode: bool, library_id: String) -> Self {
        Self {
            dev_mode,
            library_id,
        }
    }

    pub fn is_dev_mode(&self) -> bool {
        self.dev_mode
    }

    /// The library this key service is scoped to. Lets host apps namespace their
    /// own keyring accounts the same way coven does.
    pub fn library_id(&self) -> &str {
        &self.library_id
    }

    /// Build a namespaced account name for keyring entries.
    fn account(&self, base: &str) -> String {
        format!("{}:{}", base, self.library_id)
    }

    /// Read the encryption master key. Returns None if not configured.
    ///
    /// Dev mode: reads `BAE_ENCRYPTION_KEY` env var.
    /// Prod mode: reads from OS keyring (may trigger a system prompt on first access).
    pub fn get_encryption_key(&self) -> Option<String> {
        if self.dev_mode {
            std::env::var("BAE_ENCRYPTION_KEY")
                .ok()
                .filter(|k| !k.is_empty())
        } else {
            read_keyring(&self.account("encryption_master_key"))
        }
    }

    /// Get the encryption key, creating a new one if none exists.
    /// Errors in dev mode (use environment variables instead).
    pub fn get_or_create_encryption_key(&self) -> Result<String, KeyError> {
        if self.dev_mode {
            return self.get_encryption_key().ok_or(KeyError::DevMode);
        }

        if let Some(key) = self.get_encryption_key() {
            return Ok(key);
        }

        let key_hex = hex::encode(crate::encryption::generate_random_key());
        keyring_core::Entry::new(keyring_service(), &self.account("encryption_master_key"))?
            .set_password(&key_hex)?;
        info!("Generated and saved new encryption key to keyring");
        Ok(key_hex)
    }

    /// Save the encryption master key to the OS keyring.
    /// Errors in dev mode (use environment variables instead).
    pub fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        if self.dev_mode {
            return Err(KeyError::DevMode);
        }

        keyring_core::Entry::new(keyring_service(), &self.account("encryption_master_key"))?
            .set_password(value)?;
        info!("Encryption key saved to keyring");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Per-storage-profile S3 credentials
    // -------------------------------------------------------------------------

    /// Read the S3 access key for a storage profile. Returns None if not set.
    ///
    /// Dev mode: reads `BAE_S3_ACCESS_KEY_{profile_id}`, then falls back to `BAE_S3_ACCESS_KEY`.
    /// Prod mode: reads from OS keyring.
    pub fn get_profile_access_key(&self, profile_id: &str) -> Option<String> {
        if self.dev_mode {
            std::env::var(format!("BAE_S3_ACCESS_KEY_{}", profile_id))
                .ok()
                .filter(|k| !k.is_empty())
                .or_else(|| {
                    std::env::var("BAE_S3_ACCESS_KEY")
                        .ok()
                        .filter(|k| !k.is_empty())
                })
        } else {
            let account = self.account(&format!("s3_access_key:{}", profile_id));
            read_keyring(&account)
        }
    }

    /// Save the S3 access key for a storage profile.
    ///
    /// Dev mode: sets the env var (so tests can round-trip without a real keyring).
    /// Prod mode: writes to OS keyring.
    pub fn set_profile_access_key(&self, profile_id: &str, value: &str) -> Result<(), KeyError> {
        if self.dev_mode {
            std::env::set_var(format!("BAE_S3_ACCESS_KEY_{}", profile_id), value);
            return Ok(());
        }

        let account = self.account(&format!("s3_access_key:{}", profile_id));
        keyring_core::Entry::new(keyring_service(), &account)?.set_password(value)?;
        info!("S3 access key saved for profile {}", profile_id);
        Ok(())
    }

    /// Read the S3 secret key for a storage profile. Returns None if not set.
    ///
    /// Dev mode: reads `BAE_S3_SECRET_KEY_{profile_id}`, then falls back to `BAE_S3_SECRET_KEY`.
    /// Prod mode: reads from OS keyring.
    pub fn get_profile_secret_key(&self, profile_id: &str) -> Option<String> {
        if self.dev_mode {
            std::env::var(format!("BAE_S3_SECRET_KEY_{}", profile_id))
                .ok()
                .filter(|k| !k.is_empty())
                .or_else(|| {
                    std::env::var("BAE_S3_SECRET_KEY")
                        .ok()
                        .filter(|k| !k.is_empty())
                })
        } else {
            let account = self.account(&format!("s3_secret_key:{}", profile_id));
            read_keyring(&account)
        }
    }

    /// Save the S3 secret key for a storage profile.
    ///
    /// Dev mode: sets the env var (so tests can round-trip without a real keyring).
    /// Prod mode: writes to OS keyring.
    pub fn set_profile_secret_key(&self, profile_id: &str, value: &str) -> Result<(), KeyError> {
        if self.dev_mode {
            std::env::set_var(format!("BAE_S3_SECRET_KEY_{}", profile_id), value);
            return Ok(());
        }

        let account = self.account(&format!("s3_secret_key:{}", profile_id));
        keyring_core::Entry::new(keyring_service(), &account)?.set_password(value)?;
        info!("S3 secret key saved for profile {}", profile_id);
        Ok(())
    }

    /// Delete S3 credentials for a storage profile from the keyring.
    ///
    /// Dev mode: removes env vars.
    /// Prod mode: deletes from OS keyring. Silently ignores missing entries.
    pub fn delete_profile_credentials(&self, profile_id: &str) -> Result<(), KeyError> {
        if self.dev_mode {
            std::env::remove_var(format!("BAE_S3_ACCESS_KEY_{}", profile_id));
            std::env::remove_var(format!("BAE_S3_SECRET_KEY_{}", profile_id));
            return Ok(());
        }

        for key_type in ["s3_access_key", "s3_secret_key"] {
            let account = self.account(&format!("{}:{}", key_type, profile_id));
            match keyring_core::Entry::new(keyring_service(), &account)?.delete_credential() {
                Ok(()) => info!("Deleted {} for profile {}", key_type, profile_id),
                Err(keyring_core::Error::NoEntry) => {}
                Err(e) => return Err(KeyError::Keyring(e)),
            }
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Cloud home credentials (library-scoped, single entry)
    // -------------------------------------------------------------------------

    /// Read cloud home credentials. Returns `Ok(None)` if not set,
    /// `Err` if the stored value can't be parsed.
    ///
    /// Dev mode: reads `BAE_CLOUD_HOME_CREDENTIALS` env var (JSON).
    /// Prod mode: reads from OS keyring.
    pub fn get_cloud_home_credentials(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        let json = if self.dev_mode {
            std::env::var("BAE_CLOUD_HOME_CREDENTIALS")
                .ok()
                .filter(|k| !k.is_empty())
        } else {
            let account = self.account("cloud_home_credentials");
            read_keyring(&account)
        };

        match json {
            None => Ok(None),
            Some(j) => {
                let creds = serde_json::from_str(&j).map_err(|e| {
                    KeyError::Crypto(format!("malformed cloud home credentials JSON: {e}"))
                })?;
                Ok(Some(creds))
            }
        }
    }

    /// Save cloud home credentials.
    ///
    /// Dev mode: sets the env var.
    /// Prod mode: writes to OS keyring.
    pub fn set_cloud_home_credentials(&self, creds: &CloudHomeCredentials) -> Result<(), KeyError> {
        let json = serde_json::to_string(creds)
            .map_err(|e| KeyError::Crypto(format!("serialize credentials: {e}")))?;

        if self.dev_mode {
            std::env::set_var("BAE_CLOUD_HOME_CREDENTIALS", &json);
            return Ok(());
        }

        let account = self.account("cloud_home_credentials");
        keyring_core::Entry::new(keyring_service(), &account)?.set_password(&json)?;
        info!("Cloud home credentials saved to keyring");
        Ok(())
    }

    /// Delete cloud home credentials.
    ///
    /// Dev mode: removes the env var.
    /// Prod mode: deletes from OS keyring. Silently ignores missing entries.
    pub fn delete_cloud_home_credentials(&self) -> Result<(), KeyError> {
        if self.dev_mode {
            std::env::remove_var("BAE_CLOUD_HOME_CREDENTIALS");
            return Ok(());
        }

        let account = self.account("cloud_home_credentials");
        match keyring_core::Entry::new(keyring_service(), &account)?.delete_credential() {
            Ok(()) => {
                info!("Cloud home credentials deleted from keyring");
                Ok(())
            }
            Err(keyring_core::Error::NoEntry) => Ok(()),
            Err(e) => Err(KeyError::Keyring(e)),
        }
    }

    // -------------------------------------------------------------------------
    // Global user keypair (Ed25519 identity, NOT library-scoped)
    // -------------------------------------------------------------------------

    /// Dev-mode env var names, namespaced by library_id so parallel tests
    /// don't stomp on each other's keypairs.
    fn signing_key_env_var(&self) -> String {
        format!("BAE_USER_SIGNING_KEY_{}", self.library_id)
    }

    fn public_key_env_var(&self) -> String {
        format!("BAE_USER_PUBLIC_KEY_{}", self.library_id)
    }

    /// Load the user's Ed25519 keypair from the keyring. Returns an error if
    /// no keypair exists (unlike `get_or_create_user_keypair` which creates one).
    pub fn get_user_keypair(&self) -> Result<UserKeypair, KeyError> {
        self.get_user_keypair_inner()?
            .ok_or_else(|| KeyError::Crypto("No user keypair found in keyring".to_string()))
    }

    /// Load the user's Ed25519 keypair from the keyring, creating a new one if
    /// none exists. This is a global identity shared across all libraries.
    ///
    /// Dev mode: reads env vars namespaced by library_id (hex).
    /// Falls back to generating and storing in env vars so tests can round-trip.
    pub fn get_or_create_user_keypair(&self) -> Result<UserKeypair, KeyError> {
        if let Some(kp) = self.get_user_keypair_inner()? {
            return Ok(kp);
        }

        let kp = UserKeypair::generate();
        let sk_hex = hex::encode(kp.signing_key);
        let pk_hex = hex::encode(kp.public_key);

        if self.dev_mode {
            std::env::set_var(self.signing_key_env_var(), &sk_hex);
            std::env::set_var(self.public_key_env_var(), &pk_hex);
        } else {
            keyring_core::Entry::new(keyring_service(), "bae_user_signing_key")?.set_password(&sk_hex)?;
            keyring_core::Entry::new(keyring_service(), "bae_user_public_key")?.set_password(&pk_hex)?;
        }

        info!("Generated and saved new user Ed25519 keypair");
        Ok(kp)
    }

    /// Return just the user's Ed25519 public key, or None if no keypair exists.
    pub fn get_user_public_key(&self) -> Option<[u8; SIGN_PUBLICKEYBYTES]> {
        let pk_hex = if self.dev_mode {
            std::env::var(self.public_key_env_var())
                .ok()
                .filter(|k| !k.is_empty())
        } else {
            read_keyring("bae_user_public_key")
        };

        let pk_hex = pk_hex?;
        let pk_bytes = hex::decode(&pk_hex).ok()?;
        pk_bytes.try_into().ok()
    }

    /// Import an Ed25519 keypair from raw bytes (64 bytes: seed + public key).
    /// Overwrites any existing keypair. Used during restore to preserve the
    /// original device's membership identity.
    pub fn import_user_keypair(&self, signing_key_bytes: &[u8]) -> Result<(), KeyError> {
        if signing_key_bytes.len() != 64 {
            return Err(KeyError::Crypto(format!(
                "Signing key must be 64 bytes, got {}",
                signing_key_bytes.len()
            )));
        }

        let sk =
            ed25519_dalek::SigningKey::from_keypair_bytes(signing_key_bytes.try_into().unwrap())
                .map_err(|e| KeyError::Crypto(format!("Invalid keypair bytes: {e}")))?;
        let pk = sk.verifying_key();

        let sk_hex = hex::encode(signing_key_bytes);
        let pk_hex = hex::encode(pk.to_bytes());

        if self.dev_mode {
            std::env::set_var(self.signing_key_env_var(), &sk_hex);
            std::env::set_var(self.public_key_env_var(), &pk_hex);
        } else {
            keyring_core::Entry::new(keyring_service(), "bae_user_signing_key")?.set_password(&sk_hex)?;
            keyring_core::Entry::new(keyring_service(), "bae_user_public_key")?.set_password(&pk_hex)?;
        }

        info!("Imported user Ed25519 keypair");
        Ok(())
    }

    /// Internal: try to load an existing keypair from the keyring.
    fn get_user_keypair_inner(&self) -> Result<Option<UserKeypair>, KeyError> {
        let (sk_hex, pk_hex) = if self.dev_mode {
            let sk = std::env::var(self.signing_key_env_var())
                .ok()
                .filter(|k| !k.is_empty());
            let pk = std::env::var(self.public_key_env_var())
                .ok()
                .filter(|k| !k.is_empty());
            match (sk, pk) {
                (Some(s), Some(p)) => (s, p),
                _ => return Ok(None),
            }
        } else {
            let sk = read_keyring("bae_user_signing_key");
            let pk = read_keyring("bae_user_public_key");
            match (sk, pk) {
                (Some(s), Some(p)) => (s, p),
                _ => return Ok(None),
            }
        };

        let sk_bytes: [u8; SIGN_SECRETKEYBYTES] = hex::decode(&sk_hex)
            .map_err(|e| KeyError::Crypto(format!("Invalid signing key hex: {e}")))?
            .try_into()
            .map_err(|_| KeyError::Crypto("Signing key wrong length".to_string()))?;

        let pk_bytes: [u8; SIGN_PUBLICKEYBYTES] = hex::decode(&pk_hex)
            .map_err(|e| KeyError::Crypto(format!("Invalid public key hex: {e}")))?
            .try_into()
            .map_err(|_| KeyError::Crypto("Public key wrong length".to_string()))?;

        Ok(Some(UserKeypair {
            signing_key: sk_bytes,
            public_key: pk_bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_generation_produces_valid_keys() {
        let kp = UserKeypair::generate();

        // Ed25519 secret key is 64 bytes, public key is 32 bytes
        assert_eq!(kp.signing_key.len(), 64);
        assert_eq!(kp.public_key.len(), 32);

        // Keys should not be all zeros (astronomically unlikely)
        assert!(kp.signing_key.iter().any(|&b| b != 0));
        assert!(kp.public_key.iter().any(|&b| b != 0));
    }

    #[test]
    fn two_keypairs_are_distinct() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();
        assert_ne!(kp1.public_key, kp2.public_key);
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let kp = UserKeypair::generate();
        let message = b"changeset payload";

        let sig = kp.sign(message);
        assert!(verify_signature(&sig, message, &kp.public_key));
    }

    #[test]
    fn verify_rejects_wrong_message() {
        let kp = UserKeypair::generate();
        let sig = kp.sign(b"original");
        assert!(!verify_signature(&sig, b"tampered", &kp.public_key));
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();
        let sig = kp1.sign(b"message");
        assert!(!verify_signature(&sig, b"message", &kp2.public_key));
    }

    #[test]
    fn sign_empty_message() {
        let kp = UserKeypair::generate();
        let sig = kp.sign(b"");
        assert!(verify_signature(&sig, b"", &kp.public_key));
    }

    #[test]
    fn ed25519_to_x25519_conversion() {
        let kp = UserKeypair::generate();
        let x_sk = kp.to_x25519_secret_key();
        let x_pk = kp.to_x25519_public_key();

        // Should produce non-zero 32-byte keys
        assert_eq!(x_sk.len(), 32);
        assert_eq!(x_pk.len(), 32);
        assert!(x_sk.iter().any(|&b| b != 0));
        assert!(x_pk.iter().any(|&b| b != 0));
    }

    #[test]
    fn ed25519_to_x25519_is_deterministic() {
        let kp = UserKeypair::generate();
        let x_sk1 = kp.to_x25519_secret_key();
        let x_sk2 = kp.to_x25519_secret_key();
        assert_eq!(x_sk1, x_sk2);
    }

    #[test]
    fn sealed_box_roundtrip() {
        let kp = UserKeypair::generate();
        let x_pk = kp.to_x25519_public_key();
        let x_sk = kp.to_x25519_secret_key();

        let plaintext = b"library encryption key material";
        let ciphertext = seal_box_encrypt(plaintext, &x_pk);

        assert_eq!(ciphertext.len(), plaintext.len() + SEALBYTES);

        let decrypted = seal_box_decrypt(&ciphertext, &x_pk, &x_sk).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn sealed_box_wrong_key_fails() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();

        let ciphertext = seal_box_encrypt(b"secret", &kp1.to_x25519_public_key());

        let result = seal_box_decrypt(
            &ciphertext,
            &kp2.to_x25519_public_key(),
            &kp2.to_x25519_secret_key(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn sealed_box_empty_message() {
        let kp = UserKeypair::generate();
        let x_pk = kp.to_x25519_public_key();
        let x_sk = kp.to_x25519_secret_key();

        let ciphertext = seal_box_encrypt(b"", &x_pk);
        let decrypted = seal_box_decrypt(&ciphertext, &x_pk, &x_sk).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn sealed_box_too_short_ciphertext() {
        let kp = UserKeypair::generate();
        let result = seal_box_decrypt(
            &[0u8; 10], // shorter than SEALBYTES
            &kp.to_x25519_public_key(),
            &kp.to_x25519_secret_key(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn key_service_user_keypair() {
        let ks = KeyService::new(true, "test-keypair".to_string());
        std::env::remove_var(ks.signing_key_env_var());
        std::env::remove_var(ks.public_key_env_var());

        // No keypair yet
        assert!(ks.get_user_public_key().is_none());

        // Generate and store
        let kp = ks.get_or_create_user_keypair().unwrap();

        // Should be retrievable now
        let pk = ks.get_user_public_key().unwrap();
        assert_eq!(pk, kp.public_key);

        // Calling again returns the same keypair (idempotent)
        let kp2 = ks.get_or_create_user_keypair().unwrap();
        assert_eq!(kp2.public_key, kp.public_key);
        assert_eq!(kp2.signing_key, kp.signing_key);

        // Different library_id gets its own keypair (isolated in dev mode)
        let ks2 = KeyService::new(true, "other-library".to_string());
        assert!(ks2.get_user_public_key().is_none());

        // Stored keypair can sign and verify
        let message = b"test message for signing";
        let sig = kp.sign(message);
        assert!(verify_signature(&sig, message, &kp.public_key));

        // Reloaded keypair produces consistent verification
        let kp3 = ks.get_or_create_user_keypair().unwrap();
        assert!(verify_signature(&sig, message, &kp3.public_key));

        // Import a different keypair and verify it replaces the current one
        let new_kp = UserKeypair::generate();
        ks.import_user_keypair(&new_kp.signing_key).unwrap();

        let loaded = ks.get_user_keypair().unwrap();
        assert_eq!(loaded.public_key, new_kp.public_key);
        assert_eq!(loaded.signing_key, new_kp.signing_key);

        // Imported keypair can sign and verify
        let sig2 = loaded.sign(b"import test");
        assert!(verify_signature(&sig2, b"import test", &loaded.public_key));

        // Import rejects wrong-length bytes
        assert!(ks.import_user_keypair(&[0u8; 32]).is_err());

        // Clean up
        std::env::remove_var(ks.signing_key_env_var());
        std::env::remove_var(ks.public_key_env_var());
    }
}

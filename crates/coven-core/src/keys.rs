use ed25519_dalek::{Signer, SigningKey, Verifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SIGN_PUBLICKEYBYTES: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;
pub const SIGN_SECRETKEYBYTES: usize = ed25519_dalek::KEYPAIR_LENGTH;
pub const SIGN_BYTES: usize = ed25519_dalek::SIGNATURE_LENGTH;
pub const CURVE25519_PUBLICKEYBYTES: usize = crypto_box::KEY_SIZE;
pub const CURVE25519_SECRETKEYBYTES: usize = crypto_box::KEY_SIZE;
pub const SEALBYTES: usize = crypto_box::SEALBYTES;

#[derive(Error, Debug)]
pub enum KeyError {
    #[error("Crypto error: {0}")]
    Crypto(String),
    #[error("Key persistence error: {0}")]
    Persistence(String),
    #[error(
        "no keyring store is installed; the host must install the platform keyring store at startup (set_keyring_service) before any key operation"
    )]
    StoreNotInstalled,
    #[error(
        "no bundled keyring store exists for this target; the host must supply one via keyring_core::set_default_store before registering the keyring service"
    )]
    UnsupportedKeyringPlatform,
    #[error(
        "no keyring service is registered; the host must call set_keyring_service at startup before any key operation"
    )]
    ServiceNotRegistered,
    #[error(
        "no device identity is established; call coven::ensure_device_identity() first (or complete a join/restore, which establish one as part of accepting an invite or recovering a store)"
    )]
    NoDeviceIdentity,
    #[error(
        "device identity is already established under a different key (existing {existing_pubkey_hex}, attempted import {imported_pubkey_hex}); importing a different identity would strand every store that pinned attestations to the existing key"
    )]
    IdentityMismatch {
        existing_pubkey_hex: String,
        imported_pubkey_hex: String,
    },
    #[error("invalid host secret name {name:?}: {reason}")]
    InvalidSecretName { name: String, reason: String },
}

/// Credentials for the cloud home, stored as a single JSON keyring entry.
///
/// `Debug` is hand-written so the S3 `secret_key` and the OAuth `token_json`
/// print as `<redacted>` — `{:?}` in an error path cannot leak them.
#[derive(Clone, Serialize, Deserialize)]
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

impl std::fmt::Debug for CloudHomeCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudHomeCredentials::S3 {
                access_key,
                secret_key: _,
            } => f
                .debug_struct("S3")
                .field("access_key", access_key)
                .field("secret_key", &"<redacted>")
                .finish(),
            CloudHomeCredentials::OAuth { token_json: _ } => f
                .debug_struct("OAuth")
                .field("token_json", &"<redacted>")
                .finish(),
            CloudHomeCredentials::None => f.write_str("None"),
        }
    }
}

/// Ed25519 keypair for signing changesets and membership changes.
/// The same seed can derive an X25519 keypair for key wrapping.
///
/// This is a global identity (not per-store) so attestations accumulate
/// under one pubkey across all stores.
#[derive(Clone)]
pub struct UserKeypair {
    signing_key: SigningKey,
}

impl UserKeypair {
    /// Generate a new random Ed25519 keypair. The unmanaged primitive behind
    /// the native `DeviceKeys::get_or_create_user_keypair`; also lets host code
    /// (and its tests) mint an identity directly.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        Self { signing_key }
    }

    /// Reconstruct a keypair from its 64-byte Ed25519 signing key (seed + public),
    /// deriving the public key from it and validating that the bytes are a real
    /// keypair. The single place stored signing-key bytes become a `UserKeypair` —
    /// the keyring loader and the browser keystore both round-trip through it, so a
    /// torn or corrupt signing key fails the same way wherever it was persisted.
    pub fn from_signing_key_bytes(
        signing_key: &[u8; SIGN_SECRETKEYBYTES],
    ) -> Result<Self, KeyError> {
        let signing_key = ed25519_dalek::SigningKey::from_keypair_bytes(signing_key)
            .map_err(|e| KeyError::Crypto(format!("Invalid signing key bytes: {e}")))?;
        Ok(Self { signing_key })
    }

    pub fn public_key(&self) -> [u8; SIGN_PUBLICKEYBYTES] {
        self.signing_key.verifying_key().to_bytes()
    }

    pub fn to_keypair_bytes(&self) -> [u8; SIGN_SECRETKEYBYTES] {
        self.signing_key.to_keypair_bytes()
    }

    /// Sign a message, returning a 64-byte detached signature.
    pub fn sign(&self, message: &[u8]) -> [u8; SIGN_BYTES] {
        self.signing_key.sign(message).to_bytes()
    }

    /// Derive the X25519 secret key from this Ed25519 signing key.
    pub fn to_x25519_secret_key(&self) -> [u8; CURVE25519_SECRETKEYBYTES] {
        self.signing_key.to_scalar_bytes()
    }

    /// Derive the X25519 public key from this Ed25519 public key.
    pub fn to_x25519_public_key(&self) -> [u8; CURVE25519_PUBLICKEYBYTES] {
        self.signing_key.verifying_key().to_montgomery().to_bytes()
    }
}

/// Hex-encode the public key attached to `keypair`.
pub fn public_key_hex(keypair: &UserKeypair) -> String {
    hex::encode(keypair.public_key())
}

/// Sign `message` and return the hex-encoded public key and detached signature.
pub fn sign_hex(keypair: &UserKeypair, message: &[u8]) -> (String, String) {
    (public_key_hex(keypair), hex::encode(keypair.sign(message)))
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

/// Verify a hex-encoded detached Ed25519 signature (`sig_hex`) over `message`
/// against a hex-encoded public key (`pk_hex`). Malformed hex, a wrong-length key
/// or signature, or a non-matching signature all fail closed (false). The shared
/// hex front-end of [`verify_signature`], used by the changeset envelope and the
/// signed control objects so the decode-and-verify path lives in one place.
pub fn verify_signature_hex(pk_hex: &str, sig_hex: &str, message: &[u8]) -> bool {
    let Ok(pk_bytes) = hex::decode(pk_hex) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let Ok(pk): Result<[u8; SIGN_PUBLICKEYBYTES], _> = pk_bytes.try_into() else {
        return false;
    };
    let Ok(sig): Result<[u8; SIGN_BYTES], _> = sig_bytes.try_into() else {
        return false;
    };
    verify_signature(&sig, message, &pk)
}

/// Encrypt a message to a recipient's X25519 public key using a sealed box.
/// The sender is anonymous -- only the recipient can decrypt.
pub fn seal_box_encrypt(
    message: &[u8],
    recipient_x25519_pk: &[u8; CURVE25519_PUBLICKEYBYTES],
) -> Vec<u8> {
    crypto_box::PublicKey::from(*recipient_x25519_pk)
        .seal(&mut crypto_box::aead::OsRng, message)
        .expect("sealed box encryption should not fail")
}

/// Decrypt a sealed box using the recipient's X25519 secret key.
/// `crypto_box::SecretKey::unseal` derives the recipient public key internally.
pub fn seal_box_decrypt(
    ciphertext: &[u8],
    recipient_x25519_sk: &[u8; CURVE25519_SECRETKEYBYTES],
) -> Result<Vec<u8>, KeyError> {
    crypto_box::SecretKey::from(*recipient_x25519_sk)
        .unseal(ciphertext)
        .map_err(|_| {
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
) -> Result<[u8; CURVE25519_PUBLICKEYBYTES], KeyError> {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(ed25519_pk)
        .map_err(|_| KeyError::Crypto("invalid Ed25519 public key point".to_string()))?;
    Ok(vk.to_montgomery().to_bytes())
}

use crate::encryption::MasterKeyring;

/// A store's master keyring's custody: who unlocks it, where a newly
/// established or rotated one is written, and how it is removed. Implemented
/// once per protection policy (the OS keyring, a passphrase-wrapped file, an
/// in-memory session value, or a host's own).
pub trait MasterKeyCustody: Send + Sync {
    /// The store's master keyring for this session. `Ok(None)` means the store
    /// has never had one established (a fresh store before create/join) —
    /// distinct from a failure to produce one (wrong passphrase, unreadable
    /// backing store), which is `Err`.
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError>;

    /// Protect and store `keyring`, replacing whatever is stored. Serves both
    /// establishment (create/join/restore) and rotation re-protection (member
    /// removal, the per-cycle refresh adoption). Idempotent.
    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError>;

    /// Remove the stored keyring. `Ok` when nothing was stored.
    fn forget(&self) -> Result<(), KeyError>;
}

/// A device's signing identity's custody: who unlocks it, where a newly
/// established one is written, and how it is removed. The signing-key
/// sibling of [`MasterKeyCustody`], same three-method shape, over
/// [`UserKeypair`] instead of a store's master keyring.
///
/// Unlike a store's master key, the signing identity is device-global (one
/// keypair per host, shared by every store — see [`UserKeypair`]'s doc), so
/// its custody is selected once per process rather than once per store.
pub trait DeviceIdentityCustody: Send + Sync {
    /// The device's established signing identity. `Ok(None)` means none has
    /// ever been established — distinct from a failure to produce one (wrong
    /// passphrase, unreadable backing store), which is `Err`.
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError>;

    /// Protect and store `keypair`, replacing whatever is stored. Idempotent.
    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError>;

    /// Remove the stored identity. `Ok` when nothing was stored.
    fn forget(&self) -> Result<(), KeyError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_generation_produces_valid_keys() {
        let kp = UserKeypair::generate();

        assert_eq!(kp.to_keypair_bytes().len(), SIGN_SECRETKEYBYTES);
        assert_eq!(kp.public_key().len(), SIGN_PUBLICKEYBYTES);

        // Keys should not be all zeros (astronomically unlikely)
        assert!(kp.to_keypair_bytes().iter().any(|&b| b != 0));
        assert!(kp.public_key().iter().any(|&b| b != 0));
    }

    #[test]
    fn two_keypairs_are_distinct() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();
        assert_ne!(kp1.public_key(), kp2.public_key());
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let kp = UserKeypair::generate();
        let message = b"changeset payload";

        let sig = kp.sign(message);
        assert!(verify_signature(&sig, message, &kp.public_key()));
    }

    #[test]
    fn keypair_bytes_roundtrip_preserves_signing_identity() {
        let kp = UserKeypair::generate();
        let keypair_bytes = kp.to_keypair_bytes();
        let restored =
            UserKeypair::from_signing_key_bytes(&keypair_bytes).expect("stored keypair bytes");
        let message = b"persisted identity";

        assert_eq!(restored.to_keypair_bytes(), keypair_bytes);
        assert_eq!(restored.public_key(), kp.public_key());
        assert!(verify_signature(
            &restored.sign(message),
            message,
            &restored.public_key()
        ));
    }

    #[test]
    fn sign_hex_returns_public_key_and_valid_signature() {
        let kp = UserKeypair::generate();
        let message = b"changeset payload";

        let (pk_hex, sig_hex) = sign_hex(&kp, message);

        assert_eq!(pk_hex, public_key_hex(&kp));
        assert!(verify_signature_hex(&pk_hex, &sig_hex, message));
    }

    #[test]
    fn verify_rejects_wrong_message() {
        let kp = UserKeypair::generate();
        let sig = kp.sign(b"original");
        assert!(!verify_signature(&sig, b"tampered", &kp.public_key()));
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();
        let sig = kp1.sign(b"message");
        assert!(!verify_signature(&sig, b"message", &kp2.public_key()));
    }

    #[test]
    fn sign_empty_message() {
        let kp = UserKeypair::generate();
        let sig = kp.sign(b"");
        assert!(verify_signature(&sig, b"", &kp.public_key()));
    }

    #[test]
    fn ed25519_to_x25519_conversion() {
        let kp = UserKeypair::generate();
        let x_sk = kp.to_x25519_secret_key();
        let x_pk = kp.to_x25519_public_key();
        let converted = ed25519_to_x25519_public_key(&kp.public_key()).unwrap();

        // Should produce non-zero 32-byte keys
        assert_eq!(x_sk.len(), 32);
        assert_eq!(x_pk.len(), 32);
        assert!(x_sk.iter().any(|&b| b != 0));
        assert!(x_pk.iter().any(|&b| b != 0));
        assert_eq!(converted, x_pk);
    }

    #[test]
    fn ed25519_to_x25519_rejects_off_curve_bytes() {
        let mut bytes = [0u8; SIGN_PUBLICKEYBYTES];
        bytes[0] = 2;

        let error = ed25519_to_x25519_public_key(&bytes).expect_err("invalid point fails");

        assert!(matches!(error, KeyError::Crypto(_)));
        assert!(error
            .to_string()
            .contains("invalid Ed25519 public key point"));
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

        let plaintext = b"store encryption key material";
        let ciphertext = seal_box_encrypt(plaintext, &x_pk);

        assert_eq!(ciphertext.len(), plaintext.len() + SEALBYTES);

        let decrypted = seal_box_decrypt(&ciphertext, &x_sk).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn sealed_box_wrong_key_fails() {
        let kp1 = UserKeypair::generate();
        let kp2 = UserKeypair::generate();

        let ciphertext = seal_box_encrypt(b"secret", &kp1.to_x25519_public_key());

        let result = seal_box_decrypt(&ciphertext, &kp2.to_x25519_secret_key());
        assert!(result.is_err());
    }

    #[test]
    fn sealed_box_empty_message() {
        let kp = UserKeypair::generate();
        let x_pk = kp.to_x25519_public_key();
        let x_sk = kp.to_x25519_secret_key();

        let ciphertext = seal_box_encrypt(b"", &x_pk);
        let decrypted = seal_box_decrypt(&ciphertext, &x_sk).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn sealed_box_too_short_ciphertext() {
        let kp = UserKeypair::generate();
        let result = seal_box_decrypt(&[0u8; 10], &kp.to_x25519_secret_key());
        assert!(result.is_err());
    }

    #[test]
    fn credentials_debug_redacts_s3_secret_and_oauth_token() {
        let s3 = CloudHomeCredentials::S3 {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "s3-secret-value-do-not-print".to_string(),
        };
        let debug = format!("{s3:?}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(debug.contains("AKIAIOSFODNN7EXAMPLE"), "{debug}");
        assert!(
            !debug.contains("s3-secret-value-do-not-print"),
            "S3 secret key leaked: {debug}"
        );

        let oauth = CloudHomeCredentials::OAuth {
            token_json: "{\"access_token\":\"oauth-token-do-not-print\"}".to_string(),
        };
        let debug = format!("{oauth:?}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(
            !debug.contains("oauth-token-do-not-print"),
            "OAuth token leaked: {debug}"
        );
    }
}

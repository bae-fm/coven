use ed25519_dalek::{Signer, SigningKey, Verifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) const SIGN_PUBLICKEYBYTES: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;
pub(crate) const SIGN_SECRETKEYBYTES: usize = ed25519_dalek::KEYPAIR_LENGTH;
pub(crate) const SIGN_BYTES: usize = ed25519_dalek::SIGNATURE_LENGTH;
pub(crate) const CURVE25519_PUBLICKEYBYTES: usize = crypto_box::KEY_SIZE;
pub(crate) const CURVE25519_SECRETKEYBYTES: usize = crypto_box::KEY_SIZE;
#[cfg(test)]
pub(crate) const SEALBYTES: usize = crypto_box::SEALBYTES;

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
        "no identity is established for this store; create, join, or restore the store first — each establishes this store's identity as part of what it does"
    )]
    NoDeviceIdentity,
    #[error(
        "this store's identity is already established under a different key (existing {existing_pubkey_hex}, attempted import {imported_pubkey_hex}); importing a different identity would strand this store's membership entries"
    )]
    IdentityMismatch {
        existing_pubkey_hex: String,
        imported_pubkey_hex: String,
    },
    #[error(
        "no pending identity is held for join request {request_public_key_hex}; the request may have already completed, been abandoned, or never existed"
    )]
    NoPendingIdentity { request_public_key_hex: String },
    #[error("invalid host secret name {name:?}: {reason}")]
    InvalidSecretName { name: String, reason: String },
    /// The OS refused a Keychain data-protection-store operation with
    /// `errSecMissingEntitlement` (OSStatus -34018). This is not "the binary
    /// isn't signed" — an ad-hoc or Development-signed binary with no
    /// `keychain-access-groups` entitlement at all also gets -34018, and a
    /// signed binary that *does* carry that entitlement with no provisioning
    /// profile behind it is killed by the kernel at launch instead. The fix is
    /// a team-prefixed `keychain-access-groups` entitlement backed by an
    /// embedded provisioning profile — in Xcode, set `DEVELOPMENT_TEAM` so
    /// automatic signing fetches and embeds one. A build with no team must
    /// omit the entitlement entirely, which means it also has no access to
    /// the data-protection keychain and will hit this error on first use.
    #[error(
        "the OS refused this keychain operation with errSecMissingEntitlement \
         (OSStatus -34018): the process has no team-prefixed keychain-access-groups \
         entitlement backed by an embedded provisioning profile; set DEVELOPMENT_TEAM \
         so Xcode's automatic signing fetches and embeds one (a keychain-access-groups \
         entitlement present WITHOUT a provisioning profile is a different failure: the \
         process is killed by the kernel at launch, not this error) — a build with no \
         team must omit the entitlement and will hit this same error on first key use"
    )]
    MissingKeychainEntitlement,
}

/// Credentials for the cloud home, stored as a single JSON keyring entry.
///
/// `Debug` is hand-written so the S3 `secret_key` and the OAuth tokens
/// print as `<redacted>` — `{:?}` in an error path cannot leak them.
#[derive(Clone, Serialize, Deserialize)]
pub enum CloudHomeCredentials {
    /// S3-compatible providers: access key + secret key.
    S3 {
        access_key: String,
        secret_key: String,
    },
    /// Consumer cloud providers (Google Drive, Dropbox, OneDrive).
    OAuth { tokens: crate::oauth::OAuthTokens },
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
            CloudHomeCredentials::OAuth { tokens: _ } => f
                .debug_struct("OAuth")
                .field("tokens", &"<redacted>")
                .finish(),
            CloudHomeCredentials::None => f.write_str("None"),
        }
    }
}

/// Ed25519 keypair for signing changesets and membership changes.
/// The same seed can derive an X25519 keypair for key wrapping.
///
/// One keypair is generated per (store, device) pair: a device holds a
/// distinct identity in each store it belongs to, so a key scoped to one
/// store carries no authority in another, and the same device's pubkey does
/// not appear in more than one store's membership chain.
#[derive(Clone)]
pub struct UserKeypair {
    signing_key: SigningKey,
}

impl UserKeypair {
    /// Generate a new random Ed25519 keypair. The unmanaged primitive behind
    /// every identity-establishing act — creating, joining, or restoring a
    /// store; also lets host code (and its tests) mint an identity directly.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        Self { signing_key }
    }

    /// Reconstruct a keypair from its 64-byte Ed25519 signing key (seed + public),
    /// deriving the public key from it and validating that the bytes are a real
    /// keypair. This is the single place stored signing-key bytes become a
    /// `UserKeypair`, so a torn or corrupt signing key fails at the persistence
    /// boundary.
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

    pub(crate) fn derive_signing_key(&self, domain: &[u8], context: &[u8]) -> Self {
        use sha2::{Digest, Sha256};

        let mut derivation = Sha256::new();
        derivation.update(domain);
        derivation.update(self.signing_key.to_bytes());
        derivation.update(context);
        let seed: [u8; 32] = derivation.finalize().into();
        Self {
            signing_key: SigningKey::from_bytes(&seed),
        }
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
pub(crate) fn public_key_hex(keypair: &UserKeypair) -> String {
    hex::encode(keypair.public_key())
}

/// Sign `message` and return the hex-encoded public key and detached signature.
pub(crate) fn sign_hex(keypair: &UserKeypair, message: &[u8]) -> (String, String) {
    (public_key_hex(keypair), hex::encode(keypair.sign(message)))
}

/// Verify a detached Ed25519 signature against a public key.
pub(crate) fn verify_signature(
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
/// hex front-end of [`verify_signature`], used by signed Store objects and
/// membership entries so the decode-and-verify path lives in one place.
pub(crate) fn verify_signature_hex(pk_hex: &str, sig_hex: &str, message: &[u8]) -> bool {
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
pub(crate) fn seal_box_encrypt(
    message: &[u8],
    recipient_x25519_pk: &[u8; CURVE25519_PUBLICKEYBYTES],
) -> Vec<u8> {
    crypto_box::PublicKey::from(*recipient_x25519_pk)
        .seal(&mut crypto_box::aead::OsRng, message)
        .expect("sealed box encryption should not fail")
}

/// Decrypt a sealed box using the recipient's X25519 secret key.
/// `crypto_box::SecretKey::unseal` derives the recipient public key internally.
pub(crate) fn seal_box_decrypt(
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
pub(crate) fn ed25519_to_x25519_public_key(
    ed25519_pk: &[u8; SIGN_PUBLICKEYBYTES],
) -> Result<[u8; CURVE25519_PUBLICKEYBYTES], KeyError> {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(ed25519_pk)
        .map_err(|_| KeyError::Crypto("invalid Ed25519 public key point".to_string()))?;
    if vk.is_weak() {
        return Err(KeyError::Crypto(
            "weak Ed25519 public key point cannot identify a recipient".to_string(),
        ));
    }
    Ok(vk.to_montgomery().to_bytes())
}

pub(crate) fn ed25519_hex_to_x25519_public_key(
    ed25519_pubkey_hex: &str,
) -> Result<[u8; CURVE25519_PUBLICKEYBYTES], KeyError> {
    let public_key: [u8; SIGN_PUBLICKEYBYTES] = hex::decode(ed25519_pubkey_hex)
        .map_err(|error| KeyError::Crypto(format!("invalid public-key hex: {error}")))?
        .try_into()
        .map_err(|_| KeyError::Crypto("public key has the wrong length".to_string()))?;
    ed25519_to_x25519_public_key(&public_key)
}

/// Derive an X25519 shared secret after rejecting public inputs that cannot
/// identify a peer. Low-order public keys produce the all-zero shared secret;
/// that result is never usable as recipient identity material.
pub(crate) fn x25519_shared_secret(
    local_secret: [u8; CURVE25519_SECRETKEYBYTES],
    peer_public: [u8; CURVE25519_PUBLICKEYBYTES],
) -> Result<[u8; CURVE25519_PUBLICKEYBYTES], KeyError> {
    if peer_public == [0; CURVE25519_PUBLICKEYBYTES] {
        return Err(KeyError::Crypto(
            "all-zero X25519 public key cannot identify a recipient".to_string(),
        ));
    }
    let shared = x25519_dalek::x25519(local_secret, peer_public);
    if shared == [0; CURVE25519_PUBLICKEYBYTES] {
        return Err(KeyError::Crypto(
            "all-zero X25519 shared secret cannot identify a recipient".to_string(),
        ));
    }
    Ok(shared)
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

/// A device's signing identity's custody FOR ONE STORE: who unlocks it,
/// where a newly established one is written, and how it is removed. The
/// signing-key sibling of [`MasterKeyCustody`], same three-method shape and
/// the same per-store selection, over [`UserKeypair`] instead of a store's
/// master keyring.
pub trait DeviceIdentityCustody: Send + Sync {
    /// This store's established signing identity. `Ok(None)` means none has
    /// ever been established — distinct from a failure to produce one (wrong
    /// passphrase, unreadable backing store), which is `Err`.
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError>;

    /// Protect and store `keypair`, replacing whatever is stored. Idempotent.
    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError>;

    /// Establish this Store's identity without replacing a different identity.
    /// Repeating the same identity is idempotent.
    fn establish(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        if let Some(existing) = self.unlock()? {
            if existing.public_key() != keypair.public_key() {
                return Err(KeyError::IdentityMismatch {
                    existing_pubkey_hex: public_key_hex(&existing),
                    imported_pubkey_hex: public_key_hex(keypair),
                });
            }
        }
        self.persist(keypair)?;
        tracing::info!("Established this store's Ed25519 signing identity");
        Ok(())
    }

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
    fn ed25519_to_x25519_rejects_the_identity_point() {
        let mut identity = [0; SIGN_PUBLICKEYBYTES];
        identity[0] = 1;

        let error = ed25519_to_x25519_public_key(&identity)
            .expect_err("a weak recipient point must not produce a shared key");

        assert!(matches!(error, KeyError::Crypto(_)));
    }

    #[test]
    fn x25519_shared_secret_rejects_the_all_zero_public_key() {
        let local = UserKeypair::generate();

        let error =
            x25519_shared_secret(local.to_x25519_secret_key(), [0; CURVE25519_PUBLICKEYBYTES])
                .expect_err("an all-zero public key must not produce recipient identity material");

        assert!(matches!(error, KeyError::Crypto(_)));
    }

    #[test]
    fn x25519_shared_secret_rejects_a_nonzero_low_order_public_key() {
        let local = UserKeypair::generate();
        let mut low_order = [0; CURVE25519_PUBLICKEYBYTES];
        low_order[0] = 1;

        let error = x25519_shared_secret(local.to_x25519_secret_key(), low_order)
            .expect_err("a low-order public key must not produce recipient identity material");

        assert!(matches!(error, KeyError::Crypto(_)));
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

    /// Pins the actionable content of `MissingKeychainEntitlement`'s message:
    /// the real OS error and the real fix (a team-prefixed
    /// `keychain-access-groups` entitlement backed by a provisioning
    /// profile), not the wrong "must be signed" advice this replaced.
    #[test]
    fn missing_keychain_entitlement_message_names_the_real_error_and_fix() {
        let message = KeyError::MissingKeychainEntitlement.to_string();

        assert!(message.contains("-34018"), "{message}");
        assert!(message.contains("errSecMissingEntitlement"), "{message}");
        assert!(message.contains("keychain-access-groups"), "{message}");
        assert!(message.contains("provisioning profile"), "{message}");
        assert!(message.contains("DEVELOPMENT_TEAM"), "{message}");
        assert!(
            !message.contains("must be signed"),
            "a bare 'signed binary' is the wrong fix and must not be implied: {message}"
        );
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
            tokens: crate::oauth::OAuthTokens {
                access_token: "oauth-token-do-not-print".to_string(),
                refresh_token: None,
                expires_at: None,
            },
        };
        let debug = format!("{oauth:?}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(
            !debug.contains("oauth-token-do-not-print"),
            "OAuth token leaked: {debug}"
        );
    }
}

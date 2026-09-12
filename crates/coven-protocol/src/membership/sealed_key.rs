//! The Store keyring as a member reads it: a sealed box carried inside the
//! Owner-signed membership entry that grants or rotates that member's access.

use serde::{Deserialize, Serialize};

use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{self, IdentityKeyAuthority};

/// Store keyring bytes sealed to one member's X25519 key, carried inside the
/// Owner-signed membership entry that grants or rotates that member's access.
/// The entry signature authenticates it; the sealed box itself names no sender.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedStoreKey {
    /// Hex-encoded sealed box (`seal_box_encrypt` output) of the keyring payload.
    pub sealed: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SealedStoreKeyError {
    #[error("sealed Store key is not valid hex")]
    MalformedSealed,
    #[error("decrypt sealed Store keyring: {0}")]
    Decryption(#[from] coven_keys::keys::KeyError),
    #[error("decode Store keyring payload: {0}")]
    Payload(#[from] coven_keys::encryption::EncryptionError),
    #[error(
        "membership entry expects keyring generation {expected}, but the sealed keyring declares {payload}"
    )]
    GenerationMismatch { expected: u64, payload: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum SealedStoreKeySealError {
    #[error("recipient public key: {0}")]
    Recipient(#[from] coven_keys::keys::KeyError),
    #[error("serialize Store keyring: {0}")]
    Payload(#[from] coven_keys::encryption::EncryptionError),
}

impl SealedStoreKey {
    /// Seal `encryption`'s keyring payload to the member whose Ed25519 public
    /// key is `recipient_pubkey_hex`.
    pub fn seal(
        recipient_pubkey_hex: &str,
        encryption: &EncryptionService,
    ) -> Result<Self, SealedStoreKeySealError> {
        let recipient = keys::ed25519_hex_to_x25519_public_key(recipient_pubkey_hex)?;
        let payload = encryption.to_keyring_payload()?;
        Ok(Self {
            sealed: hex::encode(keys::seal_box_encrypt(&payload, &recipient)),
        })
    }

    /// Structural check used by chain validation: non-empty, valid hex.
    pub fn validate(&self) -> Result<(), SealedStoreKeyError> {
        self.sealed_bytes().map(|_| ())
    }

    /// Open for `recipient` and require the decrypted keyring's current
    /// generation to equal `expected_generation` (derived from the entry's
    /// position in membership history, never from the box).
    pub fn open(
        &self,
        recipient: &dyn IdentityKeyAuthority,
        expected_generation: u64,
    ) -> Result<EncryptionService, SealedStoreKeyError> {
        let sealed = self.sealed_bytes()?;
        let plaintext = keys::seal_box_decrypt(&sealed, &recipient.to_x25519_secret_key())?;
        let keyring = EncryptionService::from_keyring_payload(plaintext)?;
        if keyring.current_generation() != expected_generation {
            return Err(SealedStoreKeyError::GenerationMismatch {
                expected: expected_generation,
                payload: keyring.current_generation(),
            });
        }
        Ok(keyring)
    }

    fn sealed_bytes(&self) -> Result<Vec<u8>, SealedStoreKeyError> {
        if self.sealed.is_empty() {
            return Err(SealedStoreKeyError::MalformedSealed);
        }
        hex::decode(&self.sealed).map_err(|_| SealedStoreKeyError::MalformedSealed)
    }
}

/// A structurally valid sealed key that never opens: the hex of `label`.
#[cfg(any(test, feature = "test-utils"))]
pub fn test_sealed_store_key(label: &[u8]) -> SealedStoreKey {
    SealedStoreKey {
        sealed: hex::encode(label),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_keys::keys::UserKeypair;

    #[test]
    fn a_sealed_keyring_round_trips_to_its_recipient() {
        let recipient = UserKeypair::generate();
        let keyring = EncryptionService::from_key([7; 32]);
        let sealed =
            SealedStoreKey::seal(&keys::public_key_hex(&recipient), &keyring).expect("seal");

        let opened = sealed
            .open(&recipient, keyring.current_generation())
            .expect("open");
        assert_eq!(opened.key_bytes(), keyring.key_bytes());
        assert_eq!(opened.current_generation(), keyring.current_generation());
    }

    #[test]
    fn another_identity_cannot_open_a_sealed_keyring() {
        let recipient = UserKeypair::generate();
        let other = UserKeypair::generate();
        let keyring = EncryptionService::from_key([9; 32]);
        let sealed =
            SealedStoreKey::seal(&keys::public_key_hex(&recipient), &keyring).expect("seal");

        assert!(matches!(
            sealed.open(&other, keyring.current_generation()),
            Err(SealedStoreKeyError::Decryption(_)),
        ));
    }

    #[test]
    fn a_malformed_sealed_box_is_refused_before_decryption() {
        let recipient = UserKeypair::generate();
        let malformed = SealedStoreKey {
            sealed: "not-hex!!".to_string(),
        };

        assert!(matches!(
            malformed.validate(),
            Err(SealedStoreKeyError::MalformedSealed),
        ));
        assert!(matches!(
            malformed.open(&recipient, 1),
            Err(SealedStoreKeyError::MalformedSealed),
        ));
        assert!(matches!(
            SealedStoreKey {
                sealed: String::new()
            }
            .validate(),
            Err(SealedStoreKeyError::MalformedSealed),
        ));
    }

    #[test]
    fn the_entry_generation_must_match_the_sealed_keyring() {
        let recipient = UserKeypair::generate();
        let keyring = EncryptionService::from_key([3; 32]);
        let sealed =
            SealedStoreKey::seal(&keys::public_key_hex(&recipient), &keyring).expect("seal");

        assert!(matches!(
            sealed.open(&recipient, 2),
            Err(SealedStoreKeyError::GenerationMismatch {
                expected: 2,
                payload: 1,
            }),
        ));
    }
}

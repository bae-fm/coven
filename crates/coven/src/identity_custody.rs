//! Device-identity custody: where the device's signing keypair is unlocked
//! from, where a newly established one is written, and how it is removed.
//! [`IdentityCustody`] is the policy a host selects at process startup via
//! [`crate::set_identity_custody`]; [`IdentityCustody::resolve`] turns it
//! into the [`DeviceIdentityCustody`] trait object [`DeviceKeys`](crate::keys::DeviceKeys)
//! drives.
//!
//! Unlike [`crate::custody::KeyCustody`] (selected once per store, on the
//! builder), this is selected once per process — the signing identity is
//! device-global, shared by every store, so its custody is process-wide
//! state by the same design.

use std::path::PathBuf;
use std::sync::Arc;

use crate::envelope::PassphraseVault;
use crate::keys::{DeviceIdentityCustody, KeyError, KeyringSlot, UserKeypair, SIGN_SECRETKEYBYTES};

pub(crate) use crate::envelope::Passphrase;

/// How the device's signing identity is protected. Registered once via
/// [`crate::set_identity_custody`]; unregistered defaults to
/// [`IdentityCustody::Keyring`] — today's behavior, same fixed account,
/// byte-for-byte.
pub enum IdentityCustody {
    /// The OS keyring, device-global (no `store_id`) — the default.
    Keyring,
    /// Argon2id over a memorized passphrase wraps the keypair; the wrapped
    /// blob lives in a file at `path`. Identity is device-global, so `path`
    /// is a host-chosen location at the host's own data root — never inside
    /// a per-store directory.
    Passphrase {
        path: PathBuf,
        passphrase: Passphrase,
    },
    /// A host-supplied custody implementation.
    Custom(Arc<dyn DeviceIdentityCustody>),
}

impl IdentityCustody {
    pub(crate) fn resolve(self) -> Arc<dyn DeviceIdentityCustody> {
        match self {
            IdentityCustody::Keyring => Arc::new(KeyringIdentityCustody),
            IdentityCustody::Passphrase { path, passphrase } => {
                Arc::new(PassphraseIdentityCustody::new(passphrase, path))
            }
            IdentityCustody::Custom(custody) => custody,
        }
    }
}

// =============================================================================
// Keyring preset
// =============================================================================

/// The OS keyring, wrapping the fixed [`KeyringSlot::DeviceSigningKey`]
/// account verbatim (`keyring_account_names_are_a_stable_storage_contract`
/// pins the account name), so an already-stored device identity is found
/// unchanged.
struct KeyringIdentityCustody;

impl DeviceIdentityCustody for KeyringIdentityCustody {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        let Some(sk_hex) = crate::keys::read(&KeyringSlot::DeviceSigningKey)? else {
            return Ok(None);
        };
        let signing_key: [u8; SIGN_SECRETKEYBYTES] = hex::decode(&sk_hex)
            .map_err(|e| KeyError::Crypto(format!("Invalid signing key hex: {e}")))?
            .try_into()
            .map_err(|_| KeyError::Crypto("Signing key wrong length".to_string()))?;
        Ok(Some(UserKeypair::from_signing_key_bytes(&signing_key)?))
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        crate::keys::write(
            &KeyringSlot::DeviceSigningKey,
            &hex::encode(keypair.to_keypair_bytes()),
        )
    }

    fn forget(&self) -> Result<(), KeyError> {
        crate::keys::delete(&KeyringSlot::DeviceSigningKey).map(|_| ())
    }
}

// =============================================================================
// Passphrase preset
// =============================================================================

/// Argon2id over a [`Passphrase`] wraps the device's raw 64-byte signing
/// keypair, via the shared [`PassphraseVault`] — the same envelope format
/// [`crate::custody::KeyCustody::Passphrase`] uses for a store's master
/// keyring, parameterized here by a different payload.
struct PassphraseIdentityCustody {
    vault: PassphraseVault,
}

impl PassphraseIdentityCustody {
    fn new(passphrase: Passphrase, path: PathBuf) -> Self {
        Self {
            vault: PassphraseVault::new(passphrase, path),
        }
    }
}

impl DeviceIdentityCustody for PassphraseIdentityCustody {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        let Some(plaintext) = self.vault.unlock()? else {
            return Ok(None);
        };
        let len = plaintext.len();
        let signing_key: [u8; SIGN_SECRETKEYBYTES] = plaintext.try_into().map_err(|_| {
            KeyError::Crypto(format!(
                "decrypted device identity is {len} bytes, expected {SIGN_SECRETKEYBYTES}"
            ))
        })?;
        Ok(Some(UserKeypair::from_signing_key_bytes(&signing_key)?))
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        self.vault.persist(&keypair.to_keypair_bytes())
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.vault.forget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::test_keyring;

    // =========================================================================
    // Keyring preset
    // =========================================================================

    #[test]
    fn keyring_preset_unlock_persist_forget_round_trip() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
        crate::keys::delete(&KeyringSlot::DeviceSigningKey).expect("clear any prior test's key");

        let custody = KeyringIdentityCustody;
        assert!(
            custody.unlock().expect("unlock a fresh device").is_none(),
            "a fresh device has no established identity",
        );

        let keypair = UserKeypair::generate();
        custody.persist(&keypair).expect("persist");
        let unlocked = custody
            .unlock()
            .expect("unlock after persist")
            .expect("identity is established");
        assert_eq!(unlocked.public_key(), keypair.public_key());

        custody.forget().expect("forget");
        assert!(
            custody.unlock().expect("unlock after forget").is_none(),
            "forget removes the established identity",
        );
    }

    // =========================================================================
    // Passphrase preset
    // =========================================================================

    fn temp_path() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("identity.envelope");
        (tmp, path)
    }

    #[test]
    fn passphrase_preset_establish_then_unlock_round_trips() {
        let (_tmp, path) = temp_path();
        let custody = PassphraseIdentityCustody::new(
            Passphrase::new("correct horse battery staple".to_string()),
            path,
        );

        assert!(custody.unlock().expect("unlock before establish").is_none());

        let keypair = UserKeypair::generate();
        custody.persist(&keypair).expect("establish");
        let unlocked = custody
            .unlock()
            .expect("unlock after establish")
            .expect("identity is established");
        assert_eq!(unlocked.public_key(), keypair.public_key());
    }

    #[test]
    fn passphrase_preset_wrong_passphrase_is_err_not_none() {
        let (_tmp, path) = temp_path();
        let writer = PassphraseIdentityCustody::new(
            Passphrase::new("right passphrase".to_string()),
            path.clone(),
        );
        writer.persist(&UserKeypair::generate()).expect("establish");

        let reader =
            PassphraseIdentityCustody::new(Passphrase::new("wrong passphrase".to_string()), path);
        match reader.unlock() {
            Err(error) => assert!(matches!(error, KeyError::Crypto(_)), "got {error:?}"),
            Ok(_) => panic!("wrong passphrase must not unlock"),
        }
    }

    #[test]
    fn passphrase_preset_lives_at_the_exact_host_chosen_path_not_a_store_dir() {
        let (tmp, path) = temp_path();
        let custody =
            PassphraseIdentityCustody::new(Passphrase::new("unused".to_string()), path.clone());
        custody.persist(&UserKeypair::generate()).expect("persist");

        assert!(
            path.exists(),
            "the envelope is written at the exact host-chosen path"
        );
        let siblings: Vec<_> = std::fs::read_dir(tmp.path())
            .expect("read temp dir")
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            siblings,
            vec![path.file_name().unwrap().to_owned()],
            "no other file is written beside the host-chosen path",
        );
    }

    /// A literal v1 envelope, independently captured, pinned here to prove
    /// the identity preset reads the exact same wire format `custody.rs`'s
    /// master-key preset does — one shared envelope implementation, not two
    /// that happen to agree today. Wraps the 64-byte keypair built from
    /// Ed25519 seed `[0x11u8; 32]` under passphrase "fixture-passphrase"; a
    /// future change to the derivation, AEAD, or serialization code that
    /// diverges between the two payload types is caught here as a failing
    /// test.
    const V1_FIXTURE_PASSPHRASE: &str = "fixture-passphrase";
    const V1_FIXTURE_ENVELOPE_JSON: &str = concat!(
        r#"{"v":1,"kdf":{"algo":"argon2id","m_cost":65536,"t_cost":3,"p_cost":4,"#,
        r#""salt_b64":"yfYYT3S+eUdDHpvRkRJZZg=="},"#,
        r#""nonce_b64":"+pRYN/2QyizRpYZrpG++Y9fU7R7POwp6","#,
        r#""ciphertext_b64":"IrHxxF+oOCv4n80oKVo2VAjPA7m1rbX654FW8u4kt+0FIhqhpotFOke8JL2E8TuKuXperOtbHOtxluSb6LBGtYISbxc3RMnTot98mFXdX8A="}"#
    );

    #[test]
    fn passphrase_preset_envelope_fixture_v1_unlocks() {
        let (_tmp, path) = temp_path();
        std::fs::write(&path, V1_FIXTURE_ENVELOPE_JSON).expect("write fixture envelope");

        let custody = PassphraseIdentityCustody::new(
            Passphrase::new(V1_FIXTURE_PASSPHRASE.to_string()),
            path,
        );
        let keypair = custody
            .unlock()
            .expect("the v1 fixture must still unlock")
            .expect("the fixture names an established identity");

        let expected = UserKeypair::from_signing_key_bytes(
            &ed25519_dalek::SigningKey::from_bytes(&[0x11u8; 32]).to_keypair_bytes(),
        )
        .expect("build the expected keypair from the fixture's seed");
        assert_eq!(keypair.public_key(), expected.public_key());
    }
}

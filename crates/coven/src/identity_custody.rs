//! A store's device-identity custody: where its signing keypair is unlocked
//! from, where a newly established one is written, and how it is removed.
//! [`IdentityCustody`] is the policy a host selects on the builder, next to
//! [`crate::custody::KeyCustody`]; [`IdentityCustody::resolve`] turns it into
//! the [`DeviceIdentityCustody`] trait object the identity-establishing call
//! sites (create, join, restore) drive.

use std::sync::{Arc, RwLock};

use crate::envelope::PassphraseVault;
use crate::keys::{DeviceIdentityCustody, KeyError, KeyringSlot, UserKeypair, SIGN_SECRETKEYBYTES};
use crate::store_dir::StoreDir;

pub(crate) use crate::envelope::Passphrase;

/// How a store's device-signing identity is protected. Selected on the
/// builder, resolved once per store — the identity sibling of
/// [`crate::custody::KeyCustody`], same shape.
pub enum IdentityCustody {
    /// The OS keyring — the default, byte-for-byte today's behavior.
    Keyring,
    /// Argon2id over a memorized passphrase wraps the keypair; the wrapped
    /// blob lives in a file in the store directory.
    Passphrase(Passphrase),
    /// Supplied for this session, never persisted by coven.
    InMemory(UserKeypair),
    /// A host-supplied custody implementation.
    Custom(Arc<dyn DeviceIdentityCustody>),
}

impl IdentityCustody {
    /// Resolve the selected policy into the trait object the identity-
    /// establishing call sites drive, injecting what each preset needs from
    /// the store's identity: `store_id` for [`IdentityCustody::Keyring`] (the
    /// keyring account name), `store_dir` for [`IdentityCustody::Passphrase`]
    /// (the wrapped-file path).
    ///
    /// Public to match [`KeyCustody::resolve`](crate::KeyCustody::resolve): the
    /// low-level [`restore_from_cloud`](crate::restore_from_cloud) takes the
    /// already-resolved `Arc<dyn DeviceIdentityCustody>`, so a host restoring by
    /// a directly-supplied key (no restore code) must be able to resolve a preset
    /// itself, the same way it already resolves its `KeyCustody`.
    pub fn resolve(self, store_id: &str, store_dir: &StoreDir) -> Arc<dyn DeviceIdentityCustody> {
        match self {
            IdentityCustody::Keyring => Arc::new(KeyringIdentityCustody::new(store_id.to_string())),
            IdentityCustody::Passphrase(passphrase) => {
                Arc::new(PassphraseIdentityCustody::new(passphrase, store_dir))
            }
            IdentityCustody::InMemory(keypair) => Arc::new(InMemoryIdentityCustody::new(keypair)),
            IdentityCustody::Custom(custody) => custody,
        }
    }
}

// =============================================================================
// Keyring preset
// =============================================================================

/// The OS keyring, wrapping this store's [`KeyringSlot::DeviceSigningKey`]
/// account verbatim (`keyring_account_names_are_a_stable_storage_contract`
/// pins the account name), so an already-stored identity is found unchanged.
struct KeyringIdentityCustody {
    store_id: String,
}

impl KeyringIdentityCustody {
    fn new(store_id: String) -> Self {
        Self { store_id }
    }
}

impl DeviceIdentityCustody for KeyringIdentityCustody {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        let slot = KeyringSlot::DeviceSigningKey(self.store_id.clone());
        let Some(sk_hex) = crate::keys::read(&slot)? else {
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
            &KeyringSlot::DeviceSigningKey(self.store_id.clone()),
            &hex::encode(keypair.to_keypair_bytes()),
        )
    }

    fn forget(&self) -> Result<(), KeyError> {
        crate::keys::delete(&KeyringSlot::DeviceSigningKey(self.store_id.clone())).map(|_| ())
    }
}

// =============================================================================
// InMemory preset
// =============================================================================

/// Supplied for this session, never persisted by coven — the identity
/// sibling of [`crate::custody::KeyCustody::InMemory`].
struct InMemoryIdentityCustody {
    keypair: RwLock<Option<UserKeypair>>,
}

impl InMemoryIdentityCustody {
    fn new(seed: UserKeypair) -> Self {
        Self {
            keypair: RwLock::new(Some(seed)),
        }
    }
}

impl DeviceIdentityCustody for InMemoryIdentityCustody {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        Ok(self.keypair.read().unwrap().clone())
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        *self.keypair.write().unwrap() = Some(keypair.clone());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.keypair.write().unwrap() = None;
        Ok(())
    }
}

// =============================================================================
// Passphrase preset
// =============================================================================

/// Argon2id over a [`Passphrase`] wraps this store's raw 64-byte signing
/// keypair, via the shared [`PassphraseVault`] — the same envelope format
/// [`crate::custody::KeyCustody::Passphrase`] uses for the master keyring,
/// parameterized here by a different payload and a different file name
/// (`identity.envelope`) in the same store directory.
struct PassphraseIdentityCustody {
    vault: PassphraseVault,
}

impl PassphraseIdentityCustody {
    fn new(passphrase: Passphrase, store_dir: &StoreDir) -> Self {
        Self {
            vault: PassphraseVault::new(passphrase, store_dir.join("identity.envelope")),
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

/// Re-wrap a store's passphrase-protected signing identity under a new
/// passphrase — the identity half of a host's "change passphrase", the
/// sibling of [`crate::custody::rewrap_passphrase_custody`]. The store's
/// `identity.envelope` is decrypted with `old` and re-sealed under `new`
/// (fresh salt and nonce). Errors if nothing is established there
/// ([`KeyError::Persistence`]) or if `old` is wrong ([`KeyError::Crypto`]),
/// leaving the existing file untouched on either failure. After it returns,
/// the identity is re-opened under `new`.
pub fn rewrap_passphrase_identity_custody(
    store_dir: &StoreDir,
    old: Passphrase,
    new: &Passphrase,
) -> Result<(), KeyError> {
    PassphraseIdentityCustody::new(old, store_dir)
        .vault
        .rewrap(new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::test_keyring;

    fn temp_store_dir() -> (tempfile::TempDir, StoreDir) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new(tmp.path());
        (tmp, dir)
    }

    // =========================================================================
    // Keyring preset
    // =========================================================================

    #[test]
    fn keyring_preset_unlock_persist_forget_round_trip() {
        test_keyring::install();
        let custody = KeyringIdentityCustody::new("identity-keyring-roundtrip".to_string());

        assert!(
            custody.unlock().expect("unlock a fresh store").is_none(),
            "a fresh store has no established identity",
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

    /// Two stores' keyring identities never collide: each `KeyringIdentityCustody`
    /// is scoped by its own `store_id`, the identity sibling of the master
    /// key's per-store keyring account.
    #[test]
    fn keyring_preset_is_scoped_to_its_store() {
        test_keyring::install();
        let store_a = KeyringIdentityCustody::new("identity-keyring-scope-a".to_string());
        let store_b = KeyringIdentityCustody::new("identity-keyring-scope-b".to_string());

        let keypair_a = UserKeypair::generate();
        store_a.persist(&keypair_a).expect("persist to store a");

        assert_eq!(
            store_a.unlock().unwrap().unwrap().public_key(),
            keypair_a.public_key(),
        );
        assert!(
            store_b.unlock().unwrap().is_none(),
            "store b must not see store a's identity",
        );
    }

    // =========================================================================
    // InMemory preset
    // =========================================================================

    #[test]
    fn in_memory_preset_unlock_returns_the_seeded_keypair() {
        let seed = UserKeypair::generate();
        let expected = seed.public_key();
        let custody = InMemoryIdentityCustody::new(seed);

        let unlocked = custody
            .unlock()
            .expect("unlock")
            .expect("seeded keypair is present");
        assert_eq!(unlocked.public_key(), expected);
    }

    #[test]
    fn in_memory_preset_persist_replaces_and_forget_clears() {
        let custody = InMemoryIdentityCustody::new(UserKeypair::generate());

        let rotated = UserKeypair::generate();
        custody.persist(&rotated).expect("persist");
        assert_eq!(
            custody.unlock().unwrap().unwrap().public_key(),
            rotated.public_key(),
        );

        custody.forget().expect("forget");
        assert!(custody.unlock().unwrap().is_none());
    }

    // =========================================================================
    // Passphrase preset
    // =========================================================================

    #[test]
    fn passphrase_preset_establish_then_unlock_round_trips() {
        let (_tmp, dir) = temp_store_dir();
        let custody = PassphraseIdentityCustody::new(
            Passphrase::new("correct horse battery staple".to_string()),
            &dir,
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
        let (_tmp, dir) = temp_store_dir();
        let writer =
            PassphraseIdentityCustody::new(Passphrase::new("right passphrase".to_string()), &dir);
        writer.persist(&UserKeypair::generate()).expect("establish");

        let reader =
            PassphraseIdentityCustody::new(Passphrase::new("wrong passphrase".to_string()), &dir);
        match reader.unlock() {
            Err(error) => assert!(matches!(error, KeyError::Crypto(_)), "got {error:?}"),
            Ok(_) => panic!("wrong passphrase must not unlock"),
        }
    }

    /// The identity envelope lives inside the store directory, alongside
    /// `master.keyring` — a store's identity belongs with the rest of that
    /// store's own state.
    #[test]
    fn passphrase_preset_lives_inside_the_store_directory() {
        let (_tmp, dir) = temp_store_dir();
        let custody = PassphraseIdentityCustody::new(Passphrase::new("unused".to_string()), &dir);
        custody.persist(&UserKeypair::generate()).expect("persist");

        let path = dir.join("identity.envelope");
        assert!(
            path.exists(),
            "the envelope is written inside the store directory"
        );
    }

    /// A literal v1 envelope, independently captured, pinned here to prove the
    /// identity preset reads the exact same wire format `custody.rs`'s
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

    /// The identity "change passphrase" entry point wires through to the
    /// vault's re-wrap: after it, the old passphrase no longer unlocks the
    /// identity file and the new one does. The envelope's own re-wrap
    /// guarantees are pinned in `envelope.rs`; this only proves the wiring.
    #[test]
    fn rewrap_passphrase_identity_custody_moves_the_identity_to_the_new_passphrase() {
        let (_tmp, dir) = temp_store_dir();
        let keypair = UserKeypair::generate();
        PassphraseIdentityCustody::new(Passphrase::new("old".to_string()), &dir)
            .persist(&keypair)
            .expect("establish");

        rewrap_passphrase_identity_custody(
            &dir,
            Passphrase::new("old".to_string()),
            &Passphrase::new("new".to_string()),
        )
        .expect("re-wrap under the new passphrase");

        let with_old = PassphraseIdentityCustody::new(Passphrase::new("old".to_string()), &dir);
        assert!(
            with_old.unlock().is_err(),
            "the old passphrase must no longer unlock after a re-wrap",
        );
        let with_new = PassphraseIdentityCustody::new(Passphrase::new("new".to_string()), &dir);
        assert_eq!(
            with_new
                .unlock()
                .expect("the new passphrase unlocks")
                .expect("identity present")
                .public_key(),
            keypair.public_key(),
        );
    }

    #[test]
    fn passphrase_preset_envelope_fixture_v1_unlocks() {
        let (_tmp, dir) = temp_store_dir();
        std::fs::write(dir.join("identity.envelope"), V1_FIXTURE_ENVELOPE_JSON)
            .expect("write fixture envelope");

        let custody = PassphraseIdentityCustody::new(
            Passphrase::new(V1_FIXTURE_PASSPHRASE.to_string()),
            &dir,
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

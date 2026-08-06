//! A store's device-identity custody: where its signing keypair is unlocked
//! from, where a newly established one is written, and how it is removed.
//! [`IdentityCustody`] is the policy a host selects on the builder, next to
//! [`crate::custody::KeyCustody`]; [`IdentityCustody::resolve`] turns it into
//! the [`DeviceIdentityCustody`] trait object the identity-establishing call
//! sites (create, join, restore) drive.

use std::sync::Arc;

use crate::custody::preset::{self, CustodySecret, InMemoryCustody, PassphraseCustody};
use crate::keys::{DeviceIdentityCustody, KeyError, StoreKeys, UserKeypair, SIGN_SECRETKEYBYTES};
use coven_foundation::store_dir::StoreDir;

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
    /// the store's retained owners: `store_keys` for
    /// [`IdentityCustody::Keyring`] and `store_dir` for
    /// [`IdentityCustody::Passphrase`].
    ///
    /// Public to match [`KeyCustody::resolve`](crate::custody::KeyCustody::resolve): a
    /// host can resolve either policy against the same retained store-key
    /// capability used by the store boundary.
    pub fn resolve(
        self,
        store_keys: &StoreKeys,
        store_dir: &StoreDir,
    ) -> Arc<dyn DeviceIdentityCustody> {
        match self {
            IdentityCustody::Keyring => Arc::new(store_keys.clone()),
            IdentityCustody::Passphrase(passphrase) => {
                Arc::new(PassphraseCustody::<UserKeypair>::new(passphrase, store_dir))
            }
            IdentityCustody::InMemory(keypair) => Arc::new(InMemoryCustody::new(keypair)),
            IdentityCustody::Custom(custody) => custody,
        }
    }
}

// =============================================================================
// The signing identity as a custody secret
// =============================================================================

impl CustodySecret for UserKeypair {
    const FILE: &'static str = "identity.envelope";

    fn to_bytes(&self) -> Vec<u8> {
        self.to_keypair_bytes().to_vec()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, KeyError> {
        let len = bytes.len();
        let signing_key: [u8; SIGN_SECRETKEYBYTES] = bytes.try_into().map_err(|_| {
            KeyError::Crypto(format!(
                "decrypted device identity is {len} bytes, expected {SIGN_SECRETKEYBYTES}"
            ))
        })?;
        UserKeypair::from_signing_key_bytes(&signing_key)
    }
}

impl DeviceIdentityCustody for InMemoryCustody<UserKeypair> {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        InMemoryCustody::unlock(self)
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        InMemoryCustody::persist(self, keypair)
    }

    fn forget(&self) -> Result<(), KeyError> {
        InMemoryCustody::forget(self)
    }
}

impl DeviceIdentityCustody for PassphraseCustody<UserKeypair> {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        PassphraseCustody::unlock(self)
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        PassphraseCustody::persist(self, keypair)
    }

    fn forget(&self) -> Result<(), KeyError> {
        PassphraseCustody::forget(self)
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
    preset::rewrap::<UserKeypair>(store_dir, old, new)
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
        let store_keys = StoreKeys::bind("identity-keyring-roundtrip".to_string());
        let custody = IdentityCustody::Keyring.resolve(&store_keys, &StoreDir::new("unused"));

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

    /// Two stores' keyring identities never collide: each `StoreKeys` custody
    /// is scoped by its own `store_id`, the identity sibling of the master key's
    /// per-store keyring account.
    #[test]
    fn keyring_preset_is_scoped_to_its_store() {
        test_keyring::install();
        let store_a_keys = StoreKeys::bind("identity-keyring-scope-a".to_string());
        let store_b_keys = StoreKeys::bind("identity-keyring-scope-b".to_string());
        let store_a = IdentityCustody::Keyring.resolve(&store_a_keys, &StoreDir::new("unused"));
        let store_b = IdentityCustody::Keyring.resolve(&store_b_keys, &StoreDir::new("unused"));

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
        let custody = InMemoryCustody::new(seed);

        let unlocked = custody
            .unlock()
            .expect("unlock")
            .expect("seeded keypair is present");
        assert_eq!(unlocked.public_key(), expected);
    }

    #[test]
    fn in_memory_preset_persist_replaces_and_forget_clears() {
        let custody = InMemoryCustody::new(UserKeypair::generate());

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
        let custody = PassphraseCustody::<UserKeypair>::new(
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
        let writer = PassphraseCustody::<UserKeypair>::new(
            Passphrase::new("right passphrase".to_string()),
            &dir,
        );
        writer.persist(&UserKeypair::generate()).expect("establish");

        let reader = PassphraseCustody::<UserKeypair>::new(
            Passphrase::new("wrong passphrase".to_string()),
            &dir,
        );
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
        let custody =
            PassphraseCustody::<UserKeypair>::new(Passphrase::new("unused".to_string()), &dir);
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
        PassphraseCustody::<UserKeypair>::new(Passphrase::new("old".to_string()), &dir)
            .persist(&keypair)
            .expect("establish");

        rewrap_passphrase_identity_custody(
            &dir,
            Passphrase::new("old".to_string()),
            &Passphrase::new("new".to_string()),
        )
        .expect("re-wrap under the new passphrase");

        let with_old =
            PassphraseCustody::<UserKeypair>::new(Passphrase::new("old".to_string()), &dir);
        assert!(
            with_old.unlock().is_err(),
            "the old passphrase must no longer unlock after a re-wrap",
        );
        let with_new =
            PassphraseCustody::<UserKeypair>::new(Passphrase::new("new".to_string()), &dir);
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

        let custody = PassphraseCustody::<UserKeypair>::new(
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

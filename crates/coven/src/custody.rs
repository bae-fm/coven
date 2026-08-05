//! Master-key custody: where the store's master keyring is unlocked from,
//! where a newly established or rotated one is written, and how it is
//! removed. [`KeyCustody`] is the policy a host selects on the builder;
//! [`KeyCustody::resolve`] turns it into the [`MasterKeyCustody`] trait object
//! coven drives the rest of the sync engine through.

use std::sync::Arc;

use crate::encryption::MasterKeyring;
pub use crate::envelope::Passphrase;
use crate::keys::{KeyError, MasterKeyCustody, StoreKeys};
use crate::store_dir::StoreDir;

pub(crate) mod preset;
use preset::{CustodySecret, InMemoryCustody, PassphraseCustody};

/// How a store's master key is protected. The builder accepts this and never
/// sees a cipher again — coven resolves the selection into a
/// [`MasterKeyCustody`] and builds every cipher from what it supplies.
pub enum KeyCustody {
    /// The OS keyring — the default, byte-for-byte today's behavior.
    Keyring,
    /// Argon2id over a memorized passphrase wraps the keyring; the wrapped
    /// blob lives in a file in the store directory.
    Passphrase(Passphrase),
    /// Supplied for this session, never persisted by coven.
    InMemory(MasterKeyring),
    /// A host-supplied custody implementation.
    Custom(Arc<dyn MasterKeyCustody>),
}

impl KeyCustody {
    /// Resolve the selected policy into the trait object coven drives the
    /// sync engine through, injecting what each preset needs from the store's
    /// retained owners: `store_keys` for [`KeyCustody::Keyring`] and
    /// `store_dir` for [`KeyCustody::Passphrase`].
    pub fn resolve(
        self,
        store_keys: &StoreKeys,
        store_dir: &StoreDir,
    ) -> Arc<dyn MasterKeyCustody> {
        match self {
            KeyCustody::Keyring => Arc::new(KeyringCustody::new(store_keys.clone())),
            KeyCustody::Passphrase(passphrase) => Arc::new(
                PassphraseCustody::<MasterKeyring>::new(passphrase, store_dir),
            ),
            KeyCustody::InMemory(keyring) => Arc::new(InMemoryCustody::new(keyring)),
            KeyCustody::Custom(custody) => custody,
        }
    }
}

struct KeyringCustody {
    keys: StoreKeys,
}

impl KeyringCustody {
    fn new(keys: StoreKeys) -> Self {
        Self { keys }
    }
}

impl MasterKeyCustody for KeyringCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.keys
            .get_encryption_key()?
            .map(|serialized| {
                MasterKeyring::from_serialized(&serialized)
                    .map_err(|error| KeyError::Crypto(error.to_string()))
            })
            .transpose()
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        self.keys.set_encryption_key(&keyring.to_serialized())
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.keys.delete_encryption_key()
    }
}

// =============================================================================
// The keyring as a custody secret
// =============================================================================

impl CustodySecret for MasterKeyring {
    const FILE: &'static str = "master.keyring";

    fn to_bytes(&self) -> Vec<u8> {
        self.to_serialized().into_bytes()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, KeyError> {
        let serialized = String::from_utf8(bytes)
            .map_err(|e| KeyError::Crypto(format!("decrypted master keyring is not UTF-8: {e}")))?;
        MasterKeyring::from_serialized(&serialized).map_err(|e| KeyError::Crypto(e.to_string()))
    }
}

impl MasterKeyCustody for InMemoryCustody<MasterKeyring> {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        InMemoryCustody::unlock(self)
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        InMemoryCustody::persist(self, keyring)
    }

    fn forget(&self) -> Result<(), KeyError> {
        InMemoryCustody::forget(self)
    }
}

impl MasterKeyCustody for PassphraseCustody<MasterKeyring> {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        PassphraseCustody::unlock(self)
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        PassphraseCustody::persist(self, keyring)
    }

    fn forget(&self) -> Result<(), KeyError> {
        PassphraseCustody::forget(self)
    }
}

/// Re-wrap a store's passphrase-protected master keyring under a new
/// passphrase — the store-side half of covenpass's "change passphrase". The
/// `<store_dir>/master.keyring` envelope is decrypted with `old` and re-sealed
/// under `new` (fresh salt and nonce). Errors if nothing is established there
/// ([`KeyError::Persistence`]) or if `old` is wrong ([`KeyError::Crypto`]),
/// leaving the existing file untouched on either failure. After it returns,
/// the store's custody is re-opened under `new`.
pub fn rewrap_passphrase_custody(
    store_dir: &StoreDir,
    old: Passphrase,
    new: &Passphrase,
) -> Result<(), KeyError> {
    preset::rewrap::<MasterKeyring>(store_dir, old, new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::EncryptionService;

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
        crate::keys::test_keyring::install();
        let store_keys = StoreKeys::bind("custody-keyring-roundtrip".to_string());
        let custody = KeyCustody::Keyring.resolve(&store_keys, &StoreDir::new("unused"));

        assert!(
            custody.unlock().expect("unlock a fresh store").is_none(),
            "a fresh store has no established keyring",
        );

        let keyring = MasterKeyring::generate();
        custody.persist(&keyring).expect("persist");
        let unlocked = custody
            .unlock()
            .expect("unlock after persist")
            .expect("keyring is established");
        assert_eq!(unlocked.fingerprint(), keyring.fingerprint());

        custody.forget().expect("forget");
        assert!(
            custody.unlock().expect("unlock after forget").is_none(),
            "forget removes the established keyring",
        );
    }

    /// The corrupt-empty-entry discipline lives once, in `StoreKeys::read`
    /// (`empty_keyring_entry_is_an_error_not_absence` pins it there), and
    /// `KeyringCustody::unlock` inherits it by construction: a present-but-empty
    /// entry surfaces as `Err`, never `Ok(None)` — so `initialize_master_key`
    /// (which generates only on `unlock() == Ok(None)`) cannot clobber it.
    #[test]
    fn keyring_preset_unlock_does_not_read_a_corrupt_empty_entry_as_absent() {
        crate::keys::test_keyring::install();
        let store_id = "custody-keyring-corrupt-empty".to_string();
        let store_keys = StoreKeys::bind(store_id.clone());
        store_keys
            .write_empty_encryption_key_for_test()
            .expect("write empty entry");

        let custody = KeyCustody::Keyring.resolve(&store_keys, &StoreDir::new("unused"));
        let error = custody.unlock().expect_err("empty entry is corrupt");
        assert!(error.to_string().contains("present but empty"));
    }

    // =========================================================================
    // InMemory preset
    // =========================================================================

    #[test]
    fn in_memory_preset_unlock_returns_the_seeded_keyring() {
        let seed = MasterKeyring::generate();
        let fingerprint = seed.fingerprint();
        let custody = InMemoryCustody::new(seed);

        let unlocked = custody
            .unlock()
            .expect("unlock")
            .expect("seeded keyring is present");
        assert_eq!(unlocked.fingerprint(), fingerprint);
    }

    #[test]
    fn in_memory_preset_persist_replaces_and_forget_clears() {
        let custody = InMemoryCustody::new(MasterKeyring::generate());

        let rotated = MasterKeyring::generate();
        custody.persist(&rotated).expect("persist");
        assert_eq!(
            custody.unlock().unwrap().unwrap().fingerprint(),
            rotated.fingerprint(),
        );

        custody.forget().expect("forget");
        assert!(custody.unlock().unwrap().is_none());
    }

    #[test]
    fn in_memory_preset_never_writes_under_the_store_dir() {
        let (tmp, _dir) = temp_store_dir();
        let custody = InMemoryCustody::new(MasterKeyring::generate());
        custody
            .persist(&MasterKeyring::generate())
            .expect("persist");
        custody.forget().expect("forget");

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .expect("read store dir")
            .collect();
        assert!(
            entries.is_empty(),
            "InMemory custody must touch no file under the store dir",
        );
    }

    // =========================================================================
    // Passphrase preset
    // =========================================================================

    #[test]
    fn passphrase_preset_establish_then_unlock_round_trips() {
        let (_tmp, dir) = temp_store_dir();
        let custody = PassphraseCustody::<MasterKeyring>::new(
            Passphrase::new("correct horse battery staple".to_string()),
            &dir,
        );

        assert!(custody.unlock().expect("unlock before establish").is_none());

        let keyring = MasterKeyring::generate();
        custody.persist(&keyring).expect("establish");
        let unlocked = custody
            .unlock()
            .expect("unlock after establish")
            .expect("keyring is established");
        assert_eq!(unlocked.fingerprint(), keyring.fingerprint());
    }

    #[test]
    fn passphrase_preset_wrong_passphrase_is_err_not_none() {
        let (_tmp, dir) = temp_store_dir();
        let writer = PassphraseCustody::<MasterKeyring>::new(
            Passphrase::new("right passphrase".to_string()),
            &dir,
        );
        writer
            .persist(&MasterKeyring::generate())
            .expect("establish");

        let reader = PassphraseCustody::<MasterKeyring>::new(
            Passphrase::new("wrong passphrase".to_string()),
            &dir,
        );
        let error = reader
            .unlock()
            .expect_err("wrong passphrase must not unlock");
        assert!(
            error.to_string().to_lowercase().contains("passphrase")
                || matches!(error, KeyError::Crypto(_))
        );
    }

    #[test]
    fn passphrase_preset_missing_file_is_none() {
        let (_tmp, dir) = temp_store_dir();
        let custody =
            PassphraseCustody::<MasterKeyring>::new(Passphrase::new("unused".to_string()), &dir);
        assert!(custody.unlock().expect("unlock with no file").is_none());
    }

    // Rotation re-wrap, atomic-write-no-torn-file, and wrong-derivation
    // behavior are shared envelope-format guarantees, pinned generically once
    // in `envelope.rs` rather than duplicated per payload type here.

    /// A literal v1 envelope, pinned here so a future change to the
    /// derivation, AEAD, or serialization code is caught by a failing test
    /// rather than silently stranding every already-wrapped `master.keyring`
    /// file. Wraps `MasterKeyring::from(EncryptionService::from_key([0x11u8;
    /// 32]))` under passphrase "fixture-passphrase" with a fixed salt and
    /// nonce (real writes use random ones; the fixture fixes them only so its
    /// bytes are reproducible here).
    const V1_FIXTURE_PASSPHRASE: &str = "fixture-passphrase";
    const V1_FIXTURE_ENVELOPE_JSON: &str = concat!(
        r#"{"v":1,"kdf":{"algo":"argon2id","m_cost":65536,"t_cost":3,"p_cost":4,"#,
        r#""salt_b64":"3q2+7wEjRWeJq83vABEiMw=="},"#,
        r#""nonce_b64":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYX","#,
        r#""ciphertext_b64":"+7/Z7TSK5xtqL6fqDzh5ayBkPPtuzf/0FyBy3mrgtiFjfabWOqVb8FonvR7SwvntJd9ERnTDljuE0o3Ofzs8a6XMbf0VJ6HlDp2aB62apAV3Fv1e1eb8su6/TxVOCskR9cvDmPr2P3CnsX3YRaGcEuilgSW8uKosW6gowDDZHqGat1XSPbnG02GO5QYuMxM="}"#
    );

    /// The store-side "change passphrase" entry point wires through to the
    /// vault's re-wrap: after it, the old passphrase no longer unlocks
    /// `master.keyring` and the new one does. The envelope's own re-wrap
    /// guarantees are pinned in `envelope.rs`; this only proves the wiring.
    #[test]
    fn rewrap_passphrase_custody_moves_the_master_keyring_to_the_new_passphrase() {
        let (_tmp, dir) = temp_store_dir();
        let established =
            PassphraseCustody::<MasterKeyring>::new(Passphrase::new("old".to_string()), &dir);
        let keyring = MasterKeyring::generate();
        established.persist(&keyring).expect("establish");

        rewrap_passphrase_custody(
            &dir,
            Passphrase::new("old".to_string()),
            &Passphrase::new("new".to_string()),
        )
        .expect("re-wrap under the new passphrase");

        let with_old =
            PassphraseCustody::<MasterKeyring>::new(Passphrase::new("old".to_string()), &dir);
        assert!(
            with_old.unlock().is_err(),
            "the old passphrase must no longer unlock after a re-wrap",
        );
        let with_new =
            PassphraseCustody::<MasterKeyring>::new(Passphrase::new("new".to_string()), &dir);
        assert_eq!(
            with_new
                .unlock()
                .expect("the new passphrase unlocks")
                .expect("keyring present")
                .fingerprint(),
            keyring.fingerprint(),
        );
    }

    #[test]
    fn passphrase_preset_envelope_fixture_v1_unlocks() {
        let (_tmp, dir) = temp_store_dir();
        std::fs::write(dir.join("master.keyring"), V1_FIXTURE_ENVELOPE_JSON)
            .expect("write fixture envelope");

        let custody = PassphraseCustody::<MasterKeyring>::new(
            Passphrase::new(V1_FIXTURE_PASSPHRASE.to_string()),
            &dir,
        );
        let keyring = custody
            .unlock()
            .expect("the v1 fixture must still unlock")
            .expect("the fixture names an established keyring");
        assert_eq!(
            keyring.fingerprint(),
            EncryptionService::from_key([0x11u8; 32]).fingerprint(),
        );
    }
}

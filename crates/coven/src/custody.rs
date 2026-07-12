//! Master-key custody: where the store's master keyring is unlocked from,
//! where a newly established or rotated one is written, and how it is
//! removed. [`KeyCustody`] is the policy a host selects on the builder;
//! [`KeyCustody::resolve`] turns it into the [`MasterKeyCustody`] trait object
//! coven drives the rest of the sync engine through.

use std::sync::{Arc, RwLock};

use crate::encryption::MasterKeyring;
pub use crate::envelope::Passphrase;
use crate::envelope::PassphraseVault;
use crate::keys::{KeyError, MasterKeyCustody, StoreKeys};
use crate::store_dir::StoreDir;

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
    /// identity: `store_id` for [`KeyCustody::Keyring`] (the keyring account
    /// name), `store_dir` for [`KeyCustody::Passphrase`] (the wrapped-file
    /// path).
    pub fn resolve(self, store_id: &str, store_dir: &StoreDir) -> Arc<dyn MasterKeyCustody> {
        match self {
            KeyCustody::Keyring => Arc::new(KeyringCustody::new(store_id.to_string())),
            KeyCustody::Passphrase(passphrase) => {
                Arc::new(PassphraseCustody::new(passphrase, store_dir))
            }
            KeyCustody::InMemory(keyring) => Arc::new(InMemoryCustody::new(keyring)),
            KeyCustody::Custom(custody) => custody,
        }
    }
}

// =============================================================================
// Keyring preset
// =============================================================================

/// The OS keyring, wrapping [`StoreKeys`]'s master-key methods verbatim: same
/// `encryption_master_key:{store_id}` account
/// (`keyring_account_names_are_a_stable_storage_contract` pins it), same
/// present-but-empty-is-corrupt read discipline (enforced once, at
/// [`StoreKeys::get_encryption_key`]'s single keyring-read chokepoint), so
/// existing installs' keys are found unchanged.
struct KeyringCustody {
    keys: StoreKeys,
}

impl KeyringCustody {
    fn new(store_id: String) -> Self {
        Self {
            keys: StoreKeys::new(store_id),
        }
    }
}

impl MasterKeyCustody for KeyringCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.keys
            .get_encryption_key()?
            .map(|s| {
                MasterKeyring::from_serialized(&s).map_err(|e| KeyError::Crypto(e.to_string()))
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
// InMemory preset
// =============================================================================

/// Supplied per session, never persisted by coven — the native sibling of
/// what the wasm build already does (the page supplies the key per open).
struct InMemoryCustody {
    keyring: RwLock<Option<MasterKeyring>>,
}

impl InMemoryCustody {
    fn new(seed: MasterKeyring) -> Self {
        Self {
            keyring: RwLock::new(Some(seed)),
        }
    }
}

impl MasterKeyCustody for InMemoryCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        Ok(self.keyring.read().unwrap().clone())
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        *self.keyring.write().unwrap() = Some(keyring.clone());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.keyring.write().unwrap() = None;
        Ok(())
    }
}

// =============================================================================
// Passphrase preset
// =============================================================================

/// Argon2id over a [`Passphrase`] wraps the master keyring, via the shared
/// [`PassphraseVault`] — the wrapped blob is a JSON envelope in a file under
/// the store directory (`<store_dir>/master.keyring`), not a keyring entry.
struct PassphraseCustody {
    vault: PassphraseVault,
}

impl PassphraseCustody {
    fn new(passphrase: Passphrase, store_dir: &StoreDir) -> Self {
        Self {
            vault: PassphraseVault::new(passphrase, store_dir.join("master.keyring")),
        }
    }
}

impl MasterKeyCustody for PassphraseCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        let Some(plaintext) = self.vault.unlock()? else {
            return Ok(None);
        };
        let serialized = String::from_utf8(plaintext)
            .map_err(|e| KeyError::Crypto(format!("decrypted master keyring is not UTF-8: {e}")))?;
        MasterKeyring::from_serialized(&serialized)
            .map(Some)
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        self.vault.persist(keyring.to_serialized().as_bytes())
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.vault.forget()
    }
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
        let custody = KeyringCustody::new("custody-keyring-roundtrip".to_string());

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
        let account = crate::keys::KeyringSlot::EncryptionMasterKey(store_id.clone()).account();
        keyring_core::Entry::new(
            crate::keys::keyring_service().expect("service registered"),
            &account,
        )
        .expect("create entry")
        .set_password("")
        .expect("write empty entry");

        let custody = KeyringCustody::new(store_id);
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
        let custody = PassphraseCustody::new(
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
        let writer = PassphraseCustody::new(Passphrase::new("right passphrase".to_string()), &dir);
        writer
            .persist(&MasterKeyring::generate())
            .expect("establish");

        let reader = PassphraseCustody::new(Passphrase::new("wrong passphrase".to_string()), &dir);
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
        let custody = PassphraseCustody::new(Passphrase::new("unused".to_string()), &dir);
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

    #[test]
    fn passphrase_preset_envelope_fixture_v1_unlocks() {
        let (_tmp, dir) = temp_store_dir();
        std::fs::write(dir.join("master.keyring"), V1_FIXTURE_ENVELOPE_JSON)
            .expect("write fixture envelope");

        let custody =
            PassphraseCustody::new(Passphrase::new(V1_FIXTURE_PASSPHRASE.to_string()), &dir);
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

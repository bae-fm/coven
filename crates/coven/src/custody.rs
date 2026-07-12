//! Master-key custody: where the store's master keyring is unlocked from,
//! where a newly established or rotated one is written, and how it is
//! removed. [`KeyCustody`] is the policy a host selects on the builder;
//! [`KeyCustody::resolve`] turns it into the [`MasterKeyCustody`] trait object
//! coven drives the rest of the sync engine through.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use argon2::Argon2;
use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use zeroize::ZeroizeOnDrop;

use crate::encryption::MasterKeyring;
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

/// A memorized secret that wraps a store's master keyring under Argon2id.
/// Held zeroizing — the whole struct is cleared on drop, so no copy of the
/// passphrase outlives it.
#[derive(ZeroizeOnDrop)]
pub struct Passphrase(String);

impl Passphrase {
    pub fn new(secret: String) -> Self {
        Self(secret)
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

/// OWASP's current interactive Argon2id recommendation: 64 MiB memory, 3
/// iterations, 4-way parallelism. `argon2::Params::m_cost` is in KiB.
const ARGON2_M_COST_KIB: u32 = 64 * 1024;
const ARGON2_T_COST: u32 = 3;
const ARGON2_P_COST: u32 = 4;
const ARGON2_OUTPUT_LEN: usize = 32;
const SALT_LEN: usize = 16;
const ENVELOPE_VERSION: u32 = 1;
const ARGON2ID_ALGO: &str = "argon2id";

/// The on-disk wrapped-keyring format: `<store_dir>/master.keyring`. The KDF
/// params travel with the ciphertext so a future change to the module
/// constants only affects new wraps — unlock always re-derives from what the
/// file itself names, never from the current constants.
#[derive(Serialize, Deserialize)]
struct PassphraseEnvelope {
    v: u32,
    kdf: KdfParams,
    nonce_b64: String,
    ciphertext_b64: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct KdfParams {
    algo: String,
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
    salt_b64: String,
}

/// The Argon2id-derived wrapping key, cached after first use — Argon2id is
/// deliberately slow, so this is paid once per [`PassphraseCustody`] instance
/// (its "session"), not once per unlock/persist call. Zeroized on drop along
/// with the salt/params it was derived from.
#[derive(Clone, ZeroizeOnDrop)]
struct CachedDerivation {
    salt: Vec<u8>,
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
    key: [u8; ARGON2_OUTPUT_LEN],
}

/// Argon2id over [`Passphrase`] wraps the master keyring; the wrapped blob is
/// a JSON envelope in a file under the store directory, not a keyring entry —
/// files have no Windows Credential Manager size cap, and a keyring that has
/// rotated N times carries N generations.
struct PassphraseCustody {
    passphrase: Passphrase,
    path: PathBuf,
    derived: Mutex<Option<CachedDerivation>>,
}

impl PassphraseCustody {
    fn new(passphrase: Passphrase, store_dir: &StoreDir) -> Self {
        Self {
            passphrase,
            path: store_dir.join("master.keyring"),
            derived: Mutex::new(None),
        }
    }

    fn read_envelope(&self) -> Result<Option<PassphraseEnvelope>, KeyError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let envelope: PassphraseEnvelope = serde_json::from_slice(&bytes).map_err(|e| {
                    KeyError::Persistence(format!("master keyring envelope is malformed: {e}"))
                })?;
                Ok(Some(envelope))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(KeyError::Persistence(format!(
                "read master keyring file: {e}"
            ))),
        }
    }

    /// The wrapping key for this instance, deriving and caching it on first
    /// use. The salt/params are fixed for the life of an established
    /// wrapped file: read from the file if one exists (so a rotation re-wraps
    /// under the same derivation, only a fresh AEAD nonce), or freshly
    /// generated on the very first establishment.
    fn derived_key(&self) -> Result<CachedDerivation, KeyError> {
        if let Some(cached) = self.derived.lock().unwrap().clone() {
            return Ok(cached);
        }

        let (salt, m_cost, t_cost, p_cost) = match self.read_envelope()? {
            Some(envelope) => (
                base64_decode(&envelope.kdf.salt_b64)?,
                envelope.kdf.m_cost,
                envelope.kdf.t_cost,
                envelope.kdf.p_cost,
            ),
            None => {
                let mut salt = vec![0u8; SALT_LEN];
                rand::rng().fill_bytes(&mut salt);
                (salt, ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST)
            }
        };

        let key = derive_wrapping_key(self.passphrase.expose(), &salt, m_cost, t_cost, p_cost)?;
        let cached = CachedDerivation {
            salt,
            m_cost,
            t_cost,
            p_cost,
            key,
        };
        *self.derived.lock().unwrap() = Some(cached.clone());
        Ok(cached)
    }
}

impl MasterKeyCustody for PassphraseCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        let Some(envelope) = self.read_envelope()? else {
            return Ok(None);
        };
        let derivation = self.derived_key()?;
        let nonce = base64_decode(&envelope.nonce_b64)?;
        let ciphertext = base64_decode(&envelope.ciphertext_b64)?;
        let plaintext = open(&derivation.key, &nonce, &ciphertext)?;
        let serialized = String::from_utf8(plaintext)
            .map_err(|e| KeyError::Crypto(format!("decrypted master keyring is not UTF-8: {e}")))?;
        MasterKeyring::from_serialized(&serialized)
            .map(Some)
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        let derivation = self.derived_key()?;
        let (nonce, ciphertext) = seal(&derivation.key, keyring.to_serialized().as_bytes());
        let envelope = PassphraseEnvelope {
            v: ENVELOPE_VERSION,
            kdf: KdfParams {
                algo: ARGON2ID_ALGO.to_string(),
                m_cost: derivation.m_cost,
                t_cost: derivation.t_cost,
                p_cost: derivation.p_cost,
                salt_b64: base64_encode(&derivation.salt),
            },
            nonce_b64: base64_encode(&nonce),
            ciphertext_b64: base64_encode(&ciphertext),
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|e| {
            KeyError::Persistence(format!("serialize master keyring envelope: {e}"))
        })?;
        write_atomic(&self.path, &bytes)
    }

    fn forget(&self) -> Result<(), KeyError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(KeyError::Persistence(format!(
                "remove master keyring file: {e}"
            ))),
        }
    }
}

fn derive_wrapping_key(
    passphrase: &str,
    salt: &[u8],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> Result<[u8; ARGON2_OUTPUT_LEN], KeyError> {
    let params = argon2::Params::new(m_cost, t_cost, p_cost, Some(ARGON2_OUTPUT_LEN))
        .map_err(|e| KeyError::Crypto(format!("invalid argon2id params: {e}")))?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut out = [0u8; ARGON2_OUTPUT_LEN];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut out)
        .map_err(|e| KeyError::Crypto(format!("argon2id derivation failed: {e}")))?;
    Ok(out)
}

fn seal(wrapping_key: &[u8; ARGON2_OUTPUT_LEN], plaintext: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(wrapping_key));
    let mut nonce = vec![0u8; 24];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(GenericArray::from_slice(&nonce), plaintext)
        .expect("XChaCha20-Poly1305 encryption should not fail");
    (nonce, ciphertext)
}

fn open(
    wrapping_key: &[u8; ARGON2_OUTPUT_LEN],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, KeyError> {
    let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(wrapping_key));
    let nonce = GenericArray::from_slice(nonce);
    cipher.decrypt(nonce, ciphertext).map_err(|_| {
        KeyError::Crypto(
            "master keyring decryption failed: wrong passphrase or a corrupt file".to_string(),
        )
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn base64_decode(s: &str) -> Result<Vec<u8>, KeyError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| KeyError::Persistence(format!("master keyring envelope base64: {e}")))
}

/// Write `bytes` to `path` atomically: a uniquely-named temp sibling, fsynced,
/// then renamed over the destination — the same temp-then-rename discipline
/// coven's blob store writes with, so a crash mid-write never leaves a torn
/// `master.keyring`.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), KeyError> {
    let parent = path.parent().ok_or_else(|| {
        KeyError::Persistence(format!(
            "master keyring path has no parent directory: {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| KeyError::Persistence(format!("create master keyring directory: {e}")))?;
    let temp_path = parent.join(format!(
        "{}master.keyring.{}",
        crate::local_blob::TEMP_BLOB_PREFIX,
        uuid::Uuid::new_v4()
    ));
    let mut file = std::fs::File::create(&temp_path)
        .map_err(|e| KeyError::Persistence(format!("write master keyring temp file: {e}")))?;
    file.write_all(bytes)
        .map_err(|e| KeyError::Persistence(format!("write master keyring temp file: {e}")))?;
    file.sync_all()
        .map_err(|e| KeyError::Persistence(format!("fsync master keyring temp file: {e}")))?;
    drop(file);
    std::fs::rename(&temp_path, path)
        .map_err(|e| KeyError::Persistence(format!("install master keyring file: {e}")))?;
    // fsync the parent directory so the rename that installed the keyring is
    // itself durable — the same discipline coven's `sync_parent_dir` and
    // coven-core's `local_blob::sync_parent_dir` keep, both of which propagate
    // these errors rather than swallowing them.
    std::fs::File::open(parent)
        .map_err(|e| KeyError::Persistence(format!("open the parent dir to fsync it: {e}")))?
        .sync_all()
        .map_err(|e| KeyError::Persistence(format!("fsync the parent dir: {e}")))?;
    Ok(())
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

    #[test]
    fn passphrase_preset_rotation_re_wraps_and_old_passphrase_still_unlocks() {
        let (_tmp, dir) = temp_store_dir();
        let passphrase_text = "stable passphrase across rotation".to_string();
        let custody = PassphraseCustody::new(Passphrase::new(passphrase_text.clone()), &dir);

        let first = MasterKeyring::generate();
        custody.persist(&first).expect("establish");
        let file_after_first = std::fs::read(&custody.path).expect("read file after first persist");

        let rotated = MasterKeyring::generate();
        custody.persist(&rotated).expect("rotate");
        let file_after_rotation =
            std::fs::read(&custody.path).expect("read file after rotation persist");

        assert_ne!(
            file_after_first, file_after_rotation,
            "the file changes after a rotation persist",
        );

        // A fresh custody instance over the same passphrase and file — proving
        // the passphrase (not the process-cached derivation) is what unlocks.
        let reopened = PassphraseCustody::new(Passphrase::new(passphrase_text), &dir);
        let unlocked = reopened
            .unlock()
            .expect("unlock after rotation")
            .expect("keyring present");
        assert_eq!(unlocked.fingerprint(), rotated.fingerprint());
    }

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

    #[test]
    fn passphrase_preset_persist_over_an_existing_file_never_leaves_a_torn_file() {
        let (_tmp, dir) = temp_store_dir();
        let custody =
            PassphraseCustody::new(Passphrase::new("atomic-write-test".to_string()), &dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("first persist");
        custody
            .persist(&MasterKeyring::generate())
            .expect("second persist over the existing file");

        // The file is valid, complete JSON after two writes — never a
        // half-written temp left in place of (or beside) the real file.
        let bytes = std::fs::read(&custody.path).expect("read final file");
        let _: PassphraseEnvelope =
            serde_json::from_slice(&bytes).expect("the file is complete, parseable JSON");
        let siblings: Vec<_> = std::fs::read_dir(&*dir)
            .expect("read store dir")
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name != "master.keyring")
            .collect();
        assert!(
            siblings.is_empty(),
            "no leftover temp file after a persist over an existing file: {siblings:?}",
        );
    }
}

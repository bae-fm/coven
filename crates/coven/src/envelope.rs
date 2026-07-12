//! The passphrase-wrapped envelope: Argon2id derives a wrapping key from a
//! memorized secret, XChaCha20-Poly1305 seals arbitrary plaintext bytes under
//! it, and the result is written atomically to a file as versioned JSON. One
//! implementation, shared by every passphrase-protected secret coven stores
//! this way (a store's master keyring, a device's signing identity) —
//! [`PassphraseVault`] is parameterized only by its plaintext payload (opaque
//! bytes) and its file path; the payload's own shape (a JSON keyring, a raw
//! 64-byte keypair) is the caller's concern, not this module's.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use argon2::Argon2;
use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use zeroize::ZeroizeOnDrop;

use crate::keys::KeyError;

/// A memorized secret that wraps a payload under Argon2id. Held zeroizing —
/// the whole struct is cleared on drop, so no copy of the passphrase outlives
/// it.
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

/// The on-disk wrapped-payload format. The KDF params travel with the
/// ciphertext so a future change to the module constants only affects new
/// wraps — unlock always re-derives from what the file itself names, never
/// from the current constants.
#[derive(Serialize, Deserialize)]
struct Envelope {
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
/// deliberately slow, so this is paid once per [`PassphraseVault`] instance
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

/// Argon2id over a [`Passphrase`] wraps an arbitrary plaintext payload; the
/// wrapped blob is a JSON envelope in a file at `path`, not a keyring entry —
/// files have no Windows Credential Manager size cap, and a payload that has
/// rotated N times carries N generations.
pub(crate) struct PassphraseVault {
    passphrase: Passphrase,
    path: PathBuf,
    derived: Mutex<Option<CachedDerivation>>,
}

impl PassphraseVault {
    pub(crate) fn new(passphrase: Passphrase, path: PathBuf) -> Self {
        Self {
            passphrase,
            path,
            derived: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    fn read_envelope(&self) -> Result<Option<Envelope>, KeyError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|e| {
                    KeyError::Persistence(format!("passphrase envelope is malformed: {e}"))
                })?;
                Ok(Some(envelope))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(KeyError::Persistence(format!(
                "read passphrase envelope file: {e}"
            ))),
        }
    }

    /// The wrapping key for this instance, deriving and caching it on first
    /// use. The salt/params are fixed for the life of an established wrapped
    /// file: read from the file if one exists (so a rotation re-wraps under
    /// the same derivation, only a fresh AEAD nonce), or freshly generated on
    /// the very first establishment.
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

    /// The vault's plaintext payload, or `None` if nothing has ever been
    /// established. A wrong passphrase or a corrupt file is `Err`, never
    /// `Ok(None)`.
    pub(crate) fn unlock(&self) -> Result<Option<Vec<u8>>, KeyError> {
        let Some(envelope) = self.read_envelope()? else {
            return Ok(None);
        };
        let derivation = self.derived_key()?;
        let nonce = base64_decode(&envelope.nonce_b64)?;
        let ciphertext = base64_decode(&envelope.ciphertext_b64)?;
        open(&derivation.key, &nonce, &ciphertext).map(Some)
    }

    /// Wrap and store `plaintext`, replacing whatever is stored. Idempotent.
    pub(crate) fn persist(&self, plaintext: &[u8]) -> Result<(), KeyError> {
        let derivation = self.derived_key()?;
        let (nonce, ciphertext) = seal(&derivation.key, plaintext);
        let envelope = Envelope {
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
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|e| KeyError::Persistence(format!("serialize passphrase envelope: {e}")))?;
        write_atomic(&self.path, &bytes)
    }

    /// Remove the stored envelope. `Ok` when nothing was stored.
    pub(crate) fn forget(&self) -> Result<(), KeyError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(KeyError::Persistence(format!(
                "remove passphrase envelope file: {e}"
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
            "envelope decryption failed: wrong passphrase or a corrupt file".to_string(),
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
        .map_err(|e| KeyError::Persistence(format!("passphrase envelope base64: {e}")))
}

/// Write `bytes` to `path` atomically: a uniquely-named temp sibling (derived
/// from `path`'s own file name, so two vaults at different paths never share
/// a temp name), fsynced, then renamed over the destination — the same
/// temp-then-rename discipline coven's blob store writes with, so a crash
/// mid-write never leaves a torn file.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), KeyError> {
    let parent = path.parent().ok_or_else(|| {
        KeyError::Persistence(format!(
            "passphrase envelope path has no parent directory: {}",
            path.display()
        ))
    })?;
    let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        KeyError::Persistence(format!(
            "passphrase envelope path has no valid file name: {}",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| KeyError::Persistence(format!("create passphrase envelope directory: {e}")))?;
    let temp_path = parent.join(format!(
        "{}{file_name}.{}",
        crate::local_blob::TEMP_BLOB_PREFIX,
        uuid::Uuid::new_v4()
    ));
    let mut file = std::fs::File::create(&temp_path)
        .map_err(|e| KeyError::Persistence(format!("write passphrase envelope temp file: {e}")))?;
    file.write_all(bytes)
        .map_err(|e| KeyError::Persistence(format!("write passphrase envelope temp file: {e}")))?;
    file.sync_all()
        .map_err(|e| KeyError::Persistence(format!("fsync passphrase envelope temp file: {e}")))?;
    drop(file);
    std::fs::rename(&temp_path, path)
        .map_err(|e| KeyError::Persistence(format!("install passphrase envelope file: {e}")))?;
    // fsync the parent directory so the rename that installed the envelope is
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

    fn temp_vault(passphrase: &str) -> (tempfile::TempDir, PassphraseVault) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("payload.envelope");
        let vault = PassphraseVault::new(Passphrase::new(passphrase.to_string()), path);
        (tmp, vault)
    }

    #[test]
    fn establish_then_unlock_round_trips_arbitrary_bytes() {
        let (_tmp, vault) = temp_vault("correct horse battery staple");
        assert!(vault.unlock().expect("unlock before establish").is_none());

        // Non-UTF-8 bytes: the vault must not assume a string payload — a raw
        // 64-byte Ed25519 keypair is not valid UTF-8 in general.
        let payload: Vec<u8> = (0u8..=255).collect();
        vault.persist(&payload).expect("establish");
        let unlocked = vault
            .unlock()
            .expect("unlock after establish")
            .expect("payload is established");
        assert_eq!(unlocked, payload);
    }

    #[test]
    fn wrong_passphrase_is_err_not_none() {
        let (tmp, writer) = temp_vault("right passphrase");
        writer.persist(b"secret payload").expect("establish");

        let path = tmp.path().join("payload.envelope");
        let reader = PassphraseVault::new(Passphrase::new("wrong passphrase".to_string()), path);
        let error = reader
            .unlock()
            .expect_err("wrong passphrase must not unlock");
        assert!(matches!(error, KeyError::Crypto(_)), "got {error:?}");
    }

    #[test]
    fn missing_file_is_none() {
        let (_tmp, vault) = temp_vault("unused");
        assert!(vault.unlock().expect("unlock with no file").is_none());
    }

    #[test]
    fn rotation_re_wraps_and_old_passphrase_still_unlocks() {
        let passphrase_text = "stable passphrase across rotation".to_string();
        let (tmp, vault) = temp_vault(&passphrase_text);

        vault.persist(b"first payload").expect("establish");
        let file_after_first = std::fs::read(vault.path()).expect("read after first persist");

        vault.persist(b"rotated payload").expect("rotate");
        let file_after_rotation = std::fs::read(vault.path()).expect("read after rotation");

        assert_ne!(
            file_after_first, file_after_rotation,
            "the file changes after a rotation persist",
        );

        // A fresh vault instance over the same passphrase and file — proving
        // the passphrase (not the process-cached derivation) is what unlocks.
        let path = tmp.path().join("payload.envelope");
        let reopened = PassphraseVault::new(Passphrase::new(passphrase_text), path);
        let unlocked = reopened
            .unlock()
            .expect("unlock after rotation")
            .expect("payload present");
        assert_eq!(unlocked, b"rotated payload");
    }

    #[test]
    fn persist_over_an_existing_file_never_leaves_a_torn_file() {
        let (tmp, vault) = temp_vault("atomic-write-test");
        vault.persist(b"first").expect("first persist");
        vault
            .persist(b"second, longer payload")
            .expect("second persist");

        // The file is valid, complete JSON after two writes — never a
        // half-written temp left in place of (or beside) the real file.
        let bytes = std::fs::read(vault.path()).expect("read final file");
        let _: Envelope =
            serde_json::from_slice(&bytes).expect("the file is complete, parseable JSON");
        let siblings: Vec<_> = std::fs::read_dir(tmp.path())
            .expect("read temp dir")
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|name| name != "payload.envelope")
            .collect();
        assert!(
            siblings.is_empty(),
            "no leftover temp file after a persist over an existing file: {siblings:?}",
        );
    }

    #[test]
    fn two_vaults_at_different_paths_never_collide_on_a_temp_name() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let a = PassphraseVault::new(
            Passphrase::new("a".to_string()),
            tmp.path().join("a.envelope"),
        );
        let b = PassphraseVault::new(
            Passphrase::new("b".to_string()),
            tmp.path().join("b.envelope"),
        );
        a.persist(b"payload-a").expect("persist a");
        b.persist(b"payload-b").expect("persist b");

        assert_eq!(a.unlock().unwrap().unwrap(), b"payload-a");
        assert_eq!(b.unlock().unwrap().unwrap(), b"payload-b");
    }
}

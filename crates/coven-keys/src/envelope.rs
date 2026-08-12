//! The passphrase-wrapped envelope: Argon2id derives a wrapping key from a
//! memorized secret, XChaCha20-Poly1305 seals arbitrary plaintext bytes under
//! it, and the result is written atomically to a file as versioned JSON. One
//! implementation, shared by every passphrase-protected secret coven stores
//! this way (a store's master keyring, a device's signing identity) —
//! [`PassphraseVault`] is parameterized only by its plaintext payload (opaque
//! bytes) and its file path; the payload's own shape (a JSON keyring, a raw
//! 64-byte keypair) is the caller's concern, not this module's.

#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
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
/// XChaCha20-Poly1305's nonce length.
const NONCE_LEN: usize = 24;

/// The on-disk wrapped-payload format. The KDF params travel with the
/// ciphertext so a future change to the module constants only affects new
/// wraps — unlock always re-derives from what the file itself names, never
/// from the current constants.
#[derive(Serialize, Deserialize, Clone)]
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
    file: coven_foundation::atomic_file::AtomicFile,
    derived: Mutex<Option<CachedDerivation>>,
}

impl PassphraseVault {
    pub(crate) fn new(passphrase: Passphrase, path: PathBuf) -> Self {
        Self {
            passphrase,
            file: coven_foundation::atomic_file::AtomicFile::new(path),
            derived: Mutex::new(None),
        }
    }

    fn read_envelope(&self) -> Result<Option<Envelope>, KeyError> {
        match self.file.read_optional().map_err(KeyError::File)? {
            Some(bytes) => {
                let envelope: Envelope =
                    serde_json::from_slice(&bytes).map_err(|source| KeyError::Json {
                        operation: "parse passphrase envelope",
                        source,
                    })?;
                validate_envelope_header(&envelope)?;
                Ok(Some(envelope))
            }
            None => Ok(None),
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
            Some(envelope) => {
                validate_params_floor(
                    envelope.kdf.m_cost,
                    envelope.kdf.t_cost,
                    envelope.kdf.p_cost,
                )?;
                (
                    base64_decode(&envelope.kdf.salt_b64)?,
                    envelope.kdf.m_cost,
                    envelope.kdf.t_cost,
                    envelope.kdf.p_cost,
                )
            }
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
    ///
    /// Sealed under this instance's cached derivation with a fresh AEAD nonce,
    /// and written atomically over the retained file.
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
        let bytes = serde_json::to_vec(&envelope).map_err(|source| KeyError::Json {
            operation: "serialize passphrase envelope",
            source,
        })?;
        self.file.replace(&bytes).map_err(KeyError::File)
    }

    /// Remove the stored envelope. `Ok` when nothing was stored.
    pub(crate) fn forget(&self) -> Result<(), KeyError> {
        self.file.remove().map_err(KeyError::File)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }
}

/// Reject a header this module cannot safely act on: an envelope version this
/// build does not implement, or a KDF other than Argon2id. Runs on every read
/// that returns `Some`, before the file's declared version, algorithm, or
/// parameters ever reach [`derive_wrapping_key`] or the AEAD — the header is
/// unauthenticated (nothing has decrypted yet), so it is untrusted input that
/// this module must not act on blindly.
fn validate_envelope_header(envelope: &Envelope) -> Result<(), KeyError> {
    if envelope.v != ENVELOPE_VERSION {
        return Err(KeyError::UnsupportedPassphraseEnvelopeVersion {
            actual: envelope.v,
            expected: ENVELOPE_VERSION,
        });
    }
    if envelope.kdf.algo != ARGON2ID_ALGO {
        return Err(KeyError::UnsupportedPassphraseKdf {
            actual: envelope.kdf.algo.clone(),
            expected: ARGON2ID_ALGO,
        });
    }
    Ok(())
}

/// Reject Argon2id parameters weaker than this module's floor
/// (`ARGON2_M_COST_KIB`/`ARGON2_T_COST`/`ARGON2_P_COST`). These values come
/// from an existing file, read verbatim rather than regenerated from the
/// current constants, so that a future increase to the floor does not strand
/// a file already wrapped at the old (still-adequate) strength — reading a
/// file's own params is what makes that upgrade non-breaking. But nothing
/// authenticates the header before this point, so an on-disk value is
/// otherwise free for a file-write-capable attacker to set arbitrarily low;
/// this floor is what stops a subsequent `persist` from re-wrapping the real
/// secret at that attacker-chosen strength. Only a value below the floor is
/// refused — a file already wrapped at a stronger derivation than today's
/// constants call for unlocks unchanged.
fn validate_params_floor(m_cost: u32, t_cost: u32, p_cost: u32) -> Result<(), KeyError> {
    if m_cost < ARGON2_M_COST_KIB {
        return Err(KeyError::WeakArgon2Parameter {
            parameter: "m_cost in KiB",
            actual: m_cost,
            minimum: ARGON2_M_COST_KIB,
        });
    }
    if t_cost < ARGON2_T_COST {
        return Err(KeyError::WeakArgon2Parameter {
            parameter: "t_cost",
            actual: t_cost,
            minimum: ARGON2_T_COST,
        });
    }
    if p_cost < ARGON2_P_COST {
        return Err(KeyError::WeakArgon2Parameter {
            parameter: "p_cost",
            actual: p_cost,
            minimum: ARGON2_P_COST,
        });
    }
    Ok(())
}

fn derive_wrapping_key(
    passphrase: &str,
    salt: &[u8],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> Result<[u8; ARGON2_OUTPUT_LEN], KeyError> {
    let params =
        argon2::Params::new(m_cost, t_cost, p_cost, Some(ARGON2_OUTPUT_LEN)).map_err(|source| {
            KeyError::PassphraseKdf {
                operation: "parameter validation",
                source: Box::new(source),
            }
        })?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut out = [0u8; ARGON2_OUTPUT_LEN];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut out)
        .map_err(|source| KeyError::PassphraseKdf {
            operation: "derivation",
            source: Box::new(source),
        })?;
    Ok(out)
}

fn seal(wrapping_key: &[u8; ARGON2_OUTPUT_LEN], plaintext: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(wrapping_key));
    let mut nonce = vec![0u8; NONCE_LEN];
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
    if nonce.len() != NONCE_LEN {
        return Err(KeyError::InvalidLength {
            subject: "passphrase envelope nonce",
            expected: NONCE_LEN,
            actual: nonce.len(),
        });
    }
    let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(wrapping_key));
    let nonce = GenericArray::from_slice(nonce);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| KeyError::PassphraseEnvelopeDecryption)
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn base64_decode(s: &str) -> Result<Vec<u8>, KeyError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(KeyError::Base64)
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

    /// Write `envelope` straight to `path`, bypassing `PassphraseVault`
    /// entirely — stands in for a file an attacker planted, or one corrupted
    /// on disk, neither of which goes through this module's own `persist`.
    fn write_envelope(path: &Path, envelope: &Envelope) {
        std::fs::write(
            path,
            serde_json::to_vec(envelope).expect("serialize test envelope"),
        )
        .expect("write test envelope");
    }

    fn read_envelope_from_disk(path: &Path) -> Envelope {
        let bytes = std::fs::read(path).expect("read envelope file");
        serde_json::from_slice(&bytes).expect("parse envelope file")
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
        assert!(
            matches!(error, KeyError::PassphraseEnvelopeDecryption),
            "got {error:?}"
        );
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

    /// A file declaring Argon2id parameters below this module's floor —
    /// planted by whoever can write the file, without needing to read the
    /// passphrase — must not be honored: `unlock` refuses it, and `persist`
    /// must refuse too, since re-wrapping the real secret under those
    /// attacker-chosen parameters is exactly the escalation the floor exists
    /// to prevent.
    #[test]
    fn a_weak_params_envelope_is_refused() {
        let (_tmp, vault) = temp_vault("victim passphrase");
        let weak = Envelope {
            v: ENVELOPE_VERSION,
            kdf: KdfParams {
                algo: ARGON2ID_ALGO.to_string(),
                m_cost: 8,
                t_cost: 1,
                p_cost: 1,
                salt_b64: base64_encode(&[0u8; SALT_LEN]),
            },
            nonce_b64: base64_encode(&[0u8; NONCE_LEN]),
            ciphertext_b64: base64_encode(b"irrelevant: the floor rejects before decryption"),
        };
        write_envelope(vault.path(), &weak);

        let unlock_error = vault
            .unlock()
            .expect_err("weak Argon2id params must be refused");
        assert!(
            matches!(unlock_error, KeyError::WeakArgon2Parameter { .. }),
            "got {unlock_error:?}"
        );

        let persist_error = vault
            .persist(b"the real secret")
            .expect_err("persist must refuse to re-wrap under weak params rather than establish");
        assert!(
            matches!(persist_error, KeyError::WeakArgon2Parameter { .. }),
            "got {persist_error:?}"
        );

        // The refusal must happen before any write — the planted file, weak
        // params and all, is exactly as it was.
        let on_disk = read_envelope_from_disk(vault.path());
        assert_eq!(on_disk.kdf.m_cost, 8);
        assert_eq!(on_disk.kdf.t_cost, 1);
        assert_eq!(on_disk.kdf.p_cost, 1);
    }

    /// An envelope wrapped at parameters stronger than today's floor still
    /// unlocks: the stored values travel with the ciphertext precisely so a
    /// future increase to the module's constants does not strand an
    /// already-established file. Only a value *below* the floor is refused.
    #[test]
    fn an_envelope_with_higher_than_current_params_still_unlocks() {
        let (_tmp, vault) = temp_vault("upgrade passphrase");
        let salt = vec![7u8; SALT_LEN];
        let m_cost = ARGON2_M_COST_KIB * 2;
        let t_cost = ARGON2_T_COST + 1;
        let p_cost = ARGON2_P_COST + 1;
        let key = derive_wrapping_key("upgrade passphrase", &salt, m_cost, t_cost, p_cost)
            .expect("derive at higher-than-floor params");
        let (nonce, ciphertext) = seal(&key, b"payload sealed above the floor");
        let envelope = Envelope {
            v: ENVELOPE_VERSION,
            kdf: KdfParams {
                algo: ARGON2ID_ALGO.to_string(),
                m_cost,
                t_cost,
                p_cost,
                salt_b64: base64_encode(&salt),
            },
            nonce_b64: base64_encode(&nonce),
            ciphertext_b64: base64_encode(&ciphertext),
        };
        write_envelope(vault.path(), &envelope);

        let unlocked = vault
            .unlock()
            .expect("params above the floor must still unlock")
            .expect("payload present");
        assert_eq!(unlocked, b"payload sealed above the floor");
    }

    /// A file whose `nonce_b64` decodes to something other than 24 bytes (a
    /// corrupt file, not a wrong passphrase) must surface as `KeyError`, not
    /// panic `GenericArray::from_slice` — the module's stated contract is
    /// that a corrupt file is always `Err`.
    #[test]
    fn a_corrupt_nonce_is_an_error_not_a_panic() {
        let (_tmp, vault) = temp_vault("passphrase");
        vault.persist(b"payload").expect("establish");

        let mut envelope = read_envelope_from_disk(vault.path());
        envelope.nonce_b64 = base64_encode(&[0u8; 8]);
        write_envelope(vault.path(), &envelope);

        let error = vault
            .unlock()
            .expect_err("a corrupt nonce length must be an error, not a panic");
        assert!(
            matches!(error, KeyError::InvalidLength { .. }),
            "got {error:?}"
        );
    }

    /// An envelope naming any version other than this build's is refused —
    /// the check is on the version being the one implemented, not on which
    /// side of it the file falls.
    #[test]
    fn an_unknown_envelope_version_is_refused() {
        let (_tmp, vault) = temp_vault("passphrase");
        vault.persist(b"payload").expect("establish");
        let established = read_envelope_from_disk(vault.path());

        let mut older = established.clone();
        older.v = ENVELOPE_VERSION - 1;
        write_envelope(vault.path(), &older);
        let error = vault
            .unlock()
            .expect_err("an older envelope version must be refused");
        assert!(
            matches!(error, KeyError::UnsupportedPassphraseEnvelopeVersion { .. }),
            "got {error:?}"
        );

        let mut newer = established;
        newer.v = ENVELOPE_VERSION + 1;
        write_envelope(vault.path(), &newer);
        let error = vault
            .unlock()
            .expect_err("a newer envelope version must be refused");
        assert!(
            matches!(error, KeyError::UnsupportedPassphraseEnvelopeVersion { .. }),
            "got {error:?}"
        );
    }

    /// An envelope naming a KDF other than Argon2id is refused — the `algo`
    /// field exists to prevent KDF confusion, not to be read and ignored.
    #[test]
    fn an_unknown_kdf_algo_is_refused() {
        let (_tmp, vault) = temp_vault("passphrase");
        vault.persist(b"payload").expect("establish");

        let mut envelope = read_envelope_from_disk(vault.path());
        envelope.kdf.algo = "scrypt".to_string();
        write_envelope(vault.path(), &envelope);

        let error = vault
            .unlock()
            .expect_err("an unrecognized KDF algo must be refused");
        assert!(
            matches!(error, KeyError::UnsupportedPassphraseKdf { .. }),
            "got {error:?}"
        );
    }
}

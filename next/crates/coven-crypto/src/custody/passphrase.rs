//! Argon2id and an authenticated file, accessed only through `AtomicFile`.

use super::KeyError;
use crate::{cipher, randomness, wire, CryptoError, SecretBytes};
use coven_foundation::{files::AtomicFile, id_source::StoreId};
use std::marker::PhantomData;
use zeroize::Zeroizing;

/// A memorized secret that protects custody with Argon2id (§20.1).
pub struct Passphrase(Zeroizing<String>);

impl Passphrase {
    /// Take ownership of a passphrase and erase its allocation on drop.
    pub fn new(secret: String) -> Self {
        Self(Zeroizing::new(secret))
    }
}

impl std::fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Passphrase([REDACTED])")
    }
}

const HEADER: &[u8] = b"CVPF\x01";
const SALT_LEN: usize = 16;
// RFC 9106's memory-constrained Argon2id parameters, with a 256-bit output.
const WRITE_PARAMETERS: [u32; 3] = [65536, 3, 4];

/// Passphrase file custody for `StoreKeyring` or `MemberKeys` (§20.1).
/// The composition root supplies the corresponding reserved `AtomicFile`.
pub struct PassphraseCustody<T> {
    passphrase: Passphrase,
    file: AtomicFile,
    store: StoreId,
    material: PhantomData<fn() -> T>,
}

impl<T> PassphraseCustody<T> {
    /// Keep a passphrase and its store's reserved file for lazy unlocking.
    /// Construction reads no file and derives no key (§20.1).
    pub fn new(passphrase: Passphrase, file: AtomicFile, store: StoreId) -> Self {
        Self {
            passphrase,
            file,
            store,
            material: PhantomData,
        }
    }

    pub(crate) fn read(&self, kind: &str) -> Result<Option<SecretBytes>, KeyError> {
        let Some(bytes) = self.file.read_optional()? else {
            return Ok(None);
        };
        let mut body = bytes.as_slice();
        wire::prefix(&mut body, HEADER).map_err(|_| KeyError::PassphraseHeader)?;
        let version =
            u32::from_le_bytes(wire::array(&mut body).map_err(|_| KeyError::PassphraseHeader)?);
        if version != 0x13 {
            return Err(KeyError::PassphraseHeader);
        }
        let mut params = [0; 3];
        for param in &mut params {
            *param =
                u32::from_le_bytes(wire::array(&mut body).map_err(|_| KeyError::PassphraseHeader)?);
        }
        let salt = wire::array::<SALT_LEN>(&mut body).map_err(|_| KeyError::PassphraseHeader)?;
        if body.len() < 40 {
            return Err(KeyError::PassphraseHeader);
        }
        let header = bytes.get(..37).ok_or(KeyError::PassphraseHeader)?;
        let key = derive(&self.passphrase, &salt, read_parameters(params)?)?;
        let aad = cipher::context(&[header, self.store.0.as_bytes(), kind.as_bytes()]);
        match cipher::open_random(&key, &aad, body) {
            Ok(bytes) => Ok(Some(SecretBytes::new(bytes))),
            Err(CryptoError::Authentication) => Err(KeyError::PassphraseAuthentication),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn write(&self, kind: &str, plaintext: &[u8]) -> Result<(), KeyError> {
        // Only current writer parameters affect a new file. Reading uses the
        // exact authenticated parameters recorded in that particular file.
        let mut salt = [0; SALT_LEN];
        randomness::fill(&mut salt)?;
        let params = read_parameters(WRITE_PARAMETERS)
            .expect("custody writer parameters must satisfy the passphrase resource bounds");
        let key = derive(&self.passphrase, &salt, params)?;
        let mut header = HEADER.to_vec();
        header.extend_from_slice(&0x13u32.to_le_bytes());
        for param in WRITE_PARAMETERS {
            header.extend_from_slice(&param.to_le_bytes());
        }
        header.extend_from_slice(&salt);
        let aad = cipher::context(&[&header, self.store.0.as_bytes(), kind.as_bytes()]);
        header.extend(cipher::seal_random(&key, &aad, plaintext)?);
        self.file.replace(&header)?;
        Ok(())
    }

    pub(crate) fn remove(&self) -> Result<(), KeyError> {
        self.file.remove().map_err(KeyError::from)
    }
}

impl<T> std::fmt::Debug for PassphraseCustody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PassphraseCustody([REDACTED])")
    }
}

fn read_parameters(params: [u32; 3]) -> Result<argon2::Params, KeyError> {
    let [memory, iterations, lanes] = params;
    // These read bounds are independent of writer defaults: raising defaults
    // never strands a stored file. Upper bounds reject unauthenticated resource
    // exhaustion before allocating memory or performing Argon2 work.
    if !(65536..=1048576).contains(&memory)
        || !(3..=16).contains(&iterations)
        || !(1..=16).contains(&lanes)
    {
        return Err(KeyError::PassphraseParameters);
    }
    argon2::Params::new(memory, iterations, lanes, Some(32))
        .map_err(|_| KeyError::PassphraseParameters)
}

fn allocate_blocks(count: usize) -> Result<Zeroizing<Vec<argon2::Block>>, KeyError> {
    // Argon2's allocating convenience method frees its work area without
    // zeroizing it. Own that area so every return path erases it.
    let mut blocks = Zeroizing::new(Vec::new());
    blocks
        .try_reserve_exact(count)
        .map_err(|error| KeyError::Unavailable(Box::new(error)))?;
    blocks.resize(count, argon2::Block::default());
    Ok(blocks)
}

fn derive(
    passphrase: &Passphrase,
    salt: &[u8; SALT_LEN],
    params: argon2::Params,
) -> Result<Zeroizing<[u8; 32]>, KeyError> {
    let mut blocks = allocate_blocks(params.block_count())?;
    let argon = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut key = Zeroizing::new([0; 32]);
    argon.hash_password_into_with_memory(
        passphrase.0.as_bytes(),
        salt,
        key.as_mut(),
        blocks.as_mut_slice(),
    )
    .expect("custody Argon2id requires a 32-byte output, a 16-byte salt, sufficient blocks and a passphrase no longer than u32::MAX bytes");
    Ok(key)
}

#[cfg(test)]
#[path = "passphrase_tests.rs"]
mod tests;

//! The only source of random key material, salts and nonces in this crate.

use crate::CryptoError;

pub(crate) fn fill(bytes: &mut [u8]) -> Result<(), CryptoError> {
    getrandom::fill(bytes).map_err(CryptoError::Random)
}

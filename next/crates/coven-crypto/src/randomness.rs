//! The only source of random key material, salts and nonces in this crate.

use crate::CryptoError;

pub(crate) fn fill(bytes: &mut [u8]) -> Result<(), CryptoError> {
    fill_with(bytes, getrandom::fill)
}

fn fill_with(
    bytes: &mut [u8],
    source: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<(), CryptoError> {
    source(bytes).map_err(|error| CryptoError::Unavailable(Box::new(error)))
}

#[cfg(test)]
#[path = "randomness_tests.rs"]
mod tests;

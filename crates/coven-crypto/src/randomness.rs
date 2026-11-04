//! The only source of random key material, salts and nonces in this crate.

use crate::CryptoError;

pub(crate) fn fill(bytes: &mut [u8]) -> Result<(), CryptoError> {
    fill_with(bytes, getrandom::fill)
}

/// 32 fresh random bytes, erased on drop: the material of every key,
/// secret and seed coven makes.
pub(crate) fn secret_bytes() -> Result<zeroize::Zeroizing<[u8; 32]>, CryptoError> {
    let mut bytes = zeroize::Zeroizing::new([0; 32]);
    fill(bytes.as_mut())?;
    Ok(bytes)
}

/// A fresh random secret of 32 bytes as lowercase hex, for a credential coven
/// sets but nobody keeps, such as a service's parent user (Appendix E.12).
pub fn random_secret() -> Result<crate::SecretText, CryptoError> {
    Ok(crate::SecretText::new(hex::encode(secret_bytes()?.as_ref())))
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

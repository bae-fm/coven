//! The only source of random key material, salts and nonces in this crate.

use crate::CryptoError;

#[cfg(any(test, feature = "test-utils"))]
thread_local! {
    // Clippy asks for a `const` initializer this already has where the
    // target has no native thread locals, as Android: std builds those
    // from a function, which the lint reads as the initializer.
    #[cfg_attr(target_os = "android", allow(clippy::missing_const_for_thread_local))]
    static REFUSALS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Refuse the next `count` requests for randomness on this thread, as a
/// system whose random source fails would. Randomness comes from the system
/// with no handle to arm, and a thread's count leaves other tests alone.
#[cfg(any(test, feature = "test-utils"))]
pub fn refuse_randomness(count: usize) {
    REFUSALS.set(count);
}

pub(crate) fn fill(bytes: &mut [u8]) -> Result<(), CryptoError> {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(left) = REFUSALS.get().checked_sub(1) {
        REFUSALS.set(left);
        return fill_with(bytes, |_| Err(getrandom::Error::UNSUPPORTED));
    }
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

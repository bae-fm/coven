//! Bytes crossing a custody or restore-code boundary remain secrets.

use zeroize::Zeroizing;

/// Secret bytes for custody or a restore code, erased on drop and never printed.
/// Callers retaining a copy are responsible for erasing that copy too.
pub struct SecretBytes(pub(crate) Zeroizing<Vec<u8>>);

impl SecretBytes {
    /// Take ownership of secret bytes, including any allocation capacity.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Borrow the bytes while writing custody or encoding a restore code (§12).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretBytes([REDACTED])")
    }
}

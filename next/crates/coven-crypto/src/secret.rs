//! Bytes and text crossing custody or provider boundaries remain secrets.

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

    /// Allocation capacity for tests checking that secret encoders never grow it.
    #[cfg(feature = "test-utils")]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretBytes([REDACTED])")
    }
}

/// UTF-8 secret text, erased on drop and redacted in diagnostic output.
/// Callers retaining a copy are responsible for erasing that copy too.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SecretText(Zeroizing<String>);

impl SecretText {
    /// Take ownership of secret text, including its allocation capacity.
    pub fn new(text: String) -> Self {
        Self(Zeroizing::new(text))
    }

    /// Borrow text while making a provider request or committing to custody.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretText([REDACTED])")
    }
}

#[cfg(test)]
#[path = "secret_tests.rs"]
mod tests;

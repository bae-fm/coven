//! Location-only provider payloads used by restore codes.

use crate::{StorageConfig, StorageError, StorageFailure};
use coven_crypto::SecretBytes;

impl StorageConfig {
    /// Encode the validated location, without any storage credentials.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Decode only a provider location; unknown fields and credential payloads fail.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let location: Self = serde_json::from_slice(bytes)
            .map_err(|error| StorageFailure::Encoding.with_source(error))?;
        location.validate()?;
        Ok(location)
    }
}

#[cfg(test)]
#[path = "restore_storage_tests.rs"]
mod tests;

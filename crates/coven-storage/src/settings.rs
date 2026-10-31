use crate::{StorageConfig, StorageError};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{StoreDir, StoreFile};

/// Storage settings owned by the store directory; credentials never enter this file.
pub struct StorageSettings {
    directory: StoreDir,
}
impl StorageSettings {
    /// Bind settings to the directory supplied at the composition root.
    pub fn new(directory: StoreDir) -> Self {
        Self { directory }
    }
    /// Read the configured location, with absence distinct from malformed settings.
    pub fn read(&self) -> Result<Option<StorageConfig>, StorageError> {
        let Some(bytes) = self.directory.owned_file(StoreFile::StorageSettings).read_optional()?
        else {
            return Ok(None);
        };
        Ok(Some(StorageConfig::decode(&bytes)?))
    }
    /// Atomically commit the location after the facade has established the connection.
    pub fn commit(&self, config: &StorageConfig) -> Result<(), StorageError> {
        let bytes = config.encode()?;
        self.directory.owned_file(StoreFile::StorageSettings).replace(bytes.as_bytes())?;
        Ok(())
    }
    /// Remove settings after the facade successfully removes credentials.
    pub fn remove(&self) -> Result<(), StorageError> {
        Ok(self.directory.owned_file(StoreFile::StorageSettings).remove()?)
    }
}

impl StorageConfig {
    /// Encode the validated location as the settings file holds it, without
    /// any storage credentials.
    pub(crate) fn encode(&self) -> Result<coven_crypto::SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Decode the settings file's location; unknown fields and credential
    /// payloads fail.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let location: Self = serde_json::from_slice(bytes)
            .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
        location.validate()?;
        Ok(location)
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;

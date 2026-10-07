use crate::{StorageConfig, StorageError};
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
        let Some(bytes) = self
            .directory
            .owned_file(StoreFile::StorageSettings)
            .read_optional()?
        else {
            return Ok(None);
        };
        let config: StorageConfig = serde_json::from_slice(&bytes)
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        config.validate()?;
        Ok(Some(config))
    }
    /// Atomically commit the location after the facade has established the connection.
    pub fn commit(&self, config: &StorageConfig) -> Result<(), StorageError> {
        config.validate()?;
        self.directory
            .owned_file(StoreFile::StorageSettings)
            .replace(
                &serde_json::to_vec(config)
                    .map_err(|error| StorageError::Encoding(Box::new(error)))?,
            )?;
        Ok(())
    }
    /// Remove settings after the facade successfully removes credentials.
    pub fn remove(&self) -> Result<(), StorageError> {
        Ok(self
            .directory
            .owned_file(StoreFile::StorageSettings)
            .remove()?)
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
